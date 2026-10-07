//! The weight a coin or output adds to a transaction, and the fee for it.

use bitcoin::{Amount, FeeRate, Weight};

use crate::wallet::Utxo;

/// The non-witness fields every input carries: a 36 byte outpoint and a 4 byte nsequence.
const OUTPOINT_AND_SEQUENCE: Weight = Weight::from_vb_unchecked(40);

/// Anything that knows the weight it will take up in a transaction, such as a coin spent as
/// an input or an output created.
pub trait KnowsWeight {
    /// Weight it adds to the transaction.
    fn weight(&self) -> Weight;

    /// Fee for [`KnowsWeight::weight`] at `feerate`, rounded up. `None` if it overflows.
    fn fee(&self, feerate: FeeRate) -> Option<Amount> {
        feerate.fee_wu(self.weight())
    }
}

/// # Panics
///
/// If the satisfaction weight is so large that adding the outpoint and nsequence
/// overflows.
impl KnowsWeight for Utxo {
    fn weight(&self) -> Weight {
        self.satisfaction_weight
            .checked_add(OUTPOINT_AND_SEQUENCE)
            .expect("satisfaction weight fits in a block")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::absolute::LockTime;
    use bitcoin::transaction::{InputWeightPrediction, Version};
    use bitcoin::{OutPoint, ScriptBuf, Sequence, Transaction, TxIn, Txid, Witness, hashes::Hash};
    use proptest::prelude::*;

    struct Fixed(Weight);

    impl KnowsWeight for Fixed {
        fn weight(&self) -> Weight {
            self.0
        }
    }

    #[test]
    fn fee_is_weight_times_feerate() {
        let feerate = FeeRate::from_sat_per_vb(10).unwrap();

        assert_eq!(
            Fixed(Weight::from_vb(43).unwrap()).fee(feerate),
            Some(Amount::from_sat(430))
        );
    }

    #[test]
    fn fee_is_none_on_overflow() {
        assert_eq!(Fixed(Weight::MAX).fee(FeeRate::MAX), None);
    }

    fn coin(sats: u64, satisfaction_wu: u64) -> Utxo {
        Utxo::new(
            OutPoint::new(Txid::all_zeros(), 0),
            Amount::from_sat(sats),
            Weight::from_wu(satisfaction_wu),
        )
    }

    /// Weight a transaction gains from one more input, with a scriptSig of `script_sig_len`
    /// bytes and witness elements of `witness_lens` bytes.
    fn input_weight(script_sig_len: usize, witness_lens: &[usize]) -> Weight {
        let input = |script_sig_len: usize, witness_lens: &[usize]| TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::from_bytes(vec![0; script_sig_len]),
            sequence: Sequence::MAX,
            witness: Witness::from_slice(
                &witness_lens
                    .iter()
                    .map(|&len| vec![0; len])
                    .collect::<Vec<_>>(),
            ),
        };
        // The first input has a witness only if the added one does. Otherwise adding a legacy
        // input to a segwit transaction also adds its empty witness count, 1 WU that a
        // satisfaction weight leaves out.
        let witness_len: &[usize] = if witness_lens.is_empty() { &[] } else { &[64] };
        let mut tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: vec![input(0, witness_len)],
            output: vec![],
        };
        let before = tx.weight();
        tx.input.push(input(script_sig_len, witness_lens));

        tx.weight() - before
    }

    /// Up to 1,000,000 sat/vB.
    fn sane_feerate_sat_kwu() -> impl Strategy<Value = u64> {
        0..=250_000_000u64
    }

    #[test]
    fn a_taproot_key_spend_costs_its_input() {
        let satisfaction = InputWeightPrediction::P2TR_KEY_DEFAULT_SIGHASH.weight();
        let coin = coin(100_000, satisfaction.to_wu());
        assert_eq!(coin.weight(), input_weight(0, &[64]));

        let feerate = FeeRate::from_sat_per_vb(10).unwrap();
        assert_eq!(coin.fee(feerate), Some(Amount::from_sat(575)));
    }

    #[test]
    #[should_panic(expected = "satisfaction weight fits in a block")]
    fn a_coin_too_heavy_to_spend_panics() {
        coin(0, u64::MAX).weight();
    }

    proptest! {
        /// scriptSig and witness element lengths are > 252 bytes, where their length
        /// prefix grows from 1 to 3 bytes.
        #[test]
        fn a_coin_weighs_what_its_input_adds_to_a_transaction(
            script_sig_len in 0..=300usize,
            witness_lens in proptest::collection::vec(0..=300usize, 0..=4),
            sat_per_kwu in sane_feerate_sat_kwu(),
        ) {
            let satisfaction = InputWeightPrediction::new(script_sig_len, &witness_lens).weight();
            let coin = coin(0, satisfaction.to_wu());
            let weight = input_weight(script_sig_len, &witness_lens);

            prop_assert_eq!(coin.weight(), weight);
            prop_assert_eq!(
                coin.fee(FeeRate::from_sat_per_kwu(sat_per_kwu)),
                Some(Amount::from_sat((sat_per_kwu * weight.to_wu()).div_ceil(1000)))
            );
        }
    }
}

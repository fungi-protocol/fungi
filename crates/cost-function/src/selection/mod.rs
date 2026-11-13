//! Which coins fund a [`crate::batch::Batch`].

use bitcoin::{Amount, FeeRate, TxOut, Weight};
use rand::RngCore;

use crate::wallet::Utxo;

/// Version, locktime, segwit marker and flag, and one-byte input and output counts.
///
/// From 253 inputs or outputs their count takes 3 bytes. [`cover_count_growth`] adjusts a
/// given feerate so that by the time 3 byte counts are required, the additional 2 bytes of
/// shared overhead will be covered at the given feerate even when not accounted for explicitly.
pub(crate) const TX_OVERHEAD: Weight = Weight::from_wu(42);

/// Additional bytes required to encode an input or output count of 253 or more.
const COUNT_GROWTH_VB: u64 = 2;

/// Smallest theoretical output size: an 8 byte value and the 1 byte length of an empty script.
/// Every input is larger, since its outpoint and nsequence alone take 40 bytes.
const MIN_OUTPUT_VB: u64 = 9;

/// Adjust `feerate` so that if 253 inputs or outputs are added, the additional overhead of the
/// wider count is covered at the unadjusted feerate or higher, even if not accounted for
/// explicitly.
///
/// Multiplies `feerate` by `1 + 2 / (253 * 9)` and rounds up to a whole sat/kwu. At the adjusted
/// feerate, 253 outputs of the smallest size pay for 2 vB more than at `feerate`. Inputs and
/// larger outputs pay for more.
///
/// For nonzero feerates up to 1,138 sat/kwu (about 4.5 sat/vB), which includes the minimum
/// relay feerate where rounding can decide whether a transaction relays, this adds exactly
/// 1 sat/kwu.
pub(crate) fn cover_count_growth(feerate: FeeRate) -> Option<FeeRate> {
    let min_vb = 253 * MIN_OUTPUT_VB;
    let sat_per_kwu = feerate
        .to_sat_per_kwu()
        .checked_mul(min_vb + COUNT_GROWTH_VB)?
        .div_ceil(min_vb);
    Some(FeeRate::from_sat_per_kwu(sat_per_kwu))
}

pub(crate) mod random;

pub(crate) struct FundingRequest<'a> {
    /// Coins the selector may draw on.
    pub(crate) available: &'a [Utxo],

    /// What the transaction has to pay. This should not include change coming back to the wallet.
    pub(crate) outputs: &'a [TxOut],

    /// Feerate the transaction pays, used to price spending and creating coins.
    pub(crate) feerate: FeeRate,
}

/// What a `CoinSelector` returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InputSelection {
    /// Coins the selector chose from [`FundingRequest::available`].
    pub(crate) selected_inputs: Vec<Utxo>,
    /// Amount left over after the fees for spending the inputs, creating the
    /// [`FundingRequest::outputs`] and the transaction overhead.
    pub(crate) surplus: Amount,
}

/// A strategy for selecting coins
pub(crate) trait CoinSelector {
    type Error;

    /// Draws on `rng` for any random choice, so the same seed makes the same selection.
    fn select<R: RngCore>(
        &self,
        request: FundingRequest<'_>,
        rng: &mut R,
    ) -> Result<InputSelection, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockspace::KnowsWeight;
    use bitcoin::absolute::LockTime;
    use bitcoin::transaction::{InputWeightPrediction, Version};
    use bitcoin::{OutPoint, ScriptBuf, Sequence, Transaction, TxIn, Txid, Witness, hashes::Hash};
    use proptest::prelude::*;

    /// Build a transaction spending `inputs` taproot key path coins to `outputs` outputs with
    /// empty scripts, the smallest an output can be.
    fn transaction(inputs: usize, outputs: usize) -> (Vec<Utxo>, Transaction) {
        let satisfaction = InputWeightPrediction::P2TR_KEY_DEFAULT_SIGHASH.weight();
        let coins = (0..inputs)
            .map(|vout| {
                Utxo::new(
                    OutPoint::new(Txid::all_zeros(), vout as u32),
                    Amount::ZERO,
                    satisfaction,
                )
            })
            .collect();
        let tx = Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: (0..inputs)
                .map(|_| TxIn {
                    previous_output: OutPoint::null(),
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::MAX,
                    witness: Witness::from_slice(&[[0; 64]]),
                })
                .collect(),
            output: (0..outputs)
                .map(|_| TxOut {
                    value: Amount::ZERO,
                    script_pubkey: ScriptBuf::new(),
                })
                .collect(),
        };
        (coins, tx)
    }

    /// Sum the fees a selector and change strategy charge for `tx` at `feerate`: each coin and
    /// output priced on its own, plus [`TX_OVERHEAD`].
    fn charged(coins: &[Utxo], tx: &Transaction, feerate: FeeRate) -> Amount {
        coins
            .iter()
            .map(|c| c.fee(feerate).unwrap())
            .chain(tx.output.iter().map(|o| o.fee(feerate).unwrap()))
            .chain([feerate.fee_wu(TX_OVERHEAD).unwrap()])
            .sum()
    }

    /// Compute the fee `tx` needs at `feerate`.
    fn needed(tx: &Transaction, feerate: FeeRate) -> Amount {
        feerate.fee_wu(tx.weight()).unwrap()
    }

    #[test]
    fn the_minimum_relay_feerate_rises_by_one_sat_per_kwu() {
        assert_eq!(
            cover_count_growth(FeeRate::from_sat_per_kwu(250)),
            Some(FeeRate::from_sat_per_kwu(251))
        );
    }

    #[test]
    fn a_zero_feerate_stays_zero() {
        assert_eq!(cover_count_growth(FeeRate::ZERO), Some(FeeRate::ZERO));
    }

    #[test]
    fn an_overflowing_feerate_is_none() {
        assert_eq!(cover_count_growth(FeeRate::MAX), None);
    }

    /// 253 outputs grow the output count by 2 bytes, which [`TX_OVERHEAD`] leaves out. At
    /// 1,000 sat/vB the per-output rounding up to whole sats doesn't cover them; the raised
    /// feerate does.
    #[test]
    fn the_raised_feerate_pays_for_253_outputs() {
        let feerate = FeeRate::from_sat_per_vb(1_000).unwrap();
        let (coins, tx) = transaction(1, 253);

        assert!(charged(&coins, &tx, feerate) < needed(&tx, feerate));
        let raised = cover_count_growth(feerate).unwrap();
        assert!(charged(&coins, &tx, raised) >= needed(&tx, feerate));
    }

    /// As [`the_raised_feerate_pays_for_253_outputs`], with 253 inputs.
    #[test]
    fn the_raised_feerate_pays_for_253_inputs() {
        let feerate = FeeRate::from_sat_per_vb(1_000).unwrap();
        let (coins, tx) = transaction(253, 1);

        assert!(charged(&coins, &tx, feerate) < needed(&tx, feerate));
        let raised = cover_count_growth(feerate).unwrap();
        assert!(charged(&coins, &tx, raised) >= needed(&tx, feerate));
    }

    proptest! {
        /// Counts are drawn from either side of 252, where the count grows from 1 to 3
        /// bytes. Feerates go up to 1,000,000 sat/vB.
        #[test]
        fn the_raised_feerate_pays_for_the_whole_transaction(
            inputs in prop_oneof![1..=8usize, 250..=260usize],
            outputs in prop_oneof![0..=8usize, 250..=260usize],
            sat_per_kwu in 0..=250_000_000u64,
        ) {
            let feerate = FeeRate::from_sat_per_kwu(sat_per_kwu);
            let (coins, tx) = transaction(inputs, outputs);
            let raised = cover_count_growth(feerate).unwrap();

            prop_assert!(charged(&coins, &tx, raised) >= needed(&tx, feerate));
        }

        /// The raise is at most the multiplier plus 1 sat/kwu of rounding.
        #[test]
        fn the_raise_is_small(sat_per_kwu in 0..=250_000_000u64) {
            let raised = cover_count_growth(FeeRate::from_sat_per_kwu(sat_per_kwu))
                .unwrap()
                .to_sat_per_kwu();

            prop_assert!(raised >= sat_per_kwu);
            prop_assert!(raised <= sat_per_kwu + sat_per_kwu * 2 / (253 * 9) + 1);
        }
    }
}

//! Blockspace accounting.
//!
//! What it costs, in satoshis, to put something in a block. Everything here falls out of
//! consensus rules and the feerate.

use bitcoin::{Amount, FeeRate, SignedAmount, TxOut, Weight, transaction::InputWeightPrediction};

use crate::wallet::Utxo;

/// The non-witness fields every input carries: a 36 byte outpoint and a 4 byte nsequence.
/// [`InputWeightPrediction`] covers only the parts that depend on how the coin is spent. i.e what witness data is needed.
const OUTPOINT_AND_SEQUENCE: Weight = Weight::from_vb_unchecked(40);

/// A bare pubkey spend: a scriptSig pushing one signature of up to 72 bytes.
const P2PK_MAX: InputWeightPrediction = InputWeightPrediction::from_slice(73, &[]);

/// P2WPKH nested in P2SH: a scriptSig pushing the 22 byte witness program, and the same
/// witness as [`InputWeightPrediction::P2WPKH_MAX`].
const NESTED_P2WPKH_MAX: InputWeightPrediction = InputWeightPrediction::from_slice(23, &[72, 33]);

impl Utxo {
    /// Weight an input spending this coin adds, witness included.
    ///
    /// Unimplemented for anything else, whose spend size depends on a policy we don't
    /// know upfront.
    fn input_weight(&self) -> Weight {
        let script_pubkey = &self.prev_out.script_pubkey;
        let spend = if script_pubkey.is_p2tr() {
            InputWeightPrediction::P2TR_KEY_DEFAULT_SIGHASH
        } else if script_pubkey.is_p2wpkh() {
            InputWeightPrediction::P2WPKH_MAX
        } else if script_pubkey.is_p2sh() {
            NESTED_P2WPKH_MAX
        } else if script_pubkey.is_p2pkh() {
            InputWeightPrediction::P2PKH_COMPRESSED_MAX
        } else if script_pubkey.is_p2pk() {
            P2PK_MAX
        } else {
            unimplemented!("no input weight prediction for {script_pubkey:?}")
        };

        spend.weight() + OUTPOINT_AND_SEQUENCE
    }

    /// Value minus the blockspace needed to spend it, which is the number coin selection
    /// adds up.
    ///
    /// Negative means the coin costs more to spend than it carries. Dust at a given feerate.
    ///
    /// `None` if the feerate makes the spend cost overflow.
    ///
    /// Panics on unsupported input types: anything other than P2TR, P2WPKH, P2SH, P2PKH and
    /// P2PK.
    pub fn effective_value(&self, feerate: FeeRate) -> Option<SignedAmount> {
        let spend_cost = feerate.fee_wu(self.input_weight())?;
        Some(self.prev_out.value.to_signed().ok()? - spend_cost.to_signed().ok()?)
    }
}

/// Blockspace accounting for an output the wallet is considering creating.
pub trait EffectiveCost {
    /// Value plus the blockspace needed to create it at a given feerate.
    /// This is the cost of creating this output, i.e. the value leaving the wallet as the
    /// sender of a transaction.
    ///
    /// `None` if the fee at `feerate` overflows while it is computed, or if value plus fee
    /// does not fit in an [`Amount`].
    fn effective_cost(&self, feerate: FeeRate) -> Option<Amount>;
}

impl EffectiveCost for TxOut {
    fn effective_cost(&self, feerate: FeeRate) -> Option<Amount> {
        self.value.checked_add(feerate.fee_wu(self.weight())?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{OutPoint, PubkeyHash, ScriptBuf, ScriptHash, Txid, WPubkeyHash, hashes::Hash};

    fn p2tr_spk() -> ScriptBuf {
        let mut spk = vec![0x51, 0x20]; // OP_1 PUSH32
        spk.extend_from_slice(&[0xab; 32]);
        ScriptBuf::from_bytes(spk)
    }

    fn p2pk_spk() -> ScriptBuf {
        let key = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
            .parse()
            .expect("a valid compressed public key");

        ScriptBuf::new_p2pk(&key)
    }

    fn p2pkh_spk() -> ScriptBuf {
        ScriptBuf::new_p2pkh(&PubkeyHash::from_byte_array([0xcd; 20]))
    }

    fn p2sh_spk() -> ScriptBuf {
        ScriptBuf::new_p2sh(&ScriptHash::from_byte_array([0xcd; 20]))
    }

    fn utxo_with(script_pubkey: ScriptBuf, sats: u64) -> Utxo {
        Utxo {
            outpoint: OutPoint::new(Txid::all_zeros(), 0),
            prev_out: TxOut {
                value: Amount::from_sat(sats),
                script_pubkey,
            },
        }
    }

    fn p2tr_txout(sats: u64) -> TxOut {
        TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: p2tr_spk(),
        }
    }

    #[test]
    fn an_output_costs_its_value_plus_its_blockspace() {
        let txout = p2tr_txout(30_000);

        // 8 value + 1 length + 34 script = 43 vB.
        assert_eq!(txout.weight(), Weight::from_vb(43).unwrap());

        // At 10 sat/vB that is 430 sat to create, on top of what it carries.
        let feerate = FeeRate::from_sat_per_vb(10).unwrap();
        assert_eq!(
            txout.effective_cost(feerate).unwrap(),
            Amount::from_sat(30_430)
        );
    }

    /// A feerate no chain will ever see, but the arithmetic still has to say so rather
    /// than wrap.
    #[test]
    fn an_output_priced_past_the_money_supply_has_no_cost() {
        assert!(p2tr_txout(30_000).effective_cost(FeeRate::MAX).is_none());
    }

    /// The fee fits, but adding it to the value does not.
    #[test]
    fn an_output_worth_the_largest_amount_has_no_cost_at_a_normal_feerate() {
        let feerate = FeeRate::from_sat_per_vb(10).unwrap();

        assert!(p2tr_txout(u64::MAX).effective_cost(feerate).is_none());
    }

    #[test]
    fn a_taproot_coin_is_worth_its_value_less_the_spend() {
        let coin = utxo_with(p2tr_spk(), 100_000);

        // 40 vB outpoint and sequence, plus 70 WU for the scriptSig length and a default
        // sighash key path signature: 230 WU, or 57.5 vB.
        assert_eq!(coin.input_weight(), Weight::from_wu(230));

        // At 10 sat/vB that spend costs 575 sat.
        let feerate = FeeRate::from_sat_per_vb(10).unwrap();
        assert_eq!(
            coin.effective_value(feerate).unwrap(),
            SignedAmount::from_sat(99_425)
        );
    }

    #[test]
    fn a_segwit_v0_coin_pays_for_a_bigger_witness() {
        let coin = utxo_with(
            ScriptBuf::new_p2wpkh(&WPubkeyHash::from_byte_array([0xcd; 20])),
            100_000,
        );

        // The same 40 vB, plus 112 WU for a DER signature and a compressed key.
        assert_eq!(coin.input_weight(), Weight::from_wu(272));
    }

    #[test]
    fn a_nested_segwit_coin_also_pays_for_its_redeem_script() {
        let coin = utxo_with(p2sh_spk(), 100_000);

        // The P2WPKH input, plus 92 WU for a scriptSig pushing the 22 byte witness program.
        assert_eq!(coin.input_weight(), Weight::from_wu(364));
    }

    #[test]
    fn a_legacy_coin_pays_for_a_signature_and_a_key_in_its_script_sig() {
        let coin = utxo_with(p2pkh_spk(), 100_000);

        // The same 40 vB, plus 432 WU for a scriptSig carrying a signature and a key.
        assert_eq!(coin.input_weight(), Weight::from_wu(592));
    }

    /// A bare pubkey coin has its key in the script pubkey, so the scriptSig only
    /// carries the signature.
    #[test]
    fn a_bare_pubkey_coin_pays_for_a_signature_only() {
        let coin = utxo_with(p2pk_spk(), 100_000);

        // The same 40 vB, plus 296 WU for a scriptSig pushing a signature.
        assert_eq!(coin.input_weight(), Weight::from_wu(456));
    }

    /// A coin worth less than its own input is dust: spending it loses money.
    #[test]
    fn dust_has_negative_effective_value() {
        let coin = utxo_with(p2tr_spk(), 400);
        let feerate = FeeRate::from_sat_per_vb(10).unwrap();

        assert_eq!(
            coin.effective_value(feerate).unwrap(),
            SignedAmount::from_sat(-175)
        );
    }

    /// Script types whose spend size depends on a policy we don't have.
    #[test]
    #[should_panic(expected = "no input weight prediction")]
    fn a_coin_we_cannot_price_the_spend_of_is_unimplemented() {
        let coin = utxo_with(ScriptBuf::new(), 100_000);

        coin.effective_value(FeeRate::from_sat_per_vb(10).unwrap());
    }

    /// A coin's effective value and an output's effective cost have to agree, or coin
    /// selection cannot balance anything: paying a coin straight through to an identical
    /// output leaves exactly the transaction overhead unfunded.
    #[test]
    fn effective_value_and_cost_are_mirrors() {
        let feerate = FeeRate::from_sat_per_vb(10).unwrap();
        let coin = utxo_with(p2tr_spk(), 100_000);
        let payment = p2tr_txout(100_000);

        let have = coin.effective_value(feerate).unwrap();
        let need = payment
            .effective_cost(feerate)
            .unwrap()
            .to_signed()
            .unwrap();

        // Short by the input's 575 sat and the output's 430 sat.
        assert_eq!(need - have, SignedAmount::from_sat(1_005));
    }

    mod prop_tests {
        use super::*;
        use proptest::prelude::*;

        /// Bytes the compact size prefix takes to encode a script of `len` bytes. Scripts
        /// past 4 GiB would need 9, but none are generated.
        fn compact_size_len(len: usize) -> u128 {
            match len {
                0..=0xfc => 1,
                0xfd..=0xffff => 3,
                _ => 5,
            }
        }

        /// 8 bytes of value, the script's length prefix and the script, all non-witness so
        /// 4 wu a byte.
        fn expected_weight(script_len: usize) -> u128 {
            4 * (8 + compact_size_len(script_len) + script_len as u128)
        }

        /// The cost of an output given the spk length and feerate.
        fn expected_effective_cost(sats: u64, script_len: usize, sat_per_kwu: u64) -> u128 {
            // u128 is sufficient that nothing overflows.
            let fee = (u128::from(sat_per_kwu) * expected_weight(script_len)).div_ceil(1000);
            u128::from(sats) + fee
        }

        /// Txout helper to reduce boilerplate.
        fn txout(sats: u64, script_len: usize) -> TxOut {
            TxOut {
                value: Amount::from_sat(sats),
                script_pubkey: ScriptBuf::from_bytes(vec![0xab; script_len]),
            }
        }

        /// Script lengths clustered around where the length prefix grows, plus anything
        /// up to a size no standard output should reach.
        fn script_len() -> impl Strategy<Value = usize> {
            prop_oneof![0..=300usize, 0xfff0..=0x1_0010usize, 0..=200_000usize,]
        }

        pub(super) fn money() -> impl Strategy<Value = u64> {
            0..=Amount::MAX_MONEY.to_sat()
        }

        /// Up to 1,000,000 sat/vB
        pub(super) fn sane_feerate_sat_kwu() -> impl Strategy<Value = u64> {
            0..=250_000_000u64
        }

        fn any_feerate() -> impl Strategy<Value = u64> {
            prop_oneof![sane_feerate_sat_kwu(), any::<u64>()]
        }

        proptest! {
            #[test]
            fn cost_is_value_plus_weight_times_feerate(
                sats in money(),
                len in script_len(),
                sat_per_kwu in sane_feerate_sat_kwu(),
            ) {
                let cost = txout(sats, len)
                    .effective_cost(FeeRate::from_sat_per_kwu(sat_per_kwu))
                    .expect("realistic inputs never overflow");

                prop_assert_eq!(u128::from(cost.to_sat()), expected_effective_cost(sats, len, sat_per_kwu));
            }

            #[test]
            fn cost_is_never_less_than_value(
                sats in any::<u64>(),
                len in script_len(),
                sat_per_kwu in any_feerate(),
            ) {
                if let Some(cost) = txout(sats, len).effective_cost(FeeRate::from_sat_per_kwu(sat_per_kwu)) {
                    prop_assert!(cost >= Amount::from_sat(sats));
                }
            }

            #[test]
            fn a_longer_script_never_costs_less(
                sats in money(),
                len in script_len(),
                extra in 0..=1_000usize,
                sat_per_kwu in sane_feerate_sat_kwu(),
            ) {
                let feerate = FeeRate::from_sat_per_kwu(sat_per_kwu);

                prop_assert!(
                    txout(sats, len).effective_cost(feerate)
                        <= txout(sats, len + extra).effective_cost(feerate)
                );
            }

            #[test]
            fn a_higher_feerate_never_costs_less(
                sats in money(),
                len in script_len(),
                sat_per_kwu in sane_feerate_sat_kwu(),
                bump in 0..=1_000_000u64,
            ) {
                let txout = txout(sats, len);

                prop_assert!(
                    txout.effective_cost(FeeRate::from_sat_per_kwu(sat_per_kwu))
                        <= txout.effective_cost(FeeRate::from_sat_per_kwu(sat_per_kwu + bump))
                );
            }

            /// Every extra sat the output carries is one more sat it costs.
            #[test]
            fn cost_moves_one_for_one_with_value(
                sats in money(),
                more in 0..=Amount::MAX_MONEY.to_sat(),
                len in script_len(),
                sat_per_kwu in sane_feerate_sat_kwu(),
            ) {
                let feerate = FeeRate::from_sat_per_kwu(sat_per_kwu);
                let base = txout(sats, len).effective_cost(feerate).unwrap();
                let bigger = txout(sats + more, len).effective_cost(feerate).unwrap();

                prop_assert_eq!(bigger - base, Amount::from_sat(more));
            }

            /// Blockspace is priced by size, so what the contents of the scriptpubkey does not matter.
            #[test]
            fn only_the_script_length_matters(
                sats in money(),
                script in proptest::collection::vec(any::<u8>(), 0..=600),
                sat_per_kwu in sane_feerate_sat_kwu(),
            ) {
                let feerate = FeeRate::from_sat_per_kwu(sat_per_kwu);
                let len = script.len();
                let arbitrary = TxOut {
                    value: Amount::from_sat(sats),
                    script_pubkey: ScriptBuf::from_bytes(script),
                };

                prop_assert_eq!(
                    arbitrary.effective_cost(feerate),
                    txout(sats, len).effective_cost(feerate)
                );
            }
        }
    }

    /// Properties of what a coin is worth once its own input is paid for.
    mod spend_prop_tests {
        use super::prop_tests::{money, sane_feerate_sat_kwu};
        use super::*;
        use proptest::prelude::*;

        /// Weight of the input spending each kind of coin we can price, outpoint and
        /// nsequence included.
        const P2TR_INPUT_WU: u64 = 230;
        const P2WPKH_INPUT_WU: u64 = 272;
        const NESTED_P2WPKH_INPUT_WU: u64 = 364;
        const P2PKH_INPUT_WU: u64 = 592;
        const P2PK_INPUT_WU: u64 = 456;

        /// A coin we can price the spend of, paired with the weight its input takes.
        fn spendable_coin() -> impl Strategy<Value = (Utxo, u64)> {
            let spk = prop_oneof![
                Just((p2tr_spk(), P2TR_INPUT_WU)),
                Just((p2pk_spk(), P2PK_INPUT_WU)),
                any::<[u8; 20]>().prop_map(|hash| (
                    ScriptBuf::new_p2wpkh(&WPubkeyHash::from_byte_array(hash)),
                    P2WPKH_INPUT_WU
                )),
                any::<[u8; 20]>().prop_map(|hash| (
                    ScriptBuf::new_p2sh(&ScriptHash::from_byte_array(hash)),
                    NESTED_P2WPKH_INPUT_WU
                )),
                any::<[u8; 20]>().prop_map(|hash| (
                    ScriptBuf::new_p2pkh(&PubkeyHash::from_byte_array(hash)),
                    P2PKH_INPUT_WU
                )),
            ];

            (money(), spk).prop_map(|(sats, (spk, input_wu))| (utxo_with(spk, sats), input_wu))
        }

        /// What the spend costs: the input's weight priced per kwu and rounded up.
        fn expected_spend_cost(input_wu: u64, sat_per_kwu: u64) -> i128 {
            (u128::from(sat_per_kwu) * u128::from(input_wu)).div_ceil(1000) as i128
        }

        proptest! {
            #[test]
            fn a_coin_is_worth_its_value_less_its_spend(
                (coin, input_wu) in spendable_coin(),
                sat_per_kwu in sane_feerate_sat_kwu(),
            ) {
                let feerate = FeeRate::from_sat_per_kwu(sat_per_kwu);

                prop_assert_eq!(coin.input_weight(), Weight::from_wu(input_wu));
                prop_assert_eq!(
                    i128::from(coin.effective_value(feerate).unwrap().to_sat()),
                    i128::from(coin.prev_out.value.to_sat())
                        - expected_spend_cost(input_wu, sat_per_kwu)
                );
            }

            /// The input's weight comes from how the coin is spent, never from what it
            /// carries.
            #[test]
            fn what_a_coin_carries_does_not_change_its_input_weight(
                (coin, _) in spendable_coin(),
                other_sats in money(),
            ) {
                let other = utxo_with(coin.prev_out.script_pubkey.clone(), other_sats);

                prop_assert_eq!(coin.input_weight(), other.input_weight());
            }

            /// Blockspace only ever costs, so a coin is never worth more than its value,
            /// and costs more to spend as the feerate climbs.
            #[test]
            fn spending_only_ever_costs(
                (coin, _) in spendable_coin(),
                sat_per_kwu in sane_feerate_sat_kwu(),
                bump in 0..=1_000_000u64,
            ) {
                let value = coin.prev_out.value.to_signed().unwrap();
                let worth = coin.effective_value(FeeRate::from_sat_per_kwu(sat_per_kwu)).unwrap();
                let dearer = coin
                    .effective_value(FeeRate::from_sat_per_kwu(sat_per_kwu + bump))
                    .unwrap();

                prop_assert!(worth <= value);
                prop_assert!(dearer <= worth);
            }

            /// Nothing is charged at a feerate of zero, so the coin is worth exactly what
            /// it carries.
            #[test]
            fn a_free_block_charges_nothing_for_the_spend((coin, _) in spendable_coin()) {
                prop_assert_eq!(
                    coin.effective_value(FeeRate::from_sat_per_kwu(0)).unwrap(),
                    coin.prev_out.value.to_signed().unwrap()
                );
            }

            /// Paying a coin straight through to an identical output leaves exactly the
            /// two pieces of blockspace unfunded, which is what coin selection has to
            /// cover.
            #[test]
            fn effective_value_and_cost_are_mirrors(
                (coin, input_wu) in spendable_coin(),
                sat_per_kwu in sane_feerate_sat_kwu(),
            ) {
                let feerate = FeeRate::from_sat_per_kwu(sat_per_kwu);
                let payment = coin.prev_out.clone();

                let have = coin.effective_value(feerate).unwrap();
                let need = payment.effective_cost(feerate).unwrap().to_signed().unwrap();

                let output_wu = payment.weight().to_wu();
                prop_assert_eq!(
                    i128::from((need - have).to_sat()),
                    expected_spend_cost(input_wu, sat_per_kwu)
                        + expected_spend_cost(output_wu, sat_per_kwu)
                );
            }
        }
    }
}

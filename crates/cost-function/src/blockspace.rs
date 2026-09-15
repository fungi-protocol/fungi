//! Blockspace accounting.
//!
//! What it costs, in satoshis, to put something in a block. Everything here falls out of
//! consensus rules and the feerate.

use bitcoin::{Amount, FeeRate, TxOut};

/// Blockspace accounting for an output the wallet is considering creating.
pub trait EffectiveCost {
    /// Value plus the blockspace needed to create it at a given feerate.
    /// This is effectively the cost of spending this output. i.e value leaving the wallet
    /// as a sender of a transaction.
    ///
    /// `None` if the feerate makes the sum overflow.
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
    use bitcoin::{ScriptBuf, Weight};

    fn p2tr_txout(sats: u64) -> TxOut {
        let mut spk = vec![0x51, 0x20]; // OP_1 PUSH32
        spk.extend_from_slice(&[0xab; 32]);

        TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: ScriptBuf::from_bytes(spk),
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
}

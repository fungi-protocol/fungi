//! What a simulated transaction costs the wallet.

use std::collections::HashSet;

use bitcoin::{FeeRate, OutPoint, SignedAmount};

use crate::wallet::{Utxo, WalletStateSnapshot};

/// Prices the move from one wallet state snapshot to another.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Scorer;

impl Scorer {
    /// The effective value of the coins spent getting from `before` to `after`, minus the
    /// value of the change the wallet gets back.
    ///
    /// Negative when the change is worth more than the effective value of the spent coins.
    pub fn cost(
        &self,
        before: &WalletStateSnapshot,
        after: &WalletStateSnapshot,
        feerate: FeeRate,
    ) -> SignedAmount {
        let spent: SignedAmount = spent(before, after)
            .map(|coin| {
                coin.effective_value(feerate)
                    .expect("feerate low enough to price a spend")
            })
            .sum();
        let change: SignedAmount = credited(before, after)
            .map(|coin| {
                coin.prev_out
                    .value
                    .to_signed()
                    .expect("a coin never carries more than MAX_MONEY")
            })
            .sum();

        spent - change
    }
}

/// Coins `before` holds that `after` does not.
fn spent<'a>(
    before: &'a WalletStateSnapshot,
    after: &WalletStateSnapshot,
) -> impl Iterator<Item = &'a Utxo> {
    let kept: HashSet<OutPoint> = after.utxos().iter().map(|coin| coin.outpoint).collect();

    before
        .utxos()
        .iter()
        .filter(move |coin| !kept.contains(&coin.outpoint))
}

/// Coins `after` holds that `before` did not: the change.
fn credited<'a>(
    before: &WalletStateSnapshot,
    after: &'a WalletStateSnapshot,
) -> impl Iterator<Item = &'a Utxo> {
    spent(after, before)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{Amount, ScriptBuf, TxOut, Txid, hashes::Hash};

    fn utxo(vout: u32, sats: u64) -> Utxo {
        Utxo {
            outpoint: OutPoint::new(Txid::all_zeros(), vout),
            prev_out: TxOut {
                value: Amount::from_sat(sats),
                script_pubkey: ScriptBuf::new(),
            },
        }
    }

    fn wallet(utxos: Vec<Utxo>) -> WalletStateSnapshot {
        WalletStateSnapshot::new(utxos, [])
    }

    #[test]
    fn spent_coins_leave_and_credited_coins_arrive() {
        let before = wallet(vec![utxo(0, 100_000), utxo(1, 5_000)]);
        let after = wallet(vec![utxo(1, 5_000), utxo(2, 69_000)]);

        assert_eq!(
            spent(&before, &after).collect::<Vec<_>>(),
            [&utxo(0, 100_000)]
        );
        assert_eq!(
            credited(&before, &after).collect::<Vec<_>>(),
            [&utxo(2, 69_000)]
        );
    }

    #[test]
    fn nothing_spent_and_nothing_returned_costs_nothing() {
        let before = wallet(vec![utxo(0, 100_000)]);

        assert_eq!(
            Scorer.cost(&before, &before.clone(), FeeRate::from_sat_per_vb_u32(1)),
            SignedAmount::ZERO
        );
    }

    /// Spending a 100,000 sat taproot coin at 10 sat/vB costs 575 sat of blockspace, and
    /// 69,000 sat comes back as change.
    #[test]
    fn cost_is_what_the_spent_coins_were_worth_less_the_change() {
        let mut p2tr = vec![0x51, 0x20]; // OP_1 PUSH32
        p2tr.extend_from_slice(&[0xab; 32]);
        let coin = |vout, sats| Utxo {
            prev_out: TxOut {
                value: Amount::from_sat(sats),
                script_pubkey: ScriptBuf::from_bytes(p2tr.clone()),
            },
            ..utxo(vout, 0)
        };

        let before = wallet(vec![coin(0, 100_000), coin(1, 5_000)]);
        let after = wallet(vec![coin(1, 5_000), coin(2, 69_000)]);

        assert_eq!(
            Scorer.cost(&before, &after, FeeRate::from_sat_per_vb_u32(10)),
            SignedAmount::from_sat(30_425)
        );
    }
}

//! What a simulated transaction costs the wallet.

use std::collections::HashSet;

use bitcoin::{OutPoint, SignedAmount};

use crate::wallet::{Utxo, WalletStateSnapshot};

/// Prices the move from one wallet state snapshot to another.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Scorer;

impl Scorer {
    /// The value of the coins spent getting from `before` to `after`, minus the value of
    /// the change the wallet gets back: what the payments and the fee take out of the
    /// wallet.
    ///
    /// The fee is already paid out of the spent coins, so it is not added again.
    ///
    /// Negative when the change is worth more than the spent coins.
    pub fn cost(&self, before: &WalletStateSnapshot, after: &WalletStateSnapshot) -> SignedAmount {
        let spent: SignedAmount = spent(before, after)
            .map(|coin| {
                coin.value
                    .to_signed()
                    .expect("a coin never carries more than MAX_MONEY")
            })
            .sum();
        let change: SignedAmount = credited(before, after)
            .map(|coin| {
                coin.value
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
    use bitcoin::{Amount, Txid, Weight, hashes::Hash};

    fn utxo(vout: u32, sats: u64) -> Utxo {
        Utxo::new(
            OutPoint::new(Txid::all_zeros(), vout),
            Amount::from_sat(sats),
            Weight::ZERO,
        )
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

        assert_eq!(Scorer.cost(&before, &before.clone()), SignedAmount::ZERO);
    }

    /// A 100,000 sat coin pays 30,000 sat to someone else and 1,000 sat to the miner,
    /// and 69,000 sat comes back as change.
    #[test]
    fn cost_is_what_the_spent_coins_were_worth_less_the_change() {
        let before = wallet(vec![utxo(0, 100_000), utxo(1, 5_000)]);
        let after = wallet(vec![utxo(1, 5_000), utxo(2, 69_000)]);

        assert_eq!(Scorer.cost(&before, &after), SignedAmount::from_sat(31_000));
    }
}

//! Steps the wallet takes to construct a transaction.

use bitcoin::{Amount, SignedAmount, TxOut};

use crate::change::OutputWithoutScriptpubkey;
use crate::wallet::Utxo;

/// One step in constructing a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Spend a coin the wallet holds.
    RegisterInput(Utxo),
    /// Add an output to the transaction.
    RegisterOutput(TxOut),
    /// Add an change output,
    RegisterChangeOutPut(OutputWithoutScriptpubkey),
}

/// Actions without a known time of execution
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct UnSequencedActions(pub(crate) Vec<Action>);

impl UnSequencedActions {
    /// The coins the transaction spends.
    pub(crate) fn spent(&self) -> impl Iterator<Item = &Utxo> {
        self.0.iter().filter_map(|action| match action {
            Action::RegisterInput(utxo) => Some(utxo),
            _ => None,
        })
    }

    /// The coins the transaction pays back to the wallet.
    pub(crate) fn change(&self) -> impl Iterator<Item = &OutputWithoutScriptpubkey> {
        self.0.iter().filter_map(|action| match action {
            Action::RegisterChangeOutPut(output) => Some(output),
            _ => None,
        })
    }

    /// What the payments and the fee take out of the wallet: the value of the coins spent,
    /// minus the value of the change paid back. The fee is already paid out of the spent
    /// coins, so it is not added again.
    ///
    /// `None` if any value, or a sum of them, overflows a [`SignedAmount`].
    pub(crate) fn objective_cost(&self) -> Option<SignedAmount> {
        let spent = sum(self.spent().map(|coin| coin.value))?;
        let change = sum(self.change().map(|output| output.value))?;
        spent.checked_sub(change)
    }
}

/// `None` if a value or the sum overflows a [`SignedAmount`].
fn sum(mut values: impl Iterator<Item = Amount>) -> Option<SignedAmount> {
    values.try_fold(SignedAmount::ZERO, |sum, value| {
        sum.checked_add(value.to_signed().ok()?)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{OutPoint, ScriptBuf, Txid, Weight, hashes::Hash};

    fn coin(vout: u32, sats: u64) -> Utxo {
        Utxo::new(
            OutPoint::new(Txid::all_zeros(), vout),
            Amount::from_sat(sats),
            Weight::ZERO,
        )
    }

    fn change(sats: u64) -> OutputWithoutScriptpubkey {
        OutputWithoutScriptpubkey {
            value: Amount::from_sat(sats),
            weight: Weight::ZERO,
            satisfaction_weight: Weight::ZERO,
            dust: Amount::ZERO,
        }
    }

    /// A transaction spending a coin of each value in `spent`, paying `paid` to someone
    /// else and paying back a change output of each value in `change`.
    fn actions(spent: &[u64], paid: u64, change: &[u64]) -> UnSequencedActions {
        let mut actions: Vec<Action> = spent
            .iter()
            .zip(0..)
            .map(|(&sats, vout)| Action::RegisterInput(coin(vout, sats)))
            .collect();
        actions.push(Action::RegisterOutput(TxOut {
            value: Amount::from_sat(paid),
            script_pubkey: ScriptBuf::new(),
        }));
        actions.extend(
            change
                .iter()
                .map(|&sats| Action::RegisterChangeOutPut(self::change(sats))),
        );
        UnSequencedActions(actions)
    }

    #[test]
    fn spent_and_change_pick_out_their_actions_in_order() {
        let actions = actions(&[10_000, 5_000], 1_000, &[7_000, 6_000]);

        assert_eq!(
            actions.spent().cloned().collect::<Vec<_>>(),
            [coin(0, 10_000), coin(1, 5_000)]
        );
        assert_eq!(
            actions.change().cloned().collect::<Vec<_>>(),
            [change(7_000), change(6_000)]
        );
    }

    #[test]
    fn nothing_spent_and_nothing_returned_costs_nothing() {
        assert_eq!(
            actions(&[], 0, &[]).objective_cost(),
            Some(SignedAmount::ZERO)
        );
    }

    /// A 100,000 sat coin pays 30,000 sat to someone else and 1,000 sat to the miner,
    /// and 69,000 sat comes back as change.
    #[test]
    fn cost_is_what_the_spent_coins_were_worth_less_the_change() {
        assert_eq!(
            actions(&[100_000], 30_000, &[69_000]).objective_cost(),
            Some(SignedAmount::from_sat(31_000))
        );
    }

    /// Two coins together are worth more than a [`SignedAmount`] holds.
    #[test]
    fn spent_coins_worth_more_than_a_signed_amount_holds_have_no_cost() {
        assert_eq!(actions(&[1 << 62, 1 << 62], 0, &[]).objective_cost(), None);
    }
}

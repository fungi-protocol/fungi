//! What a simulated transaction costs the wallet.

use std::time::Instant;

use bitcoin::SignedAmount;

use crate::simulate::WalletStateDiff;

/// Prices what one transaction changes about the wallet.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Scorer;

impl Scorer {
    /// The value of the coins `diff` spends, minus the value of the change it pays back:
    /// what the payments and the fee take out of the wallet.
    ///
    /// The fee is already paid out of the spent coins, so it is not added again.
    ///
    /// Negative when the change is worth more than the spent coins. `None` if any value,
    /// or a sum of them, overflows.
    pub(crate) fn objective_cost(&self, diff: &WalletStateDiff) -> Option<SignedAmount> {
        let spent = diff
            .spent
            .iter()
            .try_fold(SignedAmount::ZERO, |sum, coin| {
                sum.checked_add(coin.value.to_signed().ok()?)
            })?;
        let change = diff
            .change
            .iter()
            .try_fold(SignedAmount::ZERO, |sum, output| {
                sum.checked_add(output.value.to_signed().ok()?)
            })?;

        spent.checked_sub(change)
    }

    /// The utility of the payments realized at `instant`.
    /// Utility is represented as a signed amount, negative values being penalties for most likely missing a deadline.
    pub(crate) fn payment_utility(
        &self,
        diff: &WalletStateDiff,
        instant: Instant,
    ) -> Option<SignedAmount> {
        // Note: currently payoff() can never return a Signed amount but we may want to support penalizing late payments
        diff.realized
            .iter()
            .try_fold(SignedAmount::ZERO, |sum, intent| {
                sum.checked_add(intent.payoff_curve.payoff(instant).to_signed().ok()?)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change::OutputWithoutScriptpubkey;
    use crate::intent::{Action, FixedPaymentInstructions, Intent, PayoffCurve};
    use crate::wallet::Utxo;
    use bitcoin::{Amount, OutPoint, ScriptBuf, Txid, Weight, hashes::Hash};
    use std::time::Duration;

    /// A transaction spending a coin of each value in `spent` and paying back a change
    /// output of each value in `change`.
    fn diff(spent: &[u64], change: &[u64]) -> WalletStateDiff {
        WalletStateDiff {
            spent: spent
                .iter()
                .zip(0..)
                .map(|(&sats, vout)| {
                    Utxo::new(
                        OutPoint::new(Txid::all_zeros(), vout),
                        Amount::from_sat(sats),
                        Weight::ZERO,
                    )
                })
                .collect(),
            change: change
                .iter()
                .map(|&sats| OutputWithoutScriptpubkey {
                    value: Amount::from_sat(sats),
                    weight: Weight::ZERO,
                    satisfaction_weight: Weight::ZERO,
                    dust: Amount::ZERO,
                })
                .collect(),
            realized: Vec::new(),
            still_owed: Vec::new(),
        }
    }

    #[test]
    fn nothing_spent_and_nothing_returned_costs_nothing() {
        assert_eq!(
            Scorer.objective_cost(&diff(&[], &[])),
            Some(SignedAmount::ZERO)
        );
    }

    /// A 100,000 sat coin pays 30,000 sat to someone else and 1,000 sat to the miner,
    /// and 69,000 sat comes back as change.
    #[test]
    fn cost_is_what_the_spent_coins_were_worth_less_the_change() {
        assert_eq!(
            Scorer.objective_cost(&diff(&[100_000], &[69_000])),
            Some(SignedAmount::from_sat(31_000))
        );
    }

    /// Two coins together are worth more than a [`SignedAmount`] holds.
    #[test]
    fn spent_coins_worth_more_than_a_signed_amount_holds_have_no_cost() {
        assert_eq!(Scorer.objective_cost(&diff(&[1 << 62, 1 << 62], &[])), None);
    }

    /// Each realized intent adds what it is worth at that instant.
    #[test]
    fn utility_sums_realized_payoffs() {
        let now = Instant::now();
        let worth = |sats| {
            Intent::new(
                Action::OutputCreation(FixedPaymentInstructions {
                    script_pubkey: ScriptBuf::new(),
                    amount: Amount::from_sat(sats),
                }),
                PayoffCurve::new(now, [(Duration::ZERO, Amount::from_sat(sats))]),
            )
        };
        let diff = WalletStateDiff {
            realized: vec![worth(100), worth(200)],
            ..diff(&[], &[])
        };

        assert_eq!(
            Scorer.payment_utility(&diff, now),
            Some(SignedAmount::from_sat(300))
        );
    }
}

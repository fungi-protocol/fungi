//! What the wallet owns, and what it has been asked to do.

use bitcoin::{Amount, FeeRate, OutPoint, TxIn, TxOut, Weight};
use rand::RngCore;
use std::collections::BTreeMap;

use crate::action::UnSequencedActions;
use crate::change::{ChangeSelector, ChangeStrategy, ScriptpubkeyAssigner};
use crate::decision_space::DecisionSpace;
use crate::decision_tree::{DecisionTree, Leaf};
use crate::intent::Intent;
use crate::queue::Queue;
use crate::selection::{CoinSelector, FundingRequest};

/// A coin the wallet can spend.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Utxo {
    pub(crate) outpoint: OutPoint,
    pub(crate) value: Amount,
    pub(crate) satisfaction_weight: Weight,
}

impl Utxo {
    /// `satisfaction_weight` is the weight of the scriptSig and witness that spend this
    /// coin, excluding the outpoint and nsequence.
    pub fn new(outpoint: OutPoint, value: Amount, satisfaction_weight: Weight) -> Self {
        Utxo {
            outpoint,
            value,
            satisfaction_weight,
        }
    }

    pub(crate) fn to_txin(&self) -> TxIn {
        TxIn {
            previous_output: self.outpoint,
            // TODO: the default nsequence, Sequence::MAX, should reflect wallet preferences.
            ..Default::default()
        }
    }
}

/// The coins a wallet holds, as it reports them.
pub trait UtxoSnapshot {
    /// Every coin, in no particular order.
    fn iter(&self) -> impl Iterator<Item = Utxo>;
}

/// What the wallet owns and what it has been asked to do, at one moment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalletStateSnapshot<Q> {
    utxos: Vec<Utxo>,
    queue: Q,
}

impl<Q: Queue> WalletStateSnapshot<Q>
where
    Q::Id: Ord,
{
    /// The coins the wallet holds, together with the queue of intents it has yet to satisfy.
    pub fn new(utxos: impl IntoIterator<Item = Utxo>, queue: Q) -> Self {
        WalletStateSnapshot {
            utxos: utxos.into_iter().collect(),
            queue,
        }
    }

    /// The actions that realize the queue: funded by `selector`, with change from
    /// `change_strategy`. `None` if the queue is empty.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "no driver loop asks for a plan yet")
    )]
    pub(crate) fn best_plan<S, C, A, R>(
        &self,
        feerate: FeeRate,
        selector: &S,
        rng: &mut R,
        change_strategy: &ChangeStrategy<C, A>,
    ) -> PlanResult<S::Error, C::Error, A::Error>
    where
        S: CoinSelector,
        C: ChangeSelector,
        A: ScriptpubkeyAssigner,
        R: RngCore,
    {
        // MVP: the decision space yields only the trivial partition: with one batch with all intents
        let Some(plan) = DecisionSpace::new(&self.queue).iter().next() else {
            return Ok(None);
        };
        // shortcut: funds only the first batch, fund each batch once plans have more than one.
        let batch = plan.batches.first().expect("a plan has a batch");

        // In id order, so the same queue always yields the same actions.
        let intents: BTreeMap<Q::Id, &Intent> = self.queue.iter().collect();
        let payments: Vec<TxOut> = batch
            .intents
            .iter()
            .filter_map(|id| intents.get(id))
            .map(|intent| intent.payment_output())
            .collect();

        let selection = selector
            .select(
                FundingRequest {
                    available: &self.utxos,
                    outputs: &payments,
                    feerate,
                },
                rng,
            )
            .map_err(PlanError::Selection)?;
        let change = change_strategy
            .change(&selection, feerate)
            .map_err(PlanError::Change)?;
        let change = change_strategy.assign(&change).map_err(PlanError::Assign)?;

        let tree = DecisionTree::Leaf(Leaf {
            inputs: selection.selected_inputs.into_iter().collect(),
            payments,
            change,
        });

        // TODO: this returns the first leaf, which is not necessarily the best plan.
        // Future work should iterate over leaves, simulate the wallet diff, and score the results.
        Ok(tree
            .iter_leaves()
            .next()
            .map(|leaf| leaf.to_sequence_of_actions(rng)))
    }
}

/// The actions of the best plan, `None` if there is nothing to do.
pub(crate) type PlanResult<S, C, A> = Result<Option<UnSequencedActions>, PlanError<S, C, A>>;

/// Why a plan could not be built.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PlanError<S, C, A> {
    /// The coins could not fund the payments.
    Selection(S),
    /// The surplus could not be split into change.
    Change(C),
    /// The change could not be given scriptpubkeys.
    Assign(A),
}

#[cfg(test)]
mod tests {
    use crate::action::Action;
    use crate::change::single_output::SingleOutputChangeSelector;
    use crate::change::tests::DummySPKAssigner;
    use crate::intent::{FixedPaymentInstructions, Outcome, PayoffCurve};
    use crate::queue::InMemoryQueue;
    use crate::selection::random::{RandomSelectionError, RandomSelector};

    use super::*;
    use bitcoin::{ScriptBuf, Sequence, Txid, hashes::Hash};
    use rand::{SeedableRng, rngs::StdRng};
    use std::convert::Infallible;
    use std::time::Instant;

    fn utxo(vout: u32, sats: u64) -> Utxo {
        Utxo::new(
            OutPoint::new(Txid::all_zeros(), vout),
            Amount::from_sat(sats),
            Weight::ZERO,
        )
    }

    fn intent(start: Instant, sats: u64) -> Intent {
        Intent::new(
            Outcome::OutputCreation(FixedPaymentInstructions {
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                amount: Amount::from_sat(sats),
            }),
            PayoffCurve::new(start, []),
        )
    }

    #[test]
    fn a_snapshot_keeps_the_coins_and_intents_it_was_given() {
        let start = Instant::now();

        let state = WalletStateSnapshot::new(
            [utxo(0, 10_000), utxo(1, 20_000)],
            InMemoryQueue::new([intent(start, 1_000)]),
        );

        assert_eq!(state.utxos, [utxo(0, 10_000), utxo(1, 20_000)]);
        assert_eq!(state.queue, InMemoryQueue::new([intent(start, 1_000)]));
    }

    type Plan = PlanResult<RandomSelectionError, Infallible, Infallible>;

    fn best_plan(utxos: impl IntoIterator<Item = Utxo>, intents: &[u64]) -> Plan {
        let start = Instant::now();
        let queue = InMemoryQueue::new(intents.iter().map(|&sats| intent(start, sats)));

        WalletStateSnapshot::new(utxos, queue).best_plan(
            FeeRate::from_sat_per_vb(1).unwrap(),
            &RandomSelector,
            &mut StdRng::seed_from_u64(0),
            &ChangeStrategy::new(SingleOutputChangeSelector, DummySPKAssigner),
        )
    }

    fn op_true_output(sats: u64) -> TxOut {
        TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
        }
    }

    #[test]
    fn an_empty_queue_has_no_plan() {
        assert_eq!(best_plan([utxo(0, 10_000)], &[]), Ok(None));
    }

    #[test]
    fn the_best_plan_spends_the_coin_pays_every_intent_and_returns_change() {
        // At 1 sat/vB the coin's input costs 40 sats, each payment 10 sats, the transaction
        // overhead 11 sats and the change output 10 sats: 10,000 - 40 - 2,000 - 20 - 11 - 10.
        // The two payments are identical, and both are registered.
        assert_eq!(
            best_plan([utxo(0, 10_000)], &[1_000, 1_000]),
            Ok(Some(UnSequencedActions(vec![
                Action::RegisterInput(utxo(0, 10_000).to_txin()),
                Action::RegisterOutput(op_true_output(1_000)),
                Action::RegisterOutput(op_true_output(1_000)),
                Action::RegisterOutput(op_true_output(7_919)),
            ])))
        );
    }

    #[test]
    fn a_plan_the_coins_cannot_fund_is_an_error() {
        assert_eq!(
            best_plan([utxo(0, 1_000)], &[1_000]),
            Err(PlanError::Selection(
                RandomSelectionError::NoCombinationCovers
            ))
        );
    }

    #[test]
    fn a_coin_becomes_an_unsigned_input_spending_its_outpoint() {
        assert_eq!(
            utxo(3, 10_000).to_txin(),
            TxIn {
                previous_output: OutPoint::new(Txid::all_zeros(), 3),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::MAX,
                witness: bitcoin::Witness::new(),
            }
        );
    }
}

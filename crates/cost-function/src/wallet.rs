//! What the wallet owns, and what it has been asked to do.

use bitcoin::{Amount, FeeRate, OutPoint, TxOut, Weight};
use rand::RngCore;
use std::{
    collections::{BTreeMap, HashSet},
    num::NonZeroUsize,
};

use crate::action::Action;
use crate::candidate::Candidate;
use crate::change::single_output::SingleOutputChangeSelector;
use crate::change::{ChangeCosts, ChangeSelector, ChangeStrategy};
use crate::decision_space::DecisionSpace;
use crate::decision_tree::TreeNode;
use crate::intent::Intent;
use crate::queue::Queue;
pub use crate::selection::random::RandomSelectionError as FundingError;
use crate::selection::random::RandomSelector;
use crate::selection::{CoinSelector, FundingRequest, cover_count_growth, shared_overhead};

/// A coin the wallet can spend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Utxo {
    pub(crate) outpoint: OutPoint,
    pub(crate) value: Amount,
    pub(crate) satisfaction_weight: Weight,
    pub(crate) liability: Amount,
}

impl Utxo {
    pub fn outpoint(&self) -> OutPoint {
        self.outpoint
    }
    pub fn value(&self) -> Amount {
        self.value
    }
    pub fn liability(&self) -> Amount {
        self.liability
    }

    /// `satisfaction_weight` is the weight of the scriptSig and witness that spend this
    /// coin, excluding the outpoint and nsequence.
    pub fn new(outpoint: OutPoint, value: Amount, satisfaction_weight: Weight) -> Self {
        Utxo {
            outpoint,
            value,
            satisfaction_weight,
            liability: Amount::ZERO,
        }
    }

    /// Attach a replacement fee liability that spending this coin must cover.
    pub fn with_liability(mut self, liability: Amount) -> Self {
        self.liability = liability;
        self
    }
}

/// The coins a wallet holds, as it reports them.
pub trait UtxoSnapshot {
    /// Every coin, in no particular order.
    fn iter(&self) -> impl Iterator<Item = Utxo>;
}

/// What the wallet owns and what it has been asked to do, at one moment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalletStateSnapshot<Q, U> {
    utxo_snapshot: U,
    queue: Q,
}

impl<Q: Queue, U: UtxoSnapshot> WalletStateSnapshot<Q, U> {
    /// The coins the wallet holds, together with the queue of intents it has yet to satisfy.
    pub fn new(utxo_snapshot: U, queue: Q) -> Self {
        WalletStateSnapshot {
            utxo_snapshot,
            queue,
        }
    }

    /// Fund the queue once using random coin selection and one change output.
    /// This is the single-outcome entry point, not a search for an optimal plan.
    pub fn funded_candidate<C: ChangeCosts, R: RngCore>(
        &self,
        feerate: FeeRate,
        rng: &mut R,
        costs: &C,
    ) -> Result<Option<Candidate<Q::Id>>, FundingError> {
        self.funded_candidate_for_session(feerate, NonZeroUsize::MIN, rng, costs)
    }

    /// Fund this participant's intents and share of transaction overhead in a
    /// `participants`-party session.
    pub fn funded_candidate_for_session<C: ChangeCosts, R: RngCore>(
        &self,
        feerate: FeeRate,
        participants: NonZeroUsize,
        rng: &mut R,
        costs: &C,
    ) -> Result<Option<Candidate<Q::Id>>, FundingError> {
        // The selector and change strategy share one feerate, so change outputs that grow the
        // output count are covered too.
        let feerate = cover_count_growth(feerate).ok_or(FundingError::Overflow)?;
        match self.fund_with(
            feerate,
            shared_overhead(participants),
            &RandomSelector,
            rng,
            &ChangeStrategy::new(SingleOutputChangeSelector, costs),
        ) {
            Ok(candidate) => Ok(candidate),
            Err(PlanError::Selection(error)) => Err(error),
            Err(PlanError::Change(never)) => match never {},
        }
    }

    fn fund_with<S, C, A, R>(
        &self,
        feerate: FeeRate,
        overhead: Weight,
        selector: &S,
        rng: &mut R,
        change_strategy: &ChangeStrategy<C, A>,
    ) -> CandidateResult<Q::Id, S::Error, C::Error>
    where
        S: CoinSelector,
        C: ChangeSelector,
        A: ChangeCosts,
        R: RngCore,
    {
        // MVP: the decision space yields only the trivial partition: with one batch with all intents
        let Some(plan_intent) = DecisionSpace::new(&self.queue).iter().next() else {
            return Ok(None);
        };
        // shortcut: funds only the first batch, fund each batch once plans have more than one.
        let batch = plan_intent.batches.first().expect("a plan has a batch");

        // In id order, so the same queue always yields the same actions.
        let intents: BTreeMap<Q::Id, &Intent> = self.queue.iter().collect();
        let payments: Vec<TxOut> = batch
            .intents
            .iter()
            // Panics on an id missing from this second queue read, since a plan omitting a
            // payment would still look valid.
            .map(|id| intents[id].payment_output())
            .collect();

        // Deduplicate the utxos before funding the request
        let mut available_utxos = self.utxo_snapshot.iter().collect::<Vec<_>>();
        let mut seen = HashSet::new();
        available_utxos.retain(|utxo| seen.insert(utxo.outpoint));
        let selection = selector
            .select(
                FundingRequest {
                    available: &available_utxos,
                    outputs: &payments,
                    feerate,
                    overhead,
                },
                rng,
            )
            .map_err(PlanError::Selection)?;
        let change = change_strategy
            .change(&selection, feerate)
            .map_err(PlanError::Change)?;

        // TODO: this takes the leaf that spends every selected coin, which is not necessarily
        // the best plan. Future work should walk the tree, simulate the wallet diff, and score
        // the leaves.
        let mut actions = TreeNode::new(&selection.selected_inputs)
            .most_inclusive()
            .to_sequence_of_actions();
        actions
            .0
            .extend(payments.into_iter().map(Action::RegisterOutput));

        actions
            .0
            .extend(change.into_iter().map(Action::RegisterChangeOutput));

        let payments = batch
            .intents
            .iter()
            .map(|id| (*id, intents[id].payment_output()))
            .collect();
        let payoffs = batch
            .intents
            .iter()
            .map(|id| intents[id].payoff_curve.clone())
            .collect();
        Ok(Some(Candidate {
            actions,
            payments,
            payoffs,
        }))
    }
}

type CandidateResult<I, S, C> = Result<Option<Candidate<I>>, PlanError<S, C>>;

/// Why a plan could not be built.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PlanError<S, C> {
    /// The coins could not fund the payments.
    Selection(S),
    /// The surplus could not be split into change.
    Change(C),
}

#[cfg(test)]
mod tests {
    use crate::action::{Action, UnSequencedActions};
    use crate::change::OutputWithoutScriptpubkey;
    use crate::change::tests::DummyChangeCosts;
    use crate::intent::{FixedPaymentInstructions, Outcome, PayoffCurve};
    use crate::queue::InMemoryQueue;

    use super::*;
    use bitcoin::{ScriptBuf, Txid, hashes::Hash};
    use rand::{SeedableRng, rngs::StdRng};

    fn utxo(vout: u32, sats: u64) -> Utxo {
        Utxo::new(
            OutPoint::new(Txid::all_zeros(), vout),
            Amount::from_sat(sats),
            Weight::ZERO,
        )
    }

    fn intent(sats: u64) -> Intent {
        Intent::new(
            Outcome::OutputCreation(FixedPaymentInstructions {
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                amount: Amount::from_sat(sats),
            }),
            PayoffCurve::new(0, []).unwrap(),
        )
    }

    struct DummyUtxoSnapshot(Vec<Utxo>);
    impl UtxoSnapshot for DummyUtxoSnapshot {
        fn iter(&self) -> impl Iterator<Item = Utxo> {
            self.0.iter().cloned()
        }
    }

    fn best_plan(
        utxos: impl IntoIterator<Item = Utxo>,
        intents: &[u64],
    ) -> Result<Option<UnSequencedActions>, FundingError> {
        let queue = InMemoryQueue::new(intents.iter().map(|&sats| intent(sats)));

        WalletStateSnapshot::new(DummyUtxoSnapshot(utxos.into_iter().collect()), queue)
            .funded_candidate(
                FeeRate::from_sat_per_vb(1).unwrap(),
                &mut StdRng::seed_from_u64(0),
                &DummyChangeCosts,
            )
            .map(|candidate| candidate.map(|candidate| candidate.actions))
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
        // At 1 sat/vB, raised to 251 sat/kwu by `cover_count_growth`, the coin's input costs
        // 41 sats, each payment 11 sats, the transaction overhead 11 sats and the change output
        // 11 sats: 10,000 - 41 - 2,000 - 22 - 11 - 11.
        // The two payments are identical, and both are registered.
        assert_eq!(
            best_plan([utxo(0, 10_000)], &[1_000, 1_000]),
            Ok(Some(UnSequencedActions(vec![
                Action::RegisterInput(utxo(0, 10_000)),
                Action::RegisterOutput(op_true_output(1_000)),
                Action::RegisterOutput(op_true_output(1_000)),
                Action::RegisterChangeOutput(OutputWithoutScriptpubkey {
                    value: Amount::from_sat(7_915),
                    weight: Weight::from_wu(40),
                    satisfaction_weight: Weight::ZERO,
                    dust: Amount::from_sat(474),
                }),
            ])))
        );
    }

    #[test]
    fn a_two_party_session_charges_half_the_transaction_overhead() {
        let queue = InMemoryQueue::new([intent(1_000)]);
        let snapshot = WalletStateSnapshot::new(DummyUtxoSnapshot(vec![utxo(0, 10_000)]), queue);
        let candidate = snapshot
            .funded_candidate_for_session(
                FeeRate::from_sat_per_vb(1).unwrap(),
                NonZeroUsize::new(2).unwrap(),
                &mut StdRng::seed_from_u64(0),
                &DummyChangeCosts,
            )
            .unwrap()
            .unwrap();
        // 21 WU at 251 sat/kwu rounds up to 6 sats instead of 11 sats, leaving 5 sats more change.
        assert_eq!(
            candidate.change().next().unwrap().value,
            Amount::from_sat(10_000 - 41 - 1_000 - 11 - 6 - 11)
        );
    }

    #[test]
    fn an_overflowing_feerate_returns_an_error() {
        let queue = InMemoryQueue::new([intent(1_000)]);
        let snapshot = WalletStateSnapshot::new(DummyUtxoSnapshot(vec![utxo(0, 10_000)]), queue);

        assert_eq!(
            snapshot.funded_candidate(
                FeeRate::MAX,
                &mut StdRng::seed_from_u64(0),
                &DummyChangeCosts
            ),
            Err(FundingError::Overflow)
        );
    }

    #[test]
    fn a_plan_the_coins_cannot_fund_is_an_error() {
        assert_eq!(
            best_plan([utxo(0, 1_000)], &[1_000]),
            Err(FundingError::NoCombinationCovers)
        );
    }

    #[test]
    fn a_coin_reported_twice_funds_the_plan_once() {
        // Counted twice, coin 0 would cover the payment; counted once, it cannot.
        assert_eq!(
            best_plan([utxo(0, 1_000), utxo(1, 10), utxo(0, 1_000)], &[1_500]),
            Err(FundingError::NoCombinationCovers)
        );
    }
}

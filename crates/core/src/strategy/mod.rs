//! Wallet strategy and execution state.
//!
//! The current tactic funds the intents outside started outcomes as one outcome of
//! disjoint transaction batches, and funds them again on new information. An outcome
//! has started once any of its transactions has started.

use crate::{
    driver::{Decision, Machine},
    wallet::Snapshot,
};
use bitcoin::{
    Amount, FeeRate, Txid,
    hashes::{Hash, sha256},
};
use cost_function::{
    candidate::Candidate,
    change::ChangeCosts,
    intent::{Intent, ScheduleGrid},
    queue::{InMemoryQueue, Queue},
    wallet::WalletStateSnapshot,
};
use rand::RngCore;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Instant,
};

mod construction;
mod execute;
pub use execute::{Broadcaster, Executor, Signers};

/// Local execution identity, independent of the shared construction-session UUID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExecutionId {
    pub wallet: [u8; 16],
    pub sequence: u64,
}

struct Plan {
    execution_id: ExecutionId,
    candidate: Candidate<u64>,
    snapshot: Snapshot,
    feerate: FeeRate,
    /// Shared construction-session UUID. The local tactic derives it from the execution identity.
    session_id: [u8; 16],
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Action {
    Start,
    Input(usize),
    CloseInputs,
    AllocateChange(usize),
    Output(usize),
    CloseOutputs,
    Sign(usize),
    Finalize,
    Record,
    Publish,
}

/// An action bound to the candidate evaluated by the decision process.
#[derive(Clone)]
pub struct Request {
    plan: Arc<Plan>,
    pub action: Action,
}

pub type OperationId = (ExecutionId, Action);

impl Request {
    pub fn id(&self) -> OperationId {
        (self.plan.execution_id, self.action.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Error(pub String);

fn error(value: impl std::fmt::Display) -> Error {
    Error(value.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Payment {
    pub transaction: bitcoin::Transaction,
    pub outputs: BTreeMap<u64, usize>,
    pub fee: Amount,
}

#[derive(Clone, Debug)]
pub enum Update {
    Applied,
    Finalized(Payment),
}

#[derive(Clone)]
pub enum Observation {
    Confirmed(Txid),
    /// Stop this candidate when new information invalidates its assumptions.
    Invalidated(String),
    TransactionInvalidated(ExecutionId, String),
    /// The user adds an intent. It joins every intent outside started outcomes.
    Intent(Intent),
    /// The wallet reports its coins. Recorded transactions are kept.
    Snapshot(Snapshot),
    FeeRate(FeeRate),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Running,
    AwaitingConfirmation,
    Confirmed,
    Failed(Error),
}

mod path;
mod signing;
mod transaction;
mod validate;
pub use transaction::Transaction;

/// State holds wallet observations, the outcome for unstarted intents and transaction executions.
pub struct State<C, R> {
    pending: InMemoryQueue,
    snapshot: Snapshot,
    feerate: FeeRate,
    wallet_id: [u8; 16],
    change: C,
    rng: R,
    grid: ScheduleGrid,
    /// Each batch holds intents outside started outcomes. Batches are disjoint.
    batches: Vec<BTreeSet<u64>>,
    /// Each outcome holds the transactions funded by one evaluation.
    outcomes: Vec<BTreeSet<ExecutionId>>,
    /// This is set when information relevant to funding the unstarted batches arrives.
    stale: bool,
    evaluated: bool,
    next_sequence: u64,
    transactions: BTreeMap<ExecutionId, Transaction>,
    planning_errors: BTreeMap<ExecutionId, Error>,
}

impl<C: ChangeCosts, R: RngCore> State<C, R> {
    /// The schedule grid places all authored payoff slots for this wallet.
    pub fn new(
        pending: InMemoryQueue,
        snapshot: Snapshot,
        feerate: FeeRate,
        wallet_id: [u8; 16],
        change_costs: C,
        rng: R,
        grid: ScheduleGrid,
    ) -> Self {
        let batches = vec![pending.iter().map(|(id, _)| id).collect()];
        Self {
            pending,
            snapshot,
            feerate,
            wallet_id,
            change: change_costs,
            rng,
            grid,
            batches,
            outcomes: Vec::new(),
            stale: true,
            evaluated: false,
            next_sequence: 0,
            transactions: BTreeMap::new(),
            planning_errors: BTreeMap::new(),
        }
    }

    /// Choose disjoint initial batches. The default is one batch.
    /// This configures the PoC tactic; it neither reserves coins nor removes intents.
    /// An intent left out of every batch is not funded by the first evaluation.
    /// An added intent replaces the batches with one batch of every intent outside
    /// started outcomes. A new snapshot or feerate keeps the batches.
    pub fn with_batches(mut self, batches: Vec<Vec<u64>>) -> Result<Self, Error> {
        if self.evaluated {
            return Err(error("initial allocation has already been evaluated"));
        }
        let known: BTreeSet<_> = self.pending.iter().map(|(id, _)| id).collect();
        let mut allocation = Vec::new();
        let mut assigned = BTreeSet::new();
        for batch in batches {
            let ids: BTreeSet<_> = batch.iter().copied().collect();
            if ids.is_empty()
                || ids.len() != batch.len()
                || !ids.is_subset(&known)
                || !ids.is_disjoint(&assigned)
            {
                return Err(error(
                    "batches must contain distinct, known intent identities",
                ));
            }
            assigned.extend(&ids);
            allocation.push(ids);
        }
        self.batches = allocation;
        Ok(self)
    }

    pub fn pending(&self) -> &InMemoryQueue {
        &self.pending
    }
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
    pub fn transactions(&self) -> &BTreeMap<ExecutionId, Transaction> {
        &self.transactions
    }
    pub fn planning_errors(&self) -> &BTreeMap<ExecutionId, Error> {
        &self.planning_errors
    }

    /// Fund each batch again, replacing the outcomes that have not started.
    fn evaluate(&mut self) {
        // Clock ticks and unrelated completions must not redraw coins or identities.
        self.stale = false;
        self.evaluated = true;
        let transactions = &self.transactions;
        self.outcomes
            .retain(|outcome| outcome.iter().any(|id| transactions[id].started()));
        let started: BTreeSet<_> = self.outcomes.iter().flatten().copied().collect();
        self.transactions.retain(|id, _| started.contains(id));
        self.planning_errors.clear();
        // Started outcomes keep their intents. Only those without a failure keep
        // their coins, so a failed attempt reserves no input.
        let mut available = self.snapshot.clone();
        let mut bound = BTreeSet::new();
        for outcome in &self.outcomes {
            let outcome: Vec<_> = outcome.iter().map(|id| &self.transactions[id]).collect();
            let failed = outcome
                .iter()
                .any(|tx| matches!(tx.status(), Status::Failed(_)));
            for tx in outcome {
                bound.extend(tx.candidate().payments().map(|(&id, _)| id));
                if !failed {
                    for coin in tx.candidate().inputs() {
                        available.coins.remove(&coin.outpoint());
                    }
                }
            }
        }
        let known: BTreeSet<_> = self.pending.iter().map(|(id, _)| id).collect();
        for batch in &mut self.batches {
            batch.retain(|id| known.contains(id) && !bound.contains(id));
        }
        self.batches.retain(|batch| !batch.is_empty());
        // Allocation within this tactic is disjoint; wallet observations remain intact.
        let mut transactions = BTreeMap::new();
        for ids in &self.batches {
            let execution_id = ExecutionId {
                wallet: self.wallet_id,
                sequence: self.next_sequence,
            };
            // Identities are not reused, even for a candidate that never started.
            self.next_sequence += 1;
            let queue = Batch {
                queue: &self.pending,
                ids,
            };
            let observed = WalletStateSnapshot::new(available.clone(), queue);
            match observed.funded_candidate(self.feerate, &mut self.rng, &self.change) {
                Ok(candidate) => {
                    let candidate = candidate.expect("a batch is not empty");
                    for coin in candidate.inputs() {
                        available.coins.remove(&coin.outpoint());
                    }
                    let seed = [
                        self.wallet_id.as_slice(),
                        &execution_id.sequence.to_be_bytes(),
                    ]
                    .concat();
                    let mut session_id = [0; 16];
                    session_id.copy_from_slice(&sha256::Hash::hash(&seed).to_byte_array()[..16]);
                    transactions.insert(
                        execution_id,
                        Transaction::new(Arc::new(Plan {
                            execution_id,
                            candidate,
                            snapshot: self.snapshot.clone(),
                            feerate: self.feerate,
                            session_id,
                        })),
                    );
                }
                Err(err) => {
                    self.planning_errors
                        .insert(execution_id, error(format!("candidate funding: {err:?}")));
                }
            }
        }
        // A partial allocation is not the complete outcome whose value was requested.
        if self.planning_errors.is_empty() && !transactions.is_empty() {
            self.outcomes.push(transactions.keys().copied().collect());
            self.transactions.extend(transactions);
        }
    }
}

struct Batch<'a> {
    queue: &'a InMemoryQueue,
    ids: &'a BTreeSet<u64>,
}
impl Queue for Batch<'_> {
    type Id = u64;
    fn iter(&self) -> impl Iterator<Item = (u64, &Intent)> {
        self.queue.iter().filter(|(id, _)| self.ids.contains(id))
    }
}

impl<C: ChangeCosts, R: RngCore> Machine for State<C, R> {
    type Action = Request;
    type Operation = Request;
    type OperationId = OperationId;
    type Input = Observation;
    type Result = Result<Update, Error>;

    fn decide(&mut self, now: Instant) -> Decision<Request> {
        if self.stale {
            self.evaluate();
        }
        // Zero estimated durations and additional action costs: all eligible
        // actions inherit their outcome's curve. A failure stops its own outcome.
        // Dispatch is a separate policy; this PoC uses the earliest crossing with
        // stable ties.
        let grid = self.grid;
        self.outcomes
            .iter()
            .map(|outcome| {
                outcome
                    .iter()
                    .map(|id| &self.transactions[id])
                    .collect::<Vec<_>>()
            })
            .filter(|outcome| {
                !outcome
                    .iter()
                    .any(|tx| matches!(tx.status(), Status::Failed(_)))
            })
            .filter_map(|outcome| {
                let at =
                    Candidate::joint_ready_at(outcome.iter().map(|tx| tx.candidate()), now, grid)?;
                Some(outcome.into_iter().map(move |tx| (at, tx)))
            })
            .flatten()
            .flat_map(|(at, tx)| {
                tx.available()
                    .map(move |action| crate::driver::Scheduled { at, action })
            })
            .min_by_key(|scheduled| (scheduled.at, scheduled.action.id()))
            .map_or(Decision::Wait, Decision::Schedule)
    }

    fn apply(&mut self, request: Request) {
        if let Some(tx) = self.transactions.get_mut(&request.plan.execution_id) {
            tx.apply(request);
        }
    }

    fn operations(&self) -> impl Iterator<Item = (OperationId, Request)> {
        self.transactions
            .values()
            .flat_map(|tx| tx.pending())
            .map(|op| (op.id(), op.clone()))
    }

    fn completed(&mut self, id: OperationId, result: Self::Result) {
        if let Some(tx) = self.transactions.get_mut(&id.0) {
            tx.completed_operation(id, result);
            if tx.completed().contains(&Action::Record) {
                let payment = tx.payment().expect("recorded payment is finalized");
                self.snapshot.recorded.insert(
                    payment.transaction.compute_txid(),
                    payment.transaction.clone(),
                );
            }
        }
    }

    fn observe(&mut self, input: Observation) {
        match input {
            Observation::Intent(intent) => {
                self.pending.push(intent);
                // Evaluation removes the intents of started outcomes.
                self.batches = vec![self.pending.iter().map(|(id, _)| id).collect()];
                self.stale = true;
                return;
            }
            Observation::Snapshot(mut snapshot) => {
                // Recorded transactions are kept. A repeated snapshot changes nothing.
                snapshot.recorded.extend(self.snapshot.recorded.clone());
                if snapshot != self.snapshot {
                    self.snapshot = snapshot;
                    self.stale = true;
                }
                return;
            }
            Observation::FeeRate(feerate) => {
                if feerate != self.feerate {
                    self.feerate = feerate;
                    self.stale = true;
                }
                return;
            }
            Observation::Invalidated(ref reason) if self.stale && !self.evaluated => {
                self.stale = false;
                self.evaluated = true;
                self.planning_errors.insert(
                    ExecutionId {
                        wallet: self.wallet_id,
                        sequence: self.next_sequence,
                    },
                    error(reason),
                );
            }
            _ => {}
        }
        for (id, tx) in &mut self.transactions {
            if let Observation::TransactionInvalidated(target, _) = &input
                && target != id
            {
                continue;
            }
            let was_confirmed = *tx.status() == Status::Confirmed;
            tx.observe(input.clone());
            if !was_confirmed && *tx.status() == Status::Confirmed {
                for id in tx
                    .payment()
                    .expect("confirmed transaction has a payment")
                    .outputs
                    .keys()
                {
                    self.pending.remove(*id);
                }
            }
        }
    }
}

mod runtime;
pub use runtime::{Events, Runtime};

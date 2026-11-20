//! Wallet strategy and execution state.
//!
//! The tactic chooses a complete outcome. A `Plan` binds one candidate, the
//! snapshot used to price it and its execution identity. The executor realizes
//! that candidate; it does not make coin-selection or output-policy decisions.
//!
//! # Construction state machine
//!
//! Unilateral construction is the semi-honest machine with one participant and
//! no transport. The agreed contribution and witness sets are therefore the
//! local sets, and readiness barriers complete without peer messages.
//!
//! ```text
//! CloseInputs                       materialize and freeze the planned inputs
//!   ├─ AllocateChange(change_id)    derive each change script
//!   └─ CloseOutputs                 add payments and change, order and freeze
//!        ├─ Sign(outpoint)          collect each local witness
//!        └─ Finalize                join witnesses to the transaction
//!             └─ Record
//!                  └─ Publish
//! ```
//!
//! `AllocateChange`, signing, recording and publication have external effects.
//! The close and finalization actions are barriers. Pure restatements of the
//! candidate, such as registering a planned input or payment output, are not
//! actions.
//!
//! Inputs, signatures and witnesses are identified by outpoint. Outputs and
//! change allocations use stable contribution identities. Numeric positions
//! are derived only for sighash calculation and final serialization.
//!
//! `CloseOutputs` freezes an unsigned transaction and its prevouts. Witnesses
//! accumulate separately by outpoint and join the transaction only at
//! finalization. Signing never mutates the frozen template.
//!
//! The current executor interprets one fixed candidate. The planner does not
//! yet retain a committed action sequence or revise a plan from peer input.
//! Those are planner/executor interface work, not different construction
//! semantics: the tactics model permits re-evaluation before phase closure.

use crate::{
    driver::{Decision, Machine},
    wallet::Snapshot,
};
use bitcoin::{
    Amount, FeeRate, OutPoint, Txid,
    hashes::{Hash, sha256},
};
use concurrent_psbt::output::UniqueId;
use cost_function::{
    candidate::Candidate,
    change::ChangeCosts,
    intent::ScheduleGrid,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    CloseInputs,
    AllocateChange(UniqueId),
    CloseOutputs,
    Sign(OutPoint),
    Finalize,
    Record,
    Publish,
}

impl PartialOrd for Action {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Action {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        fn rank(action: &Action) -> u8 {
            match action {
                Action::CloseInputs => 0,
                Action::AllocateChange(_) => 1,
                Action::CloseOutputs => 2,
                Action::Sign(_) => 3,
                Action::Finalize => 4,
                Action::Record => 5,
                Action::Publish => 6,
            }
        }

        rank(self)
            .cmp(&rank(other))
            .then_with(|| match (self, other) {
                (Action::AllocateChange(left), Action::AllocateChange(right)) => {
                    left.as_bytes().cmp(right.as_bytes())
                }
                (Action::Sign(left), Action::Sign(right)) => left.cmp(right),
                _ => std::cmp::Ordering::Equal,
            })
    }
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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Running,
    AwaitingConfirmation,
    Confirmed,
    Failed(Error),
}

mod identity;
mod path;
mod signing;
mod transaction;
mod validate;
pub use transaction::Transaction;

/// Wallet observations, one initial outcome, and its transaction executions.
pub struct State<C, R> {
    pending: InMemoryQueue,
    snapshot: Snapshot,
    feerate: FeeRate,
    wallet_id: [u8; 16],
    change: C,
    rng: R,
    grid: ScheduleGrid,
    batches: Vec<BTreeSet<u64>>,
    planned: bool,
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
            planned: false,
            transactions: BTreeMap::new(),
            planning_errors: BTreeMap::new(),
        }
    }

    /// Choose disjoint initial batches. The default is one batch.
    /// This configures the PoC tactic; it neither reserves coins nor removes intents.
    pub fn with_batches(mut self, batches: Vec<Vec<u64>>) -> Result<Self, Error> {
        if self.planned {
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

    fn generate_outcome(&mut self) {
        // Clock ticks and unrelated completions must not redraw coins or identities.
        self.planned = true;
        // Allocation within this tactic is disjoint; wallet observations remain intact.
        let mut available = self.snapshot.clone();
        let mut transactions = BTreeMap::new();
        for (sequence, ids) in self.batches.iter().enumerate() {
            let execution_id = ExecutionId {
                wallet: self.wallet_id,
                sequence: sequence as u64,
            };
            let queue = Batch {
                queue: &self.pending,
                ids,
            };
            let observed = WalletStateSnapshot::new(available.clone(), queue);
            match observed.funded_candidate(self.feerate, &mut self.rng, &self.change) {
                Ok(Some(candidate)) => {
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
                Ok(None) => {}
                Err(err) => {
                    self.planning_errors
                        .insert(execution_id, error(format!("candidate funding: {err:?}")));
                }
            }
        }
        // A partial allocation is not the complete outcome whose value was requested.
        if self.planning_errors.is_empty() {
            self.transactions = transactions;
        }
    }
}

struct Batch<'a> {
    queue: &'a InMemoryQueue,
    ids: &'a BTreeSet<u64>,
}
impl Queue for Batch<'_> {
    type Id = u64;
    fn iter(&self) -> impl Iterator<Item = (u64, &cost_function::intent::Intent)> {
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
        if !self.planned {
            self.generate_outcome();
        }
        if self
            .transactions
            .values()
            .any(|tx| matches!(tx.status(), Status::Failed(_)))
        {
            return Decision::Wait;
        }
        // Zero estimated durations and additional action costs: all eligible
        // actions inherit the selected complete outcome's curve. Dispatch is a
        // separate policy; this PoC uses the earliest crossing with stable ties.
        let Some(at) = Candidate::joint_ready_at(
            self.transactions.values().map(Transaction::candidate),
            now,
            self.grid,
        ) else {
            return Decision::Wait;
        };
        self.transactions
            .values()
            .flat_map(Transaction::available)
            .map(|action| crate::driver::Scheduled { at, action })
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
        if !self.planned
            && let Observation::Invalidated(reason) = &input
        {
            self.planned = true;
            self.planning_errors.insert(
                ExecutionId {
                    wallet: self.wallet_id,
                    sequence: 0,
                },
                error(reason),
            );
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

//! Wallet strategy and execution state.
//!
//! The current tactic funds one initial transaction from the pending intents.

use crate::{
    driver::{Decision, Machine},
    wallet::Snapshot,
};
use bitcoin::{
    FeeRate,
    hashes::{Hash, sha256},
};
use cost_function::{
    candidate::Candidate, change::ChangeCosts, intent::ScheduleGrid, queue::InMemoryQueue,
    wallet::WalletStateSnapshot,
};
use rand::RngCore;
use std::{collections::BTreeMap, sync::Arc, time::Instant};

mod construction;
mod execute;
pub use execute::Executor;

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
    /// Shared construction-session UUID. The local tactic derives it from the execution identity.
    session_id: [u8; 16],
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Action {
    Start,
    Input(usize),
    CloseInputs,
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

#[derive(Clone, Debug)]
pub enum Update {
    Applied,
}

#[derive(Clone)]
pub enum Observation {
    /// Stop this candidate when new information invalidates its assumptions.
    Invalidated(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Running,
    Failed(Error),
}

mod path;
mod transaction;
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
        Self {
            pending,
            snapshot,
            feerate,
            wallet_id,
            change: change_costs,
            rng,
            grid,
            planned: false,
            transactions: BTreeMap::new(),
            planning_errors: BTreeMap::new(),
        }
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
        let execution_id = ExecutionId {
            wallet: self.wallet_id,
            sequence: 0,
        };
        let observed = WalletStateSnapshot::new(self.snapshot.clone(), self.pending.clone());
        match observed.funded_candidate(self.feerate, &mut self.rng, &self.change) {
            Ok(Some(candidate)) => {
                let seed = [
                    self.wallet_id.as_slice(),
                    &execution_id.sequence.to_be_bytes(),
                ]
                .concat();
                let mut session_id = [0; 16];
                session_id.copy_from_slice(&sha256::Hash::hash(&seed).to_byte_array()[..16]);
                self.transactions.insert(
                    execution_id,
                    Transaction::new(Arc::new(Plan {
                        execution_id,
                        candidate,
                        snapshot: self.snapshot.clone(),
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
        // All eligible actions inherit the selected outcome's readiness.
        self.transactions
            .values()
            .filter_map(|tx| {
                let at = tx.candidate().ready_at(now, self.grid)?;
                Some((at, tx.available()))
            })
            .flat_map(|(at, actions)| {
                actions.map(move |action| crate::driver::Scheduled { at, action })
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
        }
    }

    fn observe(&mut self, input: Observation) {
        if !self.planned {
            let Observation::Invalidated(reason) = &input;
            self.planned = true;
            self.planning_errors.insert(
                ExecutionId {
                    wallet: self.wallet_id,
                    sequence: 0,
                },
                error(reason),
            );
        }
        for tx in self.transactions.values_mut() {
            tx.observe(input.clone());
        }
    }
}

mod runtime;
pub use runtime::{Events, Runtime};

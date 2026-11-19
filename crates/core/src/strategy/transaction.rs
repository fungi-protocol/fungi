//! The current execution path and results for one transaction.

use super::{
    Action, Error, Observation, Operation, Payment, Plan, Request, Status, Update,
    construction::Execution, error, path::Path,
};
use bitcoin::Amount;
use cost_function::candidate::Candidate;
use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    sync::Arc,
};

pub struct Transaction {
    plan: Arc<Plan>,
    path: Path,
    status: Status,
    execution: Execution,
    completed: BTreeSet<Action>,
    operation_errors: BTreeMap<Action, Error>,
    pending: BTreeMap<Action, Operation>,
}

impl Transaction {
    pub(super) fn new(plan: Arc<Plan>) -> Self {
        Self {
            path: Path::new(&plan.candidate),
            execution: Execution::new(plan.clone()),
            plan,
            status: Status::Running,
            completed: BTreeSet::new(),
            operation_errors: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }
    pub fn candidate(&self) -> &Candidate<u64> {
        &self.plan.candidate
    }
    pub fn status(&self) -> &Status {
        &self.status
    }
    pub fn payment(&self) -> Option<&Payment> {
        self.execution.payment()
    }
    pub fn completed(&self) -> &BTreeSet<Action> {
        &self.completed
    }

    /// Start has been dispatched, so the execution is bound to this plan.
    /// A Start that returned an error was dispatched too.
    pub(super) fn started(&self) -> bool {
        self.completed.contains(&Action::Start)
            || self.pending.contains_key(&Action::Start)
            || self.operation_errors.contains_key(&Action::Start)
    }

    /// Replacement fee liability incurred if any signature has been dispatched or completed.
    pub(super) fn replacement_liability(&self) -> Option<Amount> {
        let signed = self
            .completed
            .iter()
            .chain(self.pending.keys())
            .any(|action| matches!(action, Action::Sign(_) | Action::Record | Action::Publish));
        signed
            .then(|| self.execution.replacement_liability())
            .flatten()
    }

    /// Errors returned by dispatched operations, including late completions.
    pub fn operation_errors(&self) -> &BTreeMap<Action, Error> {
        &self.operation_errors
    }

    pub(super) fn available(&self) -> impl Iterator<Item = Request> + '_ {
        self.path
            .actions()
            .filter(|action| {
                self.status == Status::Running
                    && !self.pending.contains_key(action)
                    && self.path.enabled(action, &self.completed)
            })
            .map(|action| Request {
                plan: self.plan.clone(),
                action: action.clone(),
            })
    }

    pub(super) fn apply(&mut self, request: Request) {
        if self.status != Status::Running {
            return;
        }
        if !Arc::ptr_eq(&self.plan, &request.plan)
            || !self.path.enabled(&request.action, &self.completed)
        {
            self.status = Status::Failed(error("action prerequisites are not satisfied"));
            return;
        }
        if let Entry::Vacant(entry) = self.pending.entry(request.action.clone()) {
            // The effect is fixed now; the construction state changes on completion.
            let effect = self.execution.effect(&request.action);
            entry.insert(Operation {
                plan: request.plan,
                action: request.action,
                effect: Arc::new(effect),
                receipt: Arc::default(),
            });
        }
    }

    pub fn pending(&self) -> impl Iterator<Item = &Operation> {
        self.pending.values()
    }

    pub(super) fn completed_operation(
        &mut self,
        id: super::OperationId,
        result: Result<Update, Error>,
    ) {
        let Some(operation) = self.pending.remove(&id.1) else {
            return;
        };
        let action = operation.action;
        let result = result.and_then(|update| self.execution.complete(&action, update));
        match result {
            Ok(()) => {
                if action == Action::Publish && self.status == Status::Running {
                    self.status = Status::AwaitingConfirmation;
                }
                self.completed.insert(action);
            }
            Err(err) => {
                if self.status == Status::Running {
                    self.status = Status::Failed(err.clone());
                }
                self.operation_errors.insert(action, err);
            }
        }
    }

    pub(super) fn observe(&mut self, input: Observation) {
        match input {
            Observation::Confirmed(txid)
                if self
                    .payment()
                    .is_some_and(|p| p.transaction.compute_txid() == txid) =>
            {
                self.status = Status::Confirmed;
            }
            Observation::Invalidated(reason) | Observation::TransactionInvalidated(_, reason)
                if self.status != Status::Confirmed =>
            {
                self.status = Status::Failed(error(reason));
            }
            _ => {}
        }
    }
}

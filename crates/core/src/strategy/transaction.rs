//! The current execution path and results for one transaction.

use super::{Action, Error, Observation, Plan, Request, Status, Update, error, path::Path};
use cost_function::candidate::Candidate;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub struct Transaction {
    plan: Arc<Plan>,
    path: Path,
    status: Status,
    completed: BTreeSet<Action>,
    operation_errors: BTreeMap<Action, Error>,
    pending: BTreeMap<Action, Request>,
}

impl Transaction {
    pub(super) fn new(plan: Arc<Plan>) -> Self {
        Self {
            path: Path::new(&plan.candidate),
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
    pub fn completed(&self) -> &BTreeSet<Action> {
        &self.completed
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
        self.pending
            .entry(request.action.clone())
            .or_insert(request);
    }

    pub fn pending(&self) -> impl Iterator<Item = &Request> {
        self.pending.values()
    }

    pub(super) fn completed_operation(
        &mut self,
        id: super::OperationId,
        result: Result<Update, Error>,
    ) {
        if let Some(request) = self.pending.remove(&id.1) {
            match result {
                Ok(Update::Applied) => {
                    self.completed.insert(request.action);
                }
                Err(err) => {
                    self.operation_errors.insert(request.action, err.clone());
                    self.status = Status::Failed(err);
                }
            }
        }
    }

    pub(super) fn observe(&mut self, input: Observation) {
        let Observation::Invalidated(reason) = input;
        self.status = Status::Failed(error(reason));
    }
}

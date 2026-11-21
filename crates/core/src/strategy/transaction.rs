//! The current execution path and results for one transaction.

use super::{
    Action, Error, ExecutionContext, Observation, Payment, Request, Status, Update, error,
    path::Path,
};
use cost_function::candidate::Candidate;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

pub struct Transaction {
    context: Arc<ExecutionContext>,
    path: Path,
    status: Status,
    payment: Option<Payment>,
    eager_publishing: bool,
    completed: BTreeSet<Action>,
    operation_errors: BTreeMap<Action, Error>,
    pending: BTreeMap<Action, Request>,
}

impl Transaction {
    pub(super) fn new(context: Arc<ExecutionContext>, eager_publishing: bool) -> Self {
        Self {
            path: Path::new(&context.candidate),
            context,
            status: Status::Running,
            payment: None,
            eager_publishing,
            completed: BTreeSet::new(),
            operation_errors: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }
    pub fn candidate(&self) -> &Candidate<u64> {
        &self.context.candidate
    }
    /// The construction-session UUID retained for this execution's retries.
    pub fn construction_session_id(&self) -> [u8; 16] {
        self.context.session_id
    }
    pub fn status(&self) -> &Status {
        &self.status
    }
    pub fn payment(&self) -> Option<&Payment> {
        self.payment.as_ref()
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
                    && (self.eager_publishing || **action != Action::Publish)
                    && !self.pending.contains_key(action)
                    && self.path.enabled(action, &self.completed)
            })
            .map(|action| Request {
                context: self.context.clone(),
                action: action.clone(),
            })
    }

    pub(super) fn apply(&mut self, request: Request) {
        if self.status != Status::Running {
            return;
        }
        if !Arc::ptr_eq(&self.context, &request.context)
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
        let Some(request) = self.pending.remove(&id.1) else {
            return;
        };
        let action = request.action;
        let result = match (&action, result) {
            (Action::Sign(_), Ok(Update::Complete(payment))) => {
                self.payment = Some(payment);
                if !self.eager_publishing && self.status == Status::Running {
                    self.status = Status::AwaitingConfirmation;
                }
                Ok(())
            }
            (_, Ok(Update::Complete(_))) => Err(error("unexpected effect result")),
            (_, Ok(Update::Applied)) => Ok(()),
            (_, Err(err)) => Err(err),
        };
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
                    .payment
                    .as_ref()
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

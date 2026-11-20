//! The current execution path and results for one transaction.

use super::{
    Action, Error, Observation, Operation, Payment, Plan, Request, Status, Update,
    construction::{Effect, Execution},
    error,
    path::Path,
    session::{Message, Progress, Tracker, WitnessContribution},
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
    excluded: bool,
    execution: Execution,
    completed: BTreeSet<Action>,
    operation_errors: BTreeMap<Action, Error>,
    pending: BTreeMap<Action, Operation>,
}

impl Transaction {
    pub(super) fn new(plan: Arc<Plan>) -> Self {
        Self {
            path: Path::new(&plan.candidate, plan.session.is_some()),
            execution: Execution::new(plan.clone()),
            plan,
            status: Status::Running,
            excluded: false,
            completed: BTreeSet::new(),
            operation_errors: BTreeMap::new(),
            pending: BTreeMap::new(),
        }
    }

    pub fn candidate(&self) -> &Candidate<u64> {
        &self.plan.candidate
    }
    /// The construction-session UUID retained for this execution's retries.
    pub fn construction_session_id(&self) -> [u8; 16] {
        self.plan.session_id
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
    pub fn session_progress(&self) -> Option<&Progress> {
        self.plan.session.map(|_| self.execution.progress())
    }

    pub(super) fn session_id(&self) -> Option<[u8; 16]> {
        self.plan.session.map(|config| config.id)
    }

    pub(super) fn excluded(&self) -> bool {
        self.excluded
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

    fn enabled(&self, action: &Action) -> bool {
        match action {
            Action::SendInputsReady(count) => {
                !self.completed.contains(action)
                    && self.path.inputs_sent(&self.completed)
                    && !self
                        .pending
                        .keys()
                        .any(|pending| matches!(pending, Action::SendInputsReady(_)))
                    && self.execution.pending_inputs_ready() == Some(*count)
            }
            Action::SendOutputsReady(count) => {
                !self.completed.contains(action)
                    && self.path.outputs_sent(&self.completed)
                    && !self
                        .pending
                        .keys()
                        .any(|pending| matches!(pending, Action::SendOutputsReady(_)))
                    && self.execution.pending_outputs_ready() == Some(*count)
            }
            _ => {
                if !self.path.enabled(action, &self.completed) {
                    return false;
                }
                if self.plan.session.is_some() {
                    match action {
                        Action::CloseInputs => return self.execution.progress().inputs().is_some(),
                        Action::CloseOutputs => {
                            return self.execution.progress().outputs().is_some();
                        }
                        Action::Finalize => return self.execution.all_inputs_signed(),
                        _ => {}
                    }
                }
                true
            }
        }
    }

    pub(super) fn available(&self) -> impl Iterator<Item = Request> + '_ {
        let dynamic = [
            self.execution
                .pending_inputs_ready()
                .map(Action::SendInputsReady),
            self.execution
                .pending_outputs_ready()
                .map(Action::SendOutputsReady),
        ]
        .into_iter()
        .flatten();
        self.path
            .actions()
            .cloned()
            .chain(dynamic)
            .filter(|action| {
                self.status == Status::Running
                    && !self.pending.contains_key(action)
                    && self.enabled(action)
            })
            .map(|action| Request {
                plan: self.plan.clone(),
                action,
            })
    }

    pub(super) fn apply(&mut self, request: Request) {
        if self.status != Status::Running {
            return;
        }
        if !Arc::ptr_eq(&self.plan, &request.plan) || !self.enabled(&request.action) {
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
    ) -> Option<Message> {
        let operation = self.pending.remove(&id.1)?;
        let sent = match operation.effect.as_ref() {
            Effect::Send(message) => Some(message.clone()),
            _ => None,
        };
        let action = operation.action;
        let result = result.and_then(|update| self.execution.complete(&action, update));
        match result {
            Ok(()) => {
                if action == Action::Publish && self.status == Status::Running {
                    self.status = Status::AwaitingConfirmation;
                }
                self.completed.insert(action);
                sent
            }
            Err(err) => {
                if self.status == Status::Running {
                    self.status = Status::Failed(err.clone());
                }
                self.operation_errors.insert(action, err);
                None
            }
        }
    }

    pub(super) fn sync_tracker(&mut self, tracker: &Tracker) {
        let config = self.plan.session.expect("session transaction");
        let progress = tracker.progress();
        let inputs_ready = tracker.ready_inputs_message(config.side);
        let outputs_ready = tracker.ready_outputs_message(config.side);
        let witnesses: Vec<_> = tracker.witnesses().cloned().collect();
        self.update_session(progress, inputs_ready, outputs_ready, &witnesses);
    }

    pub(super) fn set_progress(&mut self, progress: Progress) {
        let witnesses: Vec<_> = progress
            .signatures()
            .into_iter()
            .flatten()
            .cloned()
            .collect();
        self.update_session(progress, None, None, &witnesses);
    }

    fn update_session(
        &mut self,
        progress: Progress,
        inputs_ready: Option<(usize, Message)>,
        outputs_ready: Option<(usize, Message)>,
        witnesses: &[WitnessContribution],
    ) {
        if !progress.extends(self.execution.progress()) {
            if self.status != Status::Confirmed {
                self.status = Status::Failed(error("session progress contradicts finalized phase"));
            }
            return;
        }
        if self.status != Status::Running {
            return;
        }
        if let Some(agreed_inputs) = progress.inputs()
            && !self.plan.candidate.inputs().all(|coin| {
                agreed_inputs
                    .iter()
                    .any(|contrib| contrib.outpoint == coin.outpoint())
            })
        {
            self.excluded = true;
            self.status = Status::Failed(error("session closed inputs without local input"));
            return;
        }
        if let Some((agreed_outputs, agreed_fees, _)) = progress.outputs()
            && (!self
                .plan
                .output_ids
                .iter()
                .all(|id| agreed_outputs.iter().any(|contrib| &contrib.id == id))
                || !agreed_fees.iter().any(|fee| fee.id == self.plan.fee_id))
        {
            self.excluded = true;
            self.status =
                Status::Failed(error("session closed outputs without local contribution"));
            return;
        }
        self.execution
            .set_session_view(progress, inputs_ready, outputs_ready);
        for contrib in witnesses {
            if let Err(err) = self.execution.apply_witness(contrib) {
                self.status = Status::Failed(err);
                return;
            }
        }
    }

    pub(super) fn observe_broadcast(
        &mut self,
        tx: &bitcoin::Transaction,
    ) -> Vec<WitnessContribution> {
        let mut accepted = Vec::new();
        if self.execution.frozen_txid() != Some(tx.compute_txid()) {
            return accepted;
        }
        for input in tx.input.iter().filter(|input| !input.witness.is_empty()) {
            let contrib = WitnessContribution {
                outpoint: input.previous_output,
                witness: input.witness.clone(),
            };
            if let Ok(true) = self.execution.apply_witness(&contrib) {
                accepted.push(contrib);
            }
        }
        accepted
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

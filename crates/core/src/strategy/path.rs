//! Actions and prerequisites for realizing one complete funded transaction.

use super::Action;
use cost_function::candidate::Candidate;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Path(BTreeMap<Action, Vec<Action>>);

impl Path {
    pub fn new(outcome: &Candidate<u64>, collaborative: bool) -> Self {
        let inputs: Vec<_> = (0..outcome.inputs().count()).map(Action::Input).collect();
        let signatures: Vec<_> = (0..inputs.len()).map(Action::Sign).collect();
        let payments = outcome.payments().count();
        let outputs: Vec<_> = (0..payments + outcome.change().count())
            .map(Action::Output)
            .collect();
        let mut actions = BTreeMap::from([(Action::Start, vec![])]);
        let mut close_inputs = vec![Action::Start];
        for (i, input) in inputs.into_iter().enumerate() {
            actions.insert(input.clone(), vec![Action::Start]);
            close_inputs.push(input);
            if collaborative {
                let send = Action::SendInput(i);
                actions.insert(send.clone(), vec![Action::Input(i)]);
                close_inputs.push(send);
            }
        }
        actions.insert(Action::CloseInputs, close_inputs);
        for i in 0..payments {
            actions.insert(Action::Output(i), vec![Action::CloseInputs]);
        }
        for i in 0..outcome.change().count() {
            actions.insert(Action::AllocateChange(i), vec![Action::CloseInputs]);
            actions.insert(
                Action::Output(payments + i),
                vec![Action::AllocateChange(i)],
            );
        }
        let mut close_outputs = vec![Action::CloseInputs];
        for (j, output) in outputs.into_iter().enumerate() {
            close_outputs.push(output);
            if collaborative {
                let send = Action::SendOutput(j);
                actions.insert(send.clone(), vec![Action::Output(j)]);
                close_outputs.push(send);
            }
        }
        if collaborative {
            actions.insert(Action::SendFee, vec![Action::CloseInputs]);
            close_outputs.push(Action::SendFee);
        }
        actions.insert(Action::CloseOutputs, close_outputs);
        for (i, sign) in signatures.iter().enumerate() {
            actions.insert(sign.clone(), vec![Action::CloseOutputs]);
            if collaborative {
                actions.insert(Action::SendWitness(i), vec![Action::Sign(i)]);
            }
        }
        actions.insert(
            Action::Finalize,
            signatures
                .into_iter()
                .chain([Action::CloseOutputs])
                .collect(),
        );
        actions.insert(Action::Record, vec![Action::Finalize]);
        actions.insert(Action::Publish, vec![Action::Record]);
        Self(actions)
    }

    pub fn enabled(&self, action: &Action, completed: &BTreeSet<Action>) -> bool {
        !completed.contains(action)
            && self
                .0
                .get(action)
                .is_some_and(|parents| parents.iter().all(|parent| completed.contains(parent)))
    }

    pub fn inputs_sent(&self, completed: &BTreeSet<Action>) -> bool {
        self.0
            .keys()
            .filter(|action| matches!(action, Action::SendInput(_)))
            .all(|action| completed.contains(action))
    }

    pub fn outputs_sent(&self, completed: &BTreeSet<Action>) -> bool {
        self.0
            .keys()
            .filter(|action| matches!(action, Action::SendOutput(_) | Action::SendFee))
            .all(|action| completed.contains(action))
    }

    pub fn actions(&self) -> impl Iterator<Item = &Action> {
        self.0.keys()
    }
}

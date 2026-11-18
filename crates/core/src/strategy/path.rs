//! Actions and prerequisites for realizing one complete funded transaction.

use super::Action;
use cost_function::candidate::Candidate;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Path(BTreeMap<Action, Vec<Action>>);

impl Path {
    pub fn new(outcome: &Candidate<u64>) -> Self {
        let inputs: Vec<_> = (0..outcome.inputs().count()).map(Action::Input).collect();
        let signatures: Vec<_> = (0..inputs.len()).map(Action::Sign).collect();
        let payments = outcome.payments().count();
        let outputs: Vec<_> = (0..payments + outcome.change().count())
            .map(Action::Output)
            .collect();
        let mut actions = BTreeMap::from([(Action::Start, vec![])]);
        for input in &inputs {
            actions.insert(input.clone(), vec![Action::Start]);
        }
        actions.insert(
            Action::CloseInputs,
            inputs.into_iter().chain([Action::Start]).collect(),
        );
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
        actions.insert(
            Action::CloseOutputs,
            outputs.into_iter().chain([Action::CloseInputs]).collect(),
        );
        for sign in &signatures {
            actions.insert(sign.clone(), vec![Action::CloseOutputs]);
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

    pub fn actions(&self) -> impl Iterator<Item = &Action> {
        self.0.keys()
    }
}

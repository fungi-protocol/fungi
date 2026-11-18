//! Actions and prerequisites for realizing one complete funded transaction.

use super::Action;
use cost_function::candidate::Candidate;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Path(BTreeMap<Action, Vec<Action>>);

impl Path {
    pub fn new(outcome: &Candidate<u64>) -> Self {
        let inputs: Vec<_> = (0..outcome.inputs().count()).map(Action::Input).collect();
        let mut actions = BTreeMap::from([(Action::Start, vec![])]);
        for input in &inputs {
            actions.insert(input.clone(), vec![Action::Start]);
        }
        actions.insert(
            Action::CloseInputs,
            inputs.into_iter().chain([Action::Start]).collect(),
        );
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

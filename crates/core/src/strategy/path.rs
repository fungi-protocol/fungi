//! Actions available to begin construction of a funded transaction.

use super::Action;
use std::collections::BTreeSet;

pub(super) struct Path(BTreeSet<Action>);

impl Path {
    pub fn new() -> Self {
        Self(BTreeSet::from([Action::Start]))
    }

    pub fn enabled(&self, action: &Action, completed: &BTreeSet<Action>) -> bool {
        self.0.contains(action) && !completed.contains(action)
    }

    pub fn actions(&self) -> impl Iterator<Item = &Action> {
        self.0.iter()
    }
}

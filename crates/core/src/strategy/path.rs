//! Effects and prerequisites for realizing one funded transaction.

use super::{Action, identity};
use bitcoin::OutPoint;
use concurrent_psbt::output::UniqueId;
use cost_function::candidate::Candidate;
use std::collections::{BTreeMap, BTreeSet};

pub(super) struct Path(BTreeMap<Action, Vec<Action>>);

impl Path {
    pub fn new(outcome: &Candidate<u64>) -> Self {
        Self::from_identities(
            outcome.inputs().map(|coin| coin.outpoint()),
            outcome
                .change()
                .enumerate()
                .map(|(index, _)| identity::change(index)),
        )
    }

    fn from_identities(
        inputs: impl IntoIterator<Item = OutPoint>,
        changes: impl IntoIterator<Item = UniqueId>,
    ) -> Self {
        let inputs: BTreeSet<_> = inputs.into_iter().collect();
        let mut changes: Vec<_> = changes.into_iter().collect();
        changes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

        let signatures: Vec<_> = inputs.iter().copied().map(Action::Sign).collect();
        let compositions: Vec<_> = changes.into_iter().map(Action::ComposeChange).collect();
        let mut actions = BTreeMap::new();

        for composition in &compositions {
            actions.insert(composition.clone(), vec![]);
        }
        for sign in &signatures {
            actions.insert(sign.clone(), compositions.clone());
        }
        actions.insert(Action::Publish, signatures);

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

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{Txid, hashes::Hash};

    #[test]
    fn candidate_order_does_not_identify_actions() {
        let outpoints = [1, 2, 3].map(|vout| OutPoint::new(Txid::all_zeros(), vout));
        let changes = [identity::change(0), identity::change(1)];

        let forward = Path::from_identities(outpoints, changes.clone());
        let reverse = Path::from_identities(outpoints.into_iter().rev(), changes.into_iter().rev());

        assert_eq!(forward.0, reverse.0);
        assert!(forward.0.keys().all(|action| {
            !matches!(action, Action::ComposeChange(id) if id.as_bytes().len() != 9)
        }));
    }
}

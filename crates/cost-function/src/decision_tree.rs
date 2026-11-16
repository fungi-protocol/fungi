//! Include and exclude decisions over a prefix of candidate inputs.

use crate::{
    action::{Action, UnSequencedActions},
    wallet::Utxo,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TreeNode<'a> {
    utxos: &'a [Utxo],
    remaining: &'a [Utxo],
    /// One bit per decided input, with the most recent decision in the low bit.
    picked: u64,
}

impl<'a> TreeNode<'a> {
    pub(crate) fn new(utxos: &'a [Utxo]) -> Self {
        assert!(utxos.len() <= 64, "Number of utxos is capped at 64");
        Self {
            utxos,
            remaining: utxos,
            picked: 0,
        }
    }

    /// The leaf below this node that includes every remaining utxo.
    // Coin selection already did the random draw, so every selected utxo is spent.
    pub(crate) fn most_inclusive(&self) -> Self {
        let mut node = *self;
        while let Some(next) = node.child(true) {
            node = next;
        }
        node
    }

    /// Register included inputs in their original order, even at a partial node.
    pub(crate) fn to_sequence_of_actions(self) -> UnSequencedActions {
        let decided = &self.utxos[..self.utxos.len() - self.remaining.len()];
        UnSequencedActions(
            decided
                .iter()
                .zip((0..decided.len()).rev())
                .filter(|&(_, bit)| self.picked & (1 << bit) != 0)
                .map(|(utxo, _)| Action::RegisterInput(utxo.clone()))
                .collect(),
        )
    }

    fn child(&self, include: bool) -> Option<Self> {
        let (_, remaining) = self.remaining.split_first()?;
        Some(Self {
            utxos: self.utxos,
            remaining,
            picked: (self.picked << 1) + u64::from(include),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use bitcoin::{Amount, OutPoint, Txid, Weight, hashes::Hash};

    use super::*;

    fn coin(vout: u32) -> Utxo {
        Utxo::new(
            OutPoint::new(Txid::all_zeros(), vout),
            Amount::from_sat(1_000),
            Weight::ZERO,
        )
    }

    fn coins(n: u32) -> Vec<Utxo> {
        (0..n).map(coin).collect()
    }

    #[test]
    fn the_root_has_decided_nothing() {
        let utxos = coins(2);
        let root = TreeNode::new(&utxos);

        assert_eq!(root.remaining, &utxos);
        assert_eq!(root.to_sequence_of_actions(), UnSequencedActions(vec![]));
    }

    #[test]
    fn a_tree_with_no_inputs_has_no_children_or_actions() {
        let root = TreeNode::new(&[]);

        assert!(root.child(true).is_none());
        assert!(root.child(false).is_none());
        assert_eq!(root.most_inclusive(), root);
        assert_eq!(root.to_sequence_of_actions(), UnSequencedActions(vec![]));
    }

    #[test]
    fn a_partial_node_spends_only_the_inputs_included_so_far() {
        let utxos = coins(5);
        let node = TreeNode::new(&utxos)
            .child(true)
            .unwrap()
            .child(false)
            .unwrap()
            .child(true)
            .unwrap()
            .child(false)
            .unwrap();

        assert_eq!(node.remaining, &utxos[4..]);
        assert_eq!(
            node.to_sequence_of_actions(),
            UnSequencedActions(vec![
                Action::RegisterInput(coin(0)),
                Action::RegisterInput(coin(2)),
            ])
        );
        assert_eq!(
            node.most_inclusive().to_sequence_of_actions(),
            UnSequencedActions(vec![
                Action::RegisterInput(coin(0)),
                Action::RegisterInput(coin(2)),
                Action::RegisterInput(coin(4)),
            ])
        );
    }

    #[test]
    fn a_leaf_has_decided_every_utxo_and_goes_no_further() {
        let utxos = coins(64);
        let leaf = utxos
            .iter()
            .fold(TreeNode::new(&utxos), |node, _| node.child(false).unwrap());

        assert!(leaf.remaining.is_empty());
        assert!(leaf.child(true).is_none());
        assert!(leaf.child(false).is_none());
        assert_eq!(leaf.to_sequence_of_actions(), UnSequencedActions(vec![]));
    }

    #[test]
    fn the_leaves_are_every_subset_of_the_utxos() {
        let utxos = coins(3);
        let mut nodes = vec![TreeNode::new(&utxos)];
        for _ in &utxos {
            nodes = nodes
                .iter()
                .flat_map(|node| [node.child(true).unwrap(), node.child(false).unwrap()])
                .collect();
        }

        let leaves = nodes
            .iter()
            .map(|node| node.picked)
            .collect::<BTreeSet<_>>();
        assert_eq!(leaves, (0..8).collect());
    }

    #[test]
    fn most_inclusive_is_the_leaf_that_spends_every_utxo() {
        let utxos = coins(64);
        let leaf = TreeNode::new(&utxos).most_inclusive();

        assert!(leaf.remaining.is_empty());
        assert_eq!(leaf.picked, u64::MAX);
        assert_eq!(
            leaf.to_sequence_of_actions(),
            UnSequencedActions(utxos.into_iter().map(Action::RegisterInput).collect())
        );
    }

    #[test]
    #[should_panic(expected = "capped at 64")]
    fn more_than_64_utxos_do_not_fit_the_bitset() {
        TreeNode::new(&coins(65));
    }
}

//! A decision tree for placing satisfying a batches. Each node is a way to place an input or output.
//! NOTE / TODO: currently this is a naive implemenation that does not navigate the tree at all. Its a single
//! node / leaf.

use crate::{
    action::{Action, UnSequencedActions},
    wallet::Utxo,
};

/// Allows for 64 utxos and 2^64 - 1 different combinations of them.
type Picked = u64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Node {
    /// index into a list of utxos. This is the index of the next utxo
    level: usize,
    /// The bits of the picked utxos.
    picked: Picked,
}

impl Node {
    fn new(level: usize, picked: Picked) -> Self {
        Self { level, picked }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TreeNode<'a> {
    utxos: &'a [Utxo],
    node: Node,
}

impl<'a> TreeNode<'a> {
    pub(crate) fn new(utxos: &'a [Utxo]) -> Self {
        assert!(utxos.len() <= 64, "Number of utxos is capped at 64");
        Self {
            utxos,
            node: Node {
                level: 0,
                picked: 0,
            },
        }
    }
    fn is_leaf(&self) -> bool {
        !self.is_root() && self.node.level >= self.utxos.len()
    }
    fn is_root(&self) -> bool {
        self.node.level == 0 && self.node.picked == 0
    }
    /// Include the next utxo in consideration
    fn include(&self) -> Option<Self> {
        if self.is_leaf() {
            return None;
        }

        let cur = &self.node;
        // TODO: check for overflows
        let include_bitset = cur.picked | (1 << cur.level);

        Some(TreeNode {
            utxos: self.utxos,
            node: Node::new(cur.level + 1, include_bitset),
        })
    }

    // We are not considering the next utxo
    fn exclude(&self) -> Option<Self> {
        if self.is_leaf() {
            return None;
        }
        // TODO: error out if we exclude off the root?
        // TODO: we can prune the tree here.

        Some(TreeNode {
            utxos: self.utxos,
            node: Node::new(self.node.level + 1, self.node.picked),
        })
    }

    /// The leaf below this node that includes every remaining utxo.
    // shortcut: coin selection already did the random draw, so every selected utxo is spent;
    // upgrade once branches are scored.
    pub(crate) fn most_inclusive(&self) -> Self {
        let mut node = *self;
        while let Some(next) = node.include() {
            node = next;
        }
        node
    }

    // At any node in the tree you can convert into a sequence of actions.
    // Not garunteed produce a valid balanced transaction
    pub(crate) fn to_sequence_of_actions(self) -> UnSequencedActions {
        if self.is_root() {
            return UnSequencedActions(vec![]);
        }
        let bitset = &self.node.picked;
        fn picked(bitset: &Picked, i: usize) -> bool {
            bitset & 1 << i != 0
        }
        UnSequencedActions(
            self.utxos
                .iter()
                .enumerate()
                .filter(|(i, _)| picked(bitset, *i))
                .map(|(_, utxo)| Action::RegisterInput(utxo.to_txin()))
                .collect(),
        )
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

        assert!(root.is_root());
        assert!(!root.is_leaf());
        assert_eq!(root.to_sequence_of_actions(), UnSequencedActions(vec![]));
    }

    #[test]
    fn each_level_decides_the_next_utxo_and_a_node_spends_only_what_it_included() {
        let utxos = coins(3);
        let node = TreeNode::new(&utxos)
            .include()
            .unwrap()
            .exclude()
            .unwrap()
            .include()
            .unwrap();

        assert_eq!(node.node, Node::new(3, 0b101));
        assert_eq!(
            node.to_sequence_of_actions(),
            UnSequencedActions(vec![
                Action::RegisterInput(coin(0).to_txin()),
                Action::RegisterInput(coin(2).to_txin()),
            ])
        );
    }

    #[test]
    fn a_leaf_has_decided_every_utxo_and_goes_no_further() {
        let utxos = coins(2);
        let leaf = TreeNode::new(&utxos).exclude().unwrap().exclude().unwrap();

        assert!(leaf.is_leaf());
        assert!(leaf.include().is_none());
        assert!(leaf.exclude().is_none());
        // Excluding everything is a decision, not the root, and spends nothing.
        assert!(!leaf.is_root());
        assert_eq!(leaf.to_sequence_of_actions(), UnSequencedActions(vec![]));
    }

    #[test]
    fn the_leaves_are_every_subset_of_the_utxos() {
        let utxos = coins(3);
        let mut leaves = BTreeSet::new();
        let mut frontier = vec![TreeNode::new(&utxos)];
        while let Some(node) = frontier.pop() {
            if node.is_leaf() {
                leaves.insert(node.node.picked);
            } else {
                frontier.extend([node.include().unwrap(), node.exclude().unwrap()]);
            }
        }

        assert_eq!(leaves, (0..8).collect());
    }

    #[test]
    fn most_inclusive_is_the_leaf_that_spends_every_utxo() {
        let utxos = coins(64);
        let leaf = TreeNode::new(&utxos).most_inclusive();

        assert!(leaf.is_leaf());
        assert_eq!(leaf.node.picked, u64::MAX);
        assert_eq!(leaf.to_sequence_of_actions().0.len(), 64);
    }

    #[test]
    #[should_panic(expected = "capped at 64")]
    fn more_than_64_utxos_do_not_fit_the_bitset() {
        TreeNode::new(&coins(65));
    }
}

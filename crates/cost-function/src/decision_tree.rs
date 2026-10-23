//! A decision tree for placing satisfying a batches. Each node is a way to place an input or output.
//! NOTE / TODO: currently this is a naive implemenation that does not navigate the tree at all. Its a single
//! node / leaf.

use std::collections::BTreeSet;

use bitcoin::TxOut;
use rand::{RngCore, seq::SliceRandom};

use crate::{
    action::{Action, UnSequencedActions},
    wallet::Utxo,
};

/// The coins to spend, the payments to make and the change outputs to create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Leaf {
    pub(crate) inputs: BTreeSet<Utxo>,
    pub(crate) payments: Vec<TxOut>,
    pub(crate) change: Vec<TxOut>,
}

impl Leaf {
    /// Outputs are shuffled so change does not always follow the payments.
    pub(crate) fn to_sequence_of_actions(&self, rng: &mut impl RngCore) -> UnSequencedActions {
        let inputs_actions = self
            .inputs
            .iter()
            .map(|input| Action::RegisterInput(input.to_txin()));
        let mut output_actions: Vec<Action> = self
            .payments
            .iter()
            .chain(&self.change)
            .map(|output| Action::RegisterOutput(output.clone()))
            .collect();
        output_actions.shuffle(rng);

        UnSequencedActions(inputs_actions.chain(output_actions).collect())
    }
}

/// Where the next unplaced intent could go.
pub(crate) enum DecisionTree {
    Branch(Vec<Node>),
    // Note: outputs are not id'd. This will not detect duplicates
    Leaf(Leaf),
}

impl DecisionTree {
    /// Every leaf in the tree, in tree order.
    pub(crate) fn iter_leaves(&self) -> Box<dyn Iterator<Item = &Leaf> + '_> {
        match self {
            DecisionTree::Branch(nodes) => {
                Box::new(nodes.iter().flat_map(|node| node.subtree.iter_leaves()))
            }
            DecisionTree::Leaf(leaf) => Box::new(std::iter::once(leaf)),
        }
    }
}

/// One way to place one intent.
pub(crate) struct Node {
    pub(crate) subtree: DecisionTree,
}

#[cfg(test)]
mod tests {
    use bitcoin::ScriptBuf;
    use rand::{SeedableRng, rngs::StdRng};

    use super::*;

    fn nodes_of(tree: &DecisionTree) -> &[Node] {
        match tree {
            DecisionTree::Branch(nodes) => nodes,
            DecisionTree::Leaf(_) => &[],
        }
    }

    fn plan_of(tree: &DecisionTree) -> Option<&Leaf> {
        match tree {
            DecisionTree::Leaf(plan) => Some(plan),
            DecisionTree::Branch(_) => None,
        }
    }

    #[test]
    fn an_edge_places_one_intent_and_leads_to_the_rest() {
        let finished = Leaf {
            inputs: BTreeSet::new(),
            payments: Vec::new(),
            change: Vec::new(),
        };

        let tree = DecisionTree::Branch(vec![Node {
            subtree: DecisionTree::Leaf(finished.clone()),
        }]);

        let nodes = nodes_of(&tree);
        assert_eq!(nodes.len(), 1);
        assert_eq!(plan_of(&nodes[0].subtree), Some(&finished));

        // A branch is not a finished plan, and a leaf has nowhere further to go.
        assert_eq!(plan_of(&tree), None);
        assert!(nodes_of(&DecisionTree::Leaf(finished)).is_empty());
    }

    #[test]
    fn iter_leaves_collects_every_leaf_in_tree_order() {
        let output = |sats| TxOut {
            value: bitcoin::Amount::from_sat(sats),
            script_pubkey: ScriptBuf::new(),
        };
        let leaf = |sats| Leaf {
            inputs: BTreeSet::new(),
            payments: Vec::new(),
            change: vec![output(sats)],
        };
        let node = |subtree| Node { subtree };

        let tree = DecisionTree::Branch(vec![
            node(DecisionTree::Leaf(leaf(1))),
            node(DecisionTree::Branch(vec![
                node(DecisionTree::Leaf(leaf(2))),
                node(DecisionTree::Leaf(leaf(3))),
            ])),
        ]);

        assert_eq!(
            tree.iter_leaves().collect::<Vec<_>>(),
            vec![&leaf(1), &leaf(2), &leaf(3)]
        );
    }

    #[test]
    fn change_is_not_always_the_last_output() {
        let output = |sats| TxOut {
            value: bitcoin::Amount::from_sat(sats),
            script_pubkey: ScriptBuf::new(),
        };
        let leaf = Leaf {
            inputs: BTreeSet::new(),
            payments: vec![output(1), output(2)],
            change: vec![output(3)],
        };

        let change_positions: BTreeSet<_> = (0..16)
            .map(|seed| {
                let actions = leaf.to_sequence_of_actions(&mut StdRng::seed_from_u64(seed));
                actions
                    .0
                    .iter()
                    .position(|action| *action == Action::RegisterOutput(output(3)))
            })
            .collect();

        assert!(change_positions.len() > 1);
    }
}

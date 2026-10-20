//! A decision tree for placing satisfying a batches. Each node is a way to place an input or output.
//! NOTE / TODO: currently this is a naive implemenation that does not navigate the tree at all. Its a single
//! node / leaf.

use crate::{change::OutputWithoutScriptpubkey, wallet::Utxo};

/// The coins to spend and the change outputs to create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Leaf {
    pub(crate) inputs: Vec<Utxo>,
    pub(crate) change: Vec<OutputWithoutScriptpubkey>,
}

/// Where the next input or output could go.
pub(crate) enum DecisionTree {
    Branch(Vec<Node>),
    // Note: outputs are not id'd. This will not detect duplicates
    Leaf(Leaf),
}

/// One way to place one input or output.
pub(crate) struct Node {
    pub(crate) subtree: DecisionTree,
}

#[cfg(test)]
mod tests {
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
    fn an_edge_places_one_input_or_output_and_leads_to_the_rest() {
        let finished = Leaf {
            inputs: Vec::new(),
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
}

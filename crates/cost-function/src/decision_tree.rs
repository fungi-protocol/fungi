use crate::plan::Plan;

/// Where the next unplaced intent could go.
pub(crate) enum DecisionTree<Id> {
    Branch(Vec<Node<Id>>),
    Leaf(Plan<Id>),
}

/// One way to place one intent.
pub(crate) struct Node<Id> {
    pub(crate) subtree: DecisionTree<Id>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::{InMemoryQueue, Queue};

    type Id = <InMemoryQueue as Queue>::Id;

    fn nodes_of(tree: &DecisionTree<Id>) -> &[Node<Id>] {
        match tree {
            DecisionTree::Branch(nodes) => nodes,
            DecisionTree::Leaf(_) => &[],
        }
    }

    fn plan_of(tree: &DecisionTree<Id>) -> Option<&Plan<Id>> {
        match tree {
            DecisionTree::Leaf(plan) => Some(plan),
            DecisionTree::Branch(_) => None,
        }
    }

    #[test]
    fn an_edge_places_one_intent_and_leads_to_the_rest() {
        let finished = Plan {
            batches: Vec::new(),
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

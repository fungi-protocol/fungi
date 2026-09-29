use crate::batch::Batch;
use crate::plan::PlanIntent;
use crate::wallet::WalletStateSnapshot;

/// Where the next unplaced intent could go.
#[derive(Debug)]
pub(crate) enum DecisionTree<Id> {
    Branch(Vec<Node<Id>>),
    Leaf(PlanIntent<Id>),
}

/// One way to place one intent.
#[derive(Debug)]
pub(crate) struct Node<Id> {
    pub(crate) subtree: DecisionTree<Id>,
}

/// Largest pool of intents in a queue that we will enumerate exhaustively.
pub(crate) const MAX_EXHAUSTIVE_POOL: usize = 10;

impl WalletStateSnapshot {
    /// Every way to partition the intents into batches, each intent named by its position
    /// in [`WalletStateSnapshot::intents`].
    ///
    /// Intents are placed in order, each joining an existing batch or opening a new one at
    /// the end. That restriction gives every partition exactly one path, so there are no
    /// duplicates to filter out.
    ///
    /// Refuses more than [`MAX_EXHAUSTIVE_POOL`] intents.
    pub(crate) fn enumerate(&self) -> Result<DecisionTree<usize>, EnumerationError> {
        self.enumerate_inner(MAX_EXHAUSTIVE_POOL)
    }

    /// [`WalletStateSnapshot::enumerate`] with an explicit cap, for callers that know their
    /// budget.
    fn enumerate_inner(&self, limit: usize) -> Result<DecisionTree<usize>, EnumerationError> {
        let intents = self.intents().len();
        if intents > limit {
            return Err(EnumerationError::PoolTooLarge { intents, limit });
        }

        let positions: Vec<usize> = (0..intents).collect();
        Ok(expand(&positions, &mut Vec::new()))
    }
}

/// Why a tree could not be built.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EnumerationError {
    /// Too many intents to enumerate.
    PoolTooLarge { intents: usize, limit: usize },
}

fn expand<Id: Copy>(unplaced: &[Id], batches: &mut Vec<Vec<Id>>) -> DecisionTree<Id> {
    let Some((intent, rest)) = unplaced.split_first() else {
        return DecisionTree::Leaf(PlanIntent {
            batches: batches
                .iter()
                .map(|intents| Batch {
                    intents: intents.clone(),
                })
                .collect(),
        });
    };

    let mut nodes = Vec::with_capacity(batches.len() + 1);

    // Add the current intent to all existing batches.
    for existing in 0..batches.len() {
        batches[existing].push(*intent);
        nodes.push(Node {
            subtree: expand(rest, batches),
        });
        batches[existing].pop();
    }

    // Create a new batch for the current intent and recurse.
    batches.push(vec![*intent]);
    nodes.push(Node {
        subtree: expand(rest, batches),
    });
    batches.pop();

    DecisionTree::Branch(nodes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::{Action, FixedPaymentInstructions, Intent, PayoffCurve};
    use crate::queue::{InMemoryQueue, Queue};
    use bitcoin::{Amount, ScriptBuf};
    use std::collections::HashSet;
    use std::time::Instant;

    type Id = <InMemoryQueue as Queue>::Id;

    fn nodes_of(tree: &DecisionTree<Id>) -> &[Node<Id>] {
        match tree {
            DecisionTree::Branch(nodes) => nodes,
            DecisionTree::Leaf(_) => &[],
        }
    }

    fn plan_of(tree: &DecisionTree<Id>) -> Option<&PlanIntent<Id>> {
        match tree {
            DecisionTree::Leaf(plan) => Some(plan),
            DecisionTree::Branch(_) => None,
        }
    }

    #[test]
    fn an_edge_places_one_intent_and_leads_to_the_rest() {
        let finished = PlanIntent {
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

    /// A snapshot holding `n` intents and no coins.
    fn pool(n: usize) -> WalletStateSnapshot {
        let start = Instant::now();
        let intent = |sats| {
            Intent::new(
                Action::OutputCreation(FixedPaymentInstructions {
                    script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                    amount: Amount::from_sat(sats),
                }),
                PayoffCurve::new(start, []),
            )
        };

        WalletStateSnapshot::new([], (1..=n as u64).map(intent))
    }

    /// Every plan in the tree.
    fn plans<Id>(tree: &DecisionTree<Id>) -> Vec<&PlanIntent<Id>> {
        match tree {
            DecisionTree::Leaf(plan) => vec![plan],
            DecisionTree::Branch(nodes) => nodes.iter().flat_map(|n| plans(&n.subtree)).collect(),
        }
    }

    /// Batch both, or keep them apart. Order is not a choice the tree makes.
    #[test]
    fn two_intents_give_two_plans() {
        let tree = pool(2).enumerate().unwrap();
        let plans = plans(&tree);

        assert_eq!(plans.len(), 2);
        assert!(plans.contains(&&PlanIntent {
            batches: vec![Batch {
                intents: vec![0, 1]
            }]
        }));
        assert!(plans.contains(&&PlanIntent {
            batches: vec![Batch { intents: vec![0] }, Batch { intents: vec![1] }]
        }));
    }

    #[test]
    fn both_extremes_are_present() {
        // These extremes represent two bounds. One where we are optimizing for block space (one batch)
        // and another where we are optimizing for privacy (seperate batches).
        let tree = pool(4).enumerate().unwrap();
        let plans = plans(&tree);

        let all_together = plans.iter().filter(|p| p.batches.len() == 1).count();
        let all_apart = plans.iter().filter(|p| p.batches.len() == 4).count();

        assert_eq!(all_together, 1);
        assert_eq!(all_apart, 1);
    }

    /// Restricted growth means each partition appears once, with no dedup pass.
    #[test]
    fn no_partition_appears_twice() {
        let tree = pool(5).enumerate().unwrap();
        let plans = plans(&tree);

        let distinct: HashSet<Vec<Vec<usize>>> = plans
            .iter()
            .map(|p| {
                let mut batches: Vec<Vec<usize>> =
                    p.batches.iter().map(|b| b.intents.clone()).collect();
                batches.sort();
                batches
            })
            .collect();

        assert_eq!(distinct.len(), plans.len());
    }

    #[test]
    fn an_empty_pool_has_one_empty_plan() {
        let tree = pool(0).enumerate().unwrap();

        assert_eq!(plans(&tree).len(), 1);
        assert_eq!(plans(&tree), [&PlanIntent { batches: vec![] }]);
    }

    #[test]
    fn the_cap_itself_still_enumerates() {
        assert!(pool(MAX_EXHAUSTIVE_POOL).enumerate().is_ok());
    }

    #[test]
    fn oversized_pools_are_refused() {
        let err = pool(MAX_EXHAUSTIVE_POOL + 1).enumerate().unwrap_err();

        assert_eq!(
            err,
            EnumerationError::PoolTooLarge {
                intents: MAX_EXHAUSTIVE_POOL + 1,
                limit: MAX_EXHAUSTIVE_POOL,
            }
        );
    }
}

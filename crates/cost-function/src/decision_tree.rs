use std::hash::Hash;

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

impl<Id: Copy + Ord + Hash> WalletStateSnapshot<Id> {
    /// Every way to partition the intents into batches, each intent named by its queue id.
    ///
    /// Intents are placed in id order, each joining an existing batch or opening a new one
    /// at the end. That restriction gives every partition exactly one path, so there are no
    /// duplicates to filter out.
    ///
    /// Refuses more than [`MAX_EXHAUSTIVE_POOL`] intents.
    pub(crate) fn enumerate(&self) -> Result<DecisionTree<Id>, EnumerationError> {
        self.enumerate_inner(MAX_EXHAUSTIVE_POOL)
    }

    /// [`WalletStateSnapshot::enumerate`] with an explicit cap, for callers that know their
    /// budget.
    fn enumerate_inner(&self, limit: usize) -> Result<DecisionTree<Id>, EnumerationError> {
        let mut ids: Vec<Id> = self.intents().map(|(&id, _)| id).collect();
        if ids.len() > limit {
            return Err(EnumerationError::PoolTooLarge {
                intents: ids.len(),
                limit,
            });
        }

        // The snapshot holds intents in no particular order; sorting keeps the tree the same
        // from one run to the next.
        ids.sort();
        Ok(expand(&ids, &mut Vec::new()))
    }
}

/// Why a tree could not be built.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum EnumerationError {
    /// Too many intents to enumerate.
    PoolTooLarge { intents: usize, limit: usize },
}

fn expand<Id: Copy + Ord>(unplaced: &[Id], batches: &mut Vec<Vec<Id>>) -> DecisionTree<Id> {
    let Some((intent, rest)) = unplaced.split_first() else {
        return DecisionTree::Leaf(PlanIntent {
            batches: batches
                .iter()
                .map(|intents| Batch {
                    intents: intents.iter().copied().collect(),
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
    use std::collections::BTreeSet;
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
            batches: BTreeSet::new(),
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

    /// A snapshot holding `n` intents and no coins, with queue ids 1 to `n`.
    fn pool(n: usize) -> WalletStateSnapshot<Id> {
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

        WalletStateSnapshot::new([], (1..=n as u64).map(|id| (id, intent(id))))
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
            batches: BTreeSet::from([Batch {
                intents: BTreeSet::from([1, 2])
            }])
        }));
        assert!(plans.contains(&&PlanIntent {
            batches: BTreeSet::from([
                Batch {
                    intents: BTreeSet::from([1])
                },
                Batch {
                    intents: BTreeSet::from([2])
                }
            ])
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

        let distinct: BTreeSet<_> = plans.iter().map(|p| &p.batches).collect();

        assert_eq!(distinct.len(), plans.len());
    }

    #[test]
    fn an_empty_pool_has_one_empty_plan() {
        let tree = pool(0).enumerate().unwrap();

        assert_eq!(plans(&tree).len(), 1);
        assert_eq!(
            plans(&tree),
            [&PlanIntent {
                batches: BTreeSet::new()
            }]
        );
    }

    #[test]
    fn the_cap_itself_still_enumerates() {
        assert!(pool(MAX_EXHAUSTIVE_POOL).enumerate().is_ok());
    }

    /// A small cap, so a broken check enumerates a handful of plans rather than B(11).
    #[test]
    fn oversized_pools_are_refused() {
        let err = pool(3).enumerate_inner(2).unwrap_err();

        assert_eq!(
            err,
            EnumerationError::PoolTooLarge {
                intents: 3,
                limit: 2
            }
        );
    }
}

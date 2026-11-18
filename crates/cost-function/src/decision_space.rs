//! The plans worth considering for the queued intents.

use std::collections::BTreeSet;

use crate::{batch::Batch, plan::PlanIntent, queue::Queue};

/// Plans for the queued intents, starting with the grand coalition, one batch of every
/// queued intent, and peeling intents off it one at a time. An empty queue has no plans.
pub(crate) struct DecisionSpace<'a, Q> {
    queue: &'a Q,
}

impl<'a, Q: Queue> DecisionSpace<'a, Q> {
    pub(crate) fn new(queue: &'a Q) -> Self {
        DecisionSpace { queue }
    }

    // currently this yields only the trivial partition, peel intents off once there is a cost to compare.
    // TODO: this should iterate and peel off one intent, putting it in its own batch each time.
    pub(crate) fn iter(&self) -> impl Iterator<Item = PlanIntent<Q::Id>> {
        let intents: BTreeSet<Q::Id> = self.queue.iter().map(|(id, _)| id).collect();

        (!intents.is_empty())
            .then(|| PlanIntent {
                batches: BTreeSet::from([Batch { intents }]),
            })
            .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve};
    use crate::queue::InMemoryQueue;
    use bitcoin::{Amount, ScriptBuf};

    fn intent(sats: u64) -> Intent {
        Intent::new(
            Outcome::OutputCreation(FixedPaymentInstructions {
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                amount: Amount::from_sat(sats),
            }),
            PayoffCurve::new(0, vec![]).unwrap(),
        )
    }

    #[test]
    fn the_only_plan_is_the_grand_coalition() {
        let mut queue = InMemoryQueue::new([]);
        assert_eq!(DecisionSpace::new(&queue).iter().next(), None);

        let first = queue.push(intent(1));
        let second = queue.push(intent(2));
        let space = DecisionSpace::new(&queue);
        let mut plans = space.iter();

        assert_eq!(
            plans.next(),
            Some(PlanIntent {
                batches: BTreeSet::from([Batch {
                    intents: BTreeSet::from([first, second]),
                }]),
            })
        );
        assert_eq!(plans.next(), None);
    }
}

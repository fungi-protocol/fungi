//! Groups of intents realized together.

use std::collections::BTreeSet;

/// Intents realized by one transaction, named by the ids of the queue they came from.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Batch<Id> {
    pub(crate) intents: BTreeSet<Id>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve};
    use crate::queue::{InMemoryQueue, Queue};
    use bitcoin::{Amount, ScriptBuf};
    use std::time::Instant;

    fn intent(start: Instant, sats: u64) -> Intent {
        Intent::new(
            Outcome::OutputCreation(FixedPaymentInstructions {
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                amount: Amount::from_sat(sats),
            }),
            PayoffCurve::new(start, vec![]),
        )
    }

    #[test]
    fn a_batch_names_intents_by_their_queue_ids() {
        let start = Instant::now();
        let mut queue = InMemoryQueue::new([]);
        let first = queue.push(intent(start, 1));
        let second = queue.push(intent(start, 2));

        let batch: Batch<<InMemoryQueue as Queue>::Id> = Batch {
            intents: BTreeSet::from([first, second]),
        };

        assert_eq!(batch.intents, BTreeSet::from([second, first]));
        assert_eq!(batch.clone(), batch);
    }
}

//! The wallet's outstanding obligations.

use crate::intent::IntentWithPolicy;

/// All the information the wallet has about what the user wants to do.
/// Held in the order the intents arrived. Definetly not by priority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Queue(Vec<IntentWithPolicy>);

impl Queue {
    /// Take intents into a queue, naming each one by where it landed.
    pub fn new(intents: impl IntoIterator<Item = IntentWithPolicy>) -> Self {
        Queue(intents.into_iter().collect())
    }

    /// Whether the wallet has nothing it wants to do.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// How many intents the wallet is holding.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Every intent, in the order it arrived.
    pub fn iter(&self) -> impl Iterator<Item = &IntentWithPolicy> {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::{Action, FixedPaymentInstructions, PayoffCurve};
    use bitcoin::{Amount, ScriptBuf};
    use std::time::{Duration, Instant};

    /// Built from an explicit `start` so two calls with the same arguments compare equal.
    fn intent(start: Instant, sats: u64) -> IntentWithPolicy {
        IntentWithPolicy::new(
            Action::OutputCreation(FixedPaymentInstructions {
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                amount: Amount::from_sat(sats),
            }),
            PayoffCurve::new(
                start,
                vec![(Duration::from_secs(60), Amount::from_sat(sats))],
            ),
        )
    }

    #[test]
    fn an_empty_queue_has_nothing_in_it() {
        let queue = Queue::new([]);

        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.iter().count(), 0);
    }

    #[test]
    fn a_queue_keeps_every_intent_it_was_given() {
        let start = Instant::now();
        let queue = Queue::new([intent(start, 1), intent(start, 2), intent(start, 3)]);

        assert!(!queue.is_empty());
        assert_eq!(queue.len(), 3);
        assert_eq!(queue.iter().count(), 3);
        assert_eq!(queue.clone(), queue);
    }
}

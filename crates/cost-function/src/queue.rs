//! The wallet's outstanding obligations.

use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use crate::intent::Intent;

/// All the information the wallet has about what the user wants to do.
pub trait Queue {
    /// Names one intent in this queue.
    ///
    /// An id for an intent for as long as the queue holds it, and is
    /// never reused for another.
    type Id: Copy + Eq + Hash + Debug;

    /// Every intent with its id, in no particular order.
    fn iter(&self) -> impl Iterator<Item = (Self::Id, &Intent)>;

    /// The intent `id` names, if the queue still holds it.
    fn get(&self, id: Self::Id) -> Option<&Intent>;

    /// Drop an intent, because something realized it.
    fn remove(&mut self, id: Self::Id);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InMemoryQueue {
    intents: HashMap<u64, Intent>,
    next_id: u64,
}

impl InMemoryQueue {
    /// Take intents into a queue, naming each one as it lands.
    pub fn new(intents: impl IntoIterator<Item = Intent>) -> Self {
        let mut queue = InMemoryQueue {
            intents: HashMap::new(),
            next_id: 0,
        };
        for intent in intents {
            queue.push(intent);
        }
        queue
    }

    /// Add an intent, returning the id it will be known by.
    pub fn push(&mut self, intent: Intent) -> u64 {
        let id = self.next_id;
        self.next_id += 1;
        self.intents.insert(id, intent);
        id
    }
}

impl Queue for InMemoryQueue {
    type Id = u64;

    fn iter(&self) -> impl Iterator<Item = (u64, &Intent)> {
        self.intents.iter().map(|(&id, intent)| (id, intent))
    }

    fn get(&self, id: u64) -> Option<&Intent> {
        self.intents.get(&id)
    }

    fn remove(&mut self, id: u64) {
        self.intents.remove(&id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::intent::{Action, FixedPaymentInstructions, PayoffCurve};
    use bitcoin::{Amount, ScriptBuf};
    use std::collections::HashSet;
    use std::time::{Duration, Instant};

    /// Built from an explicit `start` so two calls with the same arguments compare equal.
    fn intent(start: Instant, sats: u64) -> Intent {
        Intent::new(
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
        let queue = InMemoryQueue::new([]);

        assert_eq!(queue.iter().count(), 0);
    }

    #[test]
    fn every_intent_is_yielded_under_its_own_id() {
        let start = Instant::now();
        let mut queue = InMemoryQueue::new([intent(start, 1), intent(start, 2)]);
        let pushed = queue.push(intent(start, 3));

        let ids: HashSet<u64> = queue.iter().map(|(id, _)| id).collect();

        assert_eq!(ids.len(), 3);
        assert_eq!(
            queue
                .iter()
                .find(|&(id, _)| id == pushed)
                .map(|(_, held)| held),
            Some(&intent(start, 3))
        );
        assert_eq!(queue.clone(), queue);
    }
}

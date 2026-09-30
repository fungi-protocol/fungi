//! One writer and one reader exchanging messages over a chain of slots.

use std::error::Error as StdError;

use crate::payload::{Keys, MAX_MESSAGE_BYTES};
use crate::slot::{SlotId, derive_slot_id};
use crate::store::{MailboxStore, PutOutcome};

/// Failure to write or read a message.
#[derive(Debug, thiserror::Error)]
pub enum Error<E: StdError + 'static> {
    /// The store failed.
    #[error("mailbox store: {0}")]
    Store(#[source] E),
    /// The message does not fit in one payload.
    #[error("message has {0} bytes, more than MAX_MESSAGE_BYTES")]
    MessageTooLarge(usize),
    /// The slot at `next_index` holds a different or unreadable message.
    #[error("the next slot holds a different payload")]
    Occupied,
    /// The payload at `next_index` does not decrypt for that slot.
    #[error("slot payload cannot be decrypted")]
    InvalidPayload,
}

/// Writes messages to consecutive slots of the chain keyed by a secret.
///
/// A failed write may have stored the message; retry it unchanged.
/// See [`Writer::resume`] for recovery after a restart.
pub struct Writer<S> {
    store: S,
    shared_secret: [u8; 32],
    keys: Keys,
    next_index: u64,
}

/// Reads messages from consecutive slots of the chain keyed by a secret.
///
/// Payloads must remain available until read.
pub struct Reader<S> {
    store: S,
    shared_secret: [u8; 32],
    keys: Keys,
    next_index: u64,
}

impl<S: MailboxStore<Message = Vec<u8>>> Writer<S> {
    /// Create a writer starting at slot 0 of the chain keyed by `shared_secret`.
    pub fn new(store: S, shared_secret: [u8; 32]) -> Self {
        Self::resume(store, shared_secret, 0)
    }

    /// Resume at a saved [`next_index`](Self::next_index).
    ///
    /// Persist each pending message with its index before sending. Retain it
    /// until the updated index is durably saved after success. On restart,
    /// retry pending messages at their original indices before sending new ones.
    ///
    /// An occupied slot accepts identical content as a retry; other content
    /// returns [`Error::Occupied`] if the store detects it, and is otherwise
    /// lost (see [`MailboxStore`]). A stale index can merge distinct messages
    /// with identical content.
    pub fn resume(store: S, shared_secret: [u8; 32], next_index: u64) -> Self {
        Self {
            store,
            shared_secret,
            keys: Keys::derive(&shared_secret),
            next_index,
        }
    }

    /// Index of the slot the next write uses.
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Store `message` in the next slot.
    pub async fn write(&mut self, message: &[u8]) -> Result<(), Error<S::Error>> {
        if message.len() > MAX_MESSAGE_BYTES {
            return Err(Error::MessageTooLarge(message.len()));
        }

        let slot = derive_slot_id(&self.shared_secret, self.next_index);
        let payload = self.keys.seal(slot, message);
        let outcome = self.store.put(slot, &payload).await.map_err(Error::Store)?;
        if outcome == PutOutcome::Occupied && !self.holds(slot, message).await? {
            return Err(Error::Occupied);
        }

        self.next_index = next(self.next_index);
        Ok(())
    }

    /// Whether an earlier attempt stored this message.
    async fn holds(&self, slot: SlotId, message: &[u8]) -> Result<bool, Error<S::Error>> {
        let stored = self.store.get(slot).await.map_err(Error::Store)?;
        let opened = stored.and_then(|payload| self.keys.open(slot, &payload));
        Ok(opened.as_deref() == Some(message))
    }
}

impl<S: MailboxStore<Message = Vec<u8>>> Reader<S> {
    /// Create a reader starting at slot 0 of the chain keyed by `shared_secret`.
    pub fn new(store: S, shared_secret: [u8; 32]) -> Self {
        Self::resume(store, shared_secret, 0)
    }

    /// Resume at a saved [`next_index`](Self::next_index).
    pub fn resume(store: S, shared_secret: [u8; 32], next_index: u64) -> Self {
        Self {
            store,
            shared_secret,
            keys: Keys::derive(&shared_secret),
            next_index,
        }
    }

    /// Index of the slot the next read uses.
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Wait for the message in the next slot.
    pub async fn read(&mut self) -> Result<Vec<u8>, Error<S::Error>> {
        loop {
            let slot = derive_slot_id(&self.shared_secret, self.next_index);
            if let Some(payload) = self.store.get(slot).await.map_err(Error::Store)? {
                let message = self
                    .keys
                    .open(slot, &payload)
                    .ok_or(Error::InvalidPayload)?;
                self.next_index = next(self.next_index);
                return Ok(message);
            }

            // Give a concurrent writer a chance to fill the slot when a
            // backend reports an empty poll without waiting.
            let mut yielded = false;
            std::future::poll_fn(|cx| {
                if std::mem::replace(&mut yielded, true) {
                    std::task::Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    std::task::Poll::Pending
                }
            })
            .await;
        }
    }
}

impl<S> std::fmt::Debug for Writer<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("next_index", &self.next_index)
            .finish_non_exhaustive()
    }
}

impl<S> std::fmt::Debug for Reader<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader")
            .field("next_index", &self.next_index)
            .finish_non_exhaustive()
    }
}

fn next(index: u64) -> u64 {
    index
        .checked_add(1)
        .expect("a chain cannot hold 2^64 messages")
}

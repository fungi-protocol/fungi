//! One writer and one reader exchanging messages over a chain of slots.

use std::error::Error as StdError;

use crate::payload::{Keys, MAX_MESSAGE_BYTES};
use crate::store::MailboxStore;

/// Failure to write or read a message.
#[derive(Debug, thiserror::Error)]
pub enum Error<E: StdError + 'static> {
    /// The store failed.
    #[error("mailbox store: {0}")]
    Store(#[source] E),
    /// The message does not fit in one payload.
    #[error("message has {0} bytes, more than MAX_MESSAGE_BYTES")]
    MessageTooLarge(usize),
    /// The payload at [`Reader::next_slot_id`] does not decrypt for that slot.
    #[error("slot payload cannot be decrypted")]
    InvalidPayload,
}

/// Writes messages to consecutive slots of the chain keyed by a secret.
///
/// A failed or cancelled write may have stored the message; retry it
/// unchanged. See [`Writer::resume`] for recovery after a restart.
pub struct Writer<S> {
    store: S,
    keys: Keys,
    next_slot_id: u64,
}

/// Reads messages from consecutive slots of the chain keyed by a secret.
///
/// Payloads must remain available until read. A cancelled read consumes
/// nothing, so reading again returns the same message.
pub struct Reader<S> {
    store: S,
    keys: Keys,
    next_slot_id: u64,
}

impl<S: MailboxStore> Writer<S> {
    /// Create a writer starting at slot 0 of the chain keyed by `shared_secret`.
    pub fn new(store: S, shared_secret: [u8; 32]) -> Self {
        Self::resume(store, shared_secret, 0)
    }

    /// Resume at a saved [`next_slot_id`](Self::next_slot_id).
    ///
    /// Persist each pending message with its slot ID before sending. Retain
    /// it until the updated slot ID is durably saved after success. On
    /// restart, retry pending messages at their original slot IDs before
    /// sending new ones.
    ///
    /// A slot keeps the first message stored in it, so a message written to
    /// an occupied slot is lost without an error (see [`MailboxStore`]).
    /// Resuming at a stale slot ID therefore loses new messages until the
    /// slot ID passes the occupied slots.
    pub fn resume(store: S, shared_secret: [u8; 32], next_slot_id: u64) -> Self {
        Self {
            store,
            keys: Keys::derive(shared_secret),
            next_slot_id,
        }
    }

    /// ID of the slot the next write uses.
    pub fn next_slot_id(&self) -> u64 {
        self.next_slot_id
    }

    /// Store `message` in the next slot.
    pub async fn write(&mut self, message: &[u8]) -> Result<(), Error<S::Error>> {
        if message.len() > MAX_MESSAGE_BYTES {
            return Err(Error::MessageTooLarge(message.len()));
        }

        let slot = self.keys.slot(self.next_slot_id);
        let payload = slot.seal(message);
        self.store
            .put(slot.secret(), &payload)
            .await
            .map_err(Error::Store)?;

        self.next_slot_id = next(self.next_slot_id);
        Ok(())
    }
}

impl<S: MailboxStore> Reader<S> {
    /// Create a reader starting at slot 0 of the chain keyed by `shared_secret`.
    pub fn new(store: S, shared_secret: [u8; 32]) -> Self {
        Self::resume(store, shared_secret, 0)
    }

    /// Resume at a saved [`next_slot_id`](Self::next_slot_id).
    pub fn resume(store: S, shared_secret: [u8; 32], next_slot_id: u64) -> Self {
        Self {
            store,
            keys: Keys::derive(shared_secret),
            next_slot_id,
        }
    }

    /// ID of the slot the next read uses.
    pub fn next_slot_id(&self) -> u64 {
        self.next_slot_id
    }

    /// Wait for the message in the next slot.
    pub async fn read(&mut self) -> Result<Vec<u8>, Error<S::Error>> {
        loop {
            let slot = self.keys.slot(self.next_slot_id);
            let stored = self.store.get(slot.secret()).await.map_err(Error::Store)?;
            if let Some(payload) = stored {
                let message = slot.open(&payload).ok_or(Error::InvalidPayload)?;
                self.next_slot_id = next(self.next_slot_id);
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
            .field("next_slot_id", &self.next_slot_id)
            .finish_non_exhaustive()
    }
}

impl<S> std::fmt::Debug for Reader<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader")
            .field("next_slot_id", &self.next_slot_id)
            .finish_non_exhaustive()
    }
}

fn next(slot_id: u64) -> u64 {
    slot_id
        .checked_add(1)
        .expect("a chain cannot hold 2^64 messages")
}

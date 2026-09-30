//! Storage contract for one slot in a linked mailbox chain.

use std::error::Error;
use std::future::Future;

use crate::slot::SlotId;

/// Result of attempting to store a message in a slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    /// The message is now stored in the slot.
    Stored,
    /// A different message already occupies the slot. Only stores that
    /// can detect this return it.
    Occupied,
}

/// Storage for one linked-mailbox chain, keyed by [`SlotId`].
///
/// A slot keeps the first message stored in it. [`put`](MailboxStore::put)
/// returns [`PutOutcome::Occupied`] when it detects that a different message
/// occupies the slot. A store that cannot detect this returns
/// [`PutOutcome::Stored`] and keeps the first message, so a caller that writes
/// different messages to one slot may lose all but the first without an
/// error.
pub trait MailboxStore: Send + Sync {
    /// Message stored in one slot.
    type Message: Send + Sync;
    /// Failure to read or write a slot.
    type Error: Error + Send + Sync + 'static;

    /// Store `message` at `slot_id` unless the slot is already occupied.
    ///
    /// Storing an identical message again returns [`PutOutcome::Stored`].
    fn put(
        &self,
        slot_id: SlotId,
        message: &Self::Message,
    ) -> impl Future<Output = Result<PutOutcome, Self::Error>> + Send;

    /// Fetch the message stored at `slot_id`.
    ///
    /// If empty, wait for a write or an implementation-defined timeout
    /// before returning `None`.
    fn get(
        &self,
        slot_id: SlotId,
    ) -> impl Future<Output = Result<Option<Self::Message>, Self::Error>> + Send;
}

impl<T: MailboxStore> MailboxStore for &T {
    type Message = T::Message;
    type Error = T::Error;

    async fn put(
        &self,
        slot_id: SlotId,
        message: &Self::Message,
    ) -> Result<PutOutcome, Self::Error> {
        (**self).put(slot_id, message).await
    }

    async fn get(&self, slot_id: SlotId) -> Result<Option<Self::Message>, Self::Error> {
        (**self).get(slot_id).await
    }
}

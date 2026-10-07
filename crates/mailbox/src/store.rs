//! Storage contract for one slot in a linked mailbox chain.

use std::error::Error;
use std::future::Future;

use crate::slot::SlotSecret;

/// Storage for one linked-mailbox chain, keyed by [`SlotSecret`].
///
/// A slot keeps the first message stored in it. A later
/// [`put`](MailboxStore::put) to an occupied slot succeeds without storing
/// anything, so a caller that writes different messages to one slot loses
/// all but the first without an error.
pub trait MailboxStore: Send + Sync {
    /// Failure to read or write a slot.
    type Error: Error + Send + Sync + 'static;

    /// Store `message` at `slot` unless the slot is already occupied.
    fn put(
        &self,
        slot: SlotSecret,
        message: &[u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Fetch the message stored at `slot`.
    ///
    /// If empty, wait for a write or an implementation-defined timeout
    /// before returning `None`.
    fn get(
        &self,
        slot: SlotSecret,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;
}

impl<T: MailboxStore> MailboxStore for &T {
    type Error = T::Error;

    async fn put(&self, slot: SlotSecret, message: &[u8]) -> Result<(), Self::Error> {
        (**self).put(slot, message).await
    }

    async fn get(&self, slot: SlotSecret) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get(slot).await
    }
}

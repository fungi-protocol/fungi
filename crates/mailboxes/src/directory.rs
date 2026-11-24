//! Contract for a directory holding a sequence of mailboxes.

use std::error::Error;
use std::future::Future;

use crate::secret::MailboxId;

/// A directory of mailboxes, each addressed by a [`MailboxId`].
///
/// Each mailbox retains its first message. Subsequent writes succeed without
/// replacing it.
pub trait Directory: Send + Sync {
    /// Failure to read or write a mailbox.
    type Error: Error + Send + Sync + 'static;

    /// Store `message` in `mailbox` unless the mailbox is already occupied.
    fn put(
        &self,
        mailbox: MailboxId,
        message: &[u8],
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;

    /// Fetch the message stored in `mailbox`.
    ///
    /// If empty, wait for a write or an implementation-defined timeout
    /// before returning `None`.
    fn get(
        &self,
        mailbox: MailboxId,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, Self::Error>> + Send;
}

impl<T: Directory> Directory for &T {
    type Error = T::Error;

    async fn put(&self, mailbox: MailboxId, message: &[u8]) -> Result<(), Self::Error> {
        (**self).put(mailbox, message).await
    }

    async fn get(&self, mailbox: MailboxId) -> Result<Option<Vec<u8>>, Self::Error> {
        (**self).get(mailbox).await
    }
}

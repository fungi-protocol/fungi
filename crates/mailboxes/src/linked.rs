//! One writer and one reader sending messages through a sequence of
//! mailboxes.

use std::error::Error as StdError;

use crate::directory::Directory;
use crate::payload::{Keys, MAX_MESSAGE_BYTES};

/// Failure to write or read a message.
#[derive(Debug, thiserror::Error)]
pub enum Error<E: StdError + 'static> {
    /// The directory failed.
    #[error("directory: {0}")]
    Directory(#[source] E),
    /// The message does not fit in one payload.
    #[error("message has {0} bytes, more than MAX_MESSAGE_BYTES")]
    MessageTooLarge(usize),
    /// The payload at [`Reader::next_index`] does not decrypt for that
    /// mailbox.
    #[error("mailbox payload cannot be decrypted")]
    InvalidPayload,
}

/// Writes messages to consecutive mailboxes of the sequence keyed by a
/// secret.
///
/// A failed or cancelled write may have stored the message; retry it
/// unchanged. See [`Writer::resume`] for recovery after a restart.
pub struct Writer<D> {
    directory: D,
    keys: Keys,
    next_index: u64,
}

/// Reads messages from consecutive mailboxes of the sequence keyed by a
/// secret.
///
/// Payloads must remain available until read. A cancelled read consumes
/// nothing, so reading again returns the same message.
pub struct Reader<D> {
    directory: D,
    keys: Keys,
    next_index: u64,
}

impl<D: Directory> Writer<D> {
    /// Create a writer starting at mailbox 0 of the sequence keyed by
    /// `shared_secret`.
    pub fn new(directory: D, shared_secret: [u8; 32]) -> Self {
        Self::resume(directory, shared_secret, 0)
    }

    /// Resume at a saved [`next_index`](Self::next_index).
    ///
    /// Persist each pending message with its mailbox index before sending.
    /// Retain it until the updated index is durably saved after success. On
    /// restart, retry pending messages at their original indices before
    /// sending new ones.
    ///
    /// A mailbox keeps the first message stored in it, so a message written
    /// to an occupied mailbox is lost without an error (see [`Directory`]).
    /// Resuming at a stale index therefore loses new messages until the
    /// index passes the occupied mailboxes.
    pub fn resume(directory: D, shared_secret: [u8; 32], next_index: u64) -> Self {
        Self {
            directory,
            keys: Keys::derive(shared_secret),
            next_index,
        }
    }

    /// Index of the mailbox the next write uses.
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Store `message` in the next mailbox.
    pub async fn write(&mut self, message: &[u8]) -> Result<(), Error<D::Error>> {
        if message.len() > MAX_MESSAGE_BYTES {
            return Err(Error::MessageTooLarge(message.len()));
        }

        let mailbox = self.keys.mailbox(self.next_index);
        let payload = mailbox.seal(message);
        self.directory
            .put(mailbox.secret(), &payload)
            .await
            .map_err(Error::Directory)?;

        self.next_index = next(self.next_index);
        Ok(())
    }
}

impl<D: Directory> Reader<D> {
    /// Create a reader starting at mailbox 0 of the sequence keyed by
    /// `shared_secret`.
    pub fn new(directory: D, shared_secret: [u8; 32]) -> Self {
        Self::resume(directory, shared_secret, 0)
    }

    /// Resume at a saved [`next_index`](Self::next_index).
    pub fn resume(directory: D, shared_secret: [u8; 32], next_index: u64) -> Self {
        Self {
            directory,
            keys: Keys::derive(shared_secret),
            next_index,
        }
    }

    /// Index of the mailbox the next read uses.
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Wait for the message in the next mailbox.
    pub async fn read(&mut self) -> Result<Vec<u8>, Error<D::Error>> {
        loop {
            let mailbox = self.keys.mailbox(self.next_index);
            let stored = self
                .directory
                .get(mailbox.secret())
                .await
                .map_err(Error::Directory)?;
            if let Some(payload) = stored {
                let message = mailbox.open(&payload).ok_or(Error::InvalidPayload)?;
                self.next_index = next(self.next_index);
                return Ok(message);
            }

            // Give a concurrent writer a chance to fill the mailbox when a
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

impl<D> std::fmt::Debug for Writer<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Writer")
            .field("next_index", &self.next_index)
            .finish_non_exhaustive()
    }
}

impl<D> std::fmt::Debug for Reader<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reader")
            .field("next_index", &self.next_index)
            .finish_non_exhaustive()
    }
}

fn next(index: u64) -> u64 {
    index
        .checked_add(1)
        .expect("a sequence cannot hold 2^64 messages")
}

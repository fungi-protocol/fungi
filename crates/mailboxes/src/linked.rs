//! One writer and one reader sending messages through a sequence of
//! mailboxes.

use std::error::Error as StdError;

use crate::directory::Directory;
use crate::payload::{MessageTooLarge, Sequence};

/// Failure to write or read a message.
#[derive(Debug, thiserror::Error)]
pub enum Error<E: StdError + 'static> {
    #[error("directory: {0}")]
    Directory(#[source] E),
    /// The message does not fit in one payload.
    #[error("message has {0} bytes, more than MAX_MESSAGE_BYTES")]
    MessageTooLarge(usize),
    /// The payload at [`Reader::next_index`] does not decrypt for that
    /// mailbox.
    #[error("mailbox payload cannot be decrypted")]
    InvalidPayload,
    /// The next index is `u64::MAX`, after the last mailbox of the sequence.
    #[error("no mailbox is left in the sequence")]
    Exhausted,
}

/// Writes messages to consecutive mailboxes of the sequence keyed by a
/// secret.
///
/// A failed or cancelled write may have stored the message; retry it
/// unchanged. See [`Writer::resume`] for recovery after a restart.
pub struct Writer<D> {
    directory: D,
    sequence: Sequence,
    next_index: u64,
}

/// Reads messages from consecutive mailboxes of the sequence keyed by a
/// secret.
///
/// Payloads must remain available until read. A cancelled read consumes
/// nothing, so reading again returns the same message.
pub struct Reader<D> {
    directory: D,
    sequence: Sequence,
    next_index: u64,
}

impl<D: Directory> Writer<D> {
    /// Create a writer starting at mailbox 0.
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
    /// Resuming at a stale index discards new messages written to occupied
    /// mailboxes without an error; see [`Directory`].
    pub fn resume(directory: D, shared_secret: [u8; 32], next_index: u64) -> Self {
        Self {
            directory,
            sequence: Sequence::new(shared_secret),
            next_index,
        }
    }

    /// Index of the mailbox the next write uses.
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Store `message` in the next mailbox.
    pub async fn write(&mut self, message: &[u8]) -> Result<(), Error<D::Error>> {
        let mailbox = self.sequence.mailbox(self.next_index);
        let payload = mailbox
            .seal(message)
            .map_err(|MessageTooLarge(size)| Error::MessageTooLarge(size))?;
        let next_index = next(self.next_index)?;

        self.directory
            .put(mailbox.id(), &payload)
            .await
            .map_err(Error::Directory)?;

        self.next_index = next_index;
        Ok(())
    }
}

impl<D: Directory> Reader<D> {
    /// Create a reader starting at mailbox 0.
    pub fn new(directory: D, shared_secret: [u8; 32]) -> Self {
        Self::resume(directory, shared_secret, 0)
    }

    /// Resume at a saved [`next_index`](Self::next_index).
    pub fn resume(directory: D, shared_secret: [u8; 32], next_index: u64) -> Self {
        Self {
            directory,
            sequence: Sequence::new(shared_secret),
            next_index,
        }
    }

    /// Index of the mailbox the next read uses.
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Wait for the message in the next mailbox.
    ///
    /// Relies on [`Directory::get`] waiting before it returns `None`; a
    /// directory that returns `None` at once is polled in a loop.
    pub async fn read(&mut self) -> Result<Vec<u8>, Error<D::Error>> {
        let next_index = next(self.next_index)?;
        loop {
            let mailbox = self.sequence.mailbox(self.next_index);
            let stored = self
                .directory
                .get(mailbox.id())
                .await
                .map_err(Error::Directory)?;
            if let Some(payload) = stored {
                let message = mailbox.open(&payload).ok_or(Error::InvalidPayload)?;
                self.next_index = next_index;
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

fn next<E: StdError + 'static>(index: u64) -> Result<u64, Error<E>> {
    index.checked_add(1).ok_or(Error::Exhausted)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use proptest::prelude::*;

    use super::*;
    use crate::testing::{LostAcknowledgement, MemDirectory, expect_ready, expect_waiting};
    use crate::{MAX_MESSAGE_BYTES, PAYLOAD_BYTES, Sequences, derive_mailbox_id};

    #[tokio::test(start_paused = true)]
    async fn messages_arrive_in_order() {
        let directory = MemDirectory::default();
        let mut writer = Writer::new(&directory, [1; 32]);
        let mut reader = Reader::new(&directory, [1; 32]);

        for message in [b"first".as_slice(), b"second", b"third"] {
            writer.write(message).await.unwrap();
        }
        for message in [b"first".as_slice(), b"second", b"third"] {
            assert_eq!(expect_ready(reader.read()).await.unwrap(), message);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn each_direction_has_its_own_sequence() {
        let directory = MemDirectory::default();
        let Sequences {
            initiator_to_responder,
            responder_to_initiator,
        } = Sequences::derive(&[15; 32]);
        Writer::new(&directory, initiator_to_responder)
            .write(b"to responder")
            .await
            .unwrap();
        Writer::new(&directory, responder_to_initiator)
            .write(b"to initiator")
            .await
            .unwrap();

        let mut responder = Reader::new(&directory, initiator_to_responder);
        let mut initiator = Reader::new(&directory, responder_to_initiator);
        assert_eq!(
            expect_ready(responder.read()).await.unwrap(),
            b"to responder"
        );
        assert_eq!(
            expect_ready(initiator.read()).await.unwrap(),
            b"to initiator"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn writer_and_reader_resume_after_a_restart() {
        let directory = MemDirectory::default();
        let secret = [11; 32];
        let mut writer = Writer::new(&directory, secret);
        let mut reader = Reader::new(&directory, secret);
        assert_eq!(writer.next_index(), 0);
        writer.write(b"before").await.unwrap();
        assert_eq!(expect_ready(reader.read()).await.unwrap(), b"before");
        let (write_index, read_index) = (writer.next_index(), reader.next_index());
        assert_eq!((write_index, read_index), (1, 1));

        let mut writer = Writer::resume(&directory, secret, write_index);
        let mut reader = Reader::resume(&directory, secret, read_index);
        writer.write(b"after").await.unwrap();
        assert_eq!(expect_ready(reader.read()).await.unwrap(), b"after");
        assert_eq!(writer.next_index(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn identical_message_at_a_stale_index_is_merged() {
        let directory = MemDirectory::default();
        let secret = [12; 32];
        Writer::new(&directory, secret)
            .write(b"same")
            .await
            .unwrap();

        // A writer resumed from a stale index cannot tell a new identical
        // message from a resend of the stored one.
        let mut stale = Writer::resume(&directory, secret, 0);
        stale.write(b"same").await.unwrap();
        assert_eq!(stale.next_index(), 1);

        let mut reader = Reader::new(&directory, secret);
        assert_eq!(expect_ready(reader.read()).await.unwrap(), b"same");
        assert!(
            tokio::time::timeout(Duration::from_millis(125), reader.read())
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn read_waits_for_a_message() {
        let directory = MemDirectory::default();
        let mut writer = Writer::new(&directory, [2; 32]);
        let mut reader = Reader::new(&directory, [2; 32]);

        let read = reader.read();
        tokio::pin!(read);
        expect_waiting(read.as_mut());
        assert!(
            tokio::time::timeout(Duration::from_millis(125), &mut read)
                .await
                .is_err()
        );
        writer.write(b"only").await.unwrap();
        assert_eq!(expect_ready(read).await.unwrap(), b"only");
    }

    #[tokio::test(start_paused = true)]
    async fn different_secret_sees_no_messages() {
        let directory = MemDirectory::default();
        Writer::new(&directory, [3; 32])
            .write(b"private")
            .await
            .unwrap();

        let mut stranger = Reader::new(&directory, [4; 32]);
        let read = stranger.read();
        tokio::pin!(read);
        expect_waiting(read.as_mut());
        assert!(
            tokio::time::timeout(Duration::from_millis(125), read)
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn message_size_is_limited() {
        assert_eq!(MAX_MESSAGE_BYTES, 7126);
        let directory = MemDirectory::default();
        let mut writer = Writer::new(&directory, [7; 32]);
        let mut reader = Reader::new(&directory, [7; 32]);

        assert!(matches!(
            writer.write(&vec![0; MAX_MESSAGE_BYTES + 1]).await,
            Err(Error::MessageTooLarge(_))
        ));
        assert!(directory.payloads().is_empty());

        for message in [vec![0xab; MAX_MESSAGE_BYTES], Vec::new()] {
            writer.write(&message).await.unwrap();
            assert_eq!(expect_ready(reader.read()).await.unwrap(), message);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retrying_an_unacknowledged_write_does_not_duplicate() {
        let directory = LostAcknowledgement::default();
        let mut writer = Writer::new(&directory, [8; 32]);
        let mut reader = Reader::new(&directory, [8; 32]);

        directory.lose_next.store(true, Ordering::SeqCst);
        assert!(matches!(
            writer.write(b"once").await,
            Err(Error::Directory(_))
        ));
        writer.write(b"once").await.unwrap();
        writer.write(b"next").await.unwrap();

        assert_eq!(expect_ready(reader.read()).await.unwrap(), b"once");
        assert_eq!(expect_ready(reader.read()).await.unwrap(), b"next");
    }

    #[tokio::test(start_paused = true)]
    async fn a_different_message_after_an_unacknowledged_write_is_lost() {
        let directory = LostAcknowledgement::default();
        let mut writer = Writer::new(&directory, [9; 32]);
        let mut reader = Reader::new(&directory, [9; 32]);

        directory.lose_next.store(true, Ordering::SeqCst);
        assert!(writer.write(b"stored").await.is_err());
        writer.write(b"other").await.unwrap();

        assert_eq!(expect_ready(reader.read()).await.unwrap(), b"stored");
    }

    #[tokio::test(start_paused = true)]
    async fn a_write_to_an_occupied_mailbox_is_lost() {
        let directory = MemDirectory::default();
        let secret = [10; 32];
        directory.replace(derive_mailbox_id(&secret, 0), vec![0; PAYLOAD_BYTES]);
        Writer::new(&directory, secret)
            .write(b"mine")
            .await
            .unwrap();

        assert!(matches!(
            expect_ready(Reader::new(&directory, secret).read()).await,
            Err(Error::InvalidPayload)
        ));
    }

    #[tokio::test]
    async fn the_last_mailbox_is_before_u64_max() {
        let directory = MemDirectory::default();
        let secret = [14; 32];
        let mut writer = Writer::resume(&directory, secret, u64::MAX - 1);
        let mut reader = Reader::resume(&directory, secret, u64::MAX - 1);

        writer.write(b"last").await.unwrap();
        assert_eq!(expect_ready(reader.read()).await.unwrap(), b"last");

        assert!(matches!(
            writer.write(b"after").await,
            Err(Error::Exhausted)
        ));
        assert!(matches!(
            expect_ready(reader.read()).await,
            Err(Error::Exhausted)
        ));
        assert_eq!(directory.payloads().len(), 1);
        assert_eq!(writer.next_index(), u64::MAX);
        assert_eq!(reader.next_index(), u64::MAX);
    }

    #[test]
    fn debug_output_shows_position_without_secrets_or_directory() {
        let directory = MemDirectory::default();
        let writer = Writer::resume(&directory, [42; 32], 7);
        let reader = Reader::resume(&directory, [42; 32], 9);

        assert_eq!(format!("{writer:?}"), "Writer { next_index: 7, .. }");
        assert_eq!(format!("{reader:?}"), "Reader { next_index: 9, .. }");
        assert_eq!(
            format!("{:?}", derive_mailbox_id(&[42; 32], 0)),
            "MailboxId { .. }"
        );
    }

    #[tokio::test]
    async fn wrong_payload_sizes_do_not_advance_the_reader() {
        let directory = MemDirectory::default();
        let secret = [13; 32];
        let mailbox = derive_mailbox_id(&secret, 0);
        Writer::new(&directory, secret)
            .write(b"valid")
            .await
            .unwrap();
        let payload = directory.payloads().pop().unwrap();
        let mut reader = Reader::new(&directory, secret);

        for size in [0, PAYLOAD_BYTES - 1, PAYLOAD_BYTES + 1] {
            let mut malformed = payload.clone();
            malformed.resize(size, 0);
            directory.replace(mailbox, malformed);
            assert!(matches!(
                expect_ready(reader.read()).await,
                Err(Error::InvalidPayload)
            ));
            assert_eq!(reader.next_index(), 0);
        }

        directory.replace(mailbox, payload);
        assert_eq!(expect_ready(reader.read()).await.unwrap(), b"valid");
        assert_eq!(reader.next_index(), 1);
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]

        #[test]
        fn any_message_that_fits_round_trips(
            secret: [u8; 32],
            message in proptest::collection::vec(any::<u8>(), 0..=MAX_MESSAGE_BYTES),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let received = runtime.block_on(async {
                let directory = MemDirectory::default();
                Writer::new(&directory, secret).write(&message).await.unwrap();
                expect_ready(Reader::new(&directory, secret).read()).await.unwrap()
            });
            prop_assert_eq!(received, message);
        }
    }
}

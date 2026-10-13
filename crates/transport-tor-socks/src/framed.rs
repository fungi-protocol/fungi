//! Messages over a byte stream.
//!
//! Each message travels as one [`record`]: its type, its length, and its
//! payload. The type tells the receiver what the message is. The channel
//! yields every type; whoever dispatches them ignores an unknown odd type
//! and fails on an unknown even one.

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::mem;
use std::num::NonZeroUsize;

use fungi_transport::RecvChannel;
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::record::{self, Item};

/// How many bytes one read takes from the stream at most.
const READ_LEN: usize = 4096;

/// A message: its type and its payload.
pub type Message = (u64, Vec<u8>);

/// A message channel over a byte stream.
pub struct Channel<S> {
    stream: S,
    max_message_len: u32,
    records: record::Buffer,
    read: Box<[u8]>,
    /// What earlier reads brought and was not yet received: at most one read.
    received: VecDeque<Result<Message, record::Error>>,
    failed: bool,
}

/// Failure to receive a message. The channel yields no further messages.
#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    /// The stream ended between messages, or an earlier receive failed.
    #[error("channel closed")]
    Closed,
    /// The peer sent a malformed record, or the stream ended inside one.
    #[error(transparent)]
    Record(#[from] record::Error),
    /// The stream failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl<S> Channel<S> {
    /// Carry messages whose payload is at most `max_message_len` bytes on
    /// `stream`. That much is allocated once a message is split across reads.
    pub fn new(stream: S, max_message_len: u32) -> Self {
        Self {
            stream,
            max_message_len,
            records: record::Buffer::new(max_message_len),
            read: vec![0; READ_LEN].into_boxed_slice(),
            received: VecDeque::new(),
            failed: false,
        }
    }
}

/// Leaves out message bytes, so a channel can be logged. The stream is left
/// out too, as its own `Debug` may show the bytes it buffers.
impl<S> fmt::Debug for Channel<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Channel")
            .field("max_message_len", &self.max_message_len)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl<S: AsyncRead + Send + Unpin> RecvChannel<Message> for Channel<S> {
    type RecvError = RecvError;

    async fn recv(&mut self) -> Result<Message, RecvError> {
        if self.failed {
            return Err(RecvError::Closed);
        }

        let message = self.next_message().await;
        self.failed = message.is_err();

        message
    }
}

impl<S: AsyncRead + Unpin> Channel<S> {
    /// Reading from the stream is the only await, and what a read brings is
    /// parsed before the next, so a receive dropped by `select!` loses
    /// nothing.
    async fn next_message(&mut self) -> Result<Message, RecvError> {
        loop {
            if let Some(message) = self.received.pop_front() {
                return Ok(message?);
            }

            let read = match self.stream.read(&mut self.read).await {
                Ok(read) => read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            };
            // Reading no bytes is how a stream reports its end.
            if NonZeroUsize::new(read).is_none() {
                self.records.end()?;
                return Err(RecvError::Closed);
            }
            self.parse(read);
        }
    }

    /// Queue what the first `len` bytes of `read` complete.
    fn parse(&mut self, len: usize) {
        // Handed back at the end of the input.
        let buffer = mem::replace(&mut self.records, record::Buffer::new(0));
        let mut parsed = buffer.parse(&self.read[..len]);
        while let Some(item) = parsed.next() {
            match item {
                Ok(Item::Record(ty, payload)) => {
                    self.received.push_back(Ok((ty, payload.to_vec())))
                }
                Ok(Item::Suspend(buffer)) => self.records = buffer,
                Err(error) => self.received.push_back(Err(error)),
            }
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use fungi_transport::RecvChannel;
    use proptest::prelude::*;
    use tokio::io::{AsyncRead, AsyncWriteExt, DuplexStream, ReadBuf};

    use super::{Channel, Message, READ_LEN, RecvError};
    use crate::record;

    const MAX: u32 = 16;

    fn frame(ty: u64, payload: &[u8]) -> Vec<u8> {
        record::encode([(ty, payload)])
    }

    fn message(ty: u64, payload: &[u8]) -> Message {
        (ty, payload.to_vec())
    }

    /// A channel reading what the test writes to the returned stream.
    fn reading(max_message_len: u32) -> (DuplexStream, Channel<DuplexStream>) {
        let (peer, stream) = tokio::io::duplex(64);
        (peer, Channel::new(stream, max_message_len))
    }

    /// A channel reading `bytes` followed by the end of the stream.
    async fn reading_all(bytes: &[u8]) -> Channel<DuplexStream> {
        let (mut peer, stream) = tokio::io::duplex(bytes.len().max(1));
        peer.write_all(bytes).await.unwrap();
        Channel::new(stream, MAX)
    }

    #[tokio::test]
    async fn receives_messages_in_stream_order() {
        let bytes = [frame(2, b"first"), frame(1, b""), frame(253, b"second")].concat();
        let mut channel = reading_all(&bytes).await;

        assert_eq!(channel.recv().await.unwrap(), message(2, b"first"));
        assert_eq!(channel.recv().await.unwrap(), message(1, b""));
        assert_eq!(channel.recv().await.unwrap(), message(253, b"second"));
    }

    #[tokio::test]
    async fn refuses_a_non_minimal_length() {
        let mut channel = reading_all(&[0x02, 0xfd, 0x00, 0x01, b'x']).await;

        assert!(matches!(
            channel.recv().await,
            Err(RecvError::Record(record::Error::NonMinimal))
        ));
    }

    #[tokio::test]
    async fn stream_end_between_messages_closes() {
        let mut channel = reading_all(&frame(2, b"last")).await;

        assert_eq!(channel.recv().await.unwrap(), message(2, b"last"));
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    #[tokio::test]
    async fn stream_end_inside_a_message_truncates() {
        let mut channel = reading_all(&frame(2, b"cut")[..4]).await;

        assert!(matches!(
            channel.recv().await,
            Err(RecvError::Record(record::Error::Truncated))
        ));
    }

    /// The announced length is refused before any of the payload is read, so a
    /// peer cannot make the receiver allocate what it announces. The stream
    /// stays open, so a receive that waited for the payload would time out.
    #[tokio::test(start_paused = true)]
    async fn refuses_a_length_above_the_limit() {
        let (mut peer, mut channel) = reading(MAX);
        peer.write_all(&[0x02, 17]).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(1), channel.recv()).await;

        assert!(matches!(
            received,
            Ok(Err(RecvError::Record(record::Error::TooLarge {
                len: 17,
                max: MAX
            })))
        ));
    }

    /// After a failure the position in the stream is unknown, so bytes that
    /// follow must not be read as a message.
    #[tokio::test]
    async fn yields_nothing_after_a_failure() {
        let bytes = [&[0x02, 17][..], &frame(2, b"next")].concat();
        let mut channel = reading_all(&bytes).await;

        assert!(matches!(channel.recv().await, Err(RecvError::Record(_))));
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    /// One read can bring more than a message; the rest waits for the next
    /// receive, even past the bytes one read takes.
    #[tokio::test]
    async fn keeps_the_rest_of_a_read_for_later_messages() {
        let payload = [7; MAX as usize];
        let count = READ_LEN / payload.len() + 2;
        let bytes = frame(2, &payload).repeat(count);
        let mut channel = reading_all(&bytes).await;

        for _ in 0..count {
            assert_eq!(channel.recv().await.unwrap(), message(2, &payload));
        }
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    /// A receive dropped while waiting, as in a `select!` arm, keeps the bytes it
    /// already took from the stream.
    #[tokio::test(start_paused = true)]
    async fn canceled_receive_resumes_inside_the_message() {
        let (mut peer, mut channel) = reading(MAX);

        for byte in frame(2, b"whole") {
            let waiting = tokio::time::timeout(Duration::from_millis(1), channel.recv());
            assert!(waiting.await.is_err());

            peer.write_all(&[byte]).await.unwrap();
        }
        drop(peer);

        assert_eq!(channel.recv().await.unwrap(), message(2, b"whole"));
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    /// Neither the bytes the channel holds nor those still in the stream show.
    #[tokio::test(start_paused = true)]
    async fn debug_output_omits_message_bytes() {
        let (mut peer, mut channel) = reading(MAX);
        let secret = frame(2, b"secret");
        peer.write_all(&secret[..5]).await.unwrap();

        let waiting = tokio::time::timeout(Duration::from_millis(1), channel.recv());
        assert!(waiting.await.is_err());
        peer.write_all(&secret[5..]).await.unwrap();

        assert_eq!(
            format!("{channel:?}"),
            "Channel { max_message_len: 16, failed: false, .. }"
        );
    }

    /// Fails the first read, then yields its bytes and ends. Ending keeps a
    /// channel that wrongly retries the failure from waiting forever.
    struct FailsOnce {
        error: Option<io::ErrorKind>,
        bytes: &'static [u8],
    }

    impl AsyncRead for FailsOnce {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if let Some(kind) = self.error.take() {
                return Poll::Ready(Err(kind.into()));
            }
            buf.put_slice(std::mem::take(&mut self.bytes));
            Poll::Ready(Ok(()))
        }
    }

    fn failing_once(kind: io::ErrorKind, bytes: &'static [u8]) -> Channel<FailsOnce> {
        let stream = FailsOnce {
            error: Some(kind),
            bytes,
        };
        Channel::new(stream, MAX)
    }

    #[tokio::test]
    async fn reports_a_stream_failure() {
        let mut channel = failing_once(io::ErrorKind::ConnectionReset, &[]);

        let Err(RecvError::Io(error)) = channel.recv().await else {
            panic!("expected the stream failure");
        };
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    /// An interrupted read asks to be retried, so it must not end the channel.
    #[tokio::test]
    async fn retries_an_interrupted_read() {
        let mut channel = failing_once(io::ErrorKind::Interrupted, &[2, 1, b'x']);

        assert_eq!(channel.recv().await.unwrap(), message(2, b"x"));
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    fn within_limit() -> impl Strategy<Value = Vec<Message>> {
        let payload = prop::collection::vec(any::<u8>(), 0..=MAX as usize);
        prop::collection::vec((any::<u64>(), payload), 0..8)
    }

    proptest! {
        #[test]
        fn receives_any_messages_within_the_limit(messages in within_limit()) {
            block_on(async {
                let bytes = record::encode(messages.iter().map(|(ty, payload)| (*ty, &payload[..])));
                let (mut peer, mut channel) = reading(MAX);
                let writing = async move { peer.write_all(&bytes).await.unwrap() };
                let receiving = async {
                    for message in &messages {
                        assert_eq!(&channel.recv().await.unwrap(), message);
                    }
                    assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
                };

                tokio::join!(writing, receiving);
            });
        }

        #[test]
        fn arbitrary_bytes_end_in_a_failure(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
            block_on(async {
                let mut channel = reading_all(&bytes).await;

                // Every record takes at least a type and a length from the stream.
                for _ in 0..=bytes.len() / 2 {
                    let Ok((_, payload)) = channel.recv().await else {
                        break;
                    };
                    assert!(payload.len() <= MAX as usize);
                }
                assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
            });
        }
    }
}

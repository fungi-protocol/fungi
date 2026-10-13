//! Messages over a byte stream.
//!
//! Each message travels as a 4-byte big-endian length followed by that many
//! bytes.

use std::fmt;
use std::io;
use std::mem;
use std::num::NonZeroUsize;

use fungi_transport::RecvChannel;
use tokio::io::{AsyncRead, AsyncReadExt};

/// A message channel over a byte stream.
pub struct Channel<S> {
    stream: S,
    max_message_len: u32,
    incoming: Incoming,
    failed: bool,
}

/// Failure to receive a message. The channel yields no further messages.
#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    /// The stream ended between messages, or an earlier receive failed.
    #[error("channel closed")]
    Closed,
    /// The peer announced a message above the limit.
    #[error("peer announced {len} bytes, above the limit of {max}")]
    TooLarge {
        /// Announced length in bytes.
        len: u32,
        /// Largest accepted length in bytes.
        max: u32,
    },
    /// The stream ended inside a message.
    #[error("stream ended inside a message")]
    Truncated,
    /// The stream failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl<S> Channel<S> {
    /// Delimit messages of at most `max_message_len` bytes on `stream`.
    pub fn new(stream: S, max_message_len: u32) -> Self {
        Self {
            stream,
            max_message_len,
            incoming: Incoming::default(),
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

impl<S: AsyncRead + Send + Unpin> RecvChannel for Channel<S> {
    type RecvError = RecvError;

    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        if self.failed {
            return Err(RecvError::Closed);
        }

        let message = self
            .incoming
            .read(&mut self.stream, self.max_message_len)
            .await;
        self.failed = message.is_err();

        message
    }
}

/// The part of the next message already taken from the stream.
///
/// Kept outside the receive future, so dropping that future loses nothing.
#[derive(Default)]
struct Incoming {
    /// The length that precedes the message, filled as its bytes arrive.
    len: [u8; 4],
    /// The number of bytes of `len` that have arrived. A length split across
    /// reads, or a receive dropped midway, resumes from here.
    len_filled: usize,
    /// The bytes of the message that have arrived.
    payload: Vec<u8>,
}

impl Incoming {
    async fn read<S: AsyncRead + Unpin>(
        &mut self,
        stream: &mut S,
        max: u32,
    ) -> Result<Vec<u8>, RecvError> {
        while self.len_filled < self.len.len() {
            let read = stream.read(&mut self.len[self.len_filled..]).await?;
            // Reading no bytes is how a stream reports its end.
            let Some(read) = NonZeroUsize::new(read) else {
                return Err(if self.len_filled == 0 {
                    RecvError::Closed
                } else {
                    RecvError::Truncated
                });
            };
            self.len_filled += read.get();
        }

        let len = u32::from_be_bytes(self.len);
        if len > max {
            return Err(RecvError::TooLarge { len, max });
        }

        // The payload grows as bytes arrive, never ahead of them.
        let len = len as usize;
        while self.payload.len() < len {
            let missing = (len - self.payload.len()) as u64;
            let mut rest = (&mut *stream).take(missing);
            let read = rest.read_buf(&mut self.payload).await?;
            if NonZeroUsize::new(read).is_none() {
                return Err(RecvError::Truncated);
            }
        }

        // The next message starts with a new length.
        Ok(mem::take(self).payload)
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

    use super::{Channel, RecvError};

    const MAX: u32 = 16;

    fn frame(payload: &[u8]) -> Vec<u8> {
        let len = u32::try_from(payload.len()).unwrap();
        [&len.to_be_bytes(), payload].concat()
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
        let bytes = [frame(b"first"), frame(b""), frame(b"second")].concat();
        let mut channel = reading_all(&bytes).await;

        assert_eq!(channel.recv().await.unwrap(), b"first");
        assert_eq!(channel.recv().await.unwrap(), b"");
        assert_eq!(channel.recv().await.unwrap(), b"second");
    }

    #[tokio::test]
    async fn stream_end_between_messages_closes() {
        let mut channel = reading_all(&frame(b"last")).await;

        assert_eq!(channel.recv().await.unwrap(), b"last");
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    #[tokio::test]
    async fn stream_end_inside_the_length_truncates() {
        let mut channel = reading_all(&frame(b"cut")[..3]).await;

        assert!(matches!(channel.recv().await, Err(RecvError::Truncated)));
    }

    #[tokio::test]
    async fn stream_end_before_the_payload_truncates() {
        let mut channel = reading_all(&frame(b"cut")[..4]).await;

        assert!(matches!(channel.recv().await, Err(RecvError::Truncated)));
    }

    #[tokio::test]
    async fn stream_end_inside_the_payload_truncates() {
        let mut channel = reading_all(&frame(b"cut")[..6]).await;

        assert!(matches!(channel.recv().await, Err(RecvError::Truncated)));
    }

    #[tokio::test]
    async fn accepts_a_message_at_the_limit() {
        let payload = [7; MAX as usize];
        let mut channel = reading_all(&frame(&payload)).await;

        assert_eq!(channel.recv().await.unwrap(), payload);
    }

    /// The announced length is refused before any of the payload is read, so a
    /// peer cannot make the receiver allocate what it announces.
    #[tokio::test]
    async fn refuses_a_length_above_the_limit() {
        let mut channel = reading_all(&(MAX + 1).to_be_bytes()).await;

        assert!(matches!(
            channel.recv().await,
            Err(RecvError::TooLarge { len: 17, max: MAX })
        ));
    }

    /// After a failure the position in the stream is unknown, so bytes that
    /// follow must not be read as a message.
    #[tokio::test]
    async fn yields_nothing_after_a_failure() {
        let bytes = [(MAX + 1).to_be_bytes().to_vec(), frame(b"next")].concat();
        let mut channel = reading_all(&bytes).await;

        assert!(matches!(
            channel.recv().await,
            Err(RecvError::TooLarge { .. })
        ));
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    /// A receive dropped while waiting, as in a `select!` arm, keeps the bytes it
    /// already took from the stream.
    #[tokio::test(start_paused = true)]
    async fn canceled_receive_resumes_inside_the_message() {
        let (mut peer, mut channel) = reading(MAX);

        for byte in frame(b"whole") {
            let waiting = tokio::time::timeout(Duration::from_millis(1), channel.recv());
            assert!(waiting.await.is_err());

            peer.write_all(&[byte]).await.unwrap();
        }
        drop(peer);

        assert_eq!(channel.recv().await.unwrap(), b"whole");
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    /// Neither the bytes the channel holds nor those still in the stream show.
    #[tokio::test(start_paused = true)]
    async fn debug_output_omits_message_bytes() {
        let (mut peer, mut channel) = reading(MAX);
        let secret = frame(b"secret");
        peer.write_all(&secret[..7]).await.unwrap();

        let waiting = tokio::time::timeout(Duration::from_millis(1), channel.recv());
        assert!(waiting.await.is_err());
        peer.write_all(&secret[7..]).await.unwrap();

        assert_eq!(
            format!("{channel:?}"),
            "Channel { max_message_len: 16, failed: false, .. }"
        );
    }

    struct Failing;

    impl AsyncRead for Failing {
        fn poll_read(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            _: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()))
        }
    }

    #[tokio::test]
    async fn reports_a_stream_failure() {
        let mut channel = Channel::new(Failing, MAX);

        let Err(RecvError::Io(error)) = channel.recv().await else {
            panic!("expected the stream failure");
        };
        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    proptest! {
        #[test]
        fn receives_any_messages_within_the_limit(
            messages in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=MAX as usize), 0..8),
        ) {
            block_on(async {
                let bytes: Vec<u8> = messages.iter().flat_map(|message| frame(message)).collect();
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

                // Every message takes at least its length from the stream.
                for _ in 0..=bytes.len() / 4 {
                    let Ok(message) = channel.recv().await else {
                        break;
                    };
                    assert!(message.len() <= MAX as usize);
                }
                assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
            });
        }
    }
}

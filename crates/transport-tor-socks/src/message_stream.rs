//! Parsing of bounded length messages encoded as [`tlv`] records from a byte stream.
//!
//! A [`Reader`] receives from the read half of a stream and a [`Writer`] sends
//! to the write half. A [`Channel`] joins both over one stream. Messages of any
//! type are returned. The caller applies BOLT #1's rule for unknown types:
//! ignore odd ones, fail on even ones.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::fmt;
use std::io::{self, Cursor};
use std::marker::PhantomData;
use std::mem;
use std::num::NonZeroUsize;

use fungi_transport::reassembly::{Item, LendingIterator, State};
use fungi_transport::tlv::{self, TypedSlice};
use fungi_transport::{Bidirectional, Privacy, RecvChannel, SendChannel, Unspecified};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadHalf, WriteHalf};

/// The maximum number of bytes requested in one read.
const READ_LEN: usize = 1 << 16;

/// On the wire, messages consist of a type and a payload.
pub type Message = TypedSlice<Vec<u8>>;

/// Receives messages from a byte stream.
///
/// A message payload is at most `N` bytes long, and `N` must be at least 18,
/// the length of the longest TLV header. The decoding function receives
/// borrowed records and produces owned messages before they enter the queue.
///
/// `recv` is cancel safe. The read is its only await, and its bytes are parsed
/// before the next read. So dropping the future, as `select!` does, loses no
/// bytes.
pub struct Reader<S, const N: usize, T = Message, E = Infallible> {
    decode: fn(TypedSlice<&[u8]>) -> Result<T, E>,
    stream: S,
    /// The parse state between reads.
    state: State<N>,
    read: Box<[u8]>,
    /// Parsed from the last read, not yet returned by `recv`. Empty before
    /// each read.
    received: VecDeque<Result<T, RecvError<E>>>,
    /// Set once `recv` fails.
    failed: bool,
}

/// Why `recv` failed. Every later `recv` fails with [`RecvError::Closed`].
#[derive(Debug, thiserror::Error)]
pub enum RecvError<E = Infallible> {
    /// A complete record could not be decoded as a message.
    #[error(transparent)]
    Decode(E),
    /// The stream ended between messages, or an earlier `recv` failed. On a
    /// [`Channel`], so did an earlier `send`, unless it failed with
    /// [`SendError::TooLarge`].
    #[error("channel closed")]
    Closed,
    /// The peer sent an invalid record, or the stream ended inside one.
    #[error(transparent)]
    Record(#[from] tlv::Error),
    /// Reading from the stream failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl<S, const N: usize, T, E> Reader<S, N, T, E> {
    /// Wraps `stream`, decoding received records with `decode`.
    pub fn new(stream: S, decode: fn(TypedSlice<&[u8]>) -> Result<T, E>) -> Self {
        Self {
            decode,
            stream,
            state: State::default(),
            read: vec![0; READ_LEN].into_boxed_slice(),
            received: VecDeque::new(),
            failed: false,
        }
    }
}

/// Shows no message bytes, so a channel can be logged.
///
/// Leaves out the stream, because its own `Debug` may show buffered bytes.
impl<S, const N: usize, T, E> fmt::Debug for Reader<S, N, T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("max_message_len", &N)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl<S, const N: usize, T, E> RecvChannel<T> for Reader<S, N, T, E>
where
    S: AsyncRead + Send + Unpin,
    T: Send,
    E: std::error::Error + Send + Sync + 'static,
{
    type RecvError = RecvError<E>;

    async fn recv(&mut self) -> Result<T, RecvError<E>> {
        if self.failed {
            return Err(RecvError::Closed);
        }

        let message = self.read_message().await;
        self.failed = message.is_err();

        message
    }
}

impl<S: AsyncRead + Unpin, const N: usize, T, E> Reader<S, N, T, E> {
    async fn read_message(&mut self) -> Result<T, RecvError<E>> {
        loop {
            if let Some(message) = self.received.pop_front() {
                return message;
            }

            let read = match self.stream.read(&mut self.read).await {
                Ok(read) => read,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            };

            // A read of zero bytes means the stream ended. The check avoids
            // `read == 0`, because its `!=` mutant loops forever instead of
            // failing a test.
            if NonZeroUsize::new(read).is_none() {
                self.state.end()?;
                return Err(RecvError::Closed);
            }

            self.parse(read);
        }
    }

    /// Parses the first `len` bytes of `read` into `received`.
    fn parse(&mut self, len: usize) {
        let mut records = mem::take(&mut self.state).resume(&self.read[..len]);

        while let Some(item) = records.next() {
            match item {
                Ok(Item::Record(record)) => {
                    let message = (self.decode)(record).map_err(RecvError::Decode);
                    let failed = message.is_err();
                    self.received.push_back(message);

                    if failed {
                        break;
                    }
                }

                Ok(Item::Incomplete(state)) => self.state = state,

                Err(error) => {
                    self.received.push_back(Err(RecvError::Record(error)));

                    break;
                }
            }
        }
    }
}

/// Sends messages to a byte stream.
///
/// A message payload is at most `N` bytes long. Each message is written as one
/// [`tlv`] record. `P` is the privacy the writer reports.
///
/// Dropping a `send` future, as `select!` does, may still deliver its message,
/// whole and ahead of the next one, so the stream never carries part of a
/// record followed by the start of another.
pub struct Writer<S, const N: usize, P = Unspecified> {
    stream: S,
    /// The rest of a record that a dropped `send` left unwritten. It is
    /// written before the next record.
    outgoing: Cursor<Vec<u8>>,
    /// Set once `send` fails with an error other than [`SendError::TooLarge`].
    failed: bool,
    privacy: PhantomData<fn() -> P>,
}

/// Why `send` failed.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    /// The payload is longer than `max`. Nothing was written, and the writer
    /// stays open.
    #[error("payload exceeds {max} bytes")]
    TooLarge {
        /// The limit, `N`, in bytes.
        max: usize,
    },
    /// An earlier `send` failed with an error other than
    /// [`SendError::TooLarge`]. On a [`Channel`], so did an earlier `recv`.
    #[error("channel closed")]
    Closed,
    /// Writing to the stream failed. Every later `send` fails with
    /// [`SendError::Closed`].
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl<S, const N: usize> Writer<S, N, Unspecified> {
    /// Wraps `stream`.
    ///
    /// The privacy is [`Unspecified`], because it depends on where the stream
    /// comes from.
    pub fn new(stream: S) -> Self {
        Self::tagged(stream)
    }
}

impl<S, const N: usize, P> Writer<S, N, P> {
    fn tagged(stream: S) -> Self {
        Self {
            stream,
            outgoing: Cursor::default(),
            failed: false,
            privacy: PhantomData,
        }
    }
}

/// Shows no message bytes, so a writer can be logged.
///
/// Leaves out the stream, because its own `Debug` may show buffered bytes.
impl<S, const N: usize, P> fmt::Debug for Writer<S, N, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Writer")
            .field("max_message_len", &N)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl<S, const N: usize, P> SendChannel<Message> for Writer<S, N, P>
where
    S: AsyncWrite + Send + Unpin,
    P: Privacy,
{
    type Privacy = P;
    type SendError = SendError;

    /// Writes `message` as one [`tlv`] record.
    async fn send(&mut self, message: Message) -> Result<(), SendError> {
        if self.failed {
            return Err(SendError::Closed);
        }

        if message.value.len() > N {
            return Err(SendError::TooLarge { max: N });
        }

        let mut record = Vec::new();
        message.encode(&mut record).expect("a Vec takes every byte");

        let written = self.write(record).await;
        self.failed = written.is_err();

        Ok(written?)
    }
}

impl<S: AsyncWrite + Unpin, const N: usize, P> Writer<S, N, P> {
    /// Writes the rest of `outgoing`, then `record`.
    ///
    /// `outgoing` advances with every accepted byte and outlives the send
    /// future, so the stream never carries part of a record followed by the
    /// start of another.
    async fn write(&mut self, record: Vec<u8>) -> io::Result<()> {
        self.stream.write_all_buf(&mut self.outgoing).await?;

        self.outgoing = Cursor::new(record);
        self.stream.write_all_buf(&mut self.outgoing).await?;

        loop {
            match self.stream.flush().await {
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                flushed => return flushed,
            }
        }
    }
}

/// Sends and receives messages over one byte stream, through a [`Reader`] on
/// its read half and a [`Writer`] on its write half.
///
/// A failure in either direction ends both, because a failed write may have
/// left part of a message on the stream. [`SendError::TooLarge`] writes
/// nothing, so it leaves the channel open.
pub struct Channel<S, const N: usize, P = Unspecified, T = Message, E = Infallible> {
    reader: Reader<ReadHalf<S>, N, T, E>,
    writer: Writer<WriteHalf<S>, N, P>,
}

impl<S, const N: usize, T, E> Channel<S, N, Unspecified, T, E>
where
    S: AsyncRead + AsyncWrite,
{
    /// Wraps `stream`, decoding received records with `decode`.
    ///
    /// The privacy is [`Unspecified`], because it depends on where the stream
    /// comes from.
    pub fn new(stream: S, decode: fn(TypedSlice<&[u8]>) -> Result<T, E>) -> Self {
        let (read, write) = tokio::io::split(stream);

        Self {
            reader: Reader::new(read, decode),
            writer: Writer::tagged(write),
        }
    }
}

/// Shows no message bytes, so a channel can be logged.
impl<S, const N: usize, P, T, E> fmt::Debug for Channel<S, N, P, T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Channel")
            .field("reader", &self.reader)
            .field("writer", &self.writer)
            .finish()
    }
}

impl<S, const N: usize, P, T, E> SendChannel<Message> for Channel<S, N, P, T, E>
where
    S: AsyncWrite + Send,
    P: Privacy,
    T: Send,
    E: Send,
{
    type Privacy = P;
    type SendError = SendError;

    /// Writes `message` as one [`tlv`] record, as [`Writer`] does.
    async fn send(&mut self, message: Message) -> Result<(), SendError> {
        if self.reader.failed {
            return Err(SendError::Closed);
        }

        self.writer.send(message).await
    }
}

impl<S, const N: usize, P, T, E> RecvChannel<T> for Channel<S, N, P, T, E>
where
    S: AsyncRead + Send,
    P: Send,
    T: Send,
    E: std::error::Error + Send + Sync + 'static,
{
    type RecvError = RecvError<E>;

    async fn recv(&mut self) -> Result<T, RecvError<E>> {
        if self.writer.failed {
            return Err(RecvError::Closed);
        }

        self.reader.recv().await
    }
}

impl<S, const N: usize, P, T, E> Bidirectional<T, Message> for Channel<S, N, P, T, E>
where
    S: AsyncRead + AsyncWrite + Send,
    P: Privacy,
    T: Send,
    E: std::error::Error + Send + Sync + 'static,
{
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use std::convert::Infallible;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use fungi_transport::tlv::{self, TypedSlice};
    use fungi_transport::{Bidirectional, RecvChannel, SendChannel};
    use proptest::prelude::*;
    use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

    use super::{Channel, Message, READ_LEN, Reader, RecvError, SendError, Writer};

    const MAX: usize = 32;

    fn encode_frame(ty: u64, payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        TypedSlice { ty, value: payload }.encode(&mut out).unwrap();
        out
    }

    fn make_message(ty: u64, payload: &[u8]) -> Message {
        TypedSlice {
            ty,
            value: payload.to_vec(),
        }
    }

    fn to_message(record: TypedSlice<&[u8]>) -> Result<Message, Infallible> {
        Ok(make_message(record.ty, record.value))
    }

    /// Returns a channel and the other end of its stream, which the test
    /// writes to.
    fn open() -> (DuplexStream, Reader<DuplexStream, MAX>) {
        let (peer, stream) = tokio::io::duplex(64);
        (peer, Reader::new(stream, to_message))
    }

    /// A channel reading `bytes` followed by the end of the stream.
    async fn open_with(bytes: &[u8]) -> Reader<DuplexStream, MAX> {
        let (mut peer, stream) = tokio::io::duplex(bytes.len().max(1));
        peer.write_all(bytes).await.unwrap();

        Reader::new(stream, to_message)
    }

    #[tokio::test]
    async fn receives_messages_in_stream_order() {
        let bytes = [
            encode_frame(2, b"first"),
            encode_frame(1, b""),
            encode_frame(253, b"second"),
        ]
        .concat();
        let mut channel = open_with(&bytes).await;

        assert_eq!(channel.recv().await.unwrap(), make_message(2, b"first"));
        assert_eq!(channel.recv().await.unwrap(), make_message(1, b""));
        assert_eq!(channel.recv().await.unwrap(), make_message(253, b"second"));
    }

    #[tokio::test]
    async fn refuses_a_non_minimal_length() {
        let mut channel = open_with(&[0x02, 0xfd, 0x00, 0x01, b'x']).await;

        assert!(matches!(
            channel.recv().await,
            Err(RecvError::Record(tlv::Error::NonMinimal))
        ));
    }

    #[tokio::test]
    async fn stream_end_between_messages_closes() {
        let mut channel = open_with(&encode_frame(2, b"last")).await;

        assert_eq!(channel.recv().await.unwrap(), make_message(2, b"last"));
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    #[tokio::test]
    async fn stream_end_inside_a_message_truncates() {
        let mut channel = open_with(&encode_frame(2, b"cut")[..4]).await;

        assert!(matches!(
            channel.recv().await,
            Err(RecvError::Record(tlv::Error::Truncated))
        ));
    }

    /// A length above the limit fails as soon as the header is read, so a peer
    /// cannot make the receiver wait for, or hold, a payload that long. The
    /// stream stays open, so a `recv` waiting for the payload would time out.
    #[tokio::test(start_paused = true)]
    async fn refuses_a_length_above_the_limit() {
        let (mut peer, mut channel) = open();
        peer.write_all(&[0x02, MAX as u8 + 1]).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(1), channel.recv()).await;

        assert!(matches!(
            received,
            Ok(Err(RecvError::Record(tlv::Error::TooLarge {
                len: 33,
                max: MAX
            })))
        ));
    }

    /// After an error the position in the stream is unknown, so the bytes that
    /// follow must not be parsed as a message.
    #[tokio::test]
    async fn yields_nothing_after_a_failure() {
        let bytes = [&[0x02, MAX as u8 + 1][..], &encode_frame(2, b"next")].concat();
        let mut channel = open_with(&bytes).await;

        assert!(matches!(channel.recv().await, Err(RecvError::Record(_))));
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    /// A header cut off at the end of one read is checked once the next read
    /// completes it.
    #[tokio::test(start_paused = true)]
    async fn refuses_a_non_minimal_length_split_across_reads() {
        let (mut peer, mut channel) = open();
        peer.write_all(&[0x02, 0xfd]).await.unwrap();

        let waiting = tokio::time::timeout(Duration::from_millis(1), channel.recv());
        assert!(waiting.await.is_err());

        peer.write_all(&[0x00, 0x01, b'x']).await.unwrap();

        let received = tokio::time::timeout(Duration::from_secs(1), channel.recv()).await;

        assert!(matches!(
            received,
            Ok(Err(RecvError::Record(tlv::Error::NonMinimal)))
        ));
    }

    /// One read can hold several messages. Each later `recv` returns the next
    /// one, and messages past the first `READ_LEN` bytes arrive too.
    #[tokio::test]
    async fn keeps_the_rest_of_a_read_for_later_messages() {
        let payload = [7; MAX - 2];
        let count = READ_LEN / payload.len() + 2;
        let bytes = encode_frame(2, &payload).repeat(count);
        let mut channel = open_with(&bytes).await;

        for _ in 0..count {
            assert_eq!(channel.recv().await.unwrap(), make_message(2, &payload));
        }

        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    /// Dropping a waiting `recv`, as `select!` does, loses none of the bytes
    /// already read from the stream.
    #[tokio::test(start_paused = true)]
    async fn canceled_receive_resumes_inside_the_message() {
        let (mut peer, mut channel) = open();

        for byte in encode_frame(2, b"whole") {
            let waiting = tokio::time::timeout(Duration::from_millis(1), channel.recv());
            assert!(waiting.await.is_err());

            peer.write_all(&[byte]).await.unwrap();
        }

        drop(peer);

        assert_eq!(channel.recv().await.unwrap(), make_message(2, b"whole"));
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    /// The `Debug` output shows no message bytes. This holds for bytes the
    /// channel has read and for bytes still waiting in the stream.
    #[tokio::test(start_paused = true)]
    async fn debug_output_omits_message_bytes() {
        let (mut peer, mut channel) = open();
        let secret = encode_frame(2, b"secret");
        peer.write_all(&secret[..5]).await.unwrap();

        let waiting = tokio::time::timeout(Duration::from_millis(1), channel.recv());
        assert!(waiting.await.is_err());

        peer.write_all(&secret[5..]).await.unwrap();

        assert_eq!(
            format!("{channel:?}"),
            "Reader { max_message_len: 32, failed: false, .. }"
        );
    }

    /// A stream whose first read or write fails. Later reads return `bytes`
    /// and then report the end, and later writes take every byte, so a channel
    /// that wrongly retries after the failure ends instead of waiting forever.
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

    impl AsyncWrite for FailsOnce {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if let Some(kind) = self.error.take() {
                return Poll::Ready(Err(kind.into()));
            }

            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn failing_once(kind: io::ErrorKind, bytes: &'static [u8]) -> FailsOnce {
        FailsOnce {
            error: Some(kind),
            bytes,
        }
    }

    fn open_failing_once(kind: io::ErrorKind, bytes: &'static [u8]) -> Reader<FailsOnce, MAX> {
        Reader::new(failing_once(kind, bytes), to_message)
    }

    #[tokio::test]
    async fn reports_a_stream_failure() {
        let mut channel = open_failing_once(io::ErrorKind::ConnectionReset, &[]);

        let Err(RecvError::Io(error)) = channel.recv().await else {
            panic!("expected the stream failure");
        };

        assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);

        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    /// `Interrupted` means the read should be retried, so it does not fail the
    /// channel.
    #[tokio::test]
    async fn retries_an_interrupted_read() {
        let mut channel = open_failing_once(io::ErrorKind::Interrupted, &[2, 1, b'x']);

        assert_eq!(channel.recv().await.unwrap(), make_message(2, b"x"));
    }

    /// Returns a writer over a duplex pipe of `pipe_len` bytes, and the pipe's
    /// other end.
    fn writing(pipe_len: usize) -> (DuplexStream, Writer<DuplexStream, MAX>) {
        let (peer, stream) = tokio::io::duplex(pipe_len);
        (peer, Writer::new(stream))
    }

    /// Reads `peer` to the end and returns the bytes.
    async fn written(mut peer: DuplexStream) -> Vec<u8> {
        let mut bytes = Vec::new();
        peer.read_to_end(&mut bytes).await.unwrap();
        bytes
    }

    #[tokio::test]
    async fn writes_each_message_as_a_record() {
        let (peer, mut writer) = writing(64);

        writer.send(make_message(2, b"first")).await.unwrap();
        writer.send(make_message(253, b"")).await.unwrap();
        drop(writer);

        assert_eq!(written(peer).await, b"\x02\x05first\xfd\x00\xfd\x00");
    }

    #[tokio::test]
    async fn sends_a_payload_at_the_limit() {
        let (peer, mut writer) = writing(64);
        let payload = [7; MAX];

        writer.send(make_message(2, &payload)).await.unwrap();
        drop(writer);

        assert_eq!(written(peer).await, encode_frame(2, &payload));
    }

    #[tokio::test]
    async fn refuses_a_payload_above_the_limit_and_stays_usable() {
        let (peer, mut writer) = writing(64);

        let refused = writer.send(make_message(2, &[7; MAX + 1])).await;
        writer.send(make_message(2, b"next")).await.unwrap();
        drop(writer);

        assert!(matches!(refused, Err(SendError::TooLarge { max: MAX })));
        assert_eq!(written(peer).await, encode_frame(2, b"next"));
    }

    /// The peer counts the bytes the length announced, so a send dropped part
    /// way through must not be followed by the start of another message.
    #[tokio::test(start_paused = true)]
    async fn canceled_send_leaves_whole_messages_on_the_stream() {
        let (peer, mut writer) = writing(6);

        let blocked = tokio::time::timeout(
            Duration::from_millis(1),
            writer.send(make_message(2, b"abandoned")),
        );

        assert!(blocked.await.is_err());

        let (bytes, sent) = tokio::join!(written(peer), async move {
            writer.send(make_message(2, b"next")).await
        });

        sent.unwrap();

        assert_eq!(
            bytes,
            [encode_frame(2, b"abandoned"), encode_frame(2, b"next")].concat()
        );
    }

    #[test]
    fn debug_output_shows_the_limit_and_state() {
        let (_, writer) = writing(8);

        assert_eq!(
            format!("{writer:?}"),
            "Writer { max_message_len: 32, failed: false, .. }"
        );
    }

    #[tokio::test]
    async fn a_failed_write_fails_every_later_send() {
        let mut writer: Writer<_, MAX> = Writer::new(failing_once(io::ErrorKind::BrokenPipe, &[]));

        let Err(SendError::Io(error)) = writer.send(make_message(2, b"lost")).await else {
            panic!("expected the stream failure");
        };

        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(matches!(
            writer.send(make_message(2, b"next")).await,
            Err(SendError::Closed)
        ));
    }

    /// `Interrupted` means the write should be retried, so it does not fail
    /// the writer.
    #[tokio::test]
    async fn retries_an_interrupted_write() {
        let mut writer: Writer<_, MAX> = Writer::new(failing_once(io::ErrorKind::Interrupted, &[]));

        writer.send(make_message(2, b"x")).await.unwrap();
    }

    /// A stream that takes every write and fails the first flush. Later
    /// flushes succeed, so a `send` that wrongly retries after the failure
    /// ends instead of waiting forever.
    struct FlushFailsOnce(Option<io::ErrorKind>);

    impl AsyncWrite for FlushFailsOnce {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            match self.0.take() {
                Some(kind) => Poll::Ready(Err(kind.into())),
                None => Poll::Ready(Ok(())),
            }
        }

        fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn flushing(kind: io::ErrorKind) -> Writer<FlushFailsOnce, MAX> {
        Writer::new(FlushFailsOnce(Some(kind)))
    }

    #[tokio::test]
    async fn a_failed_flush_fails_the_send() {
        let mut writer = flushing(io::ErrorKind::BrokenPipe);

        let Err(SendError::Io(error)) = writer.send(make_message(2, b"x")).await else {
            panic!("expected the stream failure");
        };

        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    }

    /// `Interrupted` means the flush should be retried, so it does not fail
    /// the writer.
    #[tokio::test]
    async fn retries_an_interrupted_flush() {
        let mut writer = flushing(io::ErrorKind::Interrupted);

        writer.send(make_message(2, b"x")).await.unwrap();
    }

    #[test]
    fn sends_and_receives_through_one_channel() {
        fn assert_bidirectional<C: Bidirectional<Message, Message>>() {}

        assert_bidirectional::<Channel<DuplexStream, MAX>>();
    }

    /// A failed write may have left part of a message on the stream, so neither
    /// direction can be trusted afterwards. The stream still holds a whole
    /// message after the failure, so only the failed `send` stops `recv`.
    #[tokio::test]
    async fn a_failed_send_ends_both_directions() {
        let mut channel: Channel<_, MAX> = Channel::new(
            failing_once(io::ErrorKind::BrokenPipe, &[2, 1, b'x']),
            to_message,
        );

        let Err(SendError::Io(error)) = channel.send(make_message(2, b"lost")).await else {
            panic!("expected the stream failure");
        };

        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(matches!(
            channel.send(make_message(2, b"next")).await,
            Err(SendError::Closed)
        ));
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    #[tokio::test]
    async fn a_failed_receive_ends_both_directions() {
        let (mut peer, stream) = tokio::io::duplex(8);
        peer.write_all(&[0x02, MAX as u8 + 1]).await.unwrap();
        peer.shutdown().await.unwrap();
        let mut channel: Channel<_, MAX> = Channel::new(stream, to_message);

        assert!(matches!(
            channel.recv().await,
            Err(RecvError::Record(tlv::Error::TooLarge { .. }))
        ));
        assert!(matches!(
            channel.send(make_message(2, b"next")).await,
            Err(SendError::Closed)
        ));
    }

    /// A payload too large to send writes nothing, so both directions stay
    /// open.
    #[tokio::test]
    async fn a_refused_payload_leaves_the_channel_open() {
        let (mut peer, stream) = tokio::io::duplex(64);
        let mut channel: Channel<_, MAX> = Channel::new(stream, to_message);

        let refused = channel.send(make_message(2, &[7; MAX + 1])).await;
        assert!(matches!(refused, Err(SendError::TooLarge { max: MAX })));
        channel.send(make_message(2, b"out")).await.unwrap();

        peer.write_all(&encode_frame(2, b"in")).await.unwrap();
        peer.shutdown().await.unwrap();
        assert_eq!(channel.recv().await.unwrap(), make_message(2, b"in"));
    }

    #[test]
    fn debug_output_shows_both_halves() {
        let (_, stream) = tokio::io::duplex(8);
        let channel: Channel<_, MAX> = Channel::new(stream, to_message);

        assert_eq!(
            format!("{channel:?}"),
            "Channel { reader: Reader { max_message_len: 32, failed: false, .. }, \
             writer: Writer { max_message_len: 32, failed: false, .. } }"
        );
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Dummy {
        Ping,
        Note { number: u64, text: String },
    }

    #[derive(Debug, thiserror::Error)]
    enum DecodeError {
        #[error(transparent)]
        Tlv(#[from] tlv::Error),
        #[error(transparent)]
        Utf8(#[from] std::str::Utf8Error),
        #[error("invalid dummy message")]
        Invalid,
    }

    // The dummy format has two levels of nested TLVs. Unknown odd fields
    // are ignored; only the final text field is copied into the owned enum.
    fn decode_dummy(record: TypedSlice<&[u8]>) -> Result<Dummy, DecodeError> {
        match record.ty {
            0 if record.value.is_empty() => Ok(Dummy::Ping),

            2 => {
                let mut number = None;
                let mut text = None;

                for field in tlv::Reader::new(record.value) {
                    let field = field?;
                    assert_subslice(record.value, field.value);

                    match field.ty {
                        0 if number.is_none() => {
                            number = Some(u64::from_be_bytes(
                                field.value.try_into().map_err(|_| DecodeError::Invalid)?,
                            ));
                        }

                        2 => {
                            for nested in tlv::Reader::new(field.value) {
                                let nested = nested?;
                                assert_subslice(field.value, nested.value);

                                match nested.ty {
                                    0 if text.is_none() => {
                                        text = Some(std::str::from_utf8(nested.value)?);
                                    }
                                    ty if ty % 2 == 1 => {}
                                    _ => return Err(DecodeError::Invalid),
                                }
                            }
                        }

                        ty if ty % 2 == 1 => {}
                        _ => return Err(DecodeError::Invalid),
                    }
                }

                let number = number.ok_or(DecodeError::Invalid)?;
                let text = text.ok_or(DecodeError::Invalid)?;

                Ok(Dummy::Note {
                    number,
                    text: text.to_owned(),
                })
            }

            _ => Err(DecodeError::Invalid),
        }
    }

    fn assert_subslice(parent: &[u8], child: &[u8]) {
        let start = parent.as_ptr() as usize;
        let child_start = child.as_ptr() as usize;

        assert!(child_start >= start);
        assert!(child_start + child.len() <= start + parent.len());
    }

    fn dummy_note() -> Vec<u8> {
        let metadata = [encode_frame(0, b"hello"), encode_frame(1, b"ignored")].concat();
        let fields = [
            encode_frame(0, &42u64.to_be_bytes()),
            encode_frame(2, &metadata),
        ]
        .concat();

        encode_frame(2, &fields)
    }

    #[tokio::test]
    async fn decodes_nested_slices_before_queueing_owned_messages() {
        let bytes = [encode_frame(0, b""), dummy_note()].concat();

        for chunk_len in [bytes.len(), 1] {
            let mut channel = Reader::<_, 64, Dummy, DecodeError>::new(&[][..], decode_dummy);

            for chunk in bytes.chunks(chunk_len) {
                channel.read[..chunk.len()].copy_from_slice(chunk);
                channel.parse(chunk.len());
            }

            // The queue already contains enums. Reusing the read buffer cannot
            // change them, and no encoded message payload is retained in it.
            channel.read.fill(0);

            assert_eq!(channel.received.len(), 2);
            assert_eq!(channel.recv().await.unwrap(), Dummy::Ping);
            assert_eq!(
                channel.recv().await.unwrap(),
                Dummy::Note {
                    number: 42,
                    text: "hello".into()
                }
            );
            assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
        }
    }

    #[tokio::test]
    async fn decoding_failure_follows_queued_messages_and_is_terminal() {
        // A nested TLV announces two bytes but contains only one.
        let malformed = encode_frame(2, &encode_frame(2, &[0, 2, b'x']));
        let bytes = [encode_frame(0, b""), malformed, dummy_note()].concat();
        let mut channel = Reader::<_, 64, Dummy, DecodeError>::new(&bytes[..], decode_dummy);

        assert_eq!(channel.recv().await.unwrap(), Dummy::Ping);
        assert_eq!(channel.received.len(), 1);

        assert!(matches!(
            channel.recv().await,
            Err(RecvError::Decode(DecodeError::Tlv(tlv::Error::Truncated)))
        ));
        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
    }

    fn block_on<F: Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future)
    }

    fn generate_within_limit() -> impl Strategy<Value = Vec<Message>> {
        let payload = prop::collection::vec(any::<u8>(), 0..=MAX);
        prop::collection::vec(
            (any::<u64>(), payload).prop_map(|(ty, value)| TypedSlice { ty, value }),
            0..8,
        )
    }

    proptest! {
        #[test]
        fn receives_any_messages_within_the_limit(messages in generate_within_limit()) {
            block_on(async {
                let mut bytes = Vec::new();
                let records = messages.iter().map(|message| TypedSlice { ty: message.ty, value: &message.value });
                tlv::encode(records, &mut bytes).unwrap();

                let (mut peer, mut channel) = open();
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
        fn what_one_end_sends_the_other_receives(messages in generate_within_limit()) {
            block_on(async {
                let (near, far) = tokio::io::duplex(8);
                let mut sender: Channel<DuplexStream, MAX> = Channel::new(near, to_message);
                let mut receiver: Channel<DuplexStream, MAX> = Channel::new(far, to_message);

                let sent = messages.clone();

                let sending = async move {
                    for message in sent {
                        sender.send(message).await.unwrap();
                    }
                };

                let receiving = async {
                    for message in &messages {
                        assert_eq!(&receiver.recv().await.unwrap(), message);
                    }

                    assert!(matches!(receiver.recv().await, Err(RecvError::Closed)));
                };

                tokio::join!(sending, receiving);
            });
        }

        #[test]
        fn arbitrary_bytes_end_in_a_failure(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
            block_on(async {
                let mut channel = open_with(&bytes).await;

                // Every record takes at least a type and a length from the stream.
                for _ in 0..=bytes.len() / 2 {
                    let Ok(TypedSlice { value: payload, .. }) = channel.recv().await else {
                        break;
                    };

                    assert!(payload.len() <= MAX);
                }

                assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
            });
        }
    }
}

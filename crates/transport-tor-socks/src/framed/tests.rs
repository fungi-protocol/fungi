use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use fungi_transport::{Bidirectional, RecvChannel, SendChannel};
use proptest::prelude::*;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream, ReadBuf};

use super::{Channel, RecvError, SendError};

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
    let (mut peer, channel) = reading(MAX);
    peer.write_all(bytes).await.unwrap();
    channel
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

/// A channel writing what the test reads from the returned stream.
fn writing(pipe_len: usize) -> (DuplexStream, Channel<DuplexStream>) {
    let (peer, stream) = tokio::io::duplex(pipe_len);
    (peer, Channel::new(stream, MAX))
}

/// Everything written to the stream until it ends.
async fn written(mut peer: DuplexStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    peer.read_to_end(&mut bytes).await.unwrap();
    bytes
}

#[tokio::test]
async fn writes_the_length_then_the_payload() {
    let (peer, mut channel) = writing(64);

    channel.send(b"first".to_vec()).await.unwrap();
    channel.send(Vec::new()).await.unwrap();
    drop(channel);

    assert_eq!(written(peer).await, b"\0\0\0\x05first\0\0\0\0");
}

#[tokio::test]
async fn sends_a_message_at_the_limit() {
    let (peer, mut channel) = writing(64);
    let payload = vec![7; MAX as usize];

    channel.send(payload.clone()).await.unwrap();
    drop(channel);

    assert_eq!(written(peer).await, frame(&payload));
}

#[tokio::test]
async fn refuses_a_message_above_the_limit_and_stays_usable() {
    let (peer, mut channel) = writing(64);

    let refused = channel.send(vec![7; MAX as usize + 1]).await;
    channel.send(b"next".to_vec()).await.unwrap();
    drop(channel);

    assert!(matches!(refused, Err(SendError::TooLarge { max: MAX })));
    assert_eq!(written(peer).await, frame(b"next"));
}

/// The peer counts the bytes the length announced, so a send dropped part
/// way through must not be followed by the start of another message.
#[tokio::test(start_paused = true)]
async fn canceled_send_leaves_whole_messages_on_the_stream() {
    let (peer, mut channel) = writing(6);

    let blocked = tokio::time::timeout(
        Duration::from_millis(1),
        channel.send(b"abandoned".to_vec()),
    );
    assert!(blocked.await.is_err());

    let (bytes, sent) = tokio::join!(written(peer), async move {
        channel.send(b"next".to_vec()).await
    });

    sent.unwrap();
    assert_eq!(bytes, [frame(b"abandoned"), frame(b"next")].concat());
}

#[test]
fn sends_and_receives_through_one_channel() {
    fn assert_bidirectional<C: Bidirectional>() {}

    assert_bidirectional::<Channel<DuplexStream>>();
}

#[tokio::test(start_paused = true)]
async fn debug_output_omits_a_partial_message() {
    let (mut peer, mut channel) = reading(MAX);
    peer.write_all(&frame(b"secret")[..7]).await.unwrap();

    let waiting = tokio::time::timeout(Duration::from_millis(1), channel.recv());
    assert!(waiting.await.is_err());

    let debug = format!("{channel:?}");
    assert!(debug.contains("max_message_len: 16"));
    assert!(!debug.contains("115, 101, 99"));
}

struct Failing;

impl AsyncWrite for Failing {
    fn poll_write(self: Pin<&mut Self>, _: &mut Context<'_>, _: &[u8]) -> Poll<io::Result<usize>> {
        Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()))
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// A failed write may have left part of a message on the stream, so neither
/// direction can be trusted afterwards.
#[tokio::test]
async fn a_failed_send_ends_both_directions() {
    let mut channel: Channel<Failing> = Channel::new(Failing, MAX);

    let Err(SendError::Io(error)) = channel.send(b"lost".to_vec()).await else {
        panic!("expected the stream failure");
    };
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert!(matches!(
        channel.send(b"next".to_vec()).await,
        Err(SendError::Closed)
    ));
    assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
}

#[tokio::test]
async fn a_failed_receive_ends_both_directions() {
    let mut channel = reading_all(&(MAX + 1).to_be_bytes()).await;

    assert!(channel.recv().await.is_err());
    assert!(matches!(
        channel.send(b"next".to_vec()).await,
        Err(SendError::Closed)
    ));
}

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
    let mut channel: Channel<Failing> = Channel::new(Failing, MAX);

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
    fn what_one_end_sends_the_other_receives(
        messages in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..=MAX as usize), 0..8),
    ) {
        block_on(async {
            let (near, far) = tokio::io::duplex(8);
            let mut sender: Channel<DuplexStream> = Channel::new(near, MAX);
            let mut receiver: Channel<DuplexStream> = Channel::new(far, MAX);
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

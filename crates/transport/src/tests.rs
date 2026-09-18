use std::convert::Infallible;
use std::error::Error;
use std::io;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use futures_util::StreamExt;
use tokio::sync::mpsc;

use crate::*;

#[derive(Debug)]
struct Sender {
    peer: u8,
    sender: mpsc::Sender<u32>,
}

#[derive(Debug)]
struct Receiver {
    peer: u8,
    receiver: mpsc::Receiver<u32>,
}

impl PeerChannel for Sender {
    type Peer = u8;
    fn peer(&self) -> &u8 {
        &self.peer
    }
}

impl PeerChannel for Receiver {
    type Peer = u8;
    fn peer(&self) -> &u8 {
        &self.peer
    }
}

impl SendChannel<u32> for Sender {
    type Privacy = Anonymous;
    type SendError = mpsc::error::SendError<u32>;

    async fn send(&mut self, message: u32) -> Result<(), Self::SendError> {
        self.sender.send(message).await
    }
}

impl RecvChannel<u32> for Receiver {
    type RecvError = io::Error;

    async fn recv(&mut self) -> Result<u32, io::Error> {
        self.receiver
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "receiver closed"))
    }
}

fn simplex(capacity: usize) -> (Sender, Receiver) {
    let (sender, receiver) = mpsc::channel(capacity);
    (Sender { peer: 1, sender }, Receiver { peer: 1, receiver })
}

fn pair(capacity: usize) -> Channel<Sender, Receiver> {
    let (sender, receiver) = simplex(capacity);
    Channel::new(sender, receiver).unwrap()
}

async fn receive<M, C: RecvChannel<M>>(channel: &mut C) -> Result<M, C::RecvError> {
    tokio::time::timeout(Duration::from_secs(2), channel.recv())
        .await
        .expect("message reception timed out")
}

fn anonymous<C: SendChannel<M, Privacy = Anonymous>, M>(_: &C) {}
fn duplex<S: SendChannel<M>, R: RecvChannel<M>, M>(_: &Channel<S, R>) {}
fn sequential<C: SendChannel<M> + RecvChannel<M>, M>(_: &C) {}

#[tokio::test]
async fn simplex_pair_and_owned_directions_preserve_capabilities_and_privacy() {
    let (mut sender, mut receiver) = simplex(1);
    anonymous(&sender);
    sender.send(10).await.unwrap();
    assert_eq!(receive(&mut receiver).await.unwrap(), 10);

    let channel = Channel::new(sender, receiver).unwrap();
    duplex(&channel);
    anonymous(&channel);
    assert_eq!(*channel.peer(), 1);
    let (sender, receiver) = split(channel);
    assert_eq!(sender.peer(), receiver.peer());
    anonymous(&sender);
    let mut channel = Channel::new(sender, receiver).unwrap();
    duplex(&channel);
    channel.send(20).await.unwrap();
    assert_eq!(receive(&mut channel).await.unwrap(), 20);
}

#[test]
fn pairing_rejects_different_peers() {
    let (sender, mut receiver) = simplex(1);
    receiver.peer = 2;
    assert_eq!(Channel::new(sender, receiver).unwrap_err(), PeerMismatch);
    assert_eq!(PeerMismatch.to_string(), "channel peers differ");
    assert!(PeerMismatch.source().is_none());
}

#[tokio::test]
async fn pending_receive_does_not_block_send_and_cancellation_loses_nothing() {
    let mut channel = pair(1);
    let mut cx = Context::from_waker(Waker::noop());
    {
        let (mut sender, mut receiver) = channel.directions();
        anonymous(&sender);
        let receive = receiver.recv();
        let mut receive = std::pin::pin!(receive);
        assert!(matches!(
            std::future::Future::poll(receive.as_mut(), &mut cx),
            Poll::Pending
        ));
        tokio::time::timeout(Duration::from_secs(1), sender.send(42))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(receive.await.unwrap(), 42);
    }

    let (mut sender, mut receiver) = split(channel);
    {
        let receive = receiver.recv();
        let mut receive = std::pin::pin!(receive);
        assert!(matches!(
            std::future::Future::poll(receive.as_mut(), &mut cx),
            Poll::Pending
        ));
    }
    sender.send(43).await.unwrap();
    assert_eq!(receive(&mut receiver).await.unwrap(), 43);
}

#[tokio::test]
async fn pending_send_does_not_block_receive_and_cancellation_preserves_framing() {
    let (mut sender, mut receiver) = split(pair(1));
    sender.send(1).await.unwrap();
    {
        let send = sender.send(2);
        let mut send = std::pin::pin!(send);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            std::future::Future::poll(send.as_mut(), &mut cx),
            Poll::Pending
        ));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv())
                .await
                .unwrap()
                .unwrap(),
            1
        );
    }
    sender.send(3).await.unwrap();
    assert_eq!(receive(&mut receiver).await.unwrap(), 3);
}

#[tokio::test]
async fn owned_directions_run_in_separate_tasks() {
    let (mut sender, mut receiver) = split(pair(1));
    let sends = tokio::spawn(async move {
        sender.send(10).await.unwrap();
        sender.send(20).await.unwrap();
    });
    let messages = tokio::time::timeout(Duration::from_secs(2), async {
        [
            receiver.recv().await.unwrap(),
            receiver.recv().await.unwrap(),
        ]
    })
    .await
    .unwrap();
    sends.await.unwrap();
    assert_eq!(messages, [10, 20]);
    assert_eq!(
        receiver.recv().await.unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
}

#[derive(Debug, Default)]
struct Sequential(std::cell::Cell<Option<u32>>);

impl SendChannel<u32> for Sequential {
    type Privacy = Anonymous;
    type SendError = Infallible;
    async fn send(&mut self, message: u32) -> Result<(), Infallible> {
        self.0.set(Some(message));
        Ok(())
    }
}

impl RecvChannel<u32> for Sequential {
    type RecvError = io::Error;
    async fn recv(&mut self) -> Result<u32, io::Error> {
        self.0
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::WouldBlock, "empty"))
    }
}

#[tokio::test]
async fn sequential_non_sync_driver_needs_no_split_or_lock() {
    let mut channel = Sequential::default();
    sequential(&channel);
    anonymous(&channel);
    channel.send(7).await.unwrap();
    assert_eq!(receive(&mut channel).await.unwrap(), 7);
    assert_eq!(
        receive(&mut channel).await.unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let mut converted = CodecChannel::new(channel, Decimal);
    anonymous(&converted);
    converted.send("8".to_owned()).await.unwrap();
    assert_eq!(receive(&mut converted).await.unwrap(), "8");
}

#[tokio::test]
async fn stream_accepts_simplex_and_ends_after_the_first_error() {
    let (mut sender, receiver) = simplex(1);
    sender.send(8).await.unwrap();
    drop(sender);
    let stream = into_stream(receiver);
    let mut stream = std::pin::pin!(stream);
    assert_eq!(stream.next().await.unwrap().unwrap(), 8);
    assert_eq!(
        stream.next().await.unwrap().unwrap_err().kind(),
        io::ErrorKind::BrokenPipe
    );
    assert!(stream.next().await.is_none());
}

#[derive(Debug)]
struct Builder<C>(Option<C>);

impl<C: Send> ChannelBuilder for Builder<C> {
    type Input = ();
    type Channel = C;
    type BuildError = io::Error;
    async fn build(&mut self, _: &()) -> Result<C, io::Error> {
        self.0
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no local channel"))
    }
}

#[tokio::test]
async fn local_construction_supports_simplex_duplex_and_backend_errors() {
    let mut builder = Builder(Some(pair(1)));
    let mut channel = builder.build(&()).await.unwrap();
    anonymous(&channel);
    channel.send(4).await.unwrap();
    assert_eq!(receive(&mut channel).await.unwrap(), 4);
    assert_eq!(
        builder.build(&()).await.unwrap_err().kind(),
        io::ErrorKind::NotFound
    );
    let (sender, receiver) = simplex(1);
    let mut sender = Builder(Some(sender)).build(&()).await.unwrap();
    let mut receiver = Builder(Some(receiver)).build(&()).await.unwrap();
    sender.send(5).await.unwrap();
    assert_eq!(receive(&mut receiver).await.unwrap(), 5);
}

#[derive(Debug)]
struct Decimal;

impl Encode<String, u32> for Decimal {
    type EncodeError = std::num::ParseIntError;
    fn encode(&self, message: String) -> Result<u32, Self::EncodeError> {
        message.parse()
    }
}

impl Decode<u32, String> for Decimal {
    type DecodeError = io::Error;
    fn decode(&self, message: u32) -> Result<String, io::Error> {
        if message == 0 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "zero rejected"));
        }
        Ok(message.to_string())
    }
}

fn codec<K: Codec<String, u32>>(_: &K) {}

#[tokio::test]
async fn generic_duplex_conversion_preserves_peer_privacy_and_error_sources() {
    codec(&Decimal);
    let mut channel = pair(1).with_codec(Decimal);
    duplex(&channel);
    anonymous(&channel);
    assert_eq!(*channel.peer(), 1);
    let error = channel.send("invalid".to_owned()).await.unwrap_err();
    assert!(matches!(error, CodecError::Codec(_)));
    assert!(error.to_string().starts_with("codec: "));
    assert!(error.source().is_some());
    channel.send("42".to_owned()).await.unwrap();
    assert_eq!(receive(&mut channel).await.unwrap(), "42");
    let (mut sender, mut receiver) = split(channel);
    anonymous(&sender);
    sender.send("0".to_owned()).await.unwrap();
    let error = receive(&mut receiver).await.unwrap_err();
    assert!(matches!(error, CodecError::Codec(_)));
    assert_eq!(error.to_string(), "codec: zero rejected");
    assert_eq!(error.source().unwrap().to_string(), "zero rejected");
    sender.send("43".to_owned()).await.unwrap();
    assert_eq!(receive(&mut receiver).await.unwrap(), "43");
}

#[derive(Debug)]
struct EncodeOnly;
impl Encode<String, u32> for EncodeOnly {
    type EncodeError = Infallible;
    fn encode(&self, message: String) -> Result<u32, Infallible> {
        Ok(message.len() as u32)
    }
}

#[derive(Debug)]
struct DecodeOnly;
impl Decode<u32, String> for DecodeOnly {
    type DecodeError = Infallible;
    fn decode(&self, message: u32) -> Result<String, Infallible> {
        Ok(message.to_string())
    }
}

#[tokio::test]
async fn simplex_conversions_need_only_their_own_direction() {
    let (sender, receiver) = simplex(1);
    let mut sender = CodecChannel::new(sender, EncodeOnly);
    let mut receiver = CodecChannel::new(receiver, DecodeOnly);
    anonymous(&sender);
    sender.send("hello".to_owned()).await.unwrap();
    assert_eq!(receive(&mut receiver).await.unwrap(), "5");
}

#[tokio::test]
async fn converted_channels_preserve_transport_failures() {
    let (sender, receiver) = simplex(1);
    drop(receiver);
    let mut sender = CodecChannel::new(sender, Decimal);
    let error = sender.send("42".to_owned()).await.unwrap_err();
    assert!(matches!(error, CodecError::Transport(_)));
    assert!(error.to_string().starts_with("transport: "));
    assert_eq!(error.source().unwrap().to_string(), "channel closed");

    let (sender, receiver) = simplex(1);
    drop(sender);
    let mut receiver = CodecChannel::new(receiver, Decimal);
    let error = receive(&mut receiver).await.unwrap_err();
    assert!(matches!(error, CodecError::Transport(_)));
    assert_eq!(error.to_string(), "transport: receiver closed");
    assert_eq!(error.source().unwrap().to_string(), "receiver closed");
}

#[tokio::test]
async fn independent_converters_bundle_into_a_duplex_codec() {
    let conversions = CodecPair::new(EncodeOnly, DecodeOnly);
    codec(&conversions);
    let mut channel = CodecChannel::new(pair(1), conversions);
    anonymous(&channel);
    channel.send("hello".to_owned()).await.unwrap();
    assert_eq!(receive(&mut channel).await.unwrap(), "5");
}

#[derive(Debug)]
struct CountedCodec {
    drops: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for CountedCodec {
    fn drop(&mut self) {
        self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Encode<String, u32> for CountedCodec {
    type EncodeError = std::num::ParseIntError;
    fn encode(&self, message: String) -> Result<u32, Self::EncodeError> {
        Decimal.encode(message)
    }
}

impl Decode<u32, String> for CountedCodec {
    type DecodeError = io::Error;
    fn decode(&self, message: u32) -> Result<String, io::Error> {
        Decimal.decode(message)
    }
}

#[tokio::test]
async fn split_conversion_keeps_the_codec_until_both_directions_are_dropped() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let drops = Arc::new(AtomicUsize::new(0));
    let channel = pair(1).with_codec(CountedCodec {
        drops: Arc::clone(&drops),
    });
    let (mut sender, mut receiver) = split(channel);
    assert_eq!(sender.peer(), receiver.peer());
    anonymous(&sender);
    sender.send("7".to_owned()).await.unwrap();
    drop(sender);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(receive(&mut receiver).await.unwrap(), "7");
    drop(receiver);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[derive(Debug)]
struct SharedBackend {
    peer: u8,
    sender: mpsc::Sender<u32>,
    receiver: tokio::sync::Mutex<mpsc::Receiver<u32>>,
    drops: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl SharedBackend {
    fn new() -> Self {
        let (sender, receiver) = mpsc::channel(1);
        Self {
            peer: 1,
            sender,
            receiver: tokio::sync::Mutex::new(receiver),
            drops: Default::default(),
        }
    }
}

impl Drop for SharedBackend {
    fn drop(&mut self) {
        self.drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

impl PeerChannel for SharedBackend {
    type Peer = u8;
    fn peer(&self) -> &u8 {
        &self.peer
    }
}

impl SharedChannel<u32> for SharedBackend {
    type Privacy = Anonymous;
    type SendError = mpsc::error::SendError<u32>;
    type RecvError = io::Error;

    async fn send_shared(&self, message: u32) -> Result<(), Self::SendError> {
        self.sender.send(message).await
    }

    async fn recv_shared(&self) -> Result<u32, io::Error> {
        self.receiver
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "receiver closed"))
    }
}

#[tokio::test]
async fn shared_backend_gets_exclusive_capabilities_and_arc_split_from_the_adapter() {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    let backend = SharedBackend::new();
    let drops = Arc::clone(&backend.drops);
    let mut channel = Channel::from_shared(backend);
    duplex(&channel);
    anonymous(&channel);
    assert_eq!(*channel.peer(), 1);
    channel.send(10).await.unwrap();
    assert_eq!(receive(&mut channel).await.unwrap(), 10);

    let (mut sender, mut receiver) = split(channel);
    anonymous(&sender);
    assert_eq!(sender.peer(), receiver.peer());
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        let receive = receiver.recv();
        let mut receive = std::pin::pin!(receive);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            std::future::Future::poll(receive.as_mut(), &mut cx),
            Poll::Pending
        ));
        sender.send(11).await.unwrap();
        receive.await.unwrap()
    })
    .await
    .unwrap();
    assert_eq!(received, 11);
    sender.send(12).await.unwrap();
    {
        let send = sender.send(13);
        let mut send = std::pin::pin!(send);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            std::future::Future::poll(send.as_mut(), &mut cx),
            Poll::Pending
        ));
        assert_eq!(receive(&mut receiver).await.unwrap(), 12);
    }
    sender.send(14).await.unwrap();
    let mut channel = Channel::new(sender, receiver).unwrap();
    anonymous(&channel);
    assert_eq!(receive(&mut channel).await.unwrap(), 14);
    let (sender, receiver) = split(channel);
    drop(sender);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    // Retaining the whole backend keeps its resources alive for the last handle.
    drop(receiver);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shared_receive_cancellation_preserves_the_next_message() {
    let (mut sender, mut receiver) = split(Channel::from_shared(SharedBackend::new()));
    {
        let receive = receiver.recv();
        let mut receive = std::pin::pin!(receive);
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            std::future::Future::poll(receive.as_mut(), &mut cx),
            Poll::Pending
        ));
    }
    sender.send(21).await.unwrap();
    assert_eq!(receive(&mut receiver).await.unwrap(), 21);
}

#[tokio::test]
async fn conversion_before_arc_split_preserves_privacy_and_recovers_from_codec_errors() {
    let mut channel = Channel::from_shared(CodecChannel::new(SharedBackend::new(), Decimal));
    duplex(&channel);
    anonymous(&channel);
    let error = channel.send("invalid".to_owned()).await.unwrap_err();
    assert!(matches!(error, CodecError::Codec(_)));
    channel.send("42".to_owned()).await.unwrap();
    assert_eq!(receive(&mut channel).await.unwrap(), "42");
    let (mut sender, mut receiver) = split(channel);
    assert_eq!(sender.peer(), receiver.peer());
    anonymous(&sender);
    sender.send("0".to_owned()).await.unwrap();
    let error = receive(&mut receiver).await.unwrap_err();
    assert_eq!(error.source().unwrap().to_string(), "zero rejected");
    sender.send("43".to_owned()).await.unwrap();
    assert_eq!(receive(&mut receiver).await.unwrap(), "43");
}

#[tokio::test]
async fn conversion_after_shared_adaptation_can_split_and_run_in_separate_tasks() {
    let channel = Channel::from_shared(SharedBackend::new()).with_codec(Decimal);
    let (mut sender, mut receiver) = split(channel);
    anonymous(&sender);
    assert_eq!(sender.peer(), receiver.peer());
    let send = tokio::spawn(async move {
        sender.send("51".to_owned()).await.unwrap();
    });
    assert_eq!(receive(&mut receiver).await.unwrap(), "51");
    send.await.unwrap();
}

#[tokio::test]
async fn shared_conversion_preserves_backend_errors() {
    let mut backend = SharedBackend::new();
    backend.receiver.get_mut().close();
    let mut channel = Channel::from_shared(CodecChannel::new(backend, Decimal));
    let error = channel.send("42".to_owned()).await.unwrap_err();
    assert!(matches!(error, CodecError::Transport(_)));
    assert_eq!(error.source().unwrap().to_string(), "channel closed");
    let error = receive(&mut channel).await.unwrap_err();
    assert!(matches!(error, CodecError::Transport(_)));
    assert_eq!(error.source().unwrap().to_string(), "receiver closed");
}

async fn generic_duplex_roundtrip<M, S, R>(channel: Channel<S, R>, message: M)
where
    M: Send + Clone + std::fmt::Debug + PartialEq,
    S: SendChannel<M>,
    R: RecvChannel<M>,
{
    let (mut sender, mut receiver) = split(channel);
    let (sent, received) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(sender.send(message.clone()), receiver.recv())
    })
    .await
    .unwrap();
    sent.unwrap();
    assert_eq!(received.unwrap(), message);
}

#[tokio::test]
async fn public_duplex_contract_provides_owned_directions_for_every_adapter() {
    generic_duplex_roundtrip(pair(1), 31).await;
    generic_duplex_roundtrip(Channel::from_shared(SharedBackend::new()), 32).await;
    generic_duplex_roundtrip(pair(1).with_codec(Decimal), "33".to_owned()).await;
    generic_duplex_roundtrip(
        Channel::from_shared(CodecChannel::new(SharedBackend::new(), Decimal)),
        "34".to_owned(),
    )
    .await;
}

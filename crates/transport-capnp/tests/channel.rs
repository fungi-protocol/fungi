//! Consumer workflows over real RPC connections.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use fungi_transport::{
    Anonymous, Channel, Decode, Encode, PeerChannel, RecvChannel, SendChannel, into_stream,
};
use fungi_transport_capnp::{CapnpChannel, CapnpDuplex, RecvError, SendError, serve};
use fungi_transport_testkit::{
    mem::{MemConfig, MemError, MemPeer, MemReceiver, MemSender, duplex},
    testkit,
};
fn mem_send(_: MemError) -> SendError {
    SendError::Closed
}
fn mem_recv(_: MemError) -> RecvError {
    RecvError::Closed
}
trait SendFailure {
    fn into_rpc(self) -> SendError;
}
impl SendFailure for MemError {
    fn into_rpc(self) -> SendError {
        mem_send(self)
    }
}
impl SendFailure for SendError {
    fn into_rpc(self) -> SendError {
        self
    }
}
trait RecvFailure {
    fn into_rpc(self) -> RecvError;
}
impl RecvFailure for MemError {
    fn into_rpc(self) -> RecvError {
        mem_recv(self)
    }
}
impl RecvFailure for RecvError {
    fn into_rpc(self) -> RecvError {
        self
    }
}
use futures_util::StreamExt;

const MAX: usize = 1024;

fn server<S, R>(backend: Channel<S, R>, io: tokio::io::DuplexStream) -> std::thread::JoinHandle<()>
where
    S: SendChannel + 'static,
    R: RecvChannel + 'static,
    S::SendError: SendFailure,
    R::RecvError: RecvFailure,
{
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new()
            .block_on(
                &runtime,
                serve(backend, SendFailure::into_rpc, RecvFailure::into_rpc, io),
            )
            .unwrap();
    })
}
fn wrap<S, R>(backend: Channel<S, R>, max: usize) -> (CapnpDuplex, std::thread::JoinHandle<()>)
where
    S: SendChannel + 'static,
    R: RecvChannel + 'static,
    S::SendError: SendFailure,
    R::RecvError: RecvFailure,
{
    let (client, io) = tokio::io::duplex(64);
    let server = server(backend, io);
    (
        CapnpChannel::connect(client, max).unwrap().into_channel(),
        server,
    )
}
fn pair(config: MemConfig) -> (CapnpDuplex, CapnpDuplex, Vec<std::thread::JoinHandle<()>>) {
    let (left, right) = duplex(config);
    let (left, first) = wrap(left, MAX);
    let (right, second) = wrap(right, MAX);
    (left, right, vec![first, second])
}
async fn deadline<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("RPC workflow timed out")
}
async fn stopped(servers: Vec<std::thread::JoinHandle<()>>) {
    deadline(async {
        while servers.iter().any(|server| !server.is_finished()) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    for server in servers {
        server.join().unwrap();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conforms_in_both_directions_and_preserves_empty_messages() {
    let (mut left, mut right, servers) = pair(MemConfig::default());
    deadline(async {
        left.send(Vec::new()).await.unwrap();
        assert_eq!(right.recv().await.unwrap(), Vec::<u8>::new());
        testkit::roundtrip_both_directions(left, right).await;
    })
    .await;
    stopped(servers).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn duplex_bursts_preserve_every_message_under_backpressure() {
    let (mut left, mut right, servers) = pair(MemConfig::default());
    async fn exchange(channel: &mut CapnpDuplex, tag: u8) -> BTreeSet<Vec<u8>> {
        let (mut sender, mut receiver) = channel.directions();
        let sending = async move {
            for index in 0..32 {
                sender.send(vec![tag, index]).await.unwrap();
            }
        };
        let receiving = async move {
            let mut messages = BTreeSet::new();
            for _ in 0..32 {
                assert!(messages.insert(receiver.recv().await.unwrap()));
            }
            messages
        };
        futures_util::future::join(sending, receiving).await.1
    }
    let (from_right, from_left) = deadline(futures_util::future::join(
        exchange(&mut left, 0),
        exchange(&mut right, 1),
    ))
    .await;
    assert_eq!(from_right, (0..32).map(|i| vec![1, i]).collect());
    assert_eq!(from_left, (0..32).map(|i| vec![0, i]).collect());
    drop((left, right));
    stopped(servers).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_cancellation_survives_reborrowing_and_does_not_block_send() {
    let (mut left, mut right, servers) = pair(MemConfig::default());
    deadline(async {
        for _ in 0..5 {
            let (_, mut receiver) = left.directions();
            assert!(
                tokio::time::timeout(Duration::from_millis(5), receiver.recv())
                    .await
                    .is_err()
            );
        }
        left.send(b"outbound".to_vec()).await.unwrap();
        assert_eq!(right.recv().await.unwrap(), b"outbound");
        right.send(b"inbound".to_vec()).await.unwrap();
        assert_eq!(left.recv().await.unwrap(), b"inbound");
        testkit::recv_is_cancel_safe(right, left).await;
    })
    .await;
    stopped(servers).await;
}

#[derive(Debug)]
struct LimitedSend {
    sender: MemSender<Vec<u8>>,
    max: usize,
}
impl SendChannel for LimitedSend {
    type Privacy = Anonymous;
    type SendError = SendError;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        if message.len() > self.max {
            return Err(SendError::TooLarge { max: self.max });
        }
        self.sender.send(message).await.map_err(mem_send)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn local_and_remote_oversized_errors_preserve_recovery() {
    for local in [true, false] {
        let (left, right) = duplex(MemConfig::default());
        let (sender, receiver) = left.into_split();
        let left = Channel::new(LimitedSend { sender, max: 4 }, receiver).unwrap();
        let (mut left, first) = wrap(left, if local { 4 } else { MAX });
        let (mut right, second) = wrap(right, MAX);
        deadline(async {
            assert!(matches!(
                left.send(vec![0; 5]).await,
                Err(SendError::TooLarge { max: 4 })
            ));
            let (mut sender, mut receiver) = left.directions();
            assert!(matches!(
                sender.send(vec![0; 5]).await,
                Err(SendError::TooLarge { max: 4 })
            ));
            sender.send(vec![42; 4]).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), vec![42; 4]);
            right.send(vec![1]).await.unwrap();
            assert_eq!(receiver.recv().await.unwrap(), vec![1]);
        })
        .await;
        drop((left, right));
        stopped(vec![first, second]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_loss_closes_stream_and_releases_pending_receives() {
    let (left, mut right, servers) = pair(MemConfig::default());
    assert!(
        tokio::time::timeout(Duration::from_millis(5), right.recv())
            .await
            .is_err()
    );
    drop(right);
    let mut stream = Box::pin(into_stream(left));
    assert!(matches!(
        deadline(stream.next()).await,
        Some(Err(RecvError::Closed))
    ));
    assert!(deadline(stream.next()).await.is_none());
    drop(stream);
    stopped(servers).await;

    let (left, right) = duplex(MemConfig::default());
    drop(right);
    let (mut left, server) = wrap(left, MAX);
    assert!(matches!(
        deadline(left.send(vec![1])).await,
        Err(SendError::Closed)
    ));
    drop(left);
    stopped(vec![server]).await;
}

#[derive(Debug)]
struct ByteCodec;
impl Encode<u8> for ByteCodec {
    type EncodeError = std::io::Error;
    fn encode(&self, message: u8) -> Result<Vec<u8>, Self::EncodeError> {
        Ok(vec![message])
    }
}
impl Decode<Vec<u8>, u8> for ByteCodec {
    type DecodeError = std::io::Error;
    fn decode(&self, bytes: Vec<u8>) -> Result<u8, Self::DecodeError> {
        match bytes.as_slice() {
            [value] => Ok(*value),
            _ => Err(std::io::Error::other("expected one byte")),
        }
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn typed_channels_use_the_existing_codec_adapter() {
    let (left, right, servers) = pair(MemConfig::default());
    let (mut left, mut right) = (left.with_codec(ByteCodec), right.with_codec(ByteCodec));
    deadline(async {
        left.send(42).await.unwrap();
        assert_eq!(right.recv().await.unwrap(), 42);
        let (mut sender, mut receiver) = right.directions();
        sender.send(7).await.unwrap();
        assert_eq!(left.recv().await.unwrap(), 7);
        left.send(9).await.unwrap();
        assert_eq!(receiver.recv().await.unwrap(), 9);
    })
    .await;
    drop((left, right));
    stopped(servers).await;
}

#[derive(Debug)]
struct FailedSend {
    _sender: MemSender<Vec<u8>>,
}
impl SendChannel for FailedSend {
    type Privacy = Anonymous;
    type SendError = SendError;
    async fn send(&mut self, _: Vec<u8>) -> Result<(), SendError> {
        Err(SendError::Transport("injected send failure".into()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn backend_failure_keeps_diagnostics() {
    let (left, mut right) = duplex(MemConfig::default());
    let (sender, receiver) = left.into_split();
    let left = Channel::new(FailedSend { _sender: sender }, receiver).unwrap();
    let (mut left, server) = wrap(left, MAX);
    let error = deadline(left.send(vec![1])).await.unwrap_err();
    assert!(matches!(error, SendError::Transport(_)));
    assert!(error.to_string().contains("injected"));
    assert!(deadline(right.recv()).await.is_err());
    drop(left);
    stopped(vec![server]).await;
}

#[derive(Debug)]
struct FailedHalf(
    MemReceiver<Vec<u8>>,
    std::sync::Arc<std::sync::atomic::AtomicUsize>,
);
impl PeerChannel for FailedHalf {
    type Peer = MemPeer;
    fn peer(&self) -> &MemPeer {
        self.0.peer()
    }
}
impl RecvChannel for FailedHalf {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(RecvError::Transport("receive failure".into()))
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn receive_backend_error_terminates_the_stream_with_diagnostics() {
    let (backend, _peer) = duplex(MemConfig::default());
    let (client, io) = tokio::io::duplex(64);
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (sender, receiver) = backend.into_split();
    let server = server(
        Channel::new(sender, FailedHalf(receiver, calls.clone())).unwrap(),
        io,
    );
    let channel = CapnpChannel::connect(client, MAX).unwrap();
    let mut stream = Box::pin(into_stream(channel));
    let error = deadline(stream.next()).await.unwrap().unwrap_err();
    assert!(matches!(error, RecvError::Transport(_)));
    assert!(error.to_string().contains("receive failure"));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(deadline(stream.next()).await.is_none());
    drop(stream);
    stopped(vec![server]).await;
}

impl PeerChannel for LimitedSend {
    type Peer = MemPeer;
    fn peer(&self) -> &MemPeer {
        self.sender.peer()
    }
}

impl PeerChannel for FailedSend {
    type Peer = MemPeer;
    fn peer(&self) -> &MemPeer {
        self._sender.peer()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn owned_directions_preserve_peer_identity_and_reject_other_capabilities() {
    let (left, right, servers) = pair(MemConfig::default());
    let (sender, receiver) = left.into_split();
    assert_eq!(sender.peer(), receiver.peer());
    fn unspecified<C: SendChannel<Privacy = fungi_transport::Unspecified>>(_: &C) {}
    unspecified(&sender);
    let (other_sender, other_receiver) = right.into_split();
    assert_ne!(sender.peer(), other_receiver.peer());
    assert!(matches!(
        Channel::new(other_sender, receiver),
        Err(fungi_transport::PeerMismatch)
    ));
    assert!(matches!(
        Channel::new(sender, other_receiver),
        Err(fungi_transport::PeerMismatch)
    ));
    stopped(servers).await;
}

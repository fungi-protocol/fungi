//! Consumer workflows over real RPC connections.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use fungi_transport::{
    Contramap, Duplex, Map, PeerChannel, Pseudonymous, RecvChannel, SendChannel,
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

const MAX: usize = 1024;

fn server<S, R>(backend: Duplex<S, R>, io: tokio::io::DuplexStream) -> std::thread::JoinHandle<()>
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
fn wrap<S, R>(backend: Duplex<S, R>, max: usize) -> (CapnpDuplex, std::thread::JoinHandle<()>)
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
async fn receive_limit_preserves_both_directions_and_canceled_results() {
    async fn exercise<C>(mut channel: C, mut peer: fungi_transport_testkit::mem::MemChannel)
    where
        C: SendChannel<SendError = SendError> + RecvChannel<RecvError = RecvError>,
    {
        peer.send(vec![1; 4]).await.unwrap();
        assert_eq!(channel.recv().await.unwrap(), vec![1; 4]);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), channel.recv())
                .await
                .is_err()
        );
        peer.send(vec![2; 5]).await.unwrap();
        assert!(matches!(
            channel.recv().await,
            Err(RecvError::TooLarge { max: 4 })
        ));
        peer.send(vec![3]).await.unwrap();
        assert_eq!(channel.recv().await.unwrap(), vec![3]);
        channel.send(vec![4; 5]).await.unwrap();
        assert_eq!(peer.recv().await.unwrap(), vec![4; 5]);
    }
    for split in [false, true] {
        let (backend, peer) = duplex(MemConfig::default());
        let (client, io) = tokio::io::duplex(64);
        let server = server(backend, io);
        let mut channel = CapnpChannel::connect(client, MAX).unwrap();
        channel.set_max_recv_message_len(4);
        if split {
            deadline(exercise(channel.into_channel(), peer)).await;
        } else {
            deadline(exercise(channel, peer)).await;
        }
        stopped(vec![server]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_limit_changes_apply_to_new_requests_and_allow_empty_payloads() {
    let (backend, mut peer) = duplex(MemConfig::default());
    let (client, io) = tokio::io::duplex(64);
    let server = server(backend, io);
    let mut channel = CapnpChannel::connect(client, MAX).unwrap();
    channel.set_max_recv_message_len(4);
    deadline(async {
        assert!(
            tokio::time::timeout(Duration::from_millis(5), channel.recv())
                .await
                .is_err()
        );
        channel.set_max_recv_message_len(0);
        peer.send(vec![1; 4]).await.unwrap();
        assert_eq!(channel.recv().await.unwrap(), vec![1; 4]);
        peer.send(vec![2]).await.unwrap();
        assert!(matches!(
            channel.recv().await,
            Err(RecvError::TooLarge { max: 0 })
        ));
        peer.send(Vec::new()).await.unwrap();
        assert!(channel.recv().await.unwrap().is_empty());
    })
    .await;
    drop((channel, peer));
    stopped(vec![server]).await;
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
    let (left, right, servers) = pair(MemConfig::default());
    async fn exchange(channel: CapnpDuplex, tag: u8) -> BTreeSet<Vec<u8>> {
        let (mut sender, mut receiver) = channel.into_parts();
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
        exchange(left, 0),
        exchange(right, 1),
    ))
    .await;
    assert_eq!(from_right, (0..32).map(|i| vec![1, i]).collect());
    assert_eq!(from_left, (0..32).map(|i| vec![0, i]).collect());
    stopped(servers).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn receive_cancellation_does_not_block_send() {
    let (mut left, mut right, servers) = pair(MemConfig::default());
    deadline(async {
        for _ in 0..5 {
            assert!(
                tokio::time::timeout(Duration::from_millis(5), left.recv())
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
    type Privacy = Pseudonymous;
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
        let (sender, receiver) = left.into_parts();
        let left = Duplex::new(LimitedSend { sender, max: 4 }, receiver);
        let (mut left, first) = wrap(left, if local { 4 } else { MAX });
        let (mut right, second) = wrap(right, MAX);
        deadline(async {
            assert!(matches!(
                left.send(vec![0; 5]).await,
                Err(SendError::TooLarge { max: 4 })
            ));
            assert!(matches!(
                left.send(vec![0; 5]).await,
                Err(SendError::TooLarge { max: 4 })
            ));
            left.send(vec![42; 4]).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), vec![42; 4]);
            right.send(vec![1]).await.unwrap();
            assert_eq!(left.recv().await.unwrap(), vec![1]);
        })
        .await;
        drop((left, right));
        stopped(vec![first, second]).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_loss_closes_channel_and_releases_pending_receives() {
    let (left, mut right, servers) = pair(MemConfig::default());
    assert!(
        tokio::time::timeout(Duration::from_millis(5), right.recv())
            .await
            .is_err()
    );
    drop(right);
    let mut channel = left;
    assert!(matches!(
        deadline(channel.recv()).await,
        Err(RecvError::Closed)
    ));
    assert!(matches!(
        deadline(channel.recv()).await,
        Err(RecvError::Closed)
    ));
    drop(channel);
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

#[tokio::test(flavor = "multi_thread")]
async fn typed_channels_compose_message_transformations() {
    let (left, right, servers) = pair(MemConfig::default());
    let adapt = |channel: CapnpDuplex| {
        let (sender, receiver) = channel.into_parts();
        Duplex::new(
            Contramap::new(sender, |byte: u8| vec![byte]),
            Map::new(receiver, |bytes: Vec<u8>| bytes[0]),
        )
    };
    let (mut left, mut right) = (adapt(left), adapt(right));
    deadline(async {
        left.send(42).await.unwrap();
        assert_eq!(right.recv().await.unwrap(), 42);
        right.send(7).await.unwrap();
        assert_eq!(left.recv().await.unwrap(), 7);
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
    type Privacy = Pseudonymous;
    type SendError = SendError;
    async fn send(&mut self, _: Vec<u8>) -> Result<(), SendError> {
        Err(SendError::Transport("injected send failure".into()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn backend_failure_keeps_diagnostics() {
    let (left, mut right) = duplex(MemConfig::default());
    let (sender, receiver) = left.into_parts();
    let left = Duplex::new(FailedSend { _sender: sender }, receiver);
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
async fn receive_backend_error_closes_both_directions_with_diagnostics() {
    let (backend, _peer) = duplex(MemConfig::default());
    let (client, io) = tokio::io::duplex(64);
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (sender, receiver) = backend.into_parts();
    let server = server(Duplex::new(sender, FailedHalf(receiver, calls.clone())), io);
    let mut channel = CapnpChannel::connect(client, MAX).unwrap();
    let error = deadline(channel.recv()).await.unwrap_err();
    assert!(matches!(error, RecvError::Transport(_)));
    assert!(error.to_string().contains("receive failure"));
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(matches!(
        deadline(channel.send(vec![1])).await,
        Err(SendError::Closed)
    ));
    assert!(matches!(
        deadline(channel.recv()).await,
        Err(RecvError::Closed)
    ));
    drop(channel);
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
async fn owned_directions_preserve_peer_identity() {
    let (left, right, servers) = pair(MemConfig::default());
    let (sender, receiver) = left.into_parts();
    assert_eq!(sender.peer(), receiver.peer());
    fn unspecified<C: SendChannel<Privacy = fungi_transport::Unspecified>>(_: &C) {}
    unspecified(&sender);
    let (other_sender, other_receiver) = right.into_parts();
    assert_ne!(sender.peer(), other_receiver.peer());
    drop((sender, receiver, other_sender, other_receiver));
    stopped(servers).await;
}

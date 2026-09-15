//! Consumer workflows over real RPC connections.

use std::collections::BTreeSet;
use std::future::Future;
use std::time::Duration;

use fungi_transport::{
    ChannelBuilder, Contramap, Duplex, Map, PeerChannel, Pseudonymous, RecvChannel, SendChannel,
};
use fungi_transport_capnp::{
    BuildError, CapnpBuilder, CapnpChannel, CapnpDuplex, RecvError, SendError, serve, serve_builder,
};
use fungi_transport_testkit::{
    mem::{MemChannel, MemConfig, MemError, MemPeer, MemReceiver, MemSender, duplex, network},
    testkit,
};
fn mem_send(_: MemError) -> SendError {
    SendError::Closed
}
fn mem_recv(_: MemError) -> RecvError {
    RecvError::Closed
}
fn mem_build(_: MemError) -> BuildError {
    BuildError::Unreachable
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

#[tokio::test(flavor = "multi_thread")]
async fn builders_reconnect_and_keep_channels_alive_after_builder_drop() {
    let (connector, listener) = network(MemConfig::default());
    let (client, io) = tokio::io::duplex(64);
    let first = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new()
            .block_on(
                &rt,
                serve_builder(
                    connector,
                    |_| Ok(fungi_transport_testkit::mem::MemAddr),
                    mem_send,
                    mem_recv,
                    mem_build,
                    io,
                ),
            )
            .unwrap();
    });
    let (inbound, io) = tokio::io::duplex(64);
    let second = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new()
            .block_on(
                &rt,
                serve_builder(
                    listener,
                    |token| {
                        if token.is_empty() {
                            Ok(())
                        } else {
                            Err(BuildError::Transport("inbound input must be empty".into()))
                        }
                    },
                    mem_send,
                    mem_recv,
                    mem_build,
                    io,
                ),
            )
            .unwrap();
    });
    let mut connector = CapnpBuilder::connect(client, MAX).unwrap();
    let mut listener = CapnpBuilder::connect(inbound, MAX).unwrap().into_acceptor();
    for _ in 0..2 {
        connector.set_max_recv_message_len(1);
        let (left, right) = deadline(futures_util::future::join(
            connector.build(&Vec::new()),
            listener.build(&()),
        ))
        .await;
        let (mut left, mut right) = (left.unwrap(), right.unwrap());
        connector.set_max_recv_message_len(0);
        deadline(async {
            right.send(vec![9; 2]).await.unwrap();
            assert!(matches!(
                left.recv().await,
                Err(RecvError::TooLarge { max: 1 })
            ));
            right.send(vec![8]).await.unwrap();
            assert_eq!(left.recv().await.unwrap(), vec![8]);
            left.send(vec![1]).await.unwrap();
            assert_eq!(right.recv().await.unwrap(), vec![1]);
            drop(right);
            assert!(left.recv().await.is_err());
        })
        .await;
    }
    let (left, right) = deadline(futures_util::future::join(
        connector.build(&Vec::new()),
        listener.build(&()),
    ))
    .await;
    let (mut left, mut right) = (left.unwrap(), right.unwrap());
    drop((connector, listener));
    deadline(async {
        left.send(vec![42]).await.unwrap();
        assert_eq!(right.recv().await.unwrap(), vec![42]);
    })
    .await;
    drop((left, right));
    stopped(vec![first, second]).await;
}

#[derive(Debug)]
struct RejectBuilder;
impl ChannelBuilder for RejectBuilder {
    type Privacy = fungi_transport::Pseudonymous;
    type Input = Vec<u8>;
    type Channel = MemChannel;
    type BuildError = BuildError;
    async fn build(&mut self, input: &Vec<u8>) -> Result<MemChannel, BuildError> {
        if input == b"unreachable" {
            Err(BuildError::Unreachable)
        } else {
            Err(BuildError::Transport("backend refused token".into()))
        }
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn builder_errors_preserve_semantics_and_decoder_diagnostics() {
    let (client, io) = tokio::io::duplex(64);
    let server = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new()
            .block_on(
                &rt,
                serve_builder(
                    RejectBuilder,
                    |input| {
                        if input == b"invalid" {
                            Err(BuildError::Transport("invalid token".into()))
                        } else {
                            Ok(input)
                        }
                    },
                    mem_send,
                    mem_recv,
                    std::convert::identity,
                    io,
                ),
            )
            .unwrap();
    });
    let mut builder = CapnpBuilder::connect(client, MAX).unwrap();
    assert!(matches!(
        deadline(builder.build(&b"unreachable".to_vec())).await,
        Err(BuildError::Unreachable)
    ));
    for (input, diagnostic) in [
        (b"backend".to_vec(), "backend refused token"),
        (b"invalid".to_vec(), "invalid token"),
    ] {
        let error = deadline(builder.build(&input)).await.unwrap_err();
        assert!(matches!(error, BuildError::Transport(_)));
        assert!(error.to_string().contains(diagnostic));
    }
    drop(builder);
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

#[derive(Debug)]
struct Lifetime {
    _peer: MemChannel,
    dropped: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl Drop for Lifetime {
    fn drop(&mut self) {
        self.dropped
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[derive(Debug)]
struct TrackedSender(
    MemSender<Vec<u8>>,
    std::sync::Arc<Lifetime>,
    Option<tokio::sync::oneshot::Sender<()>>,
);
#[derive(Debug)]
struct TrackedReceiver(MemReceiver<Vec<u8>>, std::sync::Arc<Lifetime>);
type TrackedChannel = Duplex<TrackedSender, TrackedReceiver>;
impl PeerChannel for TrackedSender {
    type Peer = MemPeer;
    fn peer(&self) -> &MemPeer {
        self.0.peer()
    }
}
impl PeerChannel for TrackedReceiver {
    type Peer = MemPeer;
    fn peer(&self) -> &MemPeer {
        self.0.peer()
    }
}
impl SendChannel for TrackedSender {
    type Privacy = Pseudonymous;
    type SendError = MemError;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), MemError> {
        let _ = &self.1;
        let sending = self.0.send(message);
        tokio::pin!(sending);
        std::future::poll_fn(|cx| {
            let state = sending.as_mut().poll(cx);
            if state.is_pending()
                && let Some(blocked) = self.2.take()
            {
                let _ = blocked.send(());
            }
            state
        })
        .await
    }
}
impl RecvChannel for TrackedReceiver {
    type RecvError = MemError;
    async fn recv(&mut self) -> Result<Vec<u8>, MemError> {
        let _ = &self.1;
        self.0.recv().await
    }
}
#[derive(Debug)]
struct TrackingBuilder {
    gate: std::sync::Arc<tokio::sync::Semaphore>,
    started: tokio::sync::mpsc::UnboundedSender<()>,
    dropped: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    blocked_send: Option<tokio::sync::oneshot::Sender<()>>,
}
impl ChannelBuilder for TrackingBuilder {
    type Privacy = fungi_transport::Pseudonymous;
    type Input = Vec<u8>;
    type Channel = TrackedChannel;
    type BuildError = BuildError;
    async fn build(&mut self, _: &Vec<u8>) -> Result<TrackedChannel, BuildError> {
        let (channel, peer) = duplex(MemConfig::default());
        let (sender, receiver) = channel.into_parts();
        let lifetime = std::sync::Arc::new(Lifetime {
            _peer: peer,
            dropped: self.dropped.clone(),
        });
        let channel = Duplex::new(
            TrackedSender(sender, lifetime.clone(), self.blocked_send.take()),
            TrackedReceiver(receiver, lifetime),
        );
        self.started.send(()).unwrap();
        self.gate.acquire().await.unwrap().forget();
        Ok(channel)
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn canceled_builds_are_bounded_and_mass_drops_release_backends() {
    let gate = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (started, mut starts) = tokio::sync::mpsc::unbounded_channel();
    let backend = TrackingBuilder {
        gate: gate.clone(),
        started,
        dropped: dropped.clone(),
        blocked_send: None,
    };
    let decoded = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let calls = decoded.clone();
    let (client, io) = tokio::io::duplex(64);
    let server = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new()
            .block_on(
                &rt,
                serve_builder(
                    backend,
                    move |input| {
                        calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        Ok(input)
                    },
                    mem_send,
                    mem_recv,
                    std::convert::identity,
                    io,
                ),
            )
            .unwrap();
    });
    let mut builder = CapnpBuilder::connect(client, MAX).unwrap();
    let input = Vec::new();
    {
        let building = builder.build(&input);
        tokio::pin!(building);
        deadline(async {
            tokio::select! {
                result = &mut building => panic!("build completed before release: {result:?}"),
                started = starts.recv() => assert!(started.is_some()),
            }
        })
        .await;
    }
    for _ in 0..32 {
        assert!(
            tokio::time::timeout(Duration::from_millis(2), builder.build(&input))
                .await
                .is_err()
        );
    }
    assert_eq!(decoded.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(dropped.load(std::sync::atomic::Ordering::SeqCst), 0);
    gate.add_permits(1);
    deadline(async {
        while dropped.load(std::sync::atomic::Ordering::SeqCst) != 1 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    gate.add_permits(64);
    let channels = deadline(async {
        let mut channels = Vec::new();
        for _ in 0..64 {
            channels.push(builder.build(&input).await.unwrap());
        }
        channels
    })
    .await;
    assert_eq!(decoded.load(std::sync::atomic::Ordering::SeqCst), 65);
    drop(channels);
    deadline(async {
        while dropped.load(std::sync::atomic::Ordering::SeqCst) != 65 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    drop(builder);
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

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_channel_releases_a_blocked_backend_without_closing_its_builder() {
    let dropped = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (started, _starts) = tokio::sync::mpsc::unbounded_channel();
    let (blocked_send, blocked) = tokio::sync::oneshot::channel();
    let backend = TrackingBuilder {
        gate: std::sync::Arc::new(tokio::sync::Semaphore::new(2)),
        started,
        dropped: dropped.clone(),
        blocked_send: Some(blocked_send),
    };
    let (client, io) = tokio::io::duplex(64);
    let server = std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        tokio::task::LocalSet::new()
            .block_on(
                &rt,
                serve_builder(backend, Ok, mem_send, mem_recv, std::convert::identity, io),
            )
            .unwrap();
    });
    let mut builder = CapnpBuilder::connect(client, MAX).unwrap();
    let mut channel = builder.build(&Vec::new()).await.unwrap();
    channel.send(vec![1]).await.unwrap();
    {
        let sending = channel.send(vec![2]);
        tokio::pin!(sending);
        deadline(async {
            tokio::select! {
                result = &mut sending => panic!("send completed before channel drop: {result:?}"),
                result = blocked => result.expect("backend dropped before blocking"),
            }
        })
        .await;
    }
    drop(channel);
    let released = tokio::time::timeout(Duration::from_secs(5), async {
        while dropped.load(std::sync::atomic::Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .is_ok();
    let mut next = deadline(builder.build(&Vec::new())).await.unwrap();
    next.send(vec![3]).await.unwrap();
    drop(next);
    drop(builder);
    stopped(vec![server]).await;
    assert!(
        released,
        "backend retained after channel drop while builder/link stayed alive"
    );
}

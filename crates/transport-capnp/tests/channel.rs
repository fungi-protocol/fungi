#![cfg_attr(coverage_nightly, feature(coverage_attribute))]
#![cfg_attr(coverage_nightly, coverage(off))]

//! Consumer workflows over real RPC connections.

use std::future::Future;
use std::time::Duration;

use fungi_transport::{Bidirectional, ConnectionUnlinkability, RecvChannel, SendChannel};
use fungi_transport_capnp::{CapnpChannel, RecvError, SendError, serve};
use fungi_transport_testkit::{
    mem::{MemConfig, MemError, MemReceiver, MemSender, bidirectional},
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

fn server<S, R>(
    backend: Bidirectional<S, R>,
    io: tokio::io::DuplexStream,
) -> std::thread::JoinHandle<()>
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
fn wrap<S, R>(
    backend: Bidirectional<S, R>,
    max: usize,
) -> (CapnpChannel, std::thread::JoinHandle<()>)
where
    S: SendChannel + 'static,
    R: RecvChannel + 'static,
    S::SendError: SendFailure,
    R::RecvError: RecvFailure,
{
    let (client, io) = tokio::io::duplex(64);
    let server = server(backend, io);
    (CapnpChannel::connect(client, max).unwrap(), server)
}
fn pair(config: MemConfig) -> (CapnpChannel, CapnpChannel, Vec<std::thread::JoinHandle<()>>) {
    let (left, right) = bidirectional(config);
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
    type Privacy = ConnectionUnlinkability;
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
        let (left, right) = bidirectional(MemConfig::default());
        let (sender, receiver) = left.into_parts();
        let left = Bidirectional::new(LimitedSend { sender, max: 4 }, receiver);
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

    let (left, right) = bidirectional(MemConfig::default());
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
struct FailedSend {
    _sender: MemSender<Vec<u8>>,
}
impl SendChannel for FailedSend {
    type Privacy = ConnectionUnlinkability;
    type SendError = SendError;
    async fn send(&mut self, _: Vec<u8>) -> Result<(), SendError> {
        Err(SendError::Transport("injected send failure".into()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn backend_failure_keeps_diagnostics() {
    let (left, mut right) = bidirectional(MemConfig::default());
    let (sender, receiver) = left.into_parts();
    let left = Bidirectional::new(FailedSend { _sender: sender }, receiver);
    let (mut left, server) = wrap(left, MAX);
    let error = deadline(left.send(vec![1])).await.unwrap_err();
    assert!(matches!(error, SendError::Transport(_)));
    assert!(error.to_string().contains("injected"));
    assert!(deadline(right.recv()).await.is_err());
    drop(left);
    stopped(vec![server]).await;
}

#[derive(Debug)]
struct FailedHalf {
    _receiver: MemReceiver<Vec<u8>>,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl RecvChannel for FailedHalf {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Err(RecvError::Transport("receive failure".into()))
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn receive_backend_error_closes_both_directions_with_diagnostics() {
    let (backend, _peer) = bidirectional(MemConfig::default());
    let (client, io) = tokio::io::duplex(64);
    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let (sender, receiver) = backend.into_parts();
    let server = server(
        Bidirectional::new(
            sender,
            FailedHalf {
                _receiver: receiver,
                calls: calls.clone(),
            },
        ),
        io,
    );
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

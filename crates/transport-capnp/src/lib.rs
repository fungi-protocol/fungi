//! Cap'n Proto RPC channels for the Fungi transport contract.
//!
//! A dedicated current-thread executor isolates thread-local RPC capabilities
//! so client handles and their operation futures can be `Send`. Channels retain the connection for their operations. Dropping the last handle
//! schedules asynchronous cleanup so handle destruction stays immediate.
//! Peer authentication and delivery confirmation belong to the backend protocol.

use std::io;
use std::rc::Rc;
use std::sync::Arc;

use capnp::{capability::Promise, data};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side, twoparty};
use fungi_transport::{Channel, PeerChannel, RecvChannel, SendChannel, Unspecified};
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

/// A bridge or remote send failure.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    /// The bridge or backend rejected the payload length.
    #[error("message exceeds {max} bytes")]
    TooLarge {
        /// Maximum supported message length in bytes.
        max: usize,
    },
    /// The RPC channel is closed.
    #[error("RPC channel closed")]
    Closed,
    /// An RPC or backend failure, preserving its cause locally.
    #[error("transport: {0}")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// A bridge or remote receive failure.
#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    /// The RPC channel is closed.
    #[error("RPC channel closed")]
    Closed,
    /// An RPC or backend failure, preserving its cause locally.
    #[error("transport: {0}")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// Identity of one remote capability within this process.
#[derive(Debug, Clone)]
pub struct CapnpPeer(Arc<()>);

impl PartialEq for CapnpPeer {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for CapnpPeer {}

/// Owned independent RPC directions for one remote capability.
pub type CapnpDuplex = Channel<CapnpSendHalf, CapnpRecvHalf>;

mod channel_capnp {
    #![allow(dead_code, missing_docs, missing_debug_implementations, clippy::all)]
    include!(concat!(env!("OUT_DIR"), "/channel_capnp.rs"));
}
use channel_capnp::{channel, recv_failure, result as rpc_result, send_failure};

type RemoteChannel = channel::Client<data::Owned, send_failure::Owned, recv_failure::Owned>;

type Reply<T> = oneshot::Sender<T>;
type PendingReceive = Option<oneshot::Receiver<Result<Vec<u8>, RecvError>>>;

enum ChannelCommand {
    Send(Vec<u8>, Reply<Result<(), SendError>>, OwnedSemaphorePermit),
    Recv(Reply<Result<Vec<u8>, RecvError>>),
}
#[derive(Debug)]
struct Link {
    _lifetime: mpsc::Sender<()>,
    max_message_len: usize,
}
enum Bootstrap {
    Channel(mpsc::Receiver<ChannelCommand>),
}

/// One remote byte channel. Retained receiving state preserves responses across
/// caller cancellation.
///
/// At most one send RPC is queued or in flight per capability. Canceling its
/// caller leaves delivery unknown and retains the slot until the RPC finishes
/// or the link closes, keeping outstanding sends bounded. Independent receive
/// operations allow reception to progress while sends wait.
#[derive(Debug)]
pub struct CapnpChannel {
    commands: mpsc::Sender<ChannelCommand>,
    send_slot: Arc<Semaphore>,
    pending: PendingReceive,
    link: Arc<Link>,
    peer: CapnpPeer,
}

impl CapnpChannel {
    /// Connect to a channel bootstrap over an owned RPC stream.
    ///
    /// `max_message_len` sets the maximum outgoing payload size in bytes.
    pub fn connect<Io>(io: Io, max_message_len: usize) -> io::Result<Self>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (link, lifetime) = new_link(max_message_len);
        let (commands, receiver) = mpsc::channel(2);
        start_client(io, Bootstrap::Channel(receiver), lifetime, runtime)?;
        Ok(Self {
            commands,
            send_slot: Arc::new(Semaphore::new(1)),
            pending: None,
            link,
            peer: CapnpPeer(Arc::new(())),
        })
    }
}

/// Owned sending direction retaining the RPC link and the native send limit.
#[derive(Debug)]
pub struct CapnpSendHalf {
    commands: mpsc::Sender<ChannelCommand>,
    send_slot: Arc<Semaphore>,
    link: Arc<Link>,
    peer: CapnpPeer,
}
/// Owned receiving direction retaining canceled receive responses and the link.
#[derive(Debug)]
pub struct CapnpRecvHalf {
    commands: mpsc::Sender<ChannelCommand>,
    pending: PendingReceive,
    _link: Arc<Link>,
    peer: CapnpPeer,
}

impl CapnpChannel {
    /// Consume the native handle into an independently usable duplex adapter.
    ///
    /// Pending reception is retained to preserve responses across cancellation.
    /// Each direction retains the link so the other can be dropped independently.
    /// Privacy remains `Unspecified` because submission privacy depends on the
    /// backend transport. The type contract enforces this distinction:
    ///
    /// ```compile_fail
    /// use fungi_transport::{Anonymous, SendChannel};
    /// use fungi_transport_capnp::CapnpDuplex;
    /// fn anonymous<C: SendChannel<Privacy = Anonymous>>(_: C) {}
    /// fn submit(channel: CapnpDuplex) { anonymous(channel); }
    /// ```
    pub fn into_channel(self) -> CapnpDuplex {
        Channel::new(
            CapnpSendHalf {
                commands: self.commands.clone(),
                send_slot: self.send_slot,
                link: Arc::clone(&self.link),
                peer: self.peer.clone(),
            },
            CapnpRecvHalf {
                commands: self.commands,
                pending: self.pending,
                _link: self.link,
                peer: self.peer,
            },
        )
        .unwrap()
    }
}

impl PeerChannel for CapnpSendHalf {
    type Peer = CapnpPeer;
    fn peer(&self) -> &CapnpPeer {
        &self.peer
    }
}
impl PeerChannel for CapnpRecvHalf {
    type Peer = CapnpPeer;
    fn peer(&self) -> &CapnpPeer {
        &self.peer
    }
}

async fn send_message(
    commands: &mpsc::Sender<ChannelCommand>,
    send_slot: &Arc<Semaphore>,
    max: usize,
    message: Vec<u8>,
) -> Result<(), SendError> {
    if message.len() > max {
        return Err(SendError::TooLarge { max });
    }
    // Command ownership keeps outstanding sends bounded across caller cancellation.
    let permit = Arc::clone(send_slot).acquire_owned().await.unwrap();
    let (reply, result) = oneshot::channel();
    commands
        .send(ChannelCommand::Send(message, reply, permit))
        .await
        .map_err(|_| SendError::Closed)?;
    result.await.map_err(|_| SendError::Closed)?
}

async fn receive_message(
    commands: &mpsc::Sender<ChannelCommand>,
    pending: &mut PendingReceive,
) -> Result<Vec<u8>, RecvError> {
    if pending.is_none() {
        let (reply, result) = oneshot::channel();
        commands
            .send(ChannelCommand::Recv(reply))
            .await
            .map_err(|_| RecvError::Closed)?;
        // Retain the response before yielding so cancellation preserves it.
        *pending = Some(result);
    }
    let result = pending
        .as_mut()
        .unwrap()
        .await
        .map_err(|_| RecvError::Closed);
    *pending = None;
    result?
}

impl SendChannel for CapnpChannel {
    type Privacy = Unspecified;
    type SendError = SendError;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        send_message(
            &self.commands,
            &self.send_slot,
            self.link.max_message_len,
            message,
        )
        .await
    }
}
impl RecvChannel for CapnpChannel {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        receive_message(&self.commands, &mut self.pending).await
    }
}
impl SendChannel for CapnpSendHalf {
    type Privacy = Unspecified;
    type SendError = SendError;
    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        send_message(
            &self.commands,
            &self.send_slot,
            self.link.max_message_len,
            message,
        )
        .await
    }
}
impl RecvChannel for CapnpRecvHalf {
    type RecvError = RecvError;
    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        receive_message(&self.commands, &mut self.pending).await
    }
}
fn new_link(max_message_len: usize) -> (Arc<Link>, mpsc::Receiver<()>) {
    let (sender, receiver) = mpsc::channel(1);
    (
        Arc::new(Link {
            _lifetime: sender,
            max_message_len,
        }),
        receiver,
    )
}
fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}
fn start_client<Io>(
    io: Io,
    boot: Bootstrap,
    lifetime: mpsc::Receiver<()>,
    make_runtime: fn() -> io::Result<tokio::runtime::Runtime>,
) -> io::Result<()>
where
    Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (ready, started) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name("capnp-client".into())
        .spawn(move || match make_runtime() {
            Ok(runtime) => {
                let _ = ready.send(Ok(()));
                let local = tokio::task::LocalSet::new();
                let (reader, writer) = tokio::io::split(io);
                local.block_on(&runtime, run_client(reader, writer, boot, lifetime));
            }
            Err(error) => {
                let _ = ready.send(Err(error));
            }
        })?;
    started.recv().map_err(io::Error::other)??;
    Ok(())
}
async fn run_client<R, W>(reader: R, writer: W, boot: Bootstrap, mut lifetime: mpsc::Receiver<()>)
where
    R: AsyncRead + Unpin + 'static,
    W: AsyncWrite + Unpin + 'static,
{
    let network = twoparty::VatNetwork::new(
        reader.compat(),
        writer.compat_write(),
        Side::Client,
        Default::default(),
    );
    let mut rpc = RpcSystem::new(Box::new(network), None);
    match boot {
        Bootstrap::Channel(receiver) => {
            let remote = rpc.bootstrap::<RemoteChannel>(Side::Server);
            tokio::task::spawn_local(channel_actor(remote, receiver));
        }
    }
    tokio::select! { _ = rpc => {}, _ = lifetime.recv() => {} }
}
async fn channel_actor(remote: RemoteChannel, mut commands: mpsc::Receiver<ChannelCommand>) {
    let mut operations = FuturesUnordered::new();
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break };
                operations.push(dispatch_channel(remote.clone(), command));
            }
            _ = operations.next(), if !operations.is_empty() => {}
        }
    }
}
async fn dispatch_channel(remote: RemoteChannel, command: ChannelCommand) {
    match command {
        ChannelCommand::Send(message, reply, permit) => {
            let result = async {
                let mut request = remote.send_request();
                request
                    .get()
                    .set_message(message.as_slice())
                    .map_err(rpc_send_error)?;
                let response = request.send().promise.await.map_err(rpc_send_error)?;
                let result = response
                    .get()
                    .and_then(|r| r.get_result())
                    .map_err(rpc_send_error)?;
                match result
                    .which()
                    .map_err(capnp::Error::from)
                    .map_err(rpc_send_error)?
                {
                    rpc_result::Ok(unit) => {
                        unit.map_err(rpc_send_error)?;
                        Ok(())
                    }
                    rpc_result::Err(error) => match error
                        .map_err(rpc_send_error)?
                        .which()
                        .map_err(capnp::Error::from)
                        .map_err(rpc_send_error)?
                    {
                        send_failure::TooLarge(max) => Err(SendError::TooLarge {
                            max: usize::try_from(max).unwrap_or(usize::MAX),
                        }),
                        send_failure::Closed(()) => Err(SendError::Closed),
                        send_failure::Failed(error) => Err(SendError::Transport(
                            error
                                .and_then(|t| t.to_str().map_err(Into::into))
                                .map_err(rpc_send_error)?
                                .to_owned()
                                .into(),
                        )),
                    },
                }
            }
            .await;
            let _ = reply.send(result);
            drop(permit);
        }
        ChannelCommand::Recv(reply) => {
            let result = async {
                let response = remote
                    .recv_request()
                    .send()
                    .promise
                    .await
                    .map_err(rpc_recv_error)?;
                let result = response
                    .get()
                    .and_then(|r| r.get_result())
                    .map_err(rpc_recv_error)?;
                match result
                    .which()
                    .map_err(capnp::Error::from)
                    .map_err(rpc_recv_error)?
                {
                    rpc_result::Ok(message) => Ok(message.map_err(rpc_recv_error)?.to_vec()),
                    rpc_result::Err(error) => match error
                        .map_err(rpc_recv_error)?
                        .which()
                        .map_err(capnp::Error::from)
                        .map_err(rpc_recv_error)?
                    {
                        recv_failure::Closed(()) => Err(RecvError::Closed),
                        recv_failure::Failed(error) => Err(RecvError::Transport(
                            error
                                .and_then(|t| t.to_str().map_err(Into::into))
                                .map_err(rpc_recv_error)?
                                .to_owned()
                                .into(),
                        )),
                    },
                }
            }
            .await;
            let _ = reply.send(result);
        }
    }
}
fn rpc_send_error(error: capnp::Error) -> SendError {
    if error.kind == capnp::ErrorKind::Disconnected {
        SendError::Closed
    } else {
        SendError::Transport(error.into())
    }
}
fn rpc_recv_error(error: capnp::Error) -> RecvError {
    if error.kind == capnp::ErrorKind::Disconnected {
        RecvError::Closed
    } else {
        RecvError::Transport(error.into())
    }
}
type QueuedSend = (Vec<u8>, Reply<Result<(), SendError>>);
type Received = mpsc::Receiver<Result<Vec<u8>, RecvError>>;
struct ChannelServer {
    sends: mpsc::Sender<QueuedSend>,
    receives: Rc<Mutex<Received>>,
}
fn serve_channel<S, R, SE, RE>(backend: Channel<S, R>, map_send: SE, map_recv: RE) -> RemoteChannel
where
    S: SendChannel + 'static,
    R: RecvChannel + 'static,
    SE: Fn(S::SendError) -> SendError + 'static,
    RE: Fn(R::RecvError) -> RecvError + 'static,
{
    let (sends, mut send_commands) = mpsc::channel::<QueuedSend>(1);
    let (received, receives) = mpsc::channel(1);
    let lifetime = received.clone();
    tokio::task::spawn_local(async move {
        let (mut sender, mut receiver) = backend.into_split();
        let sending = async move {
            while let Some((message, reply)) = send_commands.recv().await {
                let result = sender.send(message).await.map_err(&map_send);
                let terminal = !matches!(result, Ok(()) | Err(SendError::TooLarge { .. }));
                let _ = reply.send(result);
                if terminal {
                    break;
                }
            }
        };
        let receiving = async move {
            loop {
                let result = receiver.recv().await.map_err(&map_recv);
                let terminal = result.is_err();
                if received.send(result).await.is_err() || terminal {
                    break;
                }
            }
        };
        // Dispose the whole backend on completion or abandonment to preserve
        // framing when a send is interrupted.
        tokio::select! {
            _ = sending => {},
            _ = receiving => {},
            _ = lifetime.closed() => {},
        }
    });
    capnp_rpc::new_client(ChannelServer {
        sends,
        receives: Rc::new(Mutex::new(receives)),
    })
}
impl channel::Server<data::Owned, send_failure::Owned, recv_failure::Owned> for ChannelServer {
    fn send(
        &mut self,
        params: channel::SendParams<data::Owned, send_failure::Owned, recv_failure::Owned>,
        mut results: channel::SendResults<data::Owned, send_failure::Owned, recv_failure::Owned>,
    ) -> Promise<(), capnp::Error> {
        let message = capnp_rpc::pry!(capnp_rpc::pry!(params.get()).get_message()).to_vec();
        let sends = self.sends.clone();
        Promise::from_future(async move {
            let (reply, result) = oneshot::channel();
            let result = if sends.send((message, reply)).await.is_err() {
                Err(SendError::Closed)
            } else {
                result.await.unwrap_or(Err(SendError::Closed))
            };
            let output = results.get().init_result();
            match result {
                Ok(()) => {
                    output.init_ok();
                }
                Err(SendError::TooLarge { max }) => output.init_err().set_too_large(max as u64),
                Err(SendError::Closed) => output.init_err().set_closed(()),
                Err(error) => output.init_err().set_failed(error.to_string().as_str()),
            }
            Ok(())
        })
    }
    fn recv(
        &mut self,
        _: channel::RecvParams<data::Owned, send_failure::Owned, recv_failure::Owned>,
        mut results: channel::RecvResults<data::Owned, send_failure::Owned, recv_failure::Owned>,
    ) -> Promise<(), capnp::Error> {
        let receives = self.receives.clone();
        Promise::from_future(async move {
            let result = receives
                .lock()
                .await
                .recv()
                .await
                .unwrap_or(Err(RecvError::Closed));
            let mut output = results.get().init_result();
            match result {
                Ok(message) => output.set_ok(message.as_slice())?,
                Err(RecvError::Closed) => output.init_err().set_closed(()),
                Err(error) => output.init_err().set_failed(error.to_string().as_str()),
            }
            Ok(())
        })
    }
}
async fn run_server<Io>(io: Io, bootstrap: capnp::capability::Client) -> Result<(), capnp::Error>
where
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (reader, writer) = tokio::io::split(io);
    let network = twoparty::VatNetwork::new(
        reader.compat(),
        writer.compat_write(),
        Side::Server,
        Default::default(),
    );
    RpcSystem::new(Box::new(network), Some(bootstrap)).await
}
/// Serve an owned backend channel on the caller's `LocalSet`.
///
/// Error callbacks preserve backend-specific closure and size-limit semantics
/// when translating failures into the wire protocol.
/// Receiving stops after its first error; sends continue only after size rejection.
/// Returns the RPC connection's result, including capnp-rpc's normalization of
/// some disconnects to successful termination.
pub async fn serve<S, R, SE, RE, Io>(
    backend: Channel<S, R>,
    map_send: SE,
    map_recv: RE,
    io: Io,
) -> Result<(), capnp::Error>
where
    S: SendChannel + 'static,
    R: RecvChannel + 'static,
    SE: Fn(S::SendError) -> SendError + 'static,
    RE: Fn(R::RecvError) -> RecvError + 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
{
    run_server(io, serve_channel(backend, map_send, map_recv).client).await
}
#[cfg(test)]
mod tests {
    fn mem_send(_: fungi_transport_testkit::mem::MemError) -> SendError {
        SendError::Closed
    }
    fn mem_recv(_: fungi_transport_testkit::mem::MemError) -> RecvError {
        RecvError::Closed
    }

    use super::*;
    use fungi_transport_testkit::mem::{MemConfig, duplex};
    use std::time::Duration;

    struct BlockedRpcChannel {
        started: mpsc::UnboundedSender<Vec<u8>>,
        gate: Arc<tokio::sync::Semaphore>,
    }

    impl channel::Server<data::Owned, send_failure::Owned, recv_failure::Owned> for BlockedRpcChannel {
        fn send(
            &mut self,
            params: channel::SendParams<data::Owned, send_failure::Owned, recv_failure::Owned>,
            mut results: channel::SendResults<
                data::Owned,
                send_failure::Owned,
                recv_failure::Owned,
            >,
        ) -> Promise<(), capnp::Error> {
            self.started
                .send(params.get().unwrap().get_message().unwrap().to_vec())
                .unwrap();
            let gate = Arc::clone(&self.gate);
            Promise::from_future(async move {
                gate.acquire().await.unwrap().forget();
                results.get().init_result().init_ok();
                Ok(())
            })
        }

        fn recv(
            &mut self,
            _: channel::RecvParams<data::Owned, send_failure::Owned, recv_failure::Owned>,
            mut results: channel::RecvResults<
                data::Owned,
                send_failure::Owned,
                recv_failure::Owned,
            >,
        ) -> Promise<(), capnp::Error> {
            results
                .get()
                .init_result()
                .set_ok(b"independent".as_slice())
                .unwrap();
            Promise::ok(())
        }
    }

    #[tokio::test]
    async fn canceled_sends_stay_bounded_across_splitting_without_blocking_receive() {
        tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::LocalSet::new().run_until(async {
                let gate = Arc::new(tokio::sync::Semaphore::new(0));
                let (started, mut starts) = mpsc::unbounded_channel();
                let remote: RemoteChannel = capnp_rpc::new_client(BlockedRpcChannel {
                    started,
                    gate: Arc::clone(&gate),
                });
                let (client, io) = tokio::io::duplex(64);
                let server = tokio::task::spawn_local(run_server(io, remote.client));
                let mut channel = CapnpChannel::connect(client, 1024).unwrap();
                {
                    let sending = channel.send(b"first".to_vec());
                    tokio::pin!(sending);
                    tokio::select! {
                        result = &mut sending => panic!("send completed before release: {result:?}"),
                        message = starts.recv() => assert_eq!(message.unwrap(), b"first"),
                    }
                }
                let (mut sender, mut receiver) = channel.into_channel().into_split();
                for _ in 0..32 {
                    assert!(
                        tokio::time::timeout(Duration::from_millis(2), sender.send(b"canceled".to_vec()))
                            .await
                            .is_err()
                    );
                }
                assert!(matches!(starts.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
                assert_eq!(receiver.recv().await.unwrap(), b"independent");
                gate.add_permits(1);
                {
                    let sending = sender.send(b"after".to_vec());
                    tokio::pin!(sending);
                    tokio::select! {
                        result = &mut sending => panic!("send completed before release: {result:?}"),
                        message = starts.recv() => assert_eq!(message.unwrap(), b"after"),
                    }
                    gate.add_permits(1);
                    sending.await.unwrap();
                }
                assert!(matches!(starts.try_recv(), Err(mpsc::error::TryRecvError::Empty)));
                drop((sender, receiver));
                server.await.unwrap().unwrap();
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn runtime_initialization_errors_reach_the_connecting_caller() {
        let (io, _peer) = tokio::io::duplex(64);
        let (_link, lifetime) = new_link(1024);
        let (_commands, receiver) = mpsc::channel(2);
        let error = start_client(io, Bootstrap::Channel(receiver), lifetime, || {
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "runtime setup failed",
            ))
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(error.to_string(), "runtime setup failed");
    }

    #[tokio::test]
    async fn servers_return_protocol_errors_to_their_callers() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::LocalSet::new().run_until(async {
                let (backend, _peer) = duplex(MemConfig::default());
                let (mut peer, io) = tokio::io::duplex(64);
                peer.write_all(&[1, 2, 3, 4, 0, 0, 0, 0]).await.unwrap();
                peer.shutdown().await.unwrap();
                let (result, drained) =
                    futures_util::future::join(serve(backend, mem_send, mem_recv, io), async {
                        peer.read_to_end(&mut Vec::new()).await
                    })
                    .await;
                drained.unwrap();
                let error = result.unwrap_err();
                assert_eq!(error.kind, capnp::ErrorKind::Failed);
            }),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn stopped_actors_report_closed_channels() {
        let (commands, receiver) = mpsc::channel(1);
        drop(receiver);
        assert!(matches!(
            send_message(&commands, &Arc::new(Semaphore::new(1)), 1024, vec![1]).await,
            Err(SendError::Closed)
        ));
        let mut pending = None;
        assert!(matches!(
            receive_message(&commands, &mut pending).await,
            Err(RecvError::Closed)
        ));
        assert!(pending.is_none());
    }

    #[tokio::test]
    async fn canceled_receive_preserves_a_response_already_delivered_to_the_client() {
        tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::LocalSet::new().run_until(async {
                let (backend, mut peer) = duplex(MemConfig::default());
                let (client, io) = tokio::io::duplex(64);
                let server = tokio::task::spawn_local(serve(backend, mem_send, mem_recv, io));
                let mut channel = CapnpChannel::connect(client, 1024).unwrap();
                channel.send(b"native".to_vec()).await.unwrap();
                assert_eq!(peer.recv().await.unwrap(), b"native");
                assert!(
                    tokio::time::timeout(Duration::from_millis(5), channel.recv())
                        .await
                        .is_err()
                );
                peer.send(b"retained".to_vec()).await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while channel.pending.as_ref().unwrap().is_empty() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .unwrap();
                let (sender, mut receiver) = channel.into_channel().into_split();
                assert_eq!(receiver.recv().await.unwrap(), b"retained");
                drop(peer);
                assert!(
                    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
                        .await
                        .unwrap()
                        .is_err()
                );
                drop((sender, receiver));
                tokio::time::timeout(Duration::from_secs(5), server)
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            }),
        )
        .await
        .unwrap();
    }

    #[test]
    fn rpc_disconnects_and_protocol_failures_have_distinct_diagnostics() {
        assert!(matches!(
            rpc_send_error(capnp::Error::disconnected("gone".into())),
            SendError::Closed
        ));
        assert!(matches!(
            rpc_recv_error(capnp::Error::disconnected("gone".into())),
            RecvError::Closed
        ));
        let send = rpc_send_error(capnp::Error::failed("bad response".into()));
        let recv = rpc_recv_error(capnp::Error::failed("bad response".into()));
        for error in [&send as &dyn std::error::Error, &recv] {
            assert!(error.to_string().contains("bad response"));
            assert!(error.source().is_some());
        }
    }

    #[tokio::test]
    async fn channel_errors_are_translated_by_backend_callbacks() {
        tokio::time::timeout(
            Duration::from_secs(5),
            tokio::task::LocalSet::new().run_until(async {
                for sending in [true, false] {
                    let (backend, peer) = duplex(MemConfig::default());
                    let (peer_sender, peer_receiver) = peer.into_split();
                    let retained = if sending {
                        drop(peer_receiver);
                        (Some(peer_sender), None)
                    } else {
                        drop(peer_sender);
                        (None, Some(peer_receiver))
                    };
                    let (client, io) = tokio::io::duplex(64);
                    let server = tokio::task::spawn_local(serve(backend, mem_send, mem_recv, io));
                    let mut channel = CapnpChannel::connect(client, 1024).unwrap();
                    if sending {
                        assert!(matches!(
                            channel.send(vec![1]).await,
                            Err(SendError::Closed)
                        ));
                    } else {
                        assert!(matches!(channel.recv().await, Err(RecvError::Closed)));
                    }
                    drop((channel, retained));
                    server.await.unwrap().unwrap();
                }
            }),
        )
        .await
        .unwrap();
    }
}

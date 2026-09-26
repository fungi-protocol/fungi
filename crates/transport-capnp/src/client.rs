use crate::channel::{CapnpChannel, CapnpPeer};
use crate::error::{BuildError, RecvError, SendError};
use crate::protocol::{
    RemoteBuilder, RemoteChannel, build_failure, recv_failure, rpc_result, send_failure,
};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side, twoparty};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::io;
use std::sync::{Arc, Weak};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

pub(super) type Reply<T> = oneshot::Sender<T>;
pub(super) type PendingReceive = Option<oneshot::Receiver<Result<Vec<u8>, RecvError>>>;

pub(super) enum ChannelCommand {
    Send(Vec<u8>, Reply<Result<(), SendError>>, OwnedSemaphorePermit),
    Recv(Reply<Result<Vec<u8>, RecvError>>, usize),
}
pub(super) struct BuildCommand {
    pub(super) input: Vec<u8>,
    pub(super) max_recv_message_len: usize,
    pub(super) reply: Reply<Result<CapnpChannel, BuildError>>,
}
#[derive(Debug)]
pub(super) struct Link {
    pub(super) _lifetime: mpsc::Sender<()>,
    pub(super) max_message_len: usize,
}
pub(super) enum Bootstrap {
    Channel(mpsc::Receiver<ChannelCommand>),
    Builder(mpsc::Receiver<BuildCommand>, Weak<Link>),
}

pub(super) fn new_link(max_message_len: usize) -> (Arc<Link>, mpsc::Receiver<()>) {
    let (sender, receiver) = mpsc::channel(1);
    (
        Arc::new(Link {
            _lifetime: sender,
            max_message_len,
        }),
        receiver,
    )
}
pub(super) fn runtime() -> io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}
pub(super) fn start_client<Io>(
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
pub(super) async fn run_client<R, W>(
    reader: R,
    writer: W,
    boot: Bootstrap,
    mut lifetime: mpsc::Receiver<()>,
) where
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
        Bootstrap::Builder(receiver, link) => {
            let remote = rpc.bootstrap::<RemoteBuilder>(Side::Server);
            tokio::task::spawn_local(builder_actor(remote, receiver, link));
        }
    }
    tokio::select! { _ = rpc => {}, _ = lifetime.recv() => {} }
}
pub(super) async fn channel_actor(
    remote: RemoteChannel,
    mut commands: mpsc::Receiver<ChannelCommand>,
) {
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
        ChannelCommand::Recv(reply, max) => {
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
                    rpc_result::Ok(message) => {
                        let message = message.map_err(rpc_recv_error)?;
                        if message.len() > max {
                            Err(RecvError::TooLarge { max })
                        } else {
                            Ok(message.to_vec())
                        }
                    }
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
async fn builder_actor(
    remote: RemoteBuilder,
    mut commands: mpsc::Receiver<BuildCommand>,
    link: Weak<Link>,
) {
    while let Some(command) = commands.recv().await {
        if !command.reply.is_closed() {
            dispatch_build(remote.clone(), command, link.clone()).await;
        }
    }
}
async fn dispatch_build(remote: RemoteBuilder, command: BuildCommand, link: Weak<Link>) {
    let result = async {
        let mut request = remote.build_request();
        request
            .get()
            .set_input(command.input.as_slice())
            .map_err(rpc_build_error)?;
        let response = request.send().promise.await.map_err(rpc_build_error)?;
        let result = response
            .get()
            .and_then(|r| r.get_result())
            .map_err(rpc_build_error)?;
        match result
            .which()
            .map_err(capnp::Error::from)
            .map_err(rpc_build_error)?
        {
            rpc_result::Ok(remote) => {
                let remote = remote.map_err(rpc_build_error)?;
                let link = link.upgrade().ok_or(BuildError::Unreachable)?;
                let (commands, receiver) = mpsc::channel(2);
                tokio::task::spawn_local(channel_actor(remote, receiver));
                Ok(CapnpChannel {
                    commands,
                    send_slot: Arc::new(Semaphore::new(1)),
                    pending: None,
                    max_recv_message_len: command.max_recv_message_len,
                    link,
                    peer: CapnpPeer(Arc::new(())),
                })
            }
            rpc_result::Err(error) => match error
                .map_err(rpc_build_error)?
                .which()
                .map_err(capnp::Error::from)
                .map_err(rpc_build_error)?
            {
                build_failure::Unreachable(()) => Err(BuildError::Unreachable),
                build_failure::Failed(error) => Err(BuildError::Transport(
                    error
                        .and_then(|t| t.to_str().map_err(Into::into))
                        .map_err(rpc_build_error)?
                        .to_owned()
                        .into(),
                )),
            },
        }
    }
    .await;
    // Failed reply delivery drops the channel to release an abandoned build's actor.
    let _ = command.reply.send(result);
}
pub(super) fn rpc_send_error(error: capnp::Error) -> SendError {
    if error.kind == capnp::ErrorKind::Disconnected {
        SendError::Closed
    } else {
        SendError::Transport(error.into())
    }
}
pub(super) fn rpc_recv_error(error: capnp::Error) -> RecvError {
    if error.kind == capnp::ErrorKind::Disconnected {
        RecvError::Closed
    } else {
        RecvError::Transport(error.into())
    }
}
pub(super) fn rpc_build_error(error: capnp::Error) -> BuildError {
    if error.kind == capnp::ErrorKind::Disconnected {
        BuildError::Unreachable
    } else {
        BuildError::Transport(error.into())
    }
}

use crate::client::Reply;
use crate::error::{BuildError, RecvError, SendError};
use crate::protocol::{
    ChannelSchema, RemoteBuilder, RemoteChannel, build_failure, builder, channel, recv_failure,
    send_failure,
};
use capnp::{capability::Promise, data};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side, twoparty};
use fungi_transport::{ChannelBuilder, Duplex, RecvChannel, SendChannel};
use std::rc::Rc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

/// Serve a backend channel on the caller's `LocalSet`.
///
/// Error callbacks classify backend failures. A receive error or a send error
/// other than `TooLarge` closes both directions to avoid reusing interrupted I/O.
/// Returns the RPC result; capnp-rpc treats some disconnects as successful closure.
///
/// Error diagnostics returned by the callbacks are sent to the RPC client.
/// Callbacks must return diagnostics appropriate for that recipient. A callback
/// returning `RecvError::TooLarge` is still a terminal backend error, transmitted
/// as diagnostic text; only client-side size rejection preserves the channel.
pub async fn serve<S, R, SE, RE, Io>(
    backend: Duplex<S, R>,
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
/// Serve a builder on the caller's `LocalSet`, decoding backend input tokens.
///
/// Decoding precedes backend access. Error callbacks preserve failure categories
/// and diagnostic text. Created channels follow [`serve`]'s shutdown semantics.
/// Returns the RPC result; capnp-rpc treats some disconnects as successful closure.
///
/// Error diagnostics returned by the decoder and error callbacks are sent to
/// the RPC client. They must be appropriate for that recipient.
pub async fn serve_builder<B, D, S, R, SE, RE, BE, Io>(
    backend: B,
    decode: D,
    map_send: SE,
    map_recv: RE,
    map_build: BE,
    io: Io,
) -> Result<(), capnp::Error>
where
    B: ChannelBuilder<Channel = Duplex<S, R>> + 'static,
    S: SendChannel + 'static,
    R: RecvChannel + 'static,
    SE: Fn(S::SendError) -> SendError + 'static,
    RE: Fn(R::RecvError) -> RecvError + 'static,
    BE: Fn(B::BuildError) -> BuildError + 'static,
    D: Fn(Vec<u8>) -> Result<B::Input, BuildError> + 'static,
    Io: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let bootstrap: RemoteBuilder = capnp_rpc::new_client(BuilderServer {
        backend: Rc::new(Mutex::new(backend)),
        decode: Rc::new(decode),
        map_send: Rc::new(map_send),
        map_recv: Rc::new(map_recv),
        map_build: Rc::new(map_build),
    });
    run_server(io, bootstrap.client).await
}

type QueuedSend = (Vec<u8>, Reply<Result<(), SendError>>);
type Received = mpsc::Receiver<Result<Vec<u8>, RecvError>>;
struct ChannelServer {
    sends: mpsc::Sender<QueuedSend>,
    receives: Rc<Mutex<Received>>,
}
fn serve_channel<S, R, SE, RE>(backend: Duplex<S, R>, map_send: SE, map_recv: RE) -> RemoteChannel
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
        let (mut sender, mut receiver) = backend.into_parts();
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
struct BuilderServer<B, D, SE, RE, BE> {
    backend: Rc<Mutex<B>>,
    decode: Rc<D>,
    map_send: Rc<SE>,
    map_recv: Rc<RE>,
    map_build: Rc<BE>,
}
impl<B, D, S, R, SE, RE, BE> builder::Server<data::Owned, ChannelSchema, build_failure::Owned>
    for BuilderServer<B, D, SE, RE, BE>
where
    B: ChannelBuilder<Channel = Duplex<S, R>> + 'static,
    S: SendChannel + 'static,
    R: RecvChannel + 'static,
    SE: Fn(S::SendError) -> SendError + 'static,
    RE: Fn(R::RecvError) -> RecvError + 'static,
    BE: Fn(B::BuildError) -> BuildError + 'static,
    D: Fn(Vec<u8>) -> Result<B::Input, BuildError> + 'static,
{
    fn build(
        &mut self,
        params: builder::BuildParams<data::Owned, ChannelSchema, build_failure::Owned>,
        mut results: builder::BuildResults<data::Owned, ChannelSchema, build_failure::Owned>,
    ) -> Promise<(), capnp::Error> {
        let input = capnp_rpc::pry!(capnp_rpc::pry!(params.get()).get_input()).to_vec();
        let input = (self.decode)(input);
        let backend = self.backend.clone();
        let map_send = self.map_send.clone();
        let map_recv = self.map_recv.clone();
        let map_build = self.map_build.clone();
        Promise::from_future(async move {
            let result = match input {
                Ok(input) => backend
                    .lock()
                    .await
                    .build(&input)
                    .await
                    .map_err(|error| map_build(error)),
                Err(error) => Err(error),
            };
            let mut output = results.get().init_result();
            match result {
                Ok(channel) => {
                    let remote = serve_channel(
                        channel,
                        move |error| map_send(error),
                        move |error| map_recv(error),
                    );
                    return output.set_ok(remote);
                }
                Err(BuildError::Unreachable) => output.init_err().set_unreachable(()),
                Err(error) => output.init_err().set_failed(error.to_string().as_str()),
            }
            Ok(())
        })
    }
}

pub(super) async fn run_server<Io>(
    io: Io,
    bootstrap: capnp::capability::Client,
) -> Result<(), capnp::Error>
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

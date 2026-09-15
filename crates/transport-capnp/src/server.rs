use crate::client::Reply;
use crate::error::{RecvError, SendError};
use crate::protocol::{RemoteChannel, channel, recv_failure, send_failure};
use capnp::{capability::Promise, data};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side, twoparty};
use fungi_transport::{Duplex, RecvChannel, SendChannel};
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

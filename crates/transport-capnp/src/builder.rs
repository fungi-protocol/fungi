use crate::channel::{CapnpChannel, CapnpDuplex};
use crate::client::{Bootstrap, BuildCommand, Link, new_link, run_client, runtime, start_client};
use crate::error::BuildError;
use fungi_transport::{ChannelBuilder, Unspecified};
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};

/// Remote builder accepting opaque byte tokens interpreted by the backend.
///
/// One build RPC runs at a time, with one queued request. Cancellation retains
/// the active slot until completion or disconnection; abandoned queued requests
/// are discarded to avoid creating unused channels.
#[derive(Debug)]
pub struct CapnpBuilder {
    pub(super) commands: mpsc::Sender<BuildCommand>,
    pub(super) max_recv_message_len: usize,
    pub(super) _link: Arc<Link>,
}
impl CapnpBuilder {
    /// Connect to a builder bootstrap over an owned RPC stream.
    ///
    /// `max_message_len` sets the maximum outgoing payload size in bytes
    /// for channels created by this builder.
    pub fn connect<Io>(io: Io, max_message_len: usize) -> io::Result<Self>
    where
        Io: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (link, lifetime) = new_link(max_message_len);
        let (commands, receiver) = mpsc::channel(1);
        let bootstrap = Bootstrap::Builder(receiver, Arc::downgrade(&link));
        start_client(io, bootstrap, lifetime, runtime)?;
        Ok(Self {
            commands,
            _link: link,
            max_recv_message_len: usize::MAX,
        })
    }

    /// Set the incoming payload limit for subsequently requested channels.
    ///
    /// Existing channels retain their limits. See
    /// [`CapnpChannel::set_max_recv_message_len`] for receive semantics.
    pub fn set_max_recv_message_len(&mut self, max: usize) {
        self.max_recv_message_len = max;
    }

    /// Spawn a builder server process speaking RPC on stdin/stdout.
    ///
    /// Dropping the last handle schedules termination and reaping. Startup errors
    /// are returned before a handle is exposed. `max_message_len` limits outgoing
    /// payloads on created channels.
    pub fn spawn(mut command: tokio::process::Command, max_message_len: usize) -> io::Result<Self> {
        let (link, lifetime) = new_link(max_message_len);
        let (commands, receiver) = mpsc::channel(1);
        let boot = Bootstrap::Builder(receiver, Arc::downgrade(&link));
        let (ready, started) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("capnp-client".into())
            .spawn(move || {
                let mut setup = || -> io::Result<_> {
                    let runtime = runtime()?;
                    let entered = runtime.enter();
                    let child = command
                        .stdin(std::process::Stdio::piped())
                        .stdout(std::process::Stdio::piped())
                        .kill_on_drop(true)
                        .spawn();
                    drop(entered);
                    Ok((runtime, child?))
                };
                match setup() {
                    Ok((runtime, mut child)) => {
                        let reader = child.stdout.take().unwrap();
                        let writer = child.stdin.take().unwrap();
                        let _ = ready.send(Ok(()));
                        let local = tokio::task::LocalSet::new();
                        local.block_on(&runtime, async move {
                            run_client(reader, writer, boot, lifetime).await;
                            reap_child(&mut child).await;
                        });
                    }
                    Err(error) => {
                        let _ = ready.send(Err(error));
                    }
                }
            })?;
        started.recv().map_err(io::Error::other)??;
        Ok(Self {
            commands,
            _link: link,
            max_recv_message_len: usize::MAX,
        })
    }

    /// Treat this remote builder as an inbound builder with unit input.
    pub fn into_acceptor(self) -> CapnpAcceptor {
        CapnpAcceptor(self)
    }
}
impl ChannelBuilder for CapnpBuilder {
    type Privacy = Unspecified;
    type Input = Vec<u8>;
    type Channel = CapnpDuplex;
    type BuildError = BuildError;
    async fn build(&mut self, input: &Vec<u8>) -> Result<CapnpDuplex, BuildError> {
        let (reply, result) = oneshot::channel();
        self.commands
            .send(BuildCommand {
                input: input.clone(),
                max_recv_message_len: self.max_recv_message_len,
                reply,
            })
            .await
            .map_err(|_| BuildError::Unreachable)?;
        result
            .await
            .map_err(|_| BuildError::Unreachable)?
            .map(CapnpChannel::into_channel)
    }
}
/// Inbound remote builder. Sends an empty token for each acceptance request.
#[derive(Debug)]
pub struct CapnpAcceptor(CapnpBuilder);
impl ChannelBuilder for CapnpAcceptor {
    type Privacy = Unspecified;
    type Input = ();
    type Channel = CapnpDuplex;
    type BuildError = BuildError;
    async fn build(&mut self, _: &()) -> Result<CapnpDuplex, BuildError> {
        self.0.build(&Vec::new()).await
    }
}

pub(super) async fn reap_child(child: &mut tokio::process::Child) {
    if tokio::time::timeout(std::time::Duration::from_millis(100), child.wait())
        .await
        .is_err()
    {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

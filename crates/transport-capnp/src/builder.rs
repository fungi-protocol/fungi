use crate::channel::{CapnpChannel, CapnpDuplex};
use crate::client::{Bootstrap, BuildCommand, Link, new_link, runtime, start_client};
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

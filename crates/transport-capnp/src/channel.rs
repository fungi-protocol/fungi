use crate::client::{
    Bootstrap, ChannelCommand, Link, PendingReceive, new_link, runtime, start_client,
};
use crate::error::{RecvError, SendError};
use fungi_transport::{Channel, Duplex, PeerChannel, RecvChannel, SendChannel, Unspecified};
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Semaphore, mpsc, oneshot};

/// Identity of one remote capability within this process.
#[derive(Debug, Clone)]
pub struct CapnpPeer(pub(super) Arc<()>);

impl PartialEq for CapnpPeer {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for CapnpPeer {}

/// Owned independent RPC directions for one remote capability.
pub type CapnpDuplex = Duplex<CapnpSendHalf, CapnpRecvHalf>;

/// A remote byte channel that retains received responses across cancellation.
///
/// One send RPC may be outstanding. Canceling its caller leaves delivery unknown
/// and retains the slot until completion or disconnection. Reception progresses
/// independently. Privacy is `Unspecified` because it depends on the backend.
#[derive(Debug)]
pub struct CapnpChannel {
    pub(super) commands: mpsc::Sender<ChannelCommand>,
    pub(super) send_slot: Arc<Semaphore>,
    pub(super) pending: PendingReceive,
    pub(super) max_recv_message_len: usize,
    pub(super) link: Arc<Link>,
    pub(super) peer: CapnpPeer,
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
            max_recv_message_len: usize::MAX,
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
    max_recv_message_len: usize,
    _link: Arc<Link>,
    peer: CapnpPeer,
}

impl CapnpChannel {
    /// Limit payloads from newly started receive requests, in bytes.
    ///
    /// Defaults to no additional payload limit. Oversized messages are consumed
    /// without copying the payload and return [`RecvError::TooLarge`]. Pending
    /// requests retain their original limit. The RPC reader's frame limit is
    /// unchanged; this check occurs after the frame has been received.
    pub fn set_max_recv_message_len(&mut self, max: usize) {
        self.max_recv_message_len = max;
    }

    /// Separate the directions while preserving pending reception and link ownership.
    ///
    /// Privacy remains `Unspecified`:
    ///
    /// ```compile_fail
    /// use fungi_transport::{Anonymous, SendChannel};
    /// use fungi_transport_capnp::CapnpDuplex;
    /// fn anonymous<C: SendChannel<Privacy = Anonymous>>(_: C) {}
    /// fn submit(channel: CapnpDuplex) { anonymous(channel); }
    /// ```
    pub fn into_channel(self) -> CapnpDuplex {
        Duplex::new(
            CapnpSendHalf {
                commands: self.commands.clone(),
                send_slot: self.send_slot,
                link: Arc::clone(&self.link),
                peer: self.peer.clone(),
            },
            CapnpRecvHalf {
                commands: self.commands,
                pending: self.pending,
                max_recv_message_len: self.max_recv_message_len,
                _link: self.link,
                peer: self.peer,
            },
        )
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

impl Channel for CapnpChannel {}

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
        receive_message(&self.commands, &mut self.pending, self.max_recv_message_len).await
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
        receive_message(&self.commands, &mut self.pending, self.max_recv_message_len).await
    }
}
pub(super) async fn send_message(
    commands: &mpsc::Sender<ChannelCommand>,
    send_slot: &Arc<Semaphore>,
    max: usize,
    message: Vec<u8>,
) -> Result<(), SendError> {
    if message.len() > max {
        return Err(SendError::TooLarge { max });
    }
    // Command ownership keeps outstanding sends bounded across caller cancellation.
    let permit = Arc::clone(send_slot)
        .acquire_owned()
        .await
        .expect("send slot semaphore is never closed");
    let (reply, result) = oneshot::channel();
    commands
        .send(ChannelCommand::Send(message, reply, permit))
        .await
        .map_err(|_| SendError::Closed)?;
    result.await.map_err(|_| SendError::Closed)?
}

pub(super) async fn receive_message(
    commands: &mpsc::Sender<ChannelCommand>,
    pending: &mut PendingReceive,
    max: usize,
) -> Result<Vec<u8>, RecvError> {
    if pending.is_none() {
        let (reply, result) = oneshot::channel();
        commands
            .send(ChannelCommand::Recv(reply, max))
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

use crate::client::{
    Bootstrap, ChannelCommand, Link, PendingReceive, new_link, runtime, start_client,
};
use crate::error::{RecvError, SendError};
use fungi_transport::{Channel, RecvChannel, SendChannel, Unspecified};
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Semaphore, mpsc, oneshot};

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
    pub(super) link: Arc<Link>,
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
        })
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
        receive_message(&self.commands, &mut self.pending).await
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

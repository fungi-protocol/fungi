//! Adapter for splitting a shared channel backend.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::{Channel, PeerChannel, RecvChannel, SendChannel};

/// A channel supporting independent operations through shared access.
///
/// Internal synchronization must allow sending to progress while receiving is
/// pending, and receiving to progress while sending is pending. This does not
/// guarantee progress when the network or peer is unresponsive.
pub trait SharedChannel<I = Vec<u8>, O = Vec<u8>>: Channel<I, O> + Sync {
    /// Submit one message through shared access.
    fn send_shared(&self, message: O) -> impl Future<Output = Result<(), Self::SendError>> + Send;

    /// Receive one message through shared access.
    fn recv_shared(&self) -> impl Future<Output = Result<I, Self::RecvError>> + Send;
}

/// Sending capability backed by a shared channel.
#[derive(Debug)]
pub struct SharedSendHalf<C, I, O> {
    channel: Arc<C>,
    messages: PhantomData<fn(I) -> O>,
}

/// Receiving capability backed by a shared channel.
#[derive(Debug)]
pub struct SharedRecvHalf<C, I, O> {
    channel: Arc<C>,
    messages: PhantomData<fn(I) -> O>,
}

/// Split a shared channel into independently owned capabilities.
pub fn split<I, O, C: SharedChannel<I, O>>(
    channel: C,
) -> (SharedSendHalf<C, I, O>, SharedRecvHalf<C, I, O>) {
    let channel = Arc::new(channel);
    (
        SharedSendHalf {
            channel: Arc::clone(&channel),
            messages: PhantomData,
        },
        SharedRecvHalf {
            channel,
            messages: PhantomData,
        },
    )
}

impl<I, O, C: SharedChannel<I, O>> SendChannel<O> for SharedSendHalf<C, I, O> {
    type Privacy = C::Privacy;
    type SendError = C::SendError;

    fn send(&mut self, message: O) -> impl Future<Output = Result<(), Self::SendError>> + Send {
        self.channel.send_shared(message)
    }
}

impl<I, O, C: SharedChannel<I, O>> RecvChannel<I> for SharedRecvHalf<C, I, O> {
    type RecvError = C::RecvError;

    fn recv(&mut self) -> impl Future<Output = Result<I, Self::RecvError>> + Send {
        self.channel.recv_shared()
    }
}

impl<C: PeerChannel, I, O> PeerChannel for SharedSendHalf<C, I, O> {
    type Peer = C::Peer;

    fn peer(&self) -> &Self::Peer {
        self.channel.peer()
    }
}

impl<C: PeerChannel, I, O> PeerChannel for SharedRecvHalf<C, I, O> {
    type Peer = C::Peer;

    fn peer(&self) -> &Self::Peer {
        self.channel.peer()
    }
}

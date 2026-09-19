//! Type-directed channel transformations.

use std::marker::PhantomData;

use crate::{PeerChannel, RecvChannel, SendChannel};

/// A sending channel that transforms messages before submission.
#[derive(Debug)]
pub struct Contramap<C, F, W> {
    channel: C,
    transform: F,
    message: PhantomData<fn() -> W>,
}

impl<C, F, W> Contramap<C, F, W> {
    /// Transform messages accepted by a sending channel.
    pub fn new(channel: C, transform: F) -> Self {
        Self {
            channel,
            transform,
            message: PhantomData,
        }
    }
}

impl<T: Send, W, C: SendChannel<W>, F: FnMut(T) -> W + Send> SendChannel<T> for Contramap<C, F, W> {
    type Privacy = C::Privacy;
    type SendError = C::SendError;

    async fn send(&mut self, message: T) -> Result<(), Self::SendError> {
        let message = (self.transform)(message);
        self.channel.send(message).await
    }
}

impl<C: PeerChannel, F, W> PeerChannel for Contramap<C, F, W> {
    type Peer = C::Peer;

    fn peer(&self) -> &Self::Peer {
        self.channel.peer()
    }
}

/// A receiving channel that transforms messages after reception.
#[derive(Debug)]
pub struct Map<C, F, W> {
    channel: C,
    transform: F,
    message: PhantomData<fn() -> W>,
}

impl<C, F, W> Map<C, F, W> {
    /// Transform messages produced by a receiving channel.
    pub fn new(channel: C, transform: F) -> Self {
        Self {
            channel,
            transform,
            message: PhantomData,
        }
    }
}

impl<T, W, C: RecvChannel<W>, F: FnMut(W) -> T + Send> RecvChannel<T> for Map<C, F, W> {
    type RecvError = C::RecvError;

    async fn recv(&mut self) -> Result<T, Self::RecvError> {
        let message = self.channel.recv().await?;
        Ok((self.transform)(message))
    }
}

impl<C: PeerChannel, F, W> PeerChannel for Map<C, F, W> {
    type Peer = C::Peer;

    fn peer(&self) -> &Self::Peer {
        self.channel.peer()
    }
}

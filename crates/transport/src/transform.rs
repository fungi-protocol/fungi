//! Type-directed channel transformations.

use std::future::Future;
use std::marker::PhantomData;

use crate::{RecvChannel, SendChannel};

/// A sending channel that applies a function to each message before sending it.
#[derive(Debug)]
pub struct MapBeforeSend<C, F, W> {
    channel: C,
    transform: F,
    message: PhantomData<fn() -> W>,
}

impl<C, F, W> MapBeforeSend<C, F, W> {
    /// Transform messages accepted by a sending channel.
    pub fn new(channel: C, transform: F) -> Self {
        Self {
            channel,
            transform,
            message: PhantomData,
        }
    }
}

impl<T, W, C: SendChannel<W>, F: FnMut(T) -> W + Send> SendChannel<T> for MapBeforeSend<C, F, W> {
    type Privacy = C::Privacy;
    type SendError = C::SendError;

    fn send(&mut self, message: T) -> impl Future<Output = Result<(), Self::SendError>> + Send {
        let message = (self.transform)(message);
        self.channel.send(message)
    }
}

/// A receiving channel that applies a function to each message after receiving it.
#[derive(Debug)]
pub struct MapAfterRecv<C, F, W> {
    channel: C,
    transform: F,
    message: PhantomData<fn() -> W>,
}

impl<C, F, W> MapAfterRecv<C, F, W> {
    /// Transform messages produced by a receiving channel.
    pub fn new(channel: C, transform: F) -> Self {
        Self {
            channel,
            transform,
            message: PhantomData,
        }
    }
}

impl<T, W, C: RecvChannel<W>, F: FnMut(W) -> T + Send> RecvChannel<T> for MapAfterRecv<C, F, W> {
    type RecvError = C::RecvError;

    async fn recv(&mut self) -> Result<T, Self::RecvError> {
        let message = self.channel.recv().await?;
        Ok((self.transform)(message))
    }
}

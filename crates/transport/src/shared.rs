//! Adapter for splitting a shared channel backend.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::{RecvChannel, SendChannel, SharedChannel};

/// Sending capability backed by a shared channel.
pub struct SendHalf<C, I, O> {
    channel: Arc<C>,
    // `O` is sent and `I` is received; neither is stored.
    messages: PhantomData<fn(O) -> I>,
}

/// Receiving capability backed by a shared channel.
pub struct RecvHalf<C, I, O> {
    channel: Arc<C>,
    // `O` is sent and `I` is received; neither is stored.
    messages: PhantomData<fn(O) -> I>,
}

/// Split a shared channel into independently owned capabilities.
pub fn split<C, I, O>(channel: C) -> (SendHalf<C, I, O>, RecvHalf<C, I, O>)
where
    C: SharedChannel<I, O>,
{
    let channel = Arc::new(channel);
    (
        SendHalf {
            channel: Arc::clone(&channel),
            messages: PhantomData,
        },
        RecvHalf {
            channel,
            messages: PhantomData,
        },
    )
}

impl<C, I, O> SendChannel<O> for SendHalf<C, I, O>
where
    C: SharedChannel<I, O>,
{
    type Privacy = C::Privacy;
    type SendError = C::SendError;

    fn send(&mut self, message: O) -> impl Future<Output = Result<(), Self::SendError>> + Send {
        self.channel.send_shared(message)
    }
}

impl<C, I, O> RecvChannel<I> for RecvHalf<C, I, O>
where
    C: SharedChannel<I, O>,
{
    type RecvError = C::RecvError;

    fn recv(&mut self) -> impl Future<Output = Result<I, Self::RecvError>> + Send {
        self.channel.recv_shared()
    }
}

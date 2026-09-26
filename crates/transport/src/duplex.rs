//! Adapter for combining independent channel directions.

use crate::{Channel, RecvChannel, SendChannel};

/// A channel composed from independent sending and receiving capabilities.
///
/// The capabilities need not refer to the same peer; this adapter does not
/// validate peer identity.
#[derive(Debug)]
pub struct Duplex<S, R> {
    sender: S,
    receiver: R,
}

impl<S, R> Duplex<S, R> {
    /// Combine sending and receiving capabilities.
    pub fn new(sender: S, receiver: R) -> Self {
        Self { sender, receiver }
    }

    /// Recover the sending and receiving capabilities.
    pub fn into_parts(self) -> (S, R) {
        (self.sender, self.receiver)
    }
}

impl<M, S: SendChannel<M>, R: Send> SendChannel<M> for Duplex<S, R> {
    type Privacy = S::Privacy;
    type SendError = S::SendError;

    fn send(&mut self, message: M) -> impl Future<Output = Result<(), Self::SendError>> + Send {
        self.sender.send(message)
    }
}

impl<M, S: Send, R: RecvChannel<M>> RecvChannel<M> for Duplex<S, R> {
    type RecvError = R::RecvError;

    fn recv(&mut self) -> impl Future<Output = Result<M, Self::RecvError>> + Send {
        self.receiver.recv()
    }
}

impl<I, O, S, R> Channel<I, O> for Duplex<S, R>
where
    S: SendChannel<O>,
    R: RecvChannel<I>,
{
}

//! Traits for message-oriented transport capabilities.

use std::error::Error;
use std::future::Future;

use crate::Privacy;

/// Send individual messages.
pub trait SendChannel<M = Vec<u8>>: Send {
    /// Privacy properties for this sender in relation to its destination peer.
    type Privacy: Privacy;

    /// Failure to send a message.
    type SendError: Error + Send + Sync + 'static;

    /// Submit one message.
    fn send(&mut self, message: M) -> impl Future<Output = Result<(), Self::SendError>> + Send;
}

/// Receive individual messages.
pub trait RecvChannel<M = Vec<u8>>: Send {
    /// Failure to receive a message.
    type RecvError: Error + Send + Sync + 'static;
    /// Receive one message.
    fn recv(&mut self) -> impl Future<Output = Result<M, Self::RecvError>> + Send;
}

/// A channel that can receive and send messages.
pub trait Bidirectional<I = Vec<u8>, O = Vec<u8>>: RecvChannel<I> + SendChannel<O> {}

/// A channel supporting independent operations through shared access.
///
/// Internal synchronization must allow sending to progress while receiving is
/// pending, and receiving to progress while sending is pending. This does not
/// guarantee progress when the network or peer is unresponsive.
pub trait SharedChannel<I = Vec<u8>, O = Vec<u8>>: Bidirectional<I, O> + Sync {
    /// Submit one message through shared access.
    fn send_shared(&self, message: O) -> impl Future<Output = Result<(), Self::SendError>> + Send;

    /// Receive one message through shared access.
    fn recv_shared(&self) -> impl Future<Output = Result<I, Self::RecvError>> + Send;
}

/// Build a channel with a sending capability and matching privacy tag.
pub trait ChannelBuilder<M = Vec<u8>>: Send {
    /// Construction input.
    type Input: Send + Sync;
    /// Privacy properties established by the builder.
    type Privacy: Privacy;
    /// Produced capability.
    type Channel: SendChannel<M, Privacy = Self::Privacy>;
    /// Failure to build the capability.
    type BuildError: Error + Send + Sync + 'static;
    /// Construct the next capability.
    fn build(
        &mut self,
        input: &Self::Input,
    ) -> impl Future<Output = Result<Self::Channel, Self::BuildError>> + Send;
}

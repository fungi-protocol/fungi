//! Message-oriented transport capabilities for the Fungi protocol.
//!
//! Exclusive access permits backends with mutable state to implement sending
//! and receiving.
//! [`Channel`] owns independent directions, separated by consuming [`split`].
//! [`Channel`] combines native simplex capabilities.
//! [`Channel::from_shared`] wraps an optional [`SharedChannel`] backend and supplies
//! owned splitting through `Arc`. [`CodecChannel`] adapts each available direction.
//! [`SendChannel::Privacy`] describes transport disclosure to the destination,
//! including session authentication. Application code controls payload disclosure.
//!
//! Operations requiring anonymous submission can express that requirement:
//!
//! ```
//! use fungi_transport::{Anonymous, SendChannel};
//!
//! async fn submit<C: SendChannel<Vec<u8>, Privacy = Anonymous>>(
//!     sender: &mut C,
//!     message: Vec<u8>,
//! ) -> Result<(), C::SendError> {
//!     sender.send(message).await
//! }
//! ```
//!
//! An anonymous requirement enforces a verified transport privacy guarantee:
//!
//! ```compile_fail
//! use std::convert::Infallible;
//! use fungi_transport::{Anonymous, SendChannel, Unspecified};
//!
//! struct Authenticated;
//! impl SendChannel for Authenticated {
//!     type Privacy = Unspecified;
//!     type SendError = Infallible;
//!     async fn send(&mut self, _: Vec<u8>) -> Result<(), Infallible> { Ok(()) }
//! }
//! fn anonymous<C: SendChannel<Privacy = Anonymous>>(_: &C) {}
//! anonymous(&Authenticated);
//! ```

mod channel;
mod codec;
mod privacy;

pub use channel::{
    Channel, ChannelBuilder, ConvertedChannel, PeerChannel, PeerMismatch, RecvChannel, RecvView,
    SendChannel, SendView, SharedChannel, SharedRecvHalf, SharedSendHalf, into_stream, split,
};
pub use codec::{Codec, CodecChannel, CodecError, CodecPair, Decode, Encode};
pub use privacy::{Anonymous, Privacy, Unspecified};

#[cfg(test)]
mod tests;

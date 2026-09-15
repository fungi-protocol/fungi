//! Message-oriented transport abstractions for the Fungi protocol.
//!
//! A [`Channel<M>`](Channel) exchanges messages with one peer, one message per
//! call. Byte transports implement `Channel<Vec<u8>>`; [`CodecChannel`] exposes
//! typed messages through the same contract. Peer identity, deduplication, and
//! ordering belong to other layers.

mod channel;
mod codec;
mod error;

pub use channel::{Channel, ChannelBuilder, RecvHalf, SendHalf, into_stream};
pub use codec::{Codec, CodecChannel, CodecError, CodecRecvHalf, CodecSendHalf};
pub use error::{BoxError, BuildError, RecvError, SendError};

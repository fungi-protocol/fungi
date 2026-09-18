//! Adapters for message type conversion.

use std::error::Error;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::{PeerChannel, RecvChannel, SendChannel, SharedChannel};

/// Synchronously convert an outgoing message into the underlying message type.
///
/// Use `std::convert::Infallible` for infallible conversions. Conversion
/// happens before submission; a conversion failure leaves the transport untouched.
pub trait Encode<M, W = Vec<u8>>: Send + Sync {
    /// Conversion failure.
    type EncodeError: Error + Send + Sync + 'static;
    /// Convert one owned message.
    fn encode(&self, message: M) -> Result<W, Self::EncodeError>;
}

/// Synchronously convert an underlying message into an incoming message.
///
/// Synchronous conversion after reception preserves cancel safety. Returned
/// messages must own their data. A conversion failure consumes that message;
/// transport validity remains governed by the backend contract.
pub trait Decode<W, M>: Send + Sync {
    /// Conversion failure.
    type DecodeError: Error + Send + Sync + 'static;
    /// Convert one owned message.
    fn decode(&self, message: W) -> Result<M, Self::DecodeError>;
}

/// Both conversions, implemented automatically when both capabilities exist.
pub trait Codec<M, W = Vec<u8>>: Encode<M, W> + Decode<W, M> {}

impl<M, W, K: Encode<M, W> + Decode<W, M>> Codec<M, W> for K {}

/// Independently implemented outgoing and incoming conversions bundled together.
#[derive(Debug)]
pub struct CodecPair<E, D> {
    encoder: E,
    decoder: D,
}

impl<E, D> CodecPair<E, D> {
    /// Bundle separate conversions so each implementation can serve one direction.
    pub fn new(encoder: E, decoder: D) -> Self {
        Self { encoder, decoder }
    }
}

impl<M, W, E: Encode<M, W>, D: Send + Sync> Encode<M, W> for CodecPair<E, D> {
    type EncodeError = E::EncodeError;
    fn encode(&self, message: M) -> Result<W, Self::EncodeError> {
        self.encoder.encode(message)
    }
}

impl<M, W, E: Send + Sync, D: Decode<W, M>> Decode<W, M> for CodecPair<E, D> {
    type DecodeError = D::DecodeError;
    fn decode(&self, message: W) -> Result<M, Self::DecodeError> {
        self.decoder.decode(message)
    }
}

/// Conversion failure or transport failure, preserving the original cause.
#[derive(Debug, thiserror::Error)]
pub enum CodecError<T, E> {
    /// The underlying transport failed.
    #[error("transport: {0}")]
    Transport(#[source] T),
    /// Message conversion failed.
    #[error("codec: {0}")]
    Codec(#[source] E),
}

/// A channel with independent message conversions for its available directions.
///
/// A send-only channel requires only [`Encode`]; a receive-only channel requires
/// only [`Decode`]. Both capabilities together form a duplex channel. `W` is
/// the underlying message type, allowing typed transports to retain their native data.
///
/// Use [`crate::Channel::with_codec`] to convert an owned duplex adapter while
/// retaining independent directions and sharing the immutable codec.
///
/// Sender privacy is inherited from the transport. Conversion must preserve
/// transport attribution and session configuration to uphold that guarantee.
/// Application code remains responsible for identifying payload contents.
///
/// Conversion preserves the transport's established sender privacy guarantee:
///
/// ```compile_fail
/// use fungi_transport::{Anonymous, CodecChannel, Encode, SendChannel};
/// fn anonymous<C: SendChannel<u64, Privacy = Anonymous>>(_: C) {}
/// fn elevate<C: SendChannel<u32>, K: Encode<u64, u32>>(channel: C, codec: K) {
///     let converted: CodecChannel<_, _, u32> = CodecChannel::new(channel, codec);
///     anonymous(converted);
/// }
/// ```
#[derive(Debug)]
pub struct CodecChannel<C, K, W = Vec<u8>> {
    channel: C,
    codec: K,
    message: PhantomData<fn() -> W>,
}

impl<C, K, W> CodecChannel<C, K, W> {
    /// Wrap a capability with its synchronous message conversion.
    pub fn new(channel: C, codec: K) -> Self {
        Self {
            channel,
            codec,
            message: PhantomData,
        }
    }
}

impl<M: Send, W: Send, C: SendChannel<W>, K: Encode<M, W>> SendChannel<M>
    for CodecChannel<C, K, W>
{
    type Privacy = C::Privacy;
    type SendError = CodecError<C::SendError, K::EncodeError>;

    async fn send(&mut self, message: M) -> Result<(), Self::SendError> {
        let message = self.codec.encode(message).map_err(CodecError::Codec)?;
        self.channel
            .send(message)
            .await
            .map_err(CodecError::Transport)
    }
}

impl<M, W, C: RecvChannel<W>, K: Decode<W, M>> RecvChannel<M> for CodecChannel<C, K, W> {
    type RecvError = CodecError<C::RecvError, K::DecodeError>;

    async fn recv(&mut self) -> Result<M, Self::RecvError> {
        let message = self.channel.recv().await.map_err(CodecError::Transport)?;
        self.codec.decode(message).map_err(CodecError::Codec)
    }
}

impl<C: PeerChannel, K, W> PeerChannel for CodecChannel<C, K, W> {
    type Peer = C::Peer;
    fn peer(&self) -> &Self::Peer {
        self.channel.peer()
    }
}

impl<M, W, K: Encode<M, W>> Encode<M, W> for Arc<K> {
    type EncodeError = K::EncodeError;
    fn encode(&self, message: M) -> Result<W, Self::EncodeError> {
        (**self).encode(message)
    }
}

impl<M, W, K: Decode<W, M>> Decode<W, M> for Arc<K> {
    type DecodeError = K::DecodeError;
    fn decode(&self, message: W) -> Result<M, Self::DecodeError> {
        (**self).decode(message)
    }
}

impl<M: Send, W: Send, C: SharedChannel<W>, K: Codec<M, W>> SharedChannel<M>
    for CodecChannel<C, K, W>
{
    type Privacy = C::Privacy;
    type SendError = CodecError<C::SendError, K::EncodeError>;
    type RecvError = CodecError<C::RecvError, K::DecodeError>;

    async fn send_shared(&self, message: M) -> Result<(), Self::SendError> {
        let message = self.codec.encode(message).map_err(CodecError::Codec)?;
        self.channel
            .send_shared(message)
            .await
            .map_err(CodecError::Transport)
    }

    async fn recv_shared(&self) -> Result<M, Self::RecvError> {
        let message = self
            .channel
            .recv_shared()
            .await
            .map_err(CodecError::Transport)?;
        self.codec.decode(message).map_err(CodecError::Codec)
    }
}

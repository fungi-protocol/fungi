//! A message-oriented channel connected to one peer.

use std::error::Error;
use std::future::Future;

use futures_core::Stream;

use crate::BuildError;

/// A channel exchanging messages with one peer.
///
/// Each call sends or receives exactly one message. Byte transports implement
/// `Channel<Vec<u8>>`; codec adapters can expose typed messages instead.
/// The contract does not provide ordering, deduplication, framing, or peer
/// identity.
///
/// `Ok(())` from [`send`](SendHalf::send) means that the transport accepted
/// the message, not that the peer received it. Dropping a pending send leaves
/// delivery unknown; callers must stop using that channel. A transport backed
/// by a byte stream must nevertheless keep its stream framing valid.
///
/// [`recv`](RecvHalf::recv) must be cancel-safe: dropping a pending receive
/// cannot consume a message. A silent path failure may leave it pending
/// forever, so callers own liveness timeouts.
///
/// Full-duplex consumers can borrow sending and receiving halves with
/// [`split`](Channel::split), preserving a single channel owner and a shared
/// lifetime. Each half permits one operation at a time through `&mut self`.
/// Implementations must allow both directions to make progress independently.
pub trait Channel<M = Vec<u8>>: SendHalf<M> + RecvHalf<M> {
    /// Borrowed sending half.
    type SendHalf<'a>: SendHalf<M, SendError = Self::SendError>
    where
        Self: 'a;

    /// Borrowed receiving half.
    type RecvHalf<'a>: RecvHalf<M, RecvError = Self::RecvError>
    where
        Self: 'a;

    /// Borrow both directions independently.
    fn split(&mut self) -> (Self::SendHalf<'_>, Self::RecvHalf<'_>);
}

/// The sending direction of a [`Channel`].
pub trait SendHalf<M = Vec<u8>>: Send {
    /// Failure to send a message.
    type SendError: Error + Send + Sync + 'static;
    /// Send one owned message under the [`Channel`] contract.
    fn send(&mut self, message: M) -> impl Future<Output = Result<(), Self::SendError>> + Send;
}

/// The receiving direction of a [`Channel`].
pub trait RecvHalf<M = Vec<u8>>: Send {
    /// Failure to receive a message. Every receive error is terminal.
    type RecvError: Error + Send + Sync + 'static;
    /// Receive one message under the [`Channel`] contract.
    fn recv(&mut self) -> impl Future<Output = Result<M, Self::RecvError>> + Send;
}

/// Builds a channel using implementation-specific input.
///
/// Construction may interact with the peer or only prepare local resources.
/// Success does not guarantee that the peer is online. Delivery and storage
/// behavior depend on the backend; a connection-backed channel may become
/// unusable when its underlying connection fails.
pub trait ChannelBuilder<M = Vec<u8>>: Send {
    /// Construction input, such as a peer address, or `()` for inbound acceptance.
    type Input: Send + Sync;

    /// Channel produced by this builder.
    type Channel: Channel<M>;

    /// Build the next channel using `input`.
    fn build(
        &mut self,
        input: &Self::Input,
    ) -> impl Future<Output = Result<Self::Channel, BuildError>> + Send;
}

/// Adapt a receiving direction into a stream of messages.
///
/// The adapter yields the first receive error and then terminates.
pub fn into_stream<M, C>(channel: C) -> impl Stream<Item = Result<M, C::RecvError>> + Send
where
    M: Send,
    C: RecvHalf<M>,
{
    futures_util::stream::unfold((channel, false), |(mut channel, done)| async move {
        if done {
            return None;
        }
        let item = channel.recv().await;
        let done = item.is_err();
        Some((item, (channel, done)))
    })
}

#[cfg(test)]
mod tests {

    use futures_util::StreamExt;

    use super::*;
    use crate::{RecvError, SendError};

    #[derive(Debug)]
    struct Loopback {
        sender: tokio::sync::mpsc::Sender<Vec<u8>>,
        receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    }

    impl Default for Loopback {
        fn default() -> Self {
            let (sender, receiver) = tokio::sync::mpsc::channel(4);
            Self { sender, receiver }
        }
    }

    impl SendHalf for &tokio::sync::mpsc::Sender<Vec<u8>> {
        type SendError = SendError;
        async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
            tokio::sync::mpsc::Sender::send(self, message)
                .await
                .map_err(|_| SendError::Closed)
        }
    }

    impl RecvHalf for &mut tokio::sync::mpsc::Receiver<Vec<u8>> {
        type RecvError = RecvError;
        async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
            self.try_recv().map_err(|_| RecvError::Closed)
        }
    }

    impl SendHalf for Loopback {
        type SendError = SendError;
        async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
            self.sender
                .send(message)
                .await
                .map_err(|_| SendError::Closed)
        }
    }

    impl RecvHalf for Loopback {
        type RecvError = RecvError;
        async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
            self.receiver.try_recv().map_err(|_| RecvError::Closed)
        }
    }

    impl Channel for Loopback {
        type SendHalf<'a> = &'a tokio::sync::mpsc::Sender<Vec<u8>>;
        type RecvHalf<'a> = &'a mut tokio::sync::mpsc::Receiver<Vec<u8>>;

        fn split(&mut self) -> (Self::SendHalf<'_>, Self::RecvHalf<'_>) {
            (&self.sender, &mut self.receiver)
        }
    }

    #[tokio::test]
    async fn borrowed_halves_roundtrip() {
        let mut channel = Loopback::default();
        let (mut sender, mut receiver) = channel.split();
        SendHalf::send(&mut sender, b"message".to_vec())
            .await
            .unwrap();
        assert_eq!(RecvHalf::recv(&mut receiver).await.unwrap(), b"message");
    }

    #[tokio::test]
    async fn channel_roundtrip() {
        let mut channel = Loopback::default();
        channel.send(b"message".to_vec()).await.unwrap();
        assert_eq!(channel.recv().await.unwrap(), b"message");
    }

    #[tokio::test]
    async fn stream_yields_the_first_error_then_ends() {
        let mut channel = Loopback::default();
        channel.send(b"message".to_vec()).await.unwrap();

        let items = into_stream(channel).collect::<Vec<_>>().await;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].as_ref().unwrap(), b"message");
        assert!(matches!(items[1], Err(RecvError::Closed)));
    }

    #[tokio::test]
    async fn stream_accepts_a_receive_only_half() {
        let mut channel = Loopback::default();
        channel.send(b"message".to_vec()).await.unwrap();
        let (_sender, receiver) = channel.split();

        let items = into_stream(receiver).collect::<Vec<_>>().await;
        assert_eq!(items.len(), 2);
        assert_eq!(items[0].as_ref().unwrap(), b"message");
        assert!(matches!(items[1], Err(RecvError::Closed)));
    }
}

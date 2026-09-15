//! Typed message adapters for byte channels.

use std::error::Error;
use std::fmt;

use crate::{Channel, RecvHalf, SendHalf};

/// Synchronous conversion between owned messages and transport bytes.
///
/// Decoding must finish without suspension so a cancel-safe byte receive stays
/// cancel-safe. Returned messages must own their data, not borrow the input.
pub trait Codec<M>: Send + Sync {
    /// Failure to encode a message, before any transport operation begins.
    type EncodeError: Error + Send + Sync + 'static;
    /// Failure to decode received bytes. This is a terminal receive error.
    type DecodeError: Error + Send + Sync + 'static;
    /// Encode one message.
    fn encode(&self, message: M) -> Result<Vec<u8>, Self::EncodeError>;
    /// Decode one message.
    fn decode(&self, bytes: Vec<u8>) -> Result<M, Self::DecodeError>;
}

/// A transport failure or a message conversion failure, preserving its cause.
#[derive(Debug)]
pub enum CodecError<T, E> {
    /// The underlying channel failed.
    Transport(T),
    /// Encoding or decoding failed.
    Codec(E),
}

impl<T: fmt::Display, E: fmt::Display> fmt::Display for CodecError<T, E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(f, "transport: {error}"),
            Self::Codec(error) => write!(f, "codec: {error}"),
        }
    }
}

impl<T: Error + 'static, E: Error + 'static> Error for CodecError<T, E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error),
            Self::Codec(error) => Some(error),
        }
    }
}

/// A byte channel exposed as a typed channel using a supplied codec.
///
/// Encoding errors leave the underlying channel untouched and are recoverable.
/// Every receive error, including decoding failures, is terminal. Callers must
/// discard the channel after a receive error or a canceled pending send.
#[derive(Debug)]
pub struct CodecChannel<C, K> {
    channel: C,
    codec: K,
}

impl<C, K> CodecChannel<C, K> {
    /// Wrap a channel with its message codec.
    pub fn new(channel: C, codec: K) -> Self {
        Self { channel, codec }
    }
}

/// Borrowed typed sending direction.
#[derive(Debug)]
pub struct CodecSendHalf<'a, S, K> {
    sender: S,
    codec: &'a K,
}

/// Borrowed typed receiving direction.
#[derive(Debug)]
pub struct CodecRecvHalf<'a, R, K> {
    receiver: R,
    codec: &'a K,
}

impl<M: Send, S: SendHalf<Vec<u8>>, K: Codec<M>> SendHalf<M> for CodecSendHalf<'_, S, K> {
    type SendError = CodecError<S::SendError, K::EncodeError>;

    async fn send(&mut self, message: M) -> Result<(), Self::SendError> {
        let bytes = self.codec.encode(message).map_err(CodecError::Codec)?;
        self.sender.send(bytes).await.map_err(CodecError::Transport)
    }
}

impl<M: Send, R: RecvHalf<Vec<u8>>, K: Codec<M>> RecvHalf<M> for CodecRecvHalf<'_, R, K> {
    type RecvError = CodecError<R::RecvError, K::DecodeError>;

    async fn recv(&mut self) -> Result<M, Self::RecvError> {
        let bytes = self.receiver.recv().await.map_err(CodecError::Transport)?;
        self.codec.decode(bytes).map_err(CodecError::Codec)
    }
}

impl<M: Send, C: Channel<Vec<u8>>, K: Codec<M>> SendHalf<M> for CodecChannel<C, K> {
    type SendError = CodecError<C::SendError, K::EncodeError>;
    async fn send(&mut self, message: M) -> Result<(), Self::SendError> {
        let bytes = self.codec.encode(message).map_err(CodecError::Codec)?;
        self.channel
            .send(bytes)
            .await
            .map_err(CodecError::Transport)
    }
}

impl<M: Send, C: Channel<Vec<u8>>, K: Codec<M>> RecvHalf<M> for CodecChannel<C, K> {
    type RecvError = CodecError<C::RecvError, K::DecodeError>;
    async fn recv(&mut self) -> Result<M, Self::RecvError> {
        let bytes = self.channel.recv().await.map_err(CodecError::Transport)?;
        self.codec.decode(bytes).map_err(CodecError::Codec)
    }
}

impl<M: Send, C: Channel<Vec<u8>>, K: Codec<M>> Channel<M> for CodecChannel<C, K> {
    type SendHalf<'a>
        = CodecSendHalf<'a, C::SendHalf<'a>, K>
    where
        Self: 'a;
    type RecvHalf<'a>
        = CodecRecvHalf<'a, C::RecvHalf<'a>, K>
    where
        Self: 'a;

    fn split(&mut self) -> (Self::SendHalf<'_>, Self::RecvHalf<'_>) {
        let (sender, receiver) = self.channel.split();
        (
            CodecSendHalf {
                sender,
                codec: &self.codec,
            },
            CodecRecvHalf {
                receiver,
                codec: &self.codec,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;
    use crate::{RecvError, SendError};

    #[derive(Debug, Default)]
    struct Sender {
        messages: Vec<Vec<u8>>,
        closed: bool,
    }

    impl SendHalf for &mut Sender {
        type SendError = SendError;

        async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
            if self.closed {
                return Err(SendError::Closed);
            }
            self.messages.push(message);
            Ok(())
        }
    }

    #[derive(Debug)]
    struct Receiver(Result<Vec<u8>, RecvError>);

    impl RecvHalf for &mut Receiver {
        type RecvError = RecvError;

        async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
            std::mem::replace(&mut self.0, Err(RecvError::Closed))
        }
    }

    #[derive(Debug)]
    struct StubChannel {
        sender: Sender,
        receiver: Receiver,
    }

    impl SendHalf for StubChannel {
        type SendError = SendError;

        async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
            (&mut self.sender).send(message).await
        }
    }

    impl RecvHalf for StubChannel {
        type RecvError = RecvError;

        async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
            (&mut self.receiver).recv().await
        }
    }

    impl Channel for StubChannel {
        type SendHalf<'a> = &'a mut Sender;
        type RecvHalf<'a> = &'a mut Receiver;

        fn split(&mut self) -> (Self::SendHalf<'_>, Self::RecvHalf<'_>) {
            (&mut self.sender, &mut self.receiver)
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct Message(u8);

    #[derive(Debug)]
    struct ByteCodec;

    impl Codec<Message> for ByteCodec {
        type EncodeError = io::Error;
        type DecodeError = io::Error;

        fn encode(&self, message: Message) -> Result<Vec<u8>, io::Error> {
            if message.0 == 255 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "reserved value",
                ));
            }
            Ok(vec![message.0])
        }

        fn decode(&self, bytes: Vec<u8>) -> Result<Message, io::Error> {
            match bytes.as_slice() {
                [value] => Ok(Message(*value)),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "expected one byte",
                )),
            }
        }
    }

    fn channel(
        bytes: Result<Vec<u8>, RecvError>,
        closed: bool,
    ) -> CodecChannel<StubChannel, ByteCodec> {
        CodecChannel::new(
            StubChannel {
                sender: Sender {
                    closed,
                    ..Sender::default()
                },
                receiver: Receiver(bytes),
            },
            ByteCodec,
        )
    }

    #[tokio::test]
    async fn owned_and_borrowed_sends_encode_and_preserve_failures() {
        for split in [false, true] {
            let mut channel = channel(Err(RecvError::Closed), false);
            let (encoding, success) = if split {
                let (mut sender, _) = channel.split();
                (
                    sender.send(Message(255)).await,
                    sender.send(Message(42)).await,
                )
            } else {
                (
                    channel.send(Message(255)).await,
                    channel.send(Message(42)).await,
                )
            };
            let error = encoding.unwrap_err();
            assert!(matches!(error, CodecError::Codec(_)));
            assert_eq!(error.to_string(), "codec: reserved value");
            assert_eq!(error.source().unwrap().to_string(), "reserved value");
            success.unwrap();
            assert_eq!(channel.channel.sender.messages, [vec![42]]);

            channel.channel.sender.closed = true;
            let error = if split {
                let (mut sender, _) = channel.split();
                sender.send(Message(7)).await.unwrap_err()
            } else {
                channel.send(Message(7)).await.unwrap_err()
            };
            assert!(matches!(error, CodecError::Transport(SendError::Closed)));
            assert_eq!(error.to_string(), "transport: channel closed");
            assert_eq!(error.source().unwrap().to_string(), "channel closed");
            assert_eq!(channel.channel.sender.messages, [vec![42]]);
        }
    }

    #[tokio::test]
    async fn owned_and_borrowed_receives_decode_and_preserve_failures() {
        for split in [false, true] {
            let mut channel = channel(Ok(vec![42]), false);
            let message = if split {
                let (_, mut receiver) = channel.split();
                receiver.recv().await.unwrap()
            } else {
                channel.recv().await.unwrap()
            };
            assert_eq!(message, Message(42));

            channel.channel.receiver.0 = Ok(vec![]);
            let error = if split {
                let (_, mut receiver) = channel.split();
                receiver.recv().await.unwrap_err()
            } else {
                channel.recv().await.unwrap_err()
            };
            assert!(matches!(error, CodecError::Codec(_)));
            assert_eq!(error.to_string(), "codec: expected one byte");
            assert_eq!(error.source().unwrap().to_string(), "expected one byte");

            let mut channel = self::channel(Err(RecvError::Closed), false);
            let error = if split {
                let (_, mut receiver) = channel.split();
                receiver.recv().await.unwrap_err()
            } else {
                channel.recv().await.unwrap_err()
            };
            assert!(matches!(error, CodecError::Transport(RecvError::Closed)));
            assert_eq!(error.to_string(), "transport: channel closed");
            assert_eq!(error.source().unwrap().to_string(), "channel closed");
        }
    }
}

//! Messages over a byte stream.
//!
//! Each message travels as a 4-byte big-endian length followed by that many
//! bytes.

use std::fmt;
use std::io::{self, Cursor};
use std::marker::PhantomData;
use std::mem;
use std::num::NonZeroUsize;

use fungi_transport::{Bidirectional, Privacy, RecvChannel, SendChannel, Unspecified};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// A message channel over a byte stream.
pub struct Channel<S, P = Unspecified> {
    stream: S,
    max_message_len: u32,
    incoming: Incoming,
    outgoing: Cursor<Vec<u8>>,
    failed: bool,
    privacy: PhantomData<fn() -> P>,
}

/// Failure to send a message.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    /// The message is above the limit. Nothing was written, and the channel
    /// remains usable.
    #[error("message exceeds {max} bytes")]
    TooLarge {
        /// Largest accepted length in bytes.
        max: u32,
    },
    /// The channel already ended, in either direction.
    #[error("channel closed")]
    Closed,
    /// The stream failed. The channel carries no further messages.
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Failure to receive a message. The channel yields no further messages.
#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    /// The stream ended between messages, or an earlier receive failed.
    #[error("channel closed")]
    Closed,
    /// The peer announced a message above the limit.
    #[error("peer announced {len} bytes, above the limit of {max}")]
    TooLarge {
        /// Announced length in bytes.
        len: u32,
        /// Largest accepted length in bytes.
        max: u32,
    },
    /// The stream ended inside a message.
    #[error("stream ended inside a message")]
    Truncated,
    /// The stream failed.
    #[error(transparent)]
    Io(#[from] io::Error),
}

impl<S> Channel<S> {
    /// Delimit messages of at most `max_message_len` bytes on `stream`.
    pub fn new(stream: S, max_message_len: u32) -> Self {
        Self {
            stream,
            max_message_len,
            incoming: Incoming::default(),
            outgoing: Cursor::default(),
            failed: false,
            privacy: PhantomData,
        }
    }
}

/// Leaves out message bytes, so a channel can be logged.
impl<S: fmt::Debug, P> fmt::Debug for Channel<S, P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Channel")
            .field("stream", &self.stream)
            .field("max_message_len", &self.max_message_len)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl<S: AsyncWrite + Send + Unpin, P: Privacy> SendChannel for Channel<S, P> {
    type Privacy = P;
    type SendError = SendError;

    /// A dropped send may still deliver its message, ahead of the next one.
    async fn send(&mut self, message: Vec<u8>) -> Result<(), SendError> {
        if self.failed {
            return Err(SendError::Closed);
        }

        let max = self.max_message_len;
        let len = u32::try_from(message.len())
            .ok()
            .filter(|len| *len <= max)
            .ok_or(SendError::TooLarge { max })?;

        let written = self.write(len, &message).await;
        self.failed = written.is_err();

        Ok(written?)
    }
}

impl<S: AsyncWrite + Unpin, P> Channel<S, P> {
    /// Write one message, after whatever a dropped send left unwritten.
    ///
    /// The cursor advances with every accepted byte and outlives the send
    /// future, so the stream never carries part of a message followed by the
    /// start of another.
    async fn write(&mut self, len: u32, payload: &[u8]) -> io::Result<()> {
        self.stream.write_all_buf(&mut self.outgoing).await?;

        self.outgoing = Cursor::new([&len.to_be_bytes(), payload].concat());
        self.stream.write_all_buf(&mut self.outgoing).await?;

        self.stream.flush().await
    }
}

impl<S: AsyncRead + Send + Unpin, P> RecvChannel for Channel<S, P> {
    type RecvError = RecvError;

    async fn recv(&mut self) -> Result<Vec<u8>, RecvError> {
        if self.failed {
            return Err(RecvError::Closed);
        }

        let message = self
            .incoming
            .read(&mut self.stream, self.max_message_len)
            .await;
        self.failed = message.is_err();

        message
    }
}

impl<S, P> Bidirectional for Channel<S, P>
where
    S: AsyncRead + AsyncWrite + Send + Unpin,
    P: Privacy,
{
}

/// The part of the next message already taken from the stream.
///
/// Kept outside the receive future, so dropping that future loses nothing.
#[derive(Default)]
struct Incoming {
    len: [u8; 4],
    len_filled: usize,
    payload: Vec<u8>,
}

impl Incoming {
    async fn read<S: AsyncRead + Unpin>(
        &mut self,
        stream: &mut S,
        max: u32,
    ) -> Result<Vec<u8>, RecvError> {
        // Reading no bytes is how a stream reports its end.
        while self.len_filled < self.len.len() {
            let ended = if self.len_filled == 0 {
                RecvError::Closed
            } else {
                RecvError::Truncated
            };
            let read = stream.read(&mut self.len[self.len_filled..]).await?;
            self.len_filled += NonZeroUsize::new(read).ok_or(ended)?.get();
        }

        let len = u32::from_be_bytes(self.len);
        if len > max {
            return Err(RecvError::TooLarge { len, max });
        }

        // The payload grows as bytes arrive, never ahead of them.
        let len = len as usize;
        while self.payload.len() < len {
            let missing = (len - self.payload.len()) as u64;
            let mut rest = (&mut *stream).take(missing);
            let read = rest.read_buf(&mut self.payload).await?;
            if read == 0 {
                return Err(RecvError::Truncated);
            }
        }

        self.len_filled = 0;

        Ok(mem::take(&mut self.payload))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

//! Messages over a byte stream.
//!
//! Each message travels as a 4-byte big-endian length followed by that many
//! bytes.

use std::fmt;
use std::io;
use std::mem;
use std::num::NonZeroUsize;

use fungi_transport::RecvChannel;
use tokio::io::{AsyncRead, AsyncReadExt};

/// A message channel over a byte stream.
pub struct Channel<S> {
    stream: S,
    max_message_len: u32,
    incoming: Incoming,
    failed: bool,
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
            failed: false,
        }
    }
}

/// Leaves out message bytes, so a channel can be logged.
impl<S: fmt::Debug> fmt::Debug for Channel<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Channel")
            .field("stream", &self.stream)
            .field("max_message_len", &self.max_message_len)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

impl<S: AsyncRead + Send + Unpin> RecvChannel for Channel<S> {
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

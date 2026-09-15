//! Errors exposed by message-oriented transports.

use std::error::Error;
use std::fmt;

/// An opaque backend error.
pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

/// Failure to send one message.
///
/// [`TooLarge`](SendError::TooLarge) is recoverable. Every other variant
/// means callers must replace the channel rather than use it again.
#[derive(Debug)]
#[non_exhaustive]
pub enum SendError {
    /// The message exceeds the maximum supported by this transport.
    TooLarge {
        /// Maximum supported message length in bytes.
        max: usize,
    },
    /// The channel is closed.
    Closed,
    /// A backend-specific failure occurred.
    Transport(BoxError),
}

/// Failure to receive the next message.
///
/// Every receive error makes the channel unusable. The variants are
/// diagnostic; callers recover by opening a new channel.
#[derive(Debug)]
#[non_exhaustive]
pub enum RecvError {
    /// The channel is closed.
    Closed,
    /// A backend-specific failure occurred.
    Transport(BoxError),
}

/// Failure to build or accept a channel.
#[derive(Debug)]
#[non_exhaustive]
pub enum BuildError {
    /// Construction required reaching a peer or endpoint that was unavailable.
    Unreachable,
    /// A backend-specific failure occurred.
    Transport(BoxError),
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { max } => write!(f, "message exceeds transport maximum of {max} bytes"),
            Self::Closed => f.write_str("channel closed"),
            Self::Transport(error) => write!(f, "transport error: {error}"),
        }
    }
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => f.write_str("channel closed"),
            Self::Transport(error) => write!(f, "transport error: {error}"),
        }
    }
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreachable => f.write_str("peer unreachable"),
            Self::Transport(error) => write!(f, "transport error: {error}"),
        }
    }
}

impl Error for SendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error.as_ref()),
            Self::TooLarge { .. } | Self::Closed => None,
        }
    }
}

impl Error for RecvError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error.as_ref()),
            Self::Closed => None,
        }
    }
}

impl Error for BuildError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transport(error) => Some(error.as_ref()),
            Self::Unreachable => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_actionable_messages() {
        assert_eq!(
            SendError::TooLarge { max: 1024 }.to_string(),
            "message exceeds transport maximum of 1024 bytes"
        );
        assert_eq!(SendError::Closed.to_string(), "channel closed");
        assert_eq!(RecvError::Closed.to_string(), "channel closed");
        assert_eq!(BuildError::Unreachable.to_string(), "peer unreachable");
        assert_eq!(
            RecvError::Transport("receive failed".into()).to_string(),
            "transport error: receive failed"
        );
        assert_eq!(
            BuildError::Transport("connect failed".into()).to_string(),
            "transport error: connect failed"
        );
    }

    #[test]
    fn preserves_backend_sources() {
        let send = SendError::Transport("send failed".into());
        let receive = RecvError::Transport("receive failed".into());
        let connect = BuildError::Transport("connect failed".into());
        assert_eq!(send.to_string(), "transport error: send failed");
        for error in [&send as &dyn Error, &receive, &connect] {
            assert!(error.source().is_some());
        }
    }

    #[test]
    fn semantic_errors_have_no_backend_source() {
        assert!(SendError::TooLarge { max: 1 }.source().is_none());
        assert!(SendError::Closed.source().is_none());
        assert!(RecvError::Closed.source().is_none());
        assert!(BuildError::Unreachable.source().is_none());
    }
}

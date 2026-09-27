/// A bridge or remote send failure.
#[derive(Debug, thiserror::Error)]
pub enum SendError {
    /// The bridge or backend rejected the payload length.
    #[error("message exceeds {max} bytes")]
    TooLarge {
        /// Maximum supported message length in bytes.
        max: usize,
    },
    /// The RPC channel is closed.
    #[error("RPC channel closed")]
    Closed,
    /// An RPC or backend failure, preserving its cause locally.
    #[error("transport: {0}")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync>),
}

/// A bridge or remote receive failure.
#[derive(Debug, thiserror::Error)]
pub enum RecvError {
    /// The RPC channel is closed.
    #[error("RPC channel closed")]
    Closed,
    /// An RPC or backend failure, preserving its cause locally.
    #[error("transport: {0}")]
    Transport(#[source] Box<dyn std::error::Error + Send + Sync>),
}

//! Message-oriented transport traits for the Fungi protocol.

mod privacy;
mod traits;

pub use privacy::{Anonymous, Privacy, Pseudonymous, Unspecified};
pub use traits::{Channel, ChannelBuilder, PeerChannel, RecvChannel, SendChannel};

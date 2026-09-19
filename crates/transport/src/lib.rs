//! Message-oriented transport capabilities for the Fungi protocol.

mod duplex;
mod privacy;
mod shared;
mod traits;
mod transform;

pub use duplex::Duplex;
pub use privacy::{Anonymous, Privacy, Pseudonymous, Unspecified};
pub use shared::{SharedChannel, SharedRecvHalf, SharedSendHalf, split};
pub use traits::{Channel, ChannelBuilder, PeerChannel, RecvChannel, SendChannel};
pub use transform::{Contramap, Map};

#[cfg(test)]
mod tests;

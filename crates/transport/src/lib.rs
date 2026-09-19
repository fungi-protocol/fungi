//! Message-oriented transport capabilities for the Fungi protocol.

mod duplex;
mod privacy;
mod traits;

pub use duplex::Duplex;
pub use privacy::{Anonymous, Privacy, Pseudonymous, Unspecified};
pub use traits::{Channel, ChannelBuilder, PeerChannel, RecvChannel, SendChannel};

#[cfg(test)]
mod tests;

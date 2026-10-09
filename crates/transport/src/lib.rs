//! Message-oriented transport capabilities for the Fungi protocol.

mod duplex;
mod privacy;
mod traits;

pub use duplex::Duplex;
pub use privacy::{ConnectionUnlinkability, MessageUnlinkability, Privacy, Unspecified};
pub use traits::{Bidirectional, ChannelBuilder, RecvChannel, SendChannel};

#[cfg(test)]
mod tests;

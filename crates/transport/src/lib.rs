//! Message-oriented transport capabilities for the Fungi protocol.

mod duplex;
mod privacy;
mod traits;
mod transform;

pub use duplex::Duplex;
pub use privacy::{ConnectionUnlinkability, MessageUnlinkability, Privacy, Unspecified};
pub use traits::{Bidirectional, ChannelBuilder, RecvChannel, SendChannel};
pub use transform::{MapAfterRecv, MapBeforeSend};

#[cfg(test)]
mod tests;

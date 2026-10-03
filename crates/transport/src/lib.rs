//! Message-oriented transport capabilities for the Fungi protocol.

mod bidirectional;
mod privacy;
mod shared;
mod traits;
mod transform;

pub use bidirectional::Bidirectional;
pub use privacy::{ConnectionUnlinkability, MessageUnlinkability, Privacy, Unspecified};
pub use shared::{SharedChannel, SharedRecvHalf, SharedSendHalf, split};
pub use traits::{Channel, ChannelBuilder, RecvChannel, SendChannel};
pub use transform::{MapAfterRecv, MapBeforeSend};

#[cfg(test)]
mod tests;

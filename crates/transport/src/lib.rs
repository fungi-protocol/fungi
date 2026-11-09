#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Message-oriented transport capabilities for the Fungi protocol.

mod duplex;
mod privacy;
pub mod reassembly;
pub mod shared;
pub mod tlv;
mod traits;
mod transform;

#[cfg(test)]
mod testing;

pub use duplex::Duplex;
pub use privacy::{ConnectionUnlinkability, MessageUnlinkability, Privacy, Unspecified};
pub use traits::{Bidirectional, ChannelBuilder, RecvChannel, SendChannel, SharedChannel};
pub use transform::{MapAfterRecv, MapBeforeSend};

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Message-oriented transport capabilities for the Fungi protocol.

mod duplex;
mod privacy;
pub mod shared;
mod traits;

pub use duplex::Duplex;
pub use privacy::{ConnectionUnlinkability, MessageUnlinkability, Privacy, Unspecified};
pub use traits::{Bidirectional, ChannelBuilder, RecvChannel, SendChannel, SharedChannel};

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

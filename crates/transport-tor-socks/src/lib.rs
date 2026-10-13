#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Message channels through an external tor daemon.

mod connector;
pub mod framed;
pub mod onion;
mod socks5;

pub use connector::{ConnectError, Connector};

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod testing;

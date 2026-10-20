#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Message channels through an external tor daemon.

mod connector;
mod control;
pub mod framed;
mod listener;
pub mod onion;
mod socks5;

pub use connector::{ConnectError, Connector};
pub use control::ControlAuth;
pub use listener::{ListenError, Listener};

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod testing;

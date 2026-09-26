//! Types for declaring sender privacy guarantees relative to the destination peer.

mod sealed {
    pub trait Sealed {}
}

/// A declared transport privacy guarantee.
///
/// Code that writes messages that must be anonymous should require a channel
/// tagged with [`Anonymous`].
pub trait Privacy: sealed::Sealed + Send + Sync {}

/// Messages cannot be linked to a sender identifier or to one another.
#[derive(Debug)]
pub enum Anonymous {}

/// Messages may be linked to one another, but not to a sender identifier.
#[derive(Debug)]
pub enum Pseudonymous {}

/// Sender anonymity remains unspecified.
///
/// This includes authenticated transports and configurations awaiting verification
/// of their privacy properties.
#[derive(Debug)]
pub enum Unspecified {}

impl sealed::Sealed for Anonymous {}
impl sealed::Sealed for Pseudonymous {}
impl sealed::Sealed for Unspecified {}
impl Privacy for Anonymous {}
impl Privacy for Pseudonymous {}
impl Privacy for Unspecified {}

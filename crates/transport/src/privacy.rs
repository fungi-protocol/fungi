//! Sender privacy guarantees relative to the destination peer.

mod sealed {
    pub trait Sealed {}
}

/// A transport privacy guarantee, independent of application message contents.
///
/// Guarantees cover connection establishment, authentication, and transport
/// metadata visible to the destination peer. Application disclosure and exposure
/// to other observers require separate privacy guarantees.
/// Backends must establish the guarantee for the entire session before exposing
/// a sender. Receiving and reconnecting must preserve it for the session.
pub trait Privacy: sealed::Sealed + Send + Sync {}

/// The transport preserves sender anonymity relative to its destination.
///
/// Messages within a session may remain linkable. Confidentiality, unlinkability,
/// and resistance to traffic analysis require separate guarantees.
#[derive(Debug)]
pub enum Anonymous {}

/// Sender anonymity remains unspecified.
///
/// This includes authenticated transports and configurations awaiting verification
/// of their privacy properties.
#[derive(Debug)]
pub enum Unspecified {}

impl sealed::Sealed for Anonymous {}
impl sealed::Sealed for Unspecified {}
impl Privacy for Anonymous {}
impl Privacy for Unspecified {}

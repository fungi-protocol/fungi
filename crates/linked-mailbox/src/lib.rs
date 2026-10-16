//! Linked mailboxes over BIP77 directories.
//!
//! [`client`] is a sans-I/O BIP77 directory client.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub mod client;
mod short_id;

pub use short_id::ShortId;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

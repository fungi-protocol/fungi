#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Simplex messages through a sequence of mailboxes derived from a shared
//! secret.
//!
//! Mailbox `i` of a [`Sequence`] is addressed by its [`MailboxId`] and holds
//! a fixed-size payload of [`PAYLOAD_BYTES`]: TLV records, padded and
//! encrypted with that mailbox's key. Each mailbox's ID and key are derived
//! separately from its own secret, so the ID reveals nothing about the key.
//! [`Sequences`] derives one [`Sequence`] for each direction between two
//! parties.
//!
//! A [`Writer`] and a [`Reader`] holding the same [`Sequence`] send and
//! receive messages through a [`Directory`]: message `i` is posted to
//! mailbox `i`.

mod directory;
mod linked;
mod payload;
mod secret;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod testing;

pub use directory::Directory;
pub use linked::{Error, MAX_MESSAGE_BYTES, MESSAGE_TYPE, Reader, Writer};
pub use payload::{
    MAX_RECORDS_BYTES, Mailbox, PADDING_TYPE, PAYLOAD_BYTES, RecordsTooLarge, Sequence,
};
pub use secret::{MailboxId, Sequences};

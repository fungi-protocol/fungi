#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Encrypted payloads for a sequence of mailboxes derived from a shared
//! secret.
//!
//! Mailbox `i` of a [`Sequence`] is addressed by its [`MailboxId`] and holds
//! a fixed-size payload of [`PAYLOAD_BYTES`]: TLV records, padded and
//! encrypted with that mailbox's key. Each mailbox's ID and key are derived
//! separately from its own secret, so the ID reveals nothing about the key.
//! [`Sequences`] derives one [`Sequence`] for each direction between two
//! parties.

mod payload;
mod secret;

pub use payload::{
    MAX_RECORDS_BYTES, Mailbox, PADDING_TYPE, PAYLOAD_BYTES, RecordsTooLarge, Sequence,
};
pub use secret::{MailboxId, Sequences};

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Encrypted payloads for a sequence of mailboxes derived from a shared
//! secret.
//!
//! Mailbox `i` of a [`Sequence`] is addressed by
//! [`derive_mailbox_id(secret, i)`](derive_mailbox_id) and holds a
//! fixed-size payload of [`PAYLOAD_BYTES`], encrypted with that mailbox's
//! key. Each mailbox's ID and key are derived separately from its own secret,
//! so the ID reveals nothing about the key. [`Sequences`] derives one such
//! secret for each direction between two parties.

mod payload;
mod secret;

pub use payload::{MAX_MESSAGE_BYTES, Mailbox, MessageTooLarge, PAYLOAD_BYTES, Sequence};
pub use secret::{MailboxId, Sequences, derive_mailbox_id};

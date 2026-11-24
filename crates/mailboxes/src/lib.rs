#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Simplex messages through a sequence of mailboxes derived from a shared
//! secret.
//!
//! Mailbox `i` of a [`Sequence`] is addressed by
//! [`derive_mailbox_id(secret, i)`](derive_mailbox_id) and holds a
//! fixed-size payload of [`PAYLOAD_BYTES`], encrypted with that mailbox's
//! key. Each mailbox's ID and key are derived separately from its own secret,
//! so the ID reveals nothing about the key. [`Sequences`] derives one such
//! secret for each direction between two parties.
//!
//! A [`Writer`] and a [`Reader`] holding the same sequence secret send and
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
pub use linked::{Error, Reader, Writer};
pub use payload::{MAX_MESSAGE_BYTES, Mailbox, MessageTooLarge, PAYLOAD_BYTES, Sequence};
pub use secret::{MailboxId, Sequences, derive_mailbox_id};

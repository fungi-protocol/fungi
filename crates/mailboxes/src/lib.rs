//! Simplex messages through a sequence of mailboxes derived from a shared
//! secret.
//!
//! A [`Writer`] and a [`Reader`] holding the same 32-byte secret send and
//! receive messages through a [`Directory`]. Message `i` is posted to mailbox
//! `i`, addressed by
//! [`derive_mailbox_secret(secret, i)`](derive_mailbox_secret), as a
//! fixed-size payload of [`PAYLOAD_BYTES`], encrypted with a key derived from
//! the same secret.

mod directory;
mod linked;
mod payload;
mod secret;

pub use directory::Directory;
pub use linked::{Error, Reader, Writer};
pub use payload::{MAX_MESSAGE_BYTES, PAYLOAD_BYTES};
pub use secret::{MailboxSecret, derive_mailbox_secret};

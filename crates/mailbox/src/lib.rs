//! Simplex linked mailboxes over a shared secret.
//!
//! A [`Writer`] and a [`Reader`] holding the same 32-byte secret exchange
//! messages through a [`MailboxStore`]. Message `i` is stored in slot `i`,
//! addressed by [`derive_slot_secret(secret, i)`](derive_slot_secret), as a
//! fixed-size payload of [`PAYLOAD_BYTES`], encrypted with a key derived from
//! the same secret.

mod linked;
mod payload;
mod slot;
mod store;

pub use linked::{Error, Reader, Writer};
pub use payload::{MAX_MESSAGE_BYTES, PAYLOAD_BYTES};
pub use slot::{SlotSecret, derive_slot_secret};
pub use store::MailboxStore;

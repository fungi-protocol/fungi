//! Simplex linked mailboxes over a shared secret.
//!
//! A [`Writer`] and a [`Reader`] holding the same 32-byte secret exchange
//! messages through a [`MailboxStore`]. Message `i` is stored at slot
//! [`derive_slot_id(secret, i)`](derive_slot_id) as a fixed-size payload of
//! [`PAYLOAD_BYTES`], encrypted to a key derived from the same secret.

mod linked;
mod payload;
mod slot;
mod store;

pub use linked::{Error, Reader, Writer};
pub use payload::{MAX_MESSAGE_BYTES, PAYLOAD_BYTES};
pub use slot::{SlotId, derive_slot_id};
pub use store::{MailboxStore, PutOutcome};

//! Deterministic, domain-separated per-slot shared secrets.

use hkdf::Hkdf;
use sha2::Sha256;

/// HKDF `info` prefix of every per-slot shared secret; the slot ID follows.
const SLOT_INFO: &[u8] = b"fungi/linked-mailbox/slot/v0";

/// Shared secret of one slot in a chain, which addresses the slot in a
/// [`MailboxStore`](crate::MailboxStore).
///
/// Whoever knows it can occupy the slot before the writer does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotSecret([u8; 32]);

impl SlotSecret {
    /// The 32-byte secret.
    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// HKDF-SHA256 with no salt: 32 bytes expanded from `shared_secret` with
/// the concatenation of `info` as the HKDF `info`.
pub(crate) fn hkdf_expand(shared_secret: &[u8; 32], info: &[&[u8]]) -> [u8; 32] {
    let mut output = [0u8; 32];
    Hkdf::<Sha256>::new(None, shared_secret)
        .expand_multi_info(info, &mut output)
        .expect("32 bytes is a valid HKDF-SHA256 output length");
    output
}

/// Derive the secret of slot `slot_id` in the chain keyed by
/// `shared_secret`.
///
/// `slot_secret(i) = HKDF-SHA256(salt = none, ikm = shared_secret,
/// info = SLOT_INFO || i)`, with `i` as 8 little-endian bytes.
pub fn derive_slot_secret(shared_secret: &[u8; 32], slot_id: u64) -> SlotSecret {
    SlotSecret(hkdf_expand(
        shared_secret,
        &[SLOT_INFO, &slot_id.to_le_bytes()],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_deterministic() {
        let secret = [7u8; 32];
        assert_eq!(
            derive_slot_secret(&secret, 3),
            derive_slot_secret(&secret, 3)
        );
    }

    #[test]
    fn differs_per_slot_id() {
        let secret = [1u8; 32];
        assert_ne!(
            derive_slot_secret(&secret, 0),
            derive_slot_secret(&secret, 1)
        );
    }

    #[test]
    fn differs_per_secret() {
        assert_ne!(
            derive_slot_secret(&[1u8; 32], 0),
            derive_slot_secret(&[2u8; 32], 0)
        );
    }
}

//! Deterministic, domain-separated slot identifiers.

use sha2::{Digest, Sha256};

/// Domain tag mixed into every derived id.
const DOMAIN_TAG: &[u8] = b"fungi/linked-mailbox/v0";

/// Identifier of one slot in a chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotId([u8; 32]);

impl SlotId {
    /// The 32-byte identifier.
    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// BIP340-style tagged hash: `SHA256(SHA256(tag) || SHA256(tag) || msg)`.
pub(crate) fn tagged_hash(tag: &[u8], msg: &[u8]) -> [u8; 32] {
    let tag_hash = Sha256::digest(tag);
    let mut hasher = Sha256::new();
    hasher.update(tag_hash);
    hasher.update(tag_hash);
    hasher.update(msg);
    hasher.finalize().into()
}

/// Derive slot `index` of the chain keyed by `shared_secret`.
///
/// `slot_id(i) = tagged_hash(DOMAIN_TAG, shared_secret || i)`.
pub fn derive_slot_id(shared_secret: &[u8; 32], index: u64) -> SlotId {
    let mut msg = [0u8; 32 + 8];
    msg[..32].copy_from_slice(shared_secret);
    msg[32..].copy_from_slice(&index.to_le_bytes());
    SlotId(tagged_hash(DOMAIN_TAG, &msg))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_deterministic() {
        let secret = [7u8; 32];
        assert_eq!(derive_slot_id(&secret, 3), derive_slot_id(&secret, 3));
    }

    #[test]
    fn differs_per_index() {
        let secret = [1u8; 32];
        assert_ne!(derive_slot_id(&secret, 0), derive_slot_id(&secret, 1));
    }

    #[test]
    fn differs_per_secret() {
        assert_ne!(derive_slot_id(&[1u8; 32], 0), derive_slot_id(&[2u8; 32], 0));
    }

    #[test]
    fn is_domain_separated_from_a_plain_hash() {
        let secret = [3u8; 32];
        let mut msg = Vec::new();
        msg.extend_from_slice(&secret);
        msg.extend_from_slice(&0u64.to_le_bytes());
        let plain: [u8; 32] = Sha256::digest(&msg).into();
        assert_ne!(derive_slot_id(&secret, 0).to_bytes(), plain);
    }
}

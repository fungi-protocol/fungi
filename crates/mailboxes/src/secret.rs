//! Deterministic, domain-separated per-mailbox shared secrets.

use hkdf::Hkdf;
use sha2::Sha256;

/// HKDF `info` prefix of every per-mailbox shared secret; the mailbox index
/// follows.
const MAILBOX_INFO: &[u8] = b"fungi/linked-mailbox/slot/v0";

/// Shared secret of one mailbox in a sequence, which addresses the mailbox in
/// a [`Directory`](crate::Directory).
///
/// Whoever knows it can occupy the mailbox before the writer does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MailboxSecret([u8; 32]);

impl MailboxSecret {
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

/// Derive the secret of mailbox `index` in the sequence keyed by
/// `shared_secret`.
///
/// `mailbox_secret(i) = HKDF-SHA256(salt = none, ikm = shared_secret,
/// info = MAILBOX_INFO || i)`, with `i` as 8 little-endian bytes.
pub fn derive_mailbox_secret(shared_secret: &[u8; 32], index: u64) -> MailboxSecret {
    MailboxSecret(hkdf_expand(
        shared_secret,
        &[MAILBOX_INFO, &index.to_le_bytes()],
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_deterministic() {
        let secret = [7u8; 32];
        assert_eq!(
            derive_mailbox_secret(&secret, 3),
            derive_mailbox_secret(&secret, 3)
        );
    }

    #[test]
    fn differs_per_index() {
        let secret = [1u8; 32];
        assert_ne!(
            derive_mailbox_secret(&secret, 0),
            derive_mailbox_secret(&secret, 1)
        );
    }

    #[test]
    fn differs_per_secret() {
        assert_ne!(
            derive_mailbox_secret(&[1u8; 32], 0),
            derive_mailbox_secret(&[2u8; 32], 0)
        );
    }
}

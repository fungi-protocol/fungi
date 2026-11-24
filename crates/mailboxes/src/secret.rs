//! Deterministic, domain-separated per-mailbox secrets, keys and IDs.

use fungi_aead::Key;
use hkdf::Hkdf;
use sha2::Sha256;
use zeroize::ZeroizeOnDrop;

/// HKDF `info` prefix of every per-mailbox shared secret; the mailbox index
/// follows.
const MAILBOX_INFO: &[u8] = b"fungi/linked-mailbox/mailbox/v0";

/// HKDF `info` of a mailbox's key and of its ID, each derived from the
/// mailbox secret, so the ID reveals nothing about the key.
const KEY_INFO: &[u8] = b"fungi/linked-mailbox/key/v0";
const ID_INFO: &[u8] = b"fungi/linked-mailbox/id/v0";

/// HKDF `info` of each direction's sequence secret.
const INITIATOR_TO_RESPONDER_INFO: &[u8] = b"fungi/linked-mailbox/initiator-to-responder/v0";
const RESPONDER_TO_INITIATOR_INFO: &[u8] = b"fungi/linked-mailbox/responder-to-initiator/v0";

/// Secrets of the two sequences between an initiator and a responder, one
/// for each direction, so the two parties never write to the same sequence.
/// Zeroized on drop.
#[derive(ZeroizeOnDrop)]
pub struct Sequences {
    /// Secret of the sequence the initiator writes and the responder reads.
    pub initiator_to_responder: [u8; 32],
    /// Secret of the sequence the responder writes and the initiator reads.
    pub responder_to_initiator: [u8; 32],
}

impl Sequences {
    /// Derive both sequence secrets from `shared_secret`, such as one
    /// exported by `fungi-hpke`.
    pub fn derive(shared_secret: &[u8; 32]) -> Self {
        Self {
            initiator_to_responder: hkdf_expand(shared_secret, &[INITIATOR_TO_RESPONDER_INFO]),
            responder_to_initiator: hkdf_expand(shared_secret, &[RESPONDER_TO_INITIATOR_INFO]),
        }
    }
}

/// Secret of one mailbox in a sequence. Zeroized on drop.
#[derive(ZeroizeOnDrop)]
pub(crate) struct MailboxSecret([u8; 32]);

impl MailboxSecret {
    /// Key that seals and opens the mailbox's payload.
    pub(crate) fn key(&self) -> Key {
        Key::from(hkdf_expand(&self.0, &[KEY_INFO]))
    }

    pub(crate) fn id(&self) -> MailboxId {
        MailboxId(hkdf_expand(&self.0, &[ID_INFO]))
    }
}

/// ID of one mailbox in a sequence, which addresses the mailbox in a
/// directory.
///
/// Whoever knows it can occupy the mailbox before the writer does.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct MailboxId([u8; 32]);

impl MailboxId {
    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl std::fmt::Debug for MailboxId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MailboxId").finish_non_exhaustive()
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
pub(crate) fn derive_mailbox_secret(shared_secret: &[u8; 32], index: u64) -> MailboxSecret {
    MailboxSecret(hkdf_expand(
        shared_secret,
        &[MAILBOX_INFO, &index.to_le_bytes()],
    ))
}

/// Derive the ID of mailbox `index` in the sequence keyed by `shared_secret`.
///
/// `id(i) = HKDF-SHA256(salt = none, ikm = mailbox_secret(i), info = ID_INFO)`.
pub fn derive_mailbox_id(shared_secret: &[u8; 32], index: u64) -> MailboxId {
    derive_mailbox_secret(shared_secret, index).id()
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use hpke::Kem as _;

    use super::*;
    use crate::Sequence;

    #[test]
    fn is_deterministic() {
        let secret = [7u8; 32];
        assert_eq!(derive_mailbox_id(&secret, 3), derive_mailbox_id(&secret, 3));
    }

    #[test]
    fn differs_per_index() {
        let secret = [1u8; 32];
        assert_ne!(derive_mailbox_id(&secret, 0), derive_mailbox_id(&secret, 1));
    }

    #[test]
    fn directions_differ() {
        let sequences = Sequences::derive(&[1u8; 32]);
        assert_ne!(
            sequences.initiator_to_responder,
            sequences.responder_to_initiator
        );
        assert_ne!(sequences.initiator_to_responder, [1u8; 32]);
    }

    #[test]
    fn id_is_not_the_secret() {
        let secret = derive_mailbox_secret(&[1u8; 32], 0);
        assert_ne!(secret.id().to_bytes(), secret.0);
    }

    #[test]
    fn differs_per_secret() {
        assert_ne!(
            derive_mailbox_id(&[1u8; 32], 0),
            derive_mailbox_id(&[2u8; 32], 0)
        );
    }

    #[test]
    fn debug_output_hides_the_mailbox_id() {
        assert_eq!(
            format!("{:?}", derive_mailbox_id(&[1; 32], 0)),
            "MailboxId { .. }"
        );
    }

    #[test]
    fn mailbox_id_bytes_match_the_hkdf_vector() {
        assert_eq!(
            derive_mailbox_id(&[1; 32], 0).to_bytes(),
            [
                0x73, 0xcb, 0xd9, 0x17, 0x7e, 0xa6, 0x06, 0x08, 0x40, 0x6c, 0x92, 0x06, 0xf9, 0xf5,
                0xb4, 0xa4, 0x31, 0xfa, 0x20, 0xc2, 0x91, 0x7e, 0x11, 0xc7, 0x92, 0xc9, 0xb2, 0x97,
                0xee, 0xd9, 0x44, 0xca,
            ]
        );
    }

    /// The two parties of a `fungi-hpke` handshake exchange payloads through
    /// the sequences derived from its exported shared secret.
    #[test]
    fn handshake_parties_exchange_payloads() {
        let (secret_key, public_key) = hpke::kem::XWing::derive_keypair(&[1; 32]);
        let mut first_message = b"hello".to_vec();
        let (encapsulated_key, tag, initiator_secret) =
            fungi_hpke::seal(&public_key, &mut first_message);
        let responder_secret =
            fungi_hpke::open(&secret_key, &encapsulated_key, &mut first_message, &tag).unwrap();

        let initiator = Sequences::derive(initiator_secret.as_bytes());
        let responder = Sequences::derive(responder_secret.as_bytes());

        let to_responder = Sequence::new(initiator.initiator_to_responder)
            .mailbox(0)
            .seal(b"to responder")
            .unwrap();
        let to_initiator = Sequence::new(responder.responder_to_initiator)
            .mailbox(0)
            .seal(b"to initiator")
            .unwrap();

        assert_eq!(
            Sequence::new(responder.initiator_to_responder)
                .mailbox(0)
                .open(&to_responder)
                .unwrap(),
            b"to responder"
        );
        assert_eq!(
            Sequence::new(initiator.responder_to_initiator)
                .mailbox(0)
                .open(&to_initiator)
                .unwrap(),
            b"to initiator"
        );
    }
}

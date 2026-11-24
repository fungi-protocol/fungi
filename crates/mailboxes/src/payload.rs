//! Fixed-size encrypted mailbox payloads.
//!
//! A BIP77 directory stores opaque payloads of a fixed size, so every
//! payload is [`PAYLOAD_BYTES`], the size of a payjoin v2 mailbox message.
//! A payload is a `fungi-aead` nonce, followed by the ciphertext of
//! zero-padded plaintext and the tag. Without the key, a directory cannot
//! tell payloads apart from each other or from random bytes.

use fungi_aead::{Key, Nonce, Tag};
use zeroize::ZeroizeOnDrop;

use crate::secret::{MailboxId, derive_mailbox_secret};

/// Size of every stored payload, equal to a Payjoin v2 mailbox message.
pub const PAYLOAD_BYTES: usize = 7168;

/// Largest message that fits in one payload.
pub const MAX_MESSAGE_BYTES: usize = PLAINTEXT_BYTES - LENGTH_BYTES;

const NONCE_BYTES: usize = size_of::<Nonce>();
const TAG_BYTES: usize = size_of::<Tag>();
const LENGTH_BYTES: usize = 2;
const PLAINTEXT_BYTES: usize = PAYLOAD_BYTES - NONCE_BYTES - TAG_BYTES;

/// Secret of one sequence, which each mailbox's key and ID are derived from.
/// Obtained from [`Sequences::derive`]. Zeroized on drop.
///
/// [`Sequences::derive`]: crate::Sequences::derive
#[derive(Clone, ZeroizeOnDrop)]
pub struct Sequence {
    secret: [u8; 32],
}

/// One mailbox of a sequence: its index, its ID, and the key that seals and
/// opens its payload.
pub struct Mailbox {
    index: u64,
    id: MailboxId,
    key: Key,
}

/// The message does not fit in one payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("message has {0} bytes, more than MAX_MESSAGE_BYTES")]
pub struct MessageTooLarge(pub usize);

impl Sequence {
    pub(crate) fn new(secret: [u8; 32]) -> Self {
        Self { secret }
    }

    /// Mailbox `index` of this sequence.
    pub fn mailbox(&self, index: u64) -> Mailbox {
        let secret = derive_mailbox_secret(&self.secret, index);
        Mailbox {
            index,
            id: secret.id(),
            key: secret.key(),
        }
    }
}

impl Mailbox {
    /// The ID that addresses this mailbox in a directory.
    pub fn id(&self) -> MailboxId {
        self.id
    }

    /// Encrypt a message of at most [`MAX_MESSAGE_BYTES`] for this mailbox.
    ///
    /// The mailbox index is the AAD, so a payload does not decrypt when
    /// served at another mailbox of the sequence.
    pub fn seal(&self, message: &[u8]) -> Result<Vec<u8>, MessageTooLarge> {
        if message.len() > MAX_MESSAGE_BYTES {
            return Err(MessageTooLarge(message.len()));
        }
        let length = u16::try_from(message.len()).expect("MAX_MESSAGE_BYTES fits in a u16");
        let mut payload = vec![0; PAYLOAD_BYTES];
        let (nonce, rest) = payload.split_at_mut(NONCE_BYTES);
        let (plaintext, tag) = rest.split_at_mut(PLAINTEXT_BYTES);
        plaintext[..LENGTH_BYTES].copy_from_slice(&length.to_le_bytes());
        plaintext[LENGTH_BYTES..][..message.len()].copy_from_slice(message);

        let (sealed_nonce, sealed_tag) =
            fungi_aead::seal(&self.key, &self.index.to_le_bytes(), plaintext);
        nonce.copy_from_slice(&sealed_nonce);
        tag.copy_from_slice(&sealed_tag);
        Ok(payload)
    }

    /// Decrypt a payload stored in this mailbox, or `None` if it is not
    /// [`PAYLOAD_BYTES`] long or does not decrypt for this mailbox.
    pub fn open(&self, payload: &[u8]) -> Option<Vec<u8>> {
        if payload.len() != PAYLOAD_BYTES {
            return None;
        }
        let (nonce, rest) = payload.split_at(NONCE_BYTES);
        let (ciphertext, tag) = rest.split_at(PLAINTEXT_BYTES);
        let nonce = Nonce::try_from(nonce).expect("split at NONCE_BYTES");
        let tag = Tag::try_from(tag).expect("the rest is TAG_BYTES");
        let mut plaintext = ciphertext.to_vec();
        fungi_aead::open(
            &self.key,
            &self.index.to_le_bytes(),
            &nonce,
            &mut plaintext,
            &tag,
        )
        .ok()?;

        let (length, rest) = plaintext.split_at(LENGTH_BYTES);
        let length = u16::from_le_bytes(length.try_into().ok()?);
        rest.get(..usize::from(length)).map(<[u8]>::to_vec)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    #[test]
    fn messages_up_to_the_limit_round_trip() {
        assert_eq!(MAX_MESSAGE_BYTES, 7126);
        let mailbox = Sequence::new([5; 32]).mailbox(0);
        for message in [Vec::new(), b"hello".to_vec(), vec![0xab; MAX_MESSAGE_BYTES]] {
            let payload = mailbox.seal(&message).unwrap();
            assert_eq!(mailbox.open(&payload).unwrap(), message);
        }
    }

    #[test]
    fn oversized_message_is_rejected() {
        let mailbox = Sequence::new([5; 32]).mailbox(0);
        assert_eq!(
            mailbox.seal(&vec![0; MAX_MESSAGE_BYTES + 1]),
            Err(MessageTooLarge(MAX_MESSAGE_BYTES + 1))
        );
    }

    #[test]
    fn payloads_are_fixed_size_and_hide_messages() {
        let mailbox = Sequence::new([5; 32]).mailbox(0);
        let message = b"a recognisable message";
        let first = mailbox.seal(message).unwrap();
        let second = mailbox.seal(message).unwrap();
        assert_ne!(first, second);
        for payload in [first, second] {
            assert_eq!(payload.len(), PAYLOAD_BYTES);
            assert!(!payload.windows(message.len()).any(|w| w == message));
        }
    }

    #[test]
    fn mailbox_id_is_derived_from_the_sequence_secret() {
        let secret = [6; 32];
        assert_eq!(
            Sequence::new(secret).mailbox(3).id(),
            derive_mailbox_secret(&secret, 3).id()
        );
    }

    #[test]
    fn payload_sealed_for_another_mailbox_is_rejected() {
        let sequence = Sequence::new([6; 32]);
        let payload = sequence.mailbox(0).seal(b"mailbox 0").unwrap();
        assert_eq!(sequence.mailbox(1).open(&payload), None);
        assert_eq!(Sequence::new([7; 32]).mailbox(0).open(&payload), None);
    }

    #[test]
    fn tampered_payload_is_rejected() {
        let mailbox = Sequence::new([6; 32]).mailbox(0);
        let mut payload = mailbox.seal(b"mailbox 0").unwrap();
        payload[NONCE_BYTES] ^= 1;
        assert_eq!(mailbox.open(&payload), None);
    }

    #[test]
    fn wrong_payload_sizes_are_rejected() {
        let mailbox = Sequence::new([6; 32]).mailbox(0);
        for size in [0, PAYLOAD_BYTES - 1, PAYLOAD_BYTES + 1] {
            assert_eq!(mailbox.open(&vec![0; size]), None, "{size}");
        }
    }
}

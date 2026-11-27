//! Fixed-size encrypted mailbox payloads.
//!
//! A BIP77 directory stores opaque payloads of a fixed size, so every
//! payload is [`PAYLOAD_BYTES`], the size of a payjoin v2 mailbox message.
//! A payload is a `fungi-aead` nonce, followed by the ciphertext of the
//! plaintext and the tag. The plaintext is a stream of TLV records, as in
//! [`fungi_transport::tlv`], filled to its end with [`PADDING_TYPE`]
//! records. Without the key, a directory cannot tell payloads apart from
//! each other or from random bytes.

use fungi_aead::{Key, Nonce, Tag};
use fungi_transport::tlv::{self, TypedSlice};
use zeroize::ZeroizeOnDrop;

use crate::secret::{MailboxId, derive_mailbox_secret};

/// Size of every stored payload, equal to a Payjoin v2 mailbox message.
pub const PAYLOAD_BYTES: usize = 7168;

/// Largest encoded TLV stream that fits in one payload, leaving room for the
/// smallest padding record.
pub const MAX_RECORDS_BYTES: usize = PLAINTEXT_BYTES - MIN_PADDING_BYTES;

/// TLV type of the records that pad a payload's plaintext. Odd, so a reader
/// may ignore it.
pub const PADDING_TYPE: u64 = 1;

const NONCE_BYTES: usize = size_of::<Nonce>();
const TAG_BYTES: usize = size_of::<Tag>();
const PLAINTEXT_BYTES: usize = PAYLOAD_BYTES - NONCE_BYTES - TAG_BYTES;
/// A padding record with an empty value: a 1-byte type and a 1-byte length.
const MIN_PADDING_BYTES: usize = 2;

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

/// The encoded records take more than [`MAX_RECORDS_BYTES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("records take {0} bytes, more than MAX_RECORDS_BYTES")]
pub struct RecordsTooLarge(pub usize);

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

    /// Encrypt `records` for this mailbox, followed by padding.
    ///
    /// The mailbox index is the AAD, so a payload does not decrypt when
    /// served at another mailbox of the sequence. Records of
    /// [`PADDING_TYPE`] are dropped by [`open`](Self::open).
    pub fn seal<T: AsRef<[u8]>>(
        &self,
        records: impl IntoIterator<Item = TypedSlice<T>>,
    ) -> Result<Vec<u8>, RecordsTooLarge> {
        let mut stream = Vec::new();
        tlv::encode(records, &mut stream).expect("writing to a Vec succeeds");
        if stream.len() > MAX_RECORDS_BYTES {
            return Err(RecordsTooLarge(stream.len()));
        }
        let mut payload = vec![0; PAYLOAD_BYTES];
        let (nonce, rest) = payload.split_at_mut(NONCE_BYTES);
        let (plaintext, tag) = rest.split_at_mut(PLAINTEXT_BYTES);
        let (records, padding) = plaintext.split_at_mut(stream.len());
        records.copy_from_slice(&stream);
        pad(padding);

        let (sealed_nonce, sealed_tag) =
            fungi_aead::seal(&self.key, &self.index.to_le_bytes(), plaintext);
        nonce.copy_from_slice(&sealed_nonce);
        tag.copy_from_slice(&sealed_tag);
        Ok(payload)
    }

    /// Decrypt a payload stored in this mailbox and return its records,
    /// without padding, or `None` if it is not [`PAYLOAD_BYTES`] long, does
    /// not decrypt for this mailbox, or is not a TLV stream.
    pub fn open(&self, payload: &[u8]) -> Option<Vec<TypedSlice<Vec<u8>>>> {
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

        tlv::Reader::new(&plaintext)
            .filter(|record| !matches!(record, Ok(r) if r.ty == PADDING_TYPE))
            .map(|record| {
                let TypedSlice { ty, value } = record.ok()?;
                Some(TypedSlice {
                    ty,
                    value: value.to_vec(),
                })
            })
            .collect()
    }
}

/// Fill `out`, at least [`MIN_PADDING_BYTES`] long, with [`PADDING_TYPE`]
/// records.
///
/// A record with a value of up to 252 bytes has a 2-byte header, and a
/// longer one a 4-byte header, so 255 or 256 bytes take two records.
fn pad(mut out: &mut [u8]) {
    while !out.is_empty() {
        let value_bytes = match out.len() {
            n @ ..=254 => n - 2,
            255 | 256 => 0,
            n => n - 4,
        };
        TypedSlice {
            ty: PADDING_TYPE,
            value: vec![0; value_bytes],
        }
        .encode(&mut out)
        .expect("padding fills the rest exactly");
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    fn record(ty: u64, value: &[u8]) -> TypedSlice<Vec<u8>> {
        TypedSlice {
            ty,
            value: value.to_vec(),
        }
    }

    /// A record whose encoding takes `bytes`, from 257 to 65539: a 4-byte
    /// header and the value.
    fn record_taking(bytes: usize) -> TypedSlice<Vec<u8>> {
        record(0, &vec![0xab; bytes - 4])
    }

    /// A payload whose plaintext starts with `start`, which need not be TLV.
    fn seal_plaintext(mailbox: &Mailbox, start: &[u8]) -> Vec<u8> {
        let mut plaintext = vec![0; PLAINTEXT_BYTES];
        plaintext[..start.len()].copy_from_slice(start);
        let (nonce, tag) =
            fungi_aead::seal(&mailbox.key, &mailbox.index.to_le_bytes(), &mut plaintext);
        [nonce.as_slice(), &plaintext, tag.as_slice()].concat()
    }

    #[test]
    fn records_round_trip() {
        let mailbox = Sequence::new([5; 32]).mailbox(0);
        let records = vec![record(0, b"hello"), record(7, b""), record(0, b"again")];
        let payload = mailbox.seal(records.clone()).unwrap();
        assert_eq!(mailbox.open(&payload).unwrap(), records);

        let payload = mailbox.seal(Vec::<TypedSlice<&[u8]>>::new()).unwrap();
        assert_eq!(mailbox.open(&payload).unwrap(), vec![]);
    }

    #[test]
    fn records_up_to_the_limit_round_trip() {
        assert_eq!(MAX_RECORDS_BYTES, 7126);
        let mailbox = Sequence::new([5; 32]).mailbox(0);
        for bytes in [MAX_RECORDS_BYTES - 1, MAX_RECORDS_BYTES] {
            let payload = mailbox.seal([record_taking(bytes)]).unwrap();
            assert_eq!(mailbox.open(&payload).unwrap(), [record_taking(bytes)]);
        }
    }

    #[test]
    fn records_over_the_limit_are_rejected() {
        let mailbox = Sequence::new([5; 32]).mailbox(0);
        let bytes = MAX_RECORDS_BYTES + 1;
        assert_eq!(
            mailbox.seal([record_taking(bytes)]),
            Err(RecordsTooLarge(bytes))
        );
    }

    #[test]
    fn padding_fills_every_size_from_the_minimum() {
        for bytes in MIN_PADDING_BYTES..=PLAINTEXT_BYTES {
            let mut out = vec![0xaa; bytes];
            pad(&mut out);
            assert!(
                tlv::Reader::new(&out).all(|r| r.unwrap().ty == PADDING_TYPE),
                "{bytes}"
            );
        }
    }

    #[test]
    fn plaintext_that_is_not_tlv_is_rejected() {
        let mailbox = Sequence::new([5; 32]).mailbox(0);
        let non_minimal_type = [0xfd, 0, 1].as_slice();
        let value_past_the_end = [0, 0xfe, 0, 1, 0, 0].as_slice();
        for start in [non_minimal_type, value_past_the_end] {
            assert_eq!(mailbox.open(&seal_plaintext(&mailbox, start)), None);
        }
    }

    #[test]
    fn payloads_are_fixed_size_and_hide_records() {
        let mailbox = Sequence::new([5; 32]).mailbox(0);
        let message = b"a recognisable message";
        let first = mailbox.seal([record(0, message)]).unwrap();
        let second = mailbox.seal([record(0, message)]).unwrap();
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
        let payload = sequence.mailbox(0).seal([record(0, b"mailbox 0")]).unwrap();
        assert_eq!(sequence.mailbox(1).open(&payload), None);
        assert_eq!(Sequence::new([7; 32]).mailbox(0).open(&payload), None);
    }

    #[test]
    fn tampered_payload_is_rejected() {
        let mailbox = Sequence::new([6; 32]).mailbox(0);
        let mut payload = mailbox.seal([record(0, b"mailbox 0")]).unwrap();
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

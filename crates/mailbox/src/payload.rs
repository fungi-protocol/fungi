//! Fixed-size encrypted slot payloads.
//!
//! A payload has the size and layout of a Payjoin v2 mailbox message: an
//! ElligatorSwift-encoded secp256k1 HPKE encapsulated key, followed by the
//! ChaCha20Poly1305 ciphertext of zero-padded plaintext. Without the key, a
//! directory cannot tell payloads apart from each other or from Payjoin v2
//! messages.

use hpke::aead::ChaCha20Poly1305;
use hpke::kdf::HkdfSha256;
use hpke::kem::SecpK256HkdfSha256;
use hpke::{Deserializable, Kem as _, OpModeR, OpModeS, Serializable};
use secp256k1::ellswift::ElligatorSwift;

use crate::slot::{SlotId, tagged_hash};

/// Size of every stored payload, equal to a Payjoin v2 mailbox message.
pub const PAYLOAD_BYTES: usize = 7168;

/// Largest message that fits in one payload.
pub const MAX_MESSAGE_BYTES: usize = PLAINTEXT_BYTES - LENGTH_BYTES;

const ENCAPSULATED_KEY_BYTES: usize = 64;
const TAG_BYTES: usize = 16;
const LENGTH_BYTES: usize = 2;
const PLAINTEXT_BYTES: usize = PAYLOAD_BYTES - ENCAPSULATED_KEY_BYTES - TAG_BYTES;

/// Domain tag for the HPKE key derived from a shared secret.
const KEY_TAG: &[u8] = b"fungi/linked-mailbox/key/v0";
/// HPKE `info` for every payload.
const INFO: &[u8] = b"fungi/linked-mailbox/payload/v0";

type Kem = SecpK256HkdfSha256;
type EncappedKey = <Kem as hpke::Kem>::EncappedKey;

/// HPKE keypair of one chain.
pub(crate) struct Keys {
    secret: <Kem as hpke::Kem>::PrivateKey,
    public: <Kem as hpke::Kem>::PublicKey,
}

impl Keys {
    /// Derive the keypair of the chain keyed by `shared_secret`.
    pub(crate) fn derive(shared_secret: &[u8; 32]) -> Self {
        let (secret, public) = Kem::derive_keypair(&tagged_hash(KEY_TAG, shared_secret));
        Self { secret, public }
    }

    /// Encrypt a message of at most [`MAX_MESSAGE_BYTES`] for `slot`.
    ///
    /// The slot is bound as associated data, so a payload does not decrypt
    /// when served at another slot.
    pub(crate) fn seal(&self, slot: SlotId, message: &[u8]) -> Vec<u8> {
        let length = u16::try_from(message.len())
            .ok()
            .filter(|_| message.len() <= MAX_MESSAGE_BYTES)
            .expect("callers check the message size");
        let mut plaintext = Vec::with_capacity(PLAINTEXT_BYTES);
        plaintext.extend_from_slice(&length.to_le_bytes());
        plaintext.extend_from_slice(message);
        plaintext.resize(PLAINTEXT_BYTES, 0);

        let (encapsulated_key, ciphertext) =
            hpke::single_shot_seal::<ChaCha20Poly1305, HkdfSha256, Kem, _>(
                &OpModeS::Base,
                &self.public,
                INFO,
                &plaintext,
                &slot.to_bytes(),
                &mut rand::rngs::OsRng,
            )
            .expect("sealing to a derived public key cannot fail");

        let mut payload = encode_key(&encapsulated_key).to_vec();
        payload.extend_from_slice(&ciphertext);
        debug_assert_eq!(payload.len(), PAYLOAD_BYTES);
        payload
    }

    /// Decrypt a payload stored at `slot`.
    pub(crate) fn open(&self, slot: SlotId, payload: &[u8]) -> Option<Vec<u8>> {
        if payload.len() != PAYLOAD_BYTES {
            return None;
        }
        let (key, ciphertext) = payload.split_at(ENCAPSULATED_KEY_BYTES);
        let encapsulated_key = decode_key(key.try_into().ok()?)?;
        let plaintext = hpke::single_shot_open::<ChaCha20Poly1305, HkdfSha256, Kem>(
            &OpModeR::Base,
            &self.secret,
            &encapsulated_key,
            INFO,
            ciphertext,
            &slot.to_bytes(),
        )
        .ok()?;

        let (length, rest) = plaintext.split_at(LENGTH_BYTES);
        let length = u16::from_le_bytes(length.try_into().ok()?);
        rest.get(..usize::from(length)).map(<[u8]>::to_vec)
    }
}

fn encode_key(key: &EncappedKey) -> [u8; ENCAPSULATED_KEY_BYTES] {
    let key = secp256k1::PublicKey::from_slice(&key.to_bytes())
        .expect("an HPKE encapsulated key is a valid secp256k1 point");
    ElligatorSwift::from_pubkey(key).to_array()
}

fn decode_key(encoded: [u8; ENCAPSULATED_KEY_BYTES]) -> Option<EncappedKey> {
    let key = secp256k1::PublicKey::from_ellswift(ElligatorSwift::from_array(encoded));
    EncappedKey::from_bytes(&key.serialize_uncompressed()).ok()
}

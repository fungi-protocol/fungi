//! Fixed-size payloads between a sender and a recipient: one HPKE payload
//! that gives both a key for each direction, then symmetric payloads under
//! those keys.
//!
//! Every payload is [`PAYLOAD_BYTES`], the size of a BIP77 mailbox message,
//! and encrypts a length-prefixed, zero-padded message.
//!
//! A payload from [`seal`] is an X-Wing encapsulated key followed by the HPKE
//! ChaCha20Poly1305 ciphertext. The keys are exported from the HPKE context.
//!
//! A payload from [`seal_symmetric`] is a random nonce followed by the
//! XChaCha20Poly1305 ciphertext.

use std::io::Write as _;

use chacha20poly1305::aead::{Aead as _, Generate as _, KeyInit as _, Payload};
use chacha20poly1305::{Key, XChaCha20Poly1305, XNonce};
use hpke::aead::ChaCha20Poly1305;
use hpke::hybrid_array::typenum::Unsigned as _;
use hpke::kdf::HkdfSha256;
use hpke::kem::XWing;
use hpke::{Deserializable, HpkeError, OpModeR, OpModeS, Serializable};

/// Size of a payload, as in BIP77's
/// [Uniform Payloads](https://github.com/bitcoin/bips/blob/master/bip-0077.md#uniform-payloads).
pub const PAYLOAD_BYTES: usize = 7168;

/// Largest message that fits in a payload from [`seal`].
pub const MAX_MESSAGE_BYTES: usize = HANDSHAKE_PLAINTEXT_BYTES - LENGTH_PREFIX_BYTES;

/// Largest message that fits in a payload from [`seal_symmetric`].
pub const MAX_SYMMETRIC_MESSAGE_BYTES: usize = SYMMETRIC_PLAINTEXT_BYTES - LENGTH_PREFIX_BYTES;

/// Size of each exported key.
pub const KEY_BYTES: usize = 32;

const ENCAPSULATED_KEY_BYTES: usize = <EncappedKey as Serializable>::OutputSize::USIZE;
const NONCE_BYTES: usize = 24;
const TAG_BYTES: usize = 16;
/// Size of the big-endian message length at the start of a plaintext.
const LENGTH_PREFIX_BYTES: usize = 2;
const HANDSHAKE_PLAINTEXT_BYTES: usize = PAYLOAD_BYTES - ENCAPSULATED_KEY_BYTES - TAG_BYTES;
const SYMMETRIC_PLAINTEXT_BYTES: usize = PAYLOAD_BYTES - NONCE_BYTES - TAG_BYTES;

/// HPKE info of the handshake. The value is arbitrary.
const INFO: &[u8] = b"fungi-hpke-info-v0";

/// HPKE exporter contexts of the keys. The values are arbitrary.
const SENDER_TO_RECIPIENT: &[u8] = b"fungi-hpke-sender-to-recipient-v0";
const RECIPIENT_TO_SENDER: &[u8] = b"fungi-hpke-recipient-to-sender-v0";

type Kem = XWing;
type EncappedKey = <Kem as hpke::Kem>::EncappedKey;
type SecretKey = <Kem as hpke::Kem>::PrivateKey;
type PublicKey = <Kem as hpke::Kem>::PublicKey;

/// One party's keys, one for each direction.
pub struct Keys {
    /// Key of the payloads this party sends.
    pub sending: [u8; KEY_BYTES],
    /// Key of the payloads this party receives.
    pub receiving: [u8; KEY_BYTES],
}

/// Failure to seal or open a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The message is longer than [`MAX_MESSAGE_BYTES`] or
    /// [`MAX_SYMMETRIC_MESSAGE_BYTES`].
    #[error("message has {0} bytes, more than fits in a payload")]
    MessageTooLarge(usize),
    /// The payload is not [`PAYLOAD_BYTES`] long.
    #[error("payload has {0} bytes, expected {PAYLOAD_BYTES}")]
    PayloadSize(usize),
    /// The payload was modified, or was not sealed with this key and `aad`.
    #[error("payload cannot be decrypted")]
    Decryption,
    /// The decrypted length prefix exceeds the rest of the plaintext.
    #[error("decrypted message length is invalid")]
    MessageLength,
    /// The decrypted bytes after the message are not all zero.
    #[error("decrypted padding is not zero")]
    Padding,
}

/// Encrypt `message` to `public_key` and return the payload and the sender's
/// keys.
pub fn seal(public_key: &PublicKey, message: &[u8]) -> Result<(Vec<u8>, Keys), Error> {
    let plaintext = pad::<HANDSHAKE_PLAINTEXT_BYTES>(message)?;
    let (encapsulated_key, mut context) =
        hpke::setup_sender::<ChaCha20Poly1305, HkdfSha256, Kem>(&OpModeS::Base, public_key, INFO)
            .expect("setting up to a valid public key cannot fail");
    let ciphertext = context
        .seal(&plaintext, &[])
        .expect("a fresh context seals one fixed-size plaintext");

    let mut payload = encapsulated_key.to_bytes().to_vec();
    payload.extend_from_slice(&ciphertext);
    debug_assert_eq!(payload.len(), PAYLOAD_BYTES);
    let keys = export_keys(
        |exporter_context, key| context.export(exporter_context, key),
        SENDER_TO_RECIPIENT,
        RECIPIENT_TO_SENDER,
    );
    Ok((payload, keys))
}

/// Decrypt `payload` with `secret_key` and return the message and the
/// recipient's keys.
pub fn open(secret_key: &SecretKey, payload: &[u8]) -> Result<(Vec<u8>, Keys), Error> {
    if payload.len() != PAYLOAD_BYTES {
        return Err(Error::PayloadSize(payload.len()));
    }
    let (key, ciphertext) = payload.split_at(ENCAPSULATED_KEY_BYTES);
    let encapsulated_key = EncappedKey::from_bytes(key).map_err(|_| Error::Decryption)?;
    let mut context = hpke::setup_receiver::<ChaCha20Poly1305, HkdfSha256, Kem>(
        &OpModeR::Base,
        secret_key,
        &encapsulated_key,
        INFO,
    )
    .expect("X-Wing decapsulation cannot fail");
    let plaintext = context
        .open(ciphertext, &[])
        .map_err(|_| Error::Decryption)?;
    let message = unpad(plaintext)?;
    let keys = export_keys(
        |exporter_context, key| context.export(exporter_context, key),
        RECIPIENT_TO_SENDER,
        SENDER_TO_RECIPIENT,
    );
    Ok((message, keys))
}

/// Export the keys with `export`, given the exporter contexts of the
/// directions this party sends and receives in.
fn export_keys(
    export: impl Fn(&[u8], &mut [u8]) -> Result<(), HpkeError>,
    sending: &[u8],
    receiving: &[u8],
) -> Keys {
    let mut keys = Keys {
        sending: [0; KEY_BYTES],
        receiving: [0; KEY_BYTES],
    };
    export(sending, &mut keys.sending).expect("KEY_BYTES is a valid export length");
    export(receiving, &mut keys.receiving).expect("KEY_BYTES is a valid export length");
    keys
}

/// Encrypt `message` with `key` and return the payload. [`open_symmetric`]
/// needs the same `aad`.
pub fn seal_symmetric(key: &[u8; KEY_BYTES], aad: &[u8], message: &[u8]) -> Result<Vec<u8>, Error> {
    let plaintext = pad::<SYMMETRIC_PLAINTEXT_BYTES>(message)?;
    let nonce = XNonce::generate();
    let ciphertext = XChaCha20Poly1305::new(&Key::from(*key))
        .encrypt(
            &nonce,
            Payload {
                msg: &plaintext,
                aad,
            },
        )
        .expect("a payload-sized plaintext cannot fail to encrypt");

    let mut payload = nonce.to_vec();
    payload.extend_from_slice(&ciphertext);
    debug_assert_eq!(payload.len(), PAYLOAD_BYTES);
    Ok(payload)
}

/// Decrypt `payload` with `key` and return the message.
pub fn open_symmetric(key: &[u8; KEY_BYTES], aad: &[u8], payload: &[u8]) -> Result<Vec<u8>, Error> {
    if payload.len() != PAYLOAD_BYTES {
        return Err(Error::PayloadSize(payload.len()));
    }
    let (nonce, ciphertext) = payload.split_at(NONCE_BYTES);
    let nonce = XNonce::try_from(nonce).expect("split at the nonce size");
    let plaintext = XChaCha20Poly1305::new(&Key::from(*key))
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| Error::Decryption)?;
    unpad(plaintext)
}

/// Prefix `message` with its length and pad it with zeros to `N` bytes.
fn pad<const N: usize>(message: &[u8]) -> Result<[u8; N], Error> {
    if message.len() > N - LENGTH_PREFIX_BYTES {
        return Err(Error::MessageTooLarge(message.len()));
    }
    let length = u16::try_from(message.len()).expect("PAYLOAD_BYTES fits in u16");
    let mut plaintext = [0u8; N];
    let mut w = &mut plaintext[..];
    w.write_all(&length.to_be_bytes())
        .expect("plaintext buffer sized to fit");
    w.write_all(message).expect("plaintext buffer sized to fit");
    Ok(plaintext)
}

/// Undo [`pad`].
fn unpad(mut plaintext: Vec<u8>) -> Result<Vec<u8>, Error> {
    let (length, rest) = plaintext.split_at(LENGTH_PREFIX_BYTES);
    let length = usize::from(u16::from_be_bytes(
        length.try_into().expect("split at the length size"),
    ));
    if length > rest.len() {
        return Err(Error::MessageLength);
    }
    if rest[length..].iter().any(|&byte| byte != 0) {
        return Err(Error::Padding);
    }
    plaintext.truncate(LENGTH_PREFIX_BYTES + length);
    plaintext.drain(..LENGTH_PREFIX_BYTES);
    Ok(plaintext)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use hpke::Kem as _;

    use super::*;

    fn keypair(seed: u8) -> (SecretKey, PublicKey) {
        Kem::derive_keypair(&[seed; 32])
    }

    #[test]
    fn messages_and_keys_round_trip() {
        let (secret, public) = keypair(1);
        for message in [&b""[..], b"message", &[0xff; MAX_MESSAGE_BYTES]] {
            let (payload, sender) = seal(&public, message).unwrap();
            assert_eq!(payload.len(), PAYLOAD_BYTES);
            let (opened, recipient) = open(&secret, &payload).unwrap();
            assert_eq!(opened, message);
            assert_eq!(sender.sending, recipient.receiving);
            assert_eq!(sender.receiving, recipient.sending);
        }
    }

    #[test]
    fn directions_have_different_keys() {
        let (_, public) = keypair(1);
        let (_, keys) = seal(&public, b"message").unwrap();
        assert_ne!(keys.sending, keys.receiving);
    }

    #[test]
    fn each_handshake_has_its_own_keys() {
        let (_, public) = keypair(1);
        let (_, first) = seal(&public, b"message").unwrap();
        let (_, second) = seal(&public, b"message").unwrap();
        assert_ne!(first.sending, second.sending);
        assert_ne!(first.receiving, second.receiving);
    }

    #[test]
    fn max_message_sizes() {
        // 7168 - 1120 - 16 - 2.
        assert_eq!(MAX_MESSAGE_BYTES, 6030);
        // 7168 - 24 - 16 - 2.
        assert_eq!(MAX_SYMMETRIC_MESSAGE_BYTES, 7126);
    }

    #[test]
    fn oversized_message_is_rejected() {
        let (_, public) = keypair(1);
        assert_eq!(
            seal(&public, &[0; MAX_MESSAGE_BYTES + 1]).err(),
            Some(Error::MessageTooLarge(MAX_MESSAGE_BYTES + 1))
        );
    }

    #[test]
    fn wrong_size_payload_is_rejected() {
        let (secret, public) = keypair(1);
        let (payload, _) = seal(&public, b"message").unwrap();
        for size in [PAYLOAD_BYTES - 1, PAYLOAD_BYTES + 1] {
            let mut payload = payload.clone();
            payload.resize(size, 0);
            assert_eq!(
                open(&secret, &payload).err(),
                Some(Error::PayloadSize(size))
            );
        }
    }

    #[test]
    fn wrong_key_is_rejected() {
        let (_, public) = keypair(1);
        let (other_secret, _) = keypair(2);
        let (payload, _) = seal(&public, b"message").unwrap();
        assert_eq!(open(&other_secret, &payload).err(), Some(Error::Decryption));
    }

    /// Seal `plaintext` as is, without a length prefix or padding.
    fn seal_plaintext(public: &PublicKey, plaintext: &[u8]) -> Vec<u8> {
        let (encapsulated_key, ciphertext) = hpke::single_shot_seal::<
            ChaCha20Poly1305,
            HkdfSha256,
            Kem,
        >(&OpModeS::Base, public, INFO, plaintext, &[])
        .unwrap();
        let mut payload = encapsulated_key.to_bytes().to_vec();
        payload.extend_from_slice(&ciphertext);
        payload
    }

    #[test]
    fn invalid_length_prefix_is_rejected() {
        let (secret, public) = keypair(1);
        let mut plaintext = vec![0; HANDSHAKE_PLAINTEXT_BYTES];
        plaintext[..LENGTH_PREFIX_BYTES].copy_from_slice(&u16::MAX.to_be_bytes());
        assert_eq!(
            open(&secret, &seal_plaintext(&public, &plaintext)).err(),
            Some(Error::MessageLength)
        );
    }

    #[test]
    fn nonzero_padding_is_rejected() {
        let (secret, public) = keypair(1);
        let mut plaintext = vec![0; HANDSHAKE_PLAINTEXT_BYTES];
        plaintext[HANDSHAKE_PLAINTEXT_BYTES - 1] = 1;
        assert_eq!(
            open(&secret, &seal_plaintext(&public, &plaintext)).err(),
            Some(Error::Padding)
        );
    }

    const AAD: &[u8] = b"aad";

    fn handshake() -> (Keys, Keys) {
        let (secret, public) = keypair(1);
        let (payload, sender) = seal(&public, b"").unwrap();
        let (_, recipient) = open(&secret, &payload).unwrap();
        (sender, recipient)
    }

    #[test]
    fn symmetric_messages_round_trip_in_both_directions() {
        let (sender, recipient) = handshake();
        for message in [&b""[..], b"message", &[0xff; MAX_SYMMETRIC_MESSAGE_BYTES]] {
            for (from, to) in [(&sender, &recipient), (&recipient, &sender)] {
                let payload = seal_symmetric(&from.sending, AAD, message).unwrap();
                assert_eq!(payload.len(), PAYLOAD_BYTES);
                assert_eq!(
                    open_symmetric(&to.receiving, AAD, &payload).unwrap(),
                    message
                );
            }
        }
    }

    #[test]
    fn symmetric_wrong_key_or_aad_is_rejected() {
        let (sender, _) = handshake();
        let payload = seal_symmetric(&sender.sending, AAD, b"message").unwrap();
        assert_eq!(
            open_symmetric(&sender.receiving, AAD, &payload),
            Err(Error::Decryption)
        );
        assert_eq!(
            open_symmetric(&sender.sending, b"other", &payload),
            Err(Error::Decryption)
        );
    }

    #[test]
    fn symmetric_modified_payload_is_rejected() {
        let (sender, _) = handshake();
        let mut payload = seal_symmetric(&sender.sending, AAD, b"message").unwrap();
        payload[PAYLOAD_BYTES - 1] ^= 1;
        assert_eq!(
            open_symmetric(&sender.sending, AAD, &payload),
            Err(Error::Decryption)
        );
    }

    #[test]
    fn symmetric_oversized_message_is_rejected() {
        let (sender, _) = handshake();
        assert_eq!(
            seal_symmetric(&sender.sending, AAD, &[0; MAX_SYMMETRIC_MESSAGE_BYTES + 1]),
            Err(Error::MessageTooLarge(MAX_SYMMETRIC_MESSAGE_BYTES + 1))
        );
    }

    #[test]
    fn symmetric_wrong_size_payload_is_rejected() {
        let (sender, _) = handshake();
        let payload = seal_symmetric(&sender.sending, AAD, b"message").unwrap();
        for size in [PAYLOAD_BYTES - 1, PAYLOAD_BYTES + 1] {
            let mut payload = payload.clone();
            payload.resize(size, 0);
            assert_eq!(
                open_symmetric(&sender.sending, AAD, &payload),
                Err(Error::PayloadSize(size))
            );
        }
    }

    /// Every bit position takes both values across payloads of the same
    /// message, as in rust-payjoin's `test_encrypted_payload_bit_uniformity`.
    ///
    /// Fails by chance with negligible probability.
    #[test]
    fn symmetric_payload_bits_vary() {
        let (sender, _) = handshake();
        let message = [0; MAX_SYMMETRIC_MESSAGE_BYTES];
        let seal = || seal_symmetric(&sender.sending, AAD, &message).unwrap();
        let first = seal();
        let mut differing = vec![0u8; PAYLOAD_BYTES];
        for _ in 0..80 {
            for (bits, (a, b)) in differing.iter_mut().zip(first.iter().zip(&seal())) {
                *bits |= a ^ b;
            }
        }
        assert!(differing.iter().all(|&bits| bits == 0xff));
    }
}

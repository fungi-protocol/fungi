//! One fixed-size HPKE payload that gives sender and recipient a shared
//! secret.
//!
//! A payload is [`PAYLOAD_BYTES`], the size of a BIP77 mailbox message: an
//! ElligatorSwift-encoded encapsulated key followed by the ChaCha20Poly1305
//! ciphertext of the length-prefixed, zero-padded message. The secret is
//! exported from the HPKE context.

use bitcoin_hpke::aead::{AeadCtxR, AeadCtxS, ChaCha20Poly1305};
use bitcoin_hpke::kdf::HkdfSha256;
use bitcoin_hpke::kem::SecpK256HkdfSha256;
use bitcoin_hpke::rand_core::OsRng;
use bitcoin_hpke::{Deserializable, Kem as _, OpModeR, OpModeS, Serializable};
use secp256k1::ellswift::ElligatorSwift;

/// Size of a payload.
pub const PAYLOAD_BYTES: usize = 7168;

/// Largest message that fits in one payload.
pub const MAX_MESSAGE_BYTES: usize = PLAINTEXT_BYTES - LENGTH_BYTES;

/// Size of the exported shared secret.
pub const SHARED_SECRET_BYTES: usize = 32;

const ENCAPSULATED_KEY_BYTES: usize = 64;
const TAG_BYTES: usize = 16;
const LENGTH_BYTES: usize = 2;
const PLAINTEXT_BYTES: usize = PAYLOAD_BYTES - ENCAPSULATED_KEY_BYTES - TAG_BYTES;

/// HPKE exporter context of the shared secret. The value is arbitrary.
const EXPORTER_CONTEXT: &[u8] = b"shared secret";

type Kem = SecpK256HkdfSha256;
type EncappedKey = <Kem as bitcoin_hpke::Kem>::EncappedKey;

/// A secp256k1 HPKE secret key.
pub type SecretKey = <Kem as bitcoin_hpke::Kem>::PrivateKey;

/// A secp256k1 HPKE public key.
pub type PublicKey = <Kem as bitcoin_hpke::Kem>::PublicKey;

/// Failure to seal or open a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The message is longer than [`MAX_MESSAGE_BYTES`].
    #[error("message has {0} bytes, more than {MAX_MESSAGE_BYTES}")]
    MessageTooLarge(usize),
    /// The payload is not [`PAYLOAD_BYTES`] long.
    #[error("payload has {0} bytes, expected {PAYLOAD_BYTES}")]
    PayloadSize(usize),
    /// The payload was modified, or was not sealed to this key and `info`.
    #[error("payload cannot be decrypted")]
    Decryption,
    /// The decrypted length prefix exceeds [`MAX_MESSAGE_BYTES`].
    #[error("decrypted message length is invalid")]
    MessageLength,
}

/// Derive a keypair from at least 32 bytes of `ikm` with the KEM's
/// `DeriveKeyPair`.
pub fn derive_keypair(ikm: &[u8]) -> (SecretKey, PublicKey) {
    Kem::derive_keypair(ikm)
}

/// Encrypt `message` to `public_key` and return the payload and the shared
/// secret. [`open`] needs the same `info`.
pub fn seal(
    public_key: &PublicKey,
    info: &[u8],
    message: &[u8],
) -> Result<(Vec<u8>, [u8; SHARED_SECRET_BYTES]), Error> {
    if message.len() > MAX_MESSAGE_BYTES {
        return Err(Error::MessageTooLarge(message.len()));
    }
    let length = u16::try_from(message.len()).expect("MAX_MESSAGE_BYTES fits in u16");
    let mut plaintext = Vec::with_capacity(PLAINTEXT_BYTES);
    plaintext.extend_from_slice(&length.to_le_bytes());
    plaintext.extend_from_slice(message);
    plaintext.resize(PLAINTEXT_BYTES, 0);

    let (encapsulated_key, mut context) = bitcoin_hpke::setup_sender::<
        ChaCha20Poly1305,
        HkdfSha256,
        Kem,
        _,
    >(&OpModeS::Base, public_key, info, &mut OsRng)
    .expect("setting up to a valid public key cannot fail");
    let ciphertext = context
        .seal(&plaintext, &[])
        .expect("the first message of a context cannot fail to seal");

    let mut payload = encode_key(&encapsulated_key).to_vec();
    payload.extend_from_slice(&ciphertext);
    debug_assert_eq!(payload.len(), PAYLOAD_BYTES);
    Ok((payload, export_sender(&context)))
}

/// Decrypt `payload` with `secret_key`, and return the message and the shared
/// secret.
pub fn open(
    secret_key: &SecretKey,
    info: &[u8],
    payload: &[u8],
) -> Result<(Vec<u8>, [u8; SHARED_SECRET_BYTES]), Error> {
    if payload.len() != PAYLOAD_BYTES {
        return Err(Error::PayloadSize(payload.len()));
    }
    let (key, ciphertext) = payload.split_at(ENCAPSULATED_KEY_BYTES);
    let encapsulated_key = decode_key(key.try_into().expect("split at the key size"));
    let mut context = bitcoin_hpke::setup_receiver::<ChaCha20Poly1305, HkdfSha256, Kem>(
        &OpModeR::Base,
        secret_key,
        &encapsulated_key,
        info,
    )
    .map_err(|_| Error::Decryption)?;
    let plaintext = context
        .open(ciphertext, &[])
        .map_err(|_| Error::Decryption)?;

    let (length, rest) = plaintext.split_at(LENGTH_BYTES);
    let length = u16::from_le_bytes(length.try_into().expect("split at the length size"));
    let message = rest
        .get(..usize::from(length))
        .ok_or(Error::MessageLength)?
        .to_vec();
    Ok((message, export_receiver(&context)))
}

fn export_sender(
    context: &AeadCtxS<ChaCha20Poly1305, HkdfSha256, Kem>,
) -> [u8; SHARED_SECRET_BYTES] {
    let mut secret = [0; SHARED_SECRET_BYTES];
    context
        .export(EXPORTER_CONTEXT, &mut secret)
        .expect("SHARED_SECRET_BYTES is a valid export length");
    secret
}

fn export_receiver(
    context: &AeadCtxR<ChaCha20Poly1305, HkdfSha256, Kem>,
) -> [u8; SHARED_SECRET_BYTES] {
    let mut secret = [0; SHARED_SECRET_BYTES];
    context
        .export(EXPORTER_CONTEXT, &mut secret)
        .expect("SHARED_SECRET_BYTES is a valid export length");
    secret
}

fn encode_key(key: &EncappedKey) -> [u8; ENCAPSULATED_KEY_BYTES] {
    let key = secp256k1::PublicKey::from_slice(&key.to_bytes())
        .expect("an HPKE encapsulated key is a valid secp256k1 point");
    ElligatorSwift::from_pubkey(key).to_array()
}

fn decode_key(encoded: [u8; ENCAPSULATED_KEY_BYTES]) -> EncappedKey {
    let key = secp256k1::PublicKey::from_ellswift(ElligatorSwift::from_array(encoded));
    EncappedKey::from_bytes(&key.serialize_uncompressed())
        .expect("a decoded ElligatorSwift point is a valid secp256k1 point")
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    const INFO: &[u8] = b"info";

    fn keypair(seed: u8) -> (SecretKey, PublicKey) {
        derive_keypair(&[seed; 32])
    }

    #[test]
    fn messages_and_shared_secrets_round_trip() {
        let (secret, public) = keypair(1);
        for message in [&b""[..], b"message", &[0xff; MAX_MESSAGE_BYTES]] {
            let (payload, sender_secret) = seal(&public, INFO, message).unwrap();
            assert_eq!(payload.len(), PAYLOAD_BYTES);
            let (opened, receiver_secret) = open(&secret, INFO, &payload).unwrap();
            assert_eq!(opened, message);
            assert_eq!(sender_secret, receiver_secret);
        }
    }

    #[test]
    fn each_payload_has_its_own_shared_secret() {
        let (_, public) = keypair(1);
        let (_, first) = seal(&public, INFO, b"message").unwrap();
        let (_, second) = seal(&public, INFO, b"message").unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn oversized_message_is_rejected() {
        let (_, public) = keypair(1);
        assert_eq!(
            seal(&public, INFO, &[0; MAX_MESSAGE_BYTES + 1]),
            Err(Error::MessageTooLarge(MAX_MESSAGE_BYTES + 1))
        );
    }

    #[test]
    fn wrong_size_payload_is_rejected() {
        let (secret, public) = keypair(1);
        let (payload, _) = seal(&public, INFO, b"message").unwrap();
        for size in [PAYLOAD_BYTES - 1, PAYLOAD_BYTES + 1] {
            let mut payload = payload.clone();
            payload.resize(size, 0);
            assert_eq!(open(&secret, INFO, &payload), Err(Error::PayloadSize(size)));
        }
    }

    #[test]
    fn wrong_key_or_info_is_rejected() {
        let (secret, public) = keypair(1);
        let (other_secret, _) = keypair(2);
        let (payload, _) = seal(&public, INFO, b"message").unwrap();
        assert_eq!(open(&other_secret, INFO, &payload), Err(Error::Decryption));
        assert_eq!(open(&secret, b"other", &payload), Err(Error::Decryption));
    }

    #[test]
    fn invalid_length_prefix_is_rejected() {
        let (secret, public) = keypair(1);
        let mut plaintext = vec![0; PLAINTEXT_BYTES];
        plaintext[..LENGTH_BYTES].copy_from_slice(&u16::MAX.to_le_bytes());
        let (encapsulated_key, ciphertext) =
            bitcoin_hpke::single_shot_seal::<ChaCha20Poly1305, HkdfSha256, Kem, _>(
                &OpModeS::Base,
                &public,
                INFO,
                &plaintext,
                &[],
                &mut OsRng,
            )
            .unwrap();
        let mut payload = encode_key(&encapsulated_key).to_vec();
        payload.extend_from_slice(&ciphertext);
        assert_eq!(open(&secret, INFO, &payload), Err(Error::MessageLength));
    }

    /// Every bit position takes both values across payloads of the same
    /// message, as in rust-payjoin's `test_encrypted_payload_bit_uniformity`.
    ///
    /// Fails by chance with negligible probability.
    #[test]
    fn payload_bits_vary() {
        let (_, public) = keypair(1);
        let message = [0; MAX_MESSAGE_BYTES];
        let (first, _) = seal(&public, INFO, &message).unwrap();
        let mut differing = vec![0u8; PAYLOAD_BYTES];
        for _ in 0..80 {
            let (payload, _) = seal(&public, INFO, &message).unwrap();
            for (bits, (a, b)) in differing.iter_mut().zip(first.iter().zip(&payload)) {
                *bits |= a ^ b;
            }
        }
        assert!(differing.iter().all(|&bits| bits == 0xff));
    }
}

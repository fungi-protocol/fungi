//! One fixed-size HPKE payload that gives sender and recipient a shared
//! secret.
//!
//! A payload is [`PAYLOAD_BYTES`], the size of a BIP77 mailbox message: an
//! X-Wing encapsulated key followed by the ChaCha20Poly1305 ciphertext of the
//! length-prefixed, zero-padded message. The secret is exported from the HPKE
//! context.

use std::io::Write as _;

use hpke::aead::{AeadCtxR, AeadCtxS, ChaCha20Poly1305};
use hpke::hybrid_array::typenum::Unsigned as _;
use hpke::kdf::HkdfSha256;
use hpke::kem::XWing;
use hpke::{Deserializable, OpModeR, OpModeS, Serializable};

/// Size of a payload, as in BIP77's
/// [Uniform Payloads](https://github.com/bitcoin/bips/blob/master/bip-0077.md#uniform-payloads).
pub const PAYLOAD_BYTES: usize = 7168;

/// Largest message that fits in one payload.
pub const MAX_MESSAGE_BYTES: usize = PLAINTEXT_BYTES - LENGTH_PREFIX_BYTES;

/// Size of the exported shared secret.
pub const SHARED_SECRET_BYTES: usize = 32;

const ENCAPSULATED_KEY_BYTES: usize = <EncappedKey as Serializable>::OutputSize::USIZE;
const TAG_BYTES: usize = 16;
/// Size of the big-endian message length at the start of a plaintext.
const LENGTH_PREFIX_BYTES: usize = 2;
const PLAINTEXT_BYTES: usize = PAYLOAD_BYTES - ENCAPSULATED_KEY_BYTES - TAG_BYTES;

/// HPKE info of the handshake. The value is arbitrary.
const INFO: &[u8] = b"fungi-hpke-info-v0";

/// HPKE exporter context of the shared secret. The value is arbitrary.
const EXPORTER_CONTEXT: &[u8] = b"fungi-hpke-exporter-context-v0";

type Kem = XWing;
type EncappedKey = <Kem as hpke::Kem>::EncappedKey;
type SecretKey = <Kem as hpke::Kem>::PrivateKey;
type PublicKey = <Kem as hpke::Kem>::PublicKey;

/// Failure to seal or open a payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The message is longer than [`MAX_MESSAGE_BYTES`].
    #[error("message has {0} bytes, more than {MAX_MESSAGE_BYTES}")]
    MessageTooLarge(usize),
    /// The payload is not [`PAYLOAD_BYTES`] long.
    #[error("payload has {0} bytes, expected {PAYLOAD_BYTES}")]
    PayloadSize(usize),
    /// The payload was modified, or was not sealed to this key.
    #[error("payload cannot be decrypted")]
    Decryption,
    /// The decrypted length prefix exceeds [`MAX_MESSAGE_BYTES`].
    #[error("decrypted message length is invalid")]
    MessageLength,
    /// The decrypted bytes after the message are not all zero.
    #[error("decrypted padding is not zero")]
    Padding,
}

/// Encrypt `message` to `public_key` and return the payload and the shared
/// secret.
pub fn seal(
    public_key: &PublicKey,
    message: &[u8],
) -> Result<(Vec<u8>, [u8; SHARED_SECRET_BYTES]), Error> {
    if message.len() > MAX_MESSAGE_BYTES {
        return Err(Error::MessageTooLarge(message.len()));
    }
    let length = u16::try_from(message.len()).expect("MAX_MESSAGE_BYTES fits in u16");
    let mut plaintext = [0u8; PLAINTEXT_BYTES];
    let mut w = &mut plaintext[..];
    w.write_all(&length.to_be_bytes())
        .expect("plaintext buffer sized to fit");
    w.write_all(message).expect("plaintext buffer sized to fit");

    let (encapsulated_key, mut context) =
        hpke::setup_sender::<ChaCha20Poly1305, HkdfSha256, Kem>(&OpModeS::Base, public_key, INFO)
            .expect("setting up to a valid public key cannot fail");
    let ciphertext = context
        .seal(&plaintext, &[])
        .expect("a fresh context seals one fixed-size plaintext");

    let mut payload = encapsulated_key.to_bytes().to_vec();
    payload.extend_from_slice(&ciphertext);
    debug_assert_eq!(payload.len(), PAYLOAD_BYTES);
    Ok((payload, export_sender(&context)))
}

/// Decrypt `payload` with `secret_key` and return the message and the shared
/// secret.
pub fn open(
    secret_key: &SecretKey,
    payload: &[u8],
) -> Result<(Vec<u8>, [u8; SHARED_SECRET_BYTES]), Error> {
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
    let mut plaintext = context
        .open(ciphertext, &[])
        .map_err(|_| Error::Decryption)?;

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
    Ok((plaintext, export_receiver(&context)))
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

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use hpke::Kem as _;

    use super::*;

    fn keypair(seed: u8) -> (SecretKey, PublicKey) {
        Kem::derive_keypair(&[seed; 32])
    }

    #[test]
    fn messages_and_shared_secrets_round_trip() {
        let (secret, public) = keypair(1);
        for message in [&b""[..], b"message", &[0xff; MAX_MESSAGE_BYTES]] {
            let (payload, sender_secret) = seal(&public, message).unwrap();
            assert_eq!(payload.len(), PAYLOAD_BYTES);
            let (opened, receiver_secret) = open(&secret, &payload).unwrap();
            assert_eq!(opened, message);
            assert_eq!(sender_secret, receiver_secret);
        }
    }

    #[test]
    fn each_handshake_has_its_own_shared_secret() {
        let (_, public) = keypair(1);
        let (_, first) = seal(&public, b"message").unwrap();
        let (_, second) = seal(&public, b"message").unwrap();
        assert_ne!(first, second);
    }

    #[test]
    fn max_message_size() {
        // 7168 - 1120 - 16 - 2.
        assert_eq!(MAX_MESSAGE_BYTES, 6030);
    }

    #[test]
    fn oversized_message_is_rejected() {
        let (_, public) = keypair(1);
        assert_eq!(
            seal(&public, &[0; MAX_MESSAGE_BYTES + 1]),
            Err(Error::MessageTooLarge(MAX_MESSAGE_BYTES + 1))
        );
    }

    #[test]
    fn wrong_size_payload_is_rejected() {
        let (secret, public) = keypair(1);
        let (payload, _) = seal(&public, b"message").unwrap();
        for size in [PAYLOAD_BYTES - 1, PAYLOAD_BYTES + 1] {
            let mut payload = payload.clone();
            payload.resize(size, 0);
            assert_eq!(open(&secret, &payload), Err(Error::PayloadSize(size)));
        }
    }

    #[test]
    fn wrong_key_is_rejected() {
        let (_, public) = keypair(1);
        let (other_secret, _) = keypair(2);
        let (payload, _) = seal(&public, b"message").unwrap();
        assert_eq!(open(&other_secret, &payload), Err(Error::Decryption));
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
        let mut plaintext = vec![0; PLAINTEXT_BYTES];
        plaintext[..LENGTH_PREFIX_BYTES].copy_from_slice(&u16::MAX.to_be_bytes());
        assert_eq!(
            open(&secret, &seal_plaintext(&public, &plaintext)),
            Err(Error::MessageLength)
        );
    }

    #[test]
    fn nonzero_padding_is_rejected() {
        let (secret, public) = keypair(1);
        let mut plaintext = vec![0; PLAINTEXT_BYTES];
        plaintext[PLAINTEXT_BYTES - 1] = 1;
        assert_eq!(
            open(&secret, &seal_plaintext(&public, &plaintext)),
            Err(Error::Padding)
        );
    }
}

//! One HPKE handshake that gives sender and recipient a shared secret.
//!
//! The sender encrypts a message in place to the recipient's X-Wing public
//! key and sends the encapsulated key, the ChaCha20Poly1305 ciphertext and the
//! tag. The secret is exported from the HPKE context.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use hpke::aead::{AeadTag, ChaCha20Poly1305};
use hpke::kdf::HkdfSha256;
use hpke::kem::XWing;
use hpke::{HpkeError, OpModeR, OpModeS};

/// Size of the exported shared secret.
pub const SHARED_SECRET_BYTES: usize = 32;

/// HPKE info of the handshake. A domain separator, so it must be unique
/// to this protocol.
const INFO: &[u8] = b"fungi-hpke-info-v0";

/// HPKE exporter context of the shared secret. A domain separator, so it
/// must be unique to this use.
const EXPORTER_CONTEXT: &[u8] = b"fungi-hpke-exporter-context-v0";

type Kem = XWing;
/// X-Wing secret key that opens payloads.
pub type SecretKey = <Kem as hpke::Kem>::PrivateKey;
/// X-Wing public key that payloads are sealed to.
pub type PublicKey = <Kem as hpke::Kem>::PublicKey;
/// X-Wing encapsulated key of one handshake.
pub type EncappedKey = <Kem as hpke::Kem>::EncappedKey;
/// ChaCha20Poly1305 tag of the ciphertext.
pub type Tag = AeadTag<ChaCha20Poly1305>;

/// Secret exported from the handshake.
pub struct SharedSecret([u8; SHARED_SECRET_BYTES]);

impl SharedSecret {
    /// The secret as bytes.
    pub fn into_bytes(self) -> [u8; SHARED_SECRET_BYTES] {
        self.0
    }
}

/// Failure to open a payload: it was modified or truncated, or was not sealed
/// to this key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("payload cannot be decrypted")]
pub struct Error;

/// Encrypt `message` in place to `public_key` and return the encapsulated key,
/// the tag and the shared secret. Panics if `message` is 2^38 - 64 bytes or
/// longer, the ChaCha20Poly1305 limit.
pub fn seal(public_key: &PublicKey, message: &mut [u8]) -> (EncappedKey, Tag, SharedSecret) {
    let (encapsulated_key, mut context) =
        hpke::setup_sender::<ChaCha20Poly1305, HkdfSha256, Kem>(&OpModeS::Base, public_key, INFO)
            .expect("setting up to a valid public key cannot fail");
    let tag = context
        .seal_inout_detached(message.into(), &[])
        .expect("a fresh context seals a message shorter than the ChaCha20Poly1305 limit");
    let secret = export_secret(|exporter_context, secret| context.export(exporter_context, secret));
    (encapsulated_key, tag, secret)
}

/// Decrypt `message` in place with `secret_key` and return the shared secret.
/// On error the contents of `message` are unspecified.
pub fn open(
    secret_key: &SecretKey,
    encapsulated_key: &EncappedKey,
    message: &mut [u8],
    tag: &Tag,
) -> Result<SharedSecret, Error> {
    let mut context = hpke::setup_receiver::<ChaCha20Poly1305, HkdfSha256, Kem>(
        &OpModeR::Base,
        secret_key,
        encapsulated_key,
        INFO,
    )
    .expect("X-Wing decapsulation cannot fail");
    context
        .open_inout_detached(message.into(), &[], tag)
        .map_err(|_| Error)?;
    Ok(export_secret(|exporter_context, secret| {
        context.export(exporter_context, secret)
    }))
}

/// Export the shared secret with `export`.
fn export_secret(export: impl Fn(&[u8], &mut [u8]) -> Result<(), HpkeError>) -> SharedSecret {
    let mut secret = [0; SHARED_SECRET_BYTES];
    export(EXPORTER_CONTEXT, &mut secret).expect("SHARED_SECRET_BYTES is a valid export length");
    SharedSecret(secret)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use hpke::{Deserializable as _, Kem as _, Serializable as _};

    use super::*;

    fn keypair(seed: u8) -> (SecretKey, PublicKey) {
        Kem::derive_keypair(&[seed; 32])
    }

    #[test]
    fn messages_and_shared_secrets_round_trip() {
        let (secret, public) = keypair(1);
        for message in [&b""[..], b"message", &[0xff; 10_000]] {
            let mut buffer = message.to_vec();
            let (encapsulated_key, tag, sender_secret) = seal(&public, &mut buffer);
            if !message.is_empty() {
                assert_ne!(buffer, message);
            }
            let receiver_secret = open(&secret, &encapsulated_key, &mut buffer, &tag).unwrap();
            assert_eq!(buffer, message);
            assert_eq!(sender_secret.into_bytes(), receiver_secret.into_bytes());
        }
    }

    #[test]
    fn each_handshake_has_its_own_shared_secret() {
        let (_, public) = keypair(1);
        let (_, _, first) = seal(&public, &mut b"message".to_vec());
        let (_, _, second) = seal(&public, &mut b"message".to_vec());
        assert_ne!(first.into_bytes(), second.into_bytes());
    }

    #[test]
    fn modified_ciphertext_is_rejected() {
        let (secret, public) = keypair(1);
        let mut buffer = b"message".to_vec();
        let (encapsulated_key, tag, _) = seal(&public, &mut buffer);
        buffer[0] ^= 1;
        assert_eq!(
            open(&secret, &encapsulated_key, &mut buffer, &tag).err(),
            Some(Error)
        );
    }

    #[test]
    fn modified_tag_is_rejected() {
        let (secret, public) = keypair(1);
        let mut buffer = b"message".to_vec();
        let (encapsulated_key, tag, _) = seal(&public, &mut buffer);
        let mut tag = tag.to_bytes();
        tag[0] ^= 1;
        let tag = Tag::from_bytes(&tag).unwrap();
        assert_eq!(
            open(&secret, &encapsulated_key, &mut buffer, &tag).err(),
            Some(Error)
        );
    }

    #[test]
    fn modified_encapsulated_key_is_rejected() {
        let (secret, public) = keypair(1);
        let mut buffer = b"message".to_vec();
        let (encapsulated_key, tag, _) = seal(&public, &mut buffer);
        let mut encapsulated_key = encapsulated_key.to_bytes();
        encapsulated_key[0] ^= 1;
        let encapsulated_key = EncappedKey::from_bytes(&encapsulated_key).unwrap();
        assert_eq!(
            open(&secret, &encapsulated_key, &mut buffer, &tag).err(),
            Some(Error)
        );
    }

    #[test]
    fn wrong_key_is_rejected() {
        let (_, public) = keypair(1);
        let (other_secret, _) = keypair(2);
        let mut buffer = b"message".to_vec();
        let (encapsulated_key, tag, _) = seal(&public, &mut buffer);
        assert_eq!(
            open(&other_secret, &encapsulated_key, &mut buffer, &tag).err(),
            Some(Error)
        );
    }
}

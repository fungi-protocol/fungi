//! One HPKE handshake that gives sender and recipient a key for each
//! direction.
//!
//! The sender encrypts a message in place to the recipient's X-Wing public
//! key and sends the encapsulated key, the ChaCha20Poly1305 ciphertext and the
//! tag. The keys are exported from the HPKE context.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use hpke::aead::{AeadTag, ChaCha20Poly1305};
use hpke::kdf::HkdfSha256;
use hpke::kem::XWing;
use hpke::{HpkeError, OpModeR, OpModeS};

/// Size of each exported key.
pub const KEY_BYTES: usize = 32;

/// HPKE info of the handshake. A domain separator, so it must be unique
/// to this protocol.
const INFO: &[u8] = b"fungi-hpke-info-v0";

/// HPKE exporter contexts of the keys. Domain separators, so each must be
/// unique to its direction.
const SENDER_TO_RECIPIENT: &[u8] = b"fungi-hpke-sender-to-recipient-v0";
const RECIPIENT_TO_SENDER: &[u8] = b"fungi-hpke-recipient-to-sender-v0";

type Kem = XWing;
/// X-Wing secret key that opens payloads.
pub type SecretKey = <Kem as hpke::Kem>::PrivateKey;
/// X-Wing public key that payloads are sealed to.
pub type PublicKey = <Kem as hpke::Kem>::PublicKey;
/// X-Wing encapsulated key of one handshake.
pub type EncappedKey = <Kem as hpke::Kem>::EncappedKey;
/// ChaCha20Poly1305 tag of the ciphertext.
pub type Tag = AeadTag<ChaCha20Poly1305>;

/// Key exported from the handshake for one direction.
pub struct Key([u8; KEY_BYTES]);

impl Key {
    /// The key as bytes.
    pub fn into_bytes(self) -> [u8; KEY_BYTES] {
        self.0
    }
}

/// One party's keys, one for each direction.
pub struct Keys {
    /// Key of the payloads this party sends.
    pub sending: Key,
    /// Key of the payloads this party receives.
    pub receiving: Key,
}

/// Failure to open a payload: it was modified or truncated, or was not sealed
/// to this key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("payload cannot be decrypted")]
pub struct Error;

/// Encrypt `message` in place to `public_key` and return the encapsulated key,
/// the tag and the sender's keys. Panics if `message` is 2^38 - 64 bytes or
/// longer, the ChaCha20Poly1305 limit.
pub fn seal(public_key: &PublicKey, message: &mut [u8]) -> (EncappedKey, Tag, Keys) {
    let (encapsulated_key, mut context) =
        hpke::setup_sender::<ChaCha20Poly1305, HkdfSha256, Kem>(&OpModeS::Base, public_key, INFO)
            .expect("setting up to a valid public key cannot fail");
    let tag = context
        .seal_inout_detached(message.into(), &[])
        .expect("a fresh context seals a message shorter than the ChaCha20Poly1305 limit");
    let keys = export_keys(
        |exporter_context, key| context.export(exporter_context, key),
        SENDER_TO_RECIPIENT,
        RECIPIENT_TO_SENDER,
    );
    (encapsulated_key, tag, keys)
}

/// Decrypt `message` in place with `secret_key` and return the recipient's
/// keys. On error the contents of `message` are unspecified.
pub fn open(
    secret_key: &SecretKey,
    encapsulated_key: &EncappedKey,
    message: &mut [u8],
    tag: &Tag,
) -> Result<Keys, Error> {
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
    Ok(export_keys(
        |exporter_context, key| context.export(exporter_context, key),
        RECIPIENT_TO_SENDER,
        SENDER_TO_RECIPIENT,
    ))
}

/// Export the keys with `export`, given the exporter contexts of the
/// directions this party sends and receives in.
fn export_keys(
    export: impl Fn(&[u8], &mut [u8]) -> Result<(), HpkeError>,
    sending: &[u8],
    receiving: &[u8],
) -> Keys {
    let export_key = |exporter_context| {
        let mut key = [0; KEY_BYTES];
        export(exporter_context, &mut key).expect("KEY_BYTES is a valid export length");
        Key(key)
    };
    Keys {
        sending: export_key(sending),
        receiving: export_key(receiving),
    }
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
    fn messages_and_keys_round_trip() {
        let (secret, public) = keypair(1);
        for message in [&b""[..], b"message", &[0xff; 10_000]] {
            let mut buffer = message.to_vec();
            let (encapsulated_key, tag, sender) = seal(&public, &mut buffer);
            if !message.is_empty() {
                assert_ne!(buffer, message);
            }
            let recipient = open(&secret, &encapsulated_key, &mut buffer, &tag).unwrap();
            assert_eq!(buffer, message);
            assert_eq!(
                sender.sending.into_bytes(),
                recipient.receiving.into_bytes()
            );
            assert_eq!(
                sender.receiving.into_bytes(),
                recipient.sending.into_bytes()
            );
        }
    }

    #[test]
    fn directions_have_different_keys() {
        let (_, public) = keypair(1);
        let (_, _, keys) = seal(&public, &mut b"message".to_vec());
        assert_ne!(keys.sending.into_bytes(), keys.receiving.into_bytes());
    }

    #[test]
    fn each_handshake_has_its_own_keys() {
        let (_, public) = keypair(1);
        let (_, _, first) = seal(&public, &mut b"message".to_vec());
        let (_, _, second) = seal(&public, &mut b"message".to_vec());
        assert_ne!(first.sending.into_bytes(), second.sending.into_bytes());
        assert_ne!(first.receiving.into_bytes(), second.receiving.into_bytes());
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

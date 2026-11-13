//! Messages encrypted with a key both parties already have, such as one
//! exported by `fungi-hpke`.
//!
//! A payload is a random nonce followed by the XChaCha20Poly1305 ciphertext
//! of the message. A random nonce keeps sealing stateless.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use chacha20poly1305::aead::array::typenum::Unsigned as _;
use chacha20poly1305::aead::{Aead as _, Generate as _, Payload};
use chacha20poly1305::{AeadCore, Key, KeyInit as _, KeySizeUser, XChaCha20Poly1305, XNonce};

/// Bytes a payload has beyond its message.
pub const OVERHEAD_BYTES: usize = NONCE_BYTES + TAG_BYTES;

/// Size of a key.
pub const KEY_BYTES: usize = <XChaCha20Poly1305 as KeySizeUser>::KeySize::USIZE;

const NONCE_BYTES: usize = <XChaCha20Poly1305 as AeadCore>::NonceSize::USIZE;
const TAG_BYTES: usize = <XChaCha20Poly1305 as AeadCore>::TagSize::USIZE;

/// Failure to open a payload: it was modified or truncated, or was not sealed
/// with this key and `aad`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("payload cannot be decrypted")]
pub struct Error;

/// Encrypt `message` with `key` and return the payload. [`open`] needs the
/// same `aad`. Panics if `message` is 2^38 - 64 bytes or longer, the
/// XChaCha20Poly1305 limit.
pub fn seal(key: &[u8; KEY_BYTES], aad: &[u8], message: &[u8]) -> Vec<u8> {
    let nonce = XNonce::generate();
    let ciphertext = XChaCha20Poly1305::new(&Key::from(*key))
        .encrypt(&nonce, Payload { msg: message, aad })
        .expect("message is shorter than the XChaCha20Poly1305 limit");

    let mut payload = nonce.to_vec();
    payload.extend_from_slice(&ciphertext);
    payload
}

/// Decrypt `payload` with `key` and return the message.
pub fn open(key: &[u8; KEY_BYTES], aad: &[u8], payload: &[u8]) -> Result<Vec<u8>, Error> {
    let (nonce, ciphertext) = payload.split_at_checked(NONCE_BYTES).ok_or(Error)?;
    let nonce = XNonce::try_from(nonce)
        .expect("split off of a slice whose length has been checked to be long enough");
    XChaCha20Poly1305::new(&Key::from(*key))
        .decrypt(
            &nonce,
            Payload {
                msg: ciphertext,
                aad,
            },
        )
        .map_err(|_| Error)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;

    const KEY: [u8; KEY_BYTES] = [1; KEY_BYTES];
    const AAD: &[u8] = b"aad";

    #[test]
    fn messages_round_trip() {
        for message in [&b""[..], b"message", &[0xff; 10_000]] {
            let payload = seal(&KEY, AAD, message);
            assert_eq!(payload.len(), OVERHEAD_BYTES + message.len());
            assert_eq!(open(&KEY, AAD, &payload).unwrap(), message);
        }
    }

    #[test]
    fn overhead() {
        // 24 + 16.
        assert_eq!(OVERHEAD_BYTES, 40);
    }

    #[test]
    fn wrong_key_or_aad_is_rejected() {
        let payload = seal(&KEY, AAD, b"message");
        assert_eq!(open(&[2; KEY_BYTES], AAD, &payload), Err(Error));
        assert_eq!(open(&KEY, b"other", &payload), Err(Error));
    }

    #[test]
    fn modified_payload_is_rejected() {
        let mut payload = seal(&KEY, AAD, b"message");
        *payload.last_mut().unwrap() ^= 1;
        assert_eq!(open(&KEY, AAD, &payload), Err(Error));
    }

    #[test]
    fn truncated_payload_is_rejected() {
        let payload = seal(&KEY, AAD, b"");
        for length in [NONCE_BYTES - 1, OVERHEAD_BYTES - 1] {
            assert_eq!(open(&KEY, AAD, &payload[..length]), Err(Error));
        }
    }

    #[test]
    fn each_payload_has_its_own_nonce() {
        assert_ne!(seal(&KEY, AAD, b"message"), seal(&KEY, AAD, b"message"));
    }
}

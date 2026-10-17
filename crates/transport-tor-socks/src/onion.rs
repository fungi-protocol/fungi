//! Onion service addresses.

use std::fmt;
use std::str::FromStr;

use sha3::{Digest, Sha3_256};

/// A v3 onion service hostname and a virtual port.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Addr {
    host: String,
    port: u16,
}

/// The hostname is not a v3 onion address with a valid checksum, or the text
/// it was parsed from lacks a `:<port>`.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid onion address")]
pub struct InvalidAddr;

impl Addr {
    /// Pair a v3 onion hostname with a virtual port.
    pub fn new(host: String, port: u16) -> Result<Self, InvalidAddr> {
        if !is_v3_host(&host) {
            return Err(InvalidAddr);
        }
        Ok(Self { host, port })
    }

    /// The `.onion` hostname.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The virtual port on the onion service.
    pub fn port(&self) -> u16 {
        self.port
    }
}

impl fmt::Display for Addr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for Addr {
    type Err = InvalidAddr;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let (host, port) = text.rsplit_once(':').ok_or(InvalidAddr)?;
        let port = port.parse().map_err(|_| InvalidAddr)?;
        Self::new(host.to_owned(), port)
    }
}

const VERSION: u8 = 3;

/// `<base32 of pubkey || checksum || version>.onion`, where the checksum is
/// the start of `SHA3-256(".onion checksum" || pubkey || version)`.
fn is_v3_host(host: &str) -> bool {
    let Some(decoded) = host.strip_suffix(".onion").and_then(decode_base32) else {
        return false;
    };
    let (pubkey, checksum, version) = (&decoded[..32], &decoded[32..34], decoded[34]);

    version == VERSION && checksum == checksum_of(pubkey, version)
}

fn checksum_of(pubkey: &[u8], version: u8) -> [u8; 2] {
    let digest = Sha3_256::new()
        .chain_update(b".onion checksum")
        .chain_update(pubkey)
        .chain_update([version])
        .finalize();
    [digest[0], digest[1]]
}

fn decode_base32(label: &str) -> Option<[u8; 35]> {
    if label.len() != 56 {
        return None;
    }

    let mut decoded = [0; 35];
    let (mut bits, mut filled, mut at) = (0u64, 0, 0);
    for byte in label.bytes() {
        let value = match byte {
            b'a'..=b'z' => byte - b'a',
            b'2'..=b'7' => byte - b'2' + 26,
            _ => return None,
        };
        bits = (bits << 5) + u64::from(value);
        filled += 5;
        if filled >= 8 {
            filled -= 8;
            decoded[at] = (bits >> filled) as u8;
            at += 1;
        }
    }
    Some(decoded)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests;

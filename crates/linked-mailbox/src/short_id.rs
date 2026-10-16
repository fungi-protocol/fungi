use std::fmt;

use bech32::{Hrp, NoChecksum};

/// A 64-bit mailbox identifier.
///
/// Displayed as the 13 uppercase bech32 characters that BIP77 uses in
/// mailbox paths, without the human-readable part, separator or checksum.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShortId(pub [u8; 8]);

impl fmt::Display for ShortId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // bech32 has no way to encode only the data part, so this encodes
        // with a placeholder human-readable part and strips it.
        let hrp = Hrp::parse_unchecked("ID");
        let encoded = bech32::encode_upper::<NoChecksum>(hrp, &self.0)
            .expect("8 bytes are within the bech32 length limit");
        f.write_str(&encoded["ID1".len()..])
    }
}

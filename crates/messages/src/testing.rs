//! Helpers shared by the tests of this crate.

use fungi_transport::tlv::{self, TypedSlice};

/// Encodes `records` as a TLV stream.
pub(crate) fn encode<'a>(records: impl IntoIterator<Item = (u64, &'a [u8])>) -> Vec<u8> {
    let mut out = Vec::new();

    let records = records
        .into_iter()
        .map(|(ty, value)| TypedSlice { ty, value });

    tlv::encode(records, &mut out).unwrap();

    out
}

/// Decodes hexadecimal digits, ignoring spaces.
pub(crate) fn hex(encoded: &str) -> Vec<u8> {
    let digits: Vec<u8> = encoded.bytes().filter(|byte| *byte != b' ').collect();

    digits
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

//! Lightning BOLT #1 TLV records: a BigSize type, a BigSize length, and a value
//! of that length.

use std::io::{self, Write};

/// The longest header: a type and a length, each a nine-byte BigSize integer.
pub const MAX_HEADER_LEN: usize = 18;

/// Why a record could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The payload is longer than the limit. Only
    /// [`reassembly`](crate::reassembly) sets a limit.
    #[error("payload of {len} bytes exceeds the limit of {max}")]
    TooLarge {
        /// The payload length announced in the header.
        len: u64,
        /// The limit, `N`, in bytes.
        max: usize,
    },
    /// A BigSize integer is not minimally encoded.
    #[error("non-minimal BigSize integer")]
    NonMinimal,
    /// The bytes end inside a record.
    #[error("bytes end inside a record")]
    Truncated,
}

/// A TLV record: its type and its value. `T` holds the value, borrowed
/// (`&[u8]`) or owned (`Vec<u8>`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TypedSlice<T> {
    pub ty: u64,
    pub value: T,
}

impl<T: AsRef<[u8]>> TypedSlice<T> {
    /// Writes the record to `out`.
    pub fn encode(self, mut out: impl Write) -> io::Result<()> {
        write_bigsize(&mut out, self.ty)?;
        let payload = self.value.as_ref();
        write_bigsize(&mut out, payload.len() as u64)?;

        out.write_all(payload)
    }
}

/// Writes the records to `out`, in order.
pub fn encode<T: AsRef<[u8]>>(
    records: impl IntoIterator<Item = TypedSlice<T>>,
    mut out: impl Write,
) -> io::Result<()> {
    for record in records {
        record.encode(&mut out)?;
    }

    Ok(())
}

/// Parses complete records from `bytes`.
///
/// Fails with [`Error::Truncated`] if `bytes` ends inside a record, or with
/// [`Error::NonMinimal`]. After an error the iterator ends.
pub fn decode(mut bytes: &[u8]) -> impl Iterator<Item = Result<TypedSlice<&[u8]>, Error>> {
    std::iter::from_fn(move || {
        if bytes.is_empty() {
            return None;
        }

        let record = parse_record(&mut bytes);
        if record.is_err() {
            bytes = &[];
        }

        Some(record)
    })
}

/// Parses one record from the start of `bytes`, advancing it on success.
///
/// Fails with [`Error::Truncated`] if the record is incomplete, or with
/// [`Error::NonMinimal`]. On failure, `bytes` stays at its current position.
fn parse_record<'a>(bytes: &mut &'a [u8]) -> Result<TypedSlice<&'a [u8]>, Error> {
    let mut rest = *bytes;
    let (ty, payload_len) = parse_header(&mut rest)?.ok_or(Error::Truncated)?;

    let (payload, rest) = usize::try_from(payload_len)
        .ok()
        .and_then(|payload_len| rest.split_at_checked(payload_len))
        .ok_or(Error::Truncated)?;

    *bytes = rest;
    Ok(TypedSlice { ty, value: payload })
}

/// Parses a header from the start of `bytes`, advancing it on success.
///
/// Returns the type and the payload length. If the header is incomplete,
/// returns `None` and leaves `bytes` unchanged.
pub(crate) fn parse_header(bytes: &mut &[u8]) -> Result<Option<(u64, u64)>, Error> {
    let mut rest = *bytes;
    let Some(ty) = parse_bigsize(&mut rest)? else {
        return Ok(None);
    };

    let Some(payload_len) = parse_bigsize(&mut rest)? else {
        return Ok(None);
    };

    *bytes = rest;
    Ok(Some((ty, payload_len)))
}

/// Parses a BigSize integer from the start of `bytes`, advancing it on success.
///
/// If the integer is incomplete, returns `None` and leaves `bytes` unchanged.
///
/// Fails with [`Error::NonMinimal`] if the integer is not minimally encoded.
fn parse_bigsize(bytes: &mut &[u8]) -> Result<Option<u64>, Error> {
    let Some((&first, rest)) = bytes.split_first() else {
        return Ok(None);
    };

    let (width, floor) = match first {
        0xfd => (2, 0xfd),
        0xfe => (4, 0x1_0000),
        0xff => (8, 0x1_0000_0000),
        small => {
            *bytes = rest;
            return Ok(Some(u64::from(small)));
        }
    };

    let Some((digits, rest)) = rest.split_at_checked(width) else {
        return Ok(None);
    };

    let mut be = [0; 8];
    be[8 - width..].copy_from_slice(digits);
    let value = u64::from_be_bytes(be);

    if value < floor {
        return Err(Error::NonMinimal);
    }

    *bytes = rest;
    Ok(Some(value))
}

/// Writes `value` as a minimally encoded BigSize integer.
fn write_bigsize(mut out: impl Write, value: u64) -> io::Result<()> {
    let (prefix, width) = match value {
        0..=0xfc => (None, 1),
        0xfd..=0xffff => (Some(0xfd), 2),
        0x1_0000..=0xffff_ffff => (Some(0xfe), 4),
        _ => (Some(0xff), 8),
    };

    out.write_all(prefix.as_slice())?;
    out.write_all(&value.to_be_bytes()[8 - width..])
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
pub(crate) mod tests {
    use proptest::prelude::*;

    use super::{Error, TypedSlice, decode, parse_bigsize, write_bigsize};

    pub(crate) type Owned = Vec<(u64, Vec<u8>)>;

    fn decode_to_vec(bytes: &[u8]) -> Result<Owned, Error> {
        decode(bytes)
            .take(bytes.len() + 1)
            .map(|record| record.map(|TypedSlice { ty, value: payload }| (ty, payload.to_vec())))
            .collect()
    }

    pub(crate) fn encode<'a>(records: impl IntoIterator<Item = (u64, &'a [u8])>) -> Vec<u8> {
        let mut out = Vec::new();

        let records = records
            .into_iter()
            .map(|(ty, payload)| TypedSlice { ty, value: payload });

        super::encode(records, &mut out).unwrap();

        out
    }

    pub(crate) fn encode_owned(records: &Owned) -> Vec<u8> {
        encode(records.iter().map(|(ty, payload)| (*ty, &payload[..])))
    }

    /// BOLT #1 Appendix A, BigSize decoding tests.
    #[test]
    fn decodes_the_bigsize_test_vectors() {
        let valid: [(u64, &[u8]); 8] = [
            (0, &[0x00]),
            (252, &[0xfc]),
            (253, &[0xfd, 0x00, 0xfd]),
            (65535, &[0xfd, 0xff, 0xff]),
            (65536, &[0xfe, 0x00, 0x01, 0x00, 0x00]),
            (4294967295, &[0xfe, 0xff, 0xff, 0xff, 0xff]),
            (4294967296, &[0xff, 0, 0, 0, 0x01, 0, 0, 0, 0]),
            (u64::MAX, &[0xff; 9]),
        ];

        for (value, bytes) in valid {
            let mut rest = bytes;

            assert_eq!(parse_bigsize(&mut rest).unwrap(), Some(value));
            assert!(rest.is_empty());

            let mut out = Vec::new();
            write_bigsize(&mut out, value).unwrap();

            assert_eq!(out, bytes);
        }

        let non_minimal: [&[u8]; 3] = [
            &[0xfd, 0x00, 0xfc],
            &[0xfe, 0x00, 0x00, 0xff, 0xff],
            &[0xff, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff],
        ];

        for bytes in non_minimal {
            let mut rest = bytes;

            assert_eq!(parse_bigsize(&mut rest), Err(Error::NonMinimal));
        }

        let short: [&[u8]; 7] = [
            &[],
            &[0xfd],
            &[0xfd, 0x00],
            &[0xfe],
            &[0xfe, 0xff, 0xff],
            &[0xff],
            &[0xff, 0xff, 0xff, 0xff, 0xff],
        ];

        for bytes in short {
            let mut rest = bytes;

            assert_eq!(parse_bigsize(&mut rest).unwrap(), None);
            assert_eq!(rest, bytes);
        }
    }

    #[test]
    fn encodes_the_type_the_length_then_the_payload() {
        let bytes = encode([(2, &b"first"[..]), (253, b"")]);

        assert_eq!(bytes, b"\x02\x05first\xfd\x00\xfd\x00");
    }

    #[test]
    fn decodes_records_in_order() {
        let bytes = encode([(2, &b"first"[..]), (1, b""), (2, b"second")]);

        assert_eq!(
            decode_to_vec(&bytes).unwrap(),
            [
                (2, b"first".to_vec()),
                (1, Vec::new()),
                (2, b"second".to_vec())
            ]
        );
    }

    #[test]
    fn bytes_ending_inside_a_record_are_truncated() {
        for whole in [encode([(2, &b"cut"[..])]), encode([(0xfd, &b"cut"[..])])] {
            for len in 1..whole.len() {
                let mut records = decode(&whole[..len]);

                assert_eq!(records.next(), Some(Err(Error::Truncated)));
                assert_eq!(records.next(), None);
            }
        }
    }

    /// Bytes that end before the announced payload length are truncated, for
    /// any length.
    #[test]
    fn a_length_past_the_bytes_is_truncated() {
        let mut records = decode(&[0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

        assert_eq!(records.next(), Some(Err(Error::Truncated)));
        assert_eq!(records.next(), None);
    }

    #[test]
    fn decoding_refuses_a_non_minimal_length() {
        let mut records = decode(&[0x02, 0xfd, 0x00, 0x01, b'x']);

        assert_eq!(records.next(), Some(Err(Error::NonMinimal)));
        assert_eq!(records.next(), None);
    }

    fn generate() -> impl Strategy<Value = Owned> {
        let payload = prop::collection::vec(any::<u8>(), 0..64);

        prop::collection::vec((any::<u64>(), payload), 0..8)
    }

    proptest! {
        #[test]
        fn decodes_what_was_encoded(records in generate()) {
            prop_assert_eq!(decode_to_vec(&encode_owned(&records)), Ok(records));
        }

        /// Each integer has one encoding. Re-encoding a decoded integer returns
        /// the bytes it was decoded from.
        #[test]
        fn decoded_bigsize_is_minimal(bytes in prop::collection::vec(any::<u8>(), 0..10)) {
            let mut rest = &bytes[..];

            if let Ok(Some(value)) = parse_bigsize(&mut rest) {
                let mut out = Vec::new();
                write_bigsize(&mut out, value).unwrap();

                prop_assert_eq!(&out[..], &bytes[..bytes.len() - rest.len()]);
            }
        }

        #[test]
        fn encoded_bigsize_decodes_to_itself(value in any::<u64>()) {
            let mut out = Vec::new();
            write_bigsize(&mut out, value).unwrap();

            let mut rest = &out[..];

            prop_assert_eq!(parse_bigsize(&mut rest), Ok(Some(value)));
            prop_assert!(rest.is_empty());
        }
    }
}

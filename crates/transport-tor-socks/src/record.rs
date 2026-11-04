//! Lightning TLV records (BOLT #1): a BigSize type, a BigSize length, and
//! that many bytes of payload.
//!
//! Decoding borrows payloads from its input.

/// Malformed records.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// A type or length is spelled in more bytes than it needs.
    #[error("non-minimal BigSize integer")]
    NonMinimal,
    /// The bytes end inside a record.
    #[error("bytes end inside a record")]
    Truncated,
}

/// The records, each as its type, its length and its payload.
pub fn encode<'a>(records: impl IntoIterator<Item = (u64, &'a [u8])>) -> Vec<u8> {
    let mut out = Vec::new();
    for (ty, payload) in records {
        put_bigsize(&mut out, ty);
        put_bigsize(&mut out, payload.len() as u64);
        out.extend_from_slice(payload);
    }
    out
}

/// Each record in `bytes`, which must end where a record ends.
pub fn records(mut bytes: &[u8]) -> impl Iterator<Item = Result<(u64, &[u8]), Error>> {
    std::iter::from_fn(move || {
        if bytes.is_empty() {
            return None;
        }
        let record = header(bytes).and_then(|header| {
            let (ty, len, rest) = header.ok_or(Error::Truncated)?;
            let payload = usize::try_from(len)
                .ok()
                .and_then(|len| rest.get(..len))
                .ok_or(Error::Truncated)?;
            bytes = &rest[payload.len()..];
            Ok((ty, payload))
        });
        if record.is_err() {
            bytes = &[];
        }
        Some(record)
    })
}

/// A record's type and length, and the bytes after them.
type Header<'a> = (u64, u64, &'a [u8]);

/// The header at the start of `bytes`, or `None` while incomplete.
fn header(bytes: &[u8]) -> Result<Option<Header<'_>>, Error> {
    let Some((ty, rest)) = bigsize(bytes)? else {
        return Ok(None);
    };
    let Some((len, rest)) = bigsize(rest)? else {
        return Ok(None);
    };
    Ok(Some((ty, len, rest)))
}

/// The BigSize integer at the start of `bytes` and the bytes after it, or
/// `None` while it is incomplete.
fn bigsize(bytes: &[u8]) -> Result<Option<(u64, &[u8])>, Error> {
    let Some((&first, rest)) = bytes.split_first() else {
        return Ok(None);
    };
    let (width, floor) = match first {
        0xfd => (2, 0xfd),
        0xfe => (4, 0x1_0000),
        0xff => (8, 0x1_0000_0000),
        small => return Ok(Some((u64::from(small), rest))),
    };
    if rest.len() < width {
        return Ok(None);
    }
    let (digits, rest) = rest.split_at(width);
    let mut be = [0; 8];
    be[8 - width..].copy_from_slice(digits);
    let value = u64::from_be_bytes(be);
    if value < floor {
        return Err(Error::NonMinimal);
    }
    Ok(Some((value, rest)))
}

/// Append the minimal BigSize encoding of `value`.
fn put_bigsize(out: &mut Vec<u8>, value: u64) {
    let (prefix, width) = match value {
        0..=0xfc => (None, 1),
        0xfd..=0xffff => (Some(0xfd), 2),
        0x1_0000..=0xffff_ffff => (Some(0xfe), 4),
        _ => (Some(0xff), 8),
    };
    out.extend(prefix);
    out.extend_from_slice(&value.to_be_bytes()[8 - width..]);
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use proptest::prelude::*;

    use super::{Error, bigsize, encode, put_bigsize, records};

    const MAX: u32 = 16;

    type Owned = Vec<(u64, Vec<u8>)>;

    fn owned<'a>(
        records: impl IntoIterator<Item = Result<(u64, &'a [u8]), Error>>,
    ) -> Result<Owned, Error> {
        records
            .into_iter()
            .map(|record| record.map(|(ty, payload)| (ty, payload.to_vec())))
            .collect()
    }

    /// Every record in `bytes`. Each takes at least two bytes, so the bound
    /// only cuts short a split that never ends.
    fn split(bytes: &[u8]) -> Result<Owned, Error> {
        owned(records(bytes).take(bytes.len() / 2 + 1))
    }

    fn encoded(records: &Owned) -> Vec<u8> {
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
            assert_eq!(bigsize(bytes).unwrap(), Some((value, &[][..])));
            let mut out = Vec::new();
            put_bigsize(&mut out, value);
            assert_eq!(out, bytes);
        }

        let non_minimal: [&[u8]; 3] = [
            &[0xfd, 0x00, 0xfc],
            &[0xfe, 0x00, 0x00, 0xff, 0xff],
            &[0xff, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff],
        ];
        for bytes in non_minimal {
            assert_eq!(bigsize(bytes), Err(Error::NonMinimal));
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
            assert_eq!(bigsize(bytes).unwrap(), None);
        }
    }

    #[test]
    fn encodes_the_type_the_length_then_the_payload() {
        let bytes = encode([(2, &b"first"[..]), (253, b"")]);

        assert_eq!(bytes, b"\x02\x05first\xfd\x00\xfd\x00");
    }

    #[test]
    fn splits_records_in_order() {
        let bytes = encode([(2, &b"first"[..]), (1, b""), (2, b"second")]);

        assert_eq!(
            split(&bytes).unwrap(),
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
                let mut split = records(&whole[..len]);
                assert_eq!(split.next(), Some(Err(Error::Truncated)));
                assert_eq!(split.next(), None);
            }
        }
    }

    /// Whatever the length, bytes that end before it are truncated.
    #[test]
    fn a_length_past_the_bytes_is_truncated() {
        let mut split = records(&[0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

        assert_eq!(split.next(), Some(Err(Error::Truncated)));
        assert_eq!(split.next(), None);
    }

    #[test]
    fn splitting_refuses_a_non_minimal_length() {
        let mut split = records(&[0x02, 0xfd, 0x00, 0x01, b'x']);

        assert_eq!(split.next(), Some(Err(Error::NonMinimal)));
        assert_eq!(split.next(), None);
    }

    #[test]
    fn borrows_payloads_from_the_input() {
        let bytes = encode([(1, &b"ab"[..]), (2, b"cd")]);

        let (_, payload) = records(&bytes).next().unwrap().unwrap();

        assert!(std::ptr::eq(payload, &bytes[2..4]));
    }

    fn within_limit() -> impl Strategy<Value = Owned> {
        let payload = prop::collection::vec(any::<u8>(), 0..=MAX as usize);
        prop::collection::vec((any::<u64>(), payload), 0..8)
    }

    proptest! {
        #[test]
        fn splits_what_was_encoded(records in within_limit()) {
            prop_assert_eq!(split(&encoded(&records)), Ok(records));
        }

        /// One spelling per integer: whatever decodes re-encodes to the bytes
        /// it took.
        #[test]
        fn decoded_bigsize_is_minimal(bytes in prop::collection::vec(any::<u8>(), 0..10)) {
            if let Ok(Some((value, rest))) = bigsize(&bytes) {
                let mut out = Vec::new();
                put_bigsize(&mut out, value);
                prop_assert_eq!(&out[..], &bytes[..bytes.len() - rest.len()]);
            }
        }

        #[test]
        fn encoded_bigsize_decodes_to_itself(value in any::<u64>()) {
            let mut out = Vec::new();
            put_bigsize(&mut out, value);
            prop_assert_eq!(bigsize(&out), Ok(Some((value, &[][..]))));
        }
    }
}

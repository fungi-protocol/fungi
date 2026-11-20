//! The fields of a message, read as a BOLT #1 TLV stream from a borrowed slice.
//!
//! The bytes may be adversarial. A stream is accepted only in its one valid
//! form: types strictly increasing, every known field where the caller expects
//! it, no unknown even type, and no bytes after the last record. Values are
//! borrowed from the input, so a field holding a stream of its own is read with
//! [`read`] again, without copying.

use std::fmt;

use fungi_transport::tlv::{self, TypedSlice};

/// Why a TLV stream could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A record could not be parsed.
    #[error(transparent)]
    Tlv(#[from] tlv::Error),
    /// A field's type is not greater than the type of the field before it.
    #[error("field type {ty} follows field type {previous}")]
    NotIncreasing {
        /// The type of the field before.
        previous: u64,
        /// The type of the field.
        ty: u64,
    },
    /// A required field is absent.
    #[error("missing field type {ty}")]
    Missing {
        /// The type of the field.
        ty: u64,
    },
    /// A fixed-size field has another length.
    #[error("field type {ty} is {len} bytes long, not {expected}")]
    WrongLength {
        /// The type of the field.
        ty: u64,
        /// The length the field must have.
        expected: usize,
        /// The length of the value.
        len: usize,
    },
    /// A field of an unknown even type. BOLT #1 requires the reader to fail.
    #[error("unknown even field type {ty}")]
    UnknownEven {
        /// The type of the field.
        ty: u64,
    },
}

/// Starts reading the fields of `value`.
pub fn read(
    value: &[u8],
) -> Reader<'_, impl Iterator<Item = Result<TypedSlice<&[u8]>, tlv::Error>>> {
    Reader {
        records: tlv::decode(value),
        next: None,
        last: None,
        requested: None,
        failed: None,
    }
}

/// A reader of the fields of one message.
///
/// The caller requests the fields it knows in increasing order of type, and a
/// request out of order panics, because it is a bug in the caller rather than
/// in the input. Each request skips the unknown odd fields before it.
/// [`finish`](Self::finish) checks the fields after the last request. After an
/// error, every later call fails with the same error.
#[must_use = "a stream is checked only up to the last request until `finish` is called"]
pub struct Reader<'a, I> {
    records: I,
    /// Read from `records`, not yet returned.
    next: Option<TypedSlice<&'a [u8]>>,
    /// The type of the last record read from `records`.
    last: Option<u64>,
    /// The type of the last request.
    requested: Option<u64>,
    /// The error that every later call returns.
    failed: Option<Error>,
}

impl<'a, I> Reader<'a, I>
where
    I: Iterator<Item = Result<TypedSlice<&'a [u8]>, tlv::Error>>,
{
    /// Returns the value of the field of type `ty`, or `None` if the stream has
    /// no such field.
    ///
    /// Fails if a record read while looking for it is malformed or out of order,
    /// or if a field before it is of an unknown even type. Later records are
    /// checked by later calls and by [`finish`](Self::finish).
    pub fn optional(&mut self, ty: u64) -> Result<Option<&'a [u8]>, Error> {
        let value = self.find(ty);

        self.remember(value)
    }

    /// Returns the value of the field of type `ty`.
    ///
    /// Fails as [`optional`](Self::optional) does, or with [`Error::Missing`].
    pub fn required(&mut self, ty: u64) -> Result<&'a [u8], Error> {
        let value = self
            .find(ty)
            .and_then(|value| value.ok_or(Error::Missing { ty }));

        self.remember(value)
    }

    /// Returns the value of the field of type `ty` as an array of exactly `N`
    /// bytes.
    ///
    /// Fails as [`required`](Self::required) does, or with
    /// [`Error::WrongLength`].
    pub fn required_array<const N: usize>(&mut self, ty: u64) -> Result<[u8; N], Error> {
        let value = self.required(ty)?;

        let array = value.try_into().map_err(|_| Error::WrongLength {
            ty,
            expected: N,
            len: value.len(),
        });

        self.remember(array)
    }

    /// Checks that the fields after the last request are all of unknown odd
    /// types, and that the stream ends with a complete record.
    pub fn finish(mut self) -> Result<(), Error> {
        while let Some(record) = self.peek()? {
            self.next = None;
            check_unknown(record)?;
        }

        Ok(())
    }

    fn find(&mut self, ty: u64) -> Result<Option<&'a [u8]>, Error> {
        assert!(
            self.requested.is_none_or(|requested| ty > requested),
            "fields must be requested in increasing order of type",
        );
        self.requested = Some(ty);

        while let Some(record) = self.peek()?.filter(|record| record.ty <= ty) {
            self.next = None;

            if record.ty == ty {
                return Ok(Some(record.value));
            }

            check_unknown(record)?;
        }

        Ok(None)
    }

    /// Keeps the error of `result`, if any, for every later call to return.
    fn remember<T>(&mut self, result: Result<T, Error>) -> Result<T, Error> {
        if let Err(error) = &result {
            self.failed = Some(error.clone());
        }

        result
    }

    fn peek(&mut self) -> Result<Option<TypedSlice<&'a [u8]>>, Error> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }

        if self.next.is_none() {
            self.next = self.records.next().transpose()?;

            if let Some(TypedSlice { ty, .. }) = self.next {
                if let Some(previous) = self.last.filter(|&previous| ty <= previous) {
                    return Err(Error::NotIncreasing { previous, ty });
                }

                self.last = Some(ty);
            }
        }

        Ok(self.next)
    }
}

/// Shows no field values, so a reader can be logged.
impl<I> fmt::Debug for Reader<'_, I> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("last", &self.last)
            .field("requested", &self.requested)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

/// Checks that a field the reader does not know may be skipped. BOLT #1 allows
/// this only for odd types.
fn check_unknown(record: TypedSlice<&[u8]>) -> Result<(), Error> {
    if record.ty.is_multiple_of(2) {
        return Err(Error::UnknownEven { ty: record.ty });
    }

    Ok(())
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::tlv::Error::{NonMinimal, Truncated};
    use proptest::prelude::*;

    use super::{Error, read};
    use crate::testing::{encode, hex};

    #[test]
    fn reads_fields_in_increasing_order() {
        let bytes = encode([(0, &b"first"[..]), (2, b"second")]);
        let mut fields = read(&bytes);

        assert_eq!(fields.optional(0), Ok(Some(&b"first"[..])));
        assert_eq!(fields.optional(2), Ok(Some(&b"second"[..])));
    }

    #[test]
    fn an_absent_field_leaves_the_next_one() {
        let bytes = encode([(2, &b"second"[..])]);
        let mut fields = read(&bytes);

        assert_eq!(fields.optional(0), Ok(None));
        assert_eq!(fields.optional(2), Ok(Some(&b"second"[..])));
    }

    #[test]
    fn refuses_a_repeated_type() {
        let bytes = encode([(0, &b"first"[..]), (0, b"again")]);
        let mut fields = read(&bytes);

        assert_eq!(fields.optional(0), Ok(Some(&b"first"[..])));
        assert_eq!(
            fields.optional(2),
            Err(Error::NotIncreasing { previous: 0, ty: 0 })
        );
    }

    #[test]
    fn skips_an_unknown_odd_field() {
        let bytes = encode([(1, &b"unknown"[..]), (2, b"second")]);
        let mut fields = read(&bytes);

        assert_eq!(fields.optional(2), Ok(Some(&b"second"[..])));
    }

    #[test]
    fn refuses_an_unknown_even_field() {
        let bytes = encode([(0, &b"unknown"[..]), (2, b"second")]);
        let mut fields = read(&bytes);

        assert_eq!(fields.optional(2), Err(Error::UnknownEven { ty: 0 }));
    }

    #[test]
    fn refuses_an_absent_required_field() {
        let bytes = encode([(2, &b"second"[..])]);
        let mut fields = read(&bytes);

        assert_eq!(fields.required(0), Err(Error::Missing { ty: 0 }));
    }

    #[test]
    fn finishing_skips_trailing_odd_fields() {
        let bytes = encode([(0, &b"first"[..]), (5, b"unknown")]);
        let mut fields = read(&bytes);

        assert_eq!(fields.required(0), Ok(&b"first"[..]));
        assert_eq!(fields.finish(), Ok(()));
    }

    #[test]
    fn finishing_refuses_a_trailing_even_field() {
        let bytes = encode([(0, &b"first"[..]), (4, b"unknown")]);
        let mut fields = read(&bytes);

        assert_eq!(fields.required(0), Ok(&b"first"[..]));
        assert_eq!(fields.finish(), Err(Error::UnknownEven { ty: 4 }));
    }

    /// A caller that ignores an error and goes on cannot read past it.
    #[test]
    fn every_call_after_an_error_fails_with_it() {
        let bytes = encode([(2, &b"second"[..]), (0, b"first"), (1, b"odd")]);
        let mut fields = read(&bytes);
        let error = Error::NotIncreasing { previous: 2, ty: 0 };

        assert_eq!(fields.optional(2), Ok(Some(&b"second"[..])));
        assert_eq!(fields.optional(4), Err(error.clone()));
        assert_eq!(fields.optional(5), Err(error.clone()));
        assert_eq!(fields.finish(), Err(error));
    }

    #[test]
    fn every_call_after_a_missing_field_fails_with_it() {
        let bytes = encode([(2, &b"second"[..])]);
        let mut fields = read(&bytes);
        let error = Error::Missing { ty: 0 };

        assert_eq!(fields.required(0), Err(error.clone()));
        assert_eq!(fields.optional(2), Err(error.clone()));
        assert_eq!(fields.finish(), Err(error));
    }

    /// The public key in the BOLT #1 Appendix B vectors for `n1`'s `tlv3`.
    const NODE_ID: &str = "023da092f6980e58d2c037173180e9a465476026ee50f96695963e8efe436f54eb";

    /// BOLT #1 Appendix B, the TLV decoding successes for namespace `n1`'s
    /// fixed-length types: `tlv2` of 8 bytes, `tlv3` of 49 and `tlv4` of 2.
    #[test]
    fn reads_the_bolt_fixed_length_fields() {
        let tlv3 = format!("{NODE_ID} 0000000000000001 0000000000000002");

        assert_eq!(
            read(&hex("02 08 0000000000000226")).required_array::<8>(2),
            Ok(0x226u64.to_be_bytes())
        );
        assert_eq!(
            read(&hex(&format!("03 31 {tlv3}"))).required_array::<49>(3),
            Ok(hex(&tlv3).try_into().unwrap())
        );
        assert_eq!(
            read(&hex("fd00fe 02 0226")).required_array::<2>(254),
            Ok(550u16.to_be_bytes())
        );
    }

    /// Reads `encoded` as a stream whose only field is `ty`, of `N` bytes, and
    /// checks that its length `len` is refused, also by `finish`.
    fn assert_wrong_length<const N: usize>(encoded: &str, ty: u64, len: usize) {
        let bytes = hex(encoded);
        let mut fields = read(&bytes);
        let error = Error::WrongLength {
            ty,
            expected: N,
            len,
        };

        assert_eq!(
            fields.required_array::<N>(ty),
            Err(error.clone()),
            "{encoded}"
        );
        assert_eq!(fields.finish(), Err(error), "{encoded}");
    }

    /// BOLT #1 Appendix B, the TLV decoding failures for namespace `n1`'s
    /// fixed-length types, except for the invalid point.
    #[test]
    fn refuses_the_bolt_fields_of_another_length() {
        assert_wrong_length::<8>("02 07 01010101010101", 2, 7);
        assert_wrong_length::<8>("02 09 010101010101010101", 2, 9);
        assert_wrong_length::<49>(&format!("03 21 {NODE_ID}"), 3, 0x21);
        assert_wrong_length::<49>(&format!("03 29 {NODE_ID} 0000000000000001"), 3, 0x29);
        assert_wrong_length::<49>(
            &format!("03 30 {NODE_ID} 0000000000000001 00000000000001"),
            3,
            0x30,
        );
        assert_wrong_length::<49>(
            &format!("03 32 {NODE_ID} 0000000000000001 000000000000000001"),
            3,
            0x32,
        );
        assert_wrong_length::<2>("fd00fe 00", 254, 0);
        assert_wrong_length::<2>("fd00fe 01 01", 254, 1);
        assert_wrong_length::<2>("fd00fe 03 010101", 254, 3);
    }

    /// A request below an earlier one would read past the field it asks for
    /// and report it absent.
    #[test]
    #[should_panic(expected = "requested in increasing order")]
    fn panics_on_a_request_out_of_order() {
        let bytes = encode([(1, &b"first"[..]), (3, b"second")]);
        let mut fields = read(&bytes);

        let _ = fields.optional(3);
        let _ = fields.optional(1);
    }

    #[test]
    #[should_panic(expected = "requested in increasing order")]
    fn panics_on_a_repeated_request() {
        let bytes = encode([(0, &b"first"[..])]);
        let mut fields = read(&bytes);

        let _ = fields.required(0);
        let _ = fields.required(0);
    }

    #[test]
    fn debug_shows_no_field_values() {
        let bytes = encode([(1, &b"secret"[..]), (4, b"secret")]);
        let mut fields = read(&bytes);

        assert_eq!(fields.optional(2), Ok(None));
        assert_eq!(
            format!("{fields:?}"),
            "Reader { last: Some(4), requested: Some(2), failed: None, .. }"
        );
    }

    /// Reads a stream in which no type is known.
    fn read_unknown(bytes: &[u8]) -> Result<(), Error> {
        read(bytes).finish()
    }

    /// BOLT #1 Appendix B, the TLV decoding failures for any namespace.
    #[test]
    fn refuses_the_bolt_decoding_failures() {
        let value_truncated = format!("0f fd0201 {}", "00".repeat(256));

        let failures: [(&str, Error); 13] = [
            ("fd", Truncated.into()),
            ("fd01", Truncated.into()),
            ("fd0001 00", NonMinimal.into()),
            ("fd0101", Truncated.into()),
            ("0f fd", Truncated.into()),
            ("0f fd26", Truncated.into()),
            ("0f fd2602", Truncated.into()),
            ("0f fd0001 00", NonMinimal.into()),
            (&value_truncated, Truncated.into()),
            ("12 00", Error::UnknownEven { ty: 0x12 }),
            ("fd0102 00", Error::UnknownEven { ty: 0x102 }),
            ("fe01000002 00", Error::UnknownEven { ty: 0x0100_0002 }),
            (
                "ff0100000000000002 00",
                Error::UnknownEven {
                    ty: 0x0100_0000_0000_0002,
                },
            ),
        ];

        for (encoded, error) in failures {
            assert_eq!(read_unknown(&hex(encoded)), Err(error), "{encoded}");
        }
    }

    /// BOLT #1 Appendix B, the TLV decoding successes for any namespace.
    #[test]
    fn skips_the_bolt_unknown_odd_types() {
        let successes = [
            "",
            "21 00",
            "fd0201 00",
            "fd00fd 00",
            "fd00ff 00",
            "fe02000001 00",
            "ff0200000000000001 00",
        ];

        for encoded in successes {
            assert_eq!(read_unknown(&hex(encoded)), Ok(()), "{encoded}");
        }
    }

    /// BOLT #1 Appendix B, the TLV stream decoding failures, read with the
    /// types of namespaces `n1` (1, 2, 3 and 254) and `n2` (0 and 11).
    #[test]
    fn refuses_the_bolt_stream_ordering_failures() {
        let read_known = |known: &[u64], bytes: &[u8]| {
            let mut fields = read(bytes);

            for &ty in known {
                fields.optional(ty)?;
            }

            fields.finish()
        };

        let n1 = [1, 2, 3, 254];
        let n2 = [0, 11];

        let failures: [(&[u64], &str, Error); 5] = [
            (
                &n1,
                "02 08 0000000000000226 01 01 2a",
                Error::NotIncreasing { previous: 2, ty: 1 },
            ),
            (
                &n1,
                "02 08 0000000000000231 02 08 0000000000000451",
                Error::NotIncreasing { previous: 2, ty: 2 },
            ),
            (
                &n1,
                "1f 00 0f 01 2a",
                Error::NotIncreasing {
                    previous: 0x1f,
                    ty: 0x0f,
                },
            ),
            (
                &n1,
                "1f 00 1f 01 2a",
                Error::NotIncreasing {
                    previous: 0x1f,
                    ty: 0x1f,
                },
            ),
            (
                &n2,
                "ffffffffffffffffff 00 00 00",
                Error::NotIncreasing {
                    previous: u64::MAX,
                    ty: 0,
                },
            ),
        ];

        for (known, encoded, error) in failures {
            assert_eq!(read_known(known, &hex(encoded)), Err(error), "{encoded}");
        }
    }

    /// The value of a field is read as a stream of its own, borrowed from the
    /// same bytes rather than copied.
    #[test]
    fn reads_nested_fields_from_the_same_bytes() {
        let inner = encode([(0, &b"inner"[..])]);
        let bytes = encode([(2, &inner[..])]);

        let outer = read(&bytes).required(2).unwrap();
        let value = read(outer).required(0).unwrap();

        assert_eq!(value, b"inner");
        assert!(bytes.as_ptr_range().contains(&value.as_ptr()));
    }

    /// Strictly increasing types, each with a payload.
    fn increasing() -> impl Strategy<Value = Vec<(u64, Vec<u8>)>> {
        prop::collection::btree_map(
            any::<u64>(),
            prop::collection::vec(any::<u8>(), 0..16),
            0..8,
        )
        .prop_map(|fields| fields.into_iter().collect())
    }

    proptest! {
        #[test]
        fn reads_what_was_encoded(records in increasing()) {
            let bytes = encode(records.iter().map(|(ty, value)| (*ty, &value[..])));
            let mut fields = read(&bytes);

            for (ty, value) in &records {
                prop_assert_eq!(fields.optional(*ty), Ok(Some(&value[..])));
            }

            prop_assert_eq!(fields.finish(), Ok(()));
        }

        /// Whatever the bytes, the first error is the result of every later
        /// call.
        #[test]
        fn the_first_error_on_any_bytes_repeats(
            bytes in prop::collection::vec(any::<u8>(), 0..64),
            known in prop::collection::btree_set(any::<u64>(), 0..4),
        ) {
            let mut fields = read(&bytes);
            let mut first = None;

            for ty in known {
                let result = fields.optional(ty);

                match &first {
                    Some(error) => prop_assert_eq!(result, Err(Clone::clone(error))),
                    None => first = result.err(),
                }
            }

            let finished = fields.finish();

            if let Some(error) = first {
                prop_assert_eq!(finished, Err(error));
            }
        }
    }
}

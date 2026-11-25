//! The fields of a message, read as a BOLT #1 TLV stream from a borrowed slice.
//!
//! The bytes may be adversarial. [`Reader`] reads the stream as a message
//! expects it. The types of the fields must strictly increase, so a type
//! appears at most once. The caller asks for each field it knows by type. Every
//! field it does not ask for must be of an odd type, and a required field must
//! appear. Values are borrowed from the input, so a field holding a stream of
//! its own is read again without copying.

use std::{fmt, iter::Peekable};

use fungi_transport::tlv::{self, TypedSlice};

/// Why a TLV stream could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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
    /// A field of an unknown even type. BOLT #1 requires the reader to fail.
    #[error("unexpected field type {ty}")]
    UnexpectedType {
        /// The type of the field.
        ty: u64,
    },
    /// An earlier call on the [`Reader`] failed, and that call returned the
    /// error.
    #[error("an earlier call on the reader failed")]
    PriorError,
}

/// Why [`Reader::required`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RequiredError {
    /// The stream could not be read.
    #[error(transparent)]
    Read(#[from] Error),
    /// The field is absent.
    #[error("missing field type {ty}")]
    Missing {
        /// The type of the field.
        ty: u64,
    },
}

/// A fixed-size field has another length.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("field type {ty} is {len} bytes long, not {expected}")]
pub struct WrongLength {
    /// The type of the field.
    pub ty: u64,
    /// The length the field must have.
    pub expected: usize,
    /// The length of the value.
    pub len: usize,
}

/// Parses the fields of `value`.
///
/// Fails as [`tlv::Reader`] does, or with [`Error::NotIncreasing`]. After an
/// error the iterator ends.
fn read(value: &[u8]) -> Fields<'_> {
    Fields {
        records: tlv::Reader::new(value),
        last: None,
        failed: false,
    }
}

/// The fields of a stream, returned by [`read`].
struct Fields<'a> {
    records: tlv::Reader<'a>,
    last: Option<u64>,
    failed: bool,
}

impl<'a> Iterator for Fields<'a> {
    type Item = Result<TypedSlice<&'a [u8]>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        let field = self
            .records
            .next()?
            .map_err(Error::from)
            .and_then(|record| {
                if let Some(previous) = self.last.filter(|&previous| record.ty <= previous) {
                    return Err(Error::NotIncreasing {
                        previous,
                        ty: record.ty,
                    });
                }

                self.last = Some(record.ty);
                Ok(record)
            });

        self.failed = field.is_err();
        Some(field)
    }
}

/// A scanner of the fields of one message.
///
/// The caller asks for the fields it knows in increasing order of type, and a
/// request out of order panics, because it is a bug in the caller rather than
/// in the input. Each request skips the unknown odd fields before it, and
/// [`finish`](Self::finish) checks the fields after the last request. After an
/// error, every later call fails with [`Error::PriorError`].
#[must_use = "a stream is checked only up to the last request until `finish` is called"]
pub struct Reader<'a> {
    fields: Peekable<Fields<'a>>,
    /// The type of the last request, to reject requests out of order.
    requested: Option<u64>,
    /// Whether a call has failed.
    failed: bool,
}

impl<'a> Reader<'a> {
    /// Starts scanning the fields of `value`.
    pub fn new(value: &'a [u8]) -> Self {
        Self {
            fields: read(value).peekable(),
            requested: None,
            failed: false,
        }
    }

    /// Returns the field of type `ty`, or `None` if the stream has no such
    /// field.
    ///
    /// Fails if a field read while looking for it is malformed or out of order,
    /// or if a field before it is of an unknown even type. Later fields are
    /// checked by later calls and by [`finish`](Self::finish).
    pub fn optional(&mut self, ty: u64) -> Result<Option<TypedSlice<&'a [u8]>>, Error> {
        if self.failed {
            return Err(Error::PriorError);
        }

        assert!(
            self.requested.is_none_or(|requested| ty > requested),
            "fields must be requested in increasing order of type",
        );
        self.requested = Some(ty);

        let field = self.find(ty);
        self.failed = field.is_err();

        field
    }

    /// Returns the field of type `ty`.
    ///
    /// Fails as [`optional`](Self::optional) does, or with
    /// [`RequiredError::Missing`].
    pub fn required(&mut self, ty: u64) -> Result<TypedSlice<&'a [u8]>, RequiredError> {
        let field = self.optional(ty)?.ok_or(RequiredError::Missing { ty });
        self.failed = field.is_err();

        field
    }

    /// Checks that the fields after the last request are all of unknown odd
    /// types, and that the stream ends with a complete record.
    pub fn finish(self) -> Result<(), Error> {
        if self.failed {
            return Err(Error::PriorError);
        }

        for field in self.fields {
            unknown(field?)?;
        }

        Ok(())
    }

    fn find(&mut self, ty: u64) -> Result<Option<TypedSlice<&'a [u8]>>, Error> {
        while let Some(field) = self
            .fields
            .next_if(|field| !matches!(field, Ok(field) if field.ty > ty))
        {
            let field = field?;

            if field.ty == ty {
                return Ok(Some(field));
            }

            unknown(field)?;
        }

        Ok(None)
    }
}

/// Shows no field values, so a reader can be logged.
impl fmt::Debug for Reader<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field("requested", &self.requested)
            .field("failed", &self.failed)
            .finish_non_exhaustive()
    }
}

/// Checks a field the reader does not know. BOLT #1 lets the reader skip it
/// only if the type is odd.
fn unknown(field: TypedSlice<&[u8]>) -> Result<(), Error> {
    if field.ty.is_multiple_of(2) {
        return Err(Error::UnexpectedType { ty: field.ty });
    }

    Ok(())
}

/// Returns the value of `field` as an array of exactly `N` bytes.
pub fn array<const N: usize>(field: TypedSlice<&[u8]>) -> Result<[u8; N], WrongLength> {
    field.value.try_into().map_err(|_| WrongLength {
        ty: field.ty,
        expected: N,
        len: field.value.len(),
    })
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::tlv::{
        Error::{NonMinimal, Truncated},
        TypedSlice,
    };
    use proptest::prelude::*;

    use super::{Error, Reader, RequiredError, WrongLength, array, read};
    use crate::testing::{encode, hex};

    /// Reads every field of `bytes` as a type and a value, up to the first
    /// error.
    fn fields(bytes: &[u8]) -> Result<Vec<(u64, &[u8])>, Error> {
        read(bytes)
            .map(|field| field.map(|TypedSlice { ty, value }| (ty, value)))
            .collect()
    }

    /// Returns the first field of `bytes`, which must be valid.
    fn first(bytes: &[u8]) -> TypedSlice<&[u8]> {
        read(bytes).next().unwrap().unwrap()
    }

    #[test]
    fn reads_fields_in_increasing_order() {
        let bytes = encode([(0, &b"first"[..]), (1, b"odd"), (2, b"second")]);

        assert_eq!(
            fields(&bytes),
            Ok(vec![(0, &b"first"[..]), (1, b"odd"), (2, b"second")])
        );
    }

    #[test]
    fn refuses_a_repeated_type() {
        let bytes = encode([(0, &b"first"[..]), (0, b"again")]);

        assert_eq!(
            fields(&bytes),
            Err(Error::NotIncreasing { previous: 0, ty: 0 })
        );
    }

    /// A caller that ignores an error and goes on cannot read past it.
    #[test]
    fn ends_after_fields_out_of_order() {
        let bytes = encode([(2, &b"second"[..]), (0, b"first"), (4, b"fourth")]);
        let mut fields = read(&bytes);

        assert_eq!(
            fields.next(),
            Some(Ok(TypedSlice {
                ty: 2,
                value: &b"second"[..]
            }))
        );
        assert_eq!(
            fields.next(),
            Some(Err(Error::NotIncreasing { previous: 2, ty: 0 }))
        );
        assert_eq!(fields.next(), None);
        assert_eq!(fields.next(), None);
    }

    #[test]
    fn ends_after_a_malformed_record() {
        let bytes = hex("01 01 2a fd");
        let mut fields = read(&bytes);

        assert!(matches!(fields.next(), Some(Ok(_))));
        assert_eq!(fields.next(), Some(Err(Truncated.into())));
        assert_eq!(fields.next(), None);
    }

    #[test]
    fn scans_the_requested_fields() {
        let bytes = encode([(0, &b"first"[..]), (2, b"second")]);
        let mut fields = Reader::new(&bytes);

        assert_eq!(
            fields.required(0).map(|field| field.value),
            Ok(&b"first"[..])
        );
        assert_eq!(
            fields.required(2).map(|field| field.value),
            Ok(&b"second"[..])
        );
        assert_eq!(fields.finish(), Ok(()));
    }

    #[test]
    fn an_absent_field_leaves_the_next_one() {
        let bytes = encode([(2, &b"second"[..])]);
        let mut fields = Reader::new(&bytes);

        assert_eq!(fields.optional(0), Ok(None));
        assert_eq!(
            fields.optional(2),
            Ok(Some(TypedSlice {
                ty: 2,
                value: &b"second"[..]
            }))
        );
    }

    #[test]
    fn skips_an_unknown_odd_field() {
        let bytes = encode([(1, &b"unknown"[..]), (2, b"second")]);

        assert!(Reader::new(&bytes).required(2).is_ok());
    }

    #[test]
    fn refuses_an_unknown_even_field() {
        let bytes = encode([(0, &b"unknown"[..]), (2, b"second")]);

        assert_eq!(
            Reader::new(&bytes).required(2),
            Err(Error::UnexpectedType { ty: 0 }.into())
        );
    }

    #[test]
    fn refuses_an_absent_required_field() {
        let bytes = encode([(2, &b"second"[..])]);

        assert_eq!(
            Reader::new(&bytes).required(0),
            Err(RequiredError::Missing { ty: 0 })
        );
    }

    #[test]
    fn finishing_skips_trailing_odd_fields() {
        let bytes = encode([(0, &b"first"[..]), (5, b"unknown")]);
        let mut fields = Reader::new(&bytes);

        assert!(fields.required(0).is_ok());
        assert_eq!(fields.finish(), Ok(()));
    }

    #[test]
    fn finishing_refuses_a_trailing_even_field() {
        let bytes = encode([(0, &b"first"[..]), (4, b"unknown")]);
        let mut fields = Reader::new(&bytes);

        assert!(fields.required(0).is_ok());
        assert_eq!(fields.finish(), Err(Error::UnexpectedType { ty: 4 }));
    }

    /// A caller that ignores an error and goes on cannot read past it.
    #[test]
    fn every_call_after_an_error_fails() {
        let bytes = encode([(2, &b"second"[..]), (0, b"first"), (1, b"odd")]);
        let mut fields = Reader::new(&bytes);

        assert!(fields.optional(2).is_ok());
        assert_eq!(
            fields.optional(4),
            Err(Error::NotIncreasing { previous: 2, ty: 0 })
        );
        assert_eq!(fields.optional(5), Err(Error::PriorError));
        assert_eq!(fields.finish(), Err(Error::PriorError));
    }

    #[test]
    fn every_call_after_a_missing_field_fails() {
        let bytes = encode([(2, &b"second"[..])]);
        let mut fields = Reader::new(&bytes);

        assert_eq!(fields.required(0), Err(RequiredError::Missing { ty: 0 }));
        assert_eq!(fields.optional(2), Err(Error::PriorError));
        assert_eq!(fields.finish(), Err(Error::PriorError));
    }

    #[test]
    #[should_panic = "increasing order of type"]
    fn panics_on_a_request_out_of_order() {
        let mut fields = Reader::new(&[]);

        let _ = fields.optional(2);
        let _ = fields.optional(0);
    }

    #[test]
    #[should_panic = "increasing order of type"]
    fn panics_on_a_repeated_request() {
        let bytes = encode([(0, &b"first"[..])]);
        let mut fields = Reader::new(&bytes);

        let _ = fields.required(0);
        let _ = fields.required(0);
    }

    #[test]
    fn debug_shows_no_field_values() {
        let bytes = encode([(1, &b"secret"[..]), (4, b"secret")]);
        let mut fields = Reader::new(&bytes);

        assert_eq!(fields.optional(2), Ok(None));
        assert_eq!(
            format!("{fields:?}"),
            "Reader { requested: Some(2), failed: false, .. }"
        );
    }

    /// The public key in the BOLT #1 Appendix B vectors for `n1`'s `tlv3`.
    const NODE_ID: &str = "023da092f6980e58d2c037173180e9a465476026ee50f96695963e8efe436f54eb";

    /// BOLT #1 Appendix B, the TLV decoding successes for namespace `n1`'s
    /// fixed-length types: `tlv2` of 8 bytes, `tlv3` of 49 and `tlv4` of 2.
    #[test]
    fn reads_the_bolt_fixed_length_fields() {
        let tlv3 = format!("{NODE_ID} 0000000000000001 0000000000000002");

        assert_eq!(
            array::<8>(first(&hex("02 08 0000000000000226"))),
            Ok(0x226u64.to_be_bytes())
        );
        assert_eq!(
            array::<49>(first(&hex(&format!("03 31 {tlv3}")))),
            Ok(hex(&tlv3).try_into().unwrap())
        );
        assert_eq!(
            array::<2>(first(&hex("fd00fe 02 0226"))),
            Ok(550u16.to_be_bytes())
        );
    }

    /// Reads `encoded` as a stream whose only field is `ty`, of `N` bytes, and
    /// checks that its length `len` is refused.
    fn assert_wrong_length<const N: usize>(encoded: &str, ty: u64, len: usize) {
        assert_eq!(
            array::<N>(first(&hex(encoded))),
            Err(WrongLength {
                ty,
                expected: N,
                len,
            }),
            "{encoded}"
        );
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

    /// Scans a stream in which only the types in `known` are known.
    fn read_known(known: &[u64], bytes: &[u8]) -> Result<(), Error> {
        let mut fields = Reader::new(bytes);

        for &ty in known {
            fields.optional(ty)?;
        }

        fields.finish()
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
            ("12 00", Error::UnexpectedType { ty: 0x12 }),
            ("fd0102 00", Error::UnexpectedType { ty: 0x102 }),
            ("fe01000002 00", Error::UnexpectedType { ty: 0x0100_0002 }),
            (
                "ff0100000000000002 00",
                Error::UnexpectedType {
                    ty: 0x0100_0000_0000_0002,
                },
            ),
        ];

        for (encoded, error) in failures {
            assert_eq!(read_known(&[], &hex(encoded)), Err(error), "{encoded}");
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
            assert_eq!(read_known(&[], &hex(encoded)), Ok(()), "{encoded}");
        }
    }

    /// BOLT #1 Appendix B, the TLV stream decoding failures, read with the
    /// types of namespaces `n1` (1, 2, 3 and 254) and `n2` (0 and 11).
    #[test]
    fn refuses_the_bolt_stream_ordering_failures() {
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

        let outer = Reader::new(&bytes).required(2).unwrap();
        let value = Reader::new(outer.value).required(0).unwrap().value;

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
            let expected: Vec<_> = records.iter().map(|(ty, value)| (*ty, &value[..])).collect();

            prop_assert_eq!(fields(&bytes), Ok(expected));
        }

        /// Whatever the bytes, the fields come in strictly increasing order of
        /// type, and an error is the last item.
        #[test]
        fn ends_at_the_first_error_on_any_bytes(
            bytes in prop::collection::vec(any::<u8>(), 0..64),
        ) {
            let items: Vec<_> = read(&bytes).collect();
            let valid = items.iter().take_while(|item| item.is_ok()).count();

            prop_assert!(items.len() - valid <= 1);

            let types: Vec<_> = items.iter().flatten().map(|field| field.ty).collect();
            prop_assert!(types.windows(2).all(|pair| pair[0] < pair[1]));
        }
    }
}

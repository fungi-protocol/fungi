//! A message whose [`WireValue`](crate::WireValue) is the TLV stream of its
//! fields.

/// Defines a message: a struct, its constructor and getters, its message type
/// as `TLV_TYPE`, and its [`WireValue`](crate::WireValue) as a TLV stream of
/// its fields.
///
/// The message is written as `type => struct Name`, and each field once, as
/// `type => name: Type`, so encoding and decoding cannot disagree on its type.
/// The field types must increase in the order written, which a constant
/// assertion checks at compile time. Every field is required, and each getter
/// returns a copy, so every field type must be `Copy`.
macro_rules! tlv_struct {
    (
        $(#[$attr:meta])*
        $message_ty:literal => $vis:vis struct $name:ident {
            $(
                $(#[$field_attr:meta])*
                $ty:literal => $field:ident: $field_ty:ty,
            )+
        }
    ) => {
        $(#[$attr])*
        $vis struct $name {
            $($field: $field_ty,)+
        }

        const _: () = assert!(
            $crate::tlv_struct::increasing(&[$($ty),+]),
            "field types must increase",
        );

        impl $name {
            /// The message type.
            $vis const TLV_TYPE: u64 = $message_ty;

            /// Creates the value from its fields.
            $vis fn new($($field: $field_ty),+) -> Self {
                Self { $($field),+ }
            }

            $(
                $(#[$field_attr])*
                $vis fn $field(&self) -> $field_ty {
                    self.$field
                }
            )+
        }

        /// The fields as a TLV stream, in increasing order of type.
        impl $crate::WireValue for $name {
            fn encode_value(&self) -> Vec<u8> {
                let mut value = Vec::new();

                $(
                    ::fungi_transport::tlv::TypedSlice {
                        ty: $ty,
                        value: $crate::WireValue::encode_value(&self.$field),
                    }
                    .encode(&mut value)
                    .expect("writing to a Vec does not fail");
                )+

                value
            }

            fn decode(
                record: ::fungi_transport::tlv::TypedSlice<&[u8]>,
            ) -> Result<Self, $crate::Error> {
                let mut fields = $crate::fields::Reader::new(record.value);
                $(let $field = <$field_ty as $crate::WireValue>::decode(fields.required($ty)?)?;)+
                fields.finish()?;

                Ok(Self { $($field),+ })
            }
        }
    };
}

pub(crate) use tlv_struct;

/// Returns whether `types` is strictly increasing. A `const fn`, so
/// [`tlv_struct!`] can assert it at compile time.
pub(crate) const fn increasing(types: &[u64]) -> bool {
    let mut i = 1;

    while i < types.len() {
        if types[i] <= types[i - 1] {
            return false;
        }

        i += 1;
    }

    true
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::tlv::{Error::Truncated, TypedSlice};

    use super::increasing;
    use crate::{
        Amount, Error, UniqueId, WireValue, fields,
        testing::{encode, hex},
    };

    tlv_struct! {
        /// A message with two fields.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        6 => struct Pair {
            /// The ID.
            0 => id: UniqueId,
            /// The amount.
            2 => amount: Amount,
        }
    }

    fn pair() -> Pair {
        Pair::new(UniqueId::from([7; 16]), Amount::from_sat(1))
    }

    fn decode(bytes: &[u8]) -> Result<Pair, Error> {
        Pair::decode(TypedSlice {
            ty: Pair::TLV_TYPE,
            value: bytes,
        })
    }

    #[test]
    fn sets_the_message_type() {
        assert_eq!(Pair::TLV_TYPE, 6);
    }

    #[test]
    fn encodes_the_fields_in_order_of_type() {
        assert_eq!(
            pair().encode_value(),
            hex("00 10 07070707070707070707070707070707 02 08 0000000000000001")
        );
    }

    #[test]
    fn decodes_what_it_encodes() {
        let pair = pair();

        assert_eq!(decode(&pair.encode_value()), Ok(pair));
        assert_eq!(pair.id(), UniqueId::from([7; 16]));
        assert_eq!(pair.amount(), Amount::from_sat(1));
    }

    #[test]
    fn refuses_a_missing_field() {
        assert_eq!(
            decode(&encode([(0, &[7; 16][..])])),
            Err(Error::Missing { ty: 2 })
        );
    }

    #[test]
    fn refuses_a_truncated_field() {
        assert_eq!(
            decode(&hex("00 10")),
            Err(fields::Error::Tlv(Truncated).into())
        );
    }

    #[test]
    fn refuses_a_trailing_even_field() {
        let mut bytes = pair().encode_value();
        bytes.extend(encode([(4, &b"unknown"[..])]));

        assert_eq!(
            decode(&bytes),
            Err(fields::Error::UnexpectedType { ty: 4 }.into())
        );
    }

    #[test]
    fn checks_increasing_types() {
        assert!(increasing(&[]));
        assert!(increasing(&[5]));
        assert!(increasing(&[0, 1, 4]));
        assert!(!increasing(&[0, 0]));
        assert!(!increasing(&[2, 1]));
    }
}

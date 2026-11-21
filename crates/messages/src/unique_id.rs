//! A 16-byte unique ID.

use fungi_transport::tlv::TypedSlice;

use crate::{Error, WireValue, fields};

/// A 16-byte ID, unique among the elements it identifies. Encoded as its
/// bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UniqueId([u8; 16]);

impl From<[u8; 16]> for UniqueId {
    fn from(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }
}

impl From<UniqueId> for [u8; 16] {
    fn from(id: UniqueId) -> Self {
        id.0
    }
}

impl WireValue for UniqueId {
    fn encode_value(&self) -> Vec<u8> {
        self.0.to_vec()
    }

    fn decode(field: TypedSlice<&[u8]>) -> Result<Self, Error> {
        Ok(Self(fields::array(field)?))
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::tlv::TypedSlice;
    use proptest::prelude::*;

    use super::UniqueId;
    use crate::{WireValue, fields};

    #[test]
    fn converts_to_and_from_its_bytes() {
        assert_eq!(<[u8; 16]>::from(UniqueId::from([7; 16])), [7; 16]);
    }

    #[test]
    fn refuses_another_length() {
        for len in [15, 17] {
            assert_eq!(
                UniqueId::decode(TypedSlice {
                    ty: 0,
                    value: &[7; 17][..len]
                }),
                Err(fields::WrongLength {
                    ty: 0,
                    expected: 16,
                    len
                }
                .into())
            );
        }
    }

    proptest! {
        #[test]
        fn decodes_what_was_encoded(bytes in any::<[u8; 16]>()) {
            let id = UniqueId::from(bytes);
            let value = id.encode_value();

            prop_assert_eq!(UniqueId::decode(TypedSlice { ty: 0, value: &value[..] }), Ok(id));
        }
    }
}

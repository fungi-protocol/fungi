#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Fungi protocol messages and their encoding as Lightning BOLT #1 TLV records.
//!
//! Each message is one record. The record's type is the message type, and its
//! value is the message's fields, a TLV stream read with [`fields`]. Decoding
//! borrows from the record and returns an owned message.

mod fee_contribution;
pub mod fields;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod testing;

use fungi_transport::tlv::TypedSlice;

pub use fee_contribution::FeeContribution;

/// The largest amount in satoshis, 21 million BTC, the bound that BOLT #1
/// sets for amounts in its fundamental types.
pub const MAX_MONEY: u64 = 2_100_000_000_000_000;

/// Why a record could not be decoded as a message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The fields of the message could not be read.
    #[error(transparent)]
    Fields(#[from] fields::Error),
    /// A message of an unknown even type. BOLT #1 requires the reader to fail.
    #[error("unknown even message type {ty}")]
    UnknownEven {
        /// The message type.
        ty: u64,
    },
    /// An amount is larger than [`MAX_MONEY`].
    #[error("amount of {amount} satoshis exceeds {MAX_MONEY}")]
    AmountTooLarge {
        /// The amount, in satoshis.
        amount: u64,
    },
    /// The value is longer than the limit for its message type.
    #[error("message type {ty} of {len} bytes exceeds the limit of {max}")]
    TooLarge {
        /// The message type.
        ty: u64,
        /// The length of the value.
        len: usize,
        /// The limit for the message type.
        max: usize,
    },
}

/// A message, one per TLV record.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Message {
    /// An explicit contribution to the transaction fee.
    FeeContribution(FeeContribution),
}

impl Message {
    /// Decodes `record`, or returns `None` if its type is an unknown odd one,
    /// which BOLT #1 lets the reader ignore.
    ///
    /// Fails if the type is an unknown even one, or if the value is too long or
    /// its fields are not valid for the type.
    pub fn decode(record: TypedSlice<&[u8]>) -> Result<Option<Self>, Error> {
        match record.ty {
            FeeContribution::TYPE => FeeContribution::decode_value(record.value)
                .map(|contribution| Some(Self::FeeContribution(contribution))),
            ty if ty.is_multiple_of(2) => Err(Error::UnknownEven { ty }),
            _ => Ok(None),
        }
    }
}

/// The message as the one TLV record a sender writes.
/// [`TypedSlice::encode`] writes its bytes.
impl From<Message> for TypedSlice<Vec<u8>> {
    fn from(message: Message) -> Self {
        match message {
            Message::FeeContribution(contribution) => TypedSlice {
                ty: FeeContribution::TYPE,
                value: contribution.encode_value(),
            },
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::tlv::TypedSlice;

    use super::{Error, FeeContribution, Message};
    use crate::testing::hex;

    /// Converts `message` to its record and decodes the record back.
    fn round_trip(message: Message) -> Option<Message> {
        let record = TypedSlice::from(message);

        Message::decode(TypedSlice {
            ty: record.ty,
            value: &record.value[..],
        })
        .unwrap()
    }

    #[test]
    fn decodes_an_encoded_fee_contribution() {
        let message = Message::FeeContribution(FeeContribution::new([7; 16], 1_000).unwrap());

        assert_eq!(round_trip(message.clone()), Some(message));
    }

    /// The type, the length, then the ID and amount fields, each a type, a
    /// length and the value.
    #[test]
    fn encodes_a_fee_contribution_as_its_wire_bytes() {
        let message = Message::FeeContribution(FeeContribution::new([7; 16], 1_000).unwrap());
        let mut bytes = Vec::new();
        TypedSlice::from(message).encode(&mut bytes).unwrap();

        assert_eq!(
            bytes,
            hex("04 1c 00 10 07070707070707070707070707070707 02 08 00000000000003e8")
        );
    }

    #[test]
    fn ignores_an_unknown_odd_message() {
        let record = TypedSlice {
            ty: 1,
            value: &b"unknown"[..],
        };

        assert_eq!(Message::decode(record), Ok(None));
    }

    #[test]
    fn refuses_an_unknown_even_message() {
        let record = TypedSlice {
            ty: 6,
            value: &b"unknown"[..],
        };

        assert_eq!(Message::decode(record), Err(Error::UnknownEven { ty: 6 }));
    }
}

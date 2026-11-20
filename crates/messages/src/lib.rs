#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Fungi protocol messages and their encoding as Lightning BOLT #1 TLV records.
//!
//! Each message is one record. The record's type is the message type, and its
//! value is the message's fields, a TLV stream read with [`fields`]. Decoding
//! borrows from the record and returns an owned message.

mod amount;
mod fee_contribution;
pub mod fields;
mod non_zero_amount;
mod tlv_struct;
mod unique_id;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod testing;

use fungi_transport::tlv::TypedSlice;

pub use amount::Amount;
pub use fee_contribution::FeeContribution;
pub use non_zero_amount::NonZeroAmount;
pub use unique_id::UniqueId;

/// Why a value could not be decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The fields of the value could not be read.
    #[error(transparent)]
    Fields(#[from] fields::Error),
    /// A required field is absent.
    #[error("missing field type {ty}")]
    Missing {
        /// The type of the field.
        ty: u64,
    },
    /// A fixed-size value has another length.
    #[error(transparent)]
    WrongLength(#[from] fields::WrongLength),
    /// A message of an unknown even type. BOLT #1 requires the reader to fail.
    #[error("unexpected message type {ty}")]
    UnexpectedType {
        /// The message type.
        ty: u64,
    },
    /// An amount is larger than [`bitcoin_units::Amount::MAX_MONEY`].
    #[error(
        "amount of {amount} satoshis exceeds {}",
        bitcoin_units::Amount::MAX_MONEY.to_sat()
    )]
    AmountTooLarge {
        /// The amount, in satoshis.
        amount: u64,
    },
    /// An amount is zero where at least 1 sat is required.
    #[error("amount is zero")]
    ZeroAmount,
}

impl From<fields::RequiredError> for Error {
    fn from(error: fields::RequiredError) -> Self {
        match error {
            fields::RequiredError::Read(error) => Self::Fields(error),
            fields::RequiredError::Missing { ty } => Self::Missing { ty },
        }
    }
}

/// A value that is the payload of one TLV record. The trait carries no record
/// type, because a field's type belongs to whatever holds it, so a value that
/// holds other values encodes and decodes each of them through this trait.
pub trait WireValue: Sized {
    /// Encodes the value as the payload of a record.
    fn encode_value(&self) -> Vec<u8>;

    /// Decodes the value from `field`, the record that holds it.
    fn decode(field: TypedSlice<&[u8]>) -> Result<Self, Error>;
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
    /// Fails if the type is an unknown even one, or if the fields of the value
    /// are not valid for the type.
    pub fn decode(record: TypedSlice<&[u8]>) -> Result<Option<Self>, Error> {
        match record.ty {
            FeeContribution::TLV_TYPE => FeeContribution::decode(record)
                .map(|contribution| Some(Self::FeeContribution(contribution))),
            ty if ty.is_multiple_of(2) => Err(Error::UnexpectedType { ty }),
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
                ty: FeeContribution::TLV_TYPE,
                value: contribution.encode_value(),
            },
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::tlv::TypedSlice;

    use super::{Error, Message};
    use crate::testing::{contribution, hex};

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
        let message = Message::FeeContribution(contribution(1_000));

        assert_eq!(round_trip(message.clone()), Some(message));
    }

    /// The type, the length, then the ID and amount fields, each a type, a
    /// length and the value.
    #[test]
    fn encodes_a_fee_contribution_as_its_wire_bytes() {
        let message = Message::FeeContribution(contribution(1_000));
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

        assert_eq!(
            Message::decode(record),
            Err(Error::UnexpectedType { ty: 6 })
        );
    }
}

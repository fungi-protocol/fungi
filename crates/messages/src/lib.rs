#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Fungi protocol messages and their encoding as Lightning BOLT #1 TLV records.

mod amount;
pub mod fields;
#[expect(unused, reason = "no message is defined outside the tests yet")]
mod tlv_struct;
mod unique_id;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod testing;

use fungi_transport::tlv::TypedSlice;

pub use amount::Amount;
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
    /// An amount is larger than [`bitcoin_units::Amount::MAX_MONEY`].
    #[error(
        "amount of {amount} satoshis exceeds {}",
        bitcoin_units::Amount::MAX_MONEY.to_sat()
    )]
    AmountTooLarge {
        /// The amount, in satoshis.
        amount: u64,
    },
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

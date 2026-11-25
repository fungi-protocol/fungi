//! An amount of at least 1 sat.

use fungi_transport::tlv::TypedSlice;

use crate::{Amount, Error, WireValue};

/// An [`Amount`] of at least 1 sat. Encoded as an [`Amount`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NonZeroAmount(Amount);

impl NonZeroAmount {
    /// Returns the amount.
    pub fn get(self) -> Amount {
        self.0
    }
}

/// Fails with [`Error::ZeroAmount`] if `amount` is zero.
impl TryFrom<Amount> for NonZeroAmount {
    type Error = Error;

    fn try_from(amount: Amount) -> Result<Self, Error> {
        if amount.to_sat() == 0 {
            return Err(Error::ZeroAmount);
        }

        Ok(Self(amount))
    }
}

impl From<NonZeroAmount> for Amount {
    fn from(amount: NonZeroAmount) -> Self {
        amount.0
    }
}

impl WireValue for NonZeroAmount {
    fn encode_value(&self) -> Vec<u8> {
        self.0.encode_value()
    }

    fn decode(field: TypedSlice<&[u8]>) -> Result<Self, Error> {
        Amount::decode(field)?.try_into()
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::tlv::TypedSlice;
    use proptest::prelude::*;

    use super::NonZeroAmount;
    use crate::{Amount, Error, WireValue};

    /// Decodes `satoshis` as the value of an amount field.
    fn decode(satoshis: u64) -> Result<NonZeroAmount, Error> {
        NonZeroAmount::decode(TypedSlice {
            ty: 2,
            value: &satoshis.to_be_bytes(),
        })
    }

    fn amount(satoshis: u64) -> Amount {
        Amount::from_sat(satoshis)
    }

    #[test]
    fn refuses_zero() {
        assert_eq!(NonZeroAmount::try_from(amount(0)), Err(Error::ZeroAmount));
        assert_eq!(decode(0), Err(Error::ZeroAmount));
    }

    #[test]
    fn accepts_1_sat() {
        let one = NonZeroAmount::try_from(amount(1)).unwrap();

        assert_eq!(one.get(), amount(1));
        assert_eq!(Amount::from(one), amount(1));
        assert_eq!(decode(1), Ok(one));
    }

    /// The amount is decoded by [`Amount`], which bounds it.
    #[test]
    fn refuses_more_than_21_million_btc() {
        let satoshis = bitcoin_units::Amount::MAX_MONEY.to_sat() + 1;

        assert_eq!(
            decode(satoshis),
            Err(Error::AmountTooLarge { amount: satoshis })
        );
    }

    proptest! {
        #[test]
        fn decodes_what_was_encoded(satoshis in 1..=bitcoin_units::Amount::MAX_MONEY.to_sat()) {
            let amount = NonZeroAmount::try_from(amount(satoshis)).unwrap();
            let value = amount.encode_value();

            prop_assert_eq!(
                NonZeroAmount::decode(TypedSlice { ty: 2, value: &value[..] }),
                Ok(amount)
            );
        }
    }
}

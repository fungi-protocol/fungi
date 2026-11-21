//! Bitcoin amount validation and serialization.

use fungi_transport::tlv::TypedSlice;

use crate::{Error, WireValue, fields};

/// An amount of bitcoin, encoded as a big-endian `u64` of satoshis.
///
/// BOLT #1 bounds encoded amounts to [`bitcoin_units::Amount::MAX_MONEY`].
/// Decoding refuses a larger amount as invalid input. Encoding one panics,
/// because only a bug in the sender can produce it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Amount(bitcoin_units::Amount);

impl Amount {
    /// Creates an amount of `satoshis`.
    pub fn from_sat(satoshis: u64) -> Self {
        Self(bitcoin_units::Amount::from_sat(satoshis))
    }

    /// Returns the amount in satoshis.
    pub fn to_sat(self) -> u64 {
        self.0.to_sat()
    }
}

impl From<bitcoin_units::Amount> for Amount {
    fn from(amount: bitcoin_units::Amount) -> Self {
        Self(amount)
    }
}

impl From<Amount> for bitcoin_units::Amount {
    fn from(amount: Amount) -> Self {
        amount.0
    }
}

impl WireValue for Amount {
    fn encode_value(&self) -> Vec<u8> {
        assert!(
            self.0 <= bitcoin_units::Amount::MAX_MONEY,
            "an encoded amount must be at most 21 million BTC",
        );

        self.to_sat().to_be_bytes().to_vec()
    }

    fn decode(field: TypedSlice<&[u8]>) -> Result<Self, Error> {
        let amount = Self::from_sat(u64::from_be_bytes(fields::array(field)?));
        if amount.0 > bitcoin_units::Amount::MAX_MONEY {
            return Err(Error::AmountTooLarge {
                amount: amount.to_sat(),
            });
        }

        Ok(amount)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::tlv::TypedSlice;
    use proptest::prelude::*;

    use super::Amount;
    use crate::{Error, WireValue, fields};

    /// Decodes `bytes` as the value of an amount field.
    fn decode(bytes: &[u8]) -> Result<Amount, Error> {
        Amount::decode(TypedSlice {
            ty: 2,
            value: bytes,
        })
    }

    /// 21 million BTC, in satoshis.
    const MAX: u64 = 2_100_000_000_000_000;

    #[test]
    fn decodes_21_million_btc() {
        assert_eq!(decode(&MAX.to_be_bytes()), Ok(Amount::from_sat(MAX)));
    }

    #[test]
    fn decoding_refuses_more_than_21_million_btc() {
        assert_eq!(
            decode(&(MAX + 1).to_be_bytes()),
            Err(Error::AmountTooLarge { amount: MAX + 1 })
        );
    }

    #[test]
    #[should_panic = "at most 21 million BTC"]
    fn encoding_more_than_21_million_btc_panics() {
        Amount::from_sat(MAX + 1).encode_value();
    }

    #[test]
    fn converts_to_and_from_a_bitcoin_amount() {
        let bitcoin = bitcoin_units::Amount::from_sat(1_000);

        assert_eq!(Amount::from(bitcoin), Amount::from_sat(1_000));
        assert_eq!(bitcoin_units::Amount::from(Amount::from(bitcoin)), bitcoin);
    }

    #[test]
    fn refuses_another_length() {
        for len in [7, 9] {
            assert_eq!(
                decode(&[0; 9][..len]),
                Err(fields::WrongLength {
                    ty: 2,
                    expected: 8,
                    len
                }
                .into())
            );
        }
    }

    proptest! {
        #[test]
        fn decodes_what_was_encoded(satoshis in 0..=MAX) {
            let amount = Amount::from_sat(satoshis);
            let value = amount.encode_value();

            prop_assert_eq!(decode(&value), Ok(amount));
        }
    }
}

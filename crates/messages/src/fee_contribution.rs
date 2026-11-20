//! An explicit fee contribution: a unique ID and an amount.

use crate::{NonZeroAmount, UniqueId, tlv_struct::tlv_struct};

tlv_struct! {
    /// An explicit contribution to the transaction fee. It projects into a
    /// `PSBT_GLOBAL_EXPLICIT_FEE_CONTRIBUTION` entry, keyed by its unique ID.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    4 => pub struct FeeContribution {
        /// The unique ID.
        0 => id: UniqueId,
        /// The amount. At least 1 sat, so the input balance bounds the number
        /// of contributions.
        2 => amount: NonZeroAmount,
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use fungi_transport::tlv::TypedSlice;
    use proptest::prelude::*;

    use super::FeeContribution;
    use crate::{
        Amount, Error, UniqueId, WireValue, fields,
        testing::{contribution, encode},
    };

    /// The field types, written again so that a change to them fails a test.
    const ID: u64 = 0;
    const AMOUNT: u64 = 2;

    const AMOUNT_BYTES: [u8; 8] = 1_000u64.to_be_bytes();

    /// Decodes the fields `records` as a fee contribution.
    fn decode<'a>(
        records: impl IntoIterator<Item = (u64, &'a [u8])>,
    ) -> Result<FeeContribution, Error> {
        FeeContribution::decode(TypedSlice {
            ty: FeeContribution::TLV_TYPE,
            value: &encode(records),
        })
    }

    #[test]
    fn skips_an_unknown_odd_field() {
        assert_eq!(
            decode([
                (ID, &[7; 16][..]),
                (AMOUNT, &AMOUNT_BYTES[..]),
                (5, b"unknown")
            ]),
            Ok(contribution(1_000))
        );
    }

    /// The ID is decoded by [`UniqueId`], which reports the field's type.
    #[test]
    fn refuses_an_id_of_another_length() {
        assert_eq!(
            decode([(ID, &[7; 15][..]), (AMOUNT, &AMOUNT_BYTES[..])]),
            Err(fields::WrongLength {
                ty: ID,
                expected: 16,
                len: 15
            }
            .into())
        );
    }

    /// The amount is decoded by [`Amount`], which bounds it.
    #[test]
    fn refuses_an_amount_over_21_million_btc() {
        let amount = bitcoin_units::Amount::MAX_MONEY.to_sat() + 1;

        assert_eq!(
            decode([(ID, &[7; 16][..]), (AMOUNT, &amount.to_be_bytes()[..])]),
            Err(Error::AmountTooLarge { amount })
        );
    }

    #[test]
    fn refuses_a_zero_amount() {
        assert_eq!(
            decode([(ID, &[7; 16][..]), (AMOUNT, &0u64.to_be_bytes()[..])]),
            Err(Error::ZeroAmount)
        );
    }

    #[test]
    fn refuses_a_trailing_unknown_even_field() {
        assert_eq!(
            decode([
                (ID, &[7; 16][..]),
                (AMOUNT, &AMOUNT_BYTES[..]),
                (4, b"unknown")
            ]),
            Err(fields::Error::UnexpectedType { ty: 4 }.into())
        );
    }

    #[test]
    fn returns_the_id_and_amount() {
        let contribution = contribution(1_000);

        assert_eq!(contribution.id(), UniqueId::from([7; 16]));
        assert_eq!(contribution.amount().get(), Amount::from_sat(1_000));
    }

    #[test]
    fn refuses_a_missing_id() {
        assert_eq!(
            decode([(AMOUNT, &AMOUNT_BYTES[..])]),
            Err(Error::Missing { ty: ID })
        );
    }

    #[test]
    fn refuses_a_missing_amount() {
        assert_eq!(
            decode([(ID, &[7; 16][..])]),
            Err(Error::Missing { ty: AMOUNT })
        );
    }

    /// The ID is looked for first, so the amount before it leaves it missing.
    #[test]
    fn refuses_an_amount_before_the_id() {
        assert_eq!(
            decode([(AMOUNT, &AMOUNT_BYTES[..]), (ID, &[7; 16][..])]),
            Err(Error::Missing { ty: ID })
        );
    }

    proptest! {
        #[test]
        fn decodes_what_was_encoded(
            id in any::<[u8; 16]>(),
            satoshis in 1..=bitcoin_units::Amount::MAX_MONEY.to_sat(),
        ) {
            let contribution =
                FeeContribution::new(
                    UniqueId::from(id),
                    Amount::from_sat(satoshis).try_into().unwrap(),
                );
            let value = contribution.encode_value();

            prop_assert_eq!(
                FeeContribution::decode(TypedSlice {
                    ty: FeeContribution::TLV_TYPE,
                    value: &value[..],
                }),
                Ok(contribution)
            );
        }
    }
}

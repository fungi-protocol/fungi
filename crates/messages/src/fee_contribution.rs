//! An explicit fee contribution: a unique ID and an amount.

use fungi_transport::tlv::{self, TypedSlice};

use crate::{Error, MAX_MONEY, fields};

/// The type of the unique ID field.
const ID: u64 = 0;

/// The type of the amount field, a big-endian `u64`.
const AMOUNT: u64 = 2;

/// An explicit contribution to the transaction fee. It projects into a
/// `PSBT_GLOBAL_EXPLICIT_FEE_CONTRIBUTION` entry, keyed by its unique ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeContribution {
    id: [u8; 16],
    amount: u64,
}

impl FeeContribution {
    /// The message type.
    pub const TYPE: u64 = 4;

    /// The longest value accepted. The known fields take 28 bytes, and the
    /// rest is room for unknown odd fields.
    pub const MAX_LEN: usize = 64;

    /// Creates a contribution of `amount` satoshis, identified by `id`.
    ///
    /// Fails with [`Error::AmountTooLarge`] if `amount` is larger than
    /// [`MAX_MONEY`].
    pub fn new(id: [u8; 16], amount: u64) -> Result<Self, Error> {
        if amount > MAX_MONEY {
            return Err(Error::AmountTooLarge { amount });
        }

        Ok(Self { id, amount })
    }

    /// The unique ID.
    pub fn id(&self) -> [u8; 16] {
        self.id
    }

    /// The amount, in satoshis.
    pub fn amount(&self) -> u64 {
        self.amount
    }

    /// Encodes the fields as a TLV stream.
    pub(crate) fn encode_value(&self) -> Vec<u8> {
        let amount = self.amount.to_be_bytes();
        let records = [
            TypedSlice {
                ty: ID,
                value: &self.id[..],
            },
            TypedSlice {
                ty: AMOUNT,
                value: &amount[..],
            },
        ];

        let mut value = Vec::new();
        tlv::encode(records, &mut value).expect("writing to a Vec does not fail");

        value
    }

    /// Decodes the fields from `value`.
    pub(crate) fn decode_value(value: &[u8]) -> Result<Self, Error> {
        if value.len() > Self::MAX_LEN {
            return Err(Error::TooLarge {
                ty: Self::TYPE,
                len: value.len(),
                max: Self::MAX_LEN,
            });
        }

        let mut fields = fields::read(value);

        let id = fields.required_array(ID)?;
        let amount = u64::from_be_bytes(fields.required_array(AMOUNT)?);
        fields.finish()?;

        Self::new(id, amount)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use proptest::prelude::*;

    use super::{AMOUNT, FeeContribution, ID};
    use crate::{Error, MAX_MONEY, fields, testing::encode};

    const AMOUNT_BYTES: [u8; 8] = 1_000u64.to_be_bytes();

    /// The length of the known fields: two 2-byte headers, the ID and the
    /// amount.
    const KNOWN_LEN: usize = 28;

    const _: () = assert!(KNOWN_LEN < FeeContribution::MAX_LEN);

    /// Decodes the fields `records` as a fee contribution.
    fn decode<'a>(
        records: impl IntoIterator<Item = (u64, &'a [u8])>,
    ) -> Result<FeeContribution, Error> {
        FeeContribution::decode_value(&encode(records))
    }

    /// Decodes a fee contribution filled with an odd field to `len` bytes. The
    /// odd field's header takes 2 bytes.
    fn decode_padded(len: usize) -> Result<FeeContribution, Error> {
        let padding = vec![0; len - KNOWN_LEN - 2];

        decode([
            (ID, &[7; 16][..]),
            (AMOUNT, &AMOUNT_BYTES[..]),
            (5, &padding[..]),
        ])
    }

    #[test]
    fn accepts_a_value_at_the_limit() {
        assert_eq!(
            decode_padded(FeeContribution::MAX_LEN),
            FeeContribution::new([7; 16], 1_000)
        );
    }

    #[test]
    fn refuses_a_value_over_the_limit() {
        assert_eq!(
            decode_padded(FeeContribution::MAX_LEN + 1),
            Err(Error::TooLarge {
                ty: FeeContribution::TYPE,
                len: FeeContribution::MAX_LEN + 1,
                max: FeeContribution::MAX_LEN,
            })
        );
    }

    #[test]
    fn refuses_an_id_of_another_length() {
        for len in [15, 17] {
            assert_eq!(
                decode([(ID, &[7; 17][..len]), (AMOUNT, &AMOUNT_BYTES[..])]),
                Err(fields::Error::WrongLength {
                    ty: ID,
                    expected: 16,
                    len
                }
                .into())
            );
        }
    }

    #[test]
    fn refuses_an_amount_of_another_length() {
        for len in [7, 9] {
            assert_eq!(
                decode([(ID, &[7; 16][..]), (AMOUNT, &[0; 9][..len])]),
                Err(fields::Error::WrongLength {
                    ty: AMOUNT,
                    expected: 8,
                    len
                }
                .into())
            );
        }
    }

    #[test]
    fn refuses_a_trailing_unknown_even_field() {
        assert_eq!(
            decode([
                (ID, &[7; 16][..]),
                (AMOUNT, &AMOUNT_BYTES[..]),
                (4, b"unknown")
            ]),
            Err(fields::Error::UnknownEven { ty: 4 }.into())
        );
    }

    #[test]
    fn refuses_an_amount_over_21_million_btc() {
        let amount = MAX_MONEY + 1;

        assert_eq!(
            decode([(ID, &[7; 16][..]), (AMOUNT, &amount.to_be_bytes()[..])]),
            Err(Error::AmountTooLarge { amount })
        );
    }

    #[test]
    fn accepts_an_amount_of_21_million_btc() {
        assert_eq!(
            decode([(ID, &[7; 16][..]), (AMOUNT, &MAX_MONEY.to_be_bytes()[..])]),
            FeeContribution::new([7; 16], MAX_MONEY)
        );
    }

    #[test]
    fn cannot_be_made_with_an_amount_over_21_million_btc() {
        let amount = MAX_MONEY + 1;

        assert_eq!(
            FeeContribution::new([7; 16], amount),
            Err(Error::AmountTooLarge { amount })
        );
    }

    #[test]
    fn returns_the_id_and_amount() {
        let contribution = FeeContribution::new([7; 16], 1_000).unwrap();

        assert_eq!(contribution.id(), [7; 16]);
        assert_eq!(contribution.amount(), 1_000);
    }

    #[test]
    fn refuses_a_missing_amount() {
        assert_eq!(
            decode([(ID, &[7; 16][..])]),
            Err(fields::Error::Missing { ty: AMOUNT }.into())
        );
    }

    /// Every encoded contribution has the same length, which the constant
    /// assertion above keeps under the limit, with room for unknown odd fields.
    #[test]
    fn encodes_the_known_fields_within_the_limit() {
        let value = FeeContribution::new([7; 16], MAX_MONEY)
            .unwrap()
            .encode_value();

        assert_eq!(value.len(), KNOWN_LEN);
    }

    /// The ID is requested first, so an amount before it reads as a missing ID
    /// rather than as fields out of order.
    #[test]
    fn refuses_an_amount_before_the_id() {
        assert_eq!(
            decode([(AMOUNT, &AMOUNT_BYTES[..]), (ID, &[7; 16][..])]),
            Err(fields::Error::Missing { ty: ID }.into())
        );
    }

    proptest! {
        #[test]
        fn decodes_what_was_encoded(id in any::<[u8; 16]>(), amount in 0..=MAX_MONEY) {
            let contribution = FeeContribution::new(id, amount).unwrap();

            prop_assert_eq!(
                FeeContribution::decode_value(&contribution.encode_value()),
                Ok(contribution)
            );
        }
    }
}

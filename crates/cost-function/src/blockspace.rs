//! The weight a coin or output adds to a transaction, and the fee for it.

use bitcoin::{Amount, FeeRate, Weight};

/// Anything that knows the weight it will take up in a transaction, such as a coin spent as
/// an input or an output created.
pub trait KnowsWeight {
    /// Weight it adds to the transaction.
    fn weight(&self) -> Weight;

    /// Fee for [`KnowsWeight::weight`] at `feerate`, rounded up. `None` if it overflows.
    fn fee(&self, feerate: FeeRate) -> Option<Amount> {
        feerate.fee_wu(self.weight())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(Weight);

    impl KnowsWeight for Fixed {
        fn weight(&self) -> Weight {
            self.0
        }
    }

    #[test]
    fn fee_is_weight_times_feerate() {
        let feerate = FeeRate::from_sat_per_vb(10).unwrap();

        assert_eq!(
            Fixed(Weight::from_vb(43).unwrap()).fee(feerate),
            Some(Amount::from_sat(430))
        );
    }

    #[test]
    fn fee_is_none_on_overflow() {
        assert_eq!(Fixed(Weight::MAX).fee(FeeRate::MAX), None);
    }
}

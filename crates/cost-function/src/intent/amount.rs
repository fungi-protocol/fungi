//! Interpolated utility, before quantization to a concrete Bitcoin amount.

use bitcoin::Amount;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct FractionalAmount(f64);

impl FractionalAmount {
    pub fn as_sat(self) -> f64 {
        self.0
    }

    /// Quantize only when a concrete amount is needed, not for curve comparison.
    pub fn round(self) -> Option<Amount> {
        let sats = self.0.round();
        (0.0..u64::MAX as f64)
            .contains(&sats)
            .then(|| Amount::from_sat(sats as u64))
    }
}

impl From<Amount> for FractionalAmount {
    fn from(value: Amount) -> Self {
        Self(value.to_sat() as f64)
    }
}

impl FractionalAmount {
    pub(super) fn interpolate(start: Amount, end: Amount, fraction: f64) -> Self {
        // Subtract integer endpoints before conversion, preserving small slopes.
        let delta = i128::from(end.to_sat()) - i128::from(start.to_sat());
        Self(start.to_sat() as f64 + delta as f64 * fraction)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounded_payoff_must_fit_an_unsigned_amount() {
        // Amount::MAX rounds to 2^64 in binary64, just above 2^64 - 2048.
        let upper = Amount::MAX.to_sat() as f64;
        assert_eq!(
            FractionalAmount::from(Amount::ZERO).round(),
            Some(Amount::ZERO)
        );
        assert_eq!(FractionalAmount(-0.25).round(), Some(Amount::ZERO));
        assert_eq!(FractionalAmount(-0.5).round(), None);
        assert_eq!(
            FractionalAmount(upper.next_down()).round(),
            Some(Amount::from_sat(u64::MAX - 2_047))
        );
        assert_eq!(FractionalAmount::from(Amount::MAX).round(), None);
    }
}

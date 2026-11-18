//! Outward-rounded binary64 enclosures of the combined payoff expression.

/// An inclusive enclosure of a candidate's net cost in satoshis.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct CostBounds {
    low: f64,
    high: f64,
}

/// What an enclosure alone establishes about the sign of the net cost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CostSign {
    Negative,
    Zero,
    Positive,
    /// The bounds include zero and at least one nonzero value.
    Overlap,
}

impl CostBounds {
    pub(super) const ZERO: Self = Self {
        low: 0.0,
        high: 0.0,
    };

    pub fn bounds(self) -> (f64, f64) {
        (self.low, self.high)
    }

    pub fn sign(self) -> CostSign {
        if self.high < 0.0 {
            CostSign::Negative
        } else if self.low > 0.0 {
            CostSign::Positive
        } else if self == Self::ZERO {
            CostSign::Zero
        } else {
            CostSign::Overlap
        }
    }

    pub(super) fn integer(value: i128) -> Self {
        let rounded = value as f64;
        if value.unsigned_abs() <= 1 << 53 {
            Self {
                low: rounded,
                high: rounded,
            }
        } else {
            Self::round_out(rounded, rounded)
        }
    }

    pub(super) fn ratio(rise: i128, run: i128) -> Self {
        let numerator = Self::integer(rise);
        let denominator = Self::integer(run);
        if rise.is_negative() {
            Self::round_out(
                numerator.low / denominator.low,
                numerator.high / denominator.high,
            )
        } else {
            Self::round_out(
                numerator.low / denominator.high,
                numerator.high / denominator.low,
            )
        }
    }

    pub(super) fn add(self, other: Self) -> Self {
        if self == Self::ZERO {
            other
        } else if other == Self::ZERO {
            self
        } else {
            Self::round_out(self.low + other.low, self.high + other.high)
        }
    }

    pub(super) fn scale(self, nanos: i128) -> Self {
        if nanos == 0 || self == Self::ZERO {
            return Self::ZERO;
        }
        let factor = Self::integer(nanos);
        let low_factor = if self.low.is_sign_negative() {
            factor.high
        } else {
            factor.low
        };
        let high_factor = if self.high.is_sign_negative() {
            factor.low
        } else {
            factor.high
        };
        Self::round_out(self.low * low_factor, self.high * high_factor)
    }

    fn round_out(low: f64, high: f64) -> Self {
        Self {
            low: low.next_down(),
            high: high.next_up(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigInt;
    use num_rational::BigRational;

    fn assert_encloses(bounds: CostBounds, exact: &BigRational) {
        let (low, high) = bounds.bounds();
        assert!(&BigRational::from_float(low).unwrap() <= exact);
        assert!(&BigRational::from_float(high).unwrap() >= exact);
    }

    #[test]
    fn exactly_representable_small_integers_keep_point_enclosures() {
        let limit = 1_i128 << f64::MANTISSA_DIGITS;
        for value in [-limit, -1, 0, 1, limit] {
            let expected = value as f64;
            assert_eq!(CostBounds::integer(value).bounds(), (expected, expected));
        }
    }

    #[test]
    fn bounds_certify_the_order_of_separated_ratios() {
        let difference = CostBounds::ratio(1, 8).add(CostBounds::ratio(-1, 4));
        assert_eq!(difference.sign(), CostSign::Negative);
    }

    #[test]
    fn scaling_by_zero_or_scaling_zero_keeps_the_exact_zero() {
        assert_eq!(CostBounds::ratio(1, 8).scale(0), CostBounds::ZERO);
        assert_eq!(CostBounds::ZERO.scale(37), CostBounds::ZERO);
    }

    #[test]
    fn outward_rounding_contains_signed_division_and_scaling() {
        let elapsed = (1_i128 << 53) + 1;
        // A reduced run from a 3-sat rise over 360287970189 authored slots.
        let run = 12_009_599_006_300_000;
        for sign in [-1, 1] {
            assert_encloses(
                CostBounds::integer(sign * elapsed),
                &BigRational::from_integer(BigInt::from(sign * elapsed)),
            );
            for denominator in [10, run] {
                assert_encloses(
                    CostBounds::ratio(sign, denominator),
                    &BigRational::new(BigInt::from(sign), BigInt::from(denominator)),
                );
            }
            assert_encloses(
                CostBounds::integer(3 * sign).scale(elapsed),
                &BigRational::from_integer(BigInt::from(3 * sign * elapsed)),
            );
        }
    }

    #[test]
    fn signed_bounds_enclose_the_exact_sum() {
        let terms = [(i128::MAX, 3, 7), (-i128::MAX, 3, 7), (1, 2, 1), (-2, 4, 1)];
        let mut expected = BigRational::from_integer(BigInt::from(0));
        let mut bounds = CostBounds::ZERO;
        for (rise, run, elapsed) in terms {
            expected +=
                BigRational::new(BigInt::from(rise), BigInt::from(run)) * BigInt::from(elapsed);
            bounds = bounds.add(CostBounds::ratio(rise, run).scale(elapsed));
            assert_encloses(bounds, &expected);
        }
        assert_eq!(bounds.sign(), CostSign::Overlap);
        assert_eq!(CostBounds::ZERO.sign(), CostSign::Zero);
    }
}

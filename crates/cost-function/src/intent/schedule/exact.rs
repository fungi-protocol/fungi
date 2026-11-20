//! Exact signed ratios with native storage until checked arithmetic needs more width.

use num_bigint::Sign;
use num_rational::{BigRational, Ratio};
use num_traits::{CheckedAdd, CheckedMul, ToPrimitive, Zero};

#[derive(Clone)]
pub(super) enum Exact {
    Native(Ratio<i128>),
    Wide(BigRational),
}

impl Exact {
    pub(super) fn integer(value: i128) -> Self {
        Self::Native(Ratio::from_integer(value))
    }

    pub(super) fn ratio(rise: i128, run: i128) -> Self {
        Self::Native(Ratio::new(rise, run))
    }

    pub(super) fn is_zero(&self) -> bool {
        // Wide results narrow whenever their reduced coordinates fit.
        matches!(self, Self::Native(value) if value.is_zero())
    }

    pub(super) fn is_negative(&self) -> bool {
        match self {
            Self::Native(value) => *value.numer() < 0,
            Self::Wide(value) => value.numer().sign() == Sign::Minus,
        }
    }

    pub(super) fn add(&self, other: &Self) -> Self {
        if let (Self::Native(a), Self::Native(b)) = (self, other)
            && let Some(sum) = a.checked_add(b)
        {
            return Self::Native(sum);
        }
        Self::narrow(self.wide() + other.wide())
    }

    pub(super) fn scale(&self, factor: i128) -> Self {
        if let Self::Native(value) = self
            && let Some(product) = value.checked_mul(&Ratio::from_integer(factor))
        {
            return Self::Native(product);
        }
        Self::narrow(self.wide() * BigRational::from_integer(factor.into()))
    }

    fn wide(&self) -> BigRational {
        match self {
            Self::Native(value) => {
                BigRational::new_raw((*value.numer()).into(), (*value.denom()).into())
            }
            Self::Wide(value) => value.clone(),
        }
    }

    fn narrow(value: BigRational) -> Self {
        match (value.numer().to_i128(), value.denom().to_i128()) {
            (Some(num), Some(den)) => Self::Native(Ratio::new_raw(num, den)),
            _ => Self::Wide(value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn widening_preserves_the_signed_limit_and_recovers_native_arithmetic() {
        let steep = Exact::integer(i128::MIN);
        let magnitude = steep.scale(-1);
        assert!(matches!(magnitude, Exact::Wide(_)));
        let zero = magnitude.add(&steep);
        assert!(matches!(zero, Exact::Native(_)));
        assert!(zero.is_zero());
    }
}

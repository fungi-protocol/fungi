//! Defines what the user wants, and how much they care.

use std::time::{Duration, Instant};

use bitcoin::{Amount, ScriptBuf};

/// The required information for an output creation intent.
///
/// Corresponds to just the on chain variant of `bitcoin-payment-instructions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixedPaymentInstructions {
    /// Output script the payment must pay to.
    pub script_pubkey: ScriptBuf,
    /// Exact amount the payment must carry.
    pub amount: Amount,
}

/// Something the user wants to happen, which can be realized in potentially more than
/// one way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    /// Pay someone under instructions they supplied, out of band.
    OutputCreation(FixedPaymentInstructions),
}

/// What realizing an intent is worth over time, as breakpoints on a piecewise linear
/// curve.
///
/// The curve is worth nothing at its start, and each breakpoint says how long after the
/// one before it the curve changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayoffCurve {
    start: Instant,
    breakpoints: Vec<(Duration, Amount)>,
}

impl PayoffCurve {
    /// A curve worth nothing at `start`, each duration afterwards has some amount associated with it.
    /// At start `start` the payoff is [`Amount::ZERO`]. Each breakpoint is specified in terms of
    /// a [`Duration`] from the previous point, and a payoff `Amount` at that point. After the last
    /// breakpoint the payoff curve is continued with a constant function with the same value as
    /// the last breakpoint.
    pub fn new(start: Instant, breakpoints: impl IntoIterator<Item = (Duration, Amount)>) -> Self {
        PayoffCurve {
            start,
            breakpoints: breakpoints.into_iter().collect(),
        }
    }
}

/// An [`Action`] together with a [`PayoffCurve`] determined by the user.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IntentWithPolicy {
    pub(crate) inner: Action,
    pub(crate) payoff_curve: PayoffCurve,
}

impl IntentWithPolicy {
    /// Build an intent with the payoff curve the user attached to it.
    pub fn new(inner: Action, payoff_curve: PayoffCurve) -> Self {
        IntentWithPolicy {
            inner,
            payoff_curve,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn request() -> FixedPaymentInstructions {
        FixedPaymentInstructions {
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            amount: Amount::from_sat(50_000),
        }
    }

    /// A curve rising to a peak and falling back to nothing: a payment the user gives up
    /// on once the deadline is past.
    fn single_peaked(start: Instant) -> PayoffCurve {
        PayoffCurve::new(
            start,
            vec![
                (Duration::from_secs(1800), Amount::from_sat(100_000)),
                (Duration::from_secs(1800), Amount::ZERO),
            ],
        )
    }

    #[test]
    fn payment_instructions_carry_a_script_and_an_amount() {
        let instructions = request();

        assert_eq!(instructions.amount, Amount::from_sat(50_000));
        assert_eq!(instructions.clone(), instructions);
    }

    /// An intent nobody has decided about yet is worth nothing at every moment, which
    /// needs no breakpoints to say.
    #[test]
    fn a_curve_may_declare_nothing_at_all() {
        let curve = PayoffCurve::new(Instant::now(), vec![]);

        assert!(curve.breakpoints.is_empty());
    }

    #[test]
    fn breakpoints_are_spaced_from_the_one_before_them() {
        let start = Instant::now();
        let curve = single_peaked(start);

        assert_eq!(
            curve.breakpoints,
            [
                (Duration::from_secs(1800), Amount::from_sat(100_000)),
                (Duration::from_secs(1800), Amount::ZERO),
            ]
        );
        assert_eq!(curve.clone(), curve);
    }
}

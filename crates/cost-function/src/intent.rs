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

    /// What realizing the intent is worth at `at`, interpolated between the breakpoints
    /// on either side of it.
    ///
    /// Before the curve starts it is worth nothing, and past its last breakpoint that
    /// worth holds: a cap on what the wallet will pay from then on.
    pub fn payoff(&self, at: Instant) -> Amount {
        let mut when = self.start;
        let mut worth = Amount::ZERO;

        for &(gap, next_worth) in &self.breakpoints {
            let next = when + gap;

            if at <= next {
                return interpolate(when, worth, next, next_worth, at);
            }

            when = next;
            worth = next_worth;
        }

        worth
    }
}

/// Where a straight line from `start` to `end` sits at `at`.
///
/// A breakpoint no duration after the one before it is a step rather than a line, and
/// the value it steps to is what holds from that instant on.
fn interpolate(
    start_at: Instant,
    start: Amount,
    end_at: Instant,
    end: Amount,
    at: Instant,
) -> Amount {
    let span = end_at.saturating_duration_since(start_at).as_nanos() as i128;
    if span == 0 {
        return end;
    }

    let elapsed = at.saturating_duration_since(start_at).as_nanos() as i128;
    let climb = i128::from(end.to_sat()) - i128::from(start.to_sat());

    Amount::from_sat((i128::from(start.to_sat()) + climb * elapsed / span) as u64)
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
    fn payoff_climbs_and_falls_in_a_straight_line_between_breakpoints() {
        let start = Instant::now();
        let curve = single_peaked(start);

        assert_eq!(curve.payoff(start), Amount::ZERO);
        assert_eq!(
            curve.payoff(start + Duration::from_secs(900)),
            Amount::from_sat(50_000)
        );
        assert_eq!(
            curve.payoff(start + Duration::from_secs(1800)),
            Amount::from_sat(100_000)
        );
        assert_eq!(
            curve.payoff(start + Duration::from_secs(2700)),
            Amount::from_sat(50_000)
        );
    }

    /// Both ends hold, so every instant has an answer.
    #[test]
    fn payoff_outside_the_curve_holds_the_nearer_end() {
        let start = Instant::now();
        let curve = single_peaked(start);

        assert_eq!(curve.payoff(start - Duration::from_secs(600)), Amount::ZERO);
        assert_eq!(
            curve.payoff(start + Duration::from_secs(7200)),
            Amount::ZERO
        );
    }

    /// A curve nobody has decided about is worth nothing whenever you ask.
    #[test]
    fn a_curve_without_breakpoints_is_worth_nothing() {
        let start = Instant::now();
        let curve = PayoffCurve::new(start, vec![]);

        assert_eq!(curve.payoff(start + Duration::from_secs(600)), Amount::ZERO);
    }

    /// A cap the user wants from the outset, rather than a line climbing towards one.
    #[test]
    fn a_breakpoint_no_duration_after_the_start_steps_straight_to_its_worth() {
        let start = Instant::now();
        let curve = PayoffCurve::new(
            start,
            vec![
                (Duration::ZERO, Amount::from_sat(100)),
                (Duration::from_secs(60), Amount::from_sat(100)),
            ],
        );

        assert_eq!(curve.payoff(start), Amount::from_sat(100));
        assert_eq!(
            curve.payoff(start + Duration::from_secs(30)),
            Amount::from_sat(100)
        );
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

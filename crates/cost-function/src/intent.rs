//! Defines what the user wants, and how much they care.

use std::ops::Range;
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

    /// The straight runs between the curve's breakpoints, in order.
    pub fn segments(&self) -> impl Iterator<Item = Segment> + '_ {
        let mut at = self.start;
        let mut worth = Amount::ZERO;

        self.breakpoints.iter().map(move |&(gap, next_worth)| {
            let next = at + gap;
            let segment = Segment {
                span: at..next,
                start: worth,
                end: next_worth,
            };

            at = next;
            worth = next_worth;

            segment
        })
    }

    /// What the intent is worth at `at`.
    pub fn payoff(&self, at: Instant) -> Amount {
        let mut worth = Amount::ZERO;

        for segment in self.segments() {
            if at <= segment.span.end {
                return segment.interpolate(at);
            }

            worth = segment.end;
        }

        worth
    }
}

/// A straight run of a curve, from one breakpoint to the next.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    /// When the run begins and ends.
    pub span: Range<Instant>,
    /// What the curve is worth at `span.start`.
    pub start: Amount,
    /// What the curve is worth at `span.end`.
    pub end: Amount,
}

impl Segment {
    /// What the curve is worth at `at`, exactly.
    ///
    /// `at` is clamped to the span, so either end holds beyond itself. Nanoseconds and
    /// satoshis both sit far inside `i128`, so the arithmetic cannot overflow.
    pub(crate) fn worth_at(&self, at: Instant) -> FractionalAmount {
        let at = at.clamp(self.span.start, self.span.end);

        let span = self
            .span
            .end
            .saturating_duration_since(self.span.start)
            .as_nanos() as i128;

        // A breakpoint no duration after the one before it is a step rather than a run,
        // and the value it steps to is what holds from that instant on.
        if span == 0 {
            return self.end.into();
        }

        let elapsed = at.saturating_duration_since(self.span.start).as_nanos() as i128;
        let start = i128::from(self.start.to_sat());
        let climb = i128::from(self.end.to_sat()) - start;

        FractionalAmount::new(start * span + climb * elapsed, span)
    }

    /// Where the run sits at `at`, to the nearest satoshi.
    fn interpolate(&self, at: Instant) -> Amount {
        self.worth_at(at).round()
    }
}

/// Satoshis that need not be whole.
///
/// A straight run between two breakpoints rarely passes through whole satoshis, and
/// rounding on the way is what blurs out where two curves cross. So the worth stays a
/// fraction until a caller asks for an [`Amount`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FractionalAmount {
    numerator: i128,
    denominator: i128,
}

impl FractionalAmount {
    /// `numerator / denominator` satoshis, reduced.
    ///
    /// Panics if `denominator` is zero.
    pub(crate) fn new(numerator: i128, denominator: i128) -> Self {
        assert!(denominator != 0, "a fraction needs a denominator");

        // Keep the sign on the numerator, so the denominator is always positive.
        let sign = denominator.signum();
        let divisor = gcd(numerator, denominator);

        FractionalAmount {
            numerator: sign * numerator / divisor,
            denominator: sign * denominator / divisor,
        }
    }

    /// The nearest whole satoshi, halves rounded up.
    ///
    /// NOTE: Nothing is worth a negative number of satoshis, so anything below zero rounds to
    /// [`Amount::ZERO`].
    pub(crate) fn round(self) -> Amount {
        let doubled = 2 * self.numerator + self.denominator;
        let sats = doubled.div_euclid(2 * self.denominator);

        Amount::from_sat(u64::try_from(sats).unwrap_or(0))
    }
}

impl From<Amount> for FractionalAmount {
    fn from(amount: Amount) -> Self {
        FractionalAmount {
            numerator: i128::from(amount.to_sat()),
            denominator: 1,
        }
    }
}

/// Greatest common divisor
fn gcd(a: i128, b: i128) -> i128 {
    let (mut a, mut b) = (a.abs(), b.abs());

    while b != 0 {
        (a, b) = (b, a % b);
    }

    a.max(1)
}

/// An [`Action`] together with a [`PayoffCurve`] determined by the user.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Intent {
    pub(crate) inner: Action,
    pub(crate) payoff_curve: PayoffCurve,
}

impl Intent {
    /// Build an intent with the payoff curve the user attached to it.
    pub fn new(inner: Action, payoff_curve: PayoffCurve) -> Self {
        Intent {
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

    /// The runs a curve is made of, which is what a caller comparing two curves needs.
    #[test]
    fn a_curve_is_made_of_the_runs_between_its_breakpoints() {
        let start = Instant::now();
        let peak = start + Duration::from_secs(1800);
        let over = peak + Duration::from_secs(1800);

        let segments: Vec<Segment> = single_peaked(start).segments().collect();

        assert_eq!(
            segments,
            [
                Segment {
                    span: start..peak,
                    start: Amount::ZERO,
                    end: Amount::from_sat(100_000),
                },
                Segment {
                    span: peak..over,
                    start: Amount::from_sat(100_000),
                    end: Amount::ZERO,
                },
            ]
        );
        assert_eq!(segments[0].clone(), segments[0]);
    }

    /// The flat ends run forever, so they are not runs the curve can hand out.
    #[test]
    fn a_curve_without_breakpoints_has_no_runs() {
        assert_eq!(
            PayoffCurve::new(Instant::now(), vec![]).segments().count(),
            0
        );
    }

    /// Asking past either end is answered by that end, rather than by running the line
    /// on to wherever it would have gone.
    #[test]
    fn a_run_holds_at_the_end_nearest_what_it_is_asked_about() {
        let start = Instant::now();
        let segment = Segment {
            span: start..start + Duration::from_secs(60),
            start: Amount::from_sat(100),
            end: Amount::from_sat(200),
        };

        assert_eq!(
            segment.interpolate(start - Duration::from_secs(60)),
            Amount::from_sat(100)
        );
        assert_eq!(
            segment.interpolate(start + Duration::from_secs(600)),
            Amount::from_sat(200)
        );
    }

    /// The fraction is the point of this: half of an odd number of sats is not a whole
    /// number, and saying so is what lets a caller find where two curves cross.
    #[test]
    fn a_run_is_worth_an_exact_fraction_between_its_ends() {
        let start = Instant::now();
        let segment = Segment {
            span: start..start + Duration::from_secs(2),
            start: Amount::ZERO,
            end: Amount::from_sat(1),
        };

        assert_eq!(
            segment.worth_at(start + Duration::from_secs(1)),
            FractionalAmount::new(1, 2)
        );
    }

    /// A step has no span to divide by, so its worth is whole from the outset.
    #[test]
    fn a_step_is_worth_what_it_steps_to() {
        let start = Instant::now();
        let step = Segment {
            span: start..start,
            start: Amount::ZERO,
            end: Amount::from_sat(100),
        };

        assert_eq!(step.worth_at(start), Amount::from_sat(100).into());
    }

    /// Equal worths compare equal however they were arrived at.
    #[test]
    fn a_fraction_is_held_in_lowest_terms() {
        assert_eq!(FractionalAmount::new(2, 4), FractionalAmount::new(1, 2));
        assert_eq!(FractionalAmount::new(-1, -2), FractionalAmount::new(1, 2));
        assert_eq!(FractionalAmount::new(1, -2), FractionalAmount::new(-1, 2));
        assert_eq!(FractionalAmount::new(0, 5), FractionalAmount::new(0, 1));
        assert_eq!(
            FractionalAmount::from(Amount::from_sat(3)),
            FractionalAmount::new(3, 1)
        );
    }

    #[test]
    #[should_panic(expected = "a fraction needs a denominator")]
    fn a_fraction_cannot_be_over_nothing() {
        FractionalAmount::new(1, 0);
    }

    /// Rounding is where the precision is finally given up, and only there.
    #[test]
    fn a_fraction_rounds_to_the_nearest_satoshi() {
        assert_eq!(FractionalAmount::new(1, 3).round(), Amount::ZERO);
        assert_eq!(FractionalAmount::new(2, 3).round(), Amount::from_sat(1));
        assert_eq!(FractionalAmount::new(7, 2).round(), Amount::from_sat(4));
        assert_eq!(FractionalAmount::new(-1, 2).round(), Amount::ZERO);

        // Nothing is worth a negative number of satoshis.
        assert_eq!(FractionalAmount::new(-5, 2).round(), Amount::ZERO);
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

    #[test]
    fn an_intent_keeps_its_action_and_payoff_curve() {
        let start = Instant::now();
        let intent = Intent::new(Action::OutputCreation(request()), single_peaked(start));

        assert_eq!(intent.inner, Action::OutputCreation(request()));
        assert_eq!(intent.payoff_curve, single_peaked(start));
        assert_eq!(intent.clone(), intent);
    }
}

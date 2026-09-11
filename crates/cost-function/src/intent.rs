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

    /// What the intent is worth at `at`.
    pub fn payoff(&self, at: Instant) -> Amount {
        let mut point = Point {
            at: self.start,
            worth: Amount::ZERO,
        };

        for &(gap, worth) in &self.breakpoints {
            let next = Point {
                at: point.at + gap,
                worth,
            };

            if at <= next.at {
                return Line::new(point, next).interpolate(at);
            }

            point = next;
        }

        point.worth
    }
}

/// What a curve is worth at one instant.
#[derive(Clone, Copy)]
struct Point {
    at: Instant,
    worth: Amount,
}

/// A straight run of a curve, from one point to the next.
struct Line(Point, Point);

impl Line {
    /// The straight run between two points, in whichever order they arrive.
    fn new(a: Point, b: Point) -> Self {
        if a.at <= b.at { Line(a, b) } else { Line(b, a) }
    }

    /// Where the line sits at `at`, which holds at whichever end `at` lies beyond.
    fn interpolate(&self, at: Instant) -> Amount {
        let Line(start, end) = *self;

        // Clamp `at` to the span of the line.
        let at = at.clamp(start.at, end.at);

        let span = end.at.saturating_duration_since(start.at).as_nanos() as i128;
        if span == 0 {
            return end.worth;
        }

        let elapsed = at.saturating_duration_since(start.at).as_nanos() as i128;
        let climb = i128::from(end.worth.to_sat()) - i128::from(start.worth.to_sat());

        Amount::from_sat((i128::from(start.worth.to_sat()) + climb * elapsed / span) as u64)
    }
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

    /// Which end of a line is named first says nothing about the curve it came from.
    #[test]
    fn a_line_runs_from_its_earlier_point_to_its_later_one() {
        let start = Instant::now();
        let early = Point {
            at: start,
            worth: Amount::ZERO,
        };
        let late = Point {
            at: start + Duration::from_secs(60),
            worth: Amount::from_sat(100),
        };
        let halfway = start + Duration::from_secs(30);

        assert_eq!(
            Line::new(early, late).interpolate(halfway),
            Amount::from_sat(50)
        );
        assert_eq!(
            Line::new(late, early).interpolate(halfway),
            Amount::from_sat(50)
        );
    }

    /// Asking past either end is answered by that end, rather than by running the line
    /// on to wherever it would have gone.
    #[test]
    fn a_line_holds_at_the_end_nearest_what_it_is_asked_about() {
        let start = Instant::now();
        let line = Line::new(
            Point {
                at: start,
                worth: Amount::from_sat(100),
            },
            Point {
                at: start + Duration::from_secs(60),
                worth: Amount::from_sat(200),
            },
        );

        assert_eq!(
            line.interpolate(start - Duration::from_secs(60)),
            Amount::from_sat(100)
        );
        assert_eq!(
            line.interpolate(start + Duration::from_secs(600)),
            Amount::from_sat(200)
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

    #[test]
    fn an_intent_keeps_its_action_and_payoff_curve() {
        let start = Instant::now();
        let intent = Intent::new(Action::OutputCreation(request()), single_peaked(start));

        assert_eq!(intent.inner, Action::OutputCreation(request()));
        assert_eq!(intent.payoff_curve, single_peaked(start));
        assert_eq!(intent.clone(), intent);
    }
}

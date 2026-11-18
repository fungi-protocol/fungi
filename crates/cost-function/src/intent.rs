//! Defines what the user wants, and how much they care.

use std::{ops::Range, time::Instant};

mod amount;
pub use amount::FractionalAmount;
mod grid;
pub use grid::ScheduleGrid;

use bitcoin::{Amount, ScriptBuf, TxOut};

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
pub enum Outcome {
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
    start_slot: u64,
    end_slot: u64,
    breakpoints: Vec<(u64, Amount)>,
}

impl PayoffCurve {
    /// Construct a curve on coarse schedule slots, with gaps relative to the previous point.
    /// The curve starts at zero and holds its last value indefinitely. At a zero-slot step,
    /// all entries apply in order and the last value wins. Slot overflow returns `None`.
    pub fn new(
        start_slot: u64,
        breakpoints: impl IntoIterator<Item = (u64, Amount)>,
    ) -> Option<Self> {
        let mut end_slot = start_slot;
        let mut points = Vec::new();
        for (gap, worth) in breakpoints {
            end_slot = end_slot.checked_add(gap)?;
            points.push((gap, worth));
        }
        Some(Self {
            start_slot,
            end_slot,
            breakpoints: points,
        })
    }

    /// Place the curve's bounded segments on the shared grid in time order.
    /// Returns `None` if a breakpoint lies beyond the clock's representable range.
    pub fn lines(&self, grid: ScheduleGrid) -> Option<impl Iterator<Item = Line> + '_> {
        let mut point = Point {
            at: grid.at_slot(self.start_slot)?,
            worth: Amount::ZERO,
        };
        grid.at_slot(self.end_slot)?;
        Some(self.breakpoints.iter().map(move |&(gap, worth)| {
            let next = Point {
                at: point.at + ScheduleGrid::slot_duration(gap),
                worth,
            };
            let line = Line(point, next);
            point = next;
            line
        }))
    }

    /// Approximate payoff at `at` on the shared grid.
    pub fn payoff(&self, at: Instant, grid: ScheduleGrid) -> Option<FractionalAmount> {
        let lines = self.lines(grid)?;
        if at < grid.at_slot(self.start_slot)? {
            return Some(Amount::ZERO.into());
        }
        let mut worth = Amount::ZERO;
        for line in lines {
            if at < line.1.at {
                return Some(line.interpolate(at));
            }
            worth = line.1.worth;
        }
        Some(worth.into())
    }
}

/// What a curve is worth at one instant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Point {
    /// When the curve is worth this.
    pub at: Instant,
    /// What it is worth then.
    pub worth: Amount,
}

/// A straight run of a curve, from one point to the next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Line(Point, Point);

impl Line {
    /// The straight run between two points, in whichever order they arrive.
    pub fn new(a: Point, b: Point) -> Self {
        if a.at <= b.at { Line(a, b) } else { Line(b, a) }
    }

    /// The half-open interval over which this segment supplies the curve.
    pub fn range(&self) -> Range<Instant> {
        self.0.at..self.1.at
    }

    /// The segment's start and end points.
    pub fn points(&self) -> (Point, Point) {
        (self.0, self.1)
    }

    /// Where the line sits at `at`, which holds at whichever end `at` lies beyond.
    pub fn interpolate(&self, at: Instant) -> FractionalAmount {
        let Line(start, end) = *self;

        // Clamp `at` to the span of the line.
        let at = at.clamp(start.at, end.at);

        let span = end.at.saturating_duration_since(start.at).as_secs_f64();
        if span == 0.0 {
            return end.worth.into();
        }
        let elapsed = at.saturating_duration_since(start.at).as_secs_f64();
        FractionalAmount::interpolate(start.worth, end.worth, elapsed / span)
    }
}

/// An [`Outcome`] together with a [`PayoffCurve`] determined by the user.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Intent {
    pub(crate) inner: Outcome,
    pub(crate) payoff_curve: PayoffCurve,
}

impl Intent {
    /// Build an intent with the payoff curve the user attached to it.
    pub fn new(inner: Outcome, payoff_curve: PayoffCurve) -> Self {
        Intent {
            inner,
            payoff_curve,
        }
    }

    /// The output that pays for this intent.
    pub(crate) fn payment_output(&self) -> TxOut {
        match &self.inner {
            Outcome::OutputCreation(instructions) => TxOut {
                value: instructions.amount,
                script_pubkey: instructions.script_pubkey.clone(),
            },
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
    fn single_peaked() -> PayoffCurve {
        PayoffCurve::new(
            0,
            vec![
                (18_000_000, Amount::from_sat(100_000)),
                (18_000_000, Amount::ZERO),
            ],
        )
        .unwrap()
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
        let curve = PayoffCurve::new(0, vec![]).unwrap();

        assert!(curve.breakpoints.is_empty());
    }

    #[test]
    fn payoff_climbs_and_falls_in_a_straight_line_between_breakpoints() {
        let start = Instant::now();
        let curve = single_peaked();

        assert_eq!(
            curve
                .payoff(start, ScheduleGrid::new(start))
                .unwrap()
                .round()
                .unwrap(),
            Amount::ZERO
        );
        assert_eq!(
            curve
                .payoff(start + Duration::from_secs(900), ScheduleGrid::new(start))
                .unwrap()
                .round()
                .unwrap(),
            Amount::from_sat(50_000)
        );
        assert_eq!(
            curve
                .payoff(start + Duration::from_secs(1800), ScheduleGrid::new(start))
                .unwrap()
                .round()
                .unwrap(),
            Amount::from_sat(100_000)
        );
        assert_eq!(
            curve
                .payoff(start + Duration::from_secs(2700), ScheduleGrid::new(start))
                .unwrap()
                .round()
                .unwrap(),
            Amount::from_sat(50_000)
        );
    }

    /// Both ends hold, so every instant has an answer.
    #[test]
    fn payoff_outside_the_curve_holds_the_nearer_end() {
        let start = Instant::now();
        let curve = single_peaked();

        assert_eq!(
            curve
                .payoff(start - Duration::from_secs(600), ScheduleGrid::new(start))
                .unwrap()
                .round()
                .unwrap(),
            Amount::ZERO
        );
        assert_eq!(
            curve
                .payoff(start + Duration::from_secs(7200), ScheduleGrid::new(start))
                .unwrap()
                .round()
                .unwrap(),
            Amount::ZERO
        );
    }

    /// A curve nobody has decided about is worth nothing whenever you ask.
    #[test]
    fn a_curve_without_breakpoints_is_worth_nothing() {
        let start = Instant::now();
        let curve = PayoffCurve::new(0, vec![]).unwrap();

        assert_eq!(
            curve
                .payoff(start + Duration::from_secs(600), ScheduleGrid::new(start))
                .unwrap()
                .round()
                .unwrap(),
            Amount::ZERO
        );
    }

    /// A cap the user wants from the outset, rather than a line climbing towards one.
    #[test]
    fn a_breakpoint_no_duration_after_the_start_steps_straight_to_its_worth() {
        let start = Instant::now();
        let curve = PayoffCurve::new(
            0,
            vec![(0, Amount::from_sat(100)), (600_000, Amount::from_sat(100))],
        )
        .unwrap();

        assert_eq!(
            curve
                .payoff(start, ScheduleGrid::new(start))
                .unwrap()
                .round()
                .unwrap(),
            Amount::from_sat(100)
        );
        assert_eq!(
            curve
                .payoff(start + Duration::from_secs(30), ScheduleGrid::new(start))
                .unwrap()
                .round()
                .unwrap(),
            Amount::from_sat(100)
        );
    }

    /// The lines a curve is made of, which is what a caller comparing two curves needs.
    #[test]
    fn a_curve_is_made_of_the_lines_between_its_breakpoints() {
        let start = Instant::now();
        let peak = start + Duration::from_secs(1800);
        let over = peak + Duration::from_secs(1800);

        let endpoints: Vec<_> = single_peaked()
            .lines(ScheduleGrid::new(start))
            .unwrap()
            .map(|line| line.points())
            .collect();

        assert_eq!(
            endpoints,
            [
                (
                    Point {
                        at: start,
                        worth: Amount::ZERO
                    },
                    Point {
                        at: peak,
                        worth: Amount::from_sat(100_000)
                    },
                ),
                (
                    Point {
                        at: peak,
                        worth: Amount::from_sat(100_000)
                    },
                    Point {
                        at: over,
                        worth: Amount::ZERO
                    },
                ),
            ]
        );
    }

    /// The flat ends run forever, so they are not lines the curve can hand out.
    #[test]
    fn a_curve_without_breakpoints_has_no_lines() {
        assert_eq!(
            PayoffCurve::new(0, vec![])
                .unwrap()
                .lines(ScheduleGrid::new(Instant::now()))
                .unwrap()
                .count(),
            0
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
            Line::new(early, late).interpolate(halfway).round().unwrap(),
            Amount::from_sat(50)
        );
        assert_eq!(
            Line::new(late, early).interpolate(halfway).round().unwrap(),
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
            line.interpolate(start - Duration::from_secs(60))
                .round()
                .unwrap(),
            Amount::from_sat(100)
        );
        assert_eq!(
            line.interpolate(start + Duration::from_secs(600))
                .round()
                .unwrap(),
            Amount::from_sat(200)
        );
    }

    #[test]
    fn breakpoints_are_spaced_from_the_one_before_them() {
        let curve = single_peaked();

        assert_eq!(
            curve.breakpoints,
            [
                (18_000_000, Amount::from_sat(100_000)),
                (18_000_000, Amount::ZERO),
            ]
        );
        assert_eq!(curve.clone(), curve);
    }

    #[test]
    fn authored_slots_are_checked_and_placed_on_the_shared_grid() {
        assert!(PayoffCurve::new(u64::MAX, [(1, Amount::ZERO)]).is_none());
        let curve = PayoffCurve::new(1, [(2, Amount::from_sat(1))]).unwrap();
        let origin = Instant::now();
        for grid in [
            ScheduleGrid::new(origin),
            ScheduleGrid::new(origin + ScheduleGrid::JIFFY),
        ] {
            let line = curve.lines(grid).unwrap().next().unwrap();
            assert_eq!(
                line.range(),
                grid.at_slot(1).unwrap()..grid.at_slot(3).unwrap()
            );
        }
    }

    #[test]
    fn an_intent_keeps_its_outcome_and_payoff_curve() {
        let intent = Intent::new(Outcome::OutputCreation(request()), single_peaked());

        assert_eq!(intent.inner, Outcome::OutputCreation(request()));
        assert_eq!(intent.payoff_curve, single_peaked());
        assert_eq!(intent.clone(), intent);
    }
}

#[cfg(test)]
mod comparison_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn nearby_slopes_change_order_once_without_satoshi_plateaus() {
        let start = Instant::now();
        let a = PayoffCurve::new(0, [(1, Amount::from_sat(21))]).unwrap();
        let b = PayoffCurve::new(0, [(0, Amount::from_sat(1)), (1, Amount::from_sat(20))]).unwrap();
        assert!(
            a.payoff(start + Duration::from_micros(45), ScheduleGrid::new(start))
                .unwrap()
                < b.payoff(start + Duration::from_micros(45), ScheduleGrid::new(start))
                    .unwrap()
        );
        assert_eq!(
            a.payoff(start + Duration::from_micros(50), ScheduleGrid::new(start))
                .unwrap(),
            b.payoff(start + Duration::from_micros(50), ScheduleGrid::new(start))
                .unwrap()
        );
        assert!(
            a.payoff(start + Duration::from_micros(55), ScheduleGrid::new(start))
                .unwrap()
                > b.payoff(start + Duration::from_micros(55), ScheduleGrid::new(start))
                    .unwrap()
        );
    }

    #[test]
    fn all_steps_at_a_boundary_have_taken_effect() {
        let start = Instant::now();
        let at = start + ScheduleGrid::AUTHORED_STEP;
        let curve = PayoffCurve::new(
            0,
            [
                (1, Amount::ZERO),
                (0, Amount::from_sat(50)),
                (0, Amount::from_sat(100)),
            ],
        )
        .unwrap();
        assert_eq!(
            curve.payoff(at, ScheduleGrid::new(start)).unwrap().as_sat(),
            100.0
        );

        let last_step = curve
            .lines(ScheduleGrid::new(start))
            .unwrap()
            .last()
            .unwrap();
        assert_eq!(last_step.range(), at..at);
        assert_eq!(last_step.interpolate(at).as_sat(), 100.0);
    }
}

#[cfg(test)]
mod future_step_test {
    use super::*;
    #[test]
    fn a_future_initial_step_is_zero_before_its_start() {
        let now = Instant::now();
        let curve = PayoffCurve::new(10_000, [(0, Amount::from_sat(100))]).unwrap();
        assert_eq!(
            curve.payoff(now, ScheduleGrid::new(now)).unwrap().as_sat(),
            0.0
        );
    }
}

pub(crate) mod schedule;

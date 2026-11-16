//! Certified crossings of an affine net cost between its breakpoints.

mod bounds;
mod exact;

use super::{PayoffCurve, ScheduleGrid};
use bitcoin::SignedAmount;
pub use bounds::{CostBounds, CostSign};
use exact::Exact;
use num_integer::Integer;
use std::{
    cmp::Ordering,
    collections::BTreeMap,
    time::{Duration, Instant},
};

const TICK: Duration = ScheduleGrid::JIFFY;

/// Positive terms are the other alternative's payoffs when comparing alternatives.
pub(crate) struct Comparison<'a> {
    pub cost: SignedAmount,
    pub positive: &'a [PayoffCurve],
    pub negative: &'a [PayoffCurve],
}

impl Comparison<'_> {
    pub fn bounds_at(&self, at: Instant, grid: ScheduleGrid) -> Option<CostBounds> {
        Some(self.combined(at, grid)?.segment_at(at).bounds_at(at))
    }

    pub fn first_negative(&self, now: Instant, grid: ScheduleGrid) -> Option<Instant> {
        let curve = self.combined(now, grid)?;
        let mut exact = curve.exact_cursor();
        let current = curve.segment_at(now).ordering_at(now, &mut exact);
        if current == Ordering::Less {
            return Some(now);
        }
        curve
            .first_transition(now, grid, current, Target::Negative, &mut exact)?
            .at()
    }

    pub fn next_order_change(&self, now: Instant, grid: ScheduleGrid) -> Option<OrderChange> {
        let curve = self.combined(now, grid)?;
        let mut exact = curve.exact_cursor();
        let current = curve.segment_at(now).ordering_at(now, &mut exact);
        curve.first_transition(now, grid, current, Target::Different(current), &mut exact)
    }

    fn combined(&self, at: Instant, grid: ScheduleGrid) -> Option<Combined> {
        let mut knots: BTreeMap<Instant, Knot> = BTreeMap::new();
        for (curves, sign) in [(self.positive, 1), (self.negative, -1)] {
            for curve in curves {
                for line in curve.lines(grid)? {
                    let (start, end) = line.points();
                    let rise =
                        sign * (i128::from(end.worth.to_sat()) - i128::from(start.worth.to_sat()));
                    if rise == 0 {
                        continue;
                    }
                    // A Duration has at most u64::MAX seconds, so its nanos
                    // fit in a positive i128. Each rise is at most u64::MAX.
                    let run = (end.at - start.at).as_nanos() as i128;
                    if run == 0 {
                        knots.entry(start.at).or_default().jump += rise;
                    } else {
                        *knots
                            .entry(start.at)
                            .or_default()
                            .slopes
                            .entry(run)
                            .or_default() += rise;
                        *knots
                            .entry(end.at)
                            .or_default()
                            .slopes
                            .entry(run)
                            .or_default() -= rise;
                    }
                }
            }
        }
        let changes: Vec<_> = knots
            .into_iter()
            .filter_map(|(at, knot)| knot.change(at))
            .collect();
        let start = changes.first().map_or(at, |change| at.min(change.at));
        Some(Combined {
            start,
            cost: self.cost.to_sat().into(),
            changes,
        })
    }
}

/// A certified change in the ordering of two net costs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrderChange {
    pub timing: OrderChangeTime,
    pub before: Ordering,
    pub after: Ordering,
}

/// A step at a knot or a continuous change bracketed by adjacent jiffies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderChangeTime {
    AtKnot(Instant),
    Between { lower: Instant },
}

impl OrderChange {
    /// The first instant at which the reported new ordering is certified.
    pub fn at(self) -> Option<Instant> {
        match self.timing {
            OrderChangeTime::AtKnot(at) => Some(at),
            OrderChangeTime::Between { lower } => lower.checked_add(TICK),
        }
    }
}

#[derive(Clone, Copy)]
enum Target {
    Negative,
    Different(Ordering),
}

impl Target {
    fn matches(self, order: Ordering) -> bool {
        match self {
            Self::Negative => order == Ordering::Less,
            Self::Different(current) => order != current,
        }
    }
}

#[derive(Default)]
struct Knot {
    jump: i128,
    slopes: BTreeMap<i128, i128>,
}

impl Knot {
    fn change(self, at: Instant) -> Option<Change> {
        let mut reduced = BTreeMap::<_, i128>::new();
        // First group equal raw runs, then group equal reduced denominators.
        // A collinear split installs and removes the same slope at its knot.
        for (run, rise) in self.slopes {
            if rise != 0 {
                let divisor = rise.gcd(&run);
                *reduced.entry(run / divisor).or_default() += rise / divisor;
            }
        }
        let slopes: Vec<_> = reduced.into_iter().filter(|&(_, rise)| rise != 0).collect();
        let bounds = slopes.iter().fold(CostBounds::ZERO, |sum, &(run, rise)| {
            sum.add(CostBounds::ratio(rise, run))
        });
        let change = Change {
            at,
            jump: self.jump,
            slopes,
            bounds,
        };
        if change.jump != 0 {
            return Some(change);
        }
        match bounds.sign() {
            CostSign::Zero => None,
            CostSign::Overlap if change.exact_slope().is_zero() => None,
            _ => Some(change),
        }
    }
}

struct Change {
    at: Instant,
    jump: i128,
    slopes: Vec<(i128, i128)>,
    bounds: CostBounds,
}

impl Change {
    fn exact_slope(&self) -> Exact {
        self.slopes
            .iter()
            .fold(Exact::integer(0), |sum, &(run, rise)| {
                sum.add(&Exact::ratio(rise, run))
            })
    }
}

struct Combined {
    start: Instant,
    cost: i128,
    changes: Vec<Change>,
}

impl Combined {
    fn segments(&self) -> impl Iterator<Item = Segment> + '_ {
        let mut start = self.start;
        let mut value = CostBounds::integer(self.cost);
        let mut slope = CostBounds::ZERO;
        (0..=self.changes.len()).map(move |index| {
            let next = self.changes.get(index);
            let segment = Segment {
                index,
                start,
                end: next.map(|change| change.at),
                continuous_end: next.is_some_and(|change| change.jump == 0),
                value,
                slope,
            };
            if let Some(change) = next {
                start = change.at;
                value = segment
                    .bounds_at(start)
                    .add(CostBounds::integer(change.jump));
                slope = slope.add(change.bounds);
            }
            segment
        })
    }

    fn segment_at(&self, at: Instant) -> Segment {
        self.segments()
            .find(|segment| segment.contains(at))
            .expect("the final segment continues indefinitely")
    }

    fn first_transition(
        &self,
        now: Instant,
        grid: ScheduleGrid,
        current: Ordering,
        target: Target,
        exact: &mut ExactCursor<'_>,
    ) -> Option<OrderChange> {
        for segment in self.segments() {
            let from = now.max(segment.start);
            if !segment.contains(from) {
                continue;
            }
            let first = grid.ceil(from)?;
            if segment.contains_sample(first)
                && let Some(change) = segment.first_transition(first, current, target, exact)
            {
                return Some(change);
            }
        }
        None
    }

    fn exact_cursor(&self) -> ExactCursor<'_> {
        ExactCursor {
            changes: &self.changes,
            applied: 0,
            start: self.start,
            value: Exact::integer(self.cost),
            slope: Exact::integer(0),
        }
    }
}

// Only advances on overlap; successive ambiguities replay each event at most once.
struct ExactCursor<'a> {
    changes: &'a [Change],
    applied: usize,
    start: Instant,
    value: Exact,
    slope: Exact,
}

impl ExactCursor<'_> {
    fn at(&mut self, segment: Segment, at: Instant) -> Exact {
        for change in &self.changes[self.applied..segment.index] {
            self.value = self.value_at(change.at).add(&Exact::integer(change.jump));
            self.slope = self.slope.add(&change.exact_slope());
            self.start = change.at;
        }
        self.applied = segment.index;
        self.value_at(at)
    }

    fn value_at(&self, at: Instant) -> Exact {
        self.value
            .add(&self.slope.scale((at - self.start).as_nanos() as i128))
    }
}

#[derive(Clone, Copy)]
struct Segment {
    index: usize,
    start: Instant,
    end: Option<Instant>,
    continuous_end: bool,
    value: CostBounds,
    slope: CostBounds,
}

impl Segment {
    fn contains(self, at: Instant) -> bool {
        self.end.is_none_or(|end| at < end)
    }

    fn contains_sample(self, at: Instant) -> bool {
        self.contains(at) || (self.continuous_end && self.end == Some(at))
    }

    fn bounds_at(self, at: Instant) -> CostBounds {
        self.value
            .add(self.slope.scale((at - self.start).as_nanos() as i128))
    }

    fn ordering_at(self, at: Instant, exact: &mut ExactCursor<'_>) -> Ordering {
        match self.bounds_at(at).sign() {
            CostSign::Negative => Ordering::Less,
            CostSign::Zero => Ordering::Equal,
            CostSign::Positive => Ordering::Greater,
            CostSign::Overlap => {
                let value = exact.at(self, at);
                if value.is_negative() {
                    Ordering::Less
                } else if value.is_zero() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
        }
    }

    fn first_transition(
        self,
        first: Instant,
        current: Ordering,
        target: Target,
        exact: &mut ExactCursor<'_>,
    ) -> Option<OrderChange> {
        let order = self.ordering_at(first, exact);
        if target.matches(order) {
            if let Some(lower) = first.checked_sub(TICK)
                && lower >= self.start
            {
                let before = self.ordering_at(lower, exact);
                return Some(OrderChange {
                    timing: OrderChangeTime::Between { lower },
                    before,
                    after: order,
                });
            }
            return Some(OrderChange {
                timing: OrderChangeTime::AtKnot(first),
                before: current,
                after: order,
            });
        }
        // A continuous endpoint belongs to both pieces. A step belongs to
        // the following piece, which is classified separately.
        let span = (self.end? - first).as_nanos();
        let mut high = (span - u128::from(!self.continuous_end)) / TICK.as_nanos();
        let at = |ticks: u128| {
            let seconds = (ticks / 1_000_000) as u64;
            let nanos = (ticks % 1_000_000) as u32 * TICK.subsec_nanos();
            first + Duration::new(seconds, nanos)
        };
        let mut after = self.ordering_at(at(high), exact);
        if !target.matches(after) {
            return None;
        }
        // A continuous affine piece starting outside the target can enter it
        // once. Keep certified endpoints until the bracket is one jiffy wide.
        let mut low = 0_u128;
        let mut before = order;
        loop {
            let mid = low.midpoint(high);
            if mid == low {
                break;
            }
            let order = self.ordering_at(at(mid), exact);
            if target.matches(order) {
                high = mid;
                after = order;
            } else {
                low = mid;
                before = order;
            }
        }
        Some(OrderChange {
            timing: OrderChangeTime::Between { lower: at(low) },
            before,
            after,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::Amount;

    fn curve(start: u64, points: impl IntoIterator<Item = (u64, u64)>) -> PayoffCurve {
        PayoffCurve::new(
            start,
            points
                .into_iter()
                .map(|(gap, sats)| (gap, Amount::from_sat(sats))),
        )
        .unwrap()
    }

    fn ordering(comparison: &Comparison<'_>, at: Instant, grid: ScheduleGrid) -> Ordering {
        let combined = comparison.combined(at, grid).unwrap();
        combined
            .segment_at(at)
            .ordering_at(at, &mut combined.exact_cursor())
    }

    #[test]
    fn an_intercept_is_not_yet_an_improvement() {
        let start = Instant::now();
        let grid = ScheduleGrid::new(start);
        let comparison = Comparison {
            cost: SignedAmount::from_sat(5),
            positive: &[],
            negative: &[curve(0, [(1, 100)])],
        };
        let at = start + Duration::from_micros(6);
        assert_eq!(comparison.first_negative(start, grid), Some(at));
        assert!(ordering(&comparison, at, grid) == Ordering::Less);
        assert_eq!(comparison.first_negative(at, grid), Some(at));
        assert_eq!(ordering(&comparison, at - TICK, grid), Ordering::Equal);
    }

    #[test]
    fn a_query_between_ticks_can_first_improve_at_the_next_tick() {
        let start = Instant::now();
        let grid = ScheduleGrid::new(start);
        let comparison = Comparison {
            cost: SignedAmount::from_sat(1),
            positive: &[],
            negative: &[curve(0, [(1, 200)])],
        };
        let now = start + Duration::from_nanos(250);
        assert!(ordering(&comparison, now, grid) == Ordering::Greater);
        assert_eq!(comparison.first_negative(now, grid), Some(start + TICK));
        let already_negative = start + Duration::from_nanos(750);
        assert_eq!(
            comparison.first_negative(already_negative, grid),
            Some(already_negative)
        );
    }

    #[test]
    fn nearly_parallel_large_payoffs_cross_at_the_exact_strict_tick() {
        let start = Instant::now();
        let grid = ScheduleGrid::new(start);
        let base = 1 << 50;
        let comparison = Comparison {
            cost: SignedAmount::ZERO,
            positive: &[curve(0, [(0, base), (1, base + 8)])],
            negative: &[curve(0, [(0, base - 2), (1, base + 9)])],
        };
        let at = start + Duration::from_micros(67);
        assert_eq!(comparison.first_negative(start, grid), Some(at));
        assert!(ordering(&comparison, at - TICK, grid) == Ordering::Greater);
        assert!(ordering(&comparison, at, grid) == Ordering::Less);
    }

    #[test]
    fn future_steps_and_expiration_are_not_interpolated_across() {
        let start = Instant::now();
        let grid = ScheduleGrid::new(start);
        let comparison = Comparison {
            cost: SignedAmount::from_sat(50),
            positive: &[],
            negative: &[curve(1, [(0, 100), (1, 0)])],
        };
        let knot = grid.at_slot(1).unwrap();
        assert_eq!(comparison.first_negative(start, grid), Some(knot));
        assert_eq!(
            comparison.next_order_change(start, grid),
            Some(OrderChange {
                timing: OrderChangeTime::AtKnot(knot),
                before: Ordering::Greater,
                after: Ordering::Less,
            })
        );
        assert_eq!(
            comparison.first_negative(grid.at_slot(2).unwrap(), grid),
            None
        );
    }

    #[test]
    fn a_payoff_step_at_the_origin_is_scheduled_from_an_earlier_query() {
        let origin = Instant::now() + Duration::from_secs(1);
        let grid = ScheduleGrid::new(origin);
        let now = origin - Duration::from_nanos(1_234);
        let step = [curve(0, [(0, 10)])];
        let falling_cost = Comparison {
            cost: SignedAmount::from_sat(5),
            positive: &[],
            negative: &step,
        };
        assert_eq!(falling_cost.first_negative(now, grid), Some(origin));
        assert_eq!(
            falling_cost.next_order_change(now, grid),
            Some(OrderChange {
                timing: OrderChangeTime::AtKnot(origin),
                before: Ordering::Greater,
                after: Ordering::Less,
            })
        );

        let rising_cost = Comparison {
            cost: SignedAmount::from_sat(-5),
            positive: &step,
            negative: &[],
        };
        assert_eq!(rising_cost.first_negative(now, grid), Some(now));
        assert_eq!(
            rising_cost.next_order_change(now, grid),
            Some(OrderChange {
                timing: OrderChangeTime::AtKnot(origin),
                before: Ordering::Less,
                after: Ordering::Greater,
            })
        );
    }

    #[test]
    fn collinear_splits_do_not_leave_redundant_breakpoints() {
        let origin = Instant::now();
        let comparison = Comparison {
            cost: SignedAmount::ZERO,
            positive: &[curve(0, [(2, 2)])],
            negative: &[curve(0, [(1, 1), (1, 2)])],
        };
        let combined = comparison
            .combined(origin, ScheduleGrid::new(origin))
            .unwrap();
        assert!(combined.changes.is_empty());
    }

    #[test]
    fn cancelling_curves_and_early_clock_reads_do_not_move_the_grid() {
        let start = Instant::now();
        let grid = ScheduleGrid::new(start);
        let base = curve(1, [(1, 100)]);
        let base_only = Comparison {
            cost: SignedAmount::from_sat(1),
            positive: &[],
            negative: std::slice::from_ref(&base),
        };
        let with_cancellation = Comparison {
            positive: &[
                curve(0, [(2, 1), (4, 3)]),
                curve(0, [(3, 1), (3, 2)]),
                curve(0, [(6, 1)]),
            ],
            negative: &[base.clone(), curve(0, [(6, 6)])],
            ..base_only
        };
        let first_change = grid.at_slot(1).unwrap();
        assert_eq!(
            with_cancellation.combined(start, grid).unwrap().changes[0].at,
            first_change
        );
        let first = first_change + Duration::from_micros(2);
        let already_negative = first_change + Duration::from_nanos(1250);
        for comparison in [base_only, with_cancellation] {
            assert_eq!(
                comparison.first_negative(start - Duration::from_nanos(250), grid),
                Some(first)
            );
            assert_eq!(comparison.first_negative(start, grid), Some(first));
            assert_eq!(
                comparison.first_negative(start + Duration::from_nanos(250), grid),
                Some(first)
            );
            assert_eq!(
                comparison.first_negative(already_negative, grid),
                Some(already_negative)
            );
        }
    }

    #[test]
    fn overlapping_coprime_runs_widen_without_losing_a_strict_crossing() {
        let start = Instant::now();
        let grid = ScheduleGrid::new(start);
        // At one coarse slot the net cost is (3-a)/(a*(a+1)*(a+3)) < 0.
        // Even though the ordinary enclosure overlaps zero, this is strict.
        let a = 1 << 43;
        let comparison = Comparison {
            cost: SignedAmount::from_sat(1),
            positive: &[curve(0, [(a, 1)]), curve(0, [(a + 3, 1)])],
            negative: &[curve(0, [(a + 1, a + 3)])],
        };
        let first = grid.at_slot(1).unwrap();
        let combined = comparison.combined(start, grid).unwrap();
        let segment = combined.segment_at(first);
        assert_eq!(segment.bounds_at(first).sign(), CostSign::Overlap);
        assert!(matches!(
            combined.exact_cursor().at(segment, first),
            Exact::Wide(_)
        ));
        assert_eq!(ordering(&comparison, first, grid), Ordering::Less);
        assert_eq!(comparison.first_negative(start, grid), Some(first));

        let reversed = Comparison {
            cost: SignedAmount::from_sat(-1),
            positive: comparison.negative,
            negative: comparison.positive,
        };
        assert_eq!(
            reversed.bounds_at(first, grid).unwrap().sign(),
            CostSign::Overlap
        );
        assert_eq!(ordering(&reversed, first, grid), Ordering::Greater);
    }

    #[test]
    fn nearly_cancelling_slopes_still_schedule_the_first_strict_improvement() {
        let start = Instant::now();
        let grid = ScheduleGrid::new(start);
        let a = 1 << 26;
        // These authored runs last about two hours. Their net slope per
        // slot is 2/(a+1) - 1/a - 1/(a+2) = -2/(a*(a+1)*(a+2)).
        let comparison = Comparison {
            cost: SignedAmount::ZERO,
            positive: &[curve(0, [(a + 1, 2)])],
            negative: &[curve(0, [(a, 1)]), curve(0, [(a + 2, 1)])],
        };
        let first = start + TICK;
        assert_eq!(comparison.first_negative(start, grid), Some(first));
        assert_eq!(comparison.first_negative(first, grid), Some(first));
    }

    #[test]
    fn staggered_equal_slopes_leave_an_exact_constant_offset() {
        let start = Instant::now();
        let grid = ScheduleGrid::new(start);
        let later = grid.at_slot(1).unwrap();
        let comparison = Comparison {
            cost: SignedAmount::from_sat(1),
            positive: &[curve(1, [(10, 1_000)])],
            negative: &[curve(0, [(10, 1_000)])],
        };
        let bounds = comparison.bounds_at(later, grid).unwrap();
        let (low, high) = bounds.bounds();
        assert!((low..=high).contains(&-99.0));
        assert_eq!(bounds.sign(), CostSign::Negative);
        assert_eq!(
            comparison.first_negative(start, grid),
            Some(start + Duration::from_micros(2))
        );
        assert_eq!(comparison.first_negative(later, grid), Some(later));
    }

    #[test]
    fn a_decreasing_cost_with_no_order_reversal_does_not_schedule() {
        let start = Instant::now();
        let grid = ScheduleGrid::new(start);
        let comparison = Comparison {
            cost: SignedAmount::from_sat(10),
            positive: &[],
            negative: &[curve(0, [(1, 9)])],
        };
        let at = start + Duration::from_micros(50);
        let combined = comparison.combined(at, grid).unwrap();
        let segment = combined.segment_at(at);
        let mut exact = combined.exact_cursor();
        assert_eq!(segment.bounds_at(at).sign(), CostSign::Positive);
        assert_eq!(segment.ordering_at(at, &mut exact), Ordering::Greater);
        assert_eq!(exact.applied, 0);
        assert_eq!(comparison.first_negative(start, grid), None);
    }

    #[test]
    fn a_continuous_endpoint_keeps_its_crossing_bracket() {
        let origin = Instant::now();
        let grid = ScheduleGrid::new(origin);
        let knot = grid.at_slot(1).unwrap();
        let now = knot - Duration::from_nanos(750);
        let comparison = Comparison {
            cost: SignedAmount::from_sat(100),
            positive: &[],
            negative: &[curve(0, [(1, 100)])],
        };
        assert_eq!(
            comparison.next_order_change(now, grid),
            Some(OrderChange {
                timing: OrderChangeTime::Between { lower: knot - TICK },
                before: Ordering::Greater,
                after: Ordering::Equal,
            })
        );
    }

    #[test]
    fn a_jump_does_not_make_its_left_limit_actionable() {
        let start = Instant::now();
        let grid = ScheduleGrid::new(start);
        let comparison = Comparison {
            cost: SignedAmount::from_sat(199),
            positive: &[],
            negative: &[curve(0, [(1, 200), (0, 0)])],
        };
        assert_eq!(comparison.first_negative(start, grid), None);
        let before_crossing = start + Duration::from_nanos(99_250);
        assert_eq!(comparison.first_negative(before_crossing, grid), None);
        assert_eq!(comparison.next_order_change(before_crossing, grid), None);
        let after_crossing = start + Duration::from_nanos(99_750);
        assert_eq!(
            comparison.first_negative(after_crossing, grid),
            Some(after_crossing)
        );
        let knot = grid.at_slot(1).unwrap();
        assert_eq!(comparison.first_negative(knot, grid), None);
    }

    proptest::proptest! {
        #[test]
        fn scheduling_matches_the_first_negative_jiffy(
            a in 0u64..100, b in 0u64..100, cost in 0u64..150,
            first in 1u64..3, second in 1u64..3, query in 0u64..410,
        ) {
            let start = Instant::now();
            let grid = ScheduleGrid::new(start);
            let curves = [curve(0, [(first, a), (second, 0)]), curve(0, [(second, b)])];
            let comparison = Comparison { cost: SignedAmount::from_sat(cost as i64), positive: &[], negative: &curves };
            let (rise, fall) = (first * 100, second * 100);
            // Compare exact integer numerators over the common denominator
            // rise * fall, independently of the event sweep and enclosures.
            let expected = (query..=410).find(|&tick| {
                let first = if tick <= rise { a * tick * fall } else { a * (rise + fall).saturating_sub(tick) * rise };
                let second = b * tick.min(fall) * rise;
                first + second > cost * rise * fall
            }).map(|tick| start + Duration::from_micros(tick));
            proptest::prop_assert_eq!(comparison.first_negative(start + Duration::from_micros(query), grid), expected);
        }
    }
}

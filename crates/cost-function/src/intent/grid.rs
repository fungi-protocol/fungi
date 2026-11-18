use std::time::{Duration, Instant};

/// The wallet-local time grid used when placing payoff curves and scheduling decisions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ScheduleGrid {
    origin: Instant,
}

impl ScheduleGrid {
    pub const JIFFY: Duration = Duration::from_micros(1);
    pub const AUTHORED_STEP: Duration = Duration::from_micros(100);

    pub fn new(origin: Instant) -> Self {
        Self { origin }
    }

    /// Place an authored slot on this clock, if the instant is representable.
    pub fn at_slot(self, slot: u64) -> Option<Instant> {
        self.origin.checked_add(Self::slot_duration(slot))
    }

    pub(super) fn slot_duration(slots: u64) -> Duration {
        let nanos = u128::from(slots) * Self::AUTHORED_STEP.as_nanos();
        // An authored slot is shorter than one second, so the seconds fit u64.
        Duration::new(
            (nanos / 1_000_000_000) as u64,
            (nanos % 1_000_000_000) as u32,
        )
    }

    /// The first decision jiffy at or after `at`, starting at the grid origin.
    pub(super) fn ceil(self, at: Instant) -> Option<Instant> {
        let at = at.max(self.origin);
        let nanos = (at - self.origin).subsec_nanos();
        let padding = nanos.next_multiple_of(Self::JIFFY.subsec_nanos()) - nanos;
        at.checked_add(Duration::from_nanos(padding.into()))
    }
}

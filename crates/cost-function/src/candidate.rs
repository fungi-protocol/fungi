//! One funded outcome and the local intents it realizes.

pub use crate::intent::schedule::{CostBounds, CostSign, OrderChange, OrderChangeTime};
use crate::intent::{ScheduleGrid, schedule::Comparison};
use crate::{
    action::UnSequencedActions, change::OutputWithoutScriptpubkey, intent::PayoffCurve,
    wallet::Utxo,
};
use bitcoin::{SignedAmount, TxOut};
use std::{collections::BTreeMap, time::Instant};

/// Candidate generation is pure: change has prices, but no allocated scripts.
#[derive(Debug, PartialEq, Eq)]
pub struct Candidate<I> {
    pub(crate) actions: UnSequencedActions,
    pub(crate) payments: BTreeMap<I, TxOut>,
    pub(crate) payoffs: Vec<PayoffCurve>,
}

impl<I> Candidate<I> {
    pub fn inputs(&self) -> impl Iterator<Item = &Utxo> {
        self.actions.spent()
    }
    pub fn payments(&self) -> impl Iterator<Item = (&I, &TxOut)> {
        self.payments.iter()
    }
    pub fn change(&self) -> impl Iterator<Item = &OutputWithoutScriptpubkey> {
        self.actions.change()
    }
    pub fn objective_cost(&self) -> Option<SignedAmount> {
        self.actions.objective_cost()
    }
    /// Certified lower and upper bounds on the complete outcome's net cost.
    pub fn net_cost_at(&self, at: Instant, grid: ScheduleGrid) -> Option<CostBounds> {
        Comparison {
            cost: self.objective_cost()?,
            positive: &[],
            negative: &self.payoffs,
        }
        .bounds_at(at, grid)
    }

    /// Earliest actionable microsecond. This is a cost threshold, not a
    /// commitment to dispatch: strategy may add timing or disclosure policy.
    pub fn ready_at(&self, now: Instant, grid: ScheduleGrid) -> Option<Instant> {
        Comparison {
            cost: self.objective_cost()?,
            positive: &[],
            negative: &self.payoffs,
        }
        .first_negative(now, grid)
    }

    /// Readiness of one complete outcome containing disjoint transaction batches.
    /// The caller supplies the allocation; this does not search for a partition.
    pub fn joint_ready_at<'a>(
        candidates: impl IntoIterator<Item = &'a Self>,
        now: Instant,
        grid: ScheduleGrid,
    ) -> Option<Instant>
    where
        I: 'a,
    {
        let mut cost = SignedAmount::ZERO;
        let mut payoffs = Vec::new();
        for candidate in candidates {
            cost = cost.checked_add(candidate.objective_cost()?)?;
            payoffs.extend(candidate.payoffs.iter().cloned());
        }
        Comparison {
            cost,
            positive: &[],
            negative: &payoffs,
        }
        .first_negative(now, grid)
    }

    /// The next change from the current ordering of this outcome's net cost
    /// against the other's. An existing preference alone yields no event.
    pub fn next_order_change<J>(
        &self,
        other: &Candidate<J>,
        now: Instant,
        grid: ScheduleGrid,
    ) -> Option<OrderChange> {
        Comparison {
            cost: self
                .objective_cost()?
                .checked_sub(other.objective_cost()?)?,
            positive: &other.payoffs,
            negative: &self.payoffs,
        }
        .next_order_change(now, grid)
    }
}

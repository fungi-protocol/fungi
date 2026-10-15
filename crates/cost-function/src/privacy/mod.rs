use bitcoin::{Amount, SignedAmount, hashes::Hash};

use crate::simulate::WalletStateDiff;

/// Defines interfaces for privacy metrics.

/// Convert a metric output into a `SignedAmount` relative to a privacy budget.
pub(crate) trait IntoCost {
    fn into_cost(&self, budget: Amount) -> SignedAmount;
}

/// Object-safe erased form of a privacy metric, suitable for storage as a trait object.
pub(crate) trait ErasedPrivacyMetric<Id: Eq + Hash>: std::fmt::Debug {
    fn evaluate_erased(&self, plan: &WalletStateDiff<Id>) -> Box<dyn IntoCost>;
    fn clone_box(&self) -> Box<dyn ErasedPrivacyMetric<Id>>;
}

impl<Id: Eq + Hash + Clone> Clone for Box<dyn ErasedPrivacyMetric<Id>> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[allow(unused)]
pub(crate) trait PrivacyMetric<Id: Eq + Hash>: ErasedPrivacyMetric<Id> {
    type Output: IntoCost + std::fmt::Debug;
    fn evaluate(&self, plan: WalletStateDiff<Id>) -> Self::Output;
}

/// A collection of privacy metrics evaluated together against a shared budget.
#[derive(Debug, Clone)]
pub(crate) struct PrivacyBundle<Id: Eq + Hash + Clone> {
    pub(crate) metrics: Vec<Box<dyn ErasedPrivacyMetric<Id>>>,
    pub(crate) budget: Amount,
}

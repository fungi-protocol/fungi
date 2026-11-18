//! One funded outcome and the local intents it realizes.

use crate::{
    action::UnSequencedActions, change::OutputWithoutScriptpubkey, intent::PayoffCurve,
    wallet::Utxo,
};
use bitcoin::{SignedAmount, TxOut};
use std::collections::BTreeMap;

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
}

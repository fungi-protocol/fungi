//! Steps the wallet takes to construct a transaction.

use std::time::Instant;

use bitcoin::{TxIn, TxOut};

pub(crate) mod poisson;

/// One step in constructing a transaction.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Action {
    /// Add an input to the transaction.
    RegisterInput(TxIn),
    /// Add an output to the transaction.
    RegisterOutput(TxOut),
}

/// Actions without a known time of execution
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct UnSequencedActions(pub(crate) Vec<Action>);

/// Actions that have a sequence
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct SequencedActions(pub(crate) Vec<(Action, Instant)>);

pub(crate) trait IntoSequencedActions {
    fn into_sequenced_actions(self) -> SequencedActions;
}

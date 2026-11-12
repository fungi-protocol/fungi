//! Steps the wallet takes to construct a transaction.

use bitcoin::{TxIn, TxOut};

use crate::change::OutputWithoutScriptpubkey;

/// One step in constructing a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Add an input to the transaction.
    RegisterInput(TxIn),
    /// Add an output to the transaction.
    RegisterOutput(TxOut),
    /// Add an change output,
    RegisterChangeOutPut(OutputWithoutScriptpubkey),
}

/// Actions without a known time of execution
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct UnSequencedActions(pub(crate) Vec<Action>);

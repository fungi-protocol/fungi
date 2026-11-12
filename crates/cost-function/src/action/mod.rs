//! Steps the wallet takes to construct a transaction.

use bitcoin::TxOut;

use crate::change::OutputWithoutScriptpubkey;
use crate::wallet::Utxo;

/// One step in constructing a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Spend a coin the wallet holds.
    RegisterInput(Utxo),
    /// Add an output to the transaction.
    RegisterOutput(TxOut),
    /// Add an change output,
    RegisterChangeOutPut(OutputWithoutScriptpubkey),
}

/// Actions without a known time of execution
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct UnSequencedActions(pub(crate) Vec<Action>);

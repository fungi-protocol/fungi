//! Steps the wallet takes to construct a transaction.

use bitcoin::{OutPoint, TxOut};

use crate::change::OutputWithoutScriptpubkey;
use crate::wallet::Utxo;

/// One step in constructing a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Spend a coin the wallet holds.
    Input(Utxo),
    /// Add an output to the transaction.
    Output(TxOut),
    /// Add an output that pays back to the wallet.
    SelfSpendOutPut(OutputWithoutScriptpubkey),
    /// Sign the input that spends this coin.
    #[allow(dead_code, reason = "nothing constructs a sign action yet")]
    Sign(OutPoint),
    /// Broadcast the signed transaction.
    #[allow(dead_code, reason = "nothing constructs a broadcast action yet")]
    Broadcast,
}

/// Actions without a known time of execution
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct UnSequencedActions(pub(crate) Vec<Action>);

//! Capabilities a wallet supplies to the driver.
//!
//! Initiation returns an owned IO future. Adapters copy the required request
//! data or clone their channel handles, so another request can be initiated
//! before the first finishes. Completion is reported through the future.

use std::collections::BTreeMap;
use std::error::Error;

use bitcoin::{OutPoint, Psbt, ScriptBuf, Transaction, TxOut, Txid, Weight, psbt};
use cost_function::wallet::{Utxo, UtxoSnapshot};

pub mod dummy;

/// The prevout and estimated cost of satisfying it, without signing keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Coin {
    pub txout: TxOut,
    pub satisfaction_weight: Weight,
}

/// An owned observation, unaffected by subsequent wallet operations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Snapshot {
    pub coins: BTreeMap<OutPoint, Coin>,
    /// Recorded signed transactions, independently of publication and chain status.
    /// Recording supplies information for replacement decisions, not input reservations.
    pub recorded: BTreeMap<Txid, Transaction>,
}

impl UtxoSnapshot for Snapshot {
    fn iter(&self) -> impl Iterator<Item = Utxo> {
        self.coins.iter().map(|(&outpoint, coin)| {
            Utxo::new(outpoint, coin.txout.value, coin.satisfaction_weight)
        })
    }
}

pub trait SnapshotSource: Send {
    type Error: Error + Send + Sync + 'static;

    /// Observe available coins without reserving them.
    fn snapshot(
        &mut self,
    ) -> impl Future<Output = Result<Snapshot, Self::Error>> + Send + use<Self>;
}

/// Assigning a script is an effect; pricing change need not query the wallet.
pub trait ChangeSource: Send {
    type Error: Error + Send + Sync + 'static;

    fn next_script(
        &mut self,
    ) -> impl Future<Output = Result<ScriptBuf, Self::Error>> + Send + use<Self>;
}

/// Signing access for one coin, even when other coins use different wallets.
pub trait InputSigner: Send {
    type Error: Error + Send + Sync + 'static;

    fn outpoint(&self) -> OutPoint;

    /// Return PSBT input data for this signer's outpoint, which must occur exactly once.
    ///
    /// The complete PSBT supplies the transaction and prevout context needed for signing.
    /// A backend may process multiple inputs internally; the adapter returns only this
    /// input's result. The caller validates and applies it to the unchanged transaction.
    fn sign(
        &mut self,
        psbt: &Psbt,
    ) -> impl Future<Output = Result<psbt::Input, Self::Error>> + Send + use<Self>;
}

pub trait TransactionRecorder: Send {
    type Error: Error + Send + Sync + 'static;

    /// Retain a transaction without broadcasting it or asserting chain inclusion.
    /// Repeated successful recording of the same transaction must be harmless.
    fn record(
        &mut self,
        tx: &Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + use<Self>;
}

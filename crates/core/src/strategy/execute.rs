//! Wallet capabilities shared by transaction-specific executions.

use super::{
    Action, Error, ExecutionId, Request, Update,
    construction::{Effect, Execution},
    error,
};
use crate::wallet::{ChangeSource, InputSigner};
use bitcoin::OutPoint;
use futures::lock::Mutex;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

type Executions = BTreeMap<ExecutionId, Arc<Mutex<Execution>>>;

/// Per-coin signer capabilities shared by the wallet adapter and its executors.
pub struct Signers<S>(std::sync::Mutex<BTreeMap<OutPoint, Arc<Mutex<S>>>>);

impl<S: InputSigner> Signers<S> {
    /// Register a signer before reporting its coin in a wallet snapshot.
    /// Returns whether a signer was inserted. An existing signer is kept.
    pub fn register(&self, signer: S) -> bool {
        self.try_register(signer).is_ok()
    }

    fn try_register(&self, signer: S) -> Result<(), OutPoint> {
        let outpoint = signer.outpoint();
        match self.0.lock().unwrap().entry(outpoint) {
            Entry::Vacant(entry) => {
                entry.insert(Arc::new(Mutex::new(signer)));
                Ok(())
            }
            Entry::Occupied(_) => Err(outpoint),
        }
    }

    /// Return the coin's signer without holding the registry lock during signing.
    pub fn get(&self, outpoint: &OutPoint) -> Option<Arc<Mutex<S>>> {
        self.0.lock().unwrap().get(outpoint).cloned()
    }
}

/// Capabilities are independent of candidates; a Start request binds an execution.
pub struct Executor<C, S> {
    pub change: Arc<Mutex<C>>,
    pub signers: Arc<Signers<S>>,
    transactions: Arc<std::sync::Mutex<Executions>>,
}

impl<C: ChangeSource, S: InputSigner> Executor<C, S> {
    pub fn new(change: C, signers: Vec<S>) -> Result<Self, Error> {
        let registered = Arc::new(Signers(std::sync::Mutex::default()));
        for signer in signers {
            registered
                .try_register(signer)
                .map_err(|outpoint| error(format!("duplicate signer for {outpoint}")))?;
        }
        Ok(Self {
            change: Arc::new(Mutex::new(change)),
            signers: registered,
            transactions: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        })
    }

    pub async fn execute(&self, request: &Request) -> Result<Update, Error> {
        let execution = {
            let mut transactions = self.transactions.lock().unwrap();
            match transactions.entry(request.plan.execution_id) {
                Entry::Vacant(entry) => {
                    if request.action != Action::Start {
                        return Err(error("transaction construction has not started"));
                    }
                    entry
                        .insert(Arc::new(Mutex::new(Execution::new(request.plan.clone())?)))
                        .clone()
                }
                Entry::Occupied(entry) => entry.get().clone(),
            }
        };
        let operation = {
            let mut state = execution.lock().await;
            if !Arc::ptr_eq(&state.plan, &request.plan) {
                return Err(error("execution belongs to a different candidate"));
            }
            state
                .locks
                .entry(request.action.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        // Replays of this operation share its receipt; independent operations
        // release execution/capability state before awaiting external IO.
        let _operation = operation.lock().await;
        let effect = execution.lock().await.prepare(&request.action)?;
        match effect {
            Effect::Complete(update) => Ok(update),
            Effect::AllocateChange { index, priced } => {
                let io = self.change.lock().await.next_script();
                let script = io.await.map_err(error)?;
                execution
                    .lock()
                    .await
                    .complete_change(index, &priced, script)
            }
            Effect::Sign {
                index,
                position,
                outpoint,
                psbt,
            } => {
                let signer = self
                    .signers
                    .get(&outpoint)
                    .ok_or_else(|| error("no signer for input"))?;
                let io = signer.lock().await.sign(&psbt);
                let signed = io.await.map_err(error)?;
                execution
                    .lock()
                    .await
                    .complete_sign(index, position, signed)
            }
        }
    }
}

impl<C, S> Clone for Executor<C, S> {
    fn clone(&self) -> Self {
        Self {
            change: self.change.clone(),
            signers: self.signers.clone(),
            transactions: self.transactions.clone(),
        }
    }
}

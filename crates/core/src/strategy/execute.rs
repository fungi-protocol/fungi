//! Wallet capabilities shared by transaction-specific executions.

use super::{
    Action, Error, ExecutionId, Request, Update,
    construction::{Effect, Execution},
    error,
};
use crate::wallet::ChangeSource;
use futures::lock::Mutex;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

type Executions = BTreeMap<ExecutionId, Arc<Mutex<Execution>>>;

/// Capabilities are independent of candidates; a Start request binds an execution.
pub struct Executor<C> {
    pub change: Arc<Mutex<C>>,
    transactions: Arc<std::sync::Mutex<Executions>>,
}

impl<C: ChangeSource> Executor<C> {
    pub fn new(change: C) -> Self {
        Self {
            change: Arc::new(Mutex::new(change)),
            transactions: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        }
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
        }
    }
}

impl<C> Clone for Executor<C> {
    fn clone(&self) -> Self {
        Self {
            change: self.change.clone(),
            transactions: self.transactions.clone(),
        }
    }
}

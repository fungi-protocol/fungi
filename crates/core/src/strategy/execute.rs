//! Wallet capabilities shared by transaction-specific executions.

use super::{Action, Error, ExecutionId, Request, Update, construction::Execution, error};
use futures::lock::Mutex;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

type Executions = BTreeMap<ExecutionId, Arc<Mutex<Execution>>>;

/// Capabilities are independent of candidates; a Start request binds an execution.
#[derive(Clone, Default)]
pub struct Executor {
    transactions: Arc<std::sync::Mutex<Executions>>,
}

impl Executor {
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
        let mut state = execution.lock().await;
        if !Arc::ptr_eq(&state.plan, &request.plan) {
            return Err(error("execution belongs to a different candidate"));
        }
        state.execute(&request.action)
    }
}

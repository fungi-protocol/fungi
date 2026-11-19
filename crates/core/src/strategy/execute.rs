//! Wallet capabilities shared by transaction-specific executions.

use super::{
    Action, Error, ExecutionId, Operation, Plan, Update,
    construction::Effect,
    error,
    session::{Message, NoPeer},
};
use crate::wallet::{ChangeSource, InputSigner, TransactionRecorder};
use bitcoin::{OutPoint, Transaction};
use fungi_transport::SendChannel;
use futures::lock::Mutex;
use std::{
    collections::{BTreeMap, btree_map::Entry},
    sync::Arc,
};

pub trait Broadcaster {
    type Error: std::fmt::Display;
    fn broadcast(
        &mut self,
        transaction: &Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + use<Self>;
}

type Plans = BTreeMap<ExecutionId, Arc<Plan>>;

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

/// Capabilities are independent of candidates; a Start operation binds an execution
/// identity to its plan.
pub struct Executor<C, S, R, B, P = NoPeer> {
    pub change: Arc<Mutex<C>>,
    pub signers: Arc<Signers<S>>,
    pub recorder: Arc<Mutex<R>>,
    pub broadcaster: Arc<Mutex<B>>,
    pub peer: Arc<Mutex<P>>,
    plans: Arc<std::sync::Mutex<Plans>>,
}

impl<C: ChangeSource, S: InputSigner, R: TransactionRecorder, B: Broadcaster> Executor<C, S, R, B> {
    pub fn new(change: C, signers: Vec<S>, recorder: R, broadcaster: B) -> Result<Self, Error> {
        let registered = Arc::new(Signers(std::sync::Mutex::default()));
        for signer in signers {
            registered
                .try_register(signer)
                .map_err(|outpoint| error(format!("duplicate signer for {outpoint}")))?;
        }
        Ok(Self {
            change: Arc::new(Mutex::new(change)),
            signers: registered,
            recorder: Arc::new(Mutex::new(recorder)),
            broadcaster: Arc::new(Mutex::new(broadcaster)),
            peer: Arc::new(Mutex::new(NoPeer)),
            plans: Arc::new(std::sync::Mutex::new(BTreeMap::new())),
        })
    }
}

impl<C, S, R, B, P> Executor<C, S, R, B, P> {
    /// Attach a peer message transport for collaborative session sends.
    pub fn with_peer<Q: SendChannel<Message>>(self, peer: Q) -> Executor<C, S, R, B, Q> {
        Executor {
            change: self.change,
            signers: self.signers,
            recorder: self.recorder,
            broadcaster: self.broadcaster,
            peer: Arc::new(Mutex::new(peer)),
            plans: self.plans,
        }
    }
}

impl<
    C: ChangeSource,
    S: InputSigner,
    R: TransactionRecorder,
    B: Broadcaster,
    P: SendChannel<Message>,
> Executor<C, S, R, B, P>
{
    /// Perform the operation's IO. Clones of an operation share its receipt,
    /// so the IO runs at most once.
    pub async fn execute(&self, operation: &Operation) -> Result<Update, Error> {
        let mut receipt = operation.receipt.lock().await;
        if let Some(result) = &*receipt {
            return result.clone();
        }
        let result = self.perform(operation).await;
        *receipt = Some(result.clone());
        result
    }

    async fn perform(&self, operation: &Operation) -> Result<Update, Error> {
        self.bind(operation)?;
        match operation.effect.as_ref() {
            Effect::Local => Ok(Update::Applied),
            Effect::Send(message) => {
                let mut peer = self.peer.lock().await;
                peer.send(message.clone()).await.map_err(error)?;
                Ok(Update::Applied)
            }
            Effect::AllocateChange => {
                let io = self.change.lock().await.next_script();
                Ok(Update::Change(io.await.map_err(error)?))
            }
            Effect::Sign { outpoint, psbt } => {
                let signer = self
                    .signers
                    .get(outpoint)
                    .ok_or_else(|| error("no signer for input"))?;
                let io = signer.lock().await.sign(psbt);
                Ok(Update::Signed(Box::new(io.await.map_err(error)?)))
            }
            Effect::Record(transaction) => {
                let io = self.recorder.lock().await.record(transaction);
                io.await.map_err(error)?;
                Ok(Update::Applied)
            }
            Effect::Publish(transaction) => {
                let io = self.broadcaster.lock().await.broadcast(transaction);
                io.await.map_err(error)?;
                Ok(Update::Applied)
            }
        }
    }

    /// Start binds the execution identity to its plan. Every other operation
    /// must belong to the bound plan.
    fn bind(&self, operation: &Operation) -> Result<(), Error> {
        match self
            .plans
            .lock()
            .unwrap()
            .entry(operation.plan.execution_id)
        {
            Entry::Vacant(entry) if operation.action == Action::Start => {
                entry.insert(operation.plan.clone());
                Ok(())
            }
            Entry::Vacant(_) => Err(error("transaction construction has not started")),
            Entry::Occupied(entry) if Arc::ptr_eq(entry.get(), &operation.plan) => Ok(()),
            Entry::Occupied(_) => Err(error("execution belongs to a different candidate")),
        }
    }
}

impl<C, S, R, B, P> Clone for Executor<C, S, R, B, P> {
    fn clone(&self) -> Self {
        Self {
            change: self.change.clone(),
            signers: self.signers.clone(),
            recorder: self.recorder.clone(),
            broadcaster: self.broadcaster.clone(),
            peer: self.peer.clone(),
            plans: self.plans.clone(),
        }
    }
}

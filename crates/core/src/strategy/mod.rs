//! Wallet strategy and execution state.
//!
//! The current tactic funds the intents outside started outcomes as one outcome of
//! disjoint transaction batches, and funds them again on new information. An outcome
//! has started once any of its transactions has started.

use crate::{
    driver::{Decision, Machine},
    wallet::Snapshot,
};
use bitcoin::{
    Amount, FeeRate, OutPoint, Txid,
    hashes::{Hash, sha256},
};
use concurrent_psbt::output::UniqueId;
use cost_function::{
    candidate::Candidate,
    change::ChangeCosts,
    intent::{Intent, ScheduleGrid},
    queue::{InMemoryQueue, Queue},
    wallet::{FundingError, Utxo, UtxoSnapshot, WalletStateSnapshot},
};
use rand::RngCore;
use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    num::NonZeroUsize,
    sync::Arc,
    time::Instant,
};

mod construction;
mod execute;
pub mod session;
pub use execute::{Broadcaster, Executor, Signers};

/// Local execution identity, independent of the shared construction-session UUID.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExecutionId {
    pub wallet: [u8; 16],
    pub sequence: u64,
}

struct Plan {
    execution_id: ExecutionId,
    candidate: Candidate<u64>,
    snapshot: Snapshot,
    feerate: FeeRate,
    /// Collaborative construction-session configuration, or `None` when unilateral.
    session: Option<session::Config>,
    /// PSBTv2 `PSBT_GLOBAL_TX_UUID`: shared across session peers, or derived locally when unilateral.
    session_id: [u8; 16],
    /// Opaque contribution identities for local payment and change outputs, stable across retries.
    output_ids: Vec<UniqueId>,
    /// Opaque contribution identity for this wallet's explicit fee contribution in a session.
    fee_id: UniqueId,
    /// Wallet-private mapping from payment output contribution identity bytes to queue intent ID.
    payment_outputs: BTreeMap<Vec<u8>, u64>,
}

impl Plan {
    fn new<R: RngCore>(
        execution_id: ExecutionId,
        candidate: Candidate<u64>,
        snapshot: Snapshot,
        feerate: FeeRate,
        session: Option<session::Config>,
        session_id: [u8; 16],
        rng: &mut R,
    ) -> Self {
        let mut output_ids = Vec::new();
        let mut payment_outputs = BTreeMap::new();
        for (&intent_id, _) in candidate.payments() {
            let id = random_output_id(rng, &output_ids);
            payment_outputs.insert(id.as_bytes().to_vec(), intent_id);
            output_ids.push(id);
        }
        for _ in candidate.change() {
            let id = random_output_id(rng, &output_ids);
            output_ids.push(id);
        }
        let fee_id = random_output_id(rng, &output_ids);
        Self {
            execution_id,
            candidate,
            snapshot,
            feerate,
            session,
            session_id,
            output_ids,
            fee_id,
            payment_outputs,
        }
    }

    fn local_fee(&self) -> Amount {
        let inputs = self
            .candidate
            .inputs()
            .fold(Amount::ZERO, |sum, coin| sum + coin.value());
        let payments = self
            .candidate
            .payments()
            .fold(Amount::ZERO, |sum, (_, out)| sum + out.value);
        let change = self
            .candidate
            .change()
            .fold(Amount::ZERO, |sum, out| sum + out.value);
        inputs - payments - change
    }
}

fn random_output_id<R: RngCore>(rng: &mut R, existing: &[UniqueId]) -> UniqueId {
    loop {
        let mut bytes = [0u8; 16];
        rng.fill_bytes(&mut bytes);
        let id = UniqueId::try_from(bytes.to_vec()).expect("16-byte output identity is non-empty");
        if !existing.contains(&id) {
            return id;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{SeedableRng, rngs::StdRng};

    #[test]
    fn output_identities_retry_on_collision() {
        let first = random_output_id(&mut StdRng::seed_from_u64(0), &[]);
        let second = random_output_id(&mut StdRng::seed_from_u64(0), std::slice::from_ref(&first));
        assert_ne!(second.as_bytes(), first.as_bytes());
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Action {
    Start,
    Input(usize),
    SendInput(usize),
    SendInputsReady(usize),
    CloseInputs,
    AllocateChange(usize),
    Output(usize),
    SendOutput(usize),
    SendFee,
    SendOutputsReady(usize),
    CloseOutputs,
    Sign(usize),
    SendWitness(usize),
    Finalize,
    Record,
    Publish,
}

/// An action bound to the candidate evaluated by the decision process.
#[derive(Clone)]
pub struct Request {
    plan: Arc<Plan>,
    pub action: Action,
}

pub type OperationId = (ExecutionId, Action);

impl Request {
    pub fn id(&self) -> OperationId {
        (self.plan.execution_id, self.action.clone())
    }
}

/// A dispatched action and the IO it requires.
///
/// The effect is fixed at dispatch. Clones share one receipt, so the IO runs
/// at most once and every clone completes with the same result.
#[derive(Clone)]
pub struct Operation {
    plan: Arc<Plan>,
    action: Action,
    effect: Arc<construction::Effect>,
    receipt: Arc<futures::lock::Mutex<Option<Result<Update, Error>>>>,
}

impl Operation {
    pub fn id(&self) -> OperationId {
        (self.plan.execution_id, self.action.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Error(pub String);

fn error(value: impl std::fmt::Display) -> Error {
    Error(value.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Payment {
    pub transaction: bitcoin::Transaction,
    pub outputs: BTreeMap<u64, usize>,
    pub fee: Amount,
}

/// The result of an operation's IO. The wallet state applies it on completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Update {
    /// The operation required no IO, or its IO returned no value.
    Applied,
    /// The change source returned this script.
    Change(bitcoin::ScriptBuf),
    /// The signer returned this input.
    Signed(Box<bitcoin::psbt::Input>),
}

#[derive(Clone)]
pub enum Observation {
    Confirmed(Txid),
    /// Stop this candidate when new information invalidates its assumptions.
    Invalidated(String),
    TransactionInvalidated(ExecutionId, String),
    /// The user adds an intent. It joins every intent outside started outcomes.
    Intent(Intent),
    /// The wallet reports its coins. Recorded transactions are kept.
    Snapshot(Snapshot),
    FeeRate(FeeRate),
    /// Explicitly enter a collaborative construction session.
    Session(session::Config),
    /// Receive a flooded peer message from a session transport channel.
    Peer(session::Message),
    /// Apply an externally synchronized session progress view.
    SessionProgress {
        session_id: [u8; 16],
        progress: session::Progress,
    },
    /// Observe a signed transaction broadcast on the Bitcoin network.
    Broadcast(bitcoin::Transaction),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Status {
    Running,
    AwaitingConfirmation,
    Confirmed,
    Failed(Error),
}

mod path;
mod signing;
mod transaction;
mod validate;
pub use transaction::Transaction;

struct SessionEntry {
    config: session::Config,
    tracker: session::Tracker,
    external_progress: Option<session::Progress>,
    bound_execution: Option<ExecutionId>,
}

impl SessionEntry {
    fn current_progress(&self) -> session::Progress {
        self.external_progress
            .clone()
            .unwrap_or_else(|| self.tracker.progress())
    }
}

/// State holds wallet observations, the outcome for unstarted intents and transaction executions.
pub struct State<C, R> {
    pending: InMemoryQueue,
    snapshot: Snapshot,
    feerate: FeeRate,
    wallet_id: [u8; 16],
    change: C,
    rng: R,
    grid: ScheduleGrid,
    /// Each batch holds intents outside started outcomes. Batches are disjoint.
    batches: Vec<BTreeSet<u64>>,
    explicit_batches: bool,
    /// Each outcome holds the transactions funded by one evaluation.
    outcomes: Vec<BTreeSet<ExecutionId>>,
    sessions: BTreeMap<[u8; 16], SessionEntry>,
    orphaned_messages: BTreeMap<[u8; 16], Vec<session::Message>>,
    /// This is set when information relevant to funding the unstarted batches arrives.
    stale: bool,
    evaluated: bool,
    next_sequence: u64,
    transactions: BTreeMap<ExecutionId, Transaction>,
    planning_errors: BTreeMap<ExecutionId, Error>,
    warnings: Vec<String>,
}

impl<C: ChangeCosts, R: RngCore> State<C, R> {
    /// The schedule grid places all authored payoff slots for this wallet.
    pub fn new(
        pending: InMemoryQueue,
        snapshot: Snapshot,
        feerate: FeeRate,
        wallet_id: [u8; 16],
        change_costs: C,
        rng: R,
        grid: ScheduleGrid,
    ) -> Self {
        let batches = vec![pending.iter().map(|(id, _)| id).collect()];
        Self {
            pending,
            snapshot,
            feerate,
            wallet_id,
            change: change_costs,
            rng,
            grid,
            batches,
            explicit_batches: false,
            outcomes: Vec::new(),
            sessions: BTreeMap::new(),
            orphaned_messages: BTreeMap::new(),
            stale: true,
            evaluated: false,
            next_sequence: 0,
            transactions: BTreeMap::new(),
            planning_errors: BTreeMap::new(),
            warnings: Vec::new(),
        }
    }

    /// Choose disjoint initial batches. The default is one batch.
    /// This configures the PoC tactic; it neither reserves coins nor removes intents.
    /// An intent left out of every batch is not funded by the first evaluation.
    /// An added intent replaces the batches with one batch of every intent outside
    /// started outcomes. A new snapshot or feerate keeps the batches.
    pub fn with_batches(mut self, batches: Vec<Vec<u64>>) -> Result<Self, Error> {
        if self.evaluated {
            return Err(error("initial allocation has already been evaluated"));
        }
        let known: BTreeSet<_> = self.pending.iter().map(|(id, _)| id).collect();
        let mut allocation = Vec::new();
        let mut assigned = BTreeSet::new();
        for batch in batches {
            let ids: BTreeSet<_> = batch.iter().copied().collect();
            if ids.is_empty()
                || ids.len() != batch.len()
                || !ids.is_subset(&known)
                || !ids.is_disjoint(&assigned)
            {
                return Err(error(
                    "batches must contain distinct, known intent identities",
                ));
            }
            assigned.extend(&ids);
            allocation.push(ids);
        }
        self.batches = allocation;
        self.explicit_batches = true;
        Ok(self)
    }

    /// Explicitly enter a collaborative construction session before initial evaluation.
    pub fn with_session(mut self, config: session::Config) -> Result<Self, Error> {
        if self.evaluated {
            return Err(error("initial allocation has already been evaluated"));
        }
        if self.sessions.contains_key(&config.id) {
            return Err(error("duplicate session identity"));
        }
        let mut tracker = session::Tracker::new(config.id);
        tracker.set_additional_entropy(config.additional_entropy);
        self.sessions.insert(
            config.id,
            SessionEntry {
                config,
                tracker,
                external_progress: None,
                bound_execution: None,
            },
        );
        Ok(self)
    }

    pub fn session_progress(&self, session_id: &[u8; 16]) -> Option<session::Progress> {
        self.sessions
            .get(session_id)
            .map(SessionEntry::current_progress)
    }

    pub fn pending(&self) -> &InMemoryQueue {
        &self.pending
    }
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
    pub fn transactions(&self) -> &BTreeMap<ExecutionId, Transaction> {
        &self.transactions
    }
    pub fn planning_errors(&self) -> &BTreeMap<ExecutionId, Error> {
        &self.planning_errors
    }
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    fn requeue_excluded(&mut self, exec_id: ExecutionId) {
        if self.explicit_batches {
            let already_batched: BTreeSet<_> = self.batches.iter().flatten().copied().collect();
            let ids: BTreeSet<_> = self.transactions[&exec_id]
                .candidate()
                .payments()
                .map(|(&id, _)| id)
                .filter(|id| !already_batched.contains(id))
                .collect();
            if !ids.is_empty() {
                self.batches.push(ids);
            }
        } else {
            self.batches = vec![self.pending.iter().map(|(id, _)| id).collect()];
        }
        self.stale = true;
    }

    fn refresh_session(&mut self, session_id: [u8; 16]) {
        let entry = &self.sessions[&session_id];
        let progress = entry.current_progress();
        if let Some(exec_id) = entry.bound_execution {
            let tx = self
                .transactions
                .get_mut(&exec_id)
                .expect("bound session execution exists");
            let was_excluded = tx.excluded();
            if entry.external_progress.is_some() {
                tx.set_progress(progress);
            } else {
                tx.sync_tracker(&entry.tracker);
            }
            if !was_excluded && tx.excluded() {
                self.requeue_excluded(exec_id);
            }
        } else if progress.open != Some(session::Phase::Inputs) {
            self.stale = true;
        }
    }

    /// Fund each batch again, replacing the outcomes that have not started.
    fn evaluate(&mut self) {
        // Clock ticks and unrelated completions must not redraw coins or identities.
        self.stale = false;
        self.evaluated = true;
        let transactions = &self.transactions;
        self.outcomes
            .retain(|outcome| outcome.iter().any(|id| transactions[id].started()));
        let started: BTreeSet<_> = self.outcomes.iter().flatten().copied().collect();
        self.transactions.retain(|id, _| started.contains(id));
        for entry in self.sessions.values_mut() {
            if entry
                .bound_execution
                .is_some_and(|id| !self.transactions.contains_key(&id))
            {
                entry.bound_execution = None;
            }
        }
        self.planning_errors.clear();
        self.warnings.clear();
        // Started outcomes keep their intents unless excluded by a closed session phase.
        // Active outcomes reserve their inputs; failed outcomes that already signed attach
        // an RBF replacement liability to their coins instead of locking or freeing them.
        let mut available = self.snapshot.clone();
        let mut liabilities = BTreeMap::<OutPoint, Amount>::new();
        let mut bound = BTreeSet::new();
        for outcome in &self.outcomes {
            let outcome: Vec<_> = outcome.iter().map(|id| &self.transactions[id]).collect();
            let failed = outcome
                .iter()
                .any(|tx| matches!(tx.status(), Status::Failed(_)));
            for tx in outcome {
                if !tx.excluded() {
                    bound.extend(tx.candidate().payments().map(|(&id, _)| id));
                }
                if !failed {
                    for coin in tx.candidate().inputs() {
                        available.coins.remove(&coin.outpoint());
                    }
                } else if let Some(liability) = tx.replacement_liability() {
                    for coin in tx.candidate().inputs() {
                        let entry = liabilities.entry(coin.outpoint()).or_insert(Amount::ZERO);
                        *entry = (*entry).max(liability);
                    }
                }
            }
        }
        let known: BTreeSet<_> = self.pending.iter().map(|(id, _)| id).collect();
        for batch in &mut self.batches {
            batch.retain(|id| known.contains(id) && !bound.contains(id));
        }
        self.batches.retain(|batch| !batch.is_empty());
        let mut open_sessions: Vec<_> = self
            .sessions
            .values()
            .filter(|entry| {
                entry.bound_execution.is_none()
                    && entry.current_progress().open == Some(session::Phase::Inputs)
            })
            .map(|entry| entry.config)
            .collect();
        open_sessions.reverse();
        // Allocation within this tactic is disjoint; wallet observations remain intact.
        let mut transactions = BTreeMap::new();
        let mut session_bindings = Vec::new();
        for ids in &self.batches {
            let execution_id = ExecutionId {
                wallet: self.wallet_id,
                sequence: self.next_sequence,
            };
            // Identities are not reused, even for a candidate that never started.
            self.next_sequence += 1;
            let session_config = open_sessions.pop();
            let participants =
                session_config.map_or(NonZeroUsize::MIN, |config| config.participants());
            let priced = PricedSnapshot {
                snapshot: &available,
                liabilities: &liabilities,
            };
            let observed = WalletStateSnapshot::new(
                priced,
                Batch {
                    queue: &self.pending,
                    ids,
                },
            );
            let funded = match observed.funded_candidate_for_session(
                self.feerate,
                participants,
                &mut self.rng,
                &self.change,
            ) {
                Ok(candidate) => Ok(candidate.expect("a batch is not empty")),
                Err(FundingError::NoCombinationCovers) => {
                    self.warnings
                        .push("payment queue exceeds available funding".into());
                    if !self.explicit_batches && ids.len() > 1 {
                        prioritize_subset(
                            &available,
                            &liabilities,
                            &self.pending,
                            ids,
                            self.feerate,
                            participants,
                            &mut self.rng,
                            &self.change,
                        )
                        .ok_or(FundingError::NoCombinationCovers)
                    } else {
                        Err(FundingError::NoCombinationCovers)
                    }
                }
                Err(err) => Err(err),
            };
            match funded {
                Ok(candidate) => {
                    for coin in candidate.inputs() {
                        available.coins.remove(&coin.outpoint());
                    }
                    let session_id = match session_config {
                        Some(config) => config.id,
                        None => {
                            let seed = [
                                self.wallet_id.as_slice(),
                                &execution_id.sequence.to_be_bytes(),
                            ]
                            .concat();
                            let mut id = [0; 16];
                            id.copy_from_slice(&sha256::Hash::hash(&seed).to_byte_array()[..16]);
                            id
                        }
                    };
                    let mut tx = Transaction::new(Arc::new(Plan::new(
                        execution_id,
                        candidate,
                        self.snapshot.clone(),
                        self.feerate,
                        session_config,
                        session_id,
                        &mut self.rng,
                    )));
                    if let Some(config) = session_config {
                        let entry = &self.sessions[&config.id];
                        if let Some(progress) = &entry.external_progress {
                            tx.set_progress(progress.clone());
                        } else {
                            tx.sync_tracker(&entry.tracker);
                        }
                        session_bindings.push((config.id, execution_id));
                    }
                    transactions.insert(execution_id, tx);
                }
                Err(err) => {
                    self.planning_errors
                        .insert(execution_id, error(format!("candidate funding: {err:?}")));
                }
            }
        }
        // A partial allocation is not the complete outcome whose value was requested.
        if self.planning_errors.is_empty() && !transactions.is_empty() {
            for (session_id, exec_id) in session_bindings {
                self.sessions.get_mut(&session_id).unwrap().bound_execution = Some(exec_id);
            }
            self.outcomes.push(transactions.keys().copied().collect());
            self.transactions.extend(transactions);
        }
    }
}

#[derive(Clone, Copy)]
struct PricedSnapshot<'a> {
    snapshot: &'a Snapshot,
    liabilities: &'a BTreeMap<OutPoint, Amount>,
}

impl UtxoSnapshot for PricedSnapshot<'_> {
    fn iter(&self) -> impl Iterator<Item = Utxo> {
        self.snapshot.coins.iter().map(|(&outpoint, coin)| {
            let liability = self
                .liabilities
                .get(&outpoint)
                .copied()
                .unwrap_or(Amount::ZERO);
            Utxo::new(outpoint, coin.txout.value, coin.satisfaction_weight)
                .with_liability(liability)
        })
    }
}

#[expect(clippy::too_many_arguments)]
fn prioritize_subset<C: ChangeCosts, R: RngCore>(
    available: &Snapshot,
    liabilities: &BTreeMap<OutPoint, Amount>,
    pending: &InMemoryQueue,
    ids: &BTreeSet<u64>,
    feerate: FeeRate,
    participants: NonZeroUsize,
    rng: &mut R,
    change: &C,
) -> Option<Candidate<u64>> {
    let mut chosen = BTreeSet::new();
    let mut funded = None;
    for &id in ids {
        let mut next = chosen.clone();
        next.insert(id);
        let trial = WalletStateSnapshot::new(
            PricedSnapshot {
                snapshot: available,
                liabilities,
            },
            Batch {
                queue: pending,
                ids: &next,
            },
        );
        if let Ok(Some(candidate)) =
            trial.funded_candidate_for_session(feerate, participants, rng, change)
        {
            chosen = next;
            funded = Some(candidate);
        }
    }
    funded
}

struct Batch<'a> {
    queue: &'a InMemoryQueue,
    ids: &'a BTreeSet<u64>,
}
impl Queue for Batch<'_> {
    type Id = u64;
    fn iter(&self) -> impl Iterator<Item = (u64, &Intent)> {
        self.queue.iter().filter(|(id, _)| self.ids.contains(id))
    }
}

impl<C: ChangeCosts, R: RngCore> Machine for State<C, R> {
    type Action = Request;
    type Operation = Operation;
    type OperationId = OperationId;
    type Input = Observation;
    type Result = Result<Update, Error>;

    fn decide(&mut self, now: Instant) -> Decision<Request> {
        if self.stale {
            self.evaluate();
        }
        // Zero estimated durations and additional action costs: all eligible
        // actions inherit their outcome's curve. A failure stops its own outcome.
        // Dispatch is a separate policy; this PoC uses the earliest crossing with
        // stable ties.
        let grid = self.grid;
        self.outcomes
            .iter()
            .map(|outcome| {
                outcome
                    .iter()
                    .map(|id| &self.transactions[id])
                    .collect::<Vec<_>>()
            })
            .filter(|outcome| {
                !outcome
                    .iter()
                    .any(|tx| matches!(tx.status(), Status::Failed(_)))
            })
            .filter_map(|outcome| {
                let at =
                    Candidate::joint_ready_at(outcome.iter().map(|tx| tx.candidate()), now, grid)?;
                Some(outcome.into_iter().map(move |tx| (at, tx)))
            })
            .flatten()
            .flat_map(|(at, tx)| {
                tx.available()
                    .map(move |action| crate::driver::Scheduled { at, action })
            })
            .min_by_key(|scheduled| (scheduled.at, scheduled.action.id()))
            .map_or(Decision::Wait, Decision::Schedule)
    }

    fn apply(&mut self, request: Request) {
        if let Some(tx) = self.transactions.get_mut(&request.plan.execution_id) {
            tx.apply(request);
        }
    }

    fn operations(&self) -> impl Iterator<Item = (OperationId, Operation)> {
        self.transactions
            .values()
            .flat_map(|tx| tx.pending())
            .map(|op| (op.id(), op.clone()))
    }

    fn completed(&mut self, id: OperationId, result: Self::Result) {
        if let Some(tx) = self.transactions.get_mut(&id.0) {
            let sent = tx.completed_operation(id, result);
            if tx.completed().contains(&Action::Record) {
                let payment = tx.payment().expect("recorded payment is finalized");
                self.snapshot.recorded.insert(
                    payment.transaction.compute_txid(),
                    payment.transaction.clone(),
                );
            }
            if let Some(session_id) = tx.session_id()
                && let Some(entry) = self.sessions.get_mut(&session_id)
            {
                if let Some(message) = sent {
                    let _ = entry.tracker.record(&message);
                }
                if entry.external_progress.is_none() {
                    tx.sync_tracker(&entry.tracker);
                }
            }
        }
    }

    fn observe(&mut self, input: Observation) {
        match input {
            Observation::Intent(intent) => {
                self.pending.push(intent);
                // Evaluation removes the intents of started outcomes.
                self.batches = vec![self.pending.iter().map(|(id, _)| id).collect()];
                self.explicit_batches = false;
                self.stale = true;
                return;
            }
            Observation::Snapshot(mut snapshot) => {
                // Recorded transactions are kept. A repeated snapshot changes nothing.
                snapshot.recorded.extend(self.snapshot.recorded.clone());
                if snapshot != self.snapshot {
                    self.snapshot = snapshot;
                    self.stale = true;
                }
                return;
            }
            Observation::FeeRate(feerate) => {
                if feerate != self.feerate {
                    self.feerate = feerate;
                    self.stale = true;
                }
                return;
            }
            Observation::Session(config) => {
                if let Entry::Vacant(vacant) = self.sessions.entry(config.id) {
                    let mut tracker = session::Tracker::new(config.id);
                    tracker.set_additional_entropy(config.additional_entropy);
                    for message in self
                        .orphaned_messages
                        .remove(&config.id)
                        .into_iter()
                        .flatten()
                    {
                        let _ = tracker.record(&message);
                    }
                    vacant.insert(SessionEntry {
                        config,
                        tracker,
                        external_progress: None,
                        bound_execution: None,
                    });
                    self.stale = true;
                }
                return;
            }
            Observation::Peer(message) => {
                let session_id = message.session_id;
                let Some(entry) = self.sessions.get_mut(&session_id) else {
                    self.orphaned_messages
                        .entry(session_id)
                        .or_default()
                        .push(message);
                    return;
                };
                match entry.tracker.record(&message) {
                    Ok(()) => {
                        entry.external_progress = None;
                        self.refresh_session(session_id);
                    }
                    Err(err) => {
                        if let Some(exec_id) = entry.bound_execution {
                            self.transactions
                                .get_mut(&exec_id)
                                .expect("bound session execution exists")
                                .observe(Observation::TransactionInvalidated(exec_id, err.0));
                        }
                    }
                }
                return;
            }
            Observation::SessionProgress {
                session_id,
                progress,
            } => {
                if let Some(entry) = self.sessions.get_mut(&session_id) {
                    if let Some((_, _, entropy)) = progress.outputs() {
                        entry.tracker.set_additional_entropy(entropy);
                    }
                    entry.external_progress = Some(progress);
                    self.refresh_session(session_id);
                }
                return;
            }
            Observation::Broadcast(broadcast_tx) => {
                let mut refreshed = Vec::new();
                for tx in self.transactions.values_mut() {
                    let accepted = tx.observe_broadcast(&broadcast_tx);
                    if !accepted.is_empty()
                        && let Some(session_id) = tx.session_id()
                        && let Some(entry) = self.sessions.get_mut(&session_id)
                    {
                        for contrib in accepted {
                            let _ = entry.tracker.record(&session::Message {
                                session_id,
                                sender: entry.config.side,
                                payload: session::Payload::Witness(contrib),
                            });
                        }
                        refreshed.push(session_id);
                    }
                }
                for session_id in refreshed {
                    self.refresh_session(session_id);
                }
                return;
            }
            Observation::Invalidated(ref reason) if self.stale && !self.evaluated => {
                self.stale = false;
                self.evaluated = true;
                self.planning_errors.insert(
                    ExecutionId {
                        wallet: self.wallet_id,
                        sequence: self.next_sequence,
                    },
                    error(reason),
                );
            }
            _ => {}
        }
        for (id, tx) in &mut self.transactions {
            if let Observation::TransactionInvalidated(target, _) = &input
                && target != id
            {
                continue;
            }
            let was_confirmed = *tx.status() == Status::Confirmed;
            tx.observe(input.clone());
            if !was_confirmed && *tx.status() == Status::Confirmed {
                for id in tx
                    .payment()
                    .expect("confirmed transaction has a payment")
                    .outputs
                    .keys()
                {
                    self.pending.remove(*id);
                }
            }
        }
    }
}

mod runtime;
pub use runtime::{Events, Runtime};

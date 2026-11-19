//! Intents that arrive independently share a transaction when waiting is favorable.

#[path = "support/scripted_driver.rs"]
mod scripted_driver;
#[path = "support/signing.rs"]
mod signing;

use bitcoin::{
    Amount, FeeRate, OutPoint, ScriptBuf, Transaction, TxOut, Txid, Weight, hashes::Hash,
};
use cost_function::{
    intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve, ScheduleGrid},
    queue::{InMemoryQueue, Queue},
};
use fungi_core::{
    driver::{Decision, Machine, Stopped, Wake},
    strategy::{Broadcaster, Events, Executor, Observation, Request, Runtime, State, Status},
    wallet::{Coin, Snapshot, dummy},
};
use rand::{SeedableRng, rngs::StdRng};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const SLOTS_PER_SECOND: u64 = 1_000_000 / ScheduleGrid::AUTHORED_STEP.as_micros() as u64;

type Inbox = Arc<Mutex<VecDeque<Observation>>>;

/// Confirms each publication, then reports the wallet's remaining coins.
struct Chain {
    inbox: Inbox,
    coins: BTreeMap<OutPoint, Coin>,
    published: Vec<Transaction>,
}
impl Broadcaster for Chain {
    type Error = &'static str;
    fn broadcast(
        &mut self,
        tx: &Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + use<> {
        self.published.push(tx.clone());
        for input in &tx.input {
            self.coins.remove(&input.previous_output);
        }
        let mut inbox = self.inbox.lock().unwrap();
        inbox.push_back(Observation::Confirmed(tx.compute_txid()));
        inbox.push_back(Observation::Snapshot(Snapshot {
            coins: self.coins.clone(),
            ..Default::default()
        }));
        std::future::ready(Ok(()))
    }
}

/// Virtual time with observations scheduled at fixed instants.
struct Clock {
    now: Instant,
    inbox: Inbox,
    arrivals: VecDeque<(Instant, Observation)>,
    /// The deadline requested by each wait, and the time it was requested.
    waits: Vec<(Instant, Option<Instant>)>,
}
impl Events for Clock {
    fn now(&self) -> Instant {
        self.now
    }
    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        self.waits.push((self.now, deadline));
        if let Some(input) = self.inbox.lock().unwrap().pop_front() {
            return Wake::Input(input);
        }
        match (self.arrivals.front(), deadline) {
            (Some(&(at, _)), deadline) if deadline.is_none_or(|deadline| at <= deadline) => {
                self.now = self.now.max(at);
                Wake::Input(self.arrivals.pop_front().unwrap().1)
            }
            (_, Some(deadline)) => {
                self.now = self.now.max(deadline);
                Wake::Timeout
            }
            (_, None) => Wake::Closed,
        }
    }
}

type Wallet = State<dummy::Change, StdRng>;
type TestRuntime = Runtime<dummy::Change, signing::Signer, dummy::Recorder, Chain, Clock>;

fn script(byte: u8) -> ScriptBuf {
    ScriptBuf::from_bytes([vec![0x51, 0x20], vec![byte; 32]].concat())
}

/// A 10,000 sat payment whose payoff rises to 100,000 sats over ten minutes.
fn payment(destination: u8) -> Intent {
    payment_of(destination, Amount::from_sat(10_000))
}

fn payment_of(destination: u8, amount: Amount) -> Intent {
    Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: script(destination),
            amount,
        }),
        PayoffCurve::new(0, [(600 * SLOTS_PER_SECOND, Amount::from_sat(100_000))]).unwrap(),
    )
}

fn coins() -> BTreeMap<OutPoint, Coin> {
    (0..2)
        .map(|vout| {
            let outpoint = OutPoint::new(Txid::all_zeros(), vout);
            let coin = Coin {
                txout: TxOut {
                    value: Amount::from_sat(50_000),
                    script_pubkey: signing::script(outpoint),
                },
                satisfaction_weight: Weight::from_wu(70),
            };
            (outpoint, coin)
        })
        .collect()
}

fn wallet(now: Instant) -> Wallet {
    State::new(
        InMemoryQueue::new([payment(4)]),
        Snapshot {
            coins: coins(),
            ..Default::default()
        },
        FeeRate::from_sat_per_vb(2).unwrap(),
        [1; 16],
        dummy::Change::new(script(3), Weight::from_wu(70), []),
        StdRng::seed_from_u64(0),
        ScheduleGrid::new(now),
    )
}

fn runtime(
    now: Instant,
    arrivals: impl IntoIterator<Item = (Duration, Observation)>,
) -> TestRuntime {
    let inbox = Inbox::default();
    let executor = Executor::new(
        dummy::Change::new(
            script(3),
            Weight::from_wu(70),
            [Ok(script(5)), Ok(script(6))],
        ),
        coins()
            .into_keys()
            .map(|outpoint| signing::Signer::new(outpoint, [Ok(())]))
            .collect(),
        dummy::Recorder::new([Ok(()), Ok(())]),
        Chain {
            inbox: inbox.clone(),
            coins: coins(),
            published: vec![],
        },
    )
    .unwrap();
    Runtime {
        executor,
        events: Clock {
            now,
            inbox,
            arrivals: arrivals
                .into_iter()
                .map(|(after, input)| (now + after, input))
                .collect(),
            waits: vec![],
        },
    }
}

/// The time of the next scheduled action, if the wallet schedules one.
fn start_time(state: &mut Wallet, now: Instant) -> Option<Instant> {
    match state.decide(now) {
        Decision::Schedule(next) => Some(next.at),
        Decision::Wait | Decision::Done => None,
    }
}

fn paid(tx: &Transaction) -> Vec<ScriptBuf> {
    let mut scripts: Vec<_> = tx
        .output
        .iter()
        .map(|output| output.script_pubkey.clone())
        .filter(|paid| [script(4), script(7), script(8)].contains(paid))
        .collect();
    scripts.sort();
    scripts
}

#[tokio::test]
async fn a_later_intent_joins_the_scheduled_payment() {
    let now = Instant::now();
    let alone = start_time(&mut wallet(now), now).unwrap();
    let arrival = Duration::from_secs(10);
    assert!(now + arrival < alone);
    let mut state = wallet(now);
    let mut io = runtime(now, [(arrival, Observation::Intent(payment(7)))]);

    assert_eq!(
        scripted_driver::run(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let chain = io.executor.broadcaster.lock().await;
    assert_eq!(chain.published.len(), 1);
    assert_eq!(paid(&chain.published[0]), [script(4), script(7)]);
    assert_eq!(chain.published[0].input.len(), 1);
    assert_eq!(state.pending().iter().count(), 0);

    // The first schedule was for A alone. B replaced it with an earlier one
    // before any wallet effect, and that schedule started the transaction.
    let deadlines: Vec<_> = io.events.waits.iter().filter_map(|(_, at)| *at).collect();
    assert_eq!(deadlines[0], alone);
    let together = deadlines[1];
    assert!(together < alone);
    assert!(deadlines.iter().all(|&at| at == alone || at == together));
    assert_eq!(io.events.waits[1].0, now + arrival);

    // The candidate for A alone never started and never reached the executor.
    let ids: Vec<_> = state.transactions().keys().map(|id| id.sequence).collect();
    assert_eq!(ids, [1]);
    let tx = &state.transactions().values().next().unwrap();
    assert_eq!(tx.status(), &Status::Confirmed);
    let mut intents: Vec<_> = tx.payment().unwrap().outputs.keys().copied().collect();
    intents.sort();
    assert_eq!(intents, [0, 1]);
}

#[tokio::test]
async fn an_intent_after_confirmation_starts_another_transaction() {
    let now = Instant::now();
    let later = Duration::from_secs(1_000);
    let mut state = wallet(now);
    let mut io = runtime(now, [(later, Observation::Intent(payment(8)))]);

    assert_eq!(
        scripted_driver::run(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let chain = io.executor.broadcaster.lock().await;
    assert_eq!(chain.published.len(), 2);
    assert_eq!(paid(&chain.published[0]), [script(4)]);
    assert_eq!(paid(&chain.published[1]), [script(8)]);
    let spent: Vec<_> = chain
        .published
        .iter()
        .map(|tx| tx.input[0].previous_output)
        .collect();
    assert_ne!(spent[0], spent[1]);
    assert!(chain.coins.is_empty());
    for outpoint in coins().keys() {
        let signer = io.executor.signers.get(outpoint).unwrap();
        assert_eq!(signer.lock().await.requests().len(), 1);
    }

    let ids: Vec<_> = state.transactions().keys().map(|id| id.sequence).collect();
    assert_eq!(ids.len(), 2);
    assert!(ids[0] < ids[1]);
    assert!(
        state
            .transactions()
            .values()
            .all(|tx| tx.status() == &Status::Confirmed)
    );
    assert_eq!(state.pending().iter().count(), 0);
    // Both payments stay recorded after the new coin observation.
    assert_eq!(state.snapshot().recorded.len(), 2);
    assert_eq!(io.executor.recorder.lock().await.recorded().len(), 2);
}

#[tokio::test]
async fn an_infeasible_batch_waits_for_a_new_snapshot() {
    let now = Instant::now();
    let mut state = wallet(now);
    // Fund only one coin, then add an intent the coin cannot cover alongside A.
    state.observe(Observation::Snapshot(Snapshot {
        coins: coins().into_iter().take(1).collect(),
        ..Default::default()
    }));
    state.observe(Observation::Intent(payment_of(7, Amount::from_sat(45_000))));
    assert_eq!(start_time(&mut state, now), None);
    assert_eq!(state.planning_errors().len(), 1);

    state.observe(Observation::Snapshot(Snapshot {
        coins: coins(),
        ..Default::default()
    }));
    let at = start_time(&mut state, now).unwrap();
    assert!(state.planning_errors().is_empty());
    assert!(at > now);
    let tx = state.transactions().values().next().unwrap();
    assert_eq!(tx.candidate().inputs().count(), 2);
}

//! Funding and construction use the same candidate and execution identity.

#[path = "support/scripted_driver.rs"]
mod scripted_driver;

use scripted_driver::run as run_scripted;

use bitcoin::{Amount, FeeRate, OutPoint, ScriptBuf, TxOut, Txid, Weight, hashes::Hash};
use cost_function::{
    intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve, ScheduleGrid},
    queue::{InMemoryQueue, Queue},
    wallet::UtxoSnapshot,
};
use fungi_core::{
    driver::{Decision, Machine, Scheduled, Stopped, Wake},
    strategy::{Action, Events, Executor, Observation, Request, Runtime, State, Status, Update},
    wallet::{Coin, Snapshot, dummy},
};
use rand::{SeedableRng, rngs::StdRng};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

type Wallet = State<dummy::Change, StdRng>;

const SLOTS_PER_SECOND: u64 = 1_000_000 / ScheduleGrid::AUTHORED_STEP.as_micros() as u64;

fn script(byte: u8) -> ScriptBuf {
    ScriptBuf::from_bytes([vec![0x51, 0x20], vec![byte; 32]].concat())
}

fn fixture(now: Instant, wallet: u8, funded: bool, has_intent: bool) -> Wallet {
    fixture_with_start(now, wallet, funded, has_intent, 0)
}

fn fixture_with_start(
    now: Instant,
    wallet: u8,
    funded: bool,
    has_intent: bool,
    start_slot: u64,
) -> Wallet {
    fixture_with_scripts(
        now,
        wallet,
        funded,
        has_intent,
        start_slot,
        script(1),
        script(3),
    )
}

fn fixture_with_scripts(
    now: Instant,
    wallet: u8,
    funded: bool,
    has_intent: bool,
    start_slot: u64,
    coin_script: ScriptBuf,
    change_template: ScriptBuf,
) -> Wallet {
    let coin = Coin {
        txout: TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: coin_script,
        },
        satisfaction_weight: Weight::from_wu(70),
    };
    let snapshot = Snapshot {
        coins: if funded {
            BTreeMap::from([(OutPoint::new(Txid::all_zeros(), u32::from(wallet)), coin)])
        } else {
            BTreeMap::new()
        },
        ..Default::default()
    };
    let grid = ScheduleGrid::new(now);
    let intent = Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: script(2),
            amount: Amount::from_sat(10_000),
        }),
        PayoffCurve::new(
            start_slot,
            [(60 * SLOTS_PER_SECOND, Amount::from_sat(100_000))],
        )
        .unwrap(),
    );
    State::new(
        InMemoryQueue::new(has_intent.then_some(intent)),
        snapshot,
        FeeRate::from_sat_per_vb(2).unwrap(),
        [wallet; 16],
        dummy::Change::new(change_template, Weight::from_wu(70), []),
        StdRng::seed_from_u64(0),
        grid,
    )
}

fn schedule(decision: Decision<Request>) -> Option<Scheduled<Request>> {
    match decision {
        Decision::Schedule(scheduled) => Some(scheduled),
        Decision::Wait | Decision::Done => None,
    }
}

fn scheduled(state: &mut Wallet, now: Instant) -> Request {
    let scheduled = schedule(state.decide(now)).expect("expected a funded candidate");
    assert!(scheduled.at > now);
    assert_eq!(scheduled.action.action, Action::Start);
    scheduled.action
}

struct Clock(Instant, Option<Observation>);

impl Events for Clock {
    fn now(&self) -> Instant {
        self.0
    }

    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        if let Some(input) = self.1.take() {
            return Wake::Input(input);
        }
        if let Some(at) = deadline {
            self.0 = self.0.max(at);
            Wake::Timeout
        } else {
            Wake::Closed
        }
    }
}

fn executor() -> Executor {
    Executor::default()
}

#[test]
fn the_strategy_derives_a_candidate_without_consuming_the_intent() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    assert!(state.transactions().is_empty());
    let request = scheduled(&mut state, now);
    assert_eq!(state.pending().iter().count(), 1);
    let id = request.id().0;
    assert_eq!(id.wallet, [1; 16]);
    assert_eq!(id.sequence, 0);
    let candidate = state.transactions()[&id].candidate();
    assert!(candidate.inputs().cloned().eq(state.snapshot().iter()));
    assert_eq!(
        candidate.payments().map(|(&id, _)| id).collect::<Vec<_>>(),
        [0]
    );
    assert!(candidate.objective_cost().unwrap().to_sat() > 10_000);
}

#[test]
fn future_payoff_uses_the_wallet_grid() {
    let origin = Instant::now();
    let mut state = fixture_with_start(origin, 1, true, true, 1);
    let now = origin + Duration::from_nanos(250);
    let next = schedule(state.decide(now)).expect("expected a funded candidate");

    assert!(next.at > origin + ScheduleGrid::AUTHORED_STEP);
    assert!(
        (next.at - origin)
            .subsec_nanos()
            .is_multiple_of(ScheduleGrid::JIFFY.subsec_nanos())
    );
}

#[test]
fn an_empty_queue_waits_without_constructing_a_candidate() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, false);
    let decision = state.decide(now);
    assert!(matches!(decision, Decision::Wait));
    assert!(schedule(decision).is_none());
    assert!(state.transactions().is_empty());
    assert!(state.planning_errors().is_empty());
}

#[test]
fn an_unfunded_intent_remains_pending_without_ending_the_wallet() {
    let now = Instant::now();
    let mut state = fixture(now, 1, false, true);
    assert!(matches!(state.decide(now), Decision::Wait));
    assert_eq!(state.planning_errors().len(), 1);
    assert_eq!(state.pending().iter().count(), 1);
    assert!(matches!(
        state.decide(now + Duration::from_secs(1)),
        Decision::Wait
    ));
}

#[tokio::test]
async fn a_bound_execution_rejects_another_plan_but_accepts_its_replay() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let first = scheduled(&mut state, now);
    let io = executor();
    io.execute(&first).await.unwrap();
    let other = scheduled(&mut fixture(now, 1, true, true), now);
    assert_eq!(first.id(), other.id());
    assert_eq!(
        io.execute(&other).await.unwrap_err().0,
        "execution belongs to a different candidate"
    );
    let reevaluated = scheduled(&mut state, now);
    io.execute(&reevaluated).await.unwrap();
}

#[tokio::test]
async fn the_driver_starts_the_planned_transaction_at_readiness() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let request = scheduled(&mut state, now);
    let ready_at = state.transactions()[&request.id().0]
        .candidate()
        .ready_at(now, ScheduleGrid::new(now))
        .unwrap();
    let mut io = Runtime {
        executor: executor(),
        events: Clock(now, None),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    assert_eq!(io.events.0, ready_at);
    let tx = &state.transactions()[&request.id().0];
    assert_eq!(tx.status(), &Status::Running);
    assert!(tx.completed().contains(&Action::Start));
    assert!(tx.pending().next().is_none());
    assert_eq!(state.pending().iter().count(), 1);
}

#[tokio::test]
async fn invalidated_candidate_has_no_effects() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    let reason = "coin spent elsewhere";
    let mut io = Runtime {
        executor: executor(),
        events: Clock(now, Some(Observation::Invalidated(reason.into()))),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = &state.transactions()[&start.id().0];
    assert!(matches!(tx.status(), Status::Failed(err) if err.0 == reason));
    assert!(tx.completed().is_empty());
    assert!(state.operations().next().is_none());
    assert!(state.planning_errors().is_empty());
    assert_eq!(state.pending().iter().count(), 1);
    assert_eq!(io.events.0, now);
}

#[test]
fn invalidation_before_planning_keeps_the_intents_pending() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let reason = "coin already spent";
    state.observe(Observation::Invalidated(reason.into()));

    assert!(matches!(state.decide(now), Decision::Wait));
    assert!(state.transactions().is_empty());
    assert_eq!(state.pending().iter().count(), 1);
    assert_eq!(state.planning_errors().values().next().unwrap().0, reason);
}

#[test]
fn only_this_plans_pending_request_accepts_a_completion() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    let unrelated = scheduled(&mut fixture(now, 2, true, true), now);

    state.apply(unrelated.clone());
    state.completed(start.id(), Ok(Update::Applied));
    assert!(state.operations().next().is_none());
    assert!(state.transactions()[&start.id().0].completed().is_empty());

    state.apply(start.clone());
    state.apply(start.clone());
    assert_eq!(
        state.operations().map(|(id, _)| id).collect::<Vec<_>>(),
        [start.id()]
    );
    assert!(matches!(state.decide(now), Decision::Wait));
    state.completed(unrelated.id(), Ok(Update::Applied));
    assert_eq!(state.operations().count(), 1);
    state.completed(start.id(), Ok(Update::Applied));
    assert!(state.operations().next().is_none());
    assert!(
        state.transactions()[&start.id().0]
            .completed()
            .contains(&Action::Start)
    );
}

#[tokio::test]
async fn a_failed_start_stops_the_candidates_actions() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let unrelated = scheduled(&mut fixture(now, 1, true, true), now);
    let start = scheduled(&mut state, now);
    let executor = executor();
    executor.execute(&unrelated).await.unwrap();
    let mut io = Runtime {
        executor,
        events: Clock(now, None),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = &state.transactions()[&start.id().0];
    let failure = fungi_core::strategy::Error("execution belongs to a different candidate".into());
    assert_eq!(tx.status(), &Status::Failed(failure.clone()));
    assert_eq!(
        tx.operation_errors(),
        &BTreeMap::from([(Action::Start, failure)])
    );
    assert!(tx.completed().is_empty());
    state.apply(start);
    assert!(state.operations().next().is_none());
}

#[test]
fn the_decision_rejects_a_different_plan_for_the_same_execution() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    let unrelated = scheduled(&mut fixture(now, 1, true, true), now);
    assert_eq!(start.id(), unrelated.id());

    state.apply(unrelated);

    assert!(matches!(
        state.transactions()[&start.id().0].status(),
        Status::Failed(err) if err.0 == "action prerequisites are not satisfied"
    ));
    assert!(state.operations().next().is_none());
}

#[tokio::test]
async fn a_non_start_request_cannot_bind_an_execution() {
    let now = Instant::now();
    let mut request = scheduled(&mut fixture(now, 1, true, true), now);
    request.action = Action::Input(0);
    let io = executor();
    assert_eq!(
        io.execute(&request).await.unwrap_err().0,
        "transaction construction has not started"
    );
    let other = scheduled(&mut fixture(now, 1, true, true), now);
    io.execute(&other).await.unwrap();
}

#[tokio::test]
async fn inputs_must_be_contributed_before_the_executor_can_close_them() {
    let now = Instant::now();
    let mut request = scheduled(&mut fixture(now, 1, true, true), now);
    let io = executor();
    io.execute(&request).await.unwrap();
    request.action = Action::CloseInputs;
    assert_eq!(
        io.execute(&request).await.unwrap_err().0,
        "input contributions are incomplete"
    );
    request.action = Action::Input(1);
    assert_eq!(io.execute(&request).await.unwrap_err().0, "unknown input");
    request.action = Action::Input(0);
    io.execute(&request).await.unwrap();
    request.action = Action::CloseInputs;
    io.execute(&request).await.unwrap();
}

#[tokio::test]
async fn the_driver_contributes_and_closes_the_candidate_inputs() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let id = scheduled(&mut state, now).id().0;
    let mut io = Runtime {
        executor: executor(),
        events: Clock(now, None),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = &state.transactions()[&id];
    assert_eq!(tx.status(), &Status::Running);
    assert!(tx.completed().contains(&Action::Input(0)));
    assert!(tx.completed().contains(&Action::CloseInputs));
    assert!(tx.pending().next().is_none());
}

#[tokio::test]
async fn an_unsupported_prevout_cannot_be_contributed() {
    let now = Instant::now();
    let mut state = fixture_with_scripts(now, 1, true, true, 0, ScriptBuf::new(), script(3));
    let id = scheduled(&mut state, now).id().0;
    let mut io = Runtime {
        executor: executor(),
        events: Clock(now, None),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = &state.transactions()[&id];
    assert!(
        matches!(tx.status(), Status::Failed(err) if err.0 == "PoC signer path only supports P2TR key spends")
    );
    assert_eq!(
        tx.completed().iter().cloned().collect::<Vec<_>>(),
        [Action::Start]
    );
}

#[test]
fn the_decision_will_not_dispatch_an_input_before_start() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let mut premature = scheduled(&mut state, now);
    premature.action = Action::Input(0);

    state.apply(premature.clone());

    assert!(matches!(
        state.transactions()[&premature.id().0].status(),
        Status::Failed(_)
    ));
    assert!(state.operations().next().is_none());
}

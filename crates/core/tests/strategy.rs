//! Funding and construction use the same candidate and execution identity.

#[path = "support/scripted_driver.rs"]
mod scripted_driver;

use scripted_driver::run as run_scripted;

#[path = "support/signing.rs"]
mod signing;

#[path = "support/identity.rs"]
mod identity;

use bitcoin::{
    Amount, FeeRate, OutPoint, ScriptBuf, Transaction, TxOut, Txid, Weight, hashes::Hash,
};
use cost_function::{
    intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve, ScheduleGrid},
    queue::{InMemoryQueue, Queue},
    wallet::UtxoSnapshot,
};
use fungi_core::{
    driver::{Decision, Machine, Scheduled, Stopped, Wake},
    strategy::{
        Action, Broadcaster, Error, Events, Executor, Observation, Request, Runtime, State, Status,
        Update,
    },
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
        signing::script(OutPoint::new(Txid::all_zeros(), u32::from(wallet))),
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
    assert_eq!(scheduled.action.action, Action::CloseInputs);
    scheduled.action
}

struct Clock(Instant);

impl Events for Clock {
    fn now(&self) -> Instant {
        self.0
    }

    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        if let Some(at) = deadline {
            self.0 = self.0.max(at);
            Wake::Timeout
        } else {
            Wake::Closed
        }
    }
}

type TestExecutor = Executor<dummy::Change, signing::Signer, dummy::Recorder, AcceptBroadcast>;

struct AcceptBroadcast;
impl Broadcaster for AcceptBroadcast {
    type Error = &'static str;
    fn broadcast(
        &mut self,
        _: &Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + use<> {
        std::future::ready(Ok(()))
    }
}

fn executor_with_change(change: dummy::Change) -> TestExecutor {
    Executor::new(
        change,
        vec![signing::Signer::new(
            OutPoint::new(Txid::all_zeros(), 1),
            [Ok(())],
        )],
        dummy::Recorder::new([Ok(())]),
        AcceptBroadcast,
    )
    .unwrap()
}

fn executor() -> TestExecutor {
    executor_with_change(dummy::Change::new(
        script(3),
        Weight::from_wu(70),
        [Ok(script(4)), Ok(script(5))],
    ))
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
fn construction_sessions_are_fresh_and_stable_for_retries() {
    let now = Instant::now();
    let mut first = fixture(now, 1, true, true);
    assert!(matches!(first.decide(now), Decision::Schedule(_)));
    let first_execution = *first.transactions().keys().next().unwrap();
    let first_id = first.transactions()[&first_execution].construction_session_id();

    assert!(matches!(first.decide(now), Decision::Schedule(_)));
    assert_eq!(
        first.transactions()[&first_execution].construction_session_id(),
        first_id
    );

    let mut second = fixture(now, 1, true, true);
    assert!(matches!(second.decide(now), Decision::Schedule(_)));
    let second_execution = *second.transactions().keys().next().unwrap();
    assert_eq!(second_execution, first_execution);
    assert_ne!(
        second.transactions()[&second_execution].construction_session_id(),
        first_id
    );
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
    assert_eq!(io.change.lock().await.requests(), 0);
    assert!(
        io.signers
            .get(&OutPoint::new(Txid::all_zeros(), 1))
            .unwrap()
            .lock()
            .await
            .requests()
            .is_empty()
    );
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
        events: Clock(now),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    assert_eq!(io.events.0, ready_at);
    let tx = &state.transactions()[&request.id().0];
    assert_eq!(tx.status(), &Status::AwaitingConfirmation);
    assert!(tx.completed().contains(&Action::CloseInputs));
    assert!(tx.pending().next().is_none());
    assert_eq!(state.pending().iter().count(), 1);
    assert_eq!(
        io.executor
            .signers
            .get(&OutPoint::new(Txid::all_zeros(), 1))
            .unwrap()
            .lock()
            .await
            .requests()
            .len(),
        1
    );
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
            .contains(&Action::CloseInputs)
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
        events: Clock(now),
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
        &BTreeMap::from([(Action::CloseInputs, failure)])
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
async fn a_non_close_inputs_request_cannot_bind_an_execution() {
    let now = Instant::now();
    let mut request = scheduled(&mut fixture(now, 1, true, true), now);
    request.action = Action::AllocateChange(identity::change(0));
    let io = executor();
    assert_eq!(
        io.execute(&request).await.unwrap_err().0,
        "transaction construction has not started"
    );
    let other = scheduled(&mut fixture(now, 1, true, true), now);
    io.execute(&other).await.unwrap();
}

#[tokio::test]
async fn close_inputs_materializes_the_candidate_inputs() {
    let now = Instant::now();
    let mut request = scheduled(&mut fixture(now, 1, true, true), now);
    let io = executor();
    io.execute(&request).await.unwrap();
    request.action = Action::CloseOutputs;
    assert_eq!(
        io.execute(&request).await.unwrap_err().0,
        "output contributions are incomplete"
    );
    request.action = Action::CloseInputs;
    io.execute(&request).await.unwrap();
}

#[tokio::test]
async fn the_driver_materializes_and_closes_the_candidate_inputs() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let id = scheduled(&mut state, now).id().0;
    let mut io = Runtime {
        executor: executor(),
        events: Clock(now),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = &state.transactions()[&id];
    assert_eq!(tx.status(), &Status::AwaitingConfirmation);
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
        events: Clock(now),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = &state.transactions()[&id];
    assert!(
        matches!(tx.status(), Status::Failed(err) if err.0 == "PoC signer path only supports P2TR key spends")
    );
    assert!(tx.completed().is_empty());
}

#[test]
fn the_decision_will_not_dispatch_an_allocation_before_close_inputs() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let mut premature = scheduled(&mut state, now);
    premature.action = Action::AllocateChange(identity::change(0));

    state.apply(premature.clone());

    assert!(matches!(
        state.transactions()[&premature.id().0].status(),
        Status::Failed(_)
    ));
    assert!(state.operations().next().is_none());
}

async fn run_action(io: &TestExecutor, start: &Request, action: Action) -> Result<Update, Error> {
    let mut request = start.clone();
    request.action = action;
    io.execute(&request).await
}

#[tokio::test]
async fn rejected_change_cannot_be_added_by_bypassing_the_decision_state() {
    let now = Instant::now();
    let start = scheduled(&mut fixture(now, 1, true, true), now);
    let io = executor_with_change(dummy::Change::new(
        script(3),
        Weight::from_wu(70),
        [Ok(ScriptBuf::new())],
    ));
    run_action(&io, &start, Action::CloseInputs).await.unwrap();
    let first = run_action(&io, &start, Action::AllocateChange(identity::change(0)))
        .await
        .unwrap_err();
    assert_eq!(first.0, "allocated change differs from its priced template");
    assert_eq!(
        run_action(&io, &start, Action::CloseOutputs)
            .await
            .unwrap_err()
            .0,
        "output contributions are incomplete"
    );
    let replay = run_action(&io, &start, Action::AllocateChange(identity::change(0)))
        .await
        .unwrap_err();
    assert_eq!(first, replay);
    assert_eq!(io.change.lock().await.requests(), 1);
}

#[tokio::test]
async fn concrete_change_scripts_do_not_change_candidate_cost_or_schedule() {
    let now = Instant::now();
    let mut observed = Vec::new();
    for byte in [5, 6] {
        let mut state = fixture_with_scripts(
            now,
            1,
            true,
            true,
            0,
            signing::script(OutPoint::new(Txid::all_zeros(), 1)),
            script(byte),
        );
        let executor = executor_with_change(dummy::Change::new(
            script(byte),
            Weight::from_wu(70),
            [Ok(script(byte + 10))],
        ));
        let mut io = Runtime {
            executor,
            events: Clock(now),
        };
        let scheduled = schedule(state.decide(now)).expect("expected a funded candidate");
        assert_eq!(io.executor.change.lock().await.requests(), 0);
        let id = scheduled.action.id().0;
        let cost = state.transactions()[&id]
            .candidate()
            .objective_cost()
            .unwrap();

        assert_eq!(
            run_scripted(&mut state, &mut io, Request::id).await,
            Stopped::Closed
        );

        let tx = &state.transactions()[&id];
        assert_eq!(tx.status(), &Status::AwaitingConfirmation);
        assert!(
            tx.completed().contains(&Action::CloseOutputs),
            "the allocated script must pass frozen output and accounting validation"
        );
        assert_eq!(io.executor.change.lock().await.requests(), 1);
        assert!(
            tx.payment()
                .unwrap()
                .transaction
                .output
                .iter()
                .any(|output| output.script_pubkey == script(byte + 10))
        );
        observed.push((cost, scheduled.at));
    }
    assert_eq!(observed[0], observed[1]);
}

#[tokio::test]
async fn outputs_require_closed_inputs_and_all_contributions_before_freezing() {
    let now = Instant::now();
    let start = scheduled(&mut fixture(now, 1, true, true), now);
    let io = executor();
    assert_eq!(
        run_action(&io, &start, Action::AllocateChange(identity::change(0)))
            .await
            .unwrap_err()
            .0,
        "transaction construction has not started"
    );
    run_action(&io, &start, Action::CloseInputs).await.unwrap();
    assert_eq!(
        run_action(&io, &start, Action::CloseOutputs)
            .await
            .unwrap_err()
            .0,
        "output contributions are incomplete"
    );
    assert_eq!(
        run_action(&io, &start, Action::AllocateChange(identity::change(1)))
            .await
            .unwrap_err()
            .0,
        "unknown change"
    );
    for action in [
        Action::AllocateChange(identity::change(0)),
        Action::CloseOutputs,
    ] {
        run_action(&io, &start, action).await.unwrap();
    }
    assert_eq!(
        run_action(&io, &start, Action::AllocateChange(identity::change(1)))
            .await
            .unwrap_err()
            .0,
        "transaction is frozen"
    );
}

#[tokio::test]
async fn completed_effects_belong_to_their_transaction() {
    let now = Instant::now();
    let first = scheduled(&mut fixture(now, 1, true, true), now);
    let second = scheduled(&mut fixture(now, 2, true, true), now);
    let io = executor();
    for request in [&first, &second] {
        for action in [
            Action::CloseInputs,
            Action::AllocateChange(identity::change(0)),
        ] {
            run_action(&io, request, action).await.unwrap();
        }
    }
    assert_eq!(io.change.lock().await.requests(), 2);
    run_action(&io, &first, Action::AllocateChange(identity::change(0)))
        .await
        .unwrap();
    assert_eq!(io.change.lock().await.requests(), 2);
}

#[tokio::test]
async fn an_allocation_error_prevents_output_freezing() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let id = scheduled(&mut state, now).id().0;
    let executor = executor_with_change(dummy::Change::new(
        script(3),
        Weight::from_wu(70),
        [Err(dummy::Error::Scripted("change source failed"))],
    ));
    let mut io = Runtime {
        executor,
        events: Clock(now),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = &state.transactions()[&id];
    assert!(matches!(tx.status(), Status::Failed(err) if err.0 == "change source failed"));
    assert!(!tx.completed().contains(&Action::CloseOutputs));
    assert!(tx.pending().next().is_none());
}

#[tokio::test]
async fn a_busy_signer_does_not_block_another_inputs_signer() {
    use bitcoin::{Psbt, psbt};
    use fungi_core::wallet::InputSigner;
    use futures::channel::oneshot;
    struct Signer {
        outpoint: OutPoint,
        ready: Option<oneshot::Receiver<()>>,
    }
    impl InputSigner for Signer {
        type Error = std::io::Error;
        fn outpoint(&self) -> OutPoint {
            self.outpoint
        }
        fn sign(
            &mut self,
            psbt: &Psbt,
        ) -> impl Future<Output = Result<psbt::Input, Self::Error>> + Send + use<> {
            let ready = self.ready.take();
            let signed = signing::sign(psbt, self.outpoint);
            async move {
                if let Some(ready) = ready {
                    ready.await.unwrap();
                }
                Ok(signed)
            }
        }
    }
    let (sender, receiver) = oneshot::channel();
    let now = Instant::now();
    let mut first = scheduled(&mut fixture(now, 1, true, true), now);
    let mut second = scheduled(&mut fixture(now, 2, true, true), now);
    let io = Executor::new(
        dummy::Change::new(
            script(3),
            Weight::from_wu(70),
            [Ok(script(4)), Ok(script(5))],
        ),
        vec![
            Signer {
                outpoint: OutPoint::new(Txid::all_zeros(), 1),
                ready: Some(receiver),
            },
            Signer {
                outpoint: OutPoint::new(Txid::all_zeros(), 2),
                ready: None,
            },
        ],
        dummy::Recorder::new([]),
        AcceptBroadcast,
    )
    .unwrap();
    for (request, outpoint) in [
        (&mut first, OutPoint::new(Txid::all_zeros(), 1)),
        (&mut second, OutPoint::new(Txid::all_zeros(), 2)),
    ] {
        for action in [
            Action::CloseInputs,
            Action::AllocateChange(identity::change(0)),
            Action::CloseOutputs,
        ] {
            request.action = action;
            io.execute(request).await.unwrap();
        }
        request.action = Action::Sign(outpoint);
    }
    let first_sign = io.execute(&first);
    futures::pin_mut!(first_sign);
    assert!(futures::poll!(&mut first_sign).is_pending());
    let second_sign = io.execute(&second);
    futures::pin_mut!(second_sign);
    assert!(matches!(
        futures::poll!(&mut second_sign),
        std::task::Poll::Ready(Ok(_))
    ));
    sender.send(()).unwrap();
    first_sign.await.unwrap();
}

#[test]
fn duplicate_signer_capabilities_are_rejected() {
    let coin = OutPoint::new(Txid::all_zeros(), 0);
    let result = Executor::new(
        dummy::Change::new(script(3), Weight::from_wu(70), []),
        vec![
            signing::Signer::new(coin, []),
            signing::Signer::new(coin, []),
        ],
        dummy::Recorder::new([]),
        AcceptBroadcast,
    );
    assert!(matches!(result, Err(err) if err.0.contains("duplicate signer")));
}

const BATCH_RAMPS: [u64; 2] = [10 * SLOTS_PER_SECOND, 100 * SLOTS_PER_SECOND];
const BATCH_PEAK_PAYOFF: Amount = Amount::from_sat(20_000);

fn split_fixture(now: Instant, funded: bool) -> Wallet {
    let mut snapshot = fixture(now, 1, true, true).snapshot().clone();
    if funded {
        snapshot
            .coins
            .extend(fixture(now, 2, true, true).snapshot().coins.clone());
    }
    let grid = ScheduleGrid::new(now);
    let intents = BATCH_RAMPS.map(|slots| {
        Intent::new(
            Outcome::OutputCreation(FixedPaymentInstructions {
                script_pubkey: script(2),
                amount: Amount::from_sat(10_000),
            }),
            PayoffCurve::new(0, [(slots, BATCH_PEAK_PAYOFF)]).unwrap(),
        )
    });
    State::new(
        InMemoryQueue::new(intents),
        snapshot,
        FeeRate::from_sat_per_vb(2).unwrap(),
        [1; 16],
        dummy::Change::new(script(3), Weight::from_wu(70), []),
        StdRng::seed_from_u64(0),
        grid,
    )
}

#[test]
fn joint_outcome_readiness_uses_all_batches_without_reserving_wallet_coins() {
    let now = Instant::now();
    let mut state = split_fixture(now, true)
        .with_batches(vec![vec![0], vec![1]])
        .unwrap();
    let next = schedule(state.decide(now)).expect("expected a jointly funded candidate");
    assert_eq!(state.transactions().len(), 2);
    let spent: Vec<_> = state
        .transactions()
        .values()
        .flat_map(|tx| tx.candidate().inputs().map(|coin| coin.outpoint()))
        .collect();
    assert_eq!(spent.len(), 2);
    assert_ne!(spent[0], spent[1]);
    assert!(
        spent
            .iter()
            .all(|coin| state.snapshot().coins.contains_key(coin))
    );
    assert_eq!(state.pending().iter().count(), 2);

    let cost_sats = state
        .transactions()
        .values()
        .map(|tx| tx.candidate().objective_cost().unwrap().to_sat())
        .sum::<i64>();
    let elapsed = next.at - now;
    assert!(
        elapsed
            .subsec_nanos()
            .is_multiple_of(ScheduleGrid::JIFFY.subsec_nanos())
    );

    // Both payoffs are still on their rising segments. For t microseconds
    // their sum is peak * t * (1 / short + 1 / long). Cross-multiply to
    // locate the first strictly favorable tick using integers.
    let [short, long] =
        BATCH_RAMPS.map(|slots| u128::from(slots) * ScheduleGrid::AUTHORED_STEP.as_micros());
    let scaled_cost = u128::try_from(cost_sats).unwrap() * short * long;
    let payoff_per_tick = u128::from(BATCH_PEAK_PAYOFF.to_sat()) * (short + long);
    let tick = elapsed.as_micros();
    assert!(tick > 0 && tick <= short);
    assert!((tick - 1) * payoff_per_tick <= scaled_cost);
    assert!(scaled_cost < tick * payoff_per_tick);
}

#[test]
fn actions_after_close_inputs_inherit_the_complete_outcomes_readiness() {
    let now = Instant::now();
    let grid = ScheduleGrid::new(now);
    let mut state = split_fixture(now, true)
        .with_batches(vec![vec![1], vec![0]])
        .unwrap();
    let start = schedule(state.decide(now)).unwrap();
    let execution = start.action.id().0;
    assert_eq!(start.action.action, Action::CloseInputs);
    let candidate = state.transactions()[&execution].candidate();
    assert!(candidate.ready_at(start.at, grid).unwrap() > start.at);

    state.apply(start.action);
    state.completed((execution, Action::CloseInputs), Ok(Update::Applied));

    let next = schedule(state.decide(start.at)).unwrap();
    assert_eq!(next.at, start.at);
    assert_eq!(
        next.action.id(),
        (execution, Action::AllocateChange(identity::change(0)))
    );
}

#[test]
fn a_partial_funding_does_not_admit_an_incomplete_outcome() {
    let now = Instant::now();
    let mut state = split_fixture(now, false)
        .with_batches(vec![vec![0], vec![1]])
        .unwrap();
    assert!(matches!(state.decide(now), Decision::Wait));
    assert!(state.transactions().is_empty());
    assert_eq!(state.planning_errors().len(), 1);
    assert_eq!(state.snapshot().coins.len(), 1);
    assert_eq!(state.pending().iter().count(), 2);
}

#[test]
fn initial_batches_reject_repeated_unknown_and_empty_allocations() {
    let now = Instant::now();
    for batches in [
        vec![vec![0], vec![0]],
        vec![vec![0, 0]],
        vec![vec![9]],
        vec![vec![]],
    ] {
        assert!(split_fixture(now, true).with_batches(batches).is_err());
    }
}

#[test]
fn evaluated_batches_cannot_be_reassigned() {
    let now = Instant::now();
    let mut state = split_fixture(now, true);
    let _decision = state.decide(now);
    assert!(state.with_batches(vec![vec![0], vec![1]]).is_err());
}

#[test]
fn invalidation_identifies_which_batch_lost_its_assumptions() {
    let now = Instant::now();
    let mut state = split_fixture(now, true)
        .with_batches(vec![vec![0], vec![1]])
        .unwrap();
    let _decision = state.decide(now);
    let ids: Vec<_> = state.transactions().keys().copied().collect();
    assert_eq!(ids.len(), 2);
    state.observe(Observation::TransactionInvalidated(
        ids[1],
        "input spent".into(),
    ));
    assert_eq!(state.transactions()[&ids[0]].status(), &Status::Running);
    assert!(
        matches!(state.transactions()[&ids[1]].status(), Status::Failed(err) if err.0 == "input spent")
    );
    assert!(matches!(state.decide(now), Decision::Wait));
    assert_eq!(state.pending().iter().count(), 2);
}

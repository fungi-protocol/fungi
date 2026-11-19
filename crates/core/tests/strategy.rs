//! Funding and construction use the same candidate and execution identity.

#[path = "support/scripted_driver.rs"]
mod scripted_driver;

use scripted_driver::run as run_scripted;

#[path = "support/signing.rs"]
mod signing;

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
        Action, Broadcaster, Error, Events, Executor, Observation, Operation, Request, Runtime,
        State, Status, Update,
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
    assert_eq!(scheduled.action.action, Action::Start);
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

/// Dispatch the request and return its operation.
fn dispatch(state: &mut Wallet, request: Request) -> Operation {
    let id = request.id();
    state.apply(request);
    state
        .operations()
        .find(|(operation, _)| *operation == id)
        .expect("the request was dispatched")
        .1
}

/// Dispatch one action of the candidate, execute it and complete it.
/// Panics if the action fails.
async fn run_action(state: &mut Wallet, io: &TestExecutor, start: &Request, action: Action) {
    let mut request = start.clone();
    request.action = action.clone();
    let operation = dispatch(state, request);
    let result = io.execute(&operation).await;
    state.completed(operation.id(), result);
    let errors = state.transactions()[&start.id().0].operation_errors();
    assert_eq!(errors.get(&action), None);
}

#[tokio::test]
async fn a_bound_execution_rejects_another_plan_but_accepts_its_replay() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    let first = dispatch(&mut state, start.clone());
    let io = executor();
    io.execute(&first).await.unwrap();
    let mut other_state = fixture(now, 1, true, true);
    let other = scheduled(&mut other_state, now);
    assert_eq!(first.id(), other.id());
    let other = dispatch(&mut other_state, other);
    assert_eq!(
        io.execute(&other).await.unwrap_err().0,
        "execution belongs to a different candidate"
    );
    assert_eq!(io.execute(&first).await, Ok(Update::Applied));
    state.completed(first.id(), Ok(Update::Applied));
    run_action(&mut state, &io, &start, Action::Input(0)).await;
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
        run_scripted(&mut state, &mut io, Operation::id).await,
        Stopped::Closed
    );

    assert_eq!(io.events.0, ready_at);
    let tx = &state.transactions()[&request.id().0];
    assert_eq!(tx.status(), &Status::AwaitingConfirmation);
    assert!(tx.completed().contains(&Action::Start));
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
            .contains(&Action::Start)
    );
}

#[tokio::test]
async fn a_failed_start_stops_the_candidates_actions() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let mut unrelated_state = fixture(now, 1, true, true);
    let unrelated = scheduled(&mut unrelated_state, now);
    let start = scheduled(&mut state, now);
    let executor = executor();
    executor
        .execute(&dispatch(&mut unrelated_state, unrelated))
        .await
        .unwrap();
    let mut io = Runtime {
        executor,
        events: Clock(now),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
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
    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    run_action(&mut state, &executor(), &start, Action::Start).await;
    let mut input = start.clone();
    input.action = Action::Input(0);
    let input = dispatch(&mut state, input);
    let io = executor();
    assert_eq!(
        io.execute(&input).await.unwrap_err().0,
        "transaction construction has not started"
    );
    let mut other = fixture(now, 1, true, true);
    let other_start = scheduled(&mut other, now);
    io.execute(&dispatch(&mut other, other_start))
        .await
        .unwrap();
}

#[tokio::test]
async fn closing_inputs_waits_for_the_input_receipt() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    let io = executor();
    run_action(&mut state, &io, &start, Action::Start).await;
    let mut input = start.clone();
    input.action = Action::Input(0);
    let input = dispatch(&mut state, input);
    // A dispatched contribution does not enable closing before its receipt.
    assert!(matches!(state.decide(now), Decision::Wait));
    let result = io.execute(&input).await;
    state.completed(input.id(), result);
    let next = schedule(state.decide(now)).unwrap();
    assert_eq!(next.action.action, Action::CloseInputs);
}

#[tokio::test]
async fn the_driver_contributes_and_closes_the_candidate_inputs() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let id = scheduled(&mut state, now).id().0;
    let mut io = Runtime {
        executor: executor(),
        events: Clock(now),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
        Stopped::Closed
    );

    let tx = &state.transactions()[&id];
    assert_eq!(tx.status(), &Status::AwaitingConfirmation);
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
        events: Clock(now),
    };

    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
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

#[tokio::test]
async fn rejected_change_is_not_added_and_its_allocation_runs_once() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    let io = executor_with_change(dummy::Change::new(
        script(3),
        Weight::from_wu(70),
        [Ok(ScriptBuf::new())],
    ));
    for action in [Action::Start, Action::Input(0), Action::CloseInputs] {
        run_action(&mut state, &io, &start, action).await;
    }
    let mut allocate = start.clone();
    allocate.action = Action::AllocateChange(0);
    let allocate = dispatch(&mut state, allocate);
    let result = io.execute(&allocate).await;
    assert_eq!(io.execute(&allocate.clone()).await, result);
    assert_eq!(io.change.lock().await.requests(), 1);
    state.completed(allocate.id(), result);

    let rejected = Error("allocated change differs from its priced template".into());
    let tx = &state.transactions()[&start.id().0];
    assert_eq!(tx.status(), &Status::Failed(rejected.clone()));
    assert_eq!(tx.operation_errors()[&Action::AllocateChange(0)], rejected);
    let mut output = start.clone();
    output.action = Action::Output(1);
    state.apply(output);
    assert!(state.operations().next().is_none());
    assert!(
        !state.transactions()[&start.id().0]
            .completed()
            .contains(&Action::Output(1))
    );
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
            run_scripted(&mut state, &mut io, Operation::id).await,
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
async fn completed_effects_belong_to_their_transaction() {
    let now = Instant::now();
    let io = executor();
    let mut allocations = Vec::new();
    for wallet in [1, 2] {
        let mut state = fixture(now, wallet, true, true);
        let start = scheduled(&mut state, now);
        for action in [Action::Start, Action::Input(0), Action::CloseInputs] {
            run_action(&mut state, &io, &start, action).await;
        }
        let mut allocate = start.clone();
        allocate.action = Action::AllocateChange(0);
        let allocate = dispatch(&mut state, allocate);
        assert!(matches!(io.execute(&allocate).await, Ok(Update::Change(_))));
        allocations.push(allocate);
    }
    assert_eq!(io.change.lock().await.requests(), 2);
    io.execute(&allocations[0]).await.unwrap();
    assert_eq!(io.change.lock().await.requests(), 2);
}

#[tokio::test]
async fn mismatched_effect_results_fail_the_operation() {
    let now = Instant::now();
    let unexpected = Error("unexpected effect result".into());
    let mut state = fixture(now, 1, true, true);
    let request = scheduled(&mut state, now);
    let start = dispatch(&mut state, request);
    state.completed(start.id(), Ok(Update::Change(script(4))));
    let tx = &state.transactions()[&start.id().0];
    assert_eq!(tx.operation_errors()[&Action::Start], unexpected);

    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    let io = executor();
    for action in [Action::Start, Action::Input(0), Action::CloseInputs] {
        run_action(&mut state, &io, &start, action).await;
    }
    let mut allocate = start.clone();
    allocate.action = Action::AllocateChange(0);
    let allocate = dispatch(&mut state, allocate);
    state.completed(allocate.id(), Ok(Update::Applied));
    let tx = &state.transactions()[&start.id().0];
    assert_eq!(
        tx.operation_errors()[&Action::AllocateChange(0)],
        unexpected
    );
    assert_eq!(io.change.lock().await.requests(), 0);
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
        run_scripted(&mut state, &mut io, Operation::id).await,
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
    let mut signs = Vec::new();
    for wallet in [1, 2] {
        let mut state = fixture(now, wallet, true, true);
        let mut request = scheduled(&mut state, now);
        for action in [
            Action::Start,
            Action::Input(0),
            Action::CloseInputs,
            Action::Output(0),
            Action::AllocateChange(0),
            Action::Output(1),
            Action::CloseOutputs,
        ] {
            request.action = action;
            let operation = dispatch(&mut state, request.clone());
            let result = io.execute(&operation).await;
            state.completed(operation.id(), result);
        }
        request.action = Action::Sign(0);
        signs.push(dispatch(&mut state, request));
    }
    let [first, second] = <[_; 2]>::try_from(signs).ok().unwrap();
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
fn actions_after_start_inherit_the_complete_outcomes_readiness() {
    let now = Instant::now();
    let grid = ScheduleGrid::new(now);
    let mut state = split_fixture(now, true)
        .with_batches(vec![vec![1], vec![0]])
        .unwrap();
    let start = schedule(state.decide(now)).unwrap();
    let execution = start.action.id().0;
    assert_eq!(start.action.action, Action::Start);
    let candidate = state.transactions()[&execution].candidate();
    assert!(candidate.ready_at(start.at, grid).unwrap() > start.at);

    state.apply(start.action);
    state.completed((execution, Action::Start), Ok(Update::Applied));

    let next = schedule(state.decide(start.at)).unwrap();
    assert_eq!(next.at, start.at);
    assert_eq!(next.action.id(), (execution, Action::Input(0)));
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

fn payment(ramp_seconds: u64) -> Intent {
    Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: script(2),
            amount: Amount::from_sat(10_000),
        }),
        PayoffCurve::new(
            0,
            [(ramp_seconds * SLOTS_PER_SECOND, Amount::from_sat(100_000))],
        )
        .unwrap(),
    )
}

/// Each transaction's execution sequence with the intent IDs it pays.
fn funded(state: &Wallet) -> Vec<(u64, Vec<u64>)> {
    state
        .transactions()
        .iter()
        .map(|(id, tx)| {
            (
                id.sequence,
                tx.candidate().payments().map(|(&id, _)| id).collect(),
            )
        })
        .collect()
}

#[test]
fn an_added_intent_joins_the_unstarted_candidate() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let first = schedule(state.decide(now)).unwrap();
    state.observe(Observation::Intent(payment(60)));
    assert_eq!(funded(&state), [(0, vec![0])]);

    let second = schedule(state.decide(now)).unwrap();
    assert_eq!(second.action.action, Action::Start);
    assert_eq!(funded(&state), [(1, vec![0, 1])]);
    assert_eq!(state.pending().iter().count(), 2);
    // A second payment shares the transaction's fixed cost and adds payoff.
    assert!(second.at < first.at);
}

#[test]
fn an_added_intent_merges_the_unstarted_batches() {
    let now = Instant::now();
    let mut state = split_fixture(now, true)
        .with_batches(vec![vec![0], vec![1]])
        .unwrap();
    let _decision = state.decide(now);
    assert_eq!(funded(&state), [(0, vec![0]), (1, vec![1])]);
    state.observe(Observation::Intent(payment(60)));
    let _decision = state.decide(now);
    assert_eq!(funded(&state), [(2, vec![0, 1, 2])]);
}

#[test]
fn a_started_transaction_keeps_its_intents_and_coins() {
    let now = Instant::now();
    let mut state = split_fixture(now, true);
    let start = schedule(state.decide(now)).unwrap().action;
    state.apply(start.clone());
    state.observe(Observation::Intent(payment(60)));
    let next = schedule(state.decide(now)).unwrap();

    assert_eq!(funded(&state), [(0, vec![0, 1]), (1, vec![2])]);
    assert_eq!(next.action.id().0.sequence, 1);
    assert_eq!(
        state.operations().map(|(id, _)| id).collect::<Vec<_>>(),
        [start.id()]
    );
    let inputs: Vec<_> = state
        .transactions()
        .values()
        .map(|tx| {
            tx.candidate()
                .inputs()
                .map(|coin| coin.outpoint())
                .collect::<Vec<_>>()
        })
        .collect();
    assert_eq!(inputs.iter().map(Vec::len).collect::<Vec<_>>(), [1, 1]);
    assert_ne!(inputs[0], inputs[1]);
}

#[test]
fn a_new_snapshot_restores_an_unfunded_candidate() {
    let now = Instant::now();
    let mut state = fixture(now, 1, false, true);
    assert!(matches!(state.decide(now), Decision::Wait));
    assert_eq!(state.planning_errors().len(), 1);

    let snapshot = fixture(now, 1, true, true).snapshot().clone();
    state.observe(Observation::Snapshot(snapshot.clone()));
    let request = scheduled(&mut state, now);
    assert!(state.planning_errors().is_empty());
    assert_eq!(request.id().0.sequence, 1);
    assert_eq!(state.snapshot(), &snapshot);
}

#[test]
fn a_feerate_change_funds_the_candidate_again() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let cost = |state: &Wallet, request: &Request| {
        state.transactions()[&request.id().0]
            .candidate()
            .objective_cost()
            .unwrap()
    };
    let first = scheduled(&mut state, now);
    let before = cost(&state, &first);
    state.observe(Observation::FeeRate(FeeRate::from_sat_per_vb(20).unwrap()));
    let second = scheduled(&mut state, now);
    assert_eq!(funded(&state), [(1, vec![0])]);
    assert!(cost(&state, &second) > before);
}

#[test]
fn new_information_replaces_an_invalidated_candidate() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let first = scheduled(&mut state, now);
    state.observe(Observation::Invalidated("coin spent elsewhere".into()));
    assert!(matches!(state.decide(now), Decision::Wait));

    // The invalidated coin is gone and another coin is available.
    let snapshot = fixture(now, 2, true, true).snapshot().clone();
    state.observe(Observation::Snapshot(snapshot));
    let second = scheduled(&mut state, now);
    assert_ne!(first.id(), second.id());
    assert_eq!(funded(&state), [(1, vec![0])]);
}

#[test]
fn repeated_information_keeps_the_candidate() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let first = schedule(state.decide(now)).unwrap();
    state.observe(Observation::Snapshot(state.snapshot().clone()));
    state.observe(Observation::FeeRate(FeeRate::from_sat_per_vb(2).unwrap()));
    let second = schedule(state.decide(now)).unwrap();
    assert_eq!(
        (second.at, second.action.id()),
        (first.at, first.action.id())
    );
    assert_eq!(funded(&state), [(0, vec![0])]);
}

#[test]
fn an_intent_left_out_of_the_batches_joins_the_pool() {
    let now = Instant::now();
    let mut state = split_fixture(now, true)
        .with_batches(vec![vec![0]])
        .unwrap();
    let _decision = state.decide(now);
    assert_eq!(funded(&state), [(0, vec![0])]);
    state.observe(Observation::Intent(payment(60)));
    let _decision = state.decide(now);
    assert_eq!(funded(&state), [(1, vec![0, 1, 2])]);
}

/// Start the first transaction of the split fixture, then add an intent that
/// another transaction funds.
fn two_outcomes(now: Instant) -> (Wallet, Request) {
    let mut state = split_fixture(now, true);
    let start = schedule(state.decide(now)).unwrap().action;
    state.apply(start.clone());
    state.completed(start.id(), Ok(Update::Applied));
    state.observe(Observation::Intent(payment(60)));
    let _decision = state.decide(now);
    assert_eq!(funded(&state), [(0, vec![0, 1]), (1, vec![2])]);
    (state, start)
}

#[test]
fn a_failure_stops_only_its_own_outcome() {
    let now = Instant::now();
    let (mut state, start) = two_outcomes(now);
    state.observe(Observation::TransactionInvalidated(
        start.id().0,
        "coin spent elsewhere".into(),
    ));
    let next = schedule(state.decide(now)).unwrap();
    assert_eq!(next.action.action, Action::Start);
    assert_eq!(next.action.id().0.sequence, 1);
}

#[test]
fn a_failed_outcome_keeps_its_intents_and_releases_its_coins() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    state.apply(start.clone());
    state.completed(start.id(), Ok(Update::Applied));
    state.observe(Observation::TransactionInvalidated(
        start.id().0,
        "coin spent elsewhere".into(),
    ));
    state.observe(Observation::Intent(payment(60)));
    let next = scheduled(&mut state, now);
    assert_eq!(funded(&state), [(0, vec![0]), (1, vec![1])]);
    let inputs = |id| {
        state.transactions()[&id]
            .candidate()
            .inputs()
            .map(|coin| coin.outpoint())
            .collect::<Vec<_>>()
    };
    assert_eq!(inputs(next.id().0), inputs(start.id().0));
}

#[test]
fn a_failed_start_keeps_its_attempt() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    state.apply(start.clone());
    let failure = Error("execution already bound".into());
    state.completed(start.id(), Err(failure.clone()));
    state.observe(Observation::Snapshot(
        fixture(now, 2, true, true).snapshot().clone(),
    ));
    assert!(matches!(state.decide(now), Decision::Wait));
    assert_eq!(funded(&state), [(0, vec![0])]);
    let tx = &state.transactions()[&start.id().0];
    assert_eq!(tx.operation_errors()[&Action::Start], failure);
    assert_eq!(tx.status(), &Status::Failed(failure));
}

#[test]
fn a_dust_intent_records_a_planning_error_without_panicking() {
    let now = Instant::now();
    let dust = Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: script(2),
            amount: Amount::ONE_SAT,
        }),
        PayoffCurve::new(0, [(60 * SLOTS_PER_SECOND, Amount::from_sat(100_000))]).unwrap(),
    );
    let mut state = State::new(
        InMemoryQueue::new([dust]),
        fixture(now, 1, true, false).snapshot().clone(),
        FeeRate::from_sat_per_vb(2).unwrap(),
        [1; 16],
        dummy::Change::new(script(3), Weight::from_wu(70), []),
        StdRng::seed_from_u64(0),
        ScheduleGrid::new(now),
    );
    assert!(matches!(state.decide(now), Decision::Wait));
    assert_eq!(state.planning_errors().len(), 1);
    assert!(
        state
            .planning_errors()
            .values()
            .next()
            .unwrap()
            .0
            .contains("DustOutput")
    );
}

#[tokio::test]
async fn a_signed_failed_outcome_attaches_replacement_liability_instead_of_locking_its_coin() {
    let now = Instant::now();
    let mut state = fixture(now, 1, true, true);
    let start = scheduled(&mut state, now);
    let baseline_cost = state.transactions()[&start.id().0]
        .candidate()
        .objective_cost()
        .unwrap();
    let io = executor();
    for action in [
        Action::Start,
        Action::Input(0),
        Action::CloseInputs,
        Action::Output(0),
        Action::AllocateChange(0),
        Action::Output(1),
        Action::CloseOutputs,
        Action::Sign(0),
    ] {
        run_action(&mut state, &io, &start, action).await;
    }
    state.observe(Observation::TransactionInvalidated(
        start.id().0,
        "broadcast peer dropped".into(),
    ));
    // Add another 10,000-sat intent: the signed coin is NOT locked, so it funds
    // the new intent, but its objective cost increases by the RBF replacement liability.
    state.observe(Observation::Intent(payment(60)));
    let next = scheduled(&mut state, now);
    let replacement = &state.transactions()[&next.id().0];
    let coin = replacement.candidate().inputs().next().unwrap();
    assert!(coin.liability() > Amount::ZERO);
    assert_eq!(
        replacement.candidate().objective_cost().unwrap(),
        baseline_cost + coin.liability().to_signed().unwrap()
    );
}

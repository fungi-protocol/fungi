//! Deterministic signing and finalization exercise the driver.

#[path = "support/scripted_driver.rs"]
mod scripted_driver;

use scripted_driver::run as run_scripted;

#[path = "support/signing.rs"]
mod signing;
use bitcoin::{Amount, FeeRate, OutPoint, ScriptBuf, TxOut, Txid, Weight, Witness, hashes::Hash};
use cost_function::{
    intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve, ScheduleGrid},
    queue::{InMemoryQueue, Queue},
};
use fungi_core::{
    driver::{Decision, Machine, Stopped, Wake},
    strategy::{Action, Events, Executor, Observation, Request, Runtime, State, Status},
    wallet::{Coin, Snapshot, dummy},
};
use rand::{SeedableRng, rngs::StdRng};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Instant,
};

struct Clock {
    now: Instant,
}
impl Events for Clock {
    fn now(&self) -> Instant {
        self.now
    }
    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        if let Some(at) = deadline {
            self.now = self.now.max(at);
            Wake::Timeout
        } else {
            Wake::Closed
        }
    }
}
type TestRuntime = Runtime<dummy::Change, signing::Signer, Clock>;
fn script(byte: u8) -> ScriptBuf {
    ScriptBuf::from_bytes([vec![0x51, 0x20], vec![byte; 32]].concat())
}
type TestState = State<dummy::Change, StdRng>;
fn fixture() -> (TestState, TestRuntime) {
    fixture_with_funding(false, 10_000)
}

fn fixture_with_funding(two_coins: bool, amount: u64) -> (TestState, TestRuntime) {
    fixture_with_pricing(two_coins, amount, Weight::from_wu(70))
}

fn fixture_with_pricing(
    two_coins: bool,
    amount: u64,
    input_weight: Weight,
) -> (TestState, TestRuntime) {
    let (state, io) = fixture_without_signers(two_coins, amount, input_weight);
    for &outpoint in state.snapshot().coins.keys() {
        assert!(
            io.executor
                .signers
                .register(signing::Signer::new(outpoint, [Ok(())]))
        );
    }
    (state, io)
}

fn fixture_without_signers(
    two_coins: bool,
    amount: u64,
    input_weight: Weight,
) -> (TestState, TestRuntime) {
    let template = script(2);
    let now = Instant::now();
    let outpoint = OutPoint::new(Txid::all_zeros(), 0);
    let mut snapshot = Snapshot {
        coins: BTreeMap::from([(
            outpoint,
            Coin {
                txout: TxOut {
                    value: Amount::from_sat(50_000),
                    script_pubkey: signing::script(outpoint),
                },
                satisfaction_weight: input_weight,
            },
        )]),
        ..Default::default()
    };
    if two_coins {
        let second = OutPoint::new(Txid::all_zeros(), 1);
        snapshot.coins.insert(
            second,
            Coin {
                txout: TxOut {
                    value: Amount::from_sat(50_000),
                    script_pubkey: signing::script(second),
                },
                satisfaction_weight: input_weight,
            },
        );
    }
    let change = dummy::Change::new(template.clone(), Weight::from_wu(70), [Ok(script(3))]);
    let grid = ScheduleGrid::new(now);
    let intent = Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: script(4),
            amount: Amount::from_sat(amount),
        }),
        PayoffCurve::new(0, [(0, Amount::from_sat(100_000))]).unwrap(),
    );
    let queue = InMemoryQueue::new([intent.clone(), intent]);
    let feerate = FeeRate::from_sat_per_vb(2).unwrap();
    let state = State::new(
        queue,
        snapshot,
        feerate,
        [1; 16],
        dummy::Change::new(template, Weight::from_wu(70), []),
        StdRng::seed_from_u64(0),
        grid,
    );
    let executor = Executor::new(change, vec![]).unwrap();
    (
        state,
        Runtime {
            executor,
            events: Clock { now },
        },
    )
}

fn scheduled_request(state: &mut TestState, now: Instant) -> Option<Request> {
    match state.decide(now) {
        Decision::Schedule(next) => Some(next.action),
        Decision::Wait | Decision::Done => None,
    }
}

fn next_request(state: &mut TestState, now: Instant) -> Request {
    scheduled_request(state, now).expect("expected an action")
}

async fn execute(
    io: &mut TestRuntime,
    request: &Request,
    action: Action,
) -> Result<fungi_core::strategy::Update, fungi_core::strategy::Error> {
    let mut request = request.clone();
    request.action = action;
    io.executor.execute(&request).await
}

async fn pending_action(state: &mut TestState, io: &TestRuntime, action: Action) -> Request {
    let mut requests = BTreeSet::new();
    loop {
        let request = next_request(state, io.events.now);
        let id = request.id();
        assert!(
            requests.insert(id.clone()),
            "repeated scripted request {id:?}"
        );
        state.apply(request.clone());
        if request.action == action {
            return request;
        }
        let result = io.executor.execute(&request).await;
        state.completed(id, result);
    }
}

#[tokio::test]
async fn a_late_signature_failure_preserves_the_first_failure() {
    let (mut state, io) = fixture_with_funding(true, 30_000);
    let first = pending_action(&mut state, &io, Action::Sign(0)).await;
    let second = pending_action(&mut state, &io, Action::Sign(1)).await;
    assert_eq!(state.operations().count(), 2);

    let first_error = fungi_core::strategy::Error("first signer failed".into());
    let second_error = fungi_core::strategy::Error("second signer failed".into());
    state.completed(first.id(), Err(first_error.clone()));
    state.completed(second.id(), Err(second_error.clone()));

    let tx = &state.transactions()[&first.id().0];
    assert_eq!(tx.status(), &Status::Failed(first_error.clone()));
    assert_eq!(
        tx.operation_errors(),
        &BTreeMap::from([
            (Action::Sign(0), first_error),
            (Action::Sign(1), second_error)
        ])
    );
    assert!(tx.pending().next().is_none());
    assert!(tx.payment().is_none());
}

#[tokio::test]
async fn only_finalization_can_complete_with_a_payment() {
    let (mut state, io) = fixture();
    let finalize = pending_action(&mut state, &io, Action::Finalize).await;
    let payment = io.executor.execute(&finalize).await.unwrap();
    assert!(matches!(
        payment,
        fungi_core::strategy::Update::Finalized(_)
    ));

    state.completed(finalize.id(), Ok(fungi_core::strategy::Update::Applied));
    let tx = &state.transactions()[&finalize.id().0];
    let error = fungi_core::strategy::Error("finalization did not return a transaction".into());
    assert_eq!(tx.status(), &Status::Failed(error.clone()));
    assert_eq!(
        tx.operation_errors(),
        &BTreeMap::from([(Action::Finalize, error)])
    );
    assert!(tx.payment().is_none());

    let (mut state, io) = fixture();
    let start = pending_action(&mut state, &io, Action::Start).await;
    state.completed(start.id(), Ok(payment));
    let tx = &state.transactions()[&start.id().0];
    let error = fungi_core::strategy::Error("unexpected effect result".into());
    assert_eq!(tx.status(), &Status::Failed(error.clone()));
    assert_eq!(
        tx.operation_errors(),
        &BTreeMap::from([(Action::Start, error)])
    );
    assert!(tx.payment().is_none());
}

#[tokio::test]
async fn constructs_and_finalizes_a_verified_payment() {
    let (mut state, mut io) = fixture();
    let request = next_request(&mut state, io.events.now);
    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );
    let payment = state
        .transactions()
        .values()
        .next()
        .unwrap()
        .payment()
        .unwrap();
    assert_eq!(payment.outputs.len(), 2);
    assert_ne!(payment.outputs[&0], payment.outputs[&1]);
    for &position in payment.outputs.values() {
        assert_eq!(
            payment.transaction.output[position].value,
            Amount::from_sat(10_000)
        );
    }
    assert_eq!(io.executor.change.lock().await.requests(), 1);
    let outpoint = OutPoint::new(Txid::all_zeros(), 0);
    let signer = io.executor.signers.get(&outpoint).unwrap();
    let signer = signer.lock().await;
    assert_eq!(signer.requests().len(), 1);
    let unsigned = &signer.requests()[0];
    let mut expected = unsigned.unsigned_tx.clone();
    expected.input[0].witness = signing::sign(unsigned, outpoint)
        .final_script_witness
        .unwrap();
    assert_eq!(payment.transaction, expected);
    assert_eq!(
        payment.fee,
        Amount::from_sat(50_000)
            - payment
                .transaction
                .output
                .iter()
                .map(|o| o.value)
                .sum::<Amount>()
    );
    drop(signer);

    // Replaying the action returns the same receipt without requesting another signature.
    execute(&mut io, &request, Action::Sign(0)).await.unwrap();
    let result = execute(&mut io, &request, Action::Finalize).await.unwrap();
    assert!(
        matches!(result, fungi_core::strategy::Update::Finalized(replay) if replay == *payment)
    );
    assert_eq!(
        io.executor
            .signers
            .get(&outpoint)
            .unwrap()
            .lock()
            .await
            .requests()
            .len(),
        1
    );
    assert!(scheduled_request(&mut state, io.events.now).is_none());
}

#[tokio::test]
async fn signing_and_finalization_require_their_predecessors() {
    let (mut state, mut io) = fixture();
    let request = next_request(&mut state, io.events.now);
    execute(&mut io, &request, Action::Start).await.unwrap();
    assert_eq!(
        execute(&mut io, &request, Action::Sign(0))
            .await
            .unwrap_err()
            .0,
        "transaction has not been frozen and validated"
    );
    assert_eq!(
        execute(&mut io, &request, Action::Finalize)
            .await
            .unwrap_err()
            .0,
        "signatures are incomplete"
    );
    assert_eq!(
        execute(&mut io, &request, Action::Sign(1))
            .await
            .unwrap_err()
            .0,
        "unknown input"
    );
    assert!(
        io.executor
            .signers
            .get(&OutPoint::new(Txid::all_zeros(), 0))
            .unwrap()
            .lock()
            .await
            .requests()
            .is_empty()
    );
}

#[tokio::test]
async fn a_signing_failure_is_not_payment_completion() {
    let (mut state, mut io) = fixture();
    *io.executor
        .signers
        .get(&OutPoint::new(Txid::all_zeros(), 0))
        .unwrap()
        .lock()
        .await = signing::Signer::new(
        OutPoint::new(Txid::all_zeros(), 0),
        [Err(dummy::Error::Scripted("sign failed"))],
    );
    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );
    assert!(matches!(
        state.transactions().values().next().unwrap().status(),
        Status::Failed(err) if err.0 == "sign failed"
    ));
    assert!(
        state
            .transactions()
            .values()
            .next()
            .unwrap()
            .payment()
            .is_none()
    );
    assert_eq!(state.pending().iter().count(), 2);
}

#[tokio::test]
async fn a_same_length_invalid_signature_cannot_be_finalized() {
    let (mut state, mut io) = fixture();
    io.executor
        .signers
        .get(&OutPoint::new(Txid::all_zeros(), 0))
        .unwrap()
        .lock()
        .await
        .corrupt = true;
    run_scripted(&mut state, &mut io, Request::id).await;
    assert!(matches!(
        state.transactions().values().next().unwrap().status(),
        Status::Failed(_)
    ));
    assert!(
        state
            .transactions()
            .values()
            .next()
            .unwrap()
            .payment()
            .is_none()
    );
}

#[tokio::test]
async fn a_closed_signer_channel_fails_the_pending_signature() {
    let (mut state, mut io) = fixture();
    let (send, recv) = futures::channel::oneshot::channel();
    let outpoint = OutPoint::new(Txid::all_zeros(), 0);
    io.executor
        .signers
        .get(&outpoint)
        .unwrap()
        .lock()
        .await
        .delay = Some(recv);
    drop(send);

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = state.transactions().values().next().unwrap();
    assert!(matches!(tx.status(), Status::Failed(err) if err.0 == "signer channel closed"));
    assert!(!tx.completed().contains(&Action::Sign(0)));
    assert!(tx.payment().is_none());
}

#[tokio::test]
async fn a_signer_registered_later_is_visible_to_an_existing_executor_clone() {
    let (mut state, mut io) = fixture_without_signers(false, 10_000, Weight::from_wu(70));
    let executor = io.executor.clone();
    let outpoint = OutPoint::new(Txid::all_zeros(), 0);
    assert!(executor.signers.get(&outpoint).is_none());

    assert!(
        io.executor
            .signers
            .register(signing::Signer::new(outpoint, [Ok(())]))
    );
    io.executor = executor;
    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = state.transactions().values().next().unwrap();
    assert!(tx.completed().contains(&Action::Sign(0)));
    assert!(tx.payment().is_some());
}

#[tokio::test]
async fn duplicate_signer_registration_keeps_the_original_usable() {
    let (mut state, mut io) = fixture();
    let outpoint = OutPoint::new(Txid::all_zeros(), 0);
    let duplicate = signing::Signer::new(outpoint, [Err(dummy::Error::Scripted("duplicate"))]);
    assert!(!io.executor.signers.register(duplicate));

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );
    let tx = state.transactions().values().next().unwrap();
    assert!(tx.completed().contains(&Action::Sign(0)));
    assert!(tx.payment().is_some());
}

#[tokio::test]
async fn an_input_without_a_signer_cannot_be_finalized() {
    let (mut state, mut io) = fixture_without_signers(false, 10_000, Weight::from_wu(70));

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = state.transactions().values().next().unwrap();
    assert!(matches!(tx.status(), Status::Failed(err) if err.0 == "no signer for input"));
    assert!(tx.completed().contains(&Action::CloseOutputs));
    assert!(tx.payment().is_none());
}

#[tokio::test]
async fn signing_results_must_preserve_the_prevout_and_finalize_a_key_spend() {
    type Edit = fn(&mut bitcoin::psbt::Input);
    let cases: [(Edit, &str); 4] = [
        (
            |input| input.witness_utxo = Some(TxOut::NULL),
            "signer changed the prevout",
        ),
        (
            |input| input.final_script_witness = None,
            "signer did not finalize the input",
        ),
        (
            |input| input.final_script_witness = Some(Witness::from_slice(&[[0; 65]])),
            "expected a default-sighash P2TR key-spend witness",
        ),
        (
            |input| input.final_script_witness = Some(Witness::from_slice(&[[0; 64]; 2])),
            "expected a default-sighash P2TR key-spend witness",
        ),
    ];
    for (edit, reason) in cases {
        let (mut state, mut io) = fixture();
        let outpoint = OutPoint::new(Txid::all_zeros(), 0);
        io.executor
            .signers
            .get(&outpoint)
            .unwrap()
            .lock()
            .await
            .edit = Some(edit);

        assert_eq!(
            run_scripted(&mut state, &mut io, Request::id).await,
            Stopped::Closed
        );

        let tx = state.transactions().values().next().unwrap();
        assert!(matches!(tx.status(), Status::Failed(err) if err.0 == reason));
        assert!(!tx.completed().contains(&Action::Sign(0)));
        assert!(tx.payment().is_none());
    }
}

#[tokio::test]
async fn underestimated_input_weight_can_prevent_finalization() {
    let (mut state, mut io) = fixture_with_pricing(false, 10_000, Weight::ZERO);

    assert_eq!(
        run_scripted(&mut state, &mut io, Request::id).await,
        Stopped::Closed
    );

    let tx = state.transactions().values().next().unwrap();
    assert!(matches!(
        tx.status(),
        Status::Failed(err) if err.0 == "signed transaction is below the requested feerate"
    ));
    assert!(tx.completed().contains(&Action::Sign(0)));
    assert!(tx.payment().is_none());
}

#[tokio::test]
async fn two_inputs_of_one_transaction_can_be_signed_concurrently() {
    use futures::channel::oneshot;
    let (mut state, mut io) = fixture_with_funding(true, 30_000);
    let (send, recv) = oneshot::channel();
    io.executor
        .signers
        .get(&OutPoint::new(Txid::all_zeros(), 0))
        .unwrap()
        .lock()
        .await
        .delay = Some(recv);
    let mut held = None;
    let mut requests = BTreeSet::new();
    while let Decision::Schedule(scheduled) = state.decide(io.events.now) {
        let request = scheduled.action;
        state.apply(request.clone());
        let executor = io.executor.clone();
        let id = request.id();
        assert!(
            requests.insert(id.clone()),
            "repeated scripted request {id:?}"
        );
        let mut future = Box::pin(async move { executor.execute(&request).await });
        match futures::poll!(&mut future) {
            std::task::Poll::Pending => {
                assert!(held.is_none());
                held = Some((id, future));
            }
            std::task::Poll::Ready(result) => state.completed(id, result),
        }
    }
    assert_eq!(state.transactions().len(), 1);
    assert_eq!(state.operations().count(), 1);
    for outpoint in state.snapshot().coins.keys() {
        let signer = io.executor.signers.get(outpoint).unwrap();
        assert_eq!(signer.lock().await.requests().len(), 1);
    }
    assert!(
        state
            .transactions()
            .values()
            .next()
            .unwrap()
            .payment()
            .is_none()
    );
    send.send(()).unwrap();
    let (id, future) = held.unwrap();
    state.completed(id, future.await);
    run_scripted(&mut state, &mut io, Request::id).await;
    let transaction = state.transactions().values().next().unwrap();
    assert_eq!(transaction.payment().unwrap().transaction.input.len(), 2);
}

//! Deterministic signing and simulated chain observations exercise the driver.

#[path = "support/scripted_driver.rs"]
mod scripted_driver;

use scripted_driver::run as run_scripted;

#[path = "support/signing.rs"]
mod signing;
use bitcoin::{
    Amount, FeeRate, OutPoint, ScriptBuf, Transaction, TxOut, Txid, Weight, Witness, hashes::Hash,
};
use cost_function::{
    intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve, ScheduleGrid},
    queue::{InMemoryQueue, Queue},
};
use fungi_core::{
    driver::{Decision, Machine, Stopped, Wake},
    strategy::{
        Action, Broadcaster, Events, Executor, Observation, Operation, Request, Runtime, State,
        Status,
    },
    wallet::{Coin, Snapshot, dummy},
};
use rand::{SeedableRng, rngs::StdRng};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
    time::Instant,
};

type Inbox = Arc<Mutex<VecDeque<Observation>>>;
struct Chain {
    inbox: Inbox,
    published: Vec<Transaction>,
    fail: bool,
    confirm: bool,
}
impl Broadcaster for Chain {
    type Error = &'static str;
    fn broadcast(
        &mut self,
        tx: &Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + use<> {
        let result = (|| {
            if self.fail {
                return Err("scripted broadcast failure");
            }
            self.published.push(tx.clone());
            if self.confirm {
                self.inbox
                    .lock()
                    .unwrap()
                    .push_back(Observation::Confirmed(tx.compute_txid()));
            }
            Ok(())
        })();
        std::future::ready(result)
    }
}
struct Clock {
    now: Instant,
    inbox: Inbox,
}
impl Events for Clock {
    fn now(&self) -> Instant {
        self.now
    }
    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        if let Some(event) = self.inbox.lock().unwrap().pop_front() {
            return Wake::Input(event);
        }
        if let Some(at) = deadline {
            self.now = self.now.max(at);
            Wake::Timeout
        } else {
            Wake::Closed
        }
    }
}
type TestRuntime = Runtime<dummy::Change, signing::Signer, dummy::Recorder, Chain, Clock>;
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
    let change = dummy::Change::new(
        template.clone(),
        Weight::from_wu(70),
        [Ok(script(3)), Ok(script(5))],
    );
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
    let inbox = Inbox::default();
    let executor = Executor::new(
        change,
        vec![],
        dummy::Recorder::new([Ok(()), Ok(())]),
        Chain {
            inbox: inbox.clone(),
            published: vec![],
            fail: false,
            confirm: true,
        },
    )
    .unwrap();
    (
        state,
        Runtime {
            executor,
            events: Clock { now, inbox },
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

fn dispatch(state: &mut TestState, request: Request) -> Operation {
    let id = request.id();
    state.apply(request);
    state
        .operations()
        .find(|(operation, _)| *operation == id)
        .expect("the request was dispatched")
        .1
}

/// Execute and complete scheduled operations until the action is dispatched.
/// Returns the dispatched operation without executing it.
async fn pending_action(state: &mut TestState, io: &TestRuntime, action: Action) -> Operation {
    let mut requests = BTreeSet::new();
    loop {
        let request = next_request(state, io.events.now);
        let id = request.id();
        assert!(
            requests.insert(id.clone()),
            "repeated scripted request {id:?}"
        );
        let operation = dispatch(state, request);
        if id.1 == action {
            return operation;
        }
        let result = io.executor.execute(&operation).await;
        state.completed(id, result);
    }
}

/// Run every scheduled operation and return clones of those that completed.
async fn run_retaining(state: &mut TestState, io: &mut TestRuntime) -> Vec<Operation> {
    let mut operations = vec![];
    while let Some(request) = scheduled_request(state, io.events.now) {
        let operation = dispatch(state, request);
        let result = io.executor.execute(&operation).await;
        state.completed(operation.id(), result);
        operations.push(operation);
        let observations: Vec<_> = io.events.inbox.lock().unwrap().drain(..).collect();
        for observation in observations {
            state.observe(observation);
        }
    }
    operations
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
async fn constructs_and_finalizes_a_verified_payment() {
    let (mut state, mut io) = fixture();
    let operations = run_retaining(&mut state, &mut io).await;
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

    // Replaying an operation returns its receipt without requesting another signature.
    let sign = operations
        .iter()
        .find(|operation| operation.id().1 == Action::Sign(0))
        .unwrap();
    let signed = io.executor.execute(sign).await.unwrap();
    assert!(matches!(signed, fungi_core::strategy::Update::Signed(_)));
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
        run_scripted(&mut state, &mut io, Operation::id).await,
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
    run_scripted(&mut state, &mut io, Operation::id).await;
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
    assert!(io.executor.recorder.lock().await.requests().is_empty());
    assert!(io.executor.broadcaster.lock().await.published.is_empty());
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
        run_scripted(&mut state, &mut io, Operation::id).await,
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
        run_scripted(&mut state, &mut io, Operation::id).await,
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
        run_scripted(&mut state, &mut io, Operation::id).await,
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
        run_scripted(&mut state, &mut io, Operation::id).await,
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
            run_scripted(&mut state, &mut io, Operation::id).await,
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
        run_scripted(&mut state, &mut io, Operation::id).await,
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
        let id = scheduled.action.id();
        let operation = dispatch(&mut state, scheduled.action);
        let executor = io.executor.clone();
        assert!(
            requests.insert(id.clone()),
            "repeated scripted request {id:?}"
        );
        let mut future = Box::pin(async move { executor.execute(&operation).await });
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
    assert!(io.executor.recorder.lock().await.requests().is_empty());
    send.send(()).unwrap();
    let (id, future) = held.unwrap();
    state.completed(id, future.await);
    run_scripted(&mut state, &mut io, Operation::id).await;
    let transaction = state.transactions().values().next().unwrap();
    assert_eq!(transaction.payment().unwrap().transaction.input.len(), 2);
    assert_eq!(transaction.status(), &Status::Confirmed);
    assert_eq!(io.executor.recorder.lock().await.requests().len(), 1);
}

#[tokio::test]
async fn confirmation_leaves_the_wallet_waiting_for_information() {
    let (mut state, mut io) = fixture();
    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
        Stopped::Closed
    );
    assert_eq!(
        *state.transactions().values().next().unwrap().status(),
        Status::Confirmed
    );
    assert_eq!(state.pending().iter().count(), 0);
    assert!(matches!(state.decide(io.events.now), Decision::Wait));
}

#[tokio::test]
async fn constructs_signs_retains_publishes_and_observes_the_payment() {
    let (mut state, mut io) = fixture();
    let operations = run_retaining(&mut state, &mut io).await;
    assert_eq!(
        *state.transactions().values().next().unwrap().status(),
        Status::Confirmed
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
    assert_eq!(
        io.executor
            .signers
            .get(&OutPoint::new(Txid::all_zeros(), 0))
            .unwrap()
            .lock()
            .await
            .requests()
            .len(),
        1
    );
    assert_eq!(
        io.executor.recorder.lock().await.recorded(),
        std::slice::from_ref(&payment.transaction)
    );
    assert_eq!(
        io.executor.broadcaster.lock().await.published,
        std::slice::from_ref(&payment.transaction)
    );
    assert_eq!(
        state
            .transactions()
            .values()
            .next()
            .unwrap()
            .completed()
            .last(),
        Some(&Action::Publish)
    );
    // Replaying completed operations makes no new wallet request or broadcast.
    for action in [Action::AllocateChange(0), Action::Record, Action::Publish] {
        let operation = operations
            .iter()
            .find(|operation| operation.id().1 == action)
            .unwrap();
        io.executor.execute(operation).await.unwrap();
    }
    assert_eq!(io.executor.recorder.lock().await.requests().len(), 1);
    assert_eq!(io.executor.change.lock().await.requests(), 1);
    assert_eq!(io.executor.broadcaster.lock().await.published.len(), 1);
    assert_eq!(state.pending().iter().count(), 0);
}

#[tokio::test]
async fn a_recording_failure_prevents_publication() {
    let (mut state, mut io) = fixture();
    *io.executor.recorder.lock().await =
        dummy::Recorder::new([Err(dummy::Error::Scripted("record failed"))]);
    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
        Stopped::Closed
    );
    assert!(matches!(
        state.transactions().values().next().unwrap().status(),
        Status::Failed(_)
    ));
    assert!(io.executor.broadcaster.lock().await.published.is_empty());
    assert!(
        !state
            .transactions()
            .values()
            .next()
            .unwrap()
            .completed()
            .contains(&Action::Record)
    );
}

#[tokio::test]
async fn failed_broadcast_retains_the_transaction_without_fulfilling_the_intents() {
    let (mut state, mut io) = fixture();
    io.executor.broadcaster.lock().await.fail = true;
    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
        Stopped::Closed
    );
    assert!(matches!(
        state.transactions().values().next().unwrap().status(),
        Status::Failed(_)
    ));
    assert_eq!(io.executor.recorder.lock().await.recorded().len(), 1);
    assert_eq!(state.pending().iter().count(), 2);
    assert!(matches!(state.decide(io.events.now), Decision::Wait));
    assert!(
        !state
            .transactions()
            .values()
            .next()
            .unwrap()
            .completed()
            .contains(&Action::Publish)
    );
}

#[tokio::test]
async fn invalidated_candidate_has_no_effects() {
    let (mut state, mut io) = fixture();
    io.events
        .inbox
        .lock()
        .unwrap()
        .push_back(Observation::Invalidated("coin spent elsewhere".into()));
    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
        Stopped::Closed
    );
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
            .completed()
            .is_empty()
    );
    assert_eq!(io.executor.change.lock().await.requests(), 0);
}

#[tokio::test]
async fn broadcast_acceptance_waits_for_a_matching_confirmation() {
    let (mut state, mut io) = fixture();
    io.executor.broadcaster.lock().await.confirm = false;
    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
        Stopped::Closed
    );
    assert_eq!(
        *state.transactions().values().next().unwrap().status(),
        Status::AwaitingConfirmation
    );
    assert_eq!(state.pending().iter().count(), 2);
    io.events
        .inbox
        .lock()
        .unwrap()
        .push_back(Observation::Confirmed(Txid::all_zeros()));
    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
        Stopped::Closed
    );
    assert_eq!(
        *state.transactions().values().next().unwrap().status(),
        Status::AwaitingConfirmation
    );
    assert_eq!(state.pending().iter().count(), 2);
    let txid = state
        .transactions()
        .values()
        .next()
        .unwrap()
        .payment()
        .unwrap()
        .transaction
        .compute_txid();
    io.events
        .inbox
        .lock()
        .unwrap()
        .push_back(Observation::Confirmed(txid));
    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
        Stopped::Closed
    );
    assert_eq!(
        *state.transactions().values().next().unwrap().status(),
        Status::Confirmed
    );
    assert_eq!(io.executor.broadcaster.lock().await.published.len(), 1);
    assert_eq!(state.pending().iter().count(), 0);
}

async fn pending_publication() -> (TestState, TestRuntime, Operation) {
    let (mut state, io) = fixture();
    let request = pending_action(&mut state, &io, Action::Publish).await;
    (state, io, request)
}

#[tokio::test]
async fn confirmation_can_arrive_before_the_publication_result() {
    let (mut state, io, request) = pending_publication().await;
    let txid = state
        .transactions()
        .values()
        .next()
        .unwrap()
        .payment()
        .unwrap()
        .transaction
        .compute_txid();
    state.observe(Observation::Confirmed(txid));
    assert_eq!(
        *state.transactions().values().next().unwrap().status(),
        Status::Confirmed
    );
    assert_eq!(state.pending().iter().count(), 0);
    // Even a failed RPC response cannot undo an observed chain confirmation.
    let publication_error = fungi_core::strategy::Error("RPC disconnected".into());
    state.completed(request.id(), Err(publication_error.clone()));
    let tx = &state.transactions()[&request.id().0];
    assert_eq!(tx.status(), &Status::Confirmed);
    assert_eq!(
        tx.operation_errors(),
        &BTreeMap::from([(Action::Publish, publication_error)])
    );
    assert!(state.operations().next().is_none());
    assert!(matches!(state.decide(io.events.now), Decision::Wait));
}

#[tokio::test]
async fn a_late_publication_result_preserves_invalidation() {
    let (mut state, io, request) = pending_publication().await;
    state.observe(Observation::Invalidated("candidate superseded".into()));
    assert_eq!(state.operations().count(), 1);
    state.completed(request.id(), Ok(fungi_core::strategy::Update::Applied));
    assert_eq!(
        *state.transactions().values().next().unwrap().status(),
        Status::Failed(fungi_core::strategy::Error("candidate superseded".into()))
    );
    assert!(
        state
            .transactions()
            .values()
            .next()
            .unwrap()
            .completed()
            .contains(&Action::Publish)
    );
    assert_eq!(state.pending().iter().count(), 2);
    assert!(matches!(state.decide(io.events.now), Decision::Wait));
}

#[tokio::test]
async fn late_success_cannot_undo_confirmation() {
    let (mut state, io, request) = pending_publication().await;
    let txid = state
        .transactions()
        .values()
        .next()
        .unwrap()
        .payment()
        .unwrap()
        .transaction
        .compute_txid();
    state.observe(Observation::Confirmed(txid));
    state.completed(request.id(), Ok(fungi_core::strategy::Update::Applied));
    assert_eq!(
        *state.transactions().values().next().unwrap().status(),
        Status::Confirmed
    );
    assert!(matches!(state.decide(io.events.now), Decision::Wait));
    assert_eq!(state.pending().iter().count(), 0);
}

#[tokio::test]
async fn a_stale_action_cannot_undo_confirmation() {
    let (mut state, mut io) = fixture();
    let request = next_request(&mut state, io.events.now);
    run_scripted(&mut state, &mut io, Operation::id).await;
    state.apply(request);
    assert_eq!(
        *state.transactions().values().next().unwrap().status(),
        Status::Confirmed
    );
    assert!(state.operations().next().is_none());
}

#[tokio::test]
async fn a_late_failure_preserves_the_invalidation_reason() {
    let (mut state, _, request) = pending_publication().await;
    state.observe(Observation::Invalidated("candidate superseded".into()));
    let publication_error = fungi_core::strategy::Error("RPC disconnected".into());
    state.completed(request.id(), Err(publication_error.clone()));
    let tx = &state.transactions()[&request.id().0];
    assert_eq!(
        tx.status(),
        &Status::Failed(fungi_core::strategy::Error("candidate superseded".into()))
    );
    assert_eq!(
        tx.operation_errors(),
        &BTreeMap::from([(Action::Publish, publication_error)])
    );
    assert!(state.operations().next().is_none());
}
#[test]
fn invalidation_before_planning_keeps_the_intents_pending() {
    let (mut state, io) = fixture();

    state.observe(Observation::Invalidated("coin already spent".into()));

    assert!(matches!(state.decide(io.events.now), Decision::Wait));
    assert!(state.transactions().is_empty());
    assert_eq!(state.pending().iter().count(), 2);
    assert_eq!(
        state.planning_errors().values().next().unwrap().0,
        "coin already spent"
    );
}

#[tokio::test]
async fn confirmation_recovers_a_failed_broadcast() {
    let (mut state, mut io) = fixture();
    io.executor.broadcaster.lock().await.fail = true;
    assert_eq!(
        run_scripted(&mut state, &mut io, Operation::id).await,
        Stopped::Closed
    );
    let tx = state.transactions().values().next().unwrap();
    assert!(matches!(tx.status(), Status::Failed(_)));
    let txid = tx.payment().unwrap().transaction.compute_txid();
    assert!(state.snapshot().recorded.contains_key(&txid));
    assert_eq!(state.snapshot().coins.len(), 1);

    state.observe(Observation::Confirmed(txid));
    state.observe(Observation::Invalidated("outdated coin observation".into()));
    assert_eq!(
        state.transactions().values().next().unwrap().status(),
        &Status::Confirmed
    );
    state.observe(Observation::Confirmed(txid));

    assert_eq!(
        state.transactions().values().next().unwrap().status(),
        &Status::Confirmed
    );
    assert_eq!(state.pending().iter().count(), 0);
    assert_eq!(state.snapshot().recorded.len(), 1);
}

#[tokio::test]
async fn recording_completed_after_invalidation_still_retains_the_payment() {
    let (mut state, io) = fixture();
    let record = pending_action(&mut state, &io, Action::Record).await;
    let tx = &state.transactions()[&record.id().0];
    let txid = tx.payment().unwrap().transaction.compute_txid();

    state.observe(Observation::Invalidated("coin spent elsewhere".into()));
    let result = io.executor.execute(&record).await;
    state.completed(record.id(), result);

    let tx = &state.transactions()[&record.id().0];
    assert!(matches!(tx.status(), Status::Failed(err) if err.0 == "coin spent elsewhere"));
    assert!(state.snapshot().recorded.contains_key(&txid));
    assert!(state.operations().next().is_none());
    assert!(matches!(state.decide(io.events.now), Decision::Wait));
}

#[tokio::test]
async fn confirming_one_batch_keeps_the_other_intents_pending() {
    let (mut state, mut io) = split_batches();
    io.executor.broadcaster.lock().await.confirm = false;
    run_scripted(&mut state, &mut io, Operation::id).await;
    assert_eq!(state.transactions().len(), 2);
    assert_eq!(state.pending().iter().count(), 2);
    assert_eq!(state.snapshot().coins.len(), 2);
    assert_eq!(state.snapshot().recorded.len(), 2);
    let first = state.transactions().values().next().unwrap();
    let first_id = *first.payment().unwrap().outputs.keys().next().unwrap();
    state.observe(Observation::Confirmed(
        first.payment().unwrap().transaction.compute_txid(),
    ));
    let remaining: Vec<_> = state.pending().iter().map(|(id, _)| id).collect();
    assert_eq!(remaining.len(), 1);
    assert_ne!(remaining[0], first_id);
    assert_eq!(io.executor.broadcaster.lock().await.published.len(), 2);
}

#[tokio::test]
async fn independent_batches_progress_while_one_signer_waits() {
    use futures::channel::oneshot;
    let (mut state, mut io) = split_batches();
    let (send, recv) = oneshot::channel();
    io.executor
        .signers
        .get(&OutPoint::new(Txid::all_zeros(), 0))
        .unwrap()
        .lock()
        .await
        .delay = Some(recv);
    // Drive actual strategy transitions while one real executor future is held.
    let mut held = None;
    let mut requests = BTreeSet::new();
    while let Decision::Schedule(scheduled) = state.decide(io.events.now) {
        let id = scheduled.action.id();
        let operation = dispatch(&mut state, scheduled.action);
        let executor = io.executor.clone();
        assert!(
            requests.insert(id.clone()),
            "repeated scripted request {id:?}"
        );
        let future = async move { executor.execute(&operation).await };
        let mut future = Box::pin(future);
        match futures::poll!(&mut future) {
            std::task::Poll::Pending => {
                assert!(held.is_none(), "only one signer is held");
                held = Some((id, future));
            }
            std::task::Poll::Ready(result) => state.completed(id, result),
        }
    }
    assert_eq!(state.operations().count(), 1);
    assert_eq!(io.executor.broadcaster.lock().await.published.len(), 1);
    // Observation/re-evaluation must retain the pending operation.
    state.observe(Observation::Confirmed(Txid::all_zeros()));
    assert!(matches!(state.decide(io.events.now), Decision::Wait));
    assert_eq!(state.operations().count(), 1);
    send.send(()).unwrap();
    let (id, future) = held.unwrap();
    state.completed(id, future.await);
    run_scripted(&mut state, &mut io, Operation::id).await;
    assert_eq!(io.executor.broadcaster.lock().await.published.len(), 2);
    for outpoint in state.snapshot().coins.keys() {
        let signer = io.executor.signers.get(outpoint).unwrap();
        assert_eq!(signer.lock().await.requests().len(), 1);
    }
    assert!(state.operations().next().is_none());
}

fn split_batches() -> (TestState, TestRuntime) {
    let (state, runtime) = fixture_with_funding(true, 10_000);
    (state.with_batches(vec![vec![0], vec![1]]).unwrap(), runtime)
}

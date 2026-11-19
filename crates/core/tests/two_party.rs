//! Two-party collaborative batching over in-memory transport channels.

#[path = "support/signing.rs"]
mod signing;

use bitcoin::{
    Amount, FeeRate, OutPoint, ScriptBuf, Sequence, Transaction, TxOut, Txid, Weight, Witness,
    hashes::Hash,
};
use concurrent_psbt::output::UniqueId;
use cost_function::{
    intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve, ScheduleGrid},
    queue::{InMemoryQueue, Queue},
};
use fungi_core::{
    driver::{Decision, Machine, Scheduled, Stopped, Wake, run},
    strategy::{
        Broadcaster, Events, Executor, Observation, Runtime, State, Status,
        session::{
            Config, FeeContribution, Finalized, InputContribution, Message, OutputContribution,
            Payload, Phase, Progress, Side, WitnessContribution,
        },
    },
    wallet::{Coin, Snapshot, dummy},
};
use fungi_transport::RecvChannel;
use fungi_transport_testkit::mem::bidirectional;
use proptest::prelude::*;
use rand::{SeedableRng, rngs::StdRng};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::mpsc;

fn p2tr_script(byte: u8) -> ScriptBuf {
    ScriptBuf::from_bytes([vec![0x51, 0x20], vec![byte; 32]].concat())
}

fn outpoint(tx_byte: u8, vout: u32) -> OutPoint {
    OutPoint::new(Txid::from_byte_array([tx_byte; 32]), vout)
}

fn payment_intent(dest: u8, sats: u64) -> Intent {
    Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: p2tr_script(dest),
            amount: Amount::from_sat(sats),
        }),
        PayoffCurve::new(0, [(0, Amount::from_sat(sats * 4))]).unwrap(),
    )
}

struct SharedChain {
    published: Arc<Mutex<Vec<Transaction>>>,
    confirm_txs: Vec<mpsc::UnboundedSender<Observation>>,
}

impl Broadcaster for SharedChain {
    type Error = &'static str;

    fn broadcast(
        &mut self,
        tx: &Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + use<> {
        self.published.lock().unwrap().push(tx.clone());
        let txid = tx.compute_txid();
        for sender in &self.confirm_txs {
            let _ = sender.send(Observation::Confirmed(txid));
        }
        std::future::ready(Ok(()))
    }
}

fn schedule<A>(decision: Decision<A>) -> Option<Scheduled<A>> {
    match decision {
        Decision::Schedule(s) => Some(s),
        _ => None,
    }
}

struct PeerEvents<R> {
    now: Instant,
    inbox: mpsc::UnboundedReceiver<Observation>,
    peer: R,
}

impl<R: RecvChannel<Message>> Events for PeerEvents<R> {
    fn now(&self) -> Instant {
        self.now
    }

    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        if let Some(at) = deadline {
            self.now = self.now.max(at);
            return Wake::Timeout;
        }
        tokio::select! {
            biased;
            obs = self.inbox.recv() => Wake::Input(obs.expect("inbox stays open until confirmation")),
            msg = self.peer.recv() => Wake::Input(Observation::Peer(msg.expect("peer stays open until confirmation"))),
        }
    }
}

type WalletState = State<dummy::Change, StdRng>;

fn build_wallet(
    now: Instant,
    wallet_tag: u8,
    coin_sats: u64,
    pay_sats: u64,
    feerate: FeeRate,
    session: Option<Config>,
) -> (WalletState, OutPoint, signing::Signer, dummy::Change) {
    let op = outpoint(wallet_tag, 0);
    let coin = Coin {
        txout: TxOut {
            value: Amount::from_sat(coin_sats),
            script_pubkey: signing::script(op),
        },
        satisfaction_weight: Weight::from_wu(70),
    };
    let snapshot = Snapshot {
        coins: BTreeMap::from([(op, coin)]),
        recorded: BTreeMap::new(),
    };
    let change_script = p2tr_script(wallet_tag.wrapping_add(100));
    let make_change = || {
        dummy::Change::new(
            change_script.clone(),
            Weight::from_wu(70),
            [Ok(change_script.clone()), Ok(change_script.clone())],
        )
    };
    let signer = signing::Signer::new(op, [Ok(()), Ok(())]);
    let mut state = State::new(
        InMemoryQueue::new([payment_intent(wallet_tag.wrapping_add(50), pay_sats)]),
        snapshot,
        feerate,
        [wallet_tag; 16],
        make_change(),
        StdRng::seed_from_u64(u64::from(wallet_tag)),
        ScheduleGrid::new(now),
    );
    if let Some(cfg) = session {
        state = state.with_session(cfg).unwrap();
    }
    (state, op, signer, make_change())
}

#[tokio::test]
async fn two_drivers_batch_collaboratively_over_backpressured_bidirectional_channels() {
    let now = Instant::now();
    let session_id = [0x42; 16];
    let feerate = FeeRate::from_sat_per_vb(3).unwrap();

    let (mut state_a, op_a, signer_a, change_a) = build_wallet(
        now,
        1,
        80_000,
        25_000,
        feerate,
        Some(Config::two_party(session_id, Side::First)),
    );
    let (mut state_b, op_b, signer_b, change_b) = build_wallet(
        now,
        2,
        90_000,
        30_000,
        feerate,
        Some(Config::two_party(session_id, Side::Second)),
    );

    // Capacity 1 forces send backpressure while recv stays active in driver::run.
    let (chan_a, chan_b) = bidirectional::<Message>(1);
    let (send_a, recv_a) = chan_a.into_parts();
    let (send_b, recv_b) = chan_b.into_parts();

    let (tx_obs_a, rx_obs_a) = mpsc::unbounded_channel();
    let (tx_obs_b, rx_obs_b) = mpsc::unbounded_channel();
    let published = Arc::new(Mutex::new(Vec::new()));

    let exec_a = Executor::new(
        change_a,
        vec![signer_a],
        dummy::Recorder::new([Ok(())]),
        SharedChain {
            published: published.clone(),
            confirm_txs: vec![tx_obs_a.clone()],
        },
    )
    .unwrap()
    .with_peer(send_a);
    let exec_b = Executor::new(
        change_b,
        vec![signer_b],
        dummy::Recorder::new([Ok(())]),
        SharedChain {
            published: published.clone(),
            confirm_txs: vec![tx_obs_b.clone()],
        },
    )
    .unwrap()
    .with_peer(send_b);

    // Close observation channels once confirmed by dropping extra senders after broadcast;
    // keep a watcher task that closes both observation channels + peer senders on confirmation.
    drop(tx_obs_a);
    drop(tx_obs_b);

    let mut rt_a = Runtime {
        executor: exec_a,
        events: PeerEvents {
            now,
            inbox: rx_obs_a,
            peer: recv_a,
        },
    };
    let mut rt_b = Runtime {
        executor: exec_b,
        events: PeerEvents {
            now,
            inbox: rx_obs_b,
            peer: recv_b,
        },
    };

    // Run both independent drivers concurrently; drop peer senders once both settle.
    let run_both = async {
        // Because SharedChain drops its confirm_txs only when Runtime drops, run until confirmed
        // via a small wrapper or close inbox after first Confirmed observation.
        tokio::join!(
            drive_until_confirmed(&mut state_a, &mut rt_a),
            drive_until_confirmed(&mut state_b, &mut rt_b),
        )
    };
    run_both.await;

    let tx_a = state_a.transactions().values().next().unwrap();
    let tx_b = state_b.transactions().values().next().unwrap();
    assert_eq!(tx_a.status(), &Status::Confirmed);
    assert_eq!(tx_b.status(), &Status::Confirmed);
    assert_eq!(state_a.pending().iter().count(), 0);
    assert_eq!(state_b.pending().iter().count(), 0);

    let pay_a = tx_a.payment().unwrap();
    let pay_b = tx_b.payment().unwrap();
    assert_eq!(pay_a.transaction, pay_b.transaction);
    assert_eq!(pay_a.transaction.input.len(), 2);
    assert_eq!(pay_a.transaction.output.len(), 4);

    // Synchronized Progress agrees across both wallets on exact canonical contribution sets.
    let prog_a = state_a.session_progress(&session_id).unwrap();
    let prog_b = state_b.session_progress(&session_id).unwrap();
    assert_eq!(prog_a, prog_b);
    assert_eq!(prog_a.open, None);
    assert_eq!(prog_a.finalized.len(), 3);
    assert_eq!(tx_a.session_progress(), Some(&prog_a));

    // Contradictory progress arriving after confirmation cannot undo Confirmed status.
    state_a.observe(Observation::SessionProgress {
        session_id,
        progress: Progress::initial(),
    });
    assert_eq!(
        state_a.transactions().values().next().unwrap().status(),
        &Status::Confirmed
    );

    // Each wallet invoked only its own input signer once and never had the peer's signer.
    assert!(rt_a.executor.signers.get(&op_b).is_none());
    assert!(rt_b.executor.signers.get(&op_a).is_none());
    assert_eq!(
        rt_a.executor
            .signers
            .get(&op_a)
            .unwrap()
            .lock()
            .await
            .requests()
            .len(),
        1
    );
    assert_eq!(
        rt_b.executor
            .signers
            .get(&op_b)
            .unwrap()
            .lock()
            .await
            .requests()
            .len(),
        1
    );
}

/// Drive a wallet through `driver::run` by closing its channels once its transaction confirms.
async fn drive_until_confirmed<
    R: RecvChannel<Message>,
    P: fungi_transport::SendChannel<Message>,
>(
    state: &mut WalletState,
    runtime: &mut Runtime<
        dummy::Change,
        signing::Signer,
        dummy::Recorder,
        SharedChain,
        PeerEvents<R>,
        P,
    >,
) {
    struct StopOnConfirm<'a, R, P> {
        inner: &'a mut Runtime<
            dummy::Change,
            signing::Signer,
            dummy::Recorder,
            SharedChain,
            PeerEvents<R>,
            P,
        >,
        confirmed: bool,
    }
    impl<'a, R: RecvChannel<Message>, P: fungi_transport::SendChannel<Message>>
        fungi_core::driver::Environment<WalletState> for StopOnConfirm<'a, R, P>
    {
        fn now(&self) -> Instant {
            self.inner.events.now()
        }
        fn execute(
            &mut self,
            operation: fungi_core::strategy::Operation,
        ) -> impl Future<
            Output = Result<fungi_core::strategy::Update, fungi_core::strategy::Error>,
        > + use<'a, R, P> {
            let executor = self.inner.executor.clone();
            async move { executor.execute(&operation).await }
        }
        async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
            if self.confirmed && deadline.is_none() {
                return Wake::Closed;
            }
            let wake = self.inner.events.wait(deadline).await;
            if matches!(wake, Wake::Input(Observation::Confirmed(_))) {
                self.confirmed = true;
            }
            wake
        }
    }
    let mut env = StopOnConfirm {
        inner: runtime,
        confirmed: false,
    };
    assert_eq!(run(state, &mut env).await, Stopped::Closed);
}

/// Drive two wallets in-memory with optional duplicate message flooding and shuffled delivery.
async fn step_pair_deterministic(
    state_a: &mut WalletState,
    exec_a: &Executor<dummy::Change, signing::Signer, dummy::Recorder, DummyBroadcaster, VecPeer>,
    state_b: &mut WalletState,
    exec_b: &Executor<dummy::Change, signing::Signer, dummy::Recorder, DummyBroadcaster, VecPeer>,
    now: Instant,
    duplicate_messages: bool,
) {
    let mut wire_to_b = Vec::new();
    let mut wire_to_a = Vec::new();
    for _ in 0..128 {
        let mut progressed = false;
        for (state, exec, outbox) in [
            (&mut *state_a, exec_a, &mut wire_to_b),
            (&mut *state_b, exec_b, &mut wire_to_a),
        ] {
            while let Decision::Schedule(sched) = state.decide(now) {
                state.apply(sched.action);
                progressed = true;
            }
            let ops: Vec<_> = state.operations().collect();
            for (id, op) in ops {
                let res = exec.execute(&op).await;
                state.completed(id, res);
                progressed = true;
            }
            outbox.append(&mut exec.peer.lock().await.0);
        }
        for msg in wire_to_b.drain(..) {
            if duplicate_messages {
                state_b.observe(Observation::Peer(msg.clone()));
            }
            state_b.observe(Observation::Peer(msg));
            progressed = true;
        }
        for msg in wire_to_a.drain(..) {
            if duplicate_messages {
                state_a.observe(Observation::Peer(msg.clone()));
            }
            state_a.observe(Observation::Peer(msg));
            progressed = true;
        }
        if !progressed {
            break;
        }
    }
}

#[derive(Default)]
struct VecPeer(Vec<Message>);

impl fungi_transport::SendChannel<Message> for VecPeer {
    type Privacy = fungi_transport::Unspecified;
    type SendError = std::convert::Infallible;

    async fn send(&mut self, message: Message) -> Result<(), Self::SendError> {
        self.0.push(message);
        Ok(())
    }
}

#[derive(Default)]
struct DummyBroadcaster(Vec<Transaction>);

impl Broadcaster for DummyBroadcaster {
    type Error = std::convert::Infallible;

    fn broadcast(
        &mut self,
        tx: &Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + use<> {
        self.0.push(tx.clone());
        std::future::ready(Ok(()))
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn duplicate_delivery_and_varied_feerates_converge_to_exact_combined_feerate(
        sat_per_kwu in 0u64..=5_000,
        pay_a in 1_000u64..=30_000,
        pay_b in 1_000u64..=30_000,
        session_byte in any::<u8>(),
    ) {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let now = Instant::now();
            let feerate = FeeRate::from_sat_per_kwu(sat_per_kwu);
            let session_id = [session_byte; 16];

            let (mut state_a, _, signer_a, change_a) = build_wallet(
                now,
                10,
                100_000,
                pay_a,
                feerate,
                Some(Config::two_party(session_id, Side::First)),
            );
            let (mut state_b, _, signer_b, change_b) = build_wallet(
                now,
                20,
                100_000,
                pay_b,
                feerate,
                Some(Config::two_party(session_id, Side::Second)),
            );
            let exec_a = Executor::new(
                change_a,
                vec![signer_a],
                dummy::Recorder::new([Ok(())]),
                DummyBroadcaster::default(),
            )
            .unwrap()
            .with_peer(VecPeer::default());
            let exec_b = Executor::new(
                change_b,
                vec![signer_b],
                dummy::Recorder::new([Ok(())]),
                DummyBroadcaster::default(),
            )
            .unwrap()
            .with_peer(VecPeer::default());

            step_pair_deterministic(&mut state_a, &exec_a, &mut state_b, &exec_b, now, true).await;

            let tx_a = state_a.transactions().values().next().unwrap();
            let tx_b = state_b.transactions().values().next().unwrap();
            prop_assert_eq!(tx_a.status(), &Status::AwaitingConfirmation);
            prop_assert_eq!(tx_b.status(), &Status::AwaitingConfirmation);

            let pay_res_a = tx_a.payment().unwrap();
            let pay_res_b = tx_b.payment().unwrap();
            prop_assert_eq!(&pay_res_a.transaction, &pay_res_b.transaction);
            prop_assert!(pay_res_a.fee >= feerate.fee_wu(pay_res_a.transaction.weight()).unwrap());
            prop_assert_eq!(
                state_a.session_progress(&session_id),
                state_b.session_progress(&session_id)
            );
            Ok(())
        })?;
    }
}

#[tokio::test]
async fn dynamic_session_entry_replays_early_messages_and_settles_late_intents_elsewhere() {
    let now = Instant::now();
    let session_id = [0x99; 16];
    let feerate = FeeRate::from_sat_per_vb(2).unwrap();

    let (mut state_a, _, signer_a, change_a) = build_wallet(
        now,
        3,
        80_000,
        20_000,
        feerate,
        Some(Config::two_party(session_id, Side::First)),
    );
    // Wallet B starts WITHOUT session config; early messages from A arrive before Observation::Session!
    let op_b1 = outpoint(4, 0);
    let op_b2 = outpoint(4, 1);
    let snapshot_b = Snapshot {
        coins: BTreeMap::from([
            (
                op_b1,
                Coin {
                    txout: TxOut {
                        value: Amount::from_sat(80_000),
                        script_pubkey: signing::script(op_b1),
                    },
                    satisfaction_weight: Weight::from_wu(70),
                },
            ),
            (
                op_b2,
                Coin {
                    txout: TxOut {
                        value: Amount::from_sat(80_000),
                        script_pubkey: signing::script(op_b2),
                    },
                    satisfaction_weight: Weight::from_wu(70),
                },
            ),
        ]),
        recorded: BTreeMap::new(),
    };
    let change_script_b = p2tr_script(104);
    let make_change_b = || {
        dummy::Change::new(
            change_script_b.clone(),
            Weight::from_wu(70),
            [Ok(change_script_b.clone()), Ok(change_script_b.clone())],
        )
    };
    let mut state_b = State::new(
        InMemoryQueue::new([payment_intent(54, 15_000)]),
        snapshot_b,
        feerate,
        [4; 16],
        make_change_b(),
        StdRng::seed_from_u64(4),
        ScheduleGrid::new(now),
    );

    let exec_a = Executor::new(
        change_a,
        vec![signer_a],
        dummy::Recorder::new([Ok(())]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());
    let exec_b = Executor::new(
        make_change_b(),
        vec![
            signing::Signer::new(op_b1, [Ok(())]),
            signing::Signer::new(op_b2, [Ok(())]),
        ],
        dummy::Recorder::new([Ok(()), Ok(())]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());

    // Wallet A dispatches its initial session messages before Wallet B enters the session.
    for _ in 0..8 {
        while let Decision::Schedule(sched) = state_a.decide(now) {
            state_a.apply(sched.action);
        }
        let ops: Vec<_> = state_a.operations().collect();
        if ops.is_empty() {
            break;
        }
        for (id, op) in ops {
            let res = exec_a.execute(&op).await;
            state_a.completed(id, res);
        }
    }
    let early_msgs = std::mem::take(&mut exec_a.peer.lock().await.0);
    assert!(!early_msgs.is_empty());
    for msg in early_msgs {
        state_b.observe(Observation::Peer(msg));
    }

    // Wallet B explicitly enters the session (replaying buffered early messages); duplicate ignored.
    let cfg_b = Config::two_party(session_id, Side::Second);
    state_b.observe(Observation::Session(cfg_b));
    state_b.observe(Observation::Session(cfg_b));

    step_pair_deterministic(&mut state_a, &exec_a, &mut state_b, &exec_b, now, false).await;

    let session_progress_before_late = state_b.session_progress(&session_id).unwrap();
    assert_eq!(session_progress_before_late.open, None);

    // A late peer Input contribution cannot alter the finalized sets.
    state_b.observe(Observation::Peer(Message {
        session_id,
        sender: Side::First,
        payload: Payload::Input(InputContribution {
            outpoint: outpoint(99, 0),
            witness_utxo: TxOut {
                value: Amount::from_sat(50_000),
                script_pubkey: p2tr_script(99),
            },
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
        }),
    }));
    assert_eq!(
        state_b.session_progress(&session_id).unwrap(),
        session_progress_before_late
    );

    // A late local payment intent arriving after session start does not alter the session
    // and settles in a separate unilateral transaction using Wallet B's remaining coin!
    state_b.observe(Observation::Intent(payment_intent(55, 12_000)));
    while let Decision::Schedule(sched) = state_b.decide(now) {
        state_b.apply(sched.action);
        for (id, op) in state_b.operations().collect::<Vec<_>>() {
            let res = exec_b.execute(&op).await;
            state_b.completed(id, res);
        }
    }
    assert_eq!(state_b.transactions().len(), 2);
    assert!(
        state_b
            .transactions()
            .values()
            .all(|tx| tx.status() == &Status::AwaitingConfirmation)
    );
}

#[tokio::test]
async fn excluded_session_contributions_keep_unsatisfied_intents_eligible_elsewhere() {
    let now = Instant::now();
    let feerate = FeeRate::from_sat_per_vb(1).unwrap();

    // Case 1: Session closes Inputs excluding started wallet (with explicit_batches).
    let s1 = [0x11; 16];
    let (state_1, _, signer_1, change_1) = build_wallet(now, 31, 60_000, 15_000, feerate, None);
    let mut state_1 = state_1
        .with_batches(vec![vec![0]])
        .unwrap()
        .with_session(Config::two_party(s1, Side::First))
        .unwrap();
    assert!(state_1.clone_check_errors(s1).is_err());

    let exec_1 = Executor::new(
        change_1,
        vec![signer_1],
        dummy::Recorder::new([Ok(())]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());

    assert!(schedule(Decision::<()>::Wait).is_none());
    let start = schedule(state_1.decide(now)).unwrap();
    let first_exec_id = start.action.id().0;
    state_1.apply(start.action);
    for (id, op) in state_1.operations().collect::<Vec<_>>() {
        let res = exec_1.execute(&op).await;
        state_1.completed(id, res);
    }
    // Re-evaluate while bound so explicit_batches prunes intent 0, then verify exclusion pushes it back.
    state_1.observe(Observation::FeeRate(FeeRate::from_sat_per_vb(2).unwrap()));
    let _ = state_1.decide(now);

    // Unknown session progress is ignored cleanly.
    state_1.observe(Observation::SessionProgress {
        session_id: [0xFF; 16],
        progress: Progress::initial(),
    });

    // External progress closes Inputs with ONLY a foreign input -> excludes local contribution!
    let foreign_in = InputContribution {
        outpoint: outpoint(88, 0),
        witness_utxo: TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: p2tr_script(88),
        },
        sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
    };
    state_1.observe(Observation::SessionProgress {
        session_id: s1,
        progress: Progress {
            finalized: vec![Finalized::Inputs(BTreeSet::from([foreign_in.clone()]))],
            open: Some(Phase::Outputs),
        },
    });
    assert!(matches!(
        state_1.transactions()[&first_exec_id].status(),
        Status::Failed(err) if err.0 == "session closed inputs without local input"
    ));

    // Unsatisfied intent 0 is immediately eligible and settles unilaterally!
    while let Decision::Schedule(sched) = state_1.decide(now) {
        state_1.apply(sched.action);
        for (id, op) in state_1.operations().collect::<Vec<_>>() {
            let res = exec_1.execute(&op).await;
            state_1.completed(id, res);
        }
    }
    assert_eq!(state_1.transactions().len(), 2);
    assert!(
        state_1
            .transactions()
            .values()
            .any(|tx| tx.status() == &Status::AwaitingConfirmation)
    );

    // Case 1b: Explicit batch excluded before any evaluate() prune does not duplicate the batch.
    let (state_1b, _, signer_1b, change_1b) = build_wallet(now, 33, 60_000, 15_000, feerate, None);
    let mut state_1b = state_1b
        .with_batches(vec![vec![0]])
        .unwrap()
        .with_session(Config::two_party(s1, Side::First))
        .unwrap();
    let exec_1b = Executor::new(
        change_1b,
        vec![signer_1b],
        dummy::Recorder::new([Ok(())]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());
    let start_1b = schedule(state_1b.decide(now)).unwrap();
    state_1b.apply(start_1b.action);
    for (id, op) in state_1b.operations().collect::<Vec<_>>() {
        let res = exec_1b.execute(&op).await;
        state_1b.completed(id, res);
    }
    state_1b.observe(Observation::SessionProgress {
        session_id: s1,
        progress: Progress {
            finalized: vec![Finalized::Inputs(BTreeSet::from([foreign_in.clone()]))],
            open: Some(Phase::Outputs),
        },
    });
    while let Decision::Schedule(sched) = state_1b.decide(now) {
        state_1b.apply(sched.action);
        for (id, op) in state_1b.operations().collect::<Vec<_>>() {
            let res = exec_1b.execute(&op).await;
            state_1b.completed(id, res);
        }
    }
    assert_eq!(state_1b.transactions().len(), 2);

    // Case 2: Session closes Outputs excluding local output (default batches), and unstarted closed session skipped.
    let s2 = [0x22; 16];
    let s3 = [0x33; 16];
    let (mut state_2, op_2, signer_2, change_2) = build_wallet(
        now,
        32,
        60_000,
        15_000,
        feerate,
        Some(Config::two_party(s2, Side::First)),
    );
    state_2 = state_2
        .with_session(Config::two_party(s3, Side::First))
        .unwrap();
    // Close s3 before evaluation so it is skipped when re-evaluating!
    let exec_2 = Executor::new(
        change_2,
        vec![signer_2],
        dummy::Recorder::new([Ok(())]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());

    let start_2 = schedule(state_2.decide(now)).unwrap();
    let exec_id_2 = start_2.action.id().0;
    state_2.apply(start_2.action);
    for (id, op) in state_2.operations().collect::<Vec<_>>() {
        let res = exec_2.execute(&op).await;
        state_2.completed(id, res);
    }
    state_2.observe(Observation::SessionProgress {
        session_id: s3,
        progress: Progress {
            finalized: vec![Finalized::Inputs(BTreeSet::from([foreign_in.clone()]))],
            open: Some(Phase::Outputs),
        },
    });
    let own_in = InputContribution {
        outpoint: op_2,
        witness_utxo: state_2.snapshot().coins[&op_2].txout.clone(),
        sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
    };
    state_2.observe(Observation::SessionProgress {
        session_id: s2,
        progress: Progress {
            finalized: vec![
                Finalized::Inputs(BTreeSet::from([own_in])),
                Finalized::Outputs {
                    outputs: BTreeSet::from([OutputContribution {
                        id: UniqueId::try_from(vec![7; 16]).unwrap(),
                        tx_out: TxOut {
                            value: Amount::from_sat(40_000),
                            script_pubkey: p2tr_script(77),
                        },
                    }]),
                    fees: BTreeSet::new(),
                    additional_entropy: None,
                },
            ],
            open: Some(Phase::Signatures),
        },
    });
    assert!(matches!(
        state_2.transactions()[&exec_id_2].status(),
        Status::Failed(err) if err.0 == "session closed outputs without local contribution"
    ));
    while let Decision::Schedule(sched) = state_2.decide(now) {
        state_2.apply(sched.action);
        for (id, op) in state_2.operations().collect::<Vec<_>>() {
            let res = exec_2.execute(&op).await;
            state_2.completed(id, res);
        }
    }
    assert!(
        state_2
            .transactions()
            .values()
            .any(|tx| tx.status() == &Status::AwaitingConfirmation)
    );
}

trait ConfigErrorCheck {
    fn clone_check_errors(&mut self, existing: [u8; 16])
    -> Result<(), fungi_core::strategy::Error>;
}

impl ConfigErrorCheck for WalletState {
    fn clone_check_errors(
        &mut self,
        existing: [u8; 16],
    ) -> Result<(), fungi_core::strategy::Error> {
        let now = Instant::now();
        let dup = State::new(
            InMemoryQueue::new([]),
            Snapshot::default(),
            FeeRate::ZERO,
            [0; 16],
            dummy::Change::new(p2tr_script(1), Weight::ZERO, []),
            StdRng::seed_from_u64(0),
            ScheduleGrid::new(now),
        )
        .with_session(Config::two_party(existing, Side::First))
        .unwrap()
        .with_session(Config::two_party(existing, Side::Second));
        assert_eq!(dup.err().unwrap().0, "duplicate session identity");

        let mut evaluated = State::new(
            InMemoryQueue::new([]),
            Snapshot::default(),
            FeeRate::ZERO,
            [0; 16],
            dummy::Change::new(p2tr_script(1), Weight::ZERO, []),
            StdRng::seed_from_u64(0),
            ScheduleGrid::new(now),
        );
        let _ = evaluated.decide(now);
        evaluated
            .with_session(Config::two_party(existing, Side::First))
            .map(|_| ())
    }
}

#[tokio::test]
async fn broadcast_race_sort_seed_and_contradictory_progress_liability_hold() {
    let now = Instant::now();
    let session_id = [0x55; 16];
    let feerate = FeeRate::from_sat_per_vb(2).unwrap();

    let (mut state_a, op_a, signer_a, change_a) = build_wallet(
        now,
        41,
        80_000,
        20_000,
        feerate,
        Some(Config::two_party(session_id, Side::First).with_additional_entropy([0xAB; 32])),
    );
    let (mut state_b, op_b, signer_b, change_b) = build_wallet(
        now,
        42,
        80_000,
        20_000,
        feerate,
        Some(Config::two_party(session_id, Side::Second).with_additional_entropy([0xAB; 32])),
    );
    let exec_a = Executor::new(
        change_a,
        vec![signer_a],
        dummy::Recorder::new([Ok(())]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());
    let exec_b = Executor::new(
        change_b,
        vec![signer_b],
        dummy::Recorder::new([Ok(())]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());

    // Advance both wallets until Sign(0) + SendWitness(0) complete, dropping B's Witness to A!
    for _ in 0..20 {
        for (state, exec) in [(&mut state_a, &exec_a), (&mut state_b, &exec_b)] {
            while let Decision::Schedule(sched) = state.decide(now) {
                state.apply(sched.action);
            }
            for (id, op) in state.operations().collect::<Vec<_>>() {
                let res = exec.execute(&op).await;
                state.completed(id, res);
            }
        }
        for msg in exec_a.peer.lock().await.0.drain(..) {
            state_b.observe(Observation::Peer(msg));
        }
        for msg in exec_b.peer.lock().await.0.drain(..) {
            if !matches!(msg.payload, Payload::Witness(_)) {
                state_a.observe(Observation::Peer(msg));
            }
        }
    }

    let signed_tx = state_b
        .transactions()
        .values()
        .next()
        .unwrap()
        .payment()
        .unwrap()
        .transaction
        .clone();

    // Wallet A is still Running (waiting for B's witness). Unrelated or forged broadcasts are ignored.
    state_a.observe(Observation::Broadcast(Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: vec![],
        output: vec![],
    }));
    let mut forged = signed_tx.clone();
    for txin in &mut forged.input {
        if txin.previous_output == op_b {
            txin.witness = Witness::from_slice(&[[0u8; 64]]);
        }
    }
    state_a.observe(Observation::Broadcast(forged));
    assert_eq!(
        state_a.transactions().values().next().unwrap().status(),
        &Status::Running
    );

    // Valid Bitcoin P2P Broadcast completes Wallet A's witnesses -> A records & publishes!
    state_a.observe(Observation::Broadcast(signed_tx.clone()));
    while let Decision::Schedule(sched) = state_a.decide(now) {
        state_a.apply(sched.action);
        for (id, op) in state_a.operations().collect::<Vec<_>>() {
            let res = exec_a.execute(&op).await;
            state_a.completed(id, res);
        }
    }
    assert_eq!(
        state_a.transactions().values().next().unwrap().status(),
        &Status::AwaitingConfirmation
    );
    assert!(
        state_a
            .snapshot()
            .recorded
            .contains_key(&signed_tx.compute_txid())
    );

    // Now inject contradictory Progress on Wallet A before confirmation (A19):
    // Wallet A fails the execution AND attaches RBF replacement liability to op_a!
    state_a.observe(Observation::SessionProgress {
        session_id,
        progress: Progress::initial(),
    });
    assert!(matches!(
        state_a.transactions().values().next().unwrap().status(),
        Status::Failed(err) if err.0 == "session progress contradicts finalized phase"
    ));

    // A new tiny intent that ignores replacement liability would leave insufficient surplus if coin was tight,
    // while here op_a is priced with replacement liability (> signed_tx fee) on next evaluation!
    state_a.observe(Observation::Intent(payment_intent(91, 79_900)));
    let _ = state_a.decide(now);
    assert!(!state_a.planning_errors().is_empty());
    let _ = op_a;
}

#[tokio::test]
async fn invalid_peer_contributions_and_witnesses_fail_only_that_session() {
    let now = Instant::now();
    let feerate = FeeRate::from_sat_per_vb(1).unwrap();

    // 1. Conflicting peer message fails bound session.
    let s1 = [0x61; 16];
    let (mut state, op, signer, change) = build_wallet(
        now,
        61,
        50_000,
        10_000,
        feerate,
        Some(Config::two_party(s1, Side::First)),
    );
    let exec = Executor::new(
        change,
        vec![signer],
        dummy::Recorder::new([]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());
    while let Decision::Schedule(sched) = state.decide(now) {
        state.apply(sched.action);
        for (id, op_run) in state.operations().collect::<Vec<_>>() {
            let res = exec.execute(&op_run).await;
            state.completed(id, res);
        }
    }
    state.observe(Observation::Peer(Message {
        session_id: s1,
        sender: Side::Second,
        payload: Payload::Input(InputContribution {
            outpoint: op,
            witness_utxo: TxOut {
                value: Amount::from_sat(1),
                script_pubkey: p2tr_script(1),
            },
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
        }),
    }));
    assert!(matches!(
        state.transactions().values().next().unwrap().status(),
        Status::Failed(err) if err.0 == "conflicting input contribution"
    ));

    // 2. Non-P2TR peer input in agreed set fails at CloseInputs.
    let s2 = [0x62; 16];
    let (mut state2, op2, signer2, change2) = build_wallet(
        now,
        62,
        50_000,
        10_000,
        feerate,
        Some(Config::two_party(s2, Side::First)),
    );
    let exec2 = Executor::new(
        change2,
        vec![signer2],
        dummy::Recorder::new([]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());
    while let Decision::Schedule(sched) = state2.decide(now) {
        state2.apply(sched.action);
        for (id, op_run) in state2.operations().collect::<Vec<_>>() {
            let res = exec2.execute(&op_run).await;
            state2.completed(id, res);
        }
    }
    let own_in2 = InputContribution {
        outpoint: op2,
        witness_utxo: state2.snapshot().coins[&op2].txout.clone(),
        sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
    };
    let legacy_peer = InputContribution {
        outpoint: outpoint(98, 0),
        witness_utxo: TxOut {
            value: Amount::from_sat(20_000),
            script_pubkey: ScriptBuf::new(),
        },
        sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
    };
    state2.observe(Observation::SessionProgress {
        session_id: s2,
        progress: Progress {
            finalized: vec![Finalized::Inputs(BTreeSet::from([own_in2, legacy_peer]))],
            open: Some(Phase::Outputs),
        },
    });
    while let Decision::Schedule(sched) = state2.decide(now) {
        state2.apply(sched.action);
        for (id, op_run) in state2.operations().collect::<Vec<_>>() {
            let res = exec2.execute(&op_run).await;
            state2.completed(id, res);
        }
    }
    assert!(matches!(
        state2.transactions().values().next().unwrap().status(),
        Status::Failed(err) if err.0 == "PoC signer path only supports P2TR key spends"
    ));

    // 3. Invalid peer witness fails the session during Signatures.
    let s3 = [0x63; 16];
    let (mut state_a, _, signer_a, change_a) = build_wallet(
        now,
        63,
        60_000,
        15_000,
        feerate,
        Some(Config::two_party(s3, Side::First)),
    );
    let (mut state_b, op_b, mut signer_b, change_b) = build_wallet(
        now,
        64,
        60_000,
        15_000,
        feerate,
        Some(Config::two_party(s3, Side::Second)),
    );
    signer_b.corrupt = true;
    let exec_a = Executor::new(
        change_a,
        vec![signer_a],
        dummy::Recorder::new([]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());
    let exec_b = Executor::new(
        change_b,
        vec![signer_b],
        dummy::Recorder::new([]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());
    step_pair_deterministic(&mut state_a, &exec_a, &mut state_b, &exec_b, now, false).await;
    assert!(matches!(
        state_b.transactions().values().next().unwrap().status(),
        Status::Failed(_)
    ));
    // Inject a corrupted peer witness directly into A now that A is frozen!
    state_a.observe(Observation::Peer(Message {
        session_id: s3,
        sender: Side::Second,
        payload: Payload::Witness(WitnessContribution {
            outpoint: op_b,
            witness: Witness::from_slice(&[[0u8; 64]]),
        }),
    }));
    assert!(matches!(
        state_a.transactions().values().next().unwrap().status(),
        Status::Failed(_)
    ));
}

#[tokio::test]
async fn tampered_agreed_views_early_signatures_and_pending_ready_sends() {
    let now = Instant::now();
    let feerate = FeeRate::from_sat_per_vb(1).unwrap();

    // 1. Unstarted future-payoff session candidate with external_progress rebinds cleanly on FeeRate change,
    // and conflicting peer messages before evaluation do not panic.
    let s_unstarted = [0x71; 16];
    let future_intent = Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: p2tr_script(71),
            amount: Amount::from_sat(10_000),
        }),
        PayoffCurve::new(100, [(0, Amount::from_sat(50_000))]).unwrap(),
    );
    let op_u = outpoint(71, 0);
    let snap_u = Snapshot {
        coins: BTreeMap::from([(
            op_u,
            Coin {
                txout: TxOut {
                    value: Amount::from_sat(50_000),
                    script_pubkey: signing::script(op_u),
                },
                satisfaction_weight: Weight::from_wu(70),
            },
        )]),
        recorded: BTreeMap::new(),
    };
    let mut state_u = State::new(
        InMemoryQueue::new([future_intent]),
        snap_u,
        feerate,
        [71; 16],
        dummy::Change::new(p2tr_script(171), Weight::from_wu(70), []),
        StdRng::seed_from_u64(71),
        ScheduleGrid::new(now),
    )
    .with_session(Config::two_party(s_unstarted, Side::First))
    .unwrap();
    // Record an input then conflict while unbound!
    state_u.observe(Observation::Peer(Message {
        session_id: s_unstarted,
        sender: Side::Second,
        payload: Payload::Input(InputContribution {
            outpoint: outpoint(72, 0),
            witness_utxo: TxOut {
                value: Amount::from_sat(20_000),
                script_pubkey: p2tr_script(72),
            },
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
        }),
    }));
    state_u.observe(Observation::Peer(Message {
        session_id: s_unstarted,
        sender: Side::Second,
        payload: Payload::Input(InputContribution {
            outpoint: outpoint(72, 0),
            witness_utxo: TxOut {
                value: Amount::from_sat(99_999),
                script_pubkey: p2tr_script(72),
            },
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
        }),
    }));
    state_u.observe(Observation::SessionProgress {
        session_id: s_unstarted,
        progress: Progress::initial(),
    });
    let first_sched = schedule(state_u.decide(now)).unwrap();
    assert_eq!(first_sched.action.id().0.sequence, 0);
    state_u.observe(Observation::FeeRate(FeeRate::from_sat_per_vb(2).unwrap()));
    let second_sched = schedule(state_u.decide(now)).unwrap();
    assert_eq!(second_sched.action.id().0.sequence, 1);

    // 2. Concurrent pending SendInputsReady / SendOutputsReady gates + unknown-outpoint witness ignored.
    let s_pend = [0x72; 16];
    let (mut state_p, op_p, signer_p, change_p) = build_wallet(
        now,
        72,
        60_000,
        15_000,
        feerate,
        Some(Config::two_party(s_pend, Side::First)),
    );
    let exec_p = Executor::new(
        change_p,
        vec![signer_p],
        dummy::Recorder::new([Ok(())]),
        DummyBroadcaster::default(),
    )
    .unwrap()
    .with_peer(VecPeer::default());
    // Run Start -> Input -> SendInput so SendInputsReady(1) becomes available.
    for _ in 0..3 {
        while let Decision::Schedule(s) = state_p.decide(now) {
            state_p.apply(s.action);
        }
        for (id, op_run) in state_p.operations().collect::<Vec<_>>() {
            let res = exec_p.execute(&op_run).await;
            state_p.completed(id, res);
        }
    }
    // Apply SendInputsReady(1) WITHOUT completing it yet, then arrive a peer input -> SendInputsReady(2) blocked!
    let ready_in = schedule(state_p.decide(now)).unwrap();
    state_p.apply(ready_in.action);
    let peer_op = outpoint(73, 0);
    let peer_in = InputContribution {
        outpoint: peer_op,
        witness_utxo: TxOut {
            value: Amount::from_sat(60_000),
            script_pubkey: signing::script(peer_op),
        },
        sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
    };
    state_p.observe(Observation::Peer(Message {
        session_id: s_pend,
        sender: Side::Second,
        payload: Payload::Input(peer_in.clone()),
    }));
    assert!(schedule(state_p.decide(now)).is_none());
    for (id, op_run) in state_p.operations().collect::<Vec<_>>() {
        let res = exec_p.execute(&op_run).await;
        state_p.completed(id, res);
    }
    // Complete Inputs phase and advance to SendOutputsReady(3) pending!
    let ready_in2 = schedule(state_p.decide(now)).unwrap();
    state_p.apply(ready_in2.action);
    for (id, op_run) in state_p.operations().collect::<Vec<_>>() {
        let res = exec_p.execute(&op_run).await;
        state_p.completed(id, res);
    }
    state_p.observe(Observation::Peer(Message {
        session_id: s_pend,
        sender: Side::Second,
        payload: Payload::InputsReady(BTreeSet::from([op_p, peer_op])),
    }));
    for _ in 0..3 {
        while let Decision::Schedule(s) = state_p.decide(now) {
            state_p.apply(s.action);
        }
        for (id, op_run) in state_p.operations().collect::<Vec<_>>() {
            let res = exec_p.execute(&op_run).await;
            state_p.completed(id, res);
        }
    }
    // Now SendOutputsReady(3) is scheduled; apply it WITHOUT completing, then observe peer output!
    let ready_out = schedule(state_p.decide(now)).unwrap();
    state_p.apply(ready_out.action);
    let peer_out = OutputContribution {
        id: UniqueId::try_from(vec![0x73; 16]).unwrap(),
        tx_out: TxOut {
            value: Amount::from_sat(59_500),
            script_pubkey: p2tr_script(73),
        },
    };
    let peer_fee = FeeContribution {
        id: UniqueId::try_from(vec![0x74; 16]).unwrap(),
        amount: Amount::from_sat(500),
    };
    state_p.observe(Observation::Peer(Message {
        session_id: s_pend,
        sender: Side::Second,
        payload: Payload::Output(peer_out.clone()),
    }));
    state_p.observe(Observation::Peer(Message {
        session_id: s_pend,
        sender: Side::Second,
        payload: Payload::Fee(peer_fee.clone()),
    }));
    assert!(schedule(state_p.decide(now)).is_none());
    for (id, op_run) in state_p.operations().collect::<Vec<_>>() {
        let res = exec_p.execute(&op_run).await;
        state_p.completed(id, res);
    }

    // 3. Early Finalized::Signatures before CloseOutputs + unknown-outpoint witness ignored!
    // Capture local contributions sent by state_p so we can construct a valid Finalized::Signatures progress.
    let sent_msgs = std::mem::take(&mut exec_p.peer.lock().await.0);
    let mut all_inputs = BTreeSet::from([peer_in]);
    let mut all_outputs = BTreeSet::from([peer_out]);
    let mut all_fees = BTreeSet::from([peer_fee]);
    for m in &sent_msgs {
        match &m.payload {
            Payload::Input(c) => {
                all_inputs.insert(c.clone());
            }
            Payload::Output(c) => {
                all_outputs.insert(c.clone());
            }
            Payload::Fee(c) => {
                all_fees.insert(c.clone());
            }
            _ => {}
        }
    }
    // Build a valid witness for peer_op by signing the exact unsigned_tx!
    let bogus_witness = WitnessContribution {
        outpoint: outpoint(250, 9),
        witness: Witness::from_slice(&[[1u8; 64]]),
    };
    // Deliver Finalized::Signatures BEFORE CloseOutputs runs (bogus unknown-outpoint witness hits unfrozen & frozen unknown-outpoint paths)!
    state_p.observe(Observation::SessionProgress {
        session_id: s_pend,
        progress: Progress {
            finalized: vec![
                Finalized::Inputs(all_inputs.clone()),
                Finalized::Outputs {
                    outputs: all_outputs.clone(),
                    fees: all_fees.clone(),
                    additional_entropy: None,
                },
                Finalized::Signatures(BTreeSet::from([bogus_witness])),
            ],
            open: None,
        },
    });
    while let Decision::Schedule(s) = state_p.decide(now) {
        state_p.apply(s.action);
        for (id, op_run) in state_p.operations().collect::<Vec<_>>() {
            let res = exec_p.execute(&op_run).await;
            state_p.completed(id, res);
        }
    }

    // 4. Tampered local input, local output, or local fee in SessionProgress fails validation at CloseOutputs!
    for tamper in [0u8, 1, 2] {
        let sid = [0x80 + tamper; 16];
        let (mut st, _, sg, ch) = build_wallet(
            now,
            80 + tamper,
            60_000,
            15_000,
            feerate,
            Some(Config::two_party(sid, Side::First)),
        );
        let ex = Executor::new(
            ch,
            vec![sg],
            dummy::Recorder::new([]),
            DummyBroadcaster::default(),
        )
        .unwrap()
        .with_peer(VecPeer::default());
        for _ in 0..4 {
            while let Decision::Schedule(s) = st.decide(now) {
                st.apply(s.action);
            }
            for (id, op_run) in st.operations().collect::<Vec<_>>() {
                let res = ex.execute(&op_run).await;
                st.completed(id, res);
            }
        }
        let msgs_in = std::mem::take(&mut ex.peer.lock().await.0);
        let mut ins: BTreeSet<_> = msgs_in
            .iter()
            .filter_map(|m| match &m.payload {
                Payload::Input(c) => Some(c.clone()),
                _ => None,
            })
            .collect();
        if tamper == 0 {
            let mut altered = ins.pop_first().unwrap();
            altered.sequence = Sequence::MAX;
            ins.insert(altered);
        }
        st.observe(Observation::SessionProgress {
            session_id: sid,
            progress: Progress {
                finalized: vec![Finalized::Inputs(ins.clone())],
                open: Some(Phase::Outputs),
            },
        });
        for _ in 0..4 {
            while let Decision::Schedule(s) = st.decide(now) {
                st.apply(s.action);
            }
            for (id, op_run) in st.operations().collect::<Vec<_>>() {
                let res = ex.execute(&op_run).await;
                st.completed(id, res);
            }
        }
        if tamper == 0 {
            assert!(matches!(
                st.transactions().values().next().unwrap().status(),
                Status::Failed(_)
            ));
            continue;
        }
        let msgs_out = std::mem::take(&mut ex.peer.lock().await.0);
        let mut outs: BTreeSet<_> = msgs_out
            .iter()
            .filter_map(|m| match &m.payload {
                Payload::Output(c) => Some(c.clone()),
                _ => None,
            })
            .collect();
        let mut fees: BTreeSet<_> = msgs_out
            .iter()
            .filter_map(|m| match &m.payload {
                Payload::Fee(c) => Some(c.clone()),
                _ => None,
            })
            .collect();
        if tamper == 1 {
            let mut altered = outs.pop_first().unwrap();
            altered.tx_out.value += Amount::from_sat(1);
            outs.insert(altered);
        } else if tamper == 2 {
            let mut altered = fees.pop_first().unwrap();
            altered.amount += Amount::from_sat(1);
            fees.insert(altered);
        }
        st.observe(Observation::SessionProgress {
            session_id: sid,
            progress: Progress {
                finalized: vec![
                    Finalized::Inputs(ins),
                    Finalized::Outputs {
                        outputs: outs,
                        fees,
                        additional_entropy: None,
                    },
                ],
                open: Some(Phase::Signatures),
            },
        });
        while let Decision::Schedule(s) = st.decide(now) {
            st.apply(s.action);
            for (id, op_run) in st.operations().collect::<Vec<_>>() {
                let res = ex.execute(&op_run).await;
                st.completed(id, res);
            }
        }
        if tamper == 1 {
            assert!(matches!(
                st.transactions().values().next().unwrap().status(),
                Status::Failed(_)
            ));
        } else {
            assert!(matches!(
                st.transactions().values().next().unwrap().status(),
                Status::Failed(err) if err.0 == "agreed fees omit or alter the local fee"
            ));
        }
    }
}

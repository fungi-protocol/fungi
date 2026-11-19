//! Actual signatures and chain acceptance through the same driver as the simulation.

#[path = "support/scripted_driver.rs"]
mod scripted_driver;

use scripted_driver::run as run_scripted;

#[path = "support/bdk.rs"]
mod bdk;

use bdk_wallet::KeychainKind;
use bitcoin::{Address, Amount, Block, FeeRate, Transaction, TxIn, absolute, transaction};
use cost_function::{
    intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve, ScheduleGrid},
    queue::{InMemoryQueue, Queue},
};
use fungi_core::{
    driver::{Stopped, Wake, run as run_driver},
    strategy::{
        Broadcaster, Events, Executor, Observation, Operation, Runtime, State, Status,
        session::{Config, Message, Side},
    },
    wallet::{InputSigner, SnapshotSource, TransactionRecorder},
};
use fungi_regtest::corepc_node;
use fungi_transport::RecvChannel;
use fungi_transport_testkit::mem::bidirectional;
use rand::{SeedableRng, rngs::StdRng};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

fn mine_blocks(node: &corepc_node::Node, count: usize, address: &Address) -> Vec<Block> {
    node.client
        .generate_to_address(count, address)
        .unwrap()
        .into_model()
        .unwrap()
        .0
        .into_iter()
        .map(|hash| node.client.get_block(hash).unwrap())
        .collect()
}

struct Chain {
    node: Arc<corepc_node::Node>,
    wallet: bdk::Adapter,
    height: u32,
}
impl Broadcaster for Chain {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn broadcast(
        &mut self,
        tx: &Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + use<> {
        let node = self.node.clone();
        let wallet = self.wallet.clone();
        let tx = tx.clone();
        async move {
            let snapshot = wallet.snapshot();
            assert!(
                tx.input
                    .iter()
                    .all(|input| snapshot.coins.contains_key(&input.previous_output)),
                "recording a signed transaction does not reserve its inputs"
            );
            assert_eq!(snapshot.recorded.get(&tx.compute_txid()), Some(&tx));
            let result =
                tokio::task::spawn_blocking(move || node.client.send_raw_transaction(&tx)).await?;
            result?;
            Ok(())
        }
    }
}
impl Chain {
    fn mine_confirmation(&mut self, tx: &Transaction) -> Observation {
        let miner = self.node.client.new_address().unwrap();
        let mut blocks = mine_blocks(&self.node, 1, &miner);
        let block = blocks.pop().unwrap();
        assert!(block.txdata.iter().any(|found| found == tx));
        self.height += 1;
        self.wallet
            .inner
            .lock()
            .unwrap()
            .wallet
            .apply_block(&block, self.height)
            .unwrap();
        Observation::Confirmed(tx.compute_txid())
    }
}
struct Clock {
    now: Instant,
    observation: Option<Observation>,
    /// Observations delivered at fixed instants, before a later deadline.
    arrivals: VecDeque<(Instant, Observation)>,
}
impl Events for Clock {
    fn now(&self) -> Instant {
        self.now
    }
    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        if let Some(input) = self.observation.take() {
            return Wake::Input(input);
        }
        if let Some(&(at, _)) = self.arrivals.front()
            && deadline.is_none_or(|deadline| at <= deadline)
        {
            self.now = self.now.max(at);
            return Wake::Input(self.arrivals.pop_front().unwrap().1);
        }
        match deadline {
            Some(at) => {
                self.now = self.now.max(at);
                Wake::Timeout
            }
            None => Wake::Closed,
        }
    }
}

/// Give the wallet one confirmed 100,000 sat coin. Returns the chain height.
fn fund(
    node: &corepc_node::Node,
    adapter: &bdk::Adapter,
) -> Result<u32, Box<dyn std::error::Error>> {
    let receive = adapter
        .inner
        .lock()
        .unwrap()
        .wallet
        .reveal_next_address(KeychainKind::External)
        .address;
    let miner = node.client.new_address()?;
    let mut blocks = mine_blocks(node, 101, &miner);
    node.client
        .send_to_address(&receive, Amount::from_sat(100_000))?;
    blocks.extend(mine_blocks(node, 1, &miner));
    let mut inner = adapter.inner.lock().unwrap();
    for (i, block) in blocks.iter().enumerate() {
        inner
            .wallet
            .apply_block(block, (i + 1).try_into().unwrap())
            .unwrap();
    }
    Ok(u32::try_from(blocks.len()).unwrap())
}

#[tokio::test]
async fn armins_candidate_is_signed_by_bdk_and_confirmed_on_regtest()
-> Result<(), Box<dyn std::error::Error>> {
    let node = fungi_regtest::start()?;
    let mut adapter = bdk::Adapter::new();
    let height = fund(&node, &adapter)?;
    let now = Instant::now();
    let grid = ScheduleGrid::new(now);
    let payable_at = now + Duration::from_secs(1);
    let payable_slot = 1_000_000 / ScheduleGrid::AUTHORED_STEP.as_micros() as u64;
    let destination = node.client.new_address()?.script_pubkey();
    let amount = Amount::from_sat(25_000);
    let intent = Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: destination.clone(),
            amount,
        }),
        PayoffCurve::new(payable_slot, [(0, Amount::from_sat(100_000))]).unwrap(),
    );
    let feerate = FeeRate::from_sat_per_vb(2).unwrap();
    let chain = Chain {
        node: Arc::new(node),
        wallet: adapter.clone(),
        height,
    };
    let mut runtime = Runtime {
        executor: Executor::new(adapter.clone(), vec![], adapter.clone(), chain).unwrap(),
        events: Clock {
            now,
            observation: None,
            arrivals: VecDeque::new(),
        },
    };
    let mut source = adapter.reporting_to(runtime.executor.signers.clone());
    let snapshot = source.snapshot().await?;
    assert_eq!(snapshot.coins.len(), 1);
    let mut state = State::new(
        InMemoryQueue::new([intent]),
        snapshot,
        feerate,
        [7; 16],
        adapter.clone(),
        StdRng::seed_from_u64(0),
        grid,
    );
    assert_eq!(
        run_scripted(&mut state, &mut runtime, Operation::id).await,
        Stopped::Closed
    );
    assert_eq!(runtime.events.now(), payable_at);
    assert_eq!(
        *state.transactions().values().next().unwrap().status(),
        Status::AwaitingConfirmation
    );
    assert_eq!(state.pending().iter().count(), 1);
    let transaction = state
        .transactions()
        .values()
        .next()
        .unwrap()
        .payment()
        .unwrap()
        .transaction
        .clone();
    let confirmation = runtime
        .executor
        .broadcaster
        .lock()
        .await
        .mine_confirmation(&transaction);
    runtime.events.observation = Some(confirmation);
    assert_eq!(
        run_scripted(&mut state, &mut runtime, Operation::id).await,
        Stopped::Closed
    );
    assert_eq!(state.pending().iter().count(), 0);
    let completed = state.transactions().values().next().unwrap();
    assert_eq!(*completed.status(), Status::Confirmed);
    let objective_cost = completed.candidate().objective_cost().unwrap();
    let payment = completed.payment().unwrap();
    let paid = &payment.transaction.output[payment.outputs[&0]];
    assert_eq!(paid.script_pubkey, destination);
    assert_eq!(paid.value, amount);
    assert_eq!(objective_cost, (amount + payment.fee).to_signed()?);
    let change = Amount::from_sat(100_000) - amount - payment.fee;
    assert_eq!(
        adapter.inner.lock().unwrap().wallet.balance().confirmed,
        change
    );
    let after = source.snapshot().await?;
    assert_eq!(after.coins.len(), 1);
    let (&change_outpoint, coin) = after.coins.first_key_value().unwrap();
    assert_eq!(coin.txout.value, change);
    assert_eq!(source.snapshot().await?, after);

    // The same executor can now sign a spend of the newly confirmed change.
    let mut spend = {
        let mut inner = adapter.inner.lock().unwrap();
        let mut builder = inner.wallet.build_tx();
        builder
            .drain_wallet()
            .drain_to(destination)
            .fee_rate(feerate);
        builder.finish()?
    };
    assert_eq!(spend.unsigned_tx.input[0].previous_output, change_outpoint);
    let signer = runtime.executor.signers.get(&change_outpoint).unwrap();
    let signing = signer.lock().await.sign(&spend);
    spend.inputs[0] = signing.await?;
    let chain = runtime.executor.broadcaster.lock().await;
    let acceptance = chain
        .node
        .client
        .test_mempool_accept(&[spend.extract_tx()?])?;
    assert!(acceptance.0[0].allowed, "{acceptance:?}");
    drop(chain);

    let malformed = Transaction {
        version: transaction::Version::TWO,
        lock_time: absolute::LockTime::ZERO,
        input: vec![TxIn {
            previous_output: change_outpoint,
            ..TxIn::default()
        }],
        output: vec![],
    };
    adapter.record(&malformed).await.unwrap();
    let rejection = runtime
        .executor
        .broadcaster
        .lock()
        .await
        .broadcast(&malformed);
    let error = rejection.await.unwrap_err();
    assert!(error.to_string().contains("bad-txns-vout-empty"), "{error}");

    eprintln!(
        "confirmed {}: payment={amount}, change={change}, fee={}, objective_cost={objective_cost}",
        payment.transaction.compute_txid(),
        payment.fee
    );
    Ok(())
}

/// A payment whose payoff rises to 100,000 sats over ten minutes.
fn rising_payment(script_pubkey: bitcoin::ScriptBuf, amount: Amount) -> Intent {
    let ten_minutes = 600 * (1_000_000 / ScheduleGrid::AUTHORED_STEP.as_micros() as u64);
    Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey,
            amount,
        }),
        PayoffCurve::new(0, [(ten_minutes, Amount::from_sat(100_000))]).unwrap(),
    )
}

#[tokio::test]
async fn intents_arriving_at_different_times_share_a_transaction_on_regtest()
-> Result<(), Box<dyn std::error::Error>> {
    let node = fungi_regtest::start()?;
    let adapter = bdk::Adapter::new();
    let height = fund(&node, &adapter)?;
    let now = Instant::now();
    let amount = Amount::from_sat(25_000);
    let first = node.client.new_address()?.script_pubkey();
    let second = node.client.new_address()?.script_pubkey();
    let arrival = now + Duration::from_secs(10);
    let chain = Chain {
        node: Arc::new(node),
        wallet: adapter.clone(),
        height,
    };
    let mut runtime = Runtime {
        executor: Executor::new(adapter.clone(), vec![], adapter.clone(), chain).unwrap(),
        events: Clock {
            now,
            observation: None,
            arrivals: VecDeque::from([(
                arrival,
                Observation::Intent(rising_payment(second.clone(), amount)),
            )]),
        },
    };
    let snapshot = adapter
        .reporting_to(runtime.executor.signers.clone())
        .snapshot()
        .await?;
    let mut state = State::new(
        InMemoryQueue::new([rising_payment(first.clone(), amount)]),
        snapshot,
        FeeRate::from_sat_per_vb(2).unwrap(),
        [7; 16],
        adapter.clone(),
        StdRng::seed_from_u64(0),
        ScheduleGrid::new(now),
    );
    assert_eq!(
        run_scripted(&mut state, &mut runtime, Operation::id).await,
        Stopped::Closed
    );
    assert!(runtime.events.now() > arrival);
    let (id, tx) = state.transactions().iter().next().unwrap();
    assert_eq!(state.transactions().len(), 1);
    // The candidate for the first intent alone was replaced before it started.
    assert_eq!(id.sequence, 1);
    assert_eq!(*tx.status(), Status::AwaitingConfirmation);
    let transaction = tx.payment().unwrap().transaction.clone();
    let confirmation = runtime
        .executor
        .broadcaster
        .lock()
        .await
        .mine_confirmation(&transaction);
    runtime.events.observation = Some(confirmation);
    assert_eq!(
        run_scripted(&mut state, &mut runtime, Operation::id).await,
        Stopped::Closed
    );

    assert_eq!(state.pending().iter().count(), 0);
    let completed = state.transactions().values().next().unwrap();
    assert_eq!(*completed.status(), Status::Confirmed);
    let payment = completed.payment().unwrap();
    for (intent, destination) in [(0, &first), (1, &second)] {
        let paid = &payment.transaction.output[payment.outputs[&intent]];
        assert_eq!(&paid.script_pubkey, destination);
        assert_eq!(paid.value, amount);
    }
    assert_eq!(payment.transaction.input.len(), 1);
    assert_eq!(
        adapter.inner.lock().unwrap().wallet.balance().confirmed,
        Amount::from_sat(100_000) - amount - amount - payment.fee
    );
    Ok(())
}

struct SharedRegtestChain {
    node: Arc<corepc_node::Node>,
    local_wallet: bdk::Adapter,
    shared: Arc<Mutex<SharedMining>>,
}

struct SharedMining {
    height: u32,
    wallets: Vec<bdk::Adapter>,
    confirm_senders: Vec<mpsc::UnboundedSender<Observation>>,
    mined_txid: Option<bitcoin::Txid>,
}

impl Broadcaster for SharedRegtestChain {
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn broadcast(
        &mut self,
        tx: &Transaction,
    ) -> impl Future<Output = Result<(), Self::Error>> + use<> {
        let node = self.node.clone();
        let local_wallet = self.local_wallet.clone();
        let shared = self.shared.clone();
        let tx = tx.clone();
        async move {
            let snapshot = local_wallet.snapshot();
            assert_eq!(snapshot.recorded.get(&tx.compute_txid()), Some(&tx));
            tokio::task::spawn_blocking(
                move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    let txid = tx.compute_txid();
                    let mut guard = shared.lock().unwrap();
                    if guard.mined_txid != Some(txid) {
                        node.client.send_raw_transaction(&tx)?;
                        let miner = node.client.new_address()?;
                        let mut blocks = mine_blocks(&node, 1, &miner);
                        let block = blocks.pop().unwrap();
                        assert!(block.txdata.iter().any(|found| found == &tx));
                        guard.height += 1;
                        let height = guard.height;
                        for wallet in &guard.wallets {
                            wallet
                                .inner
                                .lock()
                                .unwrap()
                                .wallet
                                .apply_block(&block, height)
                                .unwrap();
                        }
                        guard.mined_txid = Some(txid);
                        for sender in &guard.confirm_senders {
                            let _ = sender.send(Observation::Confirmed(txid));
                        }
                    }
                    Ok(())
                },
            )
            .await??;
            Ok(())
        }
    }
}

struct RegtestPeerEvents<R> {
    now: Instant,
    inbox: mpsc::UnboundedReceiver<Observation>,
    peer: R,
    confirmed: bool,
}

impl<R: RecvChannel<Message>> Events for RegtestPeerEvents<R> {
    fn now(&self) -> Instant {
        self.now
    }

    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        if self.confirmed && deadline.is_none() {
            return Wake::Closed;
        }
        if let Some(at) = deadline {
            self.now = self.now.max(at);
            return Wake::Timeout;
        }
        tokio::select! {
            biased;
            obs = self.inbox.recv() => {
                let obs = obs.expect("inbox stays open until confirmation");
                self.confirmed |= matches!(obs, Observation::Confirmed(_));
                Wake::Input(obs)
            }
            msg = self.peer.recv() => {
                Wake::Input(Observation::Peer(msg.expect("peer stays open until confirmation")))
            }
        }
    }
}

#[tokio::test]
async fn two_bdk_wallets_batch_collaboratively_and_confirm_on_regtest()
-> Result<(), Box<dyn std::error::Error>> {
    let node = Arc::new(fungi_regtest::start()?);
    let wallet_a = bdk::Adapter::with_seed([11; 32]);
    let wallet_b = bdk::Adapter::with_seed([22; 32]);

    let recv_a = wallet_a
        .inner
        .lock()
        .unwrap()
        .wallet
        .reveal_next_address(KeychainKind::External)
        .address;
    let recv_b = wallet_b
        .inner
        .lock()
        .unwrap()
        .wallet
        .reveal_next_address(KeychainKind::External)
        .address;
    let miner = node.client.new_address()?;
    let mut blocks = mine_blocks(&node, 101, &miner);
    node.client
        .send_to_address(&recv_a, Amount::from_sat(100_000))?;
    node.client
        .send_to_address(&recv_b, Amount::from_sat(100_000))?;
    blocks.extend(mine_blocks(&node, 1, &miner));
    for wallet in [&wallet_a, &wallet_b] {
        let mut inner = wallet.inner.lock().unwrap();
        for (i, block) in blocks.iter().enumerate() {
            inner
                .wallet
                .apply_block(block, (i + 1).try_into().unwrap())
                .unwrap();
        }
    }
    let height = u32::try_from(blocks.len()).unwrap();

    let now = Instant::now();
    let feerate = FeeRate::from_sat_per_vb(2).unwrap();
    let session_id = [0x77; 16];
    let dest_a = node.client.new_address()?.script_pubkey();
    let dest_b = node.client.new_address()?.script_pubkey();
    let amount_a = Amount::from_sat(25_000);
    let amount_b = Amount::from_sat(30_000);

    let intent_a = Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: dest_a.clone(),
            amount: amount_a,
        }),
        PayoffCurve::new(0, [(0, Amount::from_sat(100_000))]).unwrap(),
    );
    let intent_b = Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: dest_b.clone(),
            amount: amount_b,
        }),
        PayoffCurve::new(0, [(0, Amount::from_sat(100_000))]).unwrap(),
    );

    let (chan_a, chan_b) = bidirectional::<Message>(4);
    let (send_a, peer_recv_a) = chan_a.into_parts();
    let (send_b, peer_recv_b) = chan_b.into_parts();
    let (tx_obs_a, rx_obs_a) = mpsc::unbounded_channel();
    let (tx_obs_b, rx_obs_b) = mpsc::unbounded_channel();

    let shared = Arc::new(Mutex::new(SharedMining {
        height,
        wallets: vec![wallet_a.clone(), wallet_b.clone()],
        confirm_senders: vec![tx_obs_a, tx_obs_b],
        mined_txid: None,
    }));

    let exec_a = Executor::new(
        wallet_a.clone(),
        vec![],
        wallet_a.clone(),
        SharedRegtestChain {
            node: node.clone(),
            local_wallet: wallet_a.clone(),
            shared: shared.clone(),
        },
    )
    .unwrap()
    .with_peer(send_a);
    let exec_b = Executor::new(
        wallet_b.clone(),
        vec![],
        wallet_b.clone(),
        SharedRegtestChain {
            node: node.clone(),
            local_wallet: wallet_b.clone(),
            shared: shared.clone(),
        },
    )
    .unwrap()
    .with_peer(send_b);

    let mut source_a = wallet_a.reporting_to(exec_a.signers.clone());
    let mut source_b = wallet_b.reporting_to(exec_b.signers.clone());
    let snap_a = source_a.snapshot().await?;
    let snap_b = source_b.snapshot().await?;
    assert_eq!(snap_a.coins.len(), 1);
    assert_eq!(snap_b.coins.len(), 1);

    let mut state_a = State::new(
        InMemoryQueue::new([intent_a]),
        snap_a,
        feerate,
        [0xA1; 16],
        wallet_a.clone(),
        StdRng::seed_from_u64(11),
        ScheduleGrid::new(now),
    )
    .with_session(Config::two_party(session_id, Side::First).with_additional_entropy([0xCD; 32]))
    .unwrap();
    let mut state_b = State::new(
        InMemoryQueue::new([intent_b]),
        snap_b,
        feerate,
        [0xB2; 16],
        wallet_b.clone(),
        StdRng::seed_from_u64(22),
        ScheduleGrid::new(now),
    )
    .with_session(Config::two_party(session_id, Side::Second).with_additional_entropy([0xCD; 32]))
    .unwrap();

    let mut rt_a = Runtime {
        executor: exec_a,
        events: RegtestPeerEvents {
            now,
            inbox: rx_obs_a,
            peer: peer_recv_a,
            confirmed: false,
        },
    };
    let mut rt_b = Runtime {
        executor: exec_b,
        events: RegtestPeerEvents {
            now,
            inbox: rx_obs_b,
            peer: peer_recv_b,
            confirmed: false,
        },
    };

    let (stop_a, stop_b) = tokio::join!(
        run_driver(&mut state_a, &mut rt_a),
        run_driver(&mut state_b, &mut rt_b),
    );
    assert_eq!(stop_a, Stopped::Closed);
    assert_eq!(stop_b, Stopped::Closed);
    assert_eq!(state_a.pending().iter().count(), 0);
    assert_eq!(state_b.pending().iter().count(), 0);

    let done_a = state_a.transactions().values().next().unwrap();
    let done_b = state_b.transactions().values().next().unwrap();
    assert_eq!(*done_a.status(), Status::Confirmed);
    assert_eq!(*done_b.status(), Status::Confirmed);

    let pay_a = done_a.payment().unwrap();
    let pay_b = done_b.payment().unwrap();
    assert_eq!(pay_a.transaction, pay_b.transaction);
    assert_eq!(pay_a.transaction.input.len(), 2);
    assert_eq!(pay_a.transaction.output.len(), 4);
    assert_eq!(
        state_a.session_progress(&session_id),
        state_b.session_progress(&session_id)
    );
    assert_eq!(
        pay_a.transaction.output[pay_a.outputs[&0]].script_pubkey,
        dest_a
    );
    assert_eq!(pay_a.transaction.output[pay_a.outputs[&0]].value, amount_a);
    assert_eq!(
        pay_b.transaction.output[pay_b.outputs[&0]].script_pubkey,
        dest_b
    );
    assert_eq!(pay_b.transaction.output[pay_b.outputs[&0]].value, amount_b);

    // Each BDK wallet independently owns and can spend its newly confirmed change output.
    for (wallet, source) in [(&wallet_a, &mut source_a), (&wallet_b, &mut source_b)] {
        let after = source.snapshot().await?;
        assert_eq!(after.coins.len(), 1);
        let (&change_outpoint, _) = after.coins.first_key_value().unwrap();
        let mut spend = {
            let mut inner = wallet.inner.lock().unwrap();
            let mut builder = inner.wallet.build_tx();
            builder
                .drain_wallet()
                .drain_to(dest_a.clone())
                .fee_rate(feerate);
            builder.finish()?
        };
        let signer = if std::ptr::eq(wallet, &wallet_a) {
            rt_a.executor.signers.get(&change_outpoint).unwrap()
        } else {
            rt_b.executor.signers.get(&change_outpoint).unwrap()
        };
        let signed = signer.lock().await.sign(&spend).await?;
        spend.inputs[0] = signed;
        let accept = node.client.test_mempool_accept(&[spend.extract_tx()?])?;
        assert!(accept.0[0].allowed, "{accept:?}");
    }
    Ok(())
}

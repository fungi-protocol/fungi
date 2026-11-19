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
    driver::{Stopped, Wake},
    strategy::{Broadcaster, Events, Executor, Observation, Operation, Runtime, State, Status},
    wallet::{InputSigner, SnapshotSource, TransactionRecorder},
};
use fungi_regtest::corepc_node;
use rand::{SeedableRng, rngs::StdRng};
use std::{
    collections::VecDeque,
    sync::Arc,
    time::{Duration, Instant},
};

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

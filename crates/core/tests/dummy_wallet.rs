use bitcoin::hashes::Hash;
use bitcoin::{Amount, OutPoint, Psbt, ScriptBuf, Transaction, TxIn, TxOut, Txid, Weight, Witness};
use cost_function::change::ChangeCosts;
use cost_function::wallet::{Utxo, UtxoSnapshot};
use fungi_core::wallet::dummy::{Change, Error, Recorder, Signer, Snapshots};
use fungi_core::wallet::{
    ChangeSource, Coin, InputSigner, Snapshot, SnapshotSource, TransactionRecorder,
};

fn outpoint(n: u32) -> OutPoint {
    OutPoint::new(Txid::all_zeros(), n)
}

fn script(n: u8) -> ScriptBuf {
    ScriptBuf::from_bytes(vec![0x51, n])
}

fn snapshot(n: u32) -> Snapshot {
    Snapshot {
        coins: [(
            outpoint(n),
            Coin {
                txout: TxOut {
                    value: Amount::from_sat(10_000),
                    script_pubkey: script(n as u8),
                },
                satisfaction_weight: Weight::from_wu(70),
            },
        )]
        .into(),
        ..Default::default()
    }
}

fn psbt() -> Psbt {
    Psbt::from_unsigned_tx(Transaction {
        version: bitcoin::transaction::Version::TWO,
        lock_time: bitcoin::absolute::LockTime::ZERO,
        input: (0..2)
            .map(|n| TxIn {
                previous_output: outpoint(n),
                ..Default::default()
            })
            .collect(),
        output: vec![TxOut {
            value: Amount::from_sat(15_000),
            script_pubkey: script(3),
        }],
    })
    .unwrap()
}

async fn observe<S: SnapshotSource>(source: &mut S) -> Result<Snapshot, S::Error> {
    source.snapshot().await
}

async fn allocate<C: ChangeSource>(source: &mut C) -> Result<ScriptBuf, C::Error> {
    source.next_script().await
}

async fn sign<S: InputSigner>(
    signer: &mut S,
    psbt: &Psbt,
) -> Result<bitcoin::psbt::Input, S::Error> {
    signer.sign(psbt).await
}

async fn retain<R: TransactionRecorder>(
    recorder: &mut R,
    tx: &Transaction,
) -> Result<(), R::Error> {
    recorder.record(tx).await
}

#[tokio::test]
async fn snapshots_supply_pricing_coins_without_changing_previous_observations() {
    let mut source = Snapshots::new([Ok(snapshot(0)), Ok(snapshot(1))]);
    let first = observe(&mut source).await.unwrap();
    let second = observe(&mut source).await.unwrap();

    assert_eq!(
        first.iter().collect::<Vec<_>>(),
        vec![Utxo::new(
            outpoint(0),
            Amount::from_sat(10_000),
            Weight::from_wu(70)
        )]
    );
    assert_eq!(
        second.coins.keys().copied().collect::<Vec<_>>(),
        vec![outpoint(1)]
    );
    assert_eq!(first, snapshot(0));
    assert_eq!(source.requests(), 2);
    assert_eq!(observe(&mut source).await, Err(Error::Exhausted));
}

#[tokio::test]
async fn a_failed_snapshot_does_not_consume_the_next_success() {
    let mut source = Snapshots::new([Err(Error::Scripted("unavailable")), Ok(snapshot(0))]);
    assert_eq!(
        observe(&mut source).await,
        Err(Error::Scripted("unavailable"))
    );
    assert_eq!(observe(&mut source).await, Ok(snapshot(0)));
}

#[tokio::test]
async fn pricing_change_does_not_allocate_a_script() {
    let template = script(0);
    let mut change = Change::new(
        template.clone(),
        Weight::from_wu(70),
        [Ok(script(1)), Err(Error::Scripted("locked")), Ok(script(2))],
    );
    assert_eq!(
        change.output_weight(),
        TxOut {
            value: Amount::ZERO,
            script_pubkey: template.clone()
        }
        .weight()
    );
    assert_eq!(change.satisfaction_weight(), Weight::from_wu(70));
    assert_eq!(change.dust(), template.minimal_non_dust());
    assert_eq!(change.requests(), 0);
    assert_eq!(allocate(&mut change).await, Ok(script(1)));
    assert_eq!(allocate(&mut change).await, Err(Error::Scripted("locked")));
    assert_eq!(allocate(&mut change).await, Ok(script(2)));
    assert_eq!(allocate(&mut change).await, Err(Error::Exhausted));
    assert_eq!(change.requests(), 4);
}

#[tokio::test]
async fn each_signer_receives_the_whole_psbt_and_returns_its_input_data() {
    let mut transaction = psbt();
    transaction.inputs[0].witness_utxo = Some(snapshot(0).coins[&outpoint(0)].txout.clone());
    transaction.inputs[1].witness_utxo = Some(snapshot(1).coins[&outpoint(1)].txout.clone());
    let before = transaction.clone();
    let result = bitcoin::psbt::Input {
        final_script_witness: Some(Witness::from_slice(&[b"simulated result"])),
        ..Default::default()
    };
    let mut first = Signer::new(outpoint(0), [Ok(result.clone())]);
    let mut second = Signer::new(outpoint(1), [Err(Error::Scripted("refused"))]);

    assert_eq!(first.outpoint(), outpoint(0));
    assert_eq!(sign(&mut first, &transaction).await, Ok(result));
    assert_eq!(
        sign(&mut second, &transaction).await,
        Err(Error::Scripted("refused"))
    );
    assert_eq!(transaction, before);
    assert_eq!(first.requests(), std::slice::from_ref(&before));
    assert_eq!(second.requests(), &[before]);
    assert_eq!(sign(&mut first, &transaction).await, Err(Error::Exhausted));
}

#[tokio::test]
async fn signing_a_different_or_repeated_outpoint_does_not_consume_a_reply() {
    let result = bitcoin::psbt::Input::default();
    let mut signer = Signer::new(outpoint(2), [Ok(result.clone())]);
    let mut transaction = psbt();
    assert_eq!(
        sign(&mut signer, &transaction).await,
        Err(Error::MissingInput(outpoint(2)))
    );
    transaction.unsigned_tx.input[0].previous_output = outpoint(2);
    transaction.unsigned_tx.input[1].previous_output = outpoint(2);
    assert_eq!(
        sign(&mut signer, &transaction).await,
        Err(Error::RepeatedInput(outpoint(2)))
    );
    transaction.unsigned_tx.input[1].previous_output = outpoint(1);
    assert_eq!(sign(&mut signer, &transaction).await, Ok(result));
}

#[tokio::test]
async fn recording_retains_successful_transactions_without_requiring_a_broadcaster() {
    let transaction = psbt().unsigned_tx;
    let mut recorder = Recorder::new([Err(Error::Scripted("storage failed")), Ok(()), Ok(())]);
    assert_eq!(
        retain(&mut recorder, &transaction).await,
        Err(Error::Scripted("storage failed"))
    );
    assert!(recorder.recorded().is_empty());
    assert_eq!(retain(&mut recorder, &transaction).await, Ok(()));
    assert_eq!(retain(&mut recorder, &transaction).await, Ok(()));
    assert_eq!(recorder.recorded(), std::slice::from_ref(&transaction));
    assert_eq!(
        recorder.requests(),
        &[
            transaction.clone(),
            transaction.clone(),
            transaction.clone()
        ]
    );
    assert_eq!(
        retain(&mut recorder, &transaction).await,
        Err(Error::Exhausted)
    );
    assert_eq!(recorder.recorded(), &[transaction]);
}

#[tokio::test]
async fn a_wallet_channel_can_answer_a_second_request_while_the_first_waits() {
    struct Channel {
        requests: usize,
    }
    impl SnapshotSource for Channel {
        type Error = Error;
        fn snapshot(&mut self) -> impl Future<Output = Result<Snapshot, Error>> + Send + use<> {
            self.requests += 1;
            let request = self.requests;
            async move {
                if request == 1 {
                    std::future::pending::<()>().await;
                }
                Ok(snapshot(request as u32))
            }
        }
    }
    let mut channel = Channel { requests: 0 };
    let mut first = std::pin::pin!(channel.snapshot());
    std::future::poll_fn(|cx| {
        assert!(first.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    assert_eq!(channel.snapshot().await, Ok(snapshot(2)));
}

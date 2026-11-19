//! Test adapter for the fixture's P2TR key-spend descriptors.
//! Retention is separate from BDK's chain observations; storage is not designed here.

use bdk_wallet::{
    KeychainKind, SignOptions, Wallet, descriptor::IntoWalletDescriptor,
    miniscript::descriptor::KeyMapWrapper,
};
use bitcoin::{
    Amount, Network, OutPoint, Psbt, ScriptBuf, Transaction, TxOut, Txid, Weight, bip32::Xpriv,
    psbt, secp256k1::Secp256k1,
};
use cost_function::change::ChangeCosts;
use fungi_core::{
    strategy::{Error, Signers},
    wallet::{ChangeSource, Coin, InputSigner, Snapshot, SnapshotSource, TransactionRecorder},
};
use std::{
    collections::BTreeMap,
    convert::Infallible,
    sync::{Arc, Mutex},
};

pub struct Inner {
    pub wallet: Wallet,
    pub retained: BTreeMap<Txid, Transaction>,
}
#[derive(Clone)]
pub struct Adapter {
    pub inner: Arc<Mutex<Inner>>,
    keys: Arc<KeyMapWrapper>,
}
impl Adapter {
    pub fn new() -> Self {
        Self::with_seed([42; 32])
    }

    pub fn with_seed(seed: [u8; 32]) -> Self {
        let secp = Secp256k1::new();
        let network = Network::Regtest;
        let master = Xpriv::new_master(network, &seed).unwrap();
        let (external, mut keys) = format!("tr({master}/86'/1'/0'/0/*)")
            .into_wallet_descriptor(&secp, network.into())
            .unwrap();
        let (internal, change_keys) = format!("tr({master}/86'/1'/0'/1/*)")
            .into_wallet_descriptor(&secp, network.into())
            .unwrap();
        keys.extend(change_keys);
        let wallet = Wallet::create(external, internal)
            .network(network)
            .create_wallet_no_persist()
            .unwrap();
        Self {
            inner: Arc::new(Mutex::new(Inner {
                wallet,
                retained: BTreeMap::new(),
            })),
            keys: Arc::new(KeyMapWrapper::from(keys)),
        }
    }

    fn change_template(&self) -> ScriptBuf {
        self.inner
            .lock()
            .unwrap()
            .wallet
            .peek_address(KeychainKind::Internal, 0)
            .address
            .script_pubkey()
    }

    pub fn snapshot(&self) -> Snapshot {
        let inner = self.inner.lock().unwrap();
        let coins = inner
            .wallet
            .list_unspent()
            .map(|coin| {
                assert!(coin.txout.script_pubkey.is_p2tr());
                (
                    coin.outpoint,
                    Coin {
                        txout: coin.txout,
                        satisfaction_weight: Weight::from_wu(70),
                    },
                )
            })
            .collect();
        Snapshot {
            coins,
            recorded: inner.retained.clone(),
        }
    }

    pub fn reporting_to(&self, signers: Arc<Signers<Signer>>) -> Coins {
        Coins {
            adapter: self.clone(),
            signers,
        }
    }
}

/// Register each newly observed outpoint before delivering its wallet snapshot.
pub struct Coins {
    adapter: Adapter,
    signers: Arc<Signers<Signer>>,
}

impl SnapshotSource for Coins {
    type Error = Infallible;
    fn snapshot(&mut self) -> impl Future<Output = Result<Snapshot, Infallible>> + Send + use<> {
        let snapshot = self.adapter.snapshot();
        for &outpoint in snapshot.coins.keys() {
            self.signers.register(Signer {
                adapter: self.adapter.clone(),
                outpoint,
            });
        }
        std::future::ready(Ok(snapshot))
    }
}

impl ChangeCosts for Adapter {
    fn output_weight(&self) -> Weight {
        TxOut {
            value: Amount::ZERO,
            script_pubkey: self.change_template(),
        }
        .weight()
    }
    fn satisfaction_weight(&self) -> Weight {
        Weight::from_wu(70)
    }
    fn dust(&self) -> Amount {
        self.change_template().minimal_non_dust()
    }
}
impl ChangeSource for Adapter {
    type Error = Infallible;
    fn next_script(
        &mut self,
    ) -> impl Future<Output = Result<ScriptBuf, Infallible>> + Send + use<> {
        let inner = self.inner.clone();
        async move {
            Ok(inner
                .lock()
                .unwrap()
                .wallet
                .reveal_next_address(KeychainKind::Internal)
                .address
                .script_pubkey())
        }
    }
}
impl TransactionRecorder for Adapter {
    type Error = Infallible;
    fn record(
        &mut self,
        transaction: &Transaction,
    ) -> impl Future<Output = Result<(), Infallible>> + Send + use<> {
        let inner = self.inner.clone();
        let transaction = transaction.clone();
        async move {
            inner
                .lock()
                .unwrap()
                .retained
                .insert(transaction.compute_txid(), transaction);
            Ok(())
        }
    }
}

pub struct Signer {
    pub adapter: Adapter,
    pub outpoint: OutPoint,
}
impl InputSigner for Signer {
    type Error = Error;
    fn outpoint(&self) -> OutPoint {
        self.outpoint
    }
    fn sign(
        &mut self,
        psbt: &Psbt,
    ) -> impl Future<Output = Result<psbt::Input, Error>> + Send + use<> {
        let adapter = self.adapter.clone();
        let outpoint = self.outpoint;
        let mut psbt = psbt.clone();
        async move {
            let inner = adapter.inner.lock().unwrap();
            let wallet = &inner.wallet;
            let positions: Vec<_> = psbt
                .unsigned_tx
                .input
                .iter()
                .enumerate()
                .filter_map(|(i, input)| (input.previous_output == outpoint).then_some(i))
                .collect();
            let [index] = positions.as_slice() else {
                return Err(Error(
                    "signer requires exactly one matching outpoint".into(),
                ));
            };
            let utxo = wallet
                .get_utxo(outpoint)
                .ok_or_else(|| Error("wallet does not own input".into()))?;
            let metadata = wallet
                .get_psbt_input(utxo, None, true)
                .expect("the fixture wallet knows its P2TR input metadata");
            if metadata.witness_utxo != psbt.inputs[*index].witness_utxo {
                return Err(Error("signing prevout differs from construction".into()));
            }
            psbt.inputs[*index] = metadata;
            psbt.sign(adapter.keys.as_ref(), wallet.secp_ctx())
                .map_err(|(_, errors)| Error(format!("signing: {errors:?}")))?;
            // Other signers' inputs may still be incomplete.
            wallet
                .finalize_psbt(&mut psbt, SignOptions::default())
                .expect("the fixture supplies a well-formed PSBT");
            if psbt.inputs[*index].final_script_witness.is_none() {
                return Err(Error("owned input was not finalized".into()));
            }
            Ok(psbt.inputs.remove(*index))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{TxIn, absolute::LockTime, transaction::Version};

    #[tokio::test]
    async fn signing_requires_the_owned_prevout_and_all_taproot_signing_context() {
        let adapter = Adapter::new();
        let foreign = OutPoint::new(Txid::from_raw_hash(bitcoin::hashes::Hash::all_zeros()), 0);
        let transaction = |outpoints: &[OutPoint], output| Transaction {
            version: Version::TWO,
            lock_time: LockTime::ZERO,
            input: outpoints
                .iter()
                .map(|&previous_output| TxIn {
                    previous_output,
                    ..TxIn::default()
                })
                .collect(),
            output,
        };
        let prevout = {
            let mut inner = adapter.inner.lock().unwrap();
            let address = inner.wallet.reveal_next_address(KeychainKind::External);
            let prevout = TxOut {
                script_pubkey: address.address.script_pubkey(),
                value: Amount::from_sat(10_000),
            };
            let funding = transaction(&[foreign], vec![prevout.clone()]);
            let outpoint = OutPoint::new(funding.compute_txid(), 0);
            inner.wallet.apply_unconfirmed_txs([(funding, 0)]);
            (outpoint, prevout)
        };
        let (outpoint, prevout) = prevout;
        let psbt = |outpoints: &[OutPoint]| {
            let mut tx = Psbt::from_unsigned_tx(transaction(outpoints, vec![])).unwrap();
            tx.inputs[0].witness_utxo = Some(prevout.clone());
            tx
        };
        let mut signer = Signer {
            adapter: adapter.clone(),
            outpoint,
        };

        for outpoints in [&[foreign][..], &[outpoint, outpoint]] {
            let error = signer.sign(&psbt(outpoints)).await.unwrap_err();
            assert_eq!(error.0, "signer requires exactly one matching outpoint");
        }

        let mut foreign_signer = Signer {
            adapter,
            outpoint: foreign,
        };
        let error = foreign_signer.sign(&psbt(&[foreign])).await.unwrap_err();
        assert_eq!(error.0, "wallet does not own input");

        let mut changed = psbt(&[outpoint]);
        changed.inputs[0].witness_utxo.as_mut().unwrap().value -= Amount::from_sat(1);
        let error = signer.sign(&changed).await.unwrap_err();
        assert_eq!(error.0, "signing prevout differs from construction");

        let error = signer.sign(&psbt(&[outpoint, foreign])).await.unwrap_err();
        assert!(error.0.contains("MissingSpendUtxo"), "{error}");

        signer.adapter.keys = Arc::new(KeyMapWrapper::from(BTreeMap::new()));
        let error = signer.sign(&psbt(&[outpoint])).await.unwrap_err();
        assert_eq!(error.0, "owned input was not finalized");
    }
}

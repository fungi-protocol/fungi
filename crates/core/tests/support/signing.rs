//! Deterministic keys for simulations whose signer results are verified.
use bitcoin::{
    OutPoint, Psbt, ScriptBuf, Witness,
    hashes::Hash,
    key::TapTweak,
    psbt,
    secp256k1::{Keypair, Message, Secp256k1, SecretKey},
    sighash::{Prevouts, SighashCache, TapSighashType},
};
use fungi_core::wallet::{InputSigner, dummy::Error};
use std::collections::VecDeque;

fn key(outpoint: OutPoint) -> Keypair {
    let seed = bitcoin::hashes::sha256::Hash::hash(outpoint.to_string().as_bytes());
    Keypair::from_secret_key(
        &Secp256k1::new(),
        &SecretKey::from_slice(seed.as_byte_array()).unwrap(),
    )
}

pub fn script(outpoint: OutPoint) -> ScriptBuf {
    ScriptBuf::new_p2tr(&Secp256k1::new(), key(outpoint).x_only_public_key().0, None)
}

pub fn sign(psbt: &Psbt, outpoint: OutPoint) -> psbt::Input {
    let index = psbt
        .unsigned_tx
        .input
        .iter()
        .position(|i| i.previous_output == outpoint)
        .unwrap();
    let prevouts: Vec<_> = psbt
        .inputs
        .iter()
        .map(|i| i.witness_utxo.clone().unwrap())
        .collect();
    let sighash = SighashCache::new(&psbt.unsigned_tx)
        .taproot_key_spend_signature_hash(index, &Prevouts::All(&prevouts), TapSighashType::Default)
        .unwrap();
    let secp = Secp256k1::new();
    let key = key(outpoint).tap_tweak(&secp, None).to_keypair();
    let signature =
        secp.sign_schnorr_no_aux_rand(&Message::from_digest(sighash.to_byte_array()), &key);
    psbt::Input {
        final_script_witness: Some(Witness::from_slice(&[signature.as_ref()])),
        ..Default::default()
    }
}

pub struct Signer {
    outpoint: OutPoint,
    replies: VecDeque<Result<(), Error>>,
    requests: Vec<Psbt>,
    pub delay: Option<futures::channel::oneshot::Receiver<()>>,
    pub corrupt: bool,
    pub edit: Option<fn(&mut psbt::Input)>,
}
impl Signer {
    pub fn new(outpoint: OutPoint, replies: impl IntoIterator<Item = Result<(), Error>>) -> Self {
        Self {
            outpoint,
            replies: replies.into_iter().collect(),
            requests: vec![],
            delay: None,
            corrupt: false,
            edit: None,
        }
    }
    pub fn requests(&self) -> &[Psbt] {
        &self.requests
    }
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
        self.requests.push(psbt.clone());
        let result = self
            .replies
            .pop_front()
            .unwrap_or(Err(Error::Exhausted))
            .map(|()| {
                let mut signed = sign(psbt, self.outpoint);
                if self.corrupt {
                    signed.final_script_witness = Some(Witness::from_slice(&[vec![0; 64]]));
                }
                if let Some(edit) = self.edit {
                    edit(&mut signed);
                }
                signed
            });
        let delay = self.delay.take();
        async move {
            if let Some(delay) = delay {
                delay
                    .await
                    .map_err(|_| Error::Scripted("signer channel closed"))?;
            }
            result
        }
    }
}

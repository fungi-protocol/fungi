//! Apply verified P2TR signatures and check the signed transaction's feerate.

use super::{Error, error};
use bitcoin::{
    Amount, FeeRate, Psbt, Weight,
    hashes::Hash,
    psbt,
    secp256k1::{Message, Secp256k1, XOnlyPublicKey, schnorr::Signature},
    sighash::{Prevouts, SighashCache, TapSighashType},
};

pub(super) fn apply(psbt: &mut Psbt, position: usize, signed: psbt::Input) -> Result<(), Error> {
    if signed
        .witness_utxo
        .as_ref()
        .is_some_and(|prevout| Some(prevout) != psbt.inputs[position].witness_utxo.as_ref())
    {
        return Err(error("signer changed the prevout"));
    }
    let witness = signed
        .final_script_witness
        .ok_or_else(|| error("signer did not finalize the input"))?;
    let Some(signature) = witness
        .iter()
        .next()
        .filter(|signature| witness.len() == 1 && signature.len() == 64)
    else {
        return Err(error("expected a default-sighash P2TR key-spend witness"));
    };
    // Freeze validates every input's witness_utxo before signing, including peer inputs
    // that are absent from the local wallet snapshot.
    let prevouts: Vec<_> = psbt
        .inputs
        .iter()
        .map(|input| {
            input
                .witness_utxo
                .as_ref()
                .expect("freeze validated every witness utxo")
        })
        .collect();
    if !prevouts[position].script_pubkey.is_p2tr() {
        return Err(error("expected a P2TR prevout"));
    }
    let sighash = SighashCache::new(&psbt.unsigned_tx)
        .taproot_key_spend_signature_hash(
            position,
            &Prevouts::All(&prevouts),
            TapSighashType::Default,
        )
        .map_err(error)?;
    let key = XOnlyPublicKey::from_slice(&prevouts[position].script_pubkey.as_bytes()[2..])
        .map_err(error)?;
    let signature = Signature::from_slice(signature).map_err(error)?;
    Secp256k1::verification_only()
        .verify_schnorr(
            &signature,
            &Message::from_digest(sighash.to_byte_array()),
            &key,
        )
        .map_err(error)?;
    psbt.inputs[position].final_script_witness = Some(witness);
    Ok(())
}

pub(super) fn validate_feerate(fee: Amount, weight: Weight, feerate: FeeRate) -> Result<(), Error> {
    let required = feerate
        .fee_wu(weight)
        .ok_or_else(|| error("fee overflow"))?;
    if fee < required {
        return Err(error("signed transaction is below the requested feerate"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut, Witness, absolute};

    #[test]
    fn the_required_fee_must_fit_amount_arithmetic() {
        assert_eq!(
            validate_feerate(Amount::MAX_MONEY, Weight::from_wu(2), FeeRate::MAX)
                .unwrap_err()
                .0,
            "fee overflow"
        );
    }

    #[test]
    fn non_p2tr_prevouts_are_rejected_without_panicking() {
        let mut psbt = Psbt::from_unsigned_tx(Transaction {
            version: bitcoin::transaction::Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: vec![TxIn {
                previous_output: OutPoint::null(),
                script_sig: ScriptBuf::new(),
                sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                witness: Witness::new(),
            }],
            output: vec![],
        })
        .unwrap();
        psbt.inputs[0].witness_utxo = Some(TxOut {
            value: Amount::from_sat(1_000),
            script_pubkey: ScriptBuf::new(),
        });
        let signed = psbt::Input {
            final_script_witness: Some(Witness::from_slice(&[[0u8; 64]])),
            ..Default::default()
        };
        assert_eq!(
            apply(&mut psbt, 0, signed).unwrap_err().0,
            "expected a P2TR prevout"
        );
    }
}

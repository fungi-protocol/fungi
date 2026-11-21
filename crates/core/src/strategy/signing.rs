//! Apply verified P2TR signatures and check the signed transaction's feerate.

use super::{Error, error};
use crate::wallet::Snapshot;
use bitcoin::{
    Amount, FeeRate, Psbt, Weight,
    hashes::Hash,
    psbt,
    secp256k1::{Message, Secp256k1, XOnlyPublicKey, schnorr::Signature},
    sighash::{Prevouts, SighashCache, TapSighashType},
};

pub(super) fn apply(
    psbt: &mut Psbt,
    snapshot: &Snapshot,
    position: usize,
    signed: psbt::Input,
) -> Result<(), Error> {
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
    // Ordering validates these prevouts against the pricing snapshot. Signers
    // can only contribute the witness, so every signature uses the same prevouts.
    let prevouts: Vec<_> = psbt
        .unsigned_tx
        .input
        .iter()
        .map(|input| &snapshot.coins[&input.previous_output].txout)
        .collect();
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

    #[test]
    fn the_required_fee_must_fit_amount_arithmetic() {
        assert_eq!(
            validate_feerate(Amount::MAX_MONEY, Weight::from_wu(2), FeeRate::MAX)
                .unwrap_err()
                .0,
            "fee overflow"
        );
    }
}

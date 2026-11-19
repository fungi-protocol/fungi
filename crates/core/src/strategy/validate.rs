//! Verify change allocations and the ordered transaction before signing.

use super::{Error, error};
use bitcoin::{
    Amount, OutPoint, Psbt, Sequence, SignedAmount, TxOut, absolute, transaction::Version,
};
use cost_function::change::OutputWithoutScriptpubkey;
use std::collections::BTreeMap;

pub(super) fn change(output: &TxOut, priced: &OutputWithoutScriptpubkey) -> Result<(), Error> {
    if output.weight() != priced.weight
        || output.script_pubkey.minimal_non_dust() != priced.dust
        || output.value < priced.dust
    {
        return Err(error("allocated change differs from its priced template"));
    }
    Ok(())
}

pub(super) fn transaction(
    psbt: &Psbt,
    mut expected_outputs: Vec<TxOut>,
    mut expected_inputs: BTreeMap<OutPoint, &TxOut>,
) -> Result<(), Error> {
    let tx = &psbt.unsigned_tx;
    if tx.version != Version::TWO
        || tx.lock_time != absolute::LockTime::ZERO
        || tx.input.len() != expected_inputs.len()
        || psbt.inputs.len() != tx.input.len()
    {
        return Err(error("unexpected transaction fields"));
    }
    if tx
        .output
        .iter()
        .any(|output| output.value < output.script_pubkey.minimal_non_dust())
    {
        return Err(error("transaction output is below dust"));
    }
    let mut actual_outputs = tx.output.clone();
    expected_outputs.sort();
    actual_outputs.sort();
    if actual_outputs != expected_outputs {
        return Err(error("transaction outputs differ from the candidate"));
    }
    for (input, data) in tx.input.iter().zip(&psbt.inputs) {
        let prevout = expected_inputs
            .remove(&input.previous_output)
            .ok_or_else(|| error("unknown or repeated prevout"))?;
        if data.witness_utxo.as_ref() != Some(prevout)
            || input.sequence != Sequence::ENABLE_RBF_NO_LOCKTIME
        {
            return Err(error("unexpected input data"));
        }
    }
    Ok(())
}

/// Check global input/output balance and verify that the local wallet's net outflow
/// matches its planned objective cost without subsidizing peer outputs.
pub(super) fn accounting(
    all_inputs: impl Iterator<Item = Amount>,
    all_outputs: impl Iterator<Item = Amount>,
    explicit_fees: Option<&[Amount]>,
    local_inputs: impl Iterator<Item = Amount>,
    local_change: impl Iterator<Item = Amount>,
    local_payments: impl Iterator<Item = Amount>,
    objective_cost: Option<SignedAmount>,
) -> Result<Amount, Error> {
    let total_spent = sum(all_inputs, "input")?;
    let total_outputs = sum(all_outputs, "output")?;
    let fee = total_spent
        .checked_sub(total_outputs)
        .ok_or_else(|| error("transaction is unfunded"))?;
    if let Some(fees) = explicit_fees {
        let agreed_fee = sum(fees.iter().copied(), "fee")?;
        if fee != agreed_fee {
            return Err(error("agreed outputs and fees do not balance inputs"));
        }
    }
    let local_spent = sum(local_inputs, "local input")?;
    let local_change_sum = sum(local_change, "change")?;
    let local_payments_sum = sum(local_payments, "payment")?;
    let local_outflow = local_spent
        .checked_sub(local_change_sum)
        .ok_or_else(|| error("local change exceeds local inputs"))?;
    let local_fee = local_outflow
        .checked_sub(local_payments_sum)
        .ok_or_else(|| error("local inputs do not cover local outputs"))?;
    if fee < local_fee {
        return Err(error("peer outputs consume local fee"));
    }
    let cost = local_outflow
        .to_signed()
        .map_err(|_| error("objective cost overflow"))?;
    if Some(cost) != objective_cost {
        return Err(error("objective cost differs from final accounting"));
    }
    Ok(fee)
}

fn sum(mut values: impl Iterator<Item = Amount>, kind: &str) -> Result<Amount, Error> {
    values
        .try_fold(Amount::ZERO, Amount::checked_add)
        .ok_or_else(|| error(format!("{kind} value overflow")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{ScriptBuf, Transaction, TxIn, Txid, Weight, Witness, hashes::Hash};

    #[test]
    fn allocated_change_matches_weight_and_dust_price() {
        let script = ScriptBuf::from_bytes([vec![0x51, 0x20], vec![0; 32]].concat());
        let non_witness = ScriptBuf::from_bytes(vec![0x51; script.len()]);
        let at_dust = TxOut::minimal_non_dust(script);
        let priced = OutputWithoutScriptpubkey {
            value: at_dust.value,
            weight: at_dust.weight(),
            satisfaction_weight: Weight::ZERO,
            dust: at_dust.value,
        };
        let wrong_dust = TxOut {
            script_pubkey: non_witness,
            ..at_dust.clone()
        };
        let below_dust = TxOut {
            value: at_dust.value - Amount::ONE_SAT,
            ..at_dust.clone()
        };
        for (output, accepted) in [(at_dust, true), (wrong_dust, false), (below_dust, false)] {
            assert_eq!(change(&output, &priced).is_ok(), accepted);
        }
    }

    #[test]
    fn ordering_preserves_transaction_fields_output_values_and_selected_prevouts() {
        let outpoints = [1, 2].map(|index| OutPoint::new(Txid::all_zeros(), index));
        let prevout = TxOut {
            value: Amount::from_sat(20_000),
            script_pubkey: ScriptBuf::new(),
        };
        let expected = [TxOut {
            value: Amount::from_sat(10_000),
            script_pubkey: ScriptBuf::new(),
        }];
        let mut psbt = Psbt::from_unsigned_tx(Transaction {
            version: Version::TWO,
            lock_time: absolute::LockTime::ZERO,
            input: outpoints
                .into_iter()
                .rev()
                .map(|previous_output| TxIn {
                    previous_output,
                    script_sig: ScriptBuf::new(),
                    sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    witness: Witness::new(),
                })
                .collect(),
            output: expected.to_vec(),
        })
        .unwrap();
        for input in &mut psbt.inputs {
            input.witness_utxo = Some(prevout.clone());
        }
        let validate = |psbt: &Psbt| {
            transaction(
                psbt,
                expected.to_vec(),
                BTreeMap::from(outpoints.map(|outpoint| (outpoint, &prevout))),
            )
        };
        validate(&psbt).unwrap();

        psbt.unsigned_tx.output[0].value = Amount::ONE_SAT;
        assert_eq!(
            validate(&psbt).unwrap_err().0,
            "transaction output is below dust"
        );
        psbt.unsigned_tx.output[0] = expected[0].clone();

        psbt.unsigned_tx.version = Version::ONE;
        assert_eq!(
            validate(&psbt).unwrap_err().0,
            "unexpected transaction fields"
        );
        psbt.unsigned_tx.version = Version::TWO;
        let input = psbt.unsigned_tx.input.pop().unwrap();
        let metadata = psbt.inputs.pop().unwrap();
        assert_eq!(
            validate(&psbt).unwrap_err().0,
            "unexpected transaction fields"
        );
        psbt.unsigned_tx.input.push(input);
        assert_eq!(
            validate(&psbt).unwrap_err().0,
            "unexpected transaction fields"
        );
        psbt.inputs.push(metadata);
        psbt.unsigned_tx.output[0].value = Amount::from_sat(9_000);
        assert_eq!(
            validate(&psbt).unwrap_err().0,
            "transaction outputs differ from the candidate"
        );
        psbt.unsigned_tx.output[0] = expected[0].clone();
        for vout in [1, 3] {
            psbt.unsigned_tx.input[0].previous_output.vout = vout;
            assert_eq!(
                validate(&psbt).unwrap_err().0,
                "unknown or repeated prevout"
            );
        }
        psbt.unsigned_tx.input[0].previous_output = outpoints[1];
        psbt.inputs[0].witness_utxo = None;
        assert_eq!(validate(&psbt).unwrap_err().0, "unexpected input data");
    }

    #[test]
    fn objective_cost_and_multiparty_balance_are_checked_independently() {
        let unilateral =
            |inputs: &[u64], outputs: &[u64], change: &[u64], payments: &[u64], objective| {
                accounting(
                    inputs.iter().copied().map(Amount::from_sat),
                    outputs.iter().copied().map(Amount::from_sat),
                    None,
                    inputs.iter().copied().map(Amount::from_sat),
                    change.iter().copied().map(Amount::from_sat),
                    payments.iter().copied().map(Amount::from_sat),
                    Some(SignedAmount::from_sat(objective)),
                )
            };
        assert_eq!(
            unilateral(&[20_000], &[12_000, 7_900], &[7_900], &[12_000], 12_100),
            Ok(Amount::from_sat(100))
        );
        assert_eq!(
            unilateral(&[20_000], &[12_000, 7_900], &[7_900], &[12_000], 12_000)
                .unwrap_err()
                .0,
            "objective cost differs from final accounting"
        );
        assert_eq!(
            unilateral(&[19_000], &[20_000], &[], &[20_000], 20_000)
                .unwrap_err()
                .0,
            "transaction is unfunded"
        );
        assert_eq!(
            unilateral(&[20_000], &[10_000], &[21_000], &[], 0)
                .unwrap_err()
                .0,
            "local change exceeds local inputs"
        );
        assert_eq!(
            unilateral(&[20_000], &[10_000], &[15_000], &[6_000], 5_000)
                .unwrap_err()
                .0,
            "local inputs do not cover local outputs"
        );
        // Peer contributes 15,000 in and 14,920 out (80 sat peer fee): accepted.
        assert_eq!(
            accounting(
                [20_000, 15_000].into_iter().map(Amount::from_sat),
                [12_000, 7_900, 14_920].into_iter().map(Amount::from_sat),
                Some(&[Amount::from_sat(100), Amount::from_sat(80)]),
                [20_000].into_iter().map(Amount::from_sat),
                [7_900].into_iter().map(Amount::from_sat),
                [12_000].into_iter().map(Amount::from_sat),
                Some(SignedAmount::from_sat(12_100)),
            ),
            Ok(Amount::from_sat(180))
        );
        assert_eq!(
            accounting(
                [20_000, 15_000].into_iter().map(Amount::from_sat),
                [12_000, 7_900, 14_920].into_iter().map(Amount::from_sat),
                Some(&[Amount::from_sat(100), Amount::from_sat(79)]),
                [20_000].into_iter().map(Amount::from_sat),
                [7_900].into_iter().map(Amount::from_sat),
                [12_000].into_iter().map(Amount::from_sat),
                Some(SignedAmount::from_sat(12_100)),
            )
            .unwrap_err()
            .0,
            "agreed outputs and fees do not balance inputs"
        );
        // Peer contributes 15,000 in and 15,050 out (consumes 50 sats of local fee): rejected.
        assert_eq!(
            accounting(
                [20_000, 15_000].into_iter().map(Amount::from_sat),
                [12_000, 7_900, 15_050].into_iter().map(Amount::from_sat),
                None,
                [20_000].into_iter().map(Amount::from_sat),
                [7_900].into_iter().map(Amount::from_sat),
                [12_000].into_iter().map(Amount::from_sat),
                Some(SignedAmount::from_sat(12_100)),
            )
            .unwrap_err()
            .0,
            "peer outputs consume local fee"
        );
        assert_eq!(
            unilateral(&[u64::MAX, 1], &[], &[], &[], 0).unwrap_err().0,
            "input value overflow"
        );
        assert_eq!(
            accounting(
                [Amount::from_sat(u64::MAX)].into_iter(),
                std::iter::empty(),
                None,
                [Amount::from_sat(u64::MAX)].into_iter(),
                std::iter::empty(),
                std::iter::empty(),
                None,
            )
            .unwrap_err()
            .0,
            "objective cost overflow"
        );
    }
}

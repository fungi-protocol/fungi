//! Construction state and effect receipts for one transaction.

use super::{Action, Error, ExecutionContext, Payment, Update, error, identity, signing, validate};
use crate::wallet::{Coin, Snapshot};
use bitcoin::{Amount, OutPoint, Psbt, ScriptBuf, Sequence, TxOut, absolute, psbt, transaction};
use concurrent_psbt::{
    global::{
        PSBT_GLOBAL_SORT_KEY_DERIVATION_SUBTYPE, PSBT_GLOBAL_TX_UNORDERED_SUBTYPE,
        PSBT_GLOBAL_TX_UUID_SUBTYPE,
    },
    input::Input,
    output::{Output, UniqueId},
    roles::constructor::Constructor,
    sorter::{BothModifiable, ExplicitKeysForbidden, OutputsModifiable, Seeded},
};
use cost_function::{change::OutputWithoutScriptpubkey, wallet::Utxo};
use futures::lock::Mutex;
use std::{collections::BTreeMap, sync::Arc};

type Inputs = Constructor<BothModifiable, Seeded<ExplicitKeysForbidden>>;
type Outputs = Constructor<OutputsModifiable, Seeded<ExplicitKeysForbidden>>;

pub(super) enum Effect {
    Complete(Update),
    ComposeChange {
        id: UniqueId,
        priced: OutputWithoutScriptpubkey,
    },
    Sign {
        position: usize,
        outpoint: OutPoint,
        psbt: Psbt,
    },
    Publish(bitcoin::Transaction),
}

pub(super) struct Execution {
    pub(super) context: Arc<ExecutionContext>,
    psbt: psbt_v2::v2::Psbt,
    scripts: BTreeMap<Vec<u8>, ScriptBuf>,
    witnesses: BTreeMap<OutPoint, psbt::Input>,
    results: BTreeMap<Action, Update>,
    pub(super) retained: bool,
    pub(super) locks: BTreeMap<Action, Arc<Mutex<()>>>,
}

fn priced_prevout<'a>(snapshot: &'a Snapshot, coin: &Utxo) -> Result<&'a Coin, Error> {
    let outpoint = coin.outpoint();
    let prevout = snapshot
        .coins
        .get(&outpoint)
        .ok_or_else(|| error("missing prevout"))?;
    if Utxo::new(outpoint, prevout.txout.value, prevout.satisfaction_weight) != *coin {
        return Err(error("prevout differs from pricing snapshot"));
    }
    Ok(prevout)
}

impl Execution {
    pub(super) fn new(context: Arc<ExecutionContext>) -> Result<Self, Error> {
        // These transaction fields and derived-key ordering are PoC policy choices.
        let mut initial = psbt_v2::v2::Creator::new()
            .transaction_version(transaction::Version::TWO)
            .fallback_lock_time(absolute::LockTime::ZERO)
            .psbt();
        initial.global.tx_modifiable_flags = 3;
        for (subtype, value) in [
            (PSBT_GLOBAL_TX_UNORDERED_SUBTYPE, vec![3]),
            (PSBT_GLOBAL_SORT_KEY_DERIVATION_SUBTYPE, vec![1]),
            (PSBT_GLOBAL_TX_UUID_SUBTYPE, context.session_id.to_vec()),
        ] {
            initial.global.proprietaries.insert(
                psbt_v2::raw::ProprietaryKey {
                    prefix: concurrent_psbt::PROPRIETARY_PREFIX.to_vec(),
                    subtype,
                    key: vec![],
                },
                value,
            );
        }

        let mut inputs = Inputs::try_from_psbt(initial).map_err(error)?;
        for coin in context.candidate.inputs() {
            let prevout = priced_prevout(&context.snapshot, coin)?;
            if !prevout.txout.script_pubkey.is_p2tr() {
                return Err(error("PoC signer path only supports P2TR key spends"));
            }

            let mut input = Input::new(&coin.outpoint());
            input.set_witness_utxo(Some(prevout.txout.clone()));
            input.set_sequence(Some(Sequence::ENABLE_LOCKTIME_NO_RBF));
            inputs = inputs.input(input).map_err(error)?;
        }

        let mut outputs = inputs.no_more_inputs();
        for (&intent, output) in context.candidate.payments() {
            outputs = outputs
                .output(Output::new(
                    identity::payment(intent),
                    output.value,
                    output.script_pubkey.clone(),
                ))
                .map_err(error)?;
        }

        Ok(Self {
            context,
            psbt: outputs.into_inner().into_psbt(),
            scripts: BTreeMap::new(),
            witnesses: BTreeMap::new(),
            locks: BTreeMap::new(),
            results: BTreeMap::new(),
            retained: false,
        })
    }

    pub(super) fn prepare(&mut self, action: &Action) -> Result<Effect, Error> {
        if let Some(result) = self.results.get(action) {
            return Ok(Effect::Complete(result.clone()));
        }

        match action.clone() {
            Action::ComposeChange(id) => {
                let priced = self
                    .context
                    .candidate
                    .change()
                    .enumerate()
                    .find_map(|(index, priced)| {
                        (identity::change(index) == id).then(|| priced.clone())
                    })
                    .ok_or_else(|| error("unknown change"))?;

                if let Some(script) = self.scripts.get(id.as_bytes()).cloned() {
                    return self
                        .complete_change(id, &priced, script)
                        .map(Effect::Complete);
                }
                Ok(Effect::ComposeChange { id, priced })
            }
            Action::Sign(outpoint) => {
                if !self
                    .context
                    .candidate
                    .inputs()
                    .any(|coin| coin.outpoint() == outpoint)
                {
                    return Err(error("unknown input"));
                }

                if self.witnesses.contains_key(&outpoint) {
                    return self.signature_update(outpoint).map(Effect::Complete);
                }

                let (psbt, _, _) = self.ordered()?;
                let position = psbt
                    .unsigned_tx
                    .input
                    .iter()
                    .position(|input| input.previous_output == outpoint)
                    .expect("ordering verified the selected inputs");
                Ok(Effect::Sign {
                    position,
                    outpoint,
                    psbt,
                })
            }
            Action::Publish => {
                if !self.retained {
                    return Err(error("transaction has not been retained"));
                }
                Ok(Effect::Publish(self.payment()?.transaction))
            }
        }
    }

    pub(super) fn complete_change(
        &mut self,
        id: UniqueId,
        priced: &OutputWithoutScriptpubkey,
        script: ScriptBuf,
    ) -> Result<Update, Error> {
        // A retry must reuse the script bound to this change contribution.
        self.scripts.entry(id.as_bytes().to_vec()).or_insert(script);
        validate::change(&self.change_output(&id, priced), priced)?;

        let output = self.change_output(&id, priced);
        let constructor = self.outputs()?;
        self.psbt = constructor
            .output(Output::new(id.clone(), output.value, output.script_pubkey))
            .map_err(error)?
            .into_inner()
            .into_psbt();
        self.results
            .insert(Action::ComposeChange(id), Update::Applied);
        Ok(Update::Applied)
    }

    pub(super) fn complete_sign(
        &mut self,
        outpoint: OutPoint,
        position: usize,
        signed: psbt::Input,
    ) -> Result<Update, Error> {
        let (mut psbt, _, _) = self.ordered()?;
        if psbt.unsigned_tx.input[position].previous_output != outpoint {
            return Err(error("signed input position does not match its outpoint"));
        }
        signing::apply(&mut psbt, &self.context.snapshot, position, signed.clone())?;
        self.witnesses.insert(outpoint, signed);
        self.signature_update(outpoint)
    }

    pub(super) fn applied(&mut self, action: Action) -> Update {
        self.results.insert(action, Update::Applied);
        Update::Applied
    }

    pub(super) fn mark_retained(&mut self) {
        self.retained = true;
    }

    fn signature_update(&mut self, outpoint: OutPoint) -> Result<Update, Error> {
        let complete = self
            .context
            .candidate
            .inputs()
            .all(|coin| self.witnesses.contains_key(&coin.outpoint()));
        let result = if complete {
            Update::Complete(self.payment()?)
        } else {
            Update::Applied
        };
        self.results.insert(Action::Sign(outpoint), result.clone());
        Ok(result)
    }

    fn payment(&self) -> Result<Payment, Error> {
        let (mut psbt, outputs, fee) = self.ordered()?;
        for (outpoint, signed) in &self.witnesses {
            let position = psbt
                .unsigned_tx
                .input
                .iter()
                .position(|input| input.previous_output == *outpoint)
                .ok_or_else(|| error("witness does not belong to an ordered input"))?;
            signing::apply(&mut psbt, &self.context.snapshot, position, signed.clone())?;
        }

        let transaction = psbt.extract_tx().map_err(error)?;
        signing::validate_feerate(fee, transaction.weight(), self.context.feerate)?;
        Ok(Payment {
            transaction,
            outputs,
            fee,
        })
    }

    fn outputs(&self) -> Result<Outputs, Error> {
        Outputs::try_from_psbt(self.psbt.clone()).map_err(|_| error("outputs are not modifiable"))
    }

    fn ordered(&self) -> Result<(Psbt, BTreeMap<u64, usize>, Amount), Error> {
        if self
            .context
            .candidate
            .change()
            .enumerate()
            .any(|(index, _)| {
                !self
                    .results
                    .contains_key(&Action::ComposeChange(identity::change(index)))
            })
        {
            return Err(error("output contributions are incomplete"));
        }

        let ordered = self.outputs()?.no_more_outputs().into_ordered_psbt();
        let tx = psbt_v2::v2::Signer::new(ordered.clone())
            .map_err(error)?
            .unsigned_tx();
        let mut psbt = Psbt::from_unsigned_tx(tx).map_err(error)?;
        // This bridge only handles the metadata this executor constructs.
        // External/general PSBT conversion must preserve additional fields.
        for (to, from) in psbt.inputs.iter_mut().zip(&ordered.inputs) {
            to.witness_utxo = from.witness_utxo.clone();
        }

        let mut outputs = BTreeMap::new();
        for (index, output) in ordered.outputs.iter().enumerate() {
            let output = Output::try_from(output.clone()).map_err(error)?;
            if let Some(intent) = payment_id(output.unique_id().as_bytes())? {
                outputs.insert(intent, index);
            }
        }
        let fee = self.validate(&psbt)?;
        Ok((psbt, outputs, fee))
    }

    fn change_output(&self, id: &UniqueId, priced: &OutputWithoutScriptpubkey) -> TxOut {
        TxOut {
            value: priced.value,
            script_pubkey: self.scripts[id.as_bytes()].clone(),
        }
    }

    fn validate(&self, psbt: &Psbt) -> Result<Amount, Error> {
        let expected_outputs = self
            .context
            .candidate
            .payments()
            .map(|(_, output)| output.clone())
            .chain(
                self.context
                    .candidate
                    .change()
                    .enumerate()
                    .map(|(index, priced)| self.change_output(&identity::change(index), priced)),
            )
            .collect();
        let expected_inputs = self
            .context
            .candidate
            .inputs()
            .map(|coin| {
                let outpoint = coin.outpoint();
                (outpoint, &self.context.snapshot.coins[&outpoint].txout)
            })
            .collect();
        validate::transaction(psbt, expected_outputs, expected_inputs)?;
        validate::accounting(
            self.context.candidate.inputs().map(|coin| coin.value()),
            psbt.unsigned_tx.output.iter().map(|output| output.value),
            self.context
                .candidate
                .payments()
                .map(|(_, output)| output.value),
            self.context.candidate.objective_cost(),
        )
    }
}

fn payment_id(bytes: &[u8]) -> Result<Option<u64>, Error> {
    let Ok([tag, id @ ..]) = <[u8; 9]>::try_from(bytes) else {
        return Err(error("unexpected contribution identity"));
    };
    Ok((tag == 0).then(|| u64::from_be_bytes(id)))
}

#[cfg(test)]
mod tests {
    use super::payment_id;

    #[test]
    fn output_identity_distinguishes_intent_and_change() {
        assert_eq!(payment_id(&[0, 0, 0, 0, 0, 0, 0, 1, 2]).unwrap(), Some(258));
        assert_eq!(payment_id(&[1, 0, 0, 0, 0, 0, 0, 1, 2]).unwrap(), None);
        assert_eq!(
            payment_id(&[0; 8]).unwrap_err().0,
            "unexpected contribution identity"
        );
    }
}

#[cfg(test)]
mod prevout_tests {
    use super::*;
    use crate::{strategy::ExecutionId, wallet::dummy};
    use bitcoin::{Amount, FeeRate, OutPoint, ScriptBuf, TxOut, Weight};
    use cost_function::{
        intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve},
        queue::InMemoryQueue,
        wallet::WalletStateSnapshot,
    };
    use rand::{SeedableRng, rngs::StdRng};

    fn execution(payment: Amount) -> Execution {
        let outpoint = OutPoint {
            vout: 0,
            ..OutPoint::null()
        };
        let txout = TxOut {
            value: Amount::from_sat(50_000),
            script_pubkey: ScriptBuf::from_bytes([vec![0x51, 0x20], vec![0; 32]].concat()),
        };
        let weight = Weight::from_wu(70);
        let coin = Coin {
            txout: txout.clone(),
            satisfaction_weight: weight,
        };
        let snapshot = Snapshot {
            coins: BTreeMap::from([(outpoint, coin)]),
            ..Default::default()
        };
        let intent = Intent::new(
            Outcome::OutputCreation(FixedPaymentInstructions {
                script_pubkey: txout.script_pubkey.clone(),
                amount: payment,
            }),
            PayoffCurve::new(0, []).unwrap(),
        );
        let feerate = FeeRate::ZERO;
        let change = dummy::Change::new(txout.script_pubkey, weight, []);
        let candidate = WalletStateSnapshot::new(snapshot.clone(), InMemoryQueue::new([intent]))
            .funded_candidate(feerate, &mut StdRng::seed_from_u64(0), &change)
            .unwrap()
            .unwrap();
        Execution::new(Arc::new(ExecutionContext {
            execution_id: ExecutionId {
                wallet: [1; 16],
                sequence: 0,
            },
            candidate,
            snapshot,
            feerate,
            session_id: [1; 16],
        }))
        .unwrap()
    }

    fn compose_change(execution: &mut Execution) {
        let id = identity::change(0);
        let priced = execution.context.candidate.change().next().unwrap().clone();
        let script = execution
            .context
            .snapshot
            .coins
            .values()
            .next()
            .unwrap()
            .txout
            .script_pubkey
            .clone();
        execution.complete_change(id, &priced, script).unwrap();
    }

    #[test]
    fn ordering_requires_all_change_contributions() {
        let execution = execution(Amount::from_sat(40_000));
        assert_eq!(execution.context.candidate.change().count(), 1);
        assert_eq!(
            execution.ordered().unwrap_err().0,
            "output contributions are incomplete"
        );
    }

    #[test]
    fn ordering_rejects_an_altered_prevout() {
        let mut execution = execution(Amount::from_sat(40_000));
        execution.psbt.inputs[0]
            .witness_utxo
            .as_mut()
            .unwrap()
            .value += Amount::ONE_SAT;
        compose_change(&mut execution);

        assert_eq!(execution.ordered().unwrap_err().0, "unexpected input data");
    }

    #[test]
    fn signing_rejects_a_position_for_another_outpoint() {
        let mut execution = execution(Amount::from_sat(40_000));
        compose_change(&mut execution);
        let another_outpoint = OutPoint {
            vout: 1,
            ..OutPoint::null()
        };

        assert_eq!(
            execution
                .complete_sign(another_outpoint, 0, psbt::Input::default())
                .unwrap_err()
                .0,
            "signed input position does not match its outpoint"
        );
    }

    #[test]
    fn assembly_rejects_a_witness_for_another_outpoint() {
        let mut execution = execution(Amount::from_sat(40_000));
        compose_change(&mut execution);
        execution.witnesses.insert(
            OutPoint {
                vout: 1,
                ..OutPoint::null()
            },
            psbt::Input::default(),
        );

        assert_eq!(
            execution.payment().unwrap_err().0,
            "witness does not belong to an ordered input"
        );
    }

    #[test]
    fn ordering_requires_modifiable_outputs() {
        let mut execution = execution(Amount::from_sat(40_000));
        compose_change(&mut execution);
        execution.psbt.global.tx_modifiable_flags &= !2;

        assert_eq!(
            execution.ordered().unwrap_err().0,
            "outputs are not modifiable"
        );
    }

    #[test]
    fn the_prevout_must_match_the_candidate_input_price() {
        let outpoint = OutPoint::null();
        let value = Amount::from_sat(50_000);
        let weight = Weight::from_wu(70);
        let coin = Utxo::new(outpoint, value, weight);
        let prevout = Coin {
            txout: TxOut {
                value,
                script_pubkey: ScriptBuf::new(),
            },
            satisfaction_weight: weight,
        };
        let mut snapshot = Snapshot::default();

        assert_eq!(
            priced_prevout(&snapshot, &coin).unwrap_err().0,
            "missing prevout"
        );
        snapshot.coins.insert(outpoint, prevout.clone());
        assert_eq!(priced_prevout(&snapshot, &coin).unwrap(), &prevout);

        for coin in [
            Utxo::new(outpoint, Amount::from_sat(49_999), weight),
            Utxo::new(outpoint, value, Weight::from_wu(71)),
        ] {
            assert_eq!(
                priced_prevout(&snapshot, &coin).unwrap_err().0,
                "prevout differs from pricing snapshot"
            );
        }
    }
}

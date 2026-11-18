//! Construction state and effect receipts for one transaction.

use super::{Action, Error, Payment, Plan, Update, error, signing, validate};
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

enum Construction {
    Active(psbt_v2::v2::Psbt),
    Frozen(Frozen),
}

struct Frozen {
    psbt: Psbt,
    outputs: BTreeMap<u64, usize>,
    fee: Amount,
}

impl Construction {
    fn frozen(&mut self) -> Result<&mut Frozen, Error> {
        match self {
            Self::Frozen(frozen) => Ok(frozen),
            Self::Active(_) => Err(error("transaction has not been frozen and validated")),
        }
    }
}

pub(super) enum Effect {
    Complete(Update),
    AllocateChange {
        index: usize,
        priced: OutputWithoutScriptpubkey,
    },
    Sign {
        index: usize,
        position: usize,
        outpoint: OutPoint,
        psbt: Psbt,
    },
    Record(bitcoin::Transaction),
    Publish(bitcoin::Transaction),
}

pub(super) struct Execution {
    pub(super) plan: Arc<Plan>,
    construction: Construction,
    scripts: BTreeMap<usize, ScriptBuf>,
    results: BTreeMap<Action, Update>,
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
    pub(super) fn new(plan: Arc<Plan>) -> Result<Self, Error> {
        // These transaction fields and derived-key ordering are PoC policy choices.
        let mut initial = psbt_v2::v2::Creator::new()
            .transaction_version(transaction::Version::TWO)
            .fallback_lock_time(absolute::LockTime::ZERO)
            .psbt();
        initial.global.tx_modifiable_flags = 3;
        for (subtype, value) in [
            (PSBT_GLOBAL_TX_UNORDERED_SUBTYPE, vec![3]),
            (PSBT_GLOBAL_SORT_KEY_DERIVATION_SUBTYPE, vec![1]),
            (PSBT_GLOBAL_TX_UUID_SUBTYPE, plan.session_id.to_vec()),
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
        Inputs::try_from_psbt(initial.clone()).map_err(error)?;
        Ok(Self {
            plan,
            construction: Construction::Active(initial),
            scripts: BTreeMap::new(),
            locks: BTreeMap::new(),
            results: BTreeMap::new(),
        })
    }

    pub(super) fn prepare(&mut self, action: &Action) -> Result<Effect, Error> {
        if let Some(result) = self.results.get(action) {
            return Ok(Effect::Complete(result.clone()));
        }
        match *action {
            Action::Start => {}
            Action::Input(index) => {
                let coin = self
                    .plan
                    .candidate
                    .inputs()
                    .nth(index)
                    .ok_or_else(|| error("unknown input"))?;
                let prevout = priced_prevout(&self.plan.snapshot, coin)?;
                if !prevout.txout.script_pubkey.is_p2tr() {
                    return Err(error("PoC signer path only supports P2TR key spends"));
                }
                let mut input = Input::new(&coin.outpoint());
                input.set_witness_utxo(Some(prevout.txout.clone()));
                input.set_sequence(Some(Sequence::ENABLE_RBF_NO_LOCKTIME));
                self.construction = Construction::Active(
                    Inputs::try_from_psbt(self.active()?.clone())
                        .map_err(error)?
                        .input(input)
                        .map_err(error)?
                        .into_inner()
                        .into_psbt(),
                );
            }
            Action::CloseInputs => {
                if self
                    .plan
                    .candidate
                    .inputs()
                    .enumerate()
                    .any(|(i, _)| !self.results.contains_key(&Action::Input(i)))
                {
                    return Err(error("input contributions are incomplete"));
                }
                self.construction = Construction::Active(
                    Inputs::try_from_psbt(self.active()?.clone())
                        .map_err(error)?
                        .no_more_inputs()
                        .into_inner()
                        .into_psbt(),
                );
            }
            Action::AllocateChange(index) => {
                self.outputs()?;
                let priced = self
                    .plan
                    .candidate
                    .change()
                    .nth(index)
                    .ok_or_else(|| error("unknown change"))?
                    .clone();
                if let Some(script) = self.scripts.get(&index) {
                    return self
                        .complete_change(index, &priced, script.clone())
                        .map(Effect::Complete);
                }
                return Ok(Effect::AllocateChange { index, priced });
            }
            Action::Output(index) => {
                let constructor = self.outputs()?;
                let (id, output) = self.output(index)?;
                self.construction = Construction::Active(
                    constructor
                        .output(Output::new(id, output.value, output.script_pubkey))
                        .map_err(error)?
                        .into_inner()
                        .into_psbt(),
                );
            }
            Action::CloseOutputs => {
                let count =
                    self.plan.candidate.payments().count() + self.plan.candidate.change().count();
                if (0..count).any(|i| !self.results.contains_key(&Action::Output(i))) {
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
                self.construction = Construction::Frozen(Frozen { psbt, outputs, fee });
            }
            Action::Sign(index) => {
                let outpoint = self
                    .plan
                    .candidate
                    .inputs()
                    .nth(index)
                    .ok_or_else(|| error("unknown input"))?
                    .outpoint();
                let psbt = &self.construction.frozen()?.psbt;
                let position = psbt
                    .unsigned_tx
                    .input
                    .iter()
                    .position(|input| input.previous_output == outpoint)
                    .expect("freezing verified the selected inputs");
                return Ok(Effect::Sign {
                    index,
                    position,
                    outpoint,
                    psbt: psbt.clone(),
                });
            }
            Action::Finalize => {
                if self
                    .plan
                    .candidate
                    .inputs()
                    .enumerate()
                    .any(|(i, _)| !self.results.contains_key(&Action::Sign(i)))
                {
                    return Err(error("signatures are incomplete"));
                }
                let Frozen { psbt, outputs, fee } = self.construction.frozen()?;
                let transaction = psbt.clone().extract_tx().map_err(error)?;
                signing::validate_feerate(*fee, transaction.weight(), self.plan.feerate)?;
                let result = Update::Finalized(Payment {
                    transaction,
                    outputs: outputs.clone(),
                    fee: *fee,
                });
                self.results.insert(Action::Finalize, result.clone());
                return Ok(Effect::Complete(result));
            }
            Action::Record => {
                return Ok(Effect::Record(self.finalized_transaction()?.clone()));
            }
            Action::Publish => {
                if !self.results.contains_key(&Action::Record) {
                    return Err(error("transaction has not been retained"));
                }
                return Ok(Effect::Publish(self.finalized_transaction()?.clone()));
            }
        }
        Ok(Effect::Complete(self.applied(action.clone())))
    }

    pub(super) fn complete_change(
        &mut self,
        index: usize,
        priced: &OutputWithoutScriptpubkey,
        script: ScriptBuf,
    ) -> Result<Update, Error> {
        // Retain the allocation even if validation fails.
        self.scripts.entry(index).or_insert(script);
        validate::change(&self.change_output(index, priced), priced)?;
        self.results
            .insert(Action::AllocateChange(index), Update::Applied);
        Ok(Update::Applied)
    }

    pub(super) fn complete_sign(
        &mut self,
        index: usize,
        position: usize,
        signed: psbt::Input,
    ) -> Result<Update, Error> {
        let snapshot = &self.plan.snapshot;
        let psbt = &mut self.construction.frozen()?.psbt;
        signing::apply(psbt, snapshot, position, signed)?;
        self.results.insert(Action::Sign(index), Update::Applied);
        Ok(Update::Applied)
    }

    pub(super) fn applied(&mut self, action: Action) -> Update {
        self.results.insert(action, Update::Applied);
        Update::Applied
    }

    fn finalized_transaction(&self) -> Result<&bitcoin::Transaction, Error> {
        match self.results.get(&Action::Finalize) {
            Some(Update::Finalized(payment)) => Ok(&payment.transaction),
            _ => Err(error("transaction is not finalized")),
        }
    }

    fn active(&self) -> Result<&psbt_v2::v2::Psbt, Error> {
        match &self.construction {
            Construction::Active(psbt) => Ok(psbt),
            Construction::Frozen(_) => Err(error("transaction is frozen")),
        }
    }

    fn outputs(&self) -> Result<Outputs, Error> {
        Outputs::try_from_psbt(self.active()?.clone())
            .map_err(|_| error("outputs are not modifiable"))
    }

    fn output(&self, index: usize) -> Result<(UniqueId, TxOut), Error> {
        let payments = self.plan.candidate.payments().count();
        let (tag, id, output) =
            if let Some((&id, output)) = self.plan.candidate.payments().nth(index) {
                (0, id, output.clone())
            } else {
                let change = index - payments;
                if !self.results.contains_key(&Action::AllocateChange(change)) {
                    return Err(error("change allocation has not succeeded"));
                }
                let priced = self
                    .plan
                    .candidate
                    .change()
                    .nth(change)
                    .expect("a successful allocation has priced change");
                (1, change as u64, self.change_output(change, priced))
            };
        let bytes = [vec![tag], id.to_be_bytes().to_vec()].concat();
        Ok((UniqueId::try_from(bytes).map_err(error)?, output))
    }

    fn change_output(&self, index: usize, priced: &OutputWithoutScriptpubkey) -> TxOut {
        TxOut {
            value: priced.value,
            script_pubkey: self.scripts[&index].clone(),
        }
    }

    fn validate(&self, psbt: &Psbt) -> Result<Amount, Error> {
        let expected_outputs = self
            .plan
            .candidate
            .payments()
            .map(|(_, output)| output.clone())
            .chain(
                self.plan
                    .candidate
                    .change()
                    .enumerate()
                    .map(|(index, priced)| self.change_output(index, priced)),
            )
            .collect();
        let expected_inputs = self
            .plan
            .candidate
            .inputs()
            .map(|coin| {
                let outpoint = coin.outpoint();
                (outpoint, &self.plan.snapshot.coins[&outpoint].txout)
            })
            .collect();
        validate::transaction(psbt, expected_outputs, expected_inputs)?;
        validate::accounting(
            self.plan.candidate.inputs().map(|coin| coin.value()),
            psbt.unsigned_tx.output.iter().map(|output| output.value),
            self.plan
                .candidate
                .payments()
                .map(|(_, output)| output.value),
            self.plan.candidate.objective_cost(),
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
    use bitcoin::{Amount, OutPoint, ScriptBuf, TxOut, Weight};

    #[test]
    fn closing_outputs_rejects_an_altered_prevout() {
        use crate::{strategy::ExecutionId, wallet::dummy};
        use bitcoin::FeeRate;
        use cost_function::{
            intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve},
            queue::InMemoryQueue,
            wallet::WalletStateSnapshot,
        };
        use rand::{SeedableRng, rngs::StdRng};

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
                amount: txout.value,
            }),
            PayoffCurve::new(0, []).unwrap(),
        );
        let feerate = FeeRate::ZERO;
        let change = dummy::Change::new(txout.script_pubkey, weight, []);
        let candidate = WalletStateSnapshot::new(snapshot.clone(), InMemoryQueue::new([intent]))
            .funded_candidate(feerate, &mut StdRng::seed_from_u64(0), &change)
            .unwrap()
            .unwrap();
        let mut execution = Execution::new(Arc::new(Plan {
            execution_id: ExecutionId {
                wallet: [1; 16],
                sequence: 0,
            },
            candidate,
            snapshot,
            feerate,
            session_id: [1; 16],
        }))
        .unwrap();
        for action in [Action::Input(0), Action::CloseInputs, Action::Output(0)] {
            execution.prepare(&action).unwrap();
        }
        let mut psbt = execution.active().unwrap().clone();
        psbt.inputs[0].witness_utxo.as_mut().unwrap().value += Amount::ONE_SAT;
        execution.construction = Construction::Active(psbt);

        let error = execution.prepare(&Action::CloseOutputs).err().unwrap();
        assert_eq!(error.0, "unexpected input data");
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

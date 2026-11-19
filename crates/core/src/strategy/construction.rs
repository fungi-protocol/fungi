//! Construction state and effect receipts for one transaction.

use super::{
    Action, Error, Payment, Plan, Update, error,
    session::{
        FeeContribution, InputContribution, Message, OutputContribution, Payload, Progress,
        WitnessContribution,
    },
    signing, validate,
};
use crate::wallet::{Coin, Snapshot};
use bitcoin::{
    Amount, FeeRate, OutPoint, Psbt, ScriptBuf, Sequence, TxOut, Weight, absolute, psbt,
    transaction,
};
use concurrent_psbt::{
    global::{
        PSBT_GLOBAL_SORT_KEY_DERIVATION_SUBTYPE, PSBT_GLOBAL_TX_UNORDERED_SUBTYPE,
        PSBT_GLOBAL_TX_UUID_SUBTYPE, SortSeed,
    },
    input::Input,
    output::{Output, UniqueId},
    roles::constructor::Constructor,
    sorter::{BothModifiable, ExplicitKeysForbidden, OutputsModifiable, Seeded},
};
use cost_function::{change::OutputWithoutScriptpubkey, wallet::Utxo};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

type Inputs = Constructor<BothModifiable, Seeded<ExplicitKeysForbidden>>;
type Outputs = Constructor<OutputsModifiable, Seeded<ExplicitKeysForbidden>>;

struct Frozen {
    psbt: Psbt,
    outputs: BTreeMap<u64, usize>,
    fee: Amount,
}

/// The IO an operation requires, fixed when it is dispatched.
pub(super) enum Effect {
    /// The action changes only the construction state.
    Local,
    Send(Message),
    AllocateChange,
    Sign {
        outpoint: OutPoint,
        psbt: Psbt,
    },
    Record(bitcoin::Transaction),
    Publish(bitcoin::Transaction),
}

/// Construction state for one transaction.
pub(super) struct Execution {
    plan: Arc<Plan>,
    /// The PSBT under construction. It does not change once the transaction is frozen.
    psbt: psbt_v2::v2::Psbt,
    frozen: Option<Frozen>,
    /// Each change script is kept once allocated, even if validation rejects it.
    scripts: BTreeMap<usize, ScriptBuf>,
    completed: BTreeSet<Action>,
    payment: Option<Payment>,
    progress: Progress,
    inputs_ready: Option<(usize, Message)>,
    outputs_ready: Option<(usize, Message)>,
}

fn priced_prevout<'a>(snapshot: &'a Snapshot, coin: &Utxo) -> Result<&'a Coin, Error> {
    let outpoint = coin.outpoint();
    let prevout = snapshot
        .coins
        .get(&outpoint)
        .ok_or_else(|| error("missing prevout"))?;
    if Utxo::new(outpoint, prevout.txout.value, prevout.satisfaction_weight)
        .with_liability(coin.liability())
        != *coin
    {
        return Err(error("prevout differs from pricing snapshot"));
    }
    Ok(prevout)
}

impl Execution {
    pub(super) fn new(plan: Arc<Plan>) -> Self {
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
        Inputs::try_from_psbt(initial.clone()).expect("the initial PSBT accepts inputs");
        Self {
            plan,
            psbt: initial,
            frozen: None,
            scripts: BTreeMap::new(),
            completed: BTreeSet::new(),
            payment: None,
            progress: Progress::initial(),
            inputs_ready: None,
            outputs_ready: None,
        }
    }

    pub(super) fn payment(&self) -> Option<&Payment> {
        self.payment.as_ref()
    }

    pub(super) fn progress(&self) -> &Progress {
        &self.progress
    }

    pub(super) fn set_session_view(
        &mut self,
        progress: Progress,
        inputs_ready: Option<(usize, Message)>,
        outputs_ready: Option<(usize, Message)>,
    ) {
        self.progress = progress;
        self.inputs_ready = inputs_ready;
        self.outputs_ready = outputs_ready;
    }

    pub(super) fn pending_inputs_ready(&self) -> Option<usize> {
        self.inputs_ready.as_ref().map(|(count, _)| *count)
    }

    pub(super) fn pending_outputs_ready(&self) -> Option<usize> {
        self.outputs_ready.as_ref().map(|(count, _)| *count)
    }

    pub(super) fn all_inputs_signed(&self) -> bool {
        self.frozen.as_ref().is_some_and(|frozen| {
            frozen
                .psbt
                .inputs
                .iter()
                .all(|input| input.final_script_witness.is_some())
        })
    }

    pub(super) fn frozen_txid(&self) -> Option<bitcoin::Txid> {
        self.frozen
            .as_ref()
            .map(|frozen| frozen.psbt.unsigned_tx.compute_txid())
    }

    pub(super) fn apply_witness(&mut self, contrib: &WitnessContribution) -> Result<bool, Error> {
        let Some(frozen) = self.frozen.as_mut() else {
            return Ok(false);
        };
        let Some(position) = frozen
            .psbt
            .unsigned_tx
            .input
            .iter()
            .position(|input| input.previous_output == contrib.outpoint)
        else {
            return Ok(false);
        };
        if frozen.psbt.inputs[position].final_script_witness.as_ref() == Some(&contrib.witness) {
            return Ok(false);
        }
        signing::apply(
            &mut frozen.psbt,
            position,
            psbt::Input {
                final_script_witness: Some(contrib.witness.clone()),
                ..Default::default()
            },
        )?;
        Ok(true)
    }

    /// Replacement fee liability incurred once this frozen transaction has been signed.
    pub(super) fn replacement_liability(&self) -> Option<Amount> {
        let frozen = self.frozen.as_ref()?;
        let witness_wu = 2 + 66 * frozen.psbt.inputs.len() as u64;
        let weight = frozen.psbt.unsigned_tx.weight() + Weight::from_wu(witness_wu);
        let bump = FeeRate::BROADCAST_MIN
            .fee_wu(weight)
            .expect("transaction weight fits");
        frozen.fee.checked_add(bump)
    }

    /// Return the IO the action requires. This does not change the construction state.
    pub(super) fn effect(&self, action: &Action) -> Effect {
        match *action {
            Action::SendInput(index) => {
                let config = self.session_config();
                let coin = self.input(index);
                let outpoint = coin.outpoint();
                Effect::Send(Message {
                    session_id: config.id,
                    sender: config.side,
                    payload: Payload::Input(InputContribution {
                        outpoint,
                        witness_utxo: self.plan.snapshot.coins[&outpoint].txout.clone(),
                        sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
                    }),
                })
            }
            Action::SendInputsReady(count) => {
                let (expected, message) = self
                    .inputs_ready
                    .as_ref()
                    .expect("the path dispatches the current input readiness");
                assert_eq!(*expected, count);
                Effect::Send(message.clone())
            }
            Action::SendOutput(index) => {
                let config = self.session_config();
                let (id, tx_out) = self.output(index);
                Effect::Send(Message {
                    session_id: config.id,
                    sender: config.side,
                    payload: Payload::Output(OutputContribution { id, tx_out }),
                })
            }
            Action::SendFee => {
                let config = self.session_config();
                Effect::Send(Message {
                    session_id: config.id,
                    sender: config.side,
                    payload: Payload::Fee(FeeContribution {
                        id: self.plan.fee_id.clone(),
                        amount: self.plan.local_fee(),
                    }),
                })
            }
            Action::SendOutputsReady(count) => {
                let (expected, message) = self
                    .outputs_ready
                    .as_ref()
                    .expect("the path dispatches the current output readiness");
                assert_eq!(*expected, count);
                Effect::Send(message.clone())
            }
            Action::SendWitness(index) => {
                let config = self.session_config();
                Effect::Send(Message {
                    session_id: config.id,
                    sender: config.side,
                    payload: Payload::Witness(self.local_witness(index)),
                })
            }
            Action::AllocateChange(_) => Effect::AllocateChange,
            Action::Sign(index) => Effect::Sign {
                outpoint: self.input(index).outpoint(),
                psbt: self.frozen().psbt.clone(),
            },
            Action::Record => Effect::Record(self.finalized_transaction().clone()),
            Action::Publish => {
                assert!(
                    self.completed.contains(&Action::Record),
                    "the path publishes only a recorded transaction"
                );
                Effect::Publish(self.finalized_transaction().clone())
            }
            _ => Effect::Local,
        }
    }

    /// Apply the result of the action's IO. Only this changes the construction state.
    pub(super) fn complete(&mut self, action: &Action, update: Update) -> Result<(), Error> {
        match (action, update) {
            (&Action::AllocateChange(index), Update::Change(script)) => {
                self.allocate_change(index, script)?;
            }
            (&Action::Sign(index), Update::Signed(signed)) => self.sign(index, *signed)?,
            (action, Update::Applied) => self.construct(action)?,
            _ => return Err(error("unexpected effect result")),
        }
        self.completed.insert(action.clone());
        Ok(())
    }

    /// Apply an action whose IO returned no value.
    fn construct(&mut self, action: &Action) -> Result<(), Error> {
        match *action {
            Action::Start
            | Action::Record
            | Action::Publish
            | Action::SendInput(_)
            | Action::SendInputsReady(_)
            | Action::SendOutput(_)
            | Action::SendFee
            | Action::SendOutputsReady(_)
            | Action::SendWitness(_) => {}
            Action::AllocateChange(_) | Action::Sign(_) => {
                return Err(error("unexpected effect result"));
            }
            Action::Input(index) => {
                let coin = self.input(index);
                let prevout = priced_prevout(&self.plan.snapshot, coin)?;
                if !prevout.txout.script_pubkey.is_p2tr() {
                    return Err(error("PoC signer path only supports P2TR key spends"));
                }
                let mut input = Input::new(&coin.outpoint());
                input.set_witness_utxo(Some(prevout.txout.clone()));
                input.set_sequence(Some(Sequence::ENABLE_RBF_NO_LOCKTIME));
                self.psbt = Inputs::try_from_psbt(self.active().clone())
                    .map_err(error)?
                    .input(input)
                    .map_err(error)?
                    .into_inner()
                    .into_psbt();
            }
            Action::CloseInputs => {
                assert!(
                    (0..self.plan.candidate.inputs().count())
                        .all(|i| self.completed.contains(&Action::Input(i))),
                    "the path closes inputs after every input contribution"
                );
                let mut constructor =
                    Inputs::try_from_psbt(self.active().clone()).map_err(error)?;
                if self.plan.session.is_some() {
                    let agreed = self
                        .progress
                        .inputs()
                        .expect("the path closes inputs after session inputs finalize");
                    for contrib in agreed {
                        if !contrib.witness_utxo.script_pubkey.is_p2tr() {
                            return Err(error("PoC signer path only supports P2TR key spends"));
                        }
                        let mut input = Input::new(&contrib.outpoint);
                        input.set_witness_utxo(Some(contrib.witness_utxo.clone()));
                        input.set_sequence(Some(contrib.sequence));
                        constructor = constructor.input(input).map_err(error)?;
                    }
                }
                self.psbt = constructor.no_more_inputs().into_inner().into_psbt();
            }
            Action::Output(index) => {
                let constructor = self.outputs();
                let (id, output) = self.output(index);
                self.psbt = constructor
                    .output(Output::new(id, output.value, output.script_pubkey))
                    .map_err(error)?
                    .into_inner()
                    .into_psbt();
            }
            Action::CloseOutputs => {
                let count =
                    self.plan.candidate.payments().count() + self.plan.candidate.change().count();
                assert!(
                    (0..count).all(|i| self.completed.contains(&Action::Output(i))),
                    "the path closes outputs after every output contribution"
                );
                let mut constructor = self.outputs();
                if self.plan.session.is_some() {
                    let (agreed, _, entropy) = self
                        .progress
                        .outputs()
                        .expect("the path closes outputs after session outputs finalize");
                    for contrib in agreed {
                        constructor = constructor
                            .output(Output::new(
                                contrib.id.clone(),
                                contrib.tx_out.value,
                                contrib.tx_out.script_pubkey.clone(),
                            ))
                            .map_err(error)?;
                    }
                    if let Some(seed) = entropy {
                        constructor = constructor
                            .try_set_seed(SortSeed::from(seed.to_vec()))
                            .map_err(error)?;
                    }
                }
                let ordered = constructor.no_more_outputs().into_ordered_psbt();
                let tx = psbt_v2::v2::Signer::new(ordered.clone())
                    .map_err(error)?
                    .unsigned_tx();
                let mut psbt = Psbt::from_unsigned_tx(tx).map_err(error)?;
                // This bridge only handles the metadata this wallet constructs.
                // External/general PSBT conversion must preserve additional fields.
                for (to, from) in psbt.inputs.iter_mut().zip(&ordered.inputs) {
                    to.witness_utxo = from.witness_utxo.clone();
                }
                let mut outputs = BTreeMap::new();
                for (index, output) in ordered.outputs.iter().enumerate() {
                    let output = Output::try_from(output.clone()).map_err(error)?;
                    if let Some(&intent) =
                        self.plan.payment_outputs.get(output.unique_id().as_bytes())
                    {
                        outputs.insert(intent, index);
                    }
                }
                let fee = self.validate(&psbt)?;
                self.frozen = Some(Frozen { psbt, outputs, fee });
                if let Some(signatures) = self.progress.signatures().cloned() {
                    for contrib in &signatures {
                        self.apply_witness(contrib)?;
                    }
                }
            }
            Action::Finalize => {
                assert!(
                    (0..self.plan.candidate.inputs().count())
                        .all(|i| self.completed.contains(&Action::Sign(i)))
                        && self.all_inputs_signed(),
                    "the path finalizes after every signature"
                );
                let Frozen { psbt, outputs, fee } = self.frozen();
                let transaction = psbt.clone().extract_tx().map_err(error)?;
                signing::validate_feerate(*fee, transaction.weight(), self.plan.feerate)?;
                self.payment = Some(Payment {
                    transaction,
                    outputs: outputs.clone(),
                    fee: *fee,
                });
            }
        }
        Ok(())
    }

    fn allocate_change(&mut self, index: usize, script: ScriptBuf) -> Result<(), Error> {
        let priced = self
            .plan
            .candidate
            .change()
            .nth(index)
            .expect("the path allocates priced change")
            .clone();
        self.scripts.entry(index).or_insert(script);
        validate::change(&self.change_output(index, &priced), &priced)
    }

    fn sign(&mut self, index: usize, signed: psbt::Input) -> Result<(), Error> {
        let outpoint = self.input(index).outpoint();
        let frozen = self
            .frozen
            .as_mut()
            .expect("the path signs a frozen transaction");
        let position = frozen
            .psbt
            .unsigned_tx
            .input
            .iter()
            .position(|input| input.previous_output == outpoint)
            .expect("freezing verified the selected inputs");
        signing::apply(&mut frozen.psbt, position, signed)
    }

    fn local_witness(&self, index: usize) -> WitnessContribution {
        let outpoint = self.input(index).outpoint();
        let frozen = self.frozen();
        let position = frozen
            .psbt
            .unsigned_tx
            .input
            .iter()
            .position(|input| input.previous_output == outpoint)
            .expect("freezing verified the selected inputs");
        let witness = frozen.psbt.inputs[position]
            .final_script_witness
            .clone()
            .expect("the path sends a witness after signing");
        WitnessContribution { outpoint, witness }
    }

    fn session_config(&self) -> super::session::Config {
        self.plan
            .session
            .expect("session action requires a session plan")
    }

    fn input(&self, index: usize) -> &Utxo {
        let mut inputs = self.plan.candidate.inputs();
        inputs.nth(index).expect("the path uses candidate inputs")
    }

    fn frozen(&self) -> &Frozen {
        let frozen = self.frozen.as_ref();
        frozen.expect("the path uses a frozen transaction")
    }

    fn finalized_transaction(&self) -> &bitcoin::Transaction {
        let payment = self.payment.as_ref();
        &payment
            .expect("the path uses a finalized transaction")
            .transaction
    }

    fn active(&self) -> &psbt_v2::v2::Psbt {
        assert!(self.frozen.is_none(), "the path constructs before freezing");
        &self.psbt
    }

    fn outputs(&self) -> Outputs {
        let psbt = self.active().clone();
        Outputs::try_from_psbt(psbt).expect("the path adds outputs after closing inputs")
    }

    fn output(&self, index: usize) -> (UniqueId, TxOut) {
        let payments = self.plan.candidate.payments().count();
        let output = if let Some((_, output)) = self.plan.candidate.payments().nth(index) {
            output.clone()
        } else {
            let change = index - payments;
            assert!(
                self.completed.contains(&Action::AllocateChange(change)),
                "the path adds change after its allocation"
            );
            let priced = self
                .plan
                .candidate
                .change()
                .nth(change)
                .expect("a successful allocation has priced change");
            self.change_output(change, priced)
        };
        (self.plan.output_ids[index].clone(), output)
    }

    fn change_output(&self, index: usize, priced: &OutputWithoutScriptpubkey) -> TxOut {
        TxOut {
            value: priced.value,
            script_pubkey: self.scripts[&index].clone(),
        }
    }

    fn validate(&self, psbt: &Psbt) -> Result<Amount, Error> {
        let local_outputs: Vec<_> = (0..self.plan.output_ids.len())
            .map(|index| self.output(index))
            .collect();
        let (expected_outputs, expected_inputs, explicit_fees) = if self.plan.session.is_some() {
            let agreed_inputs = self
                .progress
                .inputs()
                .expect("session validation has agreed inputs");
            let (agreed_outputs, agreed_fees, _) = self
                .progress
                .outputs()
                .expect("session validation has agreed outputs");
            if !agreed_fees.contains(&FeeContribution {
                id: self.plan.fee_id.clone(),
                amount: self.plan.local_fee(),
            }) {
                return Err(error("agreed fees omit or alter the local fee"));
            }
            let outputs = agreed_outputs.iter().map(|c| c.tx_out.clone()).collect();
            let inputs = agreed_inputs
                .iter()
                .map(|c| (c.outpoint, &c.witness_utxo))
                .collect();
            let fees = agreed_fees.iter().map(|f| f.amount).collect::<Vec<_>>();
            (outputs, inputs, Some(fees))
        } else {
            let outputs = local_outputs.into_iter().map(|(_, out)| out).collect();
            let inputs = self
                .plan
                .candidate
                .inputs()
                .map(|coin| {
                    let outpoint = coin.outpoint();
                    (outpoint, &self.plan.snapshot.coins[&outpoint].txout)
                })
                .collect();
            (outputs, inputs, None)
        };
        validate::transaction(psbt, expected_outputs, expected_inputs)?;
        validate::accounting(
            psbt.inputs.iter().map(|input| {
                input
                    .witness_utxo
                    .as_ref()
                    .expect("validated input has witness utxo")
                    .value
            }),
            psbt.unsigned_tx.output.iter().map(|output| output.value),
            explicit_fees.as_deref(),
            self.plan.candidate.inputs().map(|coin| coin.value()),
            self.plan.candidate.change().map(|change| change.value),
            self.plan
                .candidate
                .payments()
                .map(|(_, output)| output.value),
            self.plan.candidate.objective_cost(),
        )
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
        let mut rng = StdRng::seed_from_u64(0);
        let candidate = WalletStateSnapshot::new(snapshot.clone(), InMemoryQueue::new([intent]))
            .funded_candidate(feerate, &mut rng, &change)
            .unwrap()
            .unwrap();
        let mut execution = Execution::new(Arc::new(Plan::new(
            ExecutionId {
                wallet: [1; 16],
                sequence: 0,
            },
            candidate,
            snapshot,
            feerate,
            None,
            [1; 16],
            &mut rng,
        )));
        for action in [Action::Input(0), Action::CloseInputs, Action::Output(0)] {
            execution.complete(&action, Update::Applied).unwrap();
        }
        // Output contribution identity is an opaque 16-byte random token, stable across reads.
        assert_eq!(execution.output(0).0.as_bytes().len(), 16);
        assert_eq!(execution.output(0).0, execution.plan.output_ids[0]);
        assert_eq!(execution.replacement_liability(), None);

        execution.psbt.inputs[0]
            .witness_utxo
            .as_mut()
            .unwrap()
            .value += Amount::ONE_SAT;

        let error = execution
            .complete(&Action::CloseOutputs, Update::Applied)
            .unwrap_err();
        assert_eq!(error.0, "unexpected input data");
    }

    #[test]
    fn the_prevout_must_match_the_candidate_input_price() {
        let outpoint = OutPoint::null();
        let value = Amount::from_sat(50_000);
        let weight = Weight::from_wu(70);
        let coin = Utxo::new(outpoint, value, weight).with_liability(Amount::from_sat(500));
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

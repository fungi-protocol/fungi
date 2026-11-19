//! Two-party session messages, phase tracking, and shared progress views.

use super::{Error, error};
use bitcoin::{Amount, OutPoint, Sequence, TxOut, Witness};
use concurrent_psbt::output::UniqueId;
use fungi_transport::{SendChannel, Unspecified};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
};

/// Which endpoint of a two-party session a wallet occupies.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Side {
    First,
    Second,
}

impl Side {
    fn slot(self) -> usize {
        match self {
            Self::First => 0,
            Self::Second => 1,
        }
    }
}

/// Parameters for entering one collaborative construction session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    pub id: [u8; 16],
    pub side: Side,
    participants: NonZeroUsize,
    pub additional_entropy: Option<[u8; 32]>,
}

impl Config {
    pub fn two_party(id: [u8; 16], side: Side) -> Self {
        Self {
            id,
            side,
            participants: NonZeroUsize::new(2).expect("two-party session has 2 participants"),
            additional_entropy: None,
        }
    }

    pub fn with_additional_entropy(mut self, entropy: [u8; 32]) -> Self {
        self.additional_entropy = Some(entropy);
        self
    }

    pub fn participants(&self) -> NonZeroUsize {
        self.participants
    }
}

/// Ordered phases of a collaborative construction session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Inputs,
    Outputs,
    Signatures,
}

/// Canonical shared input contribution keyed by [`OutPoint`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct InputContribution {
    pub outpoint: OutPoint,
    pub witness_utxo: TxOut,
    pub sequence: Sequence,
}

/// Canonical shared output contribution keyed by [`UniqueId`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutputContribution {
    pub id: UniqueId,
    pub tx_out: TxOut,
}

impl Ord for OutputContribution {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.id.as_bytes(), &self.tx_out).cmp(&(other.id.as_bytes(), &other.tx_out))
    }
}

impl PartialOrd for OutputContribution {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Canonical shared explicit fee contribution keyed by [`UniqueId`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FeeContribution {
    pub id: UniqueId,
    pub amount: Amount,
}

impl Ord for FeeContribution {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.id.as_bytes(), self.amount).cmp(&(other.id.as_bytes(), other.amount))
    }
}

impl PartialOrd for FeeContribution {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Canonical shared witness contribution keyed by input [`OutPoint`].
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct WitnessContribution {
    pub outpoint: OutPoint,
    pub witness: Witness,
}

/// Exact finalized contribution set for one closed phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Finalized {
    Inputs(BTreeSet<InputContribution>),
    Outputs {
        outputs: BTreeSet<OutputContribution>,
        fees: BTreeSet<FeeContribution>,
        additional_entropy: Option<[u8; 32]>,
    },
    Signatures(BTreeSet<WitnessContribution>),
}

/// Synchronized session view: closed phases in phase order and the currently open phase.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Progress {
    pub finalized: Vec<Finalized>,
    pub open: Option<Phase>,
}

/// Agreed output-phase view borrowed from [`Progress`].
pub type FinalizedOutputs<'a> = (
    &'a BTreeSet<OutputContribution>,
    &'a BTreeSet<FeeContribution>,
    Option<[u8; 32]>,
);

type OutputsReadySet = (BTreeSet<Vec<u8>>, BTreeSet<Vec<u8>>);

impl Progress {
    pub fn initial() -> Self {
        Self {
            finalized: Vec::new(),
            open: Some(Phase::Inputs),
        }
    }

    /// Check that `self` preserves every closed phase already present in `earlier`.
    pub fn extends(&self, earlier: &Self) -> bool {
        self.finalized.len() >= earlier.finalized.len()
            && self.finalized[..earlier.finalized.len()] == earlier.finalized[..]
    }

    pub fn inputs(&self) -> Option<&BTreeSet<InputContribution>> {
        match self.finalized.first() {
            Some(Finalized::Inputs(inputs)) => Some(inputs),
            _ => None,
        }
    }

    pub fn outputs(&self) -> Option<FinalizedOutputs<'_>> {
        match self.finalized.get(1) {
            Some(Finalized::Outputs {
                outputs,
                fees,
                additional_entropy,
            }) => Some((outputs, fees, *additional_entropy)),
            _ => None,
        }
    }

    pub fn signatures(&self) -> Option<&BTreeSet<WitnessContribution>> {
        match self.finalized.get(2) {
            Some(Finalized::Signatures(signatures)) => Some(signatures),
            _ => None,
        }
    }
}

/// Flooded message payload for a two-party construction session.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Payload {
    Input(InputContribution),
    InputsReady(BTreeSet<OutPoint>),
    Output(OutputContribution),
    Fee(FeeContribution),
    OutputsReady {
        outputs: BTreeSet<Vec<u8>>,
        fees: BTreeSet<Vec<u8>>,
    },
    Witness(WitnessContribution),
}

/// One message sent across the peer transport channel.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    pub session_id: [u8; 16],
    pub sender: Side,
    pub payload: Payload,
}

/// Append-only two-party session tracker computing synchronized [`Progress`].
#[derive(Clone, Debug)]
pub struct Tracker {
    id: [u8; 16],
    inputs: BTreeMap<OutPoint, InputContribution>,
    inputs_ready: [Option<BTreeSet<OutPoint>>; 2],
    closed_inputs: Option<BTreeSet<InputContribution>>,
    outputs: BTreeMap<Vec<u8>, OutputContribution>,
    fees: BTreeMap<Vec<u8>, FeeContribution>,
    outputs_ready: [Option<OutputsReadySet>; 2],
    closed_outputs: Option<(BTreeSet<OutputContribution>, BTreeSet<FeeContribution>)>,
    additional_entropy: Option<[u8; 32]>,
    witnesses: BTreeMap<OutPoint, WitnessContribution>,
    closed_signatures: Option<BTreeSet<WitnessContribution>>,
}

impl Tracker {
    pub fn new(id: [u8; 16]) -> Self {
        Self {
            id,
            inputs: BTreeMap::new(),
            inputs_ready: [None, None],
            closed_inputs: None,
            outputs: BTreeMap::new(),
            fees: BTreeMap::new(),
            outputs_ready: [None, None],
            closed_outputs: None,
            additional_entropy: None,
            witnesses: BTreeMap::new(),
            closed_signatures: None,
        }
    }

    pub(super) fn set_additional_entropy(&mut self, entropy: Option<[u8; 32]>) {
        self.additional_entropy = entropy;
    }

    /// Record an observed or sent session message. Late contributions for already-closed
    /// phases are ignored so finalized contribution sets remain immutable.
    pub fn record(&mut self, message: &Message) -> Result<(), Error> {
        if message.session_id != self.id {
            return Err(error("unexpected session identity"));
        }
        match &message.payload {
            Payload::Input(input) if self.closed_inputs.is_none() => {
                insert_idempotent(
                    &mut self.inputs,
                    input.outpoint,
                    input,
                    "conflicting input contribution",
                )?;
            }
            Payload::InputsReady(ready) if self.closed_inputs.is_none() => {
                self.inputs_ready[message.sender.slot()] = Some(ready.clone());
            }
            Payload::Output(output) if self.closed_outputs.is_none() => {
                insert_idempotent(
                    &mut self.outputs,
                    output.id.as_bytes().to_vec(),
                    output,
                    "conflicting output contribution",
                )?;
            }
            Payload::Fee(fee) if self.closed_outputs.is_none() => {
                insert_idempotent(
                    &mut self.fees,
                    fee.id.as_bytes().to_vec(),
                    fee,
                    "conflicting fee contribution",
                )?;
            }
            Payload::OutputsReady { outputs, fees } if self.closed_outputs.is_none() => {
                self.outputs_ready[message.sender.slot()] = Some((outputs.clone(), fees.clone()));
            }
            Payload::Witness(witness) if self.closed_signatures.is_none() => {
                insert_idempotent(
                    &mut self.witnesses,
                    witness.outpoint,
                    witness,
                    "conflicting witness contribution",
                )?;
            }
            _ => return Ok(()),
        }
        self.advance();
        Ok(())
    }

    pub fn progress(&self) -> Progress {
        let mut finalized = Vec::new();
        let Some(inputs) = &self.closed_inputs else {
            return Progress {
                finalized,
                open: Some(Phase::Inputs),
            };
        };
        finalized.push(Finalized::Inputs(inputs.clone()));
        let Some((outputs, fees)) = &self.closed_outputs else {
            return Progress {
                finalized,
                open: Some(Phase::Outputs),
            };
        };
        finalized.push(Finalized::Outputs {
            outputs: outputs.clone(),
            fees: fees.clone(),
            additional_entropy: self.additional_entropy,
        });
        let Some(signatures) = &self.closed_signatures else {
            return Progress {
                finalized,
                open: Some(Phase::Signatures),
            };
        };
        finalized.push(Finalized::Signatures(signatures.clone()));
        Progress {
            finalized,
            open: None,
        }
    }

    pub(super) fn witnesses(&self) -> impl Iterator<Item = &WitnessContribution> {
        self.witnesses.values()
    }

    pub(super) fn ready_inputs_message(&self, side: Side) -> Option<(usize, Message)> {
        if self.closed_inputs.is_some() || self.inputs.is_empty() {
            return None;
        }
        let keys: BTreeSet<_> = self.inputs.keys().copied().collect();
        if self.inputs_ready[side.slot()].as_ref() == Some(&keys) {
            return None;
        }
        Some((
            keys.len(),
            Message {
                session_id: self.id,
                sender: side,
                payload: Payload::InputsReady(keys),
            },
        ))
    }

    pub(super) fn ready_outputs_message(&self, side: Side) -> Option<(usize, Message)> {
        if self.closed_inputs.is_none() || self.closed_outputs.is_some() || self.outputs.is_empty()
        {
            return None;
        }
        let outputs: BTreeSet<_> = self.outputs.keys().cloned().collect();
        let fees: BTreeSet<_> = self.fees.keys().cloned().collect();
        let count = outputs.len() + fees.len();
        if self.outputs_ready[side.slot()]
            .as_ref()
            .is_some_and(|(o, f)| o == &outputs && f == &fees)
        {
            return None;
        }
        Some((
            count,
            Message {
                session_id: self.id,
                sender: side,
                payload: Payload::OutputsReady { outputs, fees },
            },
        ))
    }

    fn advance(&mut self) {
        if self.closed_inputs.is_none() && !self.inputs.is_empty() {
            let keys: BTreeSet<_> = self.inputs.keys().copied().collect();
            if self.inputs_ready[0].as_ref() == Some(&keys)
                && self.inputs_ready[1].as_ref() == Some(&keys)
            {
                self.closed_inputs = Some(self.inputs.values().cloned().collect());
            }
        }
        if self.closed_inputs.is_some() && self.closed_outputs.is_none() && !self.outputs.is_empty()
        {
            let pair = (
                self.outputs.keys().cloned().collect::<BTreeSet<_>>(),
                self.fees.keys().cloned().collect::<BTreeSet<_>>(),
            );
            if self.outputs_ready[0].as_ref() == Some(&pair)
                && self.outputs_ready[1].as_ref() == Some(&pair)
            {
                self.closed_outputs = Some((
                    self.outputs.values().cloned().collect(),
                    self.fees.values().cloned().collect(),
                ));
            }
        }
        if let Some(inputs) = &self.closed_inputs
            && self.closed_outputs.is_some()
            && self.closed_signatures.is_none()
        {
            let expected: BTreeSet<_> = inputs.iter().map(|input| input.outpoint).collect();
            if expected
                .iter()
                .all(|outpoint| self.witnesses.contains_key(outpoint))
            {
                self.closed_signatures = Some(
                    expected
                        .into_iter()
                        .map(|outpoint| self.witnesses[&outpoint].clone())
                        .collect(),
                );
            }
        }
    }
}

fn insert_idempotent<K: Ord, V: Clone + Eq>(
    map: &mut BTreeMap<K, V>,
    key: K,
    value: &V,
    conflict: &'static str,
) -> Result<(), Error> {
    match map.get(&key) {
        Some(existing) if existing != value => Err(error(conflict)),
        Some(_) => Ok(()),
        None => {
            map.insert(key, value.clone());
            Ok(())
        }
    }
}

/// Default peer sender for unilateral executors without a connected session transport.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoPeer;

/// Error returned when a session send is attempted without a connected peer transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("no peer transport configured")]
pub struct NoPeerError;

impl SendChannel<Message> for NoPeer {
    type Privacy = Unspecified;
    type SendError = NoPeerError;

    async fn send(&mut self, _message: Message) -> Result<(), Self::SendError> {
        Err(NoPeerError)
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use bitcoin::{ScriptBuf, Txid, hashes::Hash};

    fn sample_input(vout: u32, sats: u64) -> InputContribution {
        InputContribution {
            outpoint: OutPoint::new(Txid::all_zeros(), vout),
            witness_utxo: TxOut {
                value: Amount::from_sat(sats),
                script_pubkey: ScriptBuf::new(),
            },
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME,
        }
    }

    fn uid(byte: u8) -> UniqueId {
        UniqueId::try_from(vec![byte; 16]).unwrap()
    }

    #[tokio::test]
    async fn no_peer_reports_missing_transport() {
        let mut peer = NoPeer;
        let msg = Message {
            session_id: [0; 16],
            sender: Side::First,
            payload: Payload::InputsReady(BTreeSet::new()),
        };
        assert_eq!(peer.send(msg).await, Err(NoPeerError));
    }

    #[test]
    fn tracker_synchronizes_phases_idempotently_and_ignores_late_messages() {
        let id = [7u8; 16];
        let cfg = Config::two_party(id, Side::First);
        assert_eq!(cfg.participants().get(), 2);

        let mut tracker = Tracker::new(id);
        assert_eq!(tracker.progress(), Progress::initial());
        assert!(tracker.ready_inputs_message(Side::First).is_none());
        assert!(tracker.ready_outputs_message(Side::First).is_none());

        let wrong = Message {
            session_id: [9; 16],
            sender: Side::First,
            payload: Payload::InputsReady(BTreeSet::new()),
        };
        assert_eq!(
            tracker.record(&wrong).unwrap_err().0,
            "unexpected session identity"
        );

        let in_a = sample_input(0, 10_000);
        let in_b = sample_input(1, 20_000);
        let out_a = OutputContribution {
            id: uid(1),
            tx_out: TxOut {
                value: Amount::from_sat(9_900),
                script_pubkey: ScriptBuf::new(),
            },
        };
        let fee_a = FeeContribution {
            id: uid(2),
            amount: Amount::from_sat(100),
        };
        let wit_a = WitnessContribution {
            outpoint: in_a.outpoint,
            witness: Witness::from_slice(&[[1u8; 64]]),
        };
        let wit_b = WitnessContribution {
            outpoint: in_b.outpoint,
            witness: Witness::from_slice(&[[2u8; 64]]),
        };

        // Early output, fee, and witness arrive before Inputs closes; buffered without closing Outputs.
        for payload in [
            Payload::Output(out_a.clone()),
            Payload::Output(out_a.clone()),
            Payload::Fee(fee_a.clone()),
            Payload::Fee(fee_a.clone()),
            Payload::Witness(wit_a.clone()),
            Payload::Witness(wit_a.clone()),
        ] {
            tracker
                .record(&Message {
                    session_id: id,
                    sender: Side::Second,
                    payload,
                })
                .unwrap();
        }
        assert_eq!(tracker.progress().open, Some(Phase::Inputs));

        // Inputs + conflicting duplicate check.
        for (sender, input) in [(Side::First, &in_a), (Side::Second, &in_b)] {
            let msg = Message {
                session_id: id,
                sender,
                payload: Payload::Input(input.clone()),
            };
            tracker.record(&msg).unwrap();
            tracker.record(&msg).unwrap();
        }
        assert_eq!(
            tracker
                .record(&Message {
                    session_id: id,
                    sender: Side::First,
                    payload: Payload::Input(sample_input(0, 10_001)),
                })
                .unwrap_err()
                .0,
            "conflicting input contribution"
        );
        assert_eq!(
            tracker
                .record(&Message {
                    session_id: id,
                    sender: Side::First,
                    payload: Payload::Output(OutputContribution {
                        id: uid(1),
                        tx_out: TxOut {
                            value: Amount::from_sat(9_901),
                            script_pubkey: ScriptBuf::new(),
                        },
                    }),
                })
                .unwrap_err()
                .0,
            "conflicting output contribution"
        );
        assert_eq!(
            tracker
                .record(&Message {
                    session_id: id,
                    sender: Side::First,
                    payload: Payload::Fee(FeeContribution {
                        id: uid(2),
                        amount: Amount::from_sat(101),
                    }),
                })
                .unwrap_err()
                .0,
            "conflicting fee contribution"
        );
        assert_eq!(
            tracker
                .record(&Message {
                    session_id: id,
                    sender: Side::First,
                    payload: Payload::Witness(WitnessContribution {
                        outpoint: in_a.outpoint,
                        witness: Witness::from_slice(&[[9u8; 64]]),
                    }),
                })
                .unwrap_err()
                .0,
            "conflicting witness contribution"
        );

        let (count, ready_first) = tracker.ready_inputs_message(Side::First).unwrap();
        assert_eq!(count, 2);
        tracker.record(&ready_first).unwrap();
        assert!(tracker.ready_inputs_message(Side::First).is_none());
        let (_, ready_second) = tracker.ready_inputs_message(Side::Second).unwrap();
        tracker.record(&ready_second).unwrap();
        assert!(tracker.ready_inputs_message(Side::Second).is_none());

        let after_inputs = tracker.progress();
        assert_eq!(after_inputs.open, Some(Phase::Outputs));
        assert_eq!(
            after_inputs.inputs(),
            Some(&BTreeSet::from([in_a.clone(), in_b.clone()]))
        );
        assert!(after_inputs.outputs().is_none());
        assert!(after_inputs.signatures().is_none());
        assert!(after_inputs.extends(&Progress::initial()));
        assert!(!Progress::initial().extends(&after_inputs));

        // Late input after Inputs finalized is ignored.
        tracker
            .record(&Message {
                session_id: id,
                sender: Side::First,
                payload: Payload::Input(sample_input(99, 50_000)),
            })
            .unwrap();
        assert_eq!(tracker.progress(), after_inputs);

        // Finalize Outputs.
        let (out_count, out_ready_1) = tracker.ready_outputs_message(Side::First).unwrap();
        assert_eq!(out_count, 2);
        tracker.record(&out_ready_1).unwrap();
        assert!(tracker.ready_outputs_message(Side::First).is_none());
        let (_, out_ready_2) = tracker.ready_outputs_message(Side::Second).unwrap();
        tracker.record(&out_ready_2).unwrap();
        assert!(tracker.ready_outputs_message(Side::Second).is_none());

        let after_outputs = tracker.progress();
        assert_eq!(after_outputs.open, Some(Phase::Signatures));
        assert!(after_outputs.extends(&after_inputs));
        assert_eq!(
            after_outputs.outputs(),
            Some((&BTreeSet::from([out_a]), &BTreeSet::from([fee_a]), None))
        );

        // Late output/fee after Outputs finalized is ignored.
        tracker
            .record(&Message {
                session_id: id,
                sender: Side::Second,
                payload: Payload::Output(OutputContribution {
                    id: uid(99),
                    tx_out: TxOut {
                        value: Amount::from_sat(500),
                        script_pubkey: ScriptBuf::new(),
                    },
                }),
            })
            .unwrap();
        assert_eq!(tracker.progress(), after_outputs);

        // Finalize Signatures with second witness, then ignore late witness.
        tracker
            .record(&Message {
                session_id: id,
                sender: Side::Second,
                payload: Payload::Witness(wit_b.clone()),
            })
            .unwrap();
        let done = tracker.progress();
        assert_eq!(done.open, None);
        assert_eq!(done.signatures(), Some(&BTreeSet::from([wit_a, wit_b])));
        assert_eq!(tracker.witnesses().count(), 2);

        tracker
            .record(&Message {
                session_id: id,
                sender: Side::Second,
                payload: Payload::Witness(WitnessContribution {
                    outpoint: OutPoint::new(Txid::all_zeros(), 77),
                    witness: Witness::new(),
                }),
            })
            .unwrap();
        assert_eq!(tracker.progress(), done);
    }
}

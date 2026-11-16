//! Scripted wallet operations for exercising the driver without keys or a node.
//!
//! Signing results are supplied by the test and need not contain valid signatures.
//! These components do not validate transactions or connect to a broadcaster.

use std::collections::VecDeque;

use bitcoin::{Amount, OutPoint, Psbt, ScriptBuf, Transaction, TxOut, Weight, psbt};
use cost_function::change::ChangeCosts;

use super::{ChangeSource, InputSigner, Snapshot, SnapshotSource, TransactionRecorder};

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("no scripted reply remains")]
    Exhausted,
    #[error("{0}")]
    Scripted(&'static str),
    #[error("PSBT does not spend {0}")]
    MissingInput(OutPoint),
    #[error("PSBT spends {0} more than once")]
    RepeatedInput(OutPoint),
}

struct Replies<T>(VecDeque<Result<T, Error>>);

impl<T> Replies<T> {
    fn new(replies: impl IntoIterator<Item = Result<T, Error>>) -> Self {
        Self(replies.into_iter().collect())
    }

    fn next(&mut self) -> Result<T, Error> {
        self.0.pop_front().unwrap_or(Err(Error::Exhausted))
    }
}

pub struct Snapshots {
    replies: Replies<Snapshot>,
    requests: usize,
}

impl Snapshots {
    pub fn new(replies: impl IntoIterator<Item = Result<Snapshot, Error>>) -> Self {
        Self {
            replies: Replies::new(replies),
            requests: 0,
        }
    }

    pub fn requests(&self) -> usize {
        self.requests
    }
}

impl SnapshotSource for Snapshots {
    type Error = Error;

    fn snapshot(&mut self) -> impl Future<Output = Result<Snapshot, Error>> + Send + use<> {
        self.requests += 1;
        let result = self.replies.next();
        std::future::ready(result)
    }
}

pub struct Change {
    template: ScriptBuf,
    satisfaction_weight: Weight,
    replies: Replies<ScriptBuf>,
    requests: usize,
}

impl Change {
    /// Price outputs like `template`, and allocate scripts from `replies`.
    pub fn new(
        template: ScriptBuf,
        satisfaction_weight: Weight,
        replies: impl IntoIterator<Item = Result<ScriptBuf, Error>>,
    ) -> Self {
        Self {
            template,
            satisfaction_weight,
            replies: Replies::new(replies),
            requests: 0,
        }
    }

    pub fn requests(&self) -> usize {
        self.requests
    }
}

impl ChangeCosts for Change {
    fn output_weight(&self) -> Weight {
        TxOut {
            value: Amount::ZERO,
            script_pubkey: self.template.clone(),
        }
        .weight()
    }

    fn satisfaction_weight(&self) -> Weight {
        self.satisfaction_weight
    }

    fn dust(&self) -> Amount {
        self.template.minimal_non_dust()
    }
}

impl ChangeSource for Change {
    type Error = Error;

    fn next_script(&mut self) -> impl Future<Output = Result<ScriptBuf, Error>> + Send + use<> {
        self.requests += 1;
        let result = self.replies.next();
        std::future::ready(result)
    }
}

pub struct Signer {
    outpoint: OutPoint,
    replies: Replies<psbt::Input>,
    requests: Vec<Psbt>,
}

impl Signer {
    pub fn new(
        outpoint: OutPoint,
        replies: impl IntoIterator<Item = Result<psbt::Input, Error>>,
    ) -> Self {
        Self {
            outpoint,
            replies: Replies::new(replies),
            requests: Vec::new(),
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
        let result = match psbt
            .unsigned_tx
            .input
            .iter()
            .filter(|input| input.previous_output == self.outpoint)
            .count()
        {
            0 => Err(Error::MissingInput(self.outpoint)),
            1 => self.replies.next(),
            _ => Err(Error::RepeatedInput(self.outpoint)),
        };
        std::future::ready(result)
    }
}

pub struct Recorder {
    replies: Replies<()>,
    requests: Vec<Transaction>,
    recorded: Vec<Transaction>,
}

impl Recorder {
    pub fn new(replies: impl IntoIterator<Item = Result<(), Error>>) -> Self {
        Self {
            replies: Replies::new(replies),
            requests: Vec::new(),
            recorded: Vec::new(),
        }
    }

    pub fn requests(&self) -> &[Transaction] {
        &self.requests
    }

    pub fn recorded(&self) -> &[Transaction] {
        &self.recorded
    }
}

impl TransactionRecorder for Recorder {
    type Error = Error;

    fn record(
        &mut self,
        tx: &Transaction,
    ) -> impl Future<Output = Result<(), Error>> + Send + use<> {
        self.requests.push(tx.clone());
        let result = self.replies.next();
        if result.is_ok() && !self.recorded.contains(tx) {
            self.recorded.push(tx.clone());
        }
        std::future::ready(result)
    }
}

//! What the wallet owns, and what it has been asked to do.

use std::collections::HashMap;
use std::hash::Hash;

use bitcoin::{Amount, OutPoint, Weight};

use crate::intent::Intent;

/// A coin the wallet can spend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Utxo {
    pub(crate) outpoint: OutPoint,
    pub(crate) value: Amount,
    pub(crate) satisfaction_weight: Weight,
}

impl Utxo {
    /// `satisfaction_weight` is the weight of the scriptSig and witness that spend this
    /// coin, excluding the outpoint and nsequence.
    pub fn new(outpoint: OutPoint, value: Amount, satisfaction_weight: Weight) -> Self {
        Utxo {
            outpoint,
            value,
            satisfaction_weight,
        }
    }
}

/// The coins a wallet holds, as it reports them.
pub trait UtxoSnapshot {
    /// Every coin, in no particular order.
    fn iter(&self) -> impl Iterator<Item = Utxo>;
}

/// What the wallet owns and what it has been asked to do, at one moment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalletStateSnapshot<Id: Eq + Hash> {
    utxos: Vec<Utxo>,
    intents: HashMap<Id, Intent>,
}

impl<Id: Eq + Hash> WalletStateSnapshot<Id> {
    /// The coins the wallet holds, together with the intents it has yet to satisfy, each
    /// under its queue id.
    ///
    /// Both are copied in, so later changes to the wallet or the queue do not reach the
    /// snapshot.
    pub fn new(
        utxos: impl IntoIterator<Item = Utxo>,
        intents: impl IntoIterator<Item = (Id, Intent)>,
    ) -> Self {
        WalletStateSnapshot {
            utxos: utxos.into_iter().collect(),
            intents: intents.into_iter().collect(),
        }
    }

    /// The coins available to spend.
    pub(crate) fn utxos(&self) -> &[Utxo] {
        &self.utxos
    }

    /// The intents the wallet has yet to satisfy, each with its queue id.
    pub(crate) fn intents(&self) -> impl Iterator<Item = (&Id, &Intent)> {
        self.intents.iter()
    }

    /// The intent with queue id `id`, if the wallet still owes it.
    pub(crate) fn get_intent(&self, id: &Id) -> Option<&Intent> {
        self.intents.get(id)
    }
}

#[cfg(test)]
mod tests {
    use crate::intent::{Action, FixedPaymentInstructions, PayoffCurve};

    use super::*;
    use bitcoin::{ScriptBuf, Txid, hashes::Hash as _};
    use std::time::Instant;

    fn utxo(vout: u32, sats: u64) -> Utxo {
        Utxo::new(
            OutPoint::new(Txid::all_zeros(), vout),
            Amount::from_sat(sats),
            Weight::ZERO,
        )
    }

    fn intent(start: Instant, sats: u64) -> Intent {
        Intent::new(
            Action::OutputCreation(FixedPaymentInstructions {
                script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                amount: Amount::from_sat(sats),
            }),
            PayoffCurve::new(start, []),
        )
    }

    #[test]
    fn a_snapshot_keeps_the_coins_and_intents_it_was_given() {
        let start = Instant::now();

        let state = WalletStateSnapshot::new(
            [utxo(0, 10_000), utxo(1, 20_000)],
            [(7, intent(start, 1_000))],
        );

        assert_eq!(state.utxos, [utxo(0, 10_000), utxo(1, 20_000)]);
        assert_eq!(state.intents, HashMap::from([(7, intent(start, 1_000))]));
    }
}

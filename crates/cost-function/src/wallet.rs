//! What the wallet owns, and what it has been asked to do.

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
pub struct WalletStateSnapshot {
    utxos: Vec<Utxo>,
    intents: Vec<Intent>,
}

impl WalletStateSnapshot {
    /// The coins the wallet holds, together with the intents it has yet to satisfy.
    pub fn new(
        utxos: impl IntoIterator<Item = Utxo>,
        intents: impl IntoIterator<Item = Intent>,
    ) -> Self {
        WalletStateSnapshot {
            utxos: utxos.into_iter().collect(),
            intents: intents.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::intent::{Action, FixedPaymentInstructions, PayoffCurve};

    use super::*;
    use bitcoin::{ScriptBuf, Txid, hashes::Hash};
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

        let state =
            WalletStateSnapshot::new([utxo(0, 10_000), utxo(1, 20_000)], [intent(start, 1_000)]);

        assert_eq!(state.utxos, [utxo(0, 10_000), utxo(1, 20_000)]);
        assert_eq!(state.intents, [intent(start, 1_000)]);
    }
}

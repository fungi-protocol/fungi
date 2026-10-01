//! What the wallet owns, and what it has been asked to do.

use bitcoin::{OutPoint, TxOut};

use crate::intent::Intent;

/// A coin the wallet can spend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Utxo {
    pub outpoint: OutPoint,
    pub prev_out: TxOut,
}

/// The coins a wallet holds, as it reports them.
pub trait UtxoSnapshot {
    /// Every coin, in no particular order.
    fn iter(&self) -> impl Iterator<Item = Utxo>;
}

/// What the wallet owns and what it has been asked to do, at one moment.
///
/// Time and feerate are left out: those are observations of the outside world, supplied
/// alongside rather than baked in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalletStateSnapshot {
    utxos: Vec<Utxo>,
    intents: Vec<Intent>,
}

impl WalletStateSnapshot {
    /// The coins the wallet holds, together with the intents it has yet to satisfy.
    ///
    /// A wallet passes its [`UtxoSnapshot::iter`], and the intents from
    /// [`crate::queue::Queue::iter`].
    pub fn new(
        utxos: impl IntoIterator<Item = Utxo>,
        intents: impl IntoIterator<Item = Intent>,
    ) -> Self {
        WalletStateSnapshot {
            utxos: utxos.into_iter().collect(),
            intents: intents.into_iter().collect(),
        }
    }

    /// The coins available to spend.
    pub fn utxos(&self) -> &[Utxo] {
        &self.utxos
    }

    /// The intents the wallet has yet to satisfy.
    ///
    /// A position in this slice names an intent only within this snapshot.
    pub fn intents(&self) -> &[Intent] {
        &self.intents
    }
}

#[cfg(test)]
mod tests {
    use crate::intent::{Action, FixedPaymentInstructions, PayoffCurve};
    use crate::queue::{InMemoryQueue, Queue};

    use super::*;
    use bitcoin::{Amount, ScriptBuf, Txid, hashes::Hash};
    use std::time::Instant;

    /// Stands in for a wallet reporting its coins.
    impl UtxoSnapshot for Vec<Utxo> {
        fn iter(&self) -> impl Iterator<Item = Utxo> {
            self.as_slice().iter().cloned()
        }
    }

    fn utxo(vout: u32, sats: u64) -> Utxo {
        Utxo {
            outpoint: OutPoint::new(Txid::all_zeros(), vout),
            prev_out: TxOut {
                value: Amount::from_sat(sats),
                script_pubkey: ScriptBuf::new(),
            },
        }
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

    /// What a wallet passes for the intents in `queue`.
    fn copied(queue: &InMemoryQueue) -> impl Iterator<Item = Intent> {
        queue.iter().map(|(_, intent)| intent.clone())
    }

    #[test]
    fn a_coin_is_named_by_the_outpoint_it_sits_at() {
        let coin = utxo(1, 50_000);

        assert_eq!(coin.outpoint.vout, 1);
        assert_eq!(coin.clone(), coin);
    }

    #[test]
    fn a_snapshot_keeps_the_coins_and_intents_it_was_given() {
        let start = Instant::now();
        let queue = InMemoryQueue::new([intent(start, 1_000)]);

        let state = WalletStateSnapshot::new([utxo(0, 10_000), utxo(1, 20_000)], copied(&queue));

        assert_eq!(state.utxos, [utxo(0, 10_000), utxo(1, 20_000)]);
        assert_eq!(state.intents, [intent(start, 1_000)]);
        assert_eq!(state.clone(), state);
    }

    #[test]
    fn a_snapshot_does_not_follow_the_wallet_afterwards() {
        let start = Instant::now();
        let mut held = vec![utxo(0, 10_000)];
        let mut queue = InMemoryQueue::new([intent(start, 1_000)]);
        let state = WalletStateSnapshot::new(UtxoSnapshot::iter(&held), copied(&queue));

        held.push(utxo(1, 20_000));
        queue.push(intent(start, 2_000));

        assert_eq!(state.utxos, [utxo(0, 10_000)]);
        assert_eq!(state.intents, [intent(start, 1_000)]);
    }
}

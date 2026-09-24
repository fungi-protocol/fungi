//! What the wallet owns, and what it has been asked to do.

use bitcoin::{OutPoint, TxOut};

use crate::queue::Queue;

/// A coin the wallet can spend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Utxo {
    pub outpoint: OutPoint,
    pub prev_out: TxOut,
}

/// What the wallet owns and what it has been asked to do.
///
/// Time and feerate are left out: those are observations of the outside world, supplied
/// alongside rather than baked in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalletState<Q: Queue> {
    utxos: Vec<Utxo>,
    queue: Q,
}

impl<Q: Queue> WalletState<Q> {
    /// The coins the wallet holds, together with the intents it has yet to satisfy.
    pub fn new(utxos: impl IntoIterator<Item = Utxo>, queue: Q) -> Self {
        WalletState {
            utxos: utxos.into_iter().collect(),
            queue,
        }
    }

    /// The coins available to spend.
    pub fn utxos(&self) -> &[Utxo] {
        &self.utxos
    }

    /// The intents the wallet has yet to satisfy.
    pub fn queue(&self) -> &Q {
        &self.queue
    }
}

#[cfg(test)]
mod tests {
    use crate::queue::InMemoryQueue;

    use super::*;
    use bitcoin::{Amount, ScriptBuf, Txid, hashes::Hash};

    fn utxo(vout: u32, sats: u64) -> Utxo {
        Utxo {
            outpoint: OutPoint::new(Txid::all_zeros(), vout),
            prev_out: TxOut {
                value: Amount::from_sat(sats),
                script_pubkey: ScriptBuf::new(),
            },
        }
    }

    #[test]
    fn a_coin_is_named_by_the_outpoint_it_sits_at() {
        let coin = utxo(1, 50_000);

        assert_eq!(coin.outpoint.vout, 1);
        assert_eq!(coin.clone(), coin);
    }

    #[test]
    fn a_wallet_state_keeps_the_coins_and_queue_it_was_given() {
        let state = WalletState::new([utxo(0, 10_000), utxo(1, 20_000)], InMemoryQueue::new([]));

        assert_eq!(state.utxos, [utxo(0, 10_000), utxo(1, 20_000)]);
        assert_eq!(state.queue, InMemoryQueue::new([]));
        assert_eq!(state.clone(), state);
    }
}

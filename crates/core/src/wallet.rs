//! What a wallet supplies to the protocol.

use cost_function::change::ScriptpubkeyAssigner;
use cost_function::wallet::UtxoSnapshot;

/// Where the wallet's change scriptpubkeys come from, and the coins it holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalletInterface<C: ScriptpubkeyAssigner, U: UtxoSnapshot> {
    scriptpubkey_assigner: C,
    utxo_snapshot: U,
    // TODO Broadcaster, signer, feerate estimator(?)
}

impl<C: ScriptpubkeyAssigner, U: UtxoSnapshot> WalletInterface<C, U> {
    pub fn new(scriptpubkey_assigner: C, utxo_snapshot: U) -> Self {
        Self {
            scriptpubkey_assigner,
            utxo_snapshot,
        }
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use bitcoin::{Amount, OutPoint, Weight};
    use cost_function::wallet::Utxo;

    struct DummySPKAssigner;

    impl ScriptpubkeyAssigner for DummySPKAssigner {
        fn output_weight(&self) -> Weight {
            Weight::ZERO
        }

        fn satisfaction_weight(&self) -> Weight {
            Weight::ZERO
        }

        fn dust(&self) -> Amount {
            Amount::from_sat(546)
        }
    }

    struct DummyUtxoSnapshot;

    impl UtxoSnapshot for DummyUtxoSnapshot {
        fn iter(&self) -> impl Iterator<Item = Utxo> {
            std::iter::once(Utxo::new(
                OutPoint::null(),
                Amount::from_sat(1_000),
                Weight::ZERO,
            ))
        }
    }

    #[test]
    fn a_wallet_interface_holds_its_assigner_and_its_coins() {
        let wallet = WalletInterface::new(DummySPKAssigner, DummyUtxoSnapshot);

        assert_eq!(wallet.scriptpubkey_assigner.dust(), Amount::from_sat(546));
        assert_eq!(wallet.utxo_snapshot.iter().count(), 1);
    }
}

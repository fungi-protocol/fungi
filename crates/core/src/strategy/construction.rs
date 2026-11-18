//! Construction state and effect receipts for one transaction.

use super::{Action, Error, Plan, Update, error};
use crate::wallet::{Coin, Snapshot};
use bitcoin::{Sequence, absolute, transaction};
use concurrent_psbt::{
    global::{
        PSBT_GLOBAL_SORT_KEY_DERIVATION_SUBTYPE, PSBT_GLOBAL_TX_UNORDERED_SUBTYPE,
        PSBT_GLOBAL_TX_UUID_SUBTYPE,
    },
    input::Input,
    roles::constructor::Constructor,
    sorter::{BothModifiable, ExplicitKeysForbidden, Seeded},
};
use cost_function::wallet::Utxo;
use std::{collections::BTreeMap, sync::Arc};

type Inputs = Constructor<BothModifiable, Seeded<ExplicitKeysForbidden>>;
pub(super) struct Execution {
    pub(super) plan: Arc<Plan>,
    construction: psbt_v2::v2::Psbt,
    results: BTreeMap<Action, Update>,
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
            construction: initial,
            results: BTreeMap::new(),
        })
    }

    pub(super) fn execute(&mut self, action: &Action) -> Result<Update, Error> {
        if let Some(result) = self.results.get(action) {
            return Ok(result.clone());
        }
        self.apply(action)?;
        let result = Update::Applied;
        self.results.insert(action.clone(), result.clone());
        Ok(result)
    }

    fn apply(&mut self, action: &Action) -> Result<(), Error> {
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
                self.construction = Inputs::try_from_psbt(self.construction.clone())
                    .map_err(error)?
                    .input(input)
                    .map_err(error)?
                    .into_inner()
                    .into_psbt();
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
                self.construction = Inputs::try_from_psbt(self.construction.clone())
                    .map_err(error)?
                    .no_more_inputs()
                    .into_inner()
                    .into_psbt();
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod prevout_tests {
    use super::*;
    use bitcoin::{Amount, OutPoint, ScriptBuf, TxOut, Weight};

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

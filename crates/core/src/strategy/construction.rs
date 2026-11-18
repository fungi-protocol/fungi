//! Construction state and effect receipts for one transaction.

use super::{Error, Plan, Update, error};
use bitcoin::{absolute, transaction};
use concurrent_psbt::{
    global::{
        PSBT_GLOBAL_SORT_KEY_DERIVATION_SUBTYPE, PSBT_GLOBAL_TX_UNORDERED_SUBTYPE,
        PSBT_GLOBAL_TX_UUID_SUBTYPE,
    },
    roles::constructor::Constructor,
    sorter::{BothModifiable, ExplicitKeysForbidden, Seeded},
};
use std::sync::Arc;

type Inputs = Constructor<BothModifiable, Seeded<ExplicitKeysForbidden>>;
pub(super) struct Execution {
    pub(super) plan: Arc<Plan>,
    construction: Option<psbt_v2::v2::Psbt>,
}

impl Execution {
    pub(super) fn new(plan: Arc<Plan>) -> Self {
        Self {
            plan,
            construction: None,
        }
    }

    pub(super) fn start(&mut self) -> Result<Update, Error> {
        if self.construction.is_some() {
            return Ok(Update::Applied);
        }
        // These transaction fields and derived-key ordering are PoC policy choices.
        let mut initial = psbt_v2::v2::Creator::new()
            .transaction_version(transaction::Version::TWO)
            .fallback_lock_time(absolute::LockTime::ZERO)
            .psbt();
        initial.global.tx_modifiable_flags = 3;
        for (subtype, value) in [
            (PSBT_GLOBAL_TX_UNORDERED_SUBTYPE, vec![3]),
            (PSBT_GLOBAL_SORT_KEY_DERIVATION_SUBTYPE, vec![1]),
            (PSBT_GLOBAL_TX_UUID_SUBTYPE, self.plan.session_id.to_vec()),
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
        self.construction = Some(initial);
        Ok(Update::Applied)
    }
}

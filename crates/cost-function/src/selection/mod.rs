//! Which coins fund a [`crate::batch::Batch`].

use bitcoin::{Amount, FeeRate, TxOut, Weight};
use rand::RngCore;

use crate::wallet::Utxo;

/// Version, locktime, segwit marker and flag, and one-byte input and output counts.
pub(crate) const TX_OVERHEAD: Weight = Weight::from_wu(42);

pub(crate) mod random;

pub(crate) struct FundingRequest<'a> {
    /// Coins the selector may draw on.
    pub(crate) available: &'a [Utxo],

    /// What the transaction has to pay. This should not include change coming back to the wallet.
    pub(crate) outputs: &'a [TxOut],

    /// Feerate the transaction pays, used to price spending and creating coins.
    pub(crate) feerate: FeeRate,
}

/// What a `CoinSelector` returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InputSelection {
    /// Coins the selector chose from [`FundingRequest::available`].
    pub(crate) selected_inputs: Vec<Utxo>,
    /// Amount left over after the fees for spending the inputs, creating the
    /// [`FundingRequest::outputs`] and the transaction overhead.
    pub(crate) surplus: Amount,
}

/// A strategy for selecting coins
pub(crate) trait CoinSelector {
    type Error;

    /// Draws on `rng` for any random choice, so the same seed makes the same selection.
    fn select<R: RngCore>(
        &self,
        request: FundingRequest<'_>,
        rng: &mut R,
    ) -> Result<InputSelection, Self::Error>;
}

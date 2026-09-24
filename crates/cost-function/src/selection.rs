//! Which coins fund a [`crate::batch::Batch`].

use bitcoin::{Amount, TxOut, Weight};

use crate::wallet::Utxo;

/// Everything a `CoinSelector` needs that isn't wallet state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionParams {
    /// Ceiling on the weight of the selected inputs.
    pub max_selection_weight: Weight,
}

pub(crate) struct FundingRequest<'a> {
    /// Coins the batch's intents already commit to spending.
    pub(crate) required: &'a [Utxo],

    /// Coins the selector may draw on. Disjoint from `required`.
    pub(crate) available: &'a [Utxo],

    /// What the transaction has to pay. This should not include change coming back to the wallet.
    pub(crate) outputs: &'a [TxOut],

    pub(crate) params: &'a SelectionParams,
}

/// What a `CoinSelector` returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InputSelection {
    /// Coins the selector added. Excludes [`FundingRequest::required`].
    pub(crate) selected_inputs: Vec<Utxo>,
    /// amount left over.
    pub(crate) surplus: Amount,
}

/// A strategy for closing a [`FundingRequest`]'s funding gap.
pub(crate) trait CoinSelector {
    type Error;

    fn select(&self, request: FundingRequest<'_>) -> Result<InputSelection, Self::Error>;
}

/// What a `ChangeSelector` returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ChangeSelection {
    pub(crate) selected_change: Vec<TxOut>,
}

/// Change selection strategy.
pub(crate) trait ChangeSelector {
    type Error;

    fn select(
        &self,
        request: InputSelection,
        dust_limit: Amount,
    ) -> Result<ChangeSelection, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{Amount, OutPoint, ScriptBuf, Txid, hashes::Hash};

    fn utxo(vout: u32, sats: u64) -> Utxo {
        Utxo {
            outpoint: OutPoint::new(Txid::all_zeros(), vout),
            prev_out: TxOut {
                value: Amount::from_sat(sats),
                script_pubkey: ScriptBuf::new(),
            },
        }
    }

    fn params() -> SelectionParams {
        SelectionParams {
            max_selection_weight: Weight::from_wu(400_000),
        }
    }

    /// Takes every coin available.
    struct TakeEverything;

    #[derive(Debug, PartialEq, Eq)]
    struct NothingOnOffer;

    impl CoinSelector for TakeEverything {
        type Error = NothingOnOffer;

        fn select(&self, request: FundingRequest<'_>) -> Result<InputSelection, NothingOnOffer> {
            if request.available.is_empty() {
                return Err(NothingOnOffer);
            }

            Ok(InputSelection {
                selected_inputs: request.available.to_vec(),
                surplus: request
                    .available
                    .iter()
                    .map(|r| r.prev_out.value)
                    .sum::<Amount>()
                    - request.outputs.iter().map(|o| o.value).sum::<Amount>(),
            })
        }
    }

    #[test]
    fn a_selector_answers_with_coins_from_those_on_offer() {
        let required = [utxo(0, 10_000)];
        let available = [utxo(1, 20_000), utxo(2, 30_000)];
        let params = params();

        let funding = TakeEverything
            .select(FundingRequest {
                required: &required,
                available: &available,
                outputs: &[],
                params: &params,
            })
            .unwrap();

        assert_eq!(funding.selected_inputs, available);
        assert!(!funding.selected_inputs.contains(&required[0]));
        assert_eq!(funding.surplus, Amount::from_sat(50_000));
    }
}

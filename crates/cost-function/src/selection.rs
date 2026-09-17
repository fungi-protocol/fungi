//! Which coins fund a [`crate::batch::Batch`].

use bitcoin::{Amount, FeeRate, ScriptBuf, TxOut, Weight};

use crate::wallet::Utxo;

/// Everything a `CoinSelector` needs that isn't wallet state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionParams {
    pub feerate: FeeRate,
    /// What we expect to pay to spend change later.
    pub long_term_feerate: FeeRate,
    /// Where change goes, if there is any.
    pub change_script: ScriptBuf,
    /// Change below this is not worth creating.
    pub dust_limit: Amount,
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
pub(crate) struct Funding {
    /// Coins the selector added. Excludes [`FundingRequest::required`].
    pub(crate) inputs: Vec<Utxo>,

    /// Change, when the surplus is worth an output.
    pub(crate) change: Option<TxOut>,
}

/// A strategy for closing a [`FundingRequest`]'s funding gap.
pub(crate) trait CoinSelector {
    type Error;

    fn select(&self, request: FundingRequest<'_>) -> Result<Funding, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{OutPoint, Txid, hashes::Hash};

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
            feerate: FeeRate::from_sat_per_vb(10).unwrap(),
            long_term_feerate: FeeRate::from_sat_per_vb(3).unwrap(),
            change_script: ScriptBuf::new(),
            dust_limit: Amount::from_sat(330),
            max_selection_weight: Weight::from_wu(400_000),
        }
    }

    /// Takes every coin on offer, or refuses when there are none. Enough to exercise the
    /// contract, not a strategy anyone should use.
    struct TakeEverything;

    #[derive(Debug, PartialEq, Eq)]
    struct NothingOnOffer;

    impl CoinSelector for TakeEverything {
        type Error = NothingOnOffer;

        fn select(&self, request: FundingRequest<'_>) -> Result<Funding, NothingOnOffer> {
            if request.available.is_empty() {
                return Err(NothingOnOffer);
            }

            Ok(Funding {
                inputs: request.available.to_vec(),
                change: None,
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

        assert_eq!(funding.inputs, available);
        assert!(!funding.inputs.contains(&required[0]));
        assert_eq!(funding.change, None);
        assert_eq!(funding.clone(), funding);
        assert_eq!(params.clone(), params);
    }

    #[test]
    fn a_selector_reports_failure_in_its_own_terms() {
        let params = params();

        let err = TakeEverything
            .select(FundingRequest {
                required: &[],
                available: &[],
                outputs: &[],
                params: &params,
            })
            .unwrap_err();

        assert_eq!(err, NothingOnOffer);
    }
}

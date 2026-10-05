//! Which coins fund a [`crate::batch::Batch`].

use bitcoin::{Amount, FeeRate, TxOut};

use crate::wallet::Utxo;

pub(crate) mod random;

/// Everything a `CoinSelector` needs that isn't wallet state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectionParams {
    /// Feerate the transaction pays, used to price spending and creating coins.
    pub feerate: FeeRate,
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
    /// Amount left over after the fees for spending the inputs and creating the
    /// [`FundingRequest::outputs`]. Excludes the transaction overhead.
    pub(crate) surplus: Amount,
}

/// A strategy for closing a [`FundingRequest`]'s funding gap.
pub(crate) trait CoinSelector {
    type Error;

    fn select(&self, request: FundingRequest<'_>) -> Result<InputSelection, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blockspace::KnowsWeight;
    use bitcoin::{Amount, OutPoint, ScriptBuf, SignedAmount, Txid, Weight, hashes::Hash};

    /// A coin spent by a taproot key path signature, 70 WU.
    fn utxo(vout: u32, sats: u64) -> Utxo {
        Utxo::new(
            OutPoint::new(Txid::all_zeros(), vout),
            Amount::from_sat(sats),
            Weight::from_wu(70),
        )
    }

    fn params() -> SelectionParams {
        SelectionParams {
            feerate: FeeRate::from_sat_per_vb(1).unwrap(),
        }
    }

    /// Takes every coin available. The surplus is after the fees for spending the coins and
    /// creating the outputs.
    struct TakeEverything;

    #[derive(Debug, PartialEq, Eq)]
    struct NothingOnOffer;

    impl CoinSelector for TakeEverything {
        type Error = NothingOnOffer;

        fn select(&self, request: FundingRequest<'_>) -> Result<InputSelection, NothingOnOffer> {
            if request.available.is_empty() {
                return Err(NothingOnOffer);
            }

            let feerate = request.params.feerate;
            let worth: SignedAmount = (request.required.iter().chain(request.available))
                .map(|coin| {
                    coin.value.to_signed().unwrap()
                        - coin.fee(feerate).unwrap().to_signed().unwrap()
                })
                .sum();
            let cost: SignedAmount = (request.outputs.iter())
                .map(|output| {
                    (output.value + output.fee(feerate).unwrap())
                        .to_signed()
                        .unwrap()
                })
                .sum();

            Ok(InputSelection {
                selected_inputs: request.available.to_vec(),
                surplus: (worth - cost).to_unsigned().unwrap(),
            })
        }
    }

    #[test]
    fn a_selector_answers_with_coins_from_those_on_offer() {
        let required = [utxo(0, 10_000)];
        let available = [utxo(1, 20_000), utxo(2, 30_000)];
        let outputs = [TxOut {
            value: Amount::from_sat(15_000),
            script_pubkey: ScriptBuf::new(),
        }];
        let params = params();

        let funding = TakeEverything
            .select(FundingRequest {
                required: &required,
                available: &available,
                outputs: &outputs,
                params: &params,
            })
            .unwrap();

        assert_eq!(funding.selected_inputs, available);
        assert!(!funding.selected_inputs.contains(&required[0]));
        // 45,000 left over, less 58 to spend each of the three coins and 9 to create the output.
        assert_eq!(funding.surplus, Amount::from_sat(44_817));
    }

    #[test]
    fn a_selector_with_nothing_on_offer_gives_up() {
        let params = params();

        let funding = TakeEverything.select(FundingRequest {
            required: &[],
            available: &[],
            outputs: &[],
            params: &params,
        });

        assert_eq!(funding, Err(NothingOnOffer));
    }
}

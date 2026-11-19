//! A [`CoinSelector`] that spends coins in random order until they cover the deficit.

use bitcoin::{FeeRate, SignedAmount};
use rand::{RngCore, seq::SliceRandom};

use super::{CoinSelector, FundingRequest, InputSelection};
use crate::blockspace::KnowsWeight;
use crate::decision_tree::MAX_INPUTS;
use crate::wallet::Utxo;

/// Funds a request with the coins on offer, taken in random order.
pub(crate) struct RandomSelector;

impl CoinSelector for RandomSelector {
    type Error = RandomSelectionError;

    /// Shuffles [`FundingRequest::available`] and takes coins in that order until their
    /// effective value covers the deficit.
    ///
    /// The deficit is the effective cost of [`FundingRequest::outputs`] and the fee for
    /// [`FundingRequest::overhead`]. The surplus is what the selection's effective value
    /// leaves over the deficit.
    fn select<R: RngCore>(
        &self,
        request: FundingRequest<'_>,
        rng: &mut R,
    ) -> Result<InputSelection, Self::Error> {
        if request
            .outputs
            .iter()
            .any(|output| output.value < output.script_pubkey.minimal_non_dust())
        {
            return Err(RandomSelectionError::DustOutput);
        }
        let feerate = request.feerate;
        let overhead = feerate
            .fee_wu(request.overhead)
            .ok_or(RandomSelectionError::Overflow)?;
        let deficit = request
            .outputs
            .iter()
            .try_fold(overhead, |sum, o| {
                let fee = o.fee(feerate).ok_or(RandomSelectionError::Overflow)?;
                o.value
                    .checked_add(fee)
                    .and_then(|cost| sum.checked_add(cost))
                    .ok_or(RandomSelectionError::Overflow)
            })?
            .to_signed()
            .map_err(|_| RandomSelectionError::Overflow)?;

        let mut coins: Vec<&Utxo> = request.available.iter().collect();
        coins.shuffle(rng);

        let mut selected_inputs = Vec::new();
        let mut value = SignedAmount::ZERO;
        for coin in coins {
            if selected_inputs.len() == MAX_INPUTS {
                return Err(RandomSelectionError::InputLimitExceeded);
            }
            let worth = effective_value(coin, feerate).ok_or(RandomSelectionError::Overflow)?;
            value = value
                .checked_add(worth)
                .ok_or(RandomSelectionError::Overflow)?;
            selected_inputs.push(coin.clone());
            if let Some(surplus) = value
                .checked_sub(deficit)
                .and_then(|diff| diff.to_unsigned().ok())
            {
                return Ok(InputSelection {
                    selected_inputs,
                    surplus,
                });
            }
        }
        Err(RandomSelectionError::NoCombinationCovers)
    }
}

/// Effective value of spending `coin` at `feerate`, net of its input fee and any
/// replacement liability attached to the coin.
fn effective_value(coin: &Utxo, feerate: FeeRate) -> Option<SignedAmount> {
    let cost = coin.fee(feerate)?.checked_add(coin.liability)?;
    coin.value
        .to_signed()
        .ok()?
        .checked_sub(cost.to_signed().ok()?)
}

/// Why coin selection could not fund a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RandomSelectionError {
    /// A payment output is below its script's dust threshold.
    DustOutput,
    /// Fee or amount arithmetic overflowed.
    Overflow,
    /// Covering the deficit requires more than 64 inputs.
    InputLimitExceeded,
    /// The coins on offer don't cover the deficit.
    NoCombinationCovers,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::TX_OVERHEAD;
    use bitcoin::{Amount, OutPoint, ScriptBuf, TxOut, Txid, Weight, hashes::Hash};
    use rand::{SeedableRng, rngs::StdRng};

    fn p2tr() -> ScriptBuf {
        let mut spk = vec![0x51, 0x20]; // OP_1 PUSH32
        spk.extend_from_slice(&[0xab; 32]);
        ScriptBuf::from_bytes(spk)
    }

    /// A coin spent by a taproot key path signature, 70 WU.
    fn coin(vout: u32, sats: u64) -> Utxo {
        Utxo::new(
            OutPoint::new(Txid::all_zeros(), vout),
            Amount::from_sat(sats),
            Weight::from_wu(70),
        )
    }

    /// Pays `sats` from `coins`.
    fn pay(coins: &[Utxo], sats: u64) -> Result<InputSelection, RandomSelectionError> {
        pay_seeded(coins, sats, 0)
    }

    /// [`pay`], drawing with an RNG seeded with `seed`.
    fn pay_seeded(
        coins: &[Utxo],
        sats: u64,
        seed: u64,
    ) -> Result<InputSelection, RandomSelectionError> {
        let outputs = [TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: p2tr(),
        }];

        RandomSelector.select(
            FundingRequest {
                available: coins,
                outputs: &outputs,
                feerate: FeeRate::from_sat_per_vb(1).unwrap(),
                overhead: TX_OVERHEAD,
            },
            &mut StdRng::seed_from_u64(seed),
        )
    }

    /// Of coins of 10,000 and 20,000 sats, only both together pay 25,000.
    #[test]
    fn the_only_qualifying_combination_is_selected() {
        let coins = [coin(0, 10_000), coin(1, 20_000)];
        let selection = pay(&coins, 25_000).unwrap();

        assert_eq!(selection.selected_inputs.len(), 2);
        assert!(coins.iter().all(|c| selection.selected_inputs.contains(c)));
        // 5,000 left over, minus 58 to spend each of the two coins, 43 to create the payment
        // and 11 for the transaction overhead.
        assert_eq!(
            selection.surplus,
            Amount::from_sat(5_000 - 58 * 2 - 43 - 11)
        );
    }

    /// Either coin alone pays 10,000, so each comes back alone under some seed.
    #[test]
    fn each_coin_is_drawn_alone_under_some_seed() {
        let coins = [coin(0, 30_000), coin(1, 30_000)];
        let drawn: Vec<Vec<Utxo>> = (0..32)
            .map(|seed| pay_seeded(&coins, 10_000, seed).unwrap().selected_inputs)
            .collect();

        for coin in &coins {
            assert!(drawn.contains(&vec![coin.clone()]));
        }
    }

    #[test]
    fn dust_payment_outputs_are_refused() {
        let coins = [coin(0, 10_000)];
        let dust = p2tr().minimal_non_dust().to_sat() - 1;

        assert_eq!(pay(&coins, dust), Err(RandomSelectionError::DustOutput));
    }

    #[test]
    fn overflowing_fees_deficits_and_worths_are_refused() {
        let sats = (1 << 62) + 1_000;
        let coins = [coin(0, sats), coin(1, sats)];
        assert_eq!(pay(&coins, sats), Err(RandomSelectionError::Overflow));
        assert_eq!(
            pay(&coins, (i64::MAX as u64) + 1),
            Err(RandomSelectionError::Overflow)
        );

        let outputs = [TxOut {
            value: Amount::from_sat(1_000),
            script_pubkey: p2tr(),
        }];
        assert_eq!(
            RandomSelector.select(
                FundingRequest {
                    available: &[coin(0, 10_000)],
                    outputs: &outputs,
                    feerate: FeeRate::MAX,
                    overhead: TX_OVERHEAD,
                },
                &mut StdRng::seed_from_u64(0),
            ),
            Err(RandomSelectionError::Overflow)
        );
    }

    #[test]
    fn exceeding_the_input_cap_is_refused() {
        let coins: Vec<_> = (0..=MAX_INPUTS as u32).map(|i| coin(i, 1_000)).collect();
        let target = 1_000 * MAX_INPUTS as u64;

        assert_eq!(
            pay(&coins, target),
            Err(RandomSelectionError::InputLimitExceeded)
        );
    }

    #[test]
    fn coin_liability_reduces_the_surplus() {
        let coins = [coin(0, 30_000).with_liability(Amount::from_sat(500))];
        let selection = pay(&coins, 10_000).unwrap();

        assert_eq!(
            selection.surplus,
            Amount::from_sat(20_000 - 58 - 43 - 11 - 500)
        );
        assert_eq!(coins[0].liability(), Amount::from_sat(500));
    }

    #[test]
    fn a_deficit_nothing_covers_is_refused() {
        let coins = [coin(0, 10_000), coin(1, 20_000)];

        assert_eq!(
            pay(&coins, 35_000),
            Err(RandomSelectionError::NoCombinationCovers)
        );
    }
}

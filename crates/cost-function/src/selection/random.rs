//! A [`CoinSelector`] that tries combinations of coins in random order.

use bitcoin::{FeeRate, SignedAmount};
use rand::{RngCore, seq::SliceRandom};

use super::{CoinSelector, FundingRequest, InputSelection, TX_OVERHEAD};
use crate::blockspace::KnowsWeight;
use crate::wallet::Utxo;

/// Most coins a [`RandomSelector`] takes. It tries every combination of them, 2^n - 1.
pub(crate) const MAX_COINS: usize = 16;

/// Funds a request with the first combination of the coins on offer, drawn at random, that
/// qualifies.
pub(crate) struct RandomSelector;

impl CoinSelector for RandomSelector {
    type Error = RandomSelectionError;

    /// Draws combinations of [`FundingRequest::available`] in random order and returns the
    /// first whose effective value covers the deficit. Refuses more than [`MAX_COINS`]
    /// coins on offer.
    ///
    /// The deficit is the effective cost of [`FundingRequest::outputs`] and the fee for
    /// [`TX_OVERHEAD`]. The surplus is what the combination's effective value leaves over
    /// the deficit.
    fn select<R: RngCore>(
        &self,
        request: FundingRequest<'_>,
        rng: &mut R,
    ) -> Result<InputSelection, Self::Error> {
        let coins = request.available;
        if coins.len() > MAX_COINS {
            return Err(RandomSelectionError::PoolTooLarge {
                coins: coins.len(),
                limit: MAX_COINS,
            });
        }

        let feerate = request.feerate;
        let deficit: SignedAmount = request
            .outputs
            .iter()
            .map(|o| o.value.checked_add(o.fee(feerate)?)?.to_signed().ok())
            .chain([feerate
                .fee_wu(TX_OVERHEAD)
                .and_then(|fee| fee.to_signed().ok())])
            .try_fold(SignedAmount::ZERO, |sum, cost| sum.checked_add(cost?))
            .ok_or(RandomSelectionError::NoCombinationCovers)?;

        // Each combination is a bitmask over positions in `coins`.
        let combination = |mask: u32| {
            (0..coins.len())
                .filter(move |i| mask >> i & 1 == 1)
                .map(|i| &coins[i])
        };
        let mut masks: Vec<u32> = (1..1 << coins.len()).collect();
        masks.shuffle(rng);

        let (mask, surplus) = masks
            .into_iter()
            .find_map(|mask| {
                let surplus = worth(combination(mask), feerate)?.checked_sub(deficit)?;
                Some((mask, surplus.to_unsigned().ok()?))
            })
            .ok_or(RandomSelectionError::NoCombinationCovers)?;

        Ok(InputSelection {
            selected_inputs: combination(mask).cloned().collect(),
            surplus,
        })
    }
}

/// Sum of the coins' effective values. `None` if any of them, or the sum, overflows.
fn worth<'a>(mut coins: impl Iterator<Item = &'a Utxo>, feerate: FeeRate) -> Option<SignedAmount> {
    coins.try_fold(SignedAmount::ZERO, |sum, c| {
        sum.checked_add(c.value.to_signed().ok()? - c.fee(feerate)?.to_signed().ok()?)
    })
}

/// Why a [`RandomSelector`] could not fund a request.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RandomSelectionError {
    /// More coins available than [`MAX_COINS`].
    PoolTooLarge { coins: usize, limit: usize },
    /// No combination qualifies, or the deficit overflows.
    NoCombinationCovers,
}

#[cfg(test)]
mod tests {
    use super::*;
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
            },
            &mut StdRng::seed_from_u64(seed),
        )
    }

    /// `n` coins of 1,000 sats each.
    fn pool(n: usize) -> Vec<Utxo> {
        (0..n as u32).map(|vout| coin(vout, 1_000)).collect()
    }

    /// Of coins of 10,000 and 20,000 sats, only both together pay 25,000.
    #[test]
    fn the_only_qualifying_combination_is_selected() {
        let coins = [coin(0, 10_000), coin(1, 20_000)];
        let selection = pay(&coins, 25_000).unwrap();

        assert_eq!(selection.selected_inputs, coins);
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

    /// Two coins together are worth more than a [`SignedAmount`] holds, so they are skipped
    /// rather than summed.
    #[test]
    fn a_combination_whose_worth_overflows_is_skipped() {
        let sats = (1 << 62) + 1_000;
        let coins = [coin(0, sats), coin(1, sats)];

        assert_eq!(
            pay(&coins, sats),
            Err(RandomSelectionError::NoCombinationCovers)
        );
    }

    #[test]
    fn a_deficit_nothing_covers_is_refused() {
        let coins = [coin(0, 10_000), coin(1, 20_000)];

        assert_eq!(
            pay(&coins, 35_000),
            Err(RandomSelectionError::NoCombinationCovers)
        );
    }

    #[test]
    fn the_cap_itself_still_selects() {
        assert!(pay(&pool(MAX_COINS), 500).is_ok());
    }

    #[test]
    fn oversized_pools_are_refused() {
        assert_eq!(
            pay(&pool(MAX_COINS + 1), 500),
            Err(RandomSelectionError::PoolTooLarge {
                coins: MAX_COINS + 1,
                limit: MAX_COINS,
            })
        );
    }
}

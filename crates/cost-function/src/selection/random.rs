//! A [`CoinSelector`] that tries combinations of coins in random order.

use std::cell::RefCell;

use bitcoin::{FeeRate, SignedAmount};
use rand::{Rng, seq::SliceRandom};

use super::{CoinSelector, FundingRequest, InputSelection};
use crate::blockspace::KnowsWeight;
use crate::wallet::Utxo;

/// Most coins a [`RandomSelector`] takes. It holds every combination of them, 2^n - 1.
pub(crate) const MAX_COINS: usize = 16;

/// Holds every combination of a set of coins, and funds a request with the first one it
/// draws at random that qualifies.
pub(crate) struct RandomSelector<'a, R> {
    coins: &'a [Utxo],
    /// Every non-empty combination, as positions in `coins`.
    combinations: Vec<Vec<usize>>,
    rng: RefCell<R>,
}

impl<'a, R: Rng> RandomSelector<'a, R> {
    /// Builds every combination of `coins` once, to be drawn from by
    /// [`RandomSelector::select`].
    ///
    /// Refuses more than [`MAX_COINS`] coins.
    pub(crate) fn new(coins: &'a [Utxo], rng: R) -> Result<Self, RandomSelectionError> {
        if coins.len() > MAX_COINS {
            return Err(RandomSelectionError::PoolTooLarge {
                coins: coins.len(),
                limit: MAX_COINS,
            });
        }

        let combinations = (1..1usize << coins.len())
            .map(|mask| (0..coins.len()).filter(|i| mask & (1 << i) != 0).collect())
            .collect();

        Ok(Self {
            coins,
            combinations,
            rng: RefCell::new(rng),
        })
    }
}

impl<R: Rng> CoinSelector for RandomSelector<'_, R> {
    type Error = RandomSelectionError;

    /// Draws combinations in random order and returns the first that qualifies: every
    /// coin in it is in [`FundingRequest::available`], and its effective value covers the
    /// deficit.
    ///
    /// The deficit is the effective cost of [`FundingRequest::outputs`] minus the
    /// effective value of [`FundingRequest::required`]. The surplus is what the
    /// combination's effective value leaves over the deficit.
    fn select(&self, request: FundingRequest<'_>) -> Result<InputSelection, Self::Error> {
        let feerate = request.params.feerate;
        let outputs_cost: Option<SignedAmount> = request
            .outputs
            .iter()
            .map(|o| o.value.checked_add(o.fee(feerate)?)?.to_signed().ok())
            .sum();
        let deficit = outputs_cost
            .zip(worth(request.required.iter(), feerate))
            .map(|(cost, worth)| cost - worth)
            .ok_or(RandomSelectionError::NoCombinationCovers)?;

        let mut order: Vec<&Vec<usize>> = self.combinations.iter().collect();
        order.shuffle(&mut *self.rng.borrow_mut());

        let (selected, surplus) = order
            .into_iter()
            .map(|positions| positions.iter().map(|&i| &self.coins[i]).collect())
            .filter(|coins: &Vec<&Utxo>| coins.iter().all(|c| request.available.contains(c)))
            .find_map(|coins| {
                let surplus = (worth(coins.iter().copied(), feerate)? - deficit).to_unsigned();
                Some((coins, surplus.ok()?))
            })
            .ok_or(RandomSelectionError::NoCombinationCovers)?;

        Ok(InputSelection {
            selected_inputs: selected.into_iter().cloned().collect(),
            surplus,
        })
    }
}

/// Sum of the coins' effective values. `None` if any of them overflows.
fn worth<'a>(coins: impl Iterator<Item = &'a Utxo>, feerate: FeeRate) -> Option<SignedAmount> {
    coins
        .map(|c| Some(c.value.to_signed().ok()? - c.fee(feerate)?.to_signed().ok()?))
        .sum()
}

/// Why a [`RandomSelector`] could not be built or could not fund a request.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RandomSelectionError {
    /// More coins than [`MAX_COINS`].
    PoolTooLarge { coins: usize, limit: usize },
    /// No combination qualifies, or the deficit overflows.
    NoCombinationCovers,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::selection::SelectionParams;
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

    fn pool(n: usize) -> Result<(), RandomSelectionError> {
        let coins: Vec<Utxo> = (0..n as u32).map(|vout| coin(vout, 1_000)).collect();
        RandomSelector::new(&coins, StdRng::seed_from_u64(0)).map(|_| ())
    }

    /// Pays `sats` with a required coin of 5,000 sats, from coins of 10,000 and 20,000 sats,
    /// with a third coin of 30,000 held but not on offer. For 25,000 sats only the first two
    /// together qualify.
    fn pay(sats: u64) -> Result<InputSelection, RandomSelectionError> {
        let held = vec![coin(0, 10_000), coin(1, 20_000), coin(2, 30_000)];
        let available = &held[..2];
        let params = SelectionParams {
            feerate: FeeRate::from_sat_per_vb(1).unwrap(),
        };
        let outputs = [TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: p2tr(),
        }];

        RandomSelector::new(&held, StdRng::seed_from_u64(0))
            .unwrap()
            .select(FundingRequest {
                required: &[coin(3, 5_000)],
                available,
                outputs: &outputs,
                params: &params,
            })
    }

    #[test]
    fn the_only_qualifying_combination_is_selected() {
        let selection = pay(25_000).unwrap();

        assert_eq!(
            selection.selected_inputs,
            [coin(0, 10_000), coin(1, 20_000)]
        );
        // 10,000 left over, minus 58 to spend each of the three coins and 43 to create the payment.
        assert_eq!(selection.surplus, Amount::from_sat(10_000 - 58 * 3 - 43));
    }

    #[test]
    fn a_deficit_nothing_covers_is_refused() {
        assert_eq!(pay(35_000), Err(RandomSelectionError::NoCombinationCovers));
    }

    #[test]
    fn the_cap_itself_still_builds() {
        assert!(pool(MAX_COINS).is_ok());
    }

    #[test]
    fn oversized_pools_are_refused() {
        assert_eq!(
            pool(MAX_COINS + 1).err(),
            Some(RandomSelectionError::PoolTooLarge {
                coins: MAX_COINS + 1,
                limit: MAX_COINS,
            })
        );
    }
}

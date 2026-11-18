//! A [`CoinSelector`] that spends coins in random order until they cover the deficit.

use bitcoin::{Amount, FeeRate, SignedAmount};
use rand::{RngCore, seq::SliceRandom};

use super::{CoinSelector, FundingRequest, InputSelection, TX_OVERHEAD};
use crate::blockspace::KnowsWeight;
use crate::wallet::Utxo;

/// Funds a request with the coins on offer, taken in random order.
pub(crate) struct RandomSelector;

impl CoinSelector for RandomSelector {
    type Error = RandomSelectionError;

    /// Shuffles [`FundingRequest::available`] and takes coins in that order until their
    /// effective value covers the deficit.
    ///
    /// The deficit is the effective cost of [`FundingRequest::outputs`] and the fee for
    /// [`TX_OVERHEAD`]. The surplus is what the selection's effective value leaves over
    /// the deficit.
    ///
    /// # Panics
    ///
    /// If the deficit, or the selection's effective value, overflows.
    fn select<R: RngCore>(
        &self,
        request: FundingRequest<'_>,
        rng: &mut R,
    ) -> Result<InputSelection, Self::Error> {
        let feerate = request.feerate;
        let deficit = request
            .outputs
            .iter()
            .map(|o| o.value + o.fee(feerate).expect("output fee overflows"))
            .chain([feerate.fee_wu(TX_OVERHEAD).expect("overhead fee overflows")])
            .fold(Amount::ZERO, |sum, cost| sum + cost)
            .to_signed()
            .expect("deficit overflows");

        let mut coins: Vec<&Utxo> = request.available.iter().collect();
        coins.shuffle(rng);

        let mut selected_inputs = Vec::new();
        let mut value = SignedAmount::ZERO;
        for coin in coins {
            selected_inputs.push(coin.clone());
            value += worth(std::iter::once(coin), feerate);
            if let Ok(surplus) = (value - deficit).to_unsigned() {
                return Ok(InputSelection {
                    selected_inputs,
                    surplus,
                });
            }
        }
        Err(RandomSelectionError::NoCombinationCovers)
    }
}

/// Sum of the coins' effective values.
///
/// # Panics
///
/// If any of them, or the sum, overflows.
fn worth<'a>(coins: impl Iterator<Item = &'a Utxo>, feerate: FeeRate) -> SignedAmount {
    coins.fold(SignedAmount::ZERO, |sum, c| {
        let fee = c.fee(feerate).expect("coin fee overflows");
        sum + c.value.to_signed().expect("coin value overflows")
            - fee.to_signed().expect("coin fee overflows")
    })
}

/// Why coin selection could not fund a request.
#[derive(Debug, PartialEq, Eq)]
pub enum RandomSelectionError {
    /// The coins on offer don't cover the deficit.
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

    /// Two coins together are worth more than a [`SignedAmount`] holds.
    #[test]
    #[should_panic(expected = "SignedAmount addition error")]
    fn a_combination_whose_worth_overflows_panics() {
        let sats = (1 << 62) + 1_000;
        let coins = [coin(0, sats), coin(1, sats)];

        let _ = pay(&coins, sats);
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

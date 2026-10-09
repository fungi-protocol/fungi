//! The wallet state that would be left behind if the wallet decided on a specific plan.
use std::collections::HashSet;

use bitcoin::hashes::Hash;
use bitcoin::{Amount, OutPoint, TxOut, Txid};

use crate::change::{
    ChangeSelector, ChangeStrategy, ChangeStrategyError, OutputWithSatisfactionWeight,
    ScriptpubkeyAssigner,
};
use crate::intent::Intent;
use crate::selection::{CoinSelector, FundingRequest, SelectionParams};
use crate::wallet::{Utxo, WalletStateSnapshot};

impl WalletStateSnapshot {
    /// The state the wallet would be in having funded `outputs` and broadcast the result.
    ///
    /// A position given more than once in `realizing` is realized once.
    ///
    /// The change strategy's errors convert into the coin selector's error type.
    pub(crate) fn realize<S, C, A>(
        &self,
        required: &[Utxo],
        realizing: &[usize],
        params: &SelectionParams,
        dust_limit: Amount,
        selector: &S,
        change_strategy: &ChangeStrategy<C, A>,
    ) -> Result<WalletStateSnapshot, S::Error>
    where
        S: CoinSelector,
        C: ChangeSelector,
        A: ScriptpubkeyAssigner,
        S::Error: From<ChangeStrategyError<C::Error, A::Error>>,
    {
        let mut seen = HashSet::new();
        let realizing: Vec<usize> = realizing
            .iter()
            .copied()
            .filter(|&position| seen.insert(position))
            .collect();

        //Right now these are txouts. When we are different actions that encompass different user demands, we will need to change this.
        let outputs: Vec<TxOut> = realizing
            .iter()
            .filter_map(|&position| self.intents().get(position))
            .flat_map(|intent| intent.action().outputs())
            .collect();

        let committed: HashSet<OutPoint> = required.iter().map(|coin| coin.outpoint).collect();
        let available: Vec<Utxo> = self
            .utxos()
            .iter()
            .filter(|coin| !committed.contains(&coin.outpoint))
            .cloned()
            .collect();

        let funding = selector.select(FundingRequest {
            required,
            available: &available,
            outputs: &outputs,
            params,
        })?;

        let spent: Vec<Utxo> = required
            .iter()
            .chain(&funding.selected_inputs)
            .cloned()
            .collect();

        let change = change_strategy.change(funding, dust_limit, params.feerate)?;

        Ok(self.spend(&spent, &outputs, &realizing, change))
    }

    /// The intents left once this transaction realizes some of them.
    fn discharged(&self, realized: &[usize]) -> impl Iterator<Item = Intent> {
        self.intents()
            .iter()
            .enumerate()
            .filter(|(position, _)| !realized.contains(position))
            .map(|(_, intent)| intent.clone())
    }

    /// Drop the coins the transaction consumes, credit the change it pays back.
    ///
    /// Only change returns to the wallet. Every other output pays someone else, which is
    /// what makes it a payment.
    fn spend(
        &self,
        spent: &[Utxo],
        outputs: &[TxOut],
        realized: &[usize],
        change: Vec<OutputWithSatisfactionWeight>,
    ) -> WalletStateSnapshot {
        // For simulations we don't really care about the actual txid for the outpoint of the change utxo. This just uses random 32 bytes
        let dummy_txid = random_txid();
        let consumed: HashSet<OutPoint> = spent.iter().map(|coin| coin.outpoint).collect();

        let surviving = self
            .utxos()
            .iter()
            .filter(|coin| !consumed.contains(&coin.outpoint))
            .cloned();

        // Change is appended after the payments, so its vouts start at however many there were.
        let credited =
            (outputs.len() as u32..)
                .zip(change)
                .map(|(vout, (output, satisfaction_weight))| {
                    Utxo::new(
                        OutPoint::new(dummy_txid, vout),
                        output.value,
                        satisfaction_weight,
                    )
                });

        WalletStateSnapshot::new(surviving.chain(credited), self.discharged(realized))
    }
}

fn random_txid() -> Txid {
    Txid::from_slice(&rand::random::<[u8; 32]>()).expect("Always 32 bytes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{FeeRate, ScriptBuf, Weight};
    use std::cell::RefCell;
    use std::convert::Infallible;
    use std::time::Instant;

    use crate::change::{ChangeSelection, OutputWithoutScriptpubkey};
    use crate::intent::{Action, FixedPaymentInstructions, PayoffCurve};
    use crate::selection::InputSelection;

    // Setup: a wallet holding coins and owing payments.

    /// A wallet holding a coin of each value in `coins` and owing a payment of each value
    /// in `owed`. The payment owed `owed[i]` is the intent at position `i`.
    fn wallet(coins: &[u64], owed: &[u64]) -> WalletStateSnapshot {
        let coins = coins.iter().zip(0..).map(|(&sats, vout)| coin(vout, sats));
        let intents = owed.iter().map(|&sats| {
            Intent::new(
                Action::OutputCreation(FixedPaymentInstructions {
                    script_pubkey: payee(),
                    amount: Amount::from_sat(sats),
                }),
                PayoffCurve::new(Instant::now(), []),
            )
        });

        WalletStateSnapshot::new(coins, intents)
    }

    fn coin(vout: u32, sats: u64) -> Utxo {
        Utxo::new(
            OutPoint::new(Txid::all_zeros(), vout),
            Amount::from_sat(sats),
            Weight::ZERO,
        )
    }

    /// Where every payment goes, so two intents for the same amount are
    /// indistinguishable by their outputs.
    fn payee() -> ScriptBuf {
        ScriptBuf::from_bytes(vec![0x52])
    }

    // Effect: realize some intents.

    /// Realize the intents at `positions`, spending every coin on offer and getting back
    /// a change output of each value in `change`, less the 10 sats it costs to create.
    fn realize(
        before: &WalletStateSnapshot,
        positions: &[usize],
        change: &[u64],
    ) -> Result<WalletStateSnapshot, Failed> {
        realize_with(before, &[], positions, &Decided::returning(change), change)
    }

    /// [`realize`] with the coins the intents commit to and the coin selector spelled out.
    fn realize_with<S>(
        before: &WalletStateSnapshot,
        required: &[Utxo],
        positions: &[usize],
        selector: &S,
        change: &[u64],
    ) -> Result<WalletStateSnapshot, Failed>
    where
        S: CoinSelector<Error = Failed>,
    {
        let params = SelectionParams {
            feerate: FeeRate::from_sat_per_vb(1).unwrap(),
        };

        before.realize(
            required,
            positions,
            &params,
            Amount::from_sat(330),
            selector,
            &ChangeStrategy::new(Decided::returning(change), OpTrue),
        )
    }

    // Checks: what the wallet holds and still owes.

    /// The value of each coin the wallet holds.
    fn holds(state: &WalletStateSnapshot) -> Vec<u64> {
        state
            .utxos()
            .iter()
            .map(|coin| coin.value.to_sat())
            .collect()
    }

    /// The value of each payment the wallet still owes.
    fn owes(state: &WalletStateSnapshot) -> Vec<u64> {
        state
            .intents()
            .iter()
            .flat_map(|intent| intent.action().outputs())
            .map(|output| output.value.to_sat())
            .collect()
    }

    // Selectors with the funding decision written out.

    /// Takes every coin on offer and hands back `change`.
    struct Decided {
        change: Vec<OutputWithoutScriptpubkey>,
    }

    impl Decided {
        fn returning(change: &[u64]) -> Self {
            let change = change
                .iter()
                .map(|&sats| OutputWithoutScriptpubkey(Amount::from_sat(sats)));

            Decided {
                change: change.collect(),
            }
        }
    }

    impl CoinSelector for Decided {
        type Error = Failed;

        fn select(&self, request: FundingRequest<'_>) -> Result<InputSelection, Failed> {
            for coin in request.required {
                assert!(
                    !request.available.contains(coin),
                    "a coin the outputs commit to is not on offer again"
                );
            }

            if request.available.is_empty() && request.required.is_empty() {
                return Err(Failed::NothingOnOffer);
            }

            Ok(InputSelection {
                selected_inputs: request.available.to_vec(),
                surplus: Amount::ZERO,
            })
        }
    }

    impl ChangeSelector for Decided {
        type Error = Infallible;

        fn select(
            &self,
            _request: InputSelection,
            _dust_limit: Amount,
        ) -> Result<ChangeSelection, Infallible> {
            Ok(ChangeSelection::new(self.change.clone()))
        }
    }

    /// Pays all change to `OP_TRUE`, which costs 10 sats to create at 1 sat/vB.
    struct OpTrue;

    impl ScriptpubkeyAssigner for OpTrue {
        type Error = Infallible;

        fn assign(
            &self,
            change: ChangeSelection,
        ) -> Result<Vec<OutputWithSatisfactionWeight>, Infallible> {
            Ok((change.selected_change.into_iter())
                .map(|OutputWithoutScriptpubkey(value)| {
                    let output = TxOut {
                        value,
                        script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
                    };
                    (output, Weight::ZERO)
                })
                .collect())
        }
    }

    /// [`Decided`] with no change, noting the outputs it was asked to fund.
    struct Recording {
        paid: RefCell<Vec<u64>>,
    }

    impl CoinSelector for Recording {
        type Error = Failed;

        fn select(&self, request: FundingRequest<'_>) -> Result<InputSelection, Failed> {
            self.paid
                .borrow_mut()
                .extend(request.outputs.iter().map(|output| output.value.to_sat()));

            CoinSelector::select(&Decided::returning(&[]), request)
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Failed {
        NothingOnOffer,
        Change(ChangeStrategyError<Infallible, Infallible>),
    }

    impl From<ChangeStrategyError<Infallible, Infallible>> for Failed {
        fn from(error: ChangeStrategyError<Infallible, Infallible>) -> Self {
            Failed::Change(error)
        }
    }

    #[test]
    fn realizing_spends_the_selected_coins_and_credits_the_change() {
        let before = wallet(&[100_000], &[30_000]);
        let after = realize(&before, &[0], &[69_000]).unwrap();

        // 30k to a stranger and 1k to the miner, less 10 to create the change.
        assert_eq!(holds(&after), [68_990]);
        assert_eq!(owes(&after), [0u64; 0]);
    }

    /// Scoring tells spent coins from change by outpoint, so change must not reuse one.
    #[test]
    fn change_is_a_new_coin_rather_than_one_the_wallet_held() {
        let before = wallet(&[100_000, 5_000], &[10_000]);
        let after = realize(&before, &[0], &[40_000, 49_000]).unwrap();

        for change in after.utxos() {
            assert!(
                before
                    .utxos()
                    .iter()
                    .all(|coin| coin.outpoint != change.outpoint)
            );
        }
    }

    /// A changeless transaction pays the surplus to the miner, so nothing returns.
    #[test]
    fn without_change_nothing_comes_back() {
        let before = wallet(&[100_000, 5_000], &[30_000]);
        let after = realize(&before, &[0], &[]).unwrap();

        assert_eq!(holds(&after), [0u64; 0]);
    }

    /// Coins the intents already commit to are spent, not offered again.
    #[test]
    fn a_required_coin_is_spent_without_being_selected() {
        let before = wallet(&[100_000, 5_000], &[30_000]);
        let committed = before.utxos()[0].clone();
        let after =
            realize_with(&before, &[committed], &[0], &Decided::returning(&[]), &[]).unwrap();

        assert_eq!(holds(&after), [0u64; 0]);
    }

    /// A selector that cannot fund the request says so, and no state is simulated.
    #[test]
    fn a_selector_that_gives_up_is_not_simulated() {
        let before = wallet(&[], &[30_000]);

        assert_eq!(realize(&before, &[0], &[]), Err(Failed::NothingOnOffer));
    }

    /// Change that cannot pay for its own creation above the dust limit fails the
    /// simulation.
    #[test]
    fn change_the_fee_pushes_below_dust_is_not_simulated() {
        let before = wallet(&[100_000], &[30_000]);

        assert_eq!(
            realize(&before, &[0], &[339]),
            Err(Failed::Change(ChangeStrategyError::BelowDustLimit))
        );
    }

    /// Only the intents the transaction was told to realize are discharged.
    #[test]
    fn an_intent_left_out_stays_owed() {
        let before = wallet(&[100_000], &[30_000, 70_000]);
        let after = realize(&before, &[0], &[]).unwrap();

        assert_eq!(owes(&before), [30_000, 70_000]);
        assert_eq!(owes(&after), [70_000]);
    }

    /// The whole reason intents are named by position rather than by value: two asking for
    /// the same payment are two obligations, and settling both takes two outputs.
    #[test]
    fn identical_intents_are_paid_one_output_each() {
        let before = wallet(&[100_000], &[30_000, 30_000]);
        let selector = Recording {
            paid: RefCell::default(),
        };
        let after = realize_with(&before, &[], &[0, 1], &selector, &[]).unwrap();

        assert_eq!(*selector.paid.borrow(), [30_000, 30_000]);
        assert_eq!(owes(&after), [0u64; 0]);
    }

    /// One intent named twice is still one obligation, paid by one output.
    #[test]
    fn a_position_given_twice_is_realized_once() {
        let before = wallet(&[100_000], &[30_000, 70_000]);
        let selector = Recording {
            paid: RefCell::default(),
        };
        let after = realize_with(&before, &[], &[0, 0], &selector, &[]).unwrap();

        assert_eq!(*selector.paid.borrow(), [30_000]);
        assert_eq!(owes(&after), [70_000]);
    }

    /// A position the snapshot does not hold pays for nothing and discharges nothing.
    #[test]
    fn a_position_past_the_end_asks_for_no_output() {
        let before = wallet(&[100_000], &[30_000]);
        let selector = Recording {
            paid: RefCell::default(),
        };
        let after = realize_with(&before, &[], &[1], &selector, &[]).unwrap();

        assert_eq!(*selector.paid.borrow(), [0u64; 0]);
        assert_eq!(owes(&after), [30_000]);
    }
}

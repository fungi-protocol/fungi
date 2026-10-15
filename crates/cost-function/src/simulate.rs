//! What the transactions of a plan the wallet decided on would change about it.
use std::collections::HashSet;

use bitcoin::{FeeRate, TxOut};
use rand::RngCore;

use crate::change::{
    ChangeSelector, ChangeStrategy, OutputWithoutScriptpubkey, ScriptpubkeyAssigner,
};
use crate::intent::Intent;
use crate::plan::PlanIntent;
use crate::selection::{CoinSelector, FundingRequest};
use crate::wallet::{Utxo, WalletStateSnapshot};

/// What one transaction changes about the wallet.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WalletStateDiff {
    /// Coins the transaction spends.
    pub(crate) spent: Vec<Utxo>,
    /// Coins it pays back to the wallet.
    pub(crate) change: Vec<OutputWithoutScriptpubkey>,
    /// [`Intent`]s it realizes.
    pub(crate) realized: Vec<Intent>,
    /// [`Intent`]s the wallet still owes afterwards, in no particular order.
    pub(crate) still_owed: Vec<Intent>,
}

impl<Id: Copy + Eq + std::hash::Hash> WalletStateSnapshot<Id> {
    /// What broadcasting one transaction per batch of `plan` would change about the wallet:
    /// one diff per batch, in batch order.
    ///
    /// Each batch is funded from the coins no earlier batch spent. Change is not added back,
    /// so a later batch never spends it. An id in more than one batch is realized by the
    /// first, and an id the queue does not hold is left out. `rng` is passed to `selector`.
    pub(crate) fn realize<S, C, A, R>(
        &self,
        plan: &PlanIntent<Id>,
        feerate: FeeRate,
        selector: &S,
        rng: &mut R,
        change_strategy: &ChangeStrategy<C, A>,
    ) -> Result<Vec<WalletStateDiff>, RealizeError<S::Error, C::Error>>
    where
        S: CoinSelector,
        C: ChangeSelector,
        A: ScriptpubkeyAssigner,
        R: RngCore,
    {
        let mut available = self.utxos().to_vec();
        let mut realized_so_far = HashSet::new();
        let mut diffs = Vec::with_capacity(plan.batches.len());

        for batch in &plan.batches {
            let realizing: Vec<&Intent> = batch
                .intents
                .iter()
                .copied()
                .filter(|&id| realized_so_far.insert(id))
                .filter_map(|id| self.get_intent(&id))
                .collect();

            let outputs: Vec<TxOut> = realizing
                .iter()
                .flat_map(|intent| intent.inner.payment_outputs())
                .collect();

            let funding = selector
                .select(
                    FundingRequest {
                        available: &available,
                        outputs: &outputs,
                        feerate,
                    },
                    rng,
                )
                .map_err(RealizeError::Selection)?;
            available.retain(|coin| !funding.selected_inputs.contains(coin));

            let change = change_strategy
                .change(&funding, feerate)
                .map_err(RealizeError::Change)?;

            let realized = realizing.into_iter().cloned().collect();
            let still_owed = self
                .intents()
                .filter(|(id, _)| !realized_so_far.contains(id))
                .map(|(_, intent)| intent.clone())
                .collect();

            diffs.push(WalletStateDiff {
                spent: funding.selected_inputs,
                change,
                realized,
                still_owed,
            });
        }

        Ok(diffs)
    }
}

/// Why [`WalletStateSnapshot::realize`] produced no diff.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RealizeError<S, C> {
    Selection(S),
    Change(C),
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::hashes::Hash;
    use bitcoin::{Amount, OutPoint, ScriptBuf, Txid, Weight};
    use std::cell::RefCell;
    use std::time::Instant;

    use crate::batch::Batch;
    use crate::change::ChangeSelection;
    use crate::change::single_output::SingleOutputChangeSelector;
    use crate::intent::{Action, FixedPaymentInstructions, PayoffCurve};
    use crate::selection::InputSelection;
    use crate::selection::random::RandomSelector;
    use rand::{SeedableRng, rngs::StdRng};

    type Wallet = WalletStateSnapshot<u64>;
    type Diff = WalletStateDiff;

    // Setup: a wallet holding coins and owing payments.

    /// A wallet holding a coin of each value in `coins` and owing a payment of each value
    /// in `owed`. The payment owed `owed[i]` is the intent with id `i`.
    fn wallet(coins: &[u64], owed: &[u64]) -> Wallet {
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

        WalletStateSnapshot::new(coins, (0..).zip(intents))
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

    /// A plan with one batch of the intents in each of `batches`.
    fn plan(batches: &[&[u64]]) -> PlanIntent<u64> {
        let batches = batches.iter().map(|ids| Batch {
            intents: ids.iter().copied().collect(),
        });

        PlanIntent {
            batches: batches.collect(),
        }
    }

    /// Realize the intents `ids` in one transaction, spending every coin on offer and
    /// getting back a change output of each value in `change`, less the 10 sats it costs to
    /// create.
    fn realize(before: &Wallet, ids: &[u64], change: &[u64]) -> Result<Diff, Failed> {
        realize_with(before, ids, &Decided::returning(change), change)
    }

    /// [`realize`] with the coin selector spelled out.
    fn realize_with<S>(
        before: &Wallet,
        ids: &[u64],
        selector: &S,
        change: &[u64],
    ) -> Result<Diff, Failed>
    where
        S: CoinSelector<Error = NothingOnOffer>,
    {
        realize_plan(before, &plan(&[ids]), selector, change).map(|mut diffs| diffs.remove(0))
    }

    /// Realize `plan`, one transaction per batch, each getting back `change`.
    fn realize_plan<S>(
        before: &Wallet,
        plan: &PlanIntent<u64>,
        selector: &S,
        change: &[u64],
    ) -> Result<Vec<Diff>, Failed>
    where
        S: CoinSelector<Error = NothingOnOffer>,
    {
        before.realize(
            plan,
            FeeRate::from_sat_per_vb(1).unwrap(),
            selector,
            &mut StdRng::seed_from_u64(0),
            &ChangeStrategy::new(Decided::returning(change), OpTrue),
        )
    }

    // Checks: what comes back to the wallet.

    /// What each of `intents` pays, smallest first.
    fn amounts(intents: &[Intent]) -> Vec<u64> {
        let mut amounts: Vec<u64> = intents
            .iter()
            .flat_map(|intent| intent.inner.payment_outputs())
            .map(|output| output.value.to_sat())
            .collect();
        amounts.sort();
        amounts
    }

    /// The value of each change coin.
    fn credited(diff: &Diff) -> Vec<u64> {
        diff.change
            .iter()
            .map(|output| output.value.to_sat())
            .collect()
    }

    // Selectors with the funding decision written out.

    /// Takes every coin on offer and hands back `change`.
    struct Decided {
        change: Vec<Amount>,
    }

    impl Decided {
        fn returning(change: &[u64]) -> Self {
            Decided {
                change: change.iter().copied().map(Amount::from_sat).collect(),
            }
        }
    }

    impl CoinSelector for Decided {
        type Error = NothingOnOffer;

        fn select<R: RngCore>(
            &self,
            request: FundingRequest<'_>,
            _rng: &mut R,
        ) -> Result<InputSelection, NothingOnOffer> {
            if request.available.is_empty() {
                return Err(NothingOnOffer);
            }

            Ok(InputSelection {
                selected_inputs: request.available.to_vec(),
                surplus: Amount::ZERO,
            })
        }
    }

    impl ChangeSelector for Decided {
        type Error = ();

        fn select(&self, _request: &InputSelection) -> Result<ChangeSelection, ()> {
            Ok(ChangeSelection(self.change.clone()))
        }
    }

    /// Fails to select change.
    struct GivesUp;

    impl ChangeSelector for GivesUp {
        type Error = ();

        fn select(&self, _request: &InputSelection) -> Result<ChangeSelection, ()> {
            Err(())
        }
    }

    /// Prices change as if paid to `OP_TRUE`, which costs 10 sats to create at 1 sat/vB.
    struct OpTrue;

    impl ScriptpubkeyAssigner for OpTrue {
        fn output_weight(&self) -> Weight {
            let output = TxOut {
                value: Amount::ZERO,
                script_pubkey: op_true(),
            };
            output.weight()
        }

        fn satisfaction_weight(&self) -> Weight {
            Weight::ZERO
        }

        fn dust(&self) -> Amount {
            op_true().minimal_non_dust()
        }
    }

    fn op_true() -> ScriptBuf {
        ScriptBuf::from_bytes(vec![0x51])
    }

    /// Takes the first coin on offer.
    struct FirstCoin;

    impl CoinSelector for FirstCoin {
        type Error = NothingOnOffer;

        fn select<R: RngCore>(
            &self,
            request: FundingRequest<'_>,
            _rng: &mut R,
        ) -> Result<InputSelection, NothingOnOffer> {
            let coin = request.available.first().ok_or(NothingOnOffer)?;

            Ok(InputSelection {
                selected_inputs: vec![coin.clone()],
                surplus: Amount::ZERO,
            })
        }
    }

    /// [`FirstCoin`], noting the outputs it was asked to fund.
    struct Recording {
        paid: RefCell<Vec<u64>>,
    }

    impl CoinSelector for Recording {
        type Error = NothingOnOffer;

        fn select<R: RngCore>(
            &self,
            request: FundingRequest<'_>,
            rng: &mut R,
        ) -> Result<InputSelection, NothingOnOffer> {
            self.paid
                .borrow_mut()
                .extend(request.outputs.iter().map(|output| output.value.to_sat()));

            FirstCoin.select(request, rng)
        }
    }

    /// A coin selector found no coins on offer.
    #[derive(Debug, PartialEq, Eq)]
    struct NothingOnOffer;

    type Failed = RealizeError<NothingOnOffer, ()>;

    #[test]
    fn realizing_spends_the_selected_coins_and_credits_the_change() {
        let before = wallet(&[100_000], &[30_000]);
        let diff = realize(&before, &[0], &[69_000]).unwrap();

        assert_eq!(diff.spent, before.utxos());
        // 30k to a stranger and 1k to the miner, less 10 to create the change.
        assert_eq!(credited(&diff), [68_990]);
        assert_eq!(amounts(&diff.realized), [30_000]);
        assert_eq!(amounts(&diff.still_owed), [0u64; 0]);
    }

    /// With the coin selector and change selector the wallet would use, the coins pay for
    /// the payment, the fee and nothing else.
    #[test]
    fn change_is_what_the_coins_leave_after_the_payment_and_the_fee() {
        let before = wallet(&[100_000], &[30_000]);
        let diffs = before
            .realize(
                &plan(&[&[0]]),
                FeeRate::from_sat_per_vb(1).unwrap(),
                &RandomSelector,
                &mut StdRng::seed_from_u64(0),
                &ChangeStrategy::new(SingleOutputChangeSelector, OpTrue),
            )
            .unwrap();

        // 282 WU at 1 sat/vB is 71 sats: 42 WU of overhead, 160 for the coin and 40 for
        // each output.
        assert_eq!(credited(&diffs[0]), [100_000 - 30_000 - 71]);
    }

    /// Equal change outputs are separate coins, so each one comes back.
    #[test]
    fn identical_change_outputs_each_come_back() {
        let before = wallet(&[100_000], &[10_000]);
        let diff = realize(&before, &[0], &[40_000, 40_000]).unwrap();

        assert_eq!(credited(&diff), [39_990, 39_990]);
    }

    /// A changeless transaction pays the surplus to the miner, so nothing returns.
    #[test]
    fn without_change_nothing_comes_back() {
        let before = wallet(&[100_000, 5_000], &[30_000]);
        let diff = realize(&before, &[0], &[]).unwrap();

        assert_eq!(credited(&diff), [0u64; 0]);
    }

    /// A selector that cannot fund the request says so, and no state is simulated.
    #[test]
    fn a_selector_that_gives_up_is_not_simulated() {
        let before = wallet(&[], &[30_000]);

        assert_eq!(
            realize(&before, &[0], &[]),
            Err(RealizeError::Selection(NothingOnOffer))
        );
    }

    /// A change selector that cannot select change fails the simulation.
    #[test]
    fn a_change_selector_that_gives_up_is_not_simulated() {
        let before = wallet(&[100_000], &[30_000]);
        assert_eq!(
            before.realize(
                &plan(&[&[0]]),
                FeeRate::from_sat_per_vb(1).unwrap(),
                &Decided::returning(&[]),
                &mut StdRng::seed_from_u64(0),
                &ChangeStrategy::new(GivesUp, OpTrue),
            ),
            Err(RealizeError::Change(()))
        );
    }

    /// Only the intents the transaction was told to realize are discharged.
    #[test]
    fn an_intent_left_out_stays_owed() {
        let before = wallet(&[100_000], &[30_000, 70_000]);
        let diff = realize(&before, &[0], &[]).unwrap();

        assert_eq!(amounts(&diff.realized), [30_000]);
        assert_eq!(amounts(&diff.still_owed), [70_000]);
    }

    /// The whole reason intents are named by id rather than by value: two asking for
    /// the same payment are two obligations, and settling both takes two outputs.
    #[test]
    fn identical_intents_are_paid_one_output_each() {
        let before = wallet(&[100_000], &[30_000, 30_000]);
        let selector = Recording {
            paid: RefCell::default(),
        };
        let diff = realize_with(&before, &[0, 1], &selector, &[]).unwrap();

        assert_eq!(*selector.paid.borrow(), [30_000, 30_000]);
        assert_eq!(amounts(&diff.realized), [30_000, 30_000]);
        assert_eq!(amounts(&diff.still_owed), [0u64; 0]);
    }

    /// One intent in two batches is still one obligation, paid by the first.
    #[test]
    fn an_id_in_two_batches_is_realized_once() {
        let before = wallet(&[100_000, 100_000], &[30_000, 70_000]);
        let selector = Recording {
            paid: RefCell::default(),
        };
        let diffs = realize_plan(&before, &plan(&[&[0], &[0, 1]]), &selector, &[]).unwrap();

        assert_eq!(*selector.paid.borrow(), [30_000, 70_000]);
        assert_eq!(amounts(&diffs[1].realized), [70_000]);
    }

    /// A later batch is funded from what earlier batches left, and owes only what no batch
    /// so far has realized.
    #[test]
    fn each_batch_spends_coins_the_earlier_ones_left() {
        let before = wallet(&[100_000, 50_000], &[30_000, 20_000]);
        let diffs = realize_plan(&before, &plan(&[&[0], &[1]]), &FirstCoin, &[]).unwrap();

        assert_eq!(diffs[0].spent, [before.utxos()[0].clone()]);
        assert_eq!(diffs[1].spent, [before.utxos()[1].clone()]);
        assert_eq!(amounts(&diffs[0].still_owed), [20_000]);
        assert_eq!(amounts(&diffs[1].still_owed), [0u64; 0]);
    }

    /// An id the queue does not hold pays for nothing and discharges nothing.
    #[test]
    fn an_id_the_queue_does_not_hold_asks_for_no_output() {
        let before = wallet(&[100_000], &[30_000]);
        let selector = Recording {
            paid: RefCell::default(),
        };
        let diff = realize_with(&before, &[1], &selector, &[]).unwrap();

        assert_eq!(*selector.paid.borrow(), [0u64; 0]);
        assert_eq!(amounts(&diff.realized), [0u64; 0]);
        assert_eq!(amounts(&diff.still_owed), [30_000]);
    }
}

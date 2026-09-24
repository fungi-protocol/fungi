//! The wallet state that would be left behind if the wallet decided on a specific plan.
use std::collections::HashSet;

use bitcoin::hashes::Hash;
use bitcoin::{OutPoint, TxOut, Txid};

use crate::queue::Queue;
use crate::selection::{CoinSelector, FundingRequest, SelectionParams};
use crate::wallet::{Utxo, WalletState};

impl<Q: Queue + Clone> WalletState<Q> {
    /// The state the wallet would be in having funded `outputs` and broadcast the result.
    pub(crate) fn realize<S: CoinSelector>(
        &self,
        required: &[Utxo],
        realizing: &[Q::Id],
        params: &SelectionParams,
        selector: &S,
    ) -> Result<WalletState<Q>, S::Error> {
        //Right now these are txouts. When we are different actions that encompass different user demands, we will need to change this.
        let outputs: Vec<TxOut> = realizing
            .iter()
            .filter_map(|&id| self.queue().get(id))
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

        let spent: Vec<Utxo> = required.iter().chain(&funding.inputs).cloned().collect();

        Ok(self.spend(&spent, &outputs, realizing, funding.change))
    }

    /// The queue with the intents this transaction realizes taken out of it.
    fn discharged(&self, realized: &[Q::Id]) -> Q {
        let mut queue = self.queue().clone();

        for &id in realized {
            queue.remove(id);
        }

        queue
    }

    /// Drop the coins the transaction consumes, credit the change it pays back.
    ///
    /// Only change returns to the wallet. Every other output pays someone else, which is
    /// what makes it a payment.
    fn spend(
        &self,
        spent: &[Utxo],
        outputs: &[TxOut],
        realized: &[Q::Id],
        change: Option<TxOut>,
    ) -> WalletState<Q> {
        // For simulations we don't really care about the actual txid for the outpoint of the change utxo. This just uses random 32 bytes
        let dummy_txid = random_txid();
        let consumed: HashSet<OutPoint> = spent.iter().map(|coin| coin.outpoint).collect();

        let surviving = self
            .utxos()
            .iter()
            .filter(|coin| !consumed.contains(&coin.outpoint))
            .cloned();

        // Change is appended after the payments, so its vout is however many there were.
        let credited = change.map(|prev_out| Utxo {
            outpoint: OutPoint::new(dummy_txid, outputs.len() as u32),
            prev_out,
        });

        WalletState::new(
            surviving.chain(credited).collect::<Vec<_>>(),
            self.discharged(realized),
        )
    }
}

fn random_txid() -> Txid {
    Txid::from_slice(&rand::random::<[u8; 32]>()).expect("Always 32 bytes")
}

#[cfg(test)]
mod tests {
    use super::*;
    use bitcoin::{Amount, FeeRate, ScriptBuf, Txid, Weight, hashes::Hash};
    use std::time::Instant;

    use crate::intent::{Action, FixedPaymentInstructions, Intent, PayoffCurve};
    use crate::queue::InMemoryQueue;
    use crate::selection::Funding;

    fn utxo(vout: u32, sats: u64) -> Utxo {
        Utxo {
            outpoint: OutPoint::new(Txid::all_zeros(), vout),
            prev_out: TxOut {
                value: Amount::from_sat(sats),
                script_pubkey: ScriptBuf::new(),
            },
        }
    }

    /// Where every payment in these tests goes, so two intents for the same amount are
    /// genuinely indistinguishable by their outputs.
    fn payee() -> ScriptBuf {
        ScriptBuf::from_bytes(vec![0x52])
    }

    fn payment(sats: u64) -> TxOut {
        TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: payee(),
        }
    }

    fn intent_paying(sats: u64) -> Intent {
        Intent::new(
            Action::OutputCreation(FixedPaymentInstructions {
                script_pubkey: payee(),
                amount: Amount::from_sat(sats),
            }),
            PayoffCurve::new(Instant::now(), []),
        )
    }

    fn params() -> SelectionParams {
        SelectionParams {
            feerate: FeeRate::from_sat_per_vb(10).expect("a feerate in range"),
            long_term_feerate: FeeRate::from_sat_per_vb(5).expect("a feerate in range"),
            change_script: ScriptBuf::from_bytes(vec![0x51]),
            dust_limit: Amount::from_sat(330),
            max_selection_weight: Weight::from_wu(400_000),
        }
    }

    /// A wallet holding `utxos` and owing a payment for each amount in `owed`, with the
    /// ids naming them in the order given.
    fn wallet(utxos: Vec<Utxo>, owed: &[u64]) -> (WalletState<InMemoryQueue>, Vec<u64>) {
        let mut queue = InMemoryQueue::new([]);
        let ids = owed.iter().map(|&sats| queue.push(intent_paying(sats)));

        let ids: Vec<u64> = ids.collect();

        (WalletState::new(utxos, queue), ids)
    }

    fn change_of(sats: u64) -> Option<TxOut> {
        Some(TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: params().change_script,
        })
    }

    /// Takes whatever it is offered and hands back `change`, so a test says outright what
    /// the funding decision was.
    struct Decided {
        change: Option<TxOut>,
    }

    impl CoinSelector for Decided {
        type Error = NothingOnOffer;

        fn select(&self, request: FundingRequest<'_>) -> Result<Funding, NothingOnOffer> {
            for coin in request.required {
                assert!(
                    !request.available.contains(coin),
                    "a coin the outputs commit to is not on offer again"
                );
            }

            if request.available.is_empty() && request.required.is_empty() {
                return Err(NothingOnOffer);
            }

            Ok(Funding {
                inputs: request.available.to_vec(),
                change: self.change.clone(),
            })
        }
    }

    #[derive(Debug, PartialEq, Eq)]
    struct NothingOnOffer;

    #[test]
    fn realizing_spends_the_selected_coins_and_credits_the_change() {
        let (before, owed) = wallet(vec![utxo(0, 100_000)], &[30_000]);
        let selector = Decided {
            change: change_of(69_000),
        };

        let after = before
            .realize(&[], &owed, &params(), &selector)
            .expect("the wallet has a coin to offer");

        // The spent coin is gone and only the change came back: 30k to a stranger and
        // 1k to the miner.
        assert_eq!(
            after
                .utxos()
                .iter()
                .map(|coin| coin.prev_out.value)
                .collect::<Vec<_>>(),
            [Amount::from_sat(69_000)]
        );
    }

    /// Change lands after the payments, so its vout depends on how many intents the
    /// transaction realizes.
    #[test]
    fn change_is_a_coin_the_wallet_can_go_on_to_spend() {
        let (before, owed) = wallet(vec![utxo(0, 100_000)], &[10_000, 20_000]);
        let selector = Decided {
            change: change_of(69_000),
        };

        let after = before
            .realize(&[], &owed, &params(), &selector)
            .expect("the wallet has a coin to offer");

        let change = &after.utxos()[0];

        assert_eq!(change.outpoint.vout, 2);
        assert_ne!(change.outpoint.txid, Txid::all_zeros());
    }

    /// The whole reason intents are named by id: two asking for the same payment are two
    /// obligations, and settling both takes two outputs.
    #[test]
    fn identical_intents_are_paid_one_output_each() {
        let (before, owed) = wallet(vec![utxo(0, 100_000)], &[30_000, 30_000]);
        let paid = std::cell::RefCell::new(Vec::new());

        let selector = Recording { paid: &paid };

        let after = before
            .realize(&[], &owed, &params(), &selector)
            .expect("the wallet has a coin to offer");

        assert_eq!(*paid.borrow(), [payment(30_000), payment(30_000)]);
        assert_eq!(after.queue().iter().count(), 0);
    }

    /// Notes the outputs it was asked to fund, so a test can see what the transaction pays.
    struct Recording<'a> {
        paid: &'a std::cell::RefCell<Vec<TxOut>>,
    }

    impl CoinSelector for Recording<'_> {
        type Error = NothingOnOffer;

        fn select(&self, request: FundingRequest<'_>) -> Result<Funding, NothingOnOffer> {
            self.paid
                .borrow_mut()
                .extend(request.outputs.iter().cloned());

            Ok(Funding {
                inputs: request.available.to_vec(),
                change: None,
            })
        }
    }

    /// A changeless transaction pays the surplus to the miner, so nothing returns.
    #[test]
    fn without_change_nothing_comes_back() {
        let (before, owed) = wallet(vec![utxo(0, 100_000), utxo(1, 5_000)], &[30_000]);
        let selector = Decided { change: None };

        let after = before
            .realize(&[], &owed, &params(), &selector)
            .expect("the wallet has coins to offer");

        assert!(after.utxos().is_empty());
    }

    /// Coins the intents already commit to are spent, not offered again.
    #[test]
    fn a_required_coin_is_spent_without_being_selected() {
        let committed = utxo(0, 100_000);
        let (before, owed) = wallet(vec![committed.clone(), utxo(1, 5_000)], &[30_000]);
        let selector = Decided { change: None };

        let after = before
            .realize(&[committed], &owed, &params(), &selector)
            .expect("the wallet has coins to offer");

        // The selector saw only the other coin, and both are spent regardless.
        assert!(after.utxos().is_empty());
    }

    /// A selector that cannot fund the request says so, and no state is simulated.
    #[test]
    fn a_selector_that_gives_up_is_not_simulated() {
        let (before, owed) = wallet(vec![], &[30_000]);
        let selector = Decided { change: None };

        assert_eq!(
            before.realize(&[], &owed, &params(), &selector),
            Err(NothingOnOffer)
        );
    }

    /// The wallet stops owing what it just paid, so the next batch is not asked to pay
    /// it again.
    #[test]
    fn realizing_an_intent_takes_it_off_the_queue() {
        let (before, owed) = wallet(vec![utxo(0, 100_000)], &[30_000]);
        let selector = Decided { change: None };

        let after = before
            .realize(&[], &owed, &params(), &selector)
            .expect("the wallet has a coin to offer");

        assert_eq!(before.queue().iter().count(), 1);
        assert_eq!(after.queue().iter().count(), 0);
    }

    /// Only the intents the transaction was told to realize are discharged.
    #[test]
    fn an_intent_left_out_stays_owed() {
        let (before, owed) = wallet(vec![utxo(0, 100_000)], &[30_000, 70_000]);
        let selector = Decided { change: None };

        let after = before
            .realize(&[], &owed[..1], &params(), &selector)
            .expect("the wallet has a coin to offer");

        let still_owed: Vec<TxOut> = after
            .queue()
            .iter()
            .flat_map(|(_, intent)| intent.action().outputs())
            .collect();

        assert_eq!(still_owed, [payment(70_000)]);
    }

    /// An id the queue no longer holds pays for nothing and discharges nothing.
    #[test]
    fn an_id_the_queue_has_forgotten_asks_for_no_output() {
        let (before, owed) = wallet(vec![utxo(0, 100_000)], &[30_000]);
        let selector = Decided { change: None };

        let after = before
            .realize(&[], &owed, &params(), &selector)
            .expect("the wallet has a coin to offer");

        // Realizing the same intent again finds nothing left to pay.
        let again = after
            .realize(&[], &owed, &params(), &selector)
            .expect_err("the wallet has nothing left to offer");

        assert_eq!(again, NothingOnOffer);
    }
}

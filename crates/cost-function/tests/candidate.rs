use bitcoin::{Amount, FeeRate, OutPoint, ScriptBuf, Txid, Weight, hashes::Hash};
use cost_function::{
    change::ChangeCosts,
    intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve},
    queue::InMemoryQueue,
    wallet::{Utxo, UtxoSnapshot, WalletStateSnapshot},
};
use rand::{SeedableRng, rngs::StdRng};
use std::time::{Duration, Instant};

struct Coins(Vec<Utxo>);
impl UtxoSnapshot for Coins {
    fn iter(&self) -> impl Iterator<Item = Utxo> {
        self.0.iter().cloned()
    }
}
struct Change;
impl ChangeCosts for Change {
    fn output_weight(&self) -> Weight {
        Weight::from_wu(40)
    }
    fn satisfaction_weight(&self) -> Weight {
        Weight::ZERO
    }
    fn dust(&self) -> Amount {
        Amount::from_sat(474)
    }
}

#[test]
fn a_funded_candidate_retains_equal_payment_intents_and_exposes_cost() {
    let start = Instant::now();
    let intent = Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            amount: Amount::from_sat(1_000),
        }),
        PayoffCurve::new(start, [(Duration::from_secs(10), Amount::from_sat(10_000))]),
    );
    let mut queue = InMemoryQueue::new([]);
    let first = queue.push(intent.clone());
    let second = queue.push(intent);
    let outpoint = OutPoint::new(Txid::all_zeros(), 1);
    let input_value = Amount::from_sat(10_000);
    let state = WalletStateSnapshot::new(
        Coins(vec![Utxo::new(outpoint, input_value, Weight::ZERO)]),
        queue,
    );
    let candidate = state
        .funded_candidate(
            FeeRate::from_sat_per_vb(1).unwrap(),
            &mut StdRng::seed_from_u64(0),
            &Change,
        )
        .unwrap()
        .unwrap();
    assert_eq!(
        candidate.payments().map(|(id, _)| *id).collect::<Vec<_>>(),
        [first, second]
    );
    assert_eq!(
        candidate
            .inputs()
            .map(|input| (input.outpoint(), input.value()))
            .collect::<Vec<_>>(),
        [(outpoint, input_value)]
    );
    assert_eq!(candidate.objective_cost().unwrap().to_sat(), 2_081);
    assert_eq!(
        candidate.change().next().unwrap().value,
        Amount::from_sat(7_919)
    );
}

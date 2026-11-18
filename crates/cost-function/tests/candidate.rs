use bitcoin::{Amount, FeeRate, OutPoint, ScriptBuf, Txid, Weight, hashes::Hash};
use cost_function::{
    candidate::{Candidate, CostSign, OrderChange, OrderChangeTime},
    change::ChangeCosts,
    intent::{FixedPaymentInstructions, Intent, Outcome, PayoffCurve, ScheduleGrid},
    queue::InMemoryQueue,
    wallet::{Utxo, UtxoSnapshot, WalletStateSnapshot},
};
use rand::{SeedableRng, rngs::StdRng};
use std::{
    cmp::Ordering,
    time::{Duration, Instant},
};

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

fn fund(queue: InMemoryQueue, input: Utxo) -> Candidate<u64> {
    WalletStateSnapshot::new(Coins(vec![input]), queue)
        .funded_candidate(
            FeeRate::from_sat_per_vb(1).unwrap(),
            &mut StdRng::seed_from_u64(0),
            &Change,
        )
        .unwrap()
        .unwrap()
}

fn payment(amount: u64, payoff: PayoffCurve) -> Intent {
    Intent::new(
        Outcome::OutputCreation(FixedPaymentInstructions {
            script_pubkey: ScriptBuf::from_bytes(vec![0x51]),
            amount: Amount::from_sat(amount),
        }),
        payoff,
    )
}

fn coin(sats: u64) -> Utxo {
    Utxo::new(
        OutPoint::new(Txid::all_zeros(), 1),
        Amount::from_sat(sats),
        Weight::ZERO,
    )
}

fn curve(start: u64, points: impl IntoIterator<Item = (u64, u64)>) -> PayoffCurve {
    PayoffCurve::new(
        start,
        points
            .into_iter()
            .map(|(gap, sats)| (gap, Amount::from_sat(sats))),
    )
    .unwrap()
}

#[test]
fn a_funded_candidate_retains_equal_payment_intents_and_exposes_cost() {
    let start = Instant::now();
    let grid = ScheduleGrid::new(start);
    let intent = payment(1_000, curve(0, [(100_000, 10_000)]));
    let mut queue = InMemoryQueue::new([]);
    let first = queue.push(intent.clone());
    let second = queue.push(intent);
    let outpoint = OutPoint::new(Txid::all_zeros(), 1);
    let input_value = Amount::from_sat(10_000);
    let candidate = fund(queue, Utxo::new(outpoint, input_value, Weight::ZERO));
    assert_eq!(
        candidate.payments().map(|(id, _)| *id).collect::<Vec<_>>(),
        [first, second]
    );
    assert_eq!(
        candidate
            .inputs()
            .map(|coin| (coin.outpoint(), coin.value()))
            .collect::<Vec<_>>(),
        [(outpoint, input_value)],
    );
    assert_eq!(candidate.objective_cost().unwrap().to_sat(), 2_081);
    assert_eq!(
        candidate.change().next().unwrap().value,
        Amount::from_sat(7_919)
    );
    let at = candidate.ready_at(start, grid).unwrap();
    assert!(at > start);
    assert_eq!(candidate.ready_at(at - ScheduleGrid::JIFFY, grid), Some(at));
    assert_eq!(candidate.ready_at(at, grid), Some(at));
    assert_eq!(candidate.next_order_change(&candidate, start, grid), None);
    assert_eq!(
        Candidate::joint_ready_at([&candidate], start, grid),
        Some(at)
    );
    assert_eq!(
        Candidate::joint_ready_at([&candidate, &candidate], start, grid),
        Some(at)
    );
    assert_eq!(Candidate::<u64>::joint_ready_at([], start, grid), None);
}

#[test]
fn a_candidate_with_opposing_payoffs_wakes_at_the_first_negative_tick() {
    let start = Instant::now();
    let grid = ScheduleGrid::new(start);
    let twelve_hours = 12 * 60 * 60 * 10_000;
    let midpoint = grid.at_slot(twelve_hours / 2).unwrap();
    let value = 1_048_582;
    // Two outputs and one input cost 71 sats at the fixture's fee rate.
    let queue = InMemoryQueue::new([
        payment(500_000, curve(0, [(0, 5), (twelve_hours, 1)])),
        payment(
            value - 500_000 - 71,
            curve(0, [(0, 1_048_576), (twelve_hours, 1_048_582)]),
        ),
    ]);
    let candidate = fund(queue, coin(value));
    let first = midpoint + ScheduleGrid::JIFFY;
    let between = midpoint + Duration::from_nanos(500);
    let tie = candidate.net_cost_at(midpoint, grid).unwrap();
    let (low, high) = tie.bounds();
    assert!((low..=high).contains(&0.0));
    assert_eq!(tie.sign(), CostSign::Overlap);
    assert_eq!(candidate.ready_at(midpoint, grid), Some(first));
    assert_eq!(candidate.ready_at(between, grid), Some(between));
    assert_eq!(candidate.ready_at(first, grid), Some(first));
    assert_eq!(candidate.ready_at(start, grid), Some(first));
}

#[test]
fn reordering_and_splitting_the_same_payoff_preserves_the_decision() {
    let start = Instant::now();
    let grid = ScheduleGrid::new(start);
    let candidate = |payoffs: [PayoffCurve; 4]| {
        fund(
            InMemoryQueue::new(payoffs.map(|payoff| payment(1_000, payoff))),
            coin(10_000),
        )
    };

    // Four payments and their fees cost 4101 sats. At the first coarse slot
    // the step contributes 1 sat and the ramps add 1/2 + 1/3 + 1/6 sats.
    let half = curve(0, [(0, 4_099), (4, 4_101)]);
    let split_half = curve(0, [(0, 4_099), (2, 4_100), (2, 4_101)]);
    let third = curve(0, [(3, 1)]);
    let sixth = curve(0, [(6, 1)]);
    let step = curve(1, [(0, 1)]);
    let original = candidate([half.clone(), third.clone(), sixth.clone(), step.clone()]);
    let reordered = candidate([sixth.clone(), third.clone(), half, step.clone()]);
    let split = candidate([split_half, third, sixth, step]);

    let tie = grid.at_slot(1).unwrap();
    let first = tie + ScheduleGrid::JIFFY;
    for candidate in [&original, &reordered, &split] {
        assert_eq!(candidate.objective_cost().unwrap().to_sat(), 4_101);
        assert_eq!(candidate.ready_at(start, grid), Some(first));
        assert_eq!(candidate.ready_at(tie, grid), Some(first));
        assert_eq!(candidate.ready_at(first, grid), Some(first));
    }
    assert_eq!(original.next_order_change(&split, start, grid), None);
    assert_eq!(split.next_order_change(&original, start, grid), None);
    assert_eq!(original.next_order_change(&reordered, start, grid), None);
    assert_eq!(reordered.next_order_change(&original, start, grid), None);
}

#[test]
fn an_existing_preference_reverses_before_either_action_is_ready() {
    let start = Instant::now();
    let grid = ScheduleGrid::new(start);
    let smaller = fund(
        InMemoryQueue::new([payment(1_000, curve(0, [(1, 10)]))]),
        coin(10_000),
    );
    let larger = fund(
        InMemoryQueue::new([payment(1_005, curve(0, [(1, 21)]))]),
        coin(10_000),
    );
    let lower = start + Duration::from_micros(45);
    let upper = lower + ScheduleGrid::JIFFY;
    let change = smaller.next_order_change(&larger, start, grid).unwrap();
    assert_eq!(
        change,
        OrderChange {
            timing: OrderChangeTime::Between { lower },
            before: Ordering::Less,
            after: Ordering::Greater,
        }
    );
    assert_eq!(change.at(), Some(upper));
    for candidate in [&smaller, &larger] {
        assert_eq!(
            candidate.net_cost_at(upper, grid).unwrap().sign(),
            CostSign::Positive
        );
        assert_eq!(candidate.ready_at(start, grid), None);
    }
}

#[test]
fn entering_and_leaving_a_tie_are_distinct_order_changes() {
    let start = Instant::now();
    let grid = ScheduleGrid::new(start);
    let rising = fund(
        InMemoryQueue::new([payment(1_000, curve(0, [(1, 100)]))]),
        coin(10_000),
    );
    let waiting = fund(
        InMemoryQueue::new([payment(1_000, curve(0, [(0, 100), (2, 100), (1, 200)]))]),
        coin(10_000),
    );
    let entered = grid.at_slot(1).unwrap();
    let last_equal = grid.at_slot(2).unwrap();
    assert_eq!(
        rising.next_order_change(&waiting, start, grid),
        Some(OrderChange {
            timing: OrderChangeTime::Between {
                lower: entered - ScheduleGrid::JIFFY
            },
            before: Ordering::Greater,
            after: Ordering::Equal,
        })
    );
    assert_eq!(
        rising.next_order_change(&waiting, entered, grid),
        Some(OrderChange {
            timing: OrderChangeTime::Between { lower: last_equal },
            before: Ordering::Equal,
            after: Ordering::Greater,
        })
    );
}

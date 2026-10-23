//! Times actions as the arrivals of a Poisson process.

use std::time::{Duration, Instant};

use rand::{Rng, RngCore};

use super::{IntoSequencedActions, SequencedActions, UnSequencedActions};

/// Takes actions in the order given, at the arrival times of a Poisson process starting at
/// `start`: one action every `mean_gap` on average.
pub(crate) struct PoissonSequencer<'a, R> {
    pub(crate) actions: UnSequencedActions,
    pub(crate) start: Instant,
    pub(crate) mean_gap: Duration,
    pub(crate) rng: &'a mut R,
}

impl<R: RngCore> IntoSequencedActions for PoissonSequencer<'_, R> {
    fn into_sequenced_actions(self) -> SequencedActions {
        let mut at = self.start;
        SequencedActions(
            self.actions
                .0
                .into_iter()
                .map(|action| {
                    at += self.mean_gap.mul_f64(standard_exponential(self.rng));
                    (action, at)
                })
                .collect(),
        )
    }
}

/// A draw from the standard exponential distribution.
fn standard_exponential<R: RngCore>(rng: &mut R) -> f64 {
    -(1.0 - rng.random::<f64>()).ln()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::Action;
    use bitcoin::{Amount, ScriptBuf, TxOut};
    use rand::{SeedableRng, rngs::StdRng};

    fn output(sats: u64) -> Action {
        Action::RegisterOutput(TxOut {
            value: Amount::from_sat(sats),
            script_pubkey: ScriptBuf::new(),
        })
    }

    fn sequence(actions: Vec<Action>, start: Instant) -> SequencedActions {
        PoissonSequencer {
            actions: UnSequencedActions(actions),
            start,
            mean_gap: Duration::from_secs(1),
            rng: &mut StdRng::seed_from_u64(0),
        }
        .into_sequenced_actions()
    }

    #[test]
    fn no_actions_take_no_time() {
        assert_eq!(sequence(vec![], Instant::now()), SequencedActions(vec![]));
    }

    #[test]
    fn actions_keep_their_order_and_follow_one_another_from_the_start() {
        let start = Instant::now();
        let actions = vec![output(1), output(2), output(3)];

        let SequencedActions(sequenced) = sequence(actions.clone(), start);
        let (sequenced_actions, instants): (Vec<_>, Vec<_>) = sequenced.into_iter().unzip();

        assert_eq!(sequenced_actions, actions);
        assert!(start <= instants[0]);
        assert!(instants.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    #[test]
    fn gaps_are_exponential_with_a_mean_of_one() {
        let mut rng = StdRng::seed_from_u64(0);
        let draws: Vec<f64> = (0..10_000)
            .map(|_| standard_exponential(&mut rng))
            .collect();
        let n = draws.len() as f64;
        let mean = draws.iter().sum::<f64>() / n;
        let std_dev = (draws.iter().map(|d| (d - mean).powi(2)).sum::<f64>() / n).sqrt();

        // An exponential's standard deviation equals its mean, which a constant gap would fail.
        assert!((mean - 1.0).abs() < 0.05, "mean {mean}");
        assert!((std_dev - 1.0).abs() < 0.05, "standard deviation {std_dev}");
    }
}

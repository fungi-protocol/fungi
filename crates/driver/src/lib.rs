#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Drives a [`StateMachine`] and performs its IO, on any async runtime.

use std::future::Future;
use std::pin::pin;

use fungi_core::persist::Persister;
use fungi_core::state_machine::{StateMachine, commit, replay};
use futures::stream::{FuturesUnordered, Stream, StreamExt};

/// Run a session until it closes or someother externality occurs
///
/// Replays `persister` onto `initial` first. Then polls `network` for inbound events and `plan`
/// for plan actions that have come due, along with effects in flight. A resumed session should
/// start `plan` with an input that makes the state machine re-issue its pending effects. Each
/// effect runs through `exec`, and its output is fed back as an input. Effects still in flight
/// when the session closes are dropped.
///
/// Returns the final state, or the first error (?).
pub async fn run<SM, P, N, L, E, F>(
    initial: SM,
    persister: &P,
    network: N,
    plan: L,
    exec: E,
) -> Result<SM, P::InternalStorageError>
where
    SM: StateMachine,
    P: Persister<SessionEvent = SM::Event>,
    N: Stream<Item = SM::Input>,
    L: Stream<Item = SM::Input>,
    E: Fn(SM::Effect) -> F,
    F: Future<Output = SM::Input>,
{
    let mut sm = replay(initial, persister)?;
    if sm.is_closed() {
        return Ok(sm);
    }
    let mut network = pin!(network.fuse());
    let mut plan = pin!(plan.fuse());
    let mut in_flight = FuturesUnordered::new();

    loop {
        let input = futures::select! {
            input = network.select_next_some() => input,
            input = plan.select_next_some() => input,
            input = in_flight.select_next_some() => input,
            complete => return Ok(sm),
        };

        let decision = sm.decide(input);
        let (next, effects) = commit(sm, decision, persister)?;
        sm = next;
        if sm.is_closed() {
            return Ok(sm);
        }
        in_flight.extend(effects.into_iter().map(&exec));
    }
}

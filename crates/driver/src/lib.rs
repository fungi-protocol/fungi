#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Drives a [`StateMachine`] and performs its IO, on any async runtime.

use std::future::Future;
use std::pin::pin;

use fungi_core::persist::Persister;
use fungi_core::state_machine::{StateMachine, commit, replay};
use futures::stream::{FuturesUnordered, Stream, StreamExt};

/// Run a session until it closes or runs out of inputs and in-flight effects.
///
/// Replays `persister` onto `initial` first. A resumed session should start `inputs` with an input
/// that makes the state machine re-issue its pending effects. Each effect runs through `exec`, and
/// its output is fed back as an input. Effects still in flight when the session closes are
/// dropped.
///
/// Returns the final state, or the first storage error.
pub async fn run<SM, P, I, E, F>(
    initial: SM,
    persister: &P,
    inputs: I,
    exec: E,
) -> Result<SM, P::InternalStorageError>
where
    SM: StateMachine,
    P: Persister<SessionEvent = SM::Event>,
    I: Stream<Item = SM::Input>,
    E: Fn(SM::Effect) -> F,
    F: Future<Output = SM::Input>,
{
    let mut sm = replay(initial, persister)?;
    let mut inputs = pin!(inputs.fuse());
    let mut in_flight = FuturesUnordered::new();

    while !sm.is_closed() {
        let input = futures::select! {
            input = inputs.select_next_some() => input,
            input = in_flight.select_next_some() => input,
            complete => break,
        };

        let decision = sm.decide(input);
        let (next, effects) = commit(sm, decision, persister)?;
        sm = next;
        in_flight.extend(effects.into_iter().map(&exec));
    }

    Ok(sm)
}

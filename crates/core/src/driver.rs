//! Apply a due action to state, drive its requested IO, and await information.

use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::{collections::BTreeSet, time::Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Scheduled<A> {
    pub at: Instant,
    pub action: A,
}

pub enum Decision<A> {
    Schedule(Scheduled<A>),
    Wait,
    Done,
}

/// Decisions and action application only change local state; they do not perform IO.
pub trait Machine {
    type Action;
    type Operation;
    type OperationId: Clone + Ord;
    type Input;
    type Result;

    fn decide(&mut self, now: Instant) -> Decision<Self::Action>;
    fn apply(&mut self, action: Self::Action);
    /// All requested operations still awaiting a result, with stable IDs.
    fn operations(&self) -> impl Iterator<Item = (Self::OperationId, Self::Operation)>;
    fn completed(&mut self, id: Self::OperationId, result: Self::Result);
    fn observe(&mut self, input: Self::Input);
}

pub enum Wake<I> {
    Input(I),
    Timeout,
    Closed,
}

/// The clock, effects and input channels used by a driver run.
pub trait Environment<M: Machine> {
    fn now(&self) -> Instant;
    /// Own the operation's IO, without borrowing the environment until completion.
    fn execute(
        &mut self,
        operation: M::Operation,
    ) -> impl Future<Output = M::Result> + use<Self, M>;
    /// A deadline may already be due. With no deadline, wait for channel input.
    /// This wait may be canceled when an operation completes; receiving must be cancel-safe.
    fn wait(&mut self, deadline: Option<Instant>) -> impl Future<Output = Wake<M::Input>>;
}

#[derive(Debug, PartialEq, Eq)]
pub enum Stopped {
    Done,
    Closed,
}

/// Shutdown drains initiated IO. Dropping this whole future is an external cancellation;
/// persistence and recovery of such operations are outside this PoC.
pub async fn run<M: Machine>(machine: &mut M, io: &mut impl Environment<M>) -> Stopped {
    let mut pending: Option<Scheduled<M::Action>> = None;
    let mut active = BTreeSet::new();
    let mut operations = FuturesUnordered::new();
    let stopped = loop {
        if pending.as_ref().is_some_and(|action| action.at <= io.now()) {
            machine.apply(pending.take().expect("the scheduled action is due").action);
        }
        for (id, operation) in machine.operations() {
            if active.insert(id.clone()) {
                let effect = io.execute(operation);
                operations.push(async move { (id, effect.await) });
            }
        }
        pending = match machine.decide(io.now()) {
            Decision::Schedule(action) => Some(action),
            Decision::Wait => None,
            Decision::Done => break Stopped::Done,
        };
        let deadline = pending.as_ref().map(|action| action.at);
        let wake = if operations.is_empty() {
            io.wait(deadline).await
        } else {
            // Only the receive/deadline wait is recreated. Operation futures stay alive.
            futures::select_biased! {
                completed = operations.next().fuse() => {
                    let (id, result) = completed.expect("operations are not empty");
                    active.remove(&id);
                    machine.completed(id, result);
                    pending = None;
                    continue;
                },
                wake = io.wait(deadline).fuse() => wake,
            }
        };
        match wake {
            Wake::Input(input) => {
                machine.observe(input);
                pending = None;
            }
            Wake::Timeout => {}
            Wake::Closed => break Stopped::Closed,
        }
    };
    while let Some((id, result)) = operations.next().await {
        machine.completed(id, result);
    }
    stopped
}

//! Composition of execution capabilities with the driver's input channels and clock.

use super::{
    Broadcaster, Error, Executor, Observation, Operation, State, Update,
    session::{Message, NoPeer},
};
use crate::{
    driver::{Environment, Wake},
    wallet::{ChangeSource, InputSigner, TransactionRecorder},
};
use fungi_transport::SendChannel;
use std::time::Instant;

/// Time and observations for one driver run.
///
/// Waiting follows the cancellation contract of [`Environment::wait`]. An empty
/// input stream stays open while a producer can still deliver observations.
pub trait Events {
    fn now(&self) -> Instant;
    fn wait(&mut self, deadline: Option<Instant>) -> impl Future<Output = Wake<Observation>>;
}

pub struct Runtime<C, S, R, B, E, P = NoPeer> {
    pub executor: Executor<C, S, R, B, P>,
    pub events: E,
}

impl<
    C: ChangeSource,
    S: InputSigner,
    R: TransactionRecorder,
    B: Broadcaster,
    E: Events,
    P: SendChannel<Message>,
    Ch: cost_function::change::ChangeCosts,
    G: rand::RngCore,
> Environment<State<Ch, G>> for Runtime<C, S, R, B, E, P>
{
    fn now(&self) -> Instant {
        self.events.now()
    }
    fn execute(
        &mut self,
        operation: Operation,
    ) -> impl Future<Output = Result<Update, Error>> + use<C, S, R, B, E, P, Ch, G> {
        let executor = self.executor.clone();
        async move { executor.execute(&operation).await }
    }
    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        self.events.wait(deadline).await
    }
}

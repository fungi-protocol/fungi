//! Composition of execution capabilities with the driver's input channels and clock.

use super::{Error, Executor, Observation, Request, State, Update};
use crate::{
    driver::{Environment, Wake},
    wallet::{ChangeSource, InputSigner},
};
use std::time::Instant;

/// Time and observations for one driver run.
///
/// Waiting follows the cancellation contract of [`Environment::wait`]. An empty
/// input stream stays open while a producer can still deliver observations.
pub trait Events {
    fn now(&self) -> Instant;
    fn wait(&mut self, deadline: Option<Instant>) -> impl Future<Output = Wake<Observation>>;
}

pub struct Runtime<C, S, E> {
    pub executor: Executor<C, S>,
    pub events: E,
}

impl<
    C: ChangeSource,
    S: InputSigner,
    E: Events,
    P: cost_function::change::ChangeCosts,
    G: rand::RngCore,
> Environment<State<P, G>> for Runtime<C, S, E>
{
    fn now(&self) -> Instant {
        self.events.now()
    }
    fn execute(
        &mut self,
        operation: Request,
    ) -> impl Future<Output = Result<Update, Error>> + use<C, S, E, P, G> {
        let executor = self.executor.clone();
        async move { executor.execute(&operation).await }
    }
    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        self.events.wait(deadline).await
    }
}

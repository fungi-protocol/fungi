//! Composition of execution capabilities with the driver's clock.

use super::{Error, Executor, Observation, Request, State, Update};
use crate::driver::{Environment, Wake};
use std::time::Instant;

/// Time and observations for one driver run.
///
/// Waiting follows the cancellation contract of [`Environment::wait`]. An empty
/// input stream stays open while a producer can still deliver observations.
pub trait Events {
    fn now(&self) -> Instant;
    fn wait(&mut self, deadline: Option<Instant>) -> impl Future<Output = Wake<Observation>>;
}

pub struct Runtime<E> {
    pub executor: Executor,
    pub events: E,
}

impl<E: Events, P: cost_function::change::ChangeCosts, G: rand::RngCore> Environment<State<P, G>>
    for Runtime<E>
{
    fn now(&self) -> Instant {
        self.events.now()
    }
    fn execute(
        &mut self,
        operation: Request,
    ) -> impl Future<Output = Result<Update, Error>> + use<E, P, G> {
        let executor = self.executor.clone();
        async move { executor.execute(&operation).await }
    }
    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Observation> {
        self.events.wait(deadline).await
    }
}

use fungi_core::driver::{self, Environment, Machine, Stopped, Wake};
use std::{cell::Cell, collections::BTreeSet, fmt::Debug, time::Instant};

/// Drive a fixture with virtual time and one-shot effects.
pub async fn run<M: Machine, E: Environment<M>>(
    machine: &mut M,
    io: &mut E,
    id: fn(&M::Operation) -> M::OperationId,
) -> Stopped
where
    M::OperationId: Debug,
{
    let last_timeout = Cell::new(None);
    driver::run(
        machine,
        &mut Script {
            io,
            id,
            started: BTreeSet::new(),
            last_timeout: &last_timeout,
        },
    )
    .await
}

struct Script<'a, M: Machine, E> {
    io: &'a mut E,
    id: fn(&M::Operation) -> M::OperationId,
    started: BTreeSet<M::OperationId>,
    last_timeout: &'a Cell<Option<Instant>>,
}

impl<'a, M: Machine, E: Environment<M>> Environment<M> for Script<'a, M, E>
where
    M::OperationId: Debug,
{
    fn now(&self) -> Instant {
        self.io.now()
    }

    fn execute(
        &mut self,
        operation: M::Operation,
    ) -> impl Future<Output = M::Result> + use<'a, M, E> {
        let id = (self.id)(&operation);
        assert!(
            self.started.insert(id.clone()),
            "the fixture started operation {id:?} twice"
        );
        let last_timeout = self.last_timeout;
        last_timeout.set(None);
        let effect = self.io.execute(operation);
        async move {
            let result = effect.await;
            last_timeout.set(None);
            result
        }
    }

    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<M::Input> {
        let wake = self.io.wait(deadline).await;
        match &wake {
            Wake::Input(_) => self.last_timeout.set(None),
            Wake::Timeout => {
                let now = Some(self.io.now());
                assert_ne!(
                    self.last_timeout.replace(now),
                    now,
                    "a second timeout at the same virtual instant without input or effect progress"
                );
            }
            Wake::Closed => {}
        }
        wake
    }
}

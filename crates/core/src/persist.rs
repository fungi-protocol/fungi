//! Storage for the event logs a state machine replays.

/// An append only log of the events one session produced.
pub trait Persister {
    /// Whatever the storage layer underneath fails with.
    type InternalStorageError: std::error::Error + Send + Sync + 'static;

    /// The events this session records.
    type SessionEvent;

    /// Append one event to the log.
    fn save_event(&self, event: Self::SessionEvent) -> Result<(), Self::InternalStorageError>;

    /// Every event of the session, in the order they were saved.
    fn load(
        &self,
    ) -> Result<Box<dyn Iterator<Item = Self::SessionEvent>>, Self::InternalStorageError>;

    /// Close the session, after which nothing more is appended.
    fn close(&self) -> Result<(), Self::InternalStorageError>;
}

/// What a state transition asks its persister to do.
pub enum PersistActions<Event> {
    /// Nothing happened worth recording.
    NoOp,
    /// Record one event.
    Save(Event),
    /// Record one event and end the session.
    /// If close fails for any reason, the event is still recorded.
    /// Similarly if save fails, the session is not closed.
    SaveAndClose(Event),
}

impl<Event> PersistActions<Event> {
    /// Carry out the action against `persister`.
    pub fn execute<P>(self, persister: &P) -> Result<(), P::InternalStorageError>
    where
        P: Persister<SessionEvent = Event>,
    {
        match self {
            Self::NoOp => {}
            Self::Save(event) => persister.save_event(event)?,
            Self::SaveAndClose(event) => {
                persister.save_event(event)?;
                persister.close()?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use super::*;
    use std::cell::{Cell, RefCell};
    use std::convert::Infallible;

    #[derive(Default)]
    struct InMemoryPersister {
        events: RefCell<Vec<u8>>,
        closed: RefCell<bool>,
    }

    impl Persister for InMemoryPersister {
        type InternalStorageError = Infallible;
        type SessionEvent = u8;

        fn save_event(&self, event: u8) -> Result<(), Infallible> {
            assert!(!*self.closed.borrow());
            self.events.borrow_mut().push(event);
            Ok(())
        }

        fn load(&self) -> Result<Box<dyn Iterator<Item = u8>>, Infallible> {
            Ok(Box::new(self.events.borrow().clone().into_iter()))
        }

        fn close(&self) -> Result<(), Infallible> {
            *self.closed.borrow_mut() = true;
            Ok(())
        }
    }

    #[derive(Default)]
    struct FailingPersister {
        inner: InMemoryPersister,
        fail_save: Cell<bool>,
        fail_load: Cell<bool>,
        close_calls: Cell<usize>,
    }

    impl Persister for FailingPersister {
        type InternalStorageError = TestError;
        type SessionEvent = u8;

        fn save_event(&self, event: u8) -> Result<(), TestError> {
            if self.fail_save.get() {
                return Err(TestError::Save);
            }
            self.inner.save_event(event).map_err(|never| match never {})
        }

        fn load(&self) -> Result<Box<dyn Iterator<Item = u8>>, TestError> {
            if self.fail_load.get() {
                return Err(TestError::Load);
            }
            self.inner.load().map_err(|never| match never {})
        }

        fn close(&self) -> Result<(), TestError> {
            self.close_calls.set(self.close_calls.get() + 1);
            self.inner.close().map_err(|never| match never {})
        }
    }

    #[test]
    fn events_load_in_the_order_they_were_saved() {
        let persister = InMemoryPersister::default();

        persister
            .save_event(1)
            .expect("in memory storage cannot fail");
        persister
            .save_event(2)
            .expect("in memory storage cannot fail");

        let replayed: Vec<u8> = persister
            .load()
            .expect("in memory storage cannot fail")
            .collect();

        assert_eq!(replayed, [1, 2]);
    }

    #[test]
    fn a_closed_session_says_so() {
        let persister = InMemoryPersister::default();

        persister.close().expect("in memory storage cannot fail");

        assert!(*persister.closed.borrow());
    }

    fn execute(action: PersistActions<u8>) -> InMemoryPersister {
        let persister = InMemoryPersister::default();

        action
            .execute(&persister)
            .expect("in memory storage cannot fail");

        persister
    }

    #[test]
    fn a_no_op_records_nothing() {
        let persister = execute(PersistActions::NoOp);

        assert!(persister.events.borrow().is_empty());
        assert!(!*persister.closed.borrow());
    }

    #[test]
    fn a_save_leaves_the_session_open() {
        let persister = execute(PersistActions::Save(1));

        assert_eq!(*persister.events.borrow(), [1]);
        assert!(!*persister.closed.borrow());
    }

    #[test]
    fn a_save_failure_before_append_preserves_previous_events() {
        for action in [PersistActions::Save(2), PersistActions::SaveAndClose(2)] {
            let persister = FailingPersister {
                inner: execute(PersistActions::Save(1)),
                ..Default::default()
            };
            persister.fail_save.set(true);

            let result = action.execute(&persister);

            assert_eq!(result, Err(TestError::Save));
            let replayed: Vec<_> = persister
                .load()
                .expect("no load failure configured")
                .collect();
            assert_eq!(replayed, [1]);
            assert!(!*persister.inner.closed.borrow());
            assert_eq!(persister.close_calls.get(), 0);

            persister.fail_save.set(false);
            persister.save_event(3).expect("save failure cleared");
            assert_eq!(*persister.inner.events.borrow(), [1, 3]);
        }
    }

    #[test]
    fn loading_never_closes_the_session() {
        for closed in [false, true] {
            for fail_load in [false, true] {
                let persister = FailingPersister {
                    inner: execute(PersistActions::Save(1)),
                    ..Default::default()
                };
                if closed {
                    persister.close().expect("close cannot fail");
                }
                let close_calls = persister.close_calls.get();
                persister.fail_load.set(fail_load);

                let result = persister.load().map(|events| events.collect::<Vec<_>>());

                let expected = if fail_load {
                    Err(TestError::Load)
                } else {
                    Ok(vec![1])
                };
                assert_eq!(result, expected);
                assert_eq!(*persister.inner.closed.borrow(), closed);
                assert_eq!(persister.close_calls.get(), close_calls);
                assert_eq!(*persister.inner.events.borrow(), [1]);

                persister.fail_load.set(false);
                if !closed {
                    persister.save_event(2).expect("no save failure configured");
                }
                let replayed: Vec<_> = persister.load().expect("load failure cleared").collect();
                assert_eq!(replayed, if closed { vec![1] } else { vec![1, 2] });
                assert_eq!(*persister.inner.closed.borrow(), closed);
                assert_eq!(persister.close_calls.get(), close_calls);
            }
        }
    }

    #[test]
    fn a_closing_save_records_the_event_first() {
        let persister = execute(PersistActions::SaveAndClose(1));

        assert_eq!(*persister.events.borrow(), [1]);
        assert!(*persister.closed.borrow());
    }

    #[derive(Debug, PartialEq, Eq)]
    enum Call {
        Save(u8),
        Close,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
    enum TestError {
        #[error("save failed")]
        Save,
        #[error("close failed")]
        Close,
        #[error("load failed")]
        Load,
    }

    struct MockPersister {
        calls: RefCell<Vec<Call>>,
        save_result: Result<(), TestError>,
        close_result: Result<(), TestError>,
    }

    impl Persister for MockPersister {
        type InternalStorageError = TestError;
        type SessionEvent = u8;

        fn save_event(&self, event: u8) -> Result<(), TestError> {
            self.calls.borrow_mut().push(Call::Save(event));
            self.save_result
        }

        fn load(&self) -> Result<Box<dyn Iterator<Item = u8>>, TestError> {
            panic!("unexpected load call");
        }

        fn close(&self) -> Result<(), TestError> {
            self.calls.borrow_mut().push(Call::Close);
            self.close_result
        }
    }

    #[test]
    fn a_closing_save_calls_save_then_close_once() {
        let persister = MockPersister {
            calls: RefCell::new(vec![]),
            save_result: Ok(()),
            close_result: Ok(()),
        };

        let result = PersistActions::SaveAndClose(1).execute(&persister);

        assert_eq!(result, Ok(()));
        assert_eq!(*persister.calls.borrow(), [Call::Save(1), Call::Close]);
    }

    #[test]
    fn a_save_propagates_the_storage_error() {
        let persister = MockPersister {
            calls: RefCell::new(vec![]),
            save_result: Err(TestError::Save),
            close_result: Ok(()),
        };

        let result = PersistActions::Save(1).execute(&persister);

        assert_eq!(result, Err(TestError::Save));
        assert_eq!(*persister.calls.borrow(), [Call::Save(1)]);
    }

    #[test]
    fn a_failed_save_does_not_close_the_session() {
        let persister = MockPersister {
            calls: RefCell::new(vec![]),
            save_result: Err(TestError::Save),
            close_result: Ok(()),
        };

        let result = PersistActions::SaveAndClose(1).execute(&persister);

        assert_eq!(result, Err(TestError::Save));
        assert_eq!(*persister.calls.borrow(), [Call::Save(1)]);
    }

    #[test]
    fn a_failed_close_propagates_the_error_after_saving() {
        let persister = MockPersister {
            calls: RefCell::new(vec![]),
            save_result: Ok(()),
            close_result: Err(TestError::Close),
        };

        let result = PersistActions::SaveAndClose(1).execute(&persister);

        assert_eq!(result, Err(TestError::Close));
        assert_eq!(*persister.calls.borrow(), [Call::Save(1), Call::Close]);
    }
}

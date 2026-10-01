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
    use std::cell::RefCell;
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

    #[test]
    fn events_load_in_the_order_they_were_saved() {
        let persister = InMemoryPersister::default();

        persister
            .save_event(1)
            .expect("in memory storage cannot fail");
        persister
            .save_event(2)
            .expect("in memory storage cannot fail");

        let loaded_events: Vec<u8> = persister
            .load()
            .expect("in memory storage cannot fail")
            .collect();

        assert_eq!(loaded_events, [1, 2]);
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
    fn a_save_leaves_the_session_open() {
        let persister = execute(PersistActions::Save(1));

        assert_eq!(*persister.events.borrow(), [1]);
        assert!(!*persister.closed.borrow());
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
    fn a_no_op_makes_no_storage_calls() {
        let persister = MockPersister {
            calls: RefCell::new(vec![]),
            save_result: Ok(()),
            close_result: Ok(()),
        };

        let result = PersistActions::NoOp.execute(&persister);

        assert_eq!(result, Ok(()));
        assert!(persister.calls.borrow().is_empty());
    }

    #[test]
    fn a_save_calls_save_once_without_closing() {
        let persister = MockPersister {
            calls: RefCell::new(vec![]),
            save_result: Ok(()),
            close_result: Ok(()),
        };

        let result = PersistActions::Save(1).execute(&persister);

        assert_eq!(result, Ok(()));
        assert_eq!(*persister.calls.borrow(), [Call::Save(1)]);
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

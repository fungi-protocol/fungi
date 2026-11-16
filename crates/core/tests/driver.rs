#[path = "support/scripted_driver.rs"]
mod scripted_driver;

use scripted_driver::run as run_scripted;

use fungi_core::driver::{Decision, Environment, Machine, Scheduled, Stopped, Wake};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

#[derive(Clone, Debug)]
enum Input {
    Replace(u8, Instant),
    Cancel,
}
struct State {
    next: Option<(u8, Instant)>,
    later_deadlines: VecDeque<Instant>,
    results: Vec<(u8, bool)>,
    operations: Vec<u8>,
    solved_after: Vec<usize>,
}
impl Machine for State {
    type Action = u8;
    type Operation = u8;
    type OperationId = u8;
    type Input = Input;
    type Result = bool;
    fn decide(&mut self, _: Instant) -> Decision<u8> {
        self.solved_after.push(self.results.len());
        if self.results.last().is_some_and(|(_, ok)| *ok) {
            return Decision::Done;
        }
        self.next.map_or(Decision::Wait, |(action, at)| {
            Decision::Schedule(Scheduled { action, at })
        })
    }
    fn apply(&mut self, action: u8) {
        self.next = self.later_deadlines.pop_front().map(|at| (action, at));
        if self.next.is_none() {
            self.operations.push(action);
        }
    }
    fn operations(&self) -> impl Iterator<Item = (u8, u8)> {
        self.operations.iter().map(|&op| (op, op))
    }
    fn completed(&mut self, action: u8, result: bool) {
        self.operations.retain(|&op| op != action);
        self.results.push((action, result));
        self.next = None;
    }
    fn observe(&mut self, input: Input) {
        self.next = match input {
            Input::Replace(action, at) => Some((action, at)),
            Input::Cancel => None,
        };
    }
}
struct Io {
    now: Instant,
    events: VecDeque<(Instant, Input)>,
    executed: Vec<(u8, Instant)>,
    result: bool,
    deadlines: Vec<Option<Instant>>,
}
impl Environment<State> for Io {
    fn now(&self) -> Instant {
        self.now
    }
    fn execute(&mut self, action: u8) -> impl Future<Output = bool> + use<> {
        self.executed.push((action, self.now));
        std::future::ready(self.result)
    }
    async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Input> {
        self.deadlines.push(deadline);
        if self
            .events
            .front()
            .is_some_and(|(at, _)| deadline.is_none_or(|limit| *at <= limit))
        {
            let (at, input) = self.events.pop_front().unwrap();
            self.now = at;
            Wake::Input(input)
        } else if let Some(at) = deadline {
            self.now = self.now.max(at);
            Wake::Timeout
        } else {
            Wake::Closed
        }
    }
}
fn fixture() -> (State, Io) {
    let now = Instant::now();
    (
        State {
            next: Some((1, now + Duration::from_secs(10))),
            later_deadlines: VecDeque::new(),
            results: vec![],
            operations: vec![],
            solved_after: vec![],
        },
        Io {
            now,
            events: VecDeque::new(),
            executed: vec![],
            result: true,
            deadlines: vec![],
        },
    )
}

#[tokio::test]
async fn execute_due_action_and_record_result_before_solving_again() {
    let (mut state, mut io) = fixture();
    let due = state.next.unwrap().1;
    assert_eq!(
        run_scripted(&mut state, &mut io, |op| *op).await,
        Stopped::Done
    );
    assert_eq!(io.executed, [(1, due)]);
    assert_eq!(state.results, [(1, true)]);
    assert_eq!(state.solved_after, [0, 0, 1]);
}

#[tokio::test]
async fn new_information_replaces_a_scheduled_action() {
    let (mut state, mut io) = fixture();
    let due = io.now + Duration::from_secs(20);
    io.events
        .push_back((io.now + Duration::from_secs(5), Input::Replace(2, due)));
    assert_eq!(
        run_scripted(&mut state, &mut io, |op| *op).await,
        Stopped::Done
    );
    assert_eq!(io.executed, [(2, due)]);
}

#[tokio::test]
async fn new_information_can_preserve_the_same_deadline() {
    let (mut state, mut io) = fixture();
    let due = state.next.unwrap().1;
    io.events
        .push_back((io.now + Duration::from_secs(5), Input::Replace(1, due)));

    assert_eq!(
        run_scripted(&mut state, &mut io, |op| *op).await,
        Stopped::Done
    );
    assert_eq!(io.deadlines, [Some(due), Some(due)]);
    assert_eq!(io.executed, [(1, due)]);
}

#[tokio::test]
async fn consecutive_timeouts_can_advance_virtual_time() {
    let (mut state, mut io) = fixture();
    let first = state.next.unwrap().1;
    let second = first + Duration::from_secs(5);
    state.later_deadlines.push_back(second);

    assert_eq!(
        run_scripted(&mut state, &mut io, |op| *op).await,
        Stopped::Done
    );
    assert_eq!(io.deadlines, [Some(first), Some(second)]);
    assert_eq!(io.executed, [(1, second)]);
}

#[tokio::test]
async fn input_at_the_deadline_can_cancel_before_timeout_delivery() {
    let (mut state, mut io) = fixture();
    io.events.push_back((state.next.unwrap().1, Input::Cancel));
    assert_eq!(
        run_scripted(&mut state, &mut io, |op| *op).await,
        Stopped::Closed
    );
    assert!(io.executed.is_empty());
    assert_eq!(io.deadlines.last(), Some(&None));
}

#[tokio::test]
async fn no_action_still_waits_for_information() {
    let (mut state, mut io) = fixture();
    state.next = None;
    let due = io.now + Duration::from_secs(5);
    io.events.push_back((due, Input::Replace(3, due)));
    assert_eq!(
        run_scripted(&mut state, &mut io, |op| *op).await,
        Stopped::Done
    );
    assert_eq!(io.deadlines[0], None);
    assert_eq!(io.executed, [(3, due)]);
}

#[tokio::test]
async fn a_failed_effect_is_not_a_successful_transition() {
    let (mut state, mut io) = fixture();
    io.result = false;
    assert_eq!(
        run_scripted(&mut state, &mut io, |op| *op).await,
        Stopped::Closed
    );
    assert_eq!(state.results, [(1, false)]);
    assert_eq!(state.solved_after, [0, 0, 1]);
}

#[tokio::test]
async fn observations_and_another_operation_progress_while_io_is_pending() {
    use futures::channel::oneshot;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    struct Concurrent {
        now: Instant,
        next: Option<u8>,
        operations: BTreeMap<u8, u8>,
        completed: Vec<u8>,
        trace: Arc<Mutex<Vec<String>>>,
    }
    impl Machine for Concurrent {
        type Action = u8;
        type Operation = u8;
        type OperationId = u8;
        type Input = ();
        type Result = ();
        fn decide(&mut self, _: Instant) -> Decision<u8> {
            if self.completed.len() == 2 {
                return Decision::Done;
            }
            self.next.map_or(Decision::Wait, |action| {
                Decision::Schedule(Scheduled {
                    at: self.now,
                    action,
                })
            })
        }
        fn apply(&mut self, action: u8) {
            self.trace.lock().unwrap().push(format!("apply {action}"));
            self.operations.insert(action, action);
            self.next = None;
        }
        fn operations(&self) -> impl Iterator<Item = (u8, u8)> {
            self.operations.iter().map(|(&id, &op)| (id, op))
        }
        fn completed(&mut self, id: u8, _: ()) {
            assert!(self.operations.remove(&id).is_some());
            self.trace.lock().unwrap().push(format!("complete {id}"));
            self.completed.push(id);
        }
        fn observe(&mut self, _: ()) {
            self.trace.lock().unwrap().push("observe".into());
            self.next = Some(2);
        }
    }
    struct ConcurrentIo {
        now: Instant,
        receiver: Option<oneshot::Receiver<()>>,
        sender: Option<oneshot::Sender<()>>,
        observed: bool,
        trace: Arc<Mutex<Vec<String>>>,
    }
    impl Environment<Concurrent> for ConcurrentIo {
        fn now(&self) -> Instant {
            self.now
        }
        fn execute(&mut self, action: u8) -> impl Future<Output = ()> + use<> {
            let trace = self.trace.clone();
            let receiver = if action == 1 {
                self.receiver.take()
            } else {
                None
            };
            let sender = if action == 2 {
                self.sender.take()
            } else {
                None
            };
            async move {
                trace.lock().unwrap().push(format!("execute {action}"));
                if let Some(receiver) = receiver {
                    receiver.await.unwrap();
                }
                if let Some(sender) = sender {
                    sender.send(()).unwrap();
                }
            }
        }
        async fn wait(&mut self, deadline: Option<Instant>) -> Wake<()> {
            if !self.observed
                && self
                    .trace
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| event == "execute 1")
            {
                self.observed = true;
                return Wake::Input(());
            }
            let at = deadline.expect("the next scripted wake is a scheduled action");
            self.now = self.now.max(at);
            Wake::Timeout
        }
    }
    let trace = Arc::new(Mutex::new(vec![]));
    let now = Instant::now();
    let (sender, receiver) = oneshot::channel();
    let mut state = Concurrent {
        now,
        next: Some(1),
        operations: BTreeMap::new(),
        completed: vec![],
        trace: trace.clone(),
    };
    let mut io = ConcurrentIo {
        now,
        receiver: Some(receiver),
        sender: Some(sender),
        observed: false,
        trace: trace.clone(),
    };
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            run_scripted(&mut state, &mut io, |op| *op)
        )
        .await
        .unwrap(),
        Stopped::Done
    );
    assert_eq!(
        *trace.lock().unwrap(),
        [
            "apply 1",
            "execute 1",
            "observe",
            "apply 2",
            "execute 2",
            "complete 2",
            "complete 1"
        ]
    );
    assert!(state.operations.is_empty());
}

#[tokio::test]
async fn completion_invalidates_a_choice_whose_deadline_passed_during_io() {
    use futures::channel::oneshot;
    struct Io {
        now: Instant,
        receive: Option<oneshot::Receiver<()>>,
        send: Option<oneshot::Sender<()>>,
        observed: bool,
        executed: Vec<u8>,
    }
    impl Environment<State> for Io {
        fn now(&self) -> Instant {
            self.now
        }
        fn execute(&mut self, action: u8) -> impl Future<Output = bool> + use<> {
            self.executed.push(action);
            let receive = self.receive.take().unwrap();
            async move {
                receive.await.unwrap();
                false
            }
        }
        async fn wait(&mut self, deadline: Option<Instant>) -> Wake<Input> {
            if !self.executed.is_empty() && !self.observed {
                self.observed = true;
                return Wake::Input(Input::Replace(2, self.now + Duration::from_secs(5)));
            }
            if self.observed
                && let Some(at) = deadline
            {
                self.now = at;
                self.send.take().unwrap().send(()).unwrap();
                return std::future::pending().await;
            }
            if let Some(at) = deadline {
                self.now = at;
                Wake::Timeout
            } else {
                Wake::Closed
            }
        }
    }
    let (mut state, original) = fixture();
    let (send, receive) = oneshot::channel();
    let mut io = Io {
        now: original.now,
        receive: Some(receive),
        send: Some(send),
        observed: false,
        executed: vec![],
    };
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(5),
            run_scripted(&mut state, &mut io, |op| *op)
        )
        .await
        .unwrap(),
        Stopped::Closed
    );
    assert_eq!(io.executed, [1]);
    assert_eq!(state.results, [(1, false)]);
}

#[tokio::test]
async fn closing_the_input_channel_drains_started_operations() {
    use futures::channel::oneshot;
    struct Closing {
        now: Instant,
        receive: Option<oneshot::Receiver<()>>,
        send: Option<oneshot::Sender<()>>,
    }
    impl Environment<State> for Closing {
        fn now(&self) -> Instant {
            self.now
        }
        fn execute(&mut self, _: u8) -> impl Future<Output = bool> + use<> {
            let receive = self.receive.take().unwrap();
            async move { receive.await.is_ok() }
        }
        async fn wait(&mut self, _: Option<Instant>) -> Wake<Input> {
            self.send.take().unwrap().send(()).unwrap();
            Wake::Closed
        }
    }
    let (mut state, original) = fixture();
    state.next = None;
    state.operations.push(1);
    let (send, receive) = oneshot::channel();
    let mut io = Closing {
        now: original.now,
        receive: Some(receive),
        send: Some(send),
    };
    assert_eq!(
        run_scripted(&mut state, &mut io, |op| *op).await,
        Stopped::Closed
    );
    assert_eq!(state.results, [(1, true)]);
    assert!(state.operations.is_empty());
}

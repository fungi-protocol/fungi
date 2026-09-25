//! Scoring the ways a wallet could realize what the user has asked it to do.

pub mod batch;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "nothing walks or builds the tree yet")
)]
pub(crate) mod decision_tree;
pub mod intent;
pub mod plan;
pub mod queue;
pub mod wallet;

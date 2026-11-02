//! Scoring the ways a wallet could realize what the user has asked it to do.

#[expect(dead_code, reason = "nothing turns a leaf into actions yet")]
pub(crate) mod action;
pub mod batch;
pub mod blockspace;
#[expect(dead_code, reason = "no caller in the crate selects change yet")]
pub mod change;
#[cfg_attr(not(test), expect(dead_code, reason = "nothing scores the plans yet"))]
pub(crate) mod decision_space;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "nothing walks or builds the tree yet")
)]
pub(crate) mod decision_tree;
pub mod intent;
pub mod plan;
pub mod queue;
#[expect(
    dead_code,
    reason = "no caller in the crate poses a funding request yet"
)]
pub mod selection;
pub mod wallet;

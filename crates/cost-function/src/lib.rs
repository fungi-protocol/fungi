//! Scoring the ways a wallet could realize what the user has asked it to do.

pub mod batch;
pub mod intent;
pub mod queue;
#[expect(
    dead_code,
    reason = "no caller in the crate poses a funding request yet"
)]
pub mod selection;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "nothing scores a simulated state yet")
)]
pub(crate) mod simulate;
pub mod wallet;

//! Scoring the ways a wallet could realize what the user has asked it to do.

pub mod batch;
pub mod intent;
pub mod queue;
#[expect(
    dead_code,
    reason = "no caller in the crate poses a funding request yet"
)]
pub mod selection;
pub mod wallet;

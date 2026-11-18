//! Scoring the ways a wallet could realize what the user has asked it to do.

pub(crate) mod action;
pub mod batch;
pub mod blockspace;
pub mod candidate;
pub mod change;
pub(crate) mod decision_space;
pub(crate) mod decision_tree;
pub mod intent;
pub mod plan;
pub mod queue;
pub mod selection;
pub mod wallet;

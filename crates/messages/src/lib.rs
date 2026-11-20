#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

//! Fungi protocol messages and their encoding as Lightning BOLT #1 TLV records.

pub mod fields;

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod testing;

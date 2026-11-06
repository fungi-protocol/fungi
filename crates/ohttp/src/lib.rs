//! Oblivious HTTP (RFC 9458) clients and HPKE payloads for BIP77 mailboxes.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

pub use ohttp_client::{KeyConfig, OhttpRequest, Url, parse_key_config};

pub mod hpke;
pub mod mailbox_api;

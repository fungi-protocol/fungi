//! Isolated Bitcoin Core nodes for integration tests.

pub use corepc_node;

use corepc_node::anyhow::Context;

/// Starts a regtest node using the executable specified by `BITCOIND`.
///
/// The node owns a temporary data directory and stops when dropped.
pub fn start() -> corepc_node::anyhow::Result<corepc_node::Node> {
    let executable = std::env::var_os("BITCOIND")
        .context("BITCOIND must name a bitcoind executable; use BITCOIND=$(which bitcoind)")?;
    corepc_node::Node::new(executable)
}

#[test]
fn mines_and_reads_back_a_block() -> fungi_regtest::corepc_node::anyhow::Result<()> {
    let node = fungi_regtest::start()?;
    assert_eq!(node.client.get_blockchain_info()?.blocks, 0);

    let address = node.client.new_address()?;
    let mined = node.client.generate_to_address(1, &address)?;
    let hash = mined.0[0].parse()?;
    let block = node.client.get_block(hash)?;

    assert_eq!(block.block_hash(), hash);
    assert_eq!(node.client.get_blockchain_info()?.blocks, 1);
    assert_eq!(block.txdata.len(), 1);
    assert!(block.txdata[0].is_coinbase());
    Ok(())
}

use concurrent_psbt::output::UniqueId;

pub fn change(index: u64) -> UniqueId {
    let bytes = [vec![1], index.to_be_bytes().to_vec()].concat();
    UniqueId::try_from(bytes).expect("a nine-byte change identity is valid")
}

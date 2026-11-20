//! Stable identities for transaction contributions.

use concurrent_psbt::output::UniqueId;

pub(super) fn payment(id: u64) -> UniqueId {
    output(0, id)
}

pub(super) fn change(index: usize) -> UniqueId {
    output(1, index as u64)
}

fn output(tag: u8, id: u64) -> UniqueId {
    let bytes = [vec![tag], id.to_be_bytes().to_vec()].concat();

    UniqueId::try_from(bytes).expect("a nine-byte contribution identity is valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payment_and_change_identities_do_not_collide() {
        assert_ne!(payment(258).as_bytes(), change(258).as_bytes());
        assert_eq!(payment(258).as_bytes(), &[0, 0, 0, 0, 0, 0, 0, 1, 2]);
        assert_eq!(change(258).as_bytes(), &[1, 0, 0, 0, 0, 0, 0, 1, 2]);
    }
}

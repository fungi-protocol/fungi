use proptest::prelude::*;

use super::*;

/// A well-formed label for any public key.
fn label() -> impl Strategy<Value = String> {
    any::<[u8; 32]>().prop_map(|pubkey| encode(pubkey, checksum_of(&pubkey, VERSION), VERSION))
}

fn encode(pubkey: [u8; 32], checksum: [u8; 2], version: u8) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";

    let (mut bits, mut filled) = (0u64, 0);
    let mut label = String::new();
    for byte in [&pubkey[..], &checksum, &[version]].concat() {
        bits = bits << 8 | u64::from(byte);
        filled += 8;
        while filled >= 5 {
            filled -= 5;
            label.push(char::from(ALPHABET[(bits >> filled) as usize & 31]));
        }
    }
    label
}

#[test]
fn accepts_a_published_address() {
    let host = "duckduckgogg42xjoc72x3sjasowoarfbgcmvfimaftt6twagswzczad.onion";

    assert!(Addr::new(host.to_owned(), 443).is_ok());
}

#[test]
fn label_must_be_56_characters() {
    for len in [55, 57] {
        let host = format!("{}.onion", "a".repeat(len));

        assert_eq!(Addr::new(host, 1), Err(InvalidAddr));
    }
}

proptest! {
    #[test]
    fn keeps_any_v3_host_and_port(label in label(), port: u16) {
        let host = format!("{label}.onion");

        let addr = Addr::new(host.clone(), port).unwrap();

        prop_assert_eq!(addr.host(), host);
        prop_assert_eq!(addr.port(), port);
    }

    #[test]
    fn rejects_a_wrong_checksum(pubkey: [u8; 32], checksum: [u8; 2]) {
        prop_assume!(checksum != checksum_of(&pubkey, VERSION));
        let label = encode(pubkey, checksum, VERSION);

        prop_assert_eq!(Addr::new(format!("{label}.onion"), 1), Err(InvalidAddr));
    }

    #[test]
    fn rejects_any_other_version(pubkey: [u8; 32], version: u8) {
        prop_assume!(version != VERSION);
        let label = encode(pubkey, checksum_of(&pubkey, version), version);

        prop_assert_eq!(Addr::new(format!("{label}.onion"), 1), Err(InvalidAddr));
    }

    #[test]
    fn rejects_any_other_label_length(label in "[a-z2-7]{0,112}") {
        prop_assume!(label.len() != 56);

        prop_assert_eq!(Addr::new(format!("{label}.onion"), 1), Err(InvalidAddr));
    }

    #[test]
    fn rejects_a_character_outside_base32_anywhere(
        mut label in label(),
        at in 0usize..56,
        outside in "[0189A-Z_.-]",
    ) {
        label.replace_range(at..=at, &outside);

        prop_assert_eq!(Addr::new(format!("{label}.onion"), 1), Err(InvalidAddr));
    }

    #[test]
    fn rejects_any_other_suffix(label in label(), suffix in "(\\.[a-z]{1,8})?") {
        prop_assume!(suffix != ".onion");

        prop_assert_eq!(Addr::new(format!("{label}{suffix}"), 1), Err(InvalidAddr));
    }
}

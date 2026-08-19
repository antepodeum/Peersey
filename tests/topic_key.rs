use peersey::TopicKey;

#[test]
fn display_is_fixed_width_lower_hex() {
    let key = TopicKey::from_bytes([0x0f; 32]);
    let text = key.to_string();
    assert_eq!(text.len(), 64);
    assert_eq!(text, "0f".repeat(32));
}

#[test]
fn uppercase_hex_is_accepted_and_normalized() {
    let upper = "AB".repeat(32);
    let key: TopicKey = upper.parse().unwrap();
    assert_eq!(key.to_string(), "ab".repeat(32));
}

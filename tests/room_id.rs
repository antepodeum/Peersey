use peersey::{RoomId, RoomIdParseError};

#[test]
fn named_public_room_is_stable() {
    let id: RoomId = "community/rust:general".parse().unwrap();
    assert_eq!(id.to_string(), "community/rust:general");
}

#[test]
fn random_public_rooms_are_distinct() {
    assert_ne!(RoomId::random(), RoomId::random());
}

#[test]
fn public_room_id_rejects_spaces() {
    assert!(matches!(
        "general chat".parse::<RoomId>().unwrap_err(),
        RoomIdParseError::InvalidCharacter { .. }
    ));
}

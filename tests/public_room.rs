use peersey::{Error, Peersey};

#[tokio::test]
async fn empty_public_room_name_fails_before_networking() {
    let node = Peersey::new();
    let result = node.join_public_room("").await;
    assert!(matches!(result, Err(Error::InvalidRoomName)));
}

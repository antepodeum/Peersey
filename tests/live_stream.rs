use std::time::Duration;

use peersey::{LiveLink, MediaKind, Peersey};

#[tokio::test]
async fn streams_realtime_packets_between_nodes() {
    let publisher = Peersey::new();
    let viewer = Peersey::new();
    let (stream, link) = publisher.create_live_stream().await.unwrap();

    let encoded = link.to_string();
    assert_eq!(encoded.parse::<LiveLink>().unwrap(), link);
    assert_eq!(format!("{link:?}"), "LiveLink([REDACTED])");

    let mut receiver = tokio::time::timeout(Duration::from_secs(20), viewer.watch_live(&link))
        .await
        .expect("viewer timed out")
        .unwrap();
    stream.send_video("encoded frame").unwrap();

    let packet = tokio::time::timeout(Duration::from_secs(10), receiver.recv())
        .await
        .expect("packet timed out")
        .unwrap()
        .expect("publisher closed early");
    assert_eq!(packet.kind, MediaKind::Video);
    assert_eq!(packet.content, "encoded frame");

    drop(stream);
    assert!(
        tokio::time::timeout(Duration::from_secs(10), receiver.recv())
            .await
            .expect("publisher close timed out")
            .unwrap()
            .is_none()
    );

    viewer.shutdown().await.unwrap();
    publisher.shutdown().await.unwrap();
}

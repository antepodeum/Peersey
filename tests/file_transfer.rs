use peersey::Peersey;

#[tokio::test]
async fn shares_and_fetches_a_file() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source.txt");
    let destination_path = directory.path().join("destination.txt");
    let expected = b"content-addressed hello";
    std::fs::write(&source_path, expected).unwrap();

    let provider = Peersey::start().await.unwrap();
    let receiver = Peersey::start().await.unwrap();
    let link = provider.share_file(&source_path).await.unwrap();

    let size = receiver.fetch_file(&link, &destination_path).await.unwrap();

    assert_eq!(size, expected.len() as u64);
    assert_eq!(std::fs::read(destination_path).unwrap(), expected);

    receiver.shutdown().await.unwrap();
    provider.shutdown().await.unwrap();
}

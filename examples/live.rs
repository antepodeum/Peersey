use anyhow::{Context, Result};
use peersey::{MediaKind, Peersey};

#[tokio::main]
async fn main() -> Result<()> {
    let publisher = Peersey::new();
    let viewer = Peersey::new();
    let (stream, link) = publisher
        .create_live_stream()
        .await
        .context("create live stream")?;

    println!("live link: {link}");
    let mut receiver = viewer.watch_live(&link).await.context("watch live")?;
    stream
        .send_video("one encoded video frame")
        .context("send video")?;

    let packet = receiver
        .recv()
        .await
        .context("receive live packet")?
        .context("publisher closed")?;
    assert_eq!(packet.kind, MediaKind::Video);
    println!(
        "received {:?} packet: {} bytes",
        packet.kind,
        packet.content.len()
    );

    drop(stream);
    receiver.shutdown();
    viewer.shutdown().await.context("shutdown viewer")?;
    publisher.shutdown().await.context("shutdown publisher")?;
    Ok(())
}

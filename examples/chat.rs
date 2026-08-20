use anyhow::{Context, Result};
use clap::Parser;
use peersey::{Peersey, RoomEvent, RoomKey};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug, Parser)]
#[command(name = "peersey-chat")]
#[command(about = "Zero-config P2P chat over Peersey")]
struct Args {
    /// Your display name.
    #[arg(short, long)]
    name: String,

    /// Secret room key. Omit it to create a new private room.
    #[arg(short, long)]
    room: Option<RoomKey>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChatMessage {
    name: String,
    text: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let node = Peersey::start().await.context("start Peersey")?;
    let (room, room_key) = match args.room {
        Some(key) => (node.join_room(key).await.context("join room")?, key),
        None => node.create_room().await.context("create room")?,
    };

    println!("Peersey private chat");
    println!("room key: {room_key}");
    if args.room.is_none() {
        println!("share that secret key with invited peers");
    }

    println!("peer: {}", room.peer_id());
    println!("ready; type a message and press Enter");

    let mut events = room.subscribe();
    let stdin = BufReader::new(tokio::io::stdin());
    let mut lines = stdin.lines();

    loop {
        tokio::select! {
            line = lines.next_line() => {
                let Some(text) = line.context("read stdin")? else {
                    break;
                };
                if text.trim().is_empty() {
                    continue;
                }

                let message = ChatMessage {
                    name: args.name.clone(),
                    text: text.clone(),
                };
                let payload = postcard::to_stdvec(&message).context("encode chat message")?;
                room.send(payload).await.context("send chat message")?;
                println!("{}: {}", args.name, text);
            }
            event = events.recv() => {
                match event {
                    Ok(RoomEvent::Message { content }) => {
                        match postcard::from_bytes::<ChatMessage>(&content) {
                            Ok(message) => println!("{}: {}", message.name, message.text),
                            Err(error) => eprintln!("ignored invalid chat message: {error}"),
                        }
                    }
                    Ok(RoomEvent::PeerJoined { peer }) => println!("[peer joined: {peer}]"),
                    Ok(RoomEvent::PeerLeft { peer }) => println!("[peer left: {peer}]"),
                    Ok(RoomEvent::Lagged) => {
                        eprintln!("[iroh-gossip lagged; some gossip events were skipped]");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("[lagged; skipped {n} events]");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }

    room.shutdown().await;
    node.shutdown().await.context("shutdown Peersey")?;
    Ok(())
}

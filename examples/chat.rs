use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use peersey::{Event, Peersey, TopicKey};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};

#[derive(Debug, Parser)]
#[command(name = "peersey-chat")]
#[command(about = "Zero-config P2P chat over Peersey")]
struct Args {
    /// Your display name.
    #[arg(short, long)]
    name: String,

    /// 64-hex-character topic key. Omit it to create a new room.
    #[arg(short, long)]
    topic: Option<TopicKey>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChatMessage {
    name: String,
    text: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let topic = args.topic.unwrap_or_else(TopicKey::random);

    println!("Peersey chat");
    println!("topic: {topic}");
    if args.topic.is_none() {
        println!("share only that topic key with the other peer");
    }
    println!("discovering peers through the Mainline DHT...");

    let room = Peersey::builder(topic)
        .namespace("peersey-chat/v1")
        .wait_for_first_peer(Duration::from_secs(3))
        .join()
        .await
        .context("join Peersey topic")?;

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
                room.publish(payload).await.context("publish chat message")?;
                println!("{}: {}", args.name, text);
            }
            event = events.recv() => {
                match event {
                    Ok(Event::Message { content }) => {
                        match postcard::from_bytes::<ChatMessage>(&content) {
                            Ok(message) => println!("{}: {}", message.name, message.text),
                            Err(error) => eprintln!("ignored invalid chat message: {error}"),
                        }
                    }
                    Ok(Event::PeerUp { peer }) => println!("[peer joined: {peer}]"),
                    Ok(Event::PeerDown { peer }) => println!("[peer left: {peer}]"),
                    Ok(Event::Lagged) => {
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
    Ok(())
}

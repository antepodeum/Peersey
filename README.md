# Peersey

Batteries-included P2P messaging and content sharing for Rust.

Peersey hides endpoint addresses, relays, hole punching, protocol routing,
Mainline DHT records, blob stores, transfer verification, and live connection
authentication behind a small API.

```text
Peersey
├── public rooms: name -> DHT discovery -> iroh-gossip
├── private rooms: RoomKey -> secret-key rendezvous -> iroh-gossip
├── content-addressed files
│   └── ShareLink -> iroh-blobs -> verified download
└── live streams
    └── LiveLink -> independent QUIC streams -> audio/video/data packets
```

## Install

```toml
[dependencies]
peersey = "0.2"
tokio = { version = "1", features = ["full"] }
```

## Public pub/sub

Join a named open room. No secret or peer address required:

```rust
use peersey::Peersey;

async fn example() -> Result<(), Box<dyn std::error::Error>> {
  let node = Peersey::new();
  let room = node.join_public_room("community/rust:general").await?;
  room.send("hello everyone").await?;
  Ok(())
}
```

Create an unlisted open room with a random public name:

```rust
async fn example(node: &peersey::Peersey) -> Result<(), peersey::Error> {
  let (room, name) = node.create_public_room().await?;
  println!("public room: {name}");
  Ok(())
}
```

Anyone knowing the room name can join. Peersey does not yet provide a global
room directory, so “public” means open membership, not globally searchable.
Leading and trailing whitespace in names is ignored.

## Private pub/sub

Create a room:

```rust
use peersey::{Peersey, RoomEvent};

async fn example() -> Result<(), peersey::Error> {
  let node = Peersey::new();
  let (room, key) = node.create_private_room().await?;
  println!("invite key: {key}");

  let mut events = room.subscribe();
  room.send("hello").await?;

  while let Some(event) = events.recv().await {
      if let RoomEvent::Message { content } = event {
          println!("{}", String::from_utf8_lossy(&content));
      }
  }

  Ok(())
}
```

Join from another process or machine:

```rust
use peersey::{Peersey, RoomKey};

async fn example(invite: &str) -> Result<(), Box<dyn std::error::Error>> {
  let node = Peersey::new();
  let key: RoomKey = invite.parse()?;
  let room = node.join_private_room(key).await?;
  room.send("joined").await?;
  Ok(())
}
```

No public `namespace` exists. Peersey keeps protocol domain separation fixed
internally, so users cannot accidentally create incompatible rooms.

## Discovery model

Both public and private rooms use the public BitTorrent Mainline DHT. Peersey
does not run a separate private DHT.

- Public rooms deterministically derive rendezvous coordinates from their
  public name.
- Private rooms derive their gossip topic, DHT slot keys, and DHT record
  encryption keys from `RoomKey` plus Peersey's fixed internal protocol salt.
- Peers with the same `RoomKey` derive the same coordinates and decrypt the
  peer records. Other DHT participants only see encrypted rendezvous records.

This protects rendezvous contents from passive DHT observers. It does not
provide anonymity, hide all network activity, or protect a room after its
`RoomKey` leaks.

## Host a file

```rust
use peersey::Peersey;

async fn example() -> Result<(), peersey::Error> {
  let node = Peersey::persistent("./peersey-data");
  let link = node.share_file("./video.mp4").await?;
  println!("{link}");

  // Keep the node alive while the file should remain available.
  tokio::signal::ctrl_c().await?;
  node.shutdown().await?;

  Ok(())
}
```

Fetch it:

```rust
use peersey::{Peersey, ShareLink};

async fn example(text: &str) -> Result<(), Box<dyn std::error::Error>> {
  let node = Peersey::new();
  let link: ShareLink = text.parse()?;
  let bytes = node.fetch_file(&link, "./video.mp4").await?;
  println!("downloaded {bytes} bytes; id={}", link.content_id());
  node.shutdown().await?;

  Ok(())
}
```

Files are BLAKE3 content-addressed and verified while streaming. File storage
and networking start only when first used. `new()` uses an automatically
deleted temporary disk store. `persistent(path)` preserves content and provider
identity across restarts, keeping links valid when the node comes back online.

## Live audio, video, and realtime data

Create a live stream and share its capability link:

```rust
use peersey::Peersey;

async fn example() -> Result<(), peersey::Error> {
  let node = Peersey::new();
  let (stream, link) = node.create_live_stream().await?;
  println!("watch at: {link}");

  // Pass complete encoded frames or chunks from your media pipeline.
  stream.send_video("encoded H.264 frame")?;
  stream.send_audio("encoded Opus packet")?;
  stream.send_data("cursor position")?;

  Ok(())
}
```

Watch from another process or machine:

```rust
use peersey::{MediaKind, Peersey};

async fn example(text: &str) -> Result<(), Box<dyn std::error::Error>> {
  let node = Peersey::new();
  let link = text.parse()?;
  let mut live = node.watch_live(&link).await?;

  while let Some(packet) = live.recv().await? {
      match packet.kind {
          MediaKind::Video => { /* decode and render packet.content */ }
          MediaKind::Audio => { /* decode and play packet.content */ }
          MediaKind::Data => { /* handle realtime application data */ }
      }
  }

  Ok(())
}
```

Packets use separate QUIC streams, so delayed video does not block audio or
realtime data. Packets may arrive out of order; timestamps allow playback
synchronization. Streams retain no history, and slow viewers skip old packets
instead of accumulating unlimited latency. Each packet is limited to 8 MiB.

Peersey currently transports media but does not capture cameras, choose
codecs, decode, render, provide a jitter buffer, or adapt bitrate. This keeps
the core portable while `iroh-live` remains an early preview incompatible with
the Iroh version required by DHT rendezvous.

## Security model

- Public room names are safe to log. Anyone knowing a name can join.
- `RoomKey` is a secret capability. Anyone with it can discover and join the
  room. Its `Debug` output is redacted.
- Room traffic uses Iroh's authenticated encrypted QUIC connections.
- `ShareLink` is not secret or access-controlled. Anyone holding it can request
  the content while its provider is online.
- `LiveLink` is a secret capability. Anyone holding it can watch the live
  stream while its publisher is online. Its `Debug` output is redacted.
- Peersey 0.2 does not yet provide member roles, revocation, key rotation, or
  content encryption.

## Scope

Peersey 0.2 intentionally exposes only high-level handles:

```text
Peersey   Room   RoomKey   RoomEvent   ShareLink
LiveStream   LiveLink   LiveReceiver   LivePacket   MediaKind
```

Current share links name one provider. Multi-provider discovery, automatic
rehosting, directory manifests, and managed room membership can be added later
without exposing lower-level Iroh configuration.

## Chat example

Create a private room. First argument is your display name:

```bash
cargo run --example chat -- alice
```

The chat prints a private invite key. Give it only to people you want in the
room. They join by passing their name and that key:

```bash
cargo run --example chat -- bob <private-invite-key>
```

The full-screen terminal UI updates connection state, participants, presence,
messages, and the composer in place. Wide terminals include a room sidebar;
smaller terminals use a compact chat layout. Wait for `CONNECTED` before
sending because messages are not stored for offline peers.

- `Enter`: send
- `F1`: help and command reference
- `F2`: show the private invite
- `PageUp` / `PageDown`: browse chat history
- `Ctrl+L`: clear the local chat view
- `Ctrl+C`: leave and restore the terminal

The `/room`, `/clear`, and `/quit` commands remain available. Prefix a message
with `//` to send text beginning with a literal slash.

## License

MIT OR Apache-2.0

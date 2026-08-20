# Peersey

Batteries-included P2P messaging and content sharing for Rust.

Peersey hides endpoint addresses, relays, hole punching, protocol routing,
Mainline DHT records, blob stores, and transfer verification behind a small API.

```text
Peersey
├── public rooms: RoomId -> DHT discovery -> iroh-gossip
├── private rooms: RoomKey -> private DHT rendezvous -> iroh-gossip
└── content-addressed files
    └── ShareLink -> iroh-blobs -> verified download
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

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let node = Peersey::start().await?;
let room = node.join_public_room("community/rust:general").await?;
room.send("hello everyone").await?;
# Ok(())
# }
```

Create an unlisted open room with a random public ID:

```rust
# async fn example(node: &peersey::Peersey) -> Result<(), peersey::Error> {
let (room, id) = node.create_public_room().await?;
println!("public room: {id}");
# Ok(())
# }
```

Anyone knowing `RoomId` can join. Peersey does not yet provide a global room
directory, so “public” means open membership, not globally searchable.

## Private pub/sub

Create a room:

```rust
use peersey::{Peersey, RoomEvent};

# async fn example() -> Result<(), peersey::Error> {
let node = Peersey::start().await?;
let (room, key) = node.create_private_room().await?;
println!("invite key: {key}");

let mut events = room.subscribe();
room.send("hello").await?;

while let Ok(event) = events.recv().await {
    if let RoomEvent::Message { content } = event {
        println!("{}", String::from_utf8_lossy(&content));
    }
}
# Ok(())
# }
```

Join from another process or machine:

```rust
use peersey::{Peersey, RoomKey};

# async fn example(invite: &str) -> Result<(), Box<dyn std::error::Error>> {
let node = Peersey::start().await?;
let key: RoomKey = invite.parse()?;
let room = node.join_private_room(key).await?;
room.send("joined").await?;
# Ok(())
# }
```

No public `namespace` exists. Peersey keeps protocol domain separation fixed
internally, so users cannot accidentally create incompatible rooms.

## Host a file

```rust
use peersey::Peersey;

# async fn example() -> Result<(), peersey::Error> {
let node = Peersey::persistent("./peersey-data").await?;
let link = node.share_file("./video.mp4").await?;
println!("{link}");

// Keep the node alive while the file should remain available.
tokio::signal::ctrl_c().await?;
node.shutdown().await?;
# Ok(())
# }
```

Fetch it:

```rust
use peersey::{Peersey, ShareLink};

# async fn example(text: &str) -> Result<(), Box<dyn std::error::Error>> {
let node = Peersey::start().await?;
let link: ShareLink = text.parse()?;
let bytes = node.fetch_file(&link, "./video.mp4").await?;
println!("downloaded {bytes} bytes; id={}", link.content_id());
node.shutdown().await?;
# Ok(())
# }
```

Files are BLAKE3 content-addressed and verified while streaming. `start()` uses
an automatically deleted temporary disk store. `persistent(path)` preserves
content and provider identity across restarts, keeping previously issued links
valid when the node comes back online.

## Security model

- `RoomId` is public and safe to log. Anyone knowing it can join.
- `RoomKey` is a secret capability. Anyone with it can discover and join the
  room. Its `Debug` output is redacted.
- Room traffic uses Iroh's authenticated encrypted QUIC connections.
- `ShareLink` is not secret or access-controlled. Anyone holding it can request
  the content while its provider is online.
- Peersey 0.2 does not yet provide member roles, revocation, key rotation, or
  content encryption.

## Scope

Peersey 0.2 intentionally exposes only:

```text
Peersey   Room   RoomId   RoomKey   RoomEvent   ShareLink
```

Current share links name one provider. Multi-provider discovery, automatic
rehosting, directory manifests, and managed room membership can be added later
without exposing lower-level Iroh configuration.

## Chat example

```bash
cargo run --example chat -- --name alice
cargo run --example chat -- --name bob --room <secret-room-key>
```

## License

MIT OR Apache-2.0

# Peersey

Zero-config peer discovery for [`iroh-gossip`](https://github.com/n0-computer/iroh-gossip).

Peersey removes the bootstrap-peer argument from the application model. Peers that know the same random 256-bit `TopicKey` discover each other through the BitTorrent Mainline DHT and then communicate over `iroh-gossip`.

```text
TopicKey
   │
   ├─ Mainline DHT rendezvous
   │       ↓
   │   discovered peers
   │       ↓
   └─ iroh-gossip swarm
           ↓
      publish / subscribe
```

There are no IP addresses, endpoint IDs, trackers, tickets, or bootstrap peer lists in the application configuration.

## Status

Peersey 0.1 is intentionally small. It is an ergonomic layer over `iroh-gossip-rendezvous`, which already implements the difficult DHT rendezvous/healing protocol. Peersey does not reimplement Mainline DHT, gossip, durable queues, QoS, or application authorization.

A `TopicKey` is a capability: anyone who knows it can discover and join that swarm. If an application needs authorization separate from discovery, authenticate application messages or connections separately.

## Usage

```toml
[dependencies]
peersey = "0.1"
bytes = "1"
```

```rust
use bytes::Bytes;
use peersey::{Event, Peersey, TopicKey};

# async fn example() -> Result<(), peersey::Error> {
let topic = TopicKey::random();
println!("share this once: {topic}");

let room = Peersey::join(topic).await?;
let mut events = room.subscribe();

room.publish(Bytes::from_static(b"hello")).await?;

while let Ok(event) = events.recv().await {
    if let Event::Message { content } = event {
        println!("{}", String::from_utf8_lossy(&content));
    }
}
# Ok(())
# }
```

On another machine, parse the same key and join it:

```rust
use peersey::{Peersey, TopicKey};

# async fn example(text: &str) -> Result<(), Box<dyn std::error::Error>> {
let topic: TopicKey = text.parse()?;
let room = Peersey::join(topic).await?;
# drop(room);
# Ok(())
# }
```

## Chat example

Create a room:

```bash
cargo run --example chat -- --name alice
```

It prints a topic key such as:

```text
4dcb...<64 hex characters total>...91ae
```

On another machine, the only shared network-specific value is that key:

```bash
cargo run --example chat -- --name bob --topic 4dcb...91ae
```

Neither side supplies the other's IP address, iroh endpoint ID, or any bootstrap peer.

## API

The default path is deliberately short:

```rust
let room = Peersey::join(topic_key).await?;
let mut events = room.subscribe();
room.publish(bytes).await?;
```

For application isolation or startup behavior:

```rust
use std::time::Duration;
use peersey::Peersey;

# async fn example(topic_key: peersey::TopicKey) -> Result<(), peersey::Error> {
let room = Peersey::builder(topic_key)
    .namespace("my-app/v1")
    .wait_for_first_peer(Duration::from_secs(2))
    .join()
    .await?;
# drop(room);
# Ok(())
# }
```

`namespace` is a compile-time/application convention, not a peer address or bootstrap configuration. Peers must use the same namespace and topic key.

## Semantics

- `TopicKey` is 32 random bytes, encoded as 64 lowercase hex characters.
- The key is used as the rendezvous capability; Peersey does not publish it directly as plaintext DHT metadata.
- The underlying rendezvous derives its own `iroh-gossip::TopicId` from the topic key and namespace.
- Discovery is continuous. DHT maintenance stays active while the `Peersey` handle is alive.
- Gossip messages are ephemeral broadcast messages. Peersey is not a durable broker.
- In 0.1, one `Peersey` handle represents one topic. Multiple topics can be joined with multiple handles. Sharing a single iroh endpoint across many independently discovered topics is intentionally left out of the minimal API for now.

## Why not implement the DHT layer here?

The hard part is not calling `iroh-gossip::subscribe`; it is safely maintaining a many-peer rendezvous set inside BEP 44's mutable-value constraints while peers concurrently arrive, disappear, and overwrite DHT state. `iroh-gossip-rendezvous` already provides sharded slots, logical aging, vouching, healing, and encrypted DHT records. Peersey keeps that protocol below the public API instead of duplicating it.

## License

MIT OR Apache-2.0


## Compatibility note

Peersey exposes `Event::Lagged` because `iroh-gossip` can report that its own
event stream fell behind. This is distinct from Tokio broadcast receiver lag,
which is returned as `RecvError::Lagged(n)` by `Subscription::recv`.

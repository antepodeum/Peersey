//! Peersey: zero-config discovery for `iroh-gossip` topics.
//!
//! Peersey turns a single 256-bit [`TopicKey`] into a live gossip swarm. Peers
//! sharing the same key discover each other through the BitTorrent Mainline DHT;
//! applications do not need to exchange endpoint IDs, IP addresses, tickets, or
//! bootstrap peer lists.
//!
//! ```no_run
//! # async fn run() -> Result<(), peersey::Error> {
//! use bytes::Bytes;
//! use peersey::{Event, Peersey, TopicKey};
//!
//! let key = TopicKey::random();
//! println!("share this key: {key}");
//!
//! let room = Peersey::join(key).await?;
//! let mut events = room.subscribe();
//!
//! room.publish(Bytes::from_static(b"hello")).await?;
//!
//! if let Ok(Event::Message { content }) = events.recv().await {
//!     println!("{}", String::from_utf8_lossy(&content));
//! }
//! # Ok(())
//! # }
//! ```

use std::{fmt, str::FromStr, time::Duration};

use bytes::Bytes;
use iroh::PublicKey;
use iroh_gossip::{api, TopicId};
use iroh_gossip_rendezvous::{Rendezvous, RendezvousState};
use thiserror::Error as ThisError;
use tokio::sync::broadcast;

const DEFAULT_NAMESPACE: &str = "peersey/v1";

/// A 256-bit capability used to locate a Peersey swarm.
///
/// The key is both the human-facing topic identifier and the rendezvous secret.
/// Generate it with [`TopicKey::random`] for an unguessable private swarm, then
/// share it out of band with the peers that should be able to discover it.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct TopicKey([u8; 32]);

impl TopicKey {
    /// Generate a cryptographically random topic key.
    #[must_use]
    pub fn random() -> Self {
        Self(rand::random())
    }

    /// Construct a topic key from its raw 32 bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the raw 32-byte topic key.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Borrow the raw topic key bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for TopicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TopicKey").field(&self.to_string()).finish()
    }
}

impl fmt::Display for TopicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl FromStr for TopicKey {
    type Err = TopicKeyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(value).map_err(TopicKeyParseError::InvalidHex)?;
        let bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|bytes: Vec<u8>| TopicKeyParseError::InvalidLength(bytes.len()))?;
        Ok(Self(bytes))
    }
}

/// Error returned while parsing a textual [`TopicKey`].
#[derive(Debug, ThisError)]
pub enum TopicKeyParseError {
    /// The input was not valid hexadecimal.
    #[error("topic key is not valid hexadecimal: {0}")]
    InvalidHex(hex::FromHexError),
    /// The decoded key was not exactly 32 bytes.
    #[error("topic key must be exactly 32 bytes, got {0}")]
    InvalidLength(usize),
}

/// Events observed on a Peersey topic.
#[derive(Debug, Clone)]
pub enum Event {
    /// A peer became a direct gossip neighbor.
    PeerUp { peer: PublicKey },
    /// A direct gossip neighbor disappeared.
    PeerDown { peer: PublicKey },
    /// A broadcast message arrived from the gossip swarm.
    Message { content: Bytes },
    /// The underlying gossip event stream fell behind and skipped events.
    ///
    /// Unlike [`tokio::sync::broadcast::error::RecvError::Lagged`], iroh-gossip
    /// does not expose the number of skipped events for this condition.
    Lagged,
}

/// A subscription to topic events.
pub struct Subscription {
    inner: broadcast::Receiver<api::Event>,
}

impl Subscription {
    /// Receive the next topic event.
    pub async fn recv(&mut self) -> Result<Event, broadcast::error::RecvError> {
        let event = self.inner.recv().await?;
        Ok(match event {
            api::Event::NeighborUp(peer) => Event::PeerUp { peer },
            api::Event::NeighborDown(peer) => Event::PeerDown { peer },
            api::Event::Received(message) => Event::Message {
                content: message.content,
            },
            api::Event::Lagged => Event::Lagged,
        })
    }
}

/// A live zero-config iroh-gossip topic.
///
/// Keeping this value alive keeps the DHT maintenance and gossip tasks alive.
pub struct Peersey {
    topic_key: TopicKey,
    rendezvous: Rendezvous,
}

impl Peersey {
    /// Join a topic using the default Peersey namespace.
    ///
    /// This is the zero-configuration entry point: every peer that calls this
    /// with the same [`TopicKey`] can discover the others without a peer list.
    pub async fn join(topic_key: TopicKey) -> Result<Self, Error> {
        Self::builder(topic_key).join().await
    }

    /// Configure an advanced join while keeping the same TopicKey-only
    /// discovery model.
    #[must_use]
    pub fn builder(topic_key: TopicKey) -> Builder {
        Builder::new(topic_key)
    }

    /// Broadcast bytes to the topic.
    pub async fn publish(&self, content: impl Into<Bytes>) -> Result<(), Error> {
        self.rendezvous.broadcast(content.into()).await?;
        Ok(())
    }

    /// Create an independent event subscriber.
    #[must_use]
    pub fn subscribe(&self) -> Subscription {
        Subscription {
            inner: self.rendezvous.subscribe(),
        }
    }

    /// The shared capability used to join this topic.
    #[must_use]
    pub const fn topic_key(&self) -> TopicKey {
        self.topic_key
    }

    /// The actual topic ID derived for the underlying iroh-gossip swarm.
    #[must_use]
    pub fn gossip_topic_id(&self) -> TopicId {
        self.rendezvous.topic_id()
    }

    /// This node's iroh endpoint identity.
    #[must_use]
    pub fn peer_id(&self) -> PublicKey {
        self.rendezvous.node_id()
    }

    /// Snapshot of the underlying discovery state.
    #[must_use]
    pub fn state(&self) -> RendezvousState {
        self.rendezvous.state()
    }

    /// Gracefully stop DHT maintenance, gossip, and the underlying endpoint.
    pub async fn shutdown(&self) {
        self.rendezvous.shutdown().await;
    }
}

/// Advanced Peersey topic builder.
///
/// Normal applications should prefer [`Peersey::join`].
pub struct Builder {
    topic_key: TopicKey,
    namespace: String,
    wait_for_first_peer: Option<Duration>,
}

impl Builder {
    fn new(topic_key: TopicKey) -> Self {
        Self {
            topic_key,
            namespace: DEFAULT_NAMESPACE.to_owned(),
            wait_for_first_peer: None,
        }
    }

    /// Isolate this swarm from other applications using the same TopicKey.
    ///
    /// This is an application-level constant, not a bootstrap address or a
    /// runtime network setting. All peers in the same swarm must use the same
    /// namespace.
    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Optionally wait for an initial peer during join.
    ///
    /// A timeout is not an error: the caller may simply be the first peer.
    #[must_use]
    pub fn wait_for_first_peer(mut self, timeout: Duration) -> Self {
        self.wait_for_first_peer = Some(timeout);
        self
    }

    /// Join the topic and start continuous rendezvous maintenance.
    pub async fn join(self) -> Result<Peersey, Error> {
        if self.namespace.is_empty() {
            return Err(Error::EmptyNamespace);
        }

        let passphrase = self.topic_key.to_string();
        let rendezvous = Rendezvous::builder()
            .passphrase(&passphrase)
            .app_salt(&self.namespace)
            .wait_for_first_neighbor(self.wait_for_first_peer)
            .build()
            .await?;

        Ok(Peersey {
            topic_key: self.topic_key,
            rendezvous,
        })
    }
}

/// Errors produced by Peersey setup or publishing.
#[derive(Debug, ThisError)]
pub enum Error {
    /// Namespace must not be empty.
    #[error("namespace must not be empty")]
    EmptyNamespace,
    /// The underlying iroh/Mainline-DHT rendezvous failed.
    #[error(transparent)]
    Rendezvous(#[from] iroh_gossip_rendezvous::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_key_round_trips_through_hex() {
        let key = TopicKey::from_bytes([0xab; 32]);
        let encoded = key.to_string();
        assert_eq!(encoded.len(), 64);
        assert_eq!(encoded.parse::<TopicKey>().unwrap(), key);
    }

    #[test]
    fn topic_key_parser_rejects_wrong_size() {
        let error = "abcd".parse::<TopicKey>().unwrap_err();
        assert!(matches!(error, TopicKeyParseError::InvalidLength(2)));
    }

    #[test]
    fn random_topic_keys_are_not_constant() {
        assert_ne!(TopicKey::random(), TopicKey::random());
    }
}

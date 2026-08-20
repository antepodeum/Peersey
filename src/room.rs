use std::{fmt, str::FromStr};

use bytes::Bytes;
use iroh::PublicKey;
use iroh_gossip::api;
use iroh_gossip_rendezvous::Rendezvous;
use thiserror::Error as ThisError;
use tokio::sync::broadcast;

use crate::Error;

const ROOM_PROTOCOL: &str = "peersey/private-room/v1";

/// Secret 256-bit capability required to discover and join a private room.
///
/// Treat this value like a password. Anyone who has it can join the room.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RoomKey([u8; 32]);

impl RoomKey {
    /// Generate a cryptographically random room key.
    #[must_use]
    pub fn random() -> Self {
        Self(rand::random())
    }

    /// Construct a key from its raw bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the raw key bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl fmt::Debug for RoomKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RoomKey([REDACTED])")
    }
}

impl fmt::Display for RoomKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl FromStr for RoomKey {
    type Err = RoomKeyParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(value).map_err(RoomKeyParseError::InvalidHex)?;
        let bytes = bytes
            .try_into()
            .map_err(|bytes: Vec<u8>| RoomKeyParseError::InvalidLength(bytes.len()))?;
        Ok(Self(bytes))
    }
}

/// Error returned when parsing a room key.
#[derive(Debug, ThisError)]
pub enum RoomKeyParseError {
    /// Input was not hexadecimal.
    #[error("room key is not valid hexadecimal: {0}")]
    InvalidHex(hex::FromHexError),
    /// Decoded key was not 32 bytes.
    #[error("room key must be exactly 32 bytes, got {0}")]
    InvalidLength(usize),
}

/// Event received from a private room.
#[derive(Debug, Clone)]
pub enum RoomEvent {
    /// A direct gossip neighbor connected.
    PeerJoined { peer: PublicKey },
    /// A direct gossip neighbor disconnected.
    PeerLeft { peer: PublicKey },
    /// A message arrived.
    Message { content: Bytes },
    /// Underlying gossip stream skipped events.
    Lagged,
}

/// Receiver for room events.
pub struct Subscription {
    inner: broadcast::Receiver<api::Event>,
}

impl Subscription {
    /// Receive the next event.
    pub async fn recv(&mut self) -> Result<RoomEvent, broadcast::error::RecvError> {
        Ok(match self.inner.recv().await? {
            api::Event::NeighborUp(peer) => RoomEvent::PeerJoined { peer },
            api::Event::NeighborDown(peer) => RoomEvent::PeerLeft { peer },
            api::Event::Received(message) => RoomEvent::Message {
                content: message.content,
            },
            api::Event::Lagged => RoomEvent::Lagged,
        })
    }
}

/// A live private pub/sub room.
pub struct Room {
    key: RoomKey,
    rendezvous: Rendezvous,
}

impl Room {
    pub(crate) async fn join(key: RoomKey) -> Result<Self, Error> {
        let rendezvous = Rendezvous::builder()
            .passphrase(&key.to_string())
            .app_salt(ROOM_PROTOCOL)
            .build()
            .await?;
        Ok(Self { key, rendezvous })
    }

    /// Broadcast a message to current room members.
    pub async fn send(&self, content: impl Into<Bytes>) -> Result<(), Error> {
        self.rendezvous.broadcast(content.into()).await?;
        Ok(())
    }

    /// Subscribe to messages and peer presence events.
    #[must_use]
    pub fn subscribe(&self) -> Subscription {
        Subscription {
            inner: self.rendezvous.subscribe(),
        }
    }

    /// Secret capability for this room.
    #[must_use]
    pub const fn key(&self) -> RoomKey {
        self.key
    }

    /// This room connection's peer identity.
    #[must_use]
    pub fn peer_id(&self) -> PublicKey {
        self.rendezvous.node_id()
    }

    /// Leave the room and stop its discovery tasks.
    pub async fn shutdown(&self) {
        self.rendezvous.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_round_trips() {
        let key = RoomKey::from_bytes([0xab; 32]);
        assert_eq!(key.to_string().parse::<RoomKey>().unwrap(), key);
    }

    #[test]
    fn debug_redacts_secret() {
        let key = RoomKey::from_bytes([0xab; 32]);
        assert_eq!(format!("{key:?}"), "RoomKey([REDACTED])");
        assert!(!format!("{key:?}").contains("ab"));
    }

    #[test]
    fn parser_rejects_wrong_length() {
        assert!(matches!(
            "abcd".parse::<RoomKey>().unwrap_err(),
            RoomKeyParseError::InvalidLength(2)
        ));
    }
}

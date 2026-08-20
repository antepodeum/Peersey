use std::{fmt, str::FromStr};

use bytes::Bytes;
use iroh::PublicKey;
use iroh_gossip::api;
use iroh_gossip_rendezvous::Rendezvous;
use thiserror::Error as ThisError;
use tokio::sync::broadcast;

use crate::Error;

const PUBLIC_ROOM_PROTOCOL: &str = "peersey/public-room/v1";
const PRIVATE_ROOM_PROTOCOL: &str = "peersey/private-room/v1";
const MAX_ROOM_NAME_LEN: usize = 128;

fn validate_room_name(value: &str) -> Result<(), Error> {
    if !value.is_empty() && value.len() <= MAX_ROOM_NAME_LEN {
        Ok(())
    } else {
        Err(Error::InvalidRoomName)
    }
}

fn normalize_room_name(value: &str) -> Result<&str, Error> {
    let value = value.trim();
    validate_room_name(value)?;
    Ok(value)
}

/// Secret 256-bit capability required to discover and join a private room.
///
/// Treat this value like a password. Anyone who has it can join the room.
/// Peersey derives rendezvous coordinates and DHT record encryption keys from
/// it while still using the public BitTorrent Mainline DHT.
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

/// Event received from a public or private room.
#[derive(Debug, Clone)]
pub enum RoomEvent {
    /// A direct gossip neighbor connected.
    PeerJoined { peer: PublicKey },
    /// A direct gossip neighbor disconnected.
    PeerLeft { peer: PublicKey },
    /// A message arrived.
    Message { content: Bytes },
    /// Room event stream skipped events because the receiver lagged.
    Lagged,
}

/// Receiver for room events.
pub struct Subscription {
    inner: broadcast::Receiver<api::Event>,
}

impl Subscription {
    /// Receive the next event.
    ///
    /// Returns [`None`] after the room closes. Both underlying lag conditions
    /// become [`RoomEvent::Lagged`].
    pub async fn recv(&mut self) -> Option<RoomEvent> {
        match self.inner.recv().await {
            Ok(api::Event::NeighborUp(peer)) => Some(RoomEvent::PeerJoined { peer }),
            Ok(api::Event::NeighborDown(peer)) => Some(RoomEvent::PeerLeft { peer }),
            Ok(api::Event::Received(message)) => Some(RoomEvent::Message {
                content: message.content,
            }),
            Ok(api::Event::Lagged) | Err(broadcast::error::RecvError::Lagged(_)) => {
                Some(RoomEvent::Lagged)
            }
            Err(broadcast::error::RecvError::Closed) => None,
        }
    }
}

/// A live public or private pub/sub room.
pub struct Room {
    rendezvous: Rendezvous,
}

impl Room {
    pub(crate) async fn join_public(name: &str) -> Result<Self, Error> {
        let name = normalize_room_name(name)?;
        Self::join(name, PUBLIC_ROOM_PROTOCOL).await
    }

    pub(crate) async fn join_private(key: RoomKey) -> Result<Self, Error> {
        Self::join(&key.to_string(), PRIVATE_ROOM_PROTOCOL).await
    }

    async fn join(passphrase: &str, protocol: &str) -> Result<Self, Error> {
        let rendezvous = Rendezvous::builder()
            .passphrase(passphrase)
            .app_salt(protocol)
            .build()
            .await?;
        Ok(Self { rendezvous })
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

    /// This room connection's peer identity.
    #[must_use]
    pub fn peer_id(&self) -> PublicKey {
        self.rendezvous.node_id()
    }

    /// Leave the room and stop its discovery tasks.
    pub async fn shutdown(self) {
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
    fn public_name_is_valid() {
        assert!(validate_room_name("rust/networking.v1").is_ok());
        assert_eq!(
            normalize_room_name("  public chat  ").unwrap(),
            "public chat"
        );
    }

    #[test]
    fn public_name_rejects_empty_or_long_values() {
        assert!(matches!(
            validate_room_name(""),
            Err(Error::InvalidRoomName)
        ));
        assert!(matches!(
            validate_room_name(&"x".repeat(MAX_ROOM_NAME_LEN + 1)),
            Err(Error::InvalidRoomName)
        ));
        assert!(validate_room_name("общий чат").is_ok());
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

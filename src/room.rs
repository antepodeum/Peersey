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
const MAX_ROOM_ID_LEN: usize = 128;

/// Public identifier for an open room.
///
/// Anyone who knows this value can discover and join the room. Use a memorable
/// ID for shared public spaces or [`RoomId::random`] for an unlisted room.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoomId(String);

impl RoomId {
    /// Create a validated public room identifier.
    pub fn new(value: impl Into<String>) -> Result<Self, RoomIdParseError> {
        let value = value.into();
        validate_room_id(&value)?;
        Ok(Self(value))
    }

    /// Generate an unlisted public room identifier.
    #[must_use]
    pub fn random() -> Self {
        Self(hex::encode(rand::random::<[u8; 16]>()))
    }

    /// Borrow the identifier.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for RoomId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for RoomId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for RoomId {
    type Err = RoomIdParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

/// Error returned when parsing a public room identifier.
#[derive(Debug, ThisError, PartialEq, Eq)]
pub enum RoomIdParseError {
    /// Identifier was empty.
    #[error("room ID must not be empty")]
    Empty,
    /// Identifier exceeded 128 bytes.
    #[error("room ID must be at most {MAX_ROOM_ID_LEN} bytes, got {0}")]
    TooLong(usize),
    /// Identifier contained a character unsafe for URLs and command lines.
    #[error("room ID contains invalid character {character:?} at byte {index}")]
    InvalidCharacter { index: usize, character: char },
}

fn validate_room_id(value: &str) -> Result<(), RoomIdParseError> {
    if value.is_empty() {
        return Err(RoomIdParseError::Empty);
    }
    if value.len() > MAX_ROOM_ID_LEN {
        return Err(RoomIdParseError::TooLong(value.len()));
    }
    if let Some((index, character)) = value.char_indices().find(|(_, character)| {
        !character.is_ascii_alphanumeric() && !matches!(character, '-' | '_' | '.' | ':' | '/')
    }) {
        return Err(RoomIdParseError::InvalidCharacter { index, character });
    }
    Ok(())
}

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

/// Event received from a public or private room.
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

/// A live public or private pub/sub room.
pub struct Room {
    rendezvous: Rendezvous,
}

impl Room {
    pub(crate) async fn join_public(id: &RoomId) -> Result<Self, Error> {
        Self::join(id.as_str(), PUBLIC_ROOM_PROTOCOL).await
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
    fn public_id_round_trips() {
        let id: RoomId = "rust/networking.v1".parse().unwrap();
        assert_eq!(id.as_str(), "rust/networking.v1");
        assert_eq!(id.to_string().parse::<RoomId>().unwrap(), id);
    }

    #[test]
    fn public_id_rejects_unsafe_values() {
        assert_eq!("".parse::<RoomId>().unwrap_err(), RoomIdParseError::Empty);
        assert!(matches!(
            "room with spaces".parse::<RoomId>().unwrap_err(),
            RoomIdParseError::InvalidCharacter {
                index: 4,
                character: ' '
            }
        ));
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

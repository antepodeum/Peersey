//! Batteries-included peer-to-peer messaging, live streaming, and file sharing.
//!
//! [`Peersey`] owns networking and storage. Applications only handle public
//! room names, private room keys, and share links.
//!
//! Room discovery uses the public BitTorrent Mainline DHT. Private rooms derive
//! their DHT coordinates and encrypted rendezvous records from a secret
//! [`RoomKey`]; they do not use a separate private DHT network.

mod content;
mod live;
mod room;

pub use content::ShareLink;
pub use live::{LiveLink, LivePacket, LiveReceiver, LiveStream, MediaKind};
pub use room::{Room, RoomEvent, RoomKey, RoomKeyParseError, Subscription};

use std::path::{Path, PathBuf};

use content::ContentNode;
use thiserror::Error as ThisError;
use tokio::sync::OnceCell;

/// A running Peersey node.
///
/// Create one node per application. Content and live networking start lazily
/// on the first file or live operation, so room-only applications stay
/// lightweight.
pub struct Peersey {
    content: OnceCell<ContentNode>,
    storage: Option<PathBuf>,
}

impl Peersey {
    /// Create a zero-configuration node.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            content: OnceCell::const_new(),
            storage: None,
        }
    }

    /// Create a node whose content and provider identity persist at `path`.
    #[must_use]
    pub fn persistent(path: impl Into<PathBuf>) -> Self {
        Self {
            content: OnceCell::const_new(),
            storage: Some(path.into()),
        }
    }

    /// Create and join an open room with a random public name.
    pub async fn create_public_room(&self) -> Result<(Room, String), Error> {
        let name = hex::encode(rand::random::<[u8; 16]>());
        let room = Room::join_public(&name).await?;
        Ok((room, name))
    }

    /// Join an open room using its public name.
    ///
    /// Leading and trailing whitespace is ignored.
    pub async fn join_public_room(&self, name: impl AsRef<str>) -> Result<Room, Error> {
        Room::join_public(name.as_ref()).await
    }

    /// Create and join a new private room.
    pub async fn create_private_room(&self) -> Result<(Room, RoomKey), Error> {
        let key = RoomKey::random();
        let room = Room::join_private(key).await?;
        Ok((room, key))
    }

    /// Join a private room using its secret capability key.
    pub async fn join_private_room(&self, key: RoomKey) -> Result<Room, Error> {
        Room::join_private(key).await
    }

    /// Import and host a file until this node shuts down.
    pub async fn share_file(&self, path: impl AsRef<Path>) -> Result<ShareLink, Error> {
        self.content().await?.share_file(path.as_ref()).await
    }

    /// Download a shared file, verify it, and write it to `destination`.
    pub async fn fetch_file(
        &self,
        link: &ShareLink,
        destination: impl AsRef<Path>,
    ) -> Result<u64, Error> {
        self.content()
            .await?
            .fetch_file(link, destination.as_ref())
            .await
    }

    /// Create a private live stream and a link for viewers.
    ///
    /// The returned publisher accepts encoded audio/video chunks and arbitrary
    /// realtime data. Packets sent before a viewer connects are not retained.
    pub async fn create_live_stream(&self) -> Result<(LiveStream, LiveLink), Error> {
        Ok(self.content().await?.create_live_stream().await)
    }

    /// Connect to a live stream using its capability link.
    pub async fn watch_live(&self, link: &LiveLink) -> Result<LiveReceiver, Error> {
        self.content().await?.watch_live(link).await
    }

    /// Stop file serving and close the content endpoint.
    ///
    /// Rooms have independent lifetimes and should be shut down separately.
    pub async fn shutdown(self) -> Result<(), Error> {
        match self.content.get() {
            Some(content) => content.shutdown().await,
            None => Ok(()),
        }
    }

    async fn content(&self) -> Result<&ContentNode, Error> {
        self.content
            .get_or_try_init(|| async {
                match self.storage.as_deref() {
                    Some(path) => ContentNode::persistent(path).await,
                    None => ContentNode::temporary().await,
                }
            })
            .await
    }
}

impl Default for Peersey {
    fn default() -> Self {
        Self::new()
    }
}

/// Errors produced by Peersey networking, storage, or transfer operations.
#[derive(Debug, ThisError)]
pub enum Error {
    /// Local filesystem operation failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Stored endpoint identity has an invalid length.
    #[error("identity file {path:?} must contain 32 bytes, got {length}")]
    InvalidIdentity {
        /// Identity file path.
        path: std::path::PathBuf,
        /// Actual byte length.
        length: usize,
    },
    /// Public room name was empty or too long.
    #[error("room name must be 1-128 bytes")]
    InvalidRoomName,
    /// A live stream link could not be parsed.
    #[error("invalid live stream link")]
    InvalidLiveLink,
    /// The publisher rejected the live stream capability.
    #[error("live stream access denied")]
    LiveAccessDenied,
    /// A remote peer did not complete the live capability handshake.
    #[error("live stream handshake timed out")]
    LiveHandshakeTimeout,
    /// A malformed packet was received from a live stream.
    #[error("invalid live stream packet")]
    InvalidLivePacket,
    /// A live packet exceeded the bounded packet size.
    #[error("live packet is {size} bytes; maximum is {max}")]
    LivePacketTooLarge {
        /// Actual packet size.
        size: usize,
        /// Maximum accepted packet size.
        max: usize,
    },
    /// Room discovery or gossip failed.
    #[error(transparent)]
    Rendezvous(#[from] iroh_gossip_rendezvous::Error),
    /// Underlying P2P operation failed.
    #[error("{0}")]
    P2p(#[source] Box<dyn std::error::Error + Send + Sync>),
}

impl Error {
    pub(crate) fn p2p(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::P2p(Box::new(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn unused_node_starts_no_content_services() {
        let node = Peersey::new();
        assert!(node.content.get().is_none());
        node.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn persistent_node_creates_nothing_until_file_use() {
        let directory = tempfile::tempdir().unwrap();
        let storage = directory.path().join("storage");
        let node = Peersey::persistent(&storage);
        assert!(!storage.exists());
        node.shutdown().await.unwrap();
        assert!(!storage.exists());
    }
}

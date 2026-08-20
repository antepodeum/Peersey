//! Batteries-included peer-to-peer messaging and file sharing.
//!
//! [`Peersey`] owns the networking and storage. Applications only handle
//! public room IDs, private room keys, and share links.

mod content;
mod room;

pub use content::ShareLink;
pub use room::{
    Room, RoomEvent, RoomId, RoomIdParseError, RoomKey, RoomKeyParseError, Subscription,
};

use std::path::Path;

use content::ContentNode;
use thiserror::Error as ThisError;

/// A running Peersey node.
///
/// Start one node per application and create or join rooms from it. The default
/// node uses an automatically managed temporary on-disk content store.
pub struct Peersey {
    content: ContentNode,
}

impl Peersey {
    /// Start a zero-configuration node.
    pub async fn start() -> Result<Self, Error> {
        Ok(Self {
            content: ContentNode::temporary().await?,
        })
    }

    /// Start a node whose content store lives at `path`.
    pub async fn persistent(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Self {
            content: ContentNode::persistent(path.as_ref()).await?,
        })
    }

    /// Create and join an open room with a new public identifier.
    pub async fn create_public_room(&self) -> Result<(Room, RoomId), Error> {
        let id = RoomId::random();
        let room = Room::join_public(&id).await?;
        Ok((room, id))
    }

    /// Join an open room using its public identifier.
    pub async fn join_public_room(&self, id: RoomId) -> Result<Room, Error> {
        Room::join_public(&id).await
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
        self.content.share_file(path.as_ref()).await
    }

    /// Download a shared file, verify it, and write it to `destination`.
    pub async fn fetch_file(
        &self,
        link: &ShareLink,
        destination: impl AsRef<Path>,
    ) -> Result<u64, Error> {
        self.content.fetch_file(link, destination.as_ref()).await
    }

    /// Stop file serving and close the content endpoint.
    ///
    /// Rooms have independent lifetimes and should be shut down separately.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.content.shutdown().await
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

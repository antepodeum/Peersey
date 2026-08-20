use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use iroh::{
    Endpoint,
    endpoint::presets,
    protocol::{Router, RouterBuilder},
};
use iroh_blobs::{BlobsProtocol, store::fs::FsStore, ticket::BlobTicket};

use crate::Error;

/// Portable link containing a content hash and provider address.
#[derive(Clone, PartialEq, Eq)]
pub struct ShareLink(BlobTicket);

impl ShareLink {
    /// BLAKE3 content identifier encoded for display.
    #[must_use]
    pub fn content_id(&self) -> String {
        self.0.hash().to_string()
    }
}

impl fmt::Debug for ShareLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ShareLink").field(&self.to_string()).finish()
    }
}

impl fmt::Display for ShareLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl FromStr for ShareLink {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        value.parse().map(Self).map_err(Error::p2p)
    }
}

pub(crate) struct ContentNode {
    router: Router,
    store: FsStore,
    _temporary: Option<tempfile::TempDir>,
}

impl ContentNode {
    pub(crate) async fn temporary() -> Result<Self, Error> {
        let directory = tempfile::tempdir()?;
        Self::open(directory.path().to_owned(), Some(directory)).await
    }

    pub(crate) async fn persistent(path: &Path) -> Result<Self, Error> {
        Self::open(path.to_owned(), None).await
    }

    async fn open(path: PathBuf, temporary: Option<tempfile::TempDir>) -> Result<Self, Error> {
        let store = FsStore::load(path).await.map_err(Error::p2p)?;
        let endpoint = Endpoint::bind(presets::N0).await.map_err(Error::p2p)?;
        let blobs = BlobsProtocol::new(&store, None);
        let router = RouterBuilder::new(endpoint)
            .accept(iroh_blobs::ALPN, blobs)
            .spawn();
        Ok(Self {
            router,
            store,
            _temporary: temporary,
        })
    }

    pub(crate) async fn share_file(&self, path: &Path) -> Result<ShareLink, Error> {
        let path = std::path::absolute(path)?;
        let tag = self
            .store
            .blobs()
            .add_path(path)
            .await
            .map_err(Error::p2p)?;
        let ticket = BlobTicket::new(self.router.endpoint().addr(), tag.hash, tag.format);
        Ok(ShareLink(ticket))
    }

    pub(crate) async fn fetch_file(
        &self,
        link: &ShareLink,
        destination: &Path,
    ) -> Result<u64, Error> {
        let destination = std::path::absolute(destination)?;
        self.store
            .downloader(self.router.endpoint())
            .download(link.0.hash(), Some(link.0.addr().id))
            .await
            .map_err(Error::p2p)?;
        self.store
            .blobs()
            .export(link.0.hash(), destination)
            .await
            .map_err(Error::p2p)
    }

    pub(crate) async fn shutdown(&self) -> Result<(), Error> {
        self.router.shutdown().await.map_err(Error::p2p)
    }
}

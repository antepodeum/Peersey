use std::{
    fmt,
    path::{Path, PathBuf},
    str::FromStr,
};

use iroh::{
    Endpoint, SecretKey, address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router,
};
use iroh_blobs::{
    BlobsProtocol, api::downloader::Downloader, store::fs::FsStore, ticket::BlobTicket,
};

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
    downloader: Downloader,
    addresses: MemoryLookup,
    _temporary: Option<tempfile::TempDir>,
}

impl ContentNode {
    pub(crate) async fn temporary() -> Result<Self, Error> {
        let directory = tempfile::tempdir()?;
        Self::open(directory.path().to_owned(), Some(directory), None).await
    }

    pub(crate) async fn persistent(path: &Path) -> Result<Self, Error> {
        let identity = load_or_create_identity(path)?;
        Self::open(path.to_owned(), None, Some(identity)).await
    }

    async fn open(
        path: PathBuf,
        temporary: Option<tempfile::TempDir>,
        identity: Option<SecretKey>,
    ) -> Result<Self, Error> {
        let store = FsStore::load(path).await.map_err(Error::p2p)?;
        let addresses = MemoryLookup::new();
        let mut endpoint = Endpoint::builder(presets::N0).address_lookup(addresses.clone());
        if let Some(identity) = identity {
            endpoint = endpoint.secret_key(identity);
        }
        let endpoint = endpoint.bind().await.map_err(Error::p2p)?;
        let blobs = BlobsProtocol::new(&store, None);
        let router = Router::builder(endpoint)
            .accept(iroh_blobs::ALPN, blobs)
            .spawn();
        let downloader = store.downloader(router.endpoint());
        Ok(Self {
            router,
            store,
            downloader,
            addresses,
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
        let _ = tokio::time::timeout(
            std::time::Duration::from_secs(iroh::NET_REPORT_TIMEOUT),
            self.router.endpoint().online(),
        )
        .await;
        let ticket = BlobTicket::new(self.router.endpoint().addr(), tag.hash, tag.format);
        Ok(ShareLink(ticket))
    }

    pub(crate) async fn fetch_file(
        &self,
        link: &ShareLink,
        destination: &Path,
    ) -> Result<u64, Error> {
        let destination = std::path::absolute(destination)?;
        self.addresses.add_endpoint_info(link.0.addr().clone());
        self.downloader
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

fn load_or_create_identity(root: &Path) -> Result<SecretKey, Error> {
    std::fs::create_dir_all(root)?;
    let path = root.join("identity.key");
    match std::fs::read(&path) {
        Ok(bytes) => parse_identity(&path, bytes),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let key = SecretKey::generate();
            write_new_identity(&path, &key.to_bytes())?;
            parse_identity(&path, std::fs::read(&path)?)
        }
        Err(error) => Err(error.into()),
    }
}

fn parse_identity(path: &Path, bytes: Vec<u8>) -> Result<SecretKey, Error> {
    let length = bytes.len();
    let bytes: [u8; 32] = bytes.try_into().map_err(|_| Error::InvalidIdentity {
        path: path.to_owned(),
        length,
    })?;
    Ok(SecretKey::from_bytes(&bytes))
}

fn write_new_identity(path: &Path, bytes: &[u8; 32]) -> Result<(), Error> {
    use std::io::Write;

    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    match options.open(path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistent_identity_is_stable() {
        let directory = tempfile::tempdir().unwrap();
        let first = load_or_create_identity(directory.path()).unwrap();
        let second = load_or_create_identity(directory.path()).unwrap();
        assert_eq!(first.public(), second.public());
    }

    #[test]
    fn malformed_identity_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("identity.key"), [0; 3]).unwrap();
        assert!(matches!(
            load_or_create_identity(directory.path()).unwrap_err(),
            Error::InvalidIdentity { length: 3, .. }
        ));
    }
}

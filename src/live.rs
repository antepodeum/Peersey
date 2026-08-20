use std::{
    collections::HashMap,
    fmt,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use iroh::{
    Endpoint, EndpointAddr,
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::Error;

pub(crate) const ALPN: &[u8] = b"peersey/live/1";
const LINK_PREFIX: &str = "peersey-live:";
const HEADER_LEN: usize = 14;
const MAX_PACKET_SIZE: usize = 8 * 1024 * 1024;
const CHANNEL_CAPACITY: usize = 64;
const SEND_CONCURRENCY: usize = 32;
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const DENIAL_FLUSH_TIMEOUT: Duration = Duration::from_secs(1);

type StreamSender = broadcast::Sender<Option<LivePacket>>;
type StreamMap = HashMap<[u8; 32], StreamSender>;

/// Portable capability link for watching a live stream.
///
/// Anyone holding this link can watch the stream while its publisher is
/// online. Treat it as a secret when the stream is private.
#[derive(Clone, PartialEq, Eq)]
pub struct LiveLink(LiveTicket);

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LiveTicket {
    endpoint: EndpointAddr,
    token: [u8; 32],
}

impl LiveLink {
    pub(crate) fn endpoint(&self) -> &EndpointAddr {
        &self.0.endpoint
    }
}

impl fmt::Debug for LiveLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LiveLink([REDACTED])")
    }
}

impl fmt::Display for LiveLink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = postcard::to_stdvec(&self.0).map_err(|_| fmt::Error)?;
        write!(f, "{LINK_PREFIX}{}", hex::encode(bytes))
    }
}

impl FromStr for LiveLink {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let encoded = value
            .trim()
            .strip_prefix(LINK_PREFIX)
            .ok_or(Error::InvalidLiveLink)?;
        let bytes = hex::decode(encoded).map_err(|_| Error::InvalidLiveLink)?;
        postcard::from_bytes(&bytes)
            .map(Self)
            .map_err(|_| Error::InvalidLiveLink)
    }
}

/// Kind of data carried by a live packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaKind {
    /// Encoded video frame or video chunk.
    Video,
    /// Encoded audio frame or audio chunk.
    Audio,
    /// Application-defined realtime data.
    Data,
}

impl MediaKind {
    const fn as_byte(self) -> u8 {
        match self {
            Self::Video => 0,
            Self::Audio => 1,
            Self::Data => 2,
        }
    }

    fn from_byte(value: u8) -> Result<Self, Error> {
        match value {
            0 => Ok(Self::Video),
            1 => Ok(Self::Audio),
            2 => Ok(Self::Data),
            _ => Err(Error::InvalidLivePacket),
        }
    }
}

/// One timestamped packet from a live stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LivePacket {
    /// Media track carried by this packet.
    pub kind: MediaKind,
    /// Time since the publisher created the stream.
    pub timestamp: Duration,
    /// Encoded media or application bytes.
    pub content: Bytes,
}

impl LivePacket {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_LEN + self.content.len());
        bytes.push(1);
        bytes.push(self.kind.as_byte());
        let micros = self.timestamp.as_micros().min(u128::from(u64::MAX)) as u64;
        bytes.extend_from_slice(&micros.to_be_bytes());
        bytes.extend_from_slice(&(self.content.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&self.content);
        bytes
    }

    fn decode(bytes: Vec<u8>) -> Result<Self, Error> {
        if bytes.len() < HEADER_LEN || bytes[0] != 1 {
            return Err(Error::InvalidLivePacket);
        }
        let kind = MediaKind::from_byte(bytes[1])?;
        let timestamp = u64::from_be_bytes(
            bytes[2..10]
                .try_into()
                .map_err(|_| Error::InvalidLivePacket)?,
        );
        let length = u32::from_be_bytes(
            bytes[10..14]
                .try_into()
                .map_err(|_| Error::InvalidLivePacket)?,
        ) as usize;
        if length > MAX_PACKET_SIZE || bytes.len() != HEADER_LEN + length {
            return Err(Error::InvalidLivePacket);
        }
        Ok(Self {
            kind,
            timestamp: Duration::from_micros(timestamp),
            content: Bytes::from(bytes).slice(HEADER_LEN..),
        })
    }
}

/// Publisher for one audio, video, or generic realtime stream.
///
/// Packets are sent to current viewers only. Slow viewers skip old packets
/// instead of adding unbounded latency.
pub struct LiveStream {
    token: [u8; 32],
    sender: StreamSender,
    host: LiveHost,
    started: Instant,
}

impl LiveStream {
    /// Send an encoded video frame or chunk.
    pub fn send_video(&self, content: impl Into<Bytes>) -> Result<(), Error> {
        self.send(MediaKind::Video, content.into())
    }

    /// Send an encoded audio frame or chunk.
    pub fn send_audio(&self, content: impl Into<Bytes>) -> Result<(), Error> {
        self.send(MediaKind::Audio, content.into())
    }

    /// Send application-defined realtime data.
    pub fn send_data(&self, content: impl Into<Bytes>) -> Result<(), Error> {
        self.send(MediaKind::Data, content.into())
    }

    /// Number of viewers currently connected to this process.
    #[must_use]
    pub fn viewer_count(&self) -> usize {
        self.sender.receiver_count()
    }

    fn send(&self, kind: MediaKind, content: Bytes) -> Result<(), Error> {
        if content.len() > MAX_PACKET_SIZE {
            return Err(Error::LivePacketTooLarge {
                size: content.len(),
                max: MAX_PACKET_SIZE,
            });
        }
        let _ = self.sender.send(Some(LivePacket {
            kind,
            timestamp: self.started.elapsed(),
            content,
        }));
        Ok(())
    }
}

impl Drop for LiveStream {
    fn drop(&mut self) {
        let _ = self.sender.send(None);
        self.host.remove(self.token, &self.sender);
    }
}

/// Viewer connected to a remote live stream.
pub struct LiveReceiver {
    connection: Connection,
}

impl LiveReceiver {
    /// Receive the next packet, or `None` when the publisher closes.
    pub async fn recv(&mut self) -> Result<Option<LivePacket>, Error> {
        let stream = tokio::select! {
            biased;
            _ = self.connection.closed() => return Ok(None),
            stream = self.connection.accept_uni() => stream.map_err(Error::p2p)?,
        };
        let mut stream = stream;
        let bytes = stream
            .read_to_end(HEADER_LEN + MAX_PACKET_SIZE)
            .await
            .map_err(Error::p2p)?;
        LivePacket::decode(bytes).map(Some)
    }

    /// Disconnect from the publisher.
    pub fn shutdown(self) {
        self.connection.close(0u32.into(), b"done");
    }
}

#[derive(Clone, Default)]
pub(crate) struct LiveHost {
    streams: Arc<Mutex<StreamMap>>,
}

impl fmt::Debug for LiveHost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveHost").finish_non_exhaustive()
    }
}

impl LiveHost {
    pub(crate) fn create(&self, endpoint: EndpointAddr) -> (LiveStream, LiveLink) {
        let token = rand::random();
        let (sender, _) = broadcast::channel(CHANNEL_CAPACITY);
        self.lock().insert(token, sender.clone());
        let stream = LiveStream {
            token,
            sender,
            host: self.clone(),
            started: Instant::now(),
        };
        let link = LiveLink(LiveTicket { endpoint, token });
        (stream, link)
    }

    pub(crate) async fn connect(
        &self,
        endpoint: &Endpoint,
        link: &LiveLink,
    ) -> Result<LiveReceiver, Error> {
        let connection = endpoint
            .connect(link.0.endpoint.clone(), ALPN)
            .await
            .map_err(Error::p2p)?;
        let (mut send, mut receive) = connection.open_bi().await.map_err(Error::p2p)?;
        send.write_all(&link.0.token).await.map_err(Error::p2p)?;
        send.finish().map_err(Error::p2p)?;
        let response = tokio::time::timeout(HANDSHAKE_TIMEOUT, receive.read_to_end(1))
            .await
            .map_err(|_| Error::LiveHandshakeTimeout)?
            .map_err(Error::p2p)?;
        if response != [1] {
            return Err(Error::LiveAccessDenied);
        }
        Ok(LiveReceiver { connection })
    }

    fn remove(&self, token: [u8; 32], sender: &StreamSender) {
        let mut streams = self.lock();
        if streams
            .get(&token)
            .is_some_and(|current| current.same_channel(sender))
        {
            streams.remove(&token);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StreamMap> {
        self.streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl ProtocolHandler for LiveHost {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let Ok(streams) = tokio::time::timeout(HANDSHAKE_TIMEOUT, connection.accept_bi()).await
        else {
            return Ok(());
        };
        let (mut response, mut request) = streams?;
        let Ok(Ok(token)) = tokio::time::timeout(HANDSHAKE_TIMEOUT, request.read_to_end(32)).await
        else {
            return Ok(());
        };
        let sender = token
            .as_slice()
            .try_into()
            .ok()
            .and_then(|token: [u8; 32]| self.lock().get(&token).cloned());
        let Some(sender) = sender else {
            if response.write_all(&[0]).await.is_ok() {
                let _ = response.finish();
                let _ = tokio::time::timeout(DENIAL_FLUSH_TIMEOUT, response.stopped()).await;
            }
            return Ok(());
        };
        let mut packets = sender.subscribe();
        if response.write_all(&[1]).await.is_err() {
            return Ok(());
        }
        response.finish()?;

        let permits = Arc::new(tokio::sync::Semaphore::new(SEND_CONCURRENCY));
        loop {
            let packet = match packets.recv().await {
                Ok(Some(packet)) => packet,
                Ok(None) => break,
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            };
            let Ok(permit) = permits.clone().acquire_owned().await else {
                break;
            };
            let connection = connection.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let Ok(mut stream) = connection.open_uni().await else {
                    return;
                };
                if stream.write_all(&packet.encode()).await.is_ok() {
                    let _ = stream.finish();
                }
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packet_round_trips() {
        let packet = LivePacket {
            kind: MediaKind::Video,
            timestamp: Duration::from_micros(42),
            content: Bytes::from_static(b"frame"),
        };
        assert_eq!(LivePacket::decode(packet.encode()).unwrap(), packet);
    }

    #[test]
    fn malformed_packets_are_rejected() {
        assert!(matches!(
            LivePacket::decode(vec![1, 9]),
            Err(Error::InvalidLivePacket)
        ));
    }
}

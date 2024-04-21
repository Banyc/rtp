use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc};

use async_trait::async_trait;
use tokio::net::UdpSocket;
use udp_listener::{AcceptedUdpRead, AcceptedUdpWrite, Packet, UdpListener};

use crate::{
    socket::{socket, ReadSocket, WriteSocket},
    transport_layer::{UnreliableRead, UnreliableWrite},
};

const DISPATCHER_BUFFER_SIZE: usize = 1024;

type IdentityUdpListener = UdpListener<SocketAddr, Packet>;
type IdentityAcceptedUdpRead = AcceptedUdpRead<Packet>;

#[derive(Debug)]
pub struct Listener {
    listener: IdentityUdpListener,
}
impl Listener {
    pub async fn bind(addr: impl tokio::net::ToSocketAddrs) -> std::io::Result<Self> {
        let udp = UdpSocket::bind(addr).await?;
        let listener = UdpListener::new_identity_dispatch(
            udp,
            NonZeroUsize::new(DISPATCHER_BUFFER_SIZE).unwrap(),
        );
        Ok(Self { listener })
    }

    pub async fn accept(&self) -> std::io::Result<(ReadSocket, WriteSocket)> {
        let accepted = self.listener.accept().await?;
        let (read, write) = accepted.split();
        Ok(socket(Box::new(read), Box::new(write)))
    }
}

// Accepted socket
#[async_trait]
impl UnreliableRead for IdentityAcceptedUdpRead {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        let pkt = IdentityAcceptedUdpRead::recv(self)
            .try_recv()
            .map_err(|e| match e {
                tokio::sync::mpsc::error::TryRecvError::Empty => std::io::ErrorKind::WouldBlock,
                tokio::sync::mpsc::error::TryRecvError::Disconnected => {
                    std::io::ErrorKind::UnexpectedEof
                }
            })?;
        let min_len = buf.len().min(pkt.len());
        buf[..min_len].copy_from_slice(&pkt[..min_len]);
        Ok(min_len)
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        let pkt = IdentityAcceptedUdpRead::recv(self)
            .recv()
            .await
            .ok_or(std::io::ErrorKind::UnexpectedEof)?;
        let min_len = buf.len().min(pkt.len());
        buf[..min_len].copy_from_slice(&pkt[..min_len]);
        Ok(min_len)
    }
}
#[async_trait]
impl UnreliableWrite for AcceptedUdpWrite {
    async fn send(&self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
        AcceptedUdpWrite::send(self, buf)
            .await
            .map_err(|e| e.kind())
    }
}

// Connected socket
#[async_trait]
impl UnreliableRead for Arc<UdpSocket> {
    fn try_recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        UdpSocket::try_recv(self, buf).map_err(|e| e.kind())
    }

    async fn recv(&mut self, buf: &mut [u8]) -> Result<usize, std::io::ErrorKind> {
        UdpSocket::recv(self, buf).await.map_err(|e| e.kind())
    }
}
#[async_trait]
impl UnreliableWrite for Arc<UdpSocket> {
    async fn send(&self, buf: &[u8]) -> Result<usize, std::io::ErrorKind> {
        UdpSocket::send(self, buf).await.map_err(|e| e.kind())
    }
}

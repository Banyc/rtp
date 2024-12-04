use std::num::NonZeroUsize;

use clap::Parser;
use file_transfer::FileTransferCommand;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{lookup_host, TcpListener},
};

#[derive(Debug, Parser)]
pub struct Cli {
    /// The listen address
    pub listen: String,
    #[command(subcommand)]
    pub file_transfer: FileTransferCommand,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let (protocol, internet_address) = args.listen.split_once("://").unwrap();
    let (read, write): (
        Box<dyn AsyncRead + Unpin + Sync + Send + 'static>,
        Box<dyn AsyncWrite + Unpin + Sync + Send + 'static>,
    ) = match protocol {
        "tcp" => {
            let listener = TcpListener::bind(internet_address).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let (read, write) = stream.into_split();
            (Box::new(read), Box::new(write))
        }
        "rtp" => {
            let listener = rtp::udp::Listener::bind(internet_address).await.unwrap();
            let accepted = listener.accept().await.unwrap();
            tokio::spawn(async move {
                loop {
                    if listener.accept().await.is_err() {
                        break;
                    }
                }
            });
            let accepted = accepted.await.unwrap().unwrap();
            (
                Box::new(accepted.read.into_async_read()),
                Box::new(accepted.write.into_async_write()),
            )
        }
        "rtpm" => {
            let max_session_conns = NonZeroUsize::new(16).unwrap();
            let socket_addrs = lookup_host(internet_address).await.unwrap();
            let mut listener = rtp::mpudp::Listener::bind(socket_addrs, max_session_conns)
                .await
                .unwrap();
            let accepted = listener.accept_without_handshake().await.unwrap();
            tokio::spawn(async move {
                loop {
                    if listener.accept_without_handshake().await.is_err() {
                        break;
                    }
                }
            });
            (
                Box::new(accepted.read.into_async_read()),
                Box::new(accepted.write.into_async_write()),
            )
        }
        _ => panic!("unknown protocol `{protocol}`"),
    };
    println!("accepted");

    let mut res = args.file_transfer.perform(read, write).await.unwrap();
    res.write.shutdown().await.unwrap();
    println!("shutdown");
    let mut buf = [0; 1];
    let n = res.read.read(&mut buf).await.unwrap();
    assert_eq!(n, 0);

    println!("{}", res.stats);
}

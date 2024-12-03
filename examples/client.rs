use std::path::PathBuf;

use clap::Parser;
use file_transfer::FileTransferCommand;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{lookup_host, TcpStream},
};

#[derive(Debug, Parser)]
pub struct Cli {
    /// The server address
    pub server: String,
    #[command(subcommand)]
    pub file_transfer: FileTransferCommand,
    #[clap(long)]
    pub log_dir: Option<PathBuf>,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let (protocol, internet_address) = args.server.split_once("://").unwrap();
    let (read, write): (
        Box<dyn AsyncRead + Unpin + Sync + Send + 'static>,
        Box<dyn AsyncWrite + Unpin + Sync + Send + 'static>,
    ) = match protocol {
        "tcp" => {
            let stream = TcpStream::connect(internet_address).await.unwrap();
            let (read, write) = stream.into_split();
            (Box::new(read), Box::new(write))
        }
        "rtp" => {
            let log_config = args
                .log_dir
                .as_ref()
                .map(|c| rtp::udp::LogConfig { log_dir_path: c });
            let connected = rtp::udp::connect("0.0.0.0:0", internet_address, log_config)
                .await
                .unwrap();
            (
                Box::new(connected.read.into_async_read()),
                Box::new(connected.write.into_async_write()),
            )
        }
        "rtpm" => {
            let log_config = args
                .log_dir
                .as_ref()
                .map(|c| rtp::udp::LogConfig { log_dir_path: c });
            let socket_addrs = lookup_host(internet_address).await.unwrap();
            let socket_addrs = socket_addrs.flat_map(|addr| (0..2).map(move |_| addr));
            let connected = rtp::mpudp::Conn::connect_without_handshake(socket_addrs, log_config)
                .await
                .unwrap();
            (
                Box::new(connected.read.into_async_read()),
                Box::new(connected.write.into_async_write()),
            )
        }
        _ => panic!("unknown protocol `{protocol}`"),
    };
    println!("connected");

    let mut res = args.file_transfer.perform(read, write).await.unwrap();
    res.write.shutdown().await.unwrap();
    println!("shutdown");
    let mut buf = [0; 1];
    let n = res.read.read(&mut buf).await.unwrap();
    assert_eq!(n, 0);

    println!("{}", res.stats);
}

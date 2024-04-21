use std::sync::Arc;

use clap::Parser;
use file_transfer::FileTransferCommand;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpStream, UdpSocket},
};

#[derive(Debug, Parser)]
pub struct Cli {
    /// The server address
    pub server: String,
    #[command(subcommand)]
    pub file_transfer: FileTransferCommand,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();

    let (protocol, internet_address) = args.server.split_once("://").unwrap();
    let (read, write): (Box<dyn AsyncRead + Unpin>, Box<dyn AsyncWrite + Unpin>) = match protocol {
        "tcp" => {
            let stream = TcpStream::connect(internet_address).await.unwrap();
            let (read, write) = stream.into_split();
            (Box::new(read), Box::new(write))
        }
        "rtp" => {
            let udp = UdpSocket::bind("0.0.0.0:0").await.unwrap();
            udp.connect(internet_address).await.unwrap();
            let udp = Arc::new(udp);
            let (read, write) = rtp::socket::socket(Box::new(Arc::clone(&udp)), Box::new(udp));
            (
                Box::new(read.into_async_read()),
                Box::new(write.into_async_write(true)),
            )
        }
        _ => panic!("unknown protocol `{protocol}`"),
    };
    println!("connected");

    let stats = args.file_transfer.perform(read, write).await.unwrap();
    println!("{stats}");
}

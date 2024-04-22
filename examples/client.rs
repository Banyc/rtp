use clap::Parser;
use file_transfer::FileTransferCommand;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
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
            let (read, write) = rtp::udp::connect(internet_address).await.unwrap();
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

use clap::Parser;
use file_transfer::FileTransferCommand;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
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
            (
                Box::new(accepted.read.into_async_read()),
                Box::new(accepted.write.into_async_write()),
            )
        }
        _ => panic!("unknown protocol `{protocol}`"),
    };
    println!("accepted");

    let stats = args.file_transfer.perform(read, write).await.unwrap();
    println!("{stats}");
}

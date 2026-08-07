use std::num::NonZeroUsize;

use clap::Parser;
use file_transfer::FileTransferCommand;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, lookup_host},
    task::JoinSet,
};

#[path = "support/session_scope.rs"]
mod session_scope;

use session_scope::{TransportScope, TransportTaskExit};

#[derive(Debug, Parser)]
pub struct Cli {
    /// The listen address
    pub listen: String,
    #[command(subcommand)]
    pub file_transfer: FileTransferCommand,
    #[clap(long)]
    pub fec: bool,
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let fec = args.fec;

    let mut scope = TransportScope::new();

    let (protocol, internet_addresses) = args.listen.split_once("://").unwrap();
    let internet_addresses = internet_addresses.split(',').collect::<Vec<_>>();
    let (read, write): (
        Box<dyn AsyncRead + Unpin + Sync + Send + 'static>,
        Box<dyn AsyncWrite + Unpin + Sync + Send + 'static>,
    ) = match protocol {
        "tcp" => {
            let listener = TcpListener::bind(internet_addresses[0]).await.unwrap();
            let (stream, _) = listener.accept().await.unwrap();
            let (read, write) = stream.into_split();
            (Box::new(read), Box::new(write))
        }
        "rtp" => {
            let listener = rtp::udp::Listener::bind(internet_addresses[0])
                .await
                .unwrap();
            let (first_tx, first_rx) = tokio::sync::oneshot::channel::<rtp::udp::Accepted>();
            scope.spawn(|stop| {
                run_udp_accept_driver(
                    listener,
                    rtp::udp::AcceptConfig {
                        fec,
                        ..rtp::udp::AcceptConfig::default()
                    },
                    first_tx,
                    stop,
                )
            });
            let accepted = scope
                .race(first_rx)
                .await
                .expect("rtp accept driver failed");
            let rtp::udp::Accepted {
                read,
                write,
                supervisor,
                peer_addr: _,
            } = accepted;
            scope.supervise_session("rtp", supervisor);
            (
                Box::new(read.into_async_read()),
                Box::new(write.into_async_write()),
            )
        }
        "rtpm" => {
            let max_session_conns = NonZeroUsize::new(16).unwrap();
            let mut all_socket_addrs = vec![];
            for internet_address in internet_addresses {
                let socket_addrs = lookup_host(internet_address).await.unwrap();
                all_socket_addrs.extend(socket_addrs);
            }
            let listener =
                rtp::mpudp::Listener::bind(all_socket_addrs.into_iter(), max_session_conns)
                    .await
                    .unwrap();
            let (first_tx, first_rx) = tokio::sync::oneshot::channel::<rtp::mpudp::Conn>();
            scope.spawn(|stop| {
                run_mpudp_accept_driver(listener, rtp::udp::AcceptConfig::default(), first_tx, stop)
            });
            let accepted = scope
                .race(first_rx)
                .await
                .expect("rtpm accept driver failed");
            let rtp::mpudp::Conn {
                read,
                write,
                supervisor,
            } = accepted;
            scope.supervise_session("rtpm", supervisor);
            (
                Box::new(read.into_async_read()),
                Box::new(write.into_async_write()),
            )
        }
        _ => panic!("unknown protocol `{protocol}`"),
    };
    println!("accepted");

    scope
        .race(async move {
            let mut res = args.file_transfer.perform(read, write).await.unwrap();
            res.write.shutdown().await.unwrap();
            println!("shutdown");
            let mut buf = [0; 1];
            let n = res.read.read(&mut buf).await.unwrap();
            assert_eq!(n, 0);

            println!("{}", res.stats);
        })
        .await;
}

async fn run_udp_accept_driver(
    listener: rtp::udp::Listener,
    config: rtp::udp::AcceptConfig,
    first_tx: tokio::sync::oneshot::Sender<rtp::udp::Accepted>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> TransportTaskExit {
    let mut first_tx = Some(first_tx);
    let mut handshakes = JoinSet::new();
    loop {
        tokio::select! {
            next = listener.accept_with(config) => {
                match next {
                    Ok(task) => {
                        handshakes.spawn(task);
                    }
                    Err(error) => return TransportTaskExit::DriverFailed {
                        driver: "rtp_accept",
                        detail: error.to_string(),
                    },
                }
            }
            _ = stop.changed() => return TransportTaskExit::Stopped,
            joined = handshakes.join_next(), if !handshakes.is_empty() => {
                let result = joined.expect("guarded non-empty").unwrap();
                match result {
                    Ok(accepted) => match first_tx.take() {
                        Some(tx) => {
                            let _ = tx.send(accepted);
                        }
                        None => drop(accepted),
                    },
                    Err(error) => {
                        eprintln!("RTP handshake rejected: {}", error);
                    }
                }
            }
        }
    }
}

async fn run_mpudp_accept_driver(
    mut listener: rtp::mpudp::Listener,
    config: rtp::udp::AcceptConfig,
    first_tx: tokio::sync::oneshot::Sender<rtp::mpudp::Conn>,
    mut stop: tokio::sync::watch::Receiver<bool>,
) -> TransportTaskExit {
    let mut first_tx = Some(first_tx);
    loop {
        tokio::select! {
            next = listener.accept_with(config) => {
                match next {
                    Ok(accepted) => match first_tx.take() {
                        Some(tx) => {
                            let _ = tx.send(accepted);
                        }
                        None => drop(accepted),
                    },
                    Err(error) => {
                        return TransportTaskExit::DriverFailed {
                            driver: "rtpm_accept",
                            detail: error.to_string(),
                        };
                    }
                }
            }
            _ = stop.changed() => return TransportTaskExit::Stopped,
        }
    }
}

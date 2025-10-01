use std::net::SocketAddr;

use aggligator::transport::{AcceptorBuilder, ConnectorBuilder};
use anyhow::bail;

mod client {
    pub mod ssh_connector {
        use std::any::Any;
        use std::cmp::Ordering;
        use std::collections::HashSet;
        use std::fmt;
        use std::hash::{Hash, Hasher};
        use std::io::Result;
        use std::net::{Ipv4Addr, SocketAddr};

        use aggligator::control::Direction;
        use aggligator::io::{IoBox, StreamBox};
        use aggligator::{
            Link,
            transport::{ConnectingTransport, LinkTag, LinkTagBox},
        };

        static NAME: &str = "ssh_perl";

        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct SshLinkTag {
            pub jump_server: SocketAddr,
            pub jump_user: String,
            pub dest: SocketAddr,
            pub direction: Direction,
        }

        impl SshLinkTag {
            pub fn from_addr(jump: SocketAddr, dest: SocketAddr) -> Self {
                SshLinkTag {
                    jump_server: jump,
                    jump_user: "jyi".to_owned(),
                    direction: Direction::Outgoing,
                    dest,
                }
            }
        }

        impl fmt::Display for SshLinkTag {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let dir = match self.direction {
                    Direction::Incoming => "<-",
                    Direction::Outgoing => "->",
                };
                write!(f, "{dir} {} {}", self.jump_server, self.dest)
            }
        }

        impl LinkTag for SshLinkTag {
            fn transport_name(&self) -> &str {
                NAME
            }

            fn direction(&self) -> Direction {
                self.direction
            }

            fn user_data(&self) -> Vec<u8> {
                self.jump_server.to_string().into_bytes()
            }

            fn as_any(&self) -> &dyn Any {
                self
            }

            fn box_clone(&self) -> LinkTagBox {
                Box::new(self.clone())
            }

            fn dyn_cmp(&self, other: &dyn LinkTag) -> Ordering {
                let other = other.as_any().downcast_ref::<Self>().unwrap();
                Ord::cmp(self, other)
            }

            fn dyn_hash(&self, mut state: &mut dyn Hasher) {
                Hash::hash(self, &mut state)
            }
        }

        pub struct SshConnector {
            pub dest: SocketAddr,
        }

        impl SshConnector {
            pub fn new(dest: SocketAddr) -> Self {
                Self { dest }
            }
        }

        // pub static CONNECT_TO_DEST: &str = include_str!("payload.pl");

        #[async_trait::async_trait]
        impl ConnectingTransport for SshConnector {
            fn name(&self) -> &str {
                NAME
            }

            async fn link_tags(
                &self,
                tx: tokio::sync::watch::Sender<HashSet<LinkTagBox>>,
            ) -> Result<()> {
                loop {
                    let mut tags: HashSet<LinkTagBox> = HashSet::new();
                    tracing::info!("providing {} link tags", tags.len());
                    tx.send_if_modified(|v| {
                        if *v != tags {
                            *v = tags;
                            true
                        } else {
                            false
                        }
                    });
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                }
            }

            async fn connect(&self, tag: &dyn LinkTag) -> Result<StreamBox> {
                let tag: &SshLinkTag = tag.as_any().downcast_ref().unwrap();
                tracing::info!("connecting to {} through {}", tag.dest, tag.jump_server);

                let mut process = tokio::process::Command::new("ssh")
                    .args(&["-l", &tag.jump_user])
                    .arg("-oBatchMode=yes")
                    .args(&["-p", tag.jump_server.port().to_string().as_str()])
                    .arg("-T")
                    .args(&[
                        "-W",
                        format!("{}:{}", self.dest.ip(), self.dest.port()).as_str(),
                    ])
                    .arg(tag.jump_server.ip().to_string())
                    // .arg("perl")
                    // .args(&["-e", CONNECT_TO_DEST])
                    // .arg(self.dest.ip().to_string())
                    // .arg(self.dest.port().to_string())
                    .stdin(std::process::Stdio::piped())
                    .stdout(std::process::Stdio::piped())
                    .spawn()
                    .inspect_err(|e| {
                        tracing::info!(
                            "connecting to {} through {} failed, error: {e}",
                            tag.jump_server,
                            tag.jump_user
                        );
                    })?;
                let stdin = process.stdin.take().unwrap();
                let stdout = process.stdout.take().unwrap();

                Ok(IoBox::new(stdout, stdin).into())
            }

            async fn link_filter(
                &self,
                _new: &Link<LinkTagBox>,
                _existing: &[Link<LinkTagBox>],
            ) -> bool {
                true
            }
        }
    }
}

async fn client(bind: SocketAddr, dest: SocketAddr) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(bind).await?;
    tracing::info!("Server listening on port {bind}");

    loop {
        let (socket, addr) = listener.accept().await?;
        tracing::info!("New connection from: {}", addr);
        let ssh_connector = client::ssh_connector::SshConnector::new(dest);
        let mut connector = ConnectorBuilder::new(Default::default()).build();
        connector.add(ssh_connector);
        let outgoing = connector
            .channel()
            .ok_or_else(|| anyhow::anyhow!("failed to get channel from the connector"))
            .inspect_err(|e| {
                tracing::info!("failed to connect connect to remote {dest}: {e}");
            })?;
        let channel = outgoing.await?.into_stream();
        tokio::spawn(async move {
            let mut socket = socket;
            let mut channel = channel;
            if let Err(e) = tokio::io::copy_bidirectional(&mut channel, &mut socket).await {
                eprintln!("{:?}", e);
            }
        });
    }
}

async fn server(bind: SocketAddr, dest: SocketAddr) -> anyhow::Result<()> {
    let acceptor_builder = AcceptorBuilder::new(Default::default());
    let acceptor = acceptor_builder.build();
    let tcp_acceptor = aggligator_transport_tcp::TcpAcceptor::new(std::iter::once(bind)).await?;
    acceptor.add(tcp_acceptor);
    tracing::info!("listening on {bind}");

    loop {
        let (channel, _control) = acceptor.accept().await?;
        let socket = tokio::net::TcpStream::connect(dest).await?;
        tokio::spawn(async move {
            let mut socket = socket;
            let mut channel = channel.into_stream();
            if let Err(e) = tokio::io::copy_bidirectional(&mut channel, &mut socket).await {
                eprintln!("{:?}", e);
            }
        });
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args[0].as_str() {
        "client" => {
            let bind = args[1].parse()?;
            let dest = args[2].parse()?;
            client(bind, dest).await?;
        }
        "server" => {
            let bind = args[1].parse()?;
            let dest = args[2].parse()?;
            server(bind, dest).await?;
        }
        _ => bail!("client or server"),
    };
    Ok(())
}

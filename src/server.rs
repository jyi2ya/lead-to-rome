use std::net::SocketAddr;

use aggligator::transport::AcceptorBuilder;
use aggligator_transport_tcp::TcpAcceptor;

use crate::error::ServerRunError;

const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

pub async fn rvs_run_server_ABIS(
    listen: SocketAddr,
    target: SocketAddr,
) -> Result<(), ServerRunError> {
    debug_assert!(
        !listen.ip().is_unspecified() || listen.port() != 0,
        "listen addr must be bindable"
    );
    debug_assert!(
        !target.ip().is_unspecified(),
        "target must have a specific IP"
    );

    let acceptor = AcceptorBuilder::new(Default::default()).build();
    let tcp_acceptor = TcpAcceptor::new(std::iter::once(listen))
        .await
        .map_err(|source| ServerRunError::Io { listen, source })?;
    acceptor.add(tcp_acceptor);
    tracing::info!("server listening on {listen}, forwarding to {target}");

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    let mut join_set = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            accept_result = acceptor.accept() => {
                let (channel, _control) = accept_result.map_err(|source| ServerRunError::Io { listen, source })?;
                tracing::info!("server accepted aggregated connection");

                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        tracing::warn!("server rejecting connection: max concurrent connections reached");
                        continue;
                    }
                };

                join_set.spawn(async move {
                    let _permit = permit;
                    match tokio::net::TcpStream::connect(target).await {
                        Ok(socket) => {
                            tracing::info!("server connected to target {target}");
                            let mut channel = channel.into_stream();
                            let mut socket = socket;
                            match tokio::io::copy_bidirectional(&mut channel, &mut socket).await {
                                Ok(_) => tracing::info!("server connection to {target} closed normally"),
                                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof
                                    || e.kind() == std::io::ErrorKind::ConnectionReset
                                    || e.kind() == std::io::ErrorKind::BrokenPipe => {
                                    tracing::info!("server connection to {target} closed: {e}");
                                }
                                Err(e) => {
                                    tracing::error!("server copy_bidirectional error: {e}");
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!("server failed to connect to target {target}: {e}");
                        }
                    }
                });
            }
            Some(result) = join_set.join_next() => {
                if let Err(e) = result {
                    tracing::error!("server task panicked: {e}");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("server shutting down, waiting for {} active connections", join_set.len());
                while join_set.join_next().await.is_some() {}
                return Ok(());
            }
        }
    }
}

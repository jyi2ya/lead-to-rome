use std::net::SocketAddr;

use aggligator::transport::ConnectorBuilder;

use crate::error::ClientRunError;
use crate::proxy::ProxyConfig;
use crate::transport::ProxyConnector;

const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

pub async fn rvs_run_client_ABIS(
    listen: SocketAddr,
    server_addr: SocketAddr,
    proxies: Vec<ProxyConfig>,
) -> Result<(), ClientRunError> {
    debug_assert!(
        !listen.ip().is_unspecified() || listen.port() != 0,
        "listen addr must be bindable"
    );
    debug_assert!(
        !server_addr.ip().is_unspecified(),
        "server_addr must have a specific IP"
    );
    debug_assert!(!proxies.is_empty(), "at least one proxy required");

    let listener = tokio::net::TcpListener::bind(listen)
        .await
        .map_err(|source| ClientRunError::Io { listen, source })?;
    tracing::info!(
        "client listening on {listen}, connecting to server {server_addr} via {} proxy(ies)",
        proxies.len()
    );

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    let mut join_set = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (socket, addr) = accept_result.map_err(|source| ClientRunError::Io { listen, source })?;
                tracing::info!("client accepted connection from {addr}");

                let permit = match semaphore.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        tracing::warn!(
                            "client rejecting connection from {addr}: max concurrent connections reached"
                        );
                        continue;
                    }
                };

                let proxies = proxies.clone();
                join_set.spawn(async move {
                    let _permit = permit;
                    match rvs_handle_local_connection_ABIS(proxies, server_addr, socket, listen).await {
                        Ok(()) => tracing::info!("client connection from {addr} closed"),
                        Err(e) => tracing::error!("client connection error for {addr}: {e}"),
                    }
                });
            }
            Some(result) = join_set.join_next() => {
                if let Err(e) = result {
                    tracing::error!("client task panicked: {e}");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("client shutting down, waiting for {} active connections", join_set.len());
                while join_set.join_next().await.is_some() {}
                return Ok(());
            }
        }
    }
}

async fn rvs_handle_local_connection_ABIS(
    proxies: Vec<ProxyConfig>,
    server_addr: SocketAddr,
    socket: tokio::net::TcpStream,
    listen: SocketAddr,
) -> Result<(), ClientRunError> {
    let (mut channel, _guard) =
        rvs_establish_aggregated_connection_ABI(proxies, server_addr).await?;
    let mut socket = socket;
    tokio::io::copy_bidirectional(&mut channel, &mut socket)
        .await
        .map_err(|source| ClientRunError::Io { listen, source })?;
    Ok(())
}

async fn rvs_establish_aggregated_connection_ABI(
    proxies: Vec<ProxyConfig>,
    server_addr: SocketAddr,
) -> Result<(aggligator::alc::Stream, ConnectionTerminator), ClientRunError> {
    let connector_impl = ProxyConnector::rvs_new(proxies, server_addr);
    let mut connector = ConnectorBuilder::new(Default::default()).build();
    connector.add(connector_impl);

    let control = connector.control();
    let outgoing = connector.channel().ok_or_else(|| {
        let source = std::io::Error::other("failed to get channel from connector");
        ClientRunError::Io {
            listen: server_addr,
            source,
        }
    })?;

    let channel = outgoing.await.map_err(ClientRunError::Connect)?;
    Ok((channel.into_stream(), ConnectionTerminator { control }))
}

struct ConnectionTerminator {
    control: aggligator::transport::BoxControl,
}

impl Drop for ConnectionTerminator {
    fn drop(&mut self) {
        self.control.terminate();
    }
}

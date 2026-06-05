use std::net::SocketAddr;

#[derive(Debug, thiserror::Error)]
pub enum ServerRunError {
    #[error("io error on {listen}: {source}")]
    Io {
        listen: SocketAddr,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ClientRunError {
    #[error("io error on {listen}: {source}")]
    Io {
        listen: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("connect error: {0}")]
    Connect(#[from] aggligator::connect::ConnectError),
}

#[derive(Debug, thiserror::Error)]
pub enum HttpConnectError {
    #[error("io error connecting to proxy {proxy_addr}: {source}")]
    Io {
        proxy_addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("HTTP CONNECT proxy {proxy_addr} returned status {status}: {response}")]
    BadStatus {
        proxy_addr: SocketAddr,
        status: u16,
        response: String,
    },
    #[error("HTTP CONNECT proxy {proxy_addr} response too large")]
    ResponseTooLarge { proxy_addr: SocketAddr },
}

impl From<HttpConnectError> for std::io::Error {
    fn from(err: HttpConnectError) -> Self {
        match err {
            HttpConnectError::Io { source, .. } => source,
            other => std::io::Error::new(std::io::ErrorKind::ConnectionRefused, other),
        }
    }
}

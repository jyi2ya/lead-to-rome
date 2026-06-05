use std::any::Any;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;

use aggligator::Link;
use aggligator::control::Direction;
use aggligator::io::{IoBox, StreamBox};
use aggligator::transport::{ConnectingTransport, LinkTag, LinkTagBox};

use crate::error::HttpConnectError;
use crate::proxy::{ProxyConfig, Socks5Auth};

const TRANSPORT_NAME: &str = "proxy_chain";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProxyLinkTag {
    proxy: ProxyConfig,
    server_addr: SocketAddr,
    direction: Direction,
}

impl ProxyLinkTag {
    pub fn rvs_new(proxy: ProxyConfig, server_addr: SocketAddr) -> Self {
        debug_assert!(
            !server_addr.ip().is_unspecified(),
            "server_addr must have a specific IP"
        );
        Self {
            proxy,
            server_addr,
            direction: Direction::Outgoing,
        }
    }
}

impl fmt::Display for ProxyLinkTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dir = self.direction.arrow();
        write!(f, "{dir} {} via {}", self.server_addr, self.proxy)
    }
}

impl LinkTag for ProxyLinkTag {
    fn transport_name(&self) -> &str {
        TRANSPORT_NAME
    }

    fn direction(&self) -> Direction {
        self.direction
    }

    fn user_data(&self) -> Vec<u8> {
        Vec::new()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn box_clone(&self) -> LinkTagBox {
        Box::new(self.clone())
    }

    fn dyn_cmp(&self, other: &dyn LinkTag) -> Ordering {
        let other = other
            .as_any()
            .downcast_ref::<Self>()
            .expect("never: same transport");
        Ord::cmp(self, other)
    }

    fn dyn_hash(&self, mut state: &mut dyn Hasher) {
        Hash::hash(self, &mut state);
    }
}

#[derive(Debug)]
pub struct ProxyConnector {
    proxies: Vec<ProxyConfig>,
    server_addr: SocketAddr,
}

impl ProxyConnector {
    pub fn rvs_new(proxies: Vec<ProxyConfig>, server_addr: SocketAddr) -> Self {
        debug_assert!(!proxies.is_empty(), "at least one proxy required");
        debug_assert!(
            !server_addr.ip().is_unspecified(),
            "server_addr must have a specific IP"
        );
        Self {
            proxies,
            server_addr,
        }
    }
}

#[async_trait::async_trait]
impl ConnectingTransport for ProxyConnector {
    fn name(&self) -> &str {
        TRANSPORT_NAME
    }

    async fn link_tags(
        &self,
        tx: tokio::sync::watch::Sender<HashSet<LinkTagBox>>,
    ) -> std::io::Result<()> {
        let tags: HashSet<LinkTagBox> = self
            .proxies
            .iter()
            .map(|proxy| {
                let tag = ProxyLinkTag::rvs_new(proxy.clone(), self.server_addr);
                Box::new(tag) as LinkTagBox
            })
            .collect();
        tracing::info!("providing {} link tags", tags.len());
        tx.send_if_modified(|v| {
            if *v != tags {
                *v = tags;
                true
            } else {
                false
            }
        });
        tx.closed().await;
        tracing::info!("link_tags tx closed, exiting");
        Ok(())
    }

    async fn connect(&self, tag: &dyn LinkTag) -> std::io::Result<StreamBox> {
        let tag = tag
            .as_any()
            .downcast_ref::<ProxyLinkTag>()
            .expect("never: same transport");
        tracing::info!("connecting to {} via {}", tag.server_addr, tag.proxy);

        rvs_connect_through_proxy_ABIS(&tag.proxy, tag.server_addr).await
    }

    async fn link_filter(&self, _new: &Link<LinkTagBox>, _existing: &[Link<LinkTagBox>]) -> bool {
        true
    }
}

async fn rvs_connect_through_proxy_ABIS(
    proxy: &ProxyConfig,
    target: SocketAddr,
) -> std::io::Result<StreamBox> {
    match proxy {
        ProxyConfig::Socks4 { addr } => {
            let stream = fast_socks5::socks4::client::Socks4Stream::connect(
                *addr,
                target.ip().to_string(),
                target.port(),
                false,
            )
            .await
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e))?;
            let (read, write) = tokio::io::split(stream);
            Ok(IoBox::new(read, write).into())
        }
        ProxyConfig::Socks5 { addr, auth } => {
            let stream = rvs_connect_socks5_ABI(*addr, auth.as_ref(), target).await?;
            let (read, write) = tokio::io::split(stream);
            Ok(IoBox::new(read, write).into())
        }
        ProxyConfig::Http { addr } => {
            let stream = rvs_http_connect_ABI(*addr, target).await?;
            let (read, write) = tokio::io::split(stream);
            Ok(IoBox::new(read, write).into())
        }
        ProxyConfig::Ssh { addr, user } => {
            let (read, write) = rvs_ssh_connect_ABIS(*addr, user, target).await?;
            Ok(IoBox::new(read, write).into())
        }
    }
}

fn rvs_build_socks5_auth_args(auth: Option<&Socks5Auth>) -> Socks5ConnectArgs {
    match auth {
        Some(a) => Socks5ConnectArgs {
            username: a.rvs_username().to_owned(),
            password: a.rvs_password().to_owned(),
            has_auth: true,
        },
        None => Socks5ConnectArgs {
            username: String::new(),
            password: String::new(),
            has_auth: false,
        },
    }
}

struct Socks5ConnectArgs {
    username: String,
    password: String,
    has_auth: bool,
}

async fn rvs_connect_socks5_ABI(
    addr: SocketAddr,
    auth: Option<&Socks5Auth>,
    target: SocketAddr,
) -> std::io::Result<fast_socks5::client::Socks5Stream<tokio::net::TcpStream>> {
    let config = fast_socks5::client::Config::default();
    let target_addr = target.ip().to_string();
    let target_port = target.port();
    let args = rvs_build_socks5_auth_args(auth);
    let stream = if args.has_auth {
        fast_socks5::client::Socks5Stream::connect_with_password(
            addr,
            target_addr,
            target_port,
            args.username,
            args.password,
            config,
        )
        .await
    } else {
        fast_socks5::client::Socks5Stream::connect(addr, target_addr, target_port, config).await
    }
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e))?;
    Ok(stream)
}

fn rvs_build_http_connect_request(target: SocketAddr) -> String {
    format!(
        "CONNECT {}:{} HTTP/1.1\r\nHost: {}:{}\r\n\r\n",
        target.ip(),
        target.port(),
        target.ip(),
        target.port()
    )
}

fn rvs_parse_http_status_line(response: &str) -> Option<u16> {
    let status_line = response.lines().next()?;
    status_line.split(' ').nth(1)?.parse::<u16>().ok()
}

fn rvs_headers_end_offset(buf: &[u8], len: usize) -> Option<usize> {
    debug_assert!(len <= buf.len(), "len must not exceed buf.len");
    let window = buf.get(..len)?;
    let pattern = b"\r\n\r\n";
    window
        .windows(pattern.len())
        .position(|w| w == pattern)
        .map(|pos| pos + pattern.len())
}

async fn rvs_http_connect_ABI(
    proxy_addr: SocketAddr,
    target: SocketAddr,
) -> std::io::Result<tokio::net::TcpStream> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(proxy_addr)
        .await
        .map_err(|e| HttpConnectError::Io {
            proxy_addr,
            source: e,
        })?;
    let connect_req = rvs_build_http_connect_request(target);
    stream
        .write_all(connect_req.as_bytes())
        .await
        .map_err(|e| HttpConnectError::Io {
            proxy_addr,
            source: e,
        })?;

    let mut buf = vec![0u8; 8192];
    let mut total = 0usize;
    let max_headers = 65536usize;
    loop {
        if total >= max_headers {
            return Err(HttpConnectError::ResponseTooLarge { proxy_addr }.into());
        }
        if total >= buf.len() {
            let new_len = (buf.len() * 2).min(max_headers);
            buf.resize(new_len, 0);
        }
        let n = stream
            .read(buf.get_mut(total..).expect("never: total < buf.len"))
            .await
            .map_err(|e| HttpConnectError::Io {
                proxy_addr,
                source: e,
            })?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "HTTP CONNECT proxy closed connection before sending complete response",
            ));
        }
        total += n;

        let header_end = rvs_headers_end_offset(&buf, total);
        if let Some(end) = header_end {
            let response = String::from_utf8_lossy(buf.get(..total).expect("never: total valid"));
            let status = rvs_parse_http_status_line(&response).ok_or_else(|| {
                HttpConnectError::BadStatus {
                    proxy_addr,
                    status: 0,
                    response: response.to_string(),
                }
            })?;
            if status != 200 {
                return Err(HttpConnectError::BadStatus {
                    proxy_addr,
                    status,
                    response: response.to_string(),
                }
                .into());
            }
            if end < total {
                tracing::warn!(
                    "HTTP CONNECT proxy sent {} bytes after headers, data may be lost",
                    total - end
                );
            }
            return Ok(stream);
        }
    }
}

fn rvs_build_ssh_args(
    proxy_addr: SocketAddr,
    user: &str,
    target: SocketAddr,
) -> std::process::Command {
    let mut cmd = std::process::Command::new("ssh");
    cmd.args(["-l", user])
        .arg("-oBatchMode=yes")
        .args(["-p", &proxy_addr.port().to_string()])
        .arg("-T")
        .args(["-W", &format!("{}:{}", target.ip(), target.port())])
        .arg(proxy_addr.ip().to_string())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    cmd
}

struct SshChildGuard {
    child: Option<tokio::process::Child>,
}

impl SshChildGuard {
    fn rvs_new(child: tokio::process::Child) -> Self {
        Self { child: Some(child) }
    }
}

impl Drop for SshChildGuard {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            #[expect(clippy::let_underscore_must_use, reason = "best-effort kill in drop")]
            let _ = child.start_kill();
            tokio::spawn(async move {
                #[expect(
                    clippy::let_underscore_must_use,
                    reason = "best-effort reaping in spawned task"
                )]
                let _ = child.wait().await;
            });
        }
    }
}

async fn rvs_ssh_connect_ABIS(
    proxy_addr: SocketAddr,
    user: &str,
    target: SocketAddr,
) -> std::io::Result<(
    impl tokio::io::AsyncRead + Send + Sync + 'static,
    impl tokio::io::AsyncWrite + Send + Sync + 'static,
)> {
    let std_cmd = rvs_build_ssh_args(proxy_addr, user, target);
    let mut process = tokio::process::Command::from(std_cmd).spawn()?;

    let stdin = process.stdin.take().expect("never: stdin was piped");
    let stdout = process.stdout.take().expect("never: stdout was piped");
    let stderr = process.stderr.take().expect("never: stderr was piped");

    let _guard = SshChildGuard::rvs_new(process);

    tokio::spawn(rvs_drain_stderr_ABI(stderr));

    let guarded_read = SshReadGuard {
        read: stdout,
        _guard,
    };
    Ok((guarded_read, stdin))
}

async fn rvs_drain_stderr_ABI(mut stderr: tokio::process::ChildStderr) {
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 4096];
    loop {
        match stderr.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let output = String::from_utf8_lossy(buf.get(..n).expect("never: n <= buf.len"));
                for line in output.lines() {
                    if !line.is_empty() {
                        tracing::warn!("ssh stderr: {line}");
                    }
                }
            }
            Err(e) => {
                tracing::debug!("ssh stderr read error: {e}");
                break;
            }
        }
    }
}

use std::pin::Pin;
use std::task::{Context, Poll};

struct SshReadGuard {
    read: tokio::process::ChildStdout,
    _guard: SshChildGuard,
}

impl tokio::io::AsyncRead for SshReadGuard {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let read = &mut self.get_mut().read;
        Pin::new(read).poll_read(cx, buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::write_snapshot;

    #[test]
    fn test_20260605_proxy_link_tag_display() {
        let proxy = ProxyConfig::Ssh {
            addr: "10.0.0.1:22".parse().unwrap(),
            user: "testuser".to_owned(),
        };
        let server: SocketAddr = "192.168.1.1:9999".parse().unwrap();
        let tag = ProxyLinkTag::rvs_new(proxy, server);
        let displayed = format!("{tag}");
        assert!(displayed.contains("10.0.0.1:22"));
        assert!(displayed.contains("192.168.1.1:9999"));
        write_snapshot(
            "test_20260605_proxy_link_tag_display",
            &format!("{displayed}\n"),
        );
    }

    #[test]
    fn test_20260605_proxy_link_tag_ordering() {
        let proxy1 = ProxyConfig::Socks4 {
            addr: "10.0.0.1:1080".parse().unwrap(),
        };
        let proxy2 = ProxyConfig::Socks4 {
            addr: "10.0.0.2:1080".parse().unwrap(),
        };
        let server: SocketAddr = "192.168.1.1:9999".parse().unwrap();
        let tag1 = ProxyLinkTag::rvs_new(proxy1, server);
        let tag2 = ProxyLinkTag::rvs_new(proxy2, server);
        assert_ne!(tag1, tag2);
        let ord = tag1.cmp(&tag2);
        write_snapshot(
            "test_20260605_proxy_link_tag_ordering",
            &format!("ord={ord:?}\n"),
        );
    }

    #[test]
    fn test_20260605_build_http_connect_request() {
        let target: SocketAddr = "10.0.0.1:80".parse().unwrap();
        let req = rvs_build_http_connect_request(target);
        assert!(req.starts_with("CONNECT 10.0.0.1:80 HTTP/1.1\r\n"));
        assert!(req.contains("Host: 10.0.0.1:80\r\n"));
        assert!(req.ends_with("\r\n\r\n"));
        write_snapshot(
            "test_20260605_build_http_connect_request",
            &format!("{req}\n"),
        );
    }

    #[test]
    fn test_20260605_parse_http_status_line() {
        let mut out = String::new();
        let r1 = rvs_parse_http_status_line("HTTP/1.1 200 OK\r\n");
        assert_eq!(r1, Some(200));
        out.push_str(&format!("200 OK => {r1:?}\n"));
        let r2 = rvs_parse_http_status_line("HTTP/1.0 407 Proxy Authentication Required\r\n");
        assert_eq!(r2, Some(407));
        out.push_str(&format!("407 => {r2:?}\n"));
        let r3 = rvs_parse_http_status_line("garbage");
        assert_eq!(r3, None);
        out.push_str(&format!("garbage => {r3:?}\n"));
        let r4 = rvs_parse_http_status_line("");
        assert_eq!(r4, None);
        out.push_str(&format!("empty => {r4:?}\n"));
        write_snapshot("test_20260605_parse_http_status_line", &out);
    }

    #[test]
    fn test_20260605_headers_end_offset() {
        let mut out = String::new();
        let buf = b"HTTP/1.1 200 OK\r\n\r\n";
        let r1 = rvs_headers_end_offset(buf, buf.len());
        assert_eq!(r1, Some(19));
        out.push_str(&format!("simple => {r1:?}\n"));

        let buf = b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n";
        let r2 = rvs_headers_end_offset(buf, buf.len());
        assert_eq!(r2, Some(38));
        out.push_str(&format!("with-header => {r2:?}\n"));

        let buf = b"HTTP/1.1 200 OK\r\n";
        let r3 = rvs_headers_end_offset(buf, buf.len());
        assert_eq!(r3, None);
        out.push_str(&format!("incomplete => {r3:?}\n"));
        write_snapshot("test_20260605_headers_end_offset", &out);
    }

    #[test]
    fn test_20260605_build_socks5_auth_args() {
        let mut out = String::new();
        let none_args = rvs_build_socks5_auth_args(None);
        assert!(!none_args.has_auth);
        out.push_str(&format!("no-auth => has_auth={}\n", none_args.has_auth));

        let auth = Socks5Auth::rvs_new("user".to_owned(), "pass".to_owned());
        let some_args = rvs_build_socks5_auth_args(Some(&auth));
        assert!(some_args.has_auth);
        assert_eq!(some_args.username, "user");
        assert_eq!(some_args.password, "pass");
        out.push_str(&format!(
            "with-auth => user={} pass={}\n",
            some_args.username, some_args.password
        ));
        write_snapshot("test_20260605_build_socks5_auth_args", &out);
    }

    #[test]
    fn test_20260605_build_ssh_args() {
        let proxy: SocketAddr = "10.0.0.1:22".parse().unwrap();
        let target: SocketAddr = "192.168.1.1:80".parse().unwrap();
        let cmd = rvs_build_ssh_args(proxy, "testuser", target);
        let program = cmd.get_program();
        assert_eq!(program, "ssh");
        write_snapshot("test_20260605_build_ssh_args", &format!("{program:?}\n"));
    }
}

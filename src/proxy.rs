use std::fmt;
use std::net::SocketAddr;
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProxyConfig {
    Socks4 {
        addr: SocketAddr,
    },
    Socks5 {
        addr: SocketAddr,
        auth: Option<Socks5Auth>,
    },
    Http {
        addr: SocketAddr,
    },
    Ssh {
        addr: SocketAddr,
        user: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Socks5Auth {
    username: String,
    password: String,
}

impl Socks5Auth {
    pub fn rvs_new(username: String, password: String) -> Self {
        debug_assert!(!username.is_empty(), "username must not be empty");
        Self { username, password }
    }

    pub fn rvs_username(&self) -> &str {
        &self.username
    }

    pub fn rvs_password(&self) -> &str {
        &self.password
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyParseError {
    pub input: String,
    pub reason: String,
}

impl fmt::Display for ProxyParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to parse proxy '{}': {}", self.input, self.reason)
    }
}

impl std::error::Error for ProxyParseError {}

impl ProxyConfig {
    pub fn rvs_addr(&self) -> SocketAddr {
        match self {
            ProxyConfig::Socks4 { addr } => *addr,
            ProxyConfig::Socks5 { addr, .. } => *addr,
            ProxyConfig::Http { addr } => *addr,
            ProxyConfig::Ssh { addr, .. } => *addr,
        }
    }

    pub fn rvs_socks5_auth(&self) -> Option<&Socks5Auth> {
        match self {
            ProxyConfig::Socks5 { auth, .. } => auth.as_ref(),
            _ => None,
        }
    }

    pub fn rvs_ssh_user(&self) -> Option<&str> {
        match self {
            ProxyConfig::Ssh { user, .. } => Some(user),
            _ => None,
        }
    }
}

impl FromStr for ProxyConfig {
    type Err = ProxyParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = |reason: &str| ProxyParseError {
            input: s.to_owned(),
            reason: reason.to_owned(),
        };

        let (scheme, rest) = s
            .split_once("://")
            .ok_or_else(|| err("missing '://' separator"))?;

        match scheme {
            "socks4" => {
                let addr: SocketAddr = rest
                    .parse()
                    .map_err(|e| err(&format!("invalid address: {e}")))?;
                Ok(ProxyConfig::Socks4 { addr })
            }
            "socks5" => {
                let (addr_part, auth) = rvs_parse_optional_auth(rest, &err)?;
                let addr: SocketAddr = addr_part
                    .parse()
                    .map_err(|e| err(&format!("invalid address: {e}")))?;
                Ok(ProxyConfig::Socks5 { addr, auth })
            }
            "http" => {
                let (addr_part, auth) = rvs_parse_optional_auth(rest, &err)?;
                if auth.is_some() {
                    return Err(err(
                        "HTTP proxy authentication is not supported; use format http://ip:port",
                    ));
                }
                let addr: SocketAddr = addr_part
                    .parse()
                    .map_err(|e| err(&format!("invalid address: {e}")))?;
                Ok(ProxyConfig::Http { addr })
            }
            "ssh" => {
                let (addr_part, user) = rvs_parse_user_before_at(
                    rest,
                    &err,
                    "ssh proxy requires user@host:port format",
                )?;
                let addr: SocketAddr = addr_part
                    .parse()
                    .map_err(|e| err(&format!("invalid address: {e}")))?;
                Ok(ProxyConfig::Ssh { addr, user })
            }
            _ => Err(err(&format!(
                "unknown scheme '{scheme}', expected one of: socks4, socks5, http, ssh"
            ))),
        }
    }
}

fn rvs_parse_optional_auth<'a>(
    rest: &'a str,
    err: &dyn Fn(&str) -> ProxyParseError,
) -> Result<(&'a str, Option<Socks5Auth>), ProxyParseError> {
    match rest.rfind('@') {
        Some(_) => {
            let (addr_part, user_pass) =
                rvs_parse_user_before_at(rest, err, "expected username:password before '@'")?;
            let (username, password) = user_pass
                .split_once(':')
                .ok_or_else(|| err("expected username:password before '@'"))?;
            if username.is_empty() {
                return Err(err("username cannot be empty"));
            }
            if password.is_empty() {
                return Err(err("password cannot be empty"));
            }
            Ok((
                addr_part,
                Some(Socks5Auth::rvs_new(
                    username.to_owned(),
                    password.to_owned(),
                )),
            ))
        }
        None => Ok((rest, None)),
    }
}

fn rvs_parse_user_before_at<'a>(
    rest: &'a str,
    err: &dyn Fn(&str) -> ProxyParseError,
    missing_at_msg: &str,
) -> Result<(&'a str, String), ProxyParseError> {
    match rest.rfind('@') {
        Some(pos) => {
            let user = rest.get(..pos).expect("never: pos from rfind is valid");
            let addr_part = rest.get(pos + 1..).expect("never: pos+1 is valid");
            if user.is_empty() {
                return Err(err("username cannot be empty"));
            }
            Ok((addr_part, user.to_owned()))
        }
        None => Err(err(missing_at_msg)),
    }
}

fn rvs_format_socks5_display(auth: &Option<Socks5Auth>, addr: SocketAddr) -> String {
    match auth {
        None => format!("socks5://{addr}"),
        Some(a) => format!("socks5://{}:***@{addr}", a.rvs_username()),
    }
}

pub fn rvs_format_proxy_exact(proxy: &ProxyConfig) -> String {
    match proxy {
        ProxyConfig::Socks4 { addr } => format!("socks4://{addr}"),
        ProxyConfig::Socks5 { addr, auth } => match auth {
            None => format!("socks5://{addr}"),
            Some(a) => format!("socks5://{}:{}@{addr}", a.rvs_username(), a.rvs_password()),
        },
        ProxyConfig::Http { addr } => format!("http://{addr}"),
        ProxyConfig::Ssh { addr, user } => format!("ssh://{user}@{addr}"),
    }
}

impl fmt::Display for ProxyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProxyConfig::Socks4 { addr } => write!(f, "socks4://{addr}"),
            ProxyConfig::Socks5 { addr, auth } => {
                write!(f, "{}", rvs_format_socks5_display(auth, *addr))
            }
            ProxyConfig::Http { addr } => write!(f, "http://{addr}"),
            ProxyConfig::Ssh { addr, user } => write!(f, "ssh://{user}@{addr}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::write_snapshot;

    #[test]
    fn test_20260605_parse_socks4() {
        let p: ProxyConfig = "socks4://127.0.0.1:1080"
            .parse()
            .expect("never: valid socks4");
        assert_eq!(
            p,
            ProxyConfig::Socks4 {
                addr: "127.0.0.1:1080".parse().unwrap()
            }
        );
        write_snapshot("test_20260605_parse_socks4", &format!("{p}\n"));
    }

    #[test]
    fn test_20260605_parse_socks5_no_auth() {
        let p: ProxyConfig = "socks5://127.0.0.1:1080"
            .parse()
            .expect("never: valid socks5");
        assert_eq!(
            p,
            ProxyConfig::Socks5 {
                addr: "127.0.0.1:1080".parse().unwrap(),
                auth: None
            }
        );
        write_snapshot("test_20260605_parse_socks5_no_auth", &format!("{p}\n"));
    }

    #[test]
    fn test_20260605_parse_socks5_with_auth() {
        let p: ProxyConfig = "socks5://user:pass@127.0.0.1:1080"
            .parse()
            .expect("never: valid socks5");
        let auth = p.rvs_socks5_auth().expect("never: has auth");
        assert_eq!(auth.rvs_username(), "user");
        assert_eq!(auth.rvs_password(), "pass");
        write_snapshot("test_20260605_parse_socks5_with_auth", &format!("{p}\n"));
    }

    #[test]
    fn test_20260605_parse_http_proxy() {
        let p: ProxyConfig = "http://127.0.0.1:8080".parse().expect("never: valid http");
        assert_eq!(
            p,
            ProxyConfig::Http {
                addr: "127.0.0.1:8080".parse().unwrap()
            }
        );
        write_snapshot("test_20260605_parse_http_proxy", &format!("{p}\n"));
    }

    #[test]
    fn test_20260605_parse_ssh_proxy() {
        let p: ProxyConfig = "ssh://jyi@10.0.0.1:22".parse().expect("never: valid ssh");
        assert_eq!(p.rvs_ssh_user().expect("never: has user"), "jyi");
        write_snapshot("test_20260605_parse_ssh_proxy", &format!("{p}\n"));
    }

    #[test]
    fn test_20260605_parse_bad_scheme() {
        let result: Result<ProxyConfig, ProxyParseError> = "ftp://127.0.0.1:21".parse();
        let err = result.err().expect("never: invalid scheme");
        assert!(err.reason.contains("unknown scheme"));
        write_snapshot("test_20260605_parse_bad_scheme", &format!("{err}\n"));
    }

    #[test]
    fn test_20260605_parse_missing_separator() {
        let result: Result<ProxyConfig, ProxyParseError> = "socks5:127.0.0.1:1080".parse();
        let err = result.err().expect("never: missing separator is invalid");
        write_snapshot("test_20260605_parse_missing_separator", &format!("{err}\n"));
    }

    #[test]
    fn test_20260605_parse_ssh_no_user() {
        let result: Result<ProxyConfig, ProxyParseError> = "ssh://10.0.0.1:22".parse();
        let err = result.err().expect("never: missing user is invalid");
        write_snapshot("test_20260605_parse_ssh_no_user", &format!("{err}\n"));
    }

    #[test]
    fn test_20260605_parse_http_with_auth_rejected() {
        let result: Result<ProxyConfig, ProxyParseError> =
            "http://user:pass@127.0.0.1:8080".parse();
        let err = result.err().expect("never: http auth is rejected");
        assert!(
            err.reason.contains("not supported"),
            "should mention not supported: {err}"
        );
        write_snapshot(
            "test_20260605_parse_http_with_auth_rejected",
            &format!("{err}\n"),
        );
    }

    #[test]
    fn test_20260605_parse_socks5_empty_password_rejected() {
        let result: Result<ProxyConfig, ProxyParseError> = "socks5://user:@127.0.0.1:1080".parse();
        let err = result.err().expect("never: empty password is rejected");
        assert!(
            err.reason.contains("password"),
            "should mention password: {err}"
        );
        write_snapshot(
            "test_20260605_parse_socks5_empty_password_rejected",
            &format!("{err}\n"),
        );
    }

    #[test]
    fn test_20260605_display_roundtrip() {
        let configs = [
            "socks4://127.0.0.1:1080",
            "socks5://127.0.0.1:1080",
            "socks5://user:pass@127.0.0.1:1080",
            "http://127.0.0.1:8080",
            "ssh://jyi@10.0.0.1:22",
        ];
        let mut out = String::new();
        for s in &configs {
            let p: ProxyConfig = s.parse().expect("never: valid config");
            let exact = rvs_format_proxy_exact(&p);
            let p2: ProxyConfig = exact.parse().expect("never: roundtrip");
            assert_eq!(p, p2);
            let displayed = format!("{p}");
            out.push_str(&format!("{s} -> {displayed}\n"));
        }
        write_snapshot("test_20260605_display_roundtrip", &out);
    }

    #[test]
    fn test_20260605_display_masks_password() {
        let p: ProxyConfig = "socks5://user:secret@127.0.0.1:1080"
            .parse()
            .expect("never: valid");
        let displayed = format!("{p}");
        assert!(
            !displayed.contains("secret"),
            "password should be masked in Display: {displayed}"
        );
        assert!(
            displayed.contains("***"),
            "password should show ***: {displayed}"
        );
        write_snapshot(
            "test_20260605_display_masks_password",
            &format!("{displayed}\n"),
        );
    }
}

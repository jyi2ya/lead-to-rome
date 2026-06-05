use std::net::SocketAddr;
use std::process::ExitCode;

use clap::Parser;

#[derive(Parser)]
#[command(
    name = "lead-to-rome",
    version,
    about = "TCP link aggregator with multi-proxy support"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand)]
enum Command {
    #[command(about = "Run as server: accept aggregated connections and forward to target")]
    Server {
        #[arg(
            long,
            short,
            default_value = "0.0.0.0:9000",
            help = "Address to listen on for aggregated connections"
        )]
        listen: SocketAddr,
        #[arg(long, short, help = "Target address to forward connections to")]
        target: SocketAddr,
    },
    #[command(about = "Run as client: accept local connections and proxy through aggregator")]
    Client {
        #[arg(
            long,
            short,
            default_value = "127.0.0.1:8080",
            help = "Local address to listen on"
        )]
        listen: SocketAddr,
        #[arg(long, short, help = "Server address to connect to")]
        server: SocketAddr,
        #[arg(
            long,
            short,
            help = "Proxy to use (repeatable). Format: socks4://ip:port, socks5://[user:pass@]ip:port, http://ip:port, ssh://user@ip:port"
        )]
        proxy: Vec<String>,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    tracing_subscriber::fmt::init();

    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            eprintln!("{e}");
            return ExitCode::FAILURE;
        }
    };

    match cli.command {
        Command::Server { listen, target } => {
            if let Err(e) = lead_to_rome::server::rvs_run_server_ABIS(listen, target).await {
                tracing::error!("server error: {e}");
                return ExitCode::FAILURE;
            }
        }
        Command::Client {
            listen,
            server,
            proxy,
        } => {
            let proxies = match rvs_parse_proxies(proxy) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("error: {e}");
                    return ExitCode::FAILURE;
                }
            };
            if proxies.is_empty() {
                eprintln!("error: at least one --proxy is required");
                return ExitCode::FAILURE;
            }
            if let Err(e) = lead_to_rome::client::rvs_run_client_ABIS(listen, server, proxies).await
            {
                tracing::error!("client error: {e}");
                return ExitCode::FAILURE;
            }
        }
    }

    ExitCode::SUCCESS
}

fn rvs_parse_proxies(
    raw: Vec<String>,
) -> Result<Vec<lead_to_rome::proxy::ProxyConfig>, lead_to_rome::proxy::ProxyParseError> {
    raw.iter().map(|s| s.parse()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_snapshot(name: &str, content: &str) {
        std::fs::write(format!("test_out/{name}.out"), content).expect("never: writeable");
    }

    #[test]
    fn test_20260605_parse_proxies_valid() {
        let input = vec![
            "socks4://127.0.0.1:1080".to_owned(),
            "socks5://user:pass@127.0.0.1:1081".to_owned(),
        ];
        let result = rvs_parse_proxies(input).expect("never: valid inputs");
        assert_eq!(result.len(), 2);
        write_snapshot(
            "test_20260605_parse_proxies_valid",
            &format!("{}\n", result.len()),
        );
    }

    #[test]
    fn test_20260605_parse_proxies_empty() {
        let result = rvs_parse_proxies(vec![]).expect("never: empty is valid");
        assert!(result.is_empty());
        write_snapshot("test_20260605_parse_proxies_empty", "count=0\n");
    }

    #[test]
    fn test_20260605_parse_proxies_invalid() {
        let input = vec!["bad://proxy".to_owned()];
        let result = rvs_parse_proxies(input);
        let err = result.err().expect("never: invalid proxy");
        write_snapshot("test_20260605_parse_proxies_invalid", &format!("{err}\n"));
    }
}

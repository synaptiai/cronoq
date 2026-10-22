//! Connecting to the server: endpoint, TLS and the bearer token.

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, bail, ensure};
use cronoq_proto::v1 as pb;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

/// How to reach the server.
#[derive(clap::Args, Clone, Debug)]
pub struct ConnArgs {
    /// Server gRPC endpoint (`http://` or `https://`).
    #[arg(
        long,
        global = true,
        env = "CRONOQ_SERVER",
        default_value = "http://127.0.0.1:7070"
    )]
    pub server: String,
    /// File holding the bearer token (default: the CRONOQ_TOKEN environment variable).
    #[arg(long, global = true, value_name = "PATH")]
    pub token_file: Option<PathBuf>,
    /// PEM CA bundle to verify the server with (default: the system roots).
    #[arg(long = "tls-ca", global = true, value_name = "PATH")]
    pub ca: Option<PathBuf>,
    /// PEM client certificate for mTLS (with --tls-key).
    #[arg(
        long = "tls-cert",
        global = true,
        value_name = "PATH",
        requires = "tls_key"
    )]
    pub cert: Option<PathBuf>,
    /// PEM private key of --tls-cert.
    #[arg(
        id = "tls_key",
        long = "tls-key",
        global = true,
        value_name = "PATH",
        requires = "cert"
    )]
    pub key: Option<PathBuf>,
    /// Server name to verify instead of the endpoint's host.
    #[arg(long = "tls-domain", global = true)]
    pub domain: Option<String>,
}

/// Adds the bearer token to every request.
#[derive(Clone)]
pub struct Auth(Option<MetadataValue<Ascii>>);

impl Interceptor for Auth {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        Ok(req)
    }
}

type Svc = InterceptedService<Channel, Auth>;

/// Clients for the three services, sharing one connection.
#[derive(Clone)]
pub struct Clients {
    pub tasks: pb::tasks_client::TasksClient<Svc>,
    pub admin: pb::admin_client::AdminClient<Svc>,
}

/// Largest message accepted from the server (task outputs ride in these).
const MAX_MESSAGE: usize = 64 << 20;

impl ConnArgs {
    fn token(&self) -> anyhow::Result<Option<String>> {
        let raw = match &self.token_file {
            Some(p) => Some(
                std::fs::read_to_string(p)
                    .with_context(|| format!("reading the token file {}", p.display()))?,
            ),
            None => std::env::var("CRONOQ_TOKEN").ok(),
        };
        Ok(raw.map(|t| t.trim().to_string()).filter(|t| !t.is_empty()))
    }

    fn endpoint(&self, has_token: bool) -> anyhow::Result<Endpoint> {
        let https = self.server.starts_with("https://");
        ensure!(
            https || self.server.starts_with("http://"),
            "--server must start with http:// or https://, not '{}'",
            self.server
        );
        // tonic only applies TLS to https:// endpoints; anything else would silently be
        // plaintext. The worker client in cronoq-server applies the same rules.
        let tls_flags = self.ca.is_some() || self.cert.is_some() || self.domain.is_some();
        ensure!(
            https || !tls_flags,
            "TLS options need an https:// server, not '{}'",
            self.server
        );
        if !https && has_token && !is_loopback_endpoint(&self.server) {
            eprintln!(
                "warning: the token is sent over plain http to {}; use https outside a trusted network",
                self.server
            );
        }
        let mut ep = Endpoint::from_shared(self.server.clone())?
            .connect_timeout(Duration::from_secs(10))
            .http2_keep_alive_interval(Duration::from_secs(20))
            .keep_alive_timeout(Duration::from_secs(20));
        if https {
            let mut cfg = ClientTlsConfig::new();
            cfg = match &self.ca {
                Some(p) => cfg.ca_certificate(Certificate::from_pem(read(p)?)),
                None => cfg.with_native_roots(),
            };
            if let (Some(c), Some(k)) = (&self.cert, &self.key) {
                cfg = cfg.identity(Identity::from_pem(read(c)?, read(k)?));
            }
            if let Some(d) = &self.domain {
                cfg = cfg.domain_name(d.clone());
            }
            ep = ep.tls_config(cfg)?;
        }
        Ok(ep)
    }

    /// Connect lazily: the first call opens the connection, and reconnects after failures.
    pub fn clients(&self) -> anyhow::Result<Clients> {
        let token = self.token()?;
        let channel = self.endpoint(token.is_some())?.connect_lazy();
        let auth = Auth(match token {
            Some(t) => {
                let mut v: MetadataValue<Ascii> = format!("Bearer {t}")
                    .parse()
                    .context("the token contains characters not allowed in a header")?;
                v.set_sensitive(true);
                Some(v)
            }
            None => None,
        });
        Ok(Clients {
            tasks: pb::tasks_client::TasksClient::with_interceptor(channel.clone(), auth.clone())
                .max_decoding_message_size(MAX_MESSAGE),
            admin: pb::admin_client::AdminClient::with_interceptor(channel, auth)
                .max_decoding_message_size(MAX_MESSAGE),
        })
    }
}

fn read(p: &PathBuf) -> anyhow::Result<Vec<u8>> {
    std::fs::read(p).with_context(|| format!("reading {}", p.display()))
}

/// Whether the endpoint's host is loopback (a copy of the check in cronoq-server's worker).
fn is_loopback_endpoint(endpoint: &str) -> bool {
    let rest = endpoint.split_once("://").map_or(endpoint, |(_, r)| r);
    let host = rest.split(['/', '?']).next().unwrap_or("");
    let host = match host.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""),
        None => host.rsplit_once(':').map_or(host, |(h, _)| h),
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// A server error as one line, without tonic's debug details.
pub fn describe(s: &tonic::Status) -> String {
    let code = format!("{:?}", s.code());
    let mut name = String::new();
    for (i, c) in code.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            name.push('_');
        }
        name.push(c.to_ascii_lowercase());
    }
    if s.message().is_empty() {
        name
    } else {
        format!("{name}: {}", s.message())
    }
}

/// Turn a status into an error with a readable message.
pub fn rpc<T>(r: Result<tonic::Response<T>, tonic::Status>) -> anyhow::Result<T> {
    match r {
        Ok(r) => Ok(r.into_inner()),
        Err(s) => bail!("{}", describe(&s)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_hosts() {
        for e in [
            "http://127.0.0.1:7070",
            "http://localhost:7070",
            "http://[::1]:7070",
        ] {
            assert!(is_loopback_endpoint(e), "{e}");
        }
        for e in [
            "http://10.0.0.1:7070",
            "http://example.com",
            "http://[2001:db8::1]:1",
        ] {
            assert!(!is_loopback_endpoint(e), "{e}");
        }
    }

    #[test]
    fn tls_options_need_https() {
        let args = ConnArgs {
            server: "http://example.com:7070".into(),
            token_file: None,
            ca: Some("ca.pem".into()),
            cert: None,
            key: None,
            domain: None,
        };
        let e = args.endpoint(false).unwrap_err().to_string();
        assert!(e.contains("need an https://"), "{e}");
        let bad = ConnArgs {
            server: "127.0.0.1:7070".into(),
            ca: None,
            ..args
        };
        assert!(bad.endpoint(false).is_err());
    }

    #[test]
    fn statuses_read_like_grpc_codes() {
        let s = tonic::Status::permission_denied("no");
        assert_eq!(describe(&s), "permission_denied: no");
        assert_eq!(
            describe(&tonic::Status::new(tonic::Code::FailedPrecondition, "")),
            "failed_precondition"
        );
    }
}

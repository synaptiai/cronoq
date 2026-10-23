//! Calls to the servers of other shards.
//!
//! A server forwards a client's call to the shard that serves it with the caller's own bearer
//! token, so the other shard authenticates and authorizes the call again: servers do not trust
//! each other. Forwarded calls carry [`FORWARDED_HEADER`], and a server answers those from its
//! own shard only, so calls cannot loop. Callers that authenticated with a client certificate
//! have no token to forward; their calls are refused with the shard's endpoint instead.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::Context;
use cronoq_proto::v1 as pb;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};
use tonic::{Code, Status};

use crate::auth::{FORWARDED_HEADER, Principal};
use crate::config::{LimitsConfig, PeerTlsConfig};
use crate::shard::Shard;

/// Adds the caller's token and the forwarded marker to every request.
#[derive(Clone)]
pub struct Forward(Option<MetadataValue<Ascii>>);

impl Interceptor for Forward {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        req.metadata_mut()
            .insert(FORWARDED_HEADER, MetadataValue::from_static("1"));
        Ok(req)
    }
}

type Svc = InterceptedService<Channel, Forward>;

/// Clients for one shard, acting for one caller.
pub struct PeerClients {
    /// `cronoq.v1.Tasks` of the shard.
    pub tasks: pb::tasks_client::TasksClient<Svc>,
    /// `cronoq.v1.Admin` of the shard.
    pub admin: pb::admin_client::AdminClient<Svc>,
}

/// Connections to the other shards' endpoints, opened on first use.
pub struct Peers {
    tls: ClientTlsConfig,
    auth_enabled: bool,
    plain_http: bool,
    max_message: usize,
    channels: Mutex<HashMap<u8, Channel>>,
}

impl Peers {
    /// Peers reached with `tls` (`https://` endpoints; system roots by default).
    /// With `plain_http`, tokens are also forwarded to `http://` endpoints on other hosts.
    pub fn new(
        tls: Option<&PeerTlsConfig>,
        auth_enabled: bool,
        plain_http: bool,
        limits: &LimitsConfig,
    ) -> anyhow::Result<Peers> {
        let mut cfg = ClientTlsConfig::new();
        cfg = match tls.and_then(|t| t.ca.as_ref()) {
            Some(p) => cfg.ca_certificate(Certificate::from_pem(read(p)?)),
            None => cfg.with_native_roots(),
        };
        if let Some(t) = tls
            && let (Some(c), Some(k)) = (&t.cert, &t.key)
        {
            cfg = cfg.identity(Identity::from_pem(read(c)?, read(k)?));
        }
        Ok(Peers {
            tls: cfg,
            auth_enabled,
            plain_http,
            max_message: limits.max_message_bytes,
            channels: Mutex::new(HashMap::new()),
        })
    }

    fn channel(&self, shard: &Shard) -> Result<Channel, Status> {
        let mut channels = self.channels.lock().unwrap();
        if let Some(c) = channels.get(&shard.number) {
            return Ok(c.clone());
        }
        let unreachable = |e: tonic::transport::Error| {
            Status::unavailable(format!("shard '{}' is unreachable: {e}", shard.name))
        };
        let mut ep = Endpoint::from_shared(shard.endpoint.to_string())
            .map_err(unreachable)?
            .connect_timeout(Duration::from_secs(5))
            .http2_keep_alive_interval(Duration::from_secs(20))
            .keep_alive_timeout(Duration::from_secs(20));
        if shard.endpoint.starts_with("https://") {
            ep = ep.tls_config(self.tls.clone()).map_err(unreachable)?;
        }
        let c = ep.connect_lazy();
        channels.insert(shard.number, c.clone());
        Ok(c)
    }

    /// Whether a token may be sent to `shard`: over TLS, to a loopback address, or anywhere
    /// with `server.insecure_peer_http`.
    pub fn may_carry_tokens(&self, shard: &Shard) -> bool {
        self.plain_http
            || shard.endpoint.starts_with("https://")
            || is_loopback_endpoint(&shard.endpoint)
    }

    /// Clients that act for `principal` on `shard`, or `None` when its call cannot be
    /// forwarded: it authenticated with a client certificate, or its token would cross the
    /// network unencrypted.
    pub fn clients(
        &self,
        shard: &Shard,
        principal: &Principal,
    ) -> Result<Option<PeerClients>, Status> {
        let auth = match &principal.bearer {
            Some(_) if !self.may_carry_tokens(shard) => return Ok(None),
            Some(b) => {
                let mut v: MetadataValue<Ascii> = b
                    .header()
                    .parse()
                    .map_err(|_| Status::internal("token is not a valid header"))?;
                v.set_sensitive(true);
                Some(v)
            }
            None if self.auth_enabled => return Ok(None),
            None => None,
        };
        let channel = self.channel(shard)?;
        let forward = Forward(auth);
        Ok(Some(PeerClients {
            tasks: pb::tasks_client::TasksClient::with_interceptor(
                channel.clone(),
                forward.clone(),
            )
            .max_decoding_message_size(self.max_message)
            .max_encoding_message_size(self.max_message),
            admin: pb::admin_client::AdminClient::with_interceptor(channel, forward)
                .max_decoding_message_size(self.max_message)
                .max_encoding_message_size(self.max_message),
        }))
    }
}

/// Whether the endpoint's host is `localhost` or a loopback address.
fn is_loopback_endpoint(endpoint: &str) -> bool {
    let Ok(uri) = endpoint.parse::<http::Uri>() else {
        return false;
    };
    let host = uri.host().unwrap_or("");
    let host = host.trim_start_matches('[').trim_end_matches(']');
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

fn read(p: &std::path::Path) -> anyhow::Result<Vec<u8>> {
    std::fs::read(p).with_context(|| format!("reading {}", p.display()))
}

/// A peer's answer: its message, or its error with transport failures named after the shard.
pub fn answer<T>(shard: &Shard, r: Result<tonic::Response<T>, Status>) -> Result<T, Status> {
    match r {
        Ok(r) => Ok(r.into_inner()),
        Err(s) if s.code() == Code::Unavailable || s.code() == Code::Unknown => {
            Err(Status::unavailable(format!(
                "shard '{}' is unavailable: {}",
                shard.name,
                s.message()
            )))
        }
        Err(s) => Err(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shard(endpoint: &str) -> Shard {
        Shard {
            name: "gpu".into(),
            number: 2,
            endpoint: endpoint.into(),
        }
    }

    #[test]
    fn tokens_cross_the_network_only_encrypted() {
        let limits = LimitsConfig::default();
        let strict = Peers::new(None, true, false, &limits).unwrap();
        assert!(strict.may_carry_tokens(&shard("https://gpu.internal:7070")));
        assert!(strict.may_carry_tokens(&shard("http://127.0.0.1:7070")));
        assert!(strict.may_carry_tokens(&shard("http://[::1]:7070")));
        assert!(strict.may_carry_tokens(&shard("http://localhost:7070")));
        assert!(!strict.may_carry_tokens(&shard("http://gpu.internal:7070")));
        assert!(!strict.may_carry_tokens(&shard("http://10.0.0.7:7070")));
        let p = Principal {
            bearer: Some(crate::auth::Bearer::for_tests("Bearer t")),
            ..Principal::anonymous_admin()
        };
        assert!(
            strict
                .clients(&shard("http://gpu.internal:7070"), &p)
                .unwrap()
                .is_none()
        );
        let open = Peers::new(None, true, true, &limits).unwrap();
        assert!(open.may_carry_tokens(&shard("http://gpu.internal:7070")));
    }
}

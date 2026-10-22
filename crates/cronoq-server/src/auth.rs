//! Authentication (bearer tokens, client certificates) and authorization (roles, pool lists).
//!
//! Tokens are kept only as SHA-256 digests and compared in constant time. A request is
//! authenticated by its `authorization: Bearer <token>` header or, with mTLS, by a client
//! certificate whose DNS subject alternative name is mapped to a role in `[[auth.certs]]`.

use std::fmt;
use std::sync::Arc;

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::config::{AuthConfig, Role};

/// Minimum accepted token length.
pub const MIN_TOKEN_LEN: usize = 16;
/// Token length below which a warning is logged.
pub const RECOMMENDED_TOKEN_LEN: usize = 32;

/// An authenticated caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    /// Granted role.
    pub role: Role,
    /// Pools the caller may use (`None` = all).
    pub pools: Option<Arc<[Arc<str>]>>,
    /// Worker labels a worker with these credentials may claim, as `name=value` pairs
    /// (`None` = any).
    pub labels: Option<cronoq::Labels>,
    /// Tenant these credentials submit for (`None` = the caller names it).
    pub tenant: Option<Arc<str>>,
}

impl Principal {
    /// Unrestricted administrator (used when authentication is disabled).
    pub fn anonymous_admin() -> Principal {
        Principal {
            role: Role::Admin,
            pools: None,
            labels: None,
            tenant: None,
        }
    }

    /// Whether this principal may act as `required`.
    pub fn has_role(&self, required: Role) -> bool {
        self.role == Role::Admin || self.role == required
    }

    /// Whether a worker with these credentials may claim every one of `labels`.
    pub fn may_claim_labels(&self, labels: &[(Arc<str>, Arc<str>)]) -> bool {
        self.labels
            .as_ref()
            .is_none_or(|allowed| labels.iter().all(|l| allowed.contains(l)))
    }

    /// Whether this principal may use `pool`.
    pub fn may_use_pool(&self, pool: &str) -> bool {
        self.pools
            .as_ref()
            .is_none_or(|ps| ps.iter().any(|p| &**p == pool))
    }
}

struct TokenEntry {
    digest: [u8; 32],
    principal: Principal,
}

struct CertEntry {
    dns_name: String,
    principal: Principal,
}

/// Token and certificate tables. With no tokens and no certificate mappings, authentication is
/// disabled and every request acts as an unrestricted admin.
#[derive(Clone, Default)]
pub struct Auth {
    tokens: Arc<Vec<TokenEntry>>,
    certs: Arc<Vec<CertEntry>>,
}

impl fmt::Debug for Auth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Auth")
            .field("tokens", &self.tokens.len())
            .field("cert_mappings", &self.certs.len())
            .finish()
    }
}

/// Parse `name=value` label pairs from the configuration.
fn labels(v: &Option<Vec<String>>) -> anyhow::Result<Option<cronoq::Labels>> {
    let Some(v) = v else { return Ok(None) };
    v.iter()
        .map(|l| match l.split_once('=') {
            Some((k, val)) if !k.is_empty() => Ok((Arc::from(k), Arc::from(val))),
            _ => Err(anyhow::anyhow!(
                "auth label '{l}' must have the form name=value"
            )),
        })
        .collect::<anyhow::Result<Vec<_>>>()
        .map(|v| Some(v.into()))
}

fn digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn pools(list: &Option<Vec<String>>) -> Option<Arc<[Arc<str>]>> {
    list.as_ref()
        .map(|ps| ps.iter().map(|p| Arc::from(p.as_str())).collect())
}

impl Auth {
    /// Build from configuration, reading `token_env` variables.
    pub fn from_config(cfg: &AuthConfig) -> anyhow::Result<Auth> {
        let mut tokens: Vec<TokenEntry> = Vec::new();
        for t in &cfg.tokens {
            let value = match (&t.token, &t.token_env) {
                (Some(v), _) => v.clone(),
                (None, Some(env)) => std::env::var(env).map_err(|_| {
                    anyhow::anyhow!("environment variable {env} (auth token) is not set")
                })?,
                (None, None) => anyhow::bail!("auth token needs `token` or `token_env`"),
            };
            let value = value.trim().to_string();
            anyhow::ensure!(
                value.len() >= MIN_TOKEN_LEN,
                "auth tokens must be at least {MIN_TOKEN_LEN} characters ({} role token is shorter)",
                t.role.name()
            );
            if value.len() < RECOMMENDED_TOKEN_LEN {
                tracing::warn!(
                    role = t.role.name(),
                    "auth token is shorter than {RECOMMENDED_TOKEN_LEN} characters; use at least 32 random characters"
                );
            }
            let d = digest(&value);
            anyhow::ensure!(
                !tokens.iter().any(|e| bool::from(e.digest.ct_eq(&d))),
                "duplicate auth token (each token must be unique)"
            );
            tokens.push(TokenEntry {
                digest: d,
                principal: Principal {
                    role: t.role,
                    pools: pools(&t.pools),
                    labels: labels(&t.labels)?,
                    tenant: t.tenant.as_deref().map(Arc::from),
                },
            });
        }
        let certs = cfg
            .certs
            .iter()
            .map(|c| {
                Ok(CertEntry {
                    dns_name: c.dns_name.to_ascii_lowercase(),
                    principal: Principal {
                        role: c.role,
                        pools: pools(&c.pools),
                        labels: labels(&c.labels)?,
                        tenant: c.tenant.as_deref().map(Arc::from),
                    },
                })
            })
            .collect::<anyhow::Result<_>>()?;
        Ok(Auth {
            tokens: Arc::new(tokens),
            certs: Arc::new(certs),
        })
    }

    /// No authentication.
    pub fn disabled() -> Auth {
        Auth::default()
    }

    /// Whether any credential is configured.
    pub fn enabled(&self) -> bool {
        !self.tokens.is_empty() || !self.certs.is_empty()
    }

    fn token(&self, token: &str) -> Option<&Principal> {
        let d = digest(token);
        // Compare against every entry so timing does not depend on which one matches.
        let mut found = None;
        for e in self.tokens.iter() {
            if bool::from(e.digest.ct_eq(&d)) {
                found = Some(&e.principal);
            }
        }
        found
    }

    /// Principal for a client certificate (DER), by DNS subject alternative name.
    pub fn principal_for_cert(&self, der: &[u8]) -> Option<Principal> {
        if self.certs.is_empty() {
            return None;
        }
        let cert = rustls_pki_types::CertificateDer::from(der);
        let ee = webpki::EndEntityCert::try_from(&cert).ok()?;
        let names: Vec<String> = ee
            .valid_dns_names()
            .map(|n| n.to_ascii_lowercase())
            .collect();
        self.certs
            .iter()
            .find(|c| names.contains(&c.dns_name))
            .map(|c| c.principal.clone())
    }

    /// Authenticate a request from its `authorization` header and, if any, the principal derived
    /// from its client certificate, and check the required role.
    pub fn authorize(
        &self,
        header: Option<&str>,
        cert: Option<&Principal>,
        required: Role,
    ) -> Result<Principal, AuthError> {
        if !self.enabled() {
            return Ok(Principal::anonymous_admin());
        }
        let p = match header {
            Some(h) => {
                let token = h
                    .strip_prefix("Bearer ")
                    .or_else(|| h.strip_prefix("bearer "))
                    .ok_or(AuthError::Missing)?;
                self.token(token.trim())
                    .cloned()
                    .ok_or(AuthError::Invalid)?
            }
            None => cert.cloned().ok_or(AuthError::Missing)?,
        };
        if p.has_role(required) {
            Ok(p)
        } else {
            Err(AuthError::Forbidden)
        }
    }

    /// Check a header against a required role (no client certificate).
    pub fn check(&self, header: Option<&str>, required: Role) -> Result<Principal, AuthError> {
        self.authorize(header, None, required)
    }
}

/// Why a request was refused.
#[derive(Debug, PartialEq, Eq)]
pub enum AuthError {
    /// No credentials.
    Missing,
    /// Unknown token.
    Invalid,
    /// Valid credentials without the required role.
    Forbidden,
}

impl AuthError {
    /// gRPC status for this error.
    pub fn to_status(&self) -> tonic::Status {
        match self {
            AuthError::Missing => tonic::Status::unauthenticated(
                "missing credentials (bearer token or client certificate)",
            ),
            AuthError::Invalid => tonic::Status::unauthenticated("invalid token"),
            AuthError::Forbidden => {
                tonic::Status::permission_denied("credentials lack the required role")
            }
        }
    }
}

/// Authenticate a gRPC request (bearer token or mapped client certificate).
pub fn check_grpc<T>(
    auth: &Auth,
    req: &tonic::Request<T>,
    role: Role,
) -> Result<Principal, tonic::Status> {
    let header = req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok());
    let cert = if auth.certs.is_empty() {
        None
    } else {
        req.peer_certs().and_then(|certs| {
            certs
                .first()
                .and_then(|c| auth.principal_for_cert(c.as_ref()))
        })
    };
    auth.authorize(header, cert.as_ref(), role)
        .map_err(|e| e.to_status())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TokenConfig;

    const C: &str = "client-token-0123456789abcdef0123";
    const A: &str = "admin-token-0123456789abcdef01234";

    fn tok(t: &str, role: Role, pools: Option<Vec<&str>>) -> TokenConfig {
        TokenConfig {
            token: Some(t.into()),
            token_env: None,
            role,
            pools: pools.map(|p| p.into_iter().map(String::from).collect()),
            labels: None,
            tenant: None,
        }
    }

    #[test]
    fn roles_and_pools() {
        let auth = Auth::from_config(&AuthConfig {
            tokens: vec![
                tok(C, Role::Client, Some(vec!["a"])),
                tok(A, Role::Admin, None),
            ],
            certs: vec![],
        })
        .unwrap();
        let c = auth
            .check(Some(&format!("Bearer {C}")), Role::Client)
            .unwrap();
        assert!(c.may_use_pool("a") && !c.may_use_pool("b"));
        assert_eq!(
            auth.check(Some(&format!("Bearer {C}")), Role::Worker),
            Err(AuthError::Forbidden)
        );
        assert_eq!(
            auth.check(Some(&format!("Bearer {C}")), Role::Metrics),
            Err(AuthError::Forbidden)
        );
        assert!(
            auth.check(Some(&format!("Bearer {A}")), Role::Worker)
                .is_ok()
        );
        assert_eq!(
            auth.check(Some("Bearer xxxxxxxxxxxxxxxxxxxx"), Role::Client),
            Err(AuthError::Invalid)
        );
        assert_eq!(auth.check(None, Role::Client), Err(AuthError::Missing));
        assert_eq!(
            Auth::disabled().check(None, Role::Admin),
            Ok(Principal::anonymous_admin())
        );
    }

    #[test]
    fn rejects_short_and_duplicate_tokens() {
        let short = Auth::from_config(&AuthConfig {
            tokens: vec![tok("short", Role::Client, None)],
            certs: vec![],
        });
        assert!(short.is_err());
        let dup = Auth::from_config(&AuthConfig {
            tokens: vec![tok(A, Role::Admin, None), tok(A, Role::Client, None)],
            certs: vec![],
        });
        assert!(
            dup.is_err(),
            "a duplicate must not silently downgrade the admin token"
        );
    }
}

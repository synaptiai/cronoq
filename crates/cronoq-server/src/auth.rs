//! Bearer-token authentication with roles.

use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::config::{AuthConfig, Role};

/// Token table. Empty means authentication is disabled.
#[derive(Clone, Debug, Default)]
pub struct Auth {
    tokens: Arc<FxHashMap<String, Role>>,
}

impl Auth {
    pub fn from_config(cfg: &AuthConfig) -> anyhow::Result<Auth> {
        let mut tokens = FxHashMap::default();
        for t in &cfg.tokens {
            let value = match (&t.token, &t.token_env) {
                (Some(v), _) => v.clone(),
                (None, Some(env)) => std::env::var(env)
                    .map_err(|_| anyhow::anyhow!("environment variable {env} is not set"))?,
                (None, None) => unreachable!("validated"),
            };
            anyhow::ensure!(!value.is_empty(), "empty auth token");
            tokens.insert(value, t.role);
        }
        Ok(Auth {
            tokens: Arc::new(tokens),
        })
    }

    pub fn disabled() -> Auth {
        Auth::default()
    }

    pub fn enabled(&self) -> bool {
        !self.tokens.is_empty()
    }

    /// Check an `authorization` header value against a required role. Admin may do anything.
    pub fn check(&self, header: Option<&str>, required: Role) -> Result<(), AuthError> {
        if !self.enabled() {
            return Ok(());
        }
        let token = header
            .and_then(|h| {
                h.strip_prefix("Bearer ")
                    .or_else(|| h.strip_prefix("bearer "))
            })
            .ok_or(AuthError::Missing)?;
        match self.tokens.get(token.trim()) {
            Some(Role::Admin) => Ok(()),
            Some(r) if *r == required => Ok(()),
            Some(_) => Err(AuthError::Forbidden),
            None => Err(AuthError::Invalid),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum AuthError {
    Missing,
    Invalid,
    Forbidden,
}

impl AuthError {
    pub fn to_status(&self) -> tonic::Status {
        match self {
            AuthError::Missing => tonic::Status::unauthenticated("missing bearer token"),
            AuthError::Invalid => tonic::Status::unauthenticated("invalid token"),
            AuthError::Forbidden => {
                tonic::Status::permission_denied("token lacks the required role")
            }
        }
    }
}

/// gRPC check helper.
pub fn check_grpc<T>(
    auth: &Auth,
    req: &tonic::Request<T>,
    role: Role,
) -> Result<(), tonic::Status> {
    let header = req
        .metadata()
        .get("authorization")
        .and_then(|v| v.to_str().ok());
    auth.check(header, role).map_err(|e| e.to_status())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TokenConfig;

    #[test]
    fn roles() {
        let auth = Auth::from_config(&AuthConfig {
            tokens: vec![
                TokenConfig {
                    token: Some("c".into()),
                    token_env: None,
                    role: Role::Client,
                },
                TokenConfig {
                    token: Some("a".into()),
                    token_env: None,
                    role: Role::Admin,
                },
            ],
        })
        .unwrap();
        assert_eq!(auth.check(Some("Bearer c"), Role::Client), Ok(()));
        assert_eq!(
            auth.check(Some("Bearer c"), Role::Worker),
            Err(AuthError::Forbidden)
        );
        assert_eq!(auth.check(Some("Bearer a"), Role::Worker), Ok(()));
        assert_eq!(
            auth.check(Some("Bearer x"), Role::Client),
            Err(AuthError::Invalid)
        );
        assert_eq!(auth.check(None, Role::Client), Err(AuthError::Missing));
        assert_eq!(Auth::disabled().check(None, Role::Admin), Ok(()));
    }
}

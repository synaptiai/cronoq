//! Webhook scaler: forwards desired worker counts to an external orchestrator.

use async_trait::async_trait;
use cronoq::{ScaleError, Scaler};

/// Posts `{"pool": ..., "desired": n}` to a URL whenever a pool's target capacity changes.
/// Capacity itself follows from workers connecting, not from this call.
pub struct WebhookScaler {
    url: String,
    token: Option<String>,
    client: reqwest::Client,
}

impl std::fmt::Debug for WebhookScaler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookScaler")
            .field("url", &self.url)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl WebhookScaler {
    /// Scaler posting to `url`, with an optional bearer token.
    pub fn new(url: String, token: Option<String>) -> anyhow::Result<Self> {
        let parsed = reqwest::Url::parse(&url)
            .map_err(|e| anyhow::anyhow!("scaler webhook url '{url}': {e}"))?;
        match parsed.scheme() {
            "https" => {}
            "http" => {
                let local = matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
                if !local {
                    tracing::warn!(url = %url, "scaler webhook uses plain http; use https outside a trusted network");
                    if token.is_some() {
                        tracing::warn!("the scaler webhook token is sent over plain http");
                    }
                }
            }
            s => anyhow::bail!("scaler webhook url must be http or https, not {s}"),
        }
        // reqwest is built without a bundled crypto provider; use ring, like the listeners. An
        // error means a provider is already installed, which is fine.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(WebhookScaler { url, token, client })
    }
}

#[async_trait]
impl Scaler for WebhookScaler {
    async fn scale_to(&self, pool: &str, desired: u32) -> Result<u32, ScaleError> {
        let mut req = self
            .client
            .post(&self.url)
            .json(&serde_json::json!({ "pool": pool, "desired": desired }));
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| ScaleError::Other(e.without_url().to_string()))?;
        if !resp.status().is_success() {
            return Err(ScaleError::Other(format!(
                "scaler webhook returned {}",
                resp.status()
            )));
        }
        // Capacity follows from workers connecting, not from this call.
        Err(ScaleError::External)
    }
}

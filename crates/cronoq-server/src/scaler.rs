//! Webhook scaler: forwards desired worker counts to an external orchestrator.

use async_trait::async_trait;
use cronoq::{ScaleError, Scaler};

pub struct WebhookScaler {
    url: String,
    token: Option<String>,
    client: reqwest::Client,
}

impl WebhookScaler {
    pub fn new(url: String, token: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .expect("http client");
        WebhookScaler { url, token, client }
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
            .map_err(|e| ScaleError::Other(e.to_string()))?;
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

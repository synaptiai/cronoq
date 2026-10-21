//! Scalers turn the autoscaler's desired capacity into workers.

use async_trait::async_trait;

use crate::error::ScaleError;

/// Carries out capacity changes requested by the autoscaler.
///
/// Responses are applied in request order: a slow response to an older request never
/// overrides a newer one. A panic inside `scale_to` is logged and ignored.
#[async_trait]
pub trait Scaler: Send + Sync + 'static {
    /// Request `desired` workers for `pool`; return the capacity available now.
    async fn scale_to(&self, pool: &str, desired: u32) -> Result<u32, ScaleError>;
}

/// In-process concurrency slots: capacity follows the request immediately.
#[derive(Clone, Copy, Debug, Default)]
pub struct SlotScaler;

#[async_trait]
impl Scaler for SlotScaler {
    async fn scale_to(&self, _pool: &str, desired: u32) -> Result<u32, ScaleError> {
        Ok(desired)
    }
}

/// Capacity is managed elsewhere (e.g. KEDA/HPA reading the desired-workers metric, or
/// remote workers registering); report it with `Scheduler::set_capacity`.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExternalScaler;

#[async_trait]
impl Scaler for ExternalScaler {
    async fn scale_to(&self, _pool: &str, _desired: u32) -> Result<u32, ScaleError> {
        Err(ScaleError::External)
    }
}

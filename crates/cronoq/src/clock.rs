//! Mapping between engine time, tokio instants and wall-clock time.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cronoq_core::Time;
use tokio::time::Instant;

/// Engine clock anchored at scheduler start. Uses tokio's clock, so paused test time works.
#[derive(Copy, Clone, Debug)]
pub struct Clock {
    base: Instant,
    unix_nanos_at_base: i64,
}

impl Clock {
    pub fn start() -> Clock {
        let unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as i64;
        Clock {
            base: Instant::now(),
            unix_nanos_at_base: unix,
        }
    }
    #[inline]
    pub fn now(&self) -> Time {
        Time(self.base.elapsed().as_nanos() as u64)
    }
    #[inline]
    pub fn instant(&self, t: Time) -> Instant {
        self.base + Duration::from_nanos(t.0)
    }
    pub fn unix_nanos_at_zero(&self) -> i64 {
        self.unix_nanos_at_base
    }
    /// Engine time for a wall-clock time (clamped at the engine epoch).
    pub fn from_system(&self, t: SystemTime) -> Time {
        let unix = match t.duration_since(UNIX_EPOCH) {
            Ok(d) => d.as_nanos() as i64,
            Err(e) => -(e.duration().as_nanos() as i64),
        };
        Time(unix.saturating_sub(self.unix_nanos_at_base).max(0) as u64)
    }
    pub fn to_system(&self, t: Time) -> SystemTime {
        let unix = self.unix_nanos_at_base.saturating_add(t.0 as i64);
        if unix >= 0 {
            UNIX_EPOCH + Duration::from_nanos(unix as u64)
        } else {
            UNIX_EPOCH - Duration::from_nanos(unix.unsigned_abs())
        }
    }
}

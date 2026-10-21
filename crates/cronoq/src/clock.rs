//! Mapping between engine time, tokio instants and wall-clock time.
//!
//! Engine time ([`Time`]) is **nanoseconds since the Unix epoch**. The clock reads the wall
//! clock once at start and then advances with tokio's monotonic clock, so engine time never
//! goes backwards (NTP steps after start are not followed) and paused test time works.
//! Persisted timestamps are therefore plain Unix nanoseconds and survive restarts exactly.
//! `u64` nanoseconds cover 1970 to 2554; conversions saturate at both ends.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cronoq_core::Time;
use tokio::time::Instant;

/// How far ahead [`Clock::instant`] maps unreachable times (so timers stay valid).
const FAR_FUTURE: Duration = Duration::from_secs(100 * 365 * 86_400);

/// Engine clock: Unix nanoseconds, advanced by tokio's monotonic clock.
#[derive(Copy, Clone, Debug)]
pub struct Clock {
    base: Instant,
    unix_nanos_at_base: u64,
}

impl Clock {
    /// Anchor the clock at the current wall-clock time.
    pub fn start() -> Clock {
        Clock {
            base: Instant::now(),
            unix_nanos_at_base: unix_nanos(SystemTime::now()),
        }
    }

    /// Current engine time.
    #[inline]
    pub fn now(&self) -> Time {
        let elapsed = self.base.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        Time(self.unix_nanos_at_base.saturating_add(elapsed))
    }

    /// The tokio instant at which engine time `t` is reached (far future if unrepresentable).
    #[inline]
    pub fn instant(&self, t: Time) -> Instant {
        let ahead = Duration::from_nanos(t.0.saturating_sub(self.unix_nanos_at_base));
        self.base
            .checked_add(ahead.min(FAR_FUTURE))
            .unwrap_or(self.base)
    }

    /// Wall-clock time (Unix nanoseconds) that engine time zero corresponds to: always `0`.
    pub fn unix_nanos_at_zero(&self) -> i64 {
        0
    }

    /// Engine time for a wall-clock time (saturating: before 1970 maps to zero, beyond
    /// 2554 to [`Time::MAX`]).
    pub fn from_system(&self, t: SystemTime) -> Time {
        Time(unix_nanos(t))
    }

    /// Wall-clock time for an engine time (saturating at the platform's `SystemTime` range).
    pub fn to_system(&self, t: Time) -> SystemTime {
        to_system(t)
    }
}

/// Saturating Unix nanoseconds of a wall-clock time.
pub(crate) fn unix_nanos(t: SystemTime) -> u64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_nanos().min(u64::MAX as u128) as u64,
        Err(_) => 0,
    }
}

/// Saturating conversion of engine time to `SystemTime`.
pub(crate) fn to_system(t: Time) -> SystemTime {
    let d = Duration::from_nanos(t.0);
    UNIX_EPOCH.checked_add(d).unwrap_or_else(|| {
        // Platforms with a narrower SystemTime: fall back to the largest whole-year offset.
        let mut years = 584u64;
        loop {
            if let Some(s) = UNIX_EPOCH.checked_add(Duration::from_secs(years * 365 * 86_400)) {
                return s;
            }
            years /= 2;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conversions_saturate() {
        let c = Clock::start();
        assert_eq!(
            c.from_system(UNIX_EPOCH - Duration::from_secs(10)),
            Time::ZERO
        );
        let far = UNIX_EPOCH + Duration::from_secs(9_300_000_000); // year ~2265
        let t = c.from_system(far);
        assert!(t > c.now());
        assert_eq!(c.to_system(t), far);
        assert!(c.to_system(Time::MAX) > SystemTime::now());
        // Instants for unreachable times stay in the far future instead of wrapping.
        assert!(c.instant(Time::MAX) > Instant::now() + Duration::from_secs(3600));
        assert!(c.instant(Time::ZERO) <= Instant::now());
    }

    #[test]
    fn now_is_unix_time() {
        let c = Clock::start();
        let wall = unix_nanos(SystemTime::now());
        assert!(c.now().0.abs_diff(wall) < 1_000_000_000);
    }
}

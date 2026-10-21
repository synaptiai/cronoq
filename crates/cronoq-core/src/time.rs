//! Engine time. The core never reads a clock: every input carries `now`.

use core::ops::{Add, Sub};
use core::time::Duration;

/// Engine time: nanoseconds since the engine epoch.
///
/// The engine only requires that time never goes backwards; it makes no assumption about where
/// the epoch is. The `cronoq` runtime uses the Unix epoch, so `Time(n)` is `n` nanoseconds after
/// 1970-01-01T00:00:00Z (representable until the year 2554). Tests often use small values.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Time(pub u64);

impl Time {
    /// The epoch itself.
    pub const ZERO: Time = Time(0);
    /// The latest representable time (also used as "never").
    pub const MAX: Time = Time(u64::MAX);

    /// Time from (fractional) seconds since the epoch; negative values clamp to zero.
    #[inline]
    pub fn from_secs_f64(s: f64) -> Time {
        Time((s.max(0.0) * 1e9) as u64)
    }
    /// Seconds since the epoch.
    #[inline]
    pub fn as_secs_f64(self) -> f64 {
        self.0 as f64 / 1e9
    }
    /// `self - other`, or zero if `other` is later.
    #[inline]
    pub fn saturating_sub(self, other: Time) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(other.0))
    }
    /// Signed difference `self - other` in seconds.
    #[inline]
    pub fn diff_secs(self, other: Time) -> f64 {
        (self.0 as i64).wrapping_sub(other.0 as i64) as f64 / 1e9
    }
}

impl Add<Duration> for Time {
    type Output = Time;
    #[inline]
    fn add(self, d: Duration) -> Time {
        Time(
            self.0
                .saturating_add(d.as_nanos().min(u64::MAX as u128) as u64),
        )
    }
}

impl Sub<Duration> for Time {
    type Output = Time;
    #[inline]
    fn sub(self, d: Duration) -> Time {
        Time(
            self.0
                .saturating_sub(d.as_nanos().min(u64::MAX as u128) as u64),
        )
    }
}

#[inline]
pub(crate) fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

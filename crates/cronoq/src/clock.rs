//! Mapping between engine time, tokio instants and wall-clock time.
//!
//! Engine time ([`Time`]) is **nanoseconds since the Unix epoch**. The clock reads the wall
//! clock once at start and then advances with tokio's monotonic clock, so engine time never
//! goes backwards (NTP steps after start are not followed) and paused test time works.
//!
//! The clock that tokio and the Rust standard library use does not count the time a machine
//! sleeps (on Apple platforms and on Linux). After a sleep, engine time is behind by the time
//! slept, and timers are late by as much.
//! [`Scheduler::resync_clock`](crate::Scheduler::resync_clock) moves engine time forward by
//! what was missed. It measures the sleep with a monotonic clock of the operating system
//! that keeps counting during sleep, so a change of the wall clock has no effect. Engine
//! time never moves back.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use cronoq_core::Time;
use tokio::time::Instant;

/// How far ahead [`Clock::instant`] maps unreachable times (so timers stay valid).
const FAR_FUTURE: Duration = Duration::from_secs(100 * 365 * 86_400);

/// Engine clock: Unix nanoseconds, advanced by tokio's monotonic clock. Clones share one
/// clock: a resynchronisation is seen by all of them.
#[derive(Clone, Debug)]
pub struct Clock {
    base: Instant,
    unix_nanos_at_base: u64,
    /// Nanoseconds that resynchronisations have added to the monotonic clock.
    skipped: Arc<AtomicU64>,
    /// The reading of the sleep-inclusive clock at the base, where the platform has one.
    continuous_at_base: Option<Duration>,
}

/// A monotonic clock that keeps counting while the machine sleeps: `CLOCK_BOOTTIME` on Linux
/// and Android, `CLOCK_MONOTONIC` on Apple platforms. `None` where there is none we know of.
fn continuous_now() -> Option<Duration> {
    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    {
        #[cfg(target_vendor = "apple")]
        const CLOCK: libc::clockid_t = libc::CLOCK_MONOTONIC;
        #[cfg(not(target_vendor = "apple"))]
        const CLOCK: libc::clockid_t = libc::CLOCK_BOOTTIME;
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is a valid place for one timespec, and the clock id is a constant
        // of this platform.
        if unsafe { libc::clock_gettime(CLOCK, &mut ts) } != 0 {
            return None;
        }
        Some(Duration::new(
            ts.tv_sec.max(0) as u64,
            ts.tv_nsec.clamp(0, 999_999_999) as u32,
        ))
    }
    #[cfg(not(any(target_os = "linux", target_os = "android", target_vendor = "apple")))]
    {
        None
    }
}

impl Clock {
    /// Anchor the clock at the current wall-clock time.
    pub fn start() -> Clock {
        Clock {
            base: Instant::now(),
            unix_nanos_at_base: unix_nanos(SystemTime::now()),
            skipped: Arc::default(),
            continuous_at_base: continuous_now(),
        }
    }

    /// Move engine time forward by the time it missed while the machine slept, and return
    /// how far it moved. The sleep is measured with the operating system's monotonic clock
    /// that counts during sleep; where there is none, with the wall clock.
    pub(crate) fn resync(&self) -> Duration {
        match (self.continuous_at_base, continuous_now()) {
            (Some(base), Some(now)) => self.catch_up(now.saturating_sub(base)),
            _ => self.resync_to(SystemTime::now()),
        }
    }

    /// Move engine time forward so that `real` has passed since the base, if less has.
    fn catch_up(&self, real: Duration) -> Duration {
        let real = real.as_nanos().min(u64::MAX as u128) as u64;
        let behind = real.saturating_sub(self.elapsed());
        self.skipped.fetch_add(behind, Ordering::Relaxed);
        Duration::from_nanos(behind)
    }

    /// Nanoseconds since the base, including what resynchronisations skipped.
    #[inline]
    fn elapsed(&self) -> u64 {
        let run = self.base.elapsed().as_nanos().min(u64::MAX as u128) as u64;
        run.saturating_add(self.skipped.load(Ordering::Relaxed))
    }

    /// Move engine time forward to the wall-clock time `wall`, if it is behind it. Returns
    /// how far it moved. Engine time never moves back: a `wall` in the engine's past
    /// changes nothing. Used only where the platform has no sleep-inclusive clock.
    pub(crate) fn resync_to(&self, wall: SystemTime) -> Duration {
        let behind = unix_nanos(wall).saturating_sub(self.now().0);
        self.skipped.fetch_add(behind, Ordering::Relaxed);
        Duration::from_nanos(behind)
    }

    /// Move engine time forward by `by`.
    pub(crate) fn skip(&self, by: Duration) {
        let by = by.as_nanos().min(u64::MAX as u128 / 2) as u64;
        // Saturating: a sum that wraps would move time back.
        let _ = self
            .skipped
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |s| {
                Some(s.saturating_add(by))
            });
    }

    /// Current engine time.
    #[inline]
    pub fn now(&self) -> Time {
        Time(self.unix_nanos_at_base.saturating_add(self.elapsed()))
    }

    /// The tokio instant at which engine time `t` is reached (far future if unrepresentable).
    #[inline]
    pub fn instant(&self, t: Time) -> Instant {
        let skipped = self.skipped.load(Ordering::Relaxed);
        let ahead = Duration::from_nanos(
            t.0.saturating_sub(self.unix_nanos_at_base)
                .saturating_sub(skipped),
        );
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
    fn a_resync_moves_time_forward_and_never_back() {
        let c = Clock::start();
        let shared = c.clone();
        let before = c.now();
        // The wall clock is an hour ahead of engine time: as after an hour of sleep.
        let moved = c.resync_to(SystemTime::now() + Duration::from_secs(3600));
        assert!(moved > Duration::from_secs(3599) && moved <= Duration::from_secs(3600));
        assert!(shared.now().0 - before.0 >= 3_599_000_000_000);
        // A time that was an hour away is due now.
        assert!(c.instant(Time(before.0 + 3_500_000_000_000)) <= Instant::now());
        // The wall clock set back (or simply behind): engine time stays where it is.
        let at = c.now();
        assert_eq!(c.resync_to(SystemTime::now()), Duration::ZERO);
        assert_eq!(c.resync_to(UNIX_EPOCH), Duration::ZERO);
        assert!(c.now() >= at);
        c.skip(Duration::from_secs(60));
        assert!(c.now().0 - at.0 >= 60_000_000_000);
        c.skip(Duration::MAX);
        assert!(c.now() >= at);
    }

    #[test]
    fn a_resync_makes_up_the_time_slept_and_ignores_the_wall_clock() {
        let c = Clock::start();
        let before = c.now();
        // Nothing was missed: the sleep-inclusive clock and tokio's clock agree.
        assert!(c.resync() < Duration::from_secs(1));
        // The machine slept for an hour: an hour more has really passed than was counted.
        let moved = c.catch_up(Duration::from_secs(3600));
        assert!(moved > Duration::from_secs(3599) && moved <= Duration::from_secs(3600));
        assert!(c.now().0 - before.0 >= 3_599_000_000_000);
        // Asked again, there is nothing left to make up.
        assert_eq!(c.catch_up(Duration::from_secs(3600)), Duration::ZERO);
        // Engine time is ahead of what really passed (as after `skip`): it does not go back.
        let at = c.now();
        assert_eq!(c.resync(), Duration::ZERO);
        assert!(c.now() >= at);
    }

    #[cfg(any(target_os = "linux", target_os = "android", target_vendor = "apple"))]
    #[test]
    fn the_platform_has_a_sleep_inclusive_clock() {
        let a = continuous_now().expect("a sleep-inclusive monotonic clock");
        std::thread::sleep(Duration::from_millis(5));
        let b = continuous_now().unwrap();
        assert!(b > a && b - a < Duration::from_secs(5));
    }

    #[test]
    fn now_is_unix_time() {
        let c = Clock::start();
        let wall = unix_nanos(SystemTime::now());
        assert!(c.now().0.abs_diff(wall) < 1_000_000_000);
    }
}

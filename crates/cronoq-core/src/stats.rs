//! Streaming statistics: EWMA, quantile estimates, log histograms and windowed metrics.

use core::time::Duration;

use crate::time::Time;

/// Exponentially weighted moving average.
#[derive(Copy, Clone, Debug, Default)]
pub struct Ewma {
    value: f64,
    init: bool,
}

impl Ewma {
    #[inline]
    pub fn update(&mut self, x: f64, alpha: f64) {
        if self.init {
            self.value += alpha * (x - self.value);
        } else {
            self.value = x;
            self.init = true;
        }
    }
    #[inline]
    pub fn get(&self) -> Option<f64> {
        self.init.then_some(self.value)
    }
}

/// Streaming quantile estimate (stochastic approximation with adaptive step).
#[derive(Copy, Clone, Debug)]
pub struct QuantileEst {
    q: f64,
    p: f64,
    n: u64,
}

impl QuantileEst {
    pub fn new(p: f64) -> Self {
        QuantileEst { q: 0.0, p, n: 0 }
    }
    #[inline]
    pub fn update(&mut self, x: f64, scale: f64) {
        self.n += 1;
        if self.n == 1 {
            self.q = x;
            return;
        }
        // Step proportional to the scale of the data; decays to a floor so it tracks drift.
        let decay = if self.n > 2500 {
            0.02
        } else {
            (1.0 / (self.n as f64).sqrt()).max(0.02)
        };
        let step = scale.max(1e-9) * decay;
        if x > self.q {
            self.q += step * self.p;
        } else if x < self.q {
            self.q -= step * (1.0 - self.p);
        }
        if self.q < 0.0 {
            self.q = 0.0;
        }
    }
    #[inline]
    pub fn get(&self) -> Option<f64> {
        (self.n > 0).then_some(self.q)
    }
}

/// Observed runtimes for a `(pool, kind)` (seconds).
#[derive(Copy, Clone, Debug)]
pub struct DurStats {
    pub mean: Ewma,
    pub p50: QuantileEst,
    pub p90: QuantileEst,
    pub count: u64,
}

impl Default for DurStats {
    fn default() -> Self {
        DurStats {
            mean: Ewma::default(),
            p50: QuantileEst::new(0.5),
            p90: QuantileEst::new(0.9),
            count: 0,
        }
    }
}

impl DurStats {
    pub fn observe(&mut self, secs: f64) {
        self.count += 1;
        self.mean.update(secs, 0.1);
        let scale = self.mean.get().unwrap_or(secs);
        self.p50.update(secs, scale);
        self.p90.update(secs, scale);
    }
    pub fn estimate(&self) -> Option<Duration> {
        if self.count < 3 {
            return self.mean.get().map(Duration::from_secs_f64);
        }
        self.p50.get().map(Duration::from_secs_f64)
    }
    pub fn p90(&self) -> Option<Duration> {
        if self.count < 3 {
            return self.mean.get().map(Duration::from_secs_f64);
        }
        self.p90.get().map(Duration::from_secs_f64)
    }
}

/// Log-linear histogram over nanoseconds (8 sub-buckets per power of two, ~9% resolution).
#[derive(Clone, Debug, Default)]
pub struct Histogram {
    counts: Vec<u64>,
    total: u64,
    sum_ns: u128,
}

const SUB: u32 = 8;
const BUCKETS: usize = 64 * SUB as usize;

impl Histogram {
    #[inline]
    fn index(ns: u64) -> usize {
        if ns < SUB as u64 {
            return ns as usize;
        }
        let msb = 63 - ns.leading_zeros(); // >= 3
        let shift = msb - 3;
        let sub = ((ns >> shift) & (SUB as u64 - 1)) as u32;
        ((msb - 2) * SUB + sub) as usize
    }
    #[inline]
    fn lower_bound(idx: usize) -> u64 {
        let idx = idx as u32;
        if idx < SUB {
            return idx as u64;
        }
        let msb = idx / SUB + 2;
        let sub = idx % SUB;
        ((SUB + sub) as u64) << (msb - 3)
    }
    pub fn record(&mut self, d: Duration) {
        let ns = d.as_nanos().min(u64::MAX as u128) as u64;
        if self.counts.is_empty() {
            self.counts = vec![0; BUCKETS];
        }
        self.counts[Self::index(ns)] += 1;
        self.total += 1;
        self.sum_ns += ns as u128;
    }
    pub fn merge(&mut self, other: &Histogram) {
        if other.total == 0 {
            return;
        }
        if self.counts.is_empty() {
            self.counts = vec![0; BUCKETS];
        }
        for (a, b) in self.counts.iter_mut().zip(&other.counts) {
            *a += *b;
        }
        self.total += other.total;
        self.sum_ns += other.sum_ns;
    }
    pub fn count(&self) -> u64 {
        self.total
    }
    pub fn mean(&self) -> Duration {
        if self.total == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos((self.sum_ns / self.total as u128) as u64)
    }
    pub fn quantile(&self, q: f64) -> Duration {
        if self.total == 0 {
            return Duration::ZERO;
        }
        let target = ((q.clamp(0.0, 1.0) * self.total as f64).ceil() as u64).max(1);
        let mut acc = 0;
        for (i, c) in self.counts.iter().enumerate() {
            acc += c;
            if acc >= target {
                let lo = Self::lower_bound(i);
                let hi = Self::lower_bound(i + 1);
                return Duration::from_nanos(lo + (hi - lo) / 2);
            }
        }
        Duration::ZERO
    }
    pub fn clear(&mut self) {
        if self.total > 0 {
            self.counts.iter_mut().for_each(|c| *c = 0);
        }
        self.total = 0;
        self.sum_ns = 0;
    }
}

/// Counters accumulated per time bucket.
#[derive(Clone, Debug, Default)]
pub struct Bucket {
    pub dispatched: u64,
    pub inversions: u64,
    pub fixed_total: u64,
    pub fixed_hits: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub retried: u64,
    pub busy_secs: f64,
    pub capacity_secs: f64,
    pub work_secs: f64,
    pub wait: Histogram,
    pub run: Histogram,
}

impl Bucket {
    fn clear(&mut self) {
        let wait = core::mem::take(&mut self.wait);
        let run = core::mem::take(&mut self.run);
        *self = Bucket {
            wait,
            run,
            ..Default::default()
        };
        self.wait.clear();
        self.run.clear();
    }
    fn merge(&mut self, o: &Bucket) {
        self.dispatched += o.dispatched;
        self.inversions += o.inversions;
        self.fixed_total += o.fixed_total;
        self.fixed_hits += o.fixed_hits;
        self.succeeded += o.succeeded;
        self.failed += o.failed;
        self.retried += o.retried;
        self.busy_secs += o.busy_secs;
        self.capacity_secs += o.capacity_secs;
        self.work_secs += o.work_secs;
        self.wait.merge(&o.wait);
        self.run.merge(&o.run);
    }
}

/// Sliding window made of a ring of buckets.
#[derive(Clone, Debug)]
pub struct Window {
    buckets: Vec<Bucket>,
    bucket_ns: u64,
    current: u64,
}

const WINDOW_BUCKETS: usize = 12;

impl Window {
    pub fn new(window: Duration) -> Self {
        let bucket_ns = (window.as_nanos() as u64 / WINDOW_BUCKETS as u64).max(1);
        Window {
            buckets: vec![Bucket::default(); WINDOW_BUCKETS],
            bucket_ns,
            current: 0,
        }
    }
    /// Bucket for `now`, rotating out expired buckets.
    pub fn at(&mut self, now: Time) -> &mut Bucket {
        let idx = now.0 / self.bucket_ns;
        if idx > self.current {
            let steps = (idx - self.current).min(WINDOW_BUCKETS as u64);
            for s in 1..=steps {
                let b = ((self.current + s) % WINDOW_BUCKETS as u64) as usize;
                self.buckets[b].clear();
            }
            self.current = idx;
        }
        let b = (self.current % WINDOW_BUCKETS as u64) as usize;
        &mut self.buckets[b]
    }
    pub fn total(&mut self, now: Time) -> Bucket {
        self.at(now);
        let mut acc = Bucket::default();
        for b in &self.buckets {
            acc.merge(b);
        }
        acc
    }
    pub fn span(&self) -> Duration {
        Duration::from_nanos(self.bucket_ns * WINDOW_BUCKETS as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn histogram_quantiles_are_close() {
        let mut h = Histogram::default();
        for i in 1..=1000u64 {
            h.record(Duration::from_micros(i));
        }
        let p50 = h.quantile(0.5).as_micros() as f64;
        let p99 = h.quantile(0.99).as_micros() as f64;
        assert!((p50 - 500.0).abs() / 500.0 < 0.1, "p50={p50}");
        assert!((p99 - 990.0).abs() / 990.0 < 0.1, "p99={p99}");
    }

    #[test]
    fn histogram_index_monotonic() {
        let mut last = 0;
        for ns in [0u64, 1, 7, 8, 9, 15, 16, 100, 1_000, 1 << 40, u64::MAX] {
            let i = Histogram::index(ns);
            assert!(i >= last);
            assert!(i < BUCKETS);
            assert!(Histogram::lower_bound(i) <= ns);
            last = i;
        }
    }

    #[test]
    fn quantile_estimate_converges() {
        let mut d = DurStats::default();
        for i in 0..5000 {
            d.observe(((i * 7919) % 100) as f64 / 10.0); // uniform over 0..10
        }
        let p50 = d.p50.get().unwrap();
        let p90 = d.p90.get().unwrap();
        assert!((p50 - 5.0).abs() < 1.0, "p50={p50}");
        assert!((p90 - 9.0).abs() < 1.0, "p90={p90}");
    }

    #[test]
    fn window_expires_old_buckets() {
        let mut w = Window::new(Duration::from_secs(60));
        w.at(Time::ZERO).dispatched += 5;
        assert_eq!(w.total(Time::from_secs_f64(30.0)).dispatched, 5);
        assert_eq!(w.total(Time::from_secs_f64(120.0)).dispatched, 0);
    }
}

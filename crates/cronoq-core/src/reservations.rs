//! Per-pool calendar of fixed-start reservations.

use std::collections::BTreeMap;

use crate::spec::TaskId;
use crate::time::Time;

#[derive(Clone, Debug, Default)]
pub struct Reservations {
    /// (start, task) -> end
    by_start: BTreeMap<(Time, TaskId), Time>,
    /// Longest interval ever inserted; bounds the backwards scan.
    max_len: u64,
}

impl Reservations {
    pub fn len(&self) -> usize {
        self.by_start.len()
    }

    pub fn insert(&mut self, id: TaskId, start: Time, end: Time) {
        self.max_len = self.max_len.max(end.0.saturating_sub(start.0));
        self.by_start.insert((start, id), end);
    }

    pub fn remove(&mut self, id: TaskId, start: Time) {
        self.by_start.remove(&(start, id));
    }

    /// Maximum number of existing reservations simultaneously active within `[s, e)`.
    pub fn max_overlap(&self, s: Time, e: Time) -> u32 {
        let from = Time(s.0.saturating_sub(self.max_len));
        let mut events: Vec<(Time, i32)> = Vec::new();
        for (&(start, _), &end) in self.by_start.range((from, TaskId(0))..) {
            if start >= e {
                break;
            }
            if end <= s {
                continue;
            }
            events.push((start.max(s), 1));
            events.push((end.min(e), -1));
        }
        // Ends sort before starts at the same instant: touching intervals do not overlap.
        events.sort_unstable_by_key(|&(t, d)| (t, d));
        let (mut cur, mut max) = (0i32, 0i32);
        for (_, d) in events {
            cur += d;
            max = max.max(cur);
        }
        max as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: u64) -> Time {
        Time(s * 1_000_000_000)
    }

    #[test]
    fn overlap_counts() {
        let mut r = Reservations::default();
        r.insert(TaskId(1), t(10), t(20));
        r.insert(TaskId(2), t(15), t(25));
        r.insert(TaskId(3), t(30), t(40));
        assert_eq!(r.max_overlap(t(0), t(10)), 0);
        assert_eq!(r.max_overlap(t(0), t(11)), 1);
        assert_eq!(r.max_overlap(t(12), t(18)), 2);
        assert_eq!(r.max_overlap(t(20), t(30)), 1);
        assert_eq!(r.max_overlap(t(25), t(30)), 0);
        r.remove(TaskId(2), t(15));
        assert_eq!(r.max_overlap(t(12), t(18)), 1);
    }

    proptest::proptest! {
        #[test]
        fn overlap_matches_bruteforce(
            ivs in proptest::collection::vec((0u64..100, 1u64..30), 0..30),
            q in (0u64..120, 1u64..40),
        ) {
            let mut r = Reservations::default();
            for (i, &(s, l)) in ivs.iter().enumerate() {
                r.insert(TaskId(i as u64), Time(s), Time(s + l));
            }
            let (qs, ql) = q;
            let qe = qs + ql;
            let mut brute = 0;
            for x in qs..qe {
                let c = ivs.iter().filter(|&&(s, l)| s <= x && x < s + l).count();
                brute = brute.max(c);
            }
            proptest::prop_assert_eq!(r.max_overlap(Time(qs), Time(qe)) as usize, brute);
        }
    }
}

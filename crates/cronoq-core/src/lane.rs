//! FIFO lane ordered by sequence number. Fresh tasks always carry the highest sequence, so the
//! common path is `push_back`/`pop_front`; retries and resource wake-ups use binary insertion.

use std::collections::VecDeque;

#[derive(Clone, Debug)]
pub struct Lane<K> {
    q: VecDeque<(u64, K)>,
}

impl<K> Default for Lane<K> {
    fn default() -> Self {
        Lane { q: VecDeque::new() }
    }
}

impl<K: Copy + PartialEq> Lane<K> {
    #[inline]
    pub fn insert(&mut self, seq: u64, k: K) {
        match self.q.back() {
            None => self.q.push_back((seq, k)),
            Some(&(last, _)) if last < seq => self.q.push_back((seq, k)),
            _ => {
                let i = self.q.partition_point(|&(s, _)| s < seq);
                self.q.insert(i, (seq, k));
            }
        }
    }
    #[inline]
    pub fn first(&self) -> Option<K> {
        self.q.front().map(|e| e.1)
    }
    #[inline]
    pub fn last(&self) -> Option<K> {
        self.q.back().map(|e| e.1)
    }
    #[inline]
    pub fn remove(&mut self, seq: u64) -> Option<K> {
        if let Some(&(s, k)) = self.q.front() {
            if s == seq {
                self.q.pop_front();
                return Some(k);
            }
        }
        let i = self.q.partition_point(|&(s, _)| s < seq);
        match self.q.get(i) {
            Some(&(s, _)) if s == seq => self.q.remove(i).map(|e| e.1),
            _ => None,
        }
    }
    pub fn get(&self, seq: u64) -> Option<K> {
        let i = self.q.partition_point(|&(s, _)| s < seq);
        self.q.get(i).filter(|e| e.0 == seq).map(|e| e.1)
    }
    #[inline]
    pub fn len(&self) -> usize {
        self.q.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.q.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    proptest::proptest! {
        #[test]
        fn behaves_like_sorted_map(ops in proptest::collection::vec((0u8..3, 0u64..50), 0..200)) {
            let mut lane = Lane::default();
            let mut model = std::collections::BTreeMap::new();
            for (op, seq) in ops {
                match op {
                    0 => { model.entry(seq).or_insert_with(|| { lane.insert(seq, seq); seq }); }
                    1 => { proptest::prop_assert_eq!(lane.remove(seq), model.remove(&seq)); }
                    _ => { proptest::prop_assert_eq!(lane.get(seq), model.get(&seq).copied()); }
                }
                proptest::prop_assert_eq!(lane.first(), model.values().next().copied());
                proptest::prop_assert_eq!(lane.last(), model.values().next_back().copied());
                proptest::prop_assert_eq!(lane.len(), model.len());
            }
        }
    }
}

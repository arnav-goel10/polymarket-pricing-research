use std::collections::VecDeque;

use crate::state::{LatencySnapshot, LatencyStats};

#[derive(Debug, Clone)]
pub struct RollingLatency {
    cap: usize,
    tick_to_decision_micros: VecDeque<u128>,
    book_age_millis: VecDeque<u128>,
    chainlink_age_millis: VecDeque<u128>,
    binance_age_millis: VecDeque<u128>,
}

impl RollingLatency {
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(8),
            tick_to_decision_micros: VecDeque::new(),
            book_age_millis: VecDeque::new(),
            chainlink_age_millis: VecDeque::new(),
            binance_age_millis: VecDeque::new(),
        }
    }

    pub fn observe_tick_to_decision(&mut self, value: u128) {
        push(&mut self.tick_to_decision_micros, self.cap, value);
    }

    pub fn observe_book_age(&mut self, value: Option<i64>) {
        observe_i64(&mut self.book_age_millis, self.cap, value);
    }

    pub fn observe_chainlink_age(&mut self, value: Option<i64>) {
        observe_i64(&mut self.chainlink_age_millis, self.cap, value);
    }

    pub fn observe_binance_age(&mut self, value: Option<i64>) {
        observe_i64(&mut self.binance_age_millis, self.cap, value);
    }

    pub fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            tick_to_decision_micros: stats(&self.tick_to_decision_micros),
            book_age_millis: stats(&self.book_age_millis),
            chainlink_age_millis: stats(&self.chainlink_age_millis),
            binance_age_millis: stats(&self.binance_age_millis),
        }
    }
}

fn observe_i64(values: &mut VecDeque<u128>, cap: usize, value: Option<i64>) {
    if let Some(v) = value.filter(|v| *v >= 0) {
        push(values, cap, v as u128);
    }
}

fn push(values: &mut VecDeque<u128>, cap: usize, value: u128) {
    if values.len() == cap {
        values.pop_front();
    }
    values.push_back(value);
}

fn stats(values: &VecDeque<u128>) -> Option<LatencyStats> {
    if values.is_empty() {
        return None;
    }
    let mut sorted: Vec<u128> = values.iter().copied().collect();
    sorted.sort_unstable();
    Some(LatencyStats {
        samples: sorted.len(),
        p50: percentile(&sorted, 0.50),
        p90: percentile(&sorted, 0.90),
        p99: percentile(&sorted, 0.99),
        max: *sorted.last().unwrap(),
    })
}

fn percentile(sorted: &[u128], q: f64) -> u128 {
    if sorted.len() == 1 {
        return sorted[0];
    }
    let idx = ((sorted.len() - 1) as f64 * q).ceil() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rolling_latency_reports_percentiles() {
        let mut l = RollingLatency::new(4);
        for v in [10, 20, 30, 40, 50] {
            l.observe_tick_to_decision(v);
        }
        let s = l.snapshot().tick_to_decision_micros.unwrap();
        assert_eq!(s.samples, 5);
        assert_eq!(s.p50, 30);
        assert_eq!(s.p99, 50);
    }
}

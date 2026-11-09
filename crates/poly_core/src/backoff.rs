//! Capped exponential reconnect backoff.

use std::time::Duration;

/// Doubling delay between reconnect attempts, capped at `max`, and reset to
/// `initial` after a connection that stayed healthy for `healthy_after`.
/// Deterministic (no jitter) so reconnect behaviour is testable; a single
/// client per feed does not need jitter to avoid a thundering herd.
#[derive(Debug, Clone)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    next: Duration,
    /// A session lasting at least this long resets the delay.
    pub healthy_after: Duration,
}

impl Backoff {
    pub fn new(initial: Duration, max: Duration, healthy_after: Duration) -> Self {
        Self {
            initial,
            max,
            next: initial,
            healthy_after,
        }
    }

    /// The delay to wait now; the following call returns double (capped).
    pub fn next_delay(&mut self) -> Duration {
        let d = self.next;
        self.next = (self.next * 2).min(self.max);
        d
    }

    pub fn reset(&mut self) {
        self.next = self.initial;
    }

    /// Feed in how long the session that just ended lasted.
    pub fn on_session_end(&mut self, lasted: Duration) -> Duration {
        if lasted >= self.healthy_after {
            self.reset();
        }
        self.next_delay()
    }
}

impl Default for Backoff {
    /// 250 ms doubling to 10 s; a 30 s healthy session resets it.
    fn default() -> Self {
        Self::new(
            Duration::from_millis(250),
            Duration::from_secs(10),
            Duration::from_secs(30),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_and_caps() {
        let mut b = Backoff::default();
        let got: Vec<u128> = (0..8).map(|_| b.next_delay().as_millis()).collect();
        assert_eq!(got, vec![250, 500, 1000, 2000, 4000, 8000, 10000, 10000]);
    }

    #[test]
    fn healthy_session_resets_but_flapping_does_not() {
        let mut b = Backoff::default();
        b.next_delay();
        b.next_delay();
        assert_eq!(
            b.on_session_end(Duration::from_secs(1)).as_millis(),
            1000,
            "short session keeps escalating"
        );
        assert_eq!(b.on_session_end(Duration::from_secs(60)).as_millis(), 250);
    }
}

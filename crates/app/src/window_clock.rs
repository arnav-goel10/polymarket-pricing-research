use market::Window;

/// Rolls a fresh fixed-length window on UTC boundaries, freezing S0 to the spot
/// at each new boundary open.
#[derive(Debug, Clone)]
pub struct WindowClock {
    length_secs: i64,
    current: Option<Window>,
}

impl WindowClock {
    pub fn new(length_secs: i64) -> Self {
        WindowClock {
            length_secs,
            current: None,
        }
    }

    /// Floor a millisecond timestamp to the start of its window.
    pub fn window_start(&self, now_millis: i64) -> i64 {
        let len_ms = self.length_secs * 1000;
        (now_millis / len_ms) * len_ms
    }

    /// Advance the clock to `now`, opening a new window (freezing `spot` as S0)
    /// whenever the boundary changes. Returns the active window.
    pub fn tick(&mut self, now_millis: i64, spot: f64) -> Window {
        let start = self.window_start(now_millis);
        let roll = match self.current {
            Some(w) => w.t0_millis != start,
            None => true,
        };
        if roll {
            self.current = Some(Window::open(spot, start, self.length_secs));
        }
        self.current.unwrap()
    }

    pub fn current(&self) -> Option<Window> {
        self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn floors_to_boundary() {
        let c = WindowClock::new(300);
        // 300s = 300_000ms; 1_700_000_123_000 floors to a multiple of 300_000
        let s = c.window_start(1_700_000_123_000);
        assert_eq!(s % 300_000, 0);
        assert!(s <= 1_700_000_123_000 && 1_700_000_123_000 - s < 300_000);
    }

    #[test]
    fn freezes_s0_at_open_and_holds_mid_window() {
        let mut c = WindowClock::new(300);
        let start = c.window_start(1_700_000_000_000);
        let w1 = c.tick(start, 60_000.0);
        assert_eq!(w1.s0, 60_000.0);
        // mid-window, spot moves but S0 stays
        let w2 = c.tick(start + 120_000, 60_500.0);
        assert_eq!(w2.s0, 60_000.0);
        assert_eq!(w2.t0_millis, w1.t0_millis);
    }

    #[test]
    fn rolls_new_window_at_next_boundary() {
        let mut c = WindowClock::new(300);
        let start = c.window_start(1_700_000_000_000);
        let _ = c.tick(start, 60_000.0);
        let w = c.tick(start + 300_000, 61_000.0);
        assert_eq!(w.s0, 61_000.0);
        assert_eq!(w.t0_millis, start + 300_000);
    }
}

/// Adaptive polling interval for following the chain tip.
///
/// Starts at `base_ms`. On block found, resets to 0 (process immediately).
/// On 404 (no new block), decays current interval by `decay` factor, clamped to `min_ms`.
pub struct AdaptiveInterval {
    base_ms: u64,
    min_ms: u64,
    decay: f64,
    current_ms: u64,
}

impl AdaptiveInterval {
    pub fn new(base_ms: u64, min_ms: u64, decay: f64) -> Self {
        Self {
            base_ms,
            min_ms,
            decay,
            current_ms: base_ms,
        }
    }

    /// Block found — process immediately on next iteration.
    pub fn reset(&mut self) {
        self.current_ms = 0;
    }

    /// No new block (404) — increase wait time.
    /// On first backoff after a reset, starts at min_ms (fast initial retry).
    /// Subsequent backoffs grow toward base_ms (slower as tip seems further away).
    pub fn backoff(&mut self) {
        if self.current_ms == 0 {
            self.current_ms = self.min_ms;
        } else {
            // Grow toward base: current = current / decay, clamped to base
            let next = (self.current_ms as f64 / self.decay) as u64;
            self.current_ms = next.min(self.base_ms);
        }
    }

    /// Current interval in milliseconds.
    pub fn current(&self) -> u64 {
        self.current_ms
    }

    /// Whether the caller should sleep before the next poll.
    pub fn should_sleep(&self) -> bool {
        self.current_ms > 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_starts_at_base() {
        let interval = AdaptiveInterval::new(1000, 200, 0.67);
        assert_eq!(interval.current(), 1000);
        assert!(interval.should_sleep());
    }

    #[test]
    fn reset_sets_to_zero() {
        let mut interval = AdaptiveInterval::new(1000, 200, 0.67);
        interval.reset();
        assert_eq!(interval.current(), 0);
        assert!(!interval.should_sleep());
    }

    #[test]
    fn backoff_chain_grows_toward_base() {
        let mut interval = AdaptiveInterval::new(1000, 200, 0.67);
        interval.reset(); // 0
        assert_eq!(interval.current(), 0);

        interval.backoff(); // 0 -> min_ms (200)
        assert_eq!(interval.current(), 200);

        interval.backoff(); // 200 / 0.67 = 298
        assert_eq!(interval.current(), 298);

        interval.backoff(); // 298 / 0.67 = 444
        assert_eq!(interval.current(), 444);

        interval.backoff(); // 444 / 0.67 = 662
        assert_eq!(interval.current(), 662);

        interval.backoff(); // 662 / 0.67 = 988
        assert_eq!(interval.current(), 988);

        interval.backoff(); // 988 / 0.67 = 1474 -> clamped to 1000
        assert_eq!(interval.current(), 1000);

        // Stays at base
        interval.backoff();
        assert_eq!(interval.current(), 1000);
    }

    #[test]
    fn backoff_then_reset_returns_to_zero() {
        let mut interval = AdaptiveInterval::new(1000, 200, 0.67);
        interval.backoff();
        interval.backoff();
        assert!(interval.current() > 0);

        interval.reset();
        assert_eq!(interval.current(), 0);
        assert!(!interval.should_sleep());
    }

    #[test]
    fn backoff_from_base_stays_at_base() {
        let mut interval = AdaptiveInterval::new(1000, 200, 0.67);
        // Already at base_ms
        interval.backoff();
        assert_eq!(interval.current(), 1000);
    }
}

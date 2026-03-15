use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{info, warn};

pub struct ProgressTracker {
    total_blocks: u64,
    processed: AtomicU64,
    failed: AtomicU64,
    start_time: Instant,
    log_interval: u64,
}

impl ProgressTracker {
    pub fn new(total_blocks: u64) -> Self {
        Self {
            total_blocks,
            processed: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            start_time: Instant::now(),
            log_interval: 1000,
        }
    }

    /// Record a successfully processed block. Logs progress every `log_interval` blocks.
    pub fn record_success(&self) {
        let prev = self.processed.fetch_add(1, Ordering::Relaxed);
        let count = prev + 1;
        if count % self.log_interval == 0 || count == self.total_blocks {
            self.log_progress(count);
        }
    }

    /// Record a failed block fetch. Also counts toward processed total
    /// so that progress percentage and ETA remain accurate.
    pub fn record_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
        let prev = self.processed.fetch_add(1, Ordering::Relaxed);
        let count = prev + 1;
        if count % self.log_interval == 0 || count == self.total_blocks {
            self.log_progress(count);
        }
    }

    fn log_progress(&self, processed: u64) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let pct = if self.total_blocks > 0 {
            (processed as f64 / self.total_blocks as f64) * 100.0
        } else {
            0.0
        };
        let blocks_per_sec = if elapsed > 0.0 {
            processed as f64 / elapsed
        } else {
            0.0
        };
        let remaining = self.total_blocks.saturating_sub(processed);
        let eta_secs = if blocks_per_sec > 0.0 {
            remaining as f64 / blocks_per_sec
        } else {
            0.0
        };

        let eta_str = format_duration(eta_secs);
        let failed = self.failed.load(Ordering::Relaxed);

        info!(
            "[PROGRESS] {processed}/{} blocks ({pct:.1}%) | {blocks_per_sec:.0} blocks/sec | ETA: {eta_str} | failed: {failed}",
            self.total_blocks
        );
    }

    /// Log final summary.
    pub fn summary(&self) {
        let processed = self.processed.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let elapsed = self.start_time.elapsed();
        let elapsed_secs = elapsed.as_secs_f64();
        let blocks_per_sec = if elapsed_secs > 0.0 {
            processed as f64 / elapsed_secs
        } else {
            0.0
        };

        info!(
            "[COMPLETE] {processed} blocks processed, {failed} failed | elapsed: {} | avg: {blocks_per_sec:.0} blocks/sec",
            format_duration(elapsed_secs)
        );

        if failed > 0 {
            warn!("{failed} blocks failed during ingestion");
        }
    }

    pub fn processed(&self) -> u64 {
        self.processed.load(Ordering::Relaxed)
    }

    pub fn failed(&self) -> u64 {
        self.failed.load(Ordering::Relaxed)
    }
}

fn format_duration(secs: f64) -> String {
    let total = secs as u64;
    let mins = total / 60;
    let s = total % 60;
    if mins > 0 {
        format!("{mins}m {s:02}s")
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_tracker_counts() {
        let tracker = ProgressTracker::new(100);
        assert_eq!(tracker.processed(), 0);
        assert_eq!(tracker.failed(), 0);

        tracker.record_success();
        tracker.record_success();
        tracker.record_failure();

        // processed includes both successes and failures for accurate progress tracking
        assert_eq!(tracker.processed(), 3);
        assert_eq!(tracker.failed(), 1);
    }

    #[test]
    fn format_duration_seconds() {
        assert_eq!(format_duration(45.0), "45s");
    }

    #[test]
    fn format_duration_minutes() {
        assert_eq!(format_duration(176.0), "2m 56s");
    }

    #[test]
    fn format_duration_zero() {
        assert_eq!(format_duration(0.0), "0s");
    }
}

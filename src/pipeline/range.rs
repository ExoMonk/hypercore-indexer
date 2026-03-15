use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing::{info, warn};

use crate::error::Result;
use crate::s3::client::HyperEvmS3Client;
use crate::types::BlockAndReceipts;

use super::progress::ProgressTracker;
use super::worker;

pub struct RangeConfig {
    pub start_block: u64,
    pub end_block: u64,
    pub workers: usize,
    pub channel_size: usize,
    pub retry_attempts: u32,
    pub retry_delay_ms: u64,
    pub cursor_file: Option<PathBuf>,
}

impl RangeConfig {
    pub fn validate(&self) -> Result<()> {
        if self.start_block > self.end_block {
            return Err(eyre::eyre!(
                "start_block ({}) must be <= end_block ({})",
                self.start_block,
                self.end_block
            ));
        }
        if self.workers == 0 {
            return Err(eyre::eyre!("workers must be > 0"));
        }
        if self.channel_size == 0 {
            return Err(eyre::eyre!("channel_size must be > 0"));
        }
        Ok(())
    }

    /// Resolve the effective start block, considering cursor file.
    pub fn effective_start(&self) -> Result<u64> {
        if let Some(ref path) = self.cursor_file {
            if let Some(cursor) = read_cursor(path)? {
                let resumed = cursor + 1;
                if resumed > self.end_block {
                    return Err(eyre::eyre!(
                        "Cursor ({cursor}) is already past end_block ({})",
                        self.end_block
                    ));
                }
                if resumed > self.start_block {
                    info!(cursor, resumed_from = resumed, "Resuming from cursor");
                    return Ok(resumed);
                }
            }
        }
        Ok(self.start_block)
    }
}

impl Default for RangeConfig {
    fn default() -> Self {
        Self {
            start_block: 0,
            end_block: 0,
            workers: 64,
            channel_size: 1024,
            retry_attempts: 3,
            retry_delay_ms: 1000,
            cursor_file: None,
        }
    }
}

pub struct RangeFetcher {
    s3_client: Arc<HyperEvmS3Client>,
    config: RangeConfig,
}

impl RangeFetcher {
    pub fn new(s3_client: Arc<HyperEvmS3Client>, config: RangeConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { s3_client, config })
    }

    /// Run the block range pipeline. Returns a receiver that yields decoded blocks.
    ///
    /// Spawns workers in the background. The caller consumes blocks from the returned
    /// receiver. Blocks may arrive out of order.
    pub async fn run(self) -> Result<mpsc::Receiver<(u64, BlockAndReceipts)>> {
        let effective_start = self.config.effective_start()?;
        let total_blocks = self.config.end_block - effective_start + 1;

        info!(
            start = effective_start,
            end = self.config.end_block,
            total_blocks,
            workers = self.config.workers,
            "Starting block range fetch"
        );

        let progress = Arc::new(ProgressTracker::new(total_blocks));
        let semaphore = Arc::new(Semaphore::new(self.config.workers));

        // Work queue: block numbers to fetch
        let (work_tx, work_rx) = mpsc::channel::<u64>(self.config.channel_size);

        // Result channel: decoded blocks
        let (result_tx, result_rx) =
            mpsc::channel::<(u64, BlockAndReceipts)>(self.config.channel_size);

        let cursor_file = self.config.cursor_file.clone();
        let end_block = self.config.end_block;

        // Spawn the worker pool
        let s3_client = Arc::clone(&self.s3_client);
        let progress_clone = Arc::clone(&progress);
        let retry_attempts = self.config.retry_attempts;
        let retry_delay_ms = self.config.retry_delay_ms;

        tokio::spawn(async move {
            worker::spawn_workers(
                s3_client,
                semaphore,
                work_rx,
                result_tx,
                Arc::clone(&progress_clone),
                retry_attempts,
                retry_delay_ms,
            )
            .await;

            progress_clone.summary();
        });

        // Spawn the block number producer
        tokio::spawn(async move {
            for block_num in effective_start..=end_block {
                if work_tx.send(block_num).await.is_err() {
                    warn!("Work channel closed early, stopping producer");
                    break;
                }
            }
            // work_tx drops here, closing the channel
        });

        // Spawn cursor updater if configured
        if let Some(path) = cursor_file {
            let mut cursor_rx = result_rx;
            let (final_tx, final_rx) =
                mpsc::channel::<(u64, BlockAndReceipts)>(self.config.channel_size);

            tokio::spawn(async move {
                // Track the highest contiguous block completed.
                // Blocks arrive out of order, so we buffer seen blocks and advance
                // the cursor only when we can move the contiguous frontier forward.
                let mut contiguous_cursor = effective_start.saturating_sub(1);
                let mut pending: BTreeSet<u64> = BTreeSet::new();
                let mut batch_count = 0u64;

                while let Some((block_num, block)) = cursor_rx.recv().await {
                    pending.insert(block_num);
                    batch_count += 1;

                    // Advance contiguous cursor as far as possible
                    while pending.first().copied() == Some(contiguous_cursor + 1) {
                        contiguous_cursor += 1;
                        pending.pop_first();
                    }

                    // Update cursor every 100 blocks
                    if batch_count.is_multiple_of(100) {
                        if let Err(e) = write_cursor(&path, contiguous_cursor) {
                            warn!(error = %e, "Failed to write cursor file");
                        }
                    }

                    if final_tx.send((block_num, block)).await.is_err() {
                        break;
                    }
                }

                // Final cursor write
                if let Err(e) = write_cursor(&path, contiguous_cursor) {
                    warn!(error = %e, "Failed to write final cursor");
                }
            });

            Ok(final_rx)
        } else {
            Ok(result_rx)
        }
    }
}

/// Read cursor value from file. Returns None if file doesn't exist.
pub fn read_cursor(path: &Path) -> Result<Option<u64>> {
    match std::fs::read_to_string(path) {
        Ok(content) => {
            let trimmed = content.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let block_num: u64 = trimmed
                .parse()
                .map_err(|e| eyre::eyre!("Invalid cursor file content '{}': {}", trimmed, e))?;
            Ok(Some(block_num))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(eyre::eyre!("Failed to read cursor file: {e}")),
    }
}

/// Write cursor value to file atomically (write to temp, then rename).
/// Creates parent directories if needed.
pub fn write_cursor(path: &Path, block_number: u64) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| eyre::eyre!("Failed to create cursor directory: {e}"))?;
    }
    // Atomic write: write to temp file, then rename
    let tmp_path = path.with_extension("tmp");
    std::fs::write(&tmp_path, block_number.to_string())
        .map_err(|e| eyre::eyre!("Failed to write cursor temp file: {e}"))?;
    std::fs::rename(&tmp_path, path)
        .map_err(|e| eyre::eyre!("Failed to rename cursor temp file: {e}"))?;
    Ok(())
}

/// Range configuration validation and cursor file operations:
/// - Config rejects invalid ranges (start > end), zero workers, zero channel
/// - Cursor file: write/read round-trip, nested dir creation, atomic rename
/// - Effective start: from config, from cursor, cursor past end
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_validates_start_before_end() {
        let config = RangeConfig {
            start_block: 100,
            end_block: 50,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_validates_workers_positive() {
        let config = RangeConfig {
            start_block: 1,
            end_block: 100,
            workers: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_validates_channel_size_positive() {
        let config = RangeConfig {
            start_block: 1,
            end_block: 100,
            channel_size: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn config_valid_range() {
        let config = RangeConfig {
            start_block: 1,
            end_block: 100,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn config_single_block_range() {
        let config = RangeConfig {
            start_block: 50,
            end_block: 50,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn cursor_roundtrip() {
        let dir = std::env::temp_dir().join("hypercore_test_cursor");
        let path = dir.join("cursor.txt");

        // Clean up from previous runs
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);

        // No file yet
        assert_eq!(read_cursor(&path).unwrap(), None);

        // Write and read back
        write_cursor(&path, 12345).unwrap();
        assert_eq!(read_cursor(&path).unwrap(), Some(12345));

        // Overwrite
        write_cursor(&path, 99999).unwrap();
        assert_eq!(read_cursor(&path).unwrap(), Some(99999));

        // Clean up
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn cursor_creates_parent_dirs() {
        let dir = std::env::temp_dir().join("hypercore_test_nested/sub/dir");
        let path = dir.join("cursor.txt");

        let _ = std::fs::remove_file(&path);

        write_cursor(&path, 42).unwrap();
        assert_eq!(read_cursor(&path).unwrap(), Some(42));

        // Clean up
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir_all(std::env::temp_dir().join("hypercore_test_nested"));
    }

    #[test]
    fn effective_start_without_cursor() {
        let config = RangeConfig {
            start_block: 100,
            end_block: 200,
            cursor_file: None,
            ..Default::default()
        };
        assert_eq!(config.effective_start().unwrap(), 100);
    }

    #[test]
    fn effective_start_with_cursor() {
        let dir = std::env::temp_dir().join("hypercore_test_effective");
        let path = dir.join("cursor.txt");
        write_cursor(&path, 150).unwrap();

        let config = RangeConfig {
            start_block: 100,
            end_block: 200,
            cursor_file: Some(path.clone()),
            ..Default::default()
        };
        assert_eq!(config.effective_start().unwrap(), 151);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }

    #[test]
    fn effective_start_cursor_past_end() {
        let dir = std::env::temp_dir().join("hypercore_test_past_end");
        let path = dir.join("cursor.txt");
        write_cursor(&path, 200).unwrap();

        let config = RangeConfig {
            start_block: 100,
            end_block: 200,
            cursor_file: Some(path.clone()),
            ..Default::default()
        };
        assert!(config.effective_start().is_err());

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}

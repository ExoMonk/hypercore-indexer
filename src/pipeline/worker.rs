use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tracing::{debug, warn};

use crate::s3::client::HyperEvmS3Client;
use crate::s3::codec;
use crate::types::BlockAndReceipts;

use super::progress::ProgressTracker;

/// Spawn worker tasks that fetch blocks from S3 in parallel.
///
/// Workers pull block numbers from `work_rx`, fetch + decode from S3, and send
/// results through `result_tx`. Concurrency is bounded by the semaphore.
pub async fn spawn_workers(
    s3_client: Arc<HyperEvmS3Client>,
    semaphore: Arc<Semaphore>,
    mut work_rx: mpsc::Receiver<u64>,
    result_tx: mpsc::Sender<(u64, BlockAndReceipts)>,
    progress: Arc<ProgressTracker>,
    retry_attempts: u32,
    retry_delay_ms: u64,
) {
    let mut join_set = tokio::task::JoinSet::new();

    loop {
        tokio::select! {
            // Accept new work items
            block_num = work_rx.recv() => {
                match block_num {
                    Some(num) => {
                        let client = Arc::clone(&s3_client);
                        let sem = Arc::clone(&semaphore);
                        let tx = result_tx.clone();
                        let prog = Arc::clone(&progress);

                        join_set.spawn(fetch_block_task(
                            client, sem, num, tx, prog,
                            retry_attempts, retry_delay_ms,
                        ));
                    }
                    None => {
                        // Work channel closed — no more blocks to fetch.
                        // Wait for all in-flight tasks to finish.
                        break;
                    }
                }
            }
            // Reap completed tasks to avoid unbounded JoinSet growth
            Some(result) = join_set.join_next(), if !join_set.is_empty() => {
                if let Err(e) = result {
                    warn!("Worker task panicked: {e}");
                }
            }
        }
    }

    // Drain remaining tasks
    while let Some(result) = join_set.join_next().await {
        if let Err(e) = result {
            warn!("Worker task panicked: {e}");
        }
    }
}

/// Single block fetch task with retry logic.
async fn fetch_block_task(
    s3_client: Arc<HyperEvmS3Client>,
    semaphore: Arc<Semaphore>,
    block_number: u64,
    result_tx: mpsc::Sender<(u64, BlockAndReceipts)>,
    progress: Arc<ProgressTracker>,
    retry_attempts: u32,
    retry_delay_ms: u64,
) {
    // Acquire semaphore permit to bound concurrency
    let _permit = match semaphore.acquire().await {
        Ok(permit) => permit,
        Err(_) => {
            warn!(block_number, "Semaphore closed, abandoning block fetch");
            progress.record_failure();
            return;
        }
    };

    let mut last_err = String::new();

    for attempt in 0..=retry_attempts {
        if attempt > 0 {
            let delay = retry_delay_ms * 2u64.pow(attempt - 1);
            debug!(block_number, attempt, delay_ms = delay, "Retrying block fetch");
            tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;
        }

        match s3_client.fetch_block_raw(block_number).await {
            Ok(compressed) => match codec::decode_block(&compressed) {
                Ok(block) => {
                    // Send through channel; if receiver is dropped, just return
                    let _ = result_tx.send((block_number, block)).await;
                    progress.record_success();
                    return;
                }
                Err(e) => {
                    last_err = format!("decode error: {e}");
                    // Decode errors are unlikely to succeed on retry (corrupt data)
                    break;
                }
            },
            Err(e) => {
                last_err = format!("S3 fetch error: {e}");
            }
        }
    }

    // All attempts exhausted or non-retryable error
    warn!(block_number, error = %last_err, "Skipping block after failed attempts");
    progress.record_failure();
}

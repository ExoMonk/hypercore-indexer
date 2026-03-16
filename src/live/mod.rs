pub mod poll;
pub mod tip;

use std::collections::BTreeSet;
use std::str::FromStr;
use std::sync::Arc;

use tracing::{debug, info, warn};

use crate::config::{Hip4Config, LiveConfig, PipelineConfig, StorageConfig};
use crate::decode;
use crate::error::Result;
use crate::hip4;
use crate::hip4::api::HyperCoreApiClient;
use crate::hip4::poller::run_hip4_poller;
use crate::pipeline::range::{RangeConfig, RangeFetcher};
use crate::s3::client::HyperEvmS3Client;
use crate::s3::codec;
use crate::storage::Storage;

use poll::AdaptiveInterval;

/// Check if an error represents a "block not found" (S3 404/NoSuchKey).
/// Returns true for 404-like errors, false for real/transient errors.
pub(crate) fn is_block_not_found(err: &eyre::Report) -> bool {
    let msg = err.to_string();
    msg.contains("NoSuchKey") || msg.contains("404") || msg.contains("not found")
}

/// Run the live indexer: follow the chain tip by polling S3 for new blocks.
///
/// 1. Detect gaps and backfill via parallel S3 fetch if far behind.
/// 2. Follow the tip sequentially with adaptive polling.
/// 3. Graceful shutdown on Ctrl+C.
#[allow(clippy::too_many_arguments)]
pub async fn run_live(
    s3_client: Arc<HyperEvmS3Client>,
    storage: Box<dyn Storage>,
    live_config: &LiveConfig,
    pipeline_config: &PipelineConfig,
    storage_config: &StorageConfig,
    hip4_config: &Hip4Config,
    chain_id: u64,
    network: &str,
) -> Result<()> {
    // Wrap storage in Arc so it can be shared with the HIP4 poller
    let storage: Arc<dyn Storage> = Arc::from(storage);

    // Get current cursor from DB, or discover tip if first run
    let mut cursor = match storage.get_cursor(network).await? {
        Some(c) => {
            info!("[LIVE] Resuming from block {} (cursor)", c);
            c
        }
        None => {
            info!("[LIVE] No cursor found, discovering chain tip...");
            // Use RPC for instant tip discovery, fall back to S3 probing
            let rpc_url = crate::s3::client::Network::from_str(network)
                .unwrap_or_default()
                .rpc_url();
            let tip = match tip::get_rpc_tip(rpc_url).await {
                Ok(t) => {
                    info!(tip = t, "[LIVE] Chain tip from RPC");
                    t
                }
                Err(e) => {
                    warn!("[LIVE] RPC tip failed ({e}), falling back to S3 probing...");
                    let known = tip::find_existing_block(&s3_client).await?;
                    tip::find_s3_tip(&s3_client, known).await?
                }
            };
            let initial = tip.saturating_sub(1);
            storage.set_cursor(network, initial).await?;
            info!("[LIVE] Starting from chain tip (block {})", tip);
            initial
        }
    };

    let mut interval = AdaptiveInterval::new(
        live_config.poll_interval_ms,
        live_config.min_poll_interval_ms,
        live_config.poll_decay,
    );

    // Initial gap detection
    cursor = detect_and_backfill_gap(
        &s3_client,
        &*storage,
        live_config,
        pipeline_config,
        storage_config,
        hip4_config,
        chain_id,
        network,
        cursor,
    )
    .await?;

    info!("[LIVE] Caught up. Following tip...");

    // Spawn HIP4 API poller if api_url is configured
    let (poller_shutdown_tx, poller_shutdown_rx) = tokio::sync::watch::channel(false);
    let poller_handle = if hip4_config.api_url.is_some() {
        let api_url = hip4_config.api_url.as_deref().unwrap();
        let api_client = HyperCoreApiClient::new(api_url);
        let poller_storage = Arc::clone(&storage);
        let poller_config = hip4_config.clone();
        Some(tokio::spawn(async move {
            if let Err(e) = run_hip4_poller(api_client, poller_storage, &poller_config, poller_shutdown_rx).await {
                warn!("[LIVE] HIP4 poller exited with error: {e}");
            }
        }))
    } else {
        None
    };

    // Register Ctrl+C handler once (not per-iteration)
    let shutdown = tokio::signal::ctrl_c();
    tokio::pin!(shutdown);

    // Tip-following loop with graceful shutdown
    loop {
        // Check for Ctrl+C
        tokio::select! {
            _ = &mut shutdown => {
                info!("[LIVE] Shutting down gracefully...");
                break;
            }
            result = follow_tip_step(
                &s3_client,
                &*storage,
                live_config,
                pipeline_config,
                storage_config,
                hip4_config,
                chain_id,
                network,
                &mut cursor,
                &mut interval,
            ) => {
                match result {
                    Ok(()) => {}
                    Err(e) => {
                        warn!("[LIVE] Error in tip-following loop: {e}");
                        // Continue — transient errors shouldn't kill the loop
                    }
                }
            }
        }
    }

    // Signal the poller to stop and wait for it
    let _ = poller_shutdown_tx.send(true);
    if let Some(handle) = poller_handle {
        let _ = handle.await;
    }

    Ok(())
}

/// Run the live indexer starting from a specific block (--from flag).
/// Sets the cursor first, then delegates to `run_live`.
#[allow(clippy::too_many_arguments)]
pub async fn run_live_from(
    s3_client: Arc<HyperEvmS3Client>,
    storage: Box<dyn Storage>,
    live_config: &LiveConfig,
    pipeline_config: &PipelineConfig,
    storage_config: &StorageConfig,
    hip4_config: &Hip4Config,
    chain_id: u64,
    network: &str,
    from: u64,
) -> Result<()> {
    // Set the cursor to from-1 so the live loop starts fetching `from`
    let initial_cursor = from.saturating_sub(1);
    storage.set_cursor(network, initial_cursor).await?;
    info!(
        "[LIVE] Set initial cursor to {} (will start from block {})",
        initial_cursor, from
    );

    run_live(
        s3_client,
        storage,
        live_config,
        pipeline_config,
        storage_config,
        hip4_config,
        chain_id,
        network,
    )
    .await
}

/// Detect if we're far behind the S3 tip and backfill the gap using parallel workers.
/// Returns the updated cursor after backfill (or the original cursor if no gap).
#[allow(clippy::too_many_arguments)]
async fn detect_and_backfill_gap(
    s3_client: &Arc<HyperEvmS3Client>,
    storage: &dyn Storage,
    live_config: &LiveConfig,
    pipeline_config: &PipelineConfig,
    storage_config: &StorageConfig,
    hip4_config: &Hip4Config,
    chain_id: u64,
    network: &str,
    cursor: u64,
) -> Result<u64> {
    // Probe cursor + gap_threshold to check if we're far behind
    let probe_block = cursor + live_config.gap_threshold;
    let probe_result = s3_client.fetch_block_raw(probe_block).await;

    let is_behind = match probe_result {
        Ok(_) => true,
        Err(e) => {
            if is_block_not_found(&e) {
                false
            } else {
                // Real error — log and assume not behind (will retry in tip-following)
                warn!("[LIVE] Error probing gap at block {probe_block}: {e}");
                false
            }
        }
    };

    if !is_behind {
        return Ok(cursor);
    }

    // We're behind — find the tip and backfill
    let tip = tip::find_s3_tip(s3_client, cursor).await?;
    let gap = tip - cursor;
    info!("[LIVE] Gap detected: {gap} blocks behind. Backfilling via S3...");

    let start = cursor + 1;
    let end = tip;

    let config = RangeConfig {
        start_block: start,
        end_block: end,
        workers: live_config.backfill_workers,
        channel_size: pipeline_config.channel_size,
        retry_attempts: pipeline_config.retry_attempts,
        retry_delay_ms: pipeline_config.retry_delay_ms,
        cursor_file: None,
    };

    let fetcher = RangeFetcher::new(Arc::clone(s3_client), config)?;
    let mut rx = fetcher.run().await?;

    let batch_size = storage_config.batch_size;
    let mut buffer: Vec<decode::types::DecodedBlock> = Vec::with_capacity(batch_size);
    let mut hip4_buffer: Vec<hip4::types::Hip4BlockData> = Vec::with_capacity(batch_size);
    let mut count = 0u64;

    // Track contiguous frontier to avoid advancing cursor past gaps.
    // Blocks arrive out of order from parallel workers; we only set the DB cursor
    // to the highest block number where ALL preceding blocks have been stored.
    let mut contiguous_cursor = start.saturating_sub(1);
    let mut pending: BTreeSet<u64> = BTreeSet::new();

    while let Some((_block_num, raw_block)) = rx.recv().await {
        let decoded = decode::decode_block(&raw_block, chain_id)?;
        let block_num = decoded.number;

        // Process HIP4 data from the decoded block
        if hip4_config.enabled {
            let hip4_data = hip4::process_block(&decoded, hip4_config);
            hip4_buffer.push(hip4_data);
        }

        buffer.push(decoded);
        count += 1;

        // Track this block in the contiguous set
        pending.insert(block_num);
        while pending.first().copied() == Some(contiguous_cursor + 1) {
            contiguous_cursor += 1;
            pending.pop_first();
        }

        if buffer.len() >= batch_size {
            storage
                .insert_batch_and_set_cursor(&buffer, network, contiguous_cursor)
                .await?;

            // Insert HIP4 data — failure is logged but does not kill the batch
            for hip4_data in &hip4_buffer {
                if let Err(e) = storage.insert_hip4_data(hip4_data).await {
                    warn!("[LIVE] Failed to insert HIP4 data in backfill batch: {e}");
                }
            }

            info!(
                batch_blocks = buffer.len(),
                cursor = contiguous_cursor,
                total = count,
                "[LIVE] Backfill batch flushed"
            );
            buffer.clear();
            hip4_buffer.clear();
        }
    }

    // Flush remaining
    if !buffer.is_empty() {
        storage
            .insert_batch_and_set_cursor(&buffer, network, contiguous_cursor)
            .await?;

        // Insert remaining HIP4 data — failure is logged but does not kill the batch
        for hip4_data in &hip4_buffer {
            if let Err(e) = storage.insert_hip4_data(hip4_data).await {
                warn!("[LIVE] Failed to insert HIP4 data in final backfill batch: {e}");
            }
        }

        info!(
            batch_blocks = buffer.len(),
            cursor = contiguous_cursor,
            "[LIVE] Backfill final batch flushed"
        );
    }

    info!("[LIVE] Backfill complete: {count} blocks ingested");

    // Return the updated cursor from DB
    let new_cursor = storage.get_cursor(network).await?.unwrap_or(cursor);
    Ok(new_cursor)
}

/// Single step of the tip-following loop: try to fetch the next block.
#[allow(clippy::too_many_arguments)]
async fn follow_tip_step(
    s3_client: &Arc<HyperEvmS3Client>,
    storage: &dyn Storage,
    live_config: &LiveConfig,
    pipeline_config: &PipelineConfig,
    storage_config: &StorageConfig,
    hip4_config: &Hip4Config,
    chain_id: u64,
    network: &str,
    cursor: &mut u64,
    interval: &mut AdaptiveInterval,
) -> Result<()> {
    // Sleep if needed
    if interval.should_sleep() {
        let ms = interval.current();
        debug!("[LIVE] Waiting... (poll: {ms}ms)");
        tokio::time::sleep(tokio::time::Duration::from_millis(ms)).await;
    }

    let next_block = *cursor + 1;

    match s3_client.fetch_block_raw(next_block).await {
        Ok(compressed) => {
            let raw_block = codec::decode_block(&compressed)?;
            let decoded = decode::decode_block(&raw_block, chain_id)?;

            let tx_count = decoded.transactions.len();
            let system_count = decoded.system_transfers.len();

            // Process HIP4 data before consuming decoded
            let hip4_data = if hip4_config.enabled {
                Some(hip4::process_block(&decoded, hip4_config))
            } else {
                None
            };

            storage
                .insert_batch_and_set_cursor(&[decoded], network, next_block)
                .await?;

            // Insert HIP4 data AFTER block data — HIP4 failure must not block block storage.
            // If this fails, the block is still stored; HIP4 will be retried on reprocess.
            if let Some(hip4_data) = &hip4_data {
                if let Err(e) = storage.insert_hip4_data(hip4_data).await {
                    warn!("[LIVE] Failed to insert HIP4 data for block {next_block}: {e}");
                }
            }

            *cursor = next_block;
            interval.reset();

            info!(
                "[LIVE] Block {} | {} txs | {} system",
                next_block, tx_count, system_count
            );

            // After processing a block, check for gap (we may have fallen behind during processing)
            // Only do this periodically to avoid excessive probes
            if live_config.gap_threshold > 0
                && next_block.is_multiple_of(live_config.gap_threshold)
            {
                let new_cursor = detect_and_backfill_gap(
                    s3_client,
                    storage,
                    live_config,
                    &PipelineConfig {
                        workers: live_config.backfill_workers,
                        ..*pipeline_config
                    },
                    storage_config,
                    hip4_config,
                    chain_id,
                    network,
                    *cursor,
                )
                .await?;
                *cursor = new_cursor;
            }
        }
        Err(e) => {
            if is_block_not_found(&e) {
                // Block doesn't exist yet — backoff
                interval.backoff();
            } else {
                // Real error — retry with exponential backoff
                warn!("[LIVE] Error fetching block {next_block}: {e}");
                for attempt in 1..=pipeline_config.retry_attempts {
                    let delay = pipeline_config.retry_delay_ms * 2u64.pow(attempt - 1);
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay)).await;

                    match s3_client.fetch_block_raw(next_block).await {
                        Ok(compressed) => {
                            let raw_block = codec::decode_block(&compressed)?;
                            let decoded = decode::decode_block(&raw_block, chain_id)?;

                            let tx_count = decoded.transactions.len();
                            let system_count = decoded.system_transfers.len();

                            // Process HIP4 data before consuming decoded
                            let hip4_data = if hip4_config.enabled {
                                Some(hip4::process_block(&decoded, hip4_config))
                            } else {
                                None
                            };

                            storage
                                .insert_batch_and_set_cursor(&[decoded], network, next_block)
                                .await?;

                            // Insert HIP4 data AFTER block — failure must not block block storage
                            if let Some(hip4_data) = &hip4_data {
                                if let Err(e) = storage.insert_hip4_data(hip4_data).await {
                                    warn!("[LIVE] Failed to insert HIP4 data for block {next_block} (retry {attempt}): {e}");
                                }
                            }

                            *cursor = next_block;
                            interval.reset();

                            info!(
                                "[LIVE] Block {} | {} txs | {} system (retry {})",
                                next_block, tx_count, system_count, attempt
                            );
                            return Ok(());
                        }
                        Err(retry_err) => {
                            if is_block_not_found(&retry_err) {
                                // Became a 404 — just backoff
                                interval.backoff();
                                return Ok(());
                            }
                            warn!(
                                "[LIVE] Retry {attempt}/{} for block {next_block}: {retry_err}",
                                pipeline_config.retry_attempts
                            );
                        }
                    }
                }
                // All retries exhausted — do NOT advance cursor.
                // The next loop iteration will retry this block with adaptive backoff.
                warn!(
                    "[LIVE] All {} retries exhausted for block {next_block}, will retry next loop",
                    pipeline_config.retry_attempts
                );
                interval.backoff();
            }
        }
    }

    Ok(())
}

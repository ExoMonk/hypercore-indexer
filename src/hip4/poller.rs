use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tracing::{info, warn};

use crate::config::Hip4Config;
use crate::storage::Storage;

use super::api::{self, HyperCoreApiClient};

/// Run the HIP4 API poller as a background task.
///
/// Spawns two loops:
/// - outcomeMeta poll every `meta_poll_interval_s` (default 60s)
/// - allMids poll every `price_poll_interval_s` (default 5s)
///
/// Gracefully stops when `shutdown` receives `true`.
/// Never panics on API errors — logs warnings and retries on next interval.
pub async fn run_hip4_poller(
    api_client: HyperCoreApiClient,
    storage: Arc<dyn Storage>,
    config: &Hip4Config,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> eyre::Result<()> {
    let api_url = match &config.api_url {
        Some(url) => url.clone(),
        None => {
            // No API URL configured — poller not needed
            return Ok(());
        }
    };

    let meta_interval_s = config.meta_poll_interval_s.unwrap_or(60);
    let price_interval_s = config.price_poll_interval_s.unwrap_or(5);

    info!(
        api_url = %api_url,
        meta_interval_s,
        price_interval_s,
        "[HIP4-POLLER] Starting"
    );

    let api_client = Arc::new(api_client);
    let api_meta = Arc::clone(&api_client);
    let api_price = Arc::clone(&api_client);
    let storage_meta = Arc::clone(&storage);
    let storage_price = Arc::clone(&storage);

    let mut shutdown_meta = shutdown.clone();
    let mut shutdown_price = shutdown.clone();

    // Spawn meta poller
    let meta_handle = tokio::spawn(async move {
        let interval = tokio::time::Duration::from_secs(meta_interval_s);
        loop {
            // Poll first, then wait
            poll_outcome_meta(&api_meta, storage_meta.as_ref()).await;

            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = shutdown_meta.changed() => {
                    if *shutdown_meta.borrow() {
                        info!("[HIP4-POLLER] Meta poller shutting down");
                        break;
                    }
                }
            }
        }
    });

    // Spawn price poller
    let price_handle = tokio::spawn(async move {
        let interval = tokio::time::Duration::from_secs(price_interval_s);
        loop {
            poll_all_mids(&api_price, storage_price.as_ref()).await;

            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = shutdown_price.changed() => {
                    if *shutdown_price.borrow() {
                        info!("[HIP4-POLLER] Price poller shutting down");
                        break;
                    }
                }
            }
        }
    });

    // Wait for shutdown signal, then wait for both tasks to complete
    let _ = shutdown.changed().await;

    // Tasks will exit on next iteration when they see the shutdown signal
    let _ = meta_handle.await;
    let _ = price_handle.await;

    info!("[HIP4-POLLER] Stopped");
    Ok(())
}

/// Poll outcomeMeta and upsert markets. Never panics.
async fn poll_outcome_meta(client: &HyperCoreApiClient, storage: &dyn Storage) {
    match client.outcome_meta().await {
        Ok(resp) => {
            let markets = api::outcome_meta_to_markets(&resp);
            if markets.is_empty() {
                return;
            }
            match storage.upsert_hip4_markets(&markets).await {
                Ok(()) => {
                    info!(count = markets.len(), "[HIP4-POLLER] Upserted markets");
                }
                Err(e) => {
                    warn!("[HIP4-POLLER] Failed to upsert markets: {e}");
                }
            }
        }
        Err(e) => {
            warn!("[HIP4-POLLER] outcomeMeta poll failed: {e}");
        }
    }
}

/// Poll allMids and insert prices. Never panics.
async fn poll_all_mids(client: &HyperCoreApiClient, storage: &dyn Storage) {
    match client.all_mids_hip4().await {
        Ok(ref prices) => {
            if prices.is_empty() {
                return;
            }
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let rows = api::prices_to_rows(prices, now_ms);
            match storage.insert_hip4_prices(&rows).await {
                Ok(()) => {
                    info!(count = rows.len(), "[HIP4-POLLER] Inserted prices");
                }
                Err(e) => {
                    warn!("[HIP4-POLLER] Failed to insert prices: {e}");
                }
            }
        }
        Err(e) => {
            warn!("[HIP4-POLLER] allMids poll failed: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn no_api_url_returns_immediately() {
        let config = Hip4Config {
            enabled: true,
            contest_address: None,
            api_url: None,
            meta_poll_interval_s: None,
            price_poll_interval_s: None,
        };

        // This should return Ok immediately since api_url is None.
        // We create a dummy client that will never be used.
        let client = HyperCoreApiClient::new("http://unused");
        let (_tx, rx) = tokio::sync::watch::channel(false);

        // Create a mock storage that implements the trait
        // Since there's no api_url, poller returns immediately without touching storage
        let storage: Arc<dyn Storage> = Arc::new(crate::storage::sqlite::SqliteStorage::connect("sqlite::memory:").await.unwrap());

        let result = run_hip4_poller(client, storage, &config, rx).await;
        assert!(result.is_ok());
    }
}

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

mod config;
mod decode;
mod error;
mod fills;
mod hip4;
mod live;
mod pipeline;
mod s3;
mod storage;
mod types;

use config::Config;
use error::Result;
use pipeline::range::{RangeConfig, RangeFetcher};
use s3::client::{HyperEvmS3Client, Network};
use s3::codec;

#[derive(Parser)]
#[command(
    name = "hypercore-indexer",
    about = "Hyperliquid HyperCore/HyperEVM S3 block indexer"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Path to config file (default: ./hypercore.toml)
    #[arg(long, global = true, default_value = "hypercore.toml")]
    config: PathBuf,

    /// Network to index: mainnet or testnet (overrides config)
    #[arg(long, global = true, env = "HL_NETWORK")]
    network: Option<Network>,

    /// AWS region for the S3 bucket (overrides config)
    #[arg(long, global = true, env = "AWS_REGION")]
    region: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Fetch and decode a block from S3
    FetchBlock {
        /// Block number to fetch
        block_number: u64,

        /// Print raw decompressed hex instead of decoded summary
        #[arg(long)]
        raw: bool,

        /// Print only system transactions
        #[arg(long)]
        system_txs: bool,
    },

    /// Fetch, decode, and display a block with full hash computation
    DecodeBlock {
        /// Block number to fetch and decode
        block_number: u64,
    },

    /// Initialize a hypercore.toml config file
    Init {
        /// Storage backend: postgres or sqlite (default: sqlite)
        #[arg(long, default_value = "sqlite")]
        storage: String,

        /// Database URL (default: sqlite:./hypercore.db)
        #[arg(long)]
        url: Option<String>,

        /// Target network: mainnet or testnet (default: mainnet)
        #[arg(long, default_value = "mainnet")]
        target_network: String,
    },

    /// Backfill a range of blocks from S3. If --to is omitted, backfills to the
    /// S3 tip and then switches to live mode automatically.
    Backfill {
        /// Start block number (if omitted, resumes from cursor)
        #[arg(long)]
        from: Option<u64>,

        /// End block number (if omitted, backfills to tip then switches to live)
        #[arg(long)]
        to: Option<u64>,

        /// Number of concurrent workers (default: 64)
        #[arg(long, default_value = "64")]
        workers: usize,

        /// Path to cursor file for resume support
        #[arg(long)]
        cursor_file: Option<PathBuf>,

        /// PostgreSQL database URL. When provided, blocks are stored in PostgreSQL
        /// and the DB cursor is used instead of the file cursor.
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,

        /// Number of decoded blocks to buffer before flushing to storage (default: 100)
        #[arg(long, default_value = "100")]
        batch_size: usize,
    },

    /// Run only the HIP4 API poller (markets + prices) without the full indexer
    Hip4Poll {
        /// Database URL (overrides config)
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
    },

    /// Backfill trade fills from S3 node_fills data for a date range
    FillsBackfill {
        /// Start date in YYYYMMDD format (e.g. "20260315")
        #[arg(long)]
        from_date: String,

        /// End date in YYYYMMDD format (inclusive, e.g. "20260316")
        #[arg(long)]
        to_date: String,

        /// Database URL (overrides config)
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
    },

    /// Follow the chain tip, continuously indexing new blocks from S3
    Live {
        /// Start block (if omitted, resumes from DB cursor)
        #[arg(long)]
        from: Option<u64>,

        /// Base poll interval in ms (overrides config)
        #[arg(long)]
        poll_interval: Option<u64>,

        /// Gap threshold for parallel backfill (overrides config)
        #[arg(long)]
        gap_threshold: Option<u64>,

        /// Database URL (overrides config)
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let cfg = Config::load(&cli.config)?;

    // CLI flags override config values
    let network: Network = cli
        .network
        .unwrap_or_else(|| cfg.network.name.parse().unwrap_or_default());
    let region = cli.region.or_else(|| {
        if cfg.network.region != "ap-northeast-1" {
            Some(cfg.network.region.clone())
        } else {
            None
        }
    });

    match cli.command {
        Commands::Init {
            storage,
            url,
            target_network: net,
        } => {
            let config_path = &cli.config;
            if config_path.exists() {
                return Err(eyre::eyre!(
                    "{} already exists. Delete it first or edit manually.",
                    config_path.display()
                ));
            }

            let db_url = url.unwrap_or_else(|| {
                if storage == "sqlite" {
                    "sqlite:./hypercore.db".to_string()
                } else {
                    // Use env var reference — user sets DATABASE_URL or PGPASSWORD in env
                    "postgres://postgres:postgres@localhost:5432/hypercore".to_string()
                }
            });

            let content = format!(
                r#"[network]
name = "{net}"
region = "ap-northeast-1"

[storage]
url = "{db_url}"
batch_size = 100

[pipeline]
workers = 64
channel_size = 1024
retry_attempts = 3
retry_delay_ms = 1000

[live]
poll_interval_ms = 1000
min_poll_interval_ms = 200
poll_decay = 0.67
gap_threshold = 100
backfill_workers = 64

[hip4]
enabled = false
# contest_address = "0x4fd772e5708da2a7f097f51b3127e515a72744bd"
# api_url = "https://api.hyperliquid-testnet.xyz/info"
# meta_poll_interval_s = 60
# price_poll_interval_s = 5

[fills]
enabled = false
# bucket = "hl-mainnet-node-data"
# mirror_hip4 = true
"#
            );

            std::fs::write(config_path, &content)
                .map_err(|e| eyre::eyre!("Failed to write {}: {e}", config_path.display()))?;

            println!("Created {}", config_path.display());
            println!();
            println!("  network:  {net}");
            println!("  storage:  {db_url}");
            println!();
            println!("Next steps:");
            println!("  cargo run -- backfill --from <start_block> --to <end_block>");
        }

        Commands::FetchBlock {
            block_number,
            raw,
            system_txs,
        } => {
            let client = HyperEvmS3Client::new(region.clone(), network).await?;

            info!(block_number, "Fetching block from S3");
            let compressed = client.fetch_block_raw(block_number).await?;

            if raw {
                let decompressed = codec::decompress_raw(&compressed)?;
                // Print as hex
                println!("{}", hex::encode(&decompressed));
                return Ok(());
            }

            let block_and_receipts = codec::decode_block(&compressed)?;
            let summary = block_and_receipts.summary();

            if system_txs {
                // Print only system txs
                if summary.system_txs.is_empty() {
                    println!("No system transactions in block {block_number}");
                } else {
                    println!(
                        "System transactions in block {block_number} ({} total):",
                        summary.system_tx_count
                    );
                    let json = serde_json::to_string_pretty(&summary.system_txs)
                        .map_err(|e| eyre::eyre!("JSON serialization failed: {e}"))?;
                    println!("{json}");
                }
            } else {
                // Print full summary
                let json = serde_json::to_string_pretty(&summary)
                    .map_err(|e| eyre::eyre!("JSON serialization failed: {e}"))?;
                println!("{json}");
            }
        }

        Commands::DecodeBlock { block_number } => {
            let client = HyperEvmS3Client::new(region.clone(), network).await?;
            let chain_id = network.chain_id();

            info!(block_number, %chain_id, "Fetching and decoding block");
            let compressed = client.fetch_block_raw(block_number).await?;
            let block_and_receipts = codec::decode_block(&compressed)?;
            let decoded = decode::decode_block(&block_and_receipts, chain_id)?;

            let json = serde_json::to_string_pretty(&decoded)
                .map_err(|e| eyre::eyre!("JSON serialization failed: {e}"))?;
            println!("{json}");
        }

        Commands::Backfill {
            from,
            to,
            workers,
            cursor_file,
            database_url,
            batch_size,
        } => {
            let client = HyperEvmS3Client::new(region, network).await?;
            let client = Arc::new(client);
            let chain_id = network.chain_id();
            let network_name = network.to_string();

            // Resolve database URL: CLI flag > env var > config file
            let effective_db_url = cfg.database_url(database_url.as_deref());
            let effective_workers = if workers == 64 {
                cfg.pipeline.workers
            } else {
                workers
            };
            let effective_batch_size = if batch_size == 100 {
                cfg.storage.batch_size
            } else {
                batch_size
            };

            // Connect to storage backend if database_url is available
            // Detection: sqlite: → SQLite, http(s):// → ClickHouse, else → PostgreSQL
            let db_storage: Option<Box<dyn storage::Storage>> = match &effective_db_url {
                Some(url) if url.starts_with("sqlite:") => {
                    let sqlite = storage::sqlite::SqliteStorage::connect(url).await?;
                    sqlite.ensure_schema().await?;
                    Some(Box::new(sqlite))
                }
                Some(url) if url.starts_with("http://") || url.starts_with("https://") => {
                    let ch = storage::clickhouse::ClickHouseStorage::connect(url).await?;
                    ch.ensure_schema().await?;
                    Some(Box::new(ch))
                }
                Some(url) => {
                    let pg = storage::postgres::PostgresStorage::connect(url).await?;
                    pg.ensure_schema().await?;
                    Some(Box::new(pg))
                }
                None => None,
            };

            // Determine start block: --from flag, or DB cursor, or file cursor, or discover tip.
            let start_block = match from {
                Some(block) => block,
                None => {
                    // Try DB cursor first
                    if let Some(ref store) = &db_storage {
                        match store.get_cursor(&network_name).await? {
                            Some(cursor) => {
                                info!(cursor, resumed_from = cursor + 1, "Resuming from DB cursor");
                                cursor + 1
                            }
                            None => {
                                // No cursor — discover tip and start from there
                                info!("No cursor found, discovering S3 tip to start from current chain head...");
                                let known = live::tip::find_existing_block(&client).await?;
                                let tip = live::tip::find_s3_tip(&client, known).await?;
                                info!(tip, "Starting from chain tip");
                                tip
                            }
                        }
                    } else {
                        // File-based cursor
                        let cursor_path = cursor_file.clone().unwrap_or_else(|| {
                            let mut path = dirs_path(&network);
                            path.push(format!("cursor_{}.txt", network));
                            path
                        });
                        match pipeline::range::read_cursor(&cursor_path)? {
                            Some(cursor) => cursor + 1,
                            None => {
                                // No cursor — discover tip
                                info!("No cursor found, discovering S3 tip to start from current chain head...");
                                let known = live::tip::find_existing_block(&client).await?;
                                let tip = live::tip::find_s3_tip(&client, known).await?;
                                info!(tip, "Starting from chain tip");
                                tip
                            }
                        }
                    }
                }
            };

            let effective_cursor_file = if db_storage.is_some() {
                None
            } else {
                Some(cursor_file.unwrap_or_else(|| {
                    let mut path = dirs_path(&network);
                    path.push(format!("cursor_{}.txt", network));
                    path
                }))
            };

            // If --to is omitted, discover S3 tip and plan to transition to live after
            let transition_to_live = to.is_none();
            let end_block = match to {
                Some(t) => t,
                None => {
                    info!("No --to specified, discovering S3 tip...");
                    let tip = live::tip::find_s3_tip(&client, start_block).await?;
                    info!(tip, "Backfilling to tip, then switching to live mode");
                    tip
                }
            };

            let config = RangeConfig {
                start_block,
                end_block,
                workers: effective_workers,
                channel_size: cfg.pipeline.channel_size,
                retry_attempts: cfg.pipeline.retry_attempts,
                retry_delay_ms: cfg.pipeline.retry_delay_ms,
                cursor_file: effective_cursor_file,
            };

            let fetcher = RangeFetcher::new(client, config)?;
            let mut rx = fetcher.run().await?;

            let mut count = 0u64;

            if let Some(store) = db_storage {
                // Storage mode: decode, buffer, and batch insert.
                // Track contiguous frontier to avoid advancing cursor past gaps
                // when parallel workers skip blocks after exhausting retries.
                use std::collections::BTreeSet;

                let mut buffer: Vec<decode::types::DecodedBlock> =
                    Vec::with_capacity(effective_batch_size);
                let mut hip4_buffer: Vec<hip4::types::Hip4BlockData> =
                    Vec::with_capacity(effective_batch_size);
                let mut contiguous_cursor = start_block.saturating_sub(1);
                let mut pending: BTreeSet<u64> = BTreeSet::new();

                while let Some((_block_num, raw_block)) = rx.recv().await {
                    let decoded = decode::decode_block(&raw_block, chain_id)?;
                    let block_num = decoded.number;

                    // Process HIP4 data from the decoded block
                    if cfg.hip4.enabled {
                        let hip4_data = hip4::process_block(&decoded, &cfg.hip4);
                        hip4_buffer.push(hip4_data);
                    }

                    buffer.push(decoded);
                    count += 1;

                    // Track contiguous frontier
                    pending.insert(block_num);
                    while pending.first().copied() == Some(contiguous_cursor + 1) {
                        contiguous_cursor += 1;
                        pending.pop_first();
                    }

                    if buffer.len() >= effective_batch_size {
                        store
                            .insert_batch_and_set_cursor(
                                &buffer,
                                &network_name,
                                contiguous_cursor,
                            )
                            .await?;

                        // Insert HIP4 data — failure is logged but does not kill the batch
                        for hip4_data in &hip4_buffer {
                            if let Err(e) = store.insert_hip4_data(hip4_data).await {
                                tracing::warn!("Failed to insert HIP4 data: {e}");
                            }
                        }

                        info!(
                            batch_blocks = buffer.len(),
                            cursor = contiguous_cursor,
                            total_received = count,
                            "Flushed batch to storage"
                        );
                        buffer.clear();
                        hip4_buffer.clear();
                    }
                }

                // Flush remaining blocks
                if !buffer.is_empty() {
                    store
                        .insert_batch_and_set_cursor(&buffer, &network_name, contiguous_cursor)
                        .await?;

                    // Insert remaining HIP4 data — failure is logged but does not kill the batch
                    for hip4_data in &hip4_buffer {
                        if let Err(e) = store.insert_hip4_data(hip4_data).await {
                            tracing::warn!("Failed to insert HIP4 data: {e}");
                        }
                    }

                    info!(
                        batch_blocks = buffer.len(),
                        cursor = contiguous_cursor,
                        "Flushed final batch to storage"
                    );
                }
            } else {
                // No storage: just consume blocks (original behavior)
                while let Some((block_num, _block)) = rx.recv().await {
                    count += 1;
                    if count.is_multiple_of(10000) {
                        info!(block_num, total_received = count, "Consuming blocks");
                    }
                }
            }

            info!(total_received = count, "Backfill complete");

            // If --to was omitted, transition to live mode
            if transition_to_live {
                if let Some(ref effective_db_url) = effective_db_url {
                    info!("[LIVE] Transitioning from backfill to live mode...");

                    let live_region = cfg.network.region.clone();
                    let live_client = Arc::new(
                        HyperEvmS3Client::new(Some(live_region), network).await?,
                    );

                    let live_storage: Box<dyn storage::Storage> = if effective_db_url.starts_with("sqlite:") {
                        let s = storage::sqlite::SqliteStorage::connect(effective_db_url).await?;
                        Box::new(s)
                    } else if effective_db_url.starts_with("http://") || effective_db_url.starts_with("https://") {
                        let c = storage::clickhouse::ClickHouseStorage::connect(effective_db_url).await?;
                        Box::new(c)
                    } else {
                        let p = storage::postgres::PostgresStorage::connect(effective_db_url).await?;
                        Box::new(p)
                    };

                    live::run_live(
                        live_client,
                        live_storage,
                        &cfg.live,
                        &cfg.pipeline,
                        &cfg.storage,
                        &cfg.hip4,
                        chain_id,
                        &network_name,
                    )
                    .await?;
                } else {
                    info!("Backfill complete. No storage configured — cannot transition to live mode.");
                }
            }
        }

        Commands::Hip4Poll { database_url } => {
            let effective_db_url = cfg.database_url(database_url.as_deref());
            let db_url = effective_db_url.ok_or_else(|| {
                eyre::eyre!(
                    "hip4-poll requires a database. Specify --database-url, set DATABASE_URL env, \
                     or add [storage].url to your config file."
                )
            })?;

            let api_url = cfg.hip4.api_url.clone().ok_or_else(|| {
                eyre::eyre!(
                    "hip4-poll requires [hip4].api_url in your config file."
                )
            })?;

            // Connect to storage backend
            let db_storage: Arc<dyn storage::Storage> = if db_url.starts_with("sqlite:") {
                let sqlite = storage::sqlite::SqliteStorage::connect(&db_url).await?;
                sqlite.ensure_schema().await?;
                Arc::new(sqlite)
            } else if db_url.starts_with("http://") || db_url.starts_with("https://") {
                let ch = storage::clickhouse::ClickHouseStorage::connect(&db_url).await?;
                ch.ensure_schema().await?;
                Arc::new(ch)
            } else {
                let pg = storage::postgres::PostgresStorage::connect(&db_url).await?;
                pg.ensure_schema().await?;
                Arc::new(pg)
            };

            let api_client = hip4::api::HyperCoreApiClient::new(&api_url);
            let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

            // Spawn shutdown signal handler
            tokio::spawn(async move {
                let _ = tokio::signal::ctrl_c().await;
                info!("[HIP4-POLL] Shutting down...");
                let _ = shutdown_tx.send(true);
            });

            hip4::poller::run_hip4_poller(api_client, db_storage, &cfg.hip4, shutdown_rx).await?;
        }

        Commands::FillsBackfill {
            from_date,
            to_date,
            database_url,
        } => {
            let client = HyperEvmS3Client::new(region, network).await?;
            let client = Arc::new(client);

            // Resolve database URL: CLI flag > env var > config file
            let effective_db_url = cfg.database_url(database_url.as_deref());
            let db_url = effective_db_url.ok_or_else(|| {
                eyre::eyre!(
                    "fills-backfill requires a database. Specify --database-url, set DATABASE_URL env, \
                     or add [storage].url to your config file."
                )
            })?;

            // Connect to storage backend
            let db_storage: Box<dyn storage::Storage> = if db_url.starts_with("sqlite:") {
                let sqlite = storage::sqlite::SqliteStorage::connect(&db_url).await?;
                sqlite.ensure_schema().await?;
                Box::new(sqlite)
            } else if db_url.starts_with("http://") || db_url.starts_with("https://") {
                let ch = storage::clickhouse::ClickHouseStorage::connect(&db_url).await?;
                ch.ensure_schema().await?;
                Box::new(ch)
            } else {
                let pg = storage::postgres::PostgresStorage::connect(&db_url).await?;
                pg.ensure_schema().await?;
                Box::new(pg)
            };

            let bucket = cfg.fills.bucket.clone();
            let mirror_hip4 = cfg.fills.mirror_hip4;

            let total = fills::backfill_fills(
                client,
                &bucket,
                &from_date,
                &to_date,
                &*db_storage,
                mirror_hip4,
            )
            .await?;

            info!(total_fills = total, "Fills backfill complete");
        }

        Commands::Live {
            from,
            poll_interval,
            gap_threshold,
            database_url,
        } => {
            let client = HyperEvmS3Client::new(region, network).await?;
            let client = Arc::new(client);
            let chain_id = network.chain_id();
            let network_name = network.to_string();

            // Resolve database URL: CLI flag > env var > config file
            let effective_db_url = cfg.database_url(database_url.as_deref());
            let db_url = effective_db_url.ok_or_else(|| {
                eyre::eyre!(
                    "Live mode requires a database. Specify --database-url, set DATABASE_URL env, \
                     or add [storage].url to your config file."
                )
            })?;

            // Connect to storage backend (same detection as Backfill)
            let db_storage: Box<dyn storage::Storage> = if db_url.starts_with("sqlite:") {
                let sqlite = storage::sqlite::SqliteStorage::connect(&db_url).await?;
                sqlite.ensure_schema().await?;
                Box::new(sqlite)
            } else if db_url.starts_with("http://") || db_url.starts_with("https://") {
                let ch = storage::clickhouse::ClickHouseStorage::connect(&db_url).await?;
                ch.ensure_schema().await?;
                Box::new(ch)
            } else {
                let pg = storage::postgres::PostgresStorage::connect(&db_url).await?;
                pg.ensure_schema().await?;
                Box::new(pg)
            };

            // Apply CLI overrides to live config
            let mut live_config = cfg.live;
            if let Some(pi) = poll_interval {
                live_config.poll_interval_ms = pi;
            }
            if let Some(gt) = gap_threshold {
                live_config.gap_threshold = gt;
            }

            if let Some(start_block) = from {
                live::run_live_from(
                    client,
                    db_storage,
                    &live_config,
                    &cfg.pipeline,
                    &cfg.storage,
                    &cfg.hip4,
                    chain_id,
                    &network_name,
                    start_block,
                )
                .await?;
            } else {
                live::run_live(
                    client,
                    db_storage,
                    &live_config,
                    &cfg.pipeline,
                    &cfg.storage,
                    &cfg.hip4,
                    chain_id,
                    &network_name,
                )
                .await?;
            }
        }
    }

    Ok(())
}

/// Default directory for hypercore-indexer state files.
fn dirs_path(_network: &Network) -> PathBuf {
    let home = std::env::var("HOME")
        .or_else(|_| std::env::var("USERPROFILE"))
        .unwrap_or_else(|_| ".".to_string());
    let mut path = PathBuf::from(home);
    path.push(".hypercore-indexer");
    let _ = std::fs::create_dir_all(&path);
    path
}

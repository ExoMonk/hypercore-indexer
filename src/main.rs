use clap::{Parser, Subcommand};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::info;

mod config;
mod decode;
mod error;
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

    /// Backfill a range of blocks from S3
    Backfill {
        /// Start block number (if omitted, resumes from cursor)
        #[arg(long)]
        from: Option<u64>,

        /// End block number (required)
        #[arg(long)]
        to: u64,

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

            // Determine start block: --from flag, or DB cursor, or file cursor, or error.
            let start_block = match from {
                Some(block) => block,
                None => {
                    if let Some(ref store) = &db_storage {
                        match store.get_cursor(&network_name).await? {
                            Some(cursor) => {
                                info!(cursor, resumed_from = cursor + 1, "Resuming from DB cursor");
                                cursor + 1
                            }
                            None => {
                                return Err(eyre::eyre!(
                                    "No --from specified and no DB cursor found for network '{network_name}'. \
                                     Specify --from <block_number> to start."
                                ));
                            }
                        }
                    } else {
                        let cursor_path = cursor_file.clone().unwrap_or_else(|| {
                            let mut path = dirs_path(&network);
                            path.push(format!("cursor_{}.txt", network));
                            path
                        });
                        match pipeline::range::read_cursor(&cursor_path)? {
                            Some(cursor) => cursor + 1,
                            None => {
                                return Err(eyre::eyre!(
                                    "No --from specified and no cursor file found at {}. \
                                     Specify --from <block_number> to start.",
                                    cursor_path.display()
                                ));
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

            let config = RangeConfig {
                start_block,
                end_block: to,
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
                // Storage mode: decode, buffer, and batch insert
                let mut buffer: Vec<decode::types::DecodedBlock> =
                    Vec::with_capacity(effective_batch_size);

                while let Some((_block_num, raw_block)) = rx.recv().await {
                    let decoded = decode::decode_block(&raw_block, chain_id)?;
                    buffer.push(decoded);
                    count += 1;

                    if buffer.len() >= effective_batch_size {
                        let max_block = buffer.iter().map(|b| b.number).max().unwrap();
                        store
                            .insert_batch_and_set_cursor(&buffer, &network_name, max_block)
                            .await?;
                        info!(
                            batch_blocks = buffer.len(),
                            cursor = max_block,
                            total_received = count,
                            "Flushed batch to storage"
                        );
                        buffer.clear();
                    }
                }

                // Flush remaining blocks
                if !buffer.is_empty() {
                    let max_block = buffer.iter().map(|b| b.number).max().unwrap();
                    store
                        .insert_batch_and_set_cursor(&buffer, &network_name, max_block)
                        .await?;
                    info!(
                        batch_blocks = buffer.len(),
                        cursor = max_block,
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

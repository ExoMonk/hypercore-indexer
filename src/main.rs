use clap::{Parser, Subcommand};
use tracing::info;

mod error;
mod s3;
mod types;

use error::Result;
use s3::client::{HyperEvmS3Client, Network};
use s3::codec;

#[derive(Parser)]
#[command(name = "hypercore-indexer", about = "Hyperliquid HyperCore/HyperEVM S3 block indexer")]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Network to index: mainnet or testnet (default: mainnet)
    #[arg(long, global = true, default_value = "mainnet", env = "HL_NETWORK")]
    network: Network,

    /// AWS region for the S3 bucket (default: ap-northeast-1)
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

    match cli.command {
        Commands::FetchBlock {
            block_number,
            raw,
            system_txs,
        } => {
            let client = HyperEvmS3Client::new(cli.region, cli.network).await?;

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
    }

    Ok(())
}

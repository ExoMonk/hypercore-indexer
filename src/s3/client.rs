use crate::error::Result;
use aws_sdk_s3::Client;
use std::fmt;
use tracing::info;

#[derive(Debug, Clone, Copy, Default)]
pub enum Network {
    #[default]
    Mainnet,
    Testnet,
}

impl Network {
    pub fn s3_bucket(&self) -> &'static str {
        match self {
            Network::Mainnet => "hl-mainnet-evm-blocks",
            Network::Testnet => "hl-testnet-evm-blocks",
        }
    }

    pub fn chain_id(&self) -> u64 {
        match self {
            Network::Mainnet => 999,
            Network::Testnet => 998,
        }
    }

    pub fn rpc_url(&self) -> &'static str {
        match self {
            Network::Mainnet => "https://rpc.hyperliquid.xyz/evm",
            Network::Testnet => "https://rpc.hyperliquid-testnet.xyz/evm",
        }
    }
}

impl fmt::Display for Network {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Network::Mainnet => write!(f, "mainnet"),
            Network::Testnet => write!(f, "testnet"),
        }
    }
}

impl std::str::FromStr for Network {
    type Err = eyre::Report;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mainnet" => Ok(Network::Mainnet),
            "testnet" => Ok(Network::Testnet),
            _ => Err(eyre::eyre!("unknown network '{}', expected 'mainnet' or 'testnet'", s)),
        }
    }
}

pub struct HyperEvmS3Client {
    client: Client,
    bucket: String,
    network: Network,
}

impl HyperEvmS3Client {
    pub async fn new(region: Option<String>, network: Network) -> Result<Self> {
        let region_str = region.unwrap_or_else(|| "ap-northeast-1".to_string());

        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(region_str))
            .load()
            .await;

        let client = Client::new(&config);
        let bucket = network.s3_bucket().to_string();

        info!(%network, %bucket, "Initialized S3 client");

        Ok(Self {
            client,
            bucket,
            network,
        })
    }

    pub fn network(&self) -> Network {
        self.network
    }

    /// Fetch raw compressed block data from S3.
    pub async fn fetch_block_raw(&self, block_number: u64) -> Result<Vec<u8>> {
        let key = block_to_s3_key(block_number);
        info!(block_number, key = %key, "Fetching block from S3");

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester)
            .send()
            .await
            .map_err(|e| eyre::eyre!("S3 GetObject failed for key {key}: {e}"))?;

        let bytes = resp
            .body
            .collect()
            .await
            .map_err(|e| eyre::eyre!("Failed to read S3 response body: {e}"))?;

        let data = bytes.to_vec();
        info!(
            block_number,
            compressed_size = data.len(),
            "Fetched block data from S3"
        );
        Ok(data)
    }
}

/// Compute S3 key from block number.
///
/// Key pattern: `{million}/{thousand}/{blockNum}.rmp.lz4`
/// - million = floor((blockNum - 1) / 1_000_000) * 1_000_000
/// - thousand = floor((blockNum - 1) / 1_000) * 1_000
pub fn block_to_s3_key(block_number: u64) -> String {
    if block_number == 0 {
        // Edge case: block 0 maps to 0/0/0.rmp.lz4
        return "0/0/0.rmp.lz4".to_string();
    }
    let n = block_number - 1;
    let million = (n / 1_000_000) * 1_000_000;
    let thousand = (n / 1_000) * 1_000;
    format!("{million}/{thousand}/{block_number}.rmp.lz4")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_to_s3_key() {
        assert_eq!(block_to_s3_key(1), "0/0/1.rmp.lz4");
        assert_eq!(block_to_s3_key(1000), "0/0/1000.rmp.lz4");
        assert_eq!(block_to_s3_key(1001), "0/1000/1001.rmp.lz4");
        assert_eq!(block_to_s3_key(1_000_001), "1000000/1000000/1000001.rmp.lz4");
    }
}

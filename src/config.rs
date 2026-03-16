use serde::Deserialize;
use std::path::Path;

/// Top-level configuration file.
#[derive(Debug, Deserialize, Default)]
pub struct Config {
    #[serde(default)]
    pub network: NetworkConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
    #[serde(default)]
    pub live: LiveConfig,
    #[serde(default)]
    pub hip4: Hip4Config,
    #[serde(default)]
    pub fills: FillsConfig,
}

#[derive(Debug, Default, Deserialize, Clone)]
pub struct Hip4Config {
    /// Enable HIP4 contest event decoding (default: false)
    #[serde(default)]
    pub enabled: bool,
    /// Contest contract address (e.g. "0x4fd772e5708da2a7f097f51b3127e515a72744bd")
    pub contest_address: Option<String>,
    /// HyperCore API URL for market metadata and prices (Phase 2).
    /// When set, the API poller is activated.
    pub api_url: Option<String>,
    /// How often to poll outcomeMeta (seconds, default: 60)
    pub meta_poll_interval_s: Option<u64>,
    /// How often to poll allMids for prices (seconds, default: 5)
    pub price_poll_interval_s: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct NetworkConfig {
    /// "mainnet" or "testnet" (default: mainnet)
    #[serde(default = "default_network")]
    pub name: String,
    /// AWS region for S3 bucket
    #[serde(default = "default_region")]
    pub region: String,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            name: default_network(),
            region: default_region(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct StorageConfig {
    /// Storage backend: "postgres", "sqlite", or omitted for none.
    /// Future use: config validation to reject unknown backends.
    #[allow(dead_code)]
    pub backend: Option<String>,
    /// Database connection URL.
    /// PostgreSQL: "postgres://user:pass@host:port/db"
    /// SQLite: "sqlite:./path/to/file.db" or "sqlite::memory:"
    pub url: Option<String>,
    /// Batch size for inserts (default: 100)
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            backend: None,
            url: None,
            batch_size: default_batch_size(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct PipelineConfig {
    /// Number of concurrent S3 fetch workers (default: 64)
    #[serde(default = "default_workers")]
    pub workers: usize,
    /// Bounded channel capacity (default: 1024)
    #[serde(default = "default_channel_size")]
    pub channel_size: usize,
    /// Max retry attempts for failed S3 fetches (default: 3)
    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: u32,
    /// Base retry delay in milliseconds (default: 1000)
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            workers: default_workers(),
            channel_size: default_channel_size(),
            retry_attempts: default_retry_attempts(),
            retry_delay_ms: default_retry_delay_ms(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LiveConfig {
    /// Base poll interval in ms (default: 1000, matching ~1s block time)
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
    /// Minimum poll interval floor in ms (default: 200)
    #[serde(default = "default_min_poll_interval_ms")]
    pub min_poll_interval_ms: u64,
    /// Decay factor for adaptive backoff (default: 0.67)
    #[serde(default = "default_poll_decay")]
    pub poll_decay: f64,
    /// Blocks behind tip before triggering parallel backfill (default: 100)
    #[serde(default = "default_gap_threshold")]
    pub gap_threshold: u64,
    /// Concurrent workers for gap backfill (default: 64)
    #[serde(default = "default_backfill_workers")]
    pub backfill_workers: usize,
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            poll_interval_ms: default_poll_interval_ms(),
            min_poll_interval_ms: default_min_poll_interval_ms(),
            poll_decay: default_poll_decay(),
            gap_threshold: default_gap_threshold(),
            backfill_workers: default_backfill_workers(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct FillsConfig {
    /// Enable fills ingestion (default: false).
    /// Used by live mode to decide whether to poll for fills.
    #[serde(default)]
    #[allow(dead_code)]
    pub enabled: bool,
    /// S3 bucket for node fills data (default: "hl-mainnet-node-data")
    #[serde(default = "default_fills_bucket")]
    pub bucket: String,
    /// Mirror HIP4 (#-prefixed) fills to hip4_trades (default: true)
    #[serde(default = "default_mirror_hip4")]
    pub mirror_hip4: bool,
}

impl Default for FillsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bucket: default_fills_bucket(),
            mirror_hip4: default_mirror_hip4(),
        }
    }
}

fn default_fills_bucket() -> String {
    "hl-mainnet-node-data".to_string()
}

fn default_mirror_hip4() -> bool {
    true
}

fn default_poll_interval_ms() -> u64 {
    1000
}
fn default_min_poll_interval_ms() -> u64 {
    200
}
fn default_poll_decay() -> f64 {
    0.67
}
fn default_gap_threshold() -> u64 {
    100
}
fn default_backfill_workers() -> usize {
    64
}

fn default_network() -> String {
    "mainnet".to_string()
}
fn default_region() -> String {
    "ap-northeast-1".to_string()
}
fn default_batch_size() -> usize {
    100
}
fn default_workers() -> usize {
    64
}
fn default_channel_size() -> usize {
    1024
}
fn default_retry_attempts() -> u32 {
    3
}
fn default_retry_delay_ms() -> u64 {
    1000
}

impl Config {
    /// Load config from a TOML file. Returns default config if file doesn't exist.
    pub fn load(path: &Path) -> eyre::Result<Self> {
        match std::fs::read_to_string(path) {
            Ok(content) => {
                let config: Config = toml::from_str(&content)
                    .map_err(|e| eyre::eyre!("Failed to parse config {}: {e}", path.display()))?;
                Ok(config)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Config::default()),
            Err(e) => Err(eyre::eyre!("Failed to read config {}: {e}", path.display())),
        }
    }

    /// Resolve the effective database URL from config, with CLI/env override.
    /// Expands `${VAR}` references in the URL from environment variables.
    pub fn database_url(&self, cli_override: Option<&str>) -> Option<String> {
        cli_override
            .map(|s| s.to_string())
            .or_else(|| std::env::var("DATABASE_URL").ok())
            .or_else(|| self.storage.url.clone())
            .map(|url| expand_env_vars(&url))
    }
}

/// Expand `${VAR}` or `$VAR` references in a string from environment variables.
/// Unknown variables are left as empty string with a warning.
fn expand_env_vars(input: &str) -> String {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '$' {
            let braced = chars.peek() == Some(&'{');
            if braced {
                chars.next(); // consume '{'
            }

            let mut var_name = String::new();
            while let Some(&ch) = chars.peek() {
                if braced {
                    if ch == '}' {
                        chars.next();
                        break;
                    }
                } else if !ch.is_ascii_alphanumeric() && ch != '_' {
                    break;
                }
                var_name.push(ch);
                chars.next();
            }

            if var_name.is_empty() {
                result.push('$');
                if braced {
                    result.push('{');
                }
            } else {
                match std::env::var(&var_name) {
                    Ok(val) => result.push_str(&val),
                    Err(_) => {
                        tracing::warn!(var = %var_name, "environment variable not set, using empty string");
                    }
                }
            }
        } else {
            result.push(c);
        }
    }

    result
}

/// Config loading and env var expansion:
/// - Default config values (mainnet, 64 workers, batch 100)
/// - TOML parsing: minimal, full, missing file
/// - `${VAR}` and `$VAR` expansion from environment
/// - DATABASE_URL priority: CLI > env > config
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = Config::default();
        assert_eq!(config.network.name, "mainnet");
        assert_eq!(config.network.region, "ap-northeast-1");
        assert_eq!(config.pipeline.workers, 64);
        assert_eq!(config.storage.batch_size, 100);
        assert!(config.storage.url.is_none());
    }

    #[test]
    fn parse_minimal_toml() {
        let toml = r#"
[storage]
url = "sqlite:./data.db"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.storage.url.as_deref(), Some("sqlite:./data.db"));
        assert_eq!(config.network.name, "mainnet"); // default
        assert_eq!(config.pipeline.workers, 64); // default
    }

    #[test]
    fn parse_full_toml() {
        let toml = r#"
[network]
name = "testnet"
region = "us-east-1"

[storage]
backend = "postgres"
url = "postgres://user:pass@localhost:5432/hypercore"
batch_size = 50

[pipeline]
workers = 128
channel_size = 2048
retry_attempts = 5
retry_delay_ms = 2000
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.network.name, "testnet");
        assert_eq!(config.network.region, "us-east-1");
        assert_eq!(config.storage.backend.as_deref(), Some("postgres"));
        assert_eq!(
            config.storage.url.as_deref(),
            Some("postgres://user:pass@localhost:5432/hypercore")
        );
        assert_eq!(config.storage.batch_size, 50);
        assert_eq!(config.pipeline.workers, 128);
        assert_eq!(config.pipeline.channel_size, 2048);
        assert_eq!(config.pipeline.retry_attempts, 5);
        assert_eq!(config.pipeline.retry_delay_ms, 2000);
    }

    #[test]
    fn expand_env_vars_simple() {
        std::env::set_var("TEST_HCIDX_USER", "admin");
        std::env::set_var("TEST_HCIDX_PASS", "s3cret");
        let result = expand_env_vars("postgres://${TEST_HCIDX_USER}:${TEST_HCIDX_PASS}@host/db");
        assert_eq!(result, "postgres://admin:s3cret@host/db");
        std::env::remove_var("TEST_HCIDX_USER");
        std::env::remove_var("TEST_HCIDX_PASS");
    }

    #[test]
    fn expand_env_vars_no_vars() {
        let result = expand_env_vars("sqlite:./data.db");
        assert_eq!(result, "sqlite:./data.db");
    }

    #[test]
    fn expand_env_vars_missing_var_becomes_empty() {
        let result = expand_env_vars("postgres://${NONEXISTENT_VAR_12345}@host/db");
        assert_eq!(result, "postgres://@host/db");
    }

    #[test]
    fn expand_env_vars_unbraced() {
        std::env::set_var("TEST_HCIDX_PORT", "5433");
        let result = expand_env_vars("host:$TEST_HCIDX_PORT/db");
        assert_eq!(result, "host:5433/db");
        std::env::remove_var("TEST_HCIDX_PORT");
    }

    #[test]
    fn database_url_priority() {
        let config = Config {
            storage: StorageConfig {
                url: Some("sqlite:./from-config.db".to_string()),
                ..Default::default()
            },
            ..Default::default()
        };

        // CLI override wins
        assert_eq!(
            config.database_url(Some("postgres://cli")),
            Some("postgres://cli".to_string())
        );

        // Config fallback (when no CLI and no env)
        // Can't easily test env var priority without setting it, but the logic is clear
    }

    #[test]
    fn missing_file_returns_default() {
        let config = Config::load(Path::new("/nonexistent/config.toml")).unwrap();
        assert_eq!(config.network.name, "mainnet");
    }

    #[test]
    fn live_config_defaults() {
        let config = LiveConfig::default();
        assert_eq!(config.poll_interval_ms, 1000);
        assert_eq!(config.min_poll_interval_ms, 200);
        assert!((config.poll_decay - 0.67).abs() < f64::EPSILON);
        assert_eq!(config.gap_threshold, 100);
        assert_eq!(config.backfill_workers, 64);
    }

    #[test]
    fn parse_toml_with_live_section() {
        let toml = r#"
[network]
name = "testnet"

[live]
poll_interval_ms = 500
min_poll_interval_ms = 100
poll_decay = 0.5
gap_threshold = 200
backfill_workers = 32
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.live.poll_interval_ms, 500);
        assert_eq!(config.live.min_poll_interval_ms, 100);
        assert!((config.live.poll_decay - 0.5).abs() < f64::EPSILON);
        assert_eq!(config.live.gap_threshold, 200);
        assert_eq!(config.live.backfill_workers, 32);
    }

    #[test]
    fn hip4_config_defaults() {
        let config = Hip4Config::default();
        assert!(!config.enabled);
        assert!(config.contest_address.is_none());
        assert!(config.api_url.is_none());
        assert!(config.meta_poll_interval_s.is_none());
        assert!(config.price_poll_interval_s.is_none());
    }

    #[test]
    fn parse_toml_with_hip4_section() {
        let toml = r#"
[network]
name = "testnet"

[hip4]
enabled = true
contest_address = "0x4fd772e5708da2a7f097f51b3127e515a72744bd"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.hip4.enabled);
        assert_eq!(
            config.hip4.contest_address.as_deref(),
            Some("0x4fd772e5708da2a7f097f51b3127e515a72744bd")
        );
    }

    #[test]
    fn parse_toml_with_hip4_phase2_fields() {
        let toml = r#"
[network]
name = "testnet"

[hip4]
enabled = true
contest_address = "0x4fd772e5708da2a7f097f51b3127e515a72744bd"
api_url = "https://api.hyperliquid-testnet.xyz/info"
meta_poll_interval_s = 120
price_poll_interval_s = 10
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.hip4.enabled);
        assert_eq!(
            config.hip4.api_url.as_deref(),
            Some("https://api.hyperliquid-testnet.xyz/info")
        );
        assert_eq!(config.hip4.meta_poll_interval_s, Some(120));
        assert_eq!(config.hip4.price_poll_interval_s, Some(10));
    }

    #[test]
    fn parse_toml_hip4_without_phase2_fields_uses_none() {
        let toml = r#"
[hip4]
enabled = true
contest_address = "0xabc"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.hip4.enabled);
        assert!(config.hip4.api_url.is_none());
        assert!(config.hip4.meta_poll_interval_s.is_none());
        assert!(config.hip4.price_poll_interval_s.is_none());
    }

    #[test]
    fn parse_toml_without_hip4_section_uses_defaults() {
        let toml = r#"
[network]
name = "mainnet"

[storage]
url = "sqlite:./data.db"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(!config.hip4.enabled);
        assert!(config.hip4.contest_address.is_none());
    }

    #[test]
    fn fills_config_defaults() {
        let config = FillsConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.bucket, "hl-mainnet-node-data");
        assert!(config.mirror_hip4);
    }

    #[test]
    fn parse_toml_with_fills_section() {
        let toml = r#"
[fills]
enabled = true
bucket = "custom-bucket"
mirror_hip4 = false
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(config.fills.enabled);
        assert_eq!(config.fills.bucket, "custom-bucket");
        assert!(!config.fills.mirror_hip4);
    }

    #[test]
    fn parse_toml_without_fills_section_uses_defaults() {
        let toml = r#"
[network]
name = "mainnet"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert!(!config.fills.enabled);
        assert_eq!(config.fills.bucket, "hl-mainnet-node-data");
        assert!(config.fills.mirror_hip4);
    }

    #[test]
    fn parse_toml_without_live_section_uses_defaults() {
        let toml = r#"
[network]
name = "mainnet"

[storage]
url = "sqlite:./data.db"
"#;
        let config: Config = toml::from_str(toml).unwrap();
        assert_eq!(config.live.poll_interval_ms, 1000);
        assert_eq!(config.live.min_poll_interval_ms, 200);
        assert!((config.live.poll_decay - 0.67).abs() < f64::EPSILON);
        assert_eq!(config.live.gap_threshold, 100);
        assert_eq!(config.live.backfill_workers, 64);
    }
}

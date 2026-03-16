pub mod parser;
pub mod types;

use std::sync::Arc;

use eyre::Result;
use tracing::{info, warn};

use crate::s3::client::HyperEvmS3Client;
use crate::storage::Storage;

use types::FillRecord;

/// Ingest one hourly fill file: download from S3, decompress, parse, and store.
/// Returns the number of fills ingested.
pub async fn ingest_fill_file(
    s3_client: &HyperEvmS3Client,
    bucket: &str,
    key: &str,
    storage: &dyn Storage,
    mirror_hip4: bool,
) -> Result<u64> {
    let compressed = s3_client.fetch_raw(bucket, key).await?;
    let fills = parser::parse_lz4(&compressed)?;
    let fill_count = fills.len() as u64;

    if fills.is_empty() {
        return Ok(0);
    }

    storage.insert_fills(&fills).await?;

    // Mirror #-prefixed fills to hip4_trades if enabled
    if mirror_hip4 {
        let hip4_fills: Vec<&FillRecord> = fills.iter().filter(|f| f.coin.starts_with('#')).collect();
        if !hip4_fills.is_empty() {
            info!(
                count = hip4_fills.len(),
                "Mirroring HIP4 fills to hip4_trades"
            );
            storage.insert_hip4_trade_fills(&hip4_fills).await?;
        }
    }

    Ok(fill_count)
}

/// Backfill fills for a date range.
/// Dates are in "YYYYMMDD" format. Iterates each date and each hour (0-23).
/// Returns the total number of fills ingested.
pub async fn backfill_fills(
    s3_client: Arc<HyperEvmS3Client>,
    bucket: &str,
    from_date: &str,
    to_date: &str,
    storage: &dyn Storage,
    mirror_hip4: bool,
) -> Result<u64> {
    let dates = generate_date_range(from_date, to_date)?;
    let mut total_fills: u64 = 0;

    for date in &dates {
        for hour in 0..24 {
            let key = format!("node_fills_by_block/hourly/{date}/{hour}.lz4");
            info!(key = %key, "Ingesting fill file");

            match ingest_fill_file(&s3_client, bucket, &key, storage, mirror_hip4).await {
                Ok(count) => {
                    total_fills += count;
                    info!(key = %key, fills = count, total = total_fills, "Ingested fill file");
                }
                Err(e) => {
                    let msg = e.to_string();
                    if msg.contains("NoSuchKey") || msg.contains("404") || msg.contains("not found")
                    {
                        // File doesn't exist yet (future hour or no data)
                        info!(key = %key, "Fill file not found, skipping");
                    } else {
                        warn!(key = %key, error = %e, "Failed to ingest fill file");
                        return Err(e);
                    }
                }
            }
        }
    }

    info!(total_fills, dates = dates.len(), "Fills backfill complete");
    Ok(total_fills)
}

/// Generate a list of date strings from from_date to to_date (inclusive).
/// Dates in "YYYYMMDD" format.
fn generate_date_range(from_date: &str, to_date: &str) -> Result<Vec<String>> {
    let from = parse_date(from_date)?;
    let to = parse_date(to_date)?;

    if from > to {
        return Err(eyre::eyre!(
            "from_date ({from_date}) is after to_date ({to_date})"
        ));
    }

    let mut dates = Vec::new();
    let mut current = from;
    while current <= to {
        dates.push(format!(
            "{:04}{:02}{:02}",
            current.0, current.1, current.2
        ));
        current = next_day(current);
    }

    Ok(dates)
}

/// Parse "YYYYMMDD" into (year, month, day).
fn parse_date(s: &str) -> Result<(u32, u32, u32)> {
    if s.len() != 8 {
        return Err(eyre::eyre!("Invalid date format: {s}, expected YYYYMMDD"));
    }
    let year: u32 = s[0..4]
        .parse()
        .map_err(|_| eyre::eyre!("Invalid year in date: {s}"))?;
    let month: u32 = s[4..6]
        .parse()
        .map_err(|_| eyre::eyre!("Invalid month in date: {s}"))?;
    let day: u32 = s[6..8]
        .parse()
        .map_err(|_| eyre::eyre!("Invalid day in date: {s}"))?;

    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return Err(eyre::eyre!("Invalid date values: {s}"));
    }

    Ok((year, month, day))
}

/// Simple next-day calculator.
fn next_day((y, m, d): (u32, u32, u32)) -> (u32, u32, u32) {
    let days_in_month = match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                29
            } else {
                28
            }
        }
        _ => 31,
    };

    if d < days_in_month {
        (y, m, d + 1)
    } else if m < 12 {
        (y, m + 1, 1)
    } else {
        (y + 1, 1, 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_range_single_day() {
        let dates = generate_date_range("20260315", "20260315").unwrap();
        assert_eq!(dates, vec!["20260315"]);
    }

    #[test]
    fn date_range_multi_day() {
        let dates = generate_date_range("20260315", "20260317").unwrap();
        assert_eq!(dates, vec!["20260315", "20260316", "20260317"]);
    }

    #[test]
    fn date_range_cross_month() {
        let dates = generate_date_range("20260131", "20260201").unwrap();
        assert_eq!(dates, vec!["20260131", "20260201"]);
    }

    #[test]
    fn date_range_invalid_order() {
        let result = generate_date_range("20260316", "20260315");
        assert!(result.is_err());
    }
}

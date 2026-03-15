use crate::error::Result;
use crate::types::BlockAndReceipts;
use std::io::Read;
use tracing::info;

/// Decode a compressed S3 block payload into a BlockAndReceipts.
///
/// Pipeline: LZ4 frame decompress -> MessagePack deserialize -> extract single element.
pub fn decode_block(compressed: &[u8]) -> Result<BlockAndReceipts> {
    // 1. LZ4 frame decompress
    let mut decoder = lz4_flex::frame::FrameDecoder::new(compressed);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| eyre::eyre!("LZ4 decompression failed: {e}"))?;

    info!(
        compressed_size = compressed.len(),
        decompressed_size = decompressed.len(),
        "Decompressed block data"
    );

    // 2. MessagePack deserialize into Vec<BlockAndReceipts>
    let blocks: Vec<BlockAndReceipts> = rmp_serde::from_slice(&decompressed)
        .map_err(|e| eyre::eyre!("MessagePack deserialization failed: {e}"))?;

    // 3. Return first (and only) element
    blocks
        .into_iter()
        .next()
        .ok_or_else(|| eyre::eyre!("Empty block array in S3 payload"))
}

/// Return the raw decompressed bytes for inspection.
pub fn decompress_raw(compressed: &[u8]) -> Result<Vec<u8>> {
    let mut decoder = lz4_flex::frame::FrameDecoder::new(compressed);
    let mut decompressed = Vec::new();
    decoder
        .read_to_end(&mut decompressed)
        .map_err(|e| eyre::eyre!("LZ4 decompression failed: {e}"))?;
    Ok(decompressed)
}

/// Codec error handling: LZ4 decompression and MessagePack deserialization
/// fail gracefully on empty, garbage, and malformed input.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_empty_input_fails() {
        // Empty input decompresses to empty bytes, then msgpack fails
        let result = decode_block(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn decode_garbage_input_fails() {
        let result = decode_block(&[0xDE, 0xAD, 0xBE, 0xEF]);
        assert!(result.is_err());
    }

    #[test]
    fn decompress_raw_empty_returns_empty() {
        // lz4_flex treats empty input as empty output (no error)
        let result = decompress_raw(&[]);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }
}

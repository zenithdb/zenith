use std::sync::Arc;

use ::metrics::IntGauge;
use bytes::{Buf, BufMut, Bytes};
use pageserver_api::key::{Key, AUX_KEY_PREFIX, METADATA_KEY_SIZE};
use tracing::warn;

// BEGIN Copyright (c) 2017 Servo Contributors

/// Const version of FNV hash.
#[inline]
#[must_use]
pub const fn fnv_hash(bytes: &[u8]) -> u128 {
    const INITIAL_STATE: u128 = 0x6c62272e07bb014262b821756295c58d;
    const PRIME: u128 = 0x0000000001000000000000000000013B;

    let mut hash = INITIAL_STATE;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u128;
        hash = hash.wrapping_mul(PRIME);
        i += 1;
    }
    hash
}

// END Copyright (c) 2017 Servo Contributors

/// Create a metadata key from a hash, encoded as [AUX_KEY_PREFIX, 2B directory prefix, least significant 13B of FNV hash].
fn aux_hash_to_metadata_key(dir_level1: u8, dir_level2: u8, data: &[u8]) -> Key {
    let mut key: [u8; 16] = [0; METADATA_KEY_SIZE];
    let hash = fnv_hash(data).to_be_bytes();
    key[0] = AUX_KEY_PREFIX;
    key[1] = dir_level1;
    key[2] = dir_level2;
    key[3..16].copy_from_slice(&hash[3..16]);
    Key::from_metadata_key_fixed_size(&key)
}

const AUX_DIR_PG_LOGICAL: u8 = 0x01;
const AUX_DIR_PG_REPLSLOT: u8 = 0x02;
const AUX_DIR_PG_STAT: u8 = 0x03;
const AUX_DIR_PG_UNKNOWN: u8 = 0xFF;

/// Encode the aux file into a fixed-size key.
///
/// The first byte is the AUX key prefix. We use the next 2 bytes of the key for the directory / aux file type.
/// We have one-to-one mapping for each of the aux file that we support. We hash the remaining part of the path
/// (usually a single file name, or several components) into 13-byte hash. The way we determine the 2-byte prefix
/// is roughly based on the first two components of the path, one unique number for one component.
///
/// * pg_logical/mappings -> 0x0101
/// * pg_logical/snapshots -> 0x0102
/// * pg_logical/replorigin_checkpoint -> 0x0103
/// * pg_logical/others -> 0x01FF
/// * pg_replslot/ -> 0x0201
/// * others -> 0xFFFF
///
/// If you add new AUX files to this function, please also add a test case to `test_encoding_portable`.
/// The new file type must have never been written to the storage before. Otherwise, there could be data
/// corruptions as the new file belongs to a new prefix but it might have been stored under the `others` prefix.
pub fn encode_aux_file_key(path: &str) -> Key {
    if let Some(fname) = path.strip_prefix("pg_logical/mappings/") {
        aux_hash_to_metadata_key(AUX_DIR_PG_LOGICAL, 0x01, fname.as_bytes())
    } else if let Some(fname) = path.strip_prefix("pg_logical/snapshots/") {
        aux_hash_to_metadata_key(AUX_DIR_PG_LOGICAL, 0x02, fname.as_bytes())
    } else if path == "pg_logical/replorigin_checkpoint" {
        aux_hash_to_metadata_key(AUX_DIR_PG_LOGICAL, 0x03, b"")
    } else if let Some(fname) = path.strip_prefix("pg_logical/") {
        if cfg!(debug_assertions) {
            warn!(
                "unsupported pg_logical aux file type: {}, putting to 0x01FF, would affect path scanning",
                path
            );
        }
        aux_hash_to_metadata_key(AUX_DIR_PG_LOGICAL, 0xFF, fname.as_bytes())
    } else if let Some(fname) = path.strip_prefix("pg_replslot/") {
        aux_hash_to_metadata_key(AUX_DIR_PG_REPLSLOT, 0x01, fname.as_bytes())
    } else if let Some(fname) = path.strip_prefix("pg_stat/pgstat.stat") {
        aux_hash_to_metadata_key(AUX_DIR_PG_STAT, 0x01, fname.as_bytes())
    } else {
        if cfg!(debug_assertions) {
            warn!(
                "unsupported aux file type: {}, putting to 0xFFFF, would affect path scanning",
                path
            );
        }
        aux_hash_to_metadata_key(AUX_DIR_PG_UNKNOWN, 0xFF, path.as_bytes())
    }
}

const AUX_FILE_ENCODING_VERSION: u8 = 0x01;

pub fn decode_file_value(val: &[u8]) -> anyhow::Result<Vec<(&str, &[u8])>> {
    let mut ptr = val;
    if ptr.is_empty() {
        // empty value = no files
        return Ok(Vec::new());
    }
    assert_eq!(
        ptr.get_u8(),
        AUX_FILE_ENCODING_VERSION,
        "unsupported aux file value"
    );
    let mut files = vec![];
    while ptr.has_remaining() {
        let key_len = ptr.get_u32() as usize;
        let key = &ptr[..key_len];
        ptr.advance(key_len);
        let val_len = ptr.get_u32() as usize;
        let content = &ptr[..val_len];
        ptr.advance(val_len);

        let path = std::str::from_utf8(key)?;
        files.push((path, content));
    }
    Ok(files)
}

/// Decode an aux file key-value pair into a list of files. The returned `Bytes` contains reference
/// to the original value slice. Be cautious about memory consumption.
pub fn decode_file_value_bytes(val: &Bytes) -> anyhow::Result<Vec<(String, Bytes)>> {
    let mut ptr = val.clone();
    if ptr.is_empty() {
        // empty value = no files
        return Ok(Vec::new());
    }
    assert_eq!(
        ptr.get_u8(),
        AUX_FILE_ENCODING_VERSION,
        "unsupported aux file value"
    );
    let mut files = vec![];
    while ptr.has_remaining() {
        let key_len = ptr.get_u32() as usize;
        let key = ptr.slice(..key_len);
        ptr.advance(key_len);
        let val_len = ptr.get_u32() as usize;
        let content = ptr.slice(..val_len);
        ptr.advance(val_len);

        let path = std::str::from_utf8(&key)?.to_string();
        files.push((path, content));
    }
    Ok(files)
}

pub fn encode_file_value(files: &[(&str, &[u8])]) -> anyhow::Result<Vec<u8>> {
    if files.is_empty() {
        // no files = empty value
        return Ok(Vec::new());
    }
    let mut encoded = vec![];
    encoded.put_u8(AUX_FILE_ENCODING_VERSION);
    for (path, content) in files {
        if path.len() > u32::MAX as usize {
            anyhow::bail!("{} exceeds path size limit", path);
        }
        encoded.put_u32(path.len() as u32);
        encoded.put_slice(path.as_bytes());
        if content.len() > u32::MAX as usize {
            anyhow::bail!("{} exceeds content size limit", path);
        }
        encoded.put_u32(content.len() as u32);
        encoded.put_slice(content);
    }
    Ok(encoded)
}

/// An estimation of the size of aux files.
pub struct AuxFileSizeEstimator {
    aux_file_size_gauge: IntGauge,
    size: Arc<std::sync::Mutex<Option<isize>>>,
}

impl AuxFileSizeEstimator {
    pub fn new(aux_file_size_gauge: IntGauge) -> Self {
        Self {
            aux_file_size_gauge,
            size: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// When generating base backup or doing initial logical size calculation
    pub fn on_initial(&self, new_size: usize) {
        let mut guard = self.size.lock().unwrap();
        *guard = Some(new_size as isize);
        self.report(new_size as isize);
    }

    pub fn on_add(&self, file_size: usize) {
        let mut guard = self.size.lock().unwrap();
        if let Some(size) = &mut *guard {
            *size += file_size as isize;
            self.report(*size);
        }
    }

    pub fn on_remove(&self, file_size: usize) {
        let mut guard = self.size.lock().unwrap();
        if let Some(size) = &mut *guard {
            *size -= file_size as isize;
            self.report(*size);
        }
    }

    pub fn on_update(&self, old_size: usize, new_size: usize) {
        let mut guard = self.size.lock().unwrap();
        if let Some(size) = &mut *guard {
            *size += new_size as isize - old_size as isize;
            self.report(*size);
        }
    }

    pub fn report(&self, size: isize) {
        self.aux_file_size_gauge.set(size as i64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_portable() {
        // AUX file encoding requires the hash to be portable across all platforms. This test case checks
        // if the algorithm produces the same hash across different environments.

        assert_eq!(
            265160408618497461376862998434862070044,
            super::fnv_hash("test1".as_bytes())
        );
        assert_eq!(
            295486155126299629456360817749600553988,
            super::fnv_hash("test/test2".as_bytes())
        );
        assert_eq!(
            144066263297769815596495629667062367629,
            super::fnv_hash("".as_bytes())
        );
    }

    #[test]
    fn test_encoding_portable() {
        // To correct retrieve AUX files, the generated keys for the same file must be the same for all versions
        // of the page server.
        assert_eq!(
            "62000001017F8B83D94F7081693471ABF91C",
            encode_aux_file_key("pg_logical/mappings/test1").to_string(),
        );
        assert_eq!(
            "62000001027F8E83D94F7081693471ABFCCD",
            encode_aux_file_key("pg_logical/snapshots/test2").to_string(),
        );
        assert_eq!(
            "62000001032E07BB014262B821756295C58D",
            encode_aux_file_key("pg_logical/replorigin_checkpoint").to_string(),
        );
        assert_eq!(
            "62000001FF4F38E1C74754E7D03C1A660178",
            encode_aux_file_key("pg_logical/unsupported").to_string(),
        );
        assert_eq!(
            "62000002017F8D83D94F7081693471ABFB92",
            encode_aux_file_key("pg_replslot/test3").to_string()
        );
        assert_eq!(
            "620000FFFF2B6ECC8AEF93F643DC44F15E03",
            encode_aux_file_key("other_file_not_supported").to_string(),
        );
    }

    #[test]
    fn test_value_encoding() {
        let files = vec![
            ("pg_logical/1.file", "1111".as_bytes()),
            ("pg_logical/2.file", "2222".as_bytes()),
        ];
        assert_eq!(
            files,
            decode_file_value(&encode_file_value(&files).unwrap()).unwrap()
        );
        let files = vec![];
        assert_eq!(
            files,
            decode_file_value(&encode_file_value(&files).unwrap()).unwrap()
        );
    }
}

use anyhow::Ok;
use pq_proto::{id_to_cancel_key, CancelKeyData};
use serde::{Deserialize, Serialize};
use std::io::ErrorKind;

pub mod keyspace {
    pub const CANCEL_PREFIX: &str = "cancel";
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) enum KeyPrefix {
    #[serde(untagged)]
    Cancel(CancelKeyData),
}

impl KeyPrefix {
    pub(crate) fn build_redis_key(&self) -> String {
        match self {
            KeyPrefix::Cancel(key) => {
                let hi = (key.backend_pid as u64) << 32;
                let lo = (key.cancel_key as u64) & 0xffff_ffff;
                let id = hi | lo;
                let keyspace = keyspace::CANCEL_PREFIX;
                format!("{keyspace}:{id:x}")
            }
        }
    }

    #[allow(dead_code)]
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            KeyPrefix::Cancel(_) => keyspace::CANCEL_PREFIX,
        }
    }
}

#[allow(dead_code)]
pub(crate) fn parse_redis_key(key: &str) -> anyhow::Result<KeyPrefix> {
    let (prefix, key_str) = key.split_once(':').ok_or_else(|| {
        anyhow::anyhow!(std::io::Error::new(
            ErrorKind::InvalidData,
            "missing prefix"
        ))
    })?;

    match prefix {
        keyspace::CANCEL_PREFIX => {
            let id = u64::from_str_radix(key_str, 16)?;

            Ok(KeyPrefix::Cancel(id_to_cancel_key(id)))
        }
        _ => Err(anyhow::anyhow!(std::io::Error::new(
            ErrorKind::InvalidData,
            "unknown prefix"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_redis_key() {
        let cancel_key: KeyPrefix = KeyPrefix::Cancel(CancelKeyData {
            backend_pid: 12345,
            cancel_key: 54321,
        });

        let redis_key = cancel_key.build_redis_key();
        assert_eq!(redis_key, "cancel:30390000d431");
    }

    #[test]
    fn test_parse_redis_key() {
        let redis_key = "cancel:30390000d431";
        let key: KeyPrefix = parse_redis_key(redis_key).expect("Failed to parse key");

        let ref_key = CancelKeyData {
            backend_pid: 12345,
            cancel_key: 54321,
        };

        assert_eq!(key.as_str(), KeyPrefix::Cancel(ref_key).as_str());
        let KeyPrefix::Cancel(cancel_key) = key;
        assert_eq!(ref_key, cancel_key);
    }
}

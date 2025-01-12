use anyhow::Context;
use clap::Parser;
use pageserver_api::{
    key::Key,
    reltag::{BlockNumber, RelTag, SlruKind},
    shard::{ShardCount, ShardStripeSize},
};
use std::str::FromStr;

#[derive(Parser)]
pub(super) struct DescribeKeyCommand {
    /// Key material in one of the forms: hex, span attributes captured from log, reltag blocknum
    input: Vec<String>,

    /// The number of shards to calculate what Keys placement would be.
    #[arg(long)]
    shard_count: Option<CustomShardCount>,

    /// The sharding stripe size.
    ///
    /// The default is hardcoded. It makes no sense to provide this without providing
    /// `--shard-count`.
    #[arg(long, requires = "shard_count")]
    stripe_size: Option<u32>,
}

/// Sharded shard count without unsharded count, which the actual ShardCount supports.
#[derive(Clone, Copy)]
pub(super) struct CustomShardCount(std::num::NonZeroU8);

#[derive(Debug, thiserror::Error)]
pub(super) enum InvalidShardCount {
    #[error(transparent)]
    ParsingFailed(#[from] std::num::ParseIntError),
    #[error("too few shards")]
    TooFewShards,
}

impl FromStr for CustomShardCount {
    type Err = InvalidShardCount;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let inner: std::num::NonZeroU8 = s.parse()?;
        if inner.get() < 2 {
            Err(InvalidShardCount::TooFewShards)
        } else {
            Ok(CustomShardCount(inner))
        }
    }
}

impl From<CustomShardCount> for ShardCount {
    fn from(value: CustomShardCount) -> Self {
        ShardCount::new(value.0.get())
    }
}

impl DescribeKeyCommand {
    pub(super) fn execute(self) {
        let DescribeKeyCommand {
            input,
            shard_count,
            stripe_size,
        } = self;

        let material = KeyMaterial::try_from(input.as_slice()).unwrap();
        let kind = material.kind();
        let key = Key::from(material);

        println!("parsed from {kind}: {key}:");
        println!();
        println!("{key:?}");

        macro_rules! kind_query {
            ([$($name:ident),*$(,)?]) => {{[$(kind_query!($name)),*]}};
            ($name:ident) => {{
                let s: &'static str = stringify!($name);
                let s = s.strip_prefix("is_").unwrap_or(s);
                let s = s.strip_suffix("_key").unwrap_or(s);

                #[allow(clippy::needless_borrow)]
                (s, key.$name())
            }};
        }

        // the current characterization is a mess of these boolean queries and separate
        // "recognization". I think it accurately represents how strictly we model the Key
        // right now, but could of course be made less confusing.

        let queries = kind_query!([
            is_rel_block_key,
            is_rel_vm_block_key,
            is_rel_fsm_block_key,
            is_slru_block_key,
            is_inherited_key,
            is_rel_size_key,
            is_slru_segment_size_key,
        ]);

        let recognized_kind = "recognized kind";
        let metadata_key = "metadata key";
        let shard_placement = "shard placement";

        let longest = queries
            .iter()
            .map(|t| t.0)
            .chain([recognized_kind, metadata_key, shard_placement])
            .map(|s| s.len())
            .max()
            .unwrap();

        let colon = 1;
        let padding = 1;

        for (name, is) in queries {
            let width = longest - name.len() + colon + padding;
            println!("{}{:width$}{}", name, ":", is);
        }

        let width = longest - recognized_kind.len() + colon + padding;
        println!(
            "{}{:width$}{:?}",
            recognized_kind,
            ":",
            RecognizedKeyKind::new(key),
        );

        if let Some(shard_count) = shard_count {
            // seeing the sharding placement might be confusing, so leave it out unless shard
            // count was given.

            let stripe_size = stripe_size.map(ShardStripeSize).unwrap_or_default();
            println!(
                "# placement with shard_count: {} and stripe_size: {}:",
                shard_count.0, stripe_size.0
            );
            let width = longest - shard_placement.len() + colon + padding;
            println!(
                "{}{:width$}{:?}",
                shard_placement,
                ":",
                pageserver_api::shard::describe(&key, shard_count.into(), stripe_size)
            );
        }
    }
}

/// Hand-wavy "inputs we accept" for a key.
#[derive(Debug)]
pub(super) enum KeyMaterial {
    Hex(Key),
    String(SpanAttributesFromLogs),
    Split(RelTag, BlockNumber),
}

impl KeyMaterial {
    fn kind(&self) -> &'static str {
        match self {
            KeyMaterial::Hex(_) => "hex",
            KeyMaterial::String(_) | KeyMaterial::Split(_, _) => "split",
        }
    }
}

impl From<KeyMaterial> for Key {
    fn from(value: KeyMaterial) -> Self {
        match value {
            KeyMaterial::Hex(key) => key,
            KeyMaterial::String(SpanAttributesFromLogs(rt, blocknum))
            | KeyMaterial::Split(rt, blocknum) => {
                pageserver_api::key::rel_block_to_key(rt, blocknum)
            }
        }
    }
}

impl<S: AsRef<str>> TryFrom<&[S]> for KeyMaterial {
    type Error = anyhow::Error;

    fn try_from(value: &[S]) -> Result<Self, Self::Error> {
        match value {
            [] => anyhow::bail!(
                "need 1..N positional arguments describing the key, try hex or a log line"
            ),
            [one] => {
                let one = one.as_ref();

                let key = Key::from_hex(one).map(KeyMaterial::Hex);

                let attrs = SpanAttributesFromLogs::from_str(one).map(KeyMaterial::String);

                match (key, attrs) {
                    (Ok(key), _) => Ok(key),
                    (_, Ok(s)) => Ok(s),
                    (Err(e1), Err(e2)) => anyhow::bail!(
                        "failed to parse {one:?} as hex or span attributes:\n- {e1:#}\n- {e2:#}"
                    ),
                }
            }
            more => {
                // assume going left to right one of these is a reltag and then we find a blocknum
                // this works, because we don't have plain numbers at least right after reltag in
                // logs. for some definition of "works".

                let Some((reltag_at, reltag)) = more
                    .iter()
                    .map(AsRef::as_ref)
                    .enumerate()
                    .find_map(|(i, s)| {
                        s.split_once("rel=")
                            .map(|(_garbage, actual)| actual)
                            .unwrap_or(s)
                            .parse::<RelTag>()
                            .ok()
                            .map(|rt| (i, rt))
                    })
                else {
                    anyhow::bail!("found no RelTag in arguments");
                };

                let Some(blocknum) = more
                    .iter()
                    .map(AsRef::as_ref)
                    .skip(reltag_at)
                    .find_map(|s| {
                        s.split_once("blkno=")
                            .map(|(_garbage, actual)| actual)
                            .unwrap_or(s)
                            .parse::<BlockNumber>()
                            .ok()
                    })
                else {
                    anyhow::bail!("found no blocknum in arguments");
                };

                Ok(KeyMaterial::Split(reltag, blocknum))
            }
        }
    }
}

#[derive(Debug)]
pub(super) struct SpanAttributesFromLogs(RelTag, BlockNumber);

impl std::str::FromStr for SpanAttributesFromLogs {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // accept the span separator but do not require or fail if either is missing
        // "whatever{rel=1663/16389/24615 blkno=1052204 req_lsn=FFFFFFFF/FFFFFFFF}"
        let (_, reltag) = s
            .split_once("rel=")
            .ok_or_else(|| anyhow::anyhow!("cannot find 'rel='"))?;
        let reltag = reltag.split_whitespace().next().unwrap();

        let (_, blocknum) = s
            .split_once("blkno=")
            .ok_or_else(|| anyhow::anyhow!("cannot find 'blkno='"))?;
        let blocknum = blocknum.split_whitespace().next().unwrap();

        let reltag = reltag
            .parse()
            .with_context(|| format!("parse reltag from {reltag:?}"))?;
        let blocknum = blocknum
            .parse()
            .with_context(|| format!("parse blocknum from {blocknum:?}"))?;

        Ok(Self(reltag, blocknum))
    }
}

#[derive(Debug)]
#[allow(dead_code)] // debug print is used
enum RecognizedKeyKind {
    DbDir,
    ControlFile,
    Checkpoint,
    AuxFilesV1,
    SlruDir(Result<SlruKind, u32>),
    RelMap(RelTagish<2>),
    RelDir(RelTagish<2>),
    AuxFileV2(Result<AuxFileV2, utils::Hex<[u8; 16]>>),
}

#[derive(Debug, PartialEq)]
#[allow(unused)]
enum AuxFileV2 {
    Recognized(&'static str, utils::Hex<[u8; 13]>),
    OtherWithPrefix(&'static str, utils::Hex<[u8; 13]>),
    Other(utils::Hex<[u8; 13]>),
}

impl RecognizedKeyKind {
    fn new(key: Key) -> Option<Self> {
        use RecognizedKeyKind::{
            AuxFilesV1, Checkpoint, ControlFile, DbDir, RelDir, RelMap, SlruDir,
        };

        let slru_dir_kind = pageserver_api::key::slru_dir_kind(&key);

        Some(match key {
            pageserver_api::key::DBDIR_KEY => DbDir,
            pageserver_api::key::CONTROLFILE_KEY => ControlFile,
            pageserver_api::key::CHECKPOINT_KEY => Checkpoint,
            pageserver_api::key::AUX_FILES_KEY => AuxFilesV1,
            _ if slru_dir_kind.is_some() => SlruDir(slru_dir_kind.unwrap()),
            _ if key.field1 == 0 && key.field4 == 0 && key.field5 == 0 && key.field6 == 0 => {
                RelMap([key.field2, key.field3].into())
            }
            _ if key.field1 == 0 && key.field4 == 0 && key.field5 == 0 && key.field6 == 1 => {
                RelDir([key.field2, key.field3].into())
            }
            _ if key.is_metadata_key() => RecognizedKeyKind::AuxFileV2(
                AuxFileV2::new(key).ok_or_else(|| utils::Hex(key.to_i128().to_be_bytes())),
            ),
            _ => return None,
        })
    }
}

impl AuxFileV2 {
    fn new(key: Key) -> Option<AuxFileV2> {
        const EMPTY_HASH: [u8; 13] = {
            let mut out = [0u8; 13];
            let hash = pageserver::aux_file::fnv_hash(b"").to_be_bytes();
            let mut i = 3;
            while i < 16 {
                out[i - 3] = hash[i];
                i += 1;
            }
            out
        };

        let bytes = key.to_i128().to_be_bytes();
        let hash = utils::Hex(<[u8; 13]>::try_from(&bytes[3..]).unwrap());

        assert_eq!(EMPTY_HASH.len(), hash.0.len());

        // TODO: we could probably find the preimages for the hashes

        Some(match (bytes[1], bytes[2]) {
            (1, 1) => AuxFileV2::Recognized("pg_logical/mappings/", hash),
            (1, 2) => AuxFileV2::Recognized("pg_logical/snapshots/", hash),
            (1, 3) if hash.0 == EMPTY_HASH => {
                AuxFileV2::Recognized("pg_logical/replorigin_checkpoint", hash)
            }
            (2, 1) => AuxFileV2::Recognized("pg_replslot/", hash),
            (3, 1) => AuxFileV2::Recognized("pg_stat/pgstat.stat", hash),
            (1, 0xff) => AuxFileV2::OtherWithPrefix("pg_logical/", hash),
            (0xff, 0xff) => AuxFileV2::Other(hash),
            _ => return None,
        })
    }
}

/// Prefix of RelTag, currently only known use cases are the two item versions.
///
/// Renders like a reltag with `/`, nothing else.
struct RelTagish<const N: usize>([u32; N]);

impl<const N: usize> From<[u32; N]> for RelTagish<N> {
    fn from(val: [u32; N]) -> Self {
        RelTagish(val)
    }
}

impl<const N: usize> std::fmt::Debug for RelTagish<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use std::fmt::Write as _;
        let mut first = true;
        self.0.iter().try_for_each(|x| {
            if !first {
                f.write_char('/')?;
            }
            first = false;
            write!(f, "{}", x)
        })
    }
}

#[cfg(test)]
mod tests {
    use pageserver::aux_file::encode_aux_file_key;

    use super::*;

    #[test]
    fn hex_is_key_material() {
        let m = KeyMaterial::try_from(&["000000067F0000400200DF927900FFFFFFFF"][..]).unwrap();
        assert!(matches!(m, KeyMaterial::Hex(_)), "{m:?}");
    }

    #[test]
    fn single_positional_spanalike_is_key_material() {
        // why is this needed? if you are checking many, then copypaste starts to appeal
        let strings = [
            (line!(), "2024-05-15T15:33:49.873906Z ERROR page_service_conn_main{peer_addr=A:B}:process_query{tenant_id=C timeline_id=D}:handle_pagerequests:handle_get_page_at_lsn_request{rel=1663/208101/2620_fsm blkno=2 req_lsn=0/238D98C8}: error reading relation or page version: Read error: could not find data for key 000000067F00032CE5000000000000000001 (shard ShardNumber(0)) at LSN 0/1D0A16C1, request LSN 0/238D98C8, ancestor 0/0"),
            (line!(), "rel=1663/208101/2620_fsm blkno=2"),
            (line!(), "rel=1663/208101/2620.1 blkno=2"),
        ];

        let mut first: Option<Key> = None;

        for (line, example) in strings {
            let m = KeyMaterial::try_from(&[example][..])
                .unwrap_or_else(|e| panic!("failed to parse example from line {line}: {e:?}"));
            let key = Key::from(m);
            if let Some(first) = first {
                assert_eq!(first, key);
            } else {
                first = Some(key);
            }
        }

        // not supporting this is rather accidential, but I think the input parsing is lenient
        // enough already
        KeyMaterial::try_from(&["1663/208101/2620_fsm 2"][..]).unwrap_err();
    }

    #[test]
    fn multiple_spanlike_args() {
        let strings = [
            (line!(), &["process_query{tenant_id=C", "timeline_id=D}:handle_pagerequests:handle_get_page_at_lsn_request{rel=1663/208101/2620_fsm", "blkno=2", "req_lsn=0/238D98C8}"][..]),
            (line!(), &["rel=1663/208101/2620_fsm", "blkno=2"][..]),
            (line!(), &["1663/208101/2620_fsm", "2"][..]),
        ];

        let mut first: Option<Key> = None;

        for (line, example) in strings {
            let m = KeyMaterial::try_from(example)
                .unwrap_or_else(|e| panic!("failed to parse example from line {line}: {e:?}"));
            let key = Key::from(m);
            if let Some(first) = first {
                assert_eq!(first, key);
            } else {
                first = Some(key);
            }
        }
    }
    #[test]
    fn recognized_auxfiles() {
        use AuxFileV2::*;

        let empty = [
            0x2e, 0x07, 0xbb, 0x01, 0x42, 0x62, 0xb8, 0x21, 0x75, 0x62, 0x95, 0xc5, 0x8d,
        ];
        let foobar = [
            0x62, 0x79, 0x3c, 0x64, 0xbf, 0x6f, 0x0d, 0x35, 0x97, 0xba, 0x44, 0x6f, 0x18,
        ];

        #[rustfmt::skip]
        let examples = [
            (line!(), "pg_logical/mappings/foobar", Recognized("pg_logical/mappings/", utils::Hex(foobar))),
            (line!(), "pg_logical/snapshots/foobar", Recognized("pg_logical/snapshots/", utils::Hex(foobar))),
            (line!(), "pg_logical/replorigin_checkpoint", Recognized("pg_logical/replorigin_checkpoint", utils::Hex(empty))),
            (line!(), "pg_logical/foobar", OtherWithPrefix("pg_logical/", utils::Hex(foobar))),
            (line!(), "pg_replslot/foobar", Recognized("pg_replslot/", utils::Hex(foobar))),
            (line!(), "foobar", Other(utils::Hex(foobar))),
        ];

        for (line, path, expected) in examples {
            let key = encode_aux_file_key(path);
            let recognized =
                AuxFileV2::new(key).unwrap_or_else(|| panic!("line {line} example failed"));

            assert_eq!(recognized, expected);
        }

        assert_eq!(
            AuxFileV2::new(Key::from_hex("600000102000000000000000000000000000").unwrap()),
            None,
            "example key has one too few 0 after 6 before 1"
        );
    }
}

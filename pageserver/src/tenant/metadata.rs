//! Every image of a certain timeline from [`crate::tenant::Tenant`]
//! has a metadata that needs to be stored persistently.
//!
//! Later, the file gets is used in [`crate::remote_storage::storage_sync`] as a part of
//! external storage import and export operations.
//!
//! The module contains all structs and related helper methods related to timeline metadata.

use std::fs::{File, OpenOptions};
use std::io::Write;

use anyhow::{bail, ensure, Context};
use serde::{Deserialize, Serialize};
use tracing::info_span;
use utils::bin_ser::SerializeError;
use utils::{
    bin_ser::BeSer,
    id::{TenantId, TimelineId},
    lsn::Lsn,
};

use crate::config::PageServerConf;
use crate::virtual_file::VirtualFile;

/// Use special format number to enable backward compatibility.
const METADATA_FORMAT_VERSION: u16 = 5;

/// We assume that a write of up to METADATA_MAX_SIZE bytes is atomic.
///
/// This is the same assumption that PostgreSQL makes with the control file,
/// see PG_CONTROL_MAX_SAFE_SIZE
const METADATA_MAX_SIZE: usize = 512;

/// Metadata stored on disk for each timeline
///
/// The fields correspond to the values we hold in memory, in Timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimelineMetadata {
    hdr: TimelineMetadataHeader,
    body: TimelineMetadataBodyV3,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataHeader {
    checksum: u32,       // CRC of serialized metadata body
    size: u16,           // size of serialized metadata
    format_version: u16, // metadata format version (used for compatibility checks)
}
const METADATA_HDR_SIZE: usize = std::mem::size_of::<TimelineMetadataHeader>();

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataBodyV2 {
    disk_consistent_lsn: Lsn,
    // This is only set if we know it. We track it in memory when the page
    // server is running, but we only track the value corresponding to
    // 'last_record_lsn', not 'disk_consistent_lsn' which can lag behind by a
    // lot. We only store it in the metadata file when we flush *all* the
    // in-memory data so that 'last_record_lsn' is the same as
    // 'disk_consistent_lsn'.  That's OK, because after page server restart, as
    // soon as we reprocess at least one record, we will have a valid
    // 'prev_record_lsn' value in memory again. This is only really needed when
    // doing a clean shutdown, so that there is no more WAL beyond
    // 'disk_consistent_lsn'
    prev_record_lsn: Option<Lsn>,
    ancestor_timeline: Option<TimelineId>,
    ancestor_lsn: Lsn,
    latest_gc_cutoff_lsn: Lsn,
    initdb_lsn: Lsn,
    pg_version: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataBodyV3 {
    disk_consistent_lsn: Lsn,
    // This is only set if we know it. We track it in memory when the page
    // server is running, but we only track the value corresponding to
    // 'last_record_lsn', not 'disk_consistent_lsn' which can lag behind by a
    // lot. We only store it in the metadata file when we flush *all* the
    // in-memory data so that 'last_record_lsn' is the same as
    // 'disk_consistent_lsn'.  That's OK, because after page server restart, as
    // soon as we reprocess at least one record, we will have a valid
    // 'prev_record_lsn' value in memory again. This is only really needed when
    // doing a clean shutdown, so that there is no more WAL beyond
    // 'disk_consistent_lsn'
    prev_record_lsn: Option<Lsn>,
    ancestor_timeline: Option<TimelineId>,
    ancestor_lsn: Lsn,
    latest_gc_cutoff_lsn: Lsn,
    initdb_lsn: Lsn,
    pg_version: u32,
    replica_lsn: Option<Lsn>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimelineMetadataBodyV1 {
    disk_consistent_lsn: Lsn,
    // This is only set if we know it. We track it in memory when the page
    // server is running, but we only track the value corresponding to
    // 'last_record_lsn', not 'disk_consistent_lsn' which can lag behind by a
    // lot. We only store it in the metadata file when we flush *all* the
    // in-memory data so that 'last_record_lsn' is the same as
    // 'disk_consistent_lsn'.  That's OK, because after page server restart, as
    // soon as we reprocess at least one record, we will have a valid
    // 'prev_record_lsn' value in memory again. This is only really needed when
    // doing a clean shutdown, so that there is no more WAL beyond
    // 'disk_consistent_lsn'
    prev_record_lsn: Option<Lsn>,
    ancestor_timeline: Option<TimelineId>,
    ancestor_lsn: Lsn,
    latest_gc_cutoff_lsn: Lsn,
    initdb_lsn: Lsn,
}

impl TimelineMetadata {
    pub fn new(
        disk_consistent_lsn: Lsn,
        prev_record_lsn: Option<Lsn>,
        ancestor_timeline: Option<TimelineId>,
        ancestor_lsn: Lsn,
        latest_gc_cutoff_lsn: Lsn,
        initdb_lsn: Lsn,
        pg_version: u32,
        replica_lsn: Option<Lsn>,
    ) -> Self {
        Self {
            hdr: TimelineMetadataHeader {
                checksum: 0,
                size: 0,
                format_version: METADATA_FORMAT_VERSION,
            },
            body: TimelineMetadataBodyV3 {
                disk_consistent_lsn,
                prev_record_lsn,
                ancestor_timeline,
                ancestor_lsn,
                latest_gc_cutoff_lsn,
                initdb_lsn,
                pg_version,
                replica_lsn,
            },
        }
    }

    fn upgrade_timeline_metadata(metadata_bytes: &[u8]) -> anyhow::Result<Self> {
        let mut hdr = TimelineMetadataHeader::des(&metadata_bytes[0..METADATA_HDR_SIZE])?;
        let metadata_size = hdr.size as usize;

        let body = match hdr.format_version {
            3 => {
                let body: TimelineMetadataBodyV1 =
                    TimelineMetadataBodyV1::des(&metadata_bytes[METADATA_HDR_SIZE..metadata_size])?;

                TimelineMetadataBodyV3 {
                    disk_consistent_lsn: body.disk_consistent_lsn,
                    prev_record_lsn: body.prev_record_lsn,
                    ancestor_timeline: body.ancestor_timeline,
                    ancestor_lsn: body.ancestor_lsn,
                    latest_gc_cutoff_lsn: body.latest_gc_cutoff_lsn,
                    initdb_lsn: body.initdb_lsn,
                    pg_version: 14, // All timelines created before this version had pg_version 14
                    replica_lsn: None,
                }
            }
            4 => {
                let body: TimelineMetadataBodyV2 =
                    TimelineMetadataBodyV2::des(&metadata_bytes[METADATA_HDR_SIZE..metadata_size])?;

                TimelineMetadataBodyV3 {
                    disk_consistent_lsn: body.disk_consistent_lsn,
                    prev_record_lsn: body.prev_record_lsn,
                    ancestor_timeline: body.ancestor_timeline,
                    ancestor_lsn: body.ancestor_lsn,
                    latest_gc_cutoff_lsn: body.latest_gc_cutoff_lsn,
                    initdb_lsn: body.initdb_lsn,
                    pg_version: body.pg_version, // All timelines created before this version had pg_version 14
                    replica_lsn: None,
                }
            }
            _ => bail!("unsupported metadata format version {}", hdr.format_version),
        };
        hdr.format_version = METADATA_FORMAT_VERSION;

        Ok(Self { hdr, body })
    }

    pub fn from_bytes(metadata_bytes: &[u8]) -> anyhow::Result<Self> {
        ensure!(
            metadata_bytes.len() == METADATA_MAX_SIZE,
            "metadata bytes size is wrong"
        );
        let hdr = TimelineMetadataHeader::des(&metadata_bytes[0..METADATA_HDR_SIZE])?;

        let metadata_size = hdr.size as usize;
        ensure!(
            metadata_size <= METADATA_MAX_SIZE,
            "corrupted metadata file"
        );
        let calculated_checksum = crc32c::crc32c(&metadata_bytes[METADATA_HDR_SIZE..metadata_size]);
        ensure!(
            hdr.checksum == calculated_checksum,
            "metadata checksum mismatch"
        );

        if hdr.format_version != METADATA_FORMAT_VERSION {
            // If metadata has the old format,
            // upgrade it and return the result
            TimelineMetadata::upgrade_timeline_metadata(metadata_bytes)
        } else {
            let body =
                TimelineMetadataBodyV3::des(&metadata_bytes[METADATA_HDR_SIZE..metadata_size])?;
            ensure!(
                body.disk_consistent_lsn.is_aligned(),
                "disk_consistent_lsn is not aligned"
            );
            Ok(TimelineMetadata { hdr, body })
        }
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, SerializeError> {
        let body_bytes = self.body.ser()?;
        let metadata_size = METADATA_HDR_SIZE + body_bytes.len();
        let hdr = TimelineMetadataHeader {
            size: metadata_size as u16,
            format_version: METADATA_FORMAT_VERSION,
            checksum: crc32c::crc32c(&body_bytes),
        };
        let hdr_bytes = hdr.ser()?;
        let mut metadata_bytes = vec![0u8; METADATA_MAX_SIZE];
        metadata_bytes[0..METADATA_HDR_SIZE].copy_from_slice(&hdr_bytes);
        metadata_bytes[METADATA_HDR_SIZE..metadata_size].copy_from_slice(&body_bytes);
        Ok(metadata_bytes)
    }

    /// [`Lsn`] that corresponds to the corresponding timeline directory
    /// contents, stored locally in the pageserver workdir.
    pub fn disk_consistent_lsn(&self) -> Lsn {
        self.body.disk_consistent_lsn
    }

    pub fn prev_record_lsn(&self) -> Option<Lsn> {
        self.body.prev_record_lsn
    }

    pub fn ancestor_timeline(&self) -> Option<TimelineId> {
        self.body.ancestor_timeline
    }

    pub fn ancestor_lsn(&self) -> Lsn {
        self.body.ancestor_lsn
    }

    pub fn latest_gc_cutoff_lsn(&self) -> Lsn {
        self.body.latest_gc_cutoff_lsn
    }

    pub fn initdb_lsn(&self) -> Lsn {
        self.body.initdb_lsn
    }

    pub fn pg_version(&self) -> u32 {
        self.body.pg_version
    }

    pub fn replica_lsn(&self) -> Option<Lsn> {
        self.body.replica_lsn
    }
}

/// Save timeline metadata to file
pub fn save_metadata(
    conf: &'static PageServerConf,
    timeline_id: TimelineId,
    tenant_id: TenantId,
    data: &TimelineMetadata,
    first_save: bool,
) -> anyhow::Result<()> {
    let _enter = info_span!("saving metadata").entered();
    let path = conf.metadata_path(timeline_id, tenant_id);
    // use OpenOptions to ensure file presence is consistent with first_save
    let mut file = VirtualFile::open_with_options(
        &path,
        OpenOptions::new().write(true).create_new(first_save),
    )
    .context("open_with_options")?;

    let metadata_bytes = data.to_bytes().context("Failed to get metadata bytes")?;

    if file.write(&metadata_bytes)? != metadata_bytes.len() {
        bail!("Could not write all the metadata bytes in a single call");
    }
    file.sync_all()?;

    // fsync the parent directory to ensure the directory entry is durable
    if first_save {
        let timeline_dir = File::open(
            path.parent()
                .expect("Metadata should always have a parent dir"),
        )?;
        timeline_dir.sync_all()?;
    }

    Ok(())
}

pub fn load_metadata(
    conf: &'static PageServerConf,
    timeline_id: TimelineId,
    tenant_id: TenantId,
) -> anyhow::Result<TimelineMetadata> {
    let metadata_path = conf.metadata_path(timeline_id, tenant_id);
    let metadata_bytes = std::fs::read(&metadata_path).with_context(|| {
        format!(
            "Failed to read metadata bytes from path {}",
            metadata_path.display()
        )
    })?;
    TimelineMetadata::from_bytes(&metadata_bytes).with_context(|| {
        format!(
            "Failed to parse metadata bytes from path {}",
            metadata_path.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant::harness::TIMELINE_ID;

    #[test]
    fn metadata_serializes_correctly() {
        let original_metadata = TimelineMetadata::new(
            Lsn(0x200),
            Some(Lsn(0x100)),
            Some(TIMELINE_ID),
            Lsn(0),
            Lsn(0),
            Lsn(0),
            // Any version will do here, so use the default
            crate::DEFAULT_PG_VERSION,
        );

        let metadata_bytes = original_metadata
            .to_bytes()
            .expect("Should serialize correct metadata to bytes");

        let deserialized_metadata = TimelineMetadata::from_bytes(&metadata_bytes)
            .expect("Should deserialize its own bytes");

        assert_eq!(
            deserialized_metadata.body, original_metadata.body,
            "Metadata that was serialized to bytes and deserialized back should not change"
        );
    }

    // Generate old version metadata and read it with current code.
    // Ensure that it is upgraded correctly
    #[test]
    fn test_metadata_upgrade() {
        #[derive(Debug, Clone, PartialEq, Eq)]
        struct TimelineMetadataV1 {
            hdr: TimelineMetadataHeader,
            body: TimelineMetadataBodyV1,
        }

        let metadata_v1 = TimelineMetadataV1 {
            hdr: TimelineMetadataHeader {
                checksum: 0,
                size: 0,
                format_version: 3,
            },
            body: TimelineMetadataBodyV1 {
                disk_consistent_lsn: Lsn(0x200),
                prev_record_lsn: Some(Lsn(0x100)),
                ancestor_timeline: Some(TIMELINE_ID),
                ancestor_lsn: Lsn(0),
                latest_gc_cutoff_lsn: Lsn(0),
                initdb_lsn: Lsn(0),
            },
        };

        impl TimelineMetadataV1 {
            pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
                let body_bytes = self.body.ser()?;
                let metadata_size = METADATA_HDR_SIZE + body_bytes.len();
                let hdr = TimelineMetadataHeader {
                    size: metadata_size as u16,
                    format_version: METADATA_OLD_FORMAT_VERSION,
                    checksum: crc32c::crc32c(&body_bytes),
                };
                let hdr_bytes = hdr.ser()?;
                let mut metadata_bytes = vec![0u8; METADATA_MAX_SIZE];
                metadata_bytes[0..METADATA_HDR_SIZE].copy_from_slice(&hdr_bytes);
                metadata_bytes[METADATA_HDR_SIZE..metadata_size].copy_from_slice(&body_bytes);
                Ok(metadata_bytes)
            }
        }

        let metadata_bytes = metadata_v1
            .to_bytes()
            .expect("Should serialize correct metadata to bytes");

        // This should deserialize to the latest version format
        let deserialized_metadata = TimelineMetadata::from_bytes(&metadata_bytes)
            .expect("Should deserialize its own bytes");

        let expected_metadata = TimelineMetadata::new(
            Lsn(0x200),
            Some(Lsn(0x100)),
            Some(TIMELINE_ID),
            Lsn(0),
            Lsn(0),
            Lsn(0),
            14, // All timelines created before this version had pg_version 14
        );

        assert_eq!(
            deserialized_metadata.body, expected_metadata.body,
            "Metadata of the old version {} should be upgraded to the latest version {}",
            METADATA_OLD_FORMAT_VERSION, METADATA_FORMAT_VERSION
        );
    }
}

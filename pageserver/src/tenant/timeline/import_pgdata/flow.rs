//! Import a PGDATA directory into an empty root timeline.
//!
//! This module is adapted hackathon code by Heikki and Stas.
//! Other code in the parent module was written by Christian as part of a customer PoC.
//!
//! The hackathon code was producing image layer files as a free-standing program.
//!
//! It has been modified to
//! - run inside a running Pageserver, within the proper lifecycles of Timeline -> Tenant(Shard)
//! - => sharding-awareness: produce image layers with only the data relevant for this shard
//! - => S3 as the source for the PGDATA instead of local filesystem
//!
//! TODOs before productionization:
//! - ChunkProcessingJob size / ImportJob::total_size does not account for sharding.
//!   => produced image layers likely too small.
//! - ChunkProcessingJob should cut up an ImportJob to hit exactly target image layer size.
//! - asserts / unwraps need to be replaced with errors
//! - don't trust remote objects will be small (=prevent OOMs in those cases)
//!     - limit all in-memory buffers in size, or download to disk and read from there
//! - limit task concurrency
//! - generally play nice with other tenants in the system
//!   - importbucket is different bucket than main pageserver storage, so, should be fine wrt S3 rate limits
//!   - but concerns like network bandwidth, local disk write bandwidth, local disk capacity, etc
//! - integrate with layer eviction system
//! - audit for Tenant::cancel nor Timeline::cancel responsivity
//! - audit for Tenant/Timeline gate holding (we spawn tokio tasks during this flow!)
//!
//! An incomplete set of TODOs from the Hackathon:
//! - version-specific CheckPointData (=> pgv abstraction, already exists for regular walingest)

use std::sync::Arc;

use anyhow::{bail, ensure};
use bytes::Bytes;

use itertools::Itertools;
use pageserver_api::{
    key::{rel_block_to_key, rel_dir_to_key, rel_size_to_key, relmap_file_key, DBDIR_KEY},
    reltag::RelTag,
    shard::ShardIdentity,
};
use postgres_ffi::{pg_constants, relfile_utils::parse_relfilename, BLCKSZ};
use tokio::task::JoinSet;
use tracing::{debug, info_span, instrument, Instrument};

use crate::{
    assert_u64_eq_usize::UsizeIsU64,
    pgdatadir_mapping::{SlruSegmentDirectory, TwoPhaseDirectory},
};
use crate::{
    context::{DownloadBehavior, RequestContext},
    pgdatadir_mapping::{DbDirectory, RelDirectory},
    task_mgr::TaskKind,
    tenant::storage_layer::{ImageLayerWriter, Layer},
};

use pageserver_api::key::Key;
use pageserver_api::key::{
    slru_block_to_key, slru_dir_to_key, slru_segment_size_to_key, CHECKPOINT_KEY, CONTROLFILE_KEY,
    TWOPHASEDIR_KEY,
};
use pageserver_api::keyspace::singleton_range;
use pageserver_api::keyspace::{contiguous_range_len, is_contiguous_range};
use pageserver_api::reltag::SlruKind;
use utils::bin_ser::BeSer;
use utils::lsn::Lsn;

use std::collections::HashSet;
use std::ops::Range;

use super::{
    importbucket_client::{ControlFile, RemoteStorageWrapper},
    Timeline,
};

use remote_storage::RemotePath;

pub async fn run(
    timeline: Arc<Timeline>,
    pgdata_lsn: Lsn,
    control_file: ControlFile,
    storage: RemoteStorageWrapper,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    Flow {
        timeline,
        pgdata_lsn,
        control_file,
        tasks: Vec::new(),
        storage,
    }
    .run(ctx)
    .await
}

struct Flow {
    timeline: Arc<Timeline>,
    pgdata_lsn: Lsn,
    control_file: ControlFile,
    tasks: Vec<AnyImportTask>,
    storage: RemoteStorageWrapper,
}

impl Flow {
    /// Perform the ingestion into [`Self::timeline`].
    /// Assumes the timeline is empty (= no layers).
    pub async fn run(mut self, ctx: &RequestContext) -> anyhow::Result<()> {
        let pgdata_lsn = Lsn(self.control_file.control_file_data().checkPoint).align();

        self.pgdata_lsn = pgdata_lsn;

        let datadir = PgDataDir::new(&self.storage).await?;

        // Import dbdir (00:00:00 keyspace)
        // This is just constructed here, but will be written to the image layer in the first call to import_db()
        let dbdir_buf = Bytes::from(DbDirectory::ser(&DbDirectory {
            dbdirs: datadir
                .dbs
                .iter()
                .map(|db| ((db.spcnode, db.dboid), true))
                .collect(),
        })?);
        self.tasks
            .push(ImportSingleKeyTask::new(DBDIR_KEY, dbdir_buf).into());

        // Import databases (00:spcnode:dbnode keyspace for each db)
        for db in datadir.dbs {
            self.import_db(&db).await?;
        }

        // Import SLRUs
        if self.timeline.tenant_shard_id.is_shard_zero() {
            // pg_xact (01:00 keyspace)
            self.import_slru(SlruKind::Clog, &self.storage.pgdata().join("pg_xact"))
                .await?;
            // pg_multixact/members (01:01 keyspace)
            self.import_slru(
                SlruKind::MultiXactMembers,
                &self.storage.pgdata().join("pg_multixact/members"),
            )
            .await?;
            // pg_multixact/offsets (01:02 keyspace)
            self.import_slru(
                SlruKind::MultiXactOffsets,
                &self.storage.pgdata().join("pg_multixact/offsets"),
            )
            .await?;
        }

        // Import pg_twophase.
        // TODO: as empty
        let twophasedir_buf = TwoPhaseDirectory::ser(&TwoPhaseDirectory {
            xids: HashSet::new(),
        })?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                TWOPHASEDIR_KEY,
                Bytes::from(twophasedir_buf),
            )));

        // Controlfile, checkpoint
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                CONTROLFILE_KEY,
                self.control_file.control_file_buf().clone(),
            )));

        let checkpoint_buf = self
            .control_file
            .control_file_data()
            .checkPointCopy
            .encode()?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                CHECKPOINT_KEY,
                checkpoint_buf,
            )));

        // Assigns parts of key space to later parallel jobs
        let mut last_end_key = Key::MIN;
        let mut current_chunk = Vec::new();
        let mut current_chunk_size: usize = 0;
        let mut parallel_jobs = Vec::new();
        for task in std::mem::take(&mut self.tasks).into_iter() {
            if current_chunk_size + task.total_size() > 1024 * 1024 * 1024 {
                let key_range = last_end_key..task.key_range().start;
                parallel_jobs.push(ChunkProcessingJob::new(
                    key_range.clone(),
                    std::mem::take(&mut current_chunk),
                    &self,
                ));
                last_end_key = key_range.end;
                current_chunk_size = 0;
            }
            current_chunk_size += task.total_size();
            current_chunk.push(task);
        }
        parallel_jobs.push(ChunkProcessingJob::new(
            last_end_key..Key::MAX,
            current_chunk,
            &self,
        ));

        // Start all jobs simultaneosly
        let mut work = JoinSet::new();
        // TODO: semaphore?
        for job in parallel_jobs {
            let ctx: RequestContext =
                ctx.detached_child(TaskKind::ImportPgdata, DownloadBehavior::Error);
            work.spawn(async move { job.run(&ctx).await }.instrument(info_span!("parallel_job")));
        }
        let mut results = Vec::new();
        while let Some(result) = work.join_next().await {
            match result {
                Ok(res) => {
                    results.push(res);
                }
                Err(_joinset_err) => {
                    results.push(Err(anyhow::anyhow!(
                        "parallel job panicked or cancelled, check pageserver logs"
                    )));
                }
            }
        }

        if results.iter().all(|r| r.is_ok()) {
            Ok(())
        } else {
            let mut msg = String::new();
            for result in results {
                if let Err(err) = result {
                    msg.push_str(&format!("{err:?}\n\n"));
                }
            }
            bail!("Some parallel jobs failed:\n\n{msg}");
        }
    }

    #[instrument(level = tracing::Level::DEBUG, skip_all, fields(dboid=%db.dboid, tablespace=%db.spcnode, path=%db.path))]
    async fn import_db(&mut self, db: &PgDataDirDb) -> anyhow::Result<()> {
        debug!("start");
        scopeguard::defer! {
            debug!("return");
        }

        // Import relmap (00:spcnode:dbnode:00:*:00)
        let relmap_key = relmap_file_key(db.spcnode, db.dboid);
        debug!("Constructing relmap entry, key {relmap_key}");
        let relmap_path = db.path.join("pg_filenode.map");
        let relmap_buf = self.storage.get(&relmap_path).await?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                relmap_key, relmap_buf,
            )));

        // Import reldir (00:spcnode:dbnode:00:*:01)
        let reldir_key = rel_dir_to_key(db.spcnode, db.dboid);
        debug!("Constructing reldirs entry, key {reldir_key}");
        let reldir_buf = RelDirectory::ser(&RelDirectory {
            rels: db
                .files
                .iter()
                .map(|f| (f.rel_tag.relnode, f.rel_tag.forknum))
                .collect(),
        })?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                reldir_key,
                Bytes::from(reldir_buf),
            )));

        // Import data (00:spcnode:dbnode:reloid:fork:blk) and set sizes for each last
        // segment in a given relation (00:spcnode:dbnode:reloid:fork:ff)
        for file in &db.files {
            debug!(%file.path, %file.filesize, "importing file");
            let len = file.filesize;
            ensure!(len % 8192 == 0);
            let start_blk: u32 = file.segno * (1024 * 1024 * 1024 / 8192);
            let start_key = rel_block_to_key(file.rel_tag, start_blk);
            let end_key = rel_block_to_key(file.rel_tag, start_blk + (len / 8192) as u32);
            self.tasks
                .push(AnyImportTask::RelBlocks(ImportRelBlocksTask::new(
                    *self.timeline.get_shard_identity(),
                    start_key..end_key,
                    &file.path,
                    self.storage.clone(),
                )));

            // Set relsize for the last segment (00:spcnode:dbnode:reloid:fork:ff)
            if let Some(nblocks) = file.nblocks {
                let size_key = rel_size_to_key(file.rel_tag);
                //debug!("Setting relation size (path={path}, rel_tag={rel_tag}, segno={segno}) to {nblocks}, key {size_key}");
                let buf = nblocks.to_le_bytes();
                self.tasks
                    .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                        size_key,
                        Bytes::from(buf.to_vec()),
                    )));
            }
        }

        Ok(())
    }

    async fn import_slru(&mut self, kind: SlruKind, path: &RemotePath) -> anyhow::Result<()> {
        assert!(self.timeline.tenant_shard_id.is_shard_zero());

        let segments = self.storage.listfilesindir(path).await?;
        let segments: Vec<(String, u32, usize)> = segments
            .into_iter()
            .filter_map(|(path, size)| {
                let filename = path.object_name()?;
                let segno = u32::from_str_radix(filename, 16).ok()?;
                Some((filename.to_string(), segno, size))
            })
            .collect();

        // Write SlruDir
        let slrudir_key = slru_dir_to_key(kind);
        let segnos: HashSet<u32> = segments
            .iter()
            .map(|(_path, segno, _size)| *segno)
            .collect();
        let slrudir = SlruSegmentDirectory { segments: segnos };
        let slrudir_buf = SlruSegmentDirectory::ser(&slrudir)?;
        self.tasks
            .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                slrudir_key,
                Bytes::from(slrudir_buf),
            )));

        for (segpath, segno, size) in segments {
            // SlruSegBlocks for each segment
            let p = path.join(&segpath);
            let file_size = size;
            ensure!(file_size % 8192 == 0);
            let nblocks = u32::try_from(file_size / 8192)?;
            let start_key = slru_block_to_key(kind, segno, 0);
            let end_key = slru_block_to_key(kind, segno, nblocks);
            debug!(%p, segno=%segno, %size, %start_key, %end_key, "scheduling SLRU segment");
            self.tasks
                .push(AnyImportTask::SlruBlocks(ImportSlruBlocksTask::new(
                    start_key..end_key,
                    &p,
                    self.storage.clone(),
                )));

            // Followed by SlruSegSize
            let segsize_key = slru_segment_size_to_key(kind, segno);
            let segsize_buf = nblocks.to_le_bytes();
            self.tasks
                .push(AnyImportTask::SingleKey(ImportSingleKeyTask::new(
                    segsize_key,
                    Bytes::copy_from_slice(&segsize_buf),
                )));
        }
        Ok(())
    }
}

//
// dbdir iteration tools
//

struct PgDataDir {
    pub dbs: Vec<PgDataDirDb>, // spcnode, dboid, path
}

struct PgDataDirDb {
    pub spcnode: u32,
    pub dboid: u32,
    pub path: RemotePath,
    pub files: Vec<PgDataDirDbFile>,
}

struct PgDataDirDbFile {
    pub path: RemotePath,
    pub rel_tag: RelTag,
    pub segno: u32,
    pub filesize: usize,
    // Cummulative size of the given fork, set only for the last segment of that fork
    pub nblocks: Option<usize>,
}

impl PgDataDir {
    async fn new(storage: &RemoteStorageWrapper) -> anyhow::Result<Self> {
        let datadir_path = storage.pgdata();
        // Import ordinary databases, DEFAULTTABLESPACE_OID is smaller than GLOBALTABLESPACE_OID, so import them first
        // Traverse database in increasing oid order

        let basedir = &datadir_path.join("base");
        let db_oids: Vec<_> = storage
            .listdir(basedir)
            .await?
            .into_iter()
            .filter_map(|path| path.object_name().and_then(|name| name.parse::<u32>().ok()))
            .sorted()
            .collect();
        debug!(?db_oids, "found databases");
        let mut databases = Vec::new();
        for dboid in db_oids {
            databases.push(
                PgDataDirDb::new(
                    storage,
                    &basedir.join(dboid.to_string()),
                    pg_constants::DEFAULTTABLESPACE_OID,
                    dboid,
                    &datadir_path,
                )
                .await?,
            );
        }

        // special case for global catalogs
        databases.push(
            PgDataDirDb::new(
                storage,
                &datadir_path.join("global"),
                postgres_ffi::pg_constants::GLOBALTABLESPACE_OID,
                0,
                &datadir_path,
            )
            .await?,
        );

        databases.sort_by_key(|db| (db.spcnode, db.dboid));

        Ok(Self { dbs: databases })
    }
}

impl PgDataDirDb {
    #[instrument(level = tracing::Level::DEBUG, skip_all, fields(%dboid, %db_path))]
    async fn new(
        storage: &RemoteStorageWrapper,
        db_path: &RemotePath,
        spcnode: u32,
        dboid: u32,
        datadir_path: &RemotePath,
    ) -> anyhow::Result<Self> {
        let mut files: Vec<PgDataDirDbFile> = storage
            .listfilesindir(db_path)
            .await?
            .into_iter()
            .filter_map(|(path, size)| {
                debug!(%path, %size, "found file in dbdir");
                path.object_name().and_then(|name| {
                    // returns (relnode, forknum, segno)
                    parse_relfilename(name).ok().map(|x| (size, x))
                })
            })
            .sorted_by_key(|(_, relfilename)| *relfilename)
            .map(|(filesize, (relnode, forknum, segno))| {
                let rel_tag = RelTag {
                    spcnode,
                    dbnode: dboid,
                    relnode,
                    forknum,
                };

                let path = datadir_path.join(rel_tag.to_segfile_name(segno));
                assert!(filesize % BLCKSZ as usize == 0); // TODO: this should result in an error
                let nblocks = filesize / BLCKSZ as usize;

                PgDataDirDbFile {
                    path,
                    filesize,
                    rel_tag,
                    segno,
                    nblocks: Some(nblocks), // first non-cummulative sizes
                }
            })
            .collect();

        // Set cummulative sizes. Do all of that math here, so that later we could easier
        // parallelize over segments and know with which segments we need to write relsize
        // entry.
        let mut cumulative_nblocks: usize = 0;
        let mut prev_rel_tag: Option<RelTag> = None;
        for i in 0..files.len() {
            if prev_rel_tag == Some(files[i].rel_tag) {
                cumulative_nblocks += files[i].nblocks.unwrap();
            } else {
                cumulative_nblocks = files[i].nblocks.unwrap();
            }

            files[i].nblocks = if i == files.len() - 1 || files[i + 1].rel_tag != files[i].rel_tag {
                Some(cumulative_nblocks)
            } else {
                None
            };

            prev_rel_tag = Some(files[i].rel_tag);
        }

        Ok(PgDataDirDb {
            files,
            path: db_path.clone(),
            spcnode,
            dboid,
        })
    }
}

trait ImportTask {
    fn key_range(&self) -> Range<Key>;

    fn total_size(&self) -> usize {
        // TODO: revisit this
        if is_contiguous_range(&self.key_range()) {
            contiguous_range_len(&self.key_range()) as usize * 8192
        } else {
            u32::MAX as usize
        }
    }

    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize>;
}

struct ImportSingleKeyTask {
    key: Key,
    buf: Bytes,
}

impl ImportSingleKeyTask {
    fn new(key: Key, buf: Bytes) -> Self {
        ImportSingleKeyTask { key, buf }
    }
}

impl ImportTask for ImportSingleKeyTask {
    fn key_range(&self) -> Range<Key> {
        singleton_range(self.key)
    }

    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        layer_writer.put_image(self.key, self.buf, ctx).await?;
        Ok(1)
    }
}

struct ImportRelBlocksTask {
    shard_identity: ShardIdentity,
    key_range: Range<Key>,
    path: RemotePath,
    storage: RemoteStorageWrapper,
}

impl ImportRelBlocksTask {
    fn new(
        shard_identity: ShardIdentity,
        key_range: Range<Key>,
        path: &RemotePath,
        storage: RemoteStorageWrapper,
    ) -> Self {
        ImportRelBlocksTask {
            shard_identity,
            key_range,
            path: path.clone(),
            storage,
        }
    }
}

impl ImportTask for ImportRelBlocksTask {
    fn key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    #[instrument(level = tracing::Level::DEBUG, skip_all, fields(%self.path))]
    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        debug!("Importing relation file");

        let (rel_tag, start_blk) = self.key_range.start.to_rel_block()?;
        let (rel_tag_end, end_blk) = self.key_range.end.to_rel_block()?;
        assert_eq!(rel_tag, rel_tag_end);

        let ranges = (start_blk..end_blk)
            .enumerate()
            .filter_map(|(i, blknum)| {
                let key = rel_block_to_key(rel_tag, blknum);
                if self.shard_identity.is_key_disposable(&key) {
                    return None;
                }
                let file_offset = i.checked_mul(8192).unwrap();
                Some((
                    vec![key],
                    file_offset,
                    file_offset.checked_add(8192).unwrap(),
                ))
            })
            .coalesce(|(mut acc, acc_start, acc_end), (mut key, start, end)| {
                assert_eq!(key.len(), 1);
                assert!(!acc.is_empty());
                assert!(acc_end > acc_start);
                if acc_end == start /* TODO additional max range check here, to limit memory consumption per task to X */ {
                    acc.push(key.pop().unwrap());
                    Ok((acc, acc_start, end))
                } else {
                    Err(((acc, acc_start, acc_end), (key, start, end)))
                }
            });

        let mut nimages = 0;
        for (keys, range_start, range_end) in ranges {
            let range_buf = self
                .storage
                .get_range(&self.path, range_start.into_u64(), range_end.into_u64())
                .await?;
            let mut buf = Bytes::from(range_buf);
            // TODO: batched writes
            for key in keys {
                let image = buf.split_to(8192);
                layer_writer.put_image(key, image, ctx).await?;
                nimages += 1;
            }
        }

        Ok(nimages)
    }
}

struct ImportSlruBlocksTask {
    key_range: Range<Key>,
    path: RemotePath,
    storage: RemoteStorageWrapper,
}

impl ImportSlruBlocksTask {
    fn new(key_range: Range<Key>, path: &RemotePath, storage: RemoteStorageWrapper) -> Self {
        ImportSlruBlocksTask {
            key_range,
            path: path.clone(),
            storage,
        }
    }
}

impl ImportTask for ImportSlruBlocksTask {
    fn key_range(&self) -> Range<Key> {
        self.key_range.clone()
    }

    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        debug!("Importing SLRU segment file {}", self.path);
        let buf = self.storage.get(&self.path).await?;

        let (kind, segno, start_blk) = self.key_range.start.to_slru_block()?;
        let (_kind, _segno, end_blk) = self.key_range.end.to_slru_block()?;
        let mut blknum = start_blk;
        let mut nimages = 0;
        let mut file_offset = 0;
        while blknum < end_blk {
            let key = slru_block_to_key(kind, segno, blknum);
            let buf = &buf[file_offset..(file_offset + 8192)];
            file_offset += 8192;
            layer_writer
                .put_image(key, Bytes::copy_from_slice(buf), ctx)
                .await?;
            nimages += 1;
            blknum += 1;
        }
        Ok(nimages)
    }
}

enum AnyImportTask {
    SingleKey(ImportSingleKeyTask),
    RelBlocks(ImportRelBlocksTask),
    SlruBlocks(ImportSlruBlocksTask),
}

impl ImportTask for AnyImportTask {
    fn key_range(&self) -> Range<Key> {
        match self {
            Self::SingleKey(t) => t.key_range(),
            Self::RelBlocks(t) => t.key_range(),
            Self::SlruBlocks(t) => t.key_range(),
        }
    }
    /// returns the number of images put into the `layer_writer`
    async fn doit(
        self,
        layer_writer: &mut ImageLayerWriter,
        ctx: &RequestContext,
    ) -> anyhow::Result<usize> {
        match self {
            Self::SingleKey(t) => t.doit(layer_writer, ctx).await,
            Self::RelBlocks(t) => t.doit(layer_writer, ctx).await,
            Self::SlruBlocks(t) => t.doit(layer_writer, ctx).await,
        }
    }
}

impl From<ImportSingleKeyTask> for AnyImportTask {
    fn from(t: ImportSingleKeyTask) -> Self {
        Self::SingleKey(t)
    }
}

impl From<ImportRelBlocksTask> for AnyImportTask {
    fn from(t: ImportRelBlocksTask) -> Self {
        Self::RelBlocks(t)
    }
}

impl From<ImportSlruBlocksTask> for AnyImportTask {
    fn from(t: ImportSlruBlocksTask) -> Self {
        Self::SlruBlocks(t)
    }
}

struct ChunkProcessingJob {
    timeline: Arc<Timeline>,
    range: Range<Key>,
    tasks: Vec<AnyImportTask>,

    pgdata_lsn: Lsn,
}

impl ChunkProcessingJob {
    fn new(range: Range<Key>, tasks: Vec<AnyImportTask>, env: &Flow) -> Self {
        assert!(env.pgdata_lsn.is_valid());
        Self {
            timeline: env.timeline.clone(),
            range,
            tasks,
            pgdata_lsn: env.pgdata_lsn,
        }
    }

    async fn run(self, ctx: &RequestContext) -> anyhow::Result<()> {
        let mut writer = ImageLayerWriter::new(
            self.timeline.conf,
            self.timeline.timeline_id,
            self.timeline.tenant_shard_id,
            &self.range,
            self.pgdata_lsn,
            &self.timeline.gate,
            ctx,
        )
        .await?;

        let mut nimages = 0;
        for task in self.tasks {
            nimages += task.doit(&mut writer, ctx).await?;
        }

        let resident_layer = if nimages > 0 {
            let (desc, path) = writer.finish(ctx).await?;
            Layer::finish_creating(self.timeline.conf, &self.timeline, desc, &path)?
        } else {
            // dropping the writer cleans up
            return Ok(());
        };

        // this is sharing the same code as create_image_layers
        let mut guard = self.timeline.layers.write().await;
        guard
            .open_mut()?
            .track_new_image_layers(&[resident_layer.clone()], &self.timeline.metrics);
        crate::tenant::timeline::drop_wlock(guard);

        // Schedule the layer for upload but don't add barriers such as
        // wait for completion or index upload, so we don't inhibit upload parallelism.
        // TODO: limit upload parallelism somehow (e.g. by limiting concurrency of jobs?)
        // TODO: or regulate parallelism by upload queue depth? Prob should happen at a higher level.
        self.timeline
            .remote_client
            .schedule_layer_file_upload(resident_layer)?;

        Ok(())
    }
}

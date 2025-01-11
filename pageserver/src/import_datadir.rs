//!
//! Import data and WAL from a PostgreSQL data directory and WAL segments into
//! a neon Timeline.
//!
use std::path::{Path, PathBuf};

use anyhow::{bail, ensure, Context, Result};
use bytes::Bytes;
use camino::Utf8Path;
use futures::StreamExt;
use pageserver_api::key::rel_block_to_key;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_tar::Archive;
use tracing::*;
use wal_decoder::models::InterpretedWalRecord;
use walkdir::WalkDir;

use crate::context::RequestContext;
use crate::metrics::WAL_INGEST;
use crate::pgdatadir_mapping::*;
use crate::tenant::Timeline;
use crate::walingest::WalIngest;
use pageserver_api::reltag::{RelTag, SlruKind};
use postgres_ffi::pg_constants;
use postgres_ffi::relfile_utils::*;
use postgres_ffi::waldecoder::WalStreamDecoder;
use postgres_ffi::ControlFileData;
use postgres_ffi::DBState_DB_SHUTDOWNED;
use postgres_ffi::Oid;
use postgres_ffi::XLogFileName;
use postgres_ffi::{BLCKSZ, WAL_SEGMENT_SIZE};
use utils::lsn::Lsn;

// Returns checkpoint LSN from controlfile
pub fn get_lsn_from_controlfile(path: &Utf8Path) -> Result<Lsn> {
    // Read control file to extract the LSN
    let controlfile_path = path.join("global").join("pg_control");
    let controlfile_buf = std::fs::read(&controlfile_path)
        .with_context(|| format!("reading controlfile: {controlfile_path}"))?;
    let controlfile = ControlFileData::decode(&controlfile_buf)?;
    let lsn = controlfile.checkPoint;

    Ok(Lsn(lsn))
}

///
/// Import all relation data pages from local disk into the repository.
///
/// This is currently only used to import a cluster freshly created by initdb.
/// The code that deals with the checkpoint would not work right if the
/// cluster was not shut down cleanly.
pub async fn import_timeline_from_postgres_datadir(
    tline: &Timeline,
    pgdata_path: &Utf8Path,
    pgdata_lsn: Lsn,
    ctx: &RequestContext,
) -> Result<()> {
    let mut pg_control: Option<ControlFileData> = None;

    // TODO this shoud be start_lsn, which is not necessarily equal to end_lsn (aka lsn)
    // Then fishing out pg_control would be unnecessary
    let mut modification = tline.begin_modification(pgdata_lsn);
    modification.init_empty()?;

    // Import all but pg_wal
    let all_but_wal = WalkDir::new(pgdata_path)
        .into_iter()
        .filter_entry(|entry| !entry.path().ends_with("pg_wal"));
    for entry in all_but_wal {
        let entry = entry?;
        let metadata = entry.metadata().expect("error getting dir entry metadata");
        if metadata.is_file() {
            let absolute_path = entry.path();
            let relative_path = absolute_path.strip_prefix(pgdata_path)?;

            let mut file = tokio::fs::File::open(absolute_path).await?;
            let len = metadata.len() as usize;
            if let Some(control_file) =
                import_file(&mut modification, relative_path, &mut file, len, ctx).await?
            {
                pg_control = Some(control_file);
            }
            modification.flush(ctx).await?;
        }
    }

    // We're done importing all the data files.
    modification.commit(ctx).await?;

    // We expect the Postgres server to be shut down cleanly.
    let pg_control = pg_control.context("pg_control file not found")?;
    ensure!(
        pg_control.state == DBState_DB_SHUTDOWNED,
        "Postgres cluster was not shut down cleanly"
    );
    ensure!(
        pg_control.checkPointCopy.redo == pgdata_lsn.0,
        "unexpected checkpoint REDO pointer"
    );

    // Import WAL. This is needed even when starting from a shutdown checkpoint, because
    // this reads the checkpoint record itself, advancing the tip of the timeline to
    // *after* the checkpoint record. And crucially, it initializes the 'prev_lsn'.
    import_wal(
        &pgdata_path.join("pg_wal"),
        tline,
        Lsn(pg_control.checkPointCopy.redo),
        pgdata_lsn,
        ctx,
    )
    .await?;

    Ok(())
}

// subroutine of import_timeline_from_postgres_datadir(), to load one relation file.
async fn import_rel(
    modification: &mut DatadirModification<'_>,
    path: &Path,
    spcoid: Oid,
    dboid: Oid,
    reader: &mut (impl AsyncRead + Unpin),
    len: usize,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    // Does it look like a relation file?
    trace!("importing rel file {}", path.display());

    let filename = &path
        .file_name()
        .expect("missing rel filename")
        .to_string_lossy();
    let (relnode, forknum, segno) = parse_relfilename(filename).map_err(|e| {
        warn!("unrecognized file in postgres datadir: {:?} ({})", path, e);
        e
    })?;

    let mut buf: [u8; 8192] = [0u8; 8192];

    ensure!(len % BLCKSZ as usize == 0);
    let nblocks = len / BLCKSZ as usize;

    let rel = RelTag {
        spcnode: spcoid,
        dbnode: dboid,
        relnode,
        forknum,
    };

    let mut blknum: u32 = segno * (1024 * 1024 * 1024 / BLCKSZ as u32);

    // Call put_rel_creation for every segment of the relation,
    // because there is no guarantee about the order in which we are processing segments.
    // ignore "relation already exists" error
    //
    // FIXME: Keep track of which relations we've already created?
    // https://github.com/neondatabase/neon/issues/3309
    if let Err(e) = modification
        .put_rel_creation(rel, nblocks as u32, ctx)
        .await
    {
        match e {
            RelationError::AlreadyExists => {
                debug!("Relation {} already exist. We must be extending it.", rel)
            }
            _ => return Err(e.into()),
        }
    }

    loop {
        let r = reader.read_exact(&mut buf).await;
        match r {
            Ok(_) => {
                let key = rel_block_to_key(rel, blknum);
                if modification.tline.get_shard_identity().is_key_local(&key) {
                    modification.put_rel_page_image(rel, blknum, Bytes::copy_from_slice(&buf))?;
                }
            }

            // TODO: UnexpectedEof is expected
            Err(err) => match err.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // reached EOF. That's expected.
                    let relative_blknum = blknum - segno * (1024 * 1024 * 1024 / BLCKSZ as u32);
                    ensure!(relative_blknum == nblocks as u32, "unexpected EOF");
                    break;
                }
                _ => {
                    bail!("error reading file {}: {:#}", path.display(), err);
                }
            },
        };
        blknum += 1;
    }

    // Update relation size
    //
    // If we process rel segments out of order,
    // put_rel_extend will skip the update.
    modification.put_rel_extend(rel, blknum, ctx).await?;

    Ok(())
}

/// Import an SLRU segment file
///
async fn import_slru(
    modification: &mut DatadirModification<'_>,
    slru: SlruKind,
    path: &Path,
    reader: &mut (impl AsyncRead + Unpin),
    len: usize,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    info!("importing slru file {path:?}");

    let mut buf: [u8; 8192] = [0u8; 8192];
    let filename = &path
        .file_name()
        .with_context(|| format!("missing slru filename for path {path:?}"))?
        .to_string_lossy();
    let segno = u32::from_str_radix(filename, 16)?;

    ensure!(len % BLCKSZ as usize == 0); // we assume SLRU block size is the same as BLCKSZ
    let nblocks = len / BLCKSZ as usize;

    ensure!(nblocks <= pg_constants::SLRU_PAGES_PER_SEGMENT as usize);

    modification
        .put_slru_segment_creation(slru, segno, nblocks as u32, ctx)
        .await?;

    let mut rpageno = 0;
    loop {
        let r = reader.read_exact(&mut buf).await;
        match r {
            Ok(_) => {
                modification.put_slru_page_image(
                    slru,
                    segno,
                    rpageno,
                    Bytes::copy_from_slice(&buf),
                )?;
            }

            // TODO: UnexpectedEof is expected
            Err(err) => match err.kind() {
                std::io::ErrorKind::UnexpectedEof => {
                    // reached EOF. That's expected.
                    ensure!(rpageno == nblocks as u32, "unexpected EOF");
                    break;
                }
                _ => {
                    bail!("error reading file {}: {:#}", path.display(), err);
                }
            },
        };
        rpageno += 1;
    }

    Ok(())
}

/// Scan PostgreSQL WAL files in given directory and load all records between
/// 'startpoint' and 'endpoint' into the repository.
async fn import_wal(
    walpath: &Utf8Path,
    tline: &Timeline,
    startpoint: Lsn,
    endpoint: Lsn,
    ctx: &RequestContext,
) -> anyhow::Result<()> {
    let mut waldecoder = WalStreamDecoder::new(startpoint, tline.pg_version);

    let mut segno = startpoint.segment_number(WAL_SEGMENT_SIZE);
    let mut offset = startpoint.segment_offset(WAL_SEGMENT_SIZE);
    let mut last_lsn = startpoint;

    let mut walingest = WalIngest::new(tline, startpoint, ctx).await?;

    let shard = vec![*tline.get_shard_identity()];

    while last_lsn <= endpoint {
        // FIXME: assume postgresql tli 1 for now
        let filename = XLogFileName(1, segno, WAL_SEGMENT_SIZE);
        let mut buf = Vec::new();

        // Read local file
        let mut path = walpath.join(&filename);

        // It could be as .partial
        if !PathBuf::from(&path).exists() {
            path = walpath.join(filename + ".partial");
        }

        // Slurp the WAL file
        let mut file = std::fs::File::open(&path)?;

        if offset > 0 {
            use std::io::Seek;
            file.seek(std::io::SeekFrom::Start(offset as u64))?;
        }

        use std::io::Read;
        let nread = file.read_to_end(&mut buf)?;
        if nread != WAL_SEGMENT_SIZE - offset {
            // Maybe allow this for .partial files?
            error!("read only {} bytes from WAL file", nread);
        }

        waldecoder.feed_bytes(&buf);

        let mut nrecords = 0;
        let mut modification = tline.begin_modification(last_lsn);
        while last_lsn <= endpoint {
            if let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                let interpreted = InterpretedWalRecord::from_bytes_filtered(
                    recdata,
                    &shard,
                    lsn,
                    tline.pg_version,
                )?
                .remove(tline.get_shard_identity())
                .unwrap();

                walingest
                    .ingest_record(interpreted, &mut modification, ctx)
                    .await?;
                WAL_INGEST.records_committed.inc();

                modification.commit(ctx).await?;
                last_lsn = lsn;

                nrecords += 1;

                trace!("imported record at {} (end {})", lsn, endpoint);
            }
        }

        debug!("imported {} records up to {}", nrecords, last_lsn);

        segno += 1;
        offset = 0;
    }

    if last_lsn != startpoint {
        info!("reached end of WAL at {}", last_lsn);
    } else {
        info!("no WAL to import at {}", last_lsn);
    }

    Ok(())
}

pub async fn import_basebackup_from_tar(
    tline: &Timeline,
    reader: &mut (impl AsyncRead + Send + Sync + Unpin),
    base_lsn: Lsn,
    ctx: &RequestContext,
) -> Result<()> {
    info!("importing base at {base_lsn}");
    let mut modification = tline.begin_modification(base_lsn);
    modification.init_empty()?;

    let mut pg_control: Option<ControlFileData> = None;

    // Import base
    let mut entries = Archive::new(reader).entries()?;
    while let Some(base_tar_entry) = entries.next().await {
        let mut entry = base_tar_entry?;
        let header = entry.header();
        let len = header.entry_size()? as usize;
        let file_path = header.path()?.into_owned();

        match header.entry_type() {
            tokio_tar::EntryType::Regular => {
                if let Some(res) =
                    import_file(&mut modification, file_path.as_ref(), &mut entry, len, ctx).await?
                {
                    // We found the pg_control file.
                    pg_control = Some(res);
                }
                modification.flush(ctx).await?;
            }
            tokio_tar::EntryType::Directory => {
                debug!("directory {:?}", file_path);
            }
            _ => {
                bail!(
                    "entry {} in backup tar archive is of unexpected type: {:?}",
                    file_path.display(),
                    header.entry_type()
                );
            }
        }
    }

    // sanity check: ensure that pg_control is loaded
    let _pg_control = pg_control.context("pg_control file not found")?;

    modification.commit(ctx).await?;
    Ok(())
}

pub async fn import_wal_from_tar(
    tline: &Timeline,
    reader: &mut (impl AsyncRead + Send + Sync + Unpin),
    start_lsn: Lsn,
    end_lsn: Lsn,
    ctx: &RequestContext,
) -> Result<()> {
    // Set up walingest mutable state
    let mut waldecoder = WalStreamDecoder::new(start_lsn, tline.pg_version);
    let mut segno = start_lsn.segment_number(WAL_SEGMENT_SIZE);
    let mut offset = start_lsn.segment_offset(WAL_SEGMENT_SIZE);
    let mut last_lsn = start_lsn;
    let mut walingest = WalIngest::new(tline, start_lsn, ctx).await?;
    let shard = vec![*tline.get_shard_identity()];

    // Ingest wal until end_lsn
    info!("importing wal until {}", end_lsn);
    let mut pg_wal_tar = Archive::new(reader);
    let mut pg_wal_entries = pg_wal_tar.entries()?;
    while last_lsn <= end_lsn {
        let bytes = {
            let mut entry = pg_wal_entries
                .next()
                .await
                .ok_or_else(|| anyhow::anyhow!("expected more wal"))??;
            let header = entry.header();
            let file_path = header.path()?.into_owned();

            match header.entry_type() {
                tokio_tar::EntryType::Regular => {
                    // FIXME: assume postgresql tli 1 for now
                    let expected_filename = XLogFileName(1, segno, WAL_SEGMENT_SIZE);
                    let file_name = file_path
                        .file_name()
                        .expect("missing wal filename")
                        .to_string_lossy();
                    ensure!(expected_filename == file_name);

                    debug!("processing wal file {:?}", file_path);
                    read_all_bytes(&mut entry).await?
                }
                tokio_tar::EntryType::Directory => {
                    debug!("directory {:?}", file_path);
                    continue;
                }
                _ => {
                    bail!(
                        "entry {} in WAL tar archive is of unexpected type: {:?}",
                        file_path.display(),
                        header.entry_type()
                    );
                }
            }
        };

        waldecoder.feed_bytes(&bytes[offset..]);

        let mut modification = tline.begin_modification(last_lsn);
        while last_lsn <= end_lsn {
            if let Some((lsn, recdata)) = waldecoder.poll_decode()? {
                let interpreted = InterpretedWalRecord::from_bytes_filtered(
                    recdata,
                    &shard,
                    lsn,
                    tline.pg_version,
                )?
                .remove(tline.get_shard_identity())
                .unwrap();

                walingest
                    .ingest_record(interpreted, &mut modification, ctx)
                    .await?;
                modification.commit(ctx).await?;
                last_lsn = lsn;

                debug!("imported record at {} (end {})", lsn, end_lsn);
            }
        }

        debug!("imported records up to {}", last_lsn);
        segno += 1;
        offset = 0;
    }

    if last_lsn != start_lsn {
        info!("reached end of WAL at {}", last_lsn);
    } else {
        info!("there was no WAL to import at {}", last_lsn);
    }

    // Log any extra unused files
    while let Some(e) = pg_wal_entries.next().await {
        let entry = e?;
        let header = entry.header();
        let file_path = header.path()?.into_owned();
        info!("skipping {:?}", file_path);
    }

    Ok(())
}

async fn import_file(
    modification: &mut DatadirModification<'_>,
    file_path: &Path,
    reader: &mut (impl AsyncRead + Send + Sync + Unpin),
    len: usize,
    ctx: &RequestContext,
) -> Result<Option<ControlFileData>> {
    let file_name = match file_path.file_name() {
        Some(name) => name.to_string_lossy(),
        None => return Ok(None),
    };

    if file_name.starts_with('.') {
        // tar archives on macOs, created without COPYFILE_DISABLE=1 env var
        // will contain "fork files", skip them.
        return Ok(None);
    }

    if file_path.starts_with("global") {
        let spcnode = postgres_ffi::pg_constants::GLOBALTABLESPACE_OID;
        let dbnode = 0;

        match file_name.as_ref() {
            "pg_control" => {
                let bytes = read_all_bytes(reader).await?;

                // Extract the checkpoint record and import it separately.
                let pg_control = ControlFileData::decode(&bytes[..])?;
                let checkpoint_bytes = pg_control.checkPointCopy.encode()?;
                modification.put_checkpoint(checkpoint_bytes)?;
                debug!("imported control file");

                // Import it as ControlFile
                modification.put_control_file(bytes)?;
                return Ok(Some(pg_control));
            }
            "pg_filenode.map" => {
                let bytes = read_all_bytes(reader).await?;
                modification
                    .put_relmap_file(spcnode, dbnode, bytes, ctx)
                    .await?;
                debug!("imported relmap file")
            }
            "PG_VERSION" => {
                debug!("ignored PG_VERSION file");
            }
            _ => {
                import_rel(modification, file_path, spcnode, dbnode, reader, len, ctx).await?;
                debug!("imported rel creation");
            }
        }
    } else if file_path.starts_with("base") {
        let spcnode = pg_constants::DEFAULTTABLESPACE_OID;
        let dbnode: u32 = file_path
            .iter()
            .nth(1)
            .expect("invalid file path, expected dbnode")
            .to_string_lossy()
            .parse()?;

        match file_name.as_ref() {
            "pg_filenode.map" => {
                let bytes = read_all_bytes(reader).await?;
                modification
                    .put_relmap_file(spcnode, dbnode, bytes, ctx)
                    .await?;
                debug!("imported relmap file")
            }
            "PG_VERSION" => {
                debug!("ignored PG_VERSION file");
            }
            _ => {
                import_rel(modification, file_path, spcnode, dbnode, reader, len, ctx).await?;
                debug!("imported rel creation");
            }
        }
    } else if file_path.starts_with("pg_xact") {
        let slru = SlruKind::Clog;

        if modification.tline.tenant_shard_id.is_shard_zero() {
            import_slru(modification, slru, file_path, reader, len, ctx).await?;
            debug!("imported clog slru");
        }
    } else if file_path.starts_with("pg_multixact/offsets") {
        let slru = SlruKind::MultiXactOffsets;

        if modification.tline.tenant_shard_id.is_shard_zero() {
            import_slru(modification, slru, file_path, reader, len, ctx).await?;
            debug!("imported multixact offsets slru");
        }
    } else if file_path.starts_with("pg_multixact/members") {
        let slru = SlruKind::MultiXactMembers;

        if modification.tline.tenant_shard_id.is_shard_zero() {
            import_slru(modification, slru, file_path, reader, len, ctx).await?;
            debug!("imported multixact members slru");
        }
    } else if file_path.starts_with("pg_twophase") {
        let bytes = read_all_bytes(reader).await?;

        // In PostgreSQL v17, this is a 64-bit FullTransactionid. In previous versions,
        // it's a 32-bit TransactionId, which fits in u64 anyway.
        let xid = u64::from_str_radix(file_name.as_ref(), 16)?;
        modification
            .put_twophase_file(xid, Bytes::copy_from_slice(&bytes[..]), ctx)
            .await?;
        debug!("imported twophase file");
    } else if file_path.starts_with("pg_wal") {
        debug!("found wal file in base section. ignore it");
    } else if file_path.starts_with("zenith.signal") {
        // Parse zenith signal file to set correct previous LSN
        let bytes = read_all_bytes(reader).await?;
        // zenith.signal format is "PREV LSN: prev_lsn"
        // TODO write serialization and deserialization in the same place.
        let zenith_signal = std::str::from_utf8(&bytes)?.trim();
        let prev_lsn = match zenith_signal {
            "PREV LSN: none" => Lsn(0),
            "PREV LSN: invalid" => Lsn(0),
            other => {
                let split = other.split(':').collect::<Vec<_>>();
                split[1]
                    .trim()
                    .parse::<Lsn>()
                    .context("can't parse zenith.signal")?
            }
        };

        // zenith.signal is not necessarily the last file, that we handle
        // but it is ok to call `finish_write()`, because final `modification.commit()`
        // will update lsn once more to the final one.
        let writer = modification.tline.writer().await;
        writer.finish_write(prev_lsn);

        debug!("imported zenith signal {}", prev_lsn);
    } else if file_path.starts_with("pg_tblspc") {
        // TODO Backups exported from neon won't have pg_tblspc, but we will need
        // this to import arbitrary postgres databases.
        bail!("Importing pg_tblspc is not implemented");
    } else {
        debug!(
            "ignoring unrecognized file \"{}\" in tar archive",
            file_path.display()
        );
    }

    Ok(None)
}

async fn read_all_bytes(reader: &mut (impl AsyncRead + Unpin)) -> Result<Bytes> {
    let mut buf: Vec<u8> = vec![];
    reader.read_to_end(&mut buf).await?;
    Ok(Bytes::from(buf))
}

mod no_leak_child;
/// The IPC protocol that pageserver and walredo process speak over their shared pipe.
mod protocol;

use self::no_leak_child::NoLeakChild;
use crate::{
    config::PageServerConf,
    metrics::{WalRedoKillCause, WAL_REDO_PROCESS_COUNTERS, WAL_REDO_RECORD_COUNTER},
    page_cache::PAGE_SZ,
    span::debug_assert_current_span_has_tenant_id,
};
use anyhow::Context;
use bytes::Bytes;
use pageserver_api::record::NeonWalRecord;
use pageserver_api::{reltag::RelTag, shard::TenantShardId};
use postgres_ffi::BLCKSZ;
#[cfg(feature = "testing")]
use std::sync::atomic::AtomicUsize;
use std::{
    collections::VecDeque,
    process::{Command, Stdio},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, instrument, Instrument};
use utils::{lsn::Lsn, poison::Poison};

pub struct WalRedoProcess {
    #[allow(dead_code)]
    conf: &'static PageServerConf,
    #[cfg(feature = "testing")]
    tenant_shard_id: TenantShardId,
    // Some() on construction, only becomes None on Drop.
    child: Option<NoLeakChild>,
    stdout: tokio::sync::Mutex<Poison<ProcessOutput>>,
    stdin: tokio::sync::Mutex<Poison<ProcessInput>>,
    /// Counter to separate same sized walredo inputs failing at the same millisecond.
    #[cfg(feature = "testing")]
    dump_sequence: AtomicUsize,
}

struct ProcessInput {
    stdin: tokio::process::ChildStdin,
    n_requests: usize,
}

struct ProcessOutput {
    stdout: tokio::process::ChildStdout,
    pending_responses: VecDeque<Option<Bytes>>,
    n_processed_responses: usize,
}

impl WalRedoProcess {
    //
    // Start postgres binary in special WAL redo mode.
    //
    #[instrument(skip_all,fields(pg_version=pg_version))]
    pub(crate) fn launch(
        conf: &'static PageServerConf,
        tenant_shard_id: TenantShardId,
        pg_version: u32,
    ) -> anyhow::Result<Self> {
        crate::span::debug_assert_current_span_has_tenant_id();

        let pg_bin_dir_path = conf.pg_bin_dir(pg_version).context("pg_bin_dir")?; // TODO these should be infallible.
        let pg_lib_dir_path = conf.pg_lib_dir(pg_version).context("pg_lib_dir")?;

        use no_leak_child::NoLeakChildCommandExt;
        // Start postgres itself
        let child = Command::new(pg_bin_dir_path.join("postgres"))
            // the first arg must be --wal-redo so the child process enters into walredo mode
            .arg("--wal-redo")
            // the child doesn't process this arg, but, having it in the argv helps indentify the
            // walredo process for a particular tenant when debugging a pagserver
            .args(["--tenant-shard-id", &format!("{tenant_shard_id}")])
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .stdout(Stdio::piped())
            .env_clear()
            .env("LD_LIBRARY_PATH", &pg_lib_dir_path)
            .env("DYLD_LIBRARY_PATH", &pg_lib_dir_path)
            .env(
                "ASAN_OPTIONS",
                std::env::var("ASAN_OPTIONS").unwrap_or_default(),
            )
            .env(
                "UBSAN_OPTIONS",
                std::env::var("UBSAN_OPTIONS").unwrap_or_default(),
            )
            // NB: The redo process is not trusted after we sent it the first
            // walredo work. Before that, it is trusted. Specifically, we trust
            // it to
            // 1. close all file descriptors except stdin, stdout, stderr because
            //    pageserver might not be 100% diligent in setting FD_CLOEXEC on all
            //    the files it opens, and
            // 2. to use seccomp to sandbox itself before processing the first
            //    walredo request.
            .spawn_no_leak_child(tenant_shard_id)
            .context("spawn process")?;
        WAL_REDO_PROCESS_COUNTERS.started.inc();
        let mut child = scopeguard::guard(child, |child| {
            error!("killing wal-redo-postgres process due to a problem during launch");
            child.kill_and_wait(WalRedoKillCause::Startup);
        });

        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let stderr = tokio::process::ChildStderr::from_std(stderr)
            .context("convert to tokio::ChildStderr")?;
        let stdin =
            tokio::process::ChildStdin::from_std(stdin).context("convert to tokio::ChildStdin")?;
        let stdout = tokio::process::ChildStdout::from_std(stdout)
            .context("convert to tokio::ChildStdout")?;

        // all fallible operations post-spawn are complete, so get rid of the guard
        let child = scopeguard::ScopeGuard::into_inner(child);

        tokio::spawn(
            async move {
                scopeguard::defer! {
                    debug!("wal-redo-postgres stderr_logger_task finished");
                    crate::metrics::WAL_REDO_PROCESS_COUNTERS.active_stderr_logger_tasks_finished.inc();
                }
                debug!("wal-redo-postgres stderr_logger_task started");
                crate::metrics::WAL_REDO_PROCESS_COUNTERS.active_stderr_logger_tasks_started.inc();

                use tokio::io::AsyncBufReadExt;
                let mut stderr_lines = tokio::io::BufReader::new(stderr);
                let mut buf = Vec::new();
                let res = loop {
                    buf.clear();
                    // TODO we don't trust the process to cap its stderr length.
                    // Currently it can do unbounded Vec allocation.
                    match stderr_lines.read_until(b'\n', &mut buf).await {
                        Ok(0) => break Ok(()), // eof
                        Ok(num_bytes) => {
                            let output = String::from_utf8_lossy(&buf[..num_bytes]);
                            error!(%output, "received output");
                        }
                        Err(e) => {
                            break Err(e);
                        }
                    }
                };
                match res {
                    Ok(()) => (),
                    Err(e) => {
                        error!(error=?e, "failed to read from walredo stderr");
                    }
                }
            }.instrument(tracing::info_span!(parent: None, "wal-redo-postgres-stderr", pid = child.id(), tenant_id = %tenant_shard_id.tenant_id, shard_id = %tenant_shard_id.shard_slug(), %pg_version))
        );

        Ok(Self {
            conf,
            #[cfg(feature = "testing")]
            tenant_shard_id,
            child: Some(child),
            stdin: tokio::sync::Mutex::new(Poison::new(
                "stdin",
                ProcessInput {
                    stdin,
                    n_requests: 0,
                },
            )),
            stdout: tokio::sync::Mutex::new(Poison::new(
                "stdout",
                ProcessOutput {
                    stdout,
                    pending_responses: VecDeque::new(),
                    n_processed_responses: 0,
                },
            )),
            #[cfg(feature = "testing")]
            dump_sequence: AtomicUsize::default(),
        })
    }

    pub(crate) fn id(&self) -> u32 {
        self.child
            .as_ref()
            .expect("must not call this during Drop")
            .id()
    }

    /// Apply given WAL records ('records') over an old page image. Returns
    /// new page image.
    ///
    /// # Cancel-Safety
    ///
    /// Cancellation safe.
    #[instrument(skip_all, fields(pid=%self.id()))]
    pub(crate) async fn apply_wal_records(
        &self,
        rel: RelTag,
        blknum: u32,
        base_img: &Option<Bytes>,
        records: &[(Lsn, NeonWalRecord)],
        wal_redo_timeout: Duration,
    ) -> anyhow::Result<Bytes> {
        debug_assert_current_span_has_tenant_id();

        let tag = protocol::BufferTag { rel, blknum };

        // Serialize all the messages to send the WAL redo process first.
        //
        // This could be problematic if there are millions of records to replay,
        // but in practice the number of records is usually so small that it doesn't
        // matter, and it's better to keep this code simple.
        //
        // Most requests start with a before-image with BLCKSZ bytes, followed by
        // by some other WAL records. Start with a buffer that can hold that
        // comfortably.
        let mut writebuf: Vec<u8> = Vec::with_capacity((BLCKSZ as usize) * 3);
        protocol::build_begin_redo_for_block_msg(tag, &mut writebuf);
        if let Some(img) = base_img {
            protocol::build_push_page_msg(tag, img, &mut writebuf);
        }
        for (lsn, rec) in records.iter() {
            if let NeonWalRecord::Postgres {
                will_init: _,
                rec: postgres_rec,
            } = rec
            {
                protocol::build_apply_record_msg(*lsn, postgres_rec, &mut writebuf);
            } else {
                anyhow::bail!("tried to pass neon wal record to postgres WAL redo");
            }
        }
        protocol::build_get_page_msg(tag, &mut writebuf);
        WAL_REDO_RECORD_COUNTER.inc_by(records.len() as u64);

        let Ok(res) =
            tokio::time::timeout(wal_redo_timeout, self.apply_wal_records0(&writebuf)).await
        else {
            anyhow::bail!("WAL redo timed out");
        };

        if res.is_err() {
            // not all of these can be caused by this particular input, however these are so rare
            // in tests so capture all.
            self.record_and_log(&writebuf);
        }

        res
    }

    /// Do a ping request-response roundtrip.
    ///
    /// Not used in production, but by Rust benchmarks.
    pub(crate) async fn ping(&self, timeout: Duration) -> anyhow::Result<()> {
        let mut writebuf: Vec<u8> = Vec::with_capacity(4);
        protocol::build_ping_msg(&mut writebuf);
        let Ok(res) = tokio::time::timeout(timeout, self.apply_wal_records0(&writebuf)).await
        else {
            anyhow::bail!("WAL redo ping timed out");
        };
        let response = res?;
        if response.len() != PAGE_SZ {
            anyhow::bail!(
                "WAL redo ping response should respond with page-sized response: {}",
                response.len()
            );
        }
        Ok(())
    }

    /// # Cancel-Safety
    ///
    /// When not polled to completion (e.g. because in `tokio::select!` another
    /// branch becomes ready before this future), concurrent and subsequent
    /// calls may fail due to [`utils::poison::Poison::check_and_arm`] calls.
    /// Dispose of this process instance and create a new one.
    async fn apply_wal_records0(&self, writebuf: &[u8]) -> anyhow::Result<Bytes> {
        let request_no = {
            let mut lock_guard = self.stdin.lock().await;
            let mut poison_guard = lock_guard.check_and_arm()?;
            let input = poison_guard.data_mut();
            input
                .stdin
                .write_all(writebuf)
                .await
                .context("write to walredo stdin")?;
            let request_no = input.n_requests;
            input.n_requests += 1;
            poison_guard.disarm();
            request_no
        };

        // To improve walredo performance we separate sending requests and receiving
        // responses. Them are protected by different mutexes (output and input).
        // If thread T1, T2, T3 send requests D1, D2, D3 to walredo process
        // then there is not warranty that T1 will first granted output mutex lock.
        // To address this issue we maintain number of sent requests, number of processed
        // responses and ring buffer with pending responses. After sending response
        // (under input mutex), threads remembers request number. Then it releases
        // input mutex, locks output mutex and fetch in ring buffer all responses until
        // its stored request number. The it takes correspondent element from
        // pending responses ring buffer and truncate all empty elements from the front,
        // advancing processed responses number.

        let mut lock_guard = self.stdout.lock().await;
        let mut poison_guard = lock_guard.check_and_arm()?;
        let output = poison_guard.data_mut();
        let n_processed_responses = output.n_processed_responses;
        while n_processed_responses + output.pending_responses.len() <= request_no {
            // We expect the WAL redo process to respond with an 8k page image. We read it
            // into this buffer.
            let mut resultbuf = vec![0; BLCKSZ.into()];
            output
                .stdout
                .read_exact(&mut resultbuf)
                .await
                .context("read walredo stdout")?;
            output
                .pending_responses
                .push_back(Some(Bytes::from(resultbuf)));
        }
        // Replace our request's response with None in `pending_responses`.
        // Then make space in the ring buffer by clearing out any seqence of contiguous
        // `None`'s from the front of `pending_responses`.
        // NB: We can't pop_front() because other requests' responses because another
        // requester might have grabbed the output mutex before us:
        // T1: grab input mutex
        // T1: send request_no 23
        // T1: release input mutex
        // T2: grab input mutex
        // T2: send request_no 24
        // T2: release input mutex
        // T2: grab output mutex
        // T2: n_processed_responses + output.pending_responses.len() <= request_no
        //            23                                0                   24
        // T2: enters poll loop that reads stdout
        // T2: put response for 23 into pending_responses
        // T2: put response for 24 into pending_resposnes
        // pending_responses now looks like this: Front Some(response_23) Some(response_24) Back
        // T2: takes its response_24
        // pending_responses now looks like this: Front Some(response_23) None Back
        // T2: does the while loop below
        // pending_responses now looks like this: Front Some(response_23) None Back
        // T2: releases output mutex
        // T1: grabs output mutex
        // T1: n_processed_responses + output.pending_responses.len() > request_no
        //            23                                2                   23
        // T1: skips poll loop that reads stdout
        // T1: takes its response_23
        // pending_responses now looks like this: Front None None Back
        // T2: does the while loop below
        // pending_responses now looks like this: Front Back
        // n_processed_responses now has value 25
        let res = output.pending_responses[request_no - n_processed_responses]
            .take()
            .expect("we own this request_no, nobody else is supposed to take it");
        while let Some(front) = output.pending_responses.front() {
            if front.is_none() {
                output.pending_responses.pop_front();
                output.n_processed_responses += 1;
            } else {
                break;
            }
        }
        poison_guard.disarm();
        Ok(res)
    }

    #[cfg(feature = "testing")]
    fn record_and_log(&self, writebuf: &[u8]) {
        use std::sync::atomic::Ordering;

        let millis = std::time::SystemTime::now()
            .duration_since(std::time::SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();

        let seq = self.dump_sequence.fetch_add(1, Ordering::Relaxed);

        // these files will be collected to an allure report
        let filename = format!("walredo-{millis}-{}-{seq}.walredo", writebuf.len());

        let path = self.conf.tenant_path(&self.tenant_shard_id).join(&filename);

        use std::io::Write;
        let res = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .read(true)
            .open(path)
            .and_then(|mut f| f.write_all(writebuf));

        // trip up allowed_errors
        if let Err(e) = res {
            tracing::error!(target=%filename, length=writebuf.len(), "failed to write out the walredo errored input: {e}");
        } else {
            tracing::error!(filename, "erroring walredo input saved");
        }
    }

    #[cfg(not(feature = "testing"))]
    fn record_and_log(&self, _: &[u8]) {}
}

impl Drop for WalRedoProcess {
    fn drop(&mut self) {
        self.child
            .take()
            .expect("we only do this once")
            .kill_and_wait(WalRedoKillCause::WalRedoProcessDrop);
        // no way to wait for stderr_logger_task from Drop because that is async only
    }
}

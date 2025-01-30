use anyhow::{bail, ensure};
use camino_tempfile::{tempdir, Utf8TempDir};
use log::*;
use postgres::types::PgLsn;
use postgres::Client;
use postgres_ffi::{WAL_SEGMENT_SIZE, XLOG_BLCKSZ};
use postgres_ffi::{
    XLOG_SIZE_OF_XLOG_LONG_PHD, XLOG_SIZE_OF_XLOG_RECORD, XLOG_SIZE_OF_XLOG_SHORT_PHD,
};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

macro_rules! xlog_utils_test {
    ($version:ident) => {
        #[path = "."]
        mod $version {
            #[allow(unused_imports)]
            pub use postgres_ffi::$version::wal_craft_test_export::*;
            #[allow(clippy::duplicate_mod)]
            #[cfg(test)]
            mod xlog_utils_test;
        }
    };
}

postgres_ffi::for_all_postgres_versions! { xlog_utils_test }

pub struct Conf {
    pub pg_version: u32,
    pub pg_distrib_dir: PathBuf,
    pub datadir: PathBuf,
}

pub struct PostgresServer {
    process: std::process::Child,
    _unix_socket_dir: Utf8TempDir,
    client_config: postgres::Config,
}

pub static REQUIRED_POSTGRES_CONFIG: [&str; 4] = [
    "wal_keep_size=50MB",            // Ensure old WAL is not removed
    "shared_preload_libraries=neon", // can only be loaded at startup
    // Disable background processes as much as possible
    "wal_writer_delay=10s",
    "autovacuum=off",
];

impl Conf {
    pub fn pg_distrib_dir(&self) -> anyhow::Result<PathBuf> {
        let path = self.pg_distrib_dir.clone();

        #[allow(clippy::manual_range_patterns)]
        match self.pg_version {
            14 | 15 | 16 | 17 => Ok(path.join(format!("v{}", self.pg_version))),
            _ => bail!("Unsupported postgres version: {}", self.pg_version),
        }
    }

    fn pg_bin_dir(&self) -> anyhow::Result<PathBuf> {
        Ok(self.pg_distrib_dir()?.join("bin"))
    }

    fn pg_lib_dir(&self) -> anyhow::Result<PathBuf> {
        Ok(self.pg_distrib_dir()?.join("lib"))
    }

    pub fn wal_dir(&self) -> PathBuf {
        self.datadir.join("pg_wal")
    }

    fn new_pg_command(&self, command: impl AsRef<Path>) -> anyhow::Result<Command> {
        let path = self.pg_bin_dir()?.join(command);
        ensure!(path.exists(), "Command {:?} does not exist", path);
        let mut cmd = Command::new(path);
        cmd.env_clear()
            .env("LD_LIBRARY_PATH", self.pg_lib_dir()?)
            .env("DYLD_LIBRARY_PATH", self.pg_lib_dir()?)
            .env(
                "ASAN_OPTIONS",
                std::env::var("ASAN_OPTIONS").unwrap_or_default(),
            )
            .env(
                "UBSAN_OPTIONS",
                std::env::var("UBSAN_OPTIONS").unwrap_or_default(),
            );
        Ok(cmd)
    }

    pub fn initdb(&self) -> anyhow::Result<()> {
        if let Some(parent) = self.datadir.parent() {
            info!("Pre-creating parent directory {:?}", parent);
            // Tests may be run concurrently and there may be a race to create `test_output/`.
            // std::fs::create_dir_all is guaranteed to have no races with another thread creating directories.
            std::fs::create_dir_all(parent)?;
        }
        info!(
            "Running initdb in {:?} with user \"postgres\"",
            self.datadir
        );
        let output = self
            .new_pg_command("initdb")?
            .arg("--pgdata")
            .arg(&self.datadir)
            .args(["--username", "postgres", "--no-instructions", "--no-sync"])
            .output()?;
        debug!("initdb output: {:?}", output);
        ensure!(
            output.status.success(),
            "initdb failed, stdout and stderr follow:\n{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        Ok(())
    }

    pub fn start_server(&self) -> anyhow::Result<PostgresServer> {
        info!("Starting Postgres server in {:?}", self.datadir);
        let unix_socket_dir = tempdir()?; // We need a directory with a short name for Unix socket (up to 108 symbols)
        let unix_socket_dir_path = unix_socket_dir.path().to_owned();
        let server_process = self
            .new_pg_command("postgres")?
            .args(["-c", "listen_addresses="])
            .arg("-k")
            .arg(&unix_socket_dir_path)
            .arg("-D")
            .arg(&self.datadir)
            .args(REQUIRED_POSTGRES_CONFIG.iter().flat_map(|cfg| ["-c", cfg]))
            .spawn()?;
        let server = PostgresServer {
            process: server_process,
            _unix_socket_dir: unix_socket_dir,
            client_config: {
                let mut c = postgres::Config::new();
                c.host_path(&unix_socket_dir_path);
                c.user("postgres");
                c.connect_timeout(Duration::from_millis(10000));
                c
            },
        };
        Ok(server)
    }

    pub fn pg_waldump(
        &self,
        first_segment_name: &OsStr,
        last_segment_name: &OsStr,
    ) -> anyhow::Result<std::process::Output> {
        let first_segment_file = self.datadir.join(first_segment_name);
        let last_segment_file = self.datadir.join(last_segment_name);
        info!(
            "Running pg_waldump for {} .. {}",
            first_segment_file.display(),
            last_segment_file.display()
        );
        let output = self
            .new_pg_command("pg_waldump")?
            .args([&first_segment_file, &last_segment_file])
            .output()?;
        debug!("waldump output: {:?}", output);
        Ok(output)
    }
}

impl PostgresServer {
    pub fn connect_with_timeout(&self) -> anyhow::Result<Client> {
        let retry_until = Instant::now() + *self.client_config.get_connect_timeout().unwrap();
        while Instant::now() < retry_until {
            if let Ok(client) = self.client_config.connect(postgres::NoTls) {
                return Ok(client);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        bail!("Connection timed out");
    }

    pub fn kill(mut self) {
        self.process.kill().unwrap();
        self.process.wait().unwrap();
    }
}

impl Drop for PostgresServer {
    fn drop(&mut self) {
        match self.process.try_wait() {
            Ok(Some(_)) => return,
            Ok(None) => {
                warn!("Server was not terminated, will be killed");
            }
            Err(e) => {
                error!("Unable to get status of the server: {}, will be killed", e);
            }
        }
        let _ = self.process.kill();
    }
}

pub trait PostgresClientExt: postgres::GenericClient {
    fn pg_current_wal_insert_lsn(&mut self) -> anyhow::Result<PgLsn> {
        Ok(self
            .query_one("SELECT pg_current_wal_insert_lsn()", &[])?
            .get(0))
    }
    fn pg_current_wal_flush_lsn(&mut self) -> anyhow::Result<PgLsn> {
        Ok(self
            .query_one("SELECT pg_current_wal_flush_lsn()", &[])?
            .get(0))
    }
}

impl<C: postgres::GenericClient> PostgresClientExt for C {}

pub fn ensure_server_config(client: &mut impl postgres::GenericClient) -> anyhow::Result<()> {
    client.execute("create extension if not exists neon_test_utils", &[])?;

    let wal_keep_size: String = client.query_one("SHOW wal_keep_size", &[])?.get(0);
    ensure!(wal_keep_size == "50MB");
    let wal_writer_delay: String = client.query_one("SHOW wal_writer_delay", &[])?.get(0);
    ensure!(wal_writer_delay == "10s");
    let autovacuum: String = client.query_one("SHOW autovacuum", &[])?.get(0);
    ensure!(autovacuum == "off");

    let wal_segment_size = client.query_one(
        "select cast(setting as bigint) as setting, unit \
         from pg_settings where name = 'wal_segment_size'",
        &[],
    )?;
    ensure!(
        wal_segment_size.get::<_, String>("unit") == "B",
        "Unexpected wal_segment_size unit"
    );
    ensure!(
        wal_segment_size.get::<_, i64>("setting") == WAL_SEGMENT_SIZE as i64,
        "Unexpected wal_segment_size in bytes"
    );

    Ok(())
}

pub trait Crafter {
    const NAME: &'static str;

    /// Generates WAL using the client `client`. Returns a vector of some valid
    /// "interesting" intermediate LSNs which one may start reading from.
    /// test_end_of_wal uses this to check various starting points.
    ///
    /// Note that postgres is generally keen about writing some WAL. While we
    /// try to disable it (autovacuum, big wal_writer_delay, etc) it is always
    /// possible, e.g. xl_running_xacts are dumped each 15s. So checks about
    /// stable WAL end would be flaky unless postgres is shut down. For this
    /// reason returning potential end of WAL here is pointless. Most of the
    /// time this doesn't happen though, so it is reasonable to create needed
    /// WAL structure and immediately kill postgres like test_end_of_wal does.
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<Vec<PgLsn>>;
}

/// Wraps some WAL craft function, providing current LSN to it before the
/// insertion and flushing WAL afterwards. Also pushes initial LSN to the
/// result.
fn craft_internal<C: postgres::GenericClient>(
    client: &mut C,
    f: impl Fn(&mut C, PgLsn) -> anyhow::Result<Vec<PgLsn>>,
) -> anyhow::Result<Vec<PgLsn>> {
    ensure_server_config(client)?;

    let initial_lsn = client.pg_current_wal_insert_lsn()?;
    info!("LSN initial = {}", initial_lsn);

    let mut intermediate_lsns = f(client, initial_lsn)?;
    if !intermediate_lsns.starts_with(&[initial_lsn]) {
        intermediate_lsns.insert(0, initial_lsn);
    }

    // Some records may be not flushed, e.g. non-transactional logical messages. Flush now.
    //
    // If the previous WAL record ended exactly at page boundary, pg_current_wal_insert_lsn
    // returns the position just after the page header on the next page. That's where the next
    // record will be inserted. But the page header hasn't actually been written to the WAL
    // yet, and if you try to flush it, you get a "request to flush past end of generated WAL"
    // error. Because of that, if the insert location is just after a page header, back off to
    // previous page boundary.
    let mut lsn = u64::from(client.pg_current_wal_insert_lsn()?);
    if lsn % WAL_SEGMENT_SIZE as u64 == XLOG_SIZE_OF_XLOG_LONG_PHD as u64 {
        lsn -= XLOG_SIZE_OF_XLOG_LONG_PHD as u64;
    } else if lsn % XLOG_BLCKSZ as u64 == XLOG_SIZE_OF_XLOG_SHORT_PHD as u64 {
        lsn -= XLOG_SIZE_OF_XLOG_SHORT_PHD as u64;
    }
    client.execute("select neon_xlogflush($1)", &[&PgLsn::from(lsn)])?;
    Ok(intermediate_lsns)
}

pub struct Simple;
impl Crafter for Simple {
    const NAME: &'static str = "simple";
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<Vec<PgLsn>> {
        craft_internal(client, |client, _| {
            client.execute("CREATE table t(x int)", &[])?;
            Ok(Vec::new())
        })
    }
}

pub struct LastWalRecordXlogSwitch;
impl Crafter for LastWalRecordXlogSwitch {
    const NAME: &'static str = "last_wal_record_xlog_switch";
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<Vec<PgLsn>> {
        // Do not use craft_internal because here we end up with flush_lsn exactly on
        // the segment boundary and insert_lsn after the initial page header, which is unusual.
        ensure_server_config(client)?;

        client.execute("CREATE table t(x int)", &[])?;
        let before_xlog_switch = client.pg_current_wal_insert_lsn()?;
        // pg_switch_wal returns end of last record of the switched segment,
        // i.e. end of SWITCH itself.
        let xlog_switch_record_end: PgLsn = client.query_one("SELECT pg_switch_wal()", &[])?.get(0);
        let before_xlog_switch_u64 = u64::from(before_xlog_switch);
        let next_segment = PgLsn::from(
            before_xlog_switch_u64 - (before_xlog_switch_u64 % WAL_SEGMENT_SIZE as u64)
                + WAL_SEGMENT_SIZE as u64,
        );
        ensure!(
            xlog_switch_record_end <= next_segment,
            "XLOG_SWITCH record ended after the expected segment boundary: {} > {}",
            xlog_switch_record_end,
            next_segment
        );
        Ok(vec![before_xlog_switch, xlog_switch_record_end])
    }
}

pub struct LastWalRecordXlogSwitchEndsOnPageBoundary;
/// Craft xlog SWITCH record ending at page boundary.
impl Crafter for LastWalRecordXlogSwitchEndsOnPageBoundary {
    const NAME: &'static str = "last_wal_record_xlog_switch_ends_on_page_boundary";
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<Vec<PgLsn>> {
        // Do not use generate_internal because here we end up with flush_lsn exactly on
        // the segment boundary and insert_lsn after the initial page header, which is unusual.
        ensure_server_config(client)?;

        client.execute("CREATE table t(x int)", &[])?;

        // Add padding so the XLOG_SWITCH record ends exactly on XLOG_BLCKSZ boundary.  We
        // will use carefully-sized logical messages to advance WAL insert location such
        // that there is just enough space on the page for the XLOG_SWITCH record.
        loop {
            // We start with measuring how much WAL it takes for one logical message,
            // considering all alignments and headers.
            let before_lsn = client.pg_current_wal_insert_lsn()?;
            client.execute(
                "SELECT pg_logical_emit_message(false, 'swch', REPEAT('a', 10))",
                &[],
            )?;
            let after_lsn = client.pg_current_wal_insert_lsn()?;

            // Did the record cross a page boundary? If it did, start over. Crossing a
            // page boundary adds to the apparent size of the record because of the page
            // header, which throws off the calculation.
            if u64::from(before_lsn) / XLOG_BLCKSZ as u64
                != u64::from(after_lsn) / XLOG_BLCKSZ as u64
            {
                continue;
            }
            // base_size is the size of a logical message without the payload
            let base_size = u64::from(after_lsn) - u64::from(before_lsn) - 10;

            // Is there enough space on the page for another logical message and an
            // XLOG_SWITCH? If not, start over.
            let page_remain = XLOG_BLCKSZ as u64 - u64::from(after_lsn) % XLOG_BLCKSZ as u64;
            if page_remain < base_size + XLOG_SIZE_OF_XLOG_RECORD as u64 {
                continue;
            }

            // We will write another logical message, such that after the logical message
            // record, there will be space for exactly one XLOG_SWITCH. How large should
            // the logical message's payload be? An XLOG_SWITCH record has no data => its
            // size is exactly XLOG_SIZE_OF_XLOG_RECORD.
            let repeats = page_remain - base_size - XLOG_SIZE_OF_XLOG_RECORD as u64;

            client.execute(
                "SELECT pg_logical_emit_message(false, 'swch', REPEAT('a', $1))",
                &[&(repeats as i32)],
            )?;
            info!(
                "current_wal_insert_lsn={}, XLOG_SIZE_OF_XLOG_RECORD={}",
                client.pg_current_wal_insert_lsn()?,
                XLOG_SIZE_OF_XLOG_RECORD
            );

            // Emit the XLOG_SWITCH
            let before_xlog_switch = client.pg_current_wal_insert_lsn()?;
            let xlog_switch_record_end: PgLsn =
                client.query_one("SELECT pg_switch_wal()", &[])?.get(0);

            if u64::from(xlog_switch_record_end) as usize % XLOG_BLCKSZ
                != XLOG_SIZE_OF_XLOG_SHORT_PHD
            {
                warn!(
                    "XLOG_SWITCH message ended not on page boundary: {}, offset = {}, repeating",
                    xlog_switch_record_end,
                    u64::from(xlog_switch_record_end) as usize % XLOG_BLCKSZ
                );
                continue;
            }
            return Ok(vec![before_xlog_switch, xlog_switch_record_end]);
        }
    }
}

/// Write ~16MB logical message; it should cross WAL segment.
fn craft_seg_size_logical_message(
    client: &mut impl postgres::GenericClient,
    transactional: bool,
) -> anyhow::Result<Vec<PgLsn>> {
    craft_internal(client, |client, initial_lsn| {
        ensure!(
            initial_lsn < PgLsn::from(0x0200_0000 - 1024 * 1024),
            "Initial LSN is too far in the future"
        );

        let message_lsn: PgLsn = client
            .query_one(
                "select pg_logical_emit_message($1, 'big-16mb-msg', \
                 concat(repeat('abcd', 16 * 256 * 1024), 'end')) as message_lsn",
                &[&transactional],
            )?
            .get("message_lsn");
        ensure!(
            message_lsn > PgLsn::from(0x0200_0000 + 4 * 8192),
            "Logical message did not cross the segment boundary"
        );
        ensure!(
            message_lsn < PgLsn::from(0x0400_0000),
            "Logical message crossed two segments"
        );

        Ok(vec![message_lsn])
    })
}

pub struct WalRecordCrossingSegmentFollowedBySmallOne;
impl Crafter for WalRecordCrossingSegmentFollowedBySmallOne {
    const NAME: &'static str = "wal_record_crossing_segment_followed_by_small_one";
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<Vec<PgLsn>> {
        // Transactional message crossing WAL segment will be followed by small
        // commit record.
        craft_seg_size_logical_message(client, true)
    }
}

pub struct LastWalRecordCrossingSegment;
impl Crafter for LastWalRecordCrossingSegment {
    const NAME: &'static str = "last_wal_record_crossing_segment";
    fn craft(client: &mut impl postgres::GenericClient) -> anyhow::Result<Vec<PgLsn>> {
        craft_seg_size_logical_message(client, false)
    }
}

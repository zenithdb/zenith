use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::path::Path;

use anyhow::Result;

use crate::pg_helpers::escape_conf_value;
use crate::pg_helpers::{GenericOptionExt, PgOptionsSerialize};
use compute_api::spec::{ComputeMode, ComputeSpec, GenericOption};

/// Check that `line` is inside a text file and put it there if it is not.
/// Create file if it doesn't exist.
pub fn line_in_file(path: &Path, line: &str) -> Result<bool> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .append(false)
        .truncate(false)
        .open(path)?;
    let buf = io::BufReader::new(&file);
    let mut count: usize = 0;

    for l in buf.lines() {
        if l? == line {
            return Ok(false);
        }
        count = 1;
    }

    write!(file, "{}{}", "\n".repeat(count), line)?;
    Ok(true)
}

/// Create or completely rewrite configuration file specified by `path`
pub fn write_postgres_conf(
    path: &Path,
    spec: &ComputeSpec,
    extension_server_port: Option<u16>,
) -> Result<()> {
    // File::create() destroys the file content if it exists.
    let mut file = File::create(path)?;

    // Write the postgresql.conf content from the spec file as is.
    if let Some(conf) = &spec.cluster.postgresql_conf {
        writeln!(file, "{}", conf)?;
    }

    // Add options for connecting to storage
    writeln!(file, "# Neon storage settings")?;
    if let Some(s) = &spec.pageserver_connstring {
        writeln!(file, "neon.pageserver_connstring={}", escape_conf_value(s))?;
    }
    if let Some(stripe_size) = spec.shard_stripe_size {
        writeln!(file, "neon.stripe_size={stripe_size}")?;
    }
    if !spec.safekeeper_connstrings.is_empty() {
        writeln!(
            file,
            "neon.safekeepers={}",
            escape_conf_value(&spec.safekeeper_connstrings.join(","))
        )?;
    }
    if let Some(s) = &spec.tenant_id {
        writeln!(file, "neon.tenant_id={}", escape_conf_value(&s.to_string()))?;
    }
    if let Some(s) = &spec.timeline_id {
        writeln!(
            file,
            "neon.timeline_id={}",
            escape_conf_value(&s.to_string())
        )?;
    }

    // Locales
    if cfg!(target_os = "macos") {
        writeln!(file, "lc_messages='C'")?;
        writeln!(file, "lc_monetary='C'")?;
        writeln!(file, "lc_time='C'")?;
        writeln!(file, "lc_numeric='C'")?;
    } else {
        writeln!(file, "lc_messages='C.UTF-8'")?;
        writeln!(file, "lc_monetary='C.UTF-8'")?;
        writeln!(file, "lc_time='C.UTF-8'")?;
        writeln!(file, "lc_numeric='C.UTF-8'")?;
    }

    match spec.mode {
        ComputeMode::Primary => {}
        ComputeMode::Static(lsn) => {
            // hot_standby is 'on' by default, but let's be explicit
            writeln!(file, "hot_standby=on")?;
            writeln!(file, "recovery_target_lsn='{lsn}'")?;
        }
        ComputeMode::Replica => {
            // hot_standby is 'on' by default, but let's be explicit
            writeln!(file, "hot_standby=on")?;
        }
    }

    if cfg!(target_os = "linux") {
        // Check /proc/sys/vm/overcommit_memory -- if it equals 2 (i.e. linux memory overcommit is
        // disabled), then the control plane has enabled swap and we should set
        // dynamic_shared_memory_type = 'mmap'.
        //
        // This is (maybe?) temporary - for more, see https://github.com/neondatabase/cloud/issues/12047.
        let overcommit_memory_contents = std::fs::read_to_string("/proc/sys/vm/overcommit_memory")
            // ignore any errors - they may be expected to occur under certain situations (e.g. when
            // not running in Linux).
            .unwrap_or_else(|_| String::new());
        if overcommit_memory_contents.trim() == "2" {
            let opt = GenericOption {
                name: "dynamic_shared_memory_type".to_owned(),
                value: Some("mmap".to_owned()),
                vartype: "enum".to_owned(),
            };

            write!(file, "{}", opt.to_pg_setting())?;
        }
    }

    // If there are any extra options in the 'settings' field, append those
    if spec.cluster.settings.is_some() {
        writeln!(file, "# Managed by compute_ctl: begin")?;
        write!(file, "{}", spec.cluster.settings.as_pg_settings())?;
        writeln!(file, "# Managed by compute_ctl: end")?;
    }

    if let Some(port) = extension_server_port {
        writeln!(file, "neon.extension_server_port={}", port)?;
    }

    // This is essential to keep this line at the end of the file,
    // because it is intended to override any settings above.
    writeln!(file, "include_if_exists = 'compute_ctl_temp_override.conf'")?;

    Ok(())
}

pub fn with_compute_ctl_tmp_override<F>(pgdata_path: &Path, options: &str, exec: F) -> Result<()>
where
    F: FnOnce() -> Result<()>,
{
    let path = pgdata_path.join("compute_ctl_temp_override.conf");
    let mut file = File::create(path)?;
    write!(file, "{}", options)?;

    let res = exec();

    file.set_len(0)?;

    res
}

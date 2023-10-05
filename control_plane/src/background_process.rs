//! Spawns and kills background processes that are needed by Neon CLI.
//! Applies common set-up such as log and pid files (if needed) to every process.
//!
//! Neon CLI does not run in background, so it needs to store the information about
//! spawned processes, which it does in this module.
//! We do that by storing the pid of the process in the "${process_name}.pid" file.
//! The pid file can be created by the process itself
//! (Neon storage binaries do that and also ensure that a lock is taken onto that file)
//! or we create such file after starting the process
//! (non-Neon binaries don't necessarily follow our pidfile conventions).
//! The pid stored in the file is later used to stop the service.
//!
//! See the [`lock_file`](utils::lock_file) module for more info.

use std::ffi::OsStr;
use std::io::Write;
use std::os::unix::prelude::AsRawFd;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command};
use std::time::Duration;
use std::{fs, io, thread};

use anyhow::Context;
use camino::{Utf8Path, Utf8PathBuf};
use nix::errno::Errno;
use nix::fcntl::{FcntlArg, FdFlag};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use utils::pid_file::{self, PidFileRead};

// These constants control the loop used to poll for process start / stop.
//
// The loop waits for at most 10 seconds, polling every 100 ms.
// Once a second, it prints a dot ("."), to give the user an indication that
// it's waiting. If the process hasn't started/stopped after 5 seconds,
// it prints a notice that it's taking long, but keeps waiting.
//
const RETRY_UNTIL_SECS: u64 = 10;
const RETRIES: u64 = (RETRY_UNTIL_SECS * 1000) / RETRY_INTERVAL_MILLIS;
const RETRY_INTERVAL_MILLIS: u64 = 100;
const DOT_EVERY_RETRIES: u64 = 10;
const NOTICE_AFTER_RETRIES: u64 = 50;

/// Argument to `start_process`, to indicate whether it should create pidfile or if the process creates
/// it itself.
pub enum InitialPidFile<'t> {
    /// Create a pidfile, to allow future CLI invocations to manipulate the process.
    Create(&'t Utf8Path),
    /// The process will create the pidfile itself, need to wait for that event.
    Expect(&'t Utf8Path),
}

/// Start a background child process using the parameters given.
pub fn start_process<F, AI, A, EI>(
    process_name: &str,
    datadir: &Path,
    command: &Path,
    args: AI,
    envs: EI,
    initial_pid_file: InitialPidFile,
    process_status_check: F,
) -> anyhow::Result<Child>
where
    F: Fn() -> anyhow::Result<bool>,
    AI: IntoIterator<Item = A>,
    A: AsRef<OsStr>,
    // Not generic AsRef<OsStr>, otherwise empty `envs` prevents type inference
    EI: IntoIterator<Item = (String, String)>,
{
    let log_path = datadir.join(format!("{process_name}.log"));
    let process_log_file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .append(true)
        .open(&log_path)
        .with_context(|| {
            format!("Could not open {process_name} log file {log_path:?} for writing")
        })?;
    let same_file_for_stderr = process_log_file.try_clone().with_context(|| {
        format!("Could not reuse {process_name} log file {log_path:?} for writing stderr")
    })?;

    let mut command = Command::new(command);
    let background_command = command
        .stdout(process_log_file)
        .stderr(same_file_for_stderr)
        .args(args);
    let filled_cmd = fill_aws_secrets_vars(fill_rust_env_vars(background_command));
    filled_cmd.envs(envs);

    let pid_file_to_check = match initial_pid_file {
        InitialPidFile::Create(path) => {
            pre_exec_create_pidfile(filled_cmd, path);
            path
        }
        InitialPidFile::Expect(path) => path,
    };

    let mut spawned_process = filled_cmd.spawn().with_context(|| {
        format!("Could not spawn {process_name}, see console output and log files for details.")
    })?;
    let pid = spawned_process.id();
    let pid = Pid::from_raw(
        i32::try_from(pid)
            .with_context(|| format!("Subprocess {process_name} has invalid pid {pid}"))?,
    );

    for retries in 0..RETRIES {
        match process_started(pid, Some(pid_file_to_check), &process_status_check) {
            Ok(true) => {
                println!("\n{process_name} started, pid: {pid}");
                return Ok(spawned_process);
            }
            Ok(false) => {
                if retries == NOTICE_AFTER_RETRIES {
                    // The process is taking a long time to start up. Keep waiting, but
                    // print a message
                    print!("\n{process_name} has not started yet, continuing to wait");
                }
                if retries % DOT_EVERY_RETRIES == 0 {
                    print!(".");
                    io::stdout().flush().unwrap();
                }
                thread::sleep(Duration::from_millis(RETRY_INTERVAL_MILLIS));
            }
            Err(e) => {
                println!("{process_name} failed to start: {e:#}");
                if let Err(e) = spawned_process.kill() {
                    println!("Could not stop {process_name} subprocess: {e:#}")
                };
                return Err(e);
            }
        }
    }
    println!();
    anyhow::bail!("{process_name} did not start in {RETRY_UNTIL_SECS} seconds");
}

/// Stops the process, using the pid file given. Returns Ok also if the process is already not running.
pub fn stop_process(
    immediate: bool,
    process_name: &str,
    pid_file: &Utf8Path,
) -> anyhow::Result<()> {
    let pid = match pid_file::read(pid_file)
        .with_context(|| format!("read pid_file {pid_file:?}"))?
    {
        PidFileRead::NotExist => {
            println!("{process_name} is already stopped: no pid file present at {pid_file:?}");
            return Ok(());
        }
        PidFileRead::NotHeldByAnyProcess(_) => {
            // Don't try to kill according to file contents because the pid might have been reused by another process.
            // Don't delete the file either, it can race with new pid file creation.
            // Read `pid_file` module comment for details.
            println!(
                "No process is holding the pidfile. The process must have already exited. Leave in place to avoid race conditions: {pid_file:?}"
            );
            return Ok(());
        }
        PidFileRead::LockedByOtherProcess(pid) => pid,
    };
    // XXX the pid could become invalid (and recycled) at any time before the kill() below.

    // send signal
    let sig = if immediate {
        print!("Stopping {process_name} with pid {pid} immediately..");
        Signal::SIGQUIT
    } else {
        print!("Stopping {process_name} with pid {pid} gracefully..");
        Signal::SIGTERM
    };
    io::stdout().flush().unwrap();
    match kill(pid, sig) {
        Ok(()) => (),
        Err(Errno::ESRCH) => {
            // Again, don't delete the pid file. The unlink can race with a new pid file being created.
            println!(
                "{process_name} with pid {pid} does not exist, but a pid file {pid_file:?} was found. Likely the pid got recycled. Lucky we didn't harm anyone."
            );
            return Ok(());
        }
        Err(e) => anyhow::bail!("Failed to send signal to {process_name} with pid {pid}: {e}"),
    }

    // Wait until process is gone
    wait_until_stopped(process_name, pid)?;
    Ok(())
}

pub fn wait_until_stopped(process_name: &str, pid: Pid) -> anyhow::Result<()> {
    for retries in 0..RETRIES {
        match process_has_stopped(pid) {
            Ok(true) => {
                println!("\n{process_name} stopped");
                return Ok(());
            }
            Ok(false) => {
                if retries == NOTICE_AFTER_RETRIES {
                    // The process is taking a long time to start up. Keep waiting, but
                    // print a message
                    print!("\n{process_name} has not stopped yet, continuing to wait");
                }
                if retries % DOT_EVERY_RETRIES == 0 {
                    print!(".");
                    io::stdout().flush().unwrap();
                }
                thread::sleep(Duration::from_millis(RETRY_INTERVAL_MILLIS));
            }
            Err(e) => {
                println!("{process_name} with pid {pid} failed to stop: {e:#}");
                return Err(e);
            }
        }
    }
    println!();
    anyhow::bail!("{process_name} with pid {pid} did not stop in {RETRY_UNTIL_SECS} seconds");
}

fn fill_rust_env_vars(cmd: &mut Command) -> &mut Command {
    // If RUST_BACKTRACE is set, pass it through. But if it's not set, default
    // to RUST_BACKTRACE=1.
    let backtrace_setting = std::env::var_os("RUST_BACKTRACE");
    let backtrace_setting = backtrace_setting
        .as_deref()
        .unwrap_or_else(|| OsStr::new("1"));

    let mut filled_cmd = cmd.env_clear().env("RUST_BACKTRACE", backtrace_setting);

    // Pass through these environment variables to the command
    for var in ["LLVM_PROFILE_FILE", "FAILPOINTS", "RUST_LOG"] {
        if let Some(val) = std::env::var_os(var) {
            filled_cmd = filled_cmd.env(var, val);
        }
    }

    filled_cmd
}

fn fill_aws_secrets_vars(mut cmd: &mut Command) -> &mut Command {
    for env_key in [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ] {
        if let Ok(value) = std::env::var(env_key) {
            cmd = cmd.env(env_key, value);
        }
    }
    cmd
}

/// Add a `pre_exec` to the cmd that, inbetween fork() and exec(),
/// 1. Claims a pidfile with a fcntl lock on it and
/// 2. Sets up the pidfile's file descriptor so that it (and the lock)
///    will remain held until the cmd exits.
fn pre_exec_create_pidfile<P>(cmd: &mut Command, path: P) -> &mut Command
where
    P: Into<Utf8PathBuf>,
{
    let path: Utf8PathBuf = path.into();
    // SAFETY
    // pre_exec is marked unsafe because it runs between fork and exec.
    // Why is that dangerous in various ways?
    // Long answer:  https://github.com/rust-lang/rust/issues/39575
    // Short answer: in a multi-threaded program, other threads may have
    // been inside of critical sections at the time of fork. In the
    // original process, that was allright, assuming they protected
    // the critical sections appropriately, e.g., through locks.
    // Fork adds another process to the mix that
    //   1. Has a single thread T
    //   2. In an exact copy of the address space at the time of fork.
    // A variety of problems scan occur now:
    //   1. T tries to grab a lock that was locked at the time of fork.
    //      It will wait forever since in its address space, the lock
    //      is in state 'taken' but the thread that would unlock it is
    //      not there.
    //   2. A rust object that represented some external resource in the
    //      parent now got implicitly copied by the the fork, even though
    //      the object's type is not `Copy`. The parent program may use
    //      non-copyability as way to enforce unique ownership of an
    //      external resource in the typesystem. The fork breaks that
    //      assumption, as now both parent and child process have an
    //      owned instance of the object that represents the same
    //      underlying resource.
    // While these seem like niche problems, (1) in particular is
    // highly relevant. For example, `malloc()` may grab a mutex internally,
    // and so, if we forked while another thread was mallocing' and our
    // pre_exec closure allocates as well, it will block on the malloc
    // mutex forever
    //
    // The proper solution is to only use C library functions that are marked
    // "async-signal-safe": https://man7.org/linux/man-pages/man7/signal-safety.7.html
    //
    // With this specific pre_exec() closure, the non-error path doesn't allocate.
    // The error path uses `anyhow`, and hence does allocate.
    // We take our chances there, hoping that any potential disaster is constrained
    // to the child process (e.g., malloc has no state ourside of the child process).
    // Last, `expect` prints to stderr, and stdio is not async-signal-safe.
    // Again, we take our chances, making the same assumptions as for malloc.
    unsafe {
        cmd.pre_exec(move || {
            let file = pid_file::claim_for_current_process(&path).expect("claim pid file");
            // Remove the FD_CLOEXEC flag on the pidfile descriptor so that the pidfile
            // remains locked after exec.
            nix::fcntl::fcntl(file.as_raw_fd(), FcntlArg::F_SETFD(FdFlag::empty()))
                .expect("remove FD_CLOEXEC");
            // Don't run drop(file), it would close the file before we actually exec.
            std::mem::forget(file);
            Ok(())
        });
    }
    cmd
}

fn process_started<F>(
    pid: Pid,
    pid_file_to_check: Option<&Utf8Path>,
    status_check: &F,
) -> anyhow::Result<bool>
where
    F: Fn() -> anyhow::Result<bool>,
{
    match status_check() {
        Ok(true) => match pid_file_to_check {
            Some(pid_file_path) => match pid_file::read(pid_file_path)? {
                PidFileRead::NotExist => Ok(false),
                PidFileRead::LockedByOtherProcess(pid_in_file) => Ok(pid_in_file == pid),
                PidFileRead::NotHeldByAnyProcess(_) => Ok(false),
            },
            None => Ok(true),
        },
        Ok(false) => Ok(false),
        Err(e) => anyhow::bail!("process failed to start: {e}"),
    }
}

fn process_has_stopped(pid: Pid) -> anyhow::Result<bool> {
    match kill(pid, None) {
        // Process exists, keep waiting
        Ok(_) => Ok(false),
        // Process not found, we're done
        Err(Errno::ESRCH) => Ok(true),
        Err(err) => anyhow::bail!("Failed to send signal to process with pid {pid}: {err}"),
    }
}

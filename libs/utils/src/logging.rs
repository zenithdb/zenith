#[cfg(feature = "tracing-based-debugging")]
use std::{
    io::BufWriter,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
};

use std::str::FromStr;

use anyhow::Context;
use metrics::{IntCounter, IntCounterVec};
use once_cell::sync::Lazy;
use strum_macros::{EnumString, VariantNames};

#[derive(EnumString, strum_macros::Display, VariantNames, Eq, PartialEq, Debug, Clone, Copy)]
#[strum(serialize_all = "snake_case")]
pub enum LogFormat {
    Plain,
    Json,
    Test,
}

impl LogFormat {
    pub fn from_config(s: &str) -> anyhow::Result<LogFormat> {
        use strum::VariantNames;
        LogFormat::from_str(s).with_context(|| {
            format!(
                "Unrecognized log format. Please specify one of: {:?}",
                LogFormat::VARIANTS
            )
        })
    }
}

struct TracingEventCountMetric {
    error: IntCounter,
    warn: IntCounter,
    info: IntCounter,
    debug: IntCounter,
    trace: IntCounter,
}

static TRACING_EVENT_COUNT_METRIC: Lazy<TracingEventCountMetric> = Lazy::new(|| {
    let vec = metrics::register_int_counter_vec!(
        "libmetrics_tracing_event_count",
        "Number of tracing events, by level",
        &["level"]
    )
    .expect("failed to define metric");
    TracingEventCountMetric::new(vec)
});

impl TracingEventCountMetric {
    fn new(vec: IntCounterVec) -> Self {
        Self {
            error: vec.with_label_values(&["error"]),
            warn: vec.with_label_values(&["warn"]),
            info: vec.with_label_values(&["info"]),
            debug: vec.with_label_values(&["debug"]),
            trace: vec.with_label_values(&["trace"]),
        }
    }

    fn inc_for_level(&self, level: tracing::Level) {
        let counter = match level {
            tracing::Level::ERROR => &self.error,
            tracing::Level::WARN => &self.warn,
            tracing::Level::INFO => &self.info,
            tracing::Level::DEBUG => &self.debug,
            tracing::Level::TRACE => &self.trace,
        };
        counter.inc();
    }
}

struct TracingEventCountLayer(&'static TracingEventCountMetric);

impl<S> tracing_subscriber::layer::Layer<S> for TracingEventCountLayer
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.0.inc_for_level(*event.metadata().level());
    }
}

/// Whether to add the `tracing_error` crate's `ErrorLayer`
/// to the global tracing subscriber.
///
pub enum TracingErrorLayerEnablement {
    /// Do not add the `ErrorLayer`.
    Disabled,
    /// Add the `ErrorLayer` with the filter specified by RUST_LOG, defaulting to `info` if `RUST_LOG` is unset.
    EnableWithRustLogFilter,
}

/// Where the logging should output to.
#[derive(Clone, Copy)]
pub enum Output {
    Stdout,
    Stderr,
}

/// Keep alive and drop it before the program terminates.
#[allow(dead_code)] // We need to store the `Arc<>` for drop semantics.
#[must_use]
#[cfg(feature = "tracing-based-debugging")]
pub struct FlushGuard(Arc<Mutex<FlushGuardInner>>);

#[cfg(feature = "tracing-based-debugging")]
struct FlushGuardInner {
    _tracing_chrome_layer: Option<tracing_chrome::FlushGuard>,
    _tracing_flame_layer: Option<tracing_flame::FlushGuard<BufWriter<std::fs::File>>>,
}

#[cfg(not(feature = "tracing-based-debugging"))]
pub struct FlushGuard;

/// Initialize the global tracing subscriber.
///
/// # Tracing-Based Debugging
///
/// If feature `tracing-based-debugging` is enabled, this function will add support
/// for runtime enablement of various tracing-based debugging tools.
///
/// The feature is disabled by default to avoid compile time bloat.
///
/// For example, to use the `tracing-chrome` crate to debug pageserver:
///
/// 1. Enable the feature by adding `tracing-based-debugging` to the `features` list in
///    the pageserver crate's `Cargo.toml`.
/// 2. Build pageserver.
/// 3. Launch pageserver with env var `NEON_UTILS_LOGGING_ENABLE_TRACING_CHROME=1`.
/// 4. Cleanly shut down pageserver.
/// 5. Follow instructions of the `tracing-chrome` crate to post-process and visualize
///    the trace files.
pub fn init(
    log_format: LogFormat,
    tracing_error_layer_enablement: TracingErrorLayerEnablement,
    output: Output,
) -> anyhow::Result<FlushGuard> {
    // We fall back to printing all spans at info-level or above if
    // the RUST_LOG environment variable is not set.
    let rust_log_env_filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };

    // NB: the order of the with() calls does not matter.
    // See https://docs.rs/tracing-subscriber/0.3.16/tracing_subscriber/layer/index.html#per-layer-filtering
    use tracing_subscriber::prelude::*;

    let r = tracing_subscriber::registry();

    let r = r.with({
        let log_layer = tracing_subscriber::fmt::layer()
            .with_target(false)
            .with_ansi(false)
            .with_writer(move || -> Box<dyn std::io::Write> {
                match output {
                    Output::Stdout => Box::new(std::io::stdout()),
                    Output::Stderr => Box::new(std::io::stderr()),
                }
            });
        let log_layer = match log_format {
            LogFormat::Json => log_layer.json().boxed(),
            LogFormat::Plain => log_layer.boxed(),
            LogFormat::Test => log_layer.with_test_writer().boxed(),
        };
        log_layer.with_filter(rust_log_env_filter())
    });

    let r = r.with(
        TracingEventCountLayer(&TRACING_EVENT_COUNT_METRIC).with_filter(rust_log_env_filter()),
    );

    let r = r.with(match tracing_error_layer_enablement {
        TracingErrorLayerEnablement::EnableWithRustLogFilter => {
            Some(tracing_error::ErrorLayer::default().with_filter(rust_log_env_filter()))
        }
        TracingErrorLayerEnablement::Disabled => None,
    });

    #[cfg(feature = "tracing-based-debugging")]
    let (r, guard) = {
        let tracing_chrome_layer_flush_guard;
        let r = r.with(
            if crate::env::var("NEON_UTILS_LOGGING_ENABLE_TRACING_CHROME").unwrap_or(false) {
                let (layer, guard) = tracing_chrome::ChromeLayerBuilder::new()
                    .trace_style(tracing_chrome::TraceStyle::Async)
                    .build();
                tracing_chrome_layer_flush_guard = Some(guard);
                Some(layer.with_filter(rust_log_env_filter()))
            } else {
                tracing_chrome_layer_flush_guard = None;
                None
            },
        );

        let tracing_flame_flush_guard;
        let r = r.with(
            if crate::env::var("NEON_UTILS_LOGGING_ENABLE_TRACING_FLAME").unwrap_or(false) {
                let (layer, guard) =
                    tracing_flame::FlameLayer::with_file("./tracing.folded").unwrap();
                let layer = layer
                    .with_empty_samples(false)
                    .with_module_path(false)
                    .with_file_and_line(false)
                    .with_threads_collapsed(true);
                tracing_flame_flush_guard = Some(guard);
                Some(layer.with_filter(rust_log_env_filter()))
            } else {
                tracing_flame_flush_guard = None;
                None
            },
        );

        let r = {
            let varname = "NEON_UTILS_LOGGING_ENABLE_TOKIO_CONSOLE";
            let console_subscriber_config: Option<NonZeroUsize> = crate::env::var(varname);
            #[cfg(tokio_unstable)]
            {
                r.with(match console_subscriber_config {
                    Some(n) => {
                        use console_subscriber::ConsoleLayer;
                        Some(
                            console_subscriber::Builder::default()
                                .event_buffer_capacity(
                                    n.get() * ConsoleLayer::DEFAULT_EVENT_BUFFER_CAPACITY,
                                )
                                .client_buffer_capacity(
                                    n.get() * ConsoleLayer::DEFAULT_CLIENT_BUFFER_CAPACITY,
                                )
                                .spawn(),
                        )
                    }
                    None => None,
                })
            }
            #[cfg(not(tokio_unstable))]
            if console_subscriber_config.is_some() {
                panic!("recompile with --cfg tokio_unstable to enable {varname}");
            } else {
                r
            }
        };
        (
            r,
            FlushGuard(Arc::new(Mutex::new(FlushGuardInner {
                _tracing_chrome_layer: tracing_chrome_layer_flush_guard,
                _tracing_flame_layer: tracing_flame_flush_guard,
            }))),
        )
    };
    #[cfg(not(feature = "tracing-based-debugging"))]
    let (r, guard) = (r, FlushGuard);

    r.init();

    Ok(guard)
}

/// Disable the default rust panic hook by using `set_hook`.
///
/// For neon binaries, the assumption is that tracing is configured before with [`init`], after
/// that sentry is configured (if needed). sentry will install it's own on top of this, always
/// processing the panic before we log it.
///
/// When the return value is dropped, the hook is reverted to std default hook (prints to stderr).
/// If the assumptions about the initialization order are not held, use
/// [`TracingPanicHookGuard::forget`] but keep in mind, if tracing is stopped, then panics will be
/// lost.
#[must_use]
pub fn replace_panic_hook_with_tracing_panic_hook() -> TracingPanicHookGuard {
    std::panic::set_hook(Box::new(tracing_panic_hook));
    TracingPanicHookGuard::new()
}

/// Drop guard which restores the std panic hook on drop.
///
/// Tracing should not be used when it's not configured, but we cannot really latch on to any
/// imaginary lifetime of tracing.
pub struct TracingPanicHookGuard {
    act: bool,
}

impl TracingPanicHookGuard {
    fn new() -> Self {
        TracingPanicHookGuard { act: true }
    }

    /// Make this hook guard not do anything when dropped.
    pub fn forget(&mut self) {
        self.act = false;
    }
}

impl Drop for TracingPanicHookGuard {
    fn drop(&mut self) {
        if self.act {
            let _ = std::panic::take_hook();
        }
    }
}

/// Named symbol for our panic hook, which logs the panic.
fn tracing_panic_hook(info: &std::panic::PanicHookInfo) {
    // following rust 1.66.1 std implementation:
    // https://github.com/rust-lang/rust/blob/90743e7298aca107ddaa0c202a4d3604e29bfeb6/library/std/src/panicking.rs#L235-L288
    let location = info.location();

    let msg = match info.payload().downcast_ref::<&'static str>() {
        Some(s) => *s,
        None => match info.payload().downcast_ref::<String>() {
            Some(s) => &s[..],
            None => "Box<dyn Any>",
        },
    };

    let thread = std::thread::current();
    let thread = thread.name().unwrap_or("<unnamed>");
    let backtrace = std::backtrace::Backtrace::capture();

    let _entered = if let Some(location) = location {
        tracing::error_span!("panic", %thread, location = %PrettyLocation(location))
    } else {
        // very unlikely to hit here, but the guarantees of std could change
        tracing::error_span!("panic", %thread)
    }
    .entered();

    if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
        // this has an annoying extra '\n' in the end which anyhow doesn't do, but we cannot really
        // get rid of it as we cannot get in between of std::fmt::Formatter<'_>; we could format to
        // string, maybe even to a TLS one but tracing already does that.
        tracing::error!("{msg}\n\nStack backtrace:\n{backtrace}");
    } else {
        tracing::error!("{msg}");
    }

    // ensure that we log something on the panic if this hook is left after tracing has been
    // unconfigured. worst case when teardown is racing the panic is to log the panic twice.
    tracing::dispatcher::get_default(|d| {
        if let Some(_none) = d.downcast_ref::<tracing::subscriber::NoSubscriber>() {
            let location = location.map(PrettyLocation);
            log_panic_to_stderr(thread, msg, location, &backtrace);
        }
    });
}

#[cold]
fn log_panic_to_stderr(
    thread: &str,
    msg: &str,
    location: Option<PrettyLocation<'_, '_>>,
    backtrace: &std::backtrace::Backtrace,
) {
    eprintln!("panic while tracing is unconfigured: thread '{thread}' panicked at '{msg}', {location:?}\nStack backtrace:\n{backtrace}");
}

struct PrettyLocation<'a, 'b>(&'a std::panic::Location<'b>);

impl std::fmt::Display for PrettyLocation<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:{}", self.0.file(), self.0.line(), self.0.column())
    }
}

impl std::fmt::Debug for PrettyLocation<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        <Self as std::fmt::Display>::fmt(self, f)
    }
}

/// When you will store a secret but want to make sure it won't
/// be accidentally logged, wrap it in a SecretString, whose Debug
/// implementation does not expose the contents.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretString(String);

impl SecretString {
    pub fn get_contents(&self) -> &str {
        self.0.as_str()
    }
}

impl From<String> for SecretString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl FromStr for SecretString {
    type Err = std::convert::Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self(s.to_string()))
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[SECRET]")
    }
}

#[cfg(test)]
mod tests {
    use metrics::{core::Opts, IntCounterVec};

    use crate::logging::{TracingEventCountLayer, TracingEventCountMetric};

    #[test]
    fn tracing_event_count_metric() {
        let counter_vec =
            IntCounterVec::new(Opts::new("testmetric", "testhelp"), &["level"]).unwrap();
        let metric = Box::leak(Box::new(TracingEventCountMetric::new(counter_vec.clone())));
        let layer = TracingEventCountLayer(metric);
        use tracing_subscriber::prelude::*;

        tracing::subscriber::with_default(tracing_subscriber::registry().with(layer), || {
            tracing::trace!("foo");
            tracing::debug!("foo");
            tracing::info!("foo");
            tracing::warn!("foo");
            tracing::error!("foo");
        });

        assert_eq!(counter_vec.with_label_values(&["trace"]).get(), 1);
        assert_eq!(counter_vec.with_label_values(&["debug"]).get(), 1);
        assert_eq!(counter_vec.with_label_values(&["info"]).get(), 1);
        assert_eq!(counter_vec.with_label_values(&["warn"]).get(), 1);
        assert_eq!(counter_vec.with_label_values(&["error"]).get(), 1);
    }
}

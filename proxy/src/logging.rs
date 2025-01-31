use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::{env, io};

use chrono::{DateTime, Utc};
use opentelemetry::trace::TraceContextExt;
use serde::ser::{SerializeMap, Serializer};
use serde::Serialize;
use tracing::subscriber::Interest;
use tracing::{callsite, Event, Level, Metadata, Span, Subscriber};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{EnvFilter, LevelFilter};
use tracing_subscriber::fmt::format::{Format, Full};
use tracing_subscriber::fmt::time::SystemTime;
use tracing_subscriber::fmt::{FormatEvent, FormatFields};
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::LookupSpan;

/// Initialize logging and OpenTelemetry tracing and exporter.
///
/// Logging can be configured using `RUST_LOG` environment variable.
///
/// OpenTelemetry is configured with OTLP/HTTP exporter. It picks up
/// configuration from environment variables. For example, to change the
/// destination, set `OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318`.
/// See <https://opentelemetry.io/docs/reference/specification/sdk-environment-variables>
pub async fn init() -> anyhow::Result<LoggingGuard> {
    let logfmt = LogFormat::from_env()?;

    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy()
        .add_directive(
            "aws_config=info"
                .parse()
                .expect("this should be a valid filter directive"),
        )
        .add_directive(
            "azure_core::policies::transport=off"
                .parse()
                .expect("this should be a valid filter directive"),
        );

    let otlp_layer = tracing_utils::init_tracing("proxy").await;

    let json_log_layer = if logfmt == LogFormat::Json {
        Some(JsonLoggingLayer {
            clock: RealClock,
            skipped_field_index: Arc::default(),
            writer: StderrWriter {
                stderr: std::io::stderr(),
            },
        })
    } else {
        None
    };

    let text_log_layer = if logfmt == LogFormat::Text {
        Some(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(std::io::stderr)
                .with_target(false),
        )
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(otlp_layer)
        .with(json_log_layer)
        .with(text_log_layer)
        .try_init()?;

    Ok(LoggingGuard)
}

/// Initialize logging for local_proxy with log prefix and no opentelemetry.
///
/// Logging can be configured using `RUST_LOG` environment variable.
pub fn init_local_proxy() -> anyhow::Result<LoggingGuard> {
    let env_filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .from_env_lossy();

    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .event_format(LocalProxyFormatter(Format::default().with_target(false)));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .try_init()?;

    Ok(LoggingGuard)
}

pub struct LocalProxyFormatter(Format<Full, SystemTime>);

impl<S, N> FormatEvent<S, N> for LocalProxyFormatter
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
{
    fn format_event(
        &self,
        ctx: &tracing_subscriber::fmt::FmtContext<'_, S, N>,
        mut writer: tracing_subscriber::fmt::format::Writer<'_>,
        event: &tracing::Event<'_>,
    ) -> std::fmt::Result {
        writer.write_str("[local_proxy] ")?;
        self.0.format_event(ctx, writer, event)
    }
}

// TODO: make JSON the default
#[derive(Copy, Clone, PartialEq, Eq, Default, Debug)]
enum LogFormat {
    #[default]
    Text = 1,
    Json,
}

impl LogFormat {
    fn from_env() -> anyhow::Result<Self> {
        let logfmt = env::var("LOGFMT");
        Ok(match logfmt.as_deref() {
            Err(_) => LogFormat::default(),
            Ok("text") => LogFormat::Text,
            Ok("json") => LogFormat::Json,
            Ok(logfmt) => anyhow::bail!("unknown log format: {logfmt}"),
        })
    }
}

pub struct LoggingGuard;

impl Drop for LoggingGuard {
    fn drop(&mut self) {
        // Shutdown trace pipeline gracefully, so that it has a chance to send any
        // pending traces before we exit.
        tracing::info!("shutting down the tracing machinery");
        tracing_utils::shutdown_tracing();
    }
}

pub trait MakeWriter {
    fn make(&self) -> impl io::Write;
}

struct StderrWriter {
    stderr: io::Stderr,
}

impl MakeWriter for StderrWriter {
    #[inline]
    fn make(&self) -> impl io::Write {
        self.stderr.lock()
    }
}

// TODO: move into separate module or even separate crate.
pub trait Clock {
    fn now(&self) -> DateTime<Utc>;
}

struct RealClock;

impl Clock for RealClock {
    #[inline]
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

pub struct JsonLoggingLayer<C: Clock, W: MakeWriter> {
    clock: C,
    skipped_field_index: Arc<RwLock<HashMap<callsite::Identifier, Vec<usize>>>>,
    writer: W,
}

impl<S, C: Clock + 'static, W: MakeWriter + 'static> Layer<S> for JsonLoggingLayer<C, W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        use std::io::Write;

        // TODO: consider special tracing subscriber to grab timestamp very
        //       early, before OTel machinery, and add as event extension.
        let now = self.clock.now();

        let res: io::Result<()> = EVENT_FORMATTER.with_borrow_mut(move |formatter| {
            formatter.reset();
            formatter.format(
                now,
                event,
                &ctx,
                &self.skipped_field_index.read().expect("poisoned"),
            )?;

            self.writer.make().write_all(&formatter.logline_buffer)
        });

        // TODO: maybe simplify this
        if let Err(err) = res {
            if let Ok(mut line) = serde_json::to_vec(&serde_json::json!( {
                "timestamp": now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "level": "ERROR",
                "message": format_args!("cannot log event: {err:?}"),
                "fields": {
                    "event": format_args!("{event:?}"),
                },
            })) {
                line.push(b'\n');
                self.writer.make().write_all(&line).ok();
            }
        }
    }

    fn register_callsite(&self, metadata: &'static Metadata<'static>) -> Interest {
        if !metadata.is_event() {
            // Must not be never because we wouldn't get trace and span data.
            return Interest::always();
        }

        let mut skipped_field_indices = self.skipped_field_index.write().expect("poisoned");

        let mut seen_fields = HashMap::<&'static str, usize>::new();
        for field in metadata.fields() {
            use std::collections::hash_map::Entry;
            match seen_fields.entry(field.name()) {
                Entry::Vacant(entry) => {
                    // field not seen yet
                    entry.insert(field.index());
                }
                Entry::Occupied(mut entry) => {
                    // replace currently stored index
                    let old_index = entry.insert(field.index());
                    // ... and append it to list of skippable indices
                    match skipped_field_indices.entry(metadata.callsite()) {
                        Entry::Vacant(entry) => {
                            entry.insert(vec![old_index]);
                        }
                        Entry::Occupied(mut entry) => {
                            entry.get_mut().push(old_index);
                        }
                    }
                }
            }
        }

        Interest::always()
    }
}

thread_local! {
    static EVENT_FORMATTER: RefCell<EventFormatter> = RefCell::new(EventFormatter::new());
    static THREAD_ID: Cell<u64> = Cell::new(gettid::gettid());
}

// TODO: buffer capacity management
struct EventFormatter {
    logline_buffer: Vec<u8>,
    field_tracker: HashSet<u32>,
}

impl EventFormatter {
    #[inline]
    fn new() -> Self {
        EventFormatter {
            logline_buffer: Vec::new(),
            field_tracker: HashSet::new(),
        }
    }

    #[inline]
    fn reset(&mut self) {
        self.logline_buffer.clear();
        self.field_tracker.clear();
    }

    fn format<S>(
        &mut self,
        now: DateTime<Utc>,
        event: &Event<'_>,
        ctx: &Context<'_, S>,
        skipped_field_indices: &HashMap<callsite::Identifier, Vec<usize>>,
    ) -> io::Result<()>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
    {
        let timestamp = now.to_rfc3339_opts(chrono::SecondsFormat::Micros, true);

        // TODO: translate "log." fields and ignore.
        // log.target, log.module_path, log.file, log.line

        let meta = event.metadata();

        let skipped_field_indices = skipped_field_indices
            .get(&meta.callsite())
            .map(Vec::as_slice)
            .unwrap_or_default();

        let mut serialize = || {
            let mut serializer = serde_json::Serializer::new(&mut self.logline_buffer);

            let mut serializer = serializer.serialize_map(None)?;

            // Timestamp comes first, so raw lines can be sorted by timestamp.
            serializer.serialize_entry("timestamp", &timestamp)?;

            // Level next.
            serializer.serialize_entry("level", &SerializeLevel(meta.level()))?;

            // Message next.
            serializer.serialize_key("message")?;
            let mut message_extractor = MessageExtractor::new(serializer, skipped_field_indices);
            event.record(&mut message_extractor);
            let mut serializer = message_extractor.into_serializer()?;

            let mut fields_present = FieldsPresent(false, skipped_field_indices);
            event.record(&mut fields_present);
            if fields_present.0 {
                serializer.serialize_entry(
                    "fields",
                    &SerializableEventFields(event, skipped_field_indices),
                )?;
            }

            let pid = std::process::id();
            if pid != 1 {
                serializer.serialize_entry("process_id", &pid)?;
            }

            serializer.serialize_entry("thread_id", &THREAD_ID.get())?;

            // TODO: tls cache? name could change
            if let Some(thread_name) = std::thread::current().name() {
                if !thread_name.is_empty() && thread_name != "tokio-runtime-worker" {
                    serializer.serialize_entry("thread_name", thread_name)?;
                }
            }

            if let Some(task_id) = tokio::task::try_id() {
                serializer.serialize_entry("task_id", &format_args!("{task_id}"))?;
            }

            serializer.serialize_entry("target", meta.target())?;

            if let Some(module) = meta.module_path() {
                if module != meta.target() {
                    serializer.serialize_entry("module", module)?;
                }
            }

            if let Some(file) = meta.file() {
                if let Some(line) = meta.line() {
                    serializer.serialize_entry("src", &format_args!("{file}:{line}"))?;
                } else {
                    serializer.serialize_entry("src", file)?;
                }
            }

            {
                let otel_context = Span::current().context();
                let otel_spanref = otel_context.span();
                let span_context = otel_spanref.span_context();
                if span_context.is_valid() {
                    serializer.serialize_entry(
                        "trace_id",
                        &format_args!("{}", span_context.trace_id()),
                    )?;
                    serializer
                        .serialize_entry("span_id", &format_args!("{}", span_context.span_id()))?;
                }
            }

            let _ = ctx;
            // TODO: spans + span fields

            serializer.end()
        };

        serialize().map_err(io::Error::other)?;
        self.logline_buffer.push(b'\n');
        Ok(())
    }
}

struct SerializeLevel<'a>(&'a Level);

impl Serialize for SerializeLevel<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // This order was chosen to match log level probabilities.
        if self.0 == &Level::INFO {
            serializer.serialize_str("INFO")
        } else if self.0 == &Level::WARN {
            serializer.serialize_str("WARN")
        } else if self.0 == &Level::ERROR {
            serializer.serialize_str("ERROR")
        } else if self.0 == &Level::DEBUG {
            serializer.serialize_str("DEBUG")
        } else if self.0 == &Level::TRACE {
            serializer.serialize_str("TRACE")
        } else {
            unreachable!("unknown log level: {}", self.0)
        }
    }
}

/// Name of the field used by tracing crate to store the event message.
const MESSAGE_FIELD: &str = "message";

pub struct MessageExtractor<'a, S: serde::ser::SerializeMap> {
    serializer: S,
    skipped_field_indices: &'a [usize],
    state: Option<Result<(), S::Error>>,
}

impl<'a, S: serde::ser::SerializeMap> MessageExtractor<'a, S> {
    #[inline]
    fn new(serializer: S, skipped_field_indices: &'a [usize]) -> Self {
        Self {
            serializer,
            skipped_field_indices,
            state: None,
        }
    }

    #[inline]
    fn into_serializer(mut self) -> Result<S, S::Error> {
        match self.state {
            Some(Ok(())) => {}
            Some(Err(err)) => return Err(err),
            None => self.serializer.serialize_value("")?,
        }
        Ok(self.serializer)
    }

    #[inline]
    fn accept_field(&self, field: &tracing::field::Field) -> bool {
        self.state.is_none()
            && field.name() == MESSAGE_FIELD
            && !self.skipped_field_indices.contains(&field.index())
    }
}

impl<S: serde::ser::SerializeMap> tracing::field::Visit for MessageExtractor<'_, S> {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&format_args!("{value:x?}")));
        }
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&value));
        }
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&format_args!("{value:?}")));
        }
    }

    #[inline]
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        if self.accept_field(field) {
            self.state = Some(self.serializer.serialize_value(&format_args!("{value}")));
        }
    }
}

struct FieldsPresent<'a>(pub bool, &'a [usize]);

impl tracing::field::Visit for FieldsPresent<'_> {
    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, _: &dyn std::fmt::Debug) {
        if !self.1.contains(&field.index()) && field.name() != MESSAGE_FIELD {
            self.0 |= true;
        }
    }
}

struct SerializableEventFields<'a, 'event>(&'a tracing::Event<'event>, &'a [usize]);

impl serde::ser::Serialize for SerializableEventFields<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;
        let serializer = serializer.serialize_map(None)?;
        let mut message_skipper = MessageSkipper::new(serializer, self.1);
        self.0.record(&mut message_skipper);
        let serializer = message_skipper.into_serializer()?;
        serializer.end()
    }
}

pub struct MessageSkipper<'a, S: serde::ser::SerializeMap> {
    serializer: S,
    skipped_field_indices: &'a [usize],
    state: Result<(), S::Error>,
}

impl<'a, S: serde::ser::SerializeMap> MessageSkipper<'a, S> {
    #[inline]
    fn new(serializer: S, skipped_field_indices: &'a [usize]) -> Self {
        Self {
            serializer,
            skipped_field_indices,
            state: Ok(()),
        }
    }

    #[inline]
    fn accept_field(&self, field: &tracing::field::Field) -> bool {
        self.state.is_ok()
            && field.name() != MESSAGE_FIELD
            && !self.skipped_field_indices.contains(&field.index())
    }

    #[inline]
    fn into_serializer(self) -> Result<S, S::Error> {
        self.state?;
        Ok(self.serializer)
    }
}

impl<S: serde::ser::SerializeMap> tracing::field::Visit for MessageSkipper<'_, S> {
    #[inline]
    fn record_f64(&mut self, field: &tracing::field::Field, value: f64) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_i128(&mut self, field: &tracing::field::Field, value: i128) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_u128(&mut self, field: &tracing::field::Field, value: u128) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_bytes(&mut self, field: &tracing::field::Field, value: &[u8]) {
        if self.accept_field(field) {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &format_args!("{value:x?}"));
        }
    }

    #[inline]
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_entry(field.name(), &value);
        }
    }

    #[inline]
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if self.accept_field(field) {
            self.state = self
                .serializer
                .serialize_entry(field.name(), &format_args!("{value:?}"));
        }
    }

    #[inline]
    fn record_error(
        &mut self,
        field: &tracing::field::Field,
        value: &(dyn std::error::Error + 'static),
    ) {
        if self.accept_field(field) {
            self.state = self.serializer.serialize_value(&format_args!("{value}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};

    use assert_json_diff::assert_json_eq;
    use tracing::info_span;

    use super::*;

    struct TestClock {
        current_time: Mutex<DateTime<Utc>>,
    }

    impl Clock for Arc<TestClock> {
        fn now(&self) -> DateTime<Utc> {
            *self.current_time.lock().expect("poisoned")
        }
    }

    struct VecWriter<'a> {
        buffer: MutexGuard<'a, Vec<u8>>,
    }

    impl MakeWriter for Arc<Mutex<Vec<u8>>> {
        fn make(&self) -> impl io::Write {
            VecWriter {
                buffer: self.lock().expect("poisoned"),
            }
        }
    }

    impl io::Write for VecWriter<'_> {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.buffer.write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_field_collection() {
        let clock = Arc::new(TestClock {
            current_time: Mutex::new(Utc::now()),
        });
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let log_layer = JsonLoggingLayer {
            clock: clock.clone(),
            skipped_field_index: Arc::default(),
            writer: buffer.clone(),
        };

        let registry = tracing_subscriber::Registry::default().with(log_layer);

        tracing::subscriber::with_default(registry, || {
            let _span = info_span!("test_unset_fields").entered();
            tracing::info!(
                a = 1,
                a = 2,
                a = 3,
                message = "explicit message field",
                "implicit message field"
            );
        });

        let buffer = Arc::try_unwrap(buffer)
            .expect("no other reference")
            .into_inner()
            .expect("poisoned");
        let actual: serde_json::Value = serde_json::from_slice(&buffer).expect("valid JSON");
        let expected: serde_json::Value = serde_json::json!(
            {
                "timestamp": clock.now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                "level": "INFO",
                "message": "explicit message field",
                "fields": {
                    "a": 3
                },
                "src": actual.as_object().unwrap().get("src").unwrap().as_str().unwrap(),
                "target": "proxy::logging::tests",
                "process_id": actual.as_object().unwrap().get("process_id").unwrap().as_number().unwrap(),
                "thread_id": actual.as_object().unwrap().get("thread_id").unwrap().as_number().unwrap(),
                "thread_name": "logging::tests::test_field_collection",
            }
        );

        assert_json_eq!(actual, expected);
    }
}

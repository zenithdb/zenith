//! Helper functions to set up OpenTelemetry tracing.
//!
//! This comes in two variants, depending on whether you have a Tokio runtime available.
//! If you do, call `init_tracing()`. It sets up the trace processor and exporter to use
//! the current tokio runtime. If you don't have a runtime available, or you don't want
//! to share the runtime with the tracing tasks, call `init_tracing_without_runtime()`
//! instead. It sets up a dedicated single-threaded Tokio runtime for the tracing tasks.
//!
//! Example:
//!
//! ```rust,no_run
//! use tracing_subscriber::prelude::*;
//!
//! #[tokio::main]
//! async fn main() {
//!     // Set up logging to stderr
//!     let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
//!         .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
//!     let fmt_layer = tracing_subscriber::fmt::layer()
//!         .with_target(false)
//!         .with_writer(std::io::stderr);
//!
//!     // Initialize OpenTelemetry. Exports tracing spans as OpenTelemetry traces
//!     let otlp_layer = tracing_utils::init_tracing("my_application").await;
//!
//!     // Put it all together
//!     tracing_subscriber::registry()
//!         .with(env_filter)
//!         .with(otlp_layer)
//!         .with(fmt_layer)
//!         .init();
//! }
//! ```
#![deny(unsafe_code)]
#![deny(clippy::undocumented_unsafe_blocks)]

pub mod http;

use opentelemetry::trace::TracerProvider;
use opentelemetry::KeyValue;
use tracing::Subscriber;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// Set up OpenTelemetry exporter, using configuration from environment variables.
///
/// `service_name` is set as the OpenTelemetry 'service.name' resource (see
/// <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/resource/semantic_conventions/README.md#service>)
///
/// We try to follow the conventions for the environment variables specified in
/// <https://opentelemetry.io/docs/reference/specification/sdk-environment-variables/>
///
/// However, we only support a subset of those options:
///
/// - OTEL_SDK_DISABLED is supported. The default is "false", meaning tracing
///   is enabled by default. Set it to "true" to disable.
///
/// - We use the OTLP exporter, with HTTP protocol. Most of the OTEL_EXPORTER_OTLP_*
///   settings specified in
///   <https://opentelemetry.io/docs/reference/specification/protocol/exporter/>
///   are supported, as they are handled by the `opentelemetry-otlp` crate.
///   Settings related to other exporters have no effect.
///
/// - Some other settings are supported by the `opentelemetry` crate.
///
/// If you need some other setting, please test if it works first. And perhaps
/// add a comment in the list above to save the effort of testing for the next
/// person.
///
/// This doesn't block, but is marked as 'async' to hint that this must be called in
/// asynchronous execution context.
pub async fn init_tracing<S>(service_name: &str) -> Option<impl Layer<S>>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    if std::env::var("OTEL_SDK_DISABLED") == Ok("true".to_string()) {
        return None;
    };
    Some(init_tracing_internal(service_name.to_string()))
}

/// Like `init_tracing`, but creates a separate tokio Runtime for the tracing
/// tasks.
pub fn init_tracing_without_runtime<S>(service_name: &str) -> Option<impl Layer<S>>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    if std::env::var("OTEL_SDK_DISABLED") == Ok("true".to_string()) {
        return None;
    };

    // The opentelemetry batch processor and the OTLP exporter needs a Tokio
    // runtime. Create a dedicated runtime for them. One thread should be
    // enough.
    //
    // (Alternatively, instead of batching, we could use the "simple
    // processor", which doesn't need Tokio, and use "reqwest-blocking"
    // feature for the OTLP exporter, which also doesn't need Tokio.  However,
    // batching is considered best practice, and also I have the feeling that
    // the non-Tokio codepaths in the opentelemetry crate are less used and
    // might be more buggy, so better to stay on the well-beaten path.)
    //
    // We leak the runtime so that it keeps running after we exit the
    // function.
    let runtime = Box::leak(Box::new(
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("otlp runtime thread")
            .worker_threads(1)
            .build()
            .unwrap(),
    ));
    let _guard = runtime.enter();

    Some(init_tracing_internal(service_name.to_string()))
}

fn init_tracing_internal<S>(service_name: String) -> impl Layer<S>
where
    S: Subscriber + for<'span> LookupSpan<'span>,
{
    // Sets up exporter from the OTEL_EXPORTER_* environment variables.
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect("could not initialize opentelemetry exporter");

    // TODO: opentelemetry::global::set_error_handler() with custom handler that
    //       bypasses default tracing layers, but logs regular looking log
    //       messages.

    // Propagate trace information in the standard W3C TraceContext format.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let tracer = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .with_resource(opentelemetry_sdk::Resource::new(vec![KeyValue::new(
            opentelemetry_semantic_conventions::resource::SERVICE_NAME,
            service_name,
        )]))
        .build()
        .tracer("global");

    tracing_opentelemetry::layer().with_tracer(tracer)
}

// Shutdown trace pipeline gracefully, so that it has a chance to send any
// pending traces before we exit.
pub fn shutdown_tracing() {
    opentelemetry::global::shutdown_tracer_provider();
}

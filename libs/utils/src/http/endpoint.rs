use crate::auth::{AuthError, Claims, SwappableJwtAuth};
use crate::http::error::{api_error_handler, route_error_handler, ApiError};
use crate::http::request::{get_query_param, parse_query_param};
use crate::pprof;
use ::pprof::protos::Message as _;
use ::pprof::ProfilerGuardBuilder;
use anyhow::{anyhow, Context};
use bytes::{Bytes, BytesMut};
use hyper::header::{HeaderName, AUTHORIZATION, CONTENT_DISPOSITION};
use hyper::http::HeaderValue;
use hyper::Method;
use hyper::{header::CONTENT_TYPE, Body, Request, Response};
use metrics::{register_int_counter, Encoder, IntCounter, TextEncoder};
use once_cell::sync::Lazy;
use regex::Regex;
use routerify::ext::RequestExt;
use routerify::{Middleware, RequestInfo, Router, RouterBuilder};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::io::ReaderStream;
use tracing::{debug, info, info_span, warn, Instrument};

use std::future::Future;
use std::io::Write as _;
use std::str::FromStr;
use std::time::Duration;

static SERVE_METRICS_COUNT: Lazy<IntCounter> = Lazy::new(|| {
    register_int_counter!(
        "libmetrics_metric_handler_requests_total",
        "Number of metric requests made"
    )
    .expect("failed to define a metric")
});

static X_REQUEST_ID_HEADER_STR: &str = "x-request-id";

static X_REQUEST_ID_HEADER: HeaderName = HeaderName::from_static(X_REQUEST_ID_HEADER_STR);
#[derive(Debug, Default, Clone)]
struct RequestId(String);

/// Adds a tracing info_span! instrumentation around the handler events,
/// logs the request start and end events for non-GET requests and non-200 responses.
///
/// Usage: Replace `my_handler` with `|r| request_span(r, my_handler)`
///
/// Use this to distinguish between logs of different HTTP requests: every request handler wrapped
/// with this will get request info logged in the wrapping span, including the unique request ID.
///
/// This also handles errors, logging them and converting them to an HTTP error response.
///
/// NB: If the client disconnects, Hyper will drop the Future, without polling it to
/// completion. In other words, the handler must be async cancellation safe! request_span
/// prints a warning to the log when that happens, so that you have some trace of it in
/// the log.
///
///
/// There could be other ways to implement similar functionality:
///
/// * procmacros placed on top of all handler methods
///   With all the drawbacks of procmacros, brings no difference implementation-wise,
///   and little code reduction compared to the existing approach.
///
/// * Another `TraitExt` with e.g. the `get_with_span`, `post_with_span` methods to do similar logic,
///   implemented for [`RouterBuilder`].
///   Could be simpler, but we don't want to depend on [`routerify`] more, targeting to use other library later.
///
/// * In theory, a span guard could've been created in a pre-request middleware and placed into a global collection, to be dropped
///   later, in a post-response middleware.
///   Due to suspendable nature of the futures, would give contradictive results which is exactly the opposite of what `tracing-futures`
///   tries to achive with its `.instrument` used in the current approach.
///
/// If needed, a declarative macro to substitute the |r| ... closure boilerplate could be introduced.
pub async fn request_span<R, H>(request: Request<Body>, handler: H) -> R::Output
where
    R: Future<Output = Result<Response<Body>, ApiError>> + Send + 'static,
    H: FnOnce(Request<Body>) -> R + Send + Sync + 'static,
{
    let request_id = request.context::<RequestId>().unwrap_or_default().0;
    let method = request.method();
    let path = request.uri().path();
    let request_span = info_span!("request", %method, %path, %request_id);

    let log_quietly = method == Method::GET;
    async move {
        let cancellation_guard = RequestCancelled::warn_when_dropped_without_responding();
        if log_quietly {
            debug!("Handling request");
        } else {
            info!("Handling request");
        }

        // No special handling for panics here. There's a `tracing_panic_hook` from another
        // module to do that globally.
        let res = handler(request).await;

        cancellation_guard.disarm();

        // Log the result if needed.
        //
        // We also convert any errors into an Ok response with HTTP error code here.
        // `make_router` sets a last-resort error handler that would do the same, but
        // we prefer to do it here, before we exit the request span, so that the error
        // is still logged with the span.
        //
        // (Because we convert errors to Ok response, we never actually return an error,
        // and we could declare the function to return the never type (`!`). However,
        // using `routerify::RouterBuilder` requires a proper error type.)
        match res {
            Ok(response) => {
                let response_status = response.status();
                if log_quietly && response_status.is_success() {
                    debug!("Request handled, status: {response_status}");
                } else {
                    info!("Request handled, status: {response_status}");
                }
                Ok(response)
            }
            Err(err) => Ok(api_error_handler(err)),
        }
    }
    .instrument(request_span)
    .await
}

/// Drop guard to WARN in case the request was dropped before completion.
struct RequestCancelled {
    warn: Option<tracing::Span>,
}

impl RequestCancelled {
    /// Create the drop guard using the [`tracing::Span::current`] as the span.
    fn warn_when_dropped_without_responding() -> Self {
        RequestCancelled {
            warn: Some(tracing::Span::current()),
        }
    }

    /// Consume the drop guard without logging anything.
    fn disarm(mut self) {
        self.warn = None;
    }
}

impl Drop for RequestCancelled {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // we are unwinding due to panicking, assume we are not dropped for cancellation
        } else if let Some(span) = self.warn.take() {
            // the span has all of the info already, but the outer `.instrument(span)` has already
            // been dropped, so we need to manually re-enter it for this message.
            //
            // this is what the instrument would do before polling so it is fine.
            let _g = span.entered();
            warn!("request was dropped before completing");
        }
    }
}

/// An [`std::io::Write`] implementation on top of a channel sending [`bytes::Bytes`] chunks.
pub struct ChannelWriter {
    buffer: BytesMut,
    pub tx: mpsc::Sender<std::io::Result<Bytes>>,
    written: usize,
    /// Time spent waiting for the channel to make progress. It is not the same as time to upload a
    /// buffer because we cannot know anything about that, but this should allow us to understand
    /// the actual time taken without the time spent `std::thread::park`ed.
    wait_time: std::time::Duration,
}

impl ChannelWriter {
    pub fn new(buf_len: usize, tx: mpsc::Sender<std::io::Result<Bytes>>) -> Self {
        assert_ne!(buf_len, 0);
        ChannelWriter {
            // split about half off the buffer from the start, because we flush depending on
            // capacity. first flush will come sooner than without this, but now resizes will
            // have better chance of picking up the "other" half. not guaranteed of course.
            buffer: BytesMut::with_capacity(buf_len).split_off(buf_len / 2),
            tx,
            written: 0,
            wait_time: std::time::Duration::ZERO,
        }
    }

    pub fn flush0(&mut self) -> std::io::Result<usize> {
        let n = self.buffer.len();
        if n == 0 {
            return Ok(0);
        }

        tracing::trace!(n, "flushing");
        let ready = self.buffer.split().freeze();

        let wait_started_at = std::time::Instant::now();

        // not ideal to call from blocking code to block_on, but we are sure that this
        // operation does not spawn_blocking other tasks
        let res: Result<(), ()> = tokio::runtime::Handle::current().block_on(async {
            self.tx.send(Ok(ready)).await.map_err(|_| ())?;

            // throttle sending to allow reuse of our buffer in `write`.
            self.tx.reserve().await.map_err(|_| ())?;

            // now the response task has picked up the buffer and hopefully started
            // sending it to the client.
            Ok(())
        });

        self.wait_time += wait_started_at.elapsed();

        if res.is_err() {
            return Err(std::io::ErrorKind::BrokenPipe.into());
        }
        self.written += n;
        Ok(n)
    }

    pub fn flushed_bytes(&self) -> usize {
        self.written
    }

    pub fn wait_time(&self) -> std::time::Duration {
        self.wait_time
    }
}

impl std::io::Write for ChannelWriter {
    fn write(&mut self, mut buf: &[u8]) -> std::io::Result<usize> {
        let remaining = self.buffer.capacity() - self.buffer.len();

        let out_of_space = remaining < buf.len();

        let original_len = buf.len();

        if out_of_space {
            let can_still_fit = buf.len() - remaining;
            self.buffer.extend_from_slice(&buf[..can_still_fit]);
            buf = &buf[can_still_fit..];
            self.flush0()?;
        }

        // assume that this will often under normal operation just move the pointer back to the
        // beginning of allocation, because previous split off parts are already sent and
        // dropped.
        self.buffer.extend_from_slice(buf);
        Ok(original_len)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush0().map(|_| ())
    }
}

pub async fn prometheus_metrics_handler(_req: Request<Body>) -> Result<Response<Body>, ApiError> {
    SERVE_METRICS_COUNT.inc();

    let started_at = std::time::Instant::now();

    let (tx, rx) = mpsc::channel(1);

    let body = Body::wrap_stream(ReceiverStream::new(rx));

    let mut writer = ChannelWriter::new(128 * 1024, tx);

    let encoder = TextEncoder::new();

    let response = Response::builder()
        .status(200)
        .header(CONTENT_TYPE, encoder.format_type())
        .body(body)
        .unwrap();

    let span = info_span!("blocking");
    tokio::task::spawn_blocking(move || {
        // there are situations where we lose scraped metrics under load, try to gather some clues
        // since all nodes are queried this, keep the message count low.
        let spawned_at = std::time::Instant::now();

        let _span = span.entered();

        let metrics = metrics::gather();

        let gathered_at = std::time::Instant::now();

        let res = encoder
            .encode(&metrics, &mut writer)
            .and_then(|_| writer.flush().map_err(|e| e.into()));

        // this instant is not when we finally got the full response sent, sending is done by hyper
        // in another task.
        let encoded_at = std::time::Instant::now();

        let spawned_in = spawned_at - started_at;
        let collected_in = gathered_at - spawned_at;
        // remove the wait time here in case the tcp connection was clogged
        let encoded_in = encoded_at - gathered_at - writer.wait_time();
        let total = encoded_at - started_at;

        match res {
            Ok(()) => {
                tracing::info!(
                    bytes = writer.flushed_bytes(),
                    total_ms = total.as_millis(),
                    spawning_ms = spawned_in.as_millis(),
                    collection_ms = collected_in.as_millis(),
                    encoding_ms = encoded_in.as_millis(),
                    "responded /metrics"
                );
            }
            Err(e) => {
                // there is a chance that this error is not the BrokenPipe we generate in the writer
                // for "closed connection", but it is highly unlikely.
                tracing::warn!(
                    after_bytes = writer.flushed_bytes(),
                    total_ms = total.as_millis(),
                    spawning_ms = spawned_in.as_millis(),
                    collection_ms = collected_in.as_millis(),
                    encoding_ms = encoded_in.as_millis(),
                    "failed to write out /metrics response: {e:?}"
                );
                // semantics of this error are quite... unclear. we want to error the stream out to
                // abort the response to somehow notify the client that we failed.
                //
                // though, most likely the reason for failure is that the receiver is already gone.
                drop(
                    writer
                        .tx
                        .blocking_send(Err(std::io::ErrorKind::BrokenPipe.into())),
                );
            }
        }
    });

    Ok(response)
}

/// Generates CPU profiles.
pub async fn profile_cpu_handler(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    enum Format {
        Pprof,
        Svg,
    }

    // Parameters.
    let format = match get_query_param(&req, "format")?.as_deref() {
        None => Format::Pprof,
        Some("pprof") => Format::Pprof,
        Some("svg") => Format::Svg,
        Some(format) => return Err(ApiError::BadRequest(anyhow!("invalid format {format}"))),
    };
    let seconds = match parse_query_param(&req, "seconds")? {
        None => 5,
        Some(seconds @ 1..=30) => seconds,
        Some(_) => return Err(ApiError::BadRequest(anyhow!("duration must be 1-30 secs"))),
    };
    let frequency_hz = match parse_query_param(&req, "frequency")? {
        None => 99,
        Some(1001..) => return Err(ApiError::BadRequest(anyhow!("frequency must be <=1000 Hz"))),
        Some(frequency) => frequency,
    };

    // Only allow one profiler at a time.
    static PROFILE_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
    let _lock = PROFILE_LOCK
        .try_lock()
        .map_err(|_| ApiError::Conflict("profiler already running".into()))?;

    // Take the profile.
    let report = tokio::task::spawn_blocking(move || {
        let guard = ProfilerGuardBuilder::default()
            .frequency(frequency_hz)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()?;
        std::thread::sleep(Duration::from_secs(seconds));
        guard.report().build()
    })
    .await
    .map_err(|join_err| ApiError::InternalServerError(join_err.into()))?
    .map_err(|pprof_err| ApiError::InternalServerError(pprof_err.into()))?;

    // Return the report in the requested format.
    match format {
        Format::Pprof => {
            let mut body = Vec::new();
            report
                .pprof()
                .map_err(|err| ApiError::InternalServerError(err.into()))?
                .write_to_vec(&mut body)
                .map_err(|err| ApiError::InternalServerError(err.into()))?;

            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(CONTENT_DISPOSITION, "attachment; filename=\"profile.pb\"")
                .body(Body::from(body))
                .map_err(|err| ApiError::InternalServerError(err.into()))
        }

        Format::Svg => {
            let mut body = Vec::new();
            report
                .flamegraph(&mut body)
                .map_err(|err| ApiError::InternalServerError(err.into()))?;
            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, "image/svg+xml")
                .body(Body::from(body))
                .map_err(|err| ApiError::InternalServerError(err.into()))
        }
    }
}

/// Generates heap profiles.
///
/// This only works with jemalloc on Linux.
pub async fn profile_heap_handler(req: Request<Body>) -> Result<Response<Body>, ApiError> {
    enum Format {
        Jemalloc,
        Pprof,
        Svg,
    }

    // Parameters.
    let format = match get_query_param(&req, "format")?.as_deref() {
        None => Format::Pprof,
        Some("jemalloc") => Format::Jemalloc,
        Some("pprof") => Format::Pprof,
        Some("svg") => Format::Svg,
        Some(format) => return Err(ApiError::BadRequest(anyhow!("invalid format {format}"))),
    };

    // Functions and mappings to strip when symbolizing pprof profiles. If true,
    // also remove child frames.
    static STRIP_FUNCTIONS: Lazy<Vec<(Regex, bool)>> = Lazy::new(|| {
        vec![
            (Regex::new("^__rust").unwrap(), false),
            (Regex::new("^_start$").unwrap(), false),
            (Regex::new("^irallocx_prof").unwrap(), true),
            (Regex::new("^prof_alloc_prep").unwrap(), true),
            (Regex::new("^std::rt::lang_start").unwrap(), false),
            (Regex::new("^std::sys::backtrace::__rust").unwrap(), false),
        ]
    });
    const STRIP_MAPPINGS: &[&str] = &["libc", "libgcc", "pthread", "vdso"];

    // Obtain profiler handle.
    let mut prof_ctl = jemalloc_pprof::PROF_CTL
        .as_ref()
        .ok_or(ApiError::InternalServerError(anyhow!(
            "heap profiling not enabled"
        )))?
        .lock()
        .await;
    if !prof_ctl.activated() {
        return Err(ApiError::InternalServerError(anyhow!(
            "heap profiling not enabled"
        )));
    }

    // Take and return the profile.
    match format {
        Format::Jemalloc => {
            // NB: file is an open handle to a tempfile that's already deleted.
            let file = tokio::task::spawn_blocking(move || prof_ctl.dump())
                .await
                .map_err(|join_err| ApiError::InternalServerError(join_err.into()))?
                .map_err(ApiError::InternalServerError)?;
            let stream = ReaderStream::new(tokio::fs::File::from_std(file));
            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(CONTENT_DISPOSITION, "attachment; filename=\"heap.dump\"")
                .body(Body::wrap_stream(stream))
                .map_err(|err| ApiError::InternalServerError(err.into()))
        }

        Format::Pprof => {
            let data = tokio::task::spawn_blocking(move || {
                let bytes = prof_ctl.dump_pprof()?;
                // Symbolize the profile.
                // TODO: consider moving this upstream to jemalloc_pprof and avoiding the
                // serialization roundtrip.
                let profile = pprof::decode(&bytes)?;
                let profile = pprof::symbolize(profile)?;
                let profile = pprof::strip_locations(profile, STRIP_MAPPINGS, &STRIP_FUNCTIONS);
                pprof::encode(&profile)
            })
            .await
            .map_err(|join_err| ApiError::InternalServerError(join_err.into()))?
            .map_err(ApiError::InternalServerError)?;
            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, "application/octet-stream")
                .header(CONTENT_DISPOSITION, "attachment; filename=\"heap.pb\"")
                .body(Body::from(data))
                .map_err(|err| ApiError::InternalServerError(err.into()))
        }

        Format::Svg => {
            let body = tokio::task::spawn_blocking(move || {
                let bytes = prof_ctl.dump_pprof()?;
                let profile = pprof::decode(&bytes)?;
                let profile = pprof::symbolize(profile)?;
                let profile = pprof::strip_locations(profile, STRIP_MAPPINGS, &STRIP_FUNCTIONS);
                let mut opts = inferno::flamegraph::Options::default();
                opts.title = "Heap inuse".to_string();
                opts.count_name = "bytes".to_string();
                pprof::flamegraph(profile, &mut opts)
            })
            .await
            .map_err(|join_err| ApiError::InternalServerError(join_err.into()))?
            .map_err(ApiError::InternalServerError)?;
            Response::builder()
                .status(200)
                .header(CONTENT_TYPE, "image/svg+xml")
                .body(Body::from(body))
                .map_err(|err| ApiError::InternalServerError(err.into()))
        }
    }
}

pub fn add_request_id_middleware<B: hyper::body::HttpBody + Send + Sync + 'static>(
) -> Middleware<B, ApiError> {
    Middleware::pre(move |req| async move {
        let request_id = match req.headers().get(&X_REQUEST_ID_HEADER) {
            Some(request_id) => request_id
                .to_str()
                .expect("extract request id value")
                .to_owned(),
            None => {
                let request_id = uuid::Uuid::new_v4();
                request_id.to_string()
            }
        };
        req.set_context(RequestId(request_id));

        Ok(req)
    })
}

async fn add_request_id_header_to_response(
    mut res: Response<Body>,
    req_info: RequestInfo,
) -> Result<Response<Body>, ApiError> {
    if let Some(request_id) = req_info.context::<RequestId>() {
        if let Ok(request_header_value) = HeaderValue::from_str(&request_id.0) {
            res.headers_mut()
                .insert(&X_REQUEST_ID_HEADER, request_header_value);
        };
    };

    Ok(res)
}

pub fn make_router() -> RouterBuilder<hyper::Body, ApiError> {
    Router::builder()
        .middleware(add_request_id_middleware())
        .middleware(Middleware::post_with_info(
            add_request_id_header_to_response,
        ))
        .err_handler(route_error_handler)
}

pub fn attach_openapi_ui(
    router_builder: RouterBuilder<hyper::Body, ApiError>,
    spec: &'static [u8],
    spec_mount_path: &'static str,
    ui_mount_path: &'static str,
) -> RouterBuilder<hyper::Body, ApiError> {
    router_builder
        .get(spec_mount_path,
            move |r| request_span(r, move |_| async move {
                Ok(Response::builder().body(Body::from(spec)).unwrap())
            })
        )
        .get(ui_mount_path,
             move |r| request_span(r, move |_| async move {
                 Ok(Response::builder().body(Body::from(format!(r#"
                <!DOCTYPE html>
                <html lang="en">
                <head>
                <title>rweb</title>
                <link href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@3/swagger-ui.css" rel="stylesheet">
                </head>
                <body>
                    <div id="swagger-ui"></div>
                    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@3/swagger-ui-bundle.js" charset="UTF-8"> </script>
                    <script>
                        window.onload = function() {{
                        const ui = SwaggerUIBundle({{
                            "dom_id": "\#swagger-ui",
                            presets: [
                            SwaggerUIBundle.presets.apis,
                            SwaggerUIBundle.SwaggerUIStandalonePreset
                            ],
                            layout: "BaseLayout",
                            deepLinking: true,
                            showExtensions: true,
                            showCommonExtensions: true,
                            url: "{}",
                        }})
                        window.ui = ui;
                    }};
                </script>
                </body>
                </html>
            "#, spec_mount_path))).unwrap())
             })
        )
}

fn parse_token(header_value: &str) -> Result<&str, ApiError> {
    // header must be in form Bearer <token>
    let (prefix, token) = header_value
        .split_once(' ')
        .ok_or_else(|| ApiError::Unauthorized("malformed authorization header".to_string()))?;
    if prefix != "Bearer" {
        return Err(ApiError::Unauthorized(
            "malformed authorization header".to_string(),
        ));
    }
    Ok(token)
}

pub fn auth_middleware<B: hyper::body::HttpBody + Send + Sync + 'static>(
    provide_auth: fn(&Request<Body>) -> Option<&SwappableJwtAuth>,
) -> Middleware<B, ApiError> {
    Middleware::pre(move |req| async move {
        if let Some(auth) = provide_auth(&req) {
            match req.headers().get(AUTHORIZATION) {
                Some(value) => {
                    let header_value = value.to_str().map_err(|_| {
                        ApiError::Unauthorized("malformed authorization header".to_string())
                    })?;
                    let token = parse_token(header_value)?;

                    let data = auth.decode(token).map_err(|err| {
                        warn!("Authentication error: {err}");
                        // Rely on From<AuthError> for ApiError impl
                        err
                    })?;
                    req.set_context(data.claims);
                }
                None => {
                    return Err(ApiError::Unauthorized(
                        "missing authorization header".to_string(),
                    ))
                }
            }
        }
        Ok(req)
    })
}

pub fn add_response_header_middleware<B>(
    header: &str,
    value: &str,
) -> anyhow::Result<Middleware<B, ApiError>>
where
    B: hyper::body::HttpBody + Send + Sync + 'static,
{
    let name =
        HeaderName::from_str(header).with_context(|| format!("invalid header name: {header}"))?;
    let value =
        HeaderValue::from_str(value).with_context(|| format!("invalid header value: {value}"))?;
    Ok(Middleware::post_with_info(
        move |mut response, request_info| {
            let name = name.clone();
            let value = value.clone();
            async move {
                let headers = response.headers_mut();
                if headers.contains_key(&name) {
                    warn!(
                        "{} response already contains header {:?}",
                        request_info.uri(),
                        &name,
                    );
                } else {
                    headers.insert(name, value);
                }
                Ok(response)
            }
        },
    ))
}

pub fn check_permission_with(
    req: &Request<Body>,
    check_permission: impl Fn(&Claims) -> Result<(), AuthError>,
) -> Result<(), ApiError> {
    match req.context::<Claims>() {
        Some(claims) => Ok(check_permission(&claims)
            .map_err(|_err| ApiError::Forbidden("JWT authentication error".to_string()))?),
        None => Ok(()), // claims is None because auth is disabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::future::poll_fn;
    use hyper::service::Service;
    use routerify::RequestServiceBuilder;
    use std::net::{IpAddr, SocketAddr};

    #[tokio::test]
    async fn test_request_id_returned() {
        let builder = RequestServiceBuilder::new(make_router().build().unwrap()).unwrap();
        let remote_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 80);
        let mut service = builder.build(remote_addr);
        if let Err(e) = poll_fn(|ctx| service.poll_ready(ctx)).await {
            panic!("request service is not ready: {:?}", e);
        }

        let mut req: Request<Body> = Request::default();
        req.headers_mut()
            .append(&X_REQUEST_ID_HEADER, HeaderValue::from_str("42").unwrap());

        let resp: Response<hyper::body::Body> = service.call(req).await.unwrap();

        let header_val = resp.headers().get(&X_REQUEST_ID_HEADER).unwrap();

        assert!(header_val == "42", "response header mismatch");
    }

    #[tokio::test]
    async fn test_request_id_empty() {
        let builder = RequestServiceBuilder::new(make_router().build().unwrap()).unwrap();
        let remote_addr = SocketAddr::new(IpAddr::from_str("127.0.0.1").unwrap(), 80);
        let mut service = builder.build(remote_addr);
        if let Err(e) = poll_fn(|ctx| service.poll_ready(ctx)).await {
            panic!("request service is not ready: {:?}", e);
        }

        let req: Request<Body> = Request::default();
        let resp: Response<hyper::body::Body> = service.call(req).await.unwrap();

        let header_val = resp.headers().get(&X_REQUEST_ID_HEADER);

        assert_ne!(header_val, None, "response header should NOT be empty");
    }
}

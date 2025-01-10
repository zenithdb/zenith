use std::pin::pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::future::{select, try_join, Either};
use futures::{StreamExt, TryFutureExt};
use http::header::AUTHORIZATION;
use http::Method;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::http::{HeaderName, HeaderValue};
use hyper::{header, HeaderMap, Request, Response, StatusCode};
use postgres_client::error::{DbError, ErrorPosition, SqlState};
use postgres_client::{GenericClient, IsolationLevel, NoTls, ReadyForQueryStatus, Transaction};
use pq_proto::StartupMessageParamsBuilder;
use serde::Serialize;
use serde_json::Value;
use tokio::time::{self, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use typed_json::json;
use url::Url;
use urlencoding;
use utils::http::error::ApiError;
use uuid::Uuid;

use super::backend::{LocalProxyConnError, PoolingBackend};
use super::conn_pool::{AuthData, ConnInfoWithAuth};
use super::conn_pool_lib::{self, ConnInfo};
use super::error::HttpCodeError;
use super::http_util::json_response;
use super::json::{json_to_pg_text, pg_text_row_to_json, JsonConversionError};
use crate::auth::backend::{ComputeCredentialKeys, ComputeUserInfo};
use crate::auth::{endpoint_sni, ComputeUserInfoParseError};
use crate::config::{AuthenticationConfig, HttpConfig, ProxyConfig, TlsConfig};
use crate::context::RequestContext;
use crate::error::{ErrorKind, ReportableError, UserFacingError};
use crate::http::{read_body_with_limit, ReadBodyError};
use crate::metrics::{HttpDirection, Metrics};
use crate::proxy::{run_until_cancelled, NeonOptions};
use crate::serverless::backend::HttpConnError;
use crate::types::{DbName, RoleName};
use crate::usage_metrics::{MetricCounter, MetricCounterRecorder};

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct QueryData {
    query: String,
    #[serde(deserialize_with = "bytes_to_pg_text")]
    #[serde(default)]
    params: Vec<Option<String>>,
    #[serde(default)]
    array_mode: Option<bool>,
}

#[derive(serde::Deserialize)]
struct BatchQueryData {
    queries: Vec<QueryData>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum Payload {
    Single(QueryData),
    Batch(BatchQueryData),
}

pub(super) static NEON_REQUEST_ID: HeaderName = HeaderName::from_static("neon-request-id");

static CONN_STRING: HeaderName = HeaderName::from_static("neon-connection-string");
static RAW_TEXT_OUTPUT: HeaderName = HeaderName::from_static("neon-raw-text-output");
static ARRAY_MODE: HeaderName = HeaderName::from_static("neon-array-mode");
static ALLOW_POOL: HeaderName = HeaderName::from_static("neon-pool-opt-in");
static TXN_ISOLATION_LEVEL: HeaderName = HeaderName::from_static("neon-batch-isolation-level");
static TXN_READ_ONLY: HeaderName = HeaderName::from_static("neon-batch-read-only");
static TXN_DEFERRABLE: HeaderName = HeaderName::from_static("neon-batch-deferrable");

static HEADER_VALUE_TRUE: HeaderValue = HeaderValue::from_static("true");

fn bytes_to_pg_text<'de, D>(deserializer: D) -> Result<Vec<Option<String>>, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    // TODO: consider avoiding the allocation here.
    let json: Vec<Value> = serde::de::Deserialize::deserialize(deserializer)?;
    Ok(json_to_pg_text(json))
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ConnInfoError {
    #[error("invalid header: {0}")]
    InvalidHeader(&'static HeaderName),
    #[error("invalid connection string: {0}")]
    UrlParseError(#[from] url::ParseError),
    #[error("incorrect scheme")]
    IncorrectScheme,
    #[error("missing database name")]
    MissingDbName,
    #[error("invalid database name")]
    InvalidDbName,
    #[error("missing username")]
    MissingUsername,
    #[error("invalid username: {0}")]
    InvalidUsername(#[from] std::string::FromUtf8Error),
    #[error("missing authentication credentials: {0}")]
    MissingCredentials(Credentials),
    #[error("missing hostname")]
    MissingHostname,
    #[error("invalid hostname: {0}")]
    InvalidEndpoint(#[from] ComputeUserInfoParseError),
    #[error("malformed endpoint")]
    MalformedEndpoint,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum Credentials {
    #[error("required password")]
    Password,
    #[error("required authorization bearer token in JWT format")]
    BearerJwt,
}

impl ReportableError for ConnInfoError {
    fn get_error_kind(&self) -> ErrorKind {
        ErrorKind::User
    }
}

impl UserFacingError for ConnInfoError {
    fn to_string_client(&self) -> String {
        self.to_string()
    }
}

fn get_conn_info(
    config: &'static AuthenticationConfig,
    ctx: &RequestContext,
    headers: &HeaderMap,
    tls: Option<&TlsConfig>,
) -> Result<ConnInfoWithAuth, ConnInfoError> {
    let connection_string = headers
        .get(&CONN_STRING)
        .ok_or(ConnInfoError::InvalidHeader(&CONN_STRING))?
        .to_str()
        .map_err(|_| ConnInfoError::InvalidHeader(&CONN_STRING))?;

    let connection_url = Url::parse(connection_string)?;

    let protocol = connection_url.scheme();
    if protocol != "postgres" && protocol != "postgresql" {
        return Err(ConnInfoError::IncorrectScheme);
    }

    let mut url_path = connection_url
        .path_segments()
        .ok_or(ConnInfoError::MissingDbName)?;

    let dbname: DbName =
        urlencoding::decode(url_path.next().ok_or(ConnInfoError::InvalidDbName)?)?.into();
    ctx.set_dbname(dbname.clone());

    let username = RoleName::from(urlencoding::decode(connection_url.username())?);
    if username.is_empty() {
        return Err(ConnInfoError::MissingUsername);
    }
    ctx.set_user(username.clone());

    let auth = if let Some(auth) = headers.get(&AUTHORIZATION) {
        if !config.accept_jwts {
            return Err(ConnInfoError::MissingCredentials(Credentials::Password));
        }

        let auth = auth
            .to_str()
            .map_err(|_| ConnInfoError::InvalidHeader(&AUTHORIZATION))?;
        AuthData::Jwt(
            auth.strip_prefix("Bearer ")
                .ok_or(ConnInfoError::MissingCredentials(Credentials::BearerJwt))?
                .into(),
        )
    } else if let Some(pass) = connection_url.password() {
        // wrong credentials provided
        if config.accept_jwts {
            return Err(ConnInfoError::MissingCredentials(Credentials::BearerJwt));
        }

        AuthData::Password(match urlencoding::decode_binary(pass.as_bytes()) {
            std::borrow::Cow::Borrowed(b) => b.into(),
            std::borrow::Cow::Owned(b) => b.into(),
        })
    } else if config.accept_jwts {
        return Err(ConnInfoError::MissingCredentials(Credentials::BearerJwt));
    } else {
        return Err(ConnInfoError::MissingCredentials(Credentials::Password));
    };

    let endpoint = match connection_url.host() {
        Some(url::Host::Domain(hostname)) => {
            if let Some(tls) = tls {
                endpoint_sni(hostname, &tls.common_names)?
                    .ok_or(ConnInfoError::MalformedEndpoint)?
            } else {
                hostname
                    .split_once('.')
                    .map_or(hostname, |(prefix, _)| prefix)
                    .into()
            }
        }
        Some(url::Host::Ipv4(_) | url::Host::Ipv6(_)) | None => {
            return Err(ConnInfoError::MissingHostname)
        }
    };
    ctx.set_endpoint_id(endpoint.clone());

    let pairs = connection_url.query_pairs();

    let mut options = Option::None;

    let mut params = StartupMessageParamsBuilder::default();
    params.insert("user", &username);
    params.insert("database", &dbname);
    for (key, value) in pairs {
        params.insert(&key, &value);
        if key == "options" {
            options = Some(NeonOptions::parse_options_raw(&value));
        }
    }

    let user_info = ComputeUserInfo {
        endpoint,
        user: username,
        options: options.unwrap_or_default(),
    };

    let conn_info = ConnInfo { user_info, dbname };
    Ok(ConnInfoWithAuth { conn_info, auth })
}

pub(crate) async fn handle(
    config: &'static ProxyConfig,
    ctx: RequestContext,
    request: Request<Incoming>,
    backend: Arc<PoolingBackend>,
    cancel: CancellationToken,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, ApiError> {
    let result = handle_inner(cancel, config, &ctx, request, backend).await;

    let mut response = match result {
        Ok(r) => {
            ctx.set_success();
            r
        }
        Err(e @ SqlOverHttpError::Cancelled(_)) => {
            let error_kind = e.get_error_kind();
            ctx.set_error_kind(error_kind);

            let message = "Query cancelled, connection was terminated";

            tracing::info!(
                kind=error_kind.to_metric_label(),
                error=%e,
                msg=message,
                "forwarding error to user"
            );

            json_response(
                StatusCode::BAD_REQUEST,
                json!({ "message": message, "code": SqlState::PROTOCOL_VIOLATION.code() }),
            )?
        }
        Err(e) => {
            let error_kind = e.get_error_kind();
            ctx.set_error_kind(error_kind);

            let mut message = e.to_string_client();
            let db_error = match &e {
                SqlOverHttpError::ConnectCompute(HttpConnError::PostgresConnectionError(e))
                | SqlOverHttpError::Postgres(e) => e.as_db_error(),
                _ => None,
            };
            fn get<'a, T: Default>(db: Option<&'a DbError>, x: impl FnOnce(&'a DbError) -> T) -> T {
                db.map(x).unwrap_or_default()
            }

            if let Some(db_error) = db_error {
                db_error.message().clone_into(&mut message);
            }

            let position = db_error.and_then(|db| db.position());
            let (position, internal_position, internal_query) = match position {
                Some(ErrorPosition::Original(position)) => (Some(position.to_string()), None, None),
                Some(ErrorPosition::Internal { position, query }) => {
                    (None, Some(position.to_string()), Some(query.clone()))
                }
                None => (None, None, None),
            };

            let code = get(db_error, |db| db.code().code());
            let severity = get(db_error, |db| db.severity());
            let detail = get(db_error, |db| db.detail());
            let hint = get(db_error, |db| db.hint());
            let where_ = get(db_error, |db| db.where_());
            let table = get(db_error, |db| db.table());
            let column = get(db_error, |db| db.column());
            let schema = get(db_error, |db| db.schema());
            let datatype = get(db_error, |db| db.datatype());
            let constraint = get(db_error, |db| db.constraint());
            let file = get(db_error, |db| db.file());
            let line = get(db_error, |db| db.line().map(|l| l.to_string()));
            let routine = get(db_error, |db| db.routine());

            tracing::info!(
                kind=error_kind.to_metric_label(),
                error=%e,
                msg=message,
                "forwarding error to user"
            );

            json_response(
                e.get_http_status_code(),
                json!({
                    "message": message,
                    "code": code,
                    "detail": detail,
                    "hint": hint,
                    "position": position,
                    "internalPosition": internal_position,
                    "internalQuery": internal_query,
                    "severity": severity,
                    "where": where_,
                    "table": table,
                    "column": column,
                    "schema": schema,
                    "dataType": datatype,
                    "constraint": constraint,
                    "file": file,
                    "line": line,
                    "routine": routine,
                }),
            )?
        }
    };

    response
        .headers_mut()
        .insert("Access-Control-Allow-Origin", HeaderValue::from_static("*"));
    Ok(response)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SqlOverHttpError {
    #[error("{0}")]
    ReadPayload(#[from] ReadPayloadError),
    #[error("{0}")]
    ConnectCompute(#[from] HttpConnError),
    #[error("{0}")]
    ConnInfo(#[from] ConnInfoError),
    #[error("response is too large (max is {0} bytes)")]
    ResponseTooLarge(usize),
    #[error("invalid isolation level")]
    InvalidIsolationLevel,
    #[error("{0}")]
    Postgres(#[from] postgres_client::Error),
    #[error("{0}")]
    JsonConversion(#[from] JsonConversionError),
    #[error("{0}")]
    Cancelled(SqlOverHttpCancel),
}

impl ReportableError for SqlOverHttpError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            SqlOverHttpError::ReadPayload(e) => e.get_error_kind(),
            SqlOverHttpError::ConnectCompute(e) => e.get_error_kind(),
            SqlOverHttpError::ConnInfo(e) => e.get_error_kind(),
            SqlOverHttpError::ResponseTooLarge(_) => ErrorKind::User,
            SqlOverHttpError::InvalidIsolationLevel => ErrorKind::User,
            SqlOverHttpError::Postgres(p) => p.get_error_kind(),
            SqlOverHttpError::JsonConversion(_) => ErrorKind::Postgres,
            SqlOverHttpError::Cancelled(c) => c.get_error_kind(),
        }
    }
}

impl UserFacingError for SqlOverHttpError {
    fn to_string_client(&self) -> String {
        match self {
            SqlOverHttpError::ReadPayload(p) => p.to_string(),
            SqlOverHttpError::ConnectCompute(c) => c.to_string_client(),
            SqlOverHttpError::ConnInfo(c) => c.to_string_client(),
            SqlOverHttpError::ResponseTooLarge(_) => self.to_string(),
            SqlOverHttpError::InvalidIsolationLevel => self.to_string(),
            SqlOverHttpError::Postgres(p) => p.to_string(),
            SqlOverHttpError::JsonConversion(_) => "could not parse postgres response".to_string(),
            SqlOverHttpError::Cancelled(_) => self.to_string(),
        }
    }
}

impl HttpCodeError for SqlOverHttpError {
    fn get_http_status_code(&self) -> StatusCode {
        match self {
            SqlOverHttpError::ReadPayload(e) => e.get_http_status_code(),
            SqlOverHttpError::ConnectCompute(h) => match h.get_error_kind() {
                ErrorKind::User => StatusCode::BAD_REQUEST,
                _ => StatusCode::INTERNAL_SERVER_ERROR,
            },
            SqlOverHttpError::ConnInfo(_) => StatusCode::BAD_REQUEST,
            SqlOverHttpError::ResponseTooLarge(_) => StatusCode::INSUFFICIENT_STORAGE,
            SqlOverHttpError::InvalidIsolationLevel => StatusCode::BAD_REQUEST,
            SqlOverHttpError::Postgres(_) => StatusCode::BAD_REQUEST,
            SqlOverHttpError::JsonConversion(_) => StatusCode::INTERNAL_SERVER_ERROR,
            SqlOverHttpError::Cancelled(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadPayloadError {
    #[error("could not read the HTTP request body: {0}")]
    Read(#[from] hyper::Error),
    #[error("request is too large (max is {limit} bytes)")]
    BodyTooLarge { limit: usize },
    #[error("could not parse the HTTP request body: {0}")]
    Parse(#[from] serde_json::Error),
}

impl From<ReadBodyError<hyper::Error>> for ReadPayloadError {
    fn from(value: ReadBodyError<hyper::Error>) -> Self {
        match value {
            ReadBodyError::BodyTooLarge { limit } => Self::BodyTooLarge { limit },
            ReadBodyError::Read(e) => Self::Read(e),
        }
    }
}

impl ReportableError for ReadPayloadError {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            ReadPayloadError::Read(_) => ErrorKind::ClientDisconnect,
            ReadPayloadError::BodyTooLarge { .. } => ErrorKind::User,
            ReadPayloadError::Parse(_) => ErrorKind::User,
        }
    }
}

impl HttpCodeError for ReadPayloadError {
    fn get_http_status_code(&self) -> StatusCode {
        match self {
            ReadPayloadError::Read(_) => StatusCode::BAD_REQUEST,
            ReadPayloadError::BodyTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            ReadPayloadError::Parse(_) => StatusCode::BAD_REQUEST,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SqlOverHttpCancel {
    #[error("query was cancelled")]
    Postgres,
    #[error("query was cancelled while stuck trying to connect to the database")]
    Connect,
}

impl ReportableError for SqlOverHttpCancel {
    fn get_error_kind(&self) -> ErrorKind {
        match self {
            SqlOverHttpCancel::Postgres => ErrorKind::ClientDisconnect,
            SqlOverHttpCancel::Connect => ErrorKind::ClientDisconnect,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct HttpHeaders {
    raw_output: bool,
    default_array_mode: bool,
    txn_isolation_level: Option<IsolationLevel>,
    txn_read_only: bool,
    txn_deferrable: bool,
}

impl HttpHeaders {
    fn try_parse(headers: &hyper::http::HeaderMap) -> Result<Self, SqlOverHttpError> {
        // Determine the output options. Default behaviour is 'false'. Anything that is not
        // strictly 'true' assumed to be false.
        let raw_output = headers.get(&RAW_TEXT_OUTPUT) == Some(&HEADER_VALUE_TRUE);
        let default_array_mode = headers.get(&ARRAY_MODE) == Some(&HEADER_VALUE_TRUE);

        // isolation level, read only and deferrable
        let txn_isolation_level = match headers.get(&TXN_ISOLATION_LEVEL) {
            Some(x) => Some(
                map_header_to_isolation_level(x).ok_or(SqlOverHttpError::InvalidIsolationLevel)?,
            ),
            None => None,
        };

        let txn_read_only = headers.get(&TXN_READ_ONLY) == Some(&HEADER_VALUE_TRUE);
        let txn_deferrable = headers.get(&TXN_DEFERRABLE) == Some(&HEADER_VALUE_TRUE);

        Ok(Self {
            raw_output,
            default_array_mode,
            txn_isolation_level,
            txn_read_only,
            txn_deferrable,
        })
    }
}

fn map_header_to_isolation_level(level: &HeaderValue) -> Option<IsolationLevel> {
    match level.as_bytes() {
        b"Serializable" => Some(IsolationLevel::Serializable),
        b"ReadUncommitted" => Some(IsolationLevel::ReadUncommitted),
        b"ReadCommitted" => Some(IsolationLevel::ReadCommitted),
        b"RepeatableRead" => Some(IsolationLevel::RepeatableRead),
        _ => None,
    }
}

fn map_isolation_level_to_headers(level: IsolationLevel) -> Option<HeaderValue> {
    match level {
        IsolationLevel::ReadUncommitted => Some(HeaderValue::from_static("ReadUncommitted")),
        IsolationLevel::ReadCommitted => Some(HeaderValue::from_static("ReadCommitted")),
        IsolationLevel::RepeatableRead => Some(HeaderValue::from_static("RepeatableRead")),
        IsolationLevel::Serializable => Some(HeaderValue::from_static("Serializable")),
        _ => None,
    }
}

async fn handle_inner(
    cancel: CancellationToken,
    config: &'static ProxyConfig,
    ctx: &RequestContext,
    request: Request<Incoming>,
    backend: Arc<PoolingBackend>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, SqlOverHttpError> {
    let _requeset_gauge = Metrics::get()
        .proxy
        .connection_requests
        .guard(ctx.protocol());
    info!(
        protocol = %ctx.protocol(),
        "handling interactive connection from client"
    );

    let conn_info = get_conn_info(
        &config.authentication_config,
        ctx,
        request.headers(),
        config.tls_config.as_ref(),
    )?;
    info!(
        user = conn_info.conn_info.user_info.user.as_str(),
        "credentials"
    );

    match conn_info.auth {
        AuthData::Jwt(jwt) if config.authentication_config.is_auth_broker => {
            handle_auth_broker_inner(ctx, request, conn_info.conn_info, jwt, backend).await
        }
        auth => {
            handle_db_inner(
                cancel,
                config,
                ctx,
                request,
                conn_info.conn_info,
                auth,
                backend,
            )
            .await
        }
    }
}

async fn handle_db_inner(
    cancel: CancellationToken,
    config: &'static ProxyConfig,
    ctx: &RequestContext,
    request: Request<Incoming>,
    conn_info: ConnInfo,
    auth: AuthData,
    backend: Arc<PoolingBackend>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, SqlOverHttpError> {
    //
    // Determine the destination and connection params
    //
    let headers = request.headers();

    // Allow connection pooling only if explicitly requested
    // or if we have decided that http pool is no longer opt-in
    let allow_pool = !config.http_config.pool_options.opt_in
        || headers.get(&ALLOW_POOL) == Some(&HEADER_VALUE_TRUE);

    let parsed_headers = HttpHeaders::try_parse(headers)?;

    let fetch_and_process_request = Box::pin(
        async {
            let body = read_body_with_limit(
                request.into_body(),
                config.http_config.max_request_size_bytes,
            )
            .await?;

            Metrics::get()
                .proxy
                .http_conn_content_length_bytes
                .observe(HttpDirection::Request, body.len() as f64);

            debug!(length = body.len(), "request payload read");
            let payload: Payload = serde_json::from_slice(&body)?;
            Ok::<Payload, ReadPayloadError>(payload) // Adjust error type accordingly
        }
        .map_err(SqlOverHttpError::from),
    );

    let authenticate_and_connect = Box::pin(
        async {
            let is_local_proxy = matches!(backend.auth_backend, crate::auth::Backend::Local(_));

            let keys = match auth {
                AuthData::Password(pw) => {
                    backend
                        .authenticate_with_password(ctx, &conn_info.user_info, &pw)
                        .await?
                }
                AuthData::Jwt(jwt) => {
                    backend
                        .authenticate_with_jwt(ctx, &conn_info.user_info, jwt)
                        .await?
                }
            };

            let client = match keys.keys {
                ComputeCredentialKeys::JwtPayload(payload) if is_local_proxy => {
                    let mut client = backend.connect_to_local_postgres(ctx, conn_info).await?;
                    let (cli_inner, _dsc) = client.client_inner();
                    cli_inner.set_jwt_session(&payload).await?;
                    Client::Local(client)
                }
                _ => {
                    let client = backend
                        .connect_to_compute(ctx, conn_info, keys, !allow_pool)
                        .await?;
                    Client::Remote(client)
                }
            };

            // not strictly necessary to mark success here,
            // but it's just insurance for if we forget it somewhere else
            ctx.success();
            Ok::<_, HttpConnError>(client)
        }
        .map_err(SqlOverHttpError::from),
    );

    let (payload, mut client) = match run_until_cancelled(
        // Run both operations in parallel
        try_join(
            pin!(fetch_and_process_request),
            pin!(authenticate_and_connect),
        ),
        &cancel,
    )
    .await
    {
        Some(result) => result?,
        None => return Err(SqlOverHttpError::Cancelled(SqlOverHttpCancel::Connect)),
    };

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json");

    // Now execute the query and return the result.
    let json_output = match payload {
        Payload::Single(stmt) => {
            stmt.process(&config.http_config, cancel, &mut client, parsed_headers)
                .await?
        }
        Payload::Batch(statements) => {
            if parsed_headers.txn_read_only {
                response = response.header(TXN_READ_ONLY.clone(), &HEADER_VALUE_TRUE);
            }
            if parsed_headers.txn_deferrable {
                response = response.header(TXN_DEFERRABLE.clone(), &HEADER_VALUE_TRUE);
            }
            if let Some(txn_isolation_level) = parsed_headers
                .txn_isolation_level
                .and_then(map_isolation_level_to_headers)
            {
                response = response.header(TXN_ISOLATION_LEVEL.clone(), txn_isolation_level);
            }

            statements
                .process(&config.http_config, cancel, &mut client, parsed_headers)
                .await?
        }
    };

    let metrics = client.metrics();

    let len = json_output.len();
    let response = response
        .body(
            Full::new(Bytes::from(json_output))
                .map_err(|x| match x {})
                .boxed(),
        )
        // only fails if invalid status code or invalid header/values are given.
        // these are not user configurable so it cannot fail dynamically
        .expect("building response payload should not fail");

    // count the egress bytes - we miss the TLS and header overhead but oh well...
    // moving this later in the stack is going to be a lot of effort and ehhhh
    metrics.record_egress(len as u64);
    Metrics::get()
        .proxy
        .http_conn_content_length_bytes
        .observe(HttpDirection::Response, len as f64);

    Ok(response)
}

static HEADERS_TO_FORWARD: &[&HeaderName] = &[
    &AUTHORIZATION,
    &CONN_STRING,
    &RAW_TEXT_OUTPUT,
    &ARRAY_MODE,
    &TXN_ISOLATION_LEVEL,
    &TXN_READ_ONLY,
    &TXN_DEFERRABLE,
];

pub(crate) fn uuid_to_header_value(id: Uuid) -> HeaderValue {
    let mut uuid = [0; uuid::fmt::Hyphenated::LENGTH];
    HeaderValue::from_str(id.as_hyphenated().encode_lower(&mut uuid[..]))
        .expect("uuid hyphenated format should be all valid header characters")
}

async fn handle_auth_broker_inner(
    ctx: &RequestContext,
    request: Request<Incoming>,
    conn_info: ConnInfo,
    jwt: String,
    backend: Arc<PoolingBackend>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, SqlOverHttpError> {
    backend
        .authenticate_with_jwt(ctx, &conn_info.user_info, jwt)
        .await
        .map_err(HttpConnError::from)?;

    let mut client = backend.connect_to_local_proxy(ctx, conn_info).await?;

    let local_proxy_uri = ::http::Uri::from_static("http://proxy.local/sql");

    let (mut parts, body) = request.into_parts();
    let mut req = Request::builder().method(Method::POST).uri(local_proxy_uri);

    // todo(conradludgate): maybe auth-broker should parse these and re-serialize
    // these instead just to ensure they remain normalised.
    for &h in HEADERS_TO_FORWARD {
        if let Some(hv) = parts.headers.remove(h) {
            req = req.header(h, hv);
        }
    }
    req = req.header(&NEON_REQUEST_ID, uuid_to_header_value(ctx.session_id()));

    let req = req
        .body(body)
        .expect("all headers and params received via hyper should be valid for request");

    // todo: map body to count egress
    let _metrics = client.metrics();

    Ok(client
        .inner
        .inner
        .send_request(req)
        .await
        .map_err(LocalProxyConnError::from)
        .map_err(HttpConnError::from)?
        .map(|b| b.boxed()))
}

impl QueryData {
    async fn process(
        self,
        config: &'static HttpConfig,
        cancel: CancellationToken,
        client: &mut Client,
        parsed_headers: HttpHeaders,
    ) -> Result<String, SqlOverHttpError> {
        let (inner, mut discard) = client.inner();
        let cancel_token = inner.cancel_token();

        let res = match select(
            pin!(query_to_json(config, &*inner, self, &mut 0, parsed_headers)),
            pin!(cancel.cancelled()),
        )
        .await
        {
            // The query successfully completed.
            Either::Left((Ok((status, results)), __not_yet_cancelled)) => {
                discard.check_idle(status);

                let json_output =
                    serde_json::to_string(&results).expect("json serialization should not fail");
                Ok(json_output)
            }
            // The query failed with an error
            Either::Left((Err(e), __not_yet_cancelled)) => {
                discard.discard();
                return Err(e);
            }
            // The query was cancelled.
            Either::Right((_cancelled, query)) => {
                tracing::info!("cancelling query");
                if let Err(err) = cancel_token.cancel_query(NoTls).await {
                    tracing::warn!(?err, "could not cancel query");
                }
                // wait for the query cancellation
                match time::timeout(time::Duration::from_millis(100), query).await {
                    // query successed before it was cancelled.
                    Ok(Ok((status, results))) => {
                        discard.check_idle(status);

                        let json_output = serde_json::to_string(&results)
                            .expect("json serialization should not fail");
                        Ok(json_output)
                    }
                    // query failed or was cancelled.
                    Ok(Err(error)) => {
                        let db_error = match &error {
                            SqlOverHttpError::ConnectCompute(
                                HttpConnError::PostgresConnectionError(e),
                            )
                            | SqlOverHttpError::Postgres(e) => e.as_db_error(),
                            _ => None,
                        };

                        // if errored for some other reason, it might not be safe to return
                        if !db_error.is_some_and(|e| *e.code() == SqlState::QUERY_CANCELED) {
                            discard.discard();
                        }

                        Err(SqlOverHttpError::Cancelled(SqlOverHttpCancel::Postgres))
                    }
                    Err(_timeout) => {
                        discard.discard();
                        Err(SqlOverHttpError::Cancelled(SqlOverHttpCancel::Postgres))
                    }
                }
            }
        };
        res
    }
}

impl BatchQueryData {
    async fn process(
        self,
        config: &'static HttpConfig,
        cancel: CancellationToken,
        client: &mut Client,
        parsed_headers: HttpHeaders,
    ) -> Result<String, SqlOverHttpError> {
        info!("starting transaction");
        let (inner, mut discard) = client.inner();
        let cancel_token = inner.cancel_token();
        let mut builder = inner.build_transaction();
        if let Some(isolation_level) = parsed_headers.txn_isolation_level {
            builder = builder.isolation_level(isolation_level);
        }
        if parsed_headers.txn_read_only {
            builder = builder.read_only(true);
        }
        if parsed_headers.txn_deferrable {
            builder = builder.deferrable(true);
        }

        let transaction = builder.start().await.inspect_err(|_| {
            // if we cannot start a transaction, we should return immediately
            // and not return to the pool. connection is clearly broken
            discard.discard();
        })?;

        let json_output = match query_batch(
            config,
            cancel.child_token(),
            &transaction,
            self,
            parsed_headers,
        )
        .await
        {
            Ok(json_output) => {
                info!("commit");
                let status = transaction.commit().await.inspect_err(|_| {
                    // if we cannot commit - for now don't return connection to pool
                    // TODO: get a query status from the error
                    discard.discard();
                })?;
                discard.check_idle(status);
                json_output
            }
            Err(SqlOverHttpError::Cancelled(_)) => {
                if let Err(err) = cancel_token.cancel_query(NoTls).await {
                    tracing::warn!(?err, "could not cancel query");
                }
                // TODO: after cancelling, wait to see if we can get a status. maybe the connection is still safe.
                discard.discard();

                return Err(SqlOverHttpError::Cancelled(SqlOverHttpCancel::Postgres));
            }
            Err(err) => {
                info!("rollback");
                let status = transaction.rollback().await.inspect_err(|_| {
                    // if we cannot rollback - for now don't return connection to pool
                    // TODO: get a query status from the error
                    discard.discard();
                })?;
                discard.check_idle(status);
                return Err(err);
            }
        };

        Ok(json_output)
    }
}

async fn query_batch(
    config: &'static HttpConfig,
    cancel: CancellationToken,
    transaction: &Transaction<'_>,
    queries: BatchQueryData,
    parsed_headers: HttpHeaders,
) -> Result<String, SqlOverHttpError> {
    let mut results = Vec::with_capacity(queries.queries.len());
    let mut current_size = 0;
    for stmt in queries.queries {
        let query = pin!(query_to_json(
            config,
            transaction,
            stmt,
            &mut current_size,
            parsed_headers,
        ));
        let cancelled = pin!(cancel.cancelled());
        let res = select(query, cancelled).await;
        match res {
            // TODO: maybe we should check that the transaction bit is set here
            Either::Left((Ok((_, values)), _cancelled)) => {
                results.push(values);
            }
            Either::Left((Err(e), _cancelled)) => {
                return Err(e);
            }
            Either::Right((_cancelled, _)) => {
                return Err(SqlOverHttpError::Cancelled(SqlOverHttpCancel::Postgres));
            }
        }
    }

    let results = json!({ "results": results });
    let json_output = serde_json::to_string(&results).expect("json serialization should not fail");

    Ok(json_output)
}

async fn query_to_json<T: GenericClient>(
    config: &'static HttpConfig,
    client: &T,
    data: QueryData,
    current_size: &mut usize,
    parsed_headers: HttpHeaders,
) -> Result<(ReadyForQueryStatus, impl Serialize), SqlOverHttpError> {
    let query_start = Instant::now();

    let query_params = data.params;
    let mut row_stream = std::pin::pin!(client.query_raw_txt(&data.query, query_params).await?);
    let query_acknowledged = Instant::now();

    // Manually drain the stream into a vector to leave row_stream hanging
    // around to get a command tag. Also check that the response is not too
    // big.
    let mut rows: Vec<postgres_client::Row> = Vec::new();
    while let Some(row) = row_stream.next().await {
        let row = row?;
        *current_size += row.body_len();
        rows.push(row);
        // we don't have a streaming response support yet so this is to prevent OOM
        // from a malicious query (eg a cross join)
        if *current_size > config.max_response_size_bytes {
            return Err(SqlOverHttpError::ResponseTooLarge(
                config.max_response_size_bytes,
            ));
        }
    }

    let query_resp_end = Instant::now();
    let ready = row_stream.ready_status();

    // grab the command tag and number of rows affected
    let command_tag = row_stream.command_tag().unwrap_or_default();
    let mut command_tag_split = command_tag.split(' ');
    let command_tag_name = command_tag_split.next().unwrap_or_default();
    let command_tag_count = if command_tag_name == "INSERT" {
        // INSERT returns OID first and then number of rows
        command_tag_split.nth(1)
    } else {
        // other commands return number of rows (if any)
        command_tag_split.next()
    }
    .and_then(|s| s.parse::<i64>().ok());

    info!(
        rows = rows.len(),
        ?ready,
        command_tag,
        acknowledgement = ?(query_acknowledged - query_start),
        response = ?(query_resp_end - query_start),
        "finished executing query"
    );

    let columns_len = row_stream.columns().len();
    let mut fields = Vec::with_capacity(columns_len);
    let mut columns = Vec::with_capacity(columns_len);

    for c in row_stream.columns() {
        fields.push(json!({
            "name": c.name().to_owned(),
            "dataTypeID": c.type_().oid(),
            "tableID": c.table_oid(),
            "columnID": c.column_id(),
            "dataTypeSize": c.type_size(),
            "dataTypeModifier": c.type_modifier(),
            "format": "text",
        }));
        columns.push(client.get_type(c.type_oid()).await?);
    }

    let array_mode = data.array_mode.unwrap_or(parsed_headers.default_array_mode);

    // convert rows to JSON
    let rows = rows
        .iter()
        .map(|row| pg_text_row_to_json(row, &columns, parsed_headers.raw_output, array_mode))
        .collect::<Result<Vec<_>, _>>()?;

    // Resulting JSON format is based on the format of node-postgres result.
    let results = json!({
        "command": command_tag_name.to_string(),
        "rowCount": command_tag_count,
        "rows": rows,
        "fields": fields,
        "rowAsArray": array_mode,
    });

    Ok((ready, results))
}

enum Client {
    Remote(conn_pool_lib::Client<postgres_client::Client>),
    Local(conn_pool_lib::Client<postgres_client::Client>),
}

enum Discard<'a> {
    Remote(conn_pool_lib::Discard<'a, postgres_client::Client>),
    Local(conn_pool_lib::Discard<'a, postgres_client::Client>),
}

impl Client {
    fn metrics(&self) -> Arc<MetricCounter> {
        match self {
            Client::Remote(client) => client.metrics(),
            Client::Local(local_client) => local_client.metrics(),
        }
    }

    fn inner(&mut self) -> (&mut postgres_client::Client, Discard<'_>) {
        match self {
            Client::Remote(client) => {
                let (c, d) = client.inner();
                (c, Discard::Remote(d))
            }
            Client::Local(local_client) => {
                let (c, d) = local_client.inner();
                (c, Discard::Local(d))
            }
        }
    }
}

impl Discard<'_> {
    fn check_idle(&mut self, status: ReadyForQueryStatus) {
        match self {
            Discard::Remote(discard) => discard.check_idle(status),
            Discard::Local(discard) => discard.check_idle(status),
        }
    }
    fn discard(&mut self) {
        match self {
            Discard::Remote(discard) => discard.discard(),
            Discard::Local(discard) => discard.discard(),
        }
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_payload() {
        let payload = "{\"query\":\"SELECT * FROM users WHERE name = ?\",\"params\":[\"test\"],\"arrayMode\":true}";
        let deserialized_payload: Payload = serde_json::from_str(payload).unwrap();

        match deserialized_payload {
            Payload::Single(QueryData {
                query,
                params,
                array_mode,
            }) => {
                assert_eq!(query, "SELECT * FROM users WHERE name = ?");
                assert_eq!(params, vec![Some(String::from("test"))]);
                assert!(array_mode.unwrap());
            }
            Payload::Batch(_) => {
                panic!("deserialization failed: case with single query, one param, and array mode")
            }
        }

        let payload = "{\"queries\":[{\"query\":\"SELECT * FROM users0 WHERE name = ?\",\"params\":[\"test0\"], \"arrayMode\":false},{\"query\":\"SELECT * FROM users1 WHERE name = ?\",\"params\":[\"test1\"],\"arrayMode\":true}]}";
        let deserialized_payload: Payload = serde_json::from_str(payload).unwrap();

        match deserialized_payload {
            Payload::Batch(BatchQueryData { queries }) => {
                assert_eq!(queries.len(), 2);
                for (i, query) in queries.into_iter().enumerate() {
                    assert_eq!(
                        query.query,
                        format!("SELECT * FROM users{i} WHERE name = ?")
                    );
                    assert_eq!(query.params, vec![Some(format!("test{i}"))]);
                    assert_eq!(query.array_mode.unwrap(), i > 0);
                }
            }
            Payload::Single(_) => panic!("deserialization failed: case with multiple queries"),
        }

        let payload = "{\"query\":\"SELECT 1\"}";
        let deserialized_payload: Payload = serde_json::from_str(payload).unwrap();

        match deserialized_payload {
            Payload::Single(QueryData {
                query,
                params,
                array_mode,
            }) => {
                assert_eq!(query, "SELECT 1");
                assert_eq!(params, vec![]);
                assert!(array_mode.is_none());
            }
            Payload::Batch(_) => panic!("deserialization failed: case with only one query"),
        }
    }
}

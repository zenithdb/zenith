use std::convert::Infallible;
use std::net::IpAddr;
use std::net::Ipv6Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::thread;

use crate::catalog::SchemaDumpError;
use crate::catalog::{get_database_schema, get_dbs_and_roles};
use crate::compute::forward_termination_signal;
use crate::compute::{ComputeNode, ComputeState, ParsedSpec};
use crate::installed_extensions;
use compute_api::requests::{ConfigurationRequest, ExtensionInstallRequest, SetRoleGrantsRequest};
use compute_api::responses::{
    ComputeStatus, ComputeStatusResponse, ExtensionInstallResult, GenericAPIError,
    SetRoleGrantsResponse,
};

use anyhow::Result;
use hyper::header::CONTENT_TYPE;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use metrics::Encoder;
use metrics::TextEncoder;
use tokio::task;
use tracing::{debug, error, info, warn};
use tracing_utils::http::OtelName;
use utils::http::request::must_get_query_param;

fn status_response_from_state(state: &ComputeState) -> ComputeStatusResponse {
    ComputeStatusResponse {
        start_time: state.start_time,
        tenant: state
            .pspec
            .as_ref()
            .map(|pspec| pspec.tenant_id.to_string()),
        timeline: state
            .pspec
            .as_ref()
            .map(|pspec| pspec.timeline_id.to_string()),
        status: state.status,
        last_active: state.last_active,
        error: state.error.clone(),
    }
}

// Service function to handle all available routes.
async fn routes(req: Request<Body>, compute: &Arc<ComputeNode>) -> Response<Body> {
    //
    // NOTE: The URI path is currently included in traces. That's OK because
    // it doesn't contain any variable parts or sensitive information. But
    // please keep that in mind if you change the routing here.
    //
    match (req.method(), req.uri().path()) {
        // Serialized compute state.
        (&Method::GET, "/status") => {
            debug!("serving /status GET request");
            let state = compute.state.lock().unwrap();
            let status_response = status_response_from_state(&state);
            Response::new(Body::from(serde_json::to_string(&status_response).unwrap()))
        }

        // Startup metrics in JSON format. Keep /metrics reserved for a possible
        // future use for Prometheus metrics format.
        (&Method::GET, "/metrics.json") => {
            info!("serving /metrics.json GET request");
            let metrics = compute.state.lock().unwrap().metrics.clone();
            Response::new(Body::from(serde_json::to_string(&metrics).unwrap()))
        }

        // Prometheus metrics
        (&Method::GET, "/metrics") => {
            debug!("serving /metrics GET request");

            let mut buffer = vec![];
            let metrics = installed_extensions::collect();
            let encoder = TextEncoder::new();
            encoder.encode(&metrics, &mut buffer).unwrap();

            match Response::builder()
                .status(StatusCode::OK)
                .header(CONTENT_TYPE, encoder.format_type())
                .body(Body::from(buffer))
            {
                Ok(response) => response,
                Err(err) => {
                    let msg = format!("error handling /metrics request: {err}");
                    error!(msg);
                    render_json_error(&msg, StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }
        // Collect Postgres current usage insights
        (&Method::GET, "/insights") => {
            info!("serving /insights GET request");
            let status = compute.get_status();
            if status != ComputeStatus::Running {
                let msg = format!("compute is not running, current status: {:?}", status);
                error!(msg);
                return Response::new(Body::from(msg));
            }

            let insights = compute.collect_insights().await;
            Response::new(Body::from(insights))
        }

        (&Method::POST, "/check_writability") => {
            info!("serving /check_writability POST request");
            let status = compute.get_status();
            if status != ComputeStatus::Running {
                let msg = format!(
                    "invalid compute status for check_writability request: {:?}",
                    status
                );
                error!(msg);
                return Response::new(Body::from(msg));
            }

            let res = crate::checker::check_writability(compute).await;
            match res {
                Ok(_) => Response::new(Body::from("true")),
                Err(e) => {
                    error!("check_writability failed: {}", e);
                    Response::new(Body::from(e.to_string()))
                }
            }
        }

        (&Method::POST, "/extensions") => {
            info!("serving /extensions POST request");
            let status = compute.get_status();
            if status != ComputeStatus::Running {
                let msg = format!(
                    "invalid compute status for extensions request: {:?}",
                    status
                );
                error!(msg);
                return render_json_error(&msg, StatusCode::PRECONDITION_FAILED);
            }

            let request = hyper::body::to_bytes(req.into_body()).await.unwrap();
            let request = serde_json::from_slice::<ExtensionInstallRequest>(&request).unwrap();
            let res = compute
                .install_extension(&request.extension, &request.database, request.version)
                .await;
            match res {
                Ok(version) => render_json(Body::from(
                    serde_json::to_string(&ExtensionInstallResult {
                        extension: request.extension,
                        version,
                    })
                    .unwrap(),
                )),
                Err(e) => {
                    error!("install_extension failed: {}", e);
                    render_json_error(&e.to_string(), StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }

        (&Method::GET, "/info") => {
            let num_cpus = num_cpus::get_physical();
            info!("serving /info GET request. num_cpus: {}", num_cpus);
            Response::new(Body::from(
                serde_json::json!({
                    "num_cpus": num_cpus,
                })
                .to_string(),
            ))
        }

        // Accept spec in JSON format and request compute configuration. If
        // anything goes wrong after we set the compute status to `ConfigurationPending`
        // and update compute state with new spec, we basically leave compute
        // in the potentially wrong state. That said, it's control-plane's
        // responsibility to watch compute state after reconfiguration request
        // and to clean restart in case of errors.
        (&Method::POST, "/configure") => {
            info!("serving /configure POST request");
            match handle_configure_request(req, compute).await {
                Ok(msg) => Response::new(Body::from(msg)),
                Err((msg, code)) => {
                    error!("error handling /configure request: {msg}");
                    render_json_error(&msg, code)
                }
            }
        }

        (&Method::POST, "/terminate") => {
            info!("serving /terminate POST request");
            match handle_terminate_request(compute).await {
                Ok(()) => Response::new(Body::empty()),
                Err((msg, code)) => {
                    error!("error handling /terminate request: {msg}");
                    render_json_error(&msg, code)
                }
            }
        }

        (&Method::GET, "/dbs_and_roles") => {
            info!("serving /dbs_and_roles GET request",);
            match get_dbs_and_roles(compute).await {
                Ok(res) => render_json(Body::from(serde_json::to_string(&res).unwrap())),
                Err(_) => {
                    render_json_error("can't get dbs and roles", StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }

        (&Method::GET, "/database_schema") => {
            let database = match must_get_query_param(&req, "database") {
                Err(e) => return e.into_response(),
                Ok(database) => database,
            };
            info!("serving /database_schema GET request with database: {database}",);
            match get_database_schema(compute, &database).await {
                Ok(res) => render_plain(Body::wrap_stream(res)),
                Err(SchemaDumpError::DatabaseDoesNotExist) => {
                    render_json_error("database does not exist", StatusCode::NOT_FOUND)
                }
                Err(e) => {
                    error!("can't get schema dump: {}", e);
                    render_json_error("can't get schema dump", StatusCode::INTERNAL_SERVER_ERROR)
                }
            }
        }

        (&Method::POST, "/grants") => {
            info!("serving /grants POST request");
            let status = compute.get_status();
            if status != ComputeStatus::Running {
                let msg = format!(
                    "invalid compute status for set_role_grants request: {:?}",
                    status
                );
                error!(msg);
                return render_json_error(&msg, StatusCode::PRECONDITION_FAILED);
            }

            let request = hyper::body::to_bytes(req.into_body()).await.unwrap();
            let request = serde_json::from_slice::<SetRoleGrantsRequest>(&request).unwrap();

            let res = compute
                .set_role_grants(
                    &request.database,
                    &request.schema,
                    &request.privileges,
                    &request.role,
                )
                .await;
            match res {
                Ok(()) => render_json(Body::from(
                    serde_json::to_string(&SetRoleGrantsResponse {
                        database: request.database,
                        schema: request.schema,
                        role: request.role,
                        privileges: request.privileges,
                    })
                    .unwrap(),
                )),
                Err(e) => render_json_error(
                    &format!("could not grant role privileges to the schema: {e}"),
                    // TODO: can we filter on role/schema not found errors
                    // and return appropriate error code?
                    StatusCode::INTERNAL_SERVER_ERROR,
                ),
            }
        }

        // get the list of installed extensions
        // currently only used in python tests
        // TODO: call it from cplane
        (&Method::GET, "/installed_extensions") => {
            info!("serving /installed_extensions GET request");
            let status = compute.get_status();
            if status != ComputeStatus::Running {
                let msg = format!(
                    "invalid compute status for extensions request: {:?}",
                    status
                );
                error!(msg);
                return Response::new(Body::from(msg));
            }

            let connstr = compute.connstr.clone();
            let res = crate::installed_extensions::get_installed_extensions(connstr).await;
            match res {
                Ok(res) => render_json(Body::from(serde_json::to_string(&res).unwrap())),
                Err(e) => render_json_error(
                    &format!("could not get list of installed extensions: {}", e),
                    StatusCode::INTERNAL_SERVER_ERROR,
                ),
            }
        }

        // download extension files from remote extension storage on demand
        (&Method::POST, route) if route.starts_with("/extension_server/") => {
            info!("serving {:?} POST request", route);
            info!("req.uri {:?}", req.uri());

            // don't even try to download extensions
            // if no remote storage is configured
            if compute.ext_remote_storage.is_none() {
                info!("no extensions remote storage configured");
                let mut resp = Response::new(Body::from("no remote storage configured"));
                *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                return resp;
            }

            let mut is_library = false;
            if let Some(params) = req.uri().query() {
                info!("serving {:?} POST request with params: {}", route, params);
                if params == "is_library=true" {
                    is_library = true;
                } else {
                    let mut resp = Response::new(Body::from("Wrong request parameters"));
                    *resp.status_mut() = StatusCode::BAD_REQUEST;
                    return resp;
                }
            }
            let filename = route.split('/').last().unwrap().to_string();
            info!("serving /extension_server POST request, filename: {filename:?} is_library: {is_library}");

            // get ext_name and path from spec
            // don't lock compute_state for too long
            let ext = {
                let compute_state = compute.state.lock().unwrap();
                let pspec = compute_state.pspec.as_ref().expect("spec must be set");
                let spec = &pspec.spec;

                // debug only
                info!("spec: {:?}", spec);

                let remote_extensions = match spec.remote_extensions.as_ref() {
                    Some(r) => r,
                    None => {
                        info!("no remote extensions spec was provided");
                        let mut resp = Response::new(Body::from("no remote storage configured"));
                        *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                        return resp;
                    }
                };

                remote_extensions.get_ext(
                    &filename,
                    is_library,
                    &compute.build_tag,
                    &compute.pgversion,
                )
            };

            match ext {
                Ok((ext_name, ext_path)) => {
                    match compute.download_extension(ext_name, ext_path).await {
                        Ok(_) => Response::new(Body::from("OK")),
                        Err(e) => {
                            error!("extension download failed: {}", e);
                            let mut resp = Response::new(Body::from(e.to_string()));
                            *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                            resp
                        }
                    }
                }
                Err(e) => {
                    warn!("extension download failed to find extension: {}", e);
                    let mut resp = Response::new(Body::from("failed to find file"));
                    *resp.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
                    resp
                }
            }
        }

        // Return the `404 Not Found` for any other routes.
        _ => {
            let mut not_found = Response::new(Body::from("404 Not Found"));
            *not_found.status_mut() = StatusCode::NOT_FOUND;
            not_found
        }
    }
}

async fn handle_configure_request(
    req: Request<Body>,
    compute: &Arc<ComputeNode>,
) -> Result<String, (String, StatusCode)> {
    if !compute.live_config_allowed {
        return Err((
            "live configuration is not allowed for this compute node".to_string(),
            StatusCode::PRECONDITION_FAILED,
        ));
    }

    let body_bytes = hyper::body::to_bytes(req.into_body()).await.unwrap();
    let spec_raw = String::from_utf8(body_bytes.to_vec()).unwrap();
    if let Ok(request) = serde_json::from_str::<ConfigurationRequest>(&spec_raw) {
        let spec = request.spec;

        let parsed_spec = match ParsedSpec::try_from(spec) {
            Ok(ps) => ps,
            Err(msg) => return Err((msg, StatusCode::BAD_REQUEST)),
        };

        // XXX: wrap state update under lock in code blocks. Otherwise,
        // we will try to `Send` `mut state` into the spawned thread
        // bellow, which will cause error:
        // ```
        // error: future cannot be sent between threads safely
        // ```
        {
            let mut state = compute.state.lock().unwrap();
            if state.status != ComputeStatus::Empty && state.status != ComputeStatus::Running {
                let msg = format!(
                    "invalid compute status for configuration request: {:?}",
                    state.status.clone()
                );
                return Err((msg, StatusCode::PRECONDITION_FAILED));
            }
            state.pspec = Some(parsed_spec);
            state.set_status(ComputeStatus::ConfigurationPending, &compute.state_changed);
            drop(state);
            info!("set new spec and notified waiters");
        }

        // Spawn a blocking thread to wait for compute to become Running.
        // This is needed to do not block the main pool of workers and
        // be able to serve other requests while some particular request
        // is waiting for compute to finish configuration.
        let c = compute.clone();
        task::spawn_blocking(move || {
            let mut state = c.state.lock().unwrap();
            while state.status != ComputeStatus::Running {
                state = c.state_changed.wait(state).unwrap();
                info!(
                    "waiting for compute to become Running, current status: {:?}",
                    state.status
                );

                if state.status == ComputeStatus::Failed {
                    let err = state.error.as_ref().map_or("unknown error", |x| x);
                    let msg = format!("compute configuration failed: {:?}", err);
                    return Err((msg, StatusCode::INTERNAL_SERVER_ERROR));
                }
            }

            Ok(())
        })
        .await
        .unwrap()?;

        // Return current compute state if everything went well.
        let state = compute.state.lock().unwrap().clone();
        let status_response = status_response_from_state(&state);
        Ok(serde_json::to_string(&status_response).unwrap())
    } else {
        Err(("invalid spec".to_string(), StatusCode::BAD_REQUEST))
    }
}

fn render_json_error(e: &str, status: StatusCode) -> Response<Body> {
    let error = GenericAPIError {
        error: e.to_string(),
    };
    Response::builder()
        .status(status)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_string(&error).unwrap()))
        .unwrap()
}

fn render_json(body: Body) -> Response<Body> {
    Response::builder()
        .header(CONTENT_TYPE, "application/json")
        .body(body)
        .unwrap()
}

fn render_plain(body: Body) -> Response<Body> {
    Response::builder()
        .header(CONTENT_TYPE, "text/plain")
        .body(body)
        .unwrap()
}

async fn handle_terminate_request(compute: &Arc<ComputeNode>) -> Result<(), (String, StatusCode)> {
    {
        let mut state = compute.state.lock().unwrap();
        if state.status == ComputeStatus::Terminated {
            return Ok(());
        }
        if state.status != ComputeStatus::Empty && state.status != ComputeStatus::Running {
            let msg = format!(
                "invalid compute status for termination request: {}",
                state.status
            );
            return Err((msg, StatusCode::PRECONDITION_FAILED));
        }
        state.set_status(ComputeStatus::TerminationPending, &compute.state_changed);
        drop(state);
    }

    forward_termination_signal();
    info!("sent signal and notified waiters");

    // Spawn a blocking thread to wait for compute to become Terminated.
    // This is needed to do not block the main pool of workers and
    // be able to serve other requests while some particular request
    // is waiting for compute to finish configuration.
    let c = compute.clone();
    task::spawn_blocking(move || {
        let mut state = c.state.lock().unwrap();
        while state.status != ComputeStatus::Terminated {
            state = c.state_changed.wait(state).unwrap();
            info!(
                "waiting for compute to become {}, current status: {:?}",
                ComputeStatus::Terminated,
                state.status
            );
        }

        Ok(())
    })
    .await
    .unwrap()?;
    info!("terminated Postgres");
    Ok(())
}

// Main Hyper HTTP server function that runs it and blocks waiting on it forever.
#[tokio::main]
async fn serve(port: u16, state: Arc<ComputeNode>) {
    // this usually binds to both IPv4 and IPv6 on linux
    // see e.g. https://github.com/rust-lang/rust/pull/34440
    let addr = SocketAddr::new(IpAddr::from(Ipv6Addr::UNSPECIFIED), port);

    let make_service = make_service_fn(move |_conn| {
        let state = state.clone();
        async move {
            Ok::<_, Infallible>(service_fn(move |req: Request<Body>| {
                let state = state.clone();
                async move {
                    Ok::<_, Infallible>(
                        // NOTE: We include the URI path in the string. It
                        // doesn't contain any variable parts or sensitive
                        // information in this API.
                        tracing_utils::http::tracing_handler(
                            req,
                            |req| routes(req, &state),
                            OtelName::UriPath,
                        )
                        .await,
                    )
                }
            }))
        }
    });

    info!("starting HTTP server on {}", addr);

    let server = Server::bind(&addr).serve(make_service);

    // Run this server forever
    if let Err(e) = server.await {
        error!("server error: {}", e);
    }
}

/// Launch a separate Hyper HTTP API server thread and return its `JoinHandle`.
pub fn launch_http_server(port: u16, state: &Arc<ComputeNode>) -> Result<thread::JoinHandle<()>> {
    let state = Arc::clone(state);

    Ok(thread::Builder::new()
        .name("http-endpoint".into())
        .spawn(move || serve(port, state))?)
}

pub mod routes;
pub use routes::make_router;

pub use safekeeper_api::models;
use std::sync::Arc;

use crate::{GlobalTimelines, SafeKeeperConf};

pub async fn task_main(
    conf: Arc<SafeKeeperConf>,
    http_listener: std::net::TcpListener,
    global_timelines: Arc<GlobalTimelines>,
) -> anyhow::Result<()> {
    let router = make_router(conf, global_timelines)
        .build()
        .map_err(|err| anyhow::anyhow!(err))?;
    let service = utils::http::RouterService::new(router).unwrap();
    let server = hyper::Server::from_tcp(http_listener)?;
    server.serve(service).await?;
    Ok(()) // unreachable
}

pub mod routes;
pub use routes::make_router;

pub use safekeeper_api::models;
use std::sync::Arc;

use crate::{wal_backup::WalBackup, GlobalTimelines, SafeKeeperConf};

pub async fn task_main(
    conf: Arc<SafeKeeperConf>,
    http_listener: std::net::TcpListener,
    global_timelines: Arc<GlobalTimelines>,
    wal_backup: Arc<WalBackup>,
) -> anyhow::Result<()> {
    let router = make_router(conf, global_timelines, wal_backup)
        .build()
        .map_err(|err| anyhow::anyhow!(err))?;
    let service = utils::http::RouterService::new(router).unwrap();
    let server = hyper::Server::from_tcp(http_listener)?;
    server.serve(service).await?;
    Ok(()) // unreachable
}

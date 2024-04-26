//! Async dns resolvers

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
};

use aws_sdk_iam::error::BoxError;
use hickory_resolver::{error::ResolveError, proto::rr::RData};
use hyper::client::connect::dns::Name;
use reqwest::dns::Addrs;
use tokio::time::Instant;
use tracing::trace;

#[derive(Clone)]
pub struct Dns {
    resolver: Arc<hickory_resolver::TokioAsyncResolver>,
}

impl Default for Dns {
    fn default() -> Self {
        Self::new()
    }
}

impl Dns {
    pub fn new() -> Self {
        let (config, options) =
            hickory_resolver::system_conf::read_system_conf().expect("could not read resolv.conf");

        let resolver = Arc::new(hickory_resolver::TokioAsyncResolver::tokio(config, options));

        Self { resolver }
    }

    pub async fn resolve(&self, name: &str) -> Result<Vec<IpAddr>, ResolveError> {
        let start = Instant::now();

        // try to parse the host as a regular IP address first
        if let Ok(addr) = name.parse::<Ipv4Addr>() {
            return Ok(vec![IpAddr::V4(addr)]);
        }

        if let Ok(addr) = name.parse::<Ipv6Addr>() {
            return Ok(vec![IpAddr::V6(addr)]);
        }

        let res = self.resolver.lookup_ip(name).await;

        let resolve_duration = start.elapsed();
        trace!(duration = ?resolve_duration, addr = %name, "resolve host complete");

        Ok(res?
            .as_lookup()
            .records()
            .iter()
            .filter_map(|r| r.data())
            .filter_map(|rdata| match rdata {
                RData::A(ip) => Some(IpAddr::from(ip.0)),
                RData::AAAA(ip) => Some(IpAddr::from(ip.0)),
                _ => None,
            })
            .collect())
    }
}

impl hyper::service::Service<Name> for Dns {
    type Response = Addrs;
    type Error = BoxError;
    type Future = reqwest::dns::Resolving;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Name) -> Self::Future {
        reqwest::dns::Resolve::resolve(self, req)
    }
}

impl reqwest::dns::Resolve for Dns {
    fn resolve(&self, name: Name) -> reqwest::dns::Resolving {
        let this = self.clone();
        Box::pin(async move {
            match this.resolve(name.as_str()).await {
                Ok(iter) => {
                    Ok(Box::new(iter.into_iter().map(|ip| SocketAddr::new(ip, 0))) as Box<_>)
                }
                Err(e) => Err(e.into()),
            }
        })
    }
}

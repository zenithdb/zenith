use serde::Serialize;
use utils::seqwait::MonotonicCounter;

extern crate hyper0 as hyper;

mod auth;
mod background_node_operations;
mod compute_hook;
mod drain_utils;
mod heartbeater;
pub mod http;
mod id_lock_map;
mod leadership;
pub mod metrics;
mod node;
mod pageserver_client;
mod peer_client;
pub mod persistence;
mod reconciler;
mod safekeeper_client;
mod scheduler;
mod schema;
pub mod service;
mod tenant_shard;

#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone, Serialize)]
struct Sequence(u64);

impl Sequence {
    fn initial() -> Self {
        Self(0)
    }
}

impl std::fmt::Display for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::fmt::Debug for Sequence {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl MonotonicCounter<Sequence> for Sequence {
    fn cnt_advance(&mut self, v: Sequence) {
        assert!(*self <= v);
        *self = v;
    }
    fn cnt_value(&self) -> Sequence {
        *self
    }
}

impl Sequence {
    fn next(&self) -> Sequence {
        Sequence(self.0 + 1)
    }
}

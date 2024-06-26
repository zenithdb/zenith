use std::{num::NonZeroUsize, sync::Arc};

use crate::tenant::ephemeral_file;

#[derive(Default, Debug, PartialEq, Eq, Clone, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "kebab-case", deny_unknown_fields)]
pub enum L0FlushConfig {
    #[default]
    PageCached,
    #[serde(rename_all = "snake_case")]
    Direct { max_concurrency: NonZeroUsize },
    #[serde(skip)]
    Fail(String),
}

#[derive(Clone)]
pub struct L0FlushGlobalState(Arc<Inner>);

pub(crate) enum Inner {
    PageCached,
    Direct { semaphore: tokio::sync::Semaphore },
    Fail(String),
}

impl L0FlushGlobalState {
    pub fn new(config: L0FlushConfig) -> Self {
        match config {
            L0FlushConfig::PageCached => Self(Arc::new(Inner::PageCached)),
            L0FlushConfig::Direct { max_concurrency } => {
                let semaphore = tokio::sync::Semaphore::new(max_concurrency.get());
                Self(Arc::new(Inner::Direct { semaphore }))
            }
            L0FlushConfig::Fail(msg) => Self(Arc::new(Inner::Fail(msg))),
        }
    }

    pub(crate) fn inner(&self) -> &Arc<Inner> {
        &self.0
    }

    pub(crate) fn prewarm_on_write(&self) -> ephemeral_file::PrewarmPageCacheOnWrite {
        match &*self.0 {
            Inner::PageCached => ephemeral_file::PrewarmPageCacheOnWrite::Yes,
            Inner::Direct { .. } | Inner::Fail(_) => ephemeral_file::PrewarmPageCacheOnWrite::No,
        }
    }
}

use std::{borrow::Cow, fmt::Debug, fmt::Display};

use tokio_util::sync::CancellationToken;
use utils::id::NodeId;

pub(crate) const MAX_RECONCILES_PER_OPERATION: usize = 64;

#[derive(Copy, Clone)]
pub(crate) struct Drain {
    pub(crate) node_id: NodeId,
}

#[derive(Copy, Clone)]
pub(crate) struct Fill {
    pub(crate) node_id: NodeId,
}

#[derive(Copy, Clone)]
pub(crate) enum Operation {
    Drain(Drain),
    Fill(Fill),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum OperationError {
    #[error("Node state changed during operation: {0}")]
    NodeStateChanged(Cow<'static, str>),
    #[error("Operation finalize error: {0}")]
    FinalizeError(Cow<'static, str>),
    #[error("Operation cancelled")]
    Cancelled,
}

pub(crate) struct OperationHandler {
    pub(crate) operation: Operation,
    #[allow(unused)]
    pub(crate) cancel: CancellationToken,
}

impl Display for Drain {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "drain {}", self.node_id)
    }
}

impl Display for Fill {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "fill {}", self.node_id)
    }
}

impl Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Operation::Drain(op) => write!(f, "{op}"),
            Operation::Fill(op) => write!(f, "{op}"),
        }
    }
}

//! Types representing protocols and actual agent-monitor messages.
//!
//! The pervasive use of serde modifiers throughout this module is to ease
//! serialization on the go side. Because go does not have enums (which model
//! messages well), it is harder to model messages, and we accomodate that with
//! serde.
//!
//! *Note*: the agent sends and receives messages in different ways.
//!
//! The agent serializes messages in the form and then sends them. The use
//! of `#[serde(tag = "type", content = "content")]` allows us to use `Type`
//! to determine how to deserialize `Content`.
//! ```ignore
//! struct {
//!     Content any
//!     Type    string
//!     Id      uint64
//! }
//! ```
//! and receives messages in the form:
//! ```ignore
//! struct {
//!     {fields embedded}
//!     Type string
//!     Id   uint64
//! }
//! ```
//! After reading the type field, the agent will decode the entire message
//! again, this time into the correct type using the embedded fields.
//! Because the agent cannot just extract the json contained in a certain field
//! (it initially deserializes to `map[string]interface{}`), we keep the fields
//! at the top level, so the entire piece of json can be deserialized into a struct,
//! such as a `DownscaleResult`, with the `Type` and `Id` fields ignored.

use core::fmt;
use std::cmp;

use serde::{de::Error, Deserialize, Serialize};

/// A Message we send to the agent.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct OutboundMsg {
    #[serde(flatten)]
    pub(crate) inner: OutboundMsgKind,
    pub(crate) id: usize,
}

impl OutboundMsg {
    pub fn new(inner: OutboundMsgKind, id: usize) -> Self {
        Self { inner, id }
    }
}

/// The different underlying message types we can send to the agent.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type")]
pub enum OutboundMsgKind {
    /// Indicates that the agent sent an invalid message, i.e, we couldn't
    /// properly deserialize it.
    InvalidMessage { error: String },
    /// Indicates that we experienced an internal error while processing a message.
    /// For example, if a cgroup operation fails while trying to handle an upscale,
    /// we return `InternalError`.
    InternalError { error: String },
    /// Returned to the agent once we have finished handling an upscale. If the
    /// handling was unsuccessful, an `InternalError` will get returned instead.
    /// *Note*: this is a struct variant because of the way go serializes struct{}
    UpscaleConfirmation {},
    /// Indicates to the monitor that we are urgently requesting resources.
    /// *Note*: this is a struct variant because of the way go serializes struct{}
    UpscaleRequest {},
    /// Returned to the agent once we have finished attempting to downscale. If
    /// an error occurred trying to do so, an `InternalError` will get returned instead.
    /// However, if we are simply unsuccessful (for example, do to needing the resources),
    /// that gets included in the `DownscaleResult`.
    DownscaleResult {
        // FIXME for the future (once the informant is deprecated)
        // As of the time of writing, the agent/informant version of this struct is
        // called api.DownscaleResult. This struct has uppercase fields which are
        // serialized as such. Thus, we serialize using uppercase names so we don't
        // have to make a breaking change to the agent<->informant protocol. Once
        // the informant has been superseded by the monitor, we can add the correct
        // struct tags to api.DownscaleResult without causing a breaking change,
        // since we don't need to support the agent<->informant protocol anymore.
        #[serde(rename = "Ok")]
        ok: bool,
        #[serde(rename = "Status")]
        status: String,
    },
    /// Part of the bidirectional heartbeat. The heartbeat is initiated by the
    /// agent.
    /// *Note*: this is a struct variant because of the way go serializes struct{}
    HealthCheck {},
}

/// A message received form the agent.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct InboundMsg {
    #[serde(flatten)]
    pub(crate) inner: InboundMsgKind,
    pub(crate) id: usize,
}

/// The different underlying message types we can receive from the agent.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "type", content = "content")]
pub enum InboundMsgKind {
    /// Indicates that the we sent an invalid message, i.e, we couldn't
    /// properly deserialize it.
    InvalidMessage { error: String },
    /// Indicates that the informan experienced an internal error while processing
    /// a message. For example, if it failed to request upsacle from the agent, it
    /// would return an `InternalError`.
    InternalError { error: String },
    /// Indicates to us that we have been granted more resources. We should respond
    /// with an `UpscaleConfirmation` when done handling the resources (increasins
    /// file cache size, cgorup memory limits).
    UpscaleNotification { granted: Resources },
    /// A request to reduce resource usage. We should response with a `DownscaleResult`,
    /// when done.
    DownscaleRequest { target: Resources },
    /// Part of the bidirectional heartbeat. The heartbeat is initiated by the
    /// agent.
    /// *Note*: this is a struct variant because of the way go serializes struct{}
    HealthCheck {},
}

/// Represents the resources granted to a VM.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
// Renamed because the agent has multiple resources types:
// `Resources` (milliCPU/memory slots)
// `Allocation` (vCPU/bytes) <- what we correspond to
#[serde(rename(serialize = "Allocation", deserialize = "Allocation"))]
pub struct Resources {
    /// Number of vCPUs
    pub(crate) cpu: f64,
    /// Bytes of memory
    pub(crate) mem: u64,
}

impl Resources {
    pub fn new(cpu: f64, mem: u64) -> Self {
        Self { cpu, mem }
    }
}

pub const PROTOCOL_MIN_VERSION: ProtocolVersion = ProtocolVersion::V1_0;
pub const PROTOCOL_MAX_VERSION: ProtocolVersion = ProtocolVersion::V1_0;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Ord, Eq, Serialize, Deserialize)]
pub struct ProtocolVersion(u8);

impl ProtocolVersion {
    /// Represents v1.0 of the agent<-> monitor protocol - the initial version
    ///
    /// Currently the latest version.
    const V1_0: ProtocolVersion = ProtocolVersion(1);
}

impl fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            ProtocolVersion(0) => f.write_str("<invalid: zero>"),
            ProtocolVersion::V1_0 => f.write_str("v1.0"),
            other => write!(f, "<unknown: {other}>"),
        }
    }
}

/// A set of protocol bounds that determines what we are speaking.
///
/// These bounds are inclusive.
#[derive(Debug)]
pub struct ProtocolRange {
    pub min: ProtocolVersion,
    pub max: ProtocolVersion,
}

// Use a custom deserialize impl to ensure that `self.min <= self.max`
impl<'de> Deserialize<'de> for ProtocolRange {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct InnerProtocolRange {
            min: ProtocolVersion,
            max: ProtocolVersion,
        }
        let InnerProtocolRange { min, max } = InnerProtocolRange::deserialize(deserializer)?;
        if min > max {
            Err(D::Error::custom(format!(
                "min version = {min} is greater than max version = {max}",
            )))
        } else {
            Ok(ProtocolRange { min, max })
        }
    }
}

impl fmt::Display for ProtocolRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.min == self.max {
            f.write_fmt(format_args!("{}", self.max))
        } else {
            f.write_fmt(format_args!("{} to {}", self.min, self.max))
        }
    }
}

impl ProtocolRange {
    /// Find the highest shared version between two `ProtocolRange`'s
    pub fn highest_shared_version(&self, other: &Self) -> anyhow::Result<ProtocolVersion> {
        // We first have to make sure the ranges are overlapping. Once we know
        // this, we can merge the ranges by taking the max of the mins and the
        // mins of the maxes.
        if self.min > other.max {
            anyhow::bail!(
                "Non-overlapping bounds: other.max = {} was less than self.min = {}",
                other.max,
                self.min,
            )
        } else if self.max < other.min {
            anyhow::bail!(
                "Non-overlappinng bounds: self.max = {} was less than other.min = {}",
                self.max,
                other.min
            )
        } else {
            Ok(cmp::min(self.max, other.max))
        }
    }
}

/// We send this to the monitor after negotiating which protocol to use
#[derive(Serialize, Debug)]
#[serde(rename_all = "camelCase")]
pub enum ProtocolResponse {
    Error(String),
    Version(ProtocolVersion),
}

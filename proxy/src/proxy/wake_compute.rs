use crate::config::RetryConfig;
use crate::console::messages::ConsoleError;
use crate::console::{errors::WakeComputeError, provider::CachedNodeInfo};
use crate::context::RequestMonitoring;
use crate::metrics::{
    ConnectOutcome, ConnectionFailuresBreakdownGroup, Metrics, RetriesMetricGroup, RetryType,
    WakeupFailureKind,
};
use crate::proxy::retry::{retry_after, should_retry};
use hyper1::StatusCode;
use tracing::{error, info, warn};

use super::connect_compute::ComputeConnectBackend;

pub async fn wake_compute<B: ComputeConnectBackend>(
    num_retries: &mut u32,
    ctx: &mut RequestMonitoring,
    api: &B,
    config: RetryConfig,
) -> Result<CachedNodeInfo, WakeComputeError> {
    let retry_type = RetryType::WakeCompute;
    loop {
        match api.wake_compute(ctx).await {
            Err(e) if !should_retry(&e, *num_retries, config) => {
                error!(error = ?e, num_retries, retriable = false, "couldn't wake compute node");
                report_error(&e, false);
                Metrics::get().proxy.retries_metric.observe(
                    RetriesMetricGroup {
                        outcome: ConnectOutcome::Failed,
                        retry_type,
                    },
                    (*num_retries).into(),
                );
                return Err(e);
            }
            Err(e) => {
                warn!(error = ?e, num_retries, retriable = true, "couldn't wake compute node");
                report_error(&e, true);
            }
            Ok(n) => {
                Metrics::get().proxy.retries_metric.observe(
                    RetriesMetricGroup {
                        outcome: ConnectOutcome::Success,
                        retry_type,
                    },
                    (*num_retries).into(),
                );
                info!(?num_retries, "compute node woken up after");
                return Ok(n);
            }
        }

        let wait_duration = retry_after(*num_retries, config);
        *num_retries += 1;
        let pause = ctx
            .latency_timer
            .pause(crate::metrics::Waiting::RetryTimeout);
        tokio::time::sleep(wait_duration).await;
        drop(pause);
    }
}

fn report_error(e: &WakeComputeError, retry: bool) {
    use crate::console::errors::ApiError;
    let kind = match e {
        WakeComputeError::BadComputeAddress(_) => WakeupFailureKind::BadComputeAddress,
        WakeComputeError::ApiError(ApiError::Transport(_)) => WakeupFailureKind::ApiTransportError,
        WakeComputeError::ApiError(ApiError::Console(e)) => match e.get_reason() {
            crate::console::messages::Reason::RoleProtected => {
                WakeupFailureKind::ApiConsoleBadRequest
            }
            crate::console::messages::Reason::ResourceNotFound => {
                WakeupFailureKind::ApiConsoleBadRequest
            }
            crate::console::messages::Reason::ProjectNotFound => {
                WakeupFailureKind::ApiConsoleBadRequest
            }
            crate::console::messages::Reason::EndpointNotFound => {
                WakeupFailureKind::ApiConsoleBadRequest
            }
            crate::console::messages::Reason::BranchNotFound => {
                WakeupFailureKind::ApiConsoleBadRequest
            }
            crate::console::messages::Reason::RateLimitExceeded => {
                WakeupFailureKind::ApiConsoleLocked
            }
            crate::console::messages::Reason::NonPrimaryBranchComputeTimeExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::ActiveTimeQuotaExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::ComputeTimeQuotaExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::WrittenDataQuotaExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::DataTransferQuotaExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::LogicalSizeQuotaExceeded => {
                WakeupFailureKind::QuotaExceeded
            }
            crate::console::messages::Reason::Unknown => match e {
                ConsoleError {
                    http_status_code: StatusCode::LOCKED,
                    ref error,
                    ..
                } if error.contains("written data quota exceeded")
                    || error.contains("the limit for current plan reached") =>
                {
                    WakeupFailureKind::QuotaExceeded
                }
                ConsoleError {
                    http_status_code: StatusCode::UNPROCESSABLE_ENTITY,
                    ref error,
                    ..
                } if error.contains("compute time quota of non-primary branches is exceeded") => {
                    WakeupFailureKind::QuotaExceeded
                }
                ConsoleError {
                    http_status_code: StatusCode::LOCKED,
                    ..
                } => WakeupFailureKind::ApiConsoleLocked,
                ConsoleError {
                    http_status_code: StatusCode::BAD_REQUEST,
                    ..
                } => WakeupFailureKind::ApiConsoleBadRequest,
                ConsoleError {
                    http_status_code, ..
                } if http_status_code.is_server_error() => {
                    WakeupFailureKind::ApiConsoleOtherServerError
                }
                ConsoleError { .. } => WakeupFailureKind::ApiConsoleOtherError,
            },
        },
        WakeComputeError::TooManyConnections => WakeupFailureKind::ApiConsoleLocked,
        WakeComputeError::TooManyConnectionAttempts(_) => WakeupFailureKind::TimeoutError,
    };
    Metrics::get()
        .proxy
        .connection_failures_breakdown
        .inc(ConnectionFailuresBreakdownGroup {
            kind,
            retry: retry.into(),
        });
}

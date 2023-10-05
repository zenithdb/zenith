use std::fmt::{Debug, Display};

use futures::Future;
use tokio_util::sync::CancellationToken;

pub const DEFAULT_BASE_BACKOFF_SECONDS: f64 = 0.1;
pub const DEFAULT_MAX_BACKOFF_SECONDS: f64 = 3.0;

pub async fn exponential_backoff(
    n: u32,
    base_increment: f64,
    max_seconds: f64,
    cancel: &CancellationToken,
) {
    let backoff_duration_seconds =
        exponential_backoff_duration_seconds(n, base_increment, max_seconds);
    if backoff_duration_seconds > 0.0 {
        tracing::info!(
            "Backoff: waiting {backoff_duration_seconds} seconds before processing with the task",
        );

        drop(
            tokio::time::timeout(
                std::time::Duration::from_secs_f64(backoff_duration_seconds),
                cancel.cancelled(),
            )
            .await,
        )
    }
}

pub fn exponential_backoff_duration_seconds(n: u32, base_increment: f64, max_seconds: f64) -> f64 {
    if n == 0 {
        0.0
    } else {
        (1.0 + base_increment).powf(f64::from(n)).min(max_seconds)
    }
}

/// Configure cancellation for a retried operation: when to cancel (the token), and
/// what kind of error to return on cancellation
pub struct Cancel<E, CF>
where
    E: Display + Debug + 'static,
    CF: Fn() -> E,
{
    token: CancellationToken,
    on_cancel: CF,
}

impl<E, CF> Cancel<E, CF>
where
    E: Display + Debug + 'static,
    CF: Fn() -> E,
{
    pub fn new(token: CancellationToken, on_cancel: CF) -> Self {
        Self { token, on_cancel }
    }
}

/// retries passed operation until one of the following conditions are met:
/// Encountered error is considered as permanent (non-retryable)
/// Retries have been exhausted.
/// `is_permanent` closure should be used to provide distinction between permanent/non-permanent errors
/// When attempts cross `warn_threshold` function starts to emit log warnings.
/// `description` argument is added to log messages. Its value should identify the `op` is doing
/// `cancel` argument is required: any time we are looping on retry, we should be using a CancellationToken
/// to drop out promptly on shutdown.
pub async fn retry<T, O, F, E, CF>(
    mut op: O,
    is_permanent: impl Fn(&E) -> bool,
    warn_threshold: u32,
    max_retries: u32,
    description: &str,
    cancel: Cancel<E, CF>,
) -> Result<T, E>
where
    // Not std::error::Error because anyhow::Error doesn't implement it.
    // For context see https://github.com/dtolnay/anyhow/issues/63
    E: Display + Debug + 'static,
    O: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
    CF: Fn() -> E,
{
    let mut attempts = 0;
    loop {
        if cancel.token.is_cancelled() {
            return Err((cancel.on_cancel)());
        }

        let result = op().await;
        match result {
            Ok(_) => {
                if attempts > 0 {
                    tracing::info!("{description} succeeded after {attempts} retries");
                }
                return result;
            }

            // These are "permanent" errors that should not be retried.
            Err(ref e) if is_permanent(e) => {
                return result;
            }
            // Assume that any other failure might be transient, and the operation might
            // succeed if we just keep trying.
            Err(err) if attempts < warn_threshold => {
                tracing::info!("{description} failed, will retry (attempt {attempts}): {err:#}");
            }
            Err(err) if attempts < max_retries => {
                tracing::warn!("{description} failed, will retry (attempt {attempts}): {err:#}");
            }
            Err(ref err) => {
                // Operation failed `max_attempts` times. Time to give up.
                tracing::warn!(
                    "{description} still failed after {attempts} retries, giving up: {err:?}"
                );
                return result;
            }
        }
        // sleep and retry
        exponential_backoff(
            attempts,
            DEFAULT_BASE_BACKOFF_SECONDS,
            DEFAULT_MAX_BACKOFF_SECONDS,
            &cancel.token,
        )
        .await;
        attempts += 1;
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use tokio::sync::Mutex;

    use super::*;

    #[test]
    fn backoff_defaults_produce_growing_backoff_sequence() {
        let mut current_backoff_value = None;

        for i in 0..10_000 {
            let new_backoff_value = exponential_backoff_duration_seconds(
                i,
                DEFAULT_BASE_BACKOFF_SECONDS,
                DEFAULT_MAX_BACKOFF_SECONDS,
            );

            if let Some(old_backoff_value) = current_backoff_value.replace(new_backoff_value) {
                assert!(
                    old_backoff_value <= new_backoff_value,
                    "{i}th backoff value {new_backoff_value} is smaller than the previous one {old_backoff_value}"
                )
            }
        }

        assert_eq!(
            current_backoff_value.expect("Should have produced backoff values to compare"),
            DEFAULT_MAX_BACKOFF_SECONDS,
            "Given big enough of retries, backoff should reach its allowed max value"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn retry_always_error() {
        let count = Mutex::new(0);
        let err_result = retry(
            || async {
                *count.lock().await += 1;
                Result::<(), io::Error>::Err(io::Error::from(io::ErrorKind::Other))
            },
            |_e| false,
            1,
            1,
            "work",
            Cancel::new(CancellationToken::new(), || -> io::Error { unreachable!() }),
        )
        .await;

        assert!(err_result.is_err());

        assert_eq!(*count.lock().await, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn retry_ok_after_err() {
        let count = Mutex::new(0);
        retry(
            || async {
                let mut locked = count.lock().await;
                if *locked > 1 {
                    Ok(())
                } else {
                    *locked += 1;
                    Err(io::Error::from(io::ErrorKind::Other))
                }
            },
            |_e| false,
            2,
            2,
            "work",
            Cancel::new(CancellationToken::new(), || -> io::Error { unreachable!() }),
        )
        .await
        .unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn dont_retry_permanent_errors() {
        let count = Mutex::new(0);
        let _ = retry(
            || async {
                let mut locked = count.lock().await;
                if *locked > 1 {
                    Ok(())
                } else {
                    *locked += 1;
                    Err(io::Error::from(io::ErrorKind::Other))
                }
            },
            |_e| true,
            2,
            2,
            "work",
            Cancel::new(CancellationToken::new(), || -> io::Error { unreachable!() }),
        )
        .await
        .unwrap_err();

        assert_eq!(*count.lock().await, 1);
    }
}

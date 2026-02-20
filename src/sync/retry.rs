/// Database operation retry utility with exponential backoff
///
/// This module provides retry logic for critical database operations to handle
/// transient failures such as database locks, network hiccups, or temporary I/O errors.
///
/// The retry strategy uses exponential backoff: 50ms, 100ms, 200ms, 400ms, 800ms
use std::time::Duration;
use tokio::time::sleep;

/// Retry a database operation with exponential backoff
///
/// # Arguments
/// * `operation` - The async database operation to retry
/// * `max_retries` - Maximum number of retry attempts (typically 5)
/// * `description` - Human-readable description for logging
///
/// # Returns
/// * `Ok(T)` - The successful result from the operation
/// * `Err(sqlx::Error)` - The final error after all retries exhausted
///
/// # Backoff Strategy
/// - Attempt 1: Execute immediately
/// - Attempt 2: Wait 50ms
/// - Attempt 3: Wait 100ms
/// - Attempt 4: Wait 200ms
/// - Attempt 5: Wait 400ms
/// - Attempt 6: Wait 800ms
///
/// # Example
/// ```rust
/// use crate::sync::retry::retry_db_operation;
///
/// let result = retry_db_operation(
///     || async {
///         sqlx::query("UPDATE containers SET state = ? WHERE id = ?")
///             .bind("exited")
///             .bind(&container_id)
///             .execute(&pool)
///             .await
///     },
///     5,
///     "update_container_state"
/// ).await?;
/// ```
pub async fn retry_db_operation<F, T, Fut>(
    mut operation: F,
    max_retries: u32,
    description: &str,
) -> Result<T, sqlx::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
{
    let mut retries = 0;

    loop {
        match operation().await {
            Ok(result) => {
                if retries > 0 {
                    tracing::info!(
                        "Database operation '{}' succeeded after {} retries",
                        description,
                        retries
                    );
                }
                return Ok(result);
            }
            Err(e) if retries < max_retries => {
                retries += 1;
                let backoff_ms = 50 * (1 << retries); // Exponential: 100, 200, 400, 800, 1600ms

                tracing::warn!(
                    "Database operation '{}' failed (attempt {}/{}): {}. Retrying in {}ms",
                    description,
                    retries,
                    max_retries + 1,
                    e,
                    backoff_ms
                );

                sleep(Duration::from_millis(backoff_ms)).await;
            }
            Err(e) => {
                tracing::error!(
                    "Database operation '{}' failed after {} retries: {}",
                    description,
                    max_retries,
                    e
                );
                return Err(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_succeeds_immediately() {
        let result = retry_db_operation(
            || async { Ok::<i32, sqlx::Error>(42) },
            5,
            "test_immediate_success",
        )
        .await;

        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_retry_succeeds_after_failures() {
        let attempt_counter = Arc::new(AtomicU32::new(0));
        let counter_clone = attempt_counter.clone();

        let result = retry_db_operation(
            || {
                let counter = counter_clone.clone();
                async move {
                    let attempt = counter.fetch_add(1, Ordering::SeqCst);
                    if attempt < 2 {
                        // Simulate database error
                        Err(sqlx::Error::Io(std::io::Error::other("database is locked")))
                    } else {
                        Ok(100)
                    }
                }
            },
            5,
            "test_retry_success",
        )
        .await;

        assert_eq!(result.unwrap(), 100);
        assert_eq!(attempt_counter.load(Ordering::SeqCst), 3); // Failed twice, succeeded on third
    }

    #[tokio::test]
    async fn test_retry_exhausts_attempts() {
        let result = retry_db_operation(
            || async {
                Err::<i32, sqlx::Error>(sqlx::Error::Io(std::io::Error::other("persistent error")))
            },
            3,
            "test_exhausted_retries",
        )
        .await;

        assert!(result.is_err());
    }
}

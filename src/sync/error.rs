#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Container not found: {container_id}")]
    NotFound { container_id: String },

    // NetworkAllocation variant removed - not used
    #[error("State transition invalid: {from} -> {to}")]
    InvalidStateTransition { from: String, to: String },

    // ProcessMonitoring variant removed - not used
    #[error("Cleanup operation failed: {resource_type} at {path}: {message}")]
    CleanupFailed {
        resource_type: String,
        path: String,
        message: String,
    },

    #[error("No available IP addresses in range")]
    NoAvailableIp,

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("System time error: {0}")]
    SystemTime(#[from] std::time::SystemTimeError),

    #[error("Resource validation failed: {message}")]
    ValidationFailed { message: String },
}

pub type SyncResult<T> = Result<T, SyncError>;

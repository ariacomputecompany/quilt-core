use std::fmt;

/// Network-specific error type for all networking operations
#[derive(Debug)]
pub enum NetworkError {
    /// Netlink operation failed
    Netlink(rtnetlink::Error),
    /// System I/O error
    Io(std::io::Error),
    /// Shell command execution failed
    Command { cmd: String, stderr: String },
    /// Input validation failed
    Validation(String),
    /// Operation timed out
    Timeout(String),
    /// Resource not found (interface, bridge, etc.)
    NotFound(String),
    /// Resource already exists
    AlreadyExists(String),
    /// Namespace operation failed
    Namespace(String),
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkError::Netlink(e) => write!(f, "netlink error: {}", e),
            NetworkError::Io(e) => write!(f, "io error: {}", e),
            NetworkError::Command { cmd, stderr } => {
                write!(f, "command '{}' failed: {}", cmd, stderr)
            }
            NetworkError::Validation(msg) => write!(f, "validation error: {}", msg),
            NetworkError::Timeout(msg) => write!(f, "timeout: {}", msg),
            NetworkError::NotFound(msg) => write!(f, "not found: {}", msg),
            NetworkError::AlreadyExists(msg) => write!(f, "already exists: {}", msg),
            NetworkError::Namespace(msg) => write!(f, "namespace error: {}", msg),
        }
    }
}

impl std::error::Error for NetworkError {}

impl From<rtnetlink::Error> for NetworkError {
    fn from(e: rtnetlink::Error) -> Self {
        NetworkError::Netlink(e)
    }
}

impl From<std::io::Error> for NetworkError {
    fn from(e: std::io::Error) -> Self {
        NetworkError::Io(e)
    }
}

/// Convert NetworkError to String for backwards compatibility with existing code
impl From<NetworkError> for String {
    fn from(e: NetworkError) -> Self {
        e.to_string()
    }
}

pub type NetworkResult<T> = Result<T, NetworkError>;

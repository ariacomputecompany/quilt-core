//! OCI Image Configuration Parsing
//!
//! Parses the image configuration JSON blob that defines:
//! - Default runtime configuration (user, env, cmd, entrypoint)
//! - Filesystem layer information
//! - Image metadata and history
//!
//! See: https://github.com/opencontainers/image-spec/blob/main/config.md

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// OCI Image Configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciImageConfig {
    /// CPU architecture (e.g., "amd64", "arm64")
    pub architecture: String,

    /// Operating system (e.g., "linux")
    pub os: String,

    /// OS version (optional, mainly for Windows)
    #[serde(rename = "os.version", skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,

    /// Runtime configuration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config: Option<OciContainerConfig>,

    /// Layer DiffIDs (uncompressed layer digests)
    pub rootfs: RootFs,

    /// History entries
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history: Option<Vec<History>>,

    /// Image creation timestamp (RFC 3339)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<String>,

    /// Author of the image
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
}

// OciImageConfig is used via direct field access through serde deserialization

/// Container runtime configuration
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OciContainerConfig {
    /// User to run as (may be "user", "user:group", "uid", or "uid:gid")
    #[serde(rename = "User", skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,

    /// Exposed ports (e.g., {"8080/tcp": {}})
    #[serde(rename = "ExposedPorts", skip_serializing_if = "Option::is_none")]
    pub exposed_ports: Option<HashMap<String, EmptyObject>>,

    /// Environment variables (["KEY=VALUE", ...])
    #[serde(rename = "Env", skip_serializing_if = "Option::is_none")]
    pub env: Option<Vec<String>>,

    /// Entrypoint command
    #[serde(rename = "Entrypoint", skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,

    /// Default command (appended to entrypoint)
    #[serde(rename = "Cmd", skip_serializing_if = "Option::is_none")]
    pub cmd: Option<Vec<String>>,

    /// Volumes (mount points)
    #[serde(rename = "Volumes", skip_serializing_if = "Option::is_none")]
    pub volumes: Option<HashMap<String, EmptyObject>>,

    /// Working directory
    #[serde(rename = "WorkingDir", skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,

    /// Labels (key-value metadata)
    #[serde(rename = "Labels", skip_serializing_if = "Option::is_none")]
    pub labels: Option<HashMap<String, String>>,

    /// Stop signal (e.g., "SIGTERM")
    #[serde(rename = "StopSignal", skip_serializing_if = "Option::is_none")]
    pub stop_signal: Option<String>,

    /// Healthcheck configuration
    #[serde(rename = "Healthcheck", skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<HealthcheckConfig>,

    /// Shell to use for RUN commands
    #[serde(rename = "Shell", skip_serializing_if = "Option::is_none")]
    pub shell: Option<Vec<String>>,

    /// Whether to attach stdin
    #[serde(rename = "AttachStdin", skip_serializing_if = "Option::is_none")]
    pub attach_stdin: Option<bool>,

    /// Whether to attach stdout
    #[serde(rename = "AttachStdout", skip_serializing_if = "Option::is_none")]
    pub attach_stdout: Option<bool>,

    /// Whether to attach stderr
    #[serde(rename = "AttachStderr", skip_serializing_if = "Option::is_none")]
    pub attach_stderr: Option<bool>,

    /// Whether stdin is open
    #[serde(rename = "OpenStdin", skip_serializing_if = "Option::is_none")]
    pub open_stdin: Option<bool>,

    /// Whether to allocate TTY
    #[serde(rename = "Tty", skip_serializing_if = "Option::is_none")]
    pub tty: Option<bool>,
}

/// Empty JSON object {} - used for ports and volumes
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmptyObject {}

/// Healthcheck configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthcheckConfig {
    /// Test command
    #[serde(rename = "Test")]
    pub test: Vec<String>,

    /// Interval between checks (nanoseconds)
    #[serde(rename = "Interval", skip_serializing_if = "Option::is_none")]
    pub interval: Option<u64>,

    /// Timeout for a single check (nanoseconds)
    #[serde(rename = "Timeout", skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,

    /// Number of retries before marking unhealthy
    #[serde(rename = "Retries", skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,

    /// Start period (nanoseconds)
    #[serde(rename = "StartPeriod", skip_serializing_if = "Option::is_none")]
    pub start_period: Option<u64>,
}

/// Rootfs layer information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootFs {
    /// Type (always "layers")
    #[serde(rename = "type")]
    pub fs_type: String,

    /// Layer DiffIDs (uncompressed content digests)
    pub diff_ids: Vec<String>,
}

/// Image history entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct History {
    /// Creation timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created: Option<String>,

    /// Author
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,

    /// Command that created this layer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_by: Option<String>,

    /// Comment
    #[serde(skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Whether this is an empty (metadata-only) layer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub empty_layer: Option<bool>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_config() {
        let json = r#"{
            "architecture": "amd64",
            "os": "linux",
            "config": {
                "User": "1000:1000",
                "Env": ["PATH=/usr/local/bin:/usr/bin:/bin", "HOME=/home/user"],
                "Cmd": ["/bin/sh"],
                "WorkingDir": "/app"
            },
            "rootfs": {
                "type": "layers",
                "diff_ids": ["sha256:abc123"]
            }
        }"#;

        let config: OciImageConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.architecture, "amd64");
        assert_eq!(config.os, "linux");

        let container_config = config.config.unwrap();
        assert_eq!(container_config.user, Some("1000:1000".to_string()));
        assert_eq!(container_config.working_dir, Some("/app".to_string()));
    }
}

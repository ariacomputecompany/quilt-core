// Common utilities for CLI command handlers
// Shared functionality for container resolution, validation, etc.

use crate::quilt::quilt_service_client::QuiltServiceClient;
use crate::quilt::GetContainerByNameRequest;
use tonic::transport::Channel;

/// Check if a string looks like a UUID (container ID)
fn is_uuid_format(s: &str) -> bool {
    // UUID format: 8-4-4-4-12 hex digits with dashes
    // Example: 80589972-33f7-481e-a3d9-fc167f770f02
    s.len() == 36 && s.chars().enumerate().all(|(i, c)| {
        match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        }
    })
}

/// Resolve a container identifier to a container ID
/// Supports both container IDs and container names
/// Automatically detects whether input is a UUID (ID) or a name
pub async fn resolve_container_id(
    client: &mut QuiltServiceClient<Channel>,
    container: &Option<String>,
    by_name: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    let container_str = match container {
        Some(c) => c.clone(),
        None => {
            // TODO: Interactive selection when integrated
            return Err("Container ID or name required".into());
        }
    };

    // Smart detection: if it looks like a UUID, treat as ID, otherwise treat as name
    let auto_detect_by_name = !is_uuid_format(&container_str);
    let use_name_lookup = if by_name {
        // Explicit by_name request
        true
    } else {
        // Auto-detect based on format
        auto_detect_by_name
    };

    if use_name_lookup {
        // Resolve name to ID via gRPC
        let request = tonic::Request::new(GetContainerByNameRequest {
            name: container_str.clone(),
        });

        match client.get_container_by_name(request).await {
            Ok(response) => {
                let res = response.into_inner();
                if res.found {
                    Ok(res.container_id)
                } else {
                    Err(format!("Container '{}' not found", container_str).into())
                }
            }
            Err(e) => Err(format!("Failed to resolve container name: {}", e).into()),
        }
    } else {
        // Use as ID directly
        Ok(container_str)
    }
}

/// Format a timestamp into a human-readable string
pub fn format_timestamp(timestamp: u64) -> String {
    use chrono::{DateTime, Utc};

    if timestamp == 0 {
        return "N/A".to_string();
    }

    let dt = DateTime::<Utc>::from_timestamp(timestamp as i64, 0)
        .unwrap_or_else(|| Utc::now());

    dt.format("%Y-%m-%d %H:%M:%S UTC").to_string()
}

/// Format bytes into human-readable size
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];

    if bytes == 0 {
        return "0 B".to_string();
    }

    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    format!("{:.2} {}", size, UNITS[unit_idx])
}

/// Validate container ID format (basic validation)
#[allow(dead_code)]
pub fn validate_container_id(id: &str) -> Result<(), String> {
    if id.is_empty() {
        return Err("Container ID cannot be empty".to_string());
    }

    if id.len() > 256 {
        return Err("Container ID too long".to_string());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_uuid_format() {
        // Valid UUIDs
        assert!(is_uuid_format("80589972-33f7-481e-a3d9-fc167f770f02"));
        assert!(is_uuid_format("00000000-0000-0000-0000-000000000000"));
        assert!(is_uuid_format("ffffffff-ffff-ffff-ffff-ffffffffffff"));

        // Invalid UUIDs (should be treated as names)
        assert!(!is_uuid_format("test"));
        assert!(!is_uuid_format("mycontainer"));
        assert!(!is_uuid_format("test-container"));
        assert!(!is_uuid_format("container_123"));
        assert!(!is_uuid_format(""));
        assert!(!is_uuid_format("80589972")); // Too short
        assert!(!is_uuid_format("80589972-33f7-481e-a3d9-fc167f770f02-extra")); // Too long
        assert!(!is_uuid_format("80589972_33f7_481e_a3d9_fc167f770f02")); // Wrong separator
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(512), "512.00 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }

    #[test]
    fn test_validate_container_id() {
        assert!(validate_container_id("valid-id").is_ok());
        assert!(validate_container_id("").is_err());
        assert!(validate_container_id(&"x".repeat(300)).is_err());
    }
}

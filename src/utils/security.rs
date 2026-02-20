use super::validation::{MountType, VolumeMount};
use std::path::Path;

pub struct SecurityValidator;

impl SecurityValidator {
    /// Validate mount source path for security issues
    pub fn validate_mount_source(path: &str, mount_type: MountType) -> Result<(), String> {
        match mount_type {
            MountType::Bind => {
                // Prevent path traversal
                if path.contains("..") {
                    return Err("Path traversal detected".to_string());
                }

                // Check if path exists
                let path_obj = Path::new(path);
                if !path_obj.exists() {
                    return Err(format!("Mount source path does not exist: {}", path));
                }

                // Deny sensitive system paths
                const DENIED_PATHS: &[&str] = &[
                    "/etc/passwd",
                    "/etc/shadow",
                    "/etc/sudoers",
                    "/proc",
                    "/sys",
                    "/dev",
                    "/boot",
                    "/root/.ssh",
                ];

                let canonical = match path_obj.canonicalize() {
                    Ok(p) => p,
                    Err(_) => return Err(format!("Cannot resolve path: {}", path)),
                };

                let canonical_str = canonical.to_string_lossy();
                for denied in DENIED_PATHS {
                    if canonical_str.starts_with(denied) {
                        return Err(format!("Security: Mounting {} is not allowed", denied));
                    }
                }
            }
            MountType::Volume => {
                // Volume names are handled by the volume manager
                if path.is_empty() || path.len() > 64 {
                    return Err("Invalid volume name".to_string());
                }
            }
            MountType::Tmpfs => {
                // No source validation needed for tmpfs
            }
        }

        Ok(())
    }

    /// Validate mount target path
    pub fn validate_mount_target(path: &str) -> Result<(), String> {
        // Must be absolute path
        if !path.starts_with('/') {
            return Err("Mount target must be an absolute path".to_string());
        }

        // Prevent path traversal
        if path.contains("..") {
            return Err("Path traversal detected".to_string());
        }

        // Prevent mounting over critical container paths
        const PROTECTED_PATHS: &[&str] = &[
            "/", "/bin", "/sbin", "/lib", "/lib64", "/usr", "/proc", "/sys", "/dev", "/etc",
        ];

        for protected in PROTECTED_PATHS {
            if path == *protected || (path.len() > 1 && path.trim_end_matches('/') == *protected) {
                return Err(format!("Cannot mount over protected path: {}", protected));
            }
        }

        Ok(())
    }

    /// Main mount validation entry point
    pub fn validate_mount(mount: &VolumeMount) -> Result<(), String> {
        // Validate source
        Self::validate_mount_source(&mount.source, mount.mount_type.clone())?;

        // Validate target
        Self::validate_mount_target(&mount.target)?;

        // Additional validation for specific mount types
        match mount.mount_type {
            MountType::Tmpfs => {
                // Validate tmpfs options
                if let Some(size) = mount.options.get("size") {
                    Self::validate_tmpfs_size(size)?;
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Validate tmpfs size option
    fn validate_tmpfs_size(size: &str) -> Result<(), String> {
        // Parse size with units (e.g., "100m", "1g")
        let size_lower = size.to_lowercase();
        let (number_str, multiplier) = if size_lower.ends_with("g") {
            (&size_lower[..size_lower.len() - 1], 1024 * 1024 * 1024)
        } else if size_lower.ends_with("m") {
            (&size_lower[..size_lower.len() - 1], 1024 * 1024)
        } else if size_lower.ends_with("k") {
            (&size_lower[..size_lower.len() - 1], 1024)
        } else {
            return Err("Tmpfs size must include unit (k, m, or g)".to_string());
        };

        let number: u64 = number_str
            .parse()
            .map_err(|_| format!("Invalid tmpfs size: {}", size))?;

        let bytes = number * multiplier;

        // Minimum 1MB
        if bytes < 1024 * 1024 {
            return Err("Tmpfs size must be at least 1m".to_string());
        }

        // Maximum 10GB to prevent resource exhaustion
        if bytes > 10 * 1024 * 1024 * 1024 {
            return Err("Tmpfs size cannot exceed 10g".to_string());
        }

        Ok(())
    }
}

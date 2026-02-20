//! Unified constants module for Quilt
//!
//! This module centralizes all common constants to prevent duplication
//! and ensure consistency across the codebase.

// ============================================================================
// Size Constants (used by format_bytes in main.rs and qcli/output.rs)
// ============================================================================

/// One kilobyte in bytes (1024 bytes)
pub const KB: u64 = 1024;

/// One megabyte in bytes (1024 KB)
pub const MB: u64 = KB * 1024;

/// One gigabyte in bytes (1024 MB)
pub const GB: u64 = MB * 1024;

// ============================================================================
// Path Constants
// ============================================================================

/// Base directory for container rootfs on the host filesystem.
/// Must NOT be under /tmp — systemd-tmpfiles-clean will wipe it.
pub const CONTAINER_BASE_DIR: &str = "/var/lib/quilt/containers";

// ============================================================================
// Security Constants — single source of truth for all path blocklists
// ============================================================================

/// Sensitive host paths that must never be bind-mounted into containers.
/// Used by mount source validation.
pub const BLOCKED_MOUNT_SOURCES: &[&str] = &[
    "/etc/passwd",
    "/etc/shadow",
    "/etc/sudoers",
    "/proc",
    "/sys",
    "/dev",
    "/boot",
    "/root",
    "/root/.ssh",
    "/var/run",
    "/var/log",
];

/// Critical container paths that must not be overwritten by user mounts.
/// Used by mount target validation.
pub const PROTECTED_MOUNT_TARGETS: &[&str] = &[
    "/", "/bin", "/sbin", "/lib", "/lib64", "/usr", "/proc", "/sys", "/dev", "/etc",
];

/// Sensitive paths that must not be used as working directories.
/// A superset of mount sources — includes paths that are dangerous to `chdir` into.
pub const BLOCKED_WORKING_DIRS: &[&str] = &[
    "/proc",
    "/sys",
    "/dev",
    "/etc/passwd",
    "/etc/shadow",
    "/etc/sudoers",
    "/root",
    "/var/run",
    "/var/log",
    "/boot",
];

/// Environment variables that must never be overridden by user input.
pub const PROTECTED_ENV_VARS: &[&str] = &[
    "PATH",
    "HOME",
    "USER",
    "SHELL",
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
];

// ============================================================================
// Resource Limit Constants
// ============================================================================

/// Minimum memory limit in MB — must match cgroup enforcement floor.
/// Below this, containers fail to fork/exec reliably.
pub const MIN_MEMORY_LIMIT_MB: u64 = 256;

/// Maximum memory limit in MB (16 GB).
pub const MAX_MEMORY_LIMIT_MB: u64 = 16384;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_constants() {
        assert_eq!(KB, 1024);
        assert_eq!(MB, 1024 * 1024);
        assert_eq!(GB, 1024 * 1024 * 1024);
    }
}

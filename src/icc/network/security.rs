// Security module for network operations
// Contains security-critical validation and isolation verification functions

use crate::utils::command::CommandExecutor;
use crate::utils::console::ConsoleLogger;
use crate::utils::filesystem::FileSystemUtils;

/// Security validation and isolation checking for container networking
pub struct NetworkSecurity {
    pub bridge_ip: String,
}

impl NetworkSecurity {
    pub fn new(bridge_ip: String) -> Self {
        Self { bridge_ip }
    }

    /// SECURITY CRITICAL: Validate that container namespace exists and is accessible
    /// This prevents nsenter commands from falling back to host execution
    pub fn validate_container_namespace(&self, container_pid: i32) -> bool {
        // Check if PID exists and is a valid container process
        let pid_check = format!(
            "test -d /proc/{} && cat /proc/{}/comm | grep -q quilt",
            container_pid, container_pid
        );
        if let Ok(result) = CommandExecutor::execute_shell(&pid_check) {
            if !result.success {
                ConsoleLogger::warning(&format!("🚨 [SECURITY] Container PID {} validation failed - process not found or invalid", container_pid));
                return false;
            }
        } else {
            ConsoleLogger::warning(&format!(
                "🚨 [SECURITY] Container PID {} validation check failed",
                container_pid
            ));
            return false;
        }

        // Test namespace entry without dangerous operations
        let ns_test = format!(
            "nsenter -t {} -m -p -- echo 'namespace_test_ok'",
            container_pid
        );
        match CommandExecutor::execute_shell(&ns_test) {
            Ok(result) => {
                if result.success && result.stdout.trim() == "namespace_test_ok" {
                    return true;
                } else {
                    ConsoleLogger::warning(&format!(
                        "🚨 [SECURITY] Namespace entry test failed for PID {}: {}",
                        container_pid, result.stderr
                    ));
                    return false;
                }
            }
            Err(e) => {
                ConsoleLogger::warning(&format!(
                    "🚨 [SECURITY] Failed to test namespace entry for PID {}: {}",
                    container_pid, e
                ));
                return false;
            }
        }
    }

    /// SECURITY CRITICAL: Verify DNS changes only affected container, not host
    pub fn verify_dns_container_isolation(
        &self,
        container_pid: i32,
        _expected_content: &str,
    ) -> bool {
        // Check host DNS was not modified
        if let Ok(host_resolv) = FileSystemUtils::read_file("/etc/resolv.conf") {
            if host_resolv.contains(&self.bridge_ip.to_string()) {
                ConsoleLogger::error("🚨 [SECURITY BREACH] Host /etc/resolv.conf was modified by container DNS operation!");
                return false;
            }
        }

        // Verify container DNS contains expected content
        let verify_cmd = format!("nsenter -t {} -m -p -- cat /etc/resolv.conf", container_pid);
        if let Ok(result) = CommandExecutor::execute_shell(&verify_cmd) {
            if result.success && result.stdout.contains(&self.bridge_ip.to_string()) {
                return true;
            }
        }

        ConsoleLogger::warning(&format!(
            "🚨 [SECURITY] Could not verify container DNS isolation for PID {}",
            container_pid
        ));
        false
    }

    /// SECURITY CRITICAL: Validate rootfs path to prevent directory traversal attacks
    pub fn validate_rootfs_path(&self, rootfs_path: &str) -> Result<(), String> {
        // Validate rootfs path is within expected container directory
        if !rootfs_path.starts_with("/tmp/quilt-containers/") {
            return Err(format!("🚨 [SECURITY] Unsafe rootfs path: {}", rootfs_path));
        }

        // Additional validation: ensure no path traversal attempts
        if rootfs_path.contains("../") || rootfs_path.contains("/..") {
            return Err(format!(
                "🚨 [SECURITY] Path traversal attempt detected: {}",
                rootfs_path
            ));
        }

        // Check that the path actually exists and is a directory
        match std::fs::metadata(rootfs_path) {
            Ok(metadata) if metadata.is_dir() => Ok(()),
            Ok(_) => Err(format!(
                "🚨 [SECURITY] Rootfs path is not a directory: {}",
                rootfs_path
            )),
            Err(e) => Err(format!(
                "🚨 [SECURITY] Cannot access rootfs path {}: {}",
                rootfs_path, e
            )),
        }
    }

    /// SECURITY CRITICAL: Validate container ID format to prevent injection attacks
    pub fn validate_container_id(&self, container_id: &str) -> Result<(), String> {
        // Container ID should be alphanumeric and reasonable length
        if container_id.is_empty() {
            return Err("🚨 [SECURITY] Container ID cannot be empty".to_string());
        }

        if container_id.len() > 64 {
            return Err(format!(
                "🚨 [SECURITY] Container ID too long: {}",
                container_id.len()
            ));
        }

        // Allow alphanumeric characters and hyphens only
        if !container_id
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            return Err(format!(
                "🚨 [SECURITY] Container ID contains invalid characters: {}",
                container_id
            ));
        }

        Ok(())
    }

    /// SECURITY CRITICAL: Validate network interface names to prevent injection
    pub fn validate_interface_name(&self, interface_name: &str) -> Result<(), String> {
        if interface_name.is_empty() {
            return Err("🚨 [SECURITY] Interface name cannot be empty".to_string());
        }

        if interface_name.len() > 15 {
            // Linux interface name limit
            return Err(format!(
                "🚨 [SECURITY] Interface name too long: {}",
                interface_name
            ));
        }

        // Interface names should be alphanumeric
        if !interface_name.chars().all(|c| c.is_alphanumeric()) {
            return Err(format!(
                "🚨 [SECURITY] Interface name contains invalid characters: {}",
                interface_name
            ));
        }

        Ok(())
    }

    /// SECURITY CRITICAL: Validate IP addresses to prevent injection
    pub fn validate_ip_address(&self, ip_address: &str) -> Result<(), String> {
        // Basic IP address format validation
        match ip_address.parse::<std::net::Ipv4Addr>() {
            Ok(_) => Ok(()),
            Err(_) => Err(format!(
                "🚨 [SECURITY] Invalid IP address format: {}",
                ip_address
            )),
        }
    }

    /// SECURITY CRITICAL: Validate PID to ensure it's reasonable
    pub fn validate_container_pid(&self, pid: i32) -> Result<(), String> {
        if pid <= 0 {
            return Err(format!("🚨 [SECURITY] Invalid PID: {}", pid));
        }

        if pid > 4194304 {
            // Linux PID_MAX_LIMIT
            return Err(format!("🚨 [SECURITY] PID too large: {}", pid));
        }

        // Check if PID actually exists
        if !std::path::Path::new(&format!("/proc/{}", pid)).exists() {
            return Err(format!("🚨 [SECURITY] PID {} does not exist", pid));
        }

        Ok(())
    }

    /// SECURITY CRITICAL: Sanitize command arguments to prevent shell injection
    pub fn sanitize_shell_argument(&self, arg: &str) -> String {
        // Remove or escape dangerous characters
        arg.chars()
            .filter(|c| c.is_alphanumeric() || matches!(*c, '.' | '-' | '_' | '/' | ':'))
            .collect()
    }

    /// SECURITY CRITICAL: Validate that a command doesn't contain injection attempts
    pub fn validate_safe_command(&self, command: &str) -> Result<(), String> {
        // Check for common injection patterns
        let dangerous_patterns = vec![
            ";", "&&", "||", "|", "`", "$", "$(", "${", "rm ", "dd ", "> ", ">>", "< ", "<<",
        ];

        for pattern in dangerous_patterns {
            if command.contains(pattern) {
                return Err(format!(
                    "🚨 [SECURITY] Command contains dangerous pattern '{}': {}",
                    pattern, command
                ));
            }
        }

        Ok(())
    }

    /// SECURITY CRITICAL: Audit log security-sensitive operations
    pub fn audit_network_operation(&self, operation: &str, container_id: &str, details: &str) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        ConsoleLogger::info(&format!(
            "🔒 [SECURITY-AUDIT] {} | Container: {} | {} | Time: {}",
            operation, container_id, details, timestamp
        ));
    }

    /// SECURITY CRITICAL: Check for resource exhaustion attacks
    pub fn check_resource_limits(&self, container_id: &str) -> Result<(), String> {
        // Check if we're creating too many interfaces
        let interface_count_cmd = format!("ip link show | grep -c 'quilt.*@'");
        if let Ok(result) = CommandExecutor::execute_shell(&interface_count_cmd) {
            if let Ok(count) = result.stdout.trim().parse::<u32>() {
                if count > 1000 {
                    // Reasonable limit
                    ConsoleLogger::warning(&format!(
                        "🚨 [SECURITY] High interface count detected: {}",
                        count
                    ));
                    return Err(format!(
                        "🚨 [SECURITY] Too many network interfaces: {}",
                        count
                    ));
                }
            }
        }

        self.audit_network_operation(
            "RESOURCE_CHECK",
            container_id,
            &format!("Resource limits validated"),
        );
        Ok(())
    }

    /// SECURITY CRITICAL: Verify that bridge operations don't affect host networking
    pub fn verify_bridge_isolation(&self, bridge_name: &str) -> Result<(), String> {
        // Check that bridge operations haven't affected default route
        let default_route_cmd = "ip route show default";
        match CommandExecutor::execute_shell(&default_route_cmd) {
            Ok(result) if result.success => {
                if result.stdout.contains(bridge_name) {
                    return Err(format!(
                        "🚨 [SECURITY] Bridge {} appears in default route: {}",
                        bridge_name,
                        result.stdout.trim()
                    ));
                }
            }
            _ => {
                ConsoleLogger::warning("Could not verify default route for bridge isolation");
            }
        }

        // Check that bridge hasn't modified main routing table
        let main_routes_cmd = "ip route show table main";
        match CommandExecutor::execute_shell(&main_routes_cmd) {
            Ok(result) if result.success => {
                // This is just informational - we log any routes involving our bridge
                if result.stdout.contains(bridge_name) {
                    ConsoleLogger::debug(&format!(
                        "ℹ️ [SECURITY] Bridge {} found in main routing table (may be normal): {}",
                        bridge_name,
                        result
                            .stdout
                            .lines()
                            .filter(|line| line.contains(bridge_name))
                            .collect::<Vec<_>>()
                            .join(" | ")
                    ));
                }
            }
            _ => {
                ConsoleLogger::debug("Could not check main routing table for bridge isolation");
            }
        }

        Ok(())
    }
}

// DNS management module
// Handles DNS server integration, redirect rules, and container DNS configuration

use crate::icc::dns::DnsServer;
use crate::icc::network::veth::ContainerNetworkConfig;
use crate::utils::command::CommandExecutor;
use crate::utils::console::ConsoleLogger;
use crate::utils::filesystem::FileSystemUtils;
use std::net::SocketAddr;
use std::sync::Arc;

/// DNS management for container networking
pub struct DnsManager {
    pub bridge_name: String,
    pub bridge_ip: String,
    pub dns_server: Option<Arc<DnsServer>>,
}

impl DnsManager {
    pub fn new(bridge_name: String, bridge_ip: String) -> Self {
        Self {
            bridge_name,
            bridge_ip,
            dns_server: None,
        }
    }

    pub async fn start_dns_server(&mut self) -> Result<(), String> {
        ConsoleLogger::debug("Starting DNS server for container networking");

        // Try primary port 1053, with fallback to other ports if needed
        let primary_port = 1053;
        let _fallback_ports = vec![1153, 1253, 1353, 1453]; // TODO: Implement port fallback logic

        // Try to bind to primary port first
        let dns_bind_address: SocketAddr =
            format!("{}:{}", self.bridge_ip, primary_port)
                .parse()
                .map_err(|e| format!("Invalid DNS bind address: {}", e))?;

        let dns = DnsServer::new(dns_bind_address);

        // Actually start the DNS server
        if let Err(e) = dns.start().await {
            return Err(format!("Failed to start DNS server: {}", e));
        }

        ConsoleLogger::success(&format!(
            "DNS server started on {}:{}",
            self.bridge_ip, primary_port
        ));
        self.dns_server = Some(Arc::new(dns));
        self.update_dns_redirect_rules(primary_port)?;
        Ok(())
    }

    fn update_dns_redirect_rules(&self, actual_port: u16) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "🔧 [DNS-REDIRECT] Updating iptables to redirect DNS to port {}",
            actual_port
        ));

        // COMPREHENSIVE CLEANUP: Remove ALL possible DNS redirect rules to prevent accumulation
        // We try to remove rules for all possible ports that might have been used
        let all_possible_ports = vec![1053, 1153, 1253, 1353, 1453];

        for port in &all_possible_ports {
            let cleanup_cmds = vec![
                format!("iptables -t nat -D PREROUTING -i {} -p udp --dport 53 -j DNAT --to-destination {}:{} 2>/dev/null || true", self.bridge_name, self.bridge_ip, port),
                format!("iptables -t nat -D PREROUTING -i {} -p tcp --dport 53 -j DNAT --to-destination {}:{} 2>/dev/null || true", self.bridge_name, self.bridge_ip, port),
            ];

            for cmd in cleanup_cmds {
                let _ = CommandExecutor::execute_shell(&cmd);
            }
        }

        // Add the new rules for the actual port being used
        let redirect_rules = vec![
            format!("iptables -t nat -A PREROUTING -i {} -p udp --dport 53 -j DNAT --to-destination {}:{}", self.bridge_name, self.bridge_ip, actual_port),
            format!("iptables -t nat -A PREROUTING -i {} -p tcp --dport 53 -j DNAT --to-destination {}:{}", self.bridge_name, self.bridge_ip, actual_port),
        ];

        for rule in redirect_rules {
            match CommandExecutor::execute_shell(&rule) {
                Ok(result) if result.success => {
                    ConsoleLogger::debug(&format!("✅ [DNS-REDIRECT] Added rule: {}", rule));
                }
                Ok(result) => {
                    ConsoleLogger::warning(&format!(
                        "⚠️ [DNS-REDIRECT] Rule may already exist: {} - {}",
                        rule, result.stderr
                    ));
                }
                Err(e) => {
                    return Err(format!("Failed to add DNS redirect rule: {} - {}", rule, e));
                }
            }
        }

        ConsoleLogger::success(&format!(
            "✅ [DNS-REDIRECT] DNS redirect rules updated for port {}",
            actual_port
        ));
        Ok(())
    }

    pub fn register_container_dns(
        &self,
        container_id: &str,
        container_name: &str,
        ip_address: &str,
    ) -> Result<(), String> {
        if let Some(dns) = &self.dns_server {
            dns.register_container(container_id, container_name, ip_address)?;
        } else {
            ConsoleLogger::warning("DNS server not started, skipping container registration");
        }
        Ok(())
    }

    pub fn unregister_container_dns(&self, container_id: &str) -> Result<(), String> {
        if let Some(dns) = &self.dns_server {
            dns.unregister_container(container_id)?;
        }
        Ok(())
    }

    pub fn list_dns_entries(&self) -> Result<Vec<crate::icc::dns::DnsEntry>, String> {
        if let Some(dns) = &self.dns_server {
            dns.list_entries()
        } else {
            Ok(vec![])
        }
    }

    pub fn configure_container_dns(
        &self,
        config: &ContainerNetworkConfig,
        container_pid: i32,
    ) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "Configuring DNS for container {} (PID: {})",
            config.container_id, container_pid
        ));

        // SECURITY CRITICAL: This nsenter command MUST NOT run if namespace entry fails
        // If nsenter fails, the shell command would execute on the HOST and corrupt host DNS
        let dns_content = format!("nameserver {}\nsearch quilt.local\n", self.bridge_ip);

        // First validate the container PID exists and is accessible
        if !self.validate_container_namespace(container_pid) {
            ConsoleLogger::warning(&format!("⚠️ [SECURITY] Container PID {} namespace validation failed - skipping unsafe DNS operation", container_pid));
            return self.configure_dns_safe_fallback(config, &dns_content);
        }

        let write_resolv_cmd = format!(
            "nsenter -t {} -m -p -- sh -c 'mkdir -p /etc && rm -f /etc/resolv.conf && echo \"{}\" > /etc/resolv.conf && ls -la /etc/resolv.conf'",
            container_pid, dns_content
        );

        let mut dns_written = false;
        match CommandExecutor::execute_shell(&write_resolv_cmd) {
            Ok(result) => {
                if result.success {
                    // Additional verification that we actually wrote to container's resolv.conf
                    if self.verify_dns_container_isolation(container_pid, &dns_content) {
                        ConsoleLogger::debug(&format!(
                            "✅ DNS configuration written to container's /etc/resolv.conf: {}",
                            result.stdout.trim()
                        ));
                        dns_written = true;
                    } else {
                        ConsoleLogger::error(
                            "🚨 [SECURITY] DNS write may have affected host - using safe fallback",
                        );
                        dns_written = false;
                    }
                } else {
                    ConsoleLogger::warning(&format!("DNS write command failed: {}", result.stderr));
                }
            }
            Err(e) => {
                ConsoleLogger::warning(&format!("Failed to execute DNS write command: {}", e));
            }
        }

        // Try alternative method if primary method failed
        if !dns_written {
            return self.configure_dns_safe_fallback(config, &dns_content);
        }

        // Verify DNS configuration was written
        if dns_written {
            // Verify the file exists and is readable from inside container
            let verify_cmd = format!("nsenter -t {} -m -p -- cat /etc/resolv.conf", container_pid);
            match CommandExecutor::execute_shell(&verify_cmd) {
                Ok(result) if result.success => {
                    ConsoleLogger::debug(&format!(
                        "✅ DNS configuration verified in container: {}",
                        result.stdout.trim()
                    ));
                }
                _ => {
                    ConsoleLogger::warning(
                        "DNS configuration may not be accessible from inside container",
                    );
                }
            }
        } else {
            ConsoleLogger::error("❌ Failed to write DNS configuration - containers may not be able to resolve names");
        }

        Ok(())
    }

    fn validate_container_namespace(&self, container_pid: i32) -> bool {
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

    fn verify_dns_container_isolation(&self, container_pid: i32, _expected_content: &str) -> bool {
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

    fn configure_dns_safe_fallback(
        &self,
        config: &ContainerNetworkConfig,
        dns_content: &str,
    ) -> Result<(), String> {
        ConsoleLogger::debug("Using safe DNS configuration fallback method...");
        if let Some(rootfs_path) = &config.rootfs_path {
            // Validate rootfs path is within expected container directory
            if !rootfs_path.starts_with("/tmp/quilt-containers/") {
                return Err(format!("🚨 [SECURITY] Unsafe rootfs path: {}", rootfs_path));
            }

            // Ensure /etc directory exists
            let etc_path = format!("{}/etc", rootfs_path);
            if let Err(e) =
                FileSystemUtils::create_dir_all_with_logging(&etc_path, "container /etc directory")
            {
                return Err(format!("Failed to create /etc directory: {}", e));
            }

            let resolv_conf_path = format!("{}/etc/resolv.conf", rootfs_path);
            // Remove any existing file/symlink first
            let _ = FileSystemUtils::remove_path(&resolv_conf_path);

            match FileSystemUtils::write_file(&resolv_conf_path, &dns_content) {
                Ok(_) => {
                    ConsoleLogger::debug(&format!(
                        "✅ DNS configuration written via safe fallback method: {}",
                        resolv_conf_path
                    ));
                    Ok(())
                }
                Err(e) => Err(format!("Safe DNS fallback also failed: {}", e)),
            }
        } else {
            Err("No rootfs path available for safe DNS fallback".to_string())
        }
    }

    pub fn cleanup_dns_rules(&self) -> Result<(), String> {
        ConsoleLogger::info("🧹 [CLEANUP] Starting comprehensive DNS cleanup");

        // Step 1: Clean up all DNS redirect rules
        let all_possible_ports = vec![1053, 1153, 1253, 1353, 1453];
        for port in all_possible_ports {
            let cleanup_cmds = vec![
                format!("iptables -t nat -D PREROUTING -i {} -p udp --dport 53 -j DNAT --to-destination {}:{} 2>/dev/null || true", self.bridge_name, self.bridge_ip, port),
                format!("iptables -t nat -D PREROUTING -i {} -p tcp --dport 53 -j DNAT --to-destination {}:{} 2>/dev/null || true", self.bridge_name, self.bridge_ip, port),
            ];

            for cmd in cleanup_cmds {
                let _ = CommandExecutor::execute_shell(&cmd);
            }
        }

        // Step 2: Aggressive cleanup of any remaining DNS DNAT rules
        for _attempt in 0..10 {
            let generic_cleanup_cmds = vec![
                format!(
                    "iptables -t nat -D PREROUTING -i {} -p udp --dport 53 -j DNAT 2>/dev/null",
                    self.bridge_name
                ),
                format!(
                    "iptables -t nat -D PREROUTING -i {} -p tcp --dport 53 -j DNAT 2>/dev/null",
                    self.bridge_name
                ),
            ];

            let mut rules_removed = false;
            for cmd in generic_cleanup_cmds {
                if let Ok(result) = CommandExecutor::execute_shell(&cmd) {
                    if result.success {
                        rules_removed = true;
                        ConsoleLogger::debug(&format!("🧹 [CLEANUP] Removed rule: {}", cmd));
                    }
                }
            }

            if !rules_removed {
                break;
            }
        }

        ConsoleLogger::success("✅ [CLEANUP] DNS cleanup completed - all DNS rules cleaned up");
        Ok(())
    }
}

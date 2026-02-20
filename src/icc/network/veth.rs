// Virtual Ethernet (veth) pair management module
// Handles creation, configuration, and attachment of veth pairs

use crate::icc::network::security::NetworkSecurity;
use crate::utils::command::CommandExecutor;
use crate::utils::console::ConsoleLogger;
use std::thread;
use std::time::Duration;

/// Container network configuration for veth operations
#[derive(Debug, Clone)]
pub struct ContainerNetworkConfig {
    pub container_id: String,
    pub ip_address: String,
    pub subnet_mask: String,
    pub gateway_ip: String,
    pub veth_host_name: String,
    pub veth_container_name: String,
    pub rootfs_path: Option<String>,
}

/// Virtual Ethernet pair management
pub struct VethManager {
    pub bridge_name: String,
}

#[allow(dead_code)]
impl VethManager {
    pub fn new(bridge_name: String) -> Self {
        Self { bridge_name }
    }

    pub fn create_veth_pair(&self, host_name: &str, container_name: &str) -> Result<(), String> {
        // SECURITY: Validate interface names before creation
        let security = NetworkSecurity::new("192.168.100.1".to_string()); // Bridge IP placeholder
        security.validate_interface_name(host_name)?;
        security.validate_interface_name(container_name)?;

        ConsoleLogger::debug(&format!(
            "Creating veth pair: {} <-> {}",
            host_name, container_name
        ));

        // First, clean up any existing interfaces with the same names
        let _cleanup_host =
            CommandExecutor::execute_shell(&format!("ip link delete {} 2>/dev/null", host_name));
        let _cleanup_container = CommandExecutor::execute_shell(&format!(
            "ip link delete {} 2>/dev/null",
            container_name
        ));

        // Create the veth pair
        let create_cmd = format!(
            "ip link add {} type veth peer name {}",
            host_name, container_name
        );
        ConsoleLogger::debug(&format!("Executing: {}", create_cmd));

        let result = CommandExecutor::execute_shell(&create_cmd)?;
        if !result.success {
            return Err(format!("Failed to create veth pair: {}", result.stderr));
        }

        // Verify the veth pair was created successfully
        self.verify_veth_pair_created(host_name, container_name)?;

        ConsoleLogger::success(&format!(
            "Veth pair created: {} <-> {}",
            host_name, container_name
        ));
        Ok(())
    }

    pub fn verify_veth_pair_created(
        &self,
        host_name: &str,
        container_name: &str,
    ) -> Result<(), String> {
        for attempt in 1..=10 {
            // Fast polling instead of single 100ms delay
            let verify_host =
                CommandExecutor::execute_shell(&format!("ip link show {}", host_name));
            let verify_container =
                CommandExecutor::execute_shell(&format!("ip link show {}", container_name));

            if verify_host.map_or(false, |r| r.success)
                && verify_container.map_or(false, |r| r.success)
            {
                ConsoleLogger::debug(&format!(
                    "Veth pair verified: {} <-> {}",
                    host_name, container_name
                ));
                return Ok(());
            }
            if attempt < 10 {
                thread::sleep(Duration::from_millis(10));
            }
        }
        Err(format!(
            "Veth pair creation verification failed: {} <-> {}",
            host_name, container_name
        ))
    }

    pub fn move_veth_to_container(
        &self,
        veth_name: &str,
        container_pid: i32,
    ) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "Moving veth interface {} to container PID {}",
            veth_name, container_pid
        ));

        // First verify the veth interface exists
        let verify_result = CommandExecutor::execute_shell(&format!("ip link show {}", veth_name))?;
        if !verify_result.success {
            return Err(format!("Veth interface {} does not exist", veth_name));
        }

        // Move to container namespace
        let move_cmd = format!("ip link set {} netns {}", veth_name, container_pid);
        ConsoleLogger::debug(&format!("Executing: {}", move_cmd));

        let result = CommandExecutor::execute_shell(&move_cmd)?;
        if !result.success {
            return Err(format!(
                "Failed to move veth to container: {}",
                result.stderr
            ));
        }

        ConsoleLogger::success(&format!(
            "Veth {} moved to container PID {}",
            veth_name, container_pid
        ));
        Ok(())
    }

    pub fn configure_container_interface(
        &self,
        config: &ContainerNetworkConfig,
        container_pid: i32,
    ) -> Result<(), String> {
        let ns_exec = format!("nsenter -t {} -n", container_pid);

        // Use consistent interface naming to avoid eth0 conflicts
        let interface_name = format!("quilt{}", &config.container_id[..8]);

        ConsoleLogger::debug(&format!(
            "Configuring container interface as: {}",
            interface_name
        ));

        // Step 1: Rename the interface inside the container
        let rename_cmd = format!(
            "{} ip link set {} name {}",
            ns_exec, config.veth_container_name, interface_name
        );
        ConsoleLogger::debug(&format!("Renaming interface: {}", rename_cmd));
        let rename_result = CommandExecutor::execute_shell(&rename_cmd)?;
        if !rename_result.success {
            return Err(format!(
                "Failed to rename container interface: {}",
                rename_result.stderr
            ));
        }

        // Step 2: Assign IP address
        let ip_with_mask = format!("{}/{}", config.ip_address, config.subnet_mask);
        let ip_cmd = format!(
            "{} ip addr add {} dev {}",
            ns_exec, ip_with_mask, interface_name
        );
        ConsoleLogger::debug(&format!("Assigning IP: {}", ip_cmd));
        let ip_result = CommandExecutor::execute_shell(&ip_cmd)?;
        if !ip_result.success && !ip_result.stderr.contains("File exists") {
            return Err(format!(
                "Failed to assign IP to interface: {}",
                ip_result.stderr
            ));
        }

        // Step 3: Bring the interface up
        let up_cmd = format!("{} ip link set {} up", ns_exec, interface_name);
        ConsoleLogger::debug(&format!("Bringing interface up: {}", up_cmd));
        let up_result = CommandExecutor::execute_shell(&up_cmd)?;
        if !up_result.success {
            return Err(format!(
                "Failed to bring interface up: {}",
                up_result.stderr
            ));
        }

        // Step 4: Add default route through the gateway
        let route_cmd = format!("{} ip route add default via {}", ns_exec, config.gateway_ip);
        ConsoleLogger::debug(&format!("Adding default route: {}", route_cmd));
        let route_result = CommandExecutor::execute_shell(&route_cmd);
        if let Err(e) = route_result {
            if !e.contains("File exists") {
                ConsoleLogger::warning(&format!("Failed to add default route: {}", e));
            }
        }

        ConsoleLogger::success(&format!(
            "Container interface {} configured with IP {}",
            interface_name, config.ip_address
        ));
        Ok(())
    }

    pub fn attach_veth_to_bridge_with_retry(&self, veth_name: &str) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "🔗 [BRIDGE-ATTACH] Attaching {} to bridge {} with enhanced reliability",
            veth_name, self.bridge_name
        ));

        // Pre-flight checks
        self.verify_veth_exists(veth_name)?;

        const MAX_ATTEMPTS: u32 = 5;
        for attempt in 1..=MAX_ATTEMPTS {
            ConsoleLogger::debug(&format!(
                "🔗 [BRIDGE-ATTACH] Attempt {} of {}",
                attempt, MAX_ATTEMPTS
            ));

            // Perform the attachment
            let attach_cmd = format!("ip link set {} master {}", veth_name, self.bridge_name);
            match CommandExecutor::execute_shell(&attach_cmd) {
                Ok(result) if result.success => {
                    // Verify the attachment worked
                    if self
                        .verify_bridge_attachment_comprehensive(veth_name)
                        .is_ok()
                    {
                        self.post_attachment_validation(veth_name)?;
                        ConsoleLogger::success(&format!(
                            "✅ [BRIDGE-ATTACH] {} successfully attached to {}",
                            veth_name, self.bridge_name
                        ));
                        return Ok(());
                    } else {
                        ConsoleLogger::debug(&format!(
                            "❌ [BRIDGE-ATTACH] Attachment verification failed (attempt {})",
                            attempt
                        ));
                    }
                }
                Ok(result) => {
                    ConsoleLogger::debug(&format!(
                        "❌ [BRIDGE-ATTACH] Attach command failed: {} (attempt {})",
                        result.stderr, attempt
                    ));
                }
                Err(e) => {
                    ConsoleLogger::debug(&format!(
                        "❌ [BRIDGE-ATTACH] Attach command error: {} (attempt {})",
                        e, attempt
                    ));
                }
            }

            // Diagnose why it failed if not the last attempt
            if attempt < MAX_ATTEMPTS {
                self.diagnose_attachment_failure(veth_name, attempt);
                thread::sleep(Duration::from_millis(100 * attempt as u64));
            }
        }

        Err(format!(
            "Failed to attach {} to bridge {} after {} attempts",
            veth_name, self.bridge_name, MAX_ATTEMPTS
        ))
    }

    fn verify_veth_exists(&self, veth_name: &str) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "🔍 [VETH-CHECK] Verifying veth {} exists",
            veth_name
        ));

        match CommandExecutor::execute_shell(&format!("ip link show {}", veth_name)) {
            Ok(result) if result.success => {
                ConsoleLogger::debug(&format!(
                    "✅ [VETH-CHECK] Veth {} exists and is visible",
                    veth_name
                ));
                Ok(())
            }
            Ok(result) => Err(format!(
                "Veth {} does not exist: {}",
                veth_name, result.stderr
            )),
            Err(e) => Err(format!(
                "Failed to check veth {} existence: {}",
                veth_name, e
            )),
        }
    }

    fn verify_bridge_attachment_comprehensive(&self, veth_name: &str) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "🔍 [ATTACH-VERIFY] Comprehensive attachment verification for {}",
            veth_name
        ));

        // Method 1: Check master relationship
        let master_check = format!(
            "ip link show {} | grep 'master {}'",
            veth_name, self.bridge_name
        );
        let master_ok = match CommandExecutor::execute_shell(&master_check) {
            Ok(result) => result.success,
            Err(_) => false,
        };

        // Method 2: Check bridge shows the interface
        let bridge_check = format!("ip link show master {}", self.bridge_name);
        let bridge_ok = match CommandExecutor::execute_shell(&bridge_check) {
            Ok(result) => result.success && result.stdout.contains(veth_name),
            Err(_) => false,
        };

        // Method 3: Check bridge command (if available)
        let brctl_check = format!("bridge link show dev {}", veth_name);
        let brctl_ok = match CommandExecutor::execute_shell(&brctl_check) {
            Ok(result) => result.success && result.stdout.contains(&self.bridge_name),
            Err(_) => false, // bridge command may not be available
        };

        ConsoleLogger::debug(&format!("🔍 [ATTACH-VERIFY] Verification results: master_check={}, bridge_check={}, brctl_check={}", 
            master_ok, bridge_ok, brctl_ok));

        if master_ok && bridge_ok {
            ConsoleLogger::debug(&format!(
                "✅ [ATTACH-VERIFY] {} is properly attached to {}",
                veth_name, self.bridge_name
            ));
            Ok(())
        } else {
            Err(format!(
                "Attachment verification failed for {} -> {}",
                veth_name, self.bridge_name
            ))
        }
    }

    fn post_attachment_validation(&self, veth_name: &str) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "🔄 [POST-ATTACH] Post-attachment validation for {}",
            veth_name
        ));

        // Wait for attachment to stabilize
        thread::sleep(Duration::from_millis(100));

        // Bring the host-side veth up
        let up_cmd = format!("ip link set {} up", veth_name);
        match CommandExecutor::execute_shell(&up_cmd) {
            Ok(result) if result.success => {
                ConsoleLogger::debug(&format!(
                    "✅ [POST-ATTACH] {} brought up successfully",
                    veth_name
                ));
                Ok(())
            }
            Ok(result) => Err(format!(
                "Failed to bring {} up: {}",
                veth_name, result.stderr
            )),
            Err(e) => Err(format!("Error bringing {} up: {}", veth_name, e)),
        }
    }

    fn diagnose_attachment_failure(&self, veth_name: &str, attempt: u32) {
        ConsoleLogger::debug(&format!(
            "🔍 [ATTACH-DIAG] Diagnosing attachment failure for {} (attempt {})",
            veth_name, attempt
        ));

        // Check veth state
        if let Ok(result) = CommandExecutor::execute_shell(&format!("ip link show {}", veth_name)) {
            ConsoleLogger::debug(&format!(
                "ℹ️ [ATTACH-DIAG] Veth state: {}",
                result.stdout.trim()
            ));
        }

        // Check bridge state
        if let Ok(result) =
            CommandExecutor::execute_shell(&format!("ip link show {}", self.bridge_name))
        {
            ConsoleLogger::debug(&format!(
                "ℹ️ [ATTACH-DIAG] Bridge state: {}",
                result.stdout.trim()
            ));
        }

        // Check if bridge is accepting attachments
        if let Ok(result) =
            CommandExecutor::execute_shell(&format!("bridge link show master {}", self.bridge_name))
        {
            ConsoleLogger::debug(&format!(
                "ℹ️ [ATTACH-DIAG] Bridge interfaces: {}",
                result.stdout.trim()
            ));
        }
    }

    pub fn verify_bridge_attachment(&self, veth_name: &str) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "🔍 [BRIDGE-VERIFY] Verifying {} is attached to bridge {}",
            veth_name, self.bridge_name
        ));

        // Check 1: Verify veth shows bridge as master
        let master_check = format!(
            "ip link show {} | grep 'master {}'",
            veth_name, self.bridge_name
        );
        match CommandExecutor::execute_shell(&master_check) {
            Ok(result) if result.success => {
                ConsoleLogger::debug(&format!(
                    "✅ [BRIDGE-VERIFY] {} correctly shows {} as master",
                    veth_name, self.bridge_name
                ));
            }
            _ => {
                return Err(format!(
                    "Veth {} does not show {} as master",
                    veth_name, self.bridge_name
                ));
            }
        }

        // Check 2: Verify bridge lists the veth
        let bridge_check = format!(
            "bridge link show master {} | grep {}",
            self.bridge_name, veth_name
        );
        match CommandExecutor::execute_shell(&bridge_check) {
            Ok(result) if result.success => {
                ConsoleLogger::debug(&format!(
                    "✅ [BRIDGE-VERIFY] Bridge {} correctly lists {}",
                    self.bridge_name, veth_name
                ));
            }
            _ => {
                return Err(format!(
                    "Bridge {} does not list {} as attached",
                    self.bridge_name, veth_name
                ));
            }
        }

        ConsoleLogger::success(&format!(
            "✅ [BRIDGE-VERIFY] {} is properly attached to bridge {}",
            veth_name, self.bridge_name
        ));
        Ok(())
    }

    pub fn verify_bridge_attachment_fast(&self, veth_name: &str) -> bool {
        // Single fast check: Verify veth shows bridge as master
        let master_check = format!(
            "ip link show {} | grep -q 'master {}'",
            veth_name, self.bridge_name
        );
        match CommandExecutor::execute_shell(&master_check) {
            Ok(result) if result.success => {
                ConsoleLogger::debug(&format!(
                    "✅ [FAST-VERIFY] {} attached to bridge {}",
                    veth_name, self.bridge_name
                ));
                true
            }
            _ => {
                ConsoleLogger::debug(&format!(
                    "❌ [FAST-VERIFY] {} not attached to bridge {}",
                    veth_name, self.bridge_name
                ));
                false
            }
        }
    }

    pub fn get_interface_mac_address(&self, interface_name: &str) -> Result<String, String> {
        ConsoleLogger::debug(&format!(
            "🔍 [MAC-LOOKUP] Getting MAC address for interface: {}",
            interface_name
        ));

        // Use ip link show to get interface details including MAC address
        let cmd = format!(
            "ip link show {} | grep 'link/ether' | awk '{{print $2}}'",
            interface_name
        );

        match CommandExecutor::execute_shell(&cmd) {
            Ok(result) if result.success => {
                let mac = result.stdout.trim().to_string();
                if mac.len() == 17 && mac.matches(':').count() == 5 {
                    // Basic MAC format validation
                    ConsoleLogger::debug(&format!(
                        "✅ [MAC-LOOKUP] Interface {} MAC address: {}",
                        interface_name, mac
                    ));
                    Ok(mac)
                } else {
                    Err(format!(
                        "Invalid MAC address format for {}: {}",
                        interface_name, mac
                    ))
                }
            }
            Ok(result) => Err(format!(
                "Failed to get MAC address for {}: {}",
                interface_name, result.stderr
            )),
            Err(e) => Err(format!(
                "Failed to execute MAC lookup command for {}: {}",
                interface_name, e
            )),
        }
    }

    pub fn get_container_interface_mac_address(
        &self,
        container_pid: i32,
        interface_name: &str,
    ) -> Result<String, String> {
        ConsoleLogger::debug(&format!(
            "🔍 [MAC-LOOKUP-NS] Getting MAC address for interface {} in container PID {}",
            interface_name, container_pid
        ));

        // Use nsenter to get MAC address from within container namespace
        let cmd = format!(
            "nsenter -t {} -n ip link show {} | grep 'link/ether' | awk '{{print $2}}'",
            container_pid, interface_name
        );

        match CommandExecutor::execute_shell(&cmd) {
            Ok(result) if result.success => {
                let mac = result.stdout.trim().to_string();
                if mac.len() == 17 && mac.matches(':').count() == 5 {
                    // Basic MAC format validation
                    ConsoleLogger::debug(&format!(
                        "✅ [MAC-LOOKUP-NS] Container {} interface {} MAC: {}",
                        container_pid, interface_name, mac
                    ));
                    Ok(mac)
                } else {
                    Err(format!(
                        "Invalid MAC address format for container {} interface {}: {}",
                        container_pid, interface_name, mac
                    ))
                }
            }
            Ok(result) => Err(format!(
                "Failed to get MAC address for container {} interface {}: {}",
                container_pid, interface_name, result.stderr
            )),
            Err(e) => Err(format!(
                "Failed to execute MAC lookup command for {} in container {}: {}",
                interface_name, container_pid, e
            )),
        }
    }
}

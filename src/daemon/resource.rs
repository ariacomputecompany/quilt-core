use crate::daemon::cgroup::CgroupManager;
use crate::daemon::MountConfig;
use crate::icc::network::ContainerNetworkConfig;
use crate::utils::command::CommandExecutor;
use crate::utils::console::ConsoleLogger;
use crate::utils::filesystem::FileSystemUtils;
use crate::utils::validation::VolumeMount;
use nix::unistd::Pid;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// Thread-safe comprehensive resource manager for container lifecycle
pub struct ResourceManager {
    /// Track active mounts per container with full configuration (thread-safe)
    active_mounts: Arc<Mutex<HashMap<String, Vec<MountConfig>>>>,
    /// Track network interfaces per container (thread-safe)
    network_interfaces: Arc<Mutex<HashMap<String, ContainerNetworkConfig>>>,
}

impl ResourceManager {
    pub fn new() -> Self {
        ResourceManager {
            active_mounts: Arc::new(Mutex::new(HashMap::new())),
            network_interfaces: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register mounts for a container (thread-safe)
    pub fn register_mounts(&self, container_id: &str, mounts: Vec<String>) {
        ConsoleLogger::debug(&format!(
            "[RESOURCE] Registering {} mount paths for container {}",
            mounts.len(),
            container_id
        ));
        // Convert string paths to minimal MountConfig for backward compatibility
        let mount_configs: Vec<MountConfig> = mounts
            .into_iter()
            .map(|path| {
                MountConfig {
                    source: path.clone(),
                    target: path,
                    mount_type: crate::daemon::MountType::Bind, // Default assumption
                    readonly: false,                            // Default to read-write
                    options: std::collections::HashMap::new(),
                }
            })
            .collect();

        if let Ok(mut active_mounts) = self.active_mounts.lock() {
            active_mounts.insert(container_id.to_string(), mount_configs);
        }
    }

    /// Register mount configurations for a container (thread-safe, enhanced)
    pub fn register_mount_configs(&self, container_id: &str, mount_configs: Vec<MountConfig>) {
        ConsoleLogger::debug(&format!(
            "[RESOURCE] Registering {} mount configs for container {}",
            mount_configs.len(),
            container_id
        ));

        // Log readonly mounts for security awareness
        let readonly_count = mount_configs.iter().filter(|m| m.readonly).count();
        if readonly_count > 0 {
            ConsoleLogger::info(&format!(
                "[RESOURCE] Container {} has {} readonly mounts",
                container_id, readonly_count
            ));
        }

        if let Ok(mut active_mounts) = self.active_mounts.lock() {
            active_mounts.insert(container_id.to_string(), mount_configs);
        }
    }

    /// Validate mount configurations for security using VolumeMount integration
    pub fn validate_mount_security(
        &self,
        container_id: &str,
        mount_configs: &[MountConfig],
    ) -> Result<(), String> {
        use crate::utils::security::SecurityValidator;
        use crate::utils::validation::MountType as ValidationMountType;

        ConsoleLogger::debug(&format!(
            "[RESOURCE] Validating mount security for container {} ({} mounts)",
            container_id,
            mount_configs.len()
        ));

        for (i, mount_config) in mount_configs.iter().enumerate() {
            // Convert MountConfig to VolumeMount for validation
            let validation_mount = VolumeMount {
                source: mount_config.source.clone(),
                target: mount_config.target.clone(),
                mount_type: match mount_config.mount_type {
                    crate::daemon::MountType::Bind => ValidationMountType::Bind,
                    crate::daemon::MountType::Volume => ValidationMountType::Volume,
                    crate::daemon::MountType::Tmpfs => ValidationMountType::Tmpfs,
                },
                readonly: mount_config.readonly, // Use the actual readonly field from MountConfig
                options: mount_config.options.clone(),
            };

            // Perform security validation
            if let Err(e) = SecurityValidator::validate_mount(&validation_mount) {
                return Err(format!(
                    "Mount {} security validation failed for container {}: {}",
                    i + 1,
                    container_id,
                    e
                ));
            }

            // Log readonly mount validation
            if validation_mount.readonly {
                ConsoleLogger::info(&format!(
                    "[RESOURCE] Readonly mount validated for {}: {} -> {} (read-only)",
                    container_id, validation_mount.source, validation_mount.target
                ));
            }
        }

        ConsoleLogger::success(&format!(
            "[RESOURCE] All {} mounts validated for container {}",
            mount_configs.len(),
            container_id
        ));
        Ok(())
    }

    /// Get rootfs path correlation for a container's network configuration
    pub fn get_network_rootfs_correlation(&self, container_id: &str) -> Option<String> {
        if let Ok(network_interfaces) = self.network_interfaces.lock() {
            if let Some(network_config) = network_interfaces.get(container_id) {
                return network_config.rootfs_path.clone();
            }
        }
        None
    }

    /// Cleanup container with rootfs-network correlation
    pub fn cleanup_container_with_correlation(
        &self,
        container_id: &str,
        container_pid: Option<Pid>,
    ) -> Result<(), String> {
        ConsoleLogger::progress(&format!(
            "🔗 [RESOURCE] Starting correlated cleanup for container: {}",
            container_id
        ));

        // Get network configuration with rootfs correlation
        let network_config = if let Ok(mut network_interfaces) = self.network_interfaces.lock() {
            network_interfaces.remove(container_id)
        } else {
            None
        };

        // Perform correlation check
        if let Some(ref network_config) = network_config {
            if let Some(ref rootfs_path) = network_config.rootfs_path {
                ConsoleLogger::info(&format!(
                    "[RESOURCE] Network-rootfs correlation found: {} <-> {}",
                    network_config.container_id, rootfs_path
                ));

                // Validate correlation consistency
                let expected_rootfs = format!("/tmp/quilt-containers/{}", container_id);
                if rootfs_path != &expected_rootfs {
                    ConsoleLogger::warning(&format!(
                        "[RESOURCE] Rootfs path mismatch: expected {}, found {}",
                        expected_rootfs, rootfs_path
                    ));
                }
            }
        }

        // Proceed with normal cleanup using correlation info
        self.cleanup_container_resources(container_id, container_pid)
    }

    /// Register network configuration for a container (thread-safe)
    pub fn register_network(&self, container_id: &str, network_config: ContainerNetworkConfig) {
        ConsoleLogger::debug(&format!(
            "[RESOURCE] Registering network config for container {}",
            container_id
        ));
        if let Ok(mut network_interfaces) = self.network_interfaces.lock() {
            network_interfaces.insert(container_id.to_string(), network_config);
        }
    }

    /// Cleanup all resources for a container (thread-safe)
    pub fn cleanup_container_resources(
        &self,
        container_id: &str,
        container_pid: Option<Pid>,
    ) -> Result<(), String> {
        ConsoleLogger::progress(&format!(
            "🧹 Cleaning up all resources for container: {}",
            container_id
        ));

        let mut cleanup_errors: Vec<String> = Vec::new();

        // 1. Cleanup network resources (thread-safe)
        let network_config = if let Ok(mut network_interfaces) = self.network_interfaces.lock() {
            network_interfaces.remove(container_id)
        } else {
            None
        };

        if let Some(network_config) = network_config {
            if let Err(e) = self.cleanup_network_resources(&network_config, container_pid) {
                cleanup_errors.push(format!("Network cleanup failed: {}", e));
            }
        }

        // 2. Cleanup mount namespaces (thread-safe)
        let mounts = if let Ok(mut active_mounts) = self.active_mounts.lock() {
            active_mounts.remove(container_id)
        } else {
            None
        };

        if let Some(mounts) = mounts {
            if let Err(e) = self.cleanup_mount_resources(container_id, &mounts, container_pid) {
                cleanup_errors.push(format!("Mount cleanup failed: {}", e));
            }
        }

        // 3. Cleanup cgroups
        if let Err(e) = self.cleanup_cgroup_resources(container_id) {
            cleanup_errors.push(format!("Cgroup cleanup failed: {}", e));
        }

        // 4. Final rootfs cleanup with network correlation validation
        let rootfs_path = format!("/tmp/quilt-containers/{}", container_id);

        // Check if rootfs path matches network configuration correlation
        if let Some(network_rootfs) = self.get_network_rootfs_correlation(container_id) {
            if network_rootfs != rootfs_path {
                ConsoleLogger::warning(&format!(
                    "[RESOURCE] Rootfs cleanup path mismatch: cleanup={}, network={}",
                    rootfs_path, network_rootfs
                ));
            } else {
                ConsoleLogger::debug(&format!(
                    "[RESOURCE] Rootfs cleanup path correlation verified: {}",
                    rootfs_path
                ));
            }
        }

        if let Err(e) = self.cleanup_rootfs_resources_safe(&rootfs_path) {
            cleanup_errors.push(format!("Rootfs cleanup failed: {}", e));
        }

        if cleanup_errors.is_empty() {
            ConsoleLogger::success(&format!(
                "✅ All resources cleaned up for container {}",
                container_id
            ));
            Ok(())
        } else {
            let error_msg = format!("Partial cleanup failures: {}", cleanup_errors.join("; "));
            ConsoleLogger::warning(&error_msg);
            Err(error_msg)
        }
    }

    /// Cleanup network resources (veth pairs, network namespaces) with rootfs correlation
    fn cleanup_network_resources(
        &self,
        network_config: &ContainerNetworkConfig,
        container_pid: Option<Pid>,
    ) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "🌐 Cleaning up network resources: {}",
            network_config.veth_host_name
        ));

        // Use rootfs_path for cleanup correlation
        if let Some(ref rootfs_path) = network_config.rootfs_path {
            ConsoleLogger::info(&format!(
                "[RESOURCE] Network cleanup correlated with rootfs: {}",
                rootfs_path
            ));

            // Validate rootfs path exists before network cleanup for safety
            if std::path::Path::new(rootfs_path).exists() {
                ConsoleLogger::debug(&format!(
                    "[RESOURCE] Rootfs {} exists, proceeding with network cleanup",
                    rootfs_path
                ));
            } else {
                ConsoleLogger::warning(&format!("[RESOURCE] Rootfs {} not found during network cleanup - container may have been partially cleaned", rootfs_path));
            }
        } else {
            ConsoleLogger::warning(&format!(
                "[RESOURCE] No rootfs_path correlation available for container {}",
                network_config.container_id
            ));
        }

        // Clean up veth pair - delete the host side, container side will be cleaned up automatically
        let cleanup_host_veth = format!(
            "ip link delete {} 2>/dev/null || true",
            network_config.veth_host_name
        );
        if let Err(e) = CommandExecutor::execute_shell(&cleanup_host_veth) {
            ConsoleLogger::warning(&format!(
                "Failed to delete host veth {}: {}",
                network_config.veth_host_name, e
            ));
        } else {
            ConsoleLogger::debug(&format!(
                "Deleted host veth interface: {}",
                network_config.veth_host_name
            ));
        }

        // Clean up container side veth if container is still running
        if let Some(pid) = container_pid {
            // SECURITY NOTE: Safe cleanup operation - only deletes interface, with || true fallback
            let cleanup_container_veth = format!(
                "nsenter -t {} -n ip link delete {} 2>/dev/null || true",
                crate::utils::process::ProcessUtils::pid_to_i32(pid),
                network_config.veth_container_name
            );
            if let Err(e) = CommandExecutor::execute_shell(&cleanup_container_veth) {
                ConsoleLogger::debug(&format!(
                    "Container veth cleanup attempt failed (expected if container exited): {}",
                    e
                ));
            }
        }

        // Clean up any custom interface names
        let interface_name = format!("qnet{}", &network_config.container_id[..8]);
        if let Some(pid) = container_pid {
            let cleanup_custom_interface = format!(
                "nsenter -t {} -n ip link delete {} 2>/dev/null || true",
                crate::utils::process::ProcessUtils::pid_to_i32(pid),
                interface_name
            );
            if let Err(e) = CommandExecutor::execute_shell(&cleanup_custom_interface) {
                ConsoleLogger::debug(&format!(
                    "Custom interface cleanup attempt failed (expected if container exited): {}",
                    e
                ));
            }
        }

        ConsoleLogger::success("Network resources cleaned up");
        Ok(())
    }

    /// Cleanup mount namespaces
    fn cleanup_mount_resources(
        &self,
        container_id: &str,
        mounts: &[MountConfig],
        container_pid: Option<Pid>,
    ) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "📁 Cleaning up {} mount points for container {}",
            mounts.len(),
            container_id
        ));

        // Log readonly mount cleanup for security audit
        let readonly_mounts: Vec<&MountConfig> = mounts.iter().filter(|m| m.readonly).collect();
        if !readonly_mounts.is_empty() {
            ConsoleLogger::info(&format!(
                "[RESOURCE] Cleaning up {} readonly mounts for container {}",
                readonly_mounts.len(),
                container_id
            ));
        }

        // If container is still running, try to unmount from within the namespace
        if let Some(pid) = container_pid {
            for mount_config in mounts.iter().rev() {
                // Reverse order for proper unmounting
                let mount_point = &mount_config.target;
                let unmount_cmd = format!(
                    "nsenter -t {} -m umount -l {} 2>/dev/null || true",
                    pid.as_raw(),
                    mount_point
                );
                if let Err(e) = CommandExecutor::execute_shell(&unmount_cmd) {
                    ConsoleLogger::debug(&format!(
                        "Namespace unmount failed for {}: {} (may be expected)",
                        mount_point, e
                    ));
                }

                // Log readonly mount cleanup
                if mount_config.readonly {
                    ConsoleLogger::debug(&format!(
                        "[RESOURCE] Cleaned readonly mount: {} -> {} for {}",
                        mount_config.source, mount_config.target, container_id
                    ));
                }
            }
        }

        // Force unmount from host side with lazy unmount
        let rootfs_path = format!("/tmp/quilt-containers/{}", container_id);
        let common_mounts = vec![
            format!("{}/proc", rootfs_path),
            format!("{}/sys", rootfs_path),
            format!("{}/dev/pts", rootfs_path),
            rootfs_path.clone(), // The rootfs bind mount itself
        ];

        for mount_point in common_mounts.iter().rev() {
            if Path::new(mount_point).exists() {
                // Try regular unmount first
                let unmount_cmd = format!("umount {} 2>/dev/null || true", mount_point);
                let _ = CommandExecutor::execute_shell(&unmount_cmd);

                // Force lazy unmount as fallback
                let lazy_unmount_cmd = format!("umount -l {} 2>/dev/null || true", mount_point);
                if let Err(e) = CommandExecutor::execute_shell(&lazy_unmount_cmd) {
                    ConsoleLogger::debug(&format!(
                        "Lazy unmount failed for {}: {}",
                        mount_point, e
                    ));
                }
            }
        }

        // Also cleanup user-defined mounts in reverse order
        for mount_config in mounts.iter().rev() {
            let mount_point = &mount_config.target;
            if Path::new(mount_point).exists() {
                let unmount_cmd = format!(
                    "umount {} 2>/dev/null || umount -l {} 2>/dev/null || true",
                    mount_point, mount_point
                );
                if let Err(e) = CommandExecutor::execute_shell(&unmount_cmd) {
                    ConsoleLogger::debug(&format!(
                        "User mount cleanup failed for {}: {}",
                        mount_point, e
                    ));
                }

                // Log cleanup of readonly mounts
                if mount_config.readonly {
                    ConsoleLogger::info(&format!(
                        "[RESOURCE] Successfully cleaned readonly mount: {} for {}",
                        mount_point, container_id
                    ));
                }
            }
        }

        ConsoleLogger::success(&format!(
            "Mount resources cleaned up ({} user mounts, {} readonly)",
            mounts.len(),
            readonly_mounts.len()
        ));
        Ok(())
    }

    /// Cleanup cgroup resources
    fn cleanup_cgroup_resources(&self, container_id: &str) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "⚙️ Cleaning up cgroup resources for container {}",
            container_id
        ));

        let cgroup_manager = CgroupManager::new(container_id.to_string());
        cgroup_manager.cleanup()
    }

    /// Cleanup rootfs resources with improved mount ordering
    fn cleanup_rootfs_resources_safe(&self, rootfs_path: &str) -> Result<(), String> {
        ConsoleLogger::debug(&format!("📂 Cleaning up rootfs: {}", rootfs_path));

        if !Path::new(rootfs_path).exists() {
            ConsoleLogger::debug("Rootfs path doesn't exist, skipping cleanup");
            return Ok(());
        }

        // Step 1: Force unmount all nested mounts in proper order (most specific first)
        let nested_mounts = vec![
            format!("{}/proc", rootfs_path),
            format!("{}/sys", rootfs_path),
            format!("{}/dev/pts", rootfs_path),
            format!("{}/dev", rootfs_path),
        ];

        for mount_point in nested_mounts {
            if Path::new(&mount_point).exists() {
                let umount_cmd = format!("umount -l '{}' 2>/dev/null || true", mount_point);
                let _ = CommandExecutor::execute_shell(&umount_cmd);

                // Give kernel time to process unmount
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }

        // Step 2: Try graceful removal first
        match FileSystemUtils::remove_path(rootfs_path) {
            Ok(()) => {
                ConsoleLogger::success(&format!("✅ Rootfs cleanup successful: {}", rootfs_path));
                return Ok(());
            }
            Err(e) => {
                ConsoleLogger::warning(&format!(
                    "Normal rootfs removal failed, trying force removal: {}",
                    e
                ));
            }
        }

        // Step 3: Force cleanup with more aggressive unmounting
        let force_umount_cmd = format!("umount -f -l '{}' 2>/dev/null || true", rootfs_path);
        let _ = CommandExecutor::execute_shell(&force_umount_cmd);

        // Wait a bit longer for force unmount to complete
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Step 4: Force remove directory
        let force_remove_cmd = format!("rm -rf '{}'", rootfs_path);
        match CommandExecutor::execute_shell(&force_remove_cmd) {
            Ok(_) => {
                ConsoleLogger::success(&format!(
                    "✅ Force rootfs cleanup successful: {}",
                    rootfs_path
                ));
                Ok(())
            }
            Err(e) => {
                // Last resort - try with sudo if available
                let sudo_remove_cmd = format!(
                    "sudo rm -rf '{}' 2>/dev/null || rm -rf '{}'",
                    rootfs_path, rootfs_path
                );
                match CommandExecutor::execute_shell(&sudo_remove_cmd) {
                    Ok(_) => {
                        ConsoleLogger::success(&format!(
                            "✅ Emergency rootfs cleanup successful: {}",
                            rootfs_path
                        ));
                        Ok(())
                    }
                    Err(e2) => Err(format!(
                        "Failed to remove rootfs {}: {} (emergency attempt: {})",
                        rootfs_path, e, e2
                    )),
                }
            }
        }
    }

    /// Emergency cleanup for a container (thread-safe)
    pub fn emergency_cleanup(&self, container_id: &str) -> Result<(), String> {
        ConsoleLogger::warning(&format!(
            "🚨 Emergency cleanup for container: {}",
            container_id
        ));

        // Kill any remaining processes
        let kill_cmd = format!("pkill -9 -f 'quilt.*{}' 2>/dev/null || true", container_id);
        let _ = CommandExecutor::execute_shell(&kill_cmd);

        // Force cleanup all resources
        self.cleanup_container_resources(container_id, None)?;

        ConsoleLogger::success(&format!(
            "Emergency cleanup completed for: {}",
            container_id
        ));
        Ok(())
    }
}

/// Thread-safe singleton resource manager using proper synchronization
use std::sync::OnceLock;
static RESOURCE_MANAGER: OnceLock<Arc<ResourceManager>> = OnceLock::new();

impl ResourceManager {
    /// Get the global resource manager instance (thread-safe)
    pub fn global() -> Arc<ResourceManager> {
        RESOURCE_MANAGER
            .get_or_init(|| Arc::new(ResourceManager::new()))
            .clone()
    }
}

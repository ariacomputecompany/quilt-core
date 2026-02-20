use crate::utils::console::ConsoleLogger;
use crate::utils::process::ProcessUtils;
use nix::unistd::Pid;
use std::fs;
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct CgroupLimits {
    pub memory_limit_bytes: Option<u64>, // Memory limit in bytes
    pub cpu_shares: Option<u64>,         // CPU shares (relative weight)
    pub cpu_quota: Option<i64>,          // CPU quota in microseconds (-1 for unlimited)
    pub cpu_period: Option<u64>,         // CPU period in microseconds (default 100000)
    pub pids_limit: Option<u64>,         // Maximum number of PIDs
}

impl Default for CgroupLimits {
    fn default() -> Self {
        CgroupLimits {
            memory_limit_bytes: Some(512 * 1024 * 1024), // 512MB default
            cpu_shares: Some(1024),                      // Default CPU shares
            cpu_quota: None,                             // No CPU quota by default
            cpu_period: Some(100000),                    // 100ms period
            pids_limit: Some(1024),                      // 1024 PIDs limit
        }
    }
}

impl CgroupLimits {
    /// Validate and adjust limits to prevent system issues
    pub fn validated(mut self) -> Self {
        // Enforce minimum memory limit of 256MB to prevent fork failures
        if let Some(memory_limit) = self.memory_limit_bytes {
            const MIN_MEMORY_LIMIT: u64 = 256 * 1024 * 1024; // 256MB minimum
            if memory_limit < MIN_MEMORY_LIMIT {
                eprintln!(
                    "Warning: Memory limit {}MB is too low, adjusting to {}MB",
                    memory_limit / (1024 * 1024),
                    MIN_MEMORY_LIMIT / (1024 * 1024)
                );
                self.memory_limit_bytes = Some(MIN_MEMORY_LIMIT);
            }
        }

        // Ensure reasonable PIDs limit
        if let Some(pids_limit) = self.pids_limit {
            if pids_limit < 64 {
                // Increase minimum PIDs
                eprintln!(
                    "Warning: PIDs limit {} is too low, adjusting to 64",
                    pids_limit
                );
                self.pids_limit = Some(64);
            }
        }

        self
    }
}

pub struct CgroupManager {
    cgroup_root: PathBuf,
    container_id: String,
    initialization_mode: bool, // Whether we're in container initialization
}

impl CgroupManager {
    pub fn new(container_id: String) -> Self {
        CgroupManager {
            cgroup_root: PathBuf::from("/sys/fs/cgroup"),
            container_id,
            initialization_mode: true,
        }
    }

    /// Create cgroups for the container with specified limits
    pub fn create_cgroups(&self, limits: &CgroupLimits) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "Creating cgroups for container: {}",
            self.container_id
        ));

        // Validate and adjust limits
        let validated_limits = limits.clone().validated();

        // Check if cgroup v2 is available
        let cgroup_v2_path = self.cgroup_root.join("cgroup.controllers");
        let use_cgroup_v2 = cgroup_v2_path.exists();

        if use_cgroup_v2 {
            self.create_cgroup_v2(&validated_limits)
        } else {
            self.create_cgroup_v1(&validated_limits)
        }
    }

    /// Create cgroup v2 (unified hierarchy)
    fn create_cgroup_v2(&self, limits: &CgroupLimits) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "Using cgroup v2 for container: {}",
            self.container_id
        ));

        let container_cgroup = self.cgroup_root.join("quilt").join(&self.container_id);

        // Create the container cgroup directory
        if let Err(e) = fs::create_dir_all(&container_cgroup) {
            return Err(format!("Failed to create cgroup directory: {}", e));
        }

        // Enable controllers in parent cgroup
        let parent_cgroup = self.cgroup_root.join("quilt");
        if parent_cgroup.exists() {
            let subtree_control = parent_cgroup.join("cgroup.subtree_control");
            if let Err(e) = fs::write(&subtree_control, "+memory +cpu +pids") {
                ConsoleLogger::warning(&format!(
                    "Failed to enable controllers in parent cgroup: {}",
                    e
                ));
            }
        }

        // Skip memory limits during initialization to prevent fork failures
        if !self.initialization_mode {
            // Set memory limit only after initialization
            if let Some(memory_limit) = limits.memory_limit_bytes {
                let memory_max = container_cgroup.join("memory.max");
                if let Err(e) = fs::write(&memory_max, memory_limit.to_string()) {
                    ConsoleLogger::warning(&format!("Failed to set memory limit: {}", e));
                } else {
                    ConsoleLogger::resource_limit_set("memory", &format!("{} bytes", memory_limit));
                }

                // Set memory.swap.max to prevent swap thrashing
                let memory_swap_max = container_cgroup.join("memory.swap.max");
                if let Err(e) = fs::write(&memory_swap_max, "0") {
                    ConsoleLogger::warning(&format!("Failed to disable swap: {}", e));
                }
            }
        } else {
            ConsoleLogger::debug(
                "Skipping memory limits during initialization to prevent fork failures",
            );
        }

        // Set CPU limits (these are generally safe during initialization)
        if let Some(cpu_quota) = limits.cpu_quota {
            if let Some(cpu_period) = limits.cpu_period {
                let cpu_max = container_cgroup.join("cpu.max");
                let cpu_config = if cpu_quota > 0 {
                    format!("{} {}", cpu_quota, cpu_period)
                } else {
                    "max".to_string()
                };
                if let Err(e) = fs::write(&cpu_max, cpu_config) {
                    ConsoleLogger::warning(&format!("Failed to set CPU limit: {}", e));
                } else {
                    ConsoleLogger::resource_limit_set(
                        "CPU quota",
                        &format!("{} microseconds per {} microseconds", cpu_quota, cpu_period),
                    );
                }
            }
        }

        // Set CPU weight (equivalent to shares in v1)
        if let Some(cpu_shares) = limits.cpu_shares {
            let cpu_weight = container_cgroup.join("cpu.weight");
            // Convert shares to weight (shares 1024 = weight 100)
            let weight = (cpu_shares * 100) / 1024;
            if let Err(e) = fs::write(&cpu_weight, weight.to_string()) {
                ConsoleLogger::warning(&format!("Failed to set CPU weight: {}", e));
            } else {
                ConsoleLogger::resource_limit_set("CPU weight", &weight.to_string());
            }
        }

        // Set PIDs limit (but use a generous limit during initialization)
        if let Some(pids_limit) = limits.pids_limit {
            let pids_max = container_cgroup.join("pids.max");
            let effective_pids_limit = if self.initialization_mode {
                std::cmp::max(pids_limit, 512) // Ensure at least 512 PIDs during init
            } else {
                pids_limit
            };
            if let Err(e) = fs::write(&pids_max, effective_pids_limit.to_string()) {
                ConsoleLogger::warning(&format!("Failed to set PIDs limit: {}", e));
            } else {
                ConsoleLogger::resource_limit_set("PIDs limit", &effective_pids_limit.to_string());
            }
        }

        Ok(())
    }

    /// Create cgroup v1 (legacy hierarchy)
    fn create_cgroup_v1(&self, limits: &CgroupLimits) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "Using cgroup v1 for container: {}",
            self.container_id
        ));

        // Skip memory cgroup creation during initialization to prevent fork failures
        if !self.initialization_mode {
            // Memory cgroup - only create after initialization
            if let Some(memory_limit) = limits.memory_limit_bytes {
                let memory_cgroup = self
                    .cgroup_root
                    .join("memory/quilt")
                    .join(&self.container_id);
                if let Err(e) = fs::create_dir_all(&memory_cgroup) {
                    ConsoleLogger::warning(&format!("Failed to create memory cgroup: {}", e));
                } else {
                    let memory_limit_file = memory_cgroup.join("memory.limit_in_bytes");
                    if let Err(e) = fs::write(&memory_limit_file, memory_limit.to_string()) {
                        ConsoleLogger::warning(&format!("Failed to set memory limit: {}", e));
                    } else {
                        ConsoleLogger::resource_limit_set(
                            "memory",
                            &format!("{} bytes", memory_limit),
                        );
                    }

                    // Disable memory swapping
                    let memory_swappiness = memory_cgroup.join("memory.swappiness");
                    if let Err(e) = fs::write(&memory_swappiness, "0") {
                        ConsoleLogger::warning(&format!("Failed to set memory swappiness: {}", e));
                    }
                }
            }
        } else {
            ConsoleLogger::debug(
                "Skipping memory cgroup during initialization to prevent fork failures",
            );
        }

        // CPU cgroup (generally safe during initialization)
        let cpu_cgroup = self.cgroup_root.join("cpu/quilt").join(&self.container_id);
        if let Err(e) = fs::create_dir_all(&cpu_cgroup) {
            ConsoleLogger::warning(&format!("Failed to create CPU cgroup: {}", e));
        } else {
            // Set CPU shares
            if let Some(cpu_shares) = limits.cpu_shares {
                let cpu_shares_file = cpu_cgroup.join("cpu.shares");
                if let Err(e) = fs::write(&cpu_shares_file, cpu_shares.to_string()) {
                    ConsoleLogger::warning(&format!("Failed to set CPU shares: {}", e));
                } else {
                    ConsoleLogger::resource_limit_set("CPU shares", &cpu_shares.to_string());
                }
            }

            // Set CPU quota
            if let Some(cpu_quota) = limits.cpu_quota {
                let cpu_quota_file = cpu_cgroup.join("cpu.cfs_quota_us");
                if let Err(e) = fs::write(&cpu_quota_file, cpu_quota.to_string()) {
                    ConsoleLogger::warning(&format!("Failed to set CPU quota: {}", e));
                } else {
                    ConsoleLogger::resource_limit_set(
                        "CPU quota",
                        &format!("{} microseconds", cpu_quota),
                    );
                }
            }

            // Set CPU period
            if let Some(cpu_period) = limits.cpu_period {
                let cpu_period_file = cpu_cgroup.join("cpu.cfs_period_us");
                if let Err(e) = fs::write(&cpu_period_file, cpu_period.to_string()) {
                    ConsoleLogger::warning(&format!("Failed to set CPU period: {}", e));
                } else {
                    ConsoleLogger::resource_limit_set(
                        "CPU period",
                        &format!("{} microseconds", cpu_period),
                    );
                }
            }
        }

        // PIDs cgroup (with generous limits during initialization)
        if let Some(pids_limit) = limits.pids_limit {
            let pids_cgroup = self.cgroup_root.join("pids/quilt").join(&self.container_id);
            if let Err(e) = fs::create_dir_all(&pids_cgroup) {
                ConsoleLogger::warning(&format!("Failed to create PIDs cgroup: {}", e));
            } else {
                let effective_pids_limit = if self.initialization_mode {
                    std::cmp::max(pids_limit, 512) // Ensure at least 512 PIDs during init
                } else {
                    pids_limit
                };
                let pids_limit_file = pids_cgroup.join("pids.max");
                if let Err(e) = fs::write(&pids_limit_file, effective_pids_limit.to_string()) {
                    ConsoleLogger::warning(&format!("Failed to set PIDs limit: {}", e));
                } else {
                    ConsoleLogger::resource_limit_set(
                        "PIDs limit",
                        &effective_pids_limit.to_string(),
                    );
                }
            }
        }

        Ok(())
    }

    /// Finalize cgroup settings after container initialization
    pub fn finalize_limits(&mut self, limits: &CgroupLimits) -> Result<(), String> {
        if !self.initialization_mode {
            return Ok(()); // Already finalized
        }

        ConsoleLogger::debug(&format!(
            "Finalizing cgroup limits for container: {}",
            self.container_id
        ));
        self.initialization_mode = false;

        // Apply final memory limits without headroom
        if let Some(memory_limit) = limits.memory_limit_bytes {
            let cgroup_v2_path = self.cgroup_root.join("cgroup.controllers");
            let use_cgroup_v2 = cgroup_v2_path.exists();

            if use_cgroup_v2 {
                let container_cgroup = self.cgroup_root.join("quilt").join(&self.container_id);
                let memory_max = container_cgroup.join("memory.max");
                if let Err(e) = fs::write(&memory_max, memory_limit.to_string()) {
                    ConsoleLogger::warning(&format!("Failed to finalize memory limit: {}", e));
                } else {
                    ConsoleLogger::resource_limit_set(
                        "finalized memory",
                        &format!("{} bytes", memory_limit),
                    );
                }
            } else {
                let memory_cgroup = self
                    .cgroup_root
                    .join("memory/quilt")
                    .join(&self.container_id);
                let memory_limit_file = memory_cgroup.join("memory.limit_in_bytes");
                if let Err(e) = fs::write(&memory_limit_file, memory_limit.to_string()) {
                    ConsoleLogger::warning(&format!("Failed to finalize memory limit: {}", e));
                } else {
                    ConsoleLogger::resource_limit_set(
                        "finalized memory",
                        &format!("{} bytes", memory_limit),
                    );
                }
            }
        }

        Ok(())
    }

    /// Add a process to the container's cgroups
    pub fn add_process(&self, pid: Pid) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "Adding process {} to cgroups for container: {}",
            ProcessUtils::pid_to_i32(pid),
            self.container_id
        ));

        let cgroup_v2_path = self.cgroup_root.join("cgroup.controllers");
        let use_cgroup_v2 = cgroup_v2_path.exists();

        if use_cgroup_v2 {
            self.add_process_v2(pid)
        } else {
            self.add_process_v1(pid)
        }
    }

    /// Add process to cgroup v2
    fn add_process_v2(&self, pid: Pid) -> Result<(), String> {
        let container_cgroup = self.cgroup_root.join("quilt").join(&self.container_id);
        let cgroup_procs = container_cgroup.join("cgroup.procs");

        // Single attempt - cgroups should be ready by the time we reach this point
        // If they're not ready, it's a configuration issue, not a timing issue
        if let Err(e) = fs::write(&cgroup_procs, ProcessUtils::pid_to_i32(pid).to_string()) {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Err(format!("Cgroup directory not found for container {}: ensure cgroups are properly created before adding processes", self.container_id));
            }
            return Err(format!(
                "Failed to add process {} to cgroup v2: {}",
                ProcessUtils::pid_to_i32(pid),
                e
            ));
        }

        ConsoleLogger::debug(&format!(
            "Successfully added process {} to cgroup v2",
            ProcessUtils::pid_to_i32(pid)
        ));
        Ok(())
    }

    /// Add process to cgroup v1
    fn add_process_v1(&self, pid: Pid) -> Result<(), String> {
        let pid_str = ProcessUtils::pid_to_i32(pid).to_string();

        // Add to memory cgroup
        let memory_cgroup = self
            .cgroup_root
            .join("memory/quilt")
            .join(&self.container_id);
        if memory_cgroup.exists() {
            let memory_tasks = memory_cgroup.join("tasks");
            if let Err(e) = fs::write(&memory_tasks, &pid_str) {
                ConsoleLogger::warning(&format!("Failed to add process to memory cgroup: {}", e));
            }
        }

        // Add to CPU cgroup
        let cpu_cgroup = self.cgroup_root.join("cpu/quilt").join(&self.container_id);
        if cpu_cgroup.exists() {
            let cpu_tasks = cpu_cgroup.join("tasks");
            if let Err(e) = fs::write(&cpu_tasks, &pid_str) {
                ConsoleLogger::warning(&format!("Failed to add process to CPU cgroup: {}", e));
            }
        }

        // Add to PIDs cgroup
        let pids_cgroup = self.cgroup_root.join("pids/quilt").join(&self.container_id);
        if pids_cgroup.exists() {
            let pids_tasks = pids_cgroup.join("tasks");
            if let Err(e) = fs::write(&pids_tasks, &pid_str) {
                ConsoleLogger::warning(&format!("Failed to add process to PIDs cgroup: {}", e));
            }
        }

        ConsoleLogger::debug(&format!(
            "Successfully added process {} to cgroup v1",
            ProcessUtils::pid_to_i32(pid)
        ));
        Ok(())
    }

    /// Get memory usage statistics
    pub fn get_memory_usage(&self) -> Result<u64, String> {
        let cgroup_v2_path = self.cgroup_root.join("cgroup.controllers");
        let use_cgroup_v2 = cgroup_v2_path.exists();

        if use_cgroup_v2 {
            let container_cgroup = self.cgroup_root.join("quilt").join(&self.container_id);
            let memory_current = container_cgroup.join("memory.current");
            if let Ok(content) = fs::read_to_string(&memory_current) {
                content
                    .trim()
                    .parse::<u64>()
                    .map_err(|e| format!("Failed to parse memory usage: {}", e))
            } else {
                Err("Failed to read memory usage".to_string())
            }
        } else {
            let memory_cgroup = self
                .cgroup_root
                .join("memory/quilt")
                .join(&self.container_id);
            let memory_usage = memory_cgroup.join("memory.usage_in_bytes");
            if let Ok(content) = fs::read_to_string(&memory_usage) {
                content
                    .trim()
                    .parse::<u64>()
                    .map_err(|e| format!("Failed to parse memory usage: {}", e))
            } else {
                Err("Failed to read memory usage".to_string())
            }
        }
    }

    /// Remove the container's cgroups
    pub fn cleanup(&self) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "Cleaning up cgroups for container: {}",
            self.container_id
        ));

        let cgroup_v2_path = self.cgroup_root.join("cgroup.controllers");
        let use_cgroup_v2 = cgroup_v2_path.exists();

        if use_cgroup_v2 {
            let container_cgroup = self.cgroup_root.join("quilt").join(&self.container_id);
            if container_cgroup.exists() {
                if let Err(e) = fs::remove_dir(&container_cgroup) {
                    ConsoleLogger::warning(&format!("Failed to remove cgroup v2 directory: {}", e));
                } else {
                    ConsoleLogger::debug("Successfully removed cgroup v2 directory");
                }
            }
        } else {
            // Remove v1 cgroups
            let cgroups = vec!["memory", "cpu", "pids"];
            for cgroup_type in cgroups {
                let cgroup_path = self
                    .cgroup_root
                    .join(format!("{}/quilt", cgroup_type))
                    .join(&self.container_id);
                if cgroup_path.exists() {
                    if let Err(e) = fs::remove_dir(&cgroup_path) {
                        ConsoleLogger::warning(&format!(
                            "Failed to remove {} cgroup directory: {}",
                            cgroup_type, e
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_cgroup_limits() {
        let limits = CgroupLimits::default();
        assert_eq!(limits.memory_limit_bytes, Some(512 * 1024 * 1024));
        assert_eq!(limits.cpu_shares, Some(1024));
        assert_eq!(limits.cpu_period, Some(100000));
        assert_eq!(limits.pids_limit, Some(1024));
    }

    #[test]
    fn test_cgroup_manager_creation() {
        let manager = CgroupManager::new("test-container".to_string());
        assert_eq!(manager.container_id, "test-container");
        assert_eq!(manager.cgroup_root, PathBuf::from("/sys/fs/cgroup"));
    }
}

/// System capacity monitoring for production load management.
///
/// Tracks container counts, memory usage, and CPU utilization to prevent
/// system overload and provide graceful degradation signals.
use crate::sync::SyncEngine;

/// Health status levels
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// System operating normally (< 80% capacity)
    Healthy,
    /// System approaching limits (80-95% capacity) - issue warnings
    Degraded,
    /// System at capacity (> 95%) - reject new requests
    Unavailable,
}

impl HealthStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unavailable => "unavailable",
        }
    }
}

/// Current system capacity status
#[derive(Debug, Clone)]
pub struct CapacityStatus {
    /// Current number of containers across all tenants
    pub containers_current: usize,
    /// Maximum containers allowed (0 = unlimited)
    pub containers_max: usize,
    /// Used memory in MB
    pub memory_used_mb: u64,
    /// Total memory in MB
    pub memory_total_mb: u64,
    /// Memory usage percentage (0-100)
    pub memory_percent: f64,
    /// CPU usage percentage (0-100)
    pub cpu_percent: f64,
    /// Whether system accepts new container requests
    pub accepts_requests: bool,
    /// Overall health status
    pub status: HealthStatus,
}

/// Capacity monitoring configuration
#[derive(Debug, Clone)]
pub struct CapacityConfig {
    /// Maximum containers globally (0 = unlimited)
    pub max_containers_global: usize,
    /// Maximum containers per tenant (0 = unlimited)
    #[allow(dead_code)] // Will be used in Phase 2 for tenant quota checks
    pub max_containers_per_tenant: usize,
    /// Memory usage threshold for degraded status (%)
    pub memory_degraded_threshold: f64,
    /// Memory usage threshold for unavailable status (%)
    pub memory_unavailable_threshold: f64,
    /// CPU usage threshold for degraded status (%)
    pub cpu_degraded_threshold: f64,
    /// CPU usage threshold for unavailable status (%)
    pub cpu_unavailable_threshold: f64,
}

impl Default for CapacityConfig {
    fn default() -> Self {
        Self {
            max_containers_global: 1000,        // Reasonable default for production
            max_containers_per_tenant: 100,     // Per-tenant limit
            memory_degraded_threshold: 80.0,    // Warn at 80% memory
            memory_unavailable_threshold: 95.0, // Reject at 95% memory
            cpu_degraded_threshold: 80.0,       // Warn at 80% CPU
            cpu_unavailable_threshold: 95.0,    // Reject at 95% CPU
        }
    }
}

/// System capacity monitor
pub struct CapacityMonitor {
    config: CapacityConfig,
}

impl CapacityMonitor {
    pub fn new(config: CapacityConfig) -> Self {
        Self { config }
    }

    /// Get current capacity status
    pub async fn get_capacity_status(
        &self,
        sync_engine: &SyncEngine,
    ) -> Result<CapacityStatus, String> {
        // Get container count (all states)
        let containers = sync_engine
            .list_containers(None)
            .await
            .map_err(|e| format!("Failed to query containers: {}", e))?;
        let containers_current = containers.len();

        // Get memory usage
        let (memory_used_mb, memory_total_mb, memory_percent) = Self::get_memory_usage()?;

        // Get CPU usage (returns 0.0 for now - can be enhanced with actual CPU monitoring)
        let cpu_percent = Self::get_cpu_usage();

        // Determine if system accepts requests
        let memory_available = memory_percent < self.config.memory_unavailable_threshold;
        let cpu_available = cpu_percent < self.config.cpu_unavailable_threshold;
        let containers_available = self.config.max_containers_global == 0
            || containers_current < self.config.max_containers_global;

        let accepts_requests = memory_available && cpu_available && containers_available;

        // Determine overall health status
        let status = if !accepts_requests {
            HealthStatus::Unavailable
        } else if memory_percent >= self.config.memory_degraded_threshold
            || cpu_percent >= self.config.cpu_degraded_threshold
            || (self.config.max_containers_global > 0
                && containers_current as f64 >= self.config.max_containers_global as f64 * 0.8)
        {
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        Ok(CapacityStatus {
            containers_current,
            containers_max: self.config.max_containers_global,
            memory_used_mb,
            memory_total_mb,
            memory_percent,
            cpu_percent,
            accepts_requests,
            status,
        })
    }

    /// Get memory usage from /proc/meminfo
    fn get_memory_usage() -> Result<(u64, u64, f64), String> {
        let meminfo = std::fs::read_to_string("/proc/meminfo")
            .map_err(|e| format!("Failed to read /proc/meminfo: {}", e))?;

        let mut mem_total_kb = 0u64;
        let mut mem_available_kb = 0u64;

        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                mem_total_kb = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
            } else if line.starts_with("MemAvailable:") {
                mem_available_kb = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
            }
        }

        if mem_total_kb == 0 {
            return Err("Could not parse memory info".to_string());
        }

        let mem_used_kb = mem_total_kb.saturating_sub(mem_available_kb);
        let memory_percent = if mem_total_kb > 0 {
            (mem_used_kb as f64 / mem_total_kb as f64) * 100.0
        } else {
            0.0
        };

        Ok((
            mem_used_kb / 1024,  // Convert to MB
            mem_total_kb / 1024, // Convert to MB
            memory_percent,
        ))
    }

    /// Get CPU usage percentage
    /// Returns 0.0 for now - can be enhanced with actual CPU monitoring
    fn get_cpu_usage() -> f64 {
        // TODO: Implement actual CPU usage monitoring
        // For now, return 0.0 to indicate CPU is not a bottleneck
        // Can be enhanced by reading /proc/stat and calculating usage over time
        0.0
    }

    /// Check if tenant is within quota
    #[allow(dead_code)] // Will be used in Phase 2 for capacity middleware
    pub async fn check_tenant_quota(
        &self,
        _sync_engine: &SyncEngine,
        _tenant_id: &str,
    ) -> Result<bool, String> {
        Ok(true)
    }

    /// Get configuration
    #[allow(dead_code)] // May be used for runtime config updates
    pub fn config(&self) -> &CapacityConfig {
        &self.config
    }
}

/// Component health check result
#[derive(Debug, Clone)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub message: Option<String>,
}

impl ComponentHealth {
    pub fn healthy(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: HealthStatus::Healthy,
            message: None,
        }
    }

    pub fn degraded(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: HealthStatus::Degraded,
            message: Some(message.into()),
        }
    }

    pub fn unavailable(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: HealthStatus::Unavailable,
            message: Some(message.into()),
        }
    }
}

/// Perform component health checks
pub async fn check_components(sync_engine: &SyncEngine) -> Vec<ComponentHealth> {
    let mut components = Vec::new();

    // Database health check - test with simple query
    match sync_engine.list_containers(None).await {
        Ok(_) => components.push(ComponentHealth::healthy("database")),
        Err(e) => components.push(ComponentHealth::unavailable(
            "database",
            format!("Database error: {}", e),
        )),
    }

    // Cgroups health check
    let cgroup_available = std::path::Path::new("/sys/fs/cgroup").exists();
    if cgroup_available {
        components.push(ComponentHealth::healthy("cgroups"));
    } else {
        components.push(ComponentHealth::unavailable(
            "cgroups",
            "Cgroup filesystem not found",
        ));
    }

    // Network namespace check
    let netns_available = std::path::Path::new("/proc/self/ns/net").exists();
    if netns_available {
        components.push(ComponentHealth::healthy("namespaces"));
    } else {
        components.push(ComponentHealth::degraded(
            "namespaces",
            "Network namespace support limited",
        ));
    }

    components
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status_strings() {
        assert_eq!(HealthStatus::Healthy.as_str(), "healthy");
        assert_eq!(HealthStatus::Degraded.as_str(), "degraded");
        assert_eq!(HealthStatus::Unavailable.as_str(), "unavailable");
    }

    #[test]
    fn test_default_capacity_config() {
        let config = CapacityConfig::default();
        assert_eq!(config.max_containers_global, 1000);
        assert_eq!(config.memory_degraded_threshold, 80.0);
        assert_eq!(config.memory_unavailable_threshold, 95.0);
    }

    #[test]
    fn test_memory_usage() {
        // This test will only work on Linux with /proc/meminfo
        if std::path::Path::new("/proc/meminfo").exists() {
            let result = CapacityMonitor::get_memory_usage();
            assert!(result.is_ok());
            let (_used, total, percent) = result.unwrap();
            assert!(total > 0);
            assert!((0.0..=100.0).contains(&percent));
        }
    }
}

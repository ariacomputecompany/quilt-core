use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerMetrics {
    pub container_id: String,
    pub timestamp: u64,
    pub cpu: CpuMetrics,
    pub memory: MemoryMetrics,
    pub network: NetworkMetrics,
    pub disk: DiskMetrics,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CpuMetrics {
    pub usage_usec: u64,
    pub user_usec: u64,
    pub system_usec: u64,
    pub nr_periods: u64,
    pub nr_throttled: u64,
    pub throttled_usec: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryMetrics {
    pub current_bytes: u64,
    pub peak_bytes: u64,
    pub limit_bytes: u64,
    pub cache_bytes: u64,
    pub rss_bytes: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkMetrics {
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub rx_packets: u64,
    pub tx_packets: u64,
    pub rx_errors: u64,
    pub tx_errors: u64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskMetrics {
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub read_ops: u64,
    pub write_ops: u64,
}

pub struct MetricsCollector {
    cgroup_root: String,
    proc_root: String,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            cgroup_root: "/sys/fs/cgroup".to_string(),
            proc_root: "/proc".to_string(),
        }
    }

    pub fn collect_container_metrics(
        &self,
        container_id: &str,
        pid: Option<i32>,
    ) -> Result<ContainerMetrics, String> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let cpu = self.collect_cpu_metrics(container_id)?;
        let memory = self.collect_memory_metrics(container_id)?;
        let network = self.collect_network_metrics(pid)?;
        let disk = self.collect_disk_metrics(pid)?;

        Ok(ContainerMetrics {
            container_id: container_id.to_string(),
            timestamp,
            cpu,
            memory,
            network,
            disk,
        })
    }

    fn collect_cpu_metrics(&self, container_id: &str) -> Result<CpuMetrics, String> {
        let mut metrics = CpuMetrics::default();

        // Try cgroup v2 first
        let cgroup_v2_path = Path::new(&self.cgroup_root).join("cgroup.controllers");
        if cgroup_v2_path.exists() {
            let cpu_stat_path = Path::new(&self.cgroup_root)
                .join("quilt")
                .join(container_id)
                .join("cpu.stat");

            if let Ok(content) = fs::read_to_string(&cpu_stat_path) {
                for line in content.lines() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() == 2 {
                        match parts[0] {
                            "usage_usec" => metrics.usage_usec = parts[1].parse().unwrap_or(0),
                            "user_usec" => metrics.user_usec = parts[1].parse().unwrap_or(0),
                            "system_usec" => metrics.system_usec = parts[1].parse().unwrap_or(0),
                            "nr_periods" => metrics.nr_periods = parts[1].parse().unwrap_or(0),
                            "nr_throttled" => metrics.nr_throttled = parts[1].parse().unwrap_or(0),
                            "throttled_usec" => {
                                metrics.throttled_usec = parts[1].parse().unwrap_or(0)
                            }
                            _ => {}
                        }
                    }
                }
            }
        } else {
            // Fallback to cgroup v1
            let cpu_acct_path = Path::new(&self.cgroup_root)
                .join("cpu,cpuacct/quilt")
                .join(container_id);

            if let Ok(usage) = fs::read_to_string(cpu_acct_path.join("cpuacct.usage")) {
                metrics.usage_usec = usage.trim().parse::<u64>().unwrap_or(0) / 1000;
                // Convert ns to us
            }

            if let Ok(stat) = fs::read_to_string(cpu_acct_path.join("cpuacct.stat")) {
                for line in stat.lines() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() == 2 {
                        match parts[0] {
                            "user" => {
                                metrics.user_usec = parts[1].parse::<u64>().unwrap_or(0) * 10000
                            } // jiffies to us
                            "system" => {
                                metrics.system_usec = parts[1].parse::<u64>().unwrap_or(0) * 10000
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(metrics)
    }

    fn collect_memory_metrics(&self, container_id: &str) -> Result<MemoryMetrics, String> {
        let mut metrics = MemoryMetrics::default();

        // Try cgroup v2 first
        let cgroup_v2_path = Path::new(&self.cgroup_root).join("cgroup.controllers");
        if cgroup_v2_path.exists() {
            let memory_path = Path::new(&self.cgroup_root)
                .join("quilt")
                .join(container_id);

            if let Ok(current) = fs::read_to_string(memory_path.join("memory.current")) {
                metrics.current_bytes = current.trim().parse().unwrap_or(0);
            }

            if let Ok(peak) = fs::read_to_string(memory_path.join("memory.peak")) {
                metrics.peak_bytes = peak.trim().parse().unwrap_or(0);
            }

            if let Ok(max) = fs::read_to_string(memory_path.join("memory.max")) {
                metrics.limit_bytes = max.trim().parse().unwrap_or(u64::MAX);
                if metrics.limit_bytes == u64::MAX {
                    metrics.limit_bytes = 0; // No limit
                }
            }

            if let Ok(stat) = fs::read_to_string(memory_path.join("memory.stat")) {
                for line in stat.lines() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() == 2 {
                        match parts[0] {
                            "file" => metrics.cache_bytes = parts[1].parse().unwrap_or(0),
                            "anon" => metrics.rss_bytes = parts[1].parse().unwrap_or(0),
                            _ => {}
                        }
                    }
                }
            }
        } else {
            // Fallback to cgroup v1
            let memory_path = Path::new(&self.cgroup_root)
                .join("memory/quilt")
                .join(container_id);

            if let Ok(usage) = fs::read_to_string(memory_path.join("memory.usage_in_bytes")) {
                metrics.current_bytes = usage.trim().parse().unwrap_or(0);
            }

            if let Ok(limit) = fs::read_to_string(memory_path.join("memory.limit_in_bytes")) {
                metrics.limit_bytes = limit.trim().parse().unwrap_or(0);
            }

            if let Ok(stat) = fs::read_to_string(memory_path.join("memory.stat")) {
                for line in stat.lines() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() == 2 {
                        match parts[0] {
                            "cache" => metrics.cache_bytes = parts[1].parse().unwrap_or(0),
                            "rss" => metrics.rss_bytes = parts[1].parse().unwrap_or(0),
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(metrics)
    }

    fn collect_network_metrics(&self, pid: Option<i32>) -> Result<NetworkMetrics, String> {
        let mut metrics = NetworkMetrics::default();

        if let Some(pid) = pid {
            // Read network stats from /proc/[pid]/net/dev
            let net_dev_path = Path::new(&self.proc_root)
                .join(pid.to_string())
                .join("net/dev");

            if let Ok(content) = fs::read_to_string(&net_dev_path) {
                for line in content.lines().skip(2) {
                    // Skip header lines
                    if !line.contains("lo:") {
                        // Skip loopback
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 16 {
                            // Accumulate all non-lo interfaces
                            metrics.rx_bytes += parts[1].parse::<u64>().unwrap_or(0);
                            metrics.rx_packets += parts[2].parse::<u64>().unwrap_or(0);
                            metrics.rx_errors += parts[3].parse::<u64>().unwrap_or(0);
                            metrics.tx_bytes += parts[9].parse::<u64>().unwrap_or(0);
                            metrics.tx_packets += parts[10].parse::<u64>().unwrap_or(0);
                            metrics.tx_errors += parts[11].parse::<u64>().unwrap_or(0);
                        }
                    }
                }
            }
        }

        Ok(metrics)
    }

    fn collect_disk_metrics(&self, pid: Option<i32>) -> Result<DiskMetrics, String> {
        let mut metrics = DiskMetrics::default();

        if let Some(pid) = pid {
            // Read I/O stats from /proc/[pid]/io
            let io_path = Path::new(&self.proc_root).join(pid.to_string()).join("io");

            if let Ok(content) = fs::read_to_string(&io_path) {
                for line in content.lines() {
                    let parts: Vec<&str> = line.split(':').collect();
                    if parts.len() == 2 {
                        let value = parts[1].trim().parse::<u64>().unwrap_or(0);
                        match parts[0] {
                            "read_bytes" => metrics.read_bytes = value,
                            "write_bytes" => metrics.write_bytes = value,
                            "syscr" => metrics.read_ops = value,
                            "syscw" => metrics.write_ops = value,
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(metrics)
    }
}

/// System-wide metrics for the Quilt runtime
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    pub timestamp: u64,
    pub containers_total: u64,
    pub containers_running: u64,
    pub containers_stopped: u64,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
    pub cpu_count: u64,
    pub load_average: [f64; 3],
    pub uptime_seconds: u64,
}

impl SystemMetrics {
    pub fn collect() -> Result<Self, String> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Get system memory info
        let (memory_used_mb, memory_total_mb) = Self::get_memory_info()?;

        // Get CPU count
        let cpu_count = num_cpus::get() as u64;

        // Get load average
        let load_average = Self::get_load_average()?;

        // Get uptime
        let uptime_seconds = Self::get_uptime()?;

        Ok(SystemMetrics {
            timestamp,
            containers_total: 0,   // Will be filled by caller
            containers_running: 0, // Will be filled by caller
            containers_stopped: 0, // Will be filled by caller
            memory_used_mb,
            memory_total_mb,
            cpu_count,
            load_average,
            uptime_seconds,
        })
    }

    fn get_memory_info() -> Result<(u64, u64), String> {
        let meminfo = fs::read_to_string("/proc/meminfo")
            .map_err(|e| format!("Failed to read /proc/meminfo: {}", e))?;

        let mut total_kb = 0u64;
        let mut available_kb = 0u64;

        for line in meminfo.lines() {
            if line.starts_with("MemTotal:") {
                total_kb = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
            } else if line.starts_with("MemAvailable:") {
                available_kb = line
                    .split_whitespace()
                    .nth(1)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
            }
        }

        let total_mb = total_kb / 1024;
        let used_mb = (total_kb - available_kb) / 1024;

        Ok((used_mb, total_mb))
    }

    fn get_load_average() -> Result<[f64; 3], String> {
        let loadavg = fs::read_to_string("/proc/loadavg")
            .map_err(|e| format!("Failed to read /proc/loadavg: {}", e))?;

        let parts: Vec<&str> = loadavg.split_whitespace().collect();
        if parts.len() >= 3 {
            Ok([
                parts[0].parse().unwrap_or(0.0),
                parts[1].parse().unwrap_or(0.0),
                parts[2].parse().unwrap_or(0.0),
            ])
        } else {
            Ok([0.0, 0.0, 0.0])
        }
    }

    fn get_uptime() -> Result<u64, String> {
        let uptime_str = fs::read_to_string("/proc/uptime")
            .map_err(|e| format!("Failed to read /proc/uptime: {}", e))?;

        uptime_str
            .split_whitespace()
            .next()
            .and_then(|s| s.parse::<f64>().ok())
            .map(|f| f as u64)
            .ok_or_else(|| "Failed to parse uptime".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_metrics() {
        let metrics = SystemMetrics::collect();
        assert!(metrics.is_ok());
        let m = metrics.unwrap();
        assert!(m.cpu_count > 0);
        assert!(m.memory_total_mb > 0);
    }
}

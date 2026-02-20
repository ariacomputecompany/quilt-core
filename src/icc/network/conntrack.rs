use crate::icc::network::error::{NetworkError, NetworkResult};
use std::net::Ipv4Addr;

/// Conntrack management to prevent table exhaustion under high churn.
pub struct ConntrackManager;

#[allow(dead_code)]
impl ConntrackManager {
    /// Flush all conntrack entries for a specific IP (both source and destination).
    /// Must be called BEFORE veth deletion during teardown.
    pub async fn flush_for_ip(ip: Ipv4Addr) -> NetworkResult<()> {
        let ip_str = ip.to_string();

        // Flush source entries
        let _ = tokio::process::Command::new("conntrack")
            .args(["-D", "-s", &ip_str])
            .output()
            .await;

        // Flush destination entries
        let _ = tokio::process::Command::new("conntrack")
            .args(["-D", "-d", &ip_str])
            .output()
            .await;

        tracing::debug!("Conntrack entries flushed for {}", ip);
        Ok(())
    }

    /// Get current conntrack entry count
    pub fn get_count() -> NetworkResult<u64> {
        let content = std::fs::read_to_string("/proc/sys/net/netfilter/nf_conntrack_count")
            .unwrap_or_else(|_| "0".to_string());
        Ok(content.trim().parse().unwrap_or(0))
    }

    /// Get maximum conntrack table size
    pub fn get_max() -> NetworkResult<u64> {
        let content = std::fs::read_to_string("/proc/sys/net/netfilter/nf_conntrack_max")
            .unwrap_or_else(|_| "65536".to_string());
        Ok(content.trim().parse().unwrap_or(65536))
    }

    /// Set maximum conntrack table size (call at startup)
    pub fn set_max(max_entries: u64) -> NetworkResult<()> {
        std::fs::write(
            "/proc/sys/net/netfilter/nf_conntrack_max",
            max_entries.to_string(),
        )
        .map_err(|e| {
            NetworkError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!("Failed to set conntrack max: {}", e),
            ))
        })?;

        tracing::info!("Conntrack max set to {}", max_entries);
        Ok(())
    }

    /// Get conntrack utilization percentage
    pub fn utilization_percent() -> f64 {
        let count = Self::get_count().unwrap_or(0) as f64;
        let max = Self::get_max().unwrap_or(65536) as f64;
        if max == 0.0 {
            return 0.0;
        }
        (count / max) * 100.0
    }
}

use crate::icc::network::error::{NetworkError, NetworkResult};

/// Per-session traffic control via tc (token bucket filter).
/// Applied to the host-side veth so the container cannot bypass.
pub struct BandwidthLimiter;

#[allow(dead_code)]
impl BandwidthLimiter {
    /// Set egress (upload from container) rate limit on host-side veth.
    /// Uses tbf (token bucket filter) qdisc.
    pub async fn set_egress_limit(veth: &str, rate_mbit: u32, burst_kb: u32) -> NetworkResult<()> {
        if rate_mbit == 0 {
            return Ok(()); // No limit
        }

        // Remove existing qdisc first (ignore errors)
        let _ = Self::run_tc(&format!("qdisc del dev {} root", veth)).await;

        // Add tbf qdisc: rate limiting with burst buffer
        let cmd = format!(
            "qdisc add dev {} root tbf rate {}mbit burst {}kb latency 50ms",
            veth, rate_mbit, burst_kb
        );
        Self::run_tc(&cmd).await?;

        tracing::debug!(
            "Bandwidth limit set: {} rate={}mbit burst={}kb",
            veth,
            rate_mbit,
            burst_kb
        );
        Ok(())
    }

    /// Set ingress (download to container) rate limit using ingress qdisc + police.
    pub async fn set_ingress_limit(veth: &str, rate_mbit: u32, burst_kb: u32) -> NetworkResult<()> {
        if rate_mbit == 0 {
            return Ok(());
        }

        // Add ingress qdisc
        let _ = Self::run_tc(&format!("qdisc del dev {} ingress", veth)).await;
        Self::run_tc(&format!("qdisc add dev {} ingress", veth)).await?;

        // Add police filter
        let cmd = format!(
            "filter add dev {} parent ffff: protocol ip u32 match u32 0 0 \
             police rate {}mbit burst {}kb drop flowid :1",
            veth, rate_mbit, burst_kb
        );
        Self::run_tc(&cmd).await?;

        tracing::debug!(
            "Ingress limit set: {} rate={}mbit burst={}kb",
            veth,
            rate_mbit,
            burst_kb
        );
        Ok(())
    }

    /// Remove all tc limits from a veth
    pub async fn remove_limits(veth: &str) -> NetworkResult<()> {
        let _ = Self::run_tc(&format!("qdisc del dev {} root", veth)).await;
        let _ = Self::run_tc(&format!("qdisc del dev {} ingress", veth)).await;
        Ok(())
    }

    /// Read egress bytes from tc stats (replacement for iptables egress tracking)
    pub async fn read_egress_bytes(veth: &str) -> NetworkResult<u64> {
        let output = tokio::process::Command::new("tc")
            .args(["-s", "qdisc", "show", "dev", veth])
            .output()
            .await
            .map_err(NetworkError::Io)?;

        if !output.status.success() {
            return Ok(0);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        // Parse "Sent X bytes ..." from tc output
        for line in stdout.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("Sent ") {
                if let Some(bytes_str) = trimmed.strip_prefix("Sent ") {
                    if let Some(end) = bytes_str.find(' ') {
                        if let Ok(bytes) = bytes_str[..end].parse::<u64>() {
                            return Ok(bytes);
                        }
                    }
                }
            }
        }

        Ok(0)
    }

    async fn run_tc(args: &str) -> NetworkResult<()> {
        let mut cmd_parts = vec!["tc"];
        cmd_parts.extend(args.split_whitespace());

        let output = tokio::process::Command::new(cmd_parts[0])
            .args(&cmd_parts[1..])
            .output()
            .await
            .map_err(NetworkError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(NetworkError::Command {
                cmd: format!("tc {}", args),
                stderr: stderr.to_string(),
            });
        }
        Ok(())
    }
}

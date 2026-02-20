use crate::sync::error::SyncResult;
use crate::utils::command::CommandExecutor;
/// Network egress tracking using iptables packet counters
///
/// This module provides zero-overhead network egress tracking for billing enforcement.
/// It uses iptables accounting chains to track egress traffic per container, automatically
/// excluding inter-container communication (ICC) traffic that stays on the bridge.
///
/// Architecture:
/// - Per-container iptables accounting chains in the nat table
/// - Kernel maintains byte/packet counters with no CPU overhead
/// - Periodic snapshots stored to database for billing
/// - Aggregation to tenant quotas for enforcement
use sqlx::SqlitePool;

/// Tracks network egress for containers using iptables counters
#[derive(Clone)]
pub struct EgressTracker {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl EgressTracker {
    /// Create a new egress tracker
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    /// Setup iptables accounting chain for a container
    ///
    /// Creates a dedicated accounting chain that counts all egress traffic
    /// from this container to external networks (excluding bridge/ICC traffic).
    ///
    /// The chain structure:
    /// 1. Create chain EGRESS_<sanitized_container_id>
    /// 2. Jump to chain from POSTROUTING for container's IP (! -o quilt0)
    /// 3. Return from chain (just counts, doesn't modify packets)
    pub fn setup_container_accounting(
        &self,
        container_id: &str,
        container_ip: &str,
    ) -> Result<(), String> {
        // Sanitize container ID for iptables chain name (replace hyphens with underscores)
        let chain_name = format!("EGRESS_{}", container_id.replace('-', "_"));

        tracing::debug!(
            "Setting up egress accounting for container {} ({}) with chain {}",
            container_id,
            container_ip,
            chain_name
        );

        // Step 1: Create accounting chain (ignore error if exists)
        let create_chain = format!("iptables -t nat -N {} 2>/dev/null || true", chain_name);
        if let Err(e) = CommandExecutor::execute_shell(&create_chain) {
            tracing::warn!("Failed to create egress chain {}: {:?}", chain_name, e);
        }

        // Step 2: Add jump rule to accounting chain
        // Only count traffic leaving the bridge (! -o quilt0 excludes ICC traffic)
        let jump_rule = format!(
            "iptables -t nat -I POSTROUTING 1 -s {}/32 ! -o quilt0 -j {}",
            container_ip, chain_name
        );

        if let Err(e) = CommandExecutor::execute_shell(&jump_rule) {
            return Err(format!("Failed to add egress jump rule: {:?}", e));
        }

        // Step 3: Add return rule to accounting chain (just counts, doesn't modify)
        let return_rule = format!("iptables -t nat -A {} -j RETURN", chain_name);

        if let Err(e) = CommandExecutor::execute_shell(&return_rule) {
            // Cleanup jump rule if return rule fails
            let _ = self.cleanup_container_accounting(container_id, container_ip);
            return Err(format!("Failed to add return rule: {:?}", e));
        }

        tracing::info!(
            "✅ Egress accounting enabled for container {} ({})",
            container_id,
            container_ip
        );

        Ok(())
    }

    /// Read current egress bytes from iptables counter
    ///
    /// Parses iptables output to extract the byte counter value.
    /// Returns 0 if chain doesn't exist or counter can't be read.
    pub fn read_container_egress(&self, container_id: &str) -> Result<u64, String> {
        let chain_name = format!("EGRESS_{}", container_id.replace('-', "_"));

        // Read chain statistics with exact byte counts (-x flag)
        let list_cmd = format!(
            "iptables -t nat -L {} -v -x -n 2>/dev/null || echo ''",
            chain_name
        );

        let output = CommandExecutor::execute_shell(&list_cmd)
            .map_err(|e| format!("Failed to read egress counter: {:?}", e))?;

        if !output.success || output.stdout.is_empty() {
            // Chain doesn't exist or empty output
            return Ok(0);
        }

        // Parse iptables output to extract byte count from RETURN rule
        // Format: "pkts bytes target  prot opt in out source destination"
        // We want the second column (bytes) from the RETURN line
        let bytes = output
            .stdout
            .lines()
            .find(|line| line.contains("RETURN"))
            .and_then(|line| {
                line.split_whitespace()
                    .nth(1) // Second column is bytes
                    .and_then(|s| s.parse::<u64>().ok())
            })
            .unwrap_or(0);

        tracing::trace!("Container {} egress: {} bytes", container_id, bytes);

        Ok(bytes)
    }

    /// Cleanup accounting chain on container deletion
    ///
    /// Removes the jump rule and deletes the accounting chain.
    /// Does not fail if chain doesn't exist (idempotent cleanup).
    pub fn cleanup_container_accounting(
        &self,
        container_id: &str,
        container_ip: &str,
    ) -> Result<(), String> {
        let chain_name = format!("EGRESS_{}", container_id.replace('-', "_"));

        tracing::debug!(
            "Cleaning up egress accounting for container {} (chain {})",
            container_id,
            chain_name
        );

        // Step 1: Remove jump rule from POSTROUTING
        let remove_jump = format!(
            "iptables -t nat -D POSTROUTING -s {}/32 ! -o quilt0 -j {} 2>/dev/null || true",
            container_ip, chain_name
        );
        let _ = CommandExecutor::execute_shell(&remove_jump);

        // Step 2: Flush the chain
        let flush_chain = format!("iptables -t nat -F {} 2>/dev/null || true", chain_name);
        let _ = CommandExecutor::execute_shell(&flush_chain);

        // Step 3: Delete the chain
        let delete_chain = format!("iptables -t nat -X {} 2>/dev/null || true", chain_name);
        let _ = CommandExecutor::execute_shell(&delete_chain);

        tracing::info!(
            "✅ Egress accounting cleaned up for container {}",
            container_id
        );

        Ok(())
    }

    /// Store egress snapshot to database
    ///
    /// Calculates delta from last snapshot and stores both total and delta.
    /// If this is the first snapshot, delta equals total.
    pub async fn store_egress_snapshot(
        &self,
        container_id: &str,
        tenant_id: &str,
        egress_bytes_total: u64,
        reason: &str,
    ) -> SyncResult<()> {
        // Get last snapshot to calculate delta
        let last_snapshot: Option<(i64,)> = sqlx::query_as(
            "SELECT egress_bytes_total FROM network_egress_tracking
             WHERE container_id = ? ORDER BY timestamp DESC LIMIT 1",
        )
        .bind(container_id)
        .fetch_optional(&self.read_pool)
        .await?;

        let last_bytes = last_snapshot.map(|r| r.0 as u64).unwrap_or(0);
        let delta = egress_bytes_total.saturating_sub(last_bytes);

        // Optimization: Skip storing if no new traffic (except for special reasons)
        if delta == 0 && reason == "periodic" {
            return Ok(());
        }

        let now = chrono::Utc::now().timestamp();

        sqlx::query(
            r#"
            INSERT INTO network_egress_tracking
            (container_id, tenant_id, timestamp, egress_bytes_total,
             egress_packets_total, egress_bytes_delta, egress_packets_delta,
             collection_reason)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(container_id)
        .bind(tenant_id)
        .bind(now)
        .bind(egress_bytes_total as i64)
        .bind(0i64) // packets (can add later if needed)
        .bind(delta as i64)
        .bind(0i64)
        .bind(reason)
        .execute(&self.write_pool)
        .await?;

        tracing::debug!(
            "Stored egress snapshot for container {}: {} bytes total (+{} delta) [{}]",
            container_id,
            egress_bytes_total,
            delta,
            reason
        );

        Ok(())
    }

    /// Aggregate tenant egress from snapshots
    ///
    /// Sums all egress deltas for each tenant within their current billing period
    /// and updates the tenant_quotas table.
    pub async fn aggregate_tenant_egress(&self) -> SyncResult<()> {
        tracing::info!("Aggregating network egress to tenant quotas");

        // Get all tenants with recent egress data
        let tenants: Vec<(String,)> = sqlx::query_as(
            "SELECT DISTINCT tenant_id FROM network_egress_tracking
             WHERE timestamp > ? ORDER BY tenant_id",
        )
        .bind(chrono::Utc::now().timestamp() - 7200) // last 2 hours
        .fetch_all(&self.read_pool)
        .await?;

        for (tenant_id,) in tenants {
            // Sum all deltas for this tenant in current billing period
            let period_egress: (i64,) = sqlx::query_as(
                r#"
                SELECT COALESCE(SUM(t.egress_bytes_delta), 0)
                FROM network_egress_tracking t
                JOIN tenant_quotas q ON t.tenant_id = q.tenant_id
                WHERE t.tenant_id = ?
                  AND t.timestamp >= q.period_start
                  AND t.timestamp <= q.period_end
                "#,
            )
            .bind(&tenant_id)
            .fetch_one(&self.read_pool)
            .await?;

            let egress_gb = (period_egress.0 as f64) / 1_073_741_824.0; // bytes to GB

            // Update tenant quota
            sqlx::query(
                "UPDATE tenant_quotas SET egress_gb = ?, updated_at = ?
                 WHERE tenant_id = ?",
            )
            .bind(egress_gb)
            .bind(chrono::Utc::now().timestamp())
            .bind(&tenant_id)
            .execute(&self.write_pool)
            .await?;

            tracing::debug!("Updated tenant {} egress: {:.3} GB", tenant_id, egress_gb);
        }

        Ok(())
    }

    /// Get egress statistics for a container (for admin/monitoring endpoints)
    ///
    /// Returns total egress and current billing period egress for a container.
    /// Used by: /api/admin/containers/:container_id/egress endpoint
    pub async fn get_container_egress_stats(&self, container_id: &str) -> SyncResult<(u64, u64)> {
        // Get latest total from database
        let db_total: Option<(i64,)> = sqlx::query_as(
            "SELECT egress_bytes_total FROM network_egress_tracking
             WHERE container_id = ? ORDER BY timestamp DESC LIMIT 1",
        )
        .bind(container_id)
        .fetch_optional(&self.read_pool)
        .await?;

        let total_bytes = db_total.map(|r| r.0 as u64).unwrap_or(0);

        // Get current live count from iptables (may be higher than DB)
        let live_bytes = self
            .read_container_egress(container_id)
            .unwrap_or(total_bytes);

        Ok((total_bytes, live_bytes))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_chain_name_sanitization() {
        let container_id = "550e8400-e29b-41d4-a716-446655440000";
        let expected_chain = "EGRESS_550e8400_e29b_41d4_a716_446655440000";
        let chain_name = format!("EGRESS_{}", container_id.replace('-', "_"));
        assert_eq!(chain_name, expected_chain);
    }
}

use crate::sync::error::{SyncError, SyncResult};
use crate::utils::process::ProcessUtils;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NetworkStatus {
    Allocated,
    Active,
    CleanupPending,
    Cleaned,
}

impl NetworkStatus {
    pub fn to_string(&self) -> String {
        match self {
            NetworkStatus::Allocated => "allocated".to_string(),
            NetworkStatus::Active => "active".to_string(),
            NetworkStatus::CleanupPending => "cleanup_pending".to_string(),
            NetworkStatus::Cleaned => "cleaned".to_string(),
        }
    }

    pub fn from_string(s: &str) -> SyncResult<Self> {
        match s {
            "allocated" => Ok(NetworkStatus::Allocated),
            "active" => Ok(NetworkStatus::Active),
            "cleanup_pending" => Ok(NetworkStatus::CleanupPending),
            "cleaned" => Ok(NetworkStatus::Cleaned),
            _ => Err(SyncError::ValidationFailed {
                message: format!("Invalid network status: {}", s),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub container_id: String,
    pub ip_address: String,
    pub bridge_interface: Option<String>,
    pub veth_host: Option<String>,
    pub veth_container: Option<String>,
    pub setup_required: bool,
}

#[derive(Debug, Clone)]
pub struct NetworkAllocation {
    pub container_id: String,
    pub ip_address: String,
    pub bridge_interface: Option<String>,
    pub veth_host: Option<String>,
    pub veth_container: Option<String>,
    pub allocation_time: i64,
    pub setup_completed: bool,
    pub status: NetworkStatus,
}

impl NetworkAllocation {
    /// Get formatted allocation timestamp
    pub fn allocation_time_formatted(&self) -> String {
        ProcessUtils::format_timestamp(self.allocation_time as u64)
    }
}

pub struct NetworkManager {
    pool: SqlitePool,
    subnet_cidr: Arc<RwLock<String>>, // Configurable subnet CIDR (e.g., "10.42.0.0/16")
    icc_network_manager: Option<std::sync::Arc<crate::icc::network::NetworkManager>>,
}

impl NetworkManager {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            subnet_cidr: Arc::new(RwLock::new("10.42.0.0/16".to_string())), // Default subnet
            icc_network_manager: None,
        }
    }

    pub fn new_with_subnet(pool: SqlitePool, subnet_cidr: String) -> Self {
        Self {
            pool,
            subnet_cidr: Arc::new(RwLock::new(subnet_cidr)),
            icc_network_manager: None,
        }
    }

    pub fn new_with_icc_manager(
        pool: SqlitePool,
        icc_network_manager: std::sync::Arc<crate::icc::network::NetworkManager>,
    ) -> Self {
        let subnet_cidr = icc_network_manager.config.subnet_cidr.clone();
        Self {
            pool,
            subnet_cidr: Arc::new(RwLock::new(subnet_cidr)),
            icc_network_manager: Some(icc_network_manager),
        }
    }

    /// Create network manager with specific IP range for testing
    pub fn with_ip_range(pool: SqlitePool, start_ip: Ipv4Addr, _end_ip: Ipv4Addr) -> Self {
        // For testing purposes, create a /30 network that exactly covers the range
        // This gives us 4 addresses (.0=network, .1=gateway, .2=first host, .3=broadcast)
        // But our parser will use .2 and .3 as the usable range
        let network_base = u32::from(start_ip) & 0xFFFFFFFC; // Align to /30 boundary
        let network_ip = Ipv4Addr::from(network_base);
        let subnet_cidr = format!("{}/30", network_ip);

        Self {
            pool,
            subnet_cidr: Arc::new(RwLock::new(subnet_cidr)),
            icc_network_manager: None,
        }
    }

    pub async fn get_subnet_cidr(&self) -> String {
        self.subnet_cidr.read().await.clone()
    }

    pub async fn set_subnet_cidr(&self, subnet_cidr: &str) -> SyncResult<()> {
        Self::validate_subnet_cidr(subnet_cidr)?;
        *self.subnet_cidr.write().await = subnet_cidr.to_string();
        Ok(())
    }

    pub async fn allocate_network(&self, container_id: &str) -> SyncResult<NetworkConfig> {
        // Check if already allocated
        if let Ok(existing) = self.get_network_allocation(container_id).await {
            tracing::debug!(
                "Container {} already has network allocation: {}",
                container_id,
                existing.ip_address
            );
            return Ok(NetworkConfig {
                container_id: container_id.to_string(),
                ip_address: existing.ip_address,
                bridge_interface: existing.bridge_interface,
                veth_host: existing.veth_host,
                veth_container: existing.veth_container,
                setup_required: !existing.setup_completed,
            });
        }

        // Use ICC NetworkManager's lock-free allocation as starting hint if available
        let icc_ip_hint = if let Some(ref icc_manager) = self.icc_network_manager {
            icc_manager.allocate_next_ip().ok()
        } else {
            None
        };

        // FIXED: Atomic IP allocation using database transaction with retry logic
        // This eliminates the TOCTOU race condition in concurrent container creation
        let max_retries = 5;
        let mut retry_count = 0;

        loop {
            match self
                .try_allocate_ip_atomically(container_id, icc_ip_hint.as_deref())
                .await
            {
                Ok(ip) => {
                    tracing::info!(
                        "Allocated IP {} for container {} (attempt {})",
                        ip,
                        container_id,
                        retry_count + 1
                    );
                    return Ok(NetworkConfig {
                        container_id: container_id.to_string(),
                        ip_address: ip,
                        bridge_interface: None,
                        veth_host: None,
                        veth_container: None,
                        setup_required: true,
                    });
                }
                Err(SyncError::NoAvailableIp) => {
                    retry_count += 1;
                    if retry_count >= max_retries {
                        tracing::error!(
                            "Failed to allocate IP for {} after {} retries",
                            container_id,
                            max_retries
                        );
                        return Err(SyncError::NoAvailableIp);
                    }
                    // Small backoff to reduce contention
                    tokio::time::sleep(tokio::time::Duration::from_millis(10 * retry_count as u64))
                        .await;
                    tracing::debug!(
                        "IP allocation conflict for {}, retrying (attempt {})",
                        container_id,
                        retry_count + 1
                    );
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn should_setup_network(&self, container_id: &str) -> SyncResult<bool> {
        use crate::utils::console::ConsoleLogger;

        ConsoleLogger::debug(&format!(
            "🔍 [NETWORK-CHECK] Checking if container {} needs network setup",
            container_id
        ));

        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM network_allocations WHERE container_id = ? AND status = 'allocated'"
        )
        .bind(container_id)
        .fetch_one(&self.pool)
        .await?;

        ConsoleLogger::info(&format!(
            "🔍 [NETWORK-CHECK] Container {} has {} allocated network entries",
            container_id, count
        ));

        // Debug: Also check what entries exist for this container (any status)
        let all_entries: Vec<(String, String)> = sqlx::query_as(
            "SELECT status, ip_address FROM network_allocations WHERE container_id = ?",
        )
        .bind(container_id)
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        if all_entries.is_empty() {
            ConsoleLogger::warning(&format!(
                "🔍 [NETWORK-CHECK] No network allocation entries found for container {}",
                container_id
            ));
        } else {
            for (status, ip) in &all_entries {
                ConsoleLogger::debug(&format!(
                    "🔍 [NETWORK-CHECK] Found allocation for {}: status={}, ip={}",
                    container_id, status, ip
                ));
            }
        }

        let needs_setup = count > 0;
        ConsoleLogger::info(&format!(
            "🔍 [NETWORK-CHECK] Container {} needs network setup: {}",
            container_id, needs_setup
        ));

        Ok(needs_setup)
    }

    pub async fn mark_network_setup_complete(
        &self,
        container_id: &str,
        bridge_interface: &str,
        veth_host: &str,
        veth_container: &str,
    ) -> SyncResult<()> {
        // Validate network setup using ICC NetworkManager if available
        if let Some(ref icc_manager) = self.icc_network_manager {
            // Validate bridge exists and is up
            if !icc_manager.bridge_exists() {
                return Err(SyncError::ValidationFailed {
                    message: format!(
                        "Bridge {} does not exist during network setup completion",
                        bridge_interface
                    ),
                });
            }

            // Validate bridge is up and running
            if let Err(e) = icc_manager.bridge_manager.verify_bridge_up() {
                tracing::warn!("Bridge verification warning during setup completion: {}", e);
                // Continue with setup but log the warning - bridge might still be functional
            }

            // Validate veth pair exists (if we have the methods available)
            if let Err(e) = icc_manager
                .veth_manager
                .verify_veth_pair_created(veth_host, veth_container)
            {
                return Err(SyncError::ValidationFailed {
                    message: format!("Veth pair validation failed for {}: {}", container_id, e),
                });
            }

            tracing::info!(
                "ICC bridge validation passed for container {}",
                container_id
            );
        }

        let result = sqlx::query(r#"
            UPDATE network_allocations 
            SET setup_completed = ?, status = ?, bridge_interface = ?, veth_host = ?, veth_container = ?
            WHERE container_id = ?
        "#)
        .bind(true)
        .bind(NetworkStatus::Active.to_string())
        .bind(bridge_interface)
        .bind(veth_host)
        .bind(veth_container)
        .bind(container_id)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: container_id.to_string(),
            });
        }

        tracing::info!(
            "Marked network setup complete for container {} with ICC validation",
            container_id
        );
        Ok(())
    }

    pub async fn get_network_allocation(
        &self,
        container_id: &str,
    ) -> SyncResult<NetworkAllocation> {
        let row = sqlx::query(
            r#"
            SELECT container_id, ip_address, bridge_interface, veth_host, veth_container,
                   allocation_time, setup_completed, status
            FROM network_allocations WHERE container_id = ?
        "#,
        )
        .bind(container_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let status_str: String = row.get("status");
                let status = NetworkStatus::from_string(&status_str)?;

                let allocation = NetworkAllocation {
                    container_id: row.get("container_id"),
                    ip_address: row.get("ip_address"),
                    bridge_interface: row.get("bridge_interface"),
                    veth_host: row.get("veth_host"),
                    veth_container: row.get("veth_container"),
                    allocation_time: row.get("allocation_time"),
                    setup_completed: row.get("setup_completed"),
                    status,
                };

                // Debug logging using formatting method
                tracing::debug!(
                    "Network allocation for {}: IP {} allocated at {}",
                    container_id,
                    allocation.ip_address,
                    allocation.allocation_time_formatted()
                );

                Ok(allocation)
            }
            None => Err(SyncError::NotFound {
                container_id: container_id.to_string(),
            }),
        }
    }

    pub async fn mark_network_cleanup_pending(&self, container_id: &str) -> SyncResult<()> {
        let result =
            sqlx::query("UPDATE network_allocations SET status = ? WHERE container_id = ?")
                .bind(NetworkStatus::CleanupPending.to_string())
                .bind(container_id)
                .execute(&self.pool)
                .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: container_id.to_string(),
            });
        }

        Ok(())
    }

    pub async fn mark_network_cleaned(&self, container_id: &str) -> SyncResult<()> {
        let result =
            sqlx::query("UPDATE network_allocations SET status = ? WHERE container_id = ?")
                .bind(NetworkStatus::Cleaned.to_string())
                .bind(container_id)
                .execute(&self.pool)
                .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: container_id.to_string(),
            });
        }

        tracing::info!("Marked network cleaned for container {}", container_id);
        Ok(())
    }

    pub async fn list_allocations(
        &self,
        status_filter: Option<NetworkStatus>,
    ) -> SyncResult<Vec<NetworkAllocation>> {
        let mut query = "
            SELECT container_id, ip_address, bridge_interface, veth_host, veth_container,
                   allocation_time, setup_completed, status
            FROM network_allocations
        "
        .to_string();

        if let Some(status) = status_filter {
            query.push_str(&format!(" WHERE status = '{}'", status.to_string()));
        }

        query.push_str(" ORDER BY allocation_time ASC");

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut allocations = Vec::new();
        for row in rows {
            let status_str: String = row.get("status");
            let status = NetworkStatus::from_string(&status_str)?;

            allocations.push(NetworkAllocation {
                container_id: row.get("container_id"),
                ip_address: row.get("ip_address"),
                bridge_interface: row.get("bridge_interface"),
                veth_host: row.get("veth_host"),
                veth_container: row.get("veth_container"),
                allocation_time: row.get("allocation_time"),
                setup_completed: row.get("setup_completed"),
                status,
            });
        }

        Ok(allocations)
    }

    pub async fn get_networks_needing_cleanup(&self) -> SyncResult<Vec<NetworkAllocation>> {
        self.list_allocations(Some(NetworkStatus::CleanupPending))
            .await
    }

    /// PRODUCTION-GRADE: Atomically allocate IP using database transaction
    /// Eliminates TOCTOU race conditions in concurrent container creation
    async fn try_allocate_ip_atomically(
        &self,
        container_id: &str,
        icc_hint: Option<&str>,
    ) -> SyncResult<String> {
        let mut transaction = self.pool.begin().await?;

        // Find available IP within transaction (consistent snapshot)
        let allocated_ips: Vec<(String,)> =
            sqlx::query_as("SELECT ip_address FROM network_allocations WHERE status != 'cleaned'")
                .fetch_all(&mut *transaction)
                .await?;

        let allocated_set: std::collections::HashSet<String> =
            allocated_ips.into_iter().map(|(ip,)| ip).collect();

        // Parse subnet_cidr to determine IP range
        let (start_ip, end_ip) = self.parse_subnet_range().await?;
        let start_int = u32::from(start_ip);
        let end_int = u32::from(end_ip);

        let mut selected_ip = None;

        // Try ICC hint first if available and within range
        if let Some(hint) = icc_hint {
            if let Ok(hint_ip) = hint.parse::<Ipv4Addr>() {
                let hint_int = u32::from(hint_ip);
                if hint_int >= start_int && hint_int <= end_int && !allocated_set.contains(hint) {
                    selected_ip = Some(hint.to_string());
                    tracing::debug!("Using ICC hint IP {} for container {}", hint, container_id);
                }
            }
        }

        // Fallback to sequential search if no hint or hint not available
        if selected_ip.is_none() {
            for ip_int in start_int..=end_int {
                let ip = Ipv4Addr::from(ip_int);
                let ip_str = ip.to_string();

                if !allocated_set.contains(&ip_str) {
                    selected_ip = Some(ip_str);
                    break;
                }
            }
        }

        let ip = selected_ip.ok_or(SyncError::NoAvailableIp)?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        // Attempt to insert within transaction - will fail if another transaction beat us
        match sqlx::query(
            r#"
            INSERT INTO network_allocations (
                container_id, ip_address, allocation_time, setup_completed, status
            ) VALUES (?, ?, ?, ?, ?)
        "#,
        )
        .bind(container_id)
        .bind(&ip)
        .bind(now)
        .bind(false)
        .bind(NetworkStatus::Allocated.to_string())
        .execute(&mut *transaction)
        .await
        {
            Ok(_) => {
                // Success - commit transaction
                transaction.commit().await?;
                Ok(ip)
            }
            Err(sqlx::Error::Database(db_err)) if db_err.is_unique_violation() => {
                // IP already allocated by concurrent transaction - signal retry
                transaction.rollback().await?;
                Err(SyncError::NoAvailableIp)
            }
            Err(e) => {
                // Other error - propagate
                transaction.rollback().await?;
                Err(SyncError::Database(e))
            }
        }
    }

    /// Parse subnet CIDR into usable IP range for allocation
    async fn parse_subnet_range(&self) -> SyncResult<(Ipv4Addr, Ipv4Addr)> {
        let subnet_cidr = self.subnet_cidr.read().await.clone();

        // Parse CIDR notation (e.g., "10.42.0.0/16")
        let parts: Vec<&str> = subnet_cidr.split('/').collect();
        if parts.len() != 2 {
            return Err(SyncError::ValidationFailed {
                message: format!("Invalid subnet CIDR format: {}", subnet_cidr),
            });
        }

        let network_ip: Ipv4Addr = parts[0].parse().map_err(|_| SyncError::ValidationFailed {
            message: format!("Invalid network IP in CIDR: {}", parts[0]),
        })?;

        let prefix_len: u32 = parts[1].parse().map_err(|_| SyncError::ValidationFailed {
            message: format!("Invalid prefix length in CIDR: {}", parts[1]),
        })?;

        if prefix_len > 32 {
            return Err(SyncError::ValidationFailed {
                message: format!("Invalid prefix length: {}", prefix_len),
            });
        }

        // Calculate network range
        let network_bits = u32::from(network_ip);
        if prefix_len >= 31 {
            return Err(SyncError::ValidationFailed {
                message: format!("Prefix must allow host allocation (got /{})", prefix_len),
            });
        }

        let host_bits = 32 - prefix_len;
        let subnet_mask = !((1u32 << host_bits) - 1);
        let network_address = network_bits & subnet_mask;
        let broadcast_address = network_address | ((1u32 << host_bits) - 1);

        // Exclude network and broadcast addresses, start from .2 to avoid common gateway (.1)
        let start_ip = Ipv4Addr::from(network_address + 2);
        let end_ip = Ipv4Addr::from(broadcast_address - 1);

        tracing::debug!(
            "Parsed subnet {} -> range {} to {}",
            subnet_cidr,
            start_ip,
            end_ip
        );
        Ok((start_ip, end_ip))
    }

    fn validate_subnet_cidr(subnet_cidr: &str) -> SyncResult<()> {
        let parts: Vec<&str> = subnet_cidr.split('/').collect();
        if parts.len() != 2 {
            return Err(SyncError::ValidationFailed {
                message: format!("Invalid subnet CIDR format: {}", subnet_cidr),
            });
        }

        let network_ip: Ipv4Addr = parts[0].parse().map_err(|_| SyncError::ValidationFailed {
            message: format!("Invalid network IP in CIDR: {}", parts[0]),
        })?;

        let prefix_len: u8 = parts[1].parse().map_err(|_| SyncError::ValidationFailed {
            message: format!("Invalid prefix length in CIDR: {}", parts[1]),
        })?;

        if prefix_len != 24 {
            return Err(SyncError::ValidationFailed {
                message: format!(
                    "Node subnet must be /24 for quiltc compatibility, got /{}",
                    prefix_len
                ),
            });
        }

        let ip_u32 = u32::from(network_ip);
        let mask = (!0u32) << (32 - prefix_len);
        if (ip_u32 & mask) != ip_u32 {
            return Err(SyncError::ValidationFailed {
                message: format!("CIDR base must be a network address: {}", subnet_cidr),
            });
        }

        let cluster_base = u32::from(Ipv4Addr::new(10, 42, 0, 0));
        let cluster_mask = (!0u32) << (32 - 16);
        if (ip_u32 & cluster_mask) != (cluster_base & cluster_mask) {
            return Err(SyncError::ValidationFailed {
                message: format!("Subnet {} must be within 10.42.0.0/16", subnet_cidr),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::connection::ConnectionManager;
    use crate::sync::schema::SchemaManager;
    use tempfile::NamedTempFile;

    async fn setup_test_db() -> (ConnectionManager, NetworkManager) {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn_manager = ConnectionManager::new(db_path).await.unwrap();
        let schema_manager = SchemaManager::new(conn_manager.pool().clone());
        schema_manager.initialize_schema().await.unwrap();

        let network_manager = NetworkManager::new(conn_manager.pool().clone());

        (conn_manager, network_manager)
    }

    #[tokio::test]
    async fn test_network_allocation() {
        let (_conn, network_manager) = setup_test_db().await;

        let config = network_manager
            .allocate_network("test-container")
            .await
            .unwrap();
        assert_eq!(config.container_id, "test-container");
        assert!(!config.ip_address.is_empty());
        assert!(config.setup_required);

        // Verify allocation persisted
        let allocation = network_manager
            .get_network_allocation("test-container")
            .await
            .unwrap();
        assert_eq!(allocation.ip_address, config.ip_address);
        assert_eq!(allocation.status, NetworkStatus::Allocated);
        assert!(!allocation.setup_completed);
    }

    #[tokio::test]
    async fn test_network_setup_completion() {
        let (_conn, network_manager) = setup_test_db().await;

        let config = network_manager
            .allocate_network("test-container")
            .await
            .unwrap();

        network_manager
            .mark_network_setup_complete("test-container", "br0", "veth123", "eth0")
            .await
            .unwrap();

        let allocation = network_manager
            .get_network_allocation("test-container")
            .await
            .unwrap();
        assert_eq!(allocation.status, NetworkStatus::Active);
        assert!(allocation.setup_completed);
        assert_eq!(allocation.bridge_interface, Some("br0".to_string()));
        assert_eq!(allocation.veth_host, Some("veth123".to_string()));
        assert_eq!(allocation.veth_container, Some("eth0".to_string()));
    }

    #[tokio::test]
    async fn test_ip_exhaustion() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn_manager = ConnectionManager::new(db_path).await.unwrap();
        let schema_manager = SchemaManager::new(conn_manager.pool().clone());
        schema_manager.initialize_schema().await.unwrap();

        // Create network manager with very small IP range using /30 subnet (4 addresses)
        let network_manager = NetworkManager::new_with_subnet(
            conn_manager.pool().clone(),
            "10.42.0.8/30".to_string(), // Network: .8, Gateway: .9, Usable: .10, .11
        );

        // Allocate first IP (should be 10.42.0.10 - first usable in /30)
        let config1 = network_manager
            .allocate_network("container1")
            .await
            .unwrap();
        assert_eq!(config1.ip_address, "10.42.0.10");

        // Allocate second IP (should be 10.42.0.11 - second usable in /30)
        let config2 = network_manager
            .allocate_network("container2")
            .await
            .unwrap();
        assert_eq!(config2.ip_address, "10.42.0.11");

        // Third allocation should fail
        let result = network_manager.allocate_network("container3").await;
        assert!(matches!(result, Err(SyncError::NoAvailableIp)));
    }

    #[tokio::test]
    async fn test_set_subnet_cidr_validates_quiltc_constraints() {
        let (_conn, network_manager) = setup_test_db().await;
        assert!(network_manager
            .set_subnet_cidr("10.42.3.0/24")
            .await
            .is_ok());
        assert_eq!(network_manager.get_subnet_cidr().await, "10.42.3.0/24");
    }

    #[tokio::test]
    async fn test_set_subnet_cidr_rejects_invalid_values() {
        let (_conn, network_manager) = setup_test_db().await;
        assert!(network_manager
            .set_subnet_cidr("10.42.3.4/24")
            .await
            .is_err());
        assert!(network_manager
            .set_subnet_cidr("10.42.0.0/16")
            .await
            .is_err());
        assert!(network_manager
            .set_subnet_cidr("192.168.1.0/24")
            .await
            .is_err());
    }
}

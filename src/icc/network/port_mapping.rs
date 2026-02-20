use crate::icc::network::error::{NetworkError, NetworkResult};
use sqlx::SqlitePool;
use std::net::Ipv4Addr;

/// Deterministic port mapping with DB backing.
/// For Browserbase: direct IP routing is preferred (each session is 10.42.x.y, host routes directly).
/// Port mapping is a fallback for exposing container ports on the host.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct PortMapping {
    pub id: i64,
    pub container_id: String,
    pub container_ip: Ipv4Addr,
    pub container_port: u16,
    pub host_port: u16,
    pub protocol: String, // "tcp" or "udp"
}

#[allow(dead_code)]
pub struct PortMapper {
    pool: SqlitePool,
    nft_available: bool,
}

#[allow(dead_code)]
impl PortMapper {
    pub fn new(pool: SqlitePool) -> Self {
        let nft_available = std::process::Command::new("nft")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        Self {
            pool,
            nft_available,
        }
    }

    /// Map a container port to a host port. Allocates host_port from DB if not specified.
    pub async fn map_port(
        &self,
        container_id: &str,
        container_ip: Ipv4Addr,
        container_port: u16,
        host_port: Option<u16>,
        protocol: &str,
    ) -> NetworkResult<PortMapping> {
        let host_port = match host_port {
            Some(p) => p,
            None => self.allocate_host_port().await?,
        };

        // Insert into DB (unique constraint on host_port prevents collisions)
        let result = sqlx::query(
            r#"INSERT INTO port_mappings (container_id, container_ip, container_port, host_port, protocol)
               VALUES (?, ?, ?, ?, ?)"#,
        )
        .bind(container_id)
        .bind(container_ip.to_string())
        .bind(container_port as i64)
        .bind(host_port as i64)
        .bind(protocol)
        .execute(&self.pool)
        .await
        .map_err(|e| {
            NetworkError::Validation(format!("port mapping DB insert failed: {}", e))
        })?;

        let mapping = PortMapping {
            id: result.last_insert_rowid(),
            container_id: container_id.to_string(),
            container_ip,
            container_port,
            host_port,
            protocol: protocol.to_string(),
        };

        // Add DNAT rule
        self.add_dnat_rule(&mapping).await?;

        tracing::info!(
            "Port mapped: host:{} -> {}:{} ({}) for container {}",
            host_port,
            container_ip,
            container_port,
            protocol,
            container_id
        );

        Ok(mapping)
    }

    /// Remove a port mapping
    pub async fn unmap_port(&self, mapping_id: i64) -> NetworkResult<()> {
        // Get mapping details before deletion
        let row: Option<(String, i64, i64, String)> = sqlx::query_as(
            "SELECT container_ip, container_port, host_port, protocol FROM port_mappings WHERE id = ?",
        )
        .bind(mapping_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| NetworkError::Validation(format!("DB query failed: {}", e)))?;

        if let Some((ip_str, cport, hport, proto)) = row {
            let ip: Ipv4Addr = ip_str
                .parse()
                .map_err(|_| NetworkError::Validation("invalid IP in DB".to_string()))?;

            let mapping = PortMapping {
                id: mapping_id,
                container_id: String::new(),
                container_ip: ip,
                container_port: cport as u16,
                host_port: hport as u16,
                protocol: proto,
            };

            self.remove_dnat_rule(&mapping).await?;
        }

        sqlx::query("DELETE FROM port_mappings WHERE id = ?")
            .bind(mapping_id)
            .execute(&self.pool)
            .await
            .map_err(|e| NetworkError::Validation(format!("DB delete failed: {}", e)))?;

        Ok(())
    }

    /// Remove all port mappings for a container
    pub async fn unmap_all_for_container(&self, container_id: &str) -> NetworkResult<()> {
        let rows: Vec<(i64,)> =
            sqlx::query_as("SELECT id FROM port_mappings WHERE container_id = ?")
                .bind(container_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| NetworkError::Validation(format!("DB query failed: {}", e)))?;

        for (id,) in rows {
            let _ = self.unmap_port(id).await;
        }

        Ok(())
    }

    async fn allocate_host_port(&self) -> NetworkResult<u16> {
        // Find next available port starting from 30000
        let used: Vec<(i64,)> =
            sqlx::query_as("SELECT host_port FROM port_mappings ORDER BY host_port")
                .fetch_all(&self.pool)
                .await
                .map_err(|e| NetworkError::Validation(format!("DB query failed: {}", e)))?;

        let used_set: std::collections::HashSet<u16> =
            used.into_iter().map(|(p,)| p as u16).collect();

        for port in 30000..60000u16 {
            if !used_set.contains(&port) {
                return Ok(port);
            }
        }

        Err(NetworkError::Validation(
            "no available host ports in range 30000-60000".to_string(),
        ))
    }

    async fn add_dnat_rule(&self, mapping: &PortMapping) -> NetworkResult<()> {
        if self.nft_available {
            let cmd = format!(
                "nft add rule inet quilt prerouting {} dport {} dnat ip to {}:{}",
                mapping.protocol, mapping.host_port, mapping.container_ip, mapping.container_port
            );
            let output = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .output()
                .await
                .map_err(NetworkError::Io)?;

            if !output.status.success() {
                tracing::warn!(
                    "Failed to add nft DNAT rule: {}",
                    String::from_utf8_lossy(&output.stderr)
                );
            }
        } else {
            let cmd = format!(
                "iptables -t nat -A PREROUTING -p {} --dport {} -j DNAT --to-destination {}:{}",
                mapping.protocol, mapping.host_port, mapping.container_ip, mapping.container_port
            );
            let _ = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .output()
                .await;
        }
        Ok(())
    }

    async fn remove_dnat_rule(&self, mapping: &PortMapping) -> NetworkResult<()> {
        if self.nft_available {
            // nft rules are harder to remove by content; for now we flush on teardown
            // In production, use nft handle-based deletion
            tracing::debug!(
                "nft DNAT rule removal for port {} (handled by table flush)",
                mapping.host_port
            );
        } else {
            let cmd = format!(
                "iptables -t nat -D PREROUTING -p {} --dport {} -j DNAT --to-destination {}:{} 2>/dev/null || true",
                mapping.protocol, mapping.host_port, mapping.container_ip, mapping.container_port
            );
            let _ = tokio::process::Command::new("sh")
                .arg("-c")
                .arg(&cmd)
                .output()
                .await;
        }
        Ok(())
    }
}

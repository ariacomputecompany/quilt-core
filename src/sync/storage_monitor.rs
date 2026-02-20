use crate::sync::error::{SyncError, SyncResult};
/// Storage monitoring using du command for accurate disk usage tracking
///
/// This module provides accurate storage measurement for billing enforcement.
/// It measures actual disk usage of volumes and container rootfs using the `du` command,
/// replacing fixed 1GB estimates with real measurements.
///
/// Architecture:
/// - Async measurement using tokio::process::Command (non-blocking)
/// - Measurements on volume/container create/delete + hourly reconciliation
/// - Automatic drift detection and correction
/// - Historical measurements for audit trail
use sqlx::SqlitePool;
use std::path::Path;
use std::time::Duration;

/// Monitors and tracks storage usage for volumes and containers
#[derive(Clone)]
pub struct StorageMonitor {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl StorageMonitor {
    /// Create a new storage monitor
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    /// Measure directory size using du command (async, non-blocking)
    ///
    /// Returns size in bytes. Uses 30-second timeout to prevent hanging on large directories.
    /// Returns 0 if directory doesn't exist or measurement fails.
    pub async fn measure_directory_size(path: &Path) -> SyncResult<u64> {
        // Check if path exists
        if !tokio::fs::try_exists(path).await.unwrap_or(false) {
            tracing::warn!("Directory does not exist: {:?}", path);
            return Ok(0);
        }

        tracing::debug!("Measuring directory size: {:?}", path);

        // Run du command with 30-second timeout
        let path_str = path.to_string_lossy().to_string();
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            tokio::process::Command::new("du")
                .arg("-sb") // -s (summarize), -b (bytes)
                .arg(&path_str)
                .output(),
        )
        .await;

        match result {
            Ok(Ok(output)) if output.status.success() => {
                // Parse du output: "12345678    /path/to/dir"
                let stdout = String::from_utf8_lossy(&output.stdout);
                let size_bytes = stdout
                    .split_whitespace()
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| SyncError::ValidationFailed {
                        message: format!("Failed to parse du output: {}", stdout),
                    })?;

                tracing::debug!(
                    "Directory {:?} size: {} bytes ({:.2} GB)",
                    path,
                    size_bytes,
                    size_bytes as f64 / 1_073_741_824.0
                );
                Ok(size_bytes)
            }
            Ok(Ok(output)) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                tracing::warn!("du command failed for {:?}: {}", path, stderr);
                Ok(0)
            }
            Ok(Err(e)) => {
                tracing::warn!("Failed to execute du for {:?}: {}", path, e);
                Ok(0)
            }
            Err(_) => {
                tracing::warn!("du command timed out after 30s for {:?}", path);
                Ok(0)
            }
        }
    }

    /// Measure and update a single volume
    ///
    /// Returns the measured size in GB.
    pub async fn measure_volume(&self, volume_name: &str, tenant_id: &str) -> SyncResult<f64> {
        // Get volume path from database
        let volume_path: Option<(String,)> =
            sqlx::query_as("SELECT mount_point FROM volumes WHERE name = ? AND tenant_id = ?")
                .bind(volume_name)
                .bind(tenant_id)
                .fetch_optional(&self.read_pool)
                .await?;

        let mount_point = volume_path
            .ok_or_else(|| SyncError::ValidationFailed {
                message: format!(
                    "Volume '{}' not found for tenant {}",
                    volume_name, tenant_id
                ),
            })?
            .0;

        // Measure directory
        let size_bytes = Self::measure_directory_size(Path::new(&mount_point)).await?;
        let size_gb = size_bytes as f64 / 1_073_741_824.0;

        // Update database
        let now = chrono::Utc::now().timestamp();
        sqlx::query(
            "UPDATE volumes SET actual_size_gb = ?, last_measured_at = ?
             WHERE name = ? AND tenant_id = ?",
        )
        .bind(size_gb)
        .bind(now)
        .bind(volume_name)
        .bind(tenant_id)
        .execute(&self.write_pool)
        .await?;

        tracing::info!(
            "📦 Measured volume '{}' (tenant {}): {:.3} GB",
            volume_name,
            tenant_id,
            size_gb
        );

        Ok(size_gb)
    }

    /// Measure and update a single container rootfs
    ///
    /// Returns the measured size in GB.
    pub async fn measure_container_rootfs(
        &self,
        container_id: &str,
        tenant_id: &str,
    ) -> SyncResult<f64> {
        // Container rootfs is at /var/lib/quilt/containers/<container_id>
        let rootfs_path = format!(
            "{}/{}",
            crate::utils::constants::CONTAINER_BASE_DIR,
            container_id
        );

        // Measure directory
        let size_bytes = Self::measure_directory_size(Path::new(&rootfs_path)).await?;
        let size_gb = size_bytes as f64 / 1_073_741_824.0;

        // Update database
        let now = chrono::Utc::now().timestamp();
        sqlx::query(
            "UPDATE containers SET rootfs_size_gb = ?, rootfs_measured_at = ?
             WHERE id = ? AND tenant_id = ?",
        )
        .bind(size_gb)
        .bind(now)
        .bind(container_id)
        .bind(tenant_id)
        .execute(&self.write_pool)
        .await?;

        tracing::info!(
            "📦 Measured container rootfs '{}' (tenant {}): {:.3} GB",
            container_id,
            tenant_id,
            size_gb
        );

        Ok(size_gb)
    }

    /// Reconcile all tenants (called by background task)
    ///
    /// Measures all volumes and containers, updates tenant quotas, and detects drift.
    pub async fn reconcile_all_tenants(
        read_pool: &SqlitePool,
        write_pool: &SqlitePool,
    ) -> SyncResult<()> {
        let monitor = Self::new(read_pool.clone(), write_pool.clone());

        tracing::info!("🔍 Starting storage reconciliation for all tenants");

        // Get all active volumes (limit to recently active to avoid measuring old data)
        let volumes: Vec<(String, String, String)> = sqlx::query_as(
            r#"
            SELECT name, tenant_id, mount_point
            FROM volumes
            WHERE status = 'active'
              AND (last_measured_at IS NULL OR last_measured_at < ?)
            ORDER BY tenant_id, name
            LIMIT 1000
            "#,
        )
        .bind(chrono::Utc::now().timestamp() - 3600) // measure if not measured in last hour
        .fetch_all(read_pool)
        .await?;

        let mut measured_volumes = 0;
        for (name, tenant_id, mount_point) in volumes {
            match Self::measure_directory_size(Path::new(&mount_point)).await {
                Ok(size_bytes) => {
                    let size_gb = size_bytes as f64 / 1_073_741_824.0;
                    let now = chrono::Utc::now().timestamp();

                    let _ = sqlx::query(
                        "UPDATE volumes SET actual_size_gb = ?, last_measured_at = ?
                         WHERE name = ? AND tenant_id = ?",
                    )
                    .bind(size_gb)
                    .bind(now)
                    .bind(&name)
                    .bind(&tenant_id)
                    .execute(write_pool)
                    .await;

                    measured_volumes += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to measure volume '{}' (tenant {}): {}",
                        name,
                        tenant_id,
                        e
                    );
                }
            }

            // Small delay to avoid overwhelming the system
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Get all running/created containers
        let containers: Vec<(String, String)> = sqlx::query_as(
            r#"
            SELECT id, tenant_id
            FROM containers
            WHERE state IN ('created', 'running', 'paused')
              AND (rootfs_measured_at IS NULL OR rootfs_measured_at < ?)
            ORDER BY tenant_id, id
            LIMIT 1000
            "#,
        )
        .bind(chrono::Utc::now().timestamp() - 3600) // measure if not measured in last hour
        .fetch_all(read_pool)
        .await?;

        let mut measured_containers = 0;
        for (container_id, tenant_id) in containers {
            let rootfs_path = format!(
                "{}/{}",
                crate::utils::constants::CONTAINER_BASE_DIR,
                container_id
            );

            match Self::measure_directory_size(Path::new(&rootfs_path)).await {
                Ok(size_bytes) => {
                    let size_gb = size_bytes as f64 / 1_073_741_824.0;
                    let now = chrono::Utc::now().timestamp();

                    let _ = sqlx::query(
                        "UPDATE containers SET rootfs_size_gb = ?, rootfs_measured_at = ?
                         WHERE id = ? AND tenant_id = ?",
                    )
                    .bind(size_gb)
                    .bind(now)
                    .bind(&container_id)
                    .bind(&tenant_id)
                    .execute(write_pool)
                    .await;

                    measured_containers += 1;
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to measure container {} (tenant {}): {}",
                        container_id,
                        tenant_id,
                        e
                    );
                }
            }

            // Small delay
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        tracing::info!(
            "✅ Storage reconciliation complete: measured {} volumes, {} containers",
            measured_volumes,
            measured_containers
        );

        // Update tenant quotas from measured values
        monitor.reconcile_tenant_quotas().await?;

        Ok(())
    }

    /// Update tenant quotas from measured storage
    ///
    /// Compares measured storage vs quota database and fixes drift >10%.
    async fn reconcile_tenant_quotas(&self) -> SyncResult<()> {
        tracing::info!("🔄 Reconciling tenant storage quotas");

        // Get all tenants
        let tenants: Vec<(String,)> =
            sqlx::query_as("SELECT DISTINCT tenant_id FROM tenant_quotas ORDER BY tenant_id")
                .fetch_all(&self.read_pool)
                .await?;

        for (tenant_id,) in tenants {
            // Calculate actual storage from measured values
            let actual_storage: (f64,) = sqlx::query_as(
                r#"
                SELECT
                    COALESCE(SUM(v.actual_size_gb), 0.0) +
                    COALESCE((
                        SELECT SUM(c.rootfs_size_gb)
                        FROM containers c
                        WHERE c.tenant_id = ?
                          AND c.state IN ('created', 'running', 'paused')
                    ), 0.0) as total_storage_gb
                FROM volumes v
                WHERE v.tenant_id = ? AND v.status = 'active'
                "#,
            )
            .bind(&tenant_id)
            .bind(&tenant_id)
            .fetch_one(&self.read_pool)
            .await?;

            let measured_gb = actual_storage.0;

            // Get current quota value
            let current_quota: (f64,) =
                sqlx::query_as("SELECT storage_used_gb FROM tenant_quotas WHERE tenant_id = ?")
                    .bind(&tenant_id)
                    .fetch_one(&self.read_pool)
                    .await?;

            let quota_gb = current_quota.0;

            // Calculate drift percentage
            let drift_pct = if quota_gb > 0.01 {
                ((measured_gb - quota_gb).abs() / quota_gb) * 100.0
            } else if measured_gb > 0.01 {
                100.0
            } else {
                0.0
            };

            // Log drift if significant
            if drift_pct > 5.0 {
                tracing::warn!(
                    "Storage drift detected for tenant {}: quota={:.3} GB, measured={:.3} GB ({:.1}% drift)",
                    tenant_id,
                    quota_gb,
                    measured_gb,
                    drift_pct
                );
            }

            // Auto-correct if drift >10%
            if drift_pct > 10.0 {
                let now = chrono::Utc::now().timestamp();
                sqlx::query(
                    "UPDATE tenant_quotas SET storage_used_gb = ?, updated_at = ?
                     WHERE tenant_id = ?",
                )
                .bind(measured_gb)
                .bind(now)
                .bind(&tenant_id)
                .execute(&self.write_pool)
                .await?;

                tracing::info!(
                    "✅ Auto-corrected storage quota for tenant {}: {:.3} GB → {:.3} GB",
                    tenant_id,
                    quota_gb,
                    measured_gb
                );
            }

            // Store historical measurement
            let now = chrono::Utc::now().timestamp();
            let volumes_only: (f64,) = sqlx::query_as(
                "SELECT COALESCE(SUM(actual_size_gb), 0.0) FROM volumes
                 WHERE tenant_id = ? AND status = 'active'",
            )
            .bind(&tenant_id)
            .fetch_one(&self.read_pool)
            .await?;

            let containers_only = measured_gb - volumes_only.0;

            let _ = sqlx::query(
                r#"
                INSERT INTO storage_measurements
                (tenant_id, measured_at, volumes_gb, containers_gb, total_gb, measurement_type)
                VALUES (?, ?, ?, ?, ?, 'reconciliation')
                "#,
            )
            .bind(&tenant_id)
            .bind(now)
            .bind(volumes_only.0)
            .bind(containers_only)
            .bind(measured_gb)
            .execute(&self.write_pool)
            .await;
        }

        tracing::info!("✅ Tenant storage quotas reconciled");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_measure_nonexistent_directory() {
        let result =
            StorageMonitor::measure_directory_size(Path::new("/nonexistent/path/12345")).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }
}

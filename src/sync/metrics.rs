use crate::daemon::metrics::{
    ContainerMetrics, CpuMetrics, DiskMetrics, MemoryMetrics, NetworkMetrics,
};
use crate::sync::error::{SyncError, SyncResult};
use crate::utils::console::ConsoleLogger;
use sqlx::SqlitePool;

pub struct MetricsStore {
    pool: SqlitePool,
}

impl MetricsStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Store container metrics in the database
    pub async fn store_metrics(&self, metrics: &ContainerMetrics) -> SyncResult<()> {
        let result = sqlx::query(r#"
            INSERT INTO container_metrics (
                container_id, timestamp,
                cpu_usage_usec, cpu_user_usec, cpu_system_usec, cpu_throttled_usec,
                memory_current_bytes, memory_peak_bytes, memory_limit_bytes, 
                memory_cache_bytes, memory_rss_bytes,
                network_rx_bytes, network_tx_bytes, network_rx_packets, 
                network_tx_packets, network_rx_errors, network_tx_errors,
                disk_read_bytes, disk_write_bytes, disk_read_ops, disk_write_ops
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21
            )
        "#)
        .bind(&metrics.container_id)
        .bind(metrics.timestamp as i64)
        // CPU metrics
        .bind(metrics.cpu.usage_usec as i64)
        .bind(metrics.cpu.user_usec as i64)
        .bind(metrics.cpu.system_usec as i64)
        .bind(metrics.cpu.throttled_usec as i64)
        // Memory metrics
        .bind(metrics.memory.current_bytes as i64)
        .bind(metrics.memory.peak_bytes as i64)
        .bind(metrics.memory.limit_bytes as i64)
        .bind(metrics.memory.cache_bytes as i64)
        .bind(metrics.memory.rss_bytes as i64)
        // Network metrics
        .bind(metrics.network.rx_bytes as i64)
        .bind(metrics.network.tx_bytes as i64)
        .bind(metrics.network.rx_packets as i64)
        .bind(metrics.network.tx_packets as i64)
        .bind(metrics.network.rx_errors as i64)
        .bind(metrics.network.tx_errors as i64)
        // Disk metrics
        .bind(metrics.disk.read_bytes as i64)
        .bind(metrics.disk.write_bytes as i64)
        .bind(metrics.disk.read_ops as i64)
        .bind(metrics.disk.write_ops as i64)
        .execute(&self.pool)
        .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => {
                ConsoleLogger::warning(&format!(
                    "Failed to store metrics for container {}: {}",
                    metrics.container_id, e
                ));
                Err(SyncError::Database(e))
            }
        }
    }

    /// Get latest metrics for a container
    pub async fn get_latest_metrics(
        &self,
        container_id: &str,
    ) -> SyncResult<Option<ContainerMetrics>> {
        let row = sqlx::query_as::<_, MetricsRow>(
            r#"
            SELECT * FROM container_metrics 
            WHERE container_id = ?1 
            ORDER BY timestamp DESC 
            LIMIT 1
        "#,
        )
        .bind(container_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|r| r.into()))
    }

    /// Get metrics history for a container with time range
    pub async fn get_metrics_history(
        &self,
        container_id: &str,
        start_time: u64,
        end_time: u64,
        limit: Option<u32>,
    ) -> SyncResult<Vec<ContainerMetrics>> {
        let limit = limit.unwrap_or(1000).min(10000); // Cap at 10k records

        let rows = sqlx::query_as::<_, MetricsRow>(
            r#"
            SELECT * FROM container_metrics 
            WHERE container_id = ?1 AND timestamp >= ?2 AND timestamp <= ?3
            ORDER BY timestamp DESC 
            LIMIT ?4
        "#,
        )
        .bind(container_id)
        .bind(start_time as i64)
        .bind(end_time as i64)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows.into_iter().map(Into::into).collect())
    }

    /// Clean up old metrics (keep last N days)
    pub async fn cleanup_old_metrics(&self, retention_days: u32) -> SyncResult<u64> {
        let cutoff_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            - (retention_days as u64 * 24 * 60 * 60 * 1000);

        let result = sqlx::query(
            r#"
            DELETE FROM container_metrics 
            WHERE timestamp < ?1
        "#,
        )
        .bind(cutoff_time as i64)
        .execute(&self.pool)
        .await?;

        let deleted = result.rows_affected();
        if deleted > 0 {
            ConsoleLogger::info(&format!("Cleaned up {} old metric records", deleted));
        }

        Ok(deleted)
    }

    // Complex aggregated metrics method removed - over-engineered for current needs
}

// Database row types
#[derive(sqlx::FromRow)]
struct MetricsRow {
    container_id: String,
    timestamp: i64,
    cpu_usage_usec: Option<i64>,
    cpu_user_usec: Option<i64>,
    cpu_system_usec: Option<i64>,
    cpu_throttled_usec: Option<i64>,
    memory_current_bytes: Option<i64>,
    memory_peak_bytes: Option<i64>,
    memory_limit_bytes: Option<i64>,
    memory_cache_bytes: Option<i64>,
    memory_rss_bytes: Option<i64>,
    network_rx_bytes: Option<i64>,
    network_tx_bytes: Option<i64>,
    network_rx_packets: Option<i64>,
    network_tx_packets: Option<i64>,
    network_rx_errors: Option<i64>,
    network_tx_errors: Option<i64>,
    disk_read_bytes: Option<i64>,
    disk_write_bytes: Option<i64>,
    disk_read_ops: Option<i64>,
    disk_write_ops: Option<i64>,
}

impl From<MetricsRow> for ContainerMetrics {
    fn from(row: MetricsRow) -> Self {
        ContainerMetrics {
            container_id: row.container_id,
            timestamp: row.timestamp as u64,
            cpu: CpuMetrics {
                usage_usec: row.cpu_usage_usec.unwrap_or(0) as u64,
                user_usec: row.cpu_user_usec.unwrap_or(0) as u64,
                system_usec: row.cpu_system_usec.unwrap_or(0) as u64,
                throttled_usec: row.cpu_throttled_usec.unwrap_or(0) as u64,
                nr_periods: 0,
                nr_throttled: 0,
            },
            memory: MemoryMetrics {
                current_bytes: row.memory_current_bytes.unwrap_or(0) as u64,
                peak_bytes: row.memory_peak_bytes.unwrap_or(0) as u64,
                limit_bytes: row.memory_limit_bytes.unwrap_or(0) as u64,
                cache_bytes: row.memory_cache_bytes.unwrap_or(0) as u64,
                rss_bytes: row.memory_rss_bytes.unwrap_or(0) as u64,
            },
            network: NetworkMetrics {
                rx_bytes: row.network_rx_bytes.unwrap_or(0) as u64,
                tx_bytes: row.network_tx_bytes.unwrap_or(0) as u64,
                rx_packets: row.network_rx_packets.unwrap_or(0) as u64,
                tx_packets: row.network_tx_packets.unwrap_or(0) as u64,
                rx_errors: row.network_rx_errors.unwrap_or(0) as u64,
                tx_errors: row.network_tx_errors.unwrap_or(0) as u64,
            },
            disk: DiskMetrics {
                read_bytes: row.disk_read_bytes.unwrap_or(0) as u64,
                write_bytes: row.disk_write_bytes.unwrap_or(0) as u64,
                read_ops: row.disk_read_ops.unwrap_or(0) as u64,
                write_ops: row.disk_write_ops.unwrap_or(0) as u64,
            },
        }
    }
}

// Over-engineered aggregated metrics structs removed - not needed for core functionality

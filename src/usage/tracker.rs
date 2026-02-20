/// Usage tracking implementation for storing and retrieving usage events.
use super::events::{UsageEvent, UsageEventType};
use crate::sync::error::SyncResult;
use sqlx::{Row, SqlitePool};

pub struct UsageTracker {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl UsageTracker {
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    /// Emit a usage event (store in database for billing)
    pub async fn emit(&self, event: UsageEvent) -> SyncResult<()> {
        let metadata_json =
            serde_json::to_string(&event.metadata).unwrap_or_else(|_| "{}".to_string());

        sqlx::query(
            r#"
            INSERT INTO usage_events (
                tenant_id, event_type, container_id, timestamp,
                duration_seconds, memory_mb, cpu_percent, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        )
        .bind(&event.tenant_id)
        .bind(event.event_type.to_string())
        .bind(&event.container_id)
        .bind(event.timestamp)
        .bind(event.duration_seconds)
        .bind(event.memory_mb)
        .bind(event.cpu_percent)
        .bind(metadata_json)
        .execute(&self.write_pool)
        .await?;

        tracing::debug!(
            "Usage event emitted: {:?} for tenant {} container {:?}",
            event.event_type.to_string(),
            event.tenant_id,
            event.container_id
        );

        Ok(())
    }

    /// Get all usage events for a tenant in a time range
    #[allow(dead_code)]
    pub async fn get_events_for_tenant(
        &self,
        tenant_id: &str,
        start_time: i64,
        end_time: i64,
    ) -> SyncResult<Vec<UsageEvent>> {
        let rows = sqlx::query(
            r#"
            SELECT tenant_id, event_type, container_id, timestamp,
                   duration_seconds, memory_mb, cpu_percent, metadata
            FROM usage_events
            WHERE tenant_id = ? AND timestamp >= ? AND timestamp <= ?
            ORDER BY timestamp DESC
        "#,
        )
        .bind(tenant_id)
        .bind(start_time)
        .bind(end_time)
        .fetch_all(&self.read_pool)
        .await?;

        let mut events = Vec::new();
        for row in rows {
            let event_type_str: String = row.get("event_type");
            let event_type = match event_type_str.as_str() {
                "container_created" => UsageEventType::ContainerCreated,
                "container_started" => UsageEventType::ContainerStarted,
                "container_stopped" => UsageEventType::ContainerStopped,
                "container_runtime" => UsageEventType::ContainerRuntime,
                "volume_created" => UsageEventType::VolumeCreated,
                "volume_deleted" => UsageEventType::VolumeDeleted,
                "network_allocated" => UsageEventType::NetworkAllocated,
                _ => continue,
            };

            let metadata_json: String = row.get("metadata");
            let metadata = serde_json::from_str(&metadata_json).unwrap_or_default();

            events.push(UsageEvent {
                tenant_id: row.get("tenant_id"),
                event_type,
                container_id: row.get("container_id"),
                timestamp: row.get("timestamp"),
                duration_seconds: row.get("duration_seconds"),
                memory_mb: row.get("memory_mb"),
                cpu_percent: row.get("cpu_percent"),
                metadata,
            });
        }

        Ok(events)
    }

    /// Get container runtime summary for billing
    #[allow(dead_code)]
    pub async fn get_container_runtime_summary(
        &self,
        tenant_id: &str,
        start_time: i64,
        end_time: i64,
    ) -> SyncResult<ContainerRuntimeSummary> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_events,
                SUM(duration_seconds) as total_runtime_seconds,
                AVG(memory_mb) as avg_memory_mb,
                AVG(cpu_percent) as avg_cpu_percent,
                MAX(memory_mb) as peak_memory_mb
            FROM usage_events
            WHERE tenant_id = ?
              AND event_type = 'container_runtime'
              AND timestamp >= ?
              AND timestamp <= ?
        "#,
        )
        .bind(tenant_id)
        .bind(start_time)
        .bind(end_time)
        .fetch_one(&self.read_pool)
        .await?;

        Ok(ContainerRuntimeSummary {
            total_events: row.get::<i64, _>("total_events") as u64,
            total_runtime_seconds: row
                .get::<Option<i64>, _>("total_runtime_seconds")
                .unwrap_or(0) as u64,
            avg_memory_mb: row.get::<Option<f64>, _>("avg_memory_mb").unwrap_or(0.0) as f32,
            avg_cpu_percent: row.get::<Option<f64>, _>("avg_cpu_percent").unwrap_or(0.0) as f32,
            peak_memory_mb: row.get::<Option<i32>, _>("peak_memory_mb").unwrap_or(0),
        })
    }

    /// Get container lifecycle event counts
    #[allow(dead_code)]
    pub async fn get_lifecycle_counts(
        &self,
        tenant_id: &str,
        start_time: i64,
        end_time: i64,
    ) -> SyncResult<LifecycleCounts> {
        let row = sqlx::query(r#"
            SELECT
                SUM(CASE WHEN event_type = 'container_created' THEN 1 ELSE 0 END) as created_count,
                SUM(CASE WHEN event_type = 'container_started' THEN 1 ELSE 0 END) as started_count,
                SUM(CASE WHEN event_type = 'container_stopped' THEN 1 ELSE 0 END) as stopped_count,
                SUM(CASE WHEN event_type = 'volume_created' THEN 1 ELSE 0 END) as volumes_created,
                SUM(CASE WHEN event_type = 'network_allocated' THEN 1 ELSE 0 END) as networks_allocated
            FROM usage_events
            WHERE tenant_id = ?
              AND timestamp >= ?
              AND timestamp <= ?
        "#)
        .bind(tenant_id)
        .bind(start_time)
        .bind(end_time)
        .fetch_one(&self.read_pool)
        .await?;

        Ok(LifecycleCounts {
            containers_created: row.get::<Option<i64>, _>("created_count").unwrap_or(0) as u64,
            containers_started: row.get::<Option<i64>, _>("started_count").unwrap_or(0) as u64,
            containers_stopped: row.get::<Option<i64>, _>("stopped_count").unwrap_or(0) as u64,
            volumes_created: row.get::<Option<i64>, _>("volumes_created").unwrap_or(0) as u64,
            networks_allocated: row.get::<Option<i64>, _>("networks_allocated").unwrap_or(0) as u64,
        })
    }
}

/// Summary of container runtime for billing
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ContainerRuntimeSummary {
    pub total_events: u64,
    pub total_runtime_seconds: u64,
    pub avg_memory_mb: f32,
    pub avg_cpu_percent: f32,
    pub peak_memory_mb: i32,
}

/// Lifecycle event counts
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LifecycleCounts {
    pub containers_created: u64,
    pub containers_started: u64,
    pub containers_stopped: u64,
    pub volumes_created: u64,
    pub networks_allocated: u64,
}

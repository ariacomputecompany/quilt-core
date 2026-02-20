// src/sync/icc_connections.rs
// ICC Connection tracking and management via SQLite

use crate::sync::error::{SyncError, SyncResult};
use sqlx::Row;
use sqlx::SqlitePool;

/// Represents an ICC connection record from the database
#[derive(Debug, Clone)]
pub struct IccConnection {
    pub connection_id: String,
    pub from_container_id: String,
    pub to_container_id: String,
    pub protocol: String,
    pub port: i64,
    pub status: String,
    pub persistent: bool,
    pub auto_reconnect: bool,
    pub created_at: i64,
    pub last_health_check: Option<i64>,
    pub last_latency_us: Option<i64>,
    pub failure_count: i64,
    #[allow(dead_code)]
    pub metadata: String,
}

/// Manages ICC connections in the database
pub struct IccConnectionManager {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl IccConnectionManager {
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    /// Create a new ICC connection
    #[allow(clippy::too_many_arguments)]
    pub async fn create_connection(
        &self,
        connection_id: &str,
        from_container_id: &str,
        to_container_id: &str,
        protocol: &str,
        port: i64,
        persistent: bool,
        auto_reconnect: bool,
    ) -> SyncResult<IccConnection> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;

        sqlx::query(
            r#"
            INSERT INTO icc_connections (
                connection_id, from_container_id, to_container_id,
                protocol, port, status, persistent, auto_reconnect,
                created_at, failure_count, metadata
            ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?, 0, '{}')
        "#,
        )
        .bind(connection_id)
        .bind(from_container_id)
        .bind(to_container_id)
        .bind(protocol)
        .bind(port)
        .bind(persistent)
        .bind(auto_reconnect)
        .bind(now)
        .execute(&self.write_pool)
        .await?;

        Ok(IccConnection {
            connection_id: connection_id.to_string(),
            from_container_id: from_container_id.to_string(),
            to_container_id: to_container_id.to_string(),
            protocol: protocol.to_string(),
            port,
            status: "active".to_string(),
            persistent,
            auto_reconnect,
            created_at: now,
            last_health_check: None,
            last_latency_us: None,
            failure_count: 0,
            metadata: "{}".to_string(),
        })
    }

    /// Get a connection by ID
    pub async fn get_connection(&self, connection_id: &str) -> SyncResult<IccConnection> {
        let row = sqlx::query("SELECT * FROM icc_connections WHERE connection_id = ?")
            .bind(connection_id)
            .fetch_optional(&self.read_pool)
            .await?
            .ok_or_else(|| SyncError::NotFound {
                container_id: connection_id.to_string(),
            })?;

        Ok(row_to_connection(&row))
    }

    /// List connections with optional filters
    pub async fn list_connections(
        &self,
        container_id: Option<&str>,
        status_filter: Option<&str>,
    ) -> SyncResult<Vec<IccConnection>> {
        let mut query = String::from("SELECT * FROM icc_connections WHERE 1=1");
        let mut binds: Vec<String> = Vec::new();

        if let Some(cid) = container_id {
            query.push_str(" AND (from_container_id = ? OR to_container_id = ?)");
            binds.push(cid.to_string());
            binds.push(cid.to_string());
        }
        if let Some(status) = status_filter {
            query.push_str(" AND status = ?");
            binds.push(status.to_string());
        }
        query.push_str(" ORDER BY created_at DESC");

        // Build query dynamically
        let mut q = sqlx::query(&query);
        for b in &binds {
            q = q.bind(b);
        }

        let rows = q.fetch_all(&self.read_pool).await?;
        Ok(rows.iter().map(row_to_connection).collect())
    }

    /// Update connection status
    pub async fn update_status(&self, connection_id: &str, status: &str) -> SyncResult<()> {
        let result = sqlx::query("UPDATE icc_connections SET status = ? WHERE connection_id = ?")
            .bind(status)
            .bind(connection_id)
            .execute(&self.write_pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: connection_id.to_string(),
            });
        }
        Ok(())
    }

    /// Remove a connection
    pub async fn remove_connection(&self, connection_id: &str) -> SyncResult<()> {
        let result = sqlx::query("DELETE FROM icc_connections WHERE connection_id = ?")
            .bind(connection_id)
            .execute(&self.write_pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: connection_id.to_string(),
            });
        }
        Ok(())
    }

    /// Remove all connections for a container (used on container deletion)
    #[allow(dead_code)]
    pub async fn remove_connections_for_container(&self, container_id: &str) -> SyncResult<u64> {
        let result = sqlx::query(
            "DELETE FROM icc_connections WHERE from_container_id = ? OR to_container_id = ?",
        )
        .bind(container_id)
        .bind(container_id)
        .execute(&self.write_pool)
        .await?;

        Ok(result.rows_affected())
    }

    /// Update health check results for a connection
    pub async fn update_health(
        &self,
        connection_id: &str,
        latency_us: Option<i64>,
        new_status: &str,
    ) -> SyncResult<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs() as i64;

        if new_status == "failed" {
            sqlx::query(
                r#"
                UPDATE icc_connections
                SET last_health_check = ?, last_latency_us = ?, status = ?,
                    failure_count = failure_count + 1
                WHERE connection_id = ?
            "#,
            )
            .bind(now)
            .bind(latency_us)
            .bind(new_status)
            .bind(connection_id)
            .execute(&self.write_pool)
            .await?;
        } else {
            sqlx::query(
                r#"
                UPDATE icc_connections
                SET last_health_check = ?, last_latency_us = ?, status = ?,
                    failure_count = 0
                WHERE connection_id = ?
            "#,
            )
            .bind(now)
            .bind(latency_us)
            .bind(new_status)
            .bind(connection_id)
            .execute(&self.write_pool)
            .await?;
        }

        Ok(())
    }

    /// Get all active connections (for health monitor)
    pub async fn get_active_connections(&self) -> SyncResult<Vec<IccConnection>> {
        let rows = sqlx::query(
            "SELECT * FROM icc_connections WHERE status = 'active' ORDER BY created_at",
        )
        .fetch_all(&self.read_pool)
        .await?;

        Ok(rows.iter().map(row_to_connection).collect())
    }

    /// Mark failed connections that exceed threshold
    pub async fn mark_failed_connections(&self, max_failures: i64) -> SyncResult<u64> {
        let result = sqlx::query(
            "UPDATE icc_connections SET status = 'failed' WHERE status = 'active' AND auto_reconnect = 0 AND failure_count >= ?"
        )
        .bind(max_failures)
        .execute(&self.write_pool)
        .await?;

        Ok(result.rows_affected())
    }
}

fn row_to_connection(row: &sqlx::sqlite::SqliteRow) -> IccConnection {
    IccConnection {
        connection_id: row.get("connection_id"),
        from_container_id: row.get("from_container_id"),
        to_container_id: row.get("to_container_id"),
        protocol: row.get("protocol"),
        port: row.get("port"),
        status: row.get("status"),
        persistent: row.get("persistent"),
        auto_reconnect: row.get("auto_reconnect"),
        created_at: row.get("created_at"),
        last_health_check: row.get("last_health_check"),
        last_latency_us: row.get("last_latency_us"),
        failure_count: row.get("failure_count"),
        metadata: row.get("metadata"),
    }
}

use crate::sync::error::{SyncError, SyncResult};
/// Master Container Manager
///
/// Manages persistent master containers that serve as the execution environment
/// for each tenant. Each tenant gets exactly one master container that persists
/// across sessions and provides terminal access.
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::time::{SystemTime, UNIX_EPOCH};

/// Master container states
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MasterContainerState {
    Created,
    Starting,
    Running,
    Stopping,
    Stopped,
    Error,
}

impl std::fmt::Display for MasterContainerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MasterContainerState::Created => write!(f, "created"),
            MasterContainerState::Starting => write!(f, "starting"),
            MasterContainerState::Running => write!(f, "running"),
            MasterContainerState::Stopping => write!(f, "stopping"),
            MasterContainerState::Stopped => write!(f, "stopped"),
            MasterContainerState::Error => write!(f, "error"),
        }
    }
}

impl MasterContainerState {
    pub fn from_string(s: &str) -> SyncResult<Self> {
        match s {
            "created" => Ok(MasterContainerState::Created),
            "starting" => Ok(MasterContainerState::Starting),
            "running" => Ok(MasterContainerState::Running),
            "stopping" => Ok(MasterContainerState::Stopping),
            "stopped" => Ok(MasterContainerState::Stopped),
            "error" => Ok(MasterContainerState::Error),
            _ => Err(SyncError::ValidationFailed {
                message: format!("Invalid master container state: {}", s),
            }),
        }
    }

    #[allow(dead_code)] // API method - used for state validation
    pub fn can_transition_to(&self, new_state: &MasterContainerState) -> bool {
        match (self, new_state) {
            // Same state (idempotent)
            (state1, state2) if state1 == state2 => true,
            // Valid transitions
            (MasterContainerState::Created, MasterContainerState::Starting) => true,
            (MasterContainerState::Starting, MasterContainerState::Running) => true,
            (MasterContainerState::Starting, MasterContainerState::Error) => true,
            (MasterContainerState::Running, MasterContainerState::Stopping) => true,
            (MasterContainerState::Running, MasterContainerState::Error) => true,
            (MasterContainerState::Stopping, MasterContainerState::Stopped) => true,
            (MasterContainerState::Stopping, MasterContainerState::Error) => true,
            (MasterContainerState::Stopped, MasterContainerState::Starting) => true,
            (MasterContainerState::Error, MasterContainerState::Starting) => true,
            (_, MasterContainerState::Error) => true, // Can always go to error
            _ => false,
        }
    }
}

/// Master container entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterContainer {
    pub id: String,
    pub tenant_id: String,
    pub container_id: String,
    pub state: MasterContainerState,
    pub hostname: String,
    pub ip_address: Option<String>,
    pub cpu_limit: i32,
    pub memory_limit_mb: i32,
    pub disk_limit_gb: i32,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub stopped_at: Option<i64>,
    pub updated_at: i64,
}

/// Statistics for a master container
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasterContainerStats {
    pub cpu_usage_percent: f64,
    pub memory_usage_mb: i64,
    pub disk_usage_gb: f64,
    pub child_containers: i32,
    pub running_containers: i32,
}

/// Manager for master container operations
pub struct MasterContainerManager {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl MasterContainerManager {
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    /// Get the master container for a tenant, or None if it doesn't exist
    pub async fn get_master(&self, tenant_id: &str) -> SyncResult<Option<MasterContainer>> {
        let row = sqlx::query(
            r#"
            SELECT
                mc.id, mc.tenant_id, mc.container_id, mc.state, mc.hostname,
                mc.ip_address, mc.cpu_limit, mc.memory_limit_mb, mc.disk_limit_gb,
                mc.created_at, mc.started_at, mc.stopped_at, mc.updated_at
            FROM master_containers mc
            WHERE mc.tenant_id = ?
        "#,
        )
        .bind(tenant_id)
        .fetch_optional(&self.read_pool)
        .await?;

        match row {
            Some(row) => {
                let state_str: String = row.get("state");
                let state = MasterContainerState::from_string(&state_str)?;

                Ok(Some(MasterContainer {
                    id: row.get("id"),
                    tenant_id: row.get("tenant_id"),
                    container_id: row.get("container_id"),
                    state,
                    hostname: row.get("hostname"),
                    ip_address: row.get("ip_address"),
                    cpu_limit: row.get("cpu_limit"),
                    memory_limit_mb: row.get("memory_limit_mb"),
                    disk_limit_gb: row.get("disk_limit_gb"),
                    created_at: row.get("created_at"),
                    started_at: row.get("started_at"),
                    stopped_at: row.get("stopped_at"),
                    updated_at: row.get("updated_at"),
                }))
            }
            None => Ok(None),
        }
    }

    /// Get the master container by ID
    pub async fn get_master_by_id(&self, id: &str) -> SyncResult<Option<MasterContainer>> {
        let row = sqlx::query(
            r#"
            SELECT
                mc.id, mc.tenant_id, mc.container_id, mc.state, mc.hostname,
                mc.ip_address, mc.cpu_limit, mc.memory_limit_mb, mc.disk_limit_gb,
                mc.created_at, mc.started_at, mc.stopped_at, mc.updated_at
            FROM master_containers mc
            WHERE mc.id = ?
        "#,
        )
        .bind(id)
        .fetch_optional(&self.read_pool)
        .await?;

        match row {
            Some(row) => {
                let state_str: String = row.get("state");
                let state = MasterContainerState::from_string(&state_str)?;

                Ok(Some(MasterContainer {
                    id: row.get("id"),
                    tenant_id: row.get("tenant_id"),
                    container_id: row.get("container_id"),
                    state,
                    hostname: row.get("hostname"),
                    ip_address: row.get("ip_address"),
                    cpu_limit: row.get("cpu_limit"),
                    memory_limit_mb: row.get("memory_limit_mb"),
                    disk_limit_gb: row.get("disk_limit_gb"),
                    created_at: row.get("created_at"),
                    started_at: row.get("started_at"),
                    stopped_at: row.get("stopped_at"),
                    updated_at: row.get("updated_at"),
                }))
            }
            None => Ok(None),
        }
    }

    /// Create a new master container record (container_id links to containers table)
    #[allow(dead_code)] // API method for provisioning
    pub async fn create_master(
        &self,
        tenant_id: &str,
        container_id: &str,
        hostname: &str,
        cpu_limit: i32,
        memory_limit_mb: i32,
        disk_limit_gb: i32,
    ) -> SyncResult<MasterContainer> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let id = format!("mc_{}", uuid::Uuid::new_v4());

        sqlx::query(
            r#"
            INSERT INTO master_containers (
                id, tenant_id, container_id, state, hostname, ip_address,
                cpu_limit, memory_limit_mb, disk_limit_gb,
                created_at, started_at, stopped_at, updated_at
            ) VALUES (?, ?, ?, 'created', ?, NULL, ?, ?, ?, ?, NULL, NULL, ?)
        "#,
        )
        .bind(&id)
        .bind(tenant_id)
        .bind(container_id)
        .bind(hostname)
        .bind(cpu_limit)
        .bind(memory_limit_mb)
        .bind(disk_limit_gb)
        .bind(now)
        .bind(now)
        .execute(&self.write_pool)
        .await?;

        tracing::info!("Created master container {} for tenant {}", id, tenant_id);

        Ok(MasterContainer {
            id,
            tenant_id: tenant_id.to_string(),
            container_id: container_id.to_string(),
            state: MasterContainerState::Created,
            hostname: hostname.to_string(),
            ip_address: None,
            cpu_limit,
            memory_limit_mb,
            disk_limit_gb,
            created_at: now,
            started_at: None,
            stopped_at: None,
            updated_at: now,
        })
    }

    /// Update master container state
    pub async fn update_state(&self, id: &str, new_state: MasterContainerState) -> SyncResult<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        // Handle state-specific timestamp updates
        let result = match new_state {
            MasterContainerState::Running => {
                sqlx::query(
                    "UPDATE master_containers SET state = ?, started_at = ?, updated_at = ? WHERE id = ?"
                )
                .bind(new_state.to_string())
                .bind(now)
                .bind(now)
                .bind(id)
                .execute(&self.write_pool)
                .await?
            }
            MasterContainerState::Stopped => {
                sqlx::query(
                    "UPDATE master_containers SET state = ?, stopped_at = ?, updated_at = ? WHERE id = ?"
                )
                .bind(new_state.to_string())
                .bind(now)
                .bind(now)
                .bind(id)
                .execute(&self.write_pool)
                .await?
            }
            _ => {
                sqlx::query(
                    "UPDATE master_containers SET state = ?, updated_at = ? WHERE id = ?"
                )
                .bind(new_state.to_string())
                .bind(now)
                .bind(id)
                .execute(&self.write_pool)
                .await?
            }
        };

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: id.to_string(),
            });
        }

        tracing::debug!(
            "Updated master container {} state to {}",
            id,
            new_state.to_string()
        );
        Ok(())
    }

    /// Update master container state with validation
    /// Returns error if the state transition is invalid
    #[allow(dead_code)] // API method - available for validated state updates
    pub async fn update_state_validated(
        &self,
        id: &str,
        new_state: MasterContainerState,
    ) -> SyncResult<()> {
        // First, get current state
        let current = self
            .get_master_by_id(id)
            .await?
            .ok_or_else(|| SyncError::NotFound {
                container_id: id.to_string(),
            })?;

        // Validate transition
        if !current.state.can_transition_to(&new_state) {
            return Err(SyncError::InvalidStateTransition {
                from: current.state.to_string(),
                to: new_state.to_string(),
            });
        }

        // Proceed with update
        self.update_state(id, new_state).await
    }

    /// Update master container state, optionally forcing (skips validation)
    #[allow(dead_code)] // API method - available for forced state updates
    pub async fn update_state_with_force(
        &self,
        id: &str,
        new_state: MasterContainerState,
        force: bool,
    ) -> SyncResult<()> {
        if force {
            tracing::warn!(
                "Force updating master container {} state to {} (skipping validation)",
                id,
                new_state.to_string()
            );
            self.update_state(id, new_state).await
        } else {
            self.update_state_validated(id, new_state).await
        }
    }

    /// Update master container IP address (after network setup)
    #[allow(dead_code)] // API method for network setup
    pub async fn set_ip_address(&self, id: &str, ip_address: &str) -> SyncResult<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        let result =
            sqlx::query("UPDATE master_containers SET ip_address = ?, updated_at = ? WHERE id = ?")
                .bind(ip_address)
                .bind(now)
                .bind(id)
                .execute(&self.write_pool)
                .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: id.to_string(),
            });
        }

        Ok(())
    }

    /// Delete a master container record
    #[allow(dead_code)] // API method for cleanup
    pub async fn delete_master(&self, id: &str) -> SyncResult<()> {
        let result = sqlx::query("DELETE FROM master_containers WHERE id = ?")
            .bind(id)
            .execute(&self.write_pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: id.to_string(),
            });
        }

        tracing::info!("Deleted master container {}", id);
        Ok(())
    }

    /// Get the count of child containers for a master container
    /// (Containers whose tenant_id matches and are not the master container itself)
    pub async fn count_child_containers(
        &self,
        tenant_id: &str,
        _master_container_id: &str,
    ) -> SyncResult<(i32, i32)> {
        // Get the container_id that backs this master container
        let master = self.get_master(tenant_id).await?;
        let backing_container_id = match master {
            Some(m) => m.container_id,
            None => return Ok((0, 0)),
        };

        // Count all containers except the backing container
        let total: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM containers WHERE tenant_id = ? AND id != ?")
                .bind(tenant_id)
                .bind(&backing_container_id)
                .fetch_one(&self.read_pool)
                .await?;

        // Count running containers except the backing container
        let running: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM containers WHERE tenant_id = ? AND id != ? AND state = 'running'",
        )
        .bind(tenant_id)
        .bind(&backing_container_id)
        .fetch_one(&self.read_pool)
        .await?;

        Ok((total.0 as i32, running.0 as i32))
    }

    /// Get basic stats for a master container
    /// Note: CPU/memory/disk stats require reading from cgroups/filesystem
    pub async fn get_stats(&self, tenant_id: &str) -> SyncResult<MasterContainerStats> {
        let master = self.get_master(tenant_id).await?;
        let master_id = match &master {
            Some(m) => &m.id,
            None => {
                return Ok(MasterContainerStats {
                    cpu_usage_percent: 0.0,
                    memory_usage_mb: 0,
                    disk_usage_gb: 0.0,
                    child_containers: 0,
                    running_containers: 0,
                });
            }
        };

        let (total, running) = self.count_child_containers(tenant_id, master_id).await?;

        // TODO: Get actual CPU/memory/disk from cgroups or metrics table
        // For now return placeholder values
        Ok(MasterContainerStats {
            cpu_usage_percent: 0.0,
            memory_usage_mb: 0,
            disk_usage_gb: 0.0,
            child_containers: total,
            running_containers: running,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::connection::ConnectionManager;
    use crate::sync::migrations::MigrationManager;
    use crate::sync::schema::SchemaManager;
    use tempfile::NamedTempFile;

    async fn setup_test_db() -> (ConnectionManager, SqlitePool) {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();
        let conn_manager = ConnectionManager::new(db_path).await.unwrap();

        // Initialize base schema
        let schema_manager = SchemaManager::new(conn_manager.writer().clone());
        schema_manager.initialize_schema().await.unwrap();

        // Run migrations
        let migration_manager = MigrationManager::new(conn_manager.writer().clone());
        migration_manager.run_migrations().await.unwrap();

        let pool = conn_manager.writer().clone();

        // Create the required tenant for FK constraints
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        sqlx::query(
            "INSERT INTO tenants (id, name, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        )
        .bind("default")
        .bind("Default Tenant")
        .bind("active")
        .bind(now)
        .bind(now)
        .execute(&pool)
        .await
        .unwrap();

        (conn_manager, pool)
    }

    // Helper to create a test container for FK constraints
    async fn create_test_container(pool: &SqlitePool, container_id: &str) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        sqlx::query(
            "INSERT INTO containers (id, tenant_id, image_path, command, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(container_id)
        .bind("default")
        .bind("/test/image.tar.gz")
        .bind("/bin/sh")
        .bind("created")
        .bind(now)
        .bind(now)
        .execute(pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_create_master_container() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_123").await;

        let manager = MasterContainerManager::new(pool.clone(), pool);

        // Create a master container
        let master = manager
            .create_master("default", "ctr_test_123", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        assert!(master.id.starts_with("mc_"));
        assert_eq!(master.tenant_id, "default");
        assert_eq!(master.container_id, "ctr_test_123");
        assert_eq!(master.state, MasterContainerState::Created);
        assert_eq!(master.hostname, "quilt-default");
        assert_eq!(master.cpu_limit, 4);
        assert_eq!(master.memory_limit_mb, 8192);
    }

    #[tokio::test]
    async fn test_get_master_by_tenant() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_456").await;

        let manager = MasterContainerManager::new(pool.clone(), pool);

        // No master initially
        let master = manager.get_master("default").await.unwrap();
        assert!(master.is_none());

        // Create one
        manager
            .create_master("default", "ctr_test_456", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        // Now it exists
        let master = manager.get_master("default").await.unwrap();
        assert!(master.is_some());
        assert_eq!(master.unwrap().container_id, "ctr_test_456");
    }

    #[tokio::test]
    async fn test_update_master_state() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_789").await;

        let manager = MasterContainerManager::new(pool.clone(), pool);

        let master = manager
            .create_master("default", "ctr_test_789", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        // Update to Starting
        manager
            .update_state(&master.id, MasterContainerState::Starting)
            .await
            .unwrap();
        let updated = manager.get_master("default").await.unwrap().unwrap();
        assert_eq!(updated.state, MasterContainerState::Starting);

        // Update to Running
        manager
            .update_state(&master.id, MasterContainerState::Running)
            .await
            .unwrap();
        let updated = manager.get_master("default").await.unwrap().unwrap();
        assert_eq!(updated.state, MasterContainerState::Running);
        assert!(updated.started_at.is_some());
    }

    #[tokio::test]
    async fn test_set_ip_address() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_ip").await;

        let manager = MasterContainerManager::new(pool.clone(), pool);

        let master = manager
            .create_master("default", "ctr_test_ip", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        assert!(master.ip_address.is_none());

        manager
            .set_ip_address(&master.id, "10.0.0.1")
            .await
            .unwrap();

        let updated = manager.get_master("default").await.unwrap().unwrap();
        assert_eq!(updated.ip_address, Some("10.0.0.1".to_string()));
    }

    #[tokio::test]
    async fn test_delete_master() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_del").await;

        let manager = MasterContainerManager::new(pool.clone(), pool);

        let master = manager
            .create_master("default", "ctr_test_del", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        // Delete it
        manager.delete_master(&master.id).await.unwrap();

        // Gone
        let master = manager.get_master("default").await.unwrap();
        assert!(master.is_none());
    }
}

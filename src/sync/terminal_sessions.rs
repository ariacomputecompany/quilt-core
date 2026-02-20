use crate::sync::error::{SyncError, SyncResult};
/// Terminal Session Manager
///
/// Manages terminal sessions that connect to master containers or child containers.
/// Sessions are WebSocket-based with PTY support for full terminal functionality.
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::time::{SystemTime, UNIX_EPOCH};

/// Default session TTL in seconds (4 hours)
pub const DEFAULT_SESSION_TTL: i64 = 14400;

/// Default idle timeout in seconds (30 minutes)
pub const DEFAULT_IDLE_TIMEOUT: i64 = 1800;

/// Terminal target - either master container or a child container
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TerminalTarget {
    Master,
    Container,
}

impl std::fmt::Display for TerminalTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TerminalTarget::Master => write!(f, "master"),
            TerminalTarget::Container => write!(f, "container"),
        }
    }
}

impl TerminalTarget {
    pub fn from_string(s: &str) -> SyncResult<Self> {
        match s {
            "master" => Ok(TerminalTarget::Master),
            "container" => Ok(TerminalTarget::Container),
            _ => Err(SyncError::ValidationFailed {
                message: format!("Invalid terminal target: {}", s),
            }),
        }
    }
}

/// Terminal session states
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TerminalSessionState {
    /// Session created, waiting for WebSocket connection
    Ready,
    /// WebSocket connected, PTY active
    Active,
    /// No activity for > 5 minutes
    Idle,
    /// Session ended (timeout, user exit, or disconnect)
    Terminated,
    /// Error state (PTY creation failed, container not ready, etc.)
    Error,
}

impl std::fmt::Display for TerminalSessionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TerminalSessionState::Ready => write!(f, "ready"),
            TerminalSessionState::Active => write!(f, "active"),
            TerminalSessionState::Idle => write!(f, "idle"),
            TerminalSessionState::Terminated => write!(f, "terminated"),
            TerminalSessionState::Error => write!(f, "error"),
        }
    }
}

impl TerminalSessionState {
    pub fn from_string(s: &str) -> SyncResult<Self> {
        match s {
            "ready" => Ok(TerminalSessionState::Ready),
            "active" => Ok(TerminalSessionState::Active),
            "idle" => Ok(TerminalSessionState::Idle),
            "terminated" => Ok(TerminalSessionState::Terminated),
            "error" => Ok(TerminalSessionState::Error),
            _ => Err(SyncError::ValidationFailed {
                message: format!("Invalid terminal session state: {}", s),
            }),
        }
    }
}

/// Terminal session entity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TerminalSession {
    pub id: String,
    pub tenant_id: String,
    pub master_container_id: String,
    pub target: TerminalTarget,
    pub child_container_id: Option<String>,
    pub state: TerminalSessionState,
    pub shell: String,
    pub cols: i32,
    pub rows: i32,
    pub pid: Option<i32>,
    pub created_at: i64,
    pub last_activity_at: i64,
    pub expires_at: i64,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub retry_count: i32,
    pub last_error_at: Option<i64>,
}

impl TerminalSession {
    /// Check if the session has expired
    #[allow(dead_code)] // Used by cleanup logic
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        now > self.expires_at
    }

    /// Check if the session is idle (no activity for > 5 minutes)
    #[allow(dead_code)] // Used by cleanup logic
    pub fn is_idle(&self, idle_timeout_secs: i64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        (now - self.last_activity_at) > idle_timeout_secs
    }
}

/// Request to create a new terminal session
#[derive(Debug, Clone)]
pub struct CreateSessionRequest {
    pub tenant_id: String,
    pub master_container_id: String,
    pub target: TerminalTarget,
    pub child_container_id: Option<String>,
    pub shell: Option<String>,
    pub cols: Option<i32>,
    pub rows: Option<i32>,
}

/// Manager for terminal session operations
pub struct TerminalSessionManager {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl TerminalSessionManager {
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    /// Generate a session ID (sess_ + 12 hex chars)
    fn generate_session_id() -> String {
        let uuid = uuid::Uuid::new_v4();
        let hex = format!("{:x}", uuid.as_u128());
        format!("sess_{}", &hex[..12])
    }

    /// Create a new terminal session
    pub async fn create_session(&self, req: CreateSessionRequest) -> SyncResult<TerminalSession> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let id = Self::generate_session_id();
        let expires_at = now + DEFAULT_SESSION_TTL;

        let shell = req.shell.unwrap_or_else(|| "/bin/bash".to_string());
        let cols = req.cols.unwrap_or(80);
        let rows = req.rows.unwrap_or(24);

        // Validate: if target is Container, child_container_id must be provided
        if req.target == TerminalTarget::Container && req.child_container_id.is_none() {
            return Err(SyncError::ValidationFailed {
                message: "child_container_id required when target is 'container'".to_string(),
            });
        }

        sqlx::query(
            r#"
            INSERT INTO terminal_sessions (
                id, tenant_id, master_container_id, target, child_container_id,
                state, shell, cols, rows, pid,
                created_at, last_activity_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, 'ready', ?, ?, ?, NULL, ?, ?, ?)
        "#,
        )
        .bind(&id)
        .bind(&req.tenant_id)
        .bind(&req.master_container_id)
        .bind(req.target.to_string())
        .bind(&req.child_container_id)
        .bind(&shell)
        .bind(cols)
        .bind(rows)
        .bind(now)
        .bind(now)
        .bind(expires_at)
        .execute(&self.write_pool)
        .await?;

        tracing::info!(
            "Created terminal session {} for tenant {}",
            id,
            req.tenant_id
        );

        Ok(TerminalSession {
            id,
            tenant_id: req.tenant_id,
            master_container_id: req.master_container_id,
            target: req.target,
            child_container_id: req.child_container_id,
            state: TerminalSessionState::Ready,
            shell,
            cols,
            rows,
            pid: None,
            created_at: now,
            last_activity_at: now,
            expires_at,
            error_code: None,
            error_message: None,
            retry_count: 0,
            last_error_at: None,
        })
    }

    /// Get a terminal session by ID
    pub async fn get_session(&self, session_id: &str) -> SyncResult<Option<TerminalSession>> {
        let row = sqlx::query(
            r#"
            SELECT
                id, tenant_id, master_container_id, target, child_container_id,
                state, shell, cols, rows, pid,
                created_at, last_activity_at, expires_at,
                error_code, error_message, retry_count, last_error_at
            FROM terminal_sessions
            WHERE id = ?
        "#,
        )
        .bind(session_id)
        .fetch_optional(&self.read_pool)
        .await?;

        match row {
            Some(row) => {
                let target_str: String = row.get("target");
                let target = TerminalTarget::from_string(&target_str)?;

                let state_str: String = row.get("state");
                let state = TerminalSessionState::from_string(&state_str)?;

                Ok(Some(TerminalSession {
                    id: row.get("id"),
                    tenant_id: row.get("tenant_id"),
                    master_container_id: row.get("master_container_id"),
                    target,
                    child_container_id: row.get("child_container_id"),
                    state,
                    shell: row.get("shell"),
                    cols: row.get("cols"),
                    rows: row.get("rows"),
                    pid: row.get("pid"),
                    created_at: row.get("created_at"),
                    last_activity_at: row.get("last_activity_at"),
                    expires_at: row.get("expires_at"),
                    error_code: row.get("error_code"),
                    error_message: row.get("error_message"),
                    retry_count: row.get("retry_count"),
                    last_error_at: row.get("last_error_at"),
                }))
            }
            None => Ok(None),
        }
    }

    /// Get a session and verify it belongs to the tenant
    pub async fn get_session_for_tenant(
        &self,
        session_id: &str,
        tenant_id: &str,
    ) -> SyncResult<Option<TerminalSession>> {
        let row = sqlx::query(
            r#"
            SELECT
                id, tenant_id, master_container_id, target, child_container_id,
                state, shell, cols, rows, pid,
                created_at, last_activity_at, expires_at,
                error_code, error_message, retry_count, last_error_at
            FROM terminal_sessions
            WHERE id = ? AND tenant_id = ?
        "#,
        )
        .bind(session_id)
        .bind(tenant_id)
        .fetch_optional(&self.read_pool)
        .await?;

        match row {
            Some(row) => {
                let target_str: String = row.get("target");
                let target = TerminalTarget::from_string(&target_str)?;

                let state_str: String = row.get("state");
                let state = TerminalSessionState::from_string(&state_str)?;

                Ok(Some(TerminalSession {
                    id: row.get("id"),
                    tenant_id: row.get("tenant_id"),
                    master_container_id: row.get("master_container_id"),
                    target,
                    child_container_id: row.get("child_container_id"),
                    state,
                    shell: row.get("shell"),
                    cols: row.get("cols"),
                    rows: row.get("rows"),
                    pid: row.get("pid"),
                    created_at: row.get("created_at"),
                    last_activity_at: row.get("last_activity_at"),
                    expires_at: row.get("expires_at"),
                    error_code: row.get("error_code"),
                    error_message: row.get("error_message"),
                    retry_count: row.get("retry_count"),
                    last_error_at: row.get("last_error_at"),
                }))
            }
            None => Ok(None),
        }
    }

    /// List all active sessions for a tenant
    pub async fn list_sessions(&self, tenant_id: &str) -> SyncResult<Vec<TerminalSession>> {
        let rows = sqlx::query(
            r#"
            SELECT
                id, tenant_id, master_container_id, target, child_container_id,
                state, shell, cols, rows, pid,
                created_at, last_activity_at, expires_at,
                error_code, error_message, retry_count, last_error_at
            FROM terminal_sessions
            WHERE tenant_id = ? AND state != 'terminated'
            ORDER BY created_at DESC
        "#,
        )
        .bind(tenant_id)
        .fetch_all(&self.read_pool)
        .await?;

        let mut sessions = Vec::new();
        for row in rows {
            let target_str: String = row.get("target");
            let target = TerminalTarget::from_string(&target_str)?;

            let state_str: String = row.get("state");
            let state = TerminalSessionState::from_string(&state_str)?;

            sessions.push(TerminalSession {
                id: row.get("id"),
                tenant_id: row.get("tenant_id"),
                master_container_id: row.get("master_container_id"),
                target,
                child_container_id: row.get("child_container_id"),
                state,
                shell: row.get("shell"),
                cols: row.get("cols"),
                rows: row.get("rows"),
                pid: row.get("pid"),
                created_at: row.get("created_at"),
                last_activity_at: row.get("last_activity_at"),
                expires_at: row.get("expires_at"),
                error_code: row.get("error_code"),
                error_message: row.get("error_message"),
                retry_count: row.get("retry_count"),
                last_error_at: row.get("last_error_at"),
            });
        }

        Ok(sessions)
    }

    /// Update session state
    pub async fn update_state(
        &self,
        session_id: &str,
        new_state: TerminalSessionState,
    ) -> SyncResult<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        let result = sqlx::query(
            "UPDATE terminal_sessions SET state = ?, last_activity_at = ? WHERE id = ?",
        )
        .bind(new_state.to_string())
        .bind(now)
        .bind(session_id)
        .execute(&self.write_pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: session_id.to_string(),
            });
        }

        tracing::debug!(
            "Updated session {} state to {}",
            session_id,
            new_state.to_string()
        );
        Ok(())
    }

    /// Update session activity timestamp (called on each input/output)
    pub async fn update_activity(&self, session_id: &str) -> SyncResult<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        let result = sqlx::query("UPDATE terminal_sessions SET last_activity_at = ? WHERE id = ?")
            .bind(now)
            .bind(session_id)
            .execute(&self.write_pool)
            .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: session_id.to_string(),
            });
        }

        Ok(())
    }

    /// Set session PID (after PTY process is spawned)
    pub async fn set_pid(&self, session_id: &str, pid: i32) -> SyncResult<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        let result = sqlx::query(
            "UPDATE terminal_sessions SET pid = ?, state = 'active', last_activity_at = ? WHERE id = ?"
        )
        .bind(pid)
        .bind(now)
        .bind(session_id)
        .execute(&self.write_pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: session_id.to_string(),
            });
        }

        tracing::debug!("Set session {} PID to {}", session_id, pid);
        Ok(())
    }

    /// Resize terminal (update cols/rows)
    pub async fn resize_session(&self, session_id: &str, cols: i32, rows: i32) -> SyncResult<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        let result = sqlx::query(
            "UPDATE terminal_sessions SET cols = ?, rows = ?, last_activity_at = ? WHERE id = ?",
        )
        .bind(cols)
        .bind(rows)
        .bind(now)
        .bind(session_id)
        .execute(&self.write_pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: session_id.to_string(),
            });
        }

        tracing::debug!("Resized session {} to {}x{}", session_id, cols, rows);
        Ok(())
    }

    /// Set session to error state with error details
    pub async fn set_error(
        &self,
        session_id: &str,
        error_code: &str,
        error_message: &str,
    ) -> SyncResult<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        let result = sqlx::query(
            "UPDATE terminal_sessions
             SET state = 'error', error_code = ?, error_message = ?,
                 retry_count = retry_count + 1, last_error_at = ?,
                 last_activity_at = ?
             WHERE id = ?",
        )
        .bind(error_code)
        .bind(error_message)
        .bind(now)
        .bind(now)
        .bind(session_id)
        .execute(&self.write_pool)
        .await?;

        if result.rows_affected() == 0 {
            return Err(SyncError::NotFound {
                container_id: session_id.to_string(),
            });
        }

        tracing::error!(
            "Terminal session {} error: {} - {}",
            session_id,
            error_code,
            error_message
        );
        Ok(())
    }

    /// Terminate a session
    pub async fn terminate_session(&self, session_id: &str) -> SyncResult<()> {
        self.update_state(session_id, TerminalSessionState::Terminated)
            .await
    }

    /// Cleanup expired sessions
    /// Returns the number of sessions cleaned up
    pub async fn cleanup_expired_sessions(&self) -> SyncResult<i32> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        // Find expired sessions that are not already terminated
        let expired: Vec<(String,)> = sqlx::query_as(
            "SELECT id FROM terminal_sessions WHERE expires_at < ? AND state != 'terminated'",
        )
        .bind(now)
        .fetch_all(&self.read_pool)
        .await?;

        let count = expired.len() as i32;

        if count > 0 {
            // Mark them as terminated
            sqlx::query(
                "UPDATE terminal_sessions SET state = 'terminated' WHERE expires_at < ? AND state != 'terminated'"
            )
            .bind(now)
            .execute(&self.write_pool)
            .await?;

            tracing::info!("Cleaned up {} expired terminal sessions", count);
        }

        Ok(count)
    }

    /// Find idle sessions and mark them as idle
    pub async fn mark_idle_sessions(&self, idle_timeout_secs: i64) -> SyncResult<i32> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let idle_threshold = now - idle_timeout_secs;

        let result = sqlx::query(
            "UPDATE terminal_sessions SET state = 'idle' WHERE last_activity_at < ? AND state = 'active'"
        )
        .bind(idle_threshold)
        .execute(&self.write_pool)
        .await?;

        let count = result.rows_affected() as i32;
        if count > 0 {
            tracing::debug!("Marked {} sessions as idle", count);
        }

        Ok(count)
    }

    /// Count active sessions for a tenant (for enforcing limits)
    pub async fn count_active_sessions(&self, tenant_id: &str) -> SyncResult<i32> {
        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM terminal_sessions WHERE tenant_id = ? AND state IN ('ready', 'active', 'idle')"
        )
        .bind(tenant_id)
        .fetch_one(&self.read_pool)
        .await?;

        Ok(count.0 as i32)
    }

    /// Delete old terminated sessions (cleanup)
    pub async fn delete_old_terminated_sessions(&self, older_than_secs: i64) -> SyncResult<i32> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        let threshold = now - older_than_secs;

        let result = sqlx::query(
            "DELETE FROM terminal_sessions WHERE state = 'terminated' AND last_activity_at < ?",
        )
        .bind(threshold)
        .execute(&self.write_pool)
        .await?;

        let count = result.rows_affected() as i32;
        if count > 0 {
            tracing::info!("Deleted {} old terminated sessions", count);
        }

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::connection::ConnectionManager;
    use crate::sync::master_containers::MasterContainerManager;
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
    async fn test_create_session() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_123").await;

        // Create a master container first
        let master_manager = MasterContainerManager::new(pool.clone(), pool.clone());
        let master = master_manager
            .create_master("default", "ctr_test_123", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        // Create a terminal session
        let session_manager = TerminalSessionManager::new(pool.clone(), pool);
        let session = session_manager
            .create_session(CreateSessionRequest {
                tenant_id: "default".to_string(),
                master_container_id: master.id.clone(),
                target: TerminalTarget::Master,
                child_container_id: None,
                shell: None,
                cols: Some(120),
                rows: Some(30),
            })
            .await
            .unwrap();

        assert!(session.id.starts_with("sess_"));
        assert_eq!(session.tenant_id, "default");
        assert_eq!(session.target, TerminalTarget::Master);
        assert_eq!(session.state, TerminalSessionState::Ready);
        assert_eq!(session.cols, 120);
        assert_eq!(session.rows, 30);
        assert_eq!(session.shell, "/bin/bash");
    }

    #[tokio::test]
    async fn test_get_session() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_get").await;

        let master_manager = MasterContainerManager::new(pool.clone(), pool.clone());
        let master = master_manager
            .create_master("default", "ctr_test_get", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        let session_manager = TerminalSessionManager::new(pool.clone(), pool);
        let session = session_manager
            .create_session(CreateSessionRequest {
                tenant_id: "default".to_string(),
                master_container_id: master.id.clone(),
                target: TerminalTarget::Master,
                child_container_id: None,
                shell: None,
                cols: None,
                rows: None,
            })
            .await
            .unwrap();

        // Get by ID
        let retrieved = session_manager.get_session(&session.id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().id, session.id);

        // Get for tenant
        let retrieved = session_manager
            .get_session_for_tenant(&session.id, "default")
            .await
            .unwrap();
        assert!(retrieved.is_some());

        // Different tenant should fail
        let retrieved = session_manager
            .get_session_for_tenant(&session.id, "other")
            .await
            .unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    async fn test_update_state() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_state").await;

        let master_manager = MasterContainerManager::new(pool.clone(), pool.clone());
        let master = master_manager
            .create_master("default", "ctr_test_state", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        let session_manager = TerminalSessionManager::new(pool.clone(), pool);
        let session = session_manager
            .create_session(CreateSessionRequest {
                tenant_id: "default".to_string(),
                master_container_id: master.id.clone(),
                target: TerminalTarget::Master,
                child_container_id: None,
                shell: None,
                cols: None,
                rows: None,
            })
            .await
            .unwrap();

        // Update to Active
        session_manager
            .update_state(&session.id, TerminalSessionState::Active)
            .await
            .unwrap();
        let updated = session_manager
            .get_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.state, TerminalSessionState::Active);

        // Update to Terminated
        session_manager
            .terminate_session(&session.id)
            .await
            .unwrap();
        let updated = session_manager
            .get_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.state, TerminalSessionState::Terminated);
    }

    #[tokio::test]
    async fn test_set_pid() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_pid").await;

        let master_manager = MasterContainerManager::new(pool.clone(), pool.clone());
        let master = master_manager
            .create_master("default", "ctr_test_pid", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        let session_manager = TerminalSessionManager::new(pool.clone(), pool);
        let session = session_manager
            .create_session(CreateSessionRequest {
                tenant_id: "default".to_string(),
                master_container_id: master.id.clone(),
                target: TerminalTarget::Master,
                child_container_id: None,
                shell: None,
                cols: None,
                rows: None,
            })
            .await
            .unwrap();

        // Set PID
        session_manager.set_pid(&session.id, 12345).await.unwrap();
        let updated = session_manager
            .get_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.pid, Some(12345));
        assert_eq!(updated.state, TerminalSessionState::Active);
    }

    #[tokio::test]
    async fn test_resize_session() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_resize").await;

        let master_manager = MasterContainerManager::new(pool.clone(), pool.clone());
        let master = master_manager
            .create_master("default", "ctr_test_resize", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        let session_manager = TerminalSessionManager::new(pool.clone(), pool);
        let session = session_manager
            .create_session(CreateSessionRequest {
                tenant_id: "default".to_string(),
                master_container_id: master.id.clone(),
                target: TerminalTarget::Master,
                child_container_id: None,
                shell: None,
                cols: Some(80),
                rows: Some(24),
            })
            .await
            .unwrap();

        // Resize
        session_manager
            .resize_session(&session.id, 150, 40)
            .await
            .unwrap();
        let updated = session_manager
            .get_session(&session.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(updated.cols, 150);
        assert_eq!(updated.rows, 40);
    }

    #[tokio::test]
    async fn test_list_sessions() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_list").await;

        let master_manager = MasterContainerManager::new(pool.clone(), pool.clone());
        let master = master_manager
            .create_master("default", "ctr_test_list", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        let session_manager = TerminalSessionManager::new(pool.clone(), pool);

        // Create a few sessions
        for _ in 0..3 {
            session_manager
                .create_session(CreateSessionRequest {
                    tenant_id: "default".to_string(),
                    master_container_id: master.id.clone(),
                    target: TerminalTarget::Master,
                    child_container_id: None,
                    shell: None,
                    cols: None,
                    rows: None,
                })
                .await
                .unwrap();
        }

        // List sessions
        let sessions = session_manager.list_sessions("default").await.unwrap();
        assert_eq!(sessions.len(), 3);
    }

    #[tokio::test]
    async fn test_count_active_sessions() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_count").await;

        let master_manager = MasterContainerManager::new(pool.clone(), pool.clone());
        let master = master_manager
            .create_master("default", "ctr_test_count", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        let session_manager = TerminalSessionManager::new(pool.clone(), pool);

        // Create sessions
        let s1 = session_manager
            .create_session(CreateSessionRequest {
                tenant_id: "default".to_string(),
                master_container_id: master.id.clone(),
                target: TerminalTarget::Master,
                child_container_id: None,
                shell: None,
                cols: None,
                rows: None,
            })
            .await
            .unwrap();

        let _ = session_manager
            .create_session(CreateSessionRequest {
                tenant_id: "default".to_string(),
                master_container_id: master.id.clone(),
                target: TerminalTarget::Master,
                child_container_id: None,
                shell: None,
                cols: None,
                rows: None,
            })
            .await
            .unwrap();

        // Both are active
        let count = session_manager
            .count_active_sessions("default")
            .await
            .unwrap();
        assert_eq!(count, 2);

        // Terminate one
        session_manager.terminate_session(&s1.id).await.unwrap();

        // Only one left
        let count = session_manager
            .count_active_sessions("default")
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_container_target_requires_child_id() {
        let (_conn, pool) = setup_test_db().await;

        // Create required container for FK constraint
        create_test_container(&pool, "ctr_test_validate").await;

        let master_manager = MasterContainerManager::new(pool.clone(), pool.clone());
        let master = master_manager
            .create_master("default", "ctr_test_validate", "quilt-default", 4, 8192, 50)
            .await
            .unwrap();

        let session_manager = TerminalSessionManager::new(pool.clone(), pool);

        // Target=container without child_container_id should fail
        let result = session_manager
            .create_session(CreateSessionRequest {
                tenant_id: "default".to_string(),
                master_container_id: master.id.clone(),
                target: TerminalTarget::Container,
                child_container_id: None,
                shell: None,
                cols: None,
                rows: None,
            })
            .await;

        assert!(result.is_err());
    }
}

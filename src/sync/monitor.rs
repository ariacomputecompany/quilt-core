use crate::sync::error::{SyncError, SyncResult};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::Pid;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MonitorStatus {
    Monitoring,
    Completed,
    Failed,
    Aborted,
}

impl MonitorStatus {
    pub fn to_string(&self) -> String {
        match self {
            MonitorStatus::Monitoring => "monitoring".to_string(),
            MonitorStatus::Completed => "completed".to_string(),
            MonitorStatus::Failed => "failed".to_string(),
            MonitorStatus::Aborted => "aborted".to_string(),
        }
    }

    pub fn from_string(s: &str) -> SyncResult<Self> {
        match s {
            "monitoring" => Ok(MonitorStatus::Monitoring),
            "completed" => Ok(MonitorStatus::Completed),
            "failed" => Ok(MonitorStatus::Failed),
            "aborted" => Ok(MonitorStatus::Aborted),
            _ => Err(SyncError::ValidationFailed {
                message: format!("Invalid monitor status: {}", s),
            }),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProcessMonitor {
    pub container_id: String,
    pub pid: i64,
    pub monitor_started_at: i64,
    pub last_check_at: Option<i64>,
    pub status: MonitorStatus,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProcessStatus {
    Running,
    Exited(i32),
    Error,
}

pub struct ProcessMonitorService {
    pool: SqlitePool,
    active_monitors: Arc<Mutex<HashSet<String>>>,
    check_interval: Duration,
}

impl ProcessMonitorService {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            active_monitors: Arc::new(Mutex::new(HashSet::new())),
            check_interval: Duration::from_millis(100), // More responsive - 100ms checks
        }
    }

    #[allow(dead_code)] // Production monitoring infrastructure
    pub fn with_check_interval(pool: SqlitePool, interval: Duration) -> Self {
        Self {
            pool,
            active_monitors: Arc::new(Mutex::new(HashSet::new())),
            check_interval: interval,
        }
    }

    pub async fn start_monitoring(&self, container_id: &str, pid: Pid) -> SyncResult<()> {
        // Check if already monitoring
        {
            let active = self.active_monitors.lock().await;
            if active.contains(container_id) {
                tracing::debug!("Already monitoring container {}", container_id);
                return Ok(());
            }
        }

        // Record monitoring start in database
        self.start_process_monitor(container_id, pid.as_raw() as i64)
            .await?;

        // Add to active monitors
        {
            let mut active = self.active_monitors.lock().await;
            active.insert(container_id.to_string());
        }

        // Spawn DETACHED monitoring task
        let pool = self.pool.clone();
        let active_monitors = self.active_monitors.clone();
        let container_id = container_id.to_string();
        let check_interval = self.check_interval;

        tokio::spawn(async move {
            tracing::info!(
                "Started background monitoring for container {} (PID: {})",
                container_id,
                pid
            );

            loop {
                match Self::check_process_status(pid).await {
                    ProcessStatus::Running => {
                        // Update heartbeat in database
                        if let Err(e) = Self::update_monitor_heartbeat(&pool, &container_id).await {
                            tracing::warn!(
                                "Failed to update monitor heartbeat for {}: {}",
                                container_id,
                                e
                            );
                        }

                        tokio::time::sleep(check_interval).await;
                    }
                    ProcessStatus::Exited(exit_code) => {
                        tracing::info!("Process {} exited with code {}", pid, exit_code);

                        // Update database with completion
                        if let Err(e) =
                            Self::complete_process_monitor(&pool, &container_id, exit_code).await
                        {
                            tracing::error!(
                                "Failed to mark process monitor completed for {}: {}",
                                container_id,
                                e
                            );
                        }

                        // Remove from active monitors
                        {
                            let mut active = active_monitors.lock().await;
                            active.remove(&container_id);
                        }

                        break;
                    }
                    ProcessStatus::Error => {
                        tracing::warn!("Error checking process status for {}", container_id);

                        // Mark as failed in database
                        if let Err(e) =
                            Self::fail_process_monitor(&pool, &container_id, "Process check failed")
                                .await
                        {
                            tracing::error!(
                                "Failed to mark process monitor failed for {}: {}",
                                container_id,
                                e
                            );
                        }

                        // Remove from active monitors
                        {
                            let mut active = active_monitors.lock().await;
                            active.remove(&container_id);
                        }

                        break;
                    }
                }
            }

            tracing::info!("Finished monitoring container {}", container_id);
        });

        Ok(()) // ✅ INSTANT RETURN - Server not blocked
    }

    pub async fn stop_monitoring(&self, container_id: &str) -> SyncResult<()> {
        // Mark as aborted in database
        self.abort_process_monitor(container_id).await?;

        // Remove from active monitors
        {
            let mut active = self.active_monitors.lock().await;
            active.remove(container_id);
        }

        tracing::info!("Stopped monitoring container {}", container_id);
        Ok(())
    }

    pub async fn get_monitor_status(&self, container_id: &str) -> SyncResult<ProcessMonitor> {
        let row = sqlx::query(
            r#"
            SELECT container_id, pid, monitor_started_at, last_check_at, status
            FROM process_monitors WHERE container_id = ?
        "#,
        )
        .bind(container_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let status_str: String = row.get("status");
                let status = MonitorStatus::from_string(&status_str)?;

                Ok(ProcessMonitor {
                    container_id: row.get("container_id"),
                    pid: row.get("pid"),
                    monitor_started_at: row.get("monitor_started_at"),
                    last_check_at: row.get("last_check_at"),
                    status,
                })
            }
            None => Err(SyncError::NotFound {
                container_id: container_id.to_string(),
            }),
        }
    }

    pub async fn list_active_monitors(&self) -> SyncResult<Vec<ProcessMonitor>> {
        let rows = sqlx::query(
            r#"
            SELECT container_id, pid, monitor_started_at, last_check_at, status
            FROM process_monitors WHERE status = 'monitoring'
            ORDER BY monitor_started_at ASC
        "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut monitors = Vec::new();
        for row in rows {
            let status_str: String = row.get("status");
            let status = MonitorStatus::from_string(&status_str)?;

            monitors.push(ProcessMonitor {
                container_id: row.get("container_id"),
                pid: row.get("pid"),
                monitor_started_at: row.get("monitor_started_at"),
                last_check_at: row.get("last_check_at"),
                status,
            });
        }

        Ok(monitors)
    }

    pub async fn cleanup_stale_monitors(&self, stale_threshold: Duration) -> SyncResult<usize> {
        let threshold_timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64
            - stale_threshold.as_secs() as i64;

        let result = sqlx::query(
            r#"
            UPDATE process_monitors 
            SET status = 'failed' 
            WHERE status = 'monitoring' 
            AND (last_check_at IS NULL OR last_check_at < ?)
        "#,
        )
        .bind(threshold_timestamp)
        .execute(&self.pool)
        .await?;

        let count = result.rows_affected() as usize;
        if count > 0 {
            tracing::warn!("Cleaned up {} stale monitors", count);
        }

        Ok(count)
    }

    async fn start_process_monitor(&self, container_id: &str, pid: i64) -> SyncResult<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        sqlx::query(
            r#"
            INSERT OR REPLACE INTO process_monitors (
                container_id, pid, monitor_started_at, status
            ) VALUES (?, ?, ?, ?)
        "#,
        )
        .bind(container_id)
        .bind(pid)
        .bind(now)
        .bind(MonitorStatus::Monitoring.to_string())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn update_monitor_heartbeat(pool: &SqlitePool, container_id: &str) -> SyncResult<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;

        sqlx::query("UPDATE process_monitors SET last_check_at = ? WHERE container_id = ?")
            .bind(now)
            .bind(container_id)
            .execute(pool)
            .await?;

        Ok(())
    }

    async fn complete_process_monitor(
        pool: &SqlitePool,
        container_id: &str,
        exit_code: i32,
    ) -> SyncResult<()> {
        // Update process monitor status
        sqlx::query("UPDATE process_monitors SET status = ? WHERE container_id = ?")
            .bind(MonitorStatus::Completed.to_string())
            .bind(container_id)
            .execute(pool)
            .await?;

        // Update container state to Exited
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        sqlx::query("UPDATE containers SET state = ?, exit_code = ?, updated_at = ? WHERE id = ?")
            .bind("exited")
            .bind(exit_code as i64)
            .bind(now)
            .bind(container_id)
            .execute(pool)
            .await?;

        tracing::info!("Container {} exited with code {}", container_id, exit_code);

        Ok(())
    }

    async fn fail_process_monitor(
        pool: &SqlitePool,
        container_id: &str,
        error_message: &str,
    ) -> SyncResult<()> {
        // Update process monitor status
        sqlx::query("UPDATE process_monitors SET status = ? WHERE container_id = ?")
            .bind(MonitorStatus::Failed.to_string())
            .bind(container_id)
            .execute(pool)
            .await?;

        // Update container state to Error
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64;
        sqlx::query("UPDATE containers SET state = ?, updated_at = ? WHERE id = ?")
            .bind("error")
            .bind(now)
            .bind(container_id)
            .execute(pool)
            .await?;

        tracing::warn!(
            "Process monitor failed for container {}: {}",
            container_id,
            error_message
        );
        Ok(())
    }

    async fn abort_process_monitor(&self, container_id: &str) -> SyncResult<()> {
        sqlx::query("UPDATE process_monitors SET status = ? WHERE container_id = ?")
            .bind(MonitorStatus::Aborted.to_string())
            .bind(container_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    async fn check_process_status(pid: Pid) -> ProcessStatus {
        // Use tokio task to avoid blocking the async runtime
        tokio::task::spawn_blocking(move || {
            match waitpid(pid, Some(WaitPidFlag::WNOHANG)) {
                Ok(WaitStatus::StillAlive) => ProcessStatus::Running,
                Ok(WaitStatus::Exited(_, exit_code)) => ProcessStatus::Exited(exit_code),
                Ok(WaitStatus::Signaled(_, signal, _)) => {
                    tracing::debug!("Process {} terminated by signal {:?}", pid, signal);
                    ProcessStatus::Exited(128 + signal as i32) // Standard convention for signal exits
                }
                Ok(status) => {
                    tracing::debug!("Process {} status: {:?}", pid, status);
                    ProcessStatus::Exited(1) // Treat other statuses as generic failure
                }
                Err(nix::errno::Errno::ECHILD) => {
                    // Process doesn't exist or is not a child
                    ProcessStatus::Exited(0) // Assume it exited normally
                }
                Err(e) => {
                    tracing::error!("Error checking process status for {}: {}", pid, e);
                    ProcessStatus::Error
                }
            }
        })
        .await
        .unwrap_or(ProcessStatus::Error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::connection::ConnectionManager;
    use crate::sync::schema::SchemaManager;
    use nix::unistd::Pid;
    use std::process::Command;
    use tempfile::NamedTempFile;

    async fn setup_test_db() -> (ConnectionManager, ProcessMonitorService) {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn_manager = ConnectionManager::new(db_path).await.unwrap();
        let schema_manager = SchemaManager::new(conn_manager.pool().clone());
        schema_manager.initialize_schema().await.unwrap();

        let monitor_service = ProcessMonitorService::with_check_interval(
            conn_manager.pool().clone(),
            Duration::from_millis(100), // Fast polling for tests
        );

        (conn_manager, monitor_service)
    }

    #[tokio::test]
    async fn test_monitor_lifecycle() {
        let (_conn, monitor_service) = setup_test_db().await;

        // Start a short-lived process
        let mut child = Command::new("sleep")
            .arg("0.2")
            .spawn()
            .expect("Failed to start test process");

        let pid = Pid::from_raw(child.id() as i32);

        // Start monitoring
        monitor_service
            .start_monitoring("test-container", pid)
            .await
            .unwrap();

        // Check initial status
        let monitor = monitor_service
            .get_monitor_status("test-container")
            .await
            .unwrap();
        assert_eq!(monitor.status, MonitorStatus::Monitoring);
        assert_eq!(monitor.pid, pid.as_raw() as i64);

        // Wait for process to exit and monitoring to complete
        tokio::time::sleep(Duration::from_millis(500)).await;
        child.wait().expect("Failed to wait for child");

        // Check final status
        let monitor = monitor_service
            .get_monitor_status("test-container")
            .await
            .unwrap();
        assert_eq!(monitor.status, MonitorStatus::Completed);
    }

    #[tokio::test]
    async fn test_monitor_stop() {
        let (_conn, monitor_service) = setup_test_db().await;

        // Start a long-running process
        let mut child = Command::new("sleep")
            .arg("10")
            .spawn()
            .expect("Failed to start test process");

        let pid = Pid::from_raw(child.id() as i32);

        // Start monitoring
        monitor_service
            .start_monitoring("test-container", pid)
            .await
            .unwrap();

        // Stop monitoring
        monitor_service
            .stop_monitoring("test-container")
            .await
            .unwrap();

        // Check status
        let monitor = monitor_service
            .get_monitor_status("test-container")
            .await
            .unwrap();
        assert_eq!(monitor.status, MonitorStatus::Aborted);

        // Clean up
        child.kill().expect("Failed to kill child");
        child.wait().expect("Failed to wait for child");
    }

    #[tokio::test]
    async fn test_stale_monitor_cleanup() {
        let (_conn, monitor_service) = setup_test_db().await;

        // Create a stale monitor entry
        monitor_service
            .start_process_monitor("stale-container", 99999)
            .await
            .unwrap();

        // Run cleanup with very short threshold
        let count = monitor_service
            .cleanup_stale_monitors(Duration::from_secs(0))
            .await
            .unwrap();
        assert_eq!(count, 1);

        // Check that it was marked as failed
        let monitor = monitor_service
            .get_monitor_status("stale-container")
            .await
            .unwrap();
        assert_eq!(monitor.status, MonitorStatus::Failed);
    }
}

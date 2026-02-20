use crate::sync::error::{SyncError, SyncResult};
use crate::utils::console::ConsoleLogger;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Volume {
    pub name: String,
    pub driver: String,
    pub mount_point: String,
    pub labels: HashMap<String, String>,
    pub options: HashMap<String, String>,
    pub created_at: u64,
    pub updated_at: u64,
    pub status: VolumeStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum VolumeStatus {
    Active,
    Inactive,
    CleanupPending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mount {
    pub id: i64,
    pub container_id: String,
    pub source: String,
    pub target: String,
    pub mount_type: MountType,
    pub readonly: bool,
    pub options: HashMap<String, String>,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum MountType {
    Bind,
    Volume,
    Tmpfs,
}

pub struct VolumeManager {
    pool: SqlitePool,
    base_path: PathBuf,
}

impl VolumeManager {
    pub fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            base_path: PathBuf::from("/var/lib/quilt/volumes"),
        }
    }

    pub async fn initialize(&self) -> SyncResult<()> {
        // Ensure base volumes directory exists
        fs::create_dir_all(&self.base_path)
            .await
            .map_err(|e| SyncError::ValidationFailed {
                message: format!("Failed to create volumes directory: {}", e),
            })?;

        ConsoleLogger::info(&format!(
            "Volume manager initialized with base path: {:?}",
            self.base_path
        ));
        Ok(())
    }

    // Volume CRUD operations
    pub async fn create_volume(
        &self,
        name: &str,
        driver: Option<&str>,
        labels: HashMap<String, String>,
        options: HashMap<String, String>,
    ) -> SyncResult<Volume> {
        // Validate volume name
        if name.is_empty() || name.contains('/') {
            return Err(SyncError::ValidationFailed {
                message: "Invalid volume name".to_string(),
            });
        }

        // Check if volume already exists
        if self.get_volume(name).await?.is_some() {
            return Err(SyncError::ValidationFailed {
                message: format!("Volume '{}' already exists", name),
            });
        }

        let driver = driver.unwrap_or("local");
        let mount_point = self.base_path.join(name).to_string_lossy().to_string();
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Create volume directory
        fs::create_dir_all(&mount_point)
            .await
            .map_err(|e| SyncError::ValidationFailed {
                message: format!("Failed to create volume directory: {}", e),
            })?;

        // Insert into database
        let labels_json = serde_json::to_string(&labels).unwrap();
        let options_json = serde_json::to_string(&options).unwrap();

        sqlx::query(
            "INSERT INTO volumes (name, driver, mount_point, labels, options, created_at, updated_at, status) 
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(name)
        .bind(driver)
        .bind(&mount_point)
        .bind(&labels_json)
        .bind(&options_json)
        .bind(timestamp as i64)
        .bind(timestamp as i64)
        .bind("active")
        .execute(&self.pool)
        .await?;

        ConsoleLogger::success(&format!("Created volume '{}' at {}", name, mount_point));

        Ok(Volume {
            name: name.to_string(),
            driver: driver.to_string(),
            mount_point,
            labels,
            options,
            created_at: timestamp,
            updated_at: timestamp,
            status: VolumeStatus::Active,
        })
    }

    pub async fn get_volume(&self, name: &str) -> SyncResult<Option<Volume>> {
        let row = sqlx::query(
            "SELECT name, driver, mount_point, labels, options, created_at, updated_at, status 
             FROM volumes WHERE name = ?",
        )
        .bind(name)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(row) => {
                let labels: HashMap<String, String> =
                    serde_json::from_str(row.get("labels")).unwrap_or_default();
                let options: HashMap<String, String> =
                    serde_json::from_str(row.get("options")).unwrap_or_default();
                let status_str: String = row.get("status");

                Ok(Some(Volume {
                    name: row.get("name"),
                    driver: row.get("driver"),
                    mount_point: row.get("mount_point"),
                    labels,
                    options,
                    created_at: row.get::<i64, _>("created_at") as u64,
                    updated_at: row.get::<i64, _>("updated_at") as u64,
                    status: match status_str.as_str() {
                        "active" => VolumeStatus::Active,
                        "inactive" => VolumeStatus::Inactive,
                        "cleanup_pending" => VolumeStatus::CleanupPending,
                        _ => VolumeStatus::Inactive,
                    },
                }))
            }
            None => Ok(None),
        }
    }

    pub async fn list_volumes(
        &self,
        filters: Option<HashMap<String, String>>,
    ) -> SyncResult<Vec<Volume>> {
        let mut query = "SELECT name, driver, mount_point, labels, options, created_at, updated_at, status FROM volumes".to_string();

        // Apply filters if provided (filter by labels)
        if let Some(filters) = filters {
            if !filters.is_empty() {
                query.push_str(" WHERE 1=1");
                for (key, value) in filters {
                    query.push_str(&format!(
                        " AND json_extract(labels, '$.{}') = '{}'",
                        key, value
                    ));
                }
            }
        }

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;

        let mut volumes = Vec::new();
        for row in rows {
            let labels: HashMap<String, String> =
                serde_json::from_str(row.get("labels")).unwrap_or_default();
            let options: HashMap<String, String> =
                serde_json::from_str(row.get("options")).unwrap_or_default();
            let status_str: String = row.get("status");

            volumes.push(Volume {
                name: row.get("name"),
                driver: row.get("driver"),
                mount_point: row.get("mount_point"),
                labels,
                options,
                created_at: row.get::<i64, _>("created_at") as u64,
                updated_at: row.get::<i64, _>("updated_at") as u64,
                status: match status_str.as_str() {
                    "active" => VolumeStatus::Active,
                    "inactive" => VolumeStatus::Inactive,
                    "cleanup_pending" => VolumeStatus::CleanupPending,
                    _ => VolumeStatus::Inactive,
                },
            });
        }

        Ok(volumes)
    }

    pub async fn remove_volume(&self, name: &str, force: bool) -> SyncResult<()> {
        // Check if volume exists
        let volume = self
            .get_volume(name)
            .await?
            .ok_or_else(|| SyncError::NotFound {
                container_id: format!("volume:{}", name),
            })?;

        // Check if volume is in use by any container
        if !force {
            let in_use = sqlx::query_scalar::<_, i64>(
                "SELECT COUNT(*) FROM container_mounts WHERE source = ? AND mount_type = 'volume'",
            )
            .bind(name)
            .fetch_one(&self.pool)
            .await?;

            if in_use > 0 {
                return Err(SyncError::ValidationFailed {
                    message: format!("Volume '{}' is in use by {} container(s)", name, in_use),
                });
            }
        }

        // Mark volume for cleanup
        sqlx::query("UPDATE volumes SET status = 'cleanup_pending' WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await?;

        // Remove volume directory
        if let Err(e) = fs::remove_dir_all(&volume.mount_point).await {
            ConsoleLogger::warning(&format!("Failed to remove volume directory: {}", e));
        }

        // Delete from database
        sqlx::query("DELETE FROM volumes WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await?;

        ConsoleLogger::success(&format!("Removed volume '{}'", name));
        Ok(())
    }

    // Mount operations
    pub async fn add_mount(
        &self,
        container_id: &str,
        source: &str,
        target: &str,
        mount_type: MountType,
        readonly: bool,
        options: HashMap<String, String>,
    ) -> SyncResult<Mount> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let mount_type_str = match mount_type {
            MountType::Bind => "bind",
            MountType::Volume => "volume",
            MountType::Tmpfs => "tmpfs",
        };
        let options_json = serde_json::to_string(&options).unwrap();

        // For volume mounts, ensure the volume exists
        if mount_type == MountType::Volume {
            if self.get_volume(source).await?.is_none() {
                return Err(SyncError::NotFound {
                    container_id: format!("volume:{}", source),
                });
            }
        }

        let id = sqlx::query_scalar::<_, i64>(
            "INSERT INTO container_mounts (container_id, source, target, mount_type, readonly, options, created_at) 
             VALUES (?, ?, ?, ?, ?, ?, ?) RETURNING id"
        )
        .bind(container_id)
        .bind(source)
        .bind(target)
        .bind(mount_type_str)
        .bind(readonly)
        .bind(&options_json)
        .bind(timestamp as i64)
        .fetch_one(&self.pool)
        .await?;

        Ok(Mount {
            id,
            container_id: container_id.to_string(),
            source: source.to_string(),
            target: target.to_string(),
            mount_type,
            readonly,
            options,
            created_at: timestamp,
        })
    }

    pub async fn get_container_mounts(&self, container_id: &str) -> SyncResult<Vec<Mount>> {
        let rows = sqlx::query(
            "SELECT id, container_id, source, target, mount_type, readonly, options, created_at 
             FROM container_mounts WHERE container_id = ?",
        )
        .bind(container_id)
        .fetch_all(&self.pool)
        .await?;

        let mut mounts = Vec::new();
        for row in rows {
            let mount_type_str: String = row.get("mount_type");
            let options: HashMap<String, String> =
                serde_json::from_str(row.get("options")).unwrap_or_default();

            mounts.push(Mount {
                id: row.get("id"),
                container_id: row.get("container_id"),
                source: row.get("source"),
                target: row.get("target"),
                mount_type: match mount_type_str.as_str() {
                    "bind" => MountType::Bind,
                    "volume" => MountType::Volume,
                    "tmpfs" => MountType::Tmpfs,
                    _ => MountType::Bind,
                },
                readonly: row.get("readonly"),
                options,
                created_at: row.get::<i64, _>("created_at") as u64,
            });
        }

        Ok(mounts)
    }

    pub async fn remove_container_mounts(&self, container_id: &str) -> SyncResult<()> {
        sqlx::query("DELETE FROM container_mounts WHERE container_id = ?")
            .bind(container_id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    // Utility methods
    pub fn get_volume_path(&self, volume_name: &str) -> PathBuf {
        self.base_path.join(volume_name)
    }

    /// Clean up orphaned volumes that are no longer referenced by any containers
    pub async fn cleanup_orphaned_volumes(&self) -> SyncResult<u32> {
        // Find volumes marked for cleanup or not in use
        let orphaned = sqlx::query_scalar::<_, String>(
            "SELECT name FROM volumes WHERE status = 'cleanup_pending' 
             OR name NOT IN (SELECT DISTINCT source FROM container_mounts WHERE mount_type = 'volume')"
        )
        .fetch_all(&self.pool)
        .await?;

        let mut cleaned = 0;
        for volume_name in orphaned {
            if let Ok(()) = self.remove_volume(&volume_name, true).await {
                cleaned += 1;
            }
        }

        Ok(cleaned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::connection::ConnectionManager;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_volume_crud() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn_manager = ConnectionManager::new(db_path).await.unwrap();
        let volume_manager = VolumeManager::new(conn_manager.pool().clone());

        // Create volume
        let labels = HashMap::from([("env".to_string(), "test".to_string())]);
        let options = HashMap::new();
        let volume = volume_manager
            .create_volume("test-vol", None, labels, options)
            .await
            .unwrap();

        assert_eq!(volume.name, "test-vol");
        assert_eq!(volume.driver, "local");

        // Get volume
        let retrieved = volume_manager
            .get_volume("test-vol")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.name, "test-vol");

        // List volumes
        let volumes = volume_manager.list_volumes(None).await.unwrap();
        assert_eq!(volumes.len(), 1);

        // Remove volume
        volume_manager
            .remove_volume("test-vol", false)
            .await
            .unwrap();
        assert!(volume_manager
            .get_volume("test-vol")
            .await
            .unwrap()
            .is_none());

        conn_manager.close().await;
    }
}

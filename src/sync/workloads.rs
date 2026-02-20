use crate::sync::error::{SyncError, SyncResult};
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WorkloadSpec {
    pub replicas: u32,
    pub name: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub environment: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
    pub memory_limit_mb: Option<i64>,
    pub cpu_limit_percent: Option<f64>,
    pub strict: Option<bool>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Workload {
    pub id: String,
    pub cluster_id: String,
    pub tenant_id: String,
    pub name: String,
    pub spec_json: String,
    pub status_json: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Placement {
    pub id: String,
    pub workload_id: String,
    pub cluster_id: String,
    pub tenant_id: String,
    pub replica_index: i64,
    pub node_id: String,
    pub container_id: Option<String>,
    pub state: String,
    pub message: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

pub struct WorkloadManager {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl WorkloadManager {
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    pub async fn create_workload(
        &self,
        cluster_id: &str,
        tenant_id: &str,
        spec: &WorkloadSpec,
    ) -> SyncResult<Workload> {
        let now = now_ts();
        let id = uuid::Uuid::new_v4().to_string();

        if spec.replicas == 0 {
            return Err(SyncError::ValidationFailed {
                message: "replicas must be >= 1".to_string(),
            });
        }

        let spec_json = serde_json::to_string(spec).map_err(|e| SyncError::ValidationFailed {
            message: format!("invalid spec: {}", e),
        })?;
        let status_json = "{}".to_string();

        sqlx::query(
            r#"INSERT INTO workloads (id, cluster_id, tenant_id, name, spec_json, status_json, created_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&id)
        .bind(cluster_id)
        .bind(tenant_id)
        .bind(&spec.name)
        .bind(&spec_json)
        .bind(&status_json)
        .bind(now)
        .bind(now)
        .execute(&self.write_pool)
        .await
        .map_err(SyncError::Database)?;

        Ok(Workload {
            id,
            cluster_id: cluster_id.to_string(),
            tenant_id: tenant_id.to_string(),
            name: spec.name.clone(),
            spec_json,
            status_json,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn list_workloads(
        &self,
        cluster_id: &str,
        tenant_id: &str,
    ) -> SyncResult<Vec<Workload>> {
        let rows = sqlx::query(
            r#"SELECT id, cluster_id, tenant_id, name, spec_json, status_json, created_at, updated_at
               FROM workloads WHERE cluster_id = ? AND tenant_id = ? ORDER BY created_at DESC"#,
        )
        .bind(cluster_id)
        .bind(tenant_id)
        .fetch_all(&self.read_pool)
        .await
        .map_err(SyncError::Database)?;

        Ok(rows
            .into_iter()
            .map(|r| Workload {
                id: r.get::<String, _>("id"),
                cluster_id: r.get::<String, _>("cluster_id"),
                tenant_id: r.get::<String, _>("tenant_id"),
                name: r.get::<String, _>("name"),
                spec_json: r.get::<String, _>("spec_json"),
                status_json: r.get::<String, _>("status_json"),
                created_at: r.get::<i64, _>("created_at"),
                updated_at: r.get::<i64, _>("updated_at"),
            })
            .collect())
    }

    /// Minimal scheduler: ensure each workload has `replicas` placements assigned to ready nodes.
    ///
    /// Node selection is round-robin across `ready` nodes ordered by `created_at`.
    pub async fn reconcile_cluster(&self, cluster_id: &str, tenant_id: &str) -> SyncResult<()> {
        // Ready nodes
        let nodes = sqlx::query(
            r#"SELECT id FROM nodes
               WHERE cluster_id = ? AND tenant_id = ? AND state IN ('ready','registered')
               ORDER BY created_at ASC"#,
        )
        .bind(cluster_id)
        .bind(tenant_id)
        .fetch_all(&self.read_pool)
        .await
        .map_err(SyncError::Database)?;
        let node_ids: Vec<String> = nodes
            .into_iter()
            .map(|r| r.get::<String, _>("id"))
            .collect();
        let eligible: HashSet<String> = node_ids.iter().cloned().collect();

        // Workloads
        let wl_rows = sqlx::query(
            r#"SELECT id, spec_json FROM workloads WHERE cluster_id = ? AND tenant_id = ?"#,
        )
        .bind(cluster_id)
        .bind(tenant_id)
        .fetch_all(&self.read_pool)
        .await
        .map_err(SyncError::Database)?;

        for row in wl_rows {
            let workload_id: String = row.get::<String, _>("id");
            let spec_json: String = row.get::<String, _>("spec_json");
            let spec: WorkloadSpec =
                serde_json::from_str(&spec_json).map_err(|e| SyncError::ValidationFailed {
                    message: format!("invalid workload spec in DB: {}", e),
                })?;

            let desired = spec.replicas as i64;
            let now = now_ts();

            // Scale down: delete placements with replica_index >= desired.
            sqlx::query("DELETE FROM placements WHERE workload_id = ? AND replica_index >= ?")
                .bind(&workload_id)
                .bind(desired)
                .execute(&self.write_pool)
                .await
                .map_err(SyncError::Database)?;

            if desired <= 0 {
                continue;
            }

            // If we have no nodes, we can't create or reschedule assignments.
            if node_ids.is_empty() {
                continue;
            }

            // Ensure placements exist and are assigned to eligible nodes.
            for replica_index in 0..desired {
                let existing = sqlx::query(
                    "SELECT id, node_id FROM placements WHERE workload_id = ? AND replica_index = ?",
                )
                .bind(&workload_id)
                .bind(replica_index)
                .fetch_optional(&self.read_pool)
                .await
                .map_err(SyncError::Database)?;

                let target_node_idx = (replica_index as usize) % node_ids.len();
                let target_node_id = &node_ids[target_node_idx];

                match existing {
                    None => {
                        let placement_id = uuid::Uuid::new_v4().to_string();
                        sqlx::query(
                            r#"INSERT OR IGNORE INTO placements
                               (id, workload_id, cluster_id, tenant_id, replica_index, node_id, container_id, state, message, created_at, updated_at)
                               VALUES (?, ?, ?, ?, ?, ?, NULL, 'assigned', NULL, ?, ?)"#,
                        )
                        .bind(&placement_id)
                        .bind(&workload_id)
                        .bind(cluster_id)
                        .bind(tenant_id)
                        .bind(replica_index)
                        .bind(target_node_id)
                        .bind(now)
                        .bind(now)
                        .execute(&self.write_pool)
                        .await
                        .map_err(SyncError::Database)?;
                    }
                    Some(r) => {
                        let placement_id: String = r.get::<String, _>("id");
                        let current_node_id: String = r.get::<String, _>("node_id");
                        if !eligible.contains(&current_node_id) {
                            // Reschedule to an eligible node.
                            sqlx::query(
                                r#"UPDATE placements
                                   SET node_id = ?,
                                       state = 'assigned',
                                       message = ?,
                                       updated_at = ?
                                   WHERE id = ?"#,
                            )
                            .bind(target_node_id)
                            .bind(format!("rescheduled from node {}", current_node_id))
                            .bind(now)
                            .bind(&placement_id)
                            .execute(&self.write_pool)
                            .await
                            .map_err(SyncError::Database)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn report_placement(
        &self,
        placement_id: &str,
        node_id: &str,
        tenant_id: &str,
        container_id: Option<&str>,
        state: &str,
        message: Option<&str>,
    ) -> SyncResult<()> {
        let now = now_ts();
        let changed = sqlx::query(
            r#"UPDATE placements
               SET container_id = COALESCE(?, container_id),
                   state = ?,
                   message = ?,
                   updated_at = ?
               WHERE id = ? AND node_id = ? AND tenant_id = ?"#,
        )
        .bind(container_id)
        .bind(state)
        .bind(message)
        .bind(now)
        .bind(placement_id)
        .bind(node_id)
        .bind(tenant_id)
        .execute(&self.write_pool)
        .await
        .map_err(SyncError::Database)?
        .rows_affected();

        if changed == 0 {
            return Err(SyncError::NotFound {
                container_id: placement_id.to_string(),
            });
        }
        Ok(())
    }
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::password::hash_password;
    use crate::sync::connection::ConnectionManager;
    use crate::sync::nodes::{ClusterManager, NodeManager};
    use crate::sync::schema::SchemaManager;
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_reconcile_creates_round_robin_placements() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn_manager = ConnectionManager::new(db_path).await.unwrap();
        let schema_manager = SchemaManager::new(conn_manager.writer().clone());
        schema_manager.initialize_schema().await.unwrap();

        let cluster_mgr =
            ClusterManager::new(conn_manager.reader().clone(), conn_manager.writer().clone());
        let cluster = cluster_mgr
            .create_cluster("tenant-a", "c1", "10.42.0.0/16", 24)
            .await
            .unwrap();

        let node_mgr =
            NodeManager::new(conn_manager.reader().clone(), conn_manager.writer().clone());
        let (_n1, _a1) = node_mgr
            .register_node(
                &cluster,
                "node-1",
                None,
                None,
                None,
                HashMap::new(),
                "quilt0",
                53,
                0,
                &hash_password("token-1").unwrap(),
            )
            .await
            .unwrap();
        let (_n2, _a2) = node_mgr
            .register_node(
                &cluster,
                "node-2",
                None,
                None,
                None,
                HashMap::new(),
                "quilt0",
                53,
                0,
                &hash_password("token-2").unwrap(),
            )
            .await
            .unwrap();

        let mgr =
            WorkloadManager::new(conn_manager.reader().clone(), conn_manager.writer().clone());
        let spec = WorkloadSpec {
            replicas: 3,
            name: "echo".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "echo hi".to_string()],
            environment: HashMap::new(),
            labels: HashMap::new(),
            memory_limit_mb: None,
            cpu_limit_percent: None,
            strict: Some(false),
        };
        let wl = mgr
            .create_workload(&cluster.id, &cluster.tenant_id, &spec)
            .await
            .unwrap();

        mgr.reconcile_cluster(&cluster.id, &cluster.tenant_id)
            .await
            .unwrap();

        let placements =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(1) FROM placements WHERE workload_id = ?")
                .bind(&wl.id)
                .fetch_one(conn_manager.reader())
                .await
                .unwrap();
        assert_eq!(placements, 3);
    }

    #[tokio::test]
    async fn test_report_placement_updates_state() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn_manager = ConnectionManager::new(db_path).await.unwrap();
        let schema_manager = SchemaManager::new(conn_manager.writer().clone());
        schema_manager.initialize_schema().await.unwrap();

        let cluster_mgr = crate::sync::nodes::ClusterManager::new(
            conn_manager.reader().clone(),
            conn_manager.writer().clone(),
        );
        let cluster = cluster_mgr
            .create_cluster("tenant-a", "c1", "10.42.0.0/16", 24)
            .await
            .unwrap();

        let node_mgr = crate::sync::nodes::NodeManager::new(
            conn_manager.reader().clone(),
            conn_manager.writer().clone(),
        );
        let (node, _alloc) = node_mgr
            .register_node(
                &cluster,
                "node-1",
                None,
                None,
                None,
                HashMap::new(),
                "quilt0",
                53,
                0,
                &hash_password("token-1").unwrap(),
            )
            .await
            .unwrap();

        let mgr =
            WorkloadManager::new(conn_manager.reader().clone(), conn_manager.writer().clone());
        let spec = WorkloadSpec {
            replicas: 1,
            name: "w".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "true".to_string()],
            environment: HashMap::new(),
            labels: HashMap::new(),
            memory_limit_mb: None,
            cpu_limit_percent: None,
            strict: Some(false),
        };
        let wl = mgr
            .create_workload(&cluster.id, &cluster.tenant_id, &spec)
            .await
            .unwrap();
        mgr.reconcile_cluster(&cluster.id, &cluster.tenant_id)
            .await
            .unwrap();

        let placement_id: String =
            sqlx::query_scalar("SELECT id FROM placements WHERE workload_id = ? LIMIT 1")
                .bind(&wl.id)
                .fetch_one(conn_manager.reader())
                .await
                .unwrap();

        mgr.report_placement(
            &placement_id,
            &node.id,
            &cluster.tenant_id,
            Some("container-123"),
            "running",
            Some("ok"),
        )
        .await
        .unwrap();

        let row = sqlx::query("SELECT container_id, state, message FROM placements WHERE id = ?")
            .bind(&placement_id)
            .fetch_one(conn_manager.reader())
            .await
            .unwrap();
        let container_id: Option<String> = row.get("container_id");
        let state: String = row.get("state");
        let message: Option<String> = row.get("message");
        assert_eq!(container_id.as_deref(), Some("container-123"));
        assert_eq!(state, "running");
        assert_eq!(message.as_deref(), Some("ok"));
    }

    #[tokio::test]
    async fn test_reconcile_reschedules_from_deleted_node() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn_manager = ConnectionManager::new(db_path).await.unwrap();
        let schema_manager = SchemaManager::new(conn_manager.writer().clone());
        schema_manager.initialize_schema().await.unwrap();

        let cluster_mgr =
            ClusterManager::new(conn_manager.reader().clone(), conn_manager.writer().clone());
        let cluster = cluster_mgr
            .create_cluster("tenant-a", "c1", "10.42.0.0/16", 24)
            .await
            .unwrap();

        let node_mgr =
            NodeManager::new(conn_manager.reader().clone(), conn_manager.writer().clone());
        let (n1, _a1) = node_mgr
            .register_node(
                &cluster,
                "node-1",
                None,
                None,
                None,
                HashMap::new(),
                "quilt0",
                53,
                0,
                &hash_password("token-1").unwrap(),
            )
            .await
            .unwrap();
        let (n2, _a2) = node_mgr
            .register_node(
                &cluster,
                "node-2",
                None,
                None,
                None,
                HashMap::new(),
                "quilt0",
                53,
                0,
                &hash_password("token-2").unwrap(),
            )
            .await
            .unwrap();

        let mgr =
            WorkloadManager::new(conn_manager.reader().clone(), conn_manager.writer().clone());
        let spec = WorkloadSpec {
            replicas: 1,
            name: "w".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "true".to_string()],
            environment: HashMap::new(),
            labels: HashMap::new(),
            memory_limit_mb: None,
            cpu_limit_percent: None,
            strict: Some(false),
        };
        let wl = mgr
            .create_workload(&cluster.id, &cluster.tenant_id, &spec)
            .await
            .unwrap();
        mgr.reconcile_cluster(&cluster.id, &cluster.tenant_id)
            .await
            .unwrap();

        // Mark node-1 deleted; reconcile should move replica 0 to the only eligible node.
        sqlx::query("UPDATE nodes SET state = 'deleted' WHERE id = ?")
            .bind(&n1.id)
            .execute(conn_manager.writer())
            .await
            .unwrap();

        mgr.reconcile_cluster(&cluster.id, &cluster.tenant_id)
            .await
            .unwrap();

        let node_id: String = sqlx::query_scalar(
            "SELECT node_id FROM placements WHERE workload_id = ? AND replica_index = 0",
        )
        .bind(&wl.id)
        .fetch_one(conn_manager.reader())
        .await
        .unwrap();
        assert_eq!(node_id, n2.id);
    }

    #[tokio::test]
    async fn test_reconcile_scale_down_deletes_extra_placements() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn_manager = ConnectionManager::new(db_path).await.unwrap();
        let schema_manager = SchemaManager::new(conn_manager.writer().clone());
        schema_manager.initialize_schema().await.unwrap();

        let cluster_mgr =
            ClusterManager::new(conn_manager.reader().clone(), conn_manager.writer().clone());
        let cluster = cluster_mgr
            .create_cluster("tenant-a", "c1", "10.42.0.0/16", 24)
            .await
            .unwrap();

        let node_mgr =
            NodeManager::new(conn_manager.reader().clone(), conn_manager.writer().clone());
        let _ = node_mgr
            .register_node(
                &cluster,
                "node-1",
                None,
                None,
                None,
                HashMap::new(),
                "quilt0",
                53,
                0,
                &hash_password("token-1").unwrap(),
            )
            .await
            .unwrap();

        let mgr =
            WorkloadManager::new(conn_manager.reader().clone(), conn_manager.writer().clone());
        let mut spec = WorkloadSpec {
            replicas: 3,
            name: "w".to_string(),
            command: vec!["sh".to_string(), "-c".to_string(), "true".to_string()],
            environment: HashMap::new(),
            labels: HashMap::new(),
            memory_limit_mb: None,
            cpu_limit_percent: None,
            strict: Some(false),
        };
        let wl = mgr
            .create_workload(&cluster.id, &cluster.tenant_id, &spec)
            .await
            .unwrap();
        mgr.reconcile_cluster(&cluster.id, &cluster.tenant_id)
            .await
            .unwrap();

        // Scale down by updating spec JSON directly.
        spec.replicas = 1;
        let spec_json = serde_json::to_string(&spec).unwrap();
        sqlx::query("UPDATE workloads SET spec_json = ? WHERE id = ?")
            .bind(&spec_json)
            .bind(&wl.id)
            .execute(conn_manager.writer())
            .await
            .unwrap();

        mgr.reconcile_cluster(&cluster.id, &cluster.tenant_id)
            .await
            .unwrap();

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(1) FROM placements WHERE workload_id = ?")
                .bind(&wl.id)
                .fetch_one(conn_manager.reader())
                .await
                .unwrap();
        assert_eq!(count, 1);
    }
}

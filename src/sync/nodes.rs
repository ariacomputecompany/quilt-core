use crate::sync::error::{SyncError, SyncResult};
use crate::sync::ipam::allocate_next_pod_cidr;
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, serde::Serialize)]
pub struct Cluster {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub pod_cidr: String,
    pub node_cidr_prefix: i64,
    pub created_at: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct Node {
    pub id: String,
    pub cluster_id: String,
    pub tenant_id: String,
    pub name: String,
    pub public_ip: Option<String>,
    pub private_ip: Option<String>,
    pub state: String,
    pub last_heartbeat_at: Option<i64>,
    pub agent_version: Option<String>,
    pub labels_json: String,
    pub created_at: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct NodeAllocation {
    pub node_id: String,
    pub cluster_id: String,
    pub tenant_id: String,
    pub pod_cidr: String,
    pub bridge_name: String,
    pub dns_port: i64,
    pub egress_limit_mbit: i64,
    pub allocated_at: i64,
}

pub struct ClusterManager {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl ClusterManager {
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    pub async fn create_cluster(
        &self,
        tenant_id: &str,
        name: &str,
        pod_cidr: &str,
        node_cidr_prefix: i64,
    ) -> SyncResult<Cluster> {
        let now = now_ts();
        let id = uuid::Uuid::new_v4().to_string();

        if !(16..=30).contains(&(node_cidr_prefix as u8)) {
            return Err(SyncError::ValidationFailed {
                message: format!(
                    "invalid node_cidr_prefix {} (expected 16..=30)",
                    node_cidr_prefix
                ),
            });
        }

        sqlx::query(
            r#"INSERT INTO clusters (id, tenant_id, name, pod_cidr, node_cidr_prefix, created_at)
               VALUES (?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&id)
        .bind(tenant_id)
        .bind(name)
        .bind(pod_cidr)
        .bind(node_cidr_prefix)
        .bind(now)
        .execute(&self.write_pool)
        .await
        .map_err(|e| {
            // In case of concurrent creates, prefer a stable validation error.
            if let sqlx::Error::Database(db_err) = &e {
                if db_err.message().to_ascii_uppercase().contains("UNIQUE") {
                    return SyncError::ValidationFailed {
                        message: "cluster name already exists".to_string(),
                    };
                }
            }
            SyncError::Database(e)
        })?;

        Ok(Cluster {
            id,
            tenant_id: tenant_id.to_string(),
            name: name.to_string(),
            pod_cidr: pod_cidr.to_string(),
            node_cidr_prefix,
            created_at: now,
        })
    }

    pub async fn list_clusters_for_tenant(&self, tenant_id: &str) -> SyncResult<Vec<Cluster>> {
        let rows = sqlx::query(
            r#"SELECT id, tenant_id, name, pod_cidr, node_cidr_prefix, created_at
               FROM clusters WHERE tenant_id = ? ORDER BY created_at DESC"#,
        )
        .bind(tenant_id)
        .fetch_all(&self.read_pool)
        .await
        .map_err(SyncError::Database)?;

        Ok(rows
            .into_iter()
            .map(|r| Cluster {
                id: r.get::<String, _>("id"),
                tenant_id: r.get::<String, _>("tenant_id"),
                name: r.get::<String, _>("name"),
                pod_cidr: r.get::<String, _>("pod_cidr"),
                node_cidr_prefix: r.get::<i64, _>("node_cidr_prefix"),
                created_at: r.get::<i64, _>("created_at"),
            })
            .collect())
    }

    pub async fn get_cluster_for_tenant(
        &self,
        cluster_id: &str,
        tenant_id: &str,
    ) -> SyncResult<Cluster> {
        let row = sqlx::query(
            r#"SELECT id, tenant_id, name, pod_cidr, node_cidr_prefix, created_at
               FROM clusters WHERE id = ? AND tenant_id = ?"#,
        )
        .bind(cluster_id)
        .bind(tenant_id)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(SyncError::Database)?
        .ok_or_else(|| SyncError::NotFound {
            container_id: cluster_id.to_string(),
        })?;

        Ok(Cluster {
            id: row.get::<String, _>("id"),
            tenant_id: row.get::<String, _>("tenant_id"),
            name: row.get::<String, _>("name"),
            pod_cidr: row.get::<String, _>("pod_cidr"),
            node_cidr_prefix: row.get::<i64, _>("node_cidr_prefix"),
            created_at: row.get::<i64, _>("created_at"),
        })
    }

    /// Read a cluster by ID (used for agent registration paths where the caller is not yet
    /// tenant-authenticated, but cluster membership is still enforced by the bootstrap secret).
    pub async fn get_cluster_by_id(&self, cluster_id: &str) -> SyncResult<Cluster> {
        let row = sqlx::query(
            r#"SELECT id, tenant_id, name, pod_cidr, node_cidr_prefix, created_at
               FROM clusters WHERE id = ?"#,
        )
        .bind(cluster_id)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(SyncError::Database)?
        .ok_or_else(|| SyncError::NotFound {
            container_id: cluster_id.to_string(),
        })?;

        Ok(Cluster {
            id: row.get::<String, _>("id"),
            tenant_id: row.get::<String, _>("tenant_id"),
            name: row.get::<String, _>("name"),
            pod_cidr: row.get::<String, _>("pod_cidr"),
            node_cidr_prefix: row.get::<i64, _>("node_cidr_prefix"),
            created_at: row.get::<i64, _>("created_at"),
        })
    }
}

pub struct NodeManager {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl NodeManager {
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn register_node(
        &self,
        cluster: &Cluster,
        name: &str,
        public_ip: Option<&str>,
        private_ip: Option<&str>,
        agent_version: Option<&str>,
        labels: HashMap<String, String>,
        bridge_name: &str,
        dns_port: i64,
        egress_limit_mbit: i64,
        node_token_hash: &str,
    ) -> SyncResult<(Node, NodeAllocation)> {
        let now = now_ts();
        let node_id = uuid::Uuid::new_v4().to_string();

        // Find already allocated PodCIDRs for this cluster to carve the next one.
        let allocated_rows = sqlx::query(
            r#"SELECT pod_cidr FROM node_allocations WHERE cluster_id = ? ORDER BY allocated_at"#,
        )
        .bind(&cluster.id)
        .fetch_all(&self.read_pool)
        .await
        .map_err(SyncError::Database)?;

        let allocated: Vec<String> = allocated_rows
            .into_iter()
            .map(|r| r.get::<String, _>("pod_cidr"))
            .collect();

        let pod_cidr = allocate_next_pod_cidr(
            &cluster.pod_cidr,
            cluster.node_cidr_prefix as u8,
            &allocated,
        )
        .map_err(|e| SyncError::ValidationFailed { message: e })?;

        let labels_json = serde_json::to_string(&labels).unwrap_or_else(|_| "{}".to_string());

        let mut tx = self.write_pool.begin().await.map_err(SyncError::Database)?;

        sqlx::query(
            r#"INSERT INTO nodes
               (id, cluster_id, tenant_id, name, public_ip, private_ip, state, last_heartbeat_at, agent_version, labels_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&node_id)
        .bind(&cluster.id)
        .bind(&cluster.tenant_id)
        .bind(name)
        .bind(public_ip)
        .bind(private_ip)
        .bind("registered")
        .bind::<Option<i64>>(None)
        .bind(agent_version)
        .bind(&labels_json)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(SyncError::Database)?;

        sqlx::query(
            r#"INSERT INTO node_allocations
               (node_id, cluster_id, tenant_id, pod_cidr, bridge_name, dns_port, egress_limit_mbit, allocated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)"#,
        )
        .bind(&node_id)
        .bind(&cluster.id)
        .bind(&cluster.tenant_id)
        .bind(&pod_cidr)
        .bind(bridge_name)
        .bind(dns_port)
        .bind(egress_limit_mbit)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(SyncError::Database)?;

        sqlx::query(
            r#"INSERT INTO node_credentials (node_id, tenant_id, token_hash, issued_at, revoked_at)
               VALUES (?, ?, ?, ?, NULL)"#,
        )
        .bind(&node_id)
        .bind(&cluster.tenant_id)
        .bind(node_token_hash)
        .bind(now)
        .execute(&mut *tx)
        .await
        .map_err(SyncError::Database)?;

        tx.commit().await.map_err(SyncError::Database)?;

        let node = Node {
            id: node_id.clone(),
            cluster_id: cluster.id.clone(),
            tenant_id: cluster.tenant_id.clone(),
            name: name.to_string(),
            public_ip: public_ip.map(|s| s.to_string()),
            private_ip: private_ip.map(|s| s.to_string()),
            state: "registered".to_string(),
            last_heartbeat_at: None,
            agent_version: agent_version.map(|s| s.to_string()),
            labels_json,
            created_at: now,
        };

        let allocation = NodeAllocation {
            node_id,
            cluster_id: cluster.id.clone(),
            tenant_id: cluster.tenant_id.clone(),
            pod_cidr,
            bridge_name: bridge_name.to_string(),
            dns_port,
            egress_limit_mbit,
            allocated_at: now,
        };

        Ok((node, allocation))
    }

    pub async fn heartbeat(
        &self,
        node_id: &str,
        cluster_id: &str,
        tenant_id: &str,
        new_state: &str,
    ) -> SyncResult<()> {
        let now = now_ts();
        sqlx::query(
            r#"UPDATE nodes SET state = ?, last_heartbeat_at = ?
               WHERE id = ? AND cluster_id = ? AND tenant_id = ?"#,
        )
        .bind(new_state)
        .bind(now)
        .bind(node_id)
        .bind(cluster_id)
        .bind(tenant_id)
        .execute(&self.write_pool)
        .await
        .map_err(SyncError::Database)?;
        Ok(())
    }

    pub async fn list_nodes_for_cluster(
        &self,
        cluster_id: &str,
        tenant_id: &str,
    ) -> SyncResult<Vec<Node>> {
        let rows = sqlx::query(
            r#"SELECT id, cluster_id, tenant_id, name, public_ip, private_ip, state,
                      last_heartbeat_at, agent_version, labels_json, created_at
               FROM nodes WHERE cluster_id = ? AND tenant_id = ? ORDER BY created_at ASC"#,
        )
        .bind(cluster_id)
        .bind(tenant_id)
        .fetch_all(&self.read_pool)
        .await
        .map_err(SyncError::Database)?;

        Ok(rows
            .into_iter()
            .map(|r| Node {
                id: r.get::<String, _>("id"),
                cluster_id: r.get::<String, _>("cluster_id"),
                tenant_id: r.get::<String, _>("tenant_id"),
                name: r.get::<String, _>("name"),
                public_ip: r.get("public_ip"),
                private_ip: r.get("private_ip"),
                state: r.get::<String, _>("state"),
                last_heartbeat_at: r.get("last_heartbeat_at"),
                agent_version: r.get("agent_version"),
                labels_json: r.get::<String, _>("labels_json"),
                created_at: r.get::<i64, _>("created_at"),
            })
            .collect())
    }

    pub async fn get_node_for_cluster(
        &self,
        cluster_id: &str,
        tenant_id: &str,
        node_id: &str,
    ) -> SyncResult<Node> {
        let row = sqlx::query(
            r#"SELECT id, cluster_id, tenant_id, name, public_ip, private_ip, state,
                      last_heartbeat_at, agent_version, labels_json, created_at
               FROM nodes WHERE id = ? AND cluster_id = ? AND tenant_id = ?"#,
        )
        .bind(node_id)
        .bind(cluster_id)
        .bind(tenant_id)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(SyncError::Database)?
        .ok_or_else(|| SyncError::NotFound {
            container_id: node_id.to_string(),
        })?;

        Ok(Node {
            id: row.get::<String, _>("id"),
            cluster_id: row.get::<String, _>("cluster_id"),
            tenant_id: row.get::<String, _>("tenant_id"),
            name: row.get::<String, _>("name"),
            public_ip: row.get("public_ip"),
            private_ip: row.get("private_ip"),
            state: row.get::<String, _>("state"),
            last_heartbeat_at: row.get("last_heartbeat_at"),
            agent_version: row.get("agent_version"),
            labels_json: row.get::<String, _>("labels_json"),
            created_at: row.get::<i64, _>("created_at"),
        })
    }

    pub async fn set_node_state(
        &self,
        cluster_id: &str,
        tenant_id: &str,
        node_id: &str,
        new_state: &str,
    ) -> SyncResult<()> {
        let changed = sqlx::query(
            r#"UPDATE nodes SET state = ? WHERE id = ? AND cluster_id = ? AND tenant_id = ?"#,
        )
        .bind(new_state)
        .bind(node_id)
        .bind(cluster_id)
        .bind(tenant_id)
        .execute(&self.write_pool)
        .await
        .map_err(SyncError::Database)?
        .rows_affected();
        if changed == 0 {
            return Err(SyncError::NotFound {
                container_id: node_id.to_string(),
            });
        }
        Ok(())
    }

    pub async fn delete_node(
        &self,
        cluster_id: &str,
        tenant_id: &str,
        node_id: &str,
    ) -> SyncResult<()> {
        let mut tx = self.write_pool.begin().await.map_err(SyncError::Database)?;
        let changed = sqlx::query(
            r#"UPDATE nodes SET state = 'deleted' WHERE id = ? AND cluster_id = ? AND tenant_id = ?"#,
        )
        .bind(node_id)
        .bind(cluster_id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(SyncError::Database)?
        .rows_affected();
        if changed == 0 {
            return Err(SyncError::NotFound {
                container_id: node_id.to_string(),
            });
        }

        let now = now_ts();
        let _ = sqlx::query(
            r#"UPDATE node_credentials SET revoked_at = COALESCE(revoked_at, ?)
               WHERE node_id = ? AND tenant_id = ?"#,
        )
        .bind(now)
        .bind(node_id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .map_err(SyncError::Database)?;

        tx.commit().await.map_err(SyncError::Database)?;
        Ok(())
    }

    pub async fn get_allocation(
        &self,
        node_id: &str,
        tenant_id: &str,
    ) -> SyncResult<NodeAllocation> {
        let row = sqlx::query(
            r#"SELECT node_id, cluster_id, tenant_id, pod_cidr, bridge_name, dns_port,
                      egress_limit_mbit, allocated_at
               FROM node_allocations WHERE node_id = ? AND tenant_id = ?"#,
        )
        .bind(node_id)
        .bind(tenant_id)
        .fetch_optional(&self.read_pool)
        .await
        .map_err(SyncError::Database)?
        .ok_or_else(|| SyncError::NotFound {
            container_id: node_id.to_string(),
        })?;

        Ok(NodeAllocation {
            node_id: row.get::<String, _>("node_id"),
            cluster_id: row.get::<String, _>("cluster_id"),
            tenant_id: row.get::<String, _>("tenant_id"),
            pod_cidr: row.get::<String, _>("pod_cidr"),
            bridge_name: row.get::<String, _>("bridge_name"),
            dns_port: row.get::<i64, _>("dns_port"),
            egress_limit_mbit: row.get::<i64, _>("egress_limit_mbit"),
            allocated_at: row.get::<i64, _>("allocated_at"),
        })
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
    use crate::sync::schema::SchemaManager;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_register_node_allocates_unique_pod_cidrs() {
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
        let token_hash_1 = hash_password("token-1").unwrap();
        let token_hash_2 = hash_password("token-2").unwrap();

        let (_n1, a1) = node_mgr
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
                &token_hash_1,
            )
            .await
            .unwrap();

        let (_n2, a2) = node_mgr
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
                &token_hash_2,
            )
            .await
            .unwrap();

        assert_ne!(a1.pod_cidr, a2.pod_cidr);
    }
}

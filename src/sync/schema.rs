use crate::sync::error::SyncResult;
use sqlx::SqlitePool;

pub struct SchemaManager {
    pool: SqlitePool,
}

impl SchemaManager {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn initialize_schema(&self) -> SyncResult<()> {
        self.create_containers_table().await?;
        self.create_network_allocations_table().await?;
        self.create_network_state_table().await?;
        self.create_process_monitors_table().await?;
        self.create_container_logs_table().await?;
        self.create_cleanup_tasks_table().await?;
        self.create_volumes_table().await?;
        self.create_container_mounts_table().await?;
        self.create_container_metrics_table().await?;
        self.create_dns_entries_table().await?;
        self.create_tenants_table().await?;
        self.create_tenant_quotas_table().await?;
        self.create_storage_measurements_table().await?;
        self.create_master_containers_table().await?;
        self.create_terminal_sessions_table().await?;
        self.create_images_table().await?;
        self.create_image_layers_table().await?;
        self.create_icc_connections_table().await?;
        self.create_clusters_table().await?;
        self.create_nodes_table().await?;
        self.create_node_allocations_table().await?;
        self.create_node_credentials_table().await?;
        self.create_workloads_table().await?;
        self.create_placements_table().await?;
        self.run_schema_migrations().await?;
        self.create_indexes().await?;

        tracing::info!("Database schema initialized successfully");
        Ok(())
    }

    async fn create_containers_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS containers (
                id TEXT PRIMARY KEY,
                name TEXT,
                image_path TEXT NOT NULL,
                command TEXT NOT NULL,
                environment TEXT, -- JSON blob
                state TEXT CHECK(state IN ('created', 'starting', 'running', 'exited', 'error')) NOT NULL,
                exit_code INTEGER,
                pid INTEGER,
                rootfs_path TEXT,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                exited_at INTEGER,
                memory_limit_mb INTEGER,
                cpu_limit_percent REAL,
                
                -- Resource configuration
                enable_network_namespace BOOLEAN NOT NULL DEFAULT 1,
                enable_pid_namespace BOOLEAN NOT NULL DEFAULT 1,
                enable_mount_namespace BOOLEAN NOT NULL DEFAULT 1,
                enable_uts_namespace BOOLEAN NOT NULL DEFAULT 1,
                enable_ipc_namespace BOOLEAN NOT NULL DEFAULT 1,
                
                -- Metadata
                updated_at INTEGER NOT NULL
            )
        "#).execute(&self.pool).await?;

        Ok(())
    }

    async fn create_network_allocations_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS network_allocations (
                container_id TEXT PRIMARY KEY,
                ip_address TEXT NOT NULL,
                bridge_interface TEXT,
                veth_host TEXT,
                veth_container TEXT,
                allocation_time INTEGER NOT NULL,
                setup_completed BOOLEAN DEFAULT 0,
                status TEXT CHECK(status IN ('allocated', 'active', 'cleanup_pending', 'cleaned')) NOT NULL,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#).execute(&self.pool).await?;

        Ok(())
    }

    async fn create_network_state_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS network_state (
                key TEXT PRIMARY KEY,
                value TEXT,
                updated_at INTEGER NOT NULL
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_process_monitors_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS process_monitors (
                container_id TEXT PRIMARY KEY,
                pid INTEGER NOT NULL,
                monitor_started_at INTEGER NOT NULL,
                last_check_at INTEGER,
                status TEXT CHECK(status IN ('monitoring', 'completed', 'failed', 'aborted')) NOT NULL,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#).execute(&self.pool).await?;

        Ok(())
    }

    async fn create_container_logs_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS container_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                level TEXT CHECK(level IN ('debug', 'info', 'warn', 'error')) NOT NULL,
                message TEXT NOT NULL,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_cleanup_tasks_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS cleanup_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_id TEXT NOT NULL,
                resource_type TEXT CHECK(resource_type IN ('rootfs', 'network', 'cgroup', 'mounts', 'volumes')) NOT NULL,
                resource_path TEXT NOT NULL,
                status TEXT CHECK(status IN ('pending', 'in_progress', 'completed', 'failed')) NOT NULL,
                created_at INTEGER NOT NULL,
                completed_at INTEGER,
                error_message TEXT
            )
        "#).execute(&self.pool).await?;

        Ok(())
    }

    async fn create_volumes_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS volumes (
                name TEXT PRIMARY KEY,
                driver TEXT NOT NULL DEFAULT 'local',
                mount_point TEXT NOT NULL,
                labels TEXT, -- JSON blob
                options TEXT, -- JSON blob
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                status TEXT CHECK(status IN ('active', 'inactive', 'cleanup_pending')) NOT NULL
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_container_mounts_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS container_mounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_id TEXT NOT NULL,
                source TEXT NOT NULL, -- host path or volume name
                target TEXT NOT NULL, -- container path
                mount_type TEXT CHECK(mount_type IN ('bind', 'volume', 'tmpfs')) NOT NULL,
                readonly BOOLEAN NOT NULL DEFAULT 0,
                options TEXT, -- JSON blob for mount options
                created_at INTEGER NOT NULL,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_container_metrics_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS container_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_id TEXT NOT NULL,
                timestamp INTEGER NOT NULL,
                
                -- CPU metrics (microseconds)
                cpu_usage_usec INTEGER,
                cpu_user_usec INTEGER,
                cpu_system_usec INTEGER,
                cpu_throttled_usec INTEGER,
                
                -- Memory metrics (bytes)
                memory_current_bytes INTEGER,
                memory_peak_bytes INTEGER,
                memory_limit_bytes INTEGER,
                memory_cache_bytes INTEGER,
                memory_rss_bytes INTEGER,
                
                -- Network metrics
                network_rx_bytes INTEGER,
                network_tx_bytes INTEGER,
                network_rx_packets INTEGER,
                network_tx_packets INTEGER,
                network_rx_errors INTEGER,
                network_tx_errors INTEGER,
                
                -- Disk I/O metrics
                disk_read_bytes INTEGER,
                disk_write_bytes INTEGER,
                disk_read_ops INTEGER,
                disk_write_ops INTEGER,
                
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_indexes(&self) -> SyncResult<()> {
        // Performance indexes as specified in the documentation
        let indexes = [
            "CREATE INDEX IF NOT EXISTS idx_containers_state ON containers(state)",
            "CREATE INDEX IF NOT EXISTS idx_containers_updated_at ON containers(updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_network_allocations_status ON network_allocations(status)",
            "CREATE INDEX IF NOT EXISTS idx_network_allocations_ip ON network_allocations(ip_address)",
            "CREATE INDEX IF NOT EXISTS idx_process_monitors_status ON process_monitors(status)",
            "CREATE INDEX IF NOT EXISTS idx_process_monitors_pid ON process_monitors(pid)",
            "CREATE INDEX IF NOT EXISTS idx_container_logs_container_time ON container_logs(container_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_container_logs_level ON container_logs(level)",
            "CREATE INDEX IF NOT EXISTS idx_cleanup_tasks_status ON cleanup_tasks(status)",
            "CREATE INDEX IF NOT EXISTS idx_cleanup_tasks_container ON cleanup_tasks(container_id)",
            "CREATE INDEX IF NOT EXISTS idx_volumes_status ON volumes(status)",
            "CREATE INDEX IF NOT EXISTS idx_volumes_name ON volumes(name)",
            "CREATE INDEX IF NOT EXISTS idx_container_mounts_container ON container_mounts(container_id)",
            "CREATE INDEX IF NOT EXISTS idx_container_mounts_type ON container_mounts(mount_type)",
            "CREATE INDEX IF NOT EXISTS idx_container_metrics_container_time ON container_metrics(container_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_container_metrics_timestamp ON container_metrics(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_dns_entries_tenant ON dns_entries(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_master_containers_tenant ON master_containers(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_terminal_sessions_tenant ON terminal_sessions(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_terminal_sessions_state ON terminal_sessions(state)",
            "CREATE INDEX IF NOT EXISTS idx_images_tenant ON images(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_images_ref ON images(registry, repository, tag)",
            "CREATE INDEX IF NOT EXISTS idx_icc_connections_status ON icc_connections(status)",
            "CREATE INDEX IF NOT EXISTS idx_tenant_quotas_tenant ON tenant_quotas(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_storage_measurements_tenant_time ON storage_measurements(tenant_id, measured_at)",
            "CREATE INDEX IF NOT EXISTS idx_nodes_cluster_tenant ON nodes(cluster_id, tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_workloads_cluster_tenant ON workloads(cluster_id, tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_placements_workload ON placements(workload_id, replica_index)",
        ];

        for index_sql in indexes {
            sqlx::query(index_sql).execute(&self.pool).await?;
        }

        Ok(())
    }

    async fn create_tenants_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS tenants (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT CHECK(status IN ('active', 'suspended', 'deleted')) NOT NULL DEFAULT 'active',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        "#).execute(&self.pool).await?;

        Ok(())
    }

    async fn create_tenant_quotas_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS tenant_quotas (
                tenant_id TEXT PRIMARY KEY,
                storage_used_gb REAL NOT NULL DEFAULT 0.0,
                storage_quota_gb REAL NOT NULL DEFAULT 100.0,
                updated_at INTEGER NOT NULL
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_storage_measurements_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS storage_measurements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                measured_at INTEGER NOT NULL,
                volumes_gb REAL NOT NULL DEFAULT 0.0,
                containers_gb REAL NOT NULL DEFAULT 0.0,
                total_gb REAL NOT NULL DEFAULT 0.0,
                measurement_type TEXT NOT NULL DEFAULT 'reconciliation'
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_dns_entries_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS dns_entries (
                id TEXT PRIMARY KEY,
                container_id TEXT NOT NULL,
                container_name TEXT NOT NULL,
                ip_address TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                ttl INTEGER NOT NULL DEFAULT 300,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_master_containers_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS master_containers (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                container_id TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('created', 'starting', 'running', 'stopping', 'stopped', 'error')),
                hostname TEXT NOT NULL,
                ip_address TEXT,
                cpu_limit INTEGER NOT NULL,
                memory_limit_mb INTEGER NOT NULL,
                disk_limit_gb INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                stopped_at INTEGER,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#).execute(&self.pool).await?;

        Ok(())
    }

    async fn create_terminal_sessions_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS terminal_sessions (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                master_container_id TEXT NOT NULL,
                target TEXT NOT NULL CHECK(target IN ('master', 'container')),
                child_container_id TEXT,
                state TEXT NOT NULL CHECK(state IN ('ready', 'active', 'idle', 'terminated', 'error')),
                shell TEXT NOT NULL DEFAULT '/bin/bash',
                cols INTEGER NOT NULL DEFAULT 80,
                rows INTEGER NOT NULL DEFAULT 24,
                pid INTEGER,
                created_at INTEGER NOT NULL,
                last_activity_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                error_code TEXT,
                error_message TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error_at INTEGER,
                FOREIGN KEY(master_container_id) REFERENCES master_containers(id) ON DELETE CASCADE
            )
        "#).execute(&self.pool).await?;

        Ok(())
    }

    async fn create_images_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS images (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                repository TEXT NOT NULL,
                registry TEXT NOT NULL,
                tag TEXT,
                manifest_digest TEXT NOT NULL,
                config_digest TEXT NOT NULL,
                size_bytes INTEGER NOT NULL DEFAULT 0,
                architecture TEXT NOT NULL DEFAULT 'amd64',
                os TEXT NOT NULL DEFAULT 'linux',
                default_cmd TEXT,
                entrypoint TEXT,
                env TEXT,
                working_dir TEXT,
                exposed_ports TEXT,
                volumes TEXT,
                labels TEXT,
                user_spec TEXT,
                created_at INTEGER NOT NULL,
                last_used_at INTEGER NOT NULL,
                pulled_at INTEGER,
                ref_count INTEGER NOT NULL DEFAULT 1
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_image_layers_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS image_layers (
                digest TEXT PRIMARY KEY,
                size_bytes INTEGER NOT NULL,
                uncompressed_size INTEGER,
                media_type TEXT NOT NULL,
                extracted_path TEXT,
                ref_count INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL,
                last_used_at INTEGER NOT NULL
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS image_layer_mapping (
                image_id TEXT NOT NULL,
                layer_digest TEXT NOT NULL,
                layer_index INTEGER NOT NULL,
                PRIMARY KEY(image_id, layer_digest),
                FOREIGN KEY(image_id) REFERENCES images(id) ON DELETE CASCADE,
                FOREIGN KEY(layer_digest) REFERENCES image_layers(digest) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_icc_connections_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS icc_connections (
                connection_id TEXT PRIMARY KEY,
                from_container_id TEXT NOT NULL,
                to_container_id TEXT NOT NULL,
                protocol TEXT NOT NULL,
                port INTEGER NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                persistent BOOLEAN NOT NULL DEFAULT 1,
                auto_reconnect BOOLEAN NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL,
                last_health_check INTEGER,
                last_latency_us INTEGER,
                failure_count INTEGER NOT NULL DEFAULT 0,
                metadata TEXT NOT NULL DEFAULT '{}'
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_clusters_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS clusters (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                name TEXT NOT NULL,
                pod_cidr TEXT NOT NULL,
                node_cidr_prefix INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                UNIQUE(tenant_id, name)
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_nodes_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS nodes (
                id TEXT PRIMARY KEY,
                cluster_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                name TEXT NOT NULL,
                public_ip TEXT,
                private_ip TEXT,
                state TEXT NOT NULL CHECK(state IN ('registered', 'ready', 'draining', 'deleted')),
                last_heartbeat_at INTEGER,
                agent_version TEXT,
                labels_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                UNIQUE(cluster_id, tenant_id, name),
                FOREIGN KEY(cluster_id) REFERENCES clusters(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_node_allocations_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS node_allocations (
                node_id TEXT PRIMARY KEY,
                cluster_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                pod_cidr TEXT NOT NULL,
                bridge_name TEXT NOT NULL,
                dns_port INTEGER NOT NULL,
                egress_limit_mbit INTEGER NOT NULL DEFAULT 0,
                allocated_at INTEGER NOT NULL,
                FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE CASCADE,
                FOREIGN KEY(cluster_id) REFERENCES clusters(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_node_credentials_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS node_credentials (
                node_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                token_hash TEXT NOT NULL,
                issued_at INTEGER NOT NULL,
                revoked_at INTEGER,
                PRIMARY KEY(node_id, tenant_id),
                FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_workloads_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workloads (
                id TEXT PRIMARY KEY,
                cluster_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                name TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                status_json TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(cluster_id, tenant_id, name),
                FOREIGN KEY(cluster_id) REFERENCES clusters(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn create_placements_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS placements (
                id TEXT PRIMARY KEY,
                workload_id TEXT NOT NULL,
                cluster_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                replica_index INTEGER NOT NULL,
                node_id TEXT NOT NULL,
                container_id TEXT,
                state TEXT NOT NULL CHECK(state IN ('assigned', 'pulling', 'creating', 'running', 'failed', 'stopped')),
                message TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                UNIQUE(workload_id, replica_index),
                FOREIGN KEY(workload_id) REFERENCES workloads(id) ON DELETE CASCADE,
                FOREIGN KEY(node_id) REFERENCES nodes(id) ON DELETE CASCADE
            )
        "#).execute(&self.pool).await?;

        Ok(())
    }

    async fn run_schema_migrations(&self) -> SyncResult<()> {
        let migrations = [
            "ALTER TABLE containers ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default'",
            "ALTER TABLE containers ADD COLUMN project_id TEXT",
            "ALTER TABLE containers ADD COLUMN strict BOOLEAN NOT NULL DEFAULT 0",
            "ALTER TABLE containers ADD COLUMN labels TEXT DEFAULT '{}'",
            "ALTER TABLE containers ADD COLUMN rootfs_size_gb REAL NOT NULL DEFAULT 0.0",
            "ALTER TABLE containers ADD COLUMN rootfs_measured_at INTEGER",
            "ALTER TABLE volumes ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default'",
            "ALTER TABLE volumes ADD COLUMN actual_size_gb REAL NOT NULL DEFAULT 0.0",
            "ALTER TABLE volumes ADD COLUMN last_measured_at INTEGER",
            "ALTER TABLE network_allocations ADD COLUMN tenant_id TEXT",
            "ALTER TABLE process_monitors ADD COLUMN tenant_id TEXT",
            "ALTER TABLE cleanup_tasks ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default'",
        ];

        for migration in migrations {
            let _ = sqlx::query(migration).execute(&self.pool).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::connection::ConnectionManager;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_schema_creation() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn_manager = ConnectionManager::new(db_path).await.unwrap();
        let schema_manager = SchemaManager::new(conn_manager.pool().clone());

        schema_manager.initialize_schema().await.unwrap();

        // Verify tables exist
        let tables: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .fetch_all(conn_manager.pool())
                .await
                .unwrap();

        let table_names: Vec<String> = tables.into_iter().map(|(name,)| name).collect();

        assert!(table_names.contains(&"containers".to_string()));
        assert!(table_names.contains(&"network_allocations".to_string()));
        assert!(table_names.contains(&"process_monitors".to_string()));
        assert!(table_names.contains(&"volumes".to_string()));
        assert!(table_names.contains(&"container_mounts".to_string()));

        conn_manager.close().await;
    }
}

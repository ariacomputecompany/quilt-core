use crate::sync::error::SyncResult;
use sqlx::SqlitePool;

pub struct SchemaManager {
    write_pool: SqlitePool,
}

impl SchemaManager {
    pub fn new(write_pool: SqlitePool) -> Self {
        Self { write_pool }
    }

    pub async fn initialize_schema(&self) -> SyncResult<()> {
        // Core container tables
        self.create_containers_table().await?;
        self.create_network_allocations_table().await?;
        self.create_network_state_table().await?;
        self.create_process_monitors_table().await?;
        self.create_container_logs_table().await?;
        self.create_cleanup_tasks_table().await?;
        self.create_volumes_table().await?;
        self.create_container_mounts_table().await?;
        self.create_container_metrics_table().await?;
        self.create_projects_table().await?;
        self.create_usage_events_table().await?;
        self.create_dns_entries_table().await?;

        // User & auth tables
        self.create_tenants_table().await?;
        self.create_users_table().await?;
        self.create_auth_sessions_table().await?;
        self.create_user_activities_table().await?;
        self.create_admin_audit_log_table().await?;
        self.create_api_keys_table().await?;

        // Admin auth tables (separate from regular users)
        self.create_admin_accounts_table().await?;
        self.create_admin_sessions_table().await?;

        // Master container & terminal session tables
        self.create_master_containers_table().await?;
        self.create_terminal_sessions_table().await?;

        // Billing & subscription tables
        self.create_subscriptions_table().await?;
        self.create_payment_methods_table().await?;
        self.create_invoices_table().await?;
        self.create_invoice_items_table().await?;
        self.create_stripe_events_table().await?;
        self.create_tenant_quotas_table().await?;
        self.create_billing_grace_periods_table().await?;
        self.create_stripe_price_mappings_table().await?;

        // OCI Image tables
        self.create_images_table().await?;
        self.create_image_layers_table().await?;

        // ICC connection tracking
        self.create_icc_connections_table().await?;

        // Serverless functions tables
        self.create_functions_table().await?;
        self.create_function_versions_table().await?;
        self.create_function_invocations_table().await?;
        self.create_function_pool_table().await?;

        // Exec job tracking for detach mode
        self.create_exec_jobs_table().await?;

        // Port mapping for Browserbase networking
        self.create_port_mappings_table().await?;

        // Cluster-level primitives (control plane)
        self.create_clusters_table().await?;
        self.create_nodes_table().await?;
        self.create_node_allocations_table().await?;
        self.create_node_credentials_table().await?;
        self.create_cluster_join_tokens_table().await?;
        self.create_workloads_table().await?;
        self.create_placements_table().await?;

        // Schema migrations for existing databases
        self.run_schema_migrations().await?;

        // Indexes
        self.create_indexes().await?;

        tracing::info!("Database schema initialized successfully");
        Ok(())
    }

    async fn create_containers_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS containers (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                project_id TEXT,
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
                working_directory TEXT,

                -- Resource configuration
                enable_network_namespace BOOLEAN NOT NULL DEFAULT 1,
                enable_pid_namespace BOOLEAN NOT NULL DEFAULT 1,
                enable_mount_namespace BOOLEAN NOT NULL DEFAULT 1,
                enable_uts_namespace BOOLEAN NOT NULL DEFAULT 1,
                enable_ipc_namespace BOOLEAN NOT NULL DEFAULT 1,
                strict BOOLEAN NOT NULL DEFAULT 0,

                -- Storage tracking
                rootfs_size_gb REAL NOT NULL DEFAULT 0.0,
                rootfs_measured_at INTEGER,

                -- Lifecycle tracking
                generation INTEGER NOT NULL DEFAULT 0, -- Monotonically increasing per start, for monitor fencing

                -- Metadata
                labels TEXT DEFAULT '{}', -- JSON blob of key-value labels
                updated_at INTEGER NOT NULL
            )
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn create_network_allocations_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS network_allocations (
                container_id TEXT PRIMARY KEY,
                tenant_id TEXT,
                ip_address TEXT NOT NULL,
                bridge_interface TEXT,
                veth_host TEXT,
                veth_container TEXT,
                allocation_time INTEGER NOT NULL,
                setup_completed BOOLEAN DEFAULT 0,
                status TEXT CHECK(status IN ('allocated', 'active', 'cleanup_pending', 'cleaned')) NOT NULL,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        // Create index for tenant-based queries
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_network_allocations_tenant
            ON network_allocations(tenant_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

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
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_process_monitors_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS process_monitors (
                container_id TEXT PRIMARY KEY,
                tenant_id TEXT,
                pid INTEGER NOT NULL,
                monitor_started_at INTEGER NOT NULL,
                last_check_at INTEGER,
                check_count INTEGER NOT NULL DEFAULT 0,
                failure_count INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                status TEXT CHECK(status IN ('monitoring', 'completed', 'failed', 'aborted')) NOT NULL,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        // Create index on tenant_id for performance
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_process_monitors_tenant
            ON process_monitors(tenant_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        // Create index for monitoring queries (status + last_check_at for stale monitor cleanup)
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_process_monitors_status
            ON process_monitors(status, last_check_at)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

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
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_cleanup_tasks_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS cleanup_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL DEFAULT 'default',
                resource_type TEXT CHECK(resource_type IN ('rootfs', 'network', 'cgroup', 'mounts', 'volumes', 'container', 'logs')) NOT NULL,
                resource_path TEXT NOT NULL,
                status TEXT CHECK(status IN ('pending', 'in_progress', 'completed', 'failed')) NOT NULL,
                created_at INTEGER NOT NULL,
                started_at INTEGER,
                completed_at INTEGER,
                bytes_freed INTEGER,
                error_message TEXT,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn create_volumes_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS volumes (
                name TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                driver TEXT NOT NULL DEFAULT 'local',
                mount_point TEXT NOT NULL,
                labels TEXT, -- JSON blob
                options TEXT, -- JSON blob
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                status TEXT CHECK(status IN ('active', 'inactive', 'cleanup_pending')) NOT NULL,
                actual_size_gb REAL NOT NULL DEFAULT 0.0,
                last_measured_at INTEGER,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        // Create index for tenant queries
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_volumes_tenant ON volumes(tenant_id)
        "#,
        )
        .execute(&self.write_pool)
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
        .execute(&self.write_pool)
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
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_projects_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS projects (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                status TEXT CHECK(status IN ('active', 'inactive', 'deploying', 'error')) NOT NULL DEFAULT 'active',
                team TEXT,
                progress INTEGER DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        // Create index for tenant queries
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_projects_tenant ON projects(tenant_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_usage_events_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS usage_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tenant_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                container_id TEXT,
                timestamp INTEGER NOT NULL,
                duration_seconds INTEGER,
                memory_mb INTEGER,
                cpu_percent REAL,
                metadata TEXT DEFAULT '{}',
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE SET NULL
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        // Create indexes for queries
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_usage_events_tenant ON usage_events(tenant_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_usage_events_container ON usage_events(container_id)
        "#,
        )
        .execute(&self.write_pool)
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
        .execute(&self.write_pool)
        .await?;

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
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn create_users_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                name TEXT,
                phone TEXT,
                avatar_url TEXT,
                status TEXT CHECK(status IN ('active', 'suspended', 'deleted')) NOT NULL DEFAULT 'active',
                roles TEXT DEFAULT '["user"]',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                last_login_at INTEGER,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn create_auth_sessions_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS auth_sessions (
                session_id TEXT PRIMARY KEY,
                user_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                refresh_token_hash TEXT,
                ip_address TEXT,
                user_agent TEXT,
                created_at INTEGER NOT NULL,
                last_activity_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                revoked BOOLEAN NOT NULL DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_user_activities_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS user_activities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                activity_type TEXT CHECK(activity_type IN (
                    'login', 'logout', 'container_create', 'container_start',
                    'container_stop', 'container_delete', 'volume_create',
                    'volume_delete', 'api_key_create', 'api_key_delete',
                    'settings_update', 'password_change', 'profile_update'
                )) NOT NULL,
                resource_type TEXT,
                resource_id TEXT,
                ip_address TEXT,
                user_agent TEXT,
                metadata TEXT DEFAULT '{}',
                timestamp INTEGER NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_admin_audit_log_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS admin_audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_user_id TEXT NOT NULL,
                admin_email TEXT NOT NULL,
                action TEXT NOT NULL,
                target_user_id TEXT,
                target_tenant_id TEXT,
                ip_address TEXT,
                request_path TEXT,
                request_method TEXT,
                details TEXT DEFAULT '{}',
                timestamp INTEGER NOT NULL
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        // Create indexes
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_admin_audit_log_admin ON admin_audit_log(admin_user_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(r#"
            CREATE INDEX IF NOT EXISTS idx_admin_audit_log_tenant ON admin_audit_log(target_tenant_id)
        "#).execute(&self.write_pool).await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_admin_audit_log_created ON admin_audit_log(timestamp)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_api_keys_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS api_keys (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                name TEXT NOT NULL,
                key TEXT NOT NULL,
                prefix TEXT NOT NULL,

                -- Scope fields
                scope_type TEXT CHECK(scope_type IN ('full', 'container', 'project')) NOT NULL DEFAULT 'full',
                scope_resource_ids TEXT,  -- JSON array of container/project IDs
                permissions TEXT,          -- JSON array of permission strings

                -- Timestamps
                created_at INTEGER NOT NULL,
                last_used_at INTEGER,
                expires_at INTEGER,

                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        // Create indexes for lookups
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys(tenant_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_api_keys_prefix ON api_keys(prefix)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    /// Create admin_accounts table - SEPARATE from regular users table
    /// Admin accounts are stored independently for security isolation
    async fn create_admin_accounts_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS admin_accounts (
                id TEXT PRIMARY KEY,
                email TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                name TEXT,
                phone TEXT,

                -- MFA/2FA support
                mfa_secret TEXT,
                mfa_enabled BOOLEAN NOT NULL DEFAULT 0,
                mfa_backup_codes TEXT,  -- JSON array of hashed backup codes

                -- Account status
                status TEXT CHECK(status IN ('active', 'suspended', 'deleted')) NOT NULL DEFAULT 'active',

                -- Permissions (JSON array)
                permissions TEXT DEFAULT '["all"]',

                -- Security tracking
                failed_login_attempts INTEGER NOT NULL DEFAULT 0,
                locked_until INTEGER,  -- Unix timestamp when lockout expires
                last_failed_login_at INTEGER,
                password_changed_at INTEGER,

                -- Timestamps
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                last_login_at INTEGER,

                -- Audit
                created_by TEXT  -- Admin who created this account (NULL for initial seed)
            )
        "#).execute(&self.write_pool).await?;

        // Create indexes for admin accounts
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_admin_accounts_email ON admin_accounts(email)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_admin_accounts_status ON admin_accounts(status)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    /// Create admin_sessions table - SEPARATE from regular user sessions
    /// Admin sessions have shorter expiry and stricter security
    async fn create_admin_sessions_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS admin_sessions (
                session_id TEXT PRIMARY KEY,
                admin_id TEXT NOT NULL,
                refresh_token_hash TEXT,

                -- Security context
                ip_address TEXT,
                user_agent TEXT,

                -- MFA verification status for this session
                mfa_verified BOOLEAN NOT NULL DEFAULT 0,
                mfa_verified_at INTEGER,

                -- Timestamps
                created_at INTEGER NOT NULL,
                last_activity_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,

                -- Revocation
                revoked BOOLEAN NOT NULL DEFAULT 0,
                revoked_at INTEGER,
                revoked_reason TEXT,

                FOREIGN KEY(admin_id) REFERENCES admin_accounts(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        // Create indexes for admin sessions
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_admin_sessions_admin ON admin_sessions(admin_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_admin_sessions_expires ON admin_sessions(expires_at)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_admin_sessions_revoked ON admin_sessions(revoked)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_master_containers_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS master_containers (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
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
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        // Create indexes for common queries
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_master_containers_tenant
            ON master_containers(tenant_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_master_containers_state
            ON master_containers(state)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_master_containers_container_id
            ON master_containers(container_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_terminal_sessions_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS terminal_sessions (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                master_container_id TEXT NOT NULL,
                target TEXT NOT NULL CHECK(target IN ('master', 'container')),
                child_container_id TEXT,
                state TEXT NOT NULL CHECK(state IN ('ready', 'active', 'idle', 'terminated', 'error')),
                shell TEXT NOT NULL,
                cols INTEGER NOT NULL,
                rows INTEGER NOT NULL,
                pid INTEGER,
                created_at INTEGER NOT NULL,
                last_activity_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                error_code TEXT,
                error_message TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                last_error_at INTEGER,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
                FOREIGN KEY(master_container_id) REFERENCES master_containers(id) ON DELETE CASCADE,
                FOREIGN KEY(child_container_id) REFERENCES containers(id) ON DELETE SET NULL
            )
        "#).execute(&self.write_pool).await?;

        // Create indexes for common queries
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_terminal_sessions_tenant
            ON terminal_sessions(tenant_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_terminal_sessions_state
            ON terminal_sessions(state)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_terminal_sessions_master_container
            ON terminal_sessions(master_container_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_terminal_sessions_child_container
            ON terminal_sessions(child_container_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_terminal_sessions_expires_at
            ON terminal_sessions(expires_at)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_terminal_sessions_last_activity
            ON terminal_sessions(last_activity_at)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_subscriptions_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS subscriptions (
                id TEXT PRIMARY KEY,
                tenant_id TEXT UNIQUE NOT NULL,
                plan_tier TEXT CHECK(plan_tier IN ('none', 'starter', 'professional', 'business', 'enterprise')) NOT NULL DEFAULT 'none',
                status TEXT CHECK(status IN ('browse_only', 'active', 'past_due', 'canceled', 'unpaid')) NOT NULL DEFAULT 'browse_only',
                billing_cycle TEXT CHECK(billing_cycle IN ('monthly', 'yearly')),
                price_cents INTEGER,

                -- Resource limits based on plan tier
                max_memory_gb INTEGER NOT NULL DEFAULT 0,
                max_containers INTEGER, -- NULL = unlimited
                max_storage_gb INTEGER NOT NULL DEFAULT 1,
                max_egress_gb INTEGER NOT NULL DEFAULT 1,

                -- Stripe IDs
                stripe_customer_id TEXT,
                stripe_subscription_id TEXT UNIQUE,
                stripe_price_id TEXT,

                -- Timestamps
                created_at INTEGER NOT NULL,
                current_period_start INTEGER,
                current_period_end INTEGER,
                canceled_at INTEGER,
                updated_at INTEGER NOT NULL,

                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn create_payment_methods_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS payment_methods (
                id TEXT PRIMARY KEY, -- Stripe PaymentMethod ID
                tenant_id TEXT NOT NULL,
                stripe_customer_id TEXT NOT NULL,
                type TEXT NOT NULL, -- 'card', 'bank_account', etc.
                last_four TEXT,
                brand TEXT, -- 'visa', 'mastercard', etc.
                exp_month INTEGER,
                exp_year INTEGER,
                is_default BOOLEAN NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_invoices_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS invoices (
                id TEXT PRIMARY KEY, -- Stripe Invoice ID
                tenant_id TEXT NOT NULL,
                stripe_customer_id TEXT NOT NULL,
                stripe_subscription_id TEXT,
                invoice_number TEXT,
                amount_cents INTEGER NOT NULL,
                amount_paid_cents INTEGER,
                status TEXT CHECK(status IN ('draft', 'open', 'paid', 'void', 'uncollectible')) NOT NULL,
                period_start INTEGER,
                period_end INTEGER,
                due_at INTEGER,
                paid_at INTEGER,
                pdf_url TEXT,
                hosted_invoice_url TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn create_invoice_items_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS invoice_items (
                id TEXT PRIMARY KEY, -- Stripe InvoiceItem ID
                invoice_id TEXT NOT NULL,
                description TEXT NOT NULL,
                quantity INTEGER NOT NULL DEFAULT 1,
                unit_price_cents INTEGER NOT NULL,
                amount_cents INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(invoice_id) REFERENCES invoices(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_stripe_events_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS stripe_events (
                event_id TEXT PRIMARY KEY,
                event_type TEXT NOT NULL,
                tenant_id TEXT,
                payload TEXT NOT NULL, -- JSON blob
                processing_status TEXT CHECK(processing_status IN ('pending', 'processed', 'failed')) NOT NULL DEFAULT 'pending',
                error_message TEXT,
                created_at INTEGER NOT NULL,
                processed_at INTEGER,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        // Create indexes
        sqlx::query(
            r#"
            CREATE INDEX IF NOT EXISTS idx_stripe_events_tenant ON stripe_events(tenant_id)
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        sqlx::query(r#"
            CREATE INDEX IF NOT EXISTS idx_stripe_events_processed ON stripe_events(processing_status)
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn create_tenant_quotas_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS tenant_quotas (
                tenant_id TEXT PRIMARY KEY,

                -- Current usage
                containers_current INTEGER NOT NULL DEFAULT 0,
                memory_used_mb INTEGER NOT NULL DEFAULT 0,
                storage_used_gb REAL NOT NULL DEFAULT 0,
                egress_gb REAL NOT NULL DEFAULT 0,

                -- Limits (from subscription)
                max_containers INTEGER, -- NULL = unlimited
                max_memory_gb INTEGER NOT NULL DEFAULT 0,
                max_storage_gb INTEGER NOT NULL DEFAULT 1,
                max_egress_gb INTEGER NOT NULL DEFAULT 1,

                -- Enforcement flags
                containers_blocked BOOLEAN NOT NULL DEFAULT 0,
                memory_blocked BOOLEAN NOT NULL DEFAULT 0,
                storage_blocked BOOLEAN NOT NULL DEFAULT 0,
                egress_blocked BOOLEAN NOT NULL DEFAULT 0,

                -- Period tracking (for egress reset)
                period_start INTEGER NOT NULL,
                period_end INTEGER NOT NULL,

                updated_at INTEGER NOT NULL,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_billing_grace_periods_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS billing_grace_periods (
                tenant_id TEXT PRIMARY KEY,
                invoice_id TEXT NOT NULL,
                expires_at INTEGER NOT NULL,
                suspended BOOLEAN NOT NULL DEFAULT 0,
                notified BOOLEAN NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_images_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS images (
                id TEXT PRIMARY KEY,  -- Content digest (sha256:...)
                tenant_id TEXT NOT NULL DEFAULT 'default',
                repository TEXT NOT NULL,  -- e.g., "library/nginx"
                registry TEXT NOT NULL DEFAULT 'docker.io',  -- e.g., "docker.io", "ghcr.io"
                tag TEXT,  -- e.g., "latest", "1.25"
                manifest_digest TEXT NOT NULL,  -- Manifest content hash
                config_digest TEXT NOT NULL,  -- Config blob digest
                size_bytes INTEGER NOT NULL,  -- Total uncompressed size
                architecture TEXT NOT NULL DEFAULT 'amd64',
                os TEXT NOT NULL DEFAULT 'linux',

                -- Image config data (cached from config blob)
                default_cmd TEXT,  -- JSON array
                entrypoint TEXT,  -- JSON array
                env TEXT,  -- JSON array of KEY=VALUE strings
                working_dir TEXT,
                exposed_ports TEXT,  -- JSON object
                volumes TEXT,  -- JSON object
                labels TEXT,  -- JSON object
                user_spec TEXT,  -- User to run as (e.g., "1000:1000")

                -- Timestamps
                created_at INTEGER NOT NULL,
                last_used_at INTEGER NOT NULL,
                pulled_at INTEGER,

                -- Reference count for garbage collection
                ref_count INTEGER NOT NULL DEFAULT 1
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_image_layers_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS image_layers (
                digest TEXT PRIMARY KEY,  -- Layer content hash (sha256:...)
                size_bytes INTEGER NOT NULL,  -- Compressed size
                uncompressed_size INTEGER,  -- Uncompressed size (if known)
                media_type TEXT NOT NULL,  -- e.g., "application/vnd.oci.image.layer.v1.tar+gzip"
                extracted_path TEXT,  -- Path where layer is extracted

                -- Reference counting for deduplication
                ref_count INTEGER NOT NULL DEFAULT 1,

                -- Timestamps
                created_at INTEGER NOT NULL,
                last_used_at INTEGER NOT NULL
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        // Image-to-layer mapping table (many-to-many)
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS image_layer_mapping (
                image_id TEXT NOT NULL,
                layer_digest TEXT NOT NULL,
                layer_index INTEGER NOT NULL,  -- Order of layer in image
                PRIMARY KEY (image_id, layer_digest),
                FOREIGN KEY(image_id) REFERENCES images(id) ON DELETE CASCADE,
                FOREIGN KEY(layer_digest) REFERENCES image_layers(digest) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_stripe_price_mappings_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS stripe_price_mappings (
                price_id TEXT PRIMARY KEY,
                plan_tier TEXT NOT NULL CHECK(plan_tier IN ('free', 'starter', 'professional', 'business', 'enterprise')),
                billing_cycle TEXT NOT NULL CHECK(billing_cycle IN ('monthly', 'yearly')),
                is_active BOOLEAN NOT NULL DEFAULT 1,
                description TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    // ========================================================================
    // Serverless Functions Tables
    // ========================================================================

    async fn create_functions_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS functions (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                handler TEXT NOT NULL,
                runtime TEXT CHECK(runtime IN ('custom', 'python3', 'nodejs', 'shell')) NOT NULL DEFAULT 'shell',
                state TEXT CHECK(state IN ('pending', 'deploying', 'active', 'paused', 'error', 'deleting')) NOT NULL DEFAULT 'pending',
                current_version INTEGER NOT NULL DEFAULT 1,

                -- Resource limits
                memory_limit_mb INTEGER NOT NULL DEFAULT 128,
                cpu_limit_percent REAL NOT NULL DEFAULT 25.0,
                timeout_seconds INTEGER NOT NULL DEFAULT 30,

                -- Environment configuration (JSON)
                environment TEXT DEFAULT '{}',

                -- Pool configuration
                min_instances INTEGER NOT NULL DEFAULT 0,
                max_instances INTEGER NOT NULL DEFAULT 10,
                cleanup_on_exit BOOLEAN NOT NULL DEFAULT 1,
                working_directory TEXT,

                -- Timestamps
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                last_invoked_at INTEGER,

                -- Metrics
                invocation_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                error_message TEXT,

                -- Constraints
                UNIQUE(tenant_id, name),
                FOREIGN KEY(tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn create_function_versions_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS function_versions (
                id TEXT PRIMARY KEY,
                function_id TEXT NOT NULL,
                version INTEGER NOT NULL,
                handler TEXT NOT NULL,
                runtime TEXT NOT NULL,
                environment TEXT DEFAULT '{}',
                created_at INTEGER NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT 0,
                description TEXT,

                UNIQUE(function_id, version),
                FOREIGN KEY(function_id) REFERENCES functions(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    async fn create_function_invocations_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS function_invocations (
                id TEXT PRIMARY KEY,
                function_id TEXT NOT NULL,
                container_id TEXT NOT NULL,
                status TEXT CHECK(status IN ('pending', 'running', 'success', 'failed', 'timeout', 'cancelled')) NOT NULL DEFAULT 'pending',

                -- Timing
                started_at INTEGER NOT NULL,
                ended_at INTEGER,
                duration_ms INTEGER,

                -- Results
                exit_code INTEGER,
                stdout TEXT,
                stderr TEXT,
                error_message TEXT,

                -- Metadata
                cold_start BOOLEAN NOT NULL DEFAULT 0,

                FOREIGN KEY(function_id) REFERENCES functions(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn create_function_pool_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS function_pool (
                id TEXT PRIMARY KEY,
                function_id TEXT NOT NULL,
                container_id TEXT NOT NULL,
                state TEXT CHECK(state IN ('warming', 'ready', 'busy', 'recycling', 'terminating')) NOT NULL DEFAULT 'warming',

                -- Timestamps
                created_at INTEGER NOT NULL,
                last_used_at INTEGER,

                -- Usage tracking
                invocation_count INTEGER NOT NULL DEFAULT 0,
                version INTEGER NOT NULL,

                FOREIGN KEY(function_id) REFERENCES functions(id) ON DELETE CASCADE,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn run_schema_migrations(&self) -> SyncResult<()> {
        // Add labels column to containers table if it doesn't exist (added for container metadata support)
        let _ = sqlx::query("ALTER TABLE containers ADD COLUMN labels TEXT DEFAULT '{}'")
            .execute(&self.write_pool)
            .await; // Ignore error if column already exists

        // Add generation column for monitor fencing (prevents stale monitors from overwriting state)
        let _ =
            sqlx::query("ALTER TABLE containers ADD COLUMN generation INTEGER NOT NULL DEFAULT 0")
                .execute(&self.write_pool)
                .await; // Ignore error if column already exists

        // Add error_reason column for detailed error tracking
        let _ = sqlx::query("ALTER TABLE containers ADD COLUMN error_reason TEXT")
            .execute(&self.write_pool)
            .await; // Ignore error if column already exists

        // Add error_timestamp column for when the error occurred
        let _ = sqlx::query("ALTER TABLE containers ADD COLUMN error_timestamp INTEGER")
            .execute(&self.write_pool)
            .await; // Ignore error if column already exists

        // Add strict isolation mode column
        let _ = sqlx::query("ALTER TABLE containers ADD COLUMN strict BOOLEAN NOT NULL DEFAULT 0")
            .execute(&self.write_pool)
            .await; // Ignore error if column already exists

        Ok(())
    }

    async fn create_icc_connections_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS icc_connections (
                connection_id TEXT PRIMARY KEY,
                from_container_id TEXT NOT NULL,
                to_container_id TEXT NOT NULL,
                protocol TEXT CHECK(protocol IN ('tcp', 'udp', 'http')) NOT NULL,
                port INTEGER NOT NULL,
                status TEXT CHECK(status IN ('active', 'disconnected', 'failed')) NOT NULL DEFAULT 'active',
                persistent BOOLEAN NOT NULL DEFAULT 0,
                auto_reconnect BOOLEAN NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                last_health_check INTEGER,
                last_latency_us INTEGER,
                failure_count INTEGER NOT NULL DEFAULT 0,
                metadata TEXT DEFAULT '{}',
                FOREIGN KEY(from_container_id) REFERENCES containers(id) ON DELETE CASCADE,
                FOREIGN KEY(to_container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        Ok(())
    }

    async fn create_exec_jobs_table(&self) -> SyncResult<()> {
        sqlx::query(r#"
            CREATE TABLE IF NOT EXISTS exec_jobs (
                id TEXT PRIMARY KEY,
                container_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                command TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'running' CHECK(status IN ('running', 'completed', 'failed', 'timeout')),
                exit_code INTEGER,
                pid INTEGER,
                started_at INTEGER NOT NULL,
                completed_at INTEGER,
                output_path TEXT,
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#).execute(&self.write_pool).await?;

        // Create indexes for efficient queries
        let _ = sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_exec_jobs_container ON exec_jobs(container_id)",
        )
        .execute(&self.write_pool)
        .await;
        let _ =
            sqlx::query("CREATE INDEX IF NOT EXISTS idx_exec_jobs_tenant ON exec_jobs(tenant_id)")
                .execute(&self.write_pool)
                .await;
        let _ = sqlx::query("CREATE INDEX IF NOT EXISTS idx_exec_jobs_status ON exec_jobs(status)")
            .execute(&self.write_pool)
            .await;

        Ok(())
    }

    async fn create_port_mappings_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS port_mappings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                container_id TEXT NOT NULL,
                container_ip TEXT NOT NULL,
                container_port INTEGER NOT NULL,
                host_port INTEGER NOT NULL UNIQUE,
                protocol TEXT NOT NULL DEFAULT 'tcp' CHECK(protocol IN ('tcp', 'udp')),
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                FOREIGN KEY(container_id) REFERENCES containers(id) ON DELETE CASCADE
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        let _ = sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_port_mappings_container ON port_mappings(container_id)",
        )
        .execute(&self.write_pool)
        .await;
        let _ = sqlx::query("CREATE UNIQUE INDEX IF NOT EXISTS idx_port_mappings_host_port ON port_mappings(host_port)")
            .execute(&self.write_pool).await;

        Ok(())
    }

    async fn create_clusters_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS clusters (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                name TEXT NOT NULL,
                pod_cidr TEXT NOT NULL,
                node_cidr_prefix INTEGER NOT NULL DEFAULT 24,
                created_at INTEGER NOT NULL
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }

    async fn create_nodes_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS nodes (
                id TEXT PRIMARY KEY,
                cluster_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                name TEXT NOT NULL,
                public_ip TEXT,
                private_ip TEXT,
                state TEXT CHECK(state IN ('registered','ready','not_ready','draining','deleted')) NOT NULL,
                last_heartbeat_at INTEGER,
                agent_version TEXT,
                labels_json TEXT NOT NULL DEFAULT '{}',
                taints_json TEXT NOT NULL DEFAULT '[]',
                created_at INTEGER NOT NULL
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }

    async fn create_node_allocations_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS node_allocations (
                node_id TEXT PRIMARY KEY,
                cluster_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                pod_cidr TEXT NOT NULL,
                bridge_name TEXT NOT NULL,
                dns_port INTEGER NOT NULL,
                egress_limit_mbit INTEGER NOT NULL,
                allocated_at INTEGER NOT NULL
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }

    async fn create_node_credentials_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS node_credentials (
                node_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                token_hash TEXT NOT NULL,
                issued_at INTEGER NOT NULL,
                revoked_at INTEGER
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }

    async fn create_cluster_join_tokens_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS cluster_join_tokens (
                id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                cluster_id TEXT NOT NULL,
                token_hash TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                max_uses INTEGER NOT NULL,
                uses INTEGER NOT NULL DEFAULT 0,
                last_used_at INTEGER,
                revoked_at INTEGER,
                UNIQUE(cluster_id, token_hash)
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;

        let _ = sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_cluster_join_tokens_cluster ON cluster_join_tokens(cluster_id)",
        )
        .execute(&self.write_pool)
        .await;
        let _ = sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_cluster_join_tokens_hash ON cluster_join_tokens(cluster_id, token_hash)",
        )
        .execute(&self.write_pool)
        .await;

        Ok(())
    }

    async fn create_workloads_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS workloads (
                id TEXT PRIMARY KEY,
                cluster_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                name TEXT NOT NULL,
                spec_json TEXT NOT NULL,
                status_json TEXT NOT NULL DEFAULT '{}',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }

    async fn create_placements_table(&self) -> SyncResult<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS placements (
                id TEXT PRIMARY KEY,
                workload_id TEXT NOT NULL,
                cluster_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                replica_index INTEGER NOT NULL,
                node_id TEXT NOT NULL,
                container_id TEXT,
                state TEXT NOT NULL,
                message TEXT,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        "#,
        )
        .execute(&self.write_pool)
        .await?;
        Ok(())
    }

    async fn create_indexes(&self) -> SyncResult<()> {
        // Performance indexes as specified in the documentation
        let indexes = [
            // Container indexes
            "CREATE INDEX IF NOT EXISTS idx_containers_state ON containers(state)",
            "CREATE INDEX IF NOT EXISTS idx_containers_updated_at ON containers(updated_at)",
            "CREATE INDEX IF NOT EXISTS idx_containers_tenant ON containers(tenant_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_containers_tenant_name ON containers(tenant_id, name) WHERE name IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS idx_containers_project ON containers(project_id)",
            "CREATE INDEX IF NOT EXISTS idx_network_allocations_status ON network_allocations(status)",
            "CREATE INDEX IF NOT EXISTS idx_network_allocations_ip ON network_allocations(ip_address)",
            "CREATE INDEX IF NOT EXISTS idx_process_monitors_status ON process_monitors(status)",
            "CREATE INDEX IF NOT EXISTS idx_process_monitors_pid ON process_monitors(pid)",
            "CREATE INDEX IF NOT EXISTS idx_container_logs_container_time ON container_logs(container_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_container_logs_level ON container_logs(level)",
            "CREATE INDEX IF NOT EXISTS idx_cleanup_tasks_status ON cleanup_tasks(status)",
            "CREATE INDEX IF NOT EXISTS idx_cleanup_tasks_container ON cleanup_tasks(container_id)",
            "CREATE INDEX IF NOT EXISTS idx_cleanup_tasks_tenant ON cleanup_tasks(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_volumes_status ON volumes(status)",
            "CREATE INDEX IF NOT EXISTS idx_volumes_name ON volumes(name)",
            "CREATE INDEX IF NOT EXISTS idx_volumes_tenant ON volumes(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_volumes_size ON volumes(actual_size_gb)",
            "CREATE INDEX IF NOT EXISTS idx_volumes_measured_at ON volumes(last_measured_at)",
            "CREATE INDEX IF NOT EXISTS idx_containers_rootfs_size ON containers(rootfs_size_gb)",
            "CREATE INDEX IF NOT EXISTS idx_containers_rootfs_measured_at ON containers(rootfs_measured_at)",
            "CREATE INDEX IF NOT EXISTS idx_container_mounts_container ON container_mounts(container_id)",
            "CREATE INDEX IF NOT EXISTS idx_container_mounts_type ON container_mounts(mount_type)",
            "CREATE INDEX IF NOT EXISTS idx_container_metrics_container_time ON container_metrics(container_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_container_metrics_timestamp ON container_metrics(timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_projects_tenant ON projects(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status)",
            "CREATE INDEX IF NOT EXISTS idx_usage_events_tenant_time ON usage_events(tenant_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_usage_events_type ON usage_events(event_type)",
            "CREATE INDEX IF NOT EXISTS idx_dns_entries_container_id ON dns_entries(container_id)",
            "CREATE INDEX IF NOT EXISTS idx_dns_entries_tenant_id ON dns_entries(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_dns_entries_name ON dns_entries(container_name)",

            // User & auth indexes
            "CREATE INDEX IF NOT EXISTS idx_tenants_status ON tenants(status)",
            "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)",
            "CREATE INDEX IF NOT EXISTS idx_users_tenant ON users(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_users_status ON users(status)",
            "CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_auth_sessions_token ON auth_sessions(refresh_token_hash)",
            "CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires ON auth_sessions(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_auth_sessions_revoked ON auth_sessions(revoked)",
            "CREATE INDEX IF NOT EXISTS idx_user_activities_user_time ON user_activities(user_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_user_activities_tenant_time ON user_activities(tenant_id, timestamp)",
            "CREATE INDEX IF NOT EXISTS idx_user_activities_type ON user_activities(activity_type)",
            "CREATE INDEX IF NOT EXISTS idx_admin_audit_log_admin ON admin_audit_log(admin_user_id)",
            "CREATE INDEX IF NOT EXISTS idx_admin_audit_log_target ON admin_audit_log(target_user_id)",
            "CREATE INDEX IF NOT EXISTS idx_admin_audit_log_time ON admin_audit_log(timestamp)",

            // Billing & subscription indexes
            "CREATE INDEX IF NOT EXISTS idx_subscriptions_tenant ON subscriptions(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_subscriptions_status ON subscriptions(status)",
            "CREATE INDEX IF NOT EXISTS idx_subscriptions_stripe_customer ON subscriptions(stripe_customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_subscriptions_stripe_subscription ON subscriptions(stripe_subscription_id)",
            "CREATE INDEX IF NOT EXISTS idx_payment_methods_tenant ON payment_methods(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_payment_methods_customer ON payment_methods(stripe_customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_invoices_tenant ON invoices(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_invoices_customer ON invoices(stripe_customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_invoices_subscription ON invoices(stripe_subscription_id)",
            "CREATE INDEX IF NOT EXISTS idx_invoices_status ON invoices(status)",
            "CREATE INDEX IF NOT EXISTS idx_invoice_items_invoice ON invoice_items(invoice_id)",
            "CREATE INDEX IF NOT EXISTS idx_stripe_events_type ON stripe_events(event_type)",
            "CREATE INDEX IF NOT EXISTS idx_stripe_events_tenant ON stripe_events(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_stripe_events_status ON stripe_events(processing_status)",
            "CREATE INDEX IF NOT EXISTS idx_tenant_quotas_tenant ON tenant_quotas(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_billing_grace_periods_tenant ON billing_grace_periods(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_billing_grace_periods_expires ON billing_grace_periods(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_stripe_price_mappings_tier ON stripe_price_mappings(plan_tier)",
            "CREATE INDEX IF NOT EXISTS idx_stripe_price_mappings_active ON stripe_price_mappings(is_active)",

            // OCI Image indexes
            "CREATE INDEX IF NOT EXISTS idx_images_repository ON images(repository)",
            "CREATE INDEX IF NOT EXISTS idx_images_registry_repo ON images(registry, repository)",
            "CREATE INDEX IF NOT EXISTS idx_images_tag ON images(tag)",
            "CREATE INDEX IF NOT EXISTS idx_images_tenant ON images(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_images_last_used ON images(last_used_at)",
            "CREATE INDEX IF NOT EXISTS idx_image_layers_ref_count ON image_layers(ref_count)",
            "CREATE INDEX IF NOT EXISTS idx_image_layer_mapping_image ON image_layer_mapping(image_id)",
            "CREATE INDEX IF NOT EXISTS idx_image_layer_mapping_layer ON image_layer_mapping(layer_digest)",

            // ICC connection indexes
            "CREATE INDEX IF NOT EXISTS idx_icc_connections_from ON icc_connections(from_container_id)",
            "CREATE INDEX IF NOT EXISTS idx_icc_connections_to ON icc_connections(to_container_id)",
            "CREATE INDEX IF NOT EXISTS idx_icc_connections_status ON icc_connections(status)",

            // Admin accounts and sessions indexes
            "CREATE INDEX IF NOT EXISTS idx_admin_accounts_email ON admin_accounts(email)",
            "CREATE INDEX IF NOT EXISTS idx_admin_accounts_status ON admin_accounts(status)",
            "CREATE INDEX IF NOT EXISTS idx_admin_sessions_admin ON admin_sessions(admin_id)",
            "CREATE INDEX IF NOT EXISTS idx_admin_sessions_expires ON admin_sessions(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_admin_sessions_revoked ON admin_sessions(revoked)",
            "CREATE INDEX IF NOT EXISTS idx_admin_sessions_mfa ON admin_sessions(mfa_verified)",

            // Serverless functions indexes
            "CREATE INDEX IF NOT EXISTS idx_functions_tenant ON functions(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_functions_name ON functions(name)",
            "CREATE INDEX IF NOT EXISTS idx_functions_state ON functions(state)",
            "CREATE INDEX IF NOT EXISTS idx_functions_tenant_name ON functions(tenant_id, name)",
            "CREATE INDEX IF NOT EXISTS idx_function_versions_function ON function_versions(function_id)",
            "CREATE INDEX IF NOT EXISTS idx_function_versions_active ON function_versions(function_id, is_active)",
            "CREATE INDEX IF NOT EXISTS idx_function_invocations_function ON function_invocations(function_id)",
            "CREATE INDEX IF NOT EXISTS idx_function_invocations_status ON function_invocations(status)",
            "CREATE INDEX IF NOT EXISTS idx_function_invocations_started ON function_invocations(started_at)",
            "CREATE INDEX IF NOT EXISTS idx_function_pool_function ON function_pool(function_id)",
            "CREATE INDEX IF NOT EXISTS idx_function_pool_state ON function_pool(state)",
            "CREATE INDEX IF NOT EXISTS idx_function_pool_function_state ON function_pool(function_id, state)",

            // Cluster control plane indexes
            "CREATE INDEX IF NOT EXISTS idx_clusters_tenant ON clusters(tenant_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_clusters_tenant_name ON clusters(tenant_id, name)",
            "CREATE INDEX IF NOT EXISTS idx_nodes_cluster ON nodes(cluster_id)",
            "CREATE INDEX IF NOT EXISTS idx_nodes_tenant ON nodes(tenant_id)",
            // Enforce node name uniqueness within a cluster for active nodes.
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_nodes_cluster_name_active ON nodes(cluster_id, tenant_id, name) WHERE state != 'deleted'",
            "CREATE INDEX IF NOT EXISTS idx_node_allocations_cluster ON node_allocations(cluster_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_node_allocations_cluster_pod_cidr ON node_allocations(cluster_id, tenant_id, pod_cidr)",
            "CREATE INDEX IF NOT EXISTS idx_workloads_cluster ON workloads(cluster_id)",
            "CREATE INDEX IF NOT EXISTS idx_workloads_tenant ON workloads(tenant_id)",
            "CREATE INDEX IF NOT EXISTS idx_placements_node ON placements(node_id)",
            "CREATE INDEX IF NOT EXISTS idx_placements_workload ON placements(workload_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_placements_workload_replica ON placements(workload_id, replica_index)",
        ];

        for index_sql in indexes {
            sqlx::query(index_sql).execute(&self.write_pool).await?;
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
        let schema_manager = SchemaManager::new(conn_manager.writer().clone());

        schema_manager.initialize_schema().await.unwrap();

        // Verify tables exist
        let tables: Vec<(String,)> =
            sqlx::query_as("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
                .fetch_all(conn_manager.reader())
                .await
                .unwrap();

        let table_names: Vec<String> = tables.into_iter().map(|(name,)| name).collect();

        assert!(table_names.contains(&"containers".to_string()));
        assert!(table_names.contains(&"network_allocations".to_string()));
        assert!(table_names.contains(&"process_monitors".to_string()));
        assert!(table_names.contains(&"volumes".to_string()));
        assert!(table_names.contains(&"container_mounts".to_string()));
        assert!(table_names.contains(&"projects".to_string()));

        conn_manager.close().await;
    }
}

use crate::sync::error::SyncResult;
use sqlx::{FromRow, SqlitePool};

#[derive(Debug, Clone, FromRow)]
pub struct DnsEntry {
    pub id: String,
    #[allow(dead_code)] // Used in database operations
    pub container_id: String,
    pub container_name: String,
    pub ip_address: String,
    pub tenant_id: String,
    pub ttl: i64,
    #[allow(dead_code)] // Used in database operations
    pub created_at: i64,
    #[allow(dead_code)] // Used in database operations
    pub updated_at: i64,
}

pub struct DnsEntryManager {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl DnsEntryManager {
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    pub async fn create_entry(
        &self,
        container_id: &str,
        container_name: &str,
        ip_address: &str,
        tenant_id: &str,
    ) -> SyncResult<DnsEntry> {
        let now = chrono::Utc::now().timestamp();
        let id = uuid::Uuid::new_v4().to_string();
        let ttl = 300i64;

        sqlx::query(
            "INSERT INTO dns_entries (id, container_id, container_name, ip_address, tenant_id, ttl, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        .bind(&id)
        .bind(container_id)
        .bind(container_name)
        .bind(ip_address)
        .bind(tenant_id)
        .bind(ttl)
        .bind(now)
        .bind(now)
        .execute(&self.write_pool)
        .await?;

        let entry = DnsEntry {
            id: id.clone(),
            container_id: container_id.to_string(),
            container_name: container_name.to_string(),
            ip_address: ip_address.to_string(),
            tenant_id: tenant_id.to_string(),
            ttl,
            created_at: now,
            updated_at: now,
        };

        Ok(entry)
    }

    pub async fn get_by_container(&self, container_id: &str) -> SyncResult<Option<DnsEntry>> {
        sqlx::query_as::<_, DnsEntry>("SELECT * FROM dns_entries WHERE container_id = ?")
            .bind(container_id)
            .fetch_optional(&self.read_pool)
            .await
            .map_err(Into::into)
    }

    #[allow(dead_code)] // Will be used in HTTP API endpoints
    pub async fn list_by_tenant(&self, tenant_id: &str) -> SyncResult<Vec<DnsEntry>> {
        sqlx::query_as::<_, DnsEntry>(
            "SELECT * FROM dns_entries WHERE tenant_id = ? ORDER BY container_name",
        )
        .bind(tenant_id)
        .fetch_all(&self.read_pool)
        .await
        .map_err(Into::into)
    }

    #[allow(dead_code)] // Will be used in HTTP API endpoints
    pub async fn list_all(&self) -> SyncResult<Vec<DnsEntry>> {
        sqlx::query_as::<_, DnsEntry>(
            "SELECT * FROM dns_entries ORDER BY tenant_id, container_name",
        )
        .fetch_all(&self.read_pool)
        .await
        .map_err(Into::into)
    }

    pub async fn delete_by_container(&self, container_id: &str) -> SyncResult<()> {
        sqlx::query("DELETE FROM dns_entries WHERE container_id = ?")
            .bind(container_id)
            .execute(&self.write_pool)
            .await?;

        Ok(())
    }

    #[allow(dead_code)] // Will be used for container IP changes
    pub async fn update_ip(&self, container_id: &str, new_ip_address: &str) -> SyncResult<()> {
        let now = chrono::Utc::now().timestamp();

        sqlx::query("UPDATE dns_entries SET ip_address = ?, updated_at = ? WHERE container_id = ?")
            .bind(new_ip_address)
            .bind(now)
            .bind(container_id)
            .execute(&self.write_pool)
            .await?;

        Ok(())
    }
}

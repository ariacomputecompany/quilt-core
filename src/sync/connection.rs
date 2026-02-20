use crate::sync::error::SyncResult;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqlitePoolOptions},
    ConnectOptions, SqlitePool,
};
use std::time::Duration;

pub struct ConnectionManager {
    pool: SqlitePool,
}

impl ConnectionManager {
    pub async fn new(database_path: &str) -> SyncResult<Self> {
        let pool = create_optimized_pool(database_path).await?;
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    pub async fn close(&self) {
        self.pool.close().await;
    }
}

async fn create_optimized_pool(database_path: &str) -> SyncResult<SqlitePool> {
    let options = SqliteConnectOptions::new()
        .filename(database_path)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal) // Better concurrency
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal) // Performance vs safety balance
        .busy_timeout(Duration::from_secs(30))
        .pragma("cache_size", "10000") // 10MB cache
        .pragma("temp_store", "memory")
        .pragma("mmap_size", "268435456") // 256MB memory mapping
        .pragma("foreign_keys", "ON") // Enable foreign key constraints
        .create_if_missing(true)
        .disable_statement_logging();

    let pool = SqlitePoolOptions::new()
        .max_connections(32) // Increased from default for concurrent operations
        .min_connections(4) // Keep minimum connections ready
        .acquire_timeout(Duration::from_secs(30))
        .idle_timeout(Duration::from_secs(600))
        .connect_with(options)
        .await?;

    // Verify connection and performance
    let start = std::time::Instant::now();
    sqlx::query("SELECT 1").fetch_one(&pool).await?;
    let duration = start.elapsed();

    tracing::info!("SQLite connection established in {:?}", duration);

    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_connection_creation() {
        let temp_file = NamedTempFile::new().unwrap();
        let db_path = temp_file.path().to_str().unwrap();

        let conn_manager = ConnectionManager::new(db_path).await.unwrap();

        // Test basic query
        let result: (i64,) = sqlx::query_as("SELECT 1")
            .fetch_one(conn_manager.pool())
            .await
            .unwrap();

        assert_eq!(result.0, 1);

        conn_manager.close().await;
    }
}

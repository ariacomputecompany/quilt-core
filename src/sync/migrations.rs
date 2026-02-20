//! Database migrations module.
//!
//! NOTE: All migrations are done MANUALLY. No automatic migrations.
//! This file is kept for reference only.

#![allow(dead_code)]

use crate::sync::error::SyncResult;
use sqlx::SqlitePool;

pub struct MigrationManager {
    write_pool: SqlitePool,
}

impl MigrationManager {
    pub fn new(write_pool: SqlitePool) -> Self {
        Self { write_pool }
    }

    /// No-op: Migrations are run manually.
    pub async fn run_migrations(&self) -> SyncResult<()> {
        // All migrations are done manually via SQL scripts.
        // This function intentionally does nothing.
        Ok(())
    }
}

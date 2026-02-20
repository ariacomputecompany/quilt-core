#!/bin/bash
# Production migration script for storage tracking columns
# Idempotent - safe to run multiple times

set -e

DB_PATH="${1:-/var/lib/quilt/quilt.db}"

echo "=== Quilt Storage Tracking Migration ==="
echo "Database: $DB_PATH"
echo ""

# Verify database exists
if [ ! -f "$DB_PATH" ]; then
    echo "ERROR: Database not found at $DB_PATH"
    exit 1
fi

# Backup database
BACKUP_PATH="${DB_PATH}.backup.$(date +%Y%m%d_%H%M%S)"
echo "Creating backup: $BACKUP_PATH"
cp "$DB_PATH" "$BACKUP_PATH"

echo ""
echo "Applying migration..."

# Apply volumes table changes (ignore errors if columns exist)
sqlite3 "$DB_PATH" <<'EOF' 2>&1 | grep -v "duplicate column name" || true
-- Add storage tracking to volumes
ALTER TABLE volumes ADD COLUMN actual_size_gb REAL NOT NULL DEFAULT 0.0;
ALTER TABLE volumes ADD COLUMN last_measured_at INTEGER;

-- Add storage tracking to containers
ALTER TABLE containers ADD COLUMN rootfs_size_gb REAL NOT NULL DEFAULT 0.0;
ALTER TABLE containers ADD COLUMN rootfs_measured_at INTEGER;

-- Create indexes (idempotent with IF NOT EXISTS)
CREATE INDEX IF NOT EXISTS idx_volumes_size ON volumes(actual_size_gb);
CREATE INDEX IF NOT EXISTS idx_volumes_measured_at ON volumes(last_measured_at);
CREATE INDEX IF NOT EXISTS idx_containers_rootfs_size ON containers(rootfs_size_gb);
CREATE INDEX IF NOT EXISTS idx_containers_rootfs_measured_at ON containers(rootfs_measured_at);

-- Create storage_measurements table
CREATE TABLE IF NOT EXISTS storage_measurements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tenant_id TEXT NOT NULL,
    measured_at INTEGER NOT NULL,
    volumes_gb REAL NOT NULL,
    containers_gb REAL NOT NULL,
    total_gb REAL NOT NULL,
    measurement_type TEXT CHECK(measurement_type IN ('scheduled', 'manual', 'reconciliation')) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_storage_measurements_tenant_time ON storage_measurements(tenant_id, measured_at);
CREATE INDEX IF NOT EXISTS idx_storage_measurements_type ON storage_measurements(measurement_type);
EOF

echo ""
echo "Verifying migration..."

# Verify volumes columns
echo "Volumes table columns:"
sqlite3 "$DB_PATH" -header -column "SELECT name, type, \"notnull\", dflt_value FROM pragma_table_info('volumes') WHERE name IN ('actual_size_gb', 'last_measured_at');"

# Verify containers columns
echo ""
echo "Containers table columns:"
sqlite3 "$DB_PATH" -header -column "SELECT name, type, \"notnull\", dflt_value FROM pragma_table_info('containers') WHERE name IN ('rootfs_size_gb', 'rootfs_measured_at');"

echo ""
echo "✅ Migration complete!"
echo "Backup saved to: $BACKUP_PATH"

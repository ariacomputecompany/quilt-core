//! OCI Image persistence layer for SyncEngine
//!
//! Provides database storage and retrieval for pulled OCI images,
//! enabling container creation from registry images.

use crate::sync::error::SyncResult;
use sqlx::{Row, SqlitePool};

/// Record representing a stored OCI image
#[derive(Debug, Clone)]
pub struct ImageRecord {
    /// Content digest (sha256:...)
    pub id: String,
    /// Tenant ID
    pub tenant_id: String,
    /// Repository name (e.g., "library/nginx")
    pub repository: String,
    /// Registry host (e.g., "docker.io")
    pub registry: String,
    /// Image tag (e.g., "latest")
    pub tag: Option<String>,
    /// Manifest content hash
    pub manifest_digest: String,
    /// Config blob digest
    pub config_digest: String,
    /// Total uncompressed size in bytes
    pub size_bytes: i64,
    /// CPU architecture
    pub architecture: String,
    /// Operating system
    pub os: String,
    /// Default CMD (JSON array)
    pub default_cmd: Option<String>,
    /// Entrypoint (JSON array)
    pub entrypoint: Option<String>,
    /// Environment variables (JSON array)
    pub env: Option<String>,
    /// Working directory
    pub working_dir: Option<String>,
    /// Exposed ports (JSON object)
    pub exposed_ports: Option<String>,
    /// Volumes (JSON object)
    pub volumes: Option<String>,
    /// Labels (JSON object)
    pub labels: Option<String>,
    /// User to run as
    pub user_spec: Option<String>,
    /// Creation timestamp
    pub created_at: i64,
    /// Last used timestamp
    pub last_used_at: i64,
    /// When image was pulled
    pub pulled_at: Option<i64>,
    /// Reference count for garbage collection
    pub ref_count: i32,
}

/// Layer record for image layers
#[derive(Debug, Clone)]
pub struct LayerRecord {
    /// Layer digest (sha256:...)
    pub digest: String,
    /// Compressed size in bytes
    pub size_bytes: i64,
    /// Uncompressed size (if known)
    pub uncompressed_size: Option<i64>,
    /// Media type
    pub media_type: String,
    /// Path where layer is extracted
    pub extracted_path: Option<String>,
    /// Reference count
    pub ref_count: i32,
    /// Creation timestamp
    pub created_at: i64,
    /// Last used timestamp
    pub last_used_at: i64,
}

/// Image store for database operations
pub struct ImageStore {
    read_pool: SqlitePool,
    write_pool: SqlitePool,
}

impl ImageStore {
    pub fn new(read_pool: SqlitePool, write_pool: SqlitePool) -> Self {
        Self {
            read_pool,
            write_pool,
        }
    }

    /// Save an image to the database
    pub async fn save_image(&self, image: &ImageRecord) -> SyncResult<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        sqlx::query(r#"
            INSERT INTO images (
                id, tenant_id, repository, registry, tag, manifest_digest, config_digest,
                size_bytes, architecture, os, default_cmd, entrypoint, env, working_dir,
                exposed_ports, volumes, labels, user_spec, created_at, last_used_at, pulled_at, ref_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                tag = excluded.tag,
                last_used_at = excluded.last_used_at,
                pulled_at = excluded.pulled_at,
                ref_count = images.ref_count + 1
        "#)
        .bind(&image.id)
        .bind(&image.tenant_id)
        .bind(&image.repository)
        .bind(&image.registry)
        .bind(&image.tag)
        .bind(&image.manifest_digest)
        .bind(&image.config_digest)
        .bind(image.size_bytes)
        .bind(&image.architecture)
        .bind(&image.os)
        .bind(&image.default_cmd)
        .bind(&image.entrypoint)
        .bind(&image.env)
        .bind(&image.working_dir)
        .bind(&image.exposed_ports)
        .bind(&image.volumes)
        .bind(&image.labels)
        .bind(&image.user_spec)
        .bind(now)
        .bind(now)
        .bind(Some(now))
        .bind(1)
        .execute(&self.write_pool)
        .await?;

        tracing::info!(
            "Saved image {} ({}/{}:{}) to database (ref_count: {})",
            image.id,
            image.registry,
            image.repository,
            image.tag.as_deref().unwrap_or("latest"),
            image.ref_count
        );

        Ok(())
    }

    /// Save a layer to the database
    pub async fn save_layer(&self, layer: &LayerRecord) -> SyncResult<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        sqlx::query(
            r#"
            INSERT INTO image_layers (
                digest, size_bytes, uncompressed_size, media_type, extracted_path,
                ref_count, created_at, last_used_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(digest) DO UPDATE SET
                last_used_at = excluded.last_used_at,
                ref_count = image_layers.ref_count + 1
        "#,
        )
        .bind(&layer.digest)
        .bind(layer.size_bytes)
        .bind(layer.uncompressed_size)
        .bind(&layer.media_type)
        .bind(&layer.extracted_path)
        .bind(1)
        .bind(now)
        .bind(now)
        .execute(&self.write_pool)
        .await?;

        tracing::debug!(
            "Saved layer {} ({} bytes, ref_count: {}, created: {}, last_used: {})",
            layer.digest,
            layer.size_bytes,
            layer.ref_count,
            layer.created_at,
            layer.last_used_at
        );

        Ok(())
    }

    /// Save image-to-layer mapping
    pub async fn save_layer_mapping(
        &self,
        image_id: &str,
        layer_digest: &str,
        layer_index: i32,
    ) -> SyncResult<()> {
        sqlx::query(
            r#"
            INSERT INTO image_layer_mapping (image_id, layer_digest, layer_index)
            VALUES (?, ?, ?)
            ON CONFLICT(image_id, layer_digest) DO NOTHING
        "#,
        )
        .bind(image_id)
        .bind(layer_digest)
        .bind(layer_index)
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    /// Get an image by reference (registry/repository:tag or @digest)
    pub async fn get_image(
        &self,
        reference: &str,
        tenant_id: &str,
    ) -> SyncResult<Option<ImageRecord>> {
        // Parse the reference to determine if it's a tag or digest lookup
        let (registry, repository, tag, digest) = parse_reference(reference);

        let row = if let Some(d) = digest {
            // Lookup by digest
            sqlx::query(r#"
                SELECT id, tenant_id, repository, registry, tag, manifest_digest, config_digest,
                       size_bytes, architecture, os, default_cmd, entrypoint, env, working_dir,
                       exposed_ports, volumes, labels, user_spec, created_at, last_used_at, pulled_at, ref_count
                FROM images
                WHERE id = ? AND tenant_id = ?
            "#)
            .bind(&d)
            .bind(tenant_id)
            .fetch_optional(&self.read_pool)
            .await?
        } else {
            // Lookup by registry/repository:tag
            sqlx::query(r#"
                SELECT id, tenant_id, repository, registry, tag, manifest_digest, config_digest,
                       size_bytes, architecture, os, default_cmd, entrypoint, env, working_dir,
                       exposed_ports, volumes, labels, user_spec, created_at, last_used_at, pulled_at, ref_count
                FROM images
                WHERE registry = ? AND repository = ? AND tag = ? AND tenant_id = ?
            "#)
            .bind(&registry)
            .bind(&repository)
            .bind(&tag)
            .bind(tenant_id)
            .fetch_optional(&self.read_pool)
            .await?
        };

        Ok(row.map(|r| row_to_image_record(&r)))
    }

    /// Get an image by its digest only
    pub async fn get_image_by_digest(&self, digest: &str) -> SyncResult<Option<ImageRecord>> {
        let row = sqlx::query(r#"
            SELECT id, tenant_id, repository, registry, tag, manifest_digest, config_digest,
                   size_bytes, architecture, os, default_cmd, entrypoint, env, working_dir,
                   exposed_ports, volumes, labels, user_spec, created_at, last_used_at, pulled_at, ref_count
            FROM images
            WHERE id = ?
        "#)
        .bind(digest)
        .fetch_optional(&self.read_pool)
        .await?;

        Ok(row.map(|r| row_to_image_record(&r)))
    }

    /// List all images for a tenant
    pub async fn list_images(&self, tenant_id: &str) -> SyncResult<Vec<ImageRecord>> {
        let rows = sqlx::query(r#"
            SELECT id, tenant_id, repository, registry, tag, manifest_digest, config_digest,
                   size_bytes, architecture, os, default_cmd, entrypoint, env, working_dir,
                   exposed_ports, volumes, labels, user_spec, created_at, last_used_at, pulled_at, ref_count
            FROM images
            WHERE tenant_id = ?
            ORDER BY last_used_at DESC
        "#)
        .bind(tenant_id)
        .fetch_all(&self.read_pool)
        .await?;

        Ok(rows.iter().map(row_to_image_record).collect())
    }

    /// Delete an image by digest
    pub async fn delete_image(&self, digest: &str, tenant_id: &str) -> SyncResult<bool> {
        // First decrement ref_count, only delete if it reaches 0
        let result = sqlx::query(
            r#"
            DELETE FROM images
            WHERE id = ? AND tenant_id = ? AND ref_count <= 1
        "#,
        )
        .bind(digest)
        .bind(tenant_id)
        .execute(&self.write_pool)
        .await?;

        if result.rows_affected() > 0 {
            tracing::info!("Deleted image {} from database", digest);
            return Ok(true);
        }

        // If not deleted, decrement ref_count
        sqlx::query(
            r#"
            UPDATE images SET ref_count = ref_count - 1
            WHERE id = ? AND tenant_id = ?
        "#,
        )
        .bind(digest)
        .bind(tenant_id)
        .execute(&self.write_pool)
        .await?;

        Ok(false)
    }

    /// Get layers for an image
    pub async fn get_image_layers(&self, image_id: &str) -> SyncResult<Vec<LayerRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT l.digest, l.size_bytes, l.uncompressed_size, l.media_type, l.extracted_path,
                   l.ref_count, l.created_at, l.last_used_at
            FROM image_layers l
            INNER JOIN image_layer_mapping m ON l.digest = m.layer_digest
            WHERE m.image_id = ?
            ORDER BY m.layer_index ASC
        "#,
        )
        .bind(image_id)
        .fetch_all(&self.read_pool)
        .await?;

        Ok(rows
            .iter()
            .map(|r| LayerRecord {
                digest: r.get("digest"),
                size_bytes: r.get("size_bytes"),
                uncompressed_size: r.get("uncompressed_size"),
                media_type: r.get("media_type"),
                extracted_path: r.get("extracted_path"),
                ref_count: r.get("ref_count"),
                created_at: r.get("created_at"),
                last_used_at: r.get("last_used_at"),
            })
            .collect())
    }

    /// Update last_used_at timestamp for an image
    pub async fn touch_image(&self, digest: &str) -> SyncResult<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);

        sqlx::query("UPDATE images SET last_used_at = ? WHERE id = ?")
            .bind(now)
            .bind(digest)
            .execute(&self.write_pool)
            .await?;

        Ok(())
    }
}

/// Parse an image reference into components
fn parse_reference(reference: &str) -> (String, String, String, Option<String>) {
    // Handle digest references (sha256:...)
    if reference.starts_with("sha256:") {
        return (
            "".to_string(),
            "".to_string(),
            "".to_string(),
            Some(reference.to_string()),
        );
    }

    // Handle @digest references
    if let Some(at_pos) = reference.rfind('@') {
        let digest = reference[at_pos + 1..].to_string();
        return ("".to_string(), "".to_string(), "".to_string(), Some(digest));
    }

    // Default registry
    let mut registry = "docker.io".to_string();
    let mut remaining = reference;

    // Check for explicit registry (contains . or :)
    if let Some(slash_pos) = reference.find('/') {
        let potential_registry = &reference[..slash_pos];
        if potential_registry.contains('.') || potential_registry.contains(':') {
            registry = potential_registry.to_string();
            remaining = &reference[slash_pos + 1..];
        }
    }

    // Parse repository and tag
    let (repository, tag) = if let Some(colon_pos) = remaining.rfind(':') {
        // Check it's not a port in the registry
        let tag_part = &remaining[colon_pos + 1..];
        if !tag_part.contains('/') {
            (remaining[..colon_pos].to_string(), tag_part.to_string())
        } else {
            (remaining.to_string(), "latest".to_string())
        }
    } else {
        (remaining.to_string(), "latest".to_string())
    };

    // Add library/ prefix for Docker Hub official images
    let repository = if registry == "docker.io" && !repository.contains('/') {
        format!("library/{}", repository)
    } else {
        repository
    };

    (registry, repository, tag, None)
}

/// Convert database row to ImageRecord
fn row_to_image_record(row: &sqlx::sqlite::SqliteRow) -> ImageRecord {
    ImageRecord {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        repository: row.get("repository"),
        registry: row.get("registry"),
        tag: row.get("tag"),
        manifest_digest: row.get("manifest_digest"),
        config_digest: row.get("config_digest"),
        size_bytes: row.get("size_bytes"),
        architecture: row.get("architecture"),
        os: row.get("os"),
        default_cmd: row.get("default_cmd"),
        entrypoint: row.get("entrypoint"),
        env: row.get("env"),
        working_dir: row.get("working_dir"),
        exposed_ports: row.get("exposed_ports"),
        volumes: row.get("volumes"),
        labels: row.get("labels"),
        user_spec: row.get("user_spec"),
        created_at: row.get("created_at"),
        last_used_at: row.get("last_used_at"),
        pulled_at: row.get("pulled_at"),
        ref_count: row.get("ref_count"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_reference_simple() {
        let (registry, repo, tag, digest) = parse_reference("nginx");
        assert_eq!(registry, "docker.io");
        assert_eq!(repo, "library/nginx");
        assert_eq!(tag, "latest");
        assert!(digest.is_none());
    }

    #[test]
    fn test_parse_reference_with_tag() {
        let (registry, repo, tag, digest) = parse_reference("nginx:1.25");
        assert_eq!(registry, "docker.io");
        assert_eq!(repo, "library/nginx");
        assert_eq!(tag, "1.25");
        assert!(digest.is_none());
    }

    #[test]
    fn test_parse_reference_user_repo() {
        let (registry, repo, tag, digest) = parse_reference("myuser/myapp:v1");
        assert_eq!(registry, "docker.io");
        assert_eq!(repo, "myuser/myapp");
        assert_eq!(tag, "v1");
        assert!(digest.is_none());
    }

    #[test]
    fn test_parse_reference_custom_registry() {
        let (registry, repo, tag, digest) = parse_reference("ghcr.io/owner/repo:latest");
        assert_eq!(registry, "ghcr.io");
        assert_eq!(repo, "owner/repo");
        assert_eq!(tag, "latest");
        assert!(digest.is_none());
    }

    #[test]
    fn test_parse_reference_digest() {
        let (_, _, _, digest) = parse_reference("sha256:abc123");
        assert_eq!(digest, Some("sha256:abc123".to_string()));
    }
}

//! Content-Addressable Storage for OCI Blobs
//!
//! Provides deduplicated storage for image layers, configs, and manifests.
//! All content is stored by its SHA256 digest, enabling:
//! - Automatic deduplication across images
//! - Content verification on read
//! - Efficient layer sharing

use crate::image::{ImageError, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use walkdir::WalkDir;

/// Content-addressable blob store
pub struct ContentStore {
    /// Base path for blob storage
    base_path: PathBuf,

    /// In-memory cache of blob existence (digest -> size)
    cache: RwLock<HashMap<String, u64>>,
}

impl ContentStore {
    /// Create a new content store at the given path
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base = base_path.as_ref().to_path_buf();

        // Create directory structure
        std::fs::create_dir_all(base.join("sha256"))?;

        Ok(Self {
            base_path: base,
            cache: RwLock::new(HashMap::new()),
        })
    }

    /// Store a blob and return its digest
    pub fn store_blob(&self, data: &[u8]) -> Result<String> {
        let digest = self.compute_digest(data);
        let path = self.blob_path(&digest);

        // Skip if already exists
        if path.exists() {
            // Update cache
            let mut cache = self.cache.write().map_err(|e| {
                ImageError::StoreError(format!("Failed to acquire write lock: {}", e))
            })?;
            cache.insert(digest.clone(), data.len() as u64);
            return Ok(digest);
        }

        // Create parent directory if needed
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Write atomically using temp file
        let temp_path = path.with_extension("tmp");
        std::fs::write(&temp_path, data)?;
        std::fs::rename(&temp_path, &path)?;

        // Update cache
        let mut cache = self
            .cache
            .write()
            .map_err(|e| ImageError::StoreError(format!("Failed to acquire write lock: {}", e)))?;
        cache.insert(digest.clone(), data.len() as u64);

        Ok(digest)
    }

    /// Get a blob by digest
    pub fn get_blob(&self, digest: &str) -> Result<Vec<u8>> {
        let path = self.blob_path(digest);

        if !path.exists() {
            return Err(ImageError::BlobNotFound(digest.to_string()));
        }

        let data = std::fs::read(&path)?;

        // Verify content
        let actual_digest = self.compute_digest(&data);
        if actual_digest != digest {
            return Err(ImageError::ContentVerificationError {
                expected: digest.to_string(),
                actual: actual_digest,
            });
        }

        Ok(data)
    }

    /// Check if a blob exists
    pub fn has_blob(&self, digest: &str) -> Result<bool> {
        // Check cache first
        {
            let cache = self.cache.read().map_err(|e| {
                ImageError::StoreError(format!("Failed to acquire read lock: {}", e))
            })?;
            if cache.contains_key(digest) {
                return Ok(true);
            }
        }

        // Check filesystem
        let path = self.blob_path(digest);
        let exists = path.exists();

        // Update cache if exists
        if exists {
            if let Ok(metadata) = std::fs::metadata(&path) {
                let mut cache = self.cache.write().map_err(|e| {
                    ImageError::StoreError(format!("Failed to acquire write lock: {}", e))
                })?;
                cache.insert(digest.to_string(), metadata.len());
            }
        }

        Ok(exists)
    }

    /// Delete a blob
    pub fn delete_blob(&self, digest: &str) -> Result<()> {
        let path = self.blob_path(digest);

        if path.exists() {
            std::fs::remove_file(&path)?;
        }

        // Remove from cache
        let mut cache = self
            .cache
            .write()
            .map_err(|e| ImageError::StoreError(format!("Failed to acquire write lock: {}", e)))?;
        cache.remove(digest);

        Ok(())
    }

    /// Get the filesystem path for a blob
    pub fn blob_path(&self, digest: &str) -> PathBuf {
        // Parse digest: sha256:abc123... -> sha256/ab/abc123...
        let (algo, hash) = digest.split_once(':').unwrap_or(("sha256", digest));

        // Use first two chars as subdirectory for better filesystem distribution
        let prefix = &hash[..2.min(hash.len())];

        self.base_path.join(algo).join(prefix).join(hash)
    }

    /// Compute SHA256 digest of data
    pub fn compute_digest(&self, data: &[u8]) -> String {
        let hash = Sha256::digest(data);
        format!("sha256:{:x}", hash)
    }
}

/// High-level image store - manages images, manifests, and their relationships
pub struct ImageStore {
    /// Underlying content store
    content: ContentStore,

    /// Image metadata storage path
    metadata_path: PathBuf,
}

impl ImageStore {
    /// Create a new image store
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base = base_path.as_ref();

        Ok(Self {
            content: ContentStore::new(base.join("blobs"))?,
            metadata_path: base.join("metadata"),
        })
    }

    /// Get the underlying content store
    pub fn content(&self) -> &ContentStore {
        &self.content
    }

    /// Store a blob and return its digest
    pub fn store_blob(&self, data: &[u8]) -> Result<String> {
        self.content.store_blob(data)
    }

    /// Get a blob by digest
    pub fn get_blob(&self, digest: &str) -> Result<Vec<u8>> {
        self.content.get_blob(digest)
    }

    /// Store image metadata (reference -> manifest digest mapping)
    pub fn store_image_ref(
        &self,
        reference: &crate::image::ImageReference,
        manifest_digest: &str,
    ) -> Result<()> {
        std::fs::create_dir_all(&self.metadata_path)?;

        let ref_path = self.ref_path(reference);
        if let Some(parent) = ref_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        std::fs::write(&ref_path, manifest_digest)?;
        Ok(())
    }

    /// Get the path for storing a reference
    fn ref_path(&self, reference: &crate::image::ImageReference) -> PathBuf {
        let tag_or_digest = reference
            .digest
            .as_deref()
            .unwrap_or(&reference.tag)
            .replace(':', "_");

        self.metadata_path
            .join(&reference.registry)
            .join(&reference.repository)
            .join(tag_or_digest)
    }

    /// Get stored manifest digest for an image reference
    pub fn get_image_ref(&self, reference: &crate::image::ImageReference) -> Result<Option<String>> {
        let ref_path = self.ref_path(reference);
        if !ref_path.exists() {
            return Ok(None);
        }
        let digest = std::fs::read_to_string(ref_path)?.trim().to_string();
        if digest.is_empty() {
            return Ok(None);
        }
        Ok(Some(digest))
    }

    /// Remove stored image reference metadata
    pub fn remove_image_ref(&self, reference: &crate::image::ImageReference) -> Result<()> {
        let ref_path = self.ref_path(reference);
        if ref_path.exists() {
            std::fs::remove_file(ref_path)?;
        }
        Ok(())
    }

    /// List all stored image reference entries from metadata
    pub fn list_image_refs(&self) -> Result<Vec<String>> {
        if !self.metadata_path.exists() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for entry in WalkDir::new(&self.metadata_path).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }

            let rel = match entry.path().strip_prefix(&self.metadata_path) {
                Ok(r) => r,
                Err(_) => continue,
            };
            let parts: Vec<_> = rel.components().map(|c| c.as_os_str().to_string_lossy().to_string()).collect();
            if parts.len() < 3 {
                continue;
            }
            let registry = &parts[0];
            let tag = &parts[parts.len() - 1];
            let repository = parts[1..parts.len() - 1].join("/");
            out.push(format!("{}/{}:{}", registry, repository, tag));
        }

        out.sort();
        out.dedup();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_content_store() {
        let dir = tempdir().unwrap();
        let store = ContentStore::new(dir.path()).unwrap();

        let data = b"hello world";
        let digest = store.store_blob(data).unwrap();

        assert!(store.has_blob(&digest).unwrap());
        assert_eq!(store.get_blob(&digest).unwrap(), data);

        let computed = store.compute_digest(data);
        assert_eq!(digest, computed);
    }

    #[test]
    fn test_blob_deduplication() {
        let dir = tempdir().unwrap();
        let store = ContentStore::new(dir.path()).unwrap();

        let data = b"duplicate data";

        let digest1 = store.store_blob(data).unwrap();
        let digest2 = store.store_blob(data).unwrap();

        assert_eq!(digest1, digest2);

        // Verify both return the same data
        assert_eq!(store.get_blob(&digest1).unwrap(), data);
    }

    #[test]
    fn test_blob_path_distribution() {
        let dir = tempdir().unwrap();
        let store = ContentStore::new(dir.path()).unwrap();

        let path = store.blob_path("sha256:abc123def456");
        assert!(path.to_string_lossy().contains("sha256"));
        assert!(path.to_string_lossy().contains("ab")); // Prefix dir
    }
}

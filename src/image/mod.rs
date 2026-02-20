//! OCI Image Support Module
//!
//! Provides full OCI (Open Container Initiative) image format support including:
//! - Image manifest parsing (OCI and Docker v2 formats)
//! - Image configuration parsing
//! - Content-addressable storage for blobs
//! - Layer extraction with deduplication
//! - Overlay filesystem mounting
//!
//! This module enables Quilt to pull and run images from Docker Hub
//! and other OCI-compliant registries.

mod config;
mod layers;
mod manifest;
mod reference;
mod store;

// Re-export items used by other modules
pub use config::OciImageConfig;
pub use manifest::{parse_manifest, ManifestKind, MediaType, OciManifest};
pub use reference::ImageReference;
pub use store::ImageStore;

// Internal use only
use layers::LayerManager;

use std::path::Path;
use thiserror::Error;

/// Errors that can occur during image operations
#[derive(Error, Debug)]
pub enum ImageError {
    #[error("Layer extraction failed: {0}")]
    LayerExtractionError(String),

    #[error("Overlay mount failed: {0}")]
    OverlayMountError(String),

    #[error("Content verification failed: expected {expected}, got {actual}")]
    ContentVerificationError { expected: String, actual: String },

    #[error("Blob not found: {0}")]
    BlobNotFound(String),

    #[error("Store error: {0}")]
    StoreError(String),

    #[error("Invalid image reference: {0}")]
    InvalidReference(String),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),
}

pub type Result<T> = std::result::Result<T, ImageError>;

/// OCI Image - represents a fully resolved container image
#[derive(Debug, Clone)]
pub struct OciImage {
    /// Image reference (e.g., "docker.io/library/nginx:latest")
    pub reference: ImageReference,

    /// Image manifest
    pub manifest: OciManifest,

    /// Image configuration
    pub config: OciImageConfig,

    /// Manifest digest (content-addressable ID)
    pub digest: String,

    /// Total uncompressed size of all layers
    pub size: u64,
}

impl OciImage {
    /// Get the default command to run
    pub fn default_cmd(&self) -> Option<Vec<String>> {
        self.config.config.as_ref().and_then(|c| c.cmd.clone())
    }

    /// Get the entrypoint
    pub fn entrypoint(&self) -> Option<Vec<String>> {
        self.config
            .config
            .as_ref()
            .and_then(|c| c.entrypoint.clone())
    }

    /// Get environment variables
    pub fn env(&self) -> Vec<String> {
        self.config
            .config
            .as_ref()
            .and_then(|c| c.env.clone())
            .unwrap_or_default()
    }

    /// Get working directory
    pub fn working_dir(&self) -> Option<String> {
        self.config
            .config
            .as_ref()
            .and_then(|c| c.working_dir.clone())
    }

    /// Get the user to run as
    pub fn user(&self) -> Option<String> {
        self.config.config.as_ref().and_then(|c| c.user.clone())
    }

    /// Get exposed ports
    pub fn exposed_ports(&self) -> Vec<String> {
        self.config
            .config
            .as_ref()
            .and_then(|c| c.exposed_ports.clone())
            .map(|ports| ports.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Get volumes defined in the image
    pub fn volumes(&self) -> Vec<String> {
        self.config
            .config
            .as_ref()
            .and_then(|c| c.volumes.clone())
            .map(|vols| vols.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Get labels
    pub fn labels(&self) -> std::collections::HashMap<String, String> {
        self.config
            .config
            .as_ref()
            .and_then(|c| c.labels.clone())
            .unwrap_or_default()
    }
}

/// Image manager - high-level API for working with OCI images
pub struct ImageManager {
    store: ImageStore,
    layers: LayerManager,
}

impl ImageManager {
    /// Create a new image manager
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base = base_path.as_ref();
        Ok(Self {
            store: ImageStore::new(base.join("blobs"))?,
            layers: LayerManager::new(base.join("layers"))?,
        })
    }

    /// Load an image from the blob store using its manifest digest
    /// This reconstructs an OciImage from stored blobs
    pub async fn load_image(
        &self,
        reference: &ImageReference,
        manifest_digest: &str,
        config_digest: &str,
    ) -> Result<OciImage> {
        // Load manifest from blob store
        let manifest_blob = self.store.get_blob(manifest_digest)?;
        let manifest: OciManifest = serde_json::from_slice(&manifest_blob)?;

        // Load config from blob store
        let config_blob = self.store.get_blob(config_digest)?;
        let config: OciImageConfig = serde_json::from_slice(&config_blob)?;

        // Calculate total size from layers
        let size: u64 = manifest.layers.iter().map(|l| l.size).sum();

        Ok(OciImage {
            reference: reference.clone(),
            manifest,
            config,
            digest: manifest_digest.to_string(),
            size,
        })
    }

    /// Prepare rootfs for a container by extracting and mounting layers
    pub async fn prepare_rootfs(
        &self,
        image: &OciImage,
        container_id: &str,
    ) -> Result<std::path::PathBuf> {
        // Extract all layers
        let mut layer_paths = Vec::new();

        for layer_desc in &image.manifest.layers {
            let blob = self.store.get_blob(&layer_desc.digest)?;
            let layer_path = self.layers.extract_layer(&layer_desc.digest, &blob).await?;
            layer_paths.push(layer_path);
        }

        // Mount overlay filesystem
        let rootfs_path = self.layers.mount_overlay(&layer_paths, container_id)?;

        Ok(rootfs_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_image_reference_parse() {
        let ref1 = ImageReference::parse("nginx").unwrap();
        assert_eq!(ref1.registry, "docker.io");
        assert_eq!(ref1.repository, "library/nginx");
        assert_eq!(ref1.tag, "latest");

        let ref2 = ImageReference::parse("myuser/myapp:v1.0").unwrap();
        assert_eq!(ref2.registry, "docker.io");
        assert_eq!(ref2.repository, "myuser/myapp");
        assert_eq!(ref2.tag, "v1.0");

        let ref3 = ImageReference::parse("ghcr.io/owner/repo:latest").unwrap();
        assert_eq!(ref3.registry, "ghcr.io");
        assert_eq!(ref3.repository, "owner/repo");
        assert_eq!(ref3.tag, "latest");
    }
}

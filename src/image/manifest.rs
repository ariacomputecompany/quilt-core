//! OCI Image Manifest Parsing
//!
//! Supports both OCI Image Manifest and Docker Image Manifest v2 formats.
//! See: https://github.com/opencontainers/image-spec/blob/main/manifest.md

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// OCI Media Types
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaType {
    /// OCI Image Index
    OciIndex,
    /// OCI Image Manifest
    OciManifest,
    /// OCI Image Config
    OciImageConfig,
    /// OCI Layer (uncompressed tar)
    OciLayer,
    /// OCI Layer (gzip compressed)
    OciLayerGzip,
    /// OCI Layer (zstd compressed)
    OciLayerZstd,
    /// Docker Manifest List (fat manifest)
    DockerManifestList,
    /// Docker Manifest v2
    DockerManifestV2,
    /// Docker Image Config
    DockerImageConfig,
    /// Docker Layer
    DockerLayer,
    /// Unknown/other media type
    Other(String),
}

impl MediaType {
    pub fn from_str(s: &str) -> Self {
        match s {
            "application/vnd.oci.image.index.v1+json" => MediaType::OciIndex,
            "application/vnd.oci.image.manifest.v1+json" => MediaType::OciManifest,
            "application/vnd.oci.image.config.v1+json" => MediaType::OciImageConfig,
            "application/vnd.oci.image.layer.v1.tar" => MediaType::OciLayer,
            "application/vnd.oci.image.layer.v1.tar+gzip" => MediaType::OciLayerGzip,
            "application/vnd.oci.image.layer.v1.tar+zstd" => MediaType::OciLayerZstd,
            "application/vnd.docker.distribution.manifest.list.v2+json" => {
                MediaType::DockerManifestList
            }
            "application/vnd.docker.distribution.manifest.v2+json" => MediaType::DockerManifestV2,
            "application/vnd.docker.container.image.v1+json" => MediaType::DockerImageConfig,
            "application/vnd.docker.image.rootfs.diff.tar.gzip" => MediaType::DockerLayer,
            other => MediaType::Other(other.to_string()),
        }
    }
}

impl std::fmt::Display for MediaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MediaType::OciIndex => write!(f, "application/vnd.oci.image.index.v1+json"),
            MediaType::OciManifest => write!(f, "application/vnd.oci.image.manifest.v1+json"),
            MediaType::OciImageConfig => write!(f, "application/vnd.oci.image.config.v1+json"),
            MediaType::OciLayer => write!(f, "application/vnd.oci.image.layer.v1.tar"),
            MediaType::OciLayerGzip => write!(f, "application/vnd.oci.image.layer.v1.tar+gzip"),
            MediaType::OciLayerZstd => write!(f, "application/vnd.oci.image.layer.v1.tar+zstd"),
            MediaType::DockerManifestList => {
                write!(
                    f,
                    "application/vnd.docker.distribution.manifest.list.v2+json"
                )
            }
            MediaType::DockerManifestV2 => {
                write!(f, "application/vnd.docker.distribution.manifest.v2+json")
            }
            MediaType::DockerImageConfig => {
                write!(f, "application/vnd.docker.container.image.v1+json")
            }
            MediaType::DockerLayer => {
                write!(f, "application/vnd.docker.image.rootfs.diff.tar.gzip")
            }
            MediaType::Other(s) => write!(f, "{}", s),
        }
    }
}

/// Content descriptor - refers to a blob by digest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Descriptor {
    /// MIME type of the referenced content
    #[serde(rename = "mediaType")]
    pub media_type: String,

    /// Content hash (e.g., "sha256:abc123...")
    pub digest: String,

    /// Size in bytes
    pub size: u64,

    /// Optional URLs for direct access to content
    #[serde(skip_serializing_if = "Option::is_none")]
    pub urls: Option<Vec<String>>,

    /// Optional annotations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<HashMap<String, String>>,
}

// Descriptor is used as a data struct via serde deserialization

/// OCI Image Manifest
/// See: https://github.com/opencontainers/image-spec/blob/main/manifest.md
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OciManifest {
    /// Schema version (must be 2) - required for serde but not read directly
    #[serde(rename = "schemaVersion")]
    #[allow(dead_code)]
    pub schema_version: u32,

    /// Media type of the manifest
    #[serde(rename = "mediaType", skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,

    /// Reference to image configuration
    pub config: Descriptor,

    /// List of layer descriptors
    pub layers: Vec<Descriptor>,

    /// Optional annotations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<HashMap<String, String>>,
}

// OciManifest is used as a data struct via serde deserialization

/// Docker Image Manifest v2 Schema 2
/// Compatible with OCI manifest structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerManifestV2 {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u32,

    #[serde(rename = "mediaType")]
    pub media_type: String,

    pub config: Descriptor,

    pub layers: Vec<Descriptor>,
}

impl DockerManifestV2 {
    /// Convert to OCI manifest format
    pub fn to_oci_manifest(&self) -> OciManifest {
        OciManifest {
            schema_version: self.schema_version,
            media_type: Some(MediaType::OciManifest.to_string()),
            config: Descriptor {
                media_type: MediaType::OciImageConfig.to_string(),
                digest: self.config.digest.clone(),
                size: self.config.size,
                urls: self.config.urls.clone(),
                annotations: self.config.annotations.clone(),
            },
            layers: self
                .layers
                .iter()
                .map(|l| Descriptor {
                    media_type: if l.media_type.contains("gzip") {
                        MediaType::OciLayerGzip.to_string()
                    } else {
                        MediaType::OciLayer.to_string()
                    },
                    digest: l.digest.clone(),
                    size: l.size,
                    urls: l.urls.clone(),
                    annotations: l.annotations.clone(),
                })
                .collect(),
            annotations: None,
        }
    }
}

/// Platform specification for multi-arch images
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Platform {
    /// CPU architecture (e.g., "amd64", "arm64")
    pub architecture: String,

    /// Operating system (e.g., "linux", "windows")
    pub os: String,

    /// OS version (optional)
    #[serde(rename = "os.version", skip_serializing_if = "Option::is_none")]
    pub os_version: Option<String>,

    /// OS features (optional)
    #[serde(rename = "os.features", skip_serializing_if = "Option::is_none")]
    pub os_features: Option<Vec<String>>,

    /// CPU variant (e.g., "v8" for arm64)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
}

impl Platform {
    /// Check if this platform matches the current system
    pub fn matches_current(&self) -> bool {
        let current_arch = std::env::consts::ARCH;
        let current_os = std::env::consts::OS;

        let arch_match = match current_arch {
            "x86_64" => self.architecture == "amd64" || self.architecture == "x86_64",
            "aarch64" => self.architecture == "arm64" || self.architecture == "aarch64",
            _ => self.architecture == current_arch,
        };

        let os_match = self.os == current_os;

        arch_match && os_match
    }
}

/// Manifest descriptor with platform info (used in manifest lists)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestDescriptor {
    /// Base descriptor fields
    #[serde(flatten)]
    pub descriptor: Descriptor,

    /// Platform this manifest is for
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform: Option<Platform>,
}

/// OCI Image Index / Docker Manifest List
/// Used for multi-architecture images
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestList {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u32,

    #[serde(rename = "mediaType", skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,

    pub manifests: Vec<ManifestDescriptor>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<HashMap<String, String>>,
}

impl ManifestList {
    /// Find a manifest matching the current platform
    pub fn find_current_platform(&self) -> Option<&ManifestDescriptor> {
        self.manifests.iter().find(|m| {
            m.platform
                .as_ref()
                .map(|p| p.matches_current())
                .unwrap_or(false)
        })
    }

    /// Find a manifest for a specific platform
    pub fn find_platform(&self, arch: &str, os: &str) -> Option<&ManifestDescriptor> {
        self.manifests.iter().find(|m| {
            m.platform
                .as_ref()
                .map(|p| p.architecture == arch && p.os == os)
                .unwrap_or(false)
        })
    }
}

/// Parse any manifest type from JSON
pub fn parse_manifest(data: &[u8]) -> Result<ManifestKind, crate::image::ImageError> {
    // Try to detect the type from mediaType field
    #[derive(Deserialize)]
    struct MediaTypeProbe {
        #[serde(rename = "mediaType")]
        media_type: Option<String>,
        // Check for manifests array (indicates index/list)
        manifests: Option<Vec<serde_json::Value>>,
    }

    let probe: MediaTypeProbe = serde_json::from_slice(data)?;

    // If it has a manifests array, it's an index/list
    if probe.manifests.is_some() {
        let list: ManifestList = serde_json::from_slice(data)?;
        return Ok(ManifestKind::List(list));
    }

    // Check media type
    if let Some(media_type) = probe.media_type {
        match MediaType::from_str(&media_type) {
            MediaType::OciManifest => {
                let manifest: OciManifest = serde_json::from_slice(data)?;
                return Ok(ManifestKind::Oci(manifest));
            }
            MediaType::DockerManifestV2 => {
                let manifest: DockerManifestV2 = serde_json::from_slice(data)?;
                return Ok(ManifestKind::Docker(manifest));
            }
            MediaType::OciIndex | MediaType::DockerManifestList => {
                let list: ManifestList = serde_json::from_slice(data)?;
                return Ok(ManifestKind::List(list));
            }
            _ => {}
        }
    }

    // Default: try OCI manifest (most common)
    match serde_json::from_slice::<OciManifest>(data) {
        Ok(manifest) => Ok(ManifestKind::Oci(manifest)),
        Err(_) => {
            // Try Docker format
            let manifest: DockerManifestV2 = serde_json::from_slice(data)?;
            Ok(ManifestKind::Docker(manifest))
        }
    }
}

/// Enum representing different manifest types
#[derive(Debug, Clone)]
pub enum ManifestKind {
    /// OCI Image Manifest
    Oci(OciManifest),
    /// Docker Manifest v2
    Docker(DockerManifestV2),
    /// Manifest List (multi-arch)
    List(ManifestList),
}

// ManifestKind is used via pattern matching in registry client

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_oci_manifest() {
        let json = r#"{
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.image.config.v1+json",
                "digest": "sha256:abc123",
                "size": 1234
            },
            "layers": [
                {
                    "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                    "digest": "sha256:def456",
                    "size": 5678
                }
            ]
        }"#;

        let manifest: OciManifest = serde_json::from_str(json).unwrap();
        assert_eq!(manifest.schema_version, 2);
        assert_eq!(manifest.layers.len(), 1);
        assert_eq!(manifest.config.digest, "sha256:abc123");
    }

    #[test]
    fn test_platform_matching() {
        let _platform = Platform {
            architecture: "amd64".to_string(),
            os: "linux".to_string(),
            os_version: None,
            os_features: None,
            variant: None,
        };

        // This will depend on the test machine
        #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
        assert!(_platform.matches_current());
    }
}

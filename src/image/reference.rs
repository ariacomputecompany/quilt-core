//! Image Reference Parsing
//!
//! Parses Docker-style image references like:
//! - nginx
//! - nginx:1.25
//! - myuser/myapp:v1.0
//! - ghcr.io/owner/repo:tag
//! - registry.example.com:5000/app@sha256:abc...

use crate::image::ImageError;

/// Parsed image reference
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImageReference {
    /// Registry host (e.g., "docker.io", "ghcr.io")
    pub registry: String,

    /// Repository path (e.g., "library/nginx", "myuser/myapp")
    pub repository: String,

    /// Tag (e.g., "latest", "v1.0")
    pub tag: String,

    /// Digest (e.g., "sha256:abc123...") - takes precedence over tag
    pub digest: Option<String>,
}

impl ImageReference {
    /// Parse an image reference string
    ///
    /// Handles various formats:
    /// - `nginx` -> docker.io/library/nginx:latest
    /// - `nginx:1.25` -> docker.io/library/nginx:1.25
    /// - `myuser/myapp` -> docker.io/myuser/myapp:latest
    /// - `ghcr.io/owner/repo:tag`
    /// - `registry:5000/app@sha256:abc...`
    pub fn parse(s: &str) -> Result<Self, ImageError> {
        let s = s.trim();

        if s.is_empty() {
            return Err(ImageError::InvalidReference(
                "Empty image reference".to_string(),
            ));
        }

        // Split off digest if present
        let (ref_part, digest) = if let Some((r, d)) = s.split_once('@') {
            (r, Some(d.to_string()))
        } else {
            (s, None)
        };

        // Determine if the first component is a registry
        let parts: Vec<&str> = ref_part.splitn(2, '/').collect();

        let (registry, rest) = if parts.len() == 1 {
            // No slash - just an image name like "nginx"
            ("docker.io", parts[0])
        } else {
            let first = parts[0];
            // Check if first part looks like a registry (has dot, colon, or is "localhost")
            let is_registry = first.contains('.')
                || first.contains(':')
                || first == "localhost"
                || first.starts_with("localhost:");

            if is_registry {
                (first, parts[1])
            } else {
                // It's a user/repo format like "myuser/myapp"
                ("docker.io", ref_part)
            }
        };

        // Parse repository and tag from the rest
        let (repository, tag) = if let Some((r, t)) = rest.split_once(':') {
            (r.to_string(), t.to_string())
        } else {
            (rest.to_string(), "latest".to_string())
        };

        // Add "library/" prefix for Docker Hub official images
        let repository = if registry == "docker.io" && !repository.contains('/') {
            format!("library/{}", repository)
        } else {
            repository
        };

        // Normalize Docker Hub registry name
        let registry = match registry {
            "index.docker.io" | "registry-1.docker.io" | "registry.hub.docker.com" => {
                "docker.io".to_string()
            }
            r => r.to_string(),
        };

        Ok(Self {
            registry,
            repository,
            tag,
            digest,
        })
    }

    /// Create a copy of this reference with a different digest
    pub fn with_digest(&self, digest: &str) -> Self {
        Self {
            registry: self.registry.clone(),
            repository: self.repository.clone(),
            tag: "".to_string(),
            digest: Some(digest.to_string()),
        }
    }

    /// Get the full reference string
    pub fn full_name(&self) -> String {
        if let Some(digest) = &self.digest {
            format!("{}/{}@{}", self.registry, self.repository, digest)
        } else {
            format!("{}/{}:{}", self.registry, self.repository, self.tag)
        }
    }

    /// Get the API endpoint for this registry
    pub fn api_endpoint(&self) -> String {
        if self.registry == "docker.io" {
            "https://registry-1.docker.io".to_string()
        } else if self.registry.contains(':') || self.registry == "localhost" {
            // Local registry with port - use HTTP
            format!("http://{}", self.registry)
        } else {
            format!("https://{}", self.registry)
        }
    }

    /// Get the reference to use for API calls (tag or digest)
    pub fn api_reference(&self) -> &str {
        self.digest.as_deref().unwrap_or(&self.tag)
    }
}

impl std::fmt::Display for ImageReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.full_name())
    }
}

impl std::str::FromStr for ImageReference {
    type Err = ImageError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_image() {
        let r = ImageReference::parse("nginx").unwrap();
        assert_eq!(r.registry, "docker.io");
        assert_eq!(r.repository, "library/nginx");
        assert_eq!(r.tag, "latest");
        assert!(r.digest.is_none());
    }

    #[test]
    fn test_image_with_tag() {
        let r = ImageReference::parse("nginx:1.25").unwrap();
        assert_eq!(r.registry, "docker.io");
        assert_eq!(r.repository, "library/nginx");
        assert_eq!(r.tag, "1.25");
    }

    #[test]
    fn test_user_image() {
        let r = ImageReference::parse("myuser/myapp").unwrap();
        assert_eq!(r.registry, "docker.io");
        assert_eq!(r.repository, "myuser/myapp");
        assert_eq!(r.tag, "latest");
    }

    #[test]
    fn test_user_image_with_tag() {
        let r = ImageReference::parse("myuser/myapp:v1.0").unwrap();
        assert_eq!(r.registry, "docker.io");
        assert_eq!(r.repository, "myuser/myapp");
        assert_eq!(r.tag, "v1.0");
    }

    #[test]
    fn test_ghcr_image() {
        let r = ImageReference::parse("ghcr.io/owner/repo:latest").unwrap();
        assert_eq!(r.registry, "ghcr.io");
        assert_eq!(r.repository, "owner/repo");
        assert_eq!(r.tag, "latest");
    }

    #[test]
    fn test_local_registry() {
        let r = ImageReference::parse("localhost:5000/myapp:v1").unwrap();
        assert_eq!(r.registry, "localhost:5000");
        assert_eq!(r.repository, "myapp");
        assert_eq!(r.tag, "v1");
    }

    #[test]
    fn test_digest_reference() {
        let r = ImageReference::parse("nginx@sha256:abc123").unwrap();
        assert_eq!(r.registry, "docker.io");
        assert_eq!(r.repository, "library/nginx");
        assert_eq!(r.digest, Some("sha256:abc123".to_string()));
    }

    #[test]
    fn test_full_reference_with_digest() {
        let r = ImageReference::parse("ghcr.io/owner/repo@sha256:def456").unwrap();
        assert_eq!(r.registry, "ghcr.io");
        assert_eq!(r.repository, "owner/repo");
        assert_eq!(r.digest, Some("sha256:def456".to_string()));
    }

    #[test]
    fn test_api_endpoint() {
        let r1 = ImageReference::parse("nginx").unwrap();
        assert_eq!(r1.api_endpoint(), "https://registry-1.docker.io");

        let r2 = ImageReference::parse("ghcr.io/owner/repo").unwrap();
        assert_eq!(r2.api_endpoint(), "https://ghcr.io");

        let r3 = ImageReference::parse("localhost:5000/app").unwrap();
        assert_eq!(r3.api_endpoint(), "http://localhost:5000");
    }
}

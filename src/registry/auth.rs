//! Registry Authentication
//!
//! Implements Docker registry token authentication flow:
//! 1. Request to registry returns 401 with WWW-Authenticate header
//! 2. Parse realm, service, and scope from header
//! 3. Request token from auth server
//! 4. Use token for subsequent requests
//!
//! Also supports Docker credential helpers and config.json

use crate::registry::{RegistryError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::RwLock;

/// Registry credentials
#[derive(Debug, Clone)]
pub struct Credentials {
    pub username: String,
    pub password: String,
}

/// Auth token with optional expiry
#[derive(Debug, Clone)]
struct AuthToken {
    token: String,
    expires_at: Option<std::time::Instant>,
}

impl AuthToken {
    fn is_valid(&self) -> bool {
        match self.expires_at {
            Some(expiry) => std::time::Instant::now() < expiry,
            None => true,
        }
    }
}

/// Registry authentication manager
pub struct RegistryAuth {
    /// Cached tokens per scope
    tokens: RwLock<HashMap<String, AuthToken>>,

    /// Credentials per registry
    credentials: RwLock<HashMap<String, Credentials>>,

    /// HTTP client
    client: reqwest::Client,
}

impl RegistryAuth {
    /// Create a new auth manager
    pub fn new() -> Self {
        Self {
            tokens: RwLock::new(HashMap::new()),
            credentials: RwLock::new(HashMap::new()),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()
                .unwrap(),
        }
    }

    /// Load credentials from Docker config
    pub fn load_docker_config(&self) -> Result<()> {
        if let Some(config) = DockerConfig::load()? {
            let mut creds = self.credentials.write().map_err(|e| {
                RegistryError::AuthError(format!("Failed to acquire write lock: {}", e))
            })?;

            // Load from auths
            for (registry, auth) in config.auths {
                if let Some(decoded) = decode_docker_auth(&auth) {
                    creds.insert(normalize_registry(&registry), decoded);
                }
            }
        }

        Ok(())
    }

    /// Get a valid token for a scope, authenticating if necessary
    pub async fn get_token(&self, www_authenticate: &str, registry: &str) -> Result<String> {
        // Parse WWW-Authenticate header
        let params = parse_www_authenticate(www_authenticate)?;

        let realm = params.get("realm").ok_or_else(|| {
            RegistryError::AuthError("Missing realm in WWW-Authenticate".to_string())
        })?;

        let service = params.get("service").map(|s| s.as_str()).unwrap_or("");
        let scope = params.get("scope").map(|s| s.as_str()).unwrap_or("");

        // Create cache key
        let cache_key = format!("{}:{}:{}", registry, service, scope);

        // Check cache
        {
            let tokens = self.tokens.read().map_err(|e| {
                RegistryError::AuthError(format!("Failed to acquire read lock: {}", e))
            })?;
            if let Some(token) = tokens.get(&cache_key) {
                if token.is_valid() {
                    return Ok(token.token.clone());
                }
            }
        }

        // Request new token
        let token = self.request_token(realm, service, scope, registry).await?;

        // Cache token
        {
            let mut tokens = self.tokens.write().map_err(|e| {
                RegistryError::AuthError(format!("Failed to acquire write lock: {}", e))
            })?;
            tokens.insert(
                cache_key,
                AuthToken {
                    token: token.clone(),
                    // Assume 5 minute validity if not specified
                    expires_at: Some(
                        std::time::Instant::now() + std::time::Duration::from_secs(300),
                    ),
                },
            );
        }

        Ok(token)
    }

    /// Request a token from the auth server
    async fn request_token(
        &self,
        realm: &str,
        service: &str,
        scope: &str,
        registry: &str,
    ) -> Result<String> {
        let mut request = self.client.get(realm);

        // Add query parameters
        if !service.is_empty() {
            request = request.query(&[("service", service)]);
        }
        if !scope.is_empty() {
            request = request.query(&[("scope", scope)]);
        }

        // Add credentials if available - clone to avoid holding lock across await
        let cred_opt = {
            let creds = self.credentials.read().map_err(|e| {
                RegistryError::AuthError(format!("Failed to acquire read lock: {}", e))
            })?;
            let normalized = normalize_registry(registry);
            creds.get(&normalized).cloned()
        };

        if let Some(cred) = cred_opt {
            request = request.basic_auth(&cred.username, Some(&cred.password));
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            return Err(RegistryError::AuthError(format!(
                "Token request failed: {} {}",
                response.status(),
                response.text().await.unwrap_or_default()
            )));
        }

        // Parse token response
        #[derive(Deserialize)]
        struct TokenResponse {
            token: Option<String>,
            access_token: Option<String>,
            #[allow(dead_code)]
            expires_in: Option<u64>,
        }

        let token_resp: TokenResponse = response.json().await?;

        token_resp
            .token
            .or(token_resp.access_token)
            .ok_or_else(|| RegistryError::AuthError("No token in response".to_string()))
    }
}

impl Default for RegistryAuth {
    fn default() -> Self {
        Self::new()
    }
}

/// Docker config.json structure
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DockerConfig {
    /// Registry auths (base64 encoded credentials)
    #[serde(default)]
    pub auths: HashMap<String, DockerAuthEntry>,

    /// Credential store (e.g., "osxkeychain", "secretservice")
    #[serde(rename = "credsStore", skip_serializing_if = "Option::is_none")]
    pub creds_store: Option<String>,

    /// Per-registry credential helpers
    #[serde(rename = "credHelpers", default)]
    pub cred_helpers: HashMap<String, String>,
}

/// Docker auth entry
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DockerAuthEntry {
    /// Base64-encoded "username:password"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth: Option<String>,

    /// Username
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,

    /// Password
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,

    /// Email (legacy)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,

    /// Identity token
    #[serde(rename = "identitytoken", skip_serializing_if = "Option::is_none")]
    pub identity_token: Option<String>,
}

impl DockerConfig {
    /// Load Docker config from default location
    pub fn load() -> Result<Option<Self>> {
        let config_path = Self::config_path()?;

        if !config_path.exists() {
            return Ok(None);
        }

        let contents = std::fs::read_to_string(&config_path)?;
        let config: DockerConfig = serde_json::from_str(&contents)?;

        Ok(Some(config))
    }

    /// Get the Docker config path
    pub fn config_path() -> Result<PathBuf> {
        // Check DOCKER_CONFIG env var first
        if let Ok(path) = std::env::var("DOCKER_CONFIG") {
            return Ok(PathBuf::from(path).join("config.json"));
        }

        // Default to ~/.docker/config.json
        let home = dirs::home_dir().ok_or_else(|| {
            RegistryError::AuthError("Could not determine home directory".to_string())
        })?;

        Ok(home.join(".docker").join("config.json"))
    }
}

/// Parse WWW-Authenticate header
fn parse_www_authenticate(header: &str) -> Result<HashMap<String, String>> {
    let mut params = HashMap::new();

    // Format: Bearer realm="...",service="...",scope="..."
    let header = header.strip_prefix("Bearer ").unwrap_or(header);

    for part in header.split(',') {
        let part = part.trim();
        if let Some((key, value)) = part.split_once('=') {
            let value = value.trim_matches('"');
            params.insert(key.to_string(), value.to_string());
        }
    }

    Ok(params)
}

/// Decode Docker auth entry to credentials
fn decode_docker_auth(entry: &DockerAuthEntry) -> Option<Credentials> {
    // Try explicit username/password first
    if let (Some(username), Some(password)) = (&entry.username, &entry.password) {
        return Some(Credentials {
            username: username.clone(),
            password: password.clone(),
        });
    }

    // Try base64-encoded auth
    if let Some(auth) = &entry.auth {
        if let Ok(decoded) =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, auth)
        {
            if let Ok(s) = String::from_utf8(decoded) {
                if let Some((username, password)) = s.split_once(':') {
                    return Some(Credentials {
                        username: username.to_string(),
                        password: password.to_string(),
                    });
                }
            }
        }
    }

    // Try identity token
    if let Some(token) = &entry.identity_token {
        return Some(Credentials {
            username: "<token>".to_string(),
            password: token.clone(),
        });
    }

    None
}

/// Normalize registry name
fn normalize_registry(registry: &str) -> String {
    match registry {
        "docker.io" | "index.docker.io" | "registry-1.docker.io" => {
            "https://index.docker.io/v1/".to_string()
        }
        r if r.starts_with("http://") || r.starts_with("https://") => r.to_string(),
        r => format!("https://{}", r),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_www_authenticate() {
        let header = r#"Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/nginx:pull""#;

        let params = parse_www_authenticate(header).unwrap();

        assert_eq!(
            params.get("realm"),
            Some(&"https://auth.docker.io/token".to_string())
        );
        assert_eq!(
            params.get("service"),
            Some(&"registry.docker.io".to_string())
        );
        assert_eq!(
            params.get("scope"),
            Some(&"repository:library/nginx:pull".to_string())
        );
    }

    #[test]
    fn test_decode_docker_auth() {
        let entry = DockerAuthEntry {
            auth: Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                "testuser:testpass",
            )),
            ..Default::default()
        };

        let creds = decode_docker_auth(&entry).unwrap();
        assert_eq!(creds.username, "testuser");
        assert_eq!(creds.password, "testpass");
    }

    #[test]
    fn test_normalize_registry() {
        assert_eq!(
            normalize_registry("docker.io"),
            "https://index.docker.io/v1/"
        );
        assert_eq!(normalize_registry("ghcr.io"), "https://ghcr.io");
    }
}

use std::collections::HashMap;

/// Mount type enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum MountType {
    Bind,
    Volume,
    Tmpfs,
}

/// Volume mount configuration
#[derive(Debug, Clone)]
pub struct VolumeMount {
    pub source: String,
    pub target: String,
    pub mount_type: MountType,
    pub readonly: bool,
    pub options: HashMap<String, String>,
}

pub struct InputValidator;

impl InputValidator {
    /// Parse key=value pairs from strings
    pub fn parse_key_val(s: &str) -> Result<(String, String), String> {
        let pos = s
            .find('=')
            .ok_or_else(|| format!("Invalid KEY=VALUE format: '{}'", s))?;

        let key = s[..pos].trim().to_string();
        let value = s[pos + 1..].trim().to_string();

        if key.is_empty() {
            return Err("Empty key in KEY=VALUE pair".to_string());
        }

        Ok((key, value))
    }

    /// Parse volume mount specification (-v flag format)
    /// Format: source:target[:options] or name:target[:options]
    pub fn parse_volume(s: &str) -> Result<VolumeMount, String> {
        let parts: Vec<&str> = s.split(':').collect();

        if parts.len() < 2 || parts.len() > 3 {
            return Err("Volume format must be '[name:]source:dest[:options]'".to_string());
        }

        let source = parts[0].trim().to_string();
        let target = parts[1].trim().to_string();
        let options = if parts.len() == 3 {
            parts[2].trim()
        } else {
            "rw"
        };

        if source.is_empty() || target.is_empty() {
            return Err("Volume source and target cannot be empty".to_string());
        }

        if !target.starts_with('/') {
            return Err("Container path must be absolute".to_string());
        }

        // Determine mount type based on source
        let mount_type = if source.starts_with('/') {
            MountType::Bind
        } else if source
            .chars()
            .all(|c| c.is_alphanumeric() || c == '-' || c == '_')
        {
            MountType::Volume
        } else {
            // Assume bind mount for other patterns (security validation will catch issues later)
            MountType::Bind
        };

        // Parse options
        let readonly = options.contains("ro");

        Ok(VolumeMount {
            source,
            target,
            mount_type,
            readonly,
            options: HashMap::new(),
        })
    }

    /// Parse advanced mount specification (--mount flag format)
    /// Format: type=bind,source=/host/path,target=/container/path,readonly
    #[allow(dead_code)]
    pub fn parse_mount(s: &str) -> Result<VolumeMount, String> {
        let mut mount = VolumeMount {
            source: String::new(),
            target: String::new(),
            mount_type: MountType::Bind,
            readonly: false,
            options: HashMap::new(),
        };

        for part in s.split(',') {
            let trimmed = part.trim();

            // Handle boolean flags (no value)
            if trimmed == "readonly" || trimmed == "ro" {
                mount.readonly = true;
                continue;
            }

            // Handle key=value pairs
            let kv: Vec<&str> = trimmed.split('=').collect();
            if kv.len() != 2 {
                return Err(format!("Invalid mount option: '{}'", part));
            }

            let key = kv[0].trim();
            let value = kv[1].trim();

            match key {
                "type" => {
                    mount.mount_type = match value {
                        "bind" => MountType::Bind,
                        "volume" => MountType::Volume,
                        "tmpfs" => MountType::Tmpfs,
                        _ => return Err(format!("Unknown mount type: '{}'", value)),
                    };
                }
                "source" | "src" => mount.source = value.to_string(),
                "target" | "dst" | "destination" => mount.target = value.to_string(),
                "readonly" | "ro" => mount.readonly = true,
                _ => {
                    mount.options.insert(key.to_string(), value.to_string());
                }
            }
        }

        // Validate required fields
        if mount.target.is_empty() {
            return Err("Mount target is required".to_string());
        }

        if mount.mount_type != MountType::Tmpfs && mount.source.is_empty() {
            return Err("Mount source is required for bind and volume mounts".to_string());
        }

        if !mount.target.starts_with('/') {
            return Err("Mount target must be an absolute path".to_string());
        }

        Ok(mount)
    }
}

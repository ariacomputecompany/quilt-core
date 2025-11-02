use std::process::Command;
use std::fs;

#[allow(dead_code)] // Used by CLI in debug mode only
#[derive(Debug, Clone)]
pub struct CommandResult {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
}

#[allow(dead_code)] // Used by CLI in debug mode only
pub struct CommandExecutor;

#[allow(dead_code)] // Used by CLI in debug mode only
impl CommandExecutor {
    /// Execute a shell command and return result
    pub fn execute_shell(command: &str) -> Result<CommandResult, String> {
        let output = Command::new("sh")
            .arg("-c")
            .arg(command)
            .output()
            .map_err(|e| format!("Failed to execute command '{}': {}", command, e))?;

        Ok(CommandResult {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code: output.status.code(),
        })
    }

    /// Check if a command is available in the system PATH
    pub fn is_command_available(command: &str) -> bool {
        Command::new("which")
            .arg(command)
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    /// Execute a package manager command
    pub fn execute_package_manager(
        package_manager: &str,
        action: &str,
        packages: &[&str],
    ) -> Result<CommandResult, String> {
        if !Self::is_command_available(package_manager) {
            return Err(format!("Package manager '{}' not available", package_manager));
        }

        let mut cmd = Command::new(package_manager);
        cmd.arg(action);

        // Add common flags based on package manager
        match package_manager {
            "apt" | "apt-get" => {
                cmd.arg("-y"); // Assume yes to prompts
                if action == "install" || action == "upgrade" {
                    cmd.arg("--no-install-recommends"); // Minimal installation
                }
            }
            "yum" | "dnf" => {
                cmd.arg("-y"); // Assume yes to prompts
            }
            "apk" => {
                cmd.arg("--no-cache"); // Don't cache packages
            }
            _ => {} // Other package managers use defaults
        }

        // Add packages if provided
        for package in packages {
            cmd.arg(package);
        }

        let output = cmd
            .output()
            .map_err(|e| format!("Failed to execute {} {}: {}", package_manager, action, e))?;

        Ok(CommandResult {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code: output.status.code(),
        })
    }

    /// Check if a binary is linked to Nix store paths
    pub fn is_nix_linked_binary(path: &str) -> bool {
        // Check if the binary itself is in /nix/store
        if path.starts_with("/nix/store") {
            return true;
        }

        // Check if the binary is a symlink pointing to /nix/store
        if let Ok(metadata) = fs::symlink_metadata(path) {
            if metadata.file_type().is_symlink() {
                if let Ok(target) = fs::read_link(path) {
                    if target.to_string_lossy().contains("/nix/store") {
                        return true;
                    }
                }
            }
        }

        // Check if the binary has Nix store dependencies using ldd
        if let Ok(output) = Command::new("ldd").arg(path).output() {
            if output.status.success() {
                let ldd_output = String::from_utf8_lossy(&output.stdout);
                return ldd_output.contains("/nix/store");
            }
        }

        // Fallback: check if readelf shows Nix store paths in dynamic section
        if let Ok(output) = Command::new("readelf").arg("-d").arg(path).output() {
            if output.status.success() {
                let readelf_output = String::from_utf8_lossy(&output.stdout);
                return readelf_output.contains("/nix/store");
            }
        }

        false
    }
}
use crate::utils::command::CommandExecutor;
use crate::utils::console::ConsoleLogger;
use crate::utils::filesystem::FileSystemUtils;
use std::env;

pub struct SystemRuntime;

impl SystemRuntime {
    pub fn new() -> Self {
        SystemRuntime
    }

    /// Initialize the basic container environment
    pub fn initialize_container_environment(&self) -> Result<(), String> {
        ConsoleLogger::debug("Initializing container system environment...");

        // Set up basic environment variables
        self.setup_environment_variables()?;

        // Verify basic system binaries
        self.verify_system_binaries()?;

        // Initialize basic directories
        self.initialize_basic_directories()?;

        ConsoleLogger::success("Container system environment initialized");
        Ok(())
    }

    /// Set up essential environment variables
    fn setup_environment_variables(&self) -> Result<(), String> {
        // Set PATH to include both traditional and Nix store locations
        let path_dirs = vec![
            "/usr/local/sbin",
            "/usr/local/bin",
            "/usr/sbin",
            "/usr/bin",
            "/sbin",
            "/bin",
            "/nix/store/*/bin", // Include potential Nix store paths
        ];

        let path = path_dirs.join(":");
        env::set_var("PATH", &path);

        // Set other essential environment variables
        env::set_var("HOME", "/root");
        env::set_var("USER", "root");
        env::set_var("SHELL", "/bin/sh");
        env::set_var("TERM", "xterm");

        // Nix-specific environment variables
        env::set_var("NIX_SSL_CERT_FILE", "/etc/ssl/certs/ca-certificates.crt");

        ConsoleLogger::debug("Environment variables set (PATH, HOME, USER, SHELL, TERM)");
        Ok(())
    }

    /// Verify that basic system binaries are available
    fn verify_system_binaries(&self) -> Result<(), String> {
        // Check for basic shell first
        let shell_candidates = vec!["/bin/sh", "/bin/bash"];
        let mut working_shell = None;

        for shell in &shell_candidates {
            if FileSystemUtils::is_file(shell) && FileSystemUtils::is_executable(shell) {
                working_shell = Some(shell);
                ConsoleLogger::debug(&format!("Found executable shell: {}", shell));
                break;
            }
        }

        if let Some(shell) = working_shell {
            ConsoleLogger::debug(&format!("Working shell found: {}", shell));
            env::set_var("SHELL", shell);
        } else {
            // More forgiving error - warn but don't fail
            ConsoleLogger::warning("No shell found, but continuing anyway");
            ConsoleLogger::info("Container execution will depend on command availability");
            env::set_var("SHELL", "/bin/sh"); // Set default
        }

        // Verify we can find basic commands (but don't execute them in chroot)
        let test_commands = vec!["echo", "ls", "cat"];

        for cmd in test_commands {
            let cmd_path = format!("/bin/{}", cmd);
            if FileSystemUtils::is_file(&cmd_path) && FileSystemUtils::is_executable(&cmd_path) {
                ConsoleLogger::debug(&format!("Command '{}' available and executable", cmd));
            } else {
                ConsoleLogger::warning(&format!("Command '{}' not found or not executable", cmd));
            }
        }

        Ok(())
    }

    /// Initialize basic directories that should exist in containers
    fn initialize_basic_directories(&self) -> Result<(), String> {
        let basic_dirs = vec!["/tmp", "/var/log", "/var/tmp", "/root"];

        for dir in &basic_dirs {
            if !FileSystemUtils::is_directory(dir) {
                match FileSystemUtils::create_dir_all_with_logging(dir, "basic container directory")
                {
                    Ok(_) => {
                        ConsoleLogger::debug(&format!("Created directory: {}", dir));
                    }
                    Err(e) => {
                        ConsoleLogger::warning(&format!(
                            "Failed to create directory {}: {}",
                            dir, e
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Check if a package manager is available and functional
    pub fn check_package_manager_availability(&self) -> Result<String, String> {
        // First check if we're in a Nix environment
        if self.check_nix_environment() {
            ConsoleLogger::debug("Nix environment detected");
            return Ok("nix".to_string());
        }

        // Check for Debian/Ubuntu apt
        if CommandExecutor::is_command_available("apt") {
            ConsoleLogger::debug("Package manager detected: apt (Debian/Ubuntu)");
            return Ok("apt".to_string());
        }

        // Check for RedHat/CentOS yum
        if CommandExecutor::is_command_available("yum") {
            ConsoleLogger::debug("Package manager detected: yum (RedHat/CentOS)");
            return Ok("yum".to_string());
        }

        // Check for newer dnf
        if CommandExecutor::is_command_available("dnf") {
            ConsoleLogger::debug("Package manager detected: dnf (Fedora/newer RedHat)");
            return Ok("dnf".to_string());
        }

        // Fallback: assume we can work without a package manager
        ConsoleLogger::warning("No traditional package manager found, using basic environment");
        Ok("none".to_string())
    }

    /// Check if we're running in a Nix-generated environment
    fn check_nix_environment(&self) -> bool {
        // Check for Nix store paths in filesystem
        if FileSystemUtils::is_directory("/nix/store") {
            return true;
        }

        // Check if binaries are from Nix store
        if let Ok(result) = CommandExecutor::execute_shell("ls -la /bin/* 2>/dev/null | head -5") {
            if result.stdout.contains("/nix/store") {
                return true;
            }
        }

        // Check for Nix-style directory structure
        let nix_indicators = vec!["/nix", "/nix/store"];

        for indicator in nix_indicators {
            if FileSystemUtils::is_directory(indicator) {
                return true;
            }
        }

        false
    }

    /// Prepare the container for package installation
    pub fn prepare_for_package_installation(&self, package_manager: &str) -> Result<(), String> {
        ConsoleLogger::debug("Preparing container for package installation...");

        match package_manager {
            "nix" => self.prepare_nix_environment(),
            "apt" => self.prepare_apt_environment(),
            "yum" | "dnf" => self.prepare_rpm_environment(),
            "none" => {
                ConsoleLogger::debug("No package manager preparation needed");
                Ok(())
            }
            _ => Err(format!("Unsupported package manager: {}", package_manager)),
        }
    }

    /// Prepare Nix environment (mostly verification)
    fn prepare_nix_environment(&self) -> Result<(), String> {
        ConsoleLogger::debug("Nix environment detected - packages are pre-installed in rootfs");
        ConsoleLogger::info(
            "Nix setup commands will install packages directly without package manager",
        );
        Ok(())
    }

    /// Prepare Debian/Ubuntu apt environment  
    fn prepare_apt_environment(&self) -> Result<(), String> {
        // Update package index
        ConsoleLogger::progress("Updating apt package index...");
        match CommandExecutor::execute_package_manager("apt", "update", &[]) {
            Ok(result) => {
                if result.success {
                    ConsoleLogger::success("APT package index updated");
                } else {
                    ConsoleLogger::warning(&format!("APT update failed: {}", result.stderr));
                }
            }
            Err(e) => {
                return Err(format!("Failed to update APT package index: {}", e));
            }
        }

        Ok(())
    }

    /// Prepare RPM-based environment (yum/dnf)
    fn prepare_rpm_environment(&self) -> Result<(), String> {
        // RPM systems typically don't need explicit index updates
        ConsoleLogger::debug("RPM-based system ready for package installation");
        Ok(())
    }

    /// Install a runtime environment (e.g., python3, nodejs, etc.)
    pub fn install_runtime(
        &self,
        package_manager: &str,
        runtime_name: &str,
        packages: &[&str],
    ) -> Result<(), String> {
        ConsoleLogger::runtime_installing(runtime_name);

        match package_manager {
            "nix" => {
                ConsoleLogger::info(&format!(
                    "Nix environment: {} runtime should already be available",
                    runtime_name
                ));
                ConsoleLogger::debug(&format!("Requested packages: {:?}", packages));

                // For Nix, we assume packages are already available in the environment
                // but we can check if they're actually present
                for package in packages {
                    if CommandExecutor::is_command_available(package) {
                        ConsoleLogger::debug(&format!("Package '{}' available", package));
                    } else {
                        ConsoleLogger::warning(&format!("Package '{}' not found in PATH", package));
                    }
                }

                Ok(())
            }
            "none" => {
                ConsoleLogger::info(&format!(
                    "No package manager: {} runtime should be pre-installed",
                    runtime_name
                ));
                Ok(())
            }
            _ => {
                ConsoleLogger::progress(&format!("Installing packages: {:?}", packages));
                match CommandExecutor::execute_package_manager(package_manager, "install", packages)
                {
                    Ok(result) => {
                        if result.success {
                            ConsoleLogger::runtime_installed(runtime_name);

                            // Print installation output for debugging
                            if !result.stdout.trim().is_empty() {
                                ConsoleLogger::debug(&format!(
                                    "Installation output: {}",
                                    result.stdout.trim()
                                ));
                            }

                            Ok(())
                        } else {
                            Err(format!(
                                "Failed to install {} runtime: {}",
                                runtime_name, result.stderr
                            ))
                        }
                    }
                    Err(e) => Err(format!(
                        "Failed to execute package installation command: {}",
                        e
                    )),
                }
            }
        }
    }
}

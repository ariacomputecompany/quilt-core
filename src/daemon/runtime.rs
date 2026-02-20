use crate::daemon::cgroup::{CgroupLimits, CgroupManager};
use crate::daemon::manager::RuntimeManager;
use crate::daemon::namespace::{NamespaceConfig, NamespaceManager};
use crate::daemon::readiness::{
    cleanup_readiness_signal, ContainerReadinessManager, ReadinessConfig,
};
use crate::daemon::resource::ResourceManager;
use crate::icc::network::security::NetworkSecurity;
use crate::icc::network::{ContainerNetworkConfig, NetworkManager};
use crate::sync::ContainerState;
use crate::utils::command::CommandExecutor;
use crate::utils::console::ConsoleLogger;
use crate::utils::filesystem::FileSystemUtils;
use crate::utils::process::ProcessUtils;
use flate2::read::GzDecoder;
use nix::unistd::{chdir, chroot, execv, Pid};
use std::collections::HashMap;
use std::ffi::CString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use tar::Archive;

#[derive(Debug, Clone)]
pub struct ContainerConfig {
    pub image_path: String,
    pub command: Vec<String>,
    pub environment: HashMap<String, String>,
    pub setup_commands: Vec<String>, // Setup commands specification
    pub resource_limits: Option<CgroupLimits>,
    pub namespace_config: Option<NamespaceConfig>,
    #[allow(dead_code)]
    pub working_directory: Option<String>,
    pub mounts: Vec<MountConfig>,
}

#[derive(Debug, Clone)]
pub struct MountConfig {
    pub source: String,
    pub target: String,
    pub mount_type: MountType,
    pub readonly: bool,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MountType {
    Bind,
    Volume,
    Tmpfs,
}

impl Default for ContainerConfig {
    fn default() -> Self {
        ContainerConfig {
            image_path: String::new(),
            command: vec!["/bin/sh".to_string()],
            environment: HashMap::new(),
            setup_commands: vec![],
            resource_limits: Some(CgroupLimits::default()),
            namespace_config: Some(NamespaceConfig::default()),
            working_directory: None,
            mounts: vec![],
        }
    }
}

#[derive(Debug)]
pub struct Container {
    #[allow(dead_code)]
    pub id: String,
    pub config: ContainerConfig,
    pub state: ContainerState,
    pub logs: Vec<String>,
    pub pid: Option<Pid>,
    pub rootfs_path: String,
    pub created_at: u64,
    pub network_config: Option<ContainerNetworkConfig>,
    // Task management to prevent leaks
    pub monitoring_task: Option<tokio::task::JoinHandle<()>>,
}

impl Clone for Container {
    fn clone(&self) -> Self {
        Container {
            id: self.id.clone(),
            config: self.config.clone(),
            state: self.state.clone(),
            logs: self.logs.clone(),
            pid: self.pid,
            rootfs_path: self.rootfs_path.clone(),
            created_at: self.created_at,
            network_config: self.network_config.clone(),
            // JoinHandle cannot be cloned, so we set it to None
            monitoring_task: None,
        }
    }
}

impl Container {
    pub fn new(id: String, config: ContainerConfig) -> Self {
        let timestamp = ProcessUtils::get_timestamp();

        Container {
            id: id.clone(),
            config,
            state: ContainerState::Created,
            logs: Vec::new(),
            pid: None,
            rootfs_path: format!("/tmp/quilt-containers/{}", id),
            created_at: timestamp,
            network_config: None,
            monitoring_task: None,
        }
    }

    pub fn add_log(&mut self, message: String) {
        let _timestamp = ProcessUtils::get_timestamp();

        self.logs.push(message);
    }
}

pub struct ContainerRuntime {
    containers: Arc<tokio::sync::Mutex<HashMap<String, Container>>>,
    namespace_manager: NamespaceManager,
    runtime_manager: RuntimeManager,
    readiness_manager: ContainerReadinessManager,
}

impl ContainerRuntime {
    pub fn new() -> Self {
        ContainerRuntime {
            containers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            namespace_manager: NamespaceManager::new(),
            runtime_manager: RuntimeManager::new(),
            readiness_manager: ContainerReadinessManager::new(ReadinessConfig::default()),
        }
    }

    pub fn create_container(&self, id: String, config: ContainerConfig) -> Result<(), String> {
        ConsoleLogger::progress(&format!("Creating container: {}", id));

        let container = Container::new(id.clone(), config);
        let rootfs_path = container.rootfs_path.clone();

        // Lock-free container insertion
        if let Ok(mut containers) = self.containers.try_lock() {
            containers.insert(id.clone(), container);
        } else {
            return Err(format!("Failed to lock containers for insertion of {}", id));
        }

        // Setup rootfs
        ConsoleLogger::debug(&format!("Setting up rootfs for {} at {}", id, rootfs_path));
        if let Err(e) = self.setup_rootfs(&id) {
            ConsoleLogger::error(&format!("[CREATE] Rootfs setup failed for {}: {}", id, e));
            // Rollback: remove container from map
            if let Ok(mut containers) = self.containers.try_lock() {
                containers.remove(&id);
            }
            return Err(e);
        }

        // Verify rootfs was actually created and has content
        if !std::path::Path::new(&rootfs_path).exists() {
            ConsoleLogger::error(&format!(
                "Rootfs path {} was not created after setup",
                rootfs_path
            ));
            if let Ok(mut containers) = self.containers.try_lock() {
                containers.remove(&id);
            }
            return Err(format!("Rootfs creation failed for container {}", id));
        }

        // Check if rootfs has any content
        let entries = FileSystemUtils::list_dir(&rootfs_path)
            .map_err(|e| format!("Failed to read rootfs directory: {}", e))?;
        if entries.is_empty() {
            ConsoleLogger::error(&format!("Rootfs {} is empty after extraction", rootfs_path));
            if let Ok(mut containers) = self.containers.try_lock() {
                containers.remove(&id);
            }
            return Err(format!("Rootfs extraction failed - directory is empty"));
        }

        self.update_container_state(&id, ContainerState::Starting);

        ConsoleLogger::container_created(&id);
        ConsoleLogger::success(&format!(
            "Container {} created with rootfs at {}",
            id, rootfs_path
        ));
        Ok(())
    }

    pub fn register_existing_container(
        &self,
        id: String,
        config: ContainerConfig,
        rootfs_path: String,
    ) -> Result<(), String> {
        ConsoleLogger::progress(&format!("Registering existing container: {}", id));

        // Verify rootfs exists before creating container
        if !std::path::Path::new(&rootfs_path).exists() {
            return Err(format!("Rootfs path {} does not exist", rootfs_path));
        }

        let mut container = Container::new(id.clone(), config);
        container.rootfs_path = rootfs_path.clone();

        // Lock-free container insertion
        if let Ok(mut containers) = self.containers.try_lock() {
            containers.insert(id.clone(), container);
        } else {
            return Err(format!("Failed to lock containers for insertion of {}", id));
        }

        self.update_container_state(&id, ContainerState::Starting);

        ConsoleLogger::debug(&format!(
            "Registered existing container {} with rootfs {}",
            id, rootfs_path
        ));
        Ok(())
    }

    pub fn start_container(
        &self,
        id: &str,
        network_config: Option<ContainerNetworkConfig>,
    ) -> Result<(), String> {
        ConsoleLogger::progress(&format!("[START] Starting container: {}", id));

        // Get container configuration (lock-free read)
        let (config, rootfs_path) = if let Ok(containers) = self.containers.try_lock() {
            if let Some(container) = containers.get(id) {
                (container.config.clone(), container.rootfs_path.clone())
            } else {
                return Err(format!("Container {} not found", id));
            }
        } else {
            return Err(format!("Failed to lock containers for {}", id));
        };

        // Register mounts with ResourceManager
        let mount_points = vec![
            format!("{}/proc", rootfs_path),
            format!("{}/sys", rootfs_path),
            format!("{}/dev/pts", rootfs_path),
            rootfs_path.clone(),
        ];
        let resource_manager = ResourceManager::global();
        resource_manager.register_mounts(id, mount_points);

        // Register network config with ResourceManager if available
        if let Some(ref net_config) = network_config {
            resource_manager.register_network(id, net_config.clone());
        }

        // Create cgroups
        let mut cgroup_manager = CgroupManager::new(id.to_string());
        if let Some(limits) = &config.resource_limits {
            // SECURITY: Check resource limits before applying
            let security = NetworkSecurity::new("192.168.100.1".to_string()); // Bridge IP placeholder
            if let Err(e) = security.check_resource_limits(id) {
                ConsoleLogger::warning(&format!("Resource limit security check failed: {}", e));
            }
            if let Err(e) = cgroup_manager.create_cgroups(limits) {
                ConsoleLogger::warning(&format!("Failed to create cgroups: {}", e));
            }
        }

        // Parse and execute setup commands
        let setup_commands = if !config.setup_commands.is_empty() {
            let setup_spec = config.setup_commands.join("\n");
            self.runtime_manager.parse_setup_spec(&setup_spec)?
        } else {
            vec![]
        };

        // Create namespaced process for container execution
        let namespace_config = config.namespace_config.unwrap_or_default();

        // Reduce memory footprint - prepare everything needed outside the closure
        let id_for_logs = id.to_string();
        let command_for_logs = format!("{:?}", config.command);

        // Add log entry (per-container lock)
        if let Ok(mut containers) = self.containers.try_lock() {
            if let Some(container) = containers.get_mut(id) {
                container.add_log(format!(
                    "Starting container execution with command: {}",
                    command_for_logs
                ));
            }
        }

        // Prepare all data needed by child process (avoid heavy captures)
        // ENHANCED: Inject readiness check into command
        let enhanced_command = self
            .readiness_manager
            .inject_readiness_into_command(id, config.command.clone());
        let command_clone = enhanced_command;
        let environment_clone = config.environment.clone();
        let rootfs_path_clone = rootfs_path.clone();
        let setup_commands_clone = setup_commands.clone();
        let network_enabled = namespace_config.network; // Capture network flag for child process
        let mounts_clone = config.mounts.clone();

        // Create new lightweight runtime manager for child (not clone of existing)
        let child_func = move || -> i32 {
            // This runs in the child process with new namespaces
            // Keep memory allocation to minimum in child process

            // Setup mount namespace
            let namespace_manager = NamespaceManager::new();
            if let Err(e) = namespace_manager.setup_mount_namespace(&rootfs_path_clone) {
                eprintln!("Failed to setup mount namespace: {}", e);
                return 1;
            }

            // Setup container mounts (volumes, bind mounts, tmpfs)
            if !mounts_clone.is_empty() {
                println!(
                    "DEBUG: Setting up {} mounts before chroot",
                    mounts_clone.len()
                );
                if let Err(e) =
                    namespace_manager.setup_container_mounts(&rootfs_path_clone, &mounts_clone)
                {
                    eprintln!("Failed to setup container mounts: {}", e);
                    // Non-fatal, continue - container can run without extra mounts
                } else {
                    println!("DEBUG: Mounts setup complete");
                }
            }

            // Setup basic network namespace ONLY if networking is enabled
            if network_enabled {
                if let Err(e) = namespace_manager.setup_network_namespace() {
                    eprintln!("Failed to setup network namespace: {}", e);
                    // Non-fatal, continue
                }
            } else {
                println!("Skipping network namespace setup (networking disabled for container)");
            }

            // Set container hostname
            if let Err(e) = namespace_manager.set_container_hostname(&id_for_logs) {
                eprintln!("Failed to set container hostname: {}", e);
                // Non-fatal, continue
            }

            // Change root to container filesystem
            println!("DEBUG: About to chroot to {}", rootfs_path_clone);
            if let Err(e) = chroot(rootfs_path_clone.as_str()) {
                eprintln!("Failed to chroot to {}: {}", rootfs_path_clone, e);
                return 1;
            }

            // Change to root directory inside container
            if let Err(e) = chdir("/") {
                eprintln!("Failed to chdir to /: {}", e);
                return 1;
            }

            // NOW wait for network configuration AFTER chroot if networking is enabled
            if network_enabled {
                // Ensure /tmp exists in container
                if !FileSystemUtils::exists("/tmp") {
                    println!("Creating /tmp directory in container");
                    if let Err(e) =
                        FileSystemUtils::create_dir_all_with_logging("/tmp", "container /tmp")
                    {
                        eprintln!("Failed to create /tmp in container: {}", e);
                        // Try to continue anyway
                    }
                }

                println!("Waiting for network configuration from parent process...");
                let network_ready_path = format!("/tmp/quilt-network-ready-{}", id_for_logs);
                let start_time = std::time::Instant::now();
                let timeout = std::time::Duration::from_secs(60);

                loop {
                    if std::path::Path::new(&network_ready_path).exists() {
                        println!("Network configuration ready, proceeding...");
                        // Clean up the flag file
                        let _ = FileSystemUtils::remove_path(&network_ready_path);
                        break;
                    }

                    if start_time.elapsed() > timeout {
                        eprintln!("Timeout waiting for network configuration");
                        eprintln!("Expected signal file: {}", network_ready_path);
                        return 1;
                    }

                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
            }

            println!("DEBUG: After chroot, checking /mnt:");
            if let Ok(entries) = std::fs::read_dir("/mnt") {
                for entry in entries {
                    if let Ok(e) = entry {
                        println!("DEBUG: Found in /mnt: {:?}", e.path());
                    }
                }
            } else {
                println!("DEBUG: Cannot read /mnt directory");
            }

            // Initialize container system environment first
            let mut runtime_manager = RuntimeManager::new(); // Create fresh instance
            if let Err(e) = runtime_manager.initialize_container() {
                eprintln!("Failed to initialize container environment: {}", e);
                return 1;
            }

            // Execute setup commands inside the container
            if !setup_commands_clone.is_empty() {
                println!(
                    "Executing {} setup commands in container {}",
                    setup_commands_clone.len(),
                    id_for_logs
                );
                if let Err(e) = runtime_manager.execute_setup_commands(&setup_commands_clone) {
                    eprintln!("Setup commands failed: {}", e);
                    return 1;
                }
            }

            // Set environment variables
            for (key, value) in environment_clone {
                std::env::set_var(key, value);
            }

            // Execute the main command with reduced memory overhead
            println!("Executing main command in container: {:?}", command_clone);

            // Verify shell exists and find appropriate shell
            println!("DEBUG: Looking for shell in container...");
            let shell_path = if std::path::Path::new("/bin/sh").exists() {
                println!("DEBUG: Found /bin/sh");
                // Check if it's a symlink and where it points to
                if let Ok(target) = std::fs::read_link("/bin/sh") {
                    println!("DEBUG: /bin/sh is a symlink to: {:?}", target);
                }
                "/bin/sh"
            } else if std::path::Path::new("/bin/bash").exists() {
                println!("DEBUG: Found /bin/bash");
                "/bin/bash"
            } else if std::path::Path::new("/usr/bin/sh").exists() {
                println!("DEBUG: Found /usr/bin/sh");
                "/usr/bin/sh"
            } else if std::path::Path::new("/usr/bin/bash").exists() {
                println!("DEBUG: Found /usr/bin/bash");
                "/usr/bin/bash"
            } else if std::path::Path::new("/nix/var/nix/profiles/default/bin/sh").exists() {
                println!("DEBUG: Found /nix/var/nix/profiles/default/bin/sh");
                "/nix/var/nix/profiles/default/bin/sh"
            } else {
                eprintln!("ERROR: No shell found in container! Tried /bin/sh, /bin/bash, /usr/bin/sh, /usr/bin/bash, /nix/var/nix/profiles/default/bin/sh");
                eprintln!("Container filesystem might be incomplete or corrupted");
                // List what's in /bin for debugging
                eprintln!("DEBUG: Contents of /bin:");
                if let Ok(entries) = std::fs::read_dir("/bin") {
                    for entry in entries {
                        if let Ok(entry) = entry {
                            eprintln!("  - {:?}", entry.path());
                        }
                    }
                }
                return 1;
            };

            println!("Using shell: {}", shell_path);

            // Simple command execution - no detection, no overhead
            let (final_program, final_args) = if command_clone.len() >= 3
                && (command_clone[0] == "/bin/sh"
                    || command_clone[0] == "/bin/bash"
                    || command_clone[0] == "/usr/bin/sh"
                    || command_clone[0] == "/usr/bin/bash"
                    || command_clone[0].ends_with("/sh")
                    || command_clone[0].ends_with("/bash"))
                && command_clone[1] == "-c"
            {
                // Already a shell command - use as-is
                println!("Command already wrapped in shell: {:?}", command_clone);
                (command_clone[0].clone(), command_clone[1..].to_vec())
            } else if command_clone.len() == 1 {
                // Single command - execute through shell
                println!("Wrapping single command in shell: {:?}", command_clone);
                (
                    shell_path.to_string(),
                    vec!["-c".to_string(), command_clone[0].clone()],
                )
            } else {
                // Multiple arguments - execute directly without shell
                println!("Executing command directly: {:?}", command_clone);
                (command_clone[0].clone(), command_clone[1..].to_vec())
            };

            // SECURITY: Validate command before execution
            let security = NetworkSecurity::new("192.168.100.1".to_string()); // Bridge IP placeholder
            if let Err(e) = security.validate_safe_command(&final_program) {
                eprintln!("🚨 [SECURITY] Command validation failed: {}", e);
                return 1;
            }

            // SECURITY: Sanitize all arguments
            let sanitized_args: Vec<String> = final_args
                .iter()
                .map(|arg| security.sanitize_shell_argument(arg))
                .collect();

            // Convert to CString for exec (do this once, outside any fork)
            let program_cstring = match CString::new(final_program.clone()) {
                Ok(cs) => cs,
                Err(e) => {
                    eprintln!("Failed to create program CString: {}", e);
                    return 1;
                }
            };

            // Prepare all arguments as CStrings with proper lifetime management
            let mut all_args = vec![final_program];
            all_args.extend(sanitized_args);

            let args_cstrings: Vec<CString> = match all_args
                .iter()
                .map(|s| CString::new(s.clone()))
                .collect::<Result<Vec<CString>, _>>()
            {
                Ok(cstrings) => cstrings,
                Err(e) => {
                    eprintln!("Failed to prepare command arguments: {}", e);
                    return 1;
                }
            };

            // Create references with proper lifetime (after cstrings is owned)
            let arg_refs: Vec<&CString> = args_cstrings.iter().collect();

            // Direct exec without nested fork - this replaces the current process
            println!(
                "Executing: {} {:?}",
                program_cstring.to_string_lossy(),
                arg_refs
                    .iter()
                    .map(|cs| cs.to_string_lossy())
                    .collect::<Vec<_>>()
            );

            // Log the actual command details for debugging
            let exec_start = std::time::SystemTime::now();
            println!("🕐 [EXEC] Command execution started at: {:?}", exec_start);
            println!(
                "🕐 [EXEC] Full command: {} {}",
                program_cstring.to_string_lossy(),
                arg_refs[1..]
                    .iter()
                    .map(|cs| cs.to_string_lossy())
                    .collect::<Vec<_>>()
                    .join(" ")
            );

            // This will replace the current process entirely
            match execv(&program_cstring, &arg_refs) {
                Ok(_) => {
                    // This should never be reached if exec succeeds
                    0
                }
                Err(e) => {
                    eprintln!("Failed to exec command: {}", e);
                    1
                }
            }
        };

        // Create the namespaced process
        match self
            .namespace_manager
            .create_namespaced_process(&namespace_config, child_func)
        {
            Ok(pid) => {
                ConsoleLogger::debug(&format!(
                    "🚀 Container process created, PID: {} - verifying readiness...",
                    ProcessUtils::pid_to_i32(pid)
                ));

                // Add process to cgroups
                if let Err(e) = cgroup_manager.add_process(pid) {
                    ConsoleLogger::warning(&format!("Failed to add process to cgroups: {}", e));
                }

                // Finalize cgroup limits after process is started
                if let Some(limits) = &config.resource_limits {
                    if let Err(e) = cgroup_manager.finalize_limits(limits) {
                        ConsoleLogger::warning(&format!("Failed to finalize cgroup limits: {}", e));
                    }
                }

                // First verify the process actually started
                // EVENT-DRIVEN: Just check immediately, no sleep
                if !ProcessUtils::is_process_running(pid) {
                    ConsoleLogger::error(&format!(
                        "Container {} process {} died immediately after starting",
                        id,
                        ProcessUtils::pid_to_i32(pid)
                    ));
                    self.update_container_state(id, ContainerState::Error);
                    return Err(format!("Container {} process died immediately", id));
                }

                // ✅ CRITICAL: Event-driven readiness verification - NO POLLING
                match self
                    .readiness_manager
                    .wait_for_container_ready(id, pid, &rootfs_path)
                {
                    Ok(()) => {
                        // Double-check process is still alive after readiness check
                        if !ProcessUtils::is_process_running(pid) {
                            ConsoleLogger::error(&format!(
                                "Container {} process {} died during readiness check",
                                id,
                                ProcessUtils::pid_to_i32(pid)
                            ));
                            self.update_container_state(id, ContainerState::Error);
                            return Err(format!(
                                "Container {} process died during readiness check",
                                id
                            ));
                        }

                        // Now container is truly ready
                        ConsoleLogger::container_started(id, Some(ProcessUtils::pid_to_i32(pid)));

                        ConsoleLogger::debug(&format!(
                            "[START] Locking containers map to update state for {}",
                            id
                        ));
                        // Update container state using lock-free concurrent operations
                        if let Ok(mut containers) = self.containers.try_lock() {
                            if let Some(container) = containers.get_mut(id) {
                                container.pid = Some(pid);
                                container.state = ContainerState::Running;
                                container.add_log(format!("Container started with PID: {} and verified ready (event-driven)", pid));
                            }
                        }
                        ConsoleLogger::debug(&format!(
                            "[START] Unlocked containers map for {}",
                            id
                        ));
                    }
                    Err(e) => {
                        ConsoleLogger::error(&format!(
                            "Container {} failed event-driven readiness check: {}",
                            id, e
                        ));
                        // Kill the process since it's not working properly
                        let _ = ProcessUtils::terminate_process(pid, 2);
                        // Clean up readiness signal
                        cleanup_readiness_signal(id);
                        self.update_container_state(id, ContainerState::Error);
                        return Err(format!(
                            "Container {} failed to become ready (event-driven): {}",
                            id, e
                        ));
                    }
                }

                // Wait for process completion in a separate task - MANAGED TO PREVENT LEAKS
                let id_clone = id.to_string();
                let start_time = std::time::SystemTime::now();
                let containers_ref = self.containers.clone(); // Clone the Arc for the task
                let _resource_manager = ResourceManager::global();

                // ✅ CRITICAL FIX: Use a JoinHandle to manage the task lifecycle
                let wait_task = tokio::spawn(async move {
                    ConsoleLogger::debug(&format!(
                        "🕐 [TIMING] Started waiting for process {} at {:?}",
                        ProcessUtils::pid_to_i32(pid),
                        start_time
                    ));

                    let exit_code = match NamespaceManager::new().wait_for_process_async(pid).await
                    {
                        Ok(exit_code) => {
                            let elapsed = start_time.elapsed().unwrap_or_default();
                            ConsoleLogger::success(&format!(
                                "Container {} exited with code: {} after {:?}",
                                id_clone, exit_code, elapsed
                            ));
                            if elapsed.as_secs() < 10 {
                                ConsoleLogger::warning(&format!(
                                    "⚠️ Container {} exited suspiciously quickly (in {:?})",
                                    id_clone, elapsed
                                ));
                            }
                            Some(exit_code)
                        }
                        Err(e) => {
                            let elapsed = start_time.elapsed().unwrap_or_default();
                            ConsoleLogger::container_failed(&id_clone, &e);
                            ConsoleLogger::warning(&format!(
                                "Process wait failed after {:?}",
                                elapsed
                            ));
                            None
                        }
                    };

                    // Update container state to EXITED
                    if let Ok(mut containers) = containers_ref.try_lock() {
                        if let Some(container) = containers.get_mut(&id_clone) {
                            if let Some(_code) = exit_code {
                                container.state = ContainerState::Exited;
                            } else {
                                container.state = ContainerState::Error;
                            }
                            container.pid = None;
                            container.add_log("Container process completed".to_string());
                        }
                    }

                    // Don't cleanup resources on exit - container can be restarted
                    ConsoleLogger::debug(&format!(
                        "Container {} exited, resources preserved for restart",
                        id_clone
                    ));

                    ConsoleLogger::debug(&format!(
                        "✅ Container {} monitoring task completed",
                        id_clone
                    ));
                });

                // Store the task handle in container metadata for later cleanup if needed
                // For now, we'll let it run to completion since it cleans up after itself

                // Update container state to store the monitoring task
                if let Ok(mut containers) = self.containers.try_lock() {
                    if let Some(container) = containers.get_mut(id) {
                        container.monitoring_task = Some(wait_task);
                    }
                }

                Ok(())
            }
            Err(e) => {
                self.update_container_state(id, ContainerState::Error);
                Err(format!("Failed to start container {}: {}", id, e))
            }
        }
    }

    fn setup_rootfs(&self, container_id: &str) -> Result<(), String> {
        // Lock-free read of container configuration
        let image_path = if let Ok(containers) = self.containers.try_lock() {
            if let Some(container) = containers.get(container_id) {
                container.config.image_path.clone()
            } else {
                return Err(format!("Container {} not found", container_id));
            }
        } else {
            return Err(format!("Failed to lock containers for {}", container_id));
        };

        // Extract image to simple rootfs directory
        if std::path::Path::new(&image_path).is_file() {
            let rootfs_path = format!("/tmp/quilt-containers/{}", container_id);

            // Create the directory first using FileSystemUtils
            FileSystemUtils::create_dir_all_with_logging(&rootfs_path, "container rootfs")?;

            // Extract the image
            if let Err(e) = self.extract_image(&image_path, &rootfs_path) {
                return Err(format!("Failed to extract container image: {}", e));
            }

            // Fix broken symlinks and ensure working binaries
            self.fix_container_binaries(&rootfs_path)?;

            ConsoleLogger::success(&format!(
                "Rootfs setup completed for container {}",
                container_id
            ));
            Ok(())
        } else {
            Err(format!("Image file not found: {}", image_path))
        }
    }

    /// Fix broken symlinks in Nix-generated containers and ensure working binaries
    fn fix_container_binaries(&self, rootfs_path: &str) -> Result<(), String> {
        ConsoleLogger::debug("Fixing container binaries and symlinks...");

        // Essential binaries that must work
        let essential_binaries = vec![
            ("sh", vec!["/bin/sh", "/bin/bash", "/usr/bin/sh"]),
            ("echo", vec!["/bin/echo", "/usr/bin/echo"]),
            ("ls", vec!["/bin/ls", "/usr/bin/ls"]),
            ("cat", vec!["/bin/cat", "/usr/bin/cat"]),
        ];

        // First, ensure we have essential library directories
        self.setup_library_directories(rootfs_path)?;

        // Copy essential libraries early
        self.copy_essential_libraries(rootfs_path)?;

        // Install busybox FIRST so it's available for shell symlinks
        self.install_busybox(rootfs_path)?;

        // Now fix broken binaries (shell will use busybox if available)
        for (binary_name, host_paths) in essential_binaries {
            let container_binary_path = format!("{}/bin/{}", rootfs_path, binary_name);

            // Check if the binary exists and works in the container
            if FileSystemUtils::is_file(&container_binary_path) {
                // Check if it's a broken symlink
                if FileSystemUtils::is_broken_symlink(&container_binary_path) {
                    ConsoleLogger::warning(&format!(
                        "Broken symlink found for {}: {}",
                        binary_name, container_binary_path
                    ));
                    self.fix_broken_binary(&container_binary_path, binary_name, &host_paths)?;
                } else if !FileSystemUtils::is_executable(&container_binary_path) {
                    ConsoleLogger::warning(&format!("Binary {} is not executable", binary_name));
                    self.fix_broken_binary(&container_binary_path, binary_name, &host_paths)?;
                } else {
                    ConsoleLogger::debug(&format!(
                        "Binary {} exists and is executable",
                        binary_name
                    ));
                }
            } else {
                ConsoleLogger::warning(&format!("Missing binary: {}", binary_name));
                self.fix_broken_binary(&container_binary_path, binary_name, &host_paths)?;
            }
        }

        // Ensure basic shell works
        self.verify_container_shell(rootfs_path)?;

        ConsoleLogger::success("Container binaries fixed and verified");
        Ok(())
    }

    /// Setup essential library directories
    fn setup_library_directories(&self, rootfs_path: &str) -> Result<(), String> {
        let lib_dirs = vec![
            format!("{}/lib", rootfs_path),
            format!("{}/lib64", rootfs_path),
            format!("{}/lib/x86_64-linux-gnu", rootfs_path),
        ];

        for dir in lib_dirs {
            if let Err(e) = FileSystemUtils::create_dir_all_with_logging(&dir, "library directory")
            {
                ConsoleLogger::warning(&format!(
                    "Failed to create library directory {}: {}",
                    dir, e
                ));
            }
        }

        Ok(())
    }

    /// Copy essential libraries needed by binaries
    fn copy_essential_libraries(&self, rootfs_path: &str) -> Result<(), String> {
        let essential_libs = vec![
            (
                "/lib/x86_64-linux-gnu/libc.so.6",
                "lib/x86_64-linux-gnu/libc.so.6",
            ),
            ("/lib64/ld-linux-x86-64.so.2", "lib64/ld-linux-x86-64.so.2"),
            (
                "/lib/x86_64-linux-gnu/libtinfo.so.6",
                "lib/x86_64-linux-gnu/libtinfo.so.6",
            ),
            (
                "/lib/x86_64-linux-gnu/libdl.so.2",
                "lib/x86_64-linux-gnu/libdl.so.2",
            ),
        ];

        for (host_lib, container_lib) in essential_libs {
            if FileSystemUtils::is_file(host_lib) {
                let container_lib_path = format!("{}/{}", rootfs_path, container_lib);
                match FileSystemUtils::copy_file(host_lib, &container_lib_path) {
                    Ok(_) => {
                        ConsoleLogger::debug(&format!(
                            "Copied essential library: {}",
                            container_lib
                        ));
                    }
                    Err(e) => {
                        ConsoleLogger::warning(&format!(
                            "Failed to copy library {}: {}",
                            host_lib, e
                        ));
                        continue;
                    }
                }
            }
        }

        Ok(())
    }

    /// Fix a broken or missing binary by copying from host
    fn fix_broken_binary(
        &self,
        container_binary_path: &str,
        binary_name: &str,
        host_paths: &[&str],
    ) -> Result<(), String> {
        ConsoleLogger::debug(&format!("Fixing broken binary: {}", binary_name));

        // Remove the broken binary if it exists
        if FileSystemUtils::is_file(container_binary_path) {
            FileSystemUtils::remove_path(container_binary_path)?;
        }

        // Try to find a working host binary to copy
        for host_path in host_paths {
            if FileSystemUtils::is_file(host_path) && FileSystemUtils::is_executable(host_path) {
                // Check if the host binary is Nix-linked (avoid problematic dependencies)
                if CommandExecutor::is_nix_linked_binary(host_path) {
                    ConsoleLogger::debug(&format!("Skipping Nix-linked binary: {}", host_path));
                    continue;
                }

                // Copy the working binary
                match FileSystemUtils::copy_file(host_path, container_binary_path) {
                    Ok(_) => {
                        // Make it executable
                        FileSystemUtils::make_executable(container_binary_path)?;
                        ConsoleLogger::success(&format!(
                            "Fixed binary {} by copying from {}",
                            binary_name, host_path
                        ));
                        return Ok(());
                    }
                    Err(e) => {
                        ConsoleLogger::warning(&format!(
                            "Failed to copy {} from {}: {}",
                            binary_name, host_path, e
                        ));
                        continue;
                    }
                }
            }
        }

        // If no suitable host binary found, try to use busybox
        if binary_name == "sh" {
            ConsoleLogger::progress("Creating shell symlink to busybox");

            // Check if busybox is already installed
            let busybox_path = format!(
                "{}/busybox",
                container_binary_path.rsplit_once('/').unwrap().0
            );
            if FileSystemUtils::is_file(&busybox_path) {
                // Create symlink to busybox with absolute path
                let _ = std::fs::remove_file(container_binary_path);
                if let Err(e) = std::os::unix::fs::symlink("/bin/busybox", container_binary_path) {
                    ConsoleLogger::warning(&format!("Failed to create symlink to busybox: {}", e));
                    // Fall back to custom shell if symlink fails
                    return self.create_robust_shell(container_binary_path);
                } else {
                    ConsoleLogger::success(
                        "Created shell symlink to busybox (/bin/sh -> /bin/busybox)",
                    );
                    return Ok(());
                }
            } else {
                ConsoleLogger::progress(
                    "Busybox not found, creating custom shell binary as fallback",
                );
                return self.create_robust_shell(container_binary_path);
            }
        }

        // For other binaries, create simple scripts
        match binary_name {
            "echo" => self.create_echo_script(container_binary_path),
            "ls" => self.create_ls_script(container_binary_path),
            "cat" => self.create_cat_script(container_binary_path),
            _ => {
                ConsoleLogger::warning(&format!("Cannot fix unknown binary: {}", binary_name));
                Ok(())
            }
        }
    }

    /// Create a robust shell script that works without external dependencies
    fn create_robust_shell(&self, shell_path: &str) -> Result<(), String> {
        ConsoleLogger::debug("Creating robust shell binary");

        // Check if we're dealing with a Nix-linked shell using CommandExecutor
        let shell_candidates = vec!["/bin/sh", "/bin/bash"];
        let mut usable_shell = None;

        for shell in &shell_candidates {
            if FileSystemUtils::is_file(shell) && FileSystemUtils::is_executable(shell) {
                // Use CommandExecutor to check if it's Nix-linked
                if !CommandExecutor::is_nix_linked_binary(shell) {
                    usable_shell = Some(*shell);
                    break;
                }
            }
        }

        if let Some(shell_binary) = usable_shell {
            // Copy the working shell
            match FileSystemUtils::copy_file(shell_binary, shell_path) {
                Ok(_) => {
                    FileSystemUtils::make_executable(shell_path)?;

                    // Try to copy shell dependencies for better compatibility
                    let container_root = shell_path.split("/bin").next().unwrap_or("");
                    if let Err(e) = self.copy_shell_dependencies(shell_binary, container_root) {
                        ConsoleLogger::warning(&format!(
                            "Failed to copy shell dependencies: {}",
                            e
                        ));
                    } else {
                        ConsoleLogger::debug("Shell dependencies copied successfully");
                    }

                    ConsoleLogger::success(&format!(
                        "Created shell by copying from {}",
                        shell_binary
                    ));
                    return Ok(());
                }
                Err(e) => {
                    ConsoleLogger::warning(&format!(
                        "Failed to copy shell from {}: {}",
                        shell_binary, e
                    ));
                }
            }
        }

        // Fallback 1: create a minimal shell binary using C code
        ConsoleLogger::progress("Creating minimal C shell binary");
        match self.create_minimal_shell_binary(shell_path) {
            Ok(_) => return Ok(()),
            Err(e) => {
                ConsoleLogger::warning(&format!("Failed to create minimal shell binary: {}", e));
            }
        }

        // Fallback 2: create a shell script as last resort
        ConsoleLogger::progress("Creating shell script as final fallback");
        self.create_shell_script(shell_path)
    }

    /// Copy essential libraries for a shell binary
    fn copy_shell_dependencies(
        &self,
        shell_binary: &str,
        container_root: &str,
    ) -> Result<(), String> {
        // Use ldd to find dependencies
        let output = Command::new("ldd")
            .arg(shell_binary)
            .output()
            .map_err(|e| format!("Failed to run ldd: {}", e))?;

        let ldd_output = String::from_utf8_lossy(&output.stdout);

        for line in ldd_output.lines() {
            if let Some(lib_path) = self.extract_library_path(line) {
                if Path::new(&lib_path).exists() {
                    let lib_name = Path::new(&lib_path).file_name().unwrap().to_str().unwrap();
                    let container_lib_path = format!("{}/lib/{}", container_root, lib_name);

                    if let Some(parent) = Path::new(&container_lib_path).parent() {
                        fs::create_dir_all(parent).ok();
                    }

                    if fs::copy(&lib_path, &container_lib_path).is_ok() {
                        println!("    ✓ Copied library: {}", lib_name);
                    }
                }
            }
        }

        Ok(())
    }

    /// Extract library path from ldd output
    fn extract_library_path(&self, ldd_line: &str) -> Option<String> {
        // Parse lines like: "libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x...)"
        if let Some(arrow_pos) = ldd_line.find(" => ") {
            let after_arrow = &ldd_line[arrow_pos + 4..];
            if let Some(space_pos) = after_arrow.find(' ') {
                let path = after_arrow[..space_pos].trim();
                if path.starts_with('/') && Path::new(path).exists() {
                    return Some(path.to_string());
                }
            }
        }
        None
    }

    /// Create a minimal shell binary that can execute basic commands
    fn create_minimal_shell_binary(&self, shell_path: &str) -> Result<(), String> {
        // Create a more complete C program that can handle shell commands
        let c_program = r#"
#include <unistd.h>
#include <sys/wait.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

// Simple built-in command implementations
int builtin_echo(char *args) {
    if (args && strlen(args) > 0) {
        printf("%s\n", args);
    } else {
        printf("\n");
    }
    return 0;
}

int builtin_pwd(void) {
    char cwd[1024];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        printf("%s\n", cwd);
        return 0;
    }
    return 1;
}

int main(int argc, char *argv[]) {
    if (argc >= 3 && strcmp(argv[1], "-c") == 0) {
        char *command = argv[2];
        
        // Handle compound commands internally by splitting on semicolons
        if (strstr(command, ";")) {
            // Split command on semicolons and execute each part
            char cmd_copy[1024];
            strncpy(cmd_copy, command, sizeof(cmd_copy)-1);
            cmd_copy[sizeof(cmd_copy)-1] = '\0';
            
            char *cmd_part = strtok(cmd_copy, ";");
            int overall_exit_code = 0;
            
            while (cmd_part != NULL) {
                // Trim leading/trailing whitespace
                while (*cmd_part == ' ' || *cmd_part == '\t') cmd_part++;
                char *end = cmd_part + strlen(cmd_part) - 1;
                while (end > cmd_part && (*end == ' ' || *end == '\t')) {
                    *end = '\0';
                    end--;
                }
                
                if (strlen(cmd_part) > 0) {
                    // Execute this individual command
                    int exit_code = 0;
                    
                    // Handle built-in commands
                    if (strncmp(cmd_part, "echo ", 5) == 0) {
                        printf("%s\n", cmd_part + 5);
                    } else if (strcmp(cmd_part, "echo") == 0) {
                        printf("\n");
                    } else if (strcmp(cmd_part, "pwd") == 0) {
                        char cwd[1024];
                        if (getcwd(cwd, sizeof(cwd)) != NULL) {
                            printf("%s\n", cwd);
                        } else {
                            exit_code = 1;
                        }
                    } else if (strncmp(cmd_part, "echo '", 6) == 0 || strncmp(cmd_part, "echo \"", 6) == 0) {
                        // Handle quoted echo - strip quotes and print content
                        char *start = cmd_part + 6;
                        char *end_quote = strchr(start, cmd_part[5]);
                        if (end_quote) {
                            *end_quote = '\0';
                            printf("%s\n", start);
                        } else {
                            printf("%s\n", start);
                        }
                    } else {
                        // For other commands, try to execute directly with fork+exec
                        pid_t pid = fork();
                        if (pid == 0) {
                            // Child process - parse and exec the command
                            char *args[64];
                            char single_cmd_copy[256];
                            int arg_count = 0;
                            
                            strncpy(single_cmd_copy, cmd_part, sizeof(single_cmd_copy)-1);
                            single_cmd_copy[sizeof(single_cmd_copy)-1] = '\0';
                            
                            char *token = strtok(single_cmd_copy, " ");
                            while (token != NULL && arg_count < 63) {
                                args[arg_count++] = token;
                                token = strtok(NULL, " ");
                            }
                            args[arg_count] = NULL;
                            
                            if (arg_count > 0) {
                                // Try to execute the command directly
                                execvp(args[0], args);
                                // If execvp fails, try with full path
                                char full_path[512];
                                snprintf(full_path, sizeof(full_path), "/bin/%s", args[0]);
                                execv(full_path, args);
                                snprintf(full_path, sizeof(full_path), "/usr/bin/%s", args[0]);
                                execv(full_path, args);
                            }
                            
                            fprintf(stderr, "Command not found: %s\n", cmd_part);
                            exit(127);
                        } else if (pid > 0) {
                            // Parent process - wait for child
                            int status;
                            waitpid(pid, &status, 0);
                            exit_code = WEXITSTATUS(status);
                        } else {
                            // Fork failed
                            fprintf(stderr, "Failed to fork for command: %s\n", cmd_part);
                            exit_code = 1;
                        }
                    }
                    
                    // Update overall exit code (last non-zero wins)
                    if (exit_code != 0) {
                        overall_exit_code = exit_code;
                    }
                }
                
                // Get next command part
                cmd_part = strtok(NULL, ";");
            }
            
            return overall_exit_code;
        }
        
        // Handle simple commands (no semicolons)
        if (strncmp(command, "echo ", 5) == 0) {
            return builtin_echo(command + 5);
        } else if (strcmp(command, "echo") == 0) {
            return builtin_echo("");
        } else if (strcmp(command, "pwd") == 0) {
            return builtin_pwd();
        } else if (strncmp(command, "echo '", 6) == 0 || strncmp(command, "echo \"", 6) == 0) {
            // Handle quoted echo
            char *start = command + 6;
            char *end = strchr(start, command[5]); // Find matching quote
            if (end) {
                *end = '\0';
                printf("%s\n", start);
                return 0;
            }
        }
        
        // For other simple commands, try direct execution
        pid_t pid = fork();
        if (pid == 0) {
            // Child process - parse and execute
            char *args[64];
            char cmd_copy[1024];
            int arg_count = 0;
            
            strncpy(cmd_copy, command, sizeof(cmd_copy)-1);
            cmd_copy[sizeof(cmd_copy)-1] = '\0';
            
            char *token = strtok(cmd_copy, " ");
            while (token != NULL && arg_count < 63) {
                args[arg_count++] = token;
                token = strtok(NULL, " ");
            }
            args[arg_count] = NULL;
            
            if (arg_count > 0) {
                execvp(args[0], args);
                // Try with full paths if execvp fails
                char full_path[512];
                snprintf(full_path, sizeof(full_path), "/bin/%s", args[0]);
                execv(full_path, args);
                snprintf(full_path, sizeof(full_path), "/usr/bin/%s", args[0]);
                execv(full_path, args);
            }
            
            fprintf(stderr, "Command not found: %s\n", command);
            exit(127);
        } else if (pid > 0) {
            // Parent process - wait for child
            int status;
            waitpid(pid, &status, 0);
            return WEXITSTATUS(status);
        } else {
            // Fork failed
            fprintf(stderr, "Failed to fork process\n");
            return 1;
        }
    }
    
    // Interactive mode - just print a message and exit
    printf("Minimal shell ready (use -c for command execution)\n");
    return 0;
}
"#;

        // Try to compile a static shell
        let temp_c_file = "/tmp/minimal_shell.c";
        let temp_binary = "/tmp/minimal_shell";

        fs::write(temp_c_file, c_program).map_err(|e| format!("Failed to write C file: {}", e))?;

        // First try with static linking
        let mut compile_result = Command::new("gcc")
            .args(&["-static", "-o", temp_binary, temp_c_file])
            .output();

        // If static compilation fails, try regular dynamic compilation
        if compile_result.is_err() || !compile_result.as_ref().unwrap().status.success() {
            compile_result = Command::new("gcc")
                .args(&["-o", temp_binary, temp_c_file])
                .output();
        }

        match compile_result {
            Ok(output) if output.status.success() => {
                // Check if the binary is usable
                if Path::new(temp_binary).exists() {
                    match fs::copy(temp_binary, shell_path) {
                        Ok(_) => {
                            let mut perms = fs::metadata(shell_path)
                                .map_err(|e| format!("Failed to get shell permissions: {}", e))?
                                .permissions();
                            perms.set_mode(0o755);
                            fs::set_permissions(shell_path, perms)
                                .map_err(|e| format!("Failed to set shell permissions: {}", e))?;

                            // Cleanup
                            fs::remove_file(temp_c_file).ok();
                            fs::remove_file(temp_binary).ok();

                            // Check if it's statically linked
                            if let Ok(ldd_output) = Command::new("ldd").arg(shell_path).output() {
                                let ldd_str = String::from_utf8_lossy(&ldd_output.stdout);
                                if ldd_str.contains("not a dynamic executable") {
                                    println!("  ✅ Created static shell binary");
                                } else {
                                    println!("  ✅ Created dynamic shell binary");
                                }
                            } else {
                                println!("  ✅ Created shell binary");
                            }

                            return Ok(());
                        }
                        Err(e) => {
                            println!("  ⚠ Failed to copy compiled shell: {}", e);
                        }
                    }
                }
            }
            Ok(output) => {
                let stderr = String::from_utf8_lossy(&output.stderr);
                println!("  ⚠ Compilation failed: {}", stderr);
            }
            Err(e) => {
                println!("  ⚠ Failed to run compiler: {}", e);
            }
        }

        // Cleanup
        fs::remove_file(temp_c_file).ok();
        fs::remove_file(temp_binary).ok();

        Err("Could not create minimal shell binary".to_string())
    }

    /// Create a shell script implementation as final fallback
    fn create_shell_script(&self, shell_path: &str) -> Result<(), String> {
        // Create a simple shell script that uses exec to replace itself
        let shell_script = r#"#!/bin/sh
# Simple shell for Quilt containers

if [ "$1" = "-c" ]; then
    shift
    # Execute the command using exec to replace the current process
    # This avoids issues with nested shells and process management
    exec /bin/sh -c "$*"
fi

# Interactive mode - simplified
echo "Container shell ready"
        exit 0
"#;

        fs::write(shell_path, shell_script)
            .map_err(|e| format!("Failed to create shell script: {}", e))?;

        // Make it executable
        let mut perms = fs::metadata(shell_path)
            .map_err(|e| format!("Failed to get shell permissions: {}", e))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(shell_path, perms)
            .map_err(|e| format!("Failed to set shell permissions: {}", e))?;

        println!("  ✅ Created shell script at {}", shell_path);
        Ok(())
    }

    /// Create a simple echo script
    fn create_echo_script(&self, echo_path: &str) -> Result<(), String> {
        let echo_script = r#"#!/bin/sh
# Simple echo implementation
printf '%s\n' "$*"
"#;

        fs::write(echo_path, echo_script)
            .map_err(|e| format!("Failed to create echo script: {}", e))?;

        let mut perms = fs::metadata(echo_path)
            .map_err(|e| format!("Failed to get echo permissions: {}", e))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(echo_path, perms)
            .map_err(|e| format!("Failed to set echo permissions: {}", e))?;

        println!("  ✅ Created echo script at {}", echo_path);
        Ok(())
    }

    /// Create a simple ls script
    fn create_ls_script(&self, ls_path: &str) -> Result<(), String> {
        let ls_script = r#"#!/bin/sh
# Simple ls implementation
for arg in "$@"; do
    if [ -d "$arg" ]; then
        printf 'Contents of %s:\n' "$arg"
        for f in "$arg"/*; do
            [ -e "$f" ] && printf '%s\n' "${f##*/}"
        done
    elif [ -f "$arg" ]; then
        printf '%s\n' "$arg"
    else
        # Default to current directory
        for f in ./*; do
            [ -e "$f" ] && printf '%s\n' "${f##*/}"
        done
        break
    fi
done
"#;

        fs::write(ls_path, ls_script).map_err(|e| format!("Failed to create ls script: {}", e))?;

        let mut perms = fs::metadata(ls_path)
            .map_err(|e| format!("Failed to get ls permissions: {}", e))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(ls_path, perms)
            .map_err(|e| format!("Failed to set ls permissions: {}", e))?;

        println!("  ✅ Created ls script at {}", ls_path);
        Ok(())
    }

    /// Create a simple cat script
    fn create_cat_script(&self, cat_path: &str) -> Result<(), String> {
        let cat_script = r#"#!/bin/sh
# Simple cat implementation
if [ $# -eq 0 ]; then
    # Read from stdin (not practical in this context, just exit)
    exit 0
fi

for file in "$@"; do
    if [ -f "$file" ]; then
        while IFS= read -r line; do
            printf '%s\n' "$line"
        done < "$file"
    else
        printf 'cat: %s: No such file or directory\n' "$file" >&2
    fi
done
"#;

        fs::write(cat_path, cat_script)
            .map_err(|e| format!("Failed to create cat script: {}", e))?;

        let mut perms = fs::metadata(cat_path)
            .map_err(|e| format!("Failed to get cat permissions: {}", e))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(cat_path, perms)
            .map_err(|e| format!("Failed to set cat permissions: {}", e))?;

        println!("  ✅ Created cat script at {}", cat_path);
        Ok(())
    }

    /// Install busybox and create symlinks for all applets
    fn install_busybox(&self, rootfs_path: &str) -> Result<(), String> {
        ConsoleLogger::info("Installing busybox for comprehensive container utilities...");
        ConsoleLogger::debug(&format!("Installing busybox to rootfs: {}", rootfs_path));

        // Try multiple locations for busybox binary
        let busybox_sources = vec![
            "/usr/bin/busybox",             // System busybox
            "./busybox",                    // Local busybox
            "src/daemon/resources/busybox", // Build-time downloaded busybox
        ];

        let mut busybox_source_path = None;
        for source in &busybox_sources {
            if FileSystemUtils::is_file(source) {
                busybox_source_path = Some(source);
                break;
            }
        }

        let busybox_source = match busybox_source_path {
            Some(path) => path,
            None => {
                ConsoleLogger::warning("Busybox not found, downloading...");
                // Download busybox if not found
                let download_path = "/tmp/quilt-busybox";
                let download_cmd = format!(
                    "curl -L -o {} https://busybox.net/downloads/binaries/1.35.0-x86_64-linux-musl/busybox && chmod +x {}",
                    download_path, download_path
                );

                CommandExecutor::execute_shell(&download_cmd)
                    .map_err(|e| format!("Failed to download busybox: {}", e))?;

                download_path
            }
        };

        let busybox_target = format!("{}/bin/busybox", rootfs_path);

        // Ensure /bin directory exists
        let bin_dir = format!("{}/bin", rootfs_path);
        FileSystemUtils::create_dir_all_with_logging(&bin_dir, "busybox bin directory")?;

        // Copy busybox binary
        ConsoleLogger::debug(&format!(
            "Copying busybox from {} to {}",
            busybox_source, busybox_target
        ));
        FileSystemUtils::copy_file(busybox_source, &busybox_target)?;

        // Make it executable
        CommandExecutor::execute_shell(&format!("chmod +x {}", busybox_target))?;
        ConsoleLogger::success(&format!("Busybox installed to {}", busybox_target));

        // Get list of all busybox applets
        let applets_output = CommandExecutor::execute_shell(&format!("{} --list", busybox_target))?;
        let applets: Vec<&str> = applets_output.stdout.lines().collect();

        // Create symlinks for essential networking and system utilities
        let essential_applets = vec![
            "sh",
            "bash",
            "ash", // Shells - ensure we have a working shell
            "nslookup",
            "ping",
            "wget",
            "nc",
            "telnet",
            "traceroute",
            "hostname",
            "ifconfig",
            "route",
            "arp",
            "netstat",
            "ps",
            "top",
            "kill",
            "grep",
            "sed",
            "awk",
            "find",
            "tar",
            "gzip",
            "gunzip",
            "base64",
            "md5sum",
            "sha256sum",
            "head",
            "tail",
            "less",
            "more",
            "sort",
            "uniq",
            "test",
            "[",
            "[[",
            "expr",
            "seq",
            "sleep",
            "timeout",
        ];

        for applet in essential_applets {
            if applets.contains(&applet) {
                let symlink_path = format!("{}/bin/{}", rootfs_path, applet);
                // Remove existing file/symlink if it exists
                let _ = std::fs::remove_file(&symlink_path);

                // Create symlink with absolute path to busybox
                if let Err(e) = std::os::unix::fs::symlink("/bin/busybox", &symlink_path) {
                    ConsoleLogger::debug(&format!(
                        "Failed to create symlink for {}: {}",
                        applet, e
                    ));
                } else {
                    ConsoleLogger::debug(&format!(
                        "Created busybox symlink: {} -> /bin/busybox",
                        applet
                    ));
                }
            }
        }

        // Also ensure /usr/bin exists and has key symlinks
        let usr_bin_dir = format!("{}/usr/bin", rootfs_path);
        let _ = FileSystemUtils::create_dir_all_with_logging(&usr_bin_dir, "usr/bin directory");

        // Create some symlinks in /usr/bin for common paths
        for applet in &["nslookup", "wget", "nc"] {
            let usr_symlink = format!("{}/usr/bin/{}", rootfs_path, applet);
            let _ = std::fs::remove_file(&usr_symlink);
            let _ = std::os::unix::fs::symlink("/bin/busybox", &usr_symlink);
        }

        ConsoleLogger::success("Busybox installed with all networking utilities");
        Ok(())
    }

    /// Verify that the container shell works
    fn verify_container_shell(&self, rootfs_path: &str) -> Result<(), String> {
        let shell_path = format!("{}/bin/sh", rootfs_path);

        if !FileSystemUtils::is_file(&shell_path) {
            ConsoleLogger::warning("No shell found in container, basic commands may not work");
            return Ok(());
        }

        if !FileSystemUtils::is_executable(&shell_path) {
            ConsoleLogger::warning("Shell exists but is not executable");
            return Ok(());
        }

        ConsoleLogger::success("Container shell verification completed");
        Ok(())
    }

    fn extract_image(&self, image_path: &str, rootfs_path: &str) -> Result<(), String> {
        // SECURITY: Validate rootfs path to prevent directory traversal attacks
        let security = NetworkSecurity::new("192.168.100.1".to_string()); // Bridge IP placeholder
        security.validate_rootfs_path(rootfs_path)?;

        // Open and decompress the tar file
        let tar_file = std::fs::File::open(image_path)
            .map_err(|e| format!("Failed to open image file: {}", e))?;

        let tar = GzDecoder::new(tar_file);
        let mut archive = Archive::new(tar);

        // Extract to rootfs directory
        archive
            .unpack(rootfs_path)
            .map_err(|e| format!("Failed to extract image: {}", e))?;

        ConsoleLogger::success(&format!("Successfully extracted image to {}", rootfs_path));
        Ok(())
    }

    fn update_container_state(&self, container_id: &str, new_state: ContainerState) {
        // Per-container lock for state update
        if let Ok(mut containers) = self.containers.try_lock() {
            if let Some(container) = containers.get_mut(container_id) {
                container.state = new_state;
            }
        }
    }

    #[allow(dead_code)]
    pub fn get_container_state(&self, container_id: &str) -> Option<ContainerState> {
        if let Ok(containers) = self.containers.try_lock() {
            containers.get(container_id).map(|c| c.state.clone())
        } else {
            None
        }
    }

    pub fn get_container_logs(&self, container_id: &str) -> Option<Vec<String>> {
        if let Ok(containers) = self.containers.try_lock() {
            containers.get(container_id).map(|c| c.logs.clone())
        } else {
            None
        }
    }

    pub fn get_container_info(&self, container_id: &str) -> Option<Container> {
        if let Ok(containers) = self.containers.try_lock() {
            containers.get(container_id).cloned()
        } else {
            None
        }
    }

    // Internal method for getting container stats
    fn get_container_stats_for_container(
        &self,
        container: &Container,
        container_id: &str,
    ) -> Result<HashMap<String, String>, String> {
        let mut stats = HashMap::new();

        if let Some(_pid) = container.pid {
            // Get memory usage from cgroups
            let cgroup_manager = CgroupManager::new(container_id.to_string());
            if let Ok(memory_usage) = cgroup_manager.get_memory_usage() {
                stats.insert("memory_usage_bytes".to_string(), memory_usage.to_string());
            }
        }

        // Get container state
        match &container.state {
            ContainerState::Created => stats.insert("state".to_string(), "created".to_string()),
            ContainerState::Starting => stats.insert("state".to_string(), "starting".to_string()),
            ContainerState::Running => stats.insert("state".to_string(), "running".to_string()),
            ContainerState::Exited => stats.insert("state".to_string(), "exited".to_string()),
            ContainerState::Error => stats.insert("state".to_string(), "error".to_string()),
        };

        // Get PID if available
        if let Some(pid) = container.pid {
            stats.insert("pid".to_string(), ProcessUtils::pid_to_i32(pid).to_string());
        }

        Ok(stats)
    }

    pub fn get_container_stats(
        &self,
        container_id: &str,
    ) -> Result<HashMap<String, String>, String> {
        if let Ok(containers) = self.containers.try_lock() {
            if let Some(container) = containers.get(container_id) {
                self.get_container_stats_for_container(container, container_id)
            } else {
                Err(format!("Container {} not found", container_id))
            }
        } else {
            Err(format!("Failed to lock containers for {}", container_id))
        }
    }

    pub fn get_container_info_and_stats(
        &self,
        container_id: &str,
    ) -> (Option<Container>, Result<HashMap<String, String>, String>) {
        let container_info = if let Ok(containers) = self.containers.try_lock() {
            containers.get(container_id).cloned()
        } else {
            None
        };
        let container_stats = self.get_container_stats(container_id);
        (container_info, container_stats)
    }

    pub fn stop_container(&self, container_id: &str) -> Result<(), String> {
        ConsoleLogger::progress(&format!("Stopping container: {}", container_id));

        // Get container PID and monitoring task
        let (pid, monitoring_task) = if let Ok(containers) = self.containers.try_lock() {
            if let Some(container) = containers.get(container_id) {
                (
                    container.pid,
                    container.monitoring_task.as_ref().map(|t| t.abort_handle()),
                )
            } else {
                return Err(format!("Container {} not found", container_id));
            }
        } else {
            return Err(format!("Failed to lock containers for {}", container_id));
        };

        let pid = pid.ok_or_else(|| format!("Container {} is not running", container_id))?;

        // Abort the monitoring task to prevent resource leaks
        if let Some(abort_handle) = monitoring_task {
            abort_handle.abort();
            ConsoleLogger::debug(&format!(
                "Aborted monitoring task for container {}",
                container_id
            ));
        }

        match ProcessUtils::terminate_process(pid, 10) {
            Ok(()) => {
                // Update container state
                if let Ok(mut containers) = self.containers.try_lock() {
                    if let Some(container) = containers.get_mut(container_id) {
                        container.state = ContainerState::Exited;
                        container.pid = None;
                        container.monitoring_task = None; // Clear the task handle
                        container.add_log("Container stopped by user request".to_string());
                    }
                }

                // Don't cleanup resources on stop - container can be restarted
                ConsoleLogger::debug(&format!(
                    "Container {} stopped, resources preserved for restart",
                    container_id
                ));

                ConsoleLogger::container_stopped(container_id);
                Ok(())
            }
            Err(e) => Err(format!("Failed to stop container {}: {}", container_id, e)),
        }
    }

    pub fn remove_container(&self, container_id: &str) -> Result<(), String> {
        ConsoleLogger::progress(&format!("Removing container: {}", container_id));

        // Get container PID before stopping if it's running
        let container_pid = if let Ok(containers) = self.containers.try_lock() {
            if let Some(container) = containers.get(container_id) {
                container.pid
            } else {
                return Err(format!("Container {} not found", container_id));
            }
        } else {
            return Err(format!("Failed to lock containers for {}", container_id));
        };

        // Stop the container first if it's running
        if let Err(e) = self.stop_container(container_id) {
            ConsoleLogger::warning(&format!("Error stopping container before removal: {}", e));
        }

        // Remove container from registry
        let removed = if let Ok(mut containers) = self.containers.try_lock() {
            containers.remove(container_id).is_some()
        } else {
            return Err(format!(
                "Failed to lock containers for removal of {}",
                container_id
            ));
        };

        if !removed {
            return Err(format!("Container {} not found", container_id));
        }

        // Clean up readiness signal files
        cleanup_readiness_signal(container_id);

        // Use ResourceManager for comprehensive cleanup
        let resource_manager = ResourceManager::global();
        if let Err(e) = resource_manager.cleanup_container_resources(container_id, container_pid) {
            ConsoleLogger::warning(&format!("Resource cleanup failed during removal: {}", e));
            // Try emergency cleanup as fallback
            if let Err(e2) = resource_manager.emergency_cleanup(container_id) {
                return Err(format!(
                    "Failed to remove container {}: {} (emergency cleanup also failed: {})",
                    container_id, e, e2
                ));
            }
        }

        // Clean up rootfs directory using FileSystemUtils
        let rootfs_path = format!("/tmp/quilt-containers/{}", container_id);
        if let Err(e) = FileSystemUtils::remove_path(&rootfs_path) {
            ConsoleLogger::warning(&format!("Failed to remove rootfs directory: {}", e));
        }

        ConsoleLogger::container_removed(container_id);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn list_containers(&self) -> Vec<String> {
        if let Ok(containers) = self.containers.try_lock() {
            containers.keys().cloned().collect()
        } else {
            Vec::new()
        }
    }

    /// Set the network configuration for a container
    pub fn set_container_network(
        &self,
        container_id: &str,
        network_config: ContainerNetworkConfig,
    ) -> Result<(), String> {
        if let Ok(mut containers) = self.containers.try_lock() {
            if let Some(container) = containers.get_mut(container_id) {
                container.network_config = Some(network_config);
                Ok(())
            } else {
                Err(format!("Container {} not found", container_id))
            }
        } else {
            Err(format!("Failed to lock containers for {}", container_id))
        }
    }

    /// Get the network configuration for a container
    pub fn get_container_network(&self, container_id: &str) -> Option<ContainerNetworkConfig> {
        if let Ok(containers) = self.containers.try_lock() {
            containers
                .get(container_id)
                .and_then(|c| c.network_config.clone())
        } else {
            None
        }
    }

    /// Configure network for a running container
    pub fn setup_container_network_post_start(
        &self,
        container_id: &str,
        network_manager: &NetworkManager,
    ) -> Result<(), String> {
        let (network_config, pid) = if let Ok(containers) = self.containers.try_lock() {
            if let Some(container) = containers.get(container_id) {
                let network_config = container
                    .network_config
                    .as_ref()
                    .ok_or_else(|| format!("No network config for container {}", container_id))?;

                let pid = container
                    .pid
                    .ok_or_else(|| format!("Container {} is not running", container_id))?;

                (network_config.clone(), pid)
            } else {
                return Err(format!("Container {} not found", container_id));
            }
        } else {
            return Err(format!("Failed to lock containers for {}", container_id));
        };

        // Setup the container's network interface using the network manager
        network_manager.setup_container_network(&network_config, pid.as_raw())?;
        Ok(())
    }

    /// Execute a command in a running container
    pub fn exec_container(
        &self,
        container_id: &str,
        command: Vec<String>,
        working_directory: Option<String>,
        environment: HashMap<String, String>,
        capture_output: bool,
    ) -> Result<(i32, String, String), String> {
        ConsoleLogger::progress(&format!(
            "Executing command in container {}: {:?}",
            container_id, command
        ));
        ConsoleLogger::debug(&format!(
            "🔍 [EXEC] Working dir: {:?}, Env vars: {}, Capture output: {}",
            working_directory,
            environment.len(),
            capture_output
        ));

        let pid = if let Ok(containers) = self.containers.try_lock() {
            if let Some(container) = containers.get(container_id) {
                match container.state {
                    ContainerState::Running => {
                        ConsoleLogger::debug(&format!(
                            "✅ [EXEC] Container {} is running",
                            container_id
                        ));
                        container
                            .pid
                            .ok_or_else(|| format!("Container {} has no PID", container_id))
                    }
                    ref state => {
                        let state_msg = match state {
                            ContainerState::Created => "CREATED",
                            ContainerState::Starting => "STARTING",
                            ContainerState::Running => "RUNNING",
                            ContainerState::Exited => "EXITED",
                            ContainerState::Error => "ERROR",
                        };
                        ConsoleLogger::debug(&format!(
                            "❌ [EXEC] Container {} is not running, state: {}",
                            container_id, state_msg
                        ));
                        Err(format!("Container {} is not running", container_id))
                    }
                }
            } else {
                Err(format!("Container {} not found", container_id))
            }
        } else {
            Err(format!("Failed to lock containers for {}", container_id))
        }?;
        ConsoleLogger::debug(&format!(
            "🔓 [EXEC] Released containers lock, got PID: {}",
            ProcessUtils::pid_to_i32(pid)
        ));

        // Prepare the command to execute
        let cmd_str = if command.len() == 1 {
            command[0].clone()
        } else {
            command.join(" ")
        };
        ConsoleLogger::debug(&format!("📝 [EXEC] Prepared command string: '{}'", cmd_str));

        // Build nsenter command to enter container's namespaces
        let mut nsenter_cmd = vec![
            "nsenter".to_string(),
            "-t".to_string(),
            pid.as_raw().to_string(),
            "-p".to_string(),
            "-m".to_string(),
            "-n".to_string(),
            "-u".to_string(),
            "-i".to_string(),
        ];

        // Add working directory if specified
        if let Some(workdir) = working_directory {
            ConsoleLogger::debug(&format!("📁 [EXEC] Setting working directory: {}", workdir));
            nsenter_cmd.extend(vec!["--wd".to_string(), workdir]);
        }

        // Add environment variables
        for (key, value) in environment {
            ConsoleLogger::debug(&format!("🌍 [EXEC] Setting env var: {}={}", key, value));
            nsenter_cmd.extend(vec!["-E".to_string(), format!("{}={}", key, value)]);
        }

        // Add the actual command
        nsenter_cmd.extend(vec![
            "--".to_string(),
            "/bin/sh".to_string(),
            "-c".to_string(),
            cmd_str.clone(),
        ]);

        ConsoleLogger::debug(&format!(
            "🚀 [EXEC] Full nsenter command: {:?}",
            nsenter_cmd
        ));
        let exec_start = std::time::SystemTime::now();

        // Execute the command using nsenter
        let output = Command::new("nsenter")
            .args(&nsenter_cmd[1..]) // Skip the "nsenter" part since we're calling it directly
            .output()
            .map_err(|e| format!("Failed to execute nsenter: {}", e))?;

        let elapsed = exec_start.elapsed().unwrap_or_default();
        let exit_code = output.status.code().unwrap_or(-1);
        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        ConsoleLogger::debug(&format!(
            "⏱️ [EXEC] Command completed in {:?}, exit code: {}",
            elapsed, exit_code
        ));
        if !stdout.is_empty() {
            ConsoleLogger::debug(&format!("📤 [EXEC] stdout: {}", stdout.trim()));
        }
        if !stderr.is_empty() {
            ConsoleLogger::debug(&format!("📤 [EXEC] stderr: {}", stderr.trim()));
        }

        Ok((exit_code, stdout, stderr))
    }

    // OLD POLLING-BASED VERIFICATION REMOVED - REPLACED WITH EVENT-DRIVEN READINESS SYSTEM
}

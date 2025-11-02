// Production-grade Quilt unified binary
// Single binary for both daemon and client operations

mod daemon;
mod utils;
mod icc;
mod sync;
mod grpc;
mod cli;

use clap::{Parser, Subcommand};
use utils::logger::Logger;
use utils::server_manager;

// Include the generated protobuf code
pub mod quilt {
    tonic::include_proto!("quilt");
}

/// Quilt - Production Container Runtime
#[derive(Parser, Debug)]
#[clap(author, version, about = "Production container runtime with SQLite-based sync engine")]
#[clap(propagate_version = true)]
struct QuiltCli {
    #[clap(subcommand)]
    command: Commands,

    /// Server address for client commands
    #[clap(long, global = true, default_value = "http://127.0.0.1:50051")]
    server: String,

    /// Disable automatic daemon startup
    #[clap(long, global = true)]
    no_auto_start: bool,

    /// Enable JSON output format
    #[clap(long, global = true)]
    json: bool,

    /// Suppress non-error output
    #[clap(long, global = true)]
    quiet: bool,

    /// Enable verbose output
    #[clap(long, global = true)]
    verbose: bool,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run the Quilt daemon server
    Daemon {
        /// Run daemon in background (detached mode)
        #[clap(long)]
        detach: bool,

        /// Stop the running daemon
        #[clap(long)]
        stop: bool,

        /// Restart the daemon
        #[clap(long)]
        restart: bool,

        /// Show daemon status
        #[clap(long)]
        status: bool,

        /// Force kill daemon
        #[clap(long)]
        force: bool,
    },

    /// Create a new container
    Create {
        /// Container name (required)
        name: String,

        /// Container image path (.tar.gz, defaults to system default image)
        #[clap(long)]
        image_path: Option<String>,

        /// Environment variables (KEY=VALUE)
        #[clap(short, long)]
        env: Vec<String>,

        /// Memory limit in MB (default: 512)
        #[clap(long, default_value = "512")]
        memory: i32,

        /// CPU limit as percentage (default: 50.0)
        #[clap(long, default_value = "50.0")]
        cpu: f32,

        /// Volume mounts (source:dest or name:dest)
        #[clap(short = 'v', long)]
        volume: Vec<String>,

        /// Return immediately without waiting for container to be ready
        #[clap(long)]
        no_wait: bool,

        /// Command to run in container
        #[clap(last = true)]
        command: Vec<String>,
    },

    /// Get container status
    Status {
        /// Container ID or name
        container: Option<String>,
    },

    /// View container logs
    Logs {
        /// Container ID or name
        container: Option<String>,
    },

    /// Stop a container
    Stop {
        /// Container ID or name
        container: Option<String>,

        /// Timeout before force kill (seconds)
        #[clap(short = 't', long, default_value = "10")]
        timeout: u32,
    },

    /// Remove a container
    Remove {
        /// Container ID or name
        container: Option<String>,

        /// Force removal even if running
        #[clap(short = 'f', long)]
        force: bool,
    },

    /// Start a stopped container
    Start {
        /// Container ID or name
        container: Option<String>,
    },

    /// Kill a container immediately
    Kill {
        /// Container ID or name
        container: Option<String>,
    },

    /// Execute a command in a container
    Exec {
        /// Container ID or name
        container: Option<String>,

        /// Command to execute
        #[clap(short = 'c', long)]
        command: Vec<String>,

        /// Working directory
        #[clap(short = 'w', long)]
        workdir: Option<String>,

        /// Capture and display output
        #[clap(long)]
        capture: bool,
    },

    /// Enter interactive shell in container
    Shell {
        /// Container ID or name
        container: Option<String>,

        /// Shell to use (default: /bin/sh)
        #[clap(default_value = "/bin/sh")]
        shell: String,
    },

    /// Manage volumes
    Volume {
        #[clap(subcommand)]
        command: VolumeCommands,
    },

    /// Inter-container communication
    Icc {
        #[clap(subcommand)]
        command: cli::IccCommands,
    },
}

#[derive(Subcommand, Debug)]
enum VolumeCommands {
    /// Create a new volume
    Create {
        /// Volume name
        name: String,
    },

    /// List all volumes
    List,

    /// Inspect a volume
    Inspect {
        /// Volume name
        name: String,
    },

    /// Remove a volume
    Remove {
        /// Volume name
        name: String,

        /// Force removal
        #[clap(short = 'f', long)]
        force: bool,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = QuiltCli::parse();

    // Set environment variables based on global flags
    if cli.quiet {
        std::env::set_var("QUILT_QUIET", "1");
    }
    if cli.verbose {
        std::env::set_var("QUILT_VERBOSE", "1");
    }
    if cli.json {
        std::env::set_var("QUILT_JSON", "1");
    }

    match cli.command {
        Commands::Daemon { detach, stop, restart, status, force } => {
            handle_daemon_command(detach, stop, restart, status, force).await
        }
        _ => {
            // All other commands are client commands that need the daemon
            handle_client_command(cli).await
        }
    }
}

/// Auto-escalate to root for daemon operations
/// Returns Ok(true) if we escalated and should exit, Ok(false) if we're already root
fn auto_escalate_daemon_command(
    detach: bool,
    stop: bool,
    restart: bool,
    status: bool,
    force: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    use std::process::Command;

    // Check if we're already running as root
    let is_root = unsafe { libc::getuid() } == 0;

    if is_root {
        return Ok(false); // Already root, no escalation needed
    }

    // Not root - need to escalate via sudo
    // This will be passwordless after installation
    Logger::debug("Escalating privileges for daemon operation...");

    // Get current binary path
    let current_exe = std::env::current_exe()
        .map_err(|e| format!("Failed to get current executable: {}", e))?;

    // Build sudo command with same arguments
    let mut cmd = Command::new("sudo");
    cmd.arg(&current_exe);
    cmd.arg("daemon");

    // Pass through all daemon flags
    if detach { cmd.arg("--detach"); }
    if stop { cmd.arg("--stop"); }
    if restart { cmd.arg("--restart"); }
    if status { cmd.arg("--status"); }
    if force { cmd.arg("--force"); }

    // Preserve global flags via arguments
    if std::env::var("QUILT_VERBOSE").is_ok() {
        cmd.arg("--verbose");
    }
    if std::env::var("QUILT_QUIET").is_ok() {
        cmd.arg("--quiet");
    }
    if std::env::var("QUILT_JSON").is_ok() {
        cmd.arg("--json");
    }

    // Execute with sudo (passwordless after installation)
    let exit_status = cmd.status()
        .map_err(|e| format!("Failed to execute sudo: {}. Is sudo installed?", e))?;

    // Exit with the same code as the sudo command
    std::process::exit(exit_status.code().unwrap_or(1));
}

/// Check if daemon is running and healthy
async fn is_daemon_healthy() -> bool {
    server_manager::check_server_health().await
}

/// Auto-start daemon if not running (unless --no-auto-start is set)
/// Returns Ok(()) if daemon is running/started, Err if failed to start
async fn ensure_daemon_for_client(no_auto_start: bool) -> Result<(), Box<dyn std::error::Error>> {
    // Check if daemon is already running and healthy
    if is_daemon_healthy().await {
        return Ok(());
    }

    // If auto-start is disabled, fail immediately
    if no_auto_start {
        return Err("Daemon is not running. Start it with: quilt daemon --detach".into());
    }

    // Need to start daemon - requires root privileges
    let is_root = unsafe { libc::getuid() } == 0;

    if !is_root {
        // Re-execute with sudo to start daemon
        Logger::debug("Daemon not running - auto-starting with elevated privileges...");

        let current_exe = std::env::current_exe()
            .map_err(|e| format!("Failed to get current executable: {}", e))?;

        // Build sudo command to start daemon
        let mut cmd = std::process::Command::new("sudo");
        cmd.arg(&current_exe);
        cmd.arg("daemon");
        cmd.arg("--detach");

        // Preserve global flags
        if std::env::var("QUILT_VERBOSE").is_ok() {
            cmd.arg("--verbose");
        }
        if std::env::var("QUILT_QUIET").is_ok() {
            cmd.arg("--quiet");
        }

        // Start daemon
        let status = cmd.status()
            .map_err(|e| format!("Failed to start daemon: {}", e))?;

        if !status.success() {
            return Err("Failed to start daemon".into());
        }

        // Wait briefly for daemon to be ready
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        // Verify it's healthy
        if !is_daemon_healthy().await {
            return Err("Daemon started but not responsive. Try: quilt daemon --status".into());
        }

        Logger::debug("Daemon auto-started successfully");
        Ok(())
    } else {
        // We're root - start daemon directly
        server_manager::start_daemon_background()
            .map_err(|e| format!("Failed to start daemon: {}", e))?;

        // Wait for daemon to be ready
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

        if !is_daemon_healthy().await {
            return Err("Daemon started but not responsive".into());
        }

        Ok(())
    }
}

/// Handle daemon management commands
async fn handle_daemon_command(
    detach: bool,
    stop: bool,
    restart: bool,
    status: bool,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    // Auto-escalate to root if needed
    // After installation, this will be passwordless via sudoers rule
    auto_escalate_daemon_command(detach, stop, restart, status, force)?;

    // If we reach here, we're running as root
    if status {
        // Show daemon status
        let server_status = server_manager::get_server_status().await;

        if server_status.running {
            Logger::success("Daemon is running");
            if let Some(pid) = server_status.pid {
                Logger::detail("pid", &pid.to_string());
            }
            Logger::detail("responsive", if server_status.responsive { "yes" } else { "no" });
        } else {
            Logger::info("Daemon is not running");
        }
        return Ok(());
    }

    if stop {
        // Stop daemon
        match server_manager::stop_daemon(force) {
            Ok(()) => {
                Logger::success("Daemon stopped");
                Ok(())
            }
            Err(e) => {
                Logger::error(&e);
                std::process::exit(1);
            }
        }
    } else if restart {
        // Restart daemon
        match server_manager::restart_daemon().await {
            Ok(()) => {
                Logger::success("Daemon restarted");
                Ok(())
            }
            Err(e) => {
                Logger::error(&e);
                std::process::exit(1);
            }
        }
    } else {
        // Start daemon (default behavior)
        if detach {
            // Start in background
            Logger::info("Starting daemon in background...");
            match server_manager::start_daemon_background() {
                Ok(()) => {
                    // Wait briefly for it to start
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

                    // Check if it's responsive
                    if server_manager::check_server_health().await {
                        Logger::success("Daemon started successfully");
                        Ok(())
                    } else {
                        Logger::warning("Daemon started but not yet responsive");
                        Logger::info("Check status with: quilt daemon --status");
                        Ok(())
                    }
                }
                Err(e) => {
                    Logger::error(&format!("Failed to start daemon: {}", e));
                    std::process::exit(1);
                }
            }
        } else {
            // Run daemon in foreground
            daemon::server::run_server(detach).await
        }
    }
}

/// Handle client commands (requires daemon to be running)
async fn handle_client_command(cli: QuiltCli) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Duration;
    use tonic::transport::Channel;

    eprintln!("🔍 [SHELL-DEBUG] handle_client_command called");
    eprintln!("🔍 [SHELL-DEBUG] Server address: {}", cli.server);

    // Ensure daemon is running (auto-start if needed and allowed)
    eprintln!("🔍 [SHELL-DEBUG] Ensuring daemon is running (auto_start={})", !cli.no_auto_start);
    ensure_daemon_for_client(cli.no_auto_start).await?;
    eprintln!("🔍 [SHELL-DEBUG] Daemon check complete");

    // Create gRPC client connection - daemon should be running now
    eprintln!("🔍 [SHELL-DEBUG] Creating gRPC client connection to {}...", cli.server);
    let channel = match Channel::from_shared(cli.server.clone())?
        .timeout(Duration::from_secs(60))
        .connect_timeout(Duration::from_secs(10))
        .tcp_keepalive(Some(Duration::from_secs(60)))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_while_idle(true)
        .connect()
        .await
    {
        Ok(ch) => {
            eprintln!("🔍 [SHELL-DEBUG] gRPC connection established");
            ch
        },
        Err(e) => {
            eprintln!("🔍 [SHELL-DEBUG] gRPC connection FAILED: {}", e);
            Logger::error("Daemon is not running");
            Logger::info("Start the daemon with: quilt daemon --detach");
            std::process::exit(1);
        }
    };

    eprintln!("🔍 [SHELL-DEBUG] Creating QuiltServiceClient...");
    let mut client = quilt::quilt_service_client::QuiltServiceClient::new(channel);
    eprintln!("🔍 [SHELL-DEBUG] Client created successfully");

    // Route to appropriate command handler
    match cli.command {
        Commands::Create { name, image_path, env, memory, cpu, volume, no_wait, command } => {
            cli::commands::handle_create(&mut client, name, image_path, env, memory, cpu, volume, command, no_wait).await?;
            Ok(())
        }

        Commands::Status { container } => {
            cli::commands::handle_status(&mut client, container, false).await
        }

        Commands::Logs { container } => {
            cli::commands::handle_logs(&mut client, container, false).await
        }

        Commands::Stop { container, timeout } => {
            cli::commands::handle_stop(&mut client, container, false, timeout).await
        }

        Commands::Remove { container, force } => {
            cli::commands::handle_remove(&mut client, container, false, force).await
        }

        Commands::Start { container } => {
            cli::commands::handle_start(&mut client, container, false).await
        }

        Commands::Kill { container } => {
            cli::commands::handle_kill(&mut client, container, false).await
        }

        Commands::Exec { container, command, workdir, capture } => {
            cli::commands::handle_exec(&mut client, container, false, command, workdir, capture).await?;
            Ok(())
        }

        Commands::Shell { container, shell } => {
            eprintln!("🔍 [SHELL-DEBUG] Shell command initiated for container: {:?}", container);

            // Resolve container name to ID (supports both names and UUIDs)
            eprintln!("🔍 [SHELL-DEBUG] Resolving container name/ID...");
            let container_id = cli::commands::resolve_container_id(&mut client, &container, false).await?;
            eprintln!("🔍 [SHELL-DEBUG] Resolved to container ID: {}", container_id);

            // Create interactive shell and run it
            eprintln!("🔍 [SHELL-DEBUG] Creating shell session with command: {}", shell);
            let shell_session = cli::InteractiveShell::new(container_id, vec![shell])?;

            eprintln!("🔍 [SHELL-DEBUG] Running shell session...");
            let exit_code = shell_session.run(client).await?;
            eprintln!("🔍 [SHELL-DEBUG] Shell session exited with code: {}", exit_code);
            std::process::exit(exit_code);
        }

        Commands::Volume { command } => {
            match command {
                VolumeCommands::Create { name } => {
                    cli::commands::handle_volume_create(&mut client, name).await
                }
                VolumeCommands::List => {
                    cli::commands::handle_volume_list(&mut client).await
                }
                VolumeCommands::Inspect { name } => {
                    cli::commands::handle_volume_inspect(&mut client, name).await
                }
                VolumeCommands::Remove { name, force } => {
                    cli::commands::handle_volume_remove(&mut client, name, force).await
                }
            }
        }

        Commands::Icc { command } => {
            cli::icc::handle_icc_command(command, client).await
        }

        Commands::Daemon { .. } => {
            // This should never happen as daemon commands are handled before calling this function
            unreachable!("Daemon commands should be routed to handle_daemon_command")
        }
    }
}

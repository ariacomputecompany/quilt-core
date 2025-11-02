use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::Channel;

use super::logger::Logger;
use super::process::ProcessUtils;

/// Server health status
#[derive(Debug, Clone)]
pub struct ServerStatus {
    pub running: bool,
    pub pid: Option<i32>,
    pub responsive: bool,
}

/// Get the path to the PID file
/// Always use /var/run/quilt/quilt.pid - one daemon, one location
fn get_pid_file_path() -> PathBuf {
    PathBuf::from("/var/run/quilt/quilt.pid")
}

/// Write PID to file
pub fn write_pid_file(pid: u32) -> Result<(), String> {
    let pid_file = get_pid_file_path();

    // Create parent directory if needed
    if let Some(parent) = pid_file.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create PID directory: {}", e))?;
    }

    fs::write(&pid_file, pid.to_string())
        .map_err(|e| format!("Failed to write PID file: {}", e))?;

    Logger::debug(&format!("Wrote PID {} to {:?}", pid, pid_file));
    Ok(())
}

/// Read PID from file
pub fn read_pid_file() -> Option<i32> {
    let pid_file = get_pid_file_path();

    if !pid_file.exists() {
        return None;
    }

    let content = fs::read_to_string(&pid_file).ok()?;
    content.trim().parse::<i32>().ok()
}

/// Remove PID file
pub fn remove_pid_file() {
    let pid_file = get_pid_file_path();
    if pid_file.exists() {
        fs::remove_file(&pid_file).ok();
        Logger::debug(&format!("Removed PID file: {:?}", pid_file));
    }
}

/// Check if the daemon is healthy by attempting a gRPC connection
pub async fn check_server_health() -> bool {
    let endpoint = "http://127.0.0.1:50051";

    let channel_result = Channel::from_shared(endpoint.to_string());
    if channel_result.is_err() {
        return false;
    }

    match tokio::time::timeout(
        Duration::from_secs(2),
        channel_result.unwrap().connect()
    ).await {
        Ok(Ok(_)) => true,
        _ => false,
    }
}

/// Get comprehensive server status
pub async fn get_server_status() -> ServerStatus {
    let pid = read_pid_file();

    let running = if let Some(p) = pid {
        ProcessUtils::is_process_running(nix::unistd::Pid::from_raw(p))
    } else {
        false
    };

    let responsive = if running {
        check_server_health().await
    } else {
        false
    };

    ServerStatus {
        running,
        pid,
        responsive,
    }
}

/// Get the path to the quilt daemon binary
/// Prefer installed production binary over development builds
fn get_daemon_binary() -> Result<PathBuf, String> {
    // Priority 1: Same binary as currently running
    if let Ok(exe) = env::current_exe() {
        return Ok(exe);
    }

    // Priority 2: System installation
    if let Ok(path) = which::which("quilt") {
        return Ok(path);
    }

    // Priority 3: Development builds (fallback)
    let dev_candidates = vec![
        PathBuf::from("./target/release/quilt"),
        PathBuf::from("./target/debug/quilt"),
    ];

    for candidate in dev_candidates {
        if candidate.exists() && candidate.is_file() {
            return Ok(candidate);
        }
    }

    Err("Could not find quilt daemon binary".to_string())
}

/// Start the daemon in background
/// Simple direct spawn - caller must have root privileges
pub fn start_daemon_background() -> Result<(), String> {
    let binary = get_daemon_binary()?;
    let log_path = PathBuf::from("/var/log/quilt-daemon.log");

    Logger::debug(&format!("Starting daemon from: {:?}", binary));

    // Create log directory if needed
    if let Some(parent) = log_path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| format!("Failed to create log directory: {}", e))?;
    }

    // Open log file
    use std::fs::OpenOptions;
    let log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .map_err(|e| format!("Failed to open log file: {}", e))?;

    // Spawn daemon process
    let child = Command::new(&binary)
        .arg("daemon")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file))
        .spawn()
        .map_err(|e| format!("Failed to spawn daemon: {}", e))?;

    // Write PID immediately
    write_pid_file(child.id())?;

    Logger::debug(&format!("Daemon spawned with PID: {}", child.id()));

    Ok(())
}

/// Ensure the server is running, start it if necessary
pub async fn ensure_server_running() -> Result<(), String> {
    // Check if already running and healthy
    let status = get_server_status().await;

    if status.running && status.responsive {
        Logger::debug("Daemon already running and responsive");
        return Ok(());
    }

    // If process exists but not responding, kill it
    if status.running && !status.responsive {
        Logger::debug("Cleaning up unresponsive daemon");
        if let Some(pid) = status.pid {
            let pid_obj = nix::unistd::Pid::from_raw(pid);
            let _ = ProcessUtils::send_signal(pid_obj, nix::sys::signal::Signal::SIGKILL);
            sleep(Duration::from_millis(500)).await;
        }
    }

    // Clean up stale PID file
    remove_pid_file();

    // Start daemon
    Logger::info("Starting Quilt daemon...");
    start_daemon_background()?;

    // Wait for daemon to be ready (with timeout)
    for attempt in 1..=20 {
        sleep(Duration::from_millis(250)).await;

        if check_server_health().await {
            Logger::success("Daemon started successfully");
            return Ok(());
        }

        if attempt % 4 == 0 {
            Logger::debug(&format!("Waiting for daemon... ({}s)", attempt / 4));
        }
    }

    Err("Daemon started but did not become responsive within 5 seconds".to_string())
}

/// Stop the daemon gracefully
pub fn stop_daemon(force: bool) -> Result<(), String> {
    let pid = read_pid_file()
        .ok_or("Daemon is not running (no PID file)")?;

    let pid_obj = nix::unistd::Pid::from_raw(pid);

    if !ProcessUtils::is_process_running(pid_obj) {
        remove_pid_file();
        return Err("Daemon is not running (stale PID file removed)".to_string());
    }

    Logger::info(&format!("Stopping daemon (PID: {})...", pid));

    if force {
        // Send SIGKILL immediately
        ProcessUtils::send_signal(pid_obj, nix::sys::signal::Signal::SIGKILL)
            .map_err(|e| format!("Failed to kill daemon: {}", e))?;
        Logger::success("Daemon killed");
    } else {
        // Graceful shutdown with timeout
        ProcessUtils::terminate_process(pid_obj, 10)
            .map_err(|e| format!("Failed to stop daemon: {}", e))?;
        Logger::success("Daemon stopped");
    }

    remove_pid_file();
    Ok(())
}

/// Restart the daemon
pub async fn restart_daemon() -> Result<(), String> {
    Logger::info("Restarting daemon...");

    // Try to stop if running
    if read_pid_file().is_some() {
        stop_daemon(false)?;
        // Give it a moment to clean up
        sleep(Duration::from_millis(500)).await;
    }

    // Start again
    ensure_server_running().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pid_file_path_not_empty() {
        let path = get_pid_file_path();
        assert!(!path.as_os_str().is_empty());
    }

    #[test]
    fn test_write_read_pid() {
        let test_pid = 12345u32;
        assert!(write_pid_file(test_pid).is_ok());

        let read_pid = read_pid_file();
        assert_eq!(read_pid, Some(12345));

        remove_pid_file();
        assert_eq!(read_pid_file(), None);
    }
}

//! minit - Minimal init process for Quilt containers
//!
//! Runs as PID1 inside container, providing:
//! - Process supervision and zombie reaping
//! - Unix socket exec interface for daemon
//! - Signal forwarding to child processes
//! - Persistent container lifecycle
//! - Health check endpoint
//! - Timeout reporting (non-killing)
//! - One-shot stdin support

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nix::sys::signal::{self, killpg, Signal};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::{
    chdir, close, dup2, execve, fork, pipe, setpgid, setsid, write as nix_write, ForkResult, Pid,
};
use serde::{Deserialize, Serialize};
use std::ffi::{CStr, CString};
use std::mem::ManuallyDrop;
use std::os::unix::io::FromRawFd;

const SOCKET_PATH: &str = "/run/minit.sock";
const VERSION: &str = "1.1.0";

// Track uptime for health checks
static START_TIME: AtomicU64 = AtomicU64::new(0);

/// Maximum output buffer size before truncation (1MB)
const MAX_OUTPUT_BUFFER_SIZE: usize = 1024 * 1024;

/// Maximum JSON request size (1MB)
const MAX_REQUEST_SIZE: usize = 1024 * 1024;

/// Efficient line-buffered output handler with bounded memory
struct LineBuffer {
    buffer: Vec<u8>,
    max_size: usize,
    truncated: bool,
}

impl LineBuffer {
    fn new(max_size: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(4096),
            max_size,
            truncated: false,
        }
    }

    /// Append data to buffer, enforcing size limit
    fn append(&mut self, data: &[u8]) {
        if self.truncated {
            return; // Already hit limit, drop further data
        }

        let available = self.max_size.saturating_sub(self.buffer.len());
        if data.len() <= available {
            self.buffer.extend_from_slice(data);
        } else {
            // Truncate to max size
            self.buffer.extend_from_slice(&data[..available]);
            self.truncated = true;
            eprintln!("minit: output buffer truncated at {} bytes", self.max_size);
        }
    }

    /// Extract complete lines from buffer, returning them and keeping remainder
    fn extract_lines(&mut self) -> Vec<Vec<u8>> {
        let mut lines = Vec::new();

        while let Some(pos) = self.buffer.iter().position(|&b| b == b'\n') {
            // Extract line (without newline)
            let line = self.buffer.drain(..pos).collect::<Vec<u8>>();
            self.buffer.drain(..1); // Remove newline
            lines.push(line);
        }

        lines
    }

    /// Get remaining buffered data (for final flush)
    fn take_remaining(&mut self) -> Vec<u8> {
        std::mem::take(&mut self.buffer)
    }

    /// Check if buffer was truncated
    fn was_truncated(&self) -> bool {
        self.truncated
    }
}

/// Base request with type discriminator
#[derive(Debug, Deserialize)]
struct BaseRequest {
    #[serde(rename = "type")]
    request_type: String,
}

/// Exec request from daemon
#[derive(Debug, Deserialize)]
struct ExecRequest {
    #[serde(rename = "type")]
    request_type: String, // Should be "exec"
    cmd: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: HashMap<String, String>,
    #[serde(default = "default_timeout")]
    timeout_ms: u64,
    /// One-shot stdin data to send to the command
    #[serde(default)]
    stdin_data: Option<String>,
}

fn default_timeout() -> u64 {
    30000 // 30 seconds
}

/// Health check request
#[derive(Debug, Deserialize)]
struct HealthRequest {
    #[serde(rename = "type")]
    request_type: String, // Should be "ping"
}

/// Kill request - send signal to a running process
#[derive(Debug, Deserialize)]
struct KillRequest {
    #[serde(rename = "type")]
    request_type: String, // Should be "kill"
    pid: i32, // Process ID to signal
    #[serde(default = "default_signal")]
    signal: String, // Signal name (TERM, KILL, etc.)
}

fn default_signal() -> String {
    "TERM".to_string()
}

/// Kill response
#[derive(Debug, Serialize)]
struct KillResponse {
    #[serde(rename = "type")]
    msg_type: String,
    success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

/// Process information for listing
#[derive(Debug, Serialize)]
struct ProcessInfo {
    pid: i32,
    ppid: i32,
    command: String,
    state: String,
    memory_kb: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    started_at: Option<i64>,
}

/// List processes response
#[derive(Debug, Serialize)]
struct ListProcessesResponse {
    #[serde(rename = "type")]
    msg_type: String,
    processes: Vec<ProcessInfo>,
}

/// Health check response
#[derive(Debug, Serialize)]
struct HealthResponse {
    #[serde(rename = "type")]
    msg_type: String,
    version: String,
    uptime_secs: u64,
    pid: u32,
}

/// Exec response to daemon
#[derive(Debug, Serialize)]
struct ExecResponse {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    elapsed_ms: Option<u64>,
    /// Process ID of the executed command
    #[serde(skip_serializing_if = "Option::is_none")]
    pid: Option<i32>,
}

fn main() {
    let debug_mode = std::env::var("QUILT_DEBUG").is_ok();
    if debug_mode {
        eprintln!("minit v{} starting as PID {}", VERSION, std::process::id());
    }

    // Record start time for uptime tracking
    START_TIME.store(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        Ordering::Relaxed,
    );

    // CRITICAL: Fix working directory inside chroot
    // The parent process may have set cwd to an absolute path that doesn't exist
    // inside the chroot (e.g., /var/lib/quilt/containers/...), causing execve to fail
    // with "No such file or directory" for valid binaries
    // Use nix::unistd::chdir directly for better compatibility with chroot
    if let Err(e) = chdir("/") {
        eprintln!("minit: FATAL: failed to chdir to /: {}", e);
        std::process::exit(101);
    }

    // Verify cwd is now valid
    match std::env::current_dir() {
        Ok(path) => {
            if debug_mode {
                eprintln!("minit: cwd verified as {:?}", path);
            }
        }
        Err(e) => {
            eprintln!("minit: FATAL: cwd still invalid after chdir('/'): {}", e);
            std::process::exit(102);
        }
    }

    // DIAGNOSTIC: List filesystem contents to verify rootfs (only when QUILT_DEBUG is set)
    if debug_mode {
        eprintln!("minit: DIAGNOSTIC - checking filesystem contents:");
        for dir in &["/", "/bin", "/usr", "/usr/bin"] {
            match std::fs::read_dir(dir) {
                Ok(entries) => {
                    let files: Vec<String> = entries
                        .filter_map(|e| e.ok())
                        .map(|e| e.file_name().to_string_lossy().to_string())
                        .take(10) // First 10 entries
                        .collect();
                    eprintln!("minit:   {} contains: {:?}", dir, files);
                }
                Err(e) => eprintln!("minit:   {} ERROR: {}", dir, e),
            }
        }

        // Check if specific binaries exist
        for bin in &["/bin/sh", "/bin/busybox", "/usr/bin/env"] {
            match std::fs::metadata(bin) {
                Ok(meta) => eprintln!(
                    "minit:   {} exists (is_file={}, len={})",
                    bin,
                    meta.is_file(),
                    meta.len()
                ),
                Err(e) => eprintln!("minit:   {} NOT FOUND: {}", bin, e),
            }
        }
    }

    // Parse arguments: minit [-- cmd args...]
    let args: Vec<String> = std::env::args().collect();
    let entrypoint = parse_entrypoint(&args);

    // Setup signal handlers
    let shutdown = Arc::new(AtomicBool::new(false));
    let entrypoint_pgid_shared = Arc::new(Mutex::new(None));
    setup_signal_handlers(shutdown.clone(), entrypoint_pgid_shared.clone());

    // Create Unix socket for exec requests
    let listener = match create_socket() {
        Ok(l) => l,
        Err(e) => {
            eprintln!("minit: failed to create socket: {}", e);
            std::process::exit(1);
        }
    };
    listener.set_nonblocking(true).ok();

    // Fork entrypoint if provided
    let mut entrypoint_pid: Option<u32> = None;
    let mut entrypoint_pgid: Option<i32> = None;
    if let Some((cmd, args)) = entrypoint {
        match fork_entrypoint(&cmd, &args) {
            Ok((pid, pgid)) => {
                eprintln!(
                    "minit: started entrypoint '{}' as PID {} (PGID {})",
                    cmd, pid, pgid
                );
                entrypoint_pid = Some(pid);
                entrypoint_pgid = Some(pgid);

                // Update shared PGID for signal forwarding
                if let Ok(mut guard) = entrypoint_pgid_shared.lock() {
                    *guard = Some(pgid);
                }
            }
            Err(e) => {
                eprintln!("minit: failed to start entrypoint: {}", e);
            }
        }
    }

    eprintln!("minit: ready for exec requests on {}", SOCKET_PATH);

    // Shared state for coordinating zombie reaping with exec worker threads.
    // When reap_zombies() reaps an exec child via waitpid(-1), it stores the
    // exit status here so the exec worker can retrieve it instead of getting ECHILD.
    let active_exec_pids: Arc<Mutex<HashSet<i32>>> = Arc::new(Mutex::new(HashSet::new()));
    let exec_exit_statuses: Arc<Mutex<HashMap<i32, i32>>> = Arc::new(Mutex::new(HashMap::new()));

    // Main loop
    loop {
        // Check for shutdown signal
        if shutdown.load(Ordering::Relaxed) {
            eprintln!("minit: received shutdown signal");
            break;
        }

        // Reap zombie processes
        reap_zombies(
            &mut entrypoint_pid,
            &entrypoint_pgid_shared,
            &active_exec_pids,
            &exec_exit_statuses,
        );

        // Accept exec requests (non-blocking)
        match listener.accept() {
            Ok((stream, _)) => {
                // Spawn worker thread to handle request concurrently
                let shutdown_clone = shutdown.clone();
                let exec_pids_clone = active_exec_pids.clone();
                let exec_statuses_clone = exec_exit_statuses.clone();
                std::thread::Builder::new()
                    .name("minit-worker".to_string())
                    .spawn(move || {
                        handle_request(
                            stream,
                            shutdown_clone,
                            exec_pids_clone,
                            exec_statuses_clone,
                        );
                    })
                    .expect("Failed to spawn worker thread (OOM or thread limit exceeded)");
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No connection waiting, continue
            }
            Err(e) => {
                eprintln!("minit: socket accept error: {}", e);
            }
        }

        // Brief sleep to avoid busy-waiting
        std::thread::sleep(Duration::from_millis(10));
    }

    // Graceful shutdown
    eprintln!("minit: shutting down...");

    // Stop accepting new connections
    drop(listener);

    // Wait briefly for in-flight worker threads to finish
    eprintln!("minit: waiting for worker threads to complete...");
    std::thread::sleep(Duration::from_secs(2));

    // Signal was already forwarded by signal handler
    // Wait for entrypoint to exit gracefully
    if entrypoint_pid.is_some() {
        eprintln!("minit: waiting for entrypoint to exit gracefully (10s timeout)");

        let start = Instant::now();
        let timeout = Duration::from_secs(10);

        while entrypoint_pid.is_some() && start.elapsed() < timeout {
            reap_zombies(
                &mut entrypoint_pid,
                &entrypoint_pgid_shared,
                &active_exec_pids,
                &exec_exit_statuses,
            );
            std::thread::sleep(Duration::from_millis(100));
        }

        // If still running after timeout, send SIGKILL to process group
        if let Some(pgid) = entrypoint_pgid {
            if entrypoint_pid.is_some() {
                eprintln!(
                    "minit: entrypoint did not exit after 10s, sending SIGKILL to process group"
                );
                let _ = killpg(Pid::from_raw(pgid), Signal::SIGKILL);

                // Wait a bit more for SIGKILL to take effect
                for _ in 0..20 {
                    reap_zombies(
                        &mut entrypoint_pid,
                        &entrypoint_pgid_shared,
                        &active_exec_pids,
                        &exec_exit_statuses,
                    );
                    if entrypoint_pid.is_none() {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    // Clear shared PGID
    if let Ok(mut guard) = entrypoint_pgid_shared.lock() {
        *guard = None;
    }

    // Final zombie reap
    reap_zombies(
        &mut entrypoint_pid,
        &entrypoint_pgid_shared,
        &active_exec_pids,
        &exec_exit_statuses,
    );

    // Cleanup socket
    let _ = std::fs::remove_file(SOCKET_PATH);

    eprintln!("minit: exiting");
}

fn parse_entrypoint(args: &[String]) -> Option<(String, Vec<String>)> {
    // Look for -- separator
    if let Some(pos) = args.iter().position(|a| a == "--") {
        if pos + 1 < args.len() {
            let cmd = args[pos + 1].clone();
            let cmd_args = args[pos + 2..].to_vec();
            return Some((cmd, cmd_args));
        }
    }
    None
}

fn setup_signal_handlers(shutdown: Arc<AtomicBool>, entrypoint_pgid: Arc<Mutex<Option<i32>>>) {
    // Signals to forward to entrypoint process group
    // SIGCHLD is NOT forwarded - it's for PID1's zombie reaping
    let forwardable_signals = vec![
        signal_hook::consts::SIGTERM,
        signal_hook::consts::SIGINT,
        signal_hook::consts::SIGHUP,
        signal_hook::consts::SIGUSR1,
        signal_hook::consts::SIGUSR2,
    ];

    let shutdown_clone = shutdown.clone();
    let pgid_clone = entrypoint_pgid.clone();

    std::thread::spawn(move || {
        let mut signals = signal_hook::iterator::Signals::new(&forwardable_signals)
            .expect("Failed to create signal iterator");

        for sig in signals.forever() {
            eprintln!("minit: received signal {}, forwarding to entrypoint", sig);

            // Forward signal to entrypoint process group immediately
            if let Ok(pgid_guard) = pgid_clone.lock() {
                if let Some(pgid) = *pgid_guard {
                    // Use killpg to signal entire process group
                    let signal = match sig {
                        signal_hook::consts::SIGTERM => Signal::SIGTERM,
                        signal_hook::consts::SIGINT => Signal::SIGINT,
                        signal_hook::consts::SIGHUP => Signal::SIGHUP,
                        signal_hook::consts::SIGUSR1 => Signal::SIGUSR1,
                        signal_hook::consts::SIGUSR2 => Signal::SIGUSR2,
                        _ => continue, // Unknown signal, skip
                    };

                    match killpg(Pid::from_raw(pgid), signal) {
                        Ok(_) => {
                            eprintln!("minit: forwarded {:?} to process group {}", signal, pgid);
                        }
                        Err(e) => {
                            // Process group may have exited - this is normal
                            eprintln!("minit: failed to forward signal to PGID {}: {}", pgid, e);
                        }
                    }
                } else {
                    eprintln!("minit: no entrypoint running, signal not forwarded");
                }
            }

            // For SIGTERM/SIGINT, also trigger shutdown
            if sig == signal_hook::consts::SIGTERM || sig == signal_hook::consts::SIGINT {
                eprintln!("minit: shutdown signal received, initiating graceful shutdown");
                shutdown_clone.store(true, Ordering::Relaxed);
                break;
            }
        }
    });
}

fn create_socket() -> Result<UnixListener, String> {
    use std::os::unix::fs::PermissionsExt;

    // Remove existing socket if present
    let _ = std::fs::remove_file(SOCKET_PATH);

    // Ensure /run exists
    std::fs::create_dir_all("/run").map_err(|e| format!("Failed to create /run: {}", e))?;

    // Bind socket
    let listener = UnixListener::bind(SOCKET_PATH)
        .map_err(|e| format!("Failed to bind socket {}: {}", SOCKET_PATH, e))?;

    // SECURITY: Set restrictive permissions (0600 = owner-only)
    // This prevents non-root processes from connecting to the socket
    let perms = std::fs::Permissions::from_mode(0o600);
    std::fs::set_permissions(SOCKET_PATH, perms)
        .map_err(|e| format!("Failed to set socket permissions: {}", e))?;

    eprintln!(
        "minit: socket created at {} with permissions 0600 (root-only)",
        SOCKET_PATH
    );
    Ok(listener)
}

fn fork_entrypoint(cmd: &str, args: &[String]) -> Result<(u32, i32), String> {
    eprintln!(
        "minit: spawning entrypoint using raw fork+execve: {} {:?}",
        cmd, args
    );

    // Inherit environment from parent process (includes QUILT_ACCESS_TOKEN, etc.)
    // Add essential defaults if not present
    let mut env_map: std::collections::HashMap<String, String> = std::env::vars().collect();
    env_map
        .entry("PATH".to_string())
        .or_insert_with(|| "/bin:/usr/bin:/sbin:/usr/sbin".to_string());
    env_map
        .entry("HOME".to_string())
        .or_insert_with(|| "/".to_string());
    env_map
        .entry("TERM".to_string())
        .or_insert_with(|| "xterm".to_string());

    // Resolve command to absolute path
    let resolved_cmd = resolve_command_path(cmd, &env_map)?;
    eprintln!("minit: resolved '{}' to '{}'", cmd, resolved_cmd);

    // Convert command and args to CStrings for execve
    let cmd_cstr = CString::new(resolved_cmd.as_str())
        .map_err(|e| format!("Invalid command string: {}", e))?;

    // Build argv: [cmd, args..., NULL]
    let mut argv_cstrings = vec![cmd_cstr.clone()];
    for arg in args {
        argv_cstrings.push(CString::new(arg.as_str()).map_err(|e| format!("Invalid arg: {}", e))?);
    }
    let argv: Vec<&CStr> = argv_cstrings.iter().map(|s| s.as_c_str()).collect();

    let env_cstrings: Vec<CString> = env_map
        .iter()
        .filter_map(|(k, v)| CString::new(format!("{}={}", k, v)).ok())
        .collect();
    let envp: Vec<&CStr> = env_cstrings.iter().map(|s| s.as_c_str()).collect();

    eprintln!("minit: about to fork for entrypoint");

    match unsafe { fork() } {
        Ok(ForkResult::Child) => {
            // Child process - this will become the entrypoint
            eprintln!("minit: child process, about to execve {}", cmd);

            // CRITICAL: Create new process group with child as leader
            // This allows signal forwarding to the entire process group
            // setpgid(0, 0) makes this process the process group leader
            if let Err(e) = setpgid(Pid::from_raw(0), Pid::from_raw(0)) {
                eprintln!("minit: WARNING: failed to create process group: {}", e);
                // Continue anyway - signal forwarding will fall back to single PID
            }

            // execve replaces this process
            match execve(&cmd_cstr, &argv, &envp) {
                Ok(_) => unreachable!(), // execve never returns on success
                Err(e) => {
                    eprintln!("minit: EXECVE FAILED: {} - errno: {}", cmd, e);
                    std::process::exit(127);
                }
            }
        }
        Ok(ForkResult::Parent { child }) => {
            let pid = child.as_raw() as u32;
            let pgid = child.as_raw(); // PGID equals PID for process group leader
            eprintln!(
                "minit: forked child PID {} for entrypoint (PGID {})",
                pid, pgid
            );
            Ok((pid, pgid))
        }
        Err(e) => Err(format!("minit: FORK_FAILED: {}", e)),
    }
}

/// SECURITY: Dangerous environment variables that can be used for code injection
/// These are blocked from exec requests to prevent library preloading attacks
const DANGEROUS_ENV_VARS: &[&str] = &[
    // Library loading (code injection)
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
    "LD_AUDIT",
    "LD_PROFILE",
    // Dynamic linker control
    "LD_DEBUG",
    "LD_DEBUG_OUTPUT",
    "LD_SHOW_AUXV",
    "LD_BIND_NOW",
    // glibc tunables (behavior modification)
    "GLIBC_TUNABLES",
    "MALLOC_CHECK_",
    "MALLOC_PERTURB_",
    // Other injection vectors
    "PYTHONPATH", // Python module injection
    "PERL5LIB",   // Perl module injection
    "RUBYLIB",    // Ruby library injection
    "NODE_PATH",  // Node.js module injection
];

/// SECURITY: Validate peer credentials on Unix socket connection
/// Ensures only root (UID 0) can connect to minit socket
fn validate_peer_credentials(stream: &UnixStream) -> Result<(), String> {
    use std::os::unix::io::AsRawFd;

    let fd = stream.as_raw_fd();
    let mut ucred: libc::ucred = unsafe { std::mem::zeroed() };
    let mut ucred_size: libc::socklen_t = std::mem::size_of::<libc::ucred>() as libc::socklen_t;

    let ret = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut ucred as *mut _ as *mut libc::c_void,
            &mut ucred_size as *mut libc::socklen_t,
        )
    };

    if ret != 0 {
        return Err("Failed to get peer credentials".to_string());
    }

    // Validate UID is 0 (root)
    if ucred.uid != 0 {
        eprintln!(
            "minit: SECURITY: rejected connection from UID {} (only root allowed)",
            ucred.uid
        );
        return Err(format!(
            "Permission denied: only root can connect (your UID: {})",
            ucred.uid
        ));
    }

    // Validation passed
    eprintln!(
        "minit: peer credentials validated: UID={}, PID={}",
        ucred.uid, ucred.pid
    );
    Ok(())
}

/// Validate environment variables for security issues
fn validate_env_vars(env: &HashMap<String, String>) -> Result<(), String> {
    for key in env.keys() {
        let key_upper = key.to_uppercase();
        for dangerous in DANGEROUS_ENV_VARS {
            if key_upper == *dangerous {
                return Err(format!(
                    "Security: environment variable '{}' is not allowed (code injection risk)",
                    key
                ));
            }
        }
    }
    Ok(())
}

/// Resolve command to absolute path by searching PATH
/// Returns absolute path if found, original command if already absolute
fn resolve_command_path(cmd: &str, env_map: &HashMap<String, String>) -> Result<String, String> {
    // If already absolute, verify it exists
    if cmd.starts_with('/') {
        if std::path::Path::new(cmd).exists() {
            return Ok(cmd.to_string());
        }
        return Err(format!("Command not found: {}", cmd));
    }

    // Get PATH from environment
    let path_var = env_map
        .get("PATH")
        .map(|s| s.as_str())
        .unwrap_or("/bin:/usr/bin:/sbin:/usr/sbin");

    // Search each PATH directory
    for dir in path_var.split(':') {
        if dir.is_empty() {
            continue;
        }
        let candidate = format!("{}/{}", dir.trim_end_matches('/'), cmd);
        let path = std::path::Path::new(&candidate);

        // Check if file exists and is executable
        if let Ok(metadata) = std::fs::metadata(path) {
            if metadata.is_file() {
                // Check executable bit (any of owner/group/other)
                use std::os::unix::fs::PermissionsExt;
                let mode = metadata.permissions().mode();
                if mode & 0o111 != 0 {
                    return Ok(candidate);
                }
            }
        }
    }

    Err(format!(
        "Command not found in PATH: {} (searched: {})",
        cmd, path_var
    ))
}

fn reap_zombies(
    entrypoint_pid: &mut Option<u32>,
    entrypoint_pgid_shared: &Arc<Mutex<Option<i32>>>,
    active_exec_pids: &Arc<Mutex<HashSet<i32>>>,
    exec_exit_statuses: &Arc<Mutex<HashMap<i32, i32>>>,
) {
    loop {
        match waitpid(Pid::from_raw(-1), Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(pid, code)) => {
                let pid_i32 = pid.as_raw();
                eprintln!("minit: process {} exited with code {}", pid_i32, code);

                // If this is an active exec child, store exit status for the worker thread
                if let Ok(mut pids) = active_exec_pids.lock() {
                    if pids.remove(&pid_i32) {
                        if let Ok(mut statuses) = exec_exit_statuses.lock() {
                            statuses.insert(pid_i32, code);
                        }
                        continue; // Don't process as entrypoint
                    }
                }

                // Check if this was the entrypoint
                if let Some(ep_pid) = *entrypoint_pid {
                    if ep_pid == pid_i32 as u32 {
                        eprintln!("minit: entrypoint exited, container stays alive");
                        *entrypoint_pid = None;

                        // Clear shared PGID as well
                        if let Ok(mut guard) = entrypoint_pgid_shared.lock() {
                            *guard = None;
                        }
                    }
                }
            }
            Ok(WaitStatus::Signaled(pid, signal, _)) => {
                let pid_i32 = pid.as_raw();
                eprintln!("minit: process {} killed by signal {:?}", pid_i32, signal);

                // If this is an active exec child, store exit status for the worker thread
                let sig_code = 128 + signal as i32;
                if let Ok(mut pids) = active_exec_pids.lock() {
                    if pids.remove(&pid_i32) {
                        if let Ok(mut statuses) = exec_exit_statuses.lock() {
                            statuses.insert(pid_i32, sig_code);
                        }
                        continue; // Don't process as entrypoint
                    }
                }

                if let Some(ep_pid) = *entrypoint_pid {
                    if ep_pid == pid_i32 as u32 {
                        eprintln!("minit: entrypoint killed, container stays alive");
                        *entrypoint_pid = None;

                        // Clear shared PGID as well
                        if let Ok(mut guard) = entrypoint_pgid_shared.lock() {
                            *guard = None;
                        }
                    }
                }
            }
            Ok(WaitStatus::StillAlive) | Err(nix::errno::Errno::ECHILD) => {
                // No more zombies to reap
                break;
            }
            Ok(_) => {
                // Other wait status, continue reaping
            }
            Err(e) => {
                eprintln!("minit: waitpid error: {}", e);
                break;
            }
        }
    }
}

fn handle_request(
    stream: UnixStream,
    shutdown: Arc<AtomicBool>,
    active_exec_pids: Arc<Mutex<HashSet<i32>>>,
    exec_exit_statuses: Arc<Mutex<HashMap<i32, i32>>>,
) {
    // Check shutdown before processing
    if shutdown.load(Ordering::Relaxed) {
        eprintln!("minit: worker thread exiting due to shutdown");
        return;
    }

    // SECURITY: Validate peer credentials FIRST, before any processing
    if let Err(e) = validate_peer_credentials(&stream) {
        eprintln!("minit: security: {}", e);
        // Send error response
        let mut stream = stream;
        send_error(&mut stream, &format!("Security: {}", e));
        return;
    }

    // Set timeout on stream
    let mut stream = stream;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    stream.set_write_timeout(Some(Duration::from_secs(30))).ok();

    // Read request (single line JSON) with size limit
    let reader = BufReader::new(&stream);
    let mut limited_reader = reader.take(MAX_REQUEST_SIZE as u64);
    let mut request_line = String::new();

    if let Err(e) = limited_reader.read_line(&mut request_line) {
        eprintln!("minit: failed to read exec request: {}", e);
        send_error(&mut stream, &format!("Read error: {}", e));
        return;
    }

    // Check if we hit the size limit
    if request_line.len() >= MAX_REQUEST_SIZE {
        eprintln!(
            "minit: request too large: {} bytes (max: {})",
            request_line.len(),
            MAX_REQUEST_SIZE
        );
        send_error(
            &mut stream,
            &format!(
                "Request too large: {} bytes (maximum: {} bytes)",
                request_line.len(),
                MAX_REQUEST_SIZE
            ),
        );
        return;
    }

    if request_line.is_empty() {
        eprintln!("minit: empty request received");
        send_error(&mut stream, "Empty request");
        return;
    }

    // Parse base request to get type
    let base_request: BaseRequest = match serde_json::from_str(&request_line) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("minit: invalid request JSON: {}", e);
            send_error(&mut stream, &format!("Invalid JSON: {}", e));
            return;
        }
    };

    // Dispatch based on type
    match base_request.request_type.as_str() {
        "ping" => {
            // Validate full request structure
            let health_req: HealthRequest = match serde_json::from_str(&request_line) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("minit: invalid ping request: {}", e);
                    send_error(&mut stream, &format!("Invalid ping request: {}", e));
                    return;
                }
            };

            // Validate type field matches
            if health_req.request_type != "ping" {
                eprintln!(
                    "minit: type mismatch: expected 'ping', got '{}'",
                    health_req.request_type
                );
                send_error(&mut stream, "Type field must be 'ping' for health check");
                return;
            }

            eprintln!("minit: health check ping received");
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            let start = START_TIME.load(Ordering::Relaxed);
            let uptime = now.saturating_sub(start);

            let response = HealthResponse {
                msg_type: "pong".to_string(),
                version: VERSION.to_string(),
                uptime_secs: uptime,
                pid: std::process::id(),
            };
            let _ = send_json(&mut stream, &response);
        }

        "kill" => {
            let kill_req: KillRequest = match serde_json::from_str(&request_line) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("minit: invalid kill request: {}", e);
                    send_error(&mut stream, &format!("Invalid kill request: {}", e));
                    return;
                }
            };

            // Validate type field matches
            if kill_req.request_type != "kill" {
                eprintln!(
                    "minit: type mismatch: expected 'kill', got '{}'",
                    kill_req.request_type
                );
                send_error(&mut stream, "Type field must be 'kill' for kill request");
                return;
            }

            eprintln!(
                "minit: kill request for PID {} with signal {}",
                kill_req.pid, kill_req.signal
            );

            // Parse signal name to nix Signal
            let sig = match kill_req.signal.to_uppercase().as_str() {
                "TERM" | "SIGTERM" => Signal::SIGTERM,
                "KILL" | "SIGKILL" => Signal::SIGKILL,
                "INT" | "SIGINT" => Signal::SIGINT,
                "HUP" | "SIGHUP" => Signal::SIGHUP,
                "USR1" | "SIGUSR1" => Signal::SIGUSR1,
                "USR2" | "SIGUSR2" => Signal::SIGUSR2,
                "CONT" | "SIGCONT" => Signal::SIGCONT,
                "STOP" | "SIGSTOP" => Signal::SIGSTOP,
                _ => {
                    let response = KillResponse {
                        msg_type: "kill_result".to_string(),
                        success: false,
                        error: Some(format!("Unknown signal: {}", kill_req.signal)),
                    };
                    let _ = send_json(&mut stream, &response);
                    return;
                }
            };

            // Send signal to process
            match signal::kill(Pid::from_raw(kill_req.pid), sig) {
                Ok(_) => {
                    eprintln!("minit: sent {} to PID {}", kill_req.signal, kill_req.pid);
                    let response = KillResponse {
                        msg_type: "kill_result".to_string(),
                        success: true,
                        error: None,
                    };
                    let _ = send_json(&mut stream, &response);
                }
                Err(e) => {
                    eprintln!("minit: failed to send signal: {}", e);
                    let response = KillResponse {
                        msg_type: "kill_result".to_string(),
                        success: false,
                        error: Some(format!("Failed to send signal: {}", e)),
                    };
                    let _ = send_json(&mut stream, &response);
                }
            }
        }

        "list_processes" => {
            eprintln!("minit: list_processes request");

            // Read /proc to list all processes
            let mut processes = Vec::new();
            if let Ok(entries) = std::fs::read_dir("/proc") {
                for entry in entries.flatten() {
                    let name = entry.file_name();
                    let name_str = name.to_string_lossy();
                    // Check if directory name is a PID (numeric)
                    if let Ok(pid) = name_str.parse::<i32>() {
                        if let Some(info) = read_process_info(pid) {
                            processes.push(info);
                        }
                    }
                }
            }

            // Sort by PID for consistent output
            processes.sort_by_key(|p| p.pid);

            let response = ListProcessesResponse {
                msg_type: "processes".to_string(),
                processes,
            };
            let _ = send_json(&mut stream, &response);
        }

        "exec" => {
            let exec_req: ExecRequest = match serde_json::from_str(&request_line) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("minit: invalid exec request: {}", e);
                    send_error(&mut stream, &format!("Invalid exec request: {}", e));
                    return;
                }
            };

            // Validate type field matches
            if exec_req.request_type != "exec" {
                eprintln!(
                    "minit: type mismatch: expected 'exec', got '{}'",
                    exec_req.request_type
                );
                send_error(&mut stream, "Type field must be 'exec' for exec request");
                return;
            }

            eprintln!(
                "minit: exec request: {} {:?} (timeout: {}ms)",
                exec_req.cmd, exec_req.args, exec_req.timeout_ms
            );
            execute_command(
                &mut stream,
                exec_req,
                &active_exec_pids,
                &exec_exit_statuses,
            );
        }

        unknown => {
            eprintln!("minit: unknown request type: {}", unknown);
            send_error(
                &mut stream,
                &format!(
                    "Unknown request type: '{}'. Supported types: ping, kill, exec, list_processes",
                    unknown
                ),
            );
        }
    }
}

fn execute_command(
    stream: &mut UnixStream,
    request: ExecRequest,
    active_exec_pids: &Arc<Mutex<HashSet<i32>>>,
    exec_exit_statuses: &Arc<Mutex<HashMap<i32, i32>>>,
) {
    eprintln!(
        "minit: executing command using raw fork+execve: {} {:?}",
        request.cmd, request.args
    );

    // SECURITY: Validate environment variables FIRST
    if let Err(e) = validate_env_vars(&request.env) {
        eprintln!("minit: {}", e);
        send_error(stream, &e);
        return;
    }

    let start_time = Instant::now();
    let timeout = Duration::from_millis(request.timeout_ms);
    let mut timeout_reported = false;

    // Create pipes for stdin, stdout, and stderr
    let (stdin_read, stdin_write) = match pipe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("minit: failed to create stdin pipe: {}", e);
            send_error(stream, &format!("Pipe error: {}", e));
            return;
        }
    };

    let (stdout_read, stdout_write) = match pipe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("minit: failed to create stdout pipe: {}", e);
            let _ = close(stdin_read);
            let _ = close(stdin_write);
            send_error(stream, &format!("Pipe error: {}", e));
            return;
        }
    };

    let (stderr_read, stderr_write) = match pipe() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("minit: failed to create stderr pipe: {}", e);
            let _ = close(stdin_read);
            let _ = close(stdin_write);
            let _ = close(stdout_read);
            let _ = close(stdout_write);
            send_error(stream, &format!("Pipe error: {}", e));
            return;
        }
    };

    // Build environment - inherit from parent (minit's environment has QUILT_ACCESS_TOKEN, etc.)
    // Add request-specific env vars on top
    let mut env_map: std::collections::HashMap<String, String> = std::env::vars().collect();
    env_map
        .entry("PATH".to_string())
        .or_insert_with(|| "/bin:/usr/bin:/sbin:/usr/sbin".to_string());
    env_map
        .entry("HOME".to_string())
        .or_insert_with(|| "/".to_string());
    env_map
        .entry("TERM".to_string())
        .or_insert_with(|| "xterm".to_string());
    // Override with request-specific env vars
    for (key, value) in &request.env {
        env_map.insert(key.clone(), value.clone());
    }

    // Resolve command to absolute path BEFORE fork
    let resolved_cmd = match resolve_command_path(&request.cmd, &env_map) {
        Ok(path) => path,
        Err(e) => {
            eprintln!("minit: {}", e);
            send_error(stream, &e);
            return;
        }
    };
    eprintln!("minit: resolved '{}' to '{}'", request.cmd, resolved_cmd);

    // Convert command and args to CStrings
    let cmd_cstr = match CString::new(resolved_cmd.as_str()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("minit: invalid command string: {}", e);
            send_error(stream, &format!("Invalid command: {}", e));
            return;
        }
    };

    let mut argv_cstrings = vec![cmd_cstr.clone()];
    for arg in &request.args {
        match CString::new(arg.as_str()) {
            Ok(c) => argv_cstrings.push(c),
            Err(e) => {
                eprintln!("minit: invalid arg string: {}", e);
                send_error(stream, &format!("Invalid arg: {}", e));
                return;
            }
        }
    }
    let argv: Vec<&CStr> = argv_cstrings.iter().map(|s| s.as_c_str()).collect();

    let env_cstrings: Vec<CString> = env_map
        .iter()
        .filter_map(|(k, v)| CString::new(format!("{}={}", k, v)).ok())
        .collect();
    let envp: Vec<&CStr> = env_cstrings.iter().map(|s| s.as_c_str()).collect();

    eprintln!("minit: about to fork for exec command");

    match unsafe { fork() } {
        Ok(ForkResult::Child) => {
            // Child process

            // Create a new session and process group for this exec.
            // This is critical for background processes: when the shell runs
            // "sleep 60 &", the sleep becomes part of this session. When the
            // shell exits, background processes continue running independently.
            let _ = setsid();

            // CRITICAL: Ignore SIGPIPE so background processes don't die when
            // writing to closed pipes. Without this, when minit closes the pipe
            // read ends after the shell exits, any background process that writes
            // to stdout/stderr receives SIGPIPE and terminates. With SIG_IGN,
            // writes to broken pipes fail with EPIPE instead (harmless).
            // This is the industry standard approach used by nohup, Docker, etc.
            unsafe {
                libc::signal(libc::SIGPIPE, libc::SIG_IGN);
            }

            // Close write end of stdin, read ends of stdout/stderr
            let _ = close(stdin_write);
            let _ = close(stdout_read);
            let _ = close(stderr_read);

            // Redirect stdin from pipe
            let _ = dup2(stdin_read, 0); // stdin
            let _ = close(stdin_read);

            // Redirect stdout and stderr to pipes
            let _ = dup2(stdout_write, 1); // stdout
            let _ = dup2(stderr_write, 2); // stderr

            // Close write ends (now duplicated)
            let _ = close(stdout_write);
            let _ = close(stderr_write);

            // Execute the command
            match execve(&cmd_cstr, &argv, &envp) {
                Ok(_) => unreachable!(),
                Err(e) => {
                    // Write error to stderr (which goes to pipe)
                    let err_msg = format!("execve failed: {}\n", e);
                    let _ = nix_write(2, err_msg.as_bytes()); // 2 = stderr fd
                    std::process::exit(127);
                }
            }
        }
        Ok(ForkResult::Parent { child }) => {
            // Parent process
            let child_pid_raw = child.as_raw();

            // Register this child PID so reap_zombies() stores its exit status
            // instead of discarding it. Must happen before any sleep/poll.
            if let Ok(mut pids) = active_exec_pids.lock() {
                pids.insert(child_pid_raw);
            }

            // Close read end of stdin, write ends of stdout/stderr
            let _ = close(stdin_read);
            let _ = close(stdout_write);
            let _ = close(stderr_write);

            // Write stdin data if provided
            if let Some(ref stdin_data) = request.stdin_data {
                if let Err(e) = nix_write(stdin_write, stdin_data.as_bytes()) {
                    eprintln!("minit: warning: failed to write stdin data: {}", e);
                }
            }

            // CRITICAL: Always close stdin_write after one-shot payload to prevent FD leak
            // Child process expects EOF to proceed. Without this, both child and parent leak.
            if let Err(e) = close(stdin_write) {
                eprintln!("minit: warning: failed to close stdin_write: {}", e);
            }

            // Make stdout/stderr non-blocking for timeout-aware reading
            use nix::fcntl::{fcntl, FcntlArg, OFlag};
            let _ = fcntl(stdout_read, FcntlArg::F_SETFL(OFlag::O_NONBLOCK));
            let _ = fcntl(stderr_read, FcntlArg::F_SETFL(OFlag::O_NONBLOCK));

            // Read output with timeout tracking
            // CRITICAL: Use ManuallyDrop to prevent automatic pipe closure when
            // the function returns. If we use regular File objects, they close
            // on drop, sending SIGPIPE to any background processes still writing
            // to stdout/stderr. With ManuallyDrop, we explicitly close pipes only
            // after giving background processes time to detach from the pipes.
            let mut stdout_file =
                ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(stdout_read) });
            let mut stderr_file =
                ManuallyDrop::new(unsafe { std::fs::File::from_raw_fd(stderr_read) });
            let mut stdout_line_buffer = LineBuffer::new(MAX_OUTPUT_BUFFER_SIZE);
            let mut stderr_line_buffer = LineBuffer::new(MAX_OUTPUT_BUFFER_SIZE);
            let mut child_exited = false;
            let mut exit_code = -1i32;

            let mut process_killed = false;

            loop {
                // Check for timeout
                let elapsed = start_time.elapsed();
                if elapsed > timeout && !timeout_reported {
                    timeout_reported = true;

                    // Kill the process on timeout (SIGTERM first, then SIGKILL)
                    eprintln!(
                        "minit: timeout after {}ms, killing process {}",
                        elapsed.as_millis(),
                        child_pid_raw
                    );

                    // Send SIGTERM for graceful shutdown
                    if let Err(e) = signal::kill(child, Signal::SIGTERM) {
                        eprintln!("minit: failed to send SIGTERM: {}", e);
                    } else {
                        // Give process 2 seconds to exit gracefully
                        std::thread::sleep(Duration::from_secs(2));

                        // Check if still alive
                        match waitpid(child, Some(WaitPidFlag::WNOHANG)) {
                            Ok(WaitStatus::StillAlive) => {
                                // Force kill with SIGKILL
                                eprintln!(
                                    "minit: process {} still alive, sending SIGKILL",
                                    child_pid_raw
                                );
                                let _ = signal::kill(child, Signal::SIGKILL);
                            }
                            _ => {
                                // Process already exited from SIGTERM
                            }
                        }
                    }
                    process_killed = true;

                    let response = ExecResponse {
                        msg_type: "timeout".to_string(),
                        data: Some(format!(
                            "Command exceeded timeout of {}ms. Process {} was terminated. \
                            For long-running commands, use WebSocket terminal: GET /ws/containers/:id/exec \
                            or set higher timeout_ms (max 600000).",
                            request.timeout_ms, child_pid_raw
                        )),
                        code: Some(128 + 15), // SIGTERM exit code
                        pid: Some(child_pid_raw),
                        elapsed_ms: Some(elapsed.as_millis() as u64),
                    };
                    let _ = send_json(stream, &response);
                }

                // Try to read stdout (non-blocking)
                let mut buf = [0u8; 4096];
                match stdout_file.read(&mut buf) {
                    Ok(0) => {} // EOF or would block
                    Ok(n) => {
                        // Append to buffer (enforces size limit)
                        stdout_line_buffer.append(&buf[..n]);

                        // Extract and send complete lines
                        for line_bytes in stdout_line_buffer.extract_lines() {
                            let line = String::from_utf8_lossy(&line_bytes).to_string();
                            let response = ExecResponse {
                                msg_type: "stdout".to_string(),
                                data: Some(line),
                                code: None,
                                elapsed_ms: None,
                                pid: None,
                            };
                            if send_json(stream, &response).is_err() {
                                eprintln!("minit: failed to send stdout, client disconnected");
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => eprintln!("minit: stdout read error: {}", e),
                }

                // Try to read stderr (non-blocking)
                match stderr_file.read(&mut buf) {
                    Ok(0) => {} // EOF or would block
                    Ok(n) => {
                        // Append to buffer (enforces size limit)
                        stderr_line_buffer.append(&buf[..n]);

                        // Extract and send complete lines
                        for line_bytes in stderr_line_buffer.extract_lines() {
                            let line = String::from_utf8_lossy(&line_bytes).to_string();
                            let response = ExecResponse {
                                msg_type: "stderr".to_string(),
                                data: Some(line),
                                code: None,
                                elapsed_ms: None,
                                pid: None,
                            };
                            if send_json(stream, &response).is_err() {
                                eprintln!("minit: failed to send stderr, client disconnected");
                            }
                        }
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(e) => eprintln!("minit: stderr read error: {}", e),
                }

                // Check if child has exited (non-blocking)
                if !child_exited {
                    // First, check if reap_zombies() already collected this child's exit status
                    if let Ok(mut statuses) = exec_exit_statuses.lock() {
                        if let Some(code) = statuses.remove(&child_pid_raw) {
                            exit_code = code;
                            child_exited = true;
                            eprintln!("minit: child exited with code {} (via reaper)", code);
                        }
                    }

                    // If not found in shared map, try waitpid directly (worker wins the race)
                    if !child_exited {
                        match waitpid(child, Some(WaitPidFlag::WNOHANG)) {
                            Ok(WaitStatus::Exited(_, code)) => {
                                exit_code = code;
                                child_exited = true;
                                eprintln!("minit: child exited with code {}", code);
                            }
                            Ok(WaitStatus::Signaled(_, sig, _)) => {
                                exit_code = 128 + sig as i32;
                                child_exited = true;
                                eprintln!("minit: child killed by signal {:?}", sig);
                            }
                            Ok(WaitStatus::StillAlive) => {
                                // Still running
                            }
                            Ok(_) => {}
                            Err(nix::errno::Errno::ECHILD) => {
                                // Main thread reaped it — check shared map one more time
                                child_exited = true;
                                if let Ok(mut statuses) = exec_exit_statuses.lock() {
                                    if let Some(code) = statuses.remove(&child_pid_raw) {
                                        exit_code = code;
                                        eprintln!("minit: child exited with code {} (via reaper after ECHILD)", code);
                                    } else {
                                        eprintln!(
                                            "minit: warning: child reaped but exit status lost"
                                        );
                                    }
                                }
                            }
                            Err(e) => {
                                eprintln!("minit: waitpid error: {}", e);
                                child_exited = true;
                            }
                        }
                    }
                }

                // If child exited, do one more read pass then exit
                if child_exited {
                    // Give a moment for final output to be available
                    std::thread::sleep(Duration::from_millis(10));

                    // Final read pass for any remaining data
                    if let Ok(n) = stdout_file.read(&mut buf) {
                        if n > 0 {
                            stdout_line_buffer.append(&buf[..n]);
                        }
                    }
                    if let Ok(n) = stderr_file.read(&mut buf) {
                        if n > 0 {
                            stderr_line_buffer.append(&buf[..n]);
                        }
                    }

                    // Send any remaining buffered data (final flush)
                    let remaining_stdout = stdout_line_buffer.take_remaining();
                    if !remaining_stdout.is_empty() {
                        let data = String::from_utf8_lossy(&remaining_stdout).to_string();
                        let response = ExecResponse {
                            msg_type: "stdout".to_string(),
                            data: Some(data),
                            code: None,
                            elapsed_ms: None,
                            pid: None,
                        };
                        let _ = send_json(stream, &response);
                    }

                    let remaining_stderr = stderr_line_buffer.take_remaining();
                    if !remaining_stderr.is_empty() {
                        let data = String::from_utf8_lossy(&remaining_stderr).to_string();
                        let response = ExecResponse {
                            msg_type: "stderr".to_string(),
                            data: Some(data),
                            code: None,
                            elapsed_ms: None,
                            pid: None,
                        };
                        let _ = send_json(stream, &response);
                    }

                    // Log if truncation occurred
                    if stdout_line_buffer.was_truncated() || stderr_line_buffer.was_truncated() {
                        eprintln!("minit: warning: output was truncated due to buffer size limit");
                    }

                    break;
                }

                // Brief sleep to avoid busy-waiting
                std::thread::sleep(Duration::from_millis(10));
            }

            // Clean up shared exec tracking state
            if let Ok(mut pids) = active_exec_pids.lock() {
                pids.remove(&child_pid_raw);
            }
            if let Ok(mut statuses) = exec_exit_statuses.lock() {
                statuses.remove(&child_pid_raw);
            }

            // Send exit code
            let elapsed = start_time.elapsed();
            // If process was killed due to timeout and exit code wasn't captured, use SIGTERM code
            let final_exit_code = if process_killed && exit_code == -1 {
                128 + 15 // SIGTERM
            } else {
                exit_code
            };
            let response = ExecResponse {
                msg_type: "exit".to_string(),
                data: if process_killed {
                    Some("Process terminated due to timeout".to_string())
                } else {
                    None
                },
                code: Some(final_exit_code),
                elapsed_ms: Some(elapsed.as_millis() as u64),
                pid: Some(child_pid_raw),
            };
            if let Err(e) = send_json(stream, &response) {
                eprintln!(
                    "minit: FAILED to send exit message (code={}, elapsed={}ms): {}",
                    final_exit_code,
                    elapsed.as_millis(),
                    e
                );
            }

            // Allow client time to read exit message before closing connection
            std::thread::sleep(Duration::from_millis(50));

            // CRITICAL: Explicitly close pipes to prevent FD leak.
            // ManuallyDrop prevents automatic closure, so we must do it manually.
            // With SIGPIPE ignored (set in child), this is safe - background
            // processes will get EPIPE on write instead of being killed.
            drop(ManuallyDrop::into_inner(stdout_file));
            drop(ManuallyDrop::into_inner(stderr_file));

            eprintln!(
                "minit: exec completed with code {} in {}ms",
                exit_code,
                elapsed.as_millis()
            );
        }
        Err(e) => {
            eprintln!("minit: fork failed: {}", e);
            let _ = close(stdin_read);
            let _ = close(stdin_write);
            let _ = close(stdout_read);
            let _ = close(stdout_write);
            let _ = close(stderr_read);
            let _ = close(stderr_write);
            send_error(stream, &format!("Fork error: {}", e));
        }
    }
}

/// Read process information from /proc/[pid]
fn read_process_info(pid: i32) -> Option<ProcessInfo> {
    // Read /proc/[pid]/stat for process state and parent PID
    let stat_path = format!("/proc/{}/stat", pid);
    let stat_content = std::fs::read_to_string(&stat_path).ok()?;

    // Parse stat file: pid (comm) state ppid ...
    // The command name is in parentheses and may contain spaces
    let stat_parts: Vec<&str> = stat_content.split_whitespace().collect();
    if stat_parts.len() < 4 {
        return None;
    }

    // Find the closing parenthesis to get the state and ppid correctly
    let comm_end = stat_content.rfind(')')?;
    let after_comm = &stat_content[comm_end + 2..]; // Skip ") "
    let remaining: Vec<&str> = after_comm.split_whitespace().collect();

    let state = remaining.first().map(|s| s.to_string()).unwrap_or_default();
    let ppid = remaining
        .get(1)
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(0);

    // Read /proc/[pid]/cmdline for full command
    let cmdline_path = format!("/proc/{}/cmdline", pid);
    let command = std::fs::read_to_string(&cmdline_path)
        .ok()
        .map(|s| s.replace('\0', " ").trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            // Fall back to comm from stat (process name in parentheses)
            let start = stat_content.find('(')?;
            let end = stat_content.rfind(')')?;
            Some(stat_content[start + 1..end].to_string())
        })
        .unwrap_or_else(|| "[unknown]".to_string());

    // Read /proc/[pid]/status for memory info
    let status_path = format!("/proc/{}/status", pid);
    let memory_kb = std::fs::read_to_string(&status_path)
        .ok()
        .and_then(|content| {
            for line in content.lines() {
                if line.starts_with("VmRSS:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        return parts[1].parse::<u64>().ok();
                    }
                }
            }
            None
        })
        .unwrap_or(0);

    // Read start time from /proc/[pid]/stat (field 22 is starttime in jiffies)
    let started_at = remaining.get(19).and_then(|s| {
        let start_ticks = s.parse::<u64>().ok()?;
        // Convert from jiffies to seconds (assuming 100 Hz)
        let boot_time = get_boot_time()?;
        let uptime_secs = start_ticks / 100;
        Some((boot_time + uptime_secs as i64) as i64)
    });

    Some(ProcessInfo {
        pid,
        ppid,
        command,
        state,
        memory_kb,
        started_at,
    })
}

/// Get system boot time from /proc/stat
fn get_boot_time() -> Option<i64> {
    let stat_content = std::fs::read_to_string("/proc/stat").ok()?;
    for line in stat_content.lines() {
        if line.starts_with("btime ") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                return parts[1].parse::<i64>().ok();
            }
        }
    }
    None
}

fn send_error(stream: &mut UnixStream, message: &str) {
    let response = ExecResponse {
        msg_type: "error".to_string(),
        data: Some(message.to_string()),
        code: None,
        elapsed_ms: None,
        pid: None,
    };
    let _ = send_json(stream, &response);
}

fn send_json<T: Serialize>(stream: &mut UnixStream, response: &T) -> Result<(), std::io::Error> {
    let json = serde_json::to_string(response)?;
    writeln!(stream, "{}", json)?;
    stream.flush()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // TEST 1: LineBuffer - O(n) buffering with size limits
    // ========================================================================

    #[test]
    fn test_linebuffer_basic_append() {
        let mut buffer = LineBuffer::new(1024);
        buffer.append(b"hello");
        assert_eq!(buffer.buffer, b"hello");
        assert!(!buffer.truncated);
    }

    #[test]
    fn test_linebuffer_extract_lines() {
        let mut buffer = LineBuffer::new(1024);
        buffer.append(b"line1\nline2\npartial");

        let lines = buffer.extract_lines();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0], b"line1");
        assert_eq!(lines[1], b"line2");
        assert_eq!(buffer.buffer, b"partial");
    }

    #[test]
    fn test_linebuffer_size_limit_enforced() {
        let mut buffer = LineBuffer::new(10);
        buffer.append(b"12345");
        assert_eq!(buffer.buffer.len(), 5);
        assert!(!buffer.truncated);

        // Adding 6 more bytes should hit limit (5 + 6 = 11 > 10)
        buffer.append(b"678901");
        assert_eq!(buffer.buffer.len(), 10);
        assert!(buffer.truncated, "Buffer should be marked as truncated");
    }

    #[test]
    fn test_linebuffer_truncation_flag() {
        let mut buffer = LineBuffer::new(10);
        buffer.append(b"12345678901234567890"); // Way over limit

        // Buffer should be truncated to max size
        assert_eq!(buffer.buffer.len(), 10);
        assert!(buffer.was_truncated(), "Should be marked as truncated");
    }

    #[test]
    fn test_linebuffer_multiple_appends() {
        let mut buffer = LineBuffer::new(1024);
        buffer.append(b"hello ");
        buffer.append(b"world\n");
        buffer.append(b"next");

        let lines = buffer.extract_lines();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0], b"hello world");
        assert_eq!(buffer.buffer, b"next");
    }

    // ========================================================================
    // TEST 2: Environment Variable Filtering
    // ========================================================================

    #[test]
    fn test_validate_env_vars_clean() {
        let mut env = HashMap::new();
        env.insert("PATH".to_string(), "/bin:/usr/bin".to_string());
        env.insert("HOME".to_string(), "/root".to_string());

        assert!(validate_env_vars(&env).is_ok());
    }

    #[test]
    fn test_validate_env_vars_blocks_ld_preload() {
        let mut env = HashMap::new();
        env.insert("LD_PRELOAD".to_string(), "/bad.so".to_string());

        let result = validate_env_vars(&env);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("LD_PRELOAD"));
    }

    #[test]
    fn test_validate_env_vars_blocks_ld_library_path() {
        let mut env = HashMap::new();
        env.insert("LD_LIBRARY_PATH".to_string(), "/bad".to_string());

        let result = validate_env_vars(&env);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("LD_LIBRARY_PATH"));
    }

    #[test]
    fn test_validate_env_vars_blocks_pythonpath() {
        let mut env = HashMap::new();
        env.insert("PYTHONPATH".to_string(), "/malicious".to_string());

        let result = validate_env_vars(&env);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("PYTHONPATH"));
    }

    #[test]
    fn test_validate_env_vars_case_insensitive() {
        let mut env = HashMap::new();
        env.insert("ld_preload".to_string(), "/bad.so".to_string());

        let result = validate_env_vars(&env);
        assert!(result.is_err(), "Should block lowercase LD_PRELOAD");
    }

    // ========================================================================
    // TEST 3: PATH Resolution
    // ========================================================================

    #[test]
    fn test_resolve_command_path_absolute() {
        let mut env = HashMap::new();
        env.insert("PATH".to_string(), "/bin:/usr/bin".to_string());

        // Absolute paths should be checked for existence
        let result = resolve_command_path("/bin/sh", &env);
        // Will fail if /bin/sh doesn't exist, but that's expected behavior
        if std::path::Path::new("/bin/sh").exists() {
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), "/bin/sh");
        }
    }

    #[test]
    fn test_resolve_command_path_nonexistent_absolute() {
        let env = HashMap::new();

        let result = resolve_command_path("/nonexistent/command", &env);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn test_resolve_command_path_searches_path() {
        let mut env = HashMap::new();
        env.insert("PATH".to_string(), "/bin:/usr/bin".to_string());

        // Should find 'sh' in PATH
        let result = resolve_command_path("sh", &env);
        if std::path::Path::new("/bin/sh").exists() || std::path::Path::new("/usr/bin/sh").exists()
        {
            assert!(result.is_ok());
            let resolved = result.unwrap();
            assert!(resolved == "/bin/sh" || resolved == "/usr/bin/sh");
        }
    }

    #[test]
    fn test_resolve_command_path_default_path() {
        let env = HashMap::new();

        // Should use default PATH when not specified
        let result = resolve_command_path("sh", &env);
        // Should search /bin:/usr/bin:/sbin:/usr/sbin by default
        if std::path::Path::new("/bin/sh").exists() {
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_resolve_command_path_not_in_path() {
        let mut env = HashMap::new();
        env.insert("PATH".to_string(), "/nonexistent".to_string());

        let result = resolve_command_path("nonexistent_cmd", &env);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found in PATH"));
    }

    // ========================================================================
    // TEST 4: Protocol Type Field Validation
    // ========================================================================

    #[test]
    fn test_base_request_parse_ping() {
        let json = r#"{"type":"ping"}"#;
        let result: Result<BaseRequest, _> = serde_json::from_str(json);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().request_type, "ping");
    }

    #[test]
    fn test_base_request_parse_exec() {
        let json = r#"{"type":"exec"}"#;
        let result: Result<BaseRequest, _> = serde_json::from_str(json);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().request_type, "exec");
    }

    #[test]
    fn test_base_request_missing_type() {
        let json = r#"{"cmd":"echo"}"#;
        let result: Result<BaseRequest, _> = serde_json::from_str(json);
        assert!(result.is_err(), "Should fail without type field");
    }

    #[test]
    fn test_exec_request_parse_valid() {
        let json = r#"{
            "type": "exec",
            "cmd": "echo",
            "args": ["hello"],
            "env": {"PATH": "/bin"},
            "timeout_ms": 5000
        }"#;

        let result: Result<ExecRequest, _> = serde_json::from_str(json);
        assert!(result.is_ok());

        let req = result.unwrap();
        assert_eq!(req.request_type, "exec");
        assert_eq!(req.cmd, "echo");
        assert_eq!(req.args, vec!["hello"]);
        assert_eq!(req.timeout_ms, 5000);
    }

    #[test]
    fn test_exec_request_with_stdin_data() {
        let json = r#"{
            "type": "exec",
            "cmd": "cat",
            "args": [],
            "env": {},
            "timeout_ms": 1000,
            "stdin_data": "test input"
        }"#;

        let result: Result<ExecRequest, _> = serde_json::from_str(json);
        assert!(result.is_ok());

        let req = result.unwrap();
        assert_eq!(req.stdin_data, Some("test input".to_string()));
    }

    #[test]
    fn test_health_request_parse() {
        let json = r#"{"type":"ping"}"#;
        let result: Result<HealthRequest, _> = serde_json::from_str(json);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().request_type, "ping");
    }

    #[test]
    fn test_kill_request_parse() {
        let json = r#"{"type":"kill","pid":123,"signal":"TERM"}"#;
        let result: Result<KillRequest, _> = serde_json::from_str(json);
        assert!(result.is_ok());

        let req = result.unwrap();
        assert_eq!(req.request_type, "kill");
        assert_eq!(req.pid, 123);
        assert_eq!(req.signal, "TERM");
    }

    // ========================================================================
    // TEST 5: Dangerous Env Vars Constant Validation
    // ========================================================================

    #[test]
    fn test_dangerous_env_vars_contains_critical_ones() {
        assert!(DANGEROUS_ENV_VARS.contains(&"LD_PRELOAD"));
        assert!(DANGEROUS_ENV_VARS.contains(&"LD_LIBRARY_PATH"));
        assert!(DANGEROUS_ENV_VARS.contains(&"LD_AUDIT"));
        assert!(DANGEROUS_ENV_VARS.contains(&"PYTHONPATH"));
        assert!(DANGEROUS_ENV_VARS.contains(&"PERL5LIB"));
        assert!(DANGEROUS_ENV_VARS.contains(&"RUBYLIB"));
        assert!(DANGEROUS_ENV_VARS.contains(&"NODE_PATH"));
    }

    // ========================================================================
    // TEST 6: Constants Validation
    // ========================================================================

    #[test]
    fn test_max_output_buffer_size_reasonable() {
        // 1MB is reasonable for output buffering
        assert_eq!(MAX_OUTPUT_BUFFER_SIZE, 1024 * 1024);
    }

    #[test]
    fn test_max_request_size_reasonable() {
        // 1MB is reasonable for request size
        assert_eq!(MAX_REQUEST_SIZE, 1024 * 1024);
    }
}

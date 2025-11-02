use nix::unistd::Pid;
use nix::sys::signal::{self, Signal};
use std::time::{SystemTime, UNIX_EPOCH};

pub struct ProcessUtils;

#[allow(dead_code)] // Shared utilities used by different binaries
impl ProcessUtils {
    /// Convert nix::Pid to i32 for display/logging
    pub fn pid_to_i32(pid: Pid) -> i32 {
        pid.as_raw()
    }

    /// Convert i32 to nix::Pid
    pub fn i32_to_pid(pid: i32) -> Pid {
        Pid::from_raw(pid)
    }

    /// Check if a process is still running
    pub fn is_process_running(pid: Pid) -> bool {
        match signal::kill(pid, None) {
            Ok(()) => true,  // Process exists
            Err(_) => false, // Process doesn't exist or we don't have permission
        }
    }

    /// Gracefully terminate a process with SIGTERM, then SIGKILL if needed
    pub fn terminate_process(pid: Pid, timeout_seconds: u64) -> Result<(), String> {
        use std::thread;
        use std::time::Duration;

        // Check if process is still running
        if !Self::is_process_running(pid) {
            return Ok(()); // Process is already dead
        }

        // Send SIGTERM first
        if let Err(e) = signal::kill(pid, Signal::SIGTERM) {
            return Err(format!("Failed to send SIGTERM to process {}: {}", Self::pid_to_i32(pid), e));
        }

        // Wait for graceful shutdown
        for _ in 0..timeout_seconds {
            thread::sleep(Duration::from_secs(1));
            if !Self::is_process_running(pid) {
                return Ok(()); // Process terminated gracefully
            }
        }

        // Process still running, use SIGKILL
        if Self::is_process_running(pid) {
            if let Err(e) = signal::kill(pid, Signal::SIGKILL) {
                return Err(format!("Failed to send SIGKILL to process {}: {}", Self::pid_to_i32(pid), e));
            }

            // Give it a moment to die
            thread::sleep(Duration::from_millis(100));
            
            if Self::is_process_running(pid) {
                return Err(format!("Process {} refused to die even after SIGKILL", Self::pid_to_i32(pid)));
            }
        }

        Ok(())
    }

    /// Send a signal to a process
    pub fn send_signal(pid: Pid, signal: Signal) -> Result<(), String> {
        signal::kill(pid, signal)
            .map_err(|e| format!("Failed to send signal {:?} to process {}: {}", signal, Self::pid_to_i32(pid), e))
    }

    /// Get current timestamp in seconds since Unix epoch
    pub fn get_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Format timestamp as human-readable string
    pub fn format_timestamp(timestamp: u64) -> String {
        use std::time::{Duration, UNIX_EPOCH};
        
        let datetime = UNIX_EPOCH + Duration::from_secs(timestamp);
        
        // Simple formatting - in production you'd want to use chrono or similar
        match datetime.elapsed() {
            Ok(elapsed) => {
                let secs = elapsed.as_secs();
                if secs < 60 {
                    format!("{}s ago", secs)
                } else if secs < 3600 {
                    format!("{}m ago", secs / 60)
                } else if secs < 86400 {
                    format!("{}h ago", secs / 3600)
                } else {
                    format!("{}d ago", secs / 86400)
                }
            }
            Err(_) => format!("timestamp: {}", timestamp),
        }
    }


}

 
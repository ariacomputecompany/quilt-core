use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum LogLevel {
    Info,
    Success,
    Warning,
    Error,
    Progress,
    Debug,
}

pub struct ConsoleLogger;

#[allow(dead_code)]
impl ConsoleLogger {
    /// Log a message with appropriate emoji and formatting
    pub fn log(level: LogLevel, message: &str) {
        let (emoji, _prefix) = match level {
            LogLevel::Info => ("ℹ️", "INFO"),
            LogLevel::Success => ("✅", "SUCCESS"),
            LogLevel::Warning => ("⚠️", "WARNING"),
            LogLevel::Error => ("❌", "ERROR"),
            LogLevel::Progress => ("🔄", "PROGRESS"),
            LogLevel::Debug => ("🔧", "DEBUG"),
        };

        println!("{} {}", emoji, message);
    }

    /// Log info message
    pub fn info(message: &str) {
        Self::log(LogLevel::Info, message);
    }

    /// Log success message
    pub fn success(message: &str) {
        Self::log(LogLevel::Success, message);
    }

    /// Log warning message
    pub fn warning(message: &str) {
        Self::log(LogLevel::Warning, message);
    }

    /// Log error message
    pub fn error(message: &str) {
        Self::log(LogLevel::Error, message);
    }

    /// Log progress message
    pub fn progress(message: &str) {
        Self::log(LogLevel::Progress, message);
    }

    /// Log debug message
    pub fn debug(message: &str) {
        Self::log(LogLevel::Debug, message);
    }

    /// Log container creation status
    pub fn container_created(container_id: &str) {
        Self::success(&format!("Container {} created successfully", container_id));
    }

    /// Log container start status
    pub fn container_started(container_id: &str, pid: Option<i32>) {
        if let Some(pid) = pid {
            Self::success(&format!(
                "Container {} started with PID: {}",
                container_id, pid
            ));
        } else {
            Self::success(&format!("Container {} started successfully", container_id));
        }
    }

    /// Log container stop status
    pub fn container_stopped(container_id: &str) {
        Self::success(&format!("Container {} stopped successfully", container_id));
    }

    /// Log container removal status
    pub fn container_removed(container_id: &str) {
        Self::success(&format!("Container {} removed successfully", container_id));
    }

    /// Log container failure
    pub fn container_failed(container_id: &str, error: &str) {
        Self::error(&format!("Container {} failed: {}", container_id, error));
    }

    /// Log package installation
    pub fn package_installing(packages: &[String], manager: &str) {
        Self::progress(&format!(
            "Installing {} packages: {}",
            manager,
            packages.join(", ")
        ));
    }

    /// Log package installation success
    pub fn package_installed(packages: &[String], manager: &str) {
        Self::success(&format!(
            "Successfully installed {} packages: {}",
            manager,
            packages.join(", ")
        ));
    }

    /// Log runtime installation
    pub fn runtime_installing(runtime_name: &str) {
        Self::progress(&format!("Installing {} runtime...", runtime_name));
    }

    /// Log runtime installation success
    pub fn runtime_installed(runtime_name: &str) {
        Self::success(&format!("Successfully installed {} runtime", runtime_name));
    }

    /// Log server startup information
    pub fn server_starting(addr: &str) {
        println!("🚀 Starting Quilt Container Runtime Server");
        println!("Features enabled:");
        println!("  ✅ Linux Namespaces (PID, Mount, UTS, IPC, Network)");
        println!("  ✅ Cgroup Resource Management (Memory, CPU, PIDs)");
        println!("  ✅ Dynamic Runtime Setup Commands (npm, pip, gem, etc.)");
        println!("  ✅ Container Isolation and Security");
        println!("  ✅ Network Namespace (basic loopback)");
        println!();
        println!("🌐 Quilt gRPC server listening on {}", addr);
        println!("📋 Ready to accept container creation requests...");
    }

    /// Log namespace creation
    pub fn namespace_created(config: &str) {
        Self::debug(&format!(
            "Creating namespaced process with flags: {}",
            config
        ));
    }

    /// Log cgroup creation
    pub fn cgroup_created(container_id: &str, cgroup_version: &str) {
        Self::debug(&format!(
            "Created {} cgroups for container: {}",
            cgroup_version, container_id
        ));
    }

    /// Log resource limits being set
    pub fn resource_limit_set(resource: &str, value: &str) {
        Self::debug(&format!("Set {} limit to {}", resource, value));
    }

    /// Log with timestamp
    pub fn log_with_timestamp(level: LogLevel, message: &str) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let datetime = std::time::UNIX_EPOCH + std::time::Duration::from_secs(timestamp);
        let formatted_time = humantime::format_rfc3339_seconds(datetime);

        let (emoji, _) = match level {
            LogLevel::Info => ("ℹ️", "INFO"),
            LogLevel::Success => ("✅", "SUCCESS"),
            LogLevel::Warning => ("⚠️", "WARNING"),
            LogLevel::Error => ("❌", "ERROR"),
            LogLevel::Progress => ("🔄", "PROGRESS"),
            LogLevel::Debug => ("🔧", "DEBUG"),
        };

        println!("[{}] {} {}", formatted_time, emoji, message);
    }

    /// Print a separator line
    pub fn separator() {
        println!("{}", "─".repeat(60));
    }

    /// Print section header
    pub fn section_header(title: &str) {
        println!();
        Self::separator();
        println!("📋 {}", title);
        Self::separator();
    }

    /// Format memory usage
    pub fn format_memory(bytes: u64) -> String {
        if bytes >= 1024 * 1024 * 1024 {
            format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
        } else if bytes >= 1024 * 1024 {
            format!("{:.2} MB", bytes as f64 / (1024.0 * 1024.0))
        } else if bytes >= 1024 {
            format!("{:.2} KB", bytes as f64 / 1024.0)
        } else {
            format!("{} bytes", bytes)
        }
    }

    /// Format container status output
    pub fn format_container_status(
        container_id: &str,
        status: &str,
        created_at: &str,
        rootfs_path: &str,
        pid: Option<i32>,
        exit_code: Option<i32>,
        error_message: &str,
        memory_usage: Option<u64>,
        ip_address: Option<&str>,
    ) {
        println!("📋 Container Status:");
        println!("   ID: {}", container_id);
        println!("   Status: {}", status);
        println!("   Created: {}", created_at);
        println!("   Rootfs: {}", rootfs_path);

        if let Some(pid) = pid {
            println!("   PID: {}", pid);
        }

        if let Some(ip) = ip_address {
            if !ip.is_empty() && ip != "No IP assigned" {
                println!("   IP: {}", ip);
            }
        }

        if let Some(exit_code) = exit_code {
            if exit_code != 0 || status == "EXITED" {
                println!("   Exit Code: {}", exit_code);
            }
        }

        if !error_message.is_empty() {
            println!("   Error: {}", error_message);
        }

        if let Some(memory_bytes) = memory_usage {
            if memory_bytes > 0 {
                println!(
                    "   Memory Usage: {} ({})",
                    Self::format_memory(memory_bytes),
                    memory_bytes
                );
            }
        }
    }
}

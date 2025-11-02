// Container creation command handler
// Production implementation with smart defaults

use crate::quilt::quilt_service_client::QuiltServiceClient;
use crate::quilt::{CreateContainerRequest, CreateContainerResponse, Mount, MountType};
use crate::quilt::GetContainerStatusRequest;
use crate::utils::logger::Logger;
use tonic::transport::Channel;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

/// Parse volume specification into Mount messages
/// Supports: /host/path:/container/path[:ro]
///          volume-name:/container/path
fn parse_volume_spec(spec: &str) -> Result<Mount, String> {
    let parts: Vec<&str> = spec.split(':').collect();

    if parts.len() < 2 {
        return Err(format!("Invalid volume spec '{}': expected format 'source:target[:ro]'", spec));
    }

    let source = parts[0].to_string();
    let target = parts[1].to_string();
    let readonly = parts.get(2).map_or(false, |&s| s == "ro");

    // Determine if this is a bind mount or named volume
    let mount_type = if source.starts_with('/') {
        MountType::Bind
    } else {
        MountType::Volume
    };

    Ok(Mount {
        source,
        target,
        r#type: mount_type as i32,
        readonly,
        options: HashMap::new(),
    })
}

/// Parse environment variable KEY=VALUE format
fn parse_env_vars(env_vars: &[String]) -> HashMap<String, String> {
    let mut env = HashMap::new();

    for var in env_vars {
        if let Some((key, value)) = var.split_once('=') {
            env.insert(key.to_string(), value.to_string());
        } else {
            Logger::warning(&format!("Ignoring invalid env var '{}' (expected KEY=VALUE)", var));
        }
    }

    env
}

/// Wait for container to reach Running state with progress indication
async fn wait_for_container_ready(
    client: &mut QuiltServiceClient<Channel>,
    container_id: &str,
    timeout_secs: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let timeout = Duration::from_secs(timeout_secs);
    let poll_interval = Duration::from_millis(250);

    Logger::info("⏳ Setting up container...");

    // Container status values: 0=PENDING, 1=RUNNING, 2=EXITED, 3=FAILED
    let mut last_state = 0; // PENDING

    loop {
        // Check timeout
        if start.elapsed() > timeout {
            Logger::error(&format!("Container startup timeout after {}s", timeout_secs));
            Logger::info(&format!("Check logs with: quilt logs {}", container_id));
            return Err(format!("Timeout waiting for container to be ready").into());
        }

        // Poll container status
        let request = tonic::Request::new(GetContainerStatusRequest {
            container_id: container_id.to_string(),
            container_name: String::new(),
        });

        match client.get_container_status(request).await {
            Ok(response) => {
                let res = response.into_inner();
                let current_state = res.status;

                // Show progress when state changes
                if current_state != last_state {
                    match current_state {
                        0 => {
                            // PENDING
                            Logger::info("📦 Container initializing...");
                        }
                        1 => {
                            // RUNNING
                            Logger::success("✅ Container ready!");
                            Logger::detail("container_id", container_id);
                            Logger::detail("status", "RUNNING");
                            if !res.ip_address.is_empty() {
                                Logger::detail("ip_address", &res.ip_address);
                            }
                            return Ok(());
                        }
                        3 => {
                            // FAILED
                            Logger::error("❌ Container failed to start");
                            Logger::detail("container_id", container_id);
                            Logger::blank();
                            Logger::info(&format!("Check logs with: quilt logs {}", container_id));
                            return Err("Container failed to start".into());
                        }
                        2 => {
                            // EXITED - container exited during startup
                            Logger::error("❌ Container exited during startup");
                            Logger::detail("container_id", container_id);
                            Logger::detail("exit_code", &res.exit_code.to_string());
                            Logger::blank();
                            Logger::info(&format!("Check logs with: quilt logs {}", container_id));
                            return Err("Container exited during startup".into());
                        }
                        _ => {}
                    }
                    last_state = current_state;
                }

                // Still pending/starting - continue waiting
                tokio::time::sleep(poll_interval).await;
            }
            Err(e) => {
                Logger::error(&format!("Failed to check container status: {}", e));
                return Err(e.into());
            }
        }
    }
}

/// Create a new container
pub async fn handle_create(
    client: &mut QuiltServiceClient<Channel>,
    name: String,
    image_path: Option<String>,
    env: Vec<String>,
    memory: i32,
    cpu: f32,
    volume: Vec<String>,
    command: Vec<String>,
    no_wait: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    // Default to system default image if not specified
    let image_path = image_path.unwrap_or_else(||
        "/var/lib/quilt/images/default.tar.gz".to_string()
    );

    // Validate image path exists
    let p = PathBuf::from(&image_path);
    if !p.exists() {
        return Err(format!("Image file not found: {}", image_path).into());
    }
    if !image_path.ends_with(".tar.gz") && !image_path.ends_with(".tar") {
        Logger::warning("Image path should typically be a .tar.gz or .tar file");
    }

    // Parse volumes
    let mut mounts = Vec::new();
    for vol_spec in volume {
        match parse_volume_spec(&vol_spec) {
            Ok(mount) => mounts.push(mount),
            Err(e) => {
                Logger::error(&e);
                return Err(e.into());
            }
        }
    }

    // Parse environment variables
    let environment = parse_env_vars(&env);

    // Apply smart defaults
    let memory_limit_mb = if memory > 0 { memory } else { 512 };
    let cpu_limit_percent = if cpu > 0.0 { cpu } else { 50.0 };

    // Display what we're creating
    Logger::info(&format!("Creating container: {}", name));
    Logger::detail("image", &image_path);
    if !command.is_empty() {
        Logger::detail("command", &command.join(" "));
    }
    Logger::detail("memory", &format!("{} MB", memory_limit_mb));
    Logger::detail("cpu", &format!("{}%", cpu_limit_percent));

    // Build the request
    let request = tonic::Request::new(CreateContainerRequest {
        image_path: image_path.clone(),
        command: command.clone(),
        environment,
        working_directory: String::new(), // Use image default
        setup_commands: vec![],
        memory_limit_mb,
        cpu_limit_percent,
        // Enable all namespaces by default for production isolation
        enable_pid_namespace: true,
        enable_mount_namespace: true,
        enable_uts_namespace: true,
        enable_ipc_namespace: true,
        enable_network_namespace: true,
        name,
        async_mode: true, // Default to async mode for long-running containers
        mounts,
    });

    // Create the container
    match client.create_container(request).await {
        Ok(response) => {
            let res: CreateContainerResponse = response.into_inner();
            if res.success {
                Logger::success("Container created");

                if no_wait {
                    // Immediate return - original behavior
                    Logger::detail("container_id", &res.container_id);
                    Logger::blank();
                    Logger::info("Use 'quilt status <container-id>' to check status");
                    Logger::info("Use 'quilt logs <container-id>' to view logs");
                    Ok(res.container_id)
                } else {
                    // Wait for container to be ready
                    match wait_for_container_ready(client, &res.container_id, 30).await {
                        Ok(()) => {
                            Logger::blank();
                            Logger::info(&format!("Use 'quilt logs {}' to view logs", res.container_id));
                            Logger::info(&format!("Use 'quilt shell {}' for interactive access", res.container_id));
                            Ok(res.container_id)
                        }
                        Err(e) => {
                            // Container created but failed to start properly
                            Err(e)
                        }
                    }
                }
            } else {
                Logger::error(&format!("Failed to create container: {}", res.error_message));
                Err(res.error_message.into())
            }
        }
        Err(e) => {
            Logger::error(&format!("Failed to create container: {}", e));
            Err(e.into())
        }
    }
}

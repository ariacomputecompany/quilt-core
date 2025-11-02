// Container lifecycle command handlers
// Status, Start, Stop, Kill, Remove operations

use crate::quilt::quilt_service_client::QuiltServiceClient;
use crate::quilt::{
    GetContainerStatusRequest, GetContainerStatusResponse,
    StartContainerRequest, StartContainerResponse,
    StopContainerRequest, StopContainerResponse,
    KillContainerRequest, KillContainerResponse,
    RemoveContainerRequest, RemoveContainerResponse,
};
use crate::utils::logger::Logger;
use super::common::{resolve_container_id, format_timestamp, format_bytes};
use tonic::transport::Channel;
use std::time::Duration;

/// Get container status and display detailed information
pub async fn handle_status(
    client: &mut QuiltServiceClient<Channel>,
    container: Option<String>,
    by_name: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let container_id = resolve_container_id(client, &container, by_name).await?;

    Logger::info(&format!("Getting status for container: {}", container_id));

    let mut request = tonic::Request::new(GetContainerStatusRequest {
        container_id: container_id.clone(),
        container_name: String::new(),
    });
    request.set_timeout(Duration::from_secs(30));

    match client.get_container_status(request).await {
        Ok(response) => {
            let res: GetContainerStatusResponse = response.into_inner();
            display_container_status(&res);
            Ok(())
        }
        Err(e) => {
            Logger::error(&format!("Failed to get container status: {}", e));
            Err(e.into())
        }
    }
}

/// Display formatted container status information
fn display_container_status(res: &GetContainerStatusResponse) {
    let status_str = match res.status {
        0 => "PENDING",
        1 => "RUNNING",
        2 => "EXITED",
        3 => "FAILED",
        _ => "UNKNOWN",
    };

    // Main status header
    Logger::success(&format!("Container: {}", res.container_id));
    Logger::detail("status", status_str);

    // Basic info
    if res.pid > 0 {
        Logger::detail("pid", &res.pid.to_string());
    }

    if res.exit_code != 0 || res.status == 2 {
        Logger::detail("exit_code", &res.exit_code.to_string());
    }

    // Network info
    if !res.ip_address.is_empty() {
        Logger::detail("ip_address", &res.ip_address);
    }

    // Resource usage
    if res.memory_usage_bytes > 0 {
        Logger::detail("memory", &format_bytes(res.memory_usage_bytes));
    }

    // Timestamps
    if res.created_at > 0 {
        Logger::detail("created", &format_timestamp(res.created_at));
    }

    if res.started_at > 0 {
        Logger::detail("started", &format_timestamp(res.started_at));
    }

    if res.exited_at > 0 {
        Logger::detail("exited", &format_timestamp(res.exited_at));
    }

    // Rootfs path
    if !res.rootfs_path.is_empty() {
        Logger::detail("rootfs", &res.rootfs_path);
    }

    // Error message if any
    if !res.error_message.is_empty() {
        Logger::blank();
        Logger::error(&format!("Error: {}", res.error_message));
    }
}

/// Start a stopped container
pub async fn handle_start(
    client: &mut QuiltServiceClient<Channel>,
    container: Option<String>,
    by_name: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let container_id = resolve_container_id(client, &container, by_name).await?;

    Logger::info(&format!("Starting container: {}", container_id));

    let request = tonic::Request::new(StartContainerRequest {
        container_id: container_id.clone(),
        container_name: String::new(),
    });

    match client.start_container(request).await {
        Ok(response) => {
            let res: StartContainerResponse = response.into_inner();
            if res.success {
                Logger::success("Container started successfully");
                Logger::detail("container_id", &container_id);
                Ok(())
            } else {
                Logger::error(&format!("Failed to start container: {}", res.error_message));
                Err(res.error_message.into())
            }
        }
        Err(e) => {
            Logger::error(&format!("Failed to start container: {}", e));
            Err(e.into())
        }
    }
}

/// Stop a running container gracefully
pub async fn handle_stop(
    client: &mut QuiltServiceClient<Channel>,
    container: Option<String>,
    by_name: bool,
    timeout: u32,
) -> Result<(), Box<dyn std::error::Error>> {
    let container_id = resolve_container_id(client, &container, by_name).await?;

    Logger::info(&format!("Stopping container: {} (timeout: {}s)", container_id, timeout));

    let request = tonic::Request::new(StopContainerRequest {
        container_id: container_id.clone(),
        timeout_seconds: timeout as i32,
        container_name: String::new(),
    });

    match client.stop_container(request).await {
        Ok(response) => {
            let res: StopContainerResponse = response.into_inner();
            if res.success {
                Logger::success("Container stopped successfully");
                Logger::detail("container_id", &container_id);
                Ok(())
            } else {
                Logger::error(&format!("Failed to stop container: {}", res.error_message));
                Err(res.error_message.into())
            }
        }
        Err(e) => {
            Logger::error(&format!("Failed to stop container: {}", e));
            Err(e.into())
        }
    }
}

/// Kill a container immediately
pub async fn handle_kill(
    client: &mut QuiltServiceClient<Channel>,
    container: Option<String>,
    by_name: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let container_id = resolve_container_id(client, &container, by_name).await?;

    Logger::warning(&format!("Killing container: {}", container_id));

    let request = tonic::Request::new(KillContainerRequest {
        container_id: container_id.clone(),
        container_name: String::new(),
    });

    match client.kill_container(request).await {
        Ok(response) => {
            let res: KillContainerResponse = response.into_inner();
            if res.success {
                Logger::success("Container killed successfully");
                Logger::detail("container_id", &container_id);
                Ok(())
            } else {
                Logger::error(&format!("Failed to kill container: {}", res.error_message));
                Err(res.error_message.into())
            }
        }
        Err(e) => {
            Logger::error(&format!("Failed to kill container: {}", e));
            Err(e.into())
        }
    }
}

/// Remove a container
pub async fn handle_remove(
    client: &mut QuiltServiceClient<Channel>,
    container: Option<String>,
    by_name: bool,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let container_id = resolve_container_id(client, &container, by_name).await?;

    if force {
        Logger::warning(&format!("Force removing container: {}", container_id));
    } else {
        Logger::info(&format!("Removing container: {}", container_id));
    }

    let request = tonic::Request::new(RemoveContainerRequest {
        container_id: container_id.clone(),
        force,
        container_name: String::new(),
    });

    match client.remove_container(request).await {
        Ok(response) => {
            let res: RemoveContainerResponse = response.into_inner();
            if res.success {
                Logger::success("Container removed successfully");
                Logger::detail("container_id", &container_id);
                Ok(())
            } else {
                Logger::error(&format!("Failed to remove container: {}", res.error_message));
                Err(res.error_message.into())
            }
        }
        Err(e) => {
            Logger::error(&format!("Failed to remove container: {}", e));
            Err(e.into())
        }
    }
}

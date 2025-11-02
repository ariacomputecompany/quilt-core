// Volume management command handlers
// Production implementation for create, list, inspect, and remove operations

use crate::quilt::quilt_service_client::QuiltServiceClient;
use crate::quilt::{
    CreateVolumeRequest, CreateVolumeResponse,
    RemoveVolumeRequest, RemoveVolumeResponse,
    ListVolumesRequest, ListVolumesResponse,
    InspectVolumeRequest, InspectVolumeResponse,
};
use crate::utils::logger::Logger;
use super::common::format_timestamp;
use tonic::transport::Channel;
use std::collections::HashMap;

/// Create a new volume
pub async fn handle_volume_create(
    client: &mut QuiltServiceClient<Channel>,
    name: String,
) -> Result<(), Box<dyn std::error::Error>> {
    Logger::info(&format!("Creating volume: {}", name));

    let request = tonic::Request::new(CreateVolumeRequest {
        name: name.clone(),
        driver: "local".to_string(),
        labels: HashMap::new(),
        options: HashMap::new(),
    });

    match client.create_volume(request).await {
        Ok(response) => {
            let res: CreateVolumeResponse = response.into_inner();
            if res.success {
                Logger::success("Volume created successfully");
                if let Some(vol) = res.volume {
                    Logger::detail("name", &vol.name);
                    Logger::detail("driver", &vol.driver);
                    Logger::detail("mount_point", &vol.mount_point);
                }
                Ok(())
            } else {
                Logger::error(&format!("Failed to create volume: {}", res.error_message));
                Err(res.error_message.into())
            }
        }
        Err(e) => {
            Logger::error(&format!("Failed to create volume: {}", e));
            Err(e.into())
        }
    }
}

/// List all volumes
pub async fn handle_volume_list(
    client: &mut QuiltServiceClient<Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    Logger::info("Listing volumes...");

    let request = tonic::Request::new(ListVolumesRequest {
        filters: HashMap::new(),
    });

    match client.list_volumes(request).await {
        Ok(response) => {
            let res: ListVolumesResponse = response.into_inner();

            if res.volumes.is_empty() {
                Logger::info("No volumes found");
            } else {
                Logger::blank();
                Logger::success(&format!("Found {} volume(s)", res.volumes.len()));
                Logger::blank();

                for vol in res.volumes {
                    println!("Name: {}", vol.name);
                    println!("  Driver: {}", vol.driver);
                    println!("  Mount Point: {}", vol.mount_point);
                    if vol.created_at > 0 {
                        println!("  Created: {}", format_timestamp(vol.created_at));
                    }
                    if !vol.labels.is_empty() {
                        println!("  Labels:");
                        for (k, v) in vol.labels {
                            println!("    {}: {}", k, v);
                        }
                    }
                    println!();
                }
            }
            Ok(())
        }
        Err(e) => {
            Logger::error(&format!("Failed to list volumes: {}", e));
            Err(e.into())
        }
    }
}

/// Inspect a volume
pub async fn handle_volume_inspect(
    client: &mut QuiltServiceClient<Channel>,
    name: String,
) -> Result<(), Box<dyn std::error::Error>> {
    Logger::info(&format!("Inspecting volume: {}", name));

    let request = tonic::Request::new(InspectVolumeRequest {
        name: name.clone(),
    });

    match client.inspect_volume(request).await {
        Ok(response) => {
            let res: InspectVolumeResponse = response.into_inner();

            if !res.found {
                Logger::error(&format!("Volume '{}' not found", name));
                return Err(format!("Volume '{}' not found", name).into());
            }

            if let Some(vol) = res.volume {
                Logger::blank();
                Logger::success(&format!("Volume: {}", vol.name));
                Logger::detail("driver", &vol.driver);
                Logger::detail("mount_point", &vol.mount_point);

                if vol.created_at > 0 {
                    Logger::detail("created", &format_timestamp(vol.created_at));
                }

                if !vol.labels.is_empty() {
                    Logger::blank();
                    Logger::info("Labels:");
                    for (k, v) in vol.labels {
                        Logger::detail(&k, &v);
                    }
                }

                if !vol.options.is_empty() {
                    Logger::blank();
                    Logger::info("Options:");
                    for (k, v) in vol.options {
                        Logger::detail(&k, &v);
                    }
                }
            }

            Ok(())
        }
        Err(e) => {
            Logger::error(&format!("Failed to inspect volume: {}", e));
            Err(e.into())
        }
    }
}

/// Remove a volume
pub async fn handle_volume_remove(
    client: &mut QuiltServiceClient<Channel>,
    name: String,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if force {
        Logger::warning(&format!("Force removing volume: {}", name));
    } else {
        Logger::info(&format!("Removing volume: {}", name));
    }

    let request = tonic::Request::new(RemoveVolumeRequest {
        name: name.clone(),
        force,
    });

    match client.remove_volume(request).await {
        Ok(response) => {
            let res: RemoveVolumeResponse = response.into_inner();
            if res.success {
                Logger::success("Volume removed successfully");
                Logger::detail("name", &name);
                Ok(())
            } else {
                Logger::error(&format!("Failed to remove volume: {}", res.error_message));
                Err(res.error_message.into())
            }
        }
        Err(e) => {
            Logger::error(&format!("Failed to remove volume: {}", e));
            Err(e.into())
        }
    }
}

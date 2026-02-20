use crate::daemon::{CgroupLimits, ContainerConfig, NamespaceConfig};
use crate::icc;
use crate::sync::{ContainerState, MountType, SyncEngine};
use crate::utils::console::ConsoleLogger;
use crate::utils::filesystem::FileSystemUtils;

use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;

/// Background container process startup
/// This function handles the actual container creation and startup process
pub async fn start_container_process(
    sync_engine: &SyncEngine,
    container_id: &str,
    network_manager: Arc<icc::network::NetworkManager>,
) -> Result<(), String> {
    use crate::daemon::runtime::ContainerRuntime;

    let start_time = std::time::Instant::now();
    ConsoleLogger::info(&format!(
        "🚀 [STARTUP] Starting container process for {} at {:?}",
        container_id,
        std::time::SystemTime::now()
    ));

    // Store startup log
    let _ = sync_engine
        .store_container_log(container_id, "info", "Container startup initiated")
        .await;

    // Step 1: Configuration retrieval
    let config_start = std::time::Instant::now();
    ConsoleLogger::debug(&format!(
        "📋 [STARTUP-CONFIG] Retrieving configuration for {}",
        container_id
    ));

    // Get container configuration from sync engine
    let _status = sync_engine
        .get_container_status(container_id)
        .await
        .map_err(|e| {
            ConsoleLogger::error(&format!(
                "❌ [STARTUP-CONFIG] Failed to get container status for {}: {}",
                container_id, e
            ));
            format!("Failed to get container config: {}", e)
        })?;

    // Get full container config from database to get image_path and command
    ConsoleLogger::debug(&format!(
        "🔍 [STARTUP-CONFIG] Querying database for container details {}",
        container_id
    ));
    let container_record =
        sqlx::query("SELECT image_path, command, rootfs_path FROM containers WHERE id = ?")
            .bind(container_id)
            .fetch_one(sync_engine.pool())
            .await
            .map_err(|e| {
                ConsoleLogger::error(&format!(
                    "❌ [STARTUP-CONFIG] Database query failed for {}: {}",
                    container_id, e
                ));
                format!("Failed to get container details: {}", e)
            })?;

    let image_path: String = container_record.get("image_path");
    let command: String = container_record.get("command");
    let rootfs_path: Option<String> = container_record.get("rootfs_path");

    ConsoleLogger::debug(&format!(
        "📄 [STARTUP-CONFIG] Container {} details: image={}, command={}, rootfs={:?}",
        container_id, image_path, command, rootfs_path
    ));

    // Get mounts for the container
    ConsoleLogger::debug(&format!(
        "💾 [STARTUP-CONFIG] Retrieving mounts for {}",
        container_id
    ));
    let sync_mounts = sync_engine
        .get_container_mounts(container_id)
        .await
        .map_err(|e| {
            ConsoleLogger::error(&format!(
                "❌ [STARTUP-CONFIG] Failed to get mounts for {}: {}",
                container_id, e
            ));
            format!("Failed to get mounts: {}", e)
        })?;

    ConsoleLogger::debug(&format!(
        "⏱️ [STARTUP-CONFIG] Configuration retrieval completed for {} in {:?}",
        container_id,
        config_start.elapsed()
    ));

    // Step 2: Mount preparation
    let mount_start = std::time::Instant::now();
    ConsoleLogger::debug(&format!(
        "💾 [STARTUP-MOUNTS] Converting {} mounts for container {}",
        sync_mounts.len(),
        container_id
    ));

    // Convert mounts from sync engine to daemon format
    let mut daemon_mounts: Vec<crate::daemon::MountConfig> = Vec::new();
    for (i, m) in sync_mounts.iter().enumerate() {
        ConsoleLogger::debug(&format!(
            "📁 [STARTUP-MOUNTS] Processing mount {}/{}: {} -> {} (type: {:?}, readonly: {})",
            i + 1,
            sync_mounts.len(),
            m.source,
            m.target,
            m.mount_type,
            m.readonly
        ));

        let source = match m.mount_type {
            MountType::Volume => {
                // For volumes, convert volume name to actual path
                let volume_path = sync_engine
                    .get_volume_path(&m.source)
                    .to_string_lossy()
                    .to_string();
                ConsoleLogger::debug(&format!(
                    "📦 [STARTUP-MOUNTS] Volume {} resolved to path: {}",
                    m.source, volume_path
                ));
                volume_path
            }
            _ => m.source.clone(),
        };

        daemon_mounts.push(crate::daemon::MountConfig {
            source,
            target: m.target.clone(),
            mount_type: match m.mount_type {
                MountType::Bind => crate::daemon::MountType::Bind,
                MountType::Volume => crate::daemon::MountType::Volume,
                MountType::Tmpfs => crate::daemon::MountType::Tmpfs,
            },
            readonly: m.readonly,
            options: m.options.clone(),
        });
    }

    ConsoleLogger::debug(&format!(
        "⏱️ [STARTUP-MOUNTS] Mount conversion completed for {} in {:?}",
        container_id,
        mount_start.elapsed()
    ));

    // Step 2.5: Mount Security Validation
    let validation_start = std::time::Instant::now();
    ConsoleLogger::debug(&format!(
        "🔒 [STARTUP-SECURITY] Validating mount security for {} ({} mounts)",
        container_id,
        daemon_mounts.len()
    ));

    // Get resource manager and validate mount security
    use crate::daemon::resource::ResourceManager;
    let resource_manager = ResourceManager::new();

    // Validate mount security before proceeding
    if let Err(e) = resource_manager.validate_mount_security(container_id, &daemon_mounts) {
        ConsoleLogger::error(&format!(
            "❌ [STARTUP-SECURITY] Mount security validation failed for {}: {}",
            container_id, e
        ));
        let _ = sync_engine
            .store_container_log(
                container_id,
                "error",
                &format!("Mount security validation failed: {}", e),
            )
            .await;
        return Err(format!("Mount security validation failed: {}", e));
    }

    // Register mount configurations for tracking
    resource_manager.register_mount_configs(container_id, daemon_mounts.clone());

    ConsoleLogger::success(&format!(
        "✅ [STARTUP-SECURITY] Mount security validated for {} in {:?}",
        container_id,
        validation_start.elapsed()
    ));

    // Step 3: Legacy config conversion
    let legacy_start = std::time::Instant::now();
    ConsoleLogger::debug(&format!(
        "🔄 [STARTUP-LEGACY] Converting to legacy format for {}",
        container_id
    ));

    // Convert sync engine config back to legacy format for actual container startup
    // TODO: Eventually replace this with native sync engine container startup
    // Parse the command string back into a command vector to avoid double-wrapping
    let command_vec = if command.starts_with("/bin/sh -c ") {
        // Command is already shell-wrapped, parse it properly
        vec![
            "/bin/sh".to_string(),
            "-c".to_string(),
            command.strip_prefix("/bin/sh -c ").unwrap().to_string(),
        ]
    } else {
        // Command is not shell-wrapped, wrap it
        vec!["/bin/sh".to_string(), "-c".to_string(), command.clone()]
    };

    let legacy_config = ContainerConfig {
        image_path: image_path.clone(),
        command: command_vec.clone(),
        environment: HashMap::new(), // TODO: Get from sync engine
        setup_commands: vec![],
        resource_limits: Some(CgroupLimits::default()),
        namespace_config: Some(NamespaceConfig::default()),
        working_directory: None,
        mounts: daemon_mounts,
    };

    ConsoleLogger::debug(&format!(
        "📝 [STARTUP-LEGACY] Legacy config created for {}: image={}, command={:?}",
        container_id, image_path, command_vec
    ));

    // Create legacy runtime for actual process management (temporary)
    ConsoleLogger::debug(&format!(
        "🏗️ [STARTUP-RUNTIME] Creating container runtime for {}",
        container_id
    ));
    let runtime = ContainerRuntime::new();

    ConsoleLogger::debug(&format!(
        "⏱️ [STARTUP-LEGACY] Legacy conversion completed for {} in {:?}",
        container_id,
        legacy_start.elapsed()
    ));

    // Step 4: State transition to Starting
    let state_start = std::time::Instant::now();
    ConsoleLogger::info(&format!(
        "🔄 [STARTUP-STATE] Transitioning container {} to Starting state",
        container_id
    ));

    // Update state to Starting
    sync_engine
        .update_container_state(container_id, ContainerState::Starting)
        .await
        .map_err(|e| {
            ConsoleLogger::error(&format!(
                "❌ [STARTUP-STATE] Failed to update state to Starting for {}: {}",
                container_id, e
            ));
            format!("Failed to update state: {}", e)
        })?;

    ConsoleLogger::debug(&format!(
        "✅ [STARTUP-STATE] State transition to Starting completed for {} in {:?}",
        container_id,
        state_start.elapsed()
    ));

    // Step 5: Container creation/restart logic
    let creation_start = std::time::Instant::now();

    // Check if this is a restart (container already has rootfs)
    let needs_creation = if let Some(ref rootfs) = rootfs_path {
        let exists = FileSystemUtils::exists(rootfs);
        ConsoleLogger::debug(&format!(
            "🔍 [STARTUP-CREATE] Checking rootfs path {} for {}: exists={}",
            rootfs, container_id, exists
        ));
        !exists
    } else {
        ConsoleLogger::debug(&format!(
            "🔍 [STARTUP-CREATE] No existing rootfs path for {}, will create new",
            container_id
        ));
        true
    };

    if needs_creation {
        // First time starting - create container in legacy runtime
        ConsoleLogger::info(&format!(
            "🏗️ [STARTUP-CREATE] Creating NEW container runtime for {} (first time start)",
            container_id
        ));
        runtime
            .create_container(container_id.to_string(), legacy_config)
            .map_err(|e| {
                ConsoleLogger::error(&format!(
                    "❌ [STARTUP-CREATE] Failed to create legacy container {}: {}",
                    container_id, e
                ));
                format!("Failed to create legacy container: {}", e)
            })?;

        ConsoleLogger::debug(&format!(
            "✅ [STARTUP-CREATE] Container runtime created successfully for {}",
            container_id
        ));

        // Save the rootfs path back to sync engine
        if let Some(container) = runtime.get_container_info(container_id) {
            ConsoleLogger::debug(&format!(
                "💾 [STARTUP-CREATE] Saving rootfs path {} for {}",
                container.rootfs_path, container_id
            ));
            sync_engine
                .set_rootfs_path(container_id, &container.rootfs_path)
                .await
                .map_err(|e| {
                    ConsoleLogger::error(&format!(
                        "❌ [STARTUP-CREATE] Failed to save rootfs path for {}: {}",
                        container_id, e
                    ));
                    format!("Failed to save rootfs path: {}", e)
                })?;
        } else {
            ConsoleLogger::warning(&format!(
                "⚠️ [STARTUP-CREATE] Container {} created but info not available",
                container_id
            ));
        }
    } else {
        // Restarting existing container - just add to runtime registry without recreating rootfs
        ConsoleLogger::info(&format!(
            "🔄 [STARTUP-CREATE] RESTARTING existing container {} with rootfs {}",
            container_id,
            rootfs_path.as_ref().unwrap()
        ));

        // Add container to runtime's registry without creating rootfs
        // We'll implement a new method for this
        runtime
            .register_existing_container(
                container_id.to_string(),
                legacy_config,
                rootfs_path.unwrap(),
            )
            .map_err(|e| {
                ConsoleLogger::error(&format!(
                    "❌ [STARTUP-CREATE] Failed to register existing container {}: {}",
                    container_id, e
                ));
                format!("Failed to register existing container: {}", e)
            })?;

        ConsoleLogger::debug(&format!(
            "✅ [STARTUP-CREATE] Existing container {} registered successfully",
            container_id
        ));
    }

    ConsoleLogger::debug(&format!(
        "⏱️ [STARTUP-CREATE] Container creation/restart phase completed for {} in {:?}",
        container_id,
        creation_start.elapsed()
    ));

    // Step 6: Rootfs validation
    let rootfs_start = std::time::Instant::now();
    ConsoleLogger::debug(&format!(
        "🔍 [STARTUP-ROOTFS] Retrieving rootfs path for {}",
        container_id
    ));

    // Get the actual rootfs path from the runtime
    let actual_rootfs_path = if let Some(container) = runtime.get_container_info(container_id) {
        ConsoleLogger::debug(&format!(
            "📁 [STARTUP-ROOTFS] Found rootfs path for {}: {}",
            container_id, container.rootfs_path
        ));
        container.rootfs_path.clone()
    } else {
        ConsoleLogger::error(&format!(
            "❌ [STARTUP-ROOTFS] Failed to get container info for {}",
            container_id
        ));
        return Err("Failed to get container rootfs path".to_string());
    };

    ConsoleLogger::debug(&format!(
        "⏱️ [STARTUP-ROOTFS] Rootfs validation completed for {} in {:?}",
        container_id,
        rootfs_start.elapsed()
    ));

    // Step 7: Network setup preparation
    let network_prep_start = std::time::Instant::now();
    ConsoleLogger::debug(&format!(
        "🌐 [STARTUP-NETWORK] Checking network requirements for {}",
        container_id
    ));

    // Check if network setup is needed BEFORE starting container
    let needs_network_setup = sync_engine
        .should_setup_network(container_id)
        .await
        .unwrap_or(false);

    // Network ready signal should be written to container's filesystem
    let network_ready_path = format!(
        "{}/tmp/quilt-network-ready-{}",
        actual_rootfs_path, container_id
    );

    ConsoleLogger::info(&format!(
        "🌐 [STARTUP-NETWORK] Container {} network setup required: {}",
        container_id, needs_network_setup
    ));
    ConsoleLogger::debug(&format!(
        "📍 [STARTUP-NETWORK] Network ready signal path: {}",
        network_ready_path
    ));

    // Ensure /tmp exists in container rootfs
    let container_tmp_dir = format!("{}/tmp", actual_rootfs_path);
    ConsoleLogger::debug(&format!(
        "📁 [STARTUP-NETWORK] Ensuring /tmp directory exists: {}",
        container_tmp_dir
    ));
    if !FileSystemUtils::exists(&container_tmp_dir) {
        ConsoleLogger::debug(&format!(
            "📁 [STARTUP-NETWORK] Creating /tmp directory for {}",
            container_id
        ));
        FileSystemUtils::create_dir_all_with_logging(
            &container_tmp_dir,
            "container /tmp directory",
        )
        .map_err(|e| {
            ConsoleLogger::error(&format!(
                "❌ [STARTUP-NETWORK] Failed to create /tmp directory for {}: {}",
                container_id, e
            ));
            format!("Failed to create /tmp in container rootfs: {}", e)
        })?;
    } else {
        ConsoleLogger::debug(&format!(
            "✅ [STARTUP-NETWORK] /tmp directory already exists for {}",
            container_id
        ));
    }

    if !needs_network_setup {
        // No network setup needed, create signal file immediately so container doesn't wait
        ConsoleLogger::debug(&format!("📝 [STARTUP-NETWORK] Creating immediate network ready signal for {} (no network needed)", container_id));
        FileSystemUtils::write_file(&network_ready_path, "ready").map_err(|e| {
            ConsoleLogger::error(&format!(
                "❌ [STARTUP-NETWORK] Failed to create network ready signal for {}: {}",
                container_id, e
            ));
            format!("Failed to create network ready signal: {}", e)
        })?;
        ConsoleLogger::debug(&format!(
            "✅ [STARTUP-NETWORK] No network setup needed, created signal at {}",
            network_ready_path
        ));
    }

    ConsoleLogger::debug(&format!(
        "⏱️ [STARTUP-NETWORK] Network preparation completed for {} in {:?}",
        container_id,
        network_prep_start.elapsed()
    ));

    // Step 8: Start the container process
    let start_process_time = std::time::Instant::now();
    ConsoleLogger::info(&format!(
        "🚀 [STARTUP-START] Starting container process for {}",
        container_id
    ));

    // Start the container
    match runtime.start_container(container_id, None) {
        Ok(()) => {
            ConsoleLogger::success(&format!(
                "✅ [STARTUP-START] Container process started successfully for {} in {:?}",
                container_id,
                start_process_time.elapsed()
            ));

            // Store success log
            let _ = sync_engine
                .store_container_log(
                    container_id,
                    "info",
                    "Container process started successfully",
                )
                .await;

            // Step 9: PID handling and monitoring setup
            let pid_start = std::time::Instant::now();
            ConsoleLogger::debug(&format!(
                "🔍 [STARTUP-PID] Retrieving PID for {}",
                container_id
            ));

            // Get the PID from legacy runtime and store in sync engine
            if let Some(container) = runtime.get_container_info(container_id) {
                if let Some(pid) = container.pid {
                    ConsoleLogger::info(&format!(
                        "🆔 [STARTUP-PID] Container {} got PID: {}",
                        container_id,
                        pid.as_raw()
                    ));

                    // Emit process started event

                    sync_engine
                        .set_container_pid(container_id, pid)
                        .await
                        .map_err(|e| {
                            ConsoleLogger::error(&format!(
                                "❌ [STARTUP-PID] Failed to set PID for {}: {}",
                                container_id, e
                            ));
                            format!("Failed to set PID: {}", e)
                        })?;

                    ConsoleLogger::debug(&format!(
                        "⏱️ [STARTUP-PID] PID handling completed for {} in {:?}",
                        container_id,
                        pid_start.elapsed()
                    ));

                    // Step 10: Schedule background network setup (if needed) - NON-BLOCKING
                    if needs_network_setup {
                        ConsoleLogger::info(&format!("🌐 [STARTUP-NET] Scheduling background network setup for container {} (PID: {})", 
                            container_id, pid.as_raw()));

                        // Clone necessary data for background task
                        let bg_container_id = container_id.to_string();
                        let bg_sync_engine = sync_engine.clone();
                        let bg_network_manager = Arc::clone(&network_manager);
                        let bg_pid = pid.as_raw();
                        let bg_actual_rootfs_path = actual_rootfs_path.clone();

                        // Emit network setup started event

                        // Launch network setup in background task - PARALLEL EXECUTION
                        ConsoleLogger::debug(&format!(
                            "🚀 [STARTUP-NET] Spawning background network setup task for {}",
                            container_id
                        ));
                        tokio::spawn(async move {
                            let network_start = std::time::Instant::now();
                            ConsoleLogger::info(&format!("📡 [BACKGROUND-NET] Starting background network setup for {} (PID: {})", 
                                bg_container_id, bg_pid));

                            // Background network setup - this runs in parallel with other containers
                            let setup_result: Result<
                                crate::sync::network::NetworkAllocation,
                                String,
                            > = setup_container_network_async(
                                &bg_sync_engine,
                                &bg_network_manager,
                                &bg_container_id,
                                bg_pid,
                                &bg_actual_rootfs_path,
                            )
                            .await;

                            match setup_result {
                                Ok(network_alloc) => {
                                    ConsoleLogger::success(&format!("🎉 [BACKGROUND-NET] Background network setup completed for {} with IP {} in {:?}", 
                                        bg_container_id, network_alloc.ip_address, network_start.elapsed()));

                                    // Store network success log
                                    let _ = bg_sync_engine
                                        .store_container_log(
                                            &bg_container_id,
                                            "info",
                                            &format!(
                                                "Network setup completed with IP {}",
                                                network_alloc.ip_address
                                            ),
                                        )
                                        .await;

                                    // Emit network setup completed event
                                }
                                Err(e) => {
                                    ConsoleLogger::error(&format!("❌ [BACKGROUND-NET] Background network setup failed for {}: {}", bg_container_id, e));

                                    // Store network error log
                                    let _ = bg_sync_engine
                                        .store_container_log(
                                            &bg_container_id,
                                            "error",
                                            &format!("Network setup failed: {}", e),
                                        )
                                        .await;

                                    // Emit network setup failed event
                                }
                            }
                        });

                        ConsoleLogger::success(&format!("✅ [STARTUP-NET] Background network setup scheduled for {} - container startup continues", container_id));
                    }
                } else {
                    ConsoleLogger::error(&format!(
                        "❌ [STARTUP-PID] Container {} started but has no PID!",
                        container_id
                    ));
                }
            } else {
                ConsoleLogger::error(&format!(
                    "❌ [STARTUP-PID] Container {} has no info after starting",
                    container_id
                ));
            }

            // Step 11: Final state transition to Running
            let final_state_start = std::time::Instant::now();
            ConsoleLogger::info(&format!(
                "🏁 [STARTUP-FINAL] Transitioning container {} to Running state",
                container_id
            ));

            // Update state to Running
            sync_engine
                .update_container_state(container_id, ContainerState::Running)
                .await
                .map_err(|e| {
                    ConsoleLogger::error(&format!(
                        "❌ [STARTUP-FINAL] Failed to update state to Running for {}: {}",
                        container_id, e
                    ));
                    format!("Failed to update to running: {}", e)
                })?;

            ConsoleLogger::debug(&format!(
                "⏱️ [STARTUP-FINAL] Final state transition completed for {} in {:?}",
                container_id,
                final_state_start.elapsed()
            ));

            // Step 12: Success completion
            let total_time = start_time.elapsed();
            ConsoleLogger::success(&format!(
                "🎉 [STARTUP-SUCCESS] Container {} started successfully in {:?}",
                container_id, total_time
            ));

            // Store final success log
            let _ = sync_engine
                .store_container_log(
                    container_id,
                    "info",
                    &format!(
                        "Container startup completed successfully in {:.2}s",
                        total_time.as_secs_f64()
                    ),
                )
                .await;

            // Emit container ready event with timing
            let _startup_time_ms = total_time.as_millis() as u64;

            ConsoleLogger::debug(&format!(
                "📡 [STARTUP-SUCCESS] Container ready event emitted for {}",
                container_id
            ));

            Ok(())
        }
        Err(e) => {
            let total_time = start_time.elapsed();
            ConsoleLogger::error(&format!(
                "❌ [STARTUP-ERROR] Container {} startup FAILED after {:?}: {}",
                container_id, total_time, e
            ));

            // Store error log
            let _ = sync_engine
                .store_container_log(
                    container_id,
                    "error",
                    &format!("Container startup failed: {}", e),
                )
                .await;

            // Emit container startup failed event

            // Update state to Error and log the failure
            sync_engine
                .update_container_state(container_id, ContainerState::Error)
                .await
                .ok();
            ConsoleLogger::error(&format!(
                "❌ [STARTUP-ERROR] Container {} state set to Error",
                container_id
            ));

            Err(format!("Failed to start container: {}", e))
        }
    }
}

/// Background async network setup function for parallel container networking
/// This function handles all network setup operations in the background without blocking container startup
async fn setup_container_network_async(
    sync_engine: &SyncEngine,
    network_manager: &Arc<icc::network::NetworkManager>,
    container_id: &str,
    container_pid: i32,
    actual_rootfs_path: &str,
) -> Result<crate::sync::network::NetworkAllocation, String> {
    ConsoleLogger::debug(&format!(
        "📡 [ASYNC-NET] Retrieving network allocation for {}",
        container_id
    ));

    // Get network allocation from sync engine
    let network_alloc = sync_engine
        .get_network_allocation(container_id)
        .await
        .map_err(|e| {
            ConsoleLogger::error(&format!(
                "❌ [ASYNC-NET] Failed to get network allocation for {}: {}",
                container_id, e
            ));
            format!("Failed to get network allocation: {}", e)
        })?;

    ConsoleLogger::debug(&format!(
        "🌐 [ASYNC-NET] Network allocation for {}: IP={}",
        container_id, network_alloc.ip_address
    ));

    // Get rootfs path for DNS configuration
    ConsoleLogger::debug(&format!(
        "📁 [ASYNC-NET] Getting rootfs path for DNS config for {}",
        container_id
    ));
    let rootfs_path = if let Ok(status) = sync_engine.get_container_status(container_id).await {
        ConsoleLogger::debug(&format!(
            "📁 [ASYNC-NET] Got rootfs path for {}: {:?}",
            container_id, status.rootfs_path
        ));
        status.rootfs_path
    } else {
        ConsoleLogger::warning(&format!(
            "⚠️ [ASYNC-NET] Could not get rootfs path for {}",
            container_id
        ));
        None
    };

    // Create ContainerNetworkConfig for ICC network manager
    let veth_host_name = format!("veth-{}", &container_id[..8]);
    let veth_container_name = format!("vethc-{}", &container_id[..8]);

    ConsoleLogger::debug(&format!(
        "🔗 [ASYNC-NET] Creating network config for {}: veth_host={}, veth_container={}",
        container_id, veth_host_name, veth_container_name
    ));

    let icc_network_config = icc::network::ContainerNetworkConfig {
        ip_address: network_alloc.ip_address.clone(),
        subnet_mask: "16".to_string(),
        gateway_ip: "10.42.0.1".to_string(),
        container_id: container_id.to_string(),
        veth_host_name: veth_host_name.clone(),
        veth_container_name: veth_container_name.clone(),
        rootfs_path,
    };

    ConsoleLogger::debug(&format!(
        "📋 [ASYNC-NET] Network config created for {}: IP={}, gateway=10.42.0.1, subnet=/16",
        container_id, network_alloc.ip_address
    ));

    // Create network ready signal BEFORE starting network setup
    // This prevents container from timing out while we set up the network
    let network_ready_path_in_container = format!(
        "{}/tmp/quilt-network-ready-{}",
        actual_rootfs_path, container_id
    );
    ConsoleLogger::debug(&format!(
        "📝 [ASYNC-NET] Creating network ready signal for {} at {}",
        container_id, network_ready_path_in_container
    ));

    FileSystemUtils::write_file(&network_ready_path_in_container, "ready").map_err(|e| {
        ConsoleLogger::error(&format!(
            "❌ [ASYNC-NET] Failed to create network ready signal for {}: {}",
            container_id, e
        ));
        format!("Failed to create network ready signal: {}", e)
    })?;
    ConsoleLogger::debug(&format!(
        "✅ [ASYNC-NET] Created network ready signal at {}",
        network_ready_path_in_container
    ));

    // Setup container network using ICC network manager (lock-free, parallel-safe)
    ConsoleLogger::debug(&format!(
        "🔧 [ASYNC-NET] Setting up container network for {} (PID: {})",
        container_id, container_pid
    ));
    network_manager
        .setup_container_network(&icc_network_config, container_pid)
        .map_err(|e| {
            ConsoleLogger::error(&format!(
                "❌ [ASYNC-NET] Network setup failed for {}: {}",
                container_id, e
            ));
            e
        })?;

    ConsoleLogger::success(&format!(
        "✅ [ASYNC-NET] Container network setup succeeded for {}",
        container_id
    ));

    // Mark network setup complete in sync engine
    ConsoleLogger::debug(&format!(
        "📝 [ASYNC-NET] Marking network setup complete in sync engine for {}",
        container_id
    ));
    sync_engine
        .mark_network_setup_complete(
            container_id,
            "quilt0",
            &veth_host_name,
            &veth_container_name,
        )
        .await
        .map_err(|e| {
            ConsoleLogger::error(&format!(
                "❌ [ASYNC-NET] Failed to mark network setup complete for {}: {}",
                container_id, e
            ));
            format!("Failed to mark network setup complete: {}", e)
        })?;

    // Register container with DNS
    ConsoleLogger::debug(&format!(
        "🌐 [ASYNC-NET] Registering DNS for {}",
        container_id
    ));
    let container_name = if let Ok(status) = sync_engine.get_container_status(container_id).await {
        status.name.unwrap_or_else(|| container_id.to_string())
    } else {
        container_id.to_string()
    };

    ConsoleLogger::debug(&format!(
        "🌐 [ASYNC-NET] DNS name for {}: {}",
        container_id, container_name
    ));

    network_manager
        .register_container_dns(container_id, &container_name, &network_alloc.ip_address)
        .map_err(|e| {
            ConsoleLogger::error(&format!(
                "❌ [ASYNC-NET] DNS registration failed for {}: {}",
                container_id, e
            ));
            e
        })?;

    ConsoleLogger::success(&format!(
        "🎉 [ASYNC-NET] All network setup operations complete for {} with IP {}",
        container_id, network_alloc.ip_address
    ));

    Ok(network_alloc)
}

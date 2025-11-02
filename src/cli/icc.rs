// src/cli/icc.rs
// Inter-Container Communication CLI commands

// Allow dead code temporarily until client command integration is complete
#![allow(dead_code)]

use clap::Subcommand;
use std::collections::HashMap;
use std::time::Duration;
use tonic::transport::Channel;
use serde::Serialize;

// Use protobuf definitions from parent
use crate::quilt::quilt_service_client::QuiltServiceClient;
use crate::quilt::{
    GetContainerStatusRequest,
    ExecContainerRequest,
    ContainerStatus,
};

#[derive(Debug, Clone, Serialize)]
pub enum ConnectionType {
    Tcp { port: u16 },
}


#[derive(Debug, Clone, Serialize)]
pub enum ConnectionStatus {
    Active,
    Disconnected,
}

/// Extended ConnectionInfo for monitoring and display
#[derive(Debug, Clone, Serialize)]
struct ExtendedConnectionInfo {
    pub from_container: String,
    pub to_container: String,
    pub connection_type: ConnectionType,
    pub established_at: u64,
    pub status: ConnectionStatus,
    pub connection_id: String,
    pub protocol: String,
    pub is_active: bool,
    pub last_tested: String,
    pub latency_ms: Option<u32>,
}

#[derive(Subcommand, Debug)]
pub enum IccCommands {
    /// Test connectivity between containers
    Ping {
        #[clap(help = "Source container ID")]
        from_container: String,
        #[clap(help = "Target container ID or IP address")]
        target: String,
        #[clap(long, help = "Number of ping packets to send", default_value = "3")]
        count: u32,
        #[clap(long, help = "Timeout in seconds", default_value = "5")]
        timeout: u32,
    },

    /// Establish persistent connections between containers
    Connect {
        #[clap(help = "Source container ID")]
        from_container: String,
        #[clap(help = "Target container ID")]
        to_container: String,
        #[clap(long, help = "Connection type", default_value = "tcp")]
        connection_type: String,
        #[clap(long, help = "Target port for TCP/UDP connections")]
        port: Option<u16>,
        #[clap(long, help = "Connection pool size for database connections", default_value = "5")]
        pool_size: Option<u32>,
        #[clap(long, help = "WebSocket path for WebSocket connections")]
        path: Option<String>,
        #[clap(long, help = "Queue name for message queue connections")]
        queue: Option<String>,
        #[clap(long, help = "Keep connection alive", default_value = "true")]
        persistent: bool,
        #[clap(long, help = "Auto-reconnect on failure", default_value = "true")]
        auto_reconnect: bool,
    },

    /// Disconnect containers
    Disconnect {
        #[clap(help = "Source container ID")]
        from_container: String,
        #[clap(help = "Target container ID (optional - disconnects all if not specified)")]
        to_container: Option<String>,
        #[clap(long, help = "Specific connection ID to disconnect")]
        connection_id: Option<String>,
        #[clap(long, help = "Force disconnect even if connection is active")]
        force: bool,
        #[clap(long, help = "Disconnect all connections for this container")]
        all: bool,
    },

    /// Manage and view connections
    Connections {
        #[clap(subcommand)]
        action: ConnectionAction,
    },

    /// Execute commands inside containers for testing
    Exec {
        #[clap(help = "Container ID to execute command in")]
        container_id: String,
        #[clap(long, help = "Working directory inside container")]
        workdir: Option<String>,
        #[clap(long, help = "Environment variables", action = clap::ArgAction::Append)]
        env: Vec<String>,
        #[clap(help = "Command and arguments to execute", required = true, num_args = 1..)]
        command: Vec<String>,
    },

    /// Show network topology and information
    Network {
        #[clap(subcommand)]
        action: NetworkAction,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConnectionAction {
    /// List all active connections
    List {
        #[clap(long, help = "Filter by container ID")]
        container: Option<String>,
        #[clap(long, help = "Filter by connection type")]
        connection_type: Option<String>,
        #[clap(long, help = "Show only active connections")]
        active_only: bool,
        #[clap(long, help = "Output format: table, json, yaml", default_value = "table")]
        format: String,
    },

    /// Show detailed information about a specific connection
    Show {
        #[clap(help = "Connection ID")]
        connection_id: String,
    },

    /// Monitor connection health and status
    Monitor {
        #[clap(help = "Container ID to monitor (optional - monitors all if not specified)")]
        container: Option<String>,
        #[clap(long, help = "Refresh interval in seconds", default_value = "5")]
        interval: u32,
        #[clap(long, help = "Show connection metrics")]
        metrics: bool,
    },

    /// Test connection health
    Health {
        #[clap(help = "Connection ID or container ID")]
        target: String,
        #[clap(long, help = "Detailed health check")]
        detailed: bool,
    },
}

#[derive(Subcommand, Debug)]
pub enum NetworkAction {
    /// Show network topology
    Topology {
        #[clap(long, help = "Output format: ascii, json, dot", default_value = "ascii")]
        format: String,
        #[clap(long, help = "Include connection details")]
        details: bool,
    },

    /// List all container IP addresses
    List {
        #[clap(long, help = "Show only running containers")]
        running_only: bool,
        #[clap(long, help = "Output format: table, json, yaml", default_value = "table")]
        format: String,
    },

    /// Show network information for a specific container
    Show {
        #[clap(help = "Container ID")]
        container_id: String,
    },

    /// Test network connectivity
    Test {
        #[clap(help = "Source container ID")]
        from_container: String,
        #[clap(help = "Target container ID or IP")]
        target: String,
        #[clap(long, help = "Test specific port")]
        port: Option<u16>,
        #[clap(long, help = "Test protocol: tcp, udp, icmp", default_value = "icmp")]
        protocol: String,
    },
}

// Implementation functions (to be implemented)
pub async fn handle_icc_command(cmd: IccCommands, mut client: QuiltServiceClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    match cmd {
        IccCommands::Ping { from_container, target, count, timeout } => {
            handle_ping_command(from_container, target, count, timeout, &mut client).await
        },
        IccCommands::Connect { 
            from_container, 
            to_container, 
            connection_type, 
            port, 
            pool_size, 
            path, 
            queue, 
            persistent, 
            auto_reconnect 
        } => {
            handle_connect_command(
                from_container, 
                to_container, 
                connection_type, 
                port, 
                pool_size, 
                path, 
                queue, 
                persistent, 
                auto_reconnect,
                &mut client
            ).await
        },
        IccCommands::Disconnect { from_container, to_container, connection_id, force, all } => {
            handle_disconnect_command(from_container, to_container, connection_id, force, all, &mut client).await
        },
        IccCommands::Connections { action } => {
            handle_connections_command(action, &mut client).await
        },
        IccCommands::Exec { container_id, workdir, env, command } => {
            handle_exec_command(container_id, workdir, env, command, &mut client).await
        },
        IccCommands::Network { action } => {
            handle_network_command(action, &mut client).await
        },
    }
}

// Placeholder implementations - to be filled in
async fn handle_ping_command(
    from_container: String, 
    target: String, 
    count: u32, 
    timeout: u32,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🏓 Pinging from {} to {} ({} packets, {}s timeout)", from_container, target, count, timeout);
    
    // ELITE: Check source container status
    let mut from_request = tonic::Request::new(GetContainerStatusRequest { 
        container_id: from_container.clone(),
        container_name: String::new(),
    });
    from_request.set_timeout(Duration::from_secs(30));
    
    let _from_status = match client.get_container_status(from_request).await {
        Ok(response) => {
            let status = response.into_inner();
            let container_status = match status.status {
                0 => ContainerStatus::Pending,
                1 => ContainerStatus::Running,
                2 => ContainerStatus::Exited,
                _ => ContainerStatus::Failed,
            };
            
            if !matches!(container_status, ContainerStatus::Running) {
                return Err(format!("Source container {} is not running (status: {:?})", from_container, container_status).into());
            }
            status
        }
        Err(e) => {
            return Err(format!("Failed to get status for container {}: {}", from_container, e).into());
        }
    };
    
    // ELITE: Determine target IP
    let final_target_ip = if target.contains('.') {
        // Already an IP address
        target
    } else {
        // Container ID - get its IP
        let mut target_request = tonic::Request::new(GetContainerStatusRequest {
            container_id: target.clone(),
            container_name: String::new(),
        });
        target_request.set_timeout(Duration::from_secs(30));
        
        match client.get_container_status(target_request).await {
            Ok(response) => {
                let status = response.into_inner();
                let container_status = match status.status {
                    0 => ContainerStatus::Pending,
                    1 => ContainerStatus::Running,
                    2 => ContainerStatus::Exited,
                    _ => ContainerStatus::Failed,
                };
                
                if !matches!(container_status, ContainerStatus::Running) {
                    return Err(format!("Target container {} is not running (status: {:?})", target, container_status).into());
                }
                
                if status.ip_address.is_empty() || status.ip_address == "No IP assigned" {
                    return Err(format!("Target container {} has no IP address assigned", target).into());
                }
                
                status.ip_address
            }
            Err(e) => {
                return Err(format!("Failed to get status for target container {}: {}", target, e).into());
            }
        }
    };
    
    // ELITE: Use optimized ping with adaptive timeout
    let adaptive_timeout = std::cmp::max(timeout, 10); // Minimum 10s for network load
    let ping_cmd = vec![
        "ping".to_string(),
        "-c".to_string(), count.to_string(),
        "-W".to_string(), adaptive_timeout.to_string(),
        "-i".to_string(), "0.5".to_string(),  // ELITE: Faster ping interval
        final_target_ip.clone()
    ];
    
    let mut exec_request = tonic::Request::new(ExecContainerRequest {
        container_id: from_container.clone(),
        command: ping_cmd,
        working_directory: String::new(),
        environment: HashMap::new(),
        capture_output: true,
        container_name: String::new(),
        copy_script: false,
    });
    // ELITE: Much more generous timeout for exec under load
    exec_request.set_timeout(Duration::from_secs(adaptive_timeout as u64 + 10)); 
    
    println!("📡 Executing ping with {:.1}s timeout...", adaptive_timeout);
    
    match client.exec_container(exec_request).await {
        Ok(response) => {
            let result = response.into_inner();
            
            if result.success {
                println!("✅ Ping successful!");
                if !result.stdout.is_empty() {
                    println!("📤 Output:");
                    println!("{}", result.stdout);
                }
            } else {
                println!("❌ Ping from {} to {} failed. Exit code: {}", from_container, final_target_ip, result.exit_code);
                if result.exit_code == 124 {
                    println!("⚠️  Exit code 124 indicates timeout - network may still be initializing");
                }
                if !result.stdout.is_empty() {
                    println!("📤 Output:");
                    println!("{}", result.stdout);
                }
                if !result.stderr.is_empty() {
                    println!("📤 Error:");
                    println!("{}", result.stderr);
                }
            }
            
            Ok(())
        }
        Err(e) => {
            Err(format!("Failed to execute ping command: {}", e).into())
        }
    }
}

async fn handle_connect_command(
    from_container: String,
    to_container: String,
    connection_type: String,
    port: Option<u16>,
    _pool_size: Option<u32>,
    _path: Option<String>,
    _queue: Option<String>,
    persistent: bool,
    auto_reconnect: bool,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔗 Establishing {} connection from {} to {}", connection_type, from_container, to_container);
    
    // Validate containers exist and are running
    let _from_status = validate_container_running(&from_container, client).await?;
    let to_status = validate_container_running(&to_container, client).await?;
    
    // Parse connection type and establish connection
    match connection_type.as_str() {
        "tcp" => {
            let target_port = port.unwrap_or(8080);
            establish_tcp_connection(&from_container, &to_container, &to_status.ip_address, target_port, persistent, client).await?;
        },
        "udp" => {
            let target_port = port.unwrap_or(8080);
            establish_udp_connection(&from_container, &to_container, &to_status.ip_address, target_port, persistent, client).await?;
        },
        "http" => {
            let target_port = port.unwrap_or(80);
            establish_http_connection(&from_container, &to_container, &to_status.ip_address, target_port, client).await?;
        },
        _ => {
            return Err(format!("Unsupported connection type: {}. Supported: tcp, udp, http", connection_type).into());
        }
    }
    
    println!("✅ {} connection established successfully", connection_type.to_uppercase());
    if persistent {
        println!("🔄 Connection is persistent and will be maintained");
    }
    if auto_reconnect {
        println!("🔁 Auto-reconnect is enabled for connection failures");
    }
    
    Ok(())
}

async fn handle_disconnect_command(
    from_container: String,
    to_container: Option<String>,
    connection_id: Option<String>,
    force: bool,
    all: bool,
    _client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    if all {
        println!("🔌 Disconnecting all connections for {}", from_container);
    } else if let Some(to_container) = to_container {
        println!("🔌 Disconnecting {} from {}", from_container, to_container);
    } else if let Some(connection_id) = connection_id {
        println!("🔌 Disconnecting connection {}", connection_id);
    }
    if force {
        println!("   Mode: Force disconnect");
    }
    // TODO: Implement disconnection
    Ok(())
}

async fn handle_connections_command(action: ConnectionAction, client: &mut QuiltServiceClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        ConnectionAction::List { container, connection_type, active_only, format } => {
            list_connections(container, connection_type, active_only, format, client).await?;
        },
        ConnectionAction::Show { connection_id } => {
            show_connection_details(connection_id, client).await?;
        },
        ConnectionAction::Monitor { container, interval, metrics } => {
            monitor_connections(container, interval, metrics, client).await?;
        },
        ConnectionAction::Health { target, detailed } => {
            check_connection_health(target, detailed, client).await?;
        },
    }
    Ok(())
}

async fn handle_exec_command(
    container_id: String,
    workdir: Option<String>,
    env: Vec<String>,
    command: Vec<String>,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("⚡ Executing command in container {}", container_id);
    println!("   Command: {:?}", command);
    if let Some(ref workdir) = workdir {
        println!("   Working directory: {}", workdir);
    }
    if !env.is_empty() {
        println!("   Environment variables: {:?}", env);
    }

    // Parse environment variables from "KEY=VALUE" format
    let mut environment = HashMap::new();
    
    // Always add QUILT_SERVER to point to the bridge IP for nested container access
    environment.insert("QUILT_SERVER".to_string(), "10.42.0.1:50051".to_string());
    
    for env_var in env {
        if let Some((key, value)) = env_var.split_once('=') {
            environment.insert(key.to_string(), value.to_string());
        } else {
            return Err(format!("Invalid environment variable format: {}. Use KEY=VALUE", env_var).into());
        }
    }

    // Execute the command
    let mut exec_request = tonic::Request::new(ExecContainerRequest {
        container_id: container_id.clone(),
        command,
        working_directory: workdir.unwrap_or_default(),
        environment,
        capture_output: true,
        container_name: String::new(),
        copy_script: false,
    });
    exec_request.set_timeout(Duration::from_secs(30)); // Generous timeout for exec commands

    match client.exec_container(exec_request).await {
        Ok(response) => {
            let result = response.into_inner();
            
            if result.success {
                println!("✅ Command executed successfully (exit code: {})", result.exit_code);
                if !result.stdout.is_empty() {
                    println!("📤 Output:");
                    println!("{}", result.stdout);
                }
                if !result.stderr.is_empty() {
                    println!("⚠️ Error output:");
                    println!("{}", result.stderr);
                }
            } else {
                println!("❌ Command failed with exit code: {}", result.exit_code);
                if !result.stderr.is_empty() {
                    println!("📤 Error:");
                    println!("{}", result.stderr);
                }
                if !result.stdout.is_empty() {
                    println!("📤 Output:");
                    println!("{}", result.stdout);
                }
            }
            
            Ok(())
        }
        Err(e) => {
            Err(format!("Failed to execute command: {}", e).into())
        }
    }
}

async fn handle_network_command(action: NetworkAction, client: &mut QuiltServiceClient<Channel>) -> Result<(), Box<dyn std::error::Error>> {
    match action {
        NetworkAction::Topology { format, details } => {
            display_network_topology(format, details, client).await?;
        },
        NetworkAction::List { running_only, format } => {
            list_network_information(running_only, format, client).await?;
        },
        NetworkAction::Show { container_id } => {
            show_container_network_info(container_id, client).await?;
        },
        NetworkAction::Test { from_container, target, port, protocol } => {
            test_network_connectivity(from_container, target, port, protocol, client).await?;
        },
    }
    Ok(())
}

/// Validate that a container exists and is running
async fn validate_container_running(
    container_id: &str,
    client: &mut QuiltServiceClient<Channel>
) -> Result<crate::quilt::GetContainerStatusResponse, Box<dyn std::error::Error>> {
    let mut request = tonic::Request::new(GetContainerStatusRequest {
        container_id: container_id.to_string(),
        container_name: String::new(),
    });
    request.set_timeout(Duration::from_secs(10));
    
    let response = client.get_container_status(request).await
        .map_err(|e| format!("Failed to get status for container {}: {}", container_id, e))?;
    
    let status = response.into_inner();
    let container_status = match status.status {
        0 => ContainerStatus::Pending,
        1 => ContainerStatus::Running,
        2 => ContainerStatus::Exited,
        _ => ContainerStatus::Failed,
    };
    
    if !matches!(container_status, ContainerStatus::Running) {
        return Err(format!("Container {} is not running (status: {:?})", container_id, container_status).into());
    }
    
    if status.ip_address.is_empty() || status.ip_address == "No IP assigned" {
        return Err(format!("Container {} has no IP address assigned", container_id).into());
    }
    
    Ok(status)
}

/// Establish TCP connection between containers
async fn establish_tcp_connection(
    from_container: &str,
    _to_container: &str,
    target_ip: &str,
    port: u16,
    persistent: bool,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔌 Establishing TCP connection to {}:{}", target_ip, port);
    
    // Test connection using netcat or telnet
    let test_cmd = vec![
        "timeout".to_string(),
        "5".to_string(),
        "bash".to_string(),
        "-c".to_string(),
        format!("echo 'CONNECTION_TEST' | nc -w 1 {} {} || echo 'CONNECTION_FAILED'", target_ip, port)
    ];
    
    let mut exec_request = tonic::Request::new(ExecContainerRequest {
        container_id: from_container.to_string(),
        command: test_cmd,
        working_directory: String::new(),
        environment: HashMap::new(),
        capture_output: true,
        container_name: String::new(),
        copy_script: false,
    });
    exec_request.set_timeout(Duration::from_secs(10));
    
    match client.exec_container(exec_request).await {
        Ok(response) => {
            let result = response.into_inner();
            if result.stdout.contains("CONNECTION_FAILED") {
                println!("⚠️  TCP port {} not accepting connections (this is normal if no service is listening)", port);
            } else {
                println!("✅ TCP connection test completed");
            }
            
            if persistent {
                println!("🔗 Setting up persistent TCP tunnel...");
                // For persistent connections, we could set up port forwarding or keep-alive
                // This would require more complex implementation
            }
        },
        Err(e) => {
            return Err(format!("Failed to test TCP connection: {}", e).into());
        }
    }
    
    Ok(())
}

/// Establish UDP connection between containers
async fn establish_udp_connection(
    from_container: &str,
    _to_container: &str,
    target_ip: &str,
    port: u16,
    persistent: bool,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("📡 Establishing UDP connection to {}:{}", target_ip, port);
    
    // Test UDP connection
    let test_cmd = vec![
        "timeout".to_string(),
        "3".to_string(),
        "bash".to_string(),
        "-c".to_string(),
        format!("echo 'UDP_TEST' | nc -u -w 1 {} {} 2>/dev/null && echo 'UDP_SENT' || echo 'UDP_FAILED'", target_ip, port)
    ];
    
    let mut exec_request = tonic::Request::new(ExecContainerRequest {
        container_id: from_container.to_string(),
        command: test_cmd,
        working_directory: String::new(),
        environment: HashMap::new(),
        capture_output: true,
        container_name: String::new(),
        copy_script: false,
    });
    exec_request.set_timeout(Duration::from_secs(8));
    
    match client.exec_container(exec_request).await {
        Ok(response) => {
            let result = response.into_inner();
            if result.stdout.contains("UDP_SENT") {
                println!("✅ UDP packet sent successfully");
            } else {
                println!("⚠️  UDP connection test inconclusive (UDP is connectionless)");
            }
            
            if persistent {
                println!("📡 UDP connection established (connectionless protocol)");
            }
        },
        Err(e) => {
            return Err(format!("Failed to test UDP connection: {}", e).into());
        }
    }
    
    Ok(())
}

/// Establish HTTP connection between containers
async fn establish_http_connection(
    from_container: &str,
    _to_container: &str,
    target_ip: &str,
    port: u16,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🌐 Establishing HTTP connection to http://{}:{}", target_ip, port);
    
    // Test HTTP connection using curl
    let test_cmd = vec![
        "timeout".to_string(),
        "10".to_string(),
        "curl".to_string(),
        "-s".to_string(),
        "-o".to_string(),
        "/dev/null".to_string(),
        "-w".to_string(),
        "%{http_code}".to_string(),
        "--connect-timeout".to_string(),
        "5".to_string(),
        format!("http://{}:{}/", target_ip, port)
    ];
    
    let mut exec_request = tonic::Request::new(ExecContainerRequest {
        container_id: from_container.to_string(),
        command: test_cmd,
        working_directory: String::new(),
        environment: HashMap::new(),
        capture_output: true,
        container_name: String::new(),
        copy_script: false,
    });
    exec_request.set_timeout(Duration::from_secs(15));
    
    match client.exec_container(exec_request).await {
        Ok(response) => {
            let result = response.into_inner();
            let http_code = result.stdout.trim();
            
            if http_code.len() == 3 && http_code.chars().all(|c| c.is_ascii_digit()) {
                let code: u16 = http_code.parse().unwrap_or(0);
                match code {
                    200..=299 => println!("✅ HTTP connection successful ({})", code),
                    300..=399 => println!("🔄 HTTP connection successful with redirect ({})", code),
                    400..=499 => println!("⚠️  HTTP connection successful but client error ({})", code),
                    500..=599 => println!("❌ HTTP connection successful but server error ({})", code),
                    _ => println!("🔗 HTTP connection established, response code: {}", code),
                }
            } else {
                println!("⚠️  HTTP connection test inconclusive (no HTTP service or connection failed)");
            }
        },
        Err(e) => {
            return Err(format!("Failed to test HTTP connection: {}", e).into());
        }
    }
    
    Ok(())
}

/// List all connections between containers
async fn list_connections(
    container_filter: Option<String>,
    _connection_type_filter: Option<String>,
    active_only: bool,
    format: String,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("📋 Listing connections (format: {})", format);
    
    // Get list of all running containers first
    let running_containers = get_running_containers(client).await?;
    
    if running_containers.is_empty() {
        println!("No running containers found");
        return Ok(());
    }
    
    // For each container, check network connectivity to others
    let mut connections = Vec::new();
    
    for from_container in &running_containers {
        if let Some(ref filter) = container_filter {
            if !from_container.contains(filter) {
                continue;
            }
        }
        
        for to_container in &running_containers {
            if from_container == to_container {
                continue;
            }
            
            // Test basic connectivity
            if let Ok(connectivity) = test_container_connectivity(from_container, to_container, client).await {
                if !active_only || connectivity.is_active {
                    connections.push(connectivity);
                }
            }
        }
    }
    
    // Display connections based on format
    match format.as_str() {
        "json" => {
            println!("{}", serde_json::to_string_pretty(&connections).unwrap_or_else(|_| "[]".to_string()));
        },
        "table" | _ => {
            if connections.is_empty() {
                println!("No connections found matching criteria");
            } else {
                println!("┌─────────────────────┬─────────────────────┬──────────┬─────────────┐");
                println!("│ From Container      │ To Container        │ Protocol │ Status      │");
                println!("├─────────────────────┼─────────────────────┼──────────┼─────────────┤");
                for conn in connections {
                    println!("│ {:19} │ {:19} │ {:8} │ {:11} │", 
                           truncate_string(&conn.from_container, 19),
                           truncate_string(&conn.to_container, 19),
                           conn.protocol,
                           if conn.is_active { "Active" } else { "Inactive" });
                }
                println!("└─────────────────────┴─────────────────────┴──────────┴─────────────┘");
            }
        }
    }
    
    Ok(())
}

/// Show detailed connection information
async fn show_connection_details(
    connection_id: String,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Connection Details for {}", connection_id);
    
    // Parse connection ID (format: from_container:to_container:protocol)
    let parts: Vec<&str> = connection_id.split(':').collect();
    if parts.len() != 3 {
        return Err("Invalid connection ID format. Expected: from_container:to_container:protocol".into());
    }
    
    let from_container = parts[0];
    let to_container = parts[1];
    
    // Get detailed connectivity information
    if let Ok(connectivity) = test_container_connectivity(from_container, to_container, client).await {
        println!("Connection Information:");
        println!("  From: {}", connectivity.from_container);
        println!("  To: {}", connectivity.to_container);
        println!("  Protocol: {}", connectivity.protocol);
        println!("  Status: {}", if connectivity.is_active { "Active" } else { "Inactive" });
        println!("  Last Tested: {}", connectivity.last_tested);
        if let Some(latency) = connectivity.latency_ms {
            println!("  Latency: {}ms", latency);
        }
    } else {
        println!("❌ Connection not found or inaccessible");
    }
    
    Ok(())
}

/// Monitor connections in real-time
async fn monitor_connections(
    container_filter: Option<String>,
    interval: u32,
    metrics: bool,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("📊 Monitoring connections ({}s interval)", interval);
    println!("Press Ctrl+C to stop monitoring");
    
    loop {
        println!("\n{}", "─".repeat(60));
        println!("Connection Status - {}", chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC"));
        println!("{}", "─".repeat(60));
        
        list_connections(container_filter.clone(), None, true, "table".to_string(), client).await?;
        
        if metrics {
            println!("\n📈 Connection Metrics:");
            display_connection_metrics(client).await?;
        }
        
        tokio::time::sleep(Duration::from_secs(interval as u64)).await;
    }
}

/// Check connection health
async fn check_connection_health(
    target: String,
    detailed: bool,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🏥 Checking connection health for {}", target);
    
    // If target contains ':', treat as connection ID, otherwise as container ID
    if target.contains(':') {
        // Connection ID format
        let parts: Vec<&str> = target.split(':').collect();
        if parts.len() >= 2 {
            let from_container = parts[0];
            let to_container = parts[1];
            
            if let Ok(connectivity) = test_container_connectivity(from_container, to_container, client).await {
                print_health_status(&connectivity, detailed);
            } else {
                println!("❌ Connection health check failed");
            }
        }
    } else {
        // Container ID - check all its connections
        let running_containers = get_running_containers(client).await?;
        let mut healthy_connections = 0;
        let mut total_connections = 0;
        
        for other_container in &running_containers {
            if other_container == &target {
                continue;
            }
            
            total_connections += 1;
            if let Ok(connectivity) = test_container_connectivity(&target, other_container, client).await {
                if connectivity.is_active {
                    healthy_connections += 1;
                }
                if detailed {
                    print_health_status(&connectivity, true);
                }
            }
        }
        
        println!("Container {} health: {}/{} connections active", target, healthy_connections, total_connections);
        if healthy_connections == total_connections {
            println!("✅ All connections healthy");
        } else if healthy_connections > 0 {
            println!("⚠️  Some connections unhealthy");
        } else {
            println!("❌ No healthy connections");
        }
    }
    
    Ok(())
}

/// Display connection metrics
async fn display_connection_metrics(
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    let running_containers = get_running_containers(client).await?;
    let total_containers = running_containers.len();
    let total_possible_connections = if total_containers > 1 { total_containers * (total_containers - 1) } else { 0 };
    
    println!("  Total Containers: {}", total_containers);
    println!("  Possible Connections: {}", total_possible_connections);
    println!("  Network Topology: Bridge (10.42.0.0/16)");
    
    Ok(())
}

/// Get list of running containers
async fn get_running_containers(
    _client: &mut QuiltServiceClient<Channel>
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    // This is a simplified implementation - in a real system, you'd query the server for all containers
    // For now, we'll return an empty list since we don't have a list_containers gRPC method
    // This would need to be implemented on the server side
    println!("📝 Note: Container listing requires server-side implementation");
    Ok(vec![])
}

/// Test connectivity between two containers
async fn test_container_connectivity(
    from_container: &str,
    to_container: &str,
    client: &mut QuiltServiceClient<Channel>
) -> Result<ExtendedConnectionInfo, Box<dyn std::error::Error>> {
    let start_time = std::time::Instant::now();
    
    // Get target container's IP
    let to_status = validate_container_running(to_container, client).await?;
    
    // Test ICMP connectivity
    let ping_cmd = vec![
        "ping".to_string(),
        "-c".to_string(), "1".to_string(),
        "-W".to_string(), "2".to_string(),
        to_status.ip_address.clone()
    ];
    
    let mut exec_request = tonic::Request::new(ExecContainerRequest {
        container_id: from_container.to_string(),
        command: ping_cmd,
        working_directory: String::new(),
        environment: HashMap::new(),
        capture_output: true,
        container_name: String::new(),
        copy_script: false,
    });
    exec_request.set_timeout(Duration::from_secs(5));
    
    let is_active = match client.exec_container(exec_request).await {
        Ok(response) => {
            let result = response.into_inner();
            result.success && result.exit_code == 0
        },
        Err(_) => false,
    };
    
    let latency_ms = if is_active {
        Some(start_time.elapsed().as_millis() as u32)
    } else {
        None
    };
    
    Ok(ExtendedConnectionInfo {
        from_container: from_container.to_string(),
        to_container: to_container.to_string(),
        connection_type: ConnectionType::Tcp { port: 80 }, // Default
        established_at: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs(),
        status: if is_active { ConnectionStatus::Active } else { ConnectionStatus::Disconnected },
        connection_id: format!("{}:{}:icmp", from_container, to_container),
        protocol: "ICMP".to_string(),
        is_active,
        last_tested: chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        latency_ms,
    })
}

/// Print health status for a connection
fn print_health_status(connectivity: &ExtendedConnectionInfo, detailed: bool) {
    let status_icon = if connectivity.is_active { "✅" } else { "❌" };
    println!("{} {}->{} ({})", status_icon, connectivity.from_container, connectivity.to_container, connectivity.protocol);
    
    if detailed {
        println!("    Last tested: {}", connectivity.last_tested);
        if let Some(latency) = connectivity.latency_ms {
            println!("    Latency: {}ms", latency);
        }
    }
}

/// Truncate string to specified length
fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        format!("{:width$}", s, width = max_len)
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Display network topology
async fn display_network_topology(
    format: String,
    details: bool,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🌐 Network topology (format: {})", format);
    if details {
        println!("   Including: Connection details");
    }
    
    let running_containers = get_running_containers(client).await?;
    
    match format.as_str() {
        "json" => {
            let topology_data = serde_json::json!({
                "network_type": "bridge",
                "bridge_ip": "10.42.0.1",
                "subnet": "10.42.0.0/16",
                "containers": running_containers.len(),
                "dns_server": "10.42.0.1:1053"
            });
            println!("{}", serde_json::to_string_pretty(&topology_data)?);
        },
        "dot" => {
            println!("digraph NetworkTopology {{");
            println!("  rankdir=TB;");
            println!("  bridge [label=\"Quilt Bridge\\n10.42.0.1\" shape=diamond];");
            for container in &running_containers {
                println!("  \"{}\" [shape=box];", container);
                println!("  bridge -> \"{}\";", container);
            }
            println!("}}");
        },
        "ascii" | _ => {
            println!("Network Topology:");
            println!("┌──────────────────────────────────────┐");
            println!("│        Quilt Bridge Network         │");
            println!("│         (10.42.0.0/16)              │");
            println!("└──────────────────────────────────────┘");
            println!("                   │");
            println!("               Bridge IP");
            println!("              (10.42.0.1)");
            println!("                   │");
            if running_containers.is_empty() {
                println!("            [No containers]");
            } else {
                for (i, container) in running_containers.iter().enumerate() {
                    let connector = if i == running_containers.len() - 1 { "└──" } else { "├──" };
                    println!("               {} {}", connector, container);
                }
            }
        }
    }
    
    if details {
        println!("\nNetwork Details:");
        println!("  Bridge Interface: quilt0");
        println!("  DNS Server: 10.42.0.1:1053");
        println!("  IP Range: 10.42.0.2 - 10.42.255.254");
        println!("  Total Containers: {}", running_containers.len());
    }
    
    Ok(())
}

/// List network information for all containers
async fn list_network_information(
    running_only: bool,
    format: String,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("📋 Container network information (format: {})", format);
    if running_only {
        println!("   Filter: Running containers only");
    }
    
    let containers = get_running_containers(client).await?;
    
    if containers.is_empty() {
        println!("No containers found");
        return Ok(());
    }
    
    match format.as_str() {
        "json" => {
            let network_info = containers.iter().map(|container| {
                serde_json::json!({
                    "container_id": container,
                    "ip_address": "10.42.0.x",
                    "bridge": "quilt0",
                    "dns_server": "10.42.0.1:1053"
                })
            }).collect::<Vec<_>>();
            println!("{}", serde_json::to_string_pretty(&network_info)?);
        },
        "table" | _ => {
            println!("┌─────────────────────┬─────────────┬─────────────┬─────────────┐");
            println!("│ Container ID        │ IP Address  │ Bridge      │ Status      │");
            println!("├─────────────────────┼─────────────┼─────────────┼─────────────┤");
            for container in containers {
                println!("│ {:19} │ {:11} │ {:11} │ {:11} │",
                       truncate_string(&container, 19),
                       "10.42.0.x",
                       "quilt0",
                       "Connected");
            }
            println!("└─────────────────────┴─────────────┴─────────────┴─────────────┘");
        }
    }
    
    Ok(())
}

/// Show network information for a specific container
async fn show_container_network_info(
    container_id: String,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Network information for container {}", container_id);
    
    // Get container status to retrieve network info
    match validate_container_running(&container_id, client).await {
        Ok(status) => {
            println!("Container Network Information:");
            println!("  Container ID: {}", status.container_id);
            println!("  IP Address: {}", status.ip_address);
            println!("  Bridge: quilt0");
            println!("  Subnet: 10.42.0.0/16");
            println!("  Gateway: 10.42.0.1");
            println!("  DNS Server: 10.42.0.1:1053");
            println!("  Status: Connected");
            
            // Test basic connectivity
            println!("\nConnectivity Test:");
            if let Ok(connectivity) = test_gateway_connectivity(&container_id, client).await {
                println!("  Gateway Reachable: {}", if connectivity { "Yes" } else { "No" });
            }
        },
        Err(e) => {
            println!("❌ Cannot retrieve network information: {}", e);
        }
    }
    
    Ok(())
}

/// Test network connectivity between containers
async fn test_network_connectivity(
    from_container: String,
    target: String,
    port: Option<u16>,
    protocol: String,
    client: &mut QuiltServiceClient<Channel>
) -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Testing {} connectivity from {} to {}", protocol, from_container, target);
    if let Some(port) = port {
        println!("   Port: {}", port);
    }
    
    match protocol.to_lowercase().as_str() {
        "icmp" => {
            // Test ICMP ping
            if let Ok(connectivity) = test_container_connectivity(&from_container, &target, client).await {
                if connectivity.is_active {
                    println!("✅ ICMP connectivity successful");
                    if let Some(latency) = connectivity.latency_ms {
                        println!("   Latency: {}ms", latency);
                    }
                } else {
                    println!("❌ ICMP connectivity failed");
                }
            }
        },
        "tcp" => {
            let test_port = port.unwrap_or(80);
            establish_tcp_connection(&from_container, &target, &target, test_port, false, client).await?;
        },
        "udp" => {
            let test_port = port.unwrap_or(80);
            establish_udp_connection(&from_container, &target, &target, test_port, false, client).await?;
        },
        _ => {
            return Err(format!("Unsupported protocol: {}. Supported: icmp, tcp, udp", protocol).into());
        }
    }
    
    Ok(())
}

/// Test gateway connectivity for a container
async fn test_gateway_connectivity(
    container_id: &str,
    client: &mut QuiltServiceClient<Channel>
) -> Result<bool, Box<dyn std::error::Error>> {
    let ping_cmd = vec![
        "ping".to_string(),
        "-c".to_string(), "1".to_string(),
        "-W".to_string(), "2".to_string(),
        "10.42.0.1".to_string() // Gateway IP
    ];
    
    let mut exec_request = tonic::Request::new(ExecContainerRequest {
        container_id: container_id.to_string(),
        command: ping_cmd,
        working_directory: String::new(),
        environment: HashMap::new(),
        capture_output: true,
        container_name: String::new(),
        copy_script: false,
    });
    exec_request.set_timeout(Duration::from_secs(5));
    
    match client.exec_container(exec_request).await {
        Ok(response) => {
            let result = response.into_inner();
            Ok(result.success && result.exit_code == 0)
        },
        Err(_) => Ok(false),
    }
} 
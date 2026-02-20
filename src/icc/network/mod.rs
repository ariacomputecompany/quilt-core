// Network management module - Browserbase production networking stack
// Netlink-based operations, nftables firewall, tc bandwidth limiting, conntrack management.

pub mod bandwidth;
pub mod bridge;
pub mod conntrack;
pub mod dns_manager;
pub mod error;
pub mod firewall;
pub mod netlink;
pub mod port_mapping;
pub mod security;
pub mod veth;

// Keep egress_tracking for DB operations (store_egress_snapshot, aggregate_tenant_egress)
pub mod egress_tracking;

use crate::icc::network::bandwidth::BandwidthLimiter;
use crate::icc::network::bridge::BridgeManager;
use crate::icc::network::conntrack::ConntrackManager;
use crate::icc::network::error::{NetworkError, NetworkResult};
use crate::icc::network::firewall::Firewall;
use crate::icc::network::netlink::NetlinkHandle;
use crate::icc::network::veth::{SessionNetworkHandle, VethManager};
use crate::utils::console::ConsoleLogger;
use dashmap::DashMap;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

// Re-export commonly used types
pub use dns_manager::DnsManager;
pub use egress_tracking::EgressTracker;
pub use security::NetworkSecurity;
pub use veth::ContainerNetworkConfig;

/// Network configuration for the container networking system
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct NetworkConfig {
    pub bridge_name: String,
    pub subnet_cidr: String,
    pub bridge_ip: String,
    pub dns_port: u16,
    pub next_ip: Arc<AtomicU32>,
}

/// Default bandwidth limit per session (0 = unlimited)
const DEFAULT_EGRESS_LIMIT_MBIT: u32 = 0;
const DEFAULT_BURST_KB: u32 = 256;

/// Main NetworkManager that orchestrates all networking components.
/// Async-first, netlink-based, with immediate teardown and reconciliation.
pub struct NetworkManager {
    pub config: NetworkConfig,
    nl: Arc<NetlinkHandle>,
    bridge_manager: BridgeManager,
    veth_manager: VethManager,
    pub dns_manager: DnsManager,
    firewall: Firewall,
    security: NetworkSecurity,
    /// Active session handles indexed by container_id for immediate teardown
    session_handles: DashMap<String, SessionNetworkHandle>,
    /// Cached bridge ifindex
    bridge_index: AtomicU32,
    /// Egress bandwidth limit in mbit (0 = unlimited)
    egress_limit_mbit: u32,
}

/// Default network configuration values
const DEFAULT_BRIDGE_NAME: &str = "quilt0";
/// Default node PodCIDR for single-node deployments.
/// Multi-node deployments should set a non-overlapping per-node PodCIDR (commonly /24).
const DEFAULT_NODE_POD_CIDR: &str = "10.42.0.0/16";
const DEFAULT_DNS_PORT: u16 = 1053;

#[allow(dead_code)]
impl NetworkManager {
    /// Create a NetworkManager from environment variables, falling back to defaults.
    ///
    /// Environment variables:
    /// - `QUILT_NODE_BRIDGE_NAME`: Pod bridge interface name (default: quilt0)
    /// - `QUILT_NODE_POD_CIDR`: Node PodCIDR (default: 10.42.0.0/16)
    /// - `QUILT_NODE_DNS_PORT`: Internal DNS server port (default: 1053)
    /// - `QUILT_NODE_EGRESS_LIMIT_MBIT`: Per-session egress limit, 0=unlimited (default: 0)
    pub fn from_env() -> Result<Self, String> {
        let bridge_name = std::env::var("QUILT_NODE_BRIDGE_NAME")
            .unwrap_or_else(|_| DEFAULT_BRIDGE_NAME.to_string());
        let pod_cidr = std::env::var("QUILT_NODE_POD_CIDR")
            .unwrap_or_else(|_| DEFAULT_NODE_POD_CIDR.to_string());
        let (bridge_ip, prefix_len) = derive_bridge_ip_and_prefix(&pod_cidr)?;

        let dns_port: u16 = std::env::var("QUILT_NODE_DNS_PORT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_DNS_PORT);
        let egress_limit: u32 = std::env::var("QUILT_NODE_EGRESS_LIMIT_MBIT")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_EGRESS_LIMIT_MBIT);

        Self::new_with_config(
            &bridge_name,
            &pod_cidr,
            &bridge_ip,
            prefix_len,
            dns_port,
            egress_limit,
        )
    }

    pub fn new(bridge_name: &str, subnet_cidr: &str) -> Result<Self, String> {
        let (bridge_ip, prefix_len) = derive_bridge_ip_and_prefix(subnet_cidr)?;
        Self::new_with_config(
            bridge_name,
            subnet_cidr,
            &bridge_ip,
            prefix_len,
            DEFAULT_DNS_PORT,
            DEFAULT_EGRESS_LIMIT_MBIT,
        )
    }

    fn new_with_config(
        bridge_name: &str,
        subnet_cidr: &str,
        bridge_ip: &str,
        bridge_prefix_len: u8,
        dns_port: u16,
        egress_limit_mbit: u32,
    ) -> Result<Self, String> {
        let nl = NetlinkHandle::new().map_err(|e| format!("netlink init failed: {}", e))?;
        let nl = Arc::new(nl);

        let bridge_ip_addr: Ipv4Addr = bridge_ip
            .parse()
            .map_err(|e| format!("invalid bridge IP '{}': {}", bridge_ip, e))?;

        let config = NetworkConfig {
            bridge_name: bridge_name.to_string(),
            subnet_cidr: subnet_cidr.to_string(),
            bridge_ip: bridge_ip.to_string(),
            dns_port,
            next_ip: Arc::new(AtomicU32::new(2)),
        };

        let bridge_manager = BridgeManager::new(
            bridge_name.to_string(),
            bridge_ip.to_string(),
            bridge_prefix_len,
        );
        let veth_manager = VethManager::new(bridge_name.to_string());
        let dns_manager = DnsManager::new(bridge_name.to_string(), bridge_ip.to_string());
        let firewall = Firewall::new(bridge_name, bridge_ip_addr, subnet_cidr, dns_port);
        let security = NetworkSecurity::new(bridge_ip.to_string());

        Ok(Self {
            config,
            nl,
            bridge_manager,
            veth_manager,
            dns_manager,
            firewall,
            security,
            session_handles: DashMap::new(),
            bridge_index: AtomicU32::new(0),
            egress_limit_mbit,
        })
    }

    pub fn dns_port(&self) -> u16 {
        self.config.dns_port
    }

    /// Initialize the network stack: bridge + firewall + conntrack tuning.
    /// Call once at startup.
    pub async fn initialize(&self) -> Result<(), String> {
        // 1. Enable IP forwarding
        BridgeManager::setup_ip_forwarding().map_err(|e| format!("ip forwarding failed: {}", e))?;

        // 2. Create and configure bridge via netlink (~1-2ms)
        let idx = self
            .bridge_manager
            .ensure_bridge_ready(&self.nl)
            .await
            .map_err(|e| format!("bridge setup failed: {}", e))?;
        self.bridge_index.store(idx, Ordering::Relaxed);

        // 3. Initialize firewall (nftables or iptables fallback)
        self.firewall
            .initialize()
            .await
            .map_err(|e| format!("firewall init failed: {}", e))?;

        // 4. Tune conntrack table for high-churn workload
        // 256K entries supports ~2000 concurrent sessions with ~128 connections each
        if let Err(e) = ConntrackManager::set_max(262144) {
            tracing::warn!("Failed to tune conntrack (non-fatal): {}", e);
        }

        ConsoleLogger::success("Network stack initialized (netlink + nftables)");
        Ok(())
    }

    /// Ensure bridge is ready (backwards-compatible sync wrapper)
    pub fn ensure_bridge_ready(&self) -> Result<(), String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.bridge_manager
                    .ensure_bridge_ready(&self.nl)
                    .await
                    .map_err(|e| format!("bridge setup failed: {}", e))?;
                Ok(())
            })
        })
    }

    /// Check if bridge exists
    pub fn bridge_exists(&self) -> bool {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { self.bridge_manager.bridge_exists(&self.nl).await })
        })
    }

    /// Get the bridge IP address
    pub fn get_bridge_ip(&self) -> &str {
        &self.config.bridge_ip
    }

    // ── Session network setup (<10ms target) ──────────────────────────

    /// Setup complete network for a container session. Target: <10ms.
    ///
    /// Steps:
    /// 1. Validate inputs (pure Rust, <1us)
    /// 2. Create veth pair + configure in namespace (~3-5ms netlink)
    /// 3. Add session to firewall allow set (~1-2ms nft)
    /// 4. Apply bandwidth limit (~1-2ms tc)
    /// 5. Write resolv.conf via rootfs path (<1ms file write)
    ///
    /// Returns SessionNetworkHandle for instant teardown.
    pub async fn setup_session_network(
        &self,
        config: &ContainerNetworkConfig,
        container_pid: i32,
    ) -> NetworkResult<SessionNetworkHandle> {
        let pid = container_pid as u32;

        // Validate inputs
        self.security
            .validate_container_id(&config.container_id)
            .map_err(NetworkError::Validation)?;
        self.security
            .validate_ip_address(&config.ip_address)
            .map_err(NetworkError::Validation)?;

        // Get bridge index (cached after first call)
        let bridge_idx = self.get_bridge_index().await?;

        // Setup veth pair with automatic rollback on failure
        let handle = self
            .veth_manager
            .setup_container_veth(&self.nl, bridge_idx, config, pid)
            .await?;

        // Add to firewall allow set
        if let Err(e) = self.firewall.add_session(handle.container_ip).await {
            tracing::warn!("Firewall add_session failed (non-fatal): {}", e);
        }

        // Apply bandwidth limit (if configured)
        if self.egress_limit_mbit > 0 {
            if let Err(e) = BandwidthLimiter::set_egress_limit(
                &handle.host_veth_name,
                self.egress_limit_mbit,
                DEFAULT_BURST_KB,
            )
            .await
            {
                tracing::warn!("Bandwidth limit failed (non-fatal): {}", e);
            }
        }

        // Write resolv.conf directly via rootfs path (no nsenter needed)
        if let Some(rootfs) = &config.rootfs_path {
            self.write_resolv_conf(rootfs);
        }

        // Store handle for teardown
        self.session_handles
            .insert(config.container_id.clone(), handle.clone());

        tracing::info!(
            "Session network ready for container {} at {}",
            config.container_id,
            handle.container_ip
        );

        Ok(handle)
    }

    /// Synchronous wrapper for setup_container_network (backwards compatibility)
    pub fn setup_container_network(
        &self,
        config: &ContainerNetworkConfig,
        container_pid: i32,
    ) -> Result<(), String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.setup_session_network(config, container_pid)
                    .await
                    .map_err(|e| e.to_string())?;
                Ok(())
            })
        })
    }

    /// Synchronous wrapper with verbose flag (backwards compatibility)
    pub fn setup_container_network_with_options(
        &self,
        config: &ContainerNetworkConfig,
        container_pid: i32,
        _verbose: bool,
    ) -> Result<(), String> {
        self.setup_container_network(config, container_pid)
    }

    // ── Session teardown (<3ms target) ────────────────────────────────

    /// Teardown a container's network. Target: <3ms.
    ///
    /// Steps:
    /// 1. Flush conntrack entries for this IP
    /// 2. Remove from firewall allow set
    /// 3. Read final egress bytes (for billing)
    /// 4. Delete veth pair (single netlink call)
    /// 5. Verify cleanup
    pub async fn teardown_session_network(&self, container_id: &str) -> NetworkResult<Option<u64>> {
        let handle = self.session_handles.remove(container_id);

        let (host_veth_name, container_ip) = match handle {
            Some((_, h)) => (h.host_veth_name, h.container_ip),
            None => {
                // No handle - try cleanup by naming convention
                let prefix = &container_id[..8.min(container_id.len())];
                let host_name = format!("veth-{}", prefix);
                // Try to parse IP from config (best effort)
                (host_name, Ipv4Addr::new(0, 0, 0, 0))
            }
        };

        // 1. Flush conntrack entries
        let _ = ConntrackManager::flush_for_ip(container_ip).await;

        // 2. Remove from firewall
        if container_ip != Ipv4Addr::new(0, 0, 0, 0) {
            let _ = self.firewall.remove_session(container_ip).await;
        }

        // 3. Read final egress bytes
        let egress_bytes = BandwidthLimiter::read_egress_bytes(&host_veth_name)
            .await
            .unwrap_or(0);

        // 4. Delete veth pair
        self.veth_manager
            .teardown_veth(&self.nl, &host_veth_name)
            .await?;

        tracing::info!(
            "Session network torn down for container {} (egress={} bytes)",
            container_id,
            egress_bytes
        );

        Ok(Some(egress_bytes))
    }

    /// Synchronous teardown wrapper
    pub fn teardown_session_network_sync(&self, container_id: &str) -> Result<Option<u64>, String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                self.teardown_session_network(container_id)
                    .await
                    .map_err(|e| e.to_string())
            })
        })
    }

    // ── Reconciliation ────────────────────────────────────────────────

    /// Scan kernel for orphaned resources and clean them up.
    /// Called at startup and periodically (60s interval).
    pub async fn reconcile_kernel_state(
        &self,
        known_container_ids: &[String],
    ) -> NetworkResult<Vec<String>> {
        let mut cleaned = Vec::new();

        // 1. Find orphaned veth interfaces
        let all_veths = self.nl.list_links_with_prefix("veth-").await?;
        for veth_name in &all_veths {
            // Extract container ID prefix from veth name
            let prefix = veth_name.trim_start_matches("veth-");
            let is_known = known_container_ids.iter().any(|id| id.starts_with(prefix));

            if !is_known {
                tracing::warn!("Orphaned veth detected: {}", veth_name);
                if let Err(e) = self.nl.delete_link(veth_name).await {
                    tracing::warn!("Failed to cleanup orphaned veth {}: {}", veth_name, e);
                } else {
                    cleaned.push(format!("orphaned veth: {}", veth_name));
                }
            }
        }

        // 2. Reconcile firewall session set
        if let Ok(fw_sessions) = self.firewall.list_sessions().await {
            for fw_ip in fw_sessions {
                let ip_str = fw_ip.to_string();
                let is_known = self
                    .session_handles
                    .iter()
                    .any(|entry| entry.value().container_ip == fw_ip);

                if !is_known {
                    tracing::warn!("Orphaned firewall session: {}", ip_str);
                    let _ = self.firewall.remove_session(fw_ip).await;
                    cleaned.push(format!("orphaned firewall session: {}", ip_str));
                }
            }
        }

        if !cleaned.is_empty() {
            tracing::info!("Reconciliation cleaned {} resources", cleaned.len());
        }

        Ok(cleaned)
    }

    // ── Firewall delegation ────────────────────────────────────────────

    /// Add a session IP to the firewall allow set (for reconciliation)
    pub async fn add_firewall_session(&self, ip: std::net::Ipv4Addr) -> Result<(), String> {
        self.firewall
            .add_session(ip)
            .await
            .map_err(|e| e.to_string())
    }

    // ── DNS delegation ────────────────────────────────────────────────

    pub fn is_dns_server_running(&self) -> bool {
        self.dns_manager.is_running()
    }

    pub async fn start_dns_server(&mut self) -> Result<(), String> {
        self.dns_manager.start_dns_server().await
    }

    pub fn register_container_dns(
        &self,
        container_id: &str,
        container_name: &str,
        ip_address: &str,
    ) -> Result<(), String> {
        self.dns_manager
            .register_container_dns(container_id, container_name, ip_address)
    }

    pub fn unregister_container_dns(&self, container_id: &str) -> Result<(), String> {
        self.dns_manager.unregister_container_dns(container_id)
    }

    pub fn list_dns_entries(&self) -> Result<Vec<crate::icc::dns::DnsEntry>, String> {
        self.dns_manager.list_dns_entries()
    }

    pub fn rename_dns_entry(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<crate::icc::dns::DnsEntry, String> {
        self.dns_manager.rename_dns_entry(old_name, new_name)
    }

    pub fn get_dns_entry(&self, name: &str) -> Result<Option<crate::icc::dns::DnsEntry>, String> {
        self.dns_manager.get_dns_entry(name)
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    pub fn cleanup_all_resources(&self) -> Result<(), String> {
        tracing::info!("Starting comprehensive network cleanup");
        self.dns_manager.cleanup_dns_rules()?;
        // Cleanup firewall (async from sync context)
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                let _ = self.firewall.cleanup().await;
            })
        });
        tracing::info!("Network cleanup completed");
        Ok(())
    }

    #[allow(clippy::type_complexity)]
    pub fn comprehensive_cleanup(
        &self,
        dry_run: bool,
    ) -> Result<(Vec<String>, Vec<String>, Vec<String>), String> {
        let mut cleaned_allocations = Vec::new();
        let mut removed_interfaces = Vec::new();
        let mut errors = Vec::new();

        if !dry_run {
            if let Err(e) = self.dns_manager.cleanup_dns_rules() {
                errors.push(format!("DNS cleanup failed: {}", e));
            } else {
                cleaned_allocations.push("DNS rules cleaned".to_string());
            }

            // Cleanup all tracked sessions
            let container_ids: Vec<String> = self
                .session_handles
                .iter()
                .map(|entry| entry.key().clone())
                .collect();

            for container_id in container_ids {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(async {
                        match self.teardown_session_network(&container_id).await {
                            Ok(_) => {
                                removed_interfaces
                                    .push(format!("Cleaned network for {}", container_id));
                            }
                            Err(e) => {
                                errors.push(format!("Cleanup failed for {}: {}", container_id, e));
                            }
                        }
                    })
                });
            }
        }

        Ok((cleaned_allocations, removed_interfaces, errors))
    }

    /// Add a host-level route (agent-only): `ip route add <destination> dev <via_interface>`
    pub async fn inject_route(&self, destination: &str, via_interface: &str) -> Result<(), String> {
        let (ip, prefix_len) = parse_cidr(destination)?;
        self.nl
            .add_route_via_device(ip, prefix_len, via_interface)
            .await
            .map_err(|e| format!("inject route failed: {}", e))?;
        tracing::info!("Route injected: {} dev {}", destination, via_interface);
        Ok(())
    }

    /// Remove a host-level route (agent-only): `ip route del <destination>`
    pub async fn remove_route(&self, destination: &str) -> Result<(), String> {
        let (ip, prefix_len) = parse_cidr(destination)?;
        self.nl
            .delete_route(ip, prefix_len)
            .await
            .map_err(|e| format!("remove route failed: {}", e))?;
        tracing::info!("Route removed: {}", destination);
        Ok(())
    }

    // ── Container netns route operations (tenant-safe) ─────────────────

    /// Add a route inside a container's network namespace.
    ///
    /// This is explicitly NOT a host-routing mutation. It only affects the target container.
    /// Requires the container to be running so we can setns() using /proc/<pid>/ns/net.
    pub async fn inject_container_route(
        &self,
        container_pid: u32,
        destination: &str,
    ) -> Result<(), String> {
        let (ip, prefix_len) = parse_cidr(destination)?;
        if prefix_len == 0 {
            return Err("refusing to add /0 route inside container network namespace".to_string());
        }
        let gw: Ipv4Addr = self
            .config
            .bridge_ip
            .parse()
            .map_err(|e| format!("invalid bridge IP '{}': {}", self.config.bridge_ip, e))?;

        self.nl
            .add_route_in_netns(container_pid, ip, prefix_len, gw)
            .await
            .map_err(|e| format!("inject container route failed: {}", e))?;

        tracing::info!(
            "Container netns route injected (pid={}): {} via {}",
            container_pid,
            destination,
            gw
        );
        Ok(())
    }

    /// Remove a route inside a container's network namespace.
    ///
    /// This is explicitly NOT a host-routing mutation. It only affects the target container.
    pub async fn remove_container_route(
        &self,
        container_pid: u32,
        destination: &str,
    ) -> Result<(), String> {
        let (ip, prefix_len) = parse_cidr(destination)?;
        self.nl
            .delete_route_in_netns(container_pid, ip, prefix_len)
            .await
            .map_err(|e| format!("remove container route failed: {}", e))?;

        tracing::info!(
            "Container netns route removed (pid={}): {}",
            container_pid,
            destination
        );
        Ok(())
    }

    // ── Helpers ───────────────────────────────────────────────────────

    async fn get_bridge_index(&self) -> NetworkResult<u32> {
        let cached = self.bridge_index.load(Ordering::Relaxed);
        if cached > 0 {
            return Ok(cached);
        }
        let idx = self.bridge_manager.get_bridge_index(&self.nl).await?;
        self.bridge_index.store(idx, Ordering::Relaxed);
        Ok(idx)
    }

    fn write_resolv_conf(&self, rootfs_path: &str) {
        // SECURITY: Validate rootfs path is within expected container directory
        if !rootfs_path.starts_with(crate::utils::constants::CONTAINER_BASE_DIR) {
            tracing::error!(
                "SECURITY: Refusing to write resolv.conf — rootfs path outside container dir: {}",
                rootfs_path
            );
            return;
        }
        if rootfs_path.contains("../") || rootfs_path.contains("/..") {
            tracing::error!(
                "SECURITY: Refusing to write resolv.conf — path traversal detected: {}",
                rootfs_path
            );
            return;
        }

        // Use public DNS as primary for reliable external resolution.
        // Internal DNS (bridge IP) is listed for container-to-container resolution.
        let dns_content = format!(
            "nameserver 8.8.8.8\nnameserver 8.8.4.4\nnameserver {}\nsearch quilt.local\n",
            self.config.bridge_ip
        );
        let etc_path = format!("{}/etc", rootfs_path);
        let _ = std::fs::create_dir_all(&etc_path);
        let resolv_path = format!("{}/etc/resolv.conf", rootfs_path);

        // CRITICAL: Remove any existing file or symlink first.
        // Base images (e.g. Alpine) may ship /etc/resolv.conf as a symlink
        // to /run/systemd/resolve/stub-resolv.conf. If we don't remove the
        // symlink before writing, std::fs::write follows the symlink and
        // would overwrite the HOST's resolv.conf — a security breach.
        match std::fs::symlink_metadata(&resolv_path) {
            Ok(meta) if meta.file_type().is_symlink() => {
                tracing::warn!(
                    "Removing resolv.conf symlink in container rootfs (prevents host write): {}",
                    resolv_path
                );
                if let Err(e) = std::fs::remove_file(&resolv_path) {
                    tracing::error!("Failed to remove resolv.conf symlink: {}", e);
                    return;
                }
            }
            Ok(_) => {
                // Regular file — safe to overwrite
                let _ = std::fs::remove_file(&resolv_path);
            }
            Err(_) => {
                // File doesn't exist — fine, we'll create it
            }
        }

        if let Err(e) = std::fs::write(&resolv_path, &dns_content) {
            tracing::warn!("Failed to write resolv.conf at {}: {}", resolv_path, e);
            return;
        }

        // SECURITY: Post-write verification — ensure host resolv.conf was NOT modified
        if let Ok(host_resolv) = std::fs::read_to_string("/etc/resolv.conf") {
            if host_resolv.contains(&self.config.bridge_ip) {
                tracing::error!(
                    "SECURITY BREACH: Host /etc/resolv.conf was modified by container DNS write! Bridge IP {} found in host file.",
                    self.config.bridge_ip
                );
            }
        }
    }

    // ── Backwards compatibility for bridge_manager field access ──────

    pub fn bridge_manager_verify_bridge_up(&self) -> Result<(), String> {
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current()
                .block_on(async { self.bridge_manager.verify_bridge_up(&self.nl).await })
        })
    }

    /// Security validation
    pub fn validate_container_namespace(&self, container_pid: i32) -> bool {
        self.security.validate_container_namespace(container_pid)
    }

    /// Get the netlink handle (for direct operations by other modules)
    pub fn netlink_handle(&self) -> &Arc<NetlinkHandle> {
        &self.nl
    }
}

fn derive_bridge_ip_and_prefix(pod_cidr: &str) -> Result<(String, u8), String> {
    // Parse CIDR like "10.42.7.0/24"
    let parts: Vec<&str> = pod_cidr.split('/').collect();
    if parts.len() != 2 {
        return Err(format!("invalid PodCIDR: {}", pod_cidr));
    }
    let base: std::net::Ipv4Addr = parts[0]
        .parse()
        .map_err(|e| format!("invalid IP in PodCIDR: {}", e))?;
    let prefix_len: u8 = parts[1]
        .parse()
        .map_err(|_| format!("invalid prefix length: {}", parts[1]))?;
    if !(16..=30).contains(&prefix_len) {
        return Err(format!(
            "invalid PodCIDR prefix /{} (expected /16 to /30)",
            prefix_len
        ));
    }

    // We require the CIDR base to be the network address for sanity.
    // (e.g., 10.42.7.0/24, not 10.42.7.5/24)
    let base_u32 = u32::from(base);
    let mask: u32 = if prefix_len == 0 {
        0
    } else {
        (!0u32) << (32 - prefix_len)
    };
    if (base_u32 & mask) != base_u32 {
        return Err(format!(
            "PodCIDR must be a network base address, got {}",
            pod_cidr
        ));
    }

    // Bridge IP = network base + 1 (gateway)
    let bridge_u32 = base_u32
        .checked_add(1)
        .ok_or_else(|| format!("PodCIDR base overflow: {}", pod_cidr))?;
    let bridge_ip = std::net::Ipv4Addr::from(bridge_u32).to_string();

    Ok((bridge_ip, prefix_len))
}

/// Parse a CIDR string like "10.42.2.0/24" into (Ipv4Addr, prefix_len).
fn parse_cidr(cidr: &str) -> Result<(Ipv4Addr, u8), String> {
    let parts: Vec<&str> = cidr.split('/').collect();
    if parts.len() != 2 {
        return Err(format!("invalid CIDR: {}", cidr));
    }
    let ip: Ipv4Addr = parts[0]
        .parse()
        .map_err(|e| format!("invalid IP in CIDR: {}", e))?;
    let prefix_len: u8 = parts[1]
        .parse()
        .map_err(|_| format!("invalid prefix length: {}", parts[1]))?;
    Ok((ip, prefix_len))
}

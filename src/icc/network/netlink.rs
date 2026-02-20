use crate::icc::network::error::{NetworkError, NetworkResult};
use futures::TryStreamExt;
use netlink_packet_route::link::nlas::Nla as LinkNla;
use std::net::Ipv4Addr;
use std::os::unix::io::AsRawFd;

/// Persistent netlink handle wrapping rtnetlink for all bridge/veth/link operations.
/// One handle per process, reused across all container network setup/teardown.
pub struct NetlinkHandle {
    handle: rtnetlink::Handle,
    // Keep the connection task alive
    _conn_task: tokio::task::JoinHandle<()>,
}

#[allow(dead_code)]
impl NetlinkHandle {
    /// Create a new netlink handle with a persistent connection
    pub fn new() -> NetworkResult<Self> {
        let (conn, handle, _) = rtnetlink::new_connection()?;
        let conn_task = tokio::spawn(conn);
        Ok(Self {
            handle,
            _conn_task: conn_task,
        })
    }

    // ── Bridge operations ─────────────────────────────────────────────

    /// Create a bridge interface. Idempotent - returns Ok if bridge already exists.
    pub async fn create_bridge(&self, name: &str) -> NetworkResult<u32> {
        // Check if bridge already exists
        if let Ok(idx) = self.get_link_index(name).await {
            return Ok(idx);
        }

        self.handle
            .link()
            .add()
            .bridge(name.to_string())
            .execute()
            .await
            .map_err(|e| {
                // If it already exists (race condition), that's fine
                if e.to_string().contains("File exists") {
                    return NetworkError::AlreadyExists(name.to_string());
                }
                NetworkError::Netlink(e)
            })?;

        self.get_link_index(name).await
    }

    /// Add an IPv4 address with prefix to a link
    pub async fn add_address(
        &self,
        link_index: u32,
        addr: Ipv4Addr,
        prefix_len: u8,
    ) -> NetworkResult<()> {
        let result = self
            .handle
            .address()
            .add(link_index, std::net::IpAddr::V4(addr), prefix_len)
            .execute()
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().contains("File exists") => Ok(()), // already assigned
            Err(e) => Err(NetworkError::Netlink(e)),
        }
    }

    /// Set a link UP by index
    pub async fn set_link_up(&self, index: u32) -> NetworkResult<()> {
        self.handle
            .link()
            .set(index)
            .up()
            .execute()
            .await
            .map_err(NetworkError::Netlink)
    }

    /// Get a link's ifindex by name. Returns NotFound if the link doesn't exist.
    pub async fn get_link_index(&self, name: &str) -> NetworkResult<u32> {
        let mut links = self
            .handle
            .link()
            .get()
            .match_name(name.to_string())
            .execute();
        match links.try_next().await {
            Ok(Some(msg)) => Ok(msg.header.index),
            Ok(None) => Err(NetworkError::NotFound(format!("link {}", name))),
            Err(e) => {
                // rtnetlink returns an error for "not found" on some kernels
                if e.to_string().contains("No such device") {
                    Err(NetworkError::NotFound(format!("link {}", name)))
                } else {
                    Err(NetworkError::Netlink(e))
                }
            }
        }
    }

    /// Check if a link exists
    pub async fn link_exists(&self, name: &str) -> bool {
        self.get_link_index(name).await.is_ok()
    }

    /// Delete a link by name. Idempotent - returns Ok if link doesn't exist.
    pub async fn delete_link(&self, name: &str) -> NetworkResult<()> {
        let index = match self.get_link_index(name).await {
            Ok(idx) => idx,
            Err(NetworkError::NotFound(_)) => return Ok(()), // already gone
            Err(e) => return Err(e),
        };

        match self.handle.link().del(index).execute().await {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().contains("No such device") => Ok(()),
            Err(e) => Err(NetworkError::Netlink(e)),
        }
    }

    // ── Veth operations ───────────────────────────────────────────────

    /// Create a veth pair. Returns (host_index, peer_index).
    pub async fn create_veth_pair(
        &self,
        host_name: &str,
        peer_name: &str,
    ) -> NetworkResult<(u32, u32)> {
        // Clean up stale interfaces first (idempotent)
        self.delete_link(host_name).await?;
        self.delete_link(peer_name).await?;

        self.handle
            .link()
            .add()
            .veth(host_name.to_string(), peer_name.to_string())
            .execute()
            .await
            .map_err(NetworkError::Netlink)?;

        let host_idx = self.get_link_index(host_name).await?;
        let peer_idx = self.get_link_index(peer_name).await?;

        Ok((host_idx, peer_idx))
    }

    /// Set a link's master (attach to bridge)
    pub async fn set_link_master(&self, link_index: u32, master_index: u32) -> NetworkResult<()> {
        self.handle
            .link()
            .set(link_index)
            .master(master_index)
            .execute()
            .await
            .map_err(NetworkError::Netlink)
    }

    /// Move a link to a network namespace by PID
    pub async fn set_link_netns_by_pid(&self, link_index: u32, pid: u32) -> NetworkResult<()> {
        self.handle
            .link()
            .set(link_index)
            .setns_by_pid(pid)
            .execute()
            .await
            .map_err(NetworkError::Netlink)
    }

    // ── In-namespace operations ───────────────────────────────────────
    // setns() affects the calling OS thread. We MUST use std::thread::spawn,
    // never a tokio task, for in-namespace netlink operations.

    /// Configure network interfaces inside a container's network namespace.
    /// Spawns a dedicated OS thread that calls setns(), performs netlink ops, then exits.
    pub async fn configure_in_netns(
        &self,
        container_pid: u32,
        peer_name: String,
        rename_to: String,
        addr: Ipv4Addr,
        prefix_len: u8,
        gateway: Ipv4Addr,
    ) -> NetworkResult<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        std::thread::spawn(move || {
            let result = Self::do_configure_in_netns(
                container_pid,
                &peer_name,
                &rename_to,
                addr,
                prefix_len,
                gateway,
            );
            let _ = tx.send(result);
        });

        rx.await
            .map_err(|_| NetworkError::Namespace("in-namespace thread panicked".to_string()))?
    }

    /// Actual in-namespace configuration (runs on a dedicated OS thread)
    fn do_configure_in_netns(
        container_pid: u32,
        peer_name: &str,
        rename_to: &str,
        addr: Ipv4Addr,
        prefix_len: u8,
        gateway: Ipv4Addr,
    ) -> NetworkResult<()> {
        use nix::sched::{setns, CloneFlags};

        // Open the container's network namespace fd
        let ns_path = format!("/proc/{}/ns/net", container_pid);
        let ns_fd = std::fs::File::open(&ns_path)
            .map_err(|e| NetworkError::Namespace(format!("failed to open {}: {}", ns_path, e)))?;

        // Enter the namespace
        setns(ns_fd.as_raw_fd(), CloneFlags::CLONE_NEWNET).map_err(|e| {
            NetworkError::Namespace(format!("setns failed for pid {}: {}", container_pid, e))
        })?;

        // Now we're in the container's network namespace.
        // Create a new netlink connection in THIS namespace.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(NetworkError::Io)?;

        rt.block_on(async {
            let (conn, handle, _) = rtnetlink::new_connection()?;
            tokio::spawn(conn);

            // 1. Get peer interface index
            let mut links = handle
                .link()
                .get()
                .match_name(peer_name.to_string())
                .execute();
            let peer_msg = links.try_next().await?.ok_or_else(|| {
                NetworkError::NotFound(format!("peer {} in container netns", peer_name))
            })?;
            let peer_idx = peer_msg.header.index;

            // 2. Rename the interface
            handle
                .link()
                .set(peer_idx)
                .name(rename_to.to_string())
                .execute()
                .await
                .map_err(NetworkError::Netlink)?;

            // 3. Add IP address
            let result = handle
                .address()
                .add(peer_idx, std::net::IpAddr::V4(addr), prefix_len)
                .execute()
                .await;
            match result {
                Ok(()) => {}
                Err(e) if e.to_string().contains("File exists") => {} // already assigned
                Err(e) => return Err(NetworkError::Netlink(e)),
            }

            // 4. Bring interface up
            handle
                .link()
                .set(peer_idx)
                .up()
                .execute()
                .await
                .map_err(NetworkError::Netlink)?;

            // 5. Bring loopback up
            if let Ok(Some(lo_msg)) = handle
                .link()
                .get()
                .match_name("lo".to_string())
                .execute()
                .try_next()
                .await
            {
                let _ = handle.link().set(lo_msg.header.index).up().execute().await;
            }

            // 6. Add default route via gateway
            let route_result = handle.route().add().v4().gateway(gateway).execute().await;
            match route_result {
                Ok(()) => {}
                Err(e) if e.to_string().contains("File exists") => {} // already has route
                Err(e) => return Err(NetworkError::Netlink(e)),
            }

            Ok(())
        })
    }

    /// Add a route inside a container's network namespace.
    ///
    /// This does NOT touch host routing tables: it setns() into the container netns and then
    /// performs a route add using a netlink connection created inside that namespace.
    pub async fn add_route_in_netns(
        &self,
        container_pid: u32,
        destination: Ipv4Addr,
        prefix_len: u8,
        gateway: Ipv4Addr,
    ) -> NetworkResult<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        std::thread::spawn(move || {
            let result =
                Self::do_add_route_in_netns(container_pid, destination, prefix_len, gateway);
            let _ = tx.send(result);
        });

        rx.await
            .map_err(|_| NetworkError::Namespace("in-namespace thread panicked".to_string()))?
    }

    fn do_add_route_in_netns(
        container_pid: u32,
        destination: Ipv4Addr,
        prefix_len: u8,
        gateway: Ipv4Addr,
    ) -> NetworkResult<()> {
        use nix::sched::{setns, CloneFlags};

        let ns_path = format!("/proc/{}/ns/net", container_pid);
        let ns_fd = std::fs::File::open(&ns_path)
            .map_err(|e| NetworkError::Namespace(format!("failed to open {}: {}", ns_path, e)))?;

        setns(ns_fd.as_raw_fd(), CloneFlags::CLONE_NEWNET).map_err(|e| {
            NetworkError::Namespace(format!("setns failed for pid {}: {}", container_pid, e))
        })?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(NetworkError::Io)?;

        rt.block_on(async {
            let (conn, handle, _) = rtnetlink::new_connection()?;
            tokio::spawn(conn);

            let result = handle
                .route()
                .add()
                .v4()
                .destination_prefix(destination, prefix_len)
                .gateway(gateway)
                .execute()
                .await;

            match result {
                Ok(()) => Ok(()),
                Err(e) if e.to_string().contains("File exists") => Ok(()), // idempotent
                Err(e) => Err(NetworkError::Netlink(e)),
            }
        })
    }

    /// Delete a route inside a container's network namespace.
    ///
    /// This does NOT touch host routing tables: it setns() into the container netns and then
    /// performs a route delete using a netlink connection created inside that namespace.
    pub async fn delete_route_in_netns(
        &self,
        container_pid: u32,
        destination: Ipv4Addr,
        prefix_len: u8,
    ) -> NetworkResult<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();

        std::thread::spawn(move || {
            let result = Self::do_delete_route_in_netns(container_pid, destination, prefix_len);
            let _ = tx.send(result);
        });

        rx.await
            .map_err(|_| NetworkError::Namespace("in-namespace thread panicked".to_string()))?
    }

    fn do_delete_route_in_netns(
        container_pid: u32,
        destination: Ipv4Addr,
        prefix_len: u8,
    ) -> NetworkResult<()> {
        use netlink_packet_route::route::nlas::Nla as RouteNla;
        use nix::sched::{setns, CloneFlags};

        let ns_path = format!("/proc/{}/ns/net", container_pid);
        let ns_fd = std::fs::File::open(&ns_path)
            .map_err(|e| NetworkError::Namespace(format!("failed to open {}: {}", ns_path, e)))?;

        setns(ns_fd.as_raw_fd(), CloneFlags::CLONE_NEWNET).map_err(|e| {
            NetworkError::Namespace(format!("setns failed for pid {}: {}", container_pid, e))
        })?;

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(NetworkError::Io)?;

        rt.block_on(async {
            let (conn, handle, _) = rtnetlink::new_connection()?;
            tokio::spawn(conn);

            let mut routes = handle.route().get(rtnetlink::IpVersion::V4).execute();
            while let Some(route) = routes.try_next().await.map_err(NetworkError::Netlink)? {
                let dest_len = route.header.destination_prefix_length;
                if dest_len != prefix_len {
                    continue;
                }

                let mut route_dest = None;
                for nla in &route.nlas {
                    if let RouteNla::Destination(addr_bytes) = nla {
                        if addr_bytes.len() == 4 {
                            route_dest = Some(Ipv4Addr::new(
                                addr_bytes[0],
                                addr_bytes[1],
                                addr_bytes[2],
                                addr_bytes[3],
                            ));
                        }
                    }
                }

                if route_dest == Some(destination) {
                    let result = handle.route().del(route).execute().await;
                    return match result {
                        Ok(()) => Ok(()),
                        Err(e) if e.to_string().contains("No such process") => Ok(()), // idempotent
                        Err(e) => Err(NetworkError::Netlink(e)),
                    };
                }
            }

            Ok(())
        })
    }

    // ── Host-level route operations (multi-node overlay) ────────────────

    /// Add a route on the host: `ip route add <dest>/<prefix> dev <device>`
    pub async fn add_route_via_device(
        &self,
        destination: Ipv4Addr,
        prefix_len: u8,
        device_name: &str,
    ) -> NetworkResult<()> {
        let ifindex = self.get_link_index(device_name).await?;

        let result = self
            .handle
            .route()
            .add()
            .v4()
            .destination_prefix(destination, prefix_len)
            .output_interface(ifindex)
            .execute()
            .await;

        match result {
            Ok(()) => Ok(()),
            Err(e) if e.to_string().contains("File exists") => Ok(()), // idempotent
            Err(e) => Err(NetworkError::Netlink(e)),
        }
    }

    /// Delete a route on the host: `ip route del <dest>/<prefix>`
    pub async fn delete_route(&self, destination: Ipv4Addr, prefix_len: u8) -> NetworkResult<()> {
        use netlink_packet_route::route::nlas::Nla as RouteNla;

        // Find the matching route first
        let mut routes = self.handle.route().get(rtnetlink::IpVersion::V4).execute();
        while let Some(route) = routes.try_next().await.map_err(NetworkError::Netlink)? {
            let dest_len = route.header.destination_prefix_length;
            if dest_len != prefix_len {
                continue;
            }

            let mut route_dest = None;
            for nla in &route.nlas {
                if let RouteNla::Destination(addr_bytes) = nla {
                    if addr_bytes.len() == 4 {
                        route_dest = Some(Ipv4Addr::new(
                            addr_bytes[0],
                            addr_bytes[1],
                            addr_bytes[2],
                            addr_bytes[3],
                        ));
                    }
                }
            }

            if route_dest == Some(destination) {
                let result = self.handle.route().del(route).execute().await;
                match result {
                    Ok(()) => return Ok(()),
                    Err(e) if e.to_string().contains("No such process") => return Ok(()), // already gone
                    Err(e) => return Err(NetworkError::Netlink(e)),
                }
            }
        }

        // Route not found — treat as success (idempotent)
        Ok(())
    }

    // ── Enumeration (for reconciliation) ──────────────────────────────

    /// List all link names matching a prefix (e.g., "veth-" for host-side veths)
    pub async fn list_links_with_prefix(&self, prefix: &str) -> NetworkResult<Vec<String>> {
        let mut links = self.handle.link().get().execute();
        let mut names = Vec::new();

        while let Some(msg) = links.try_next().await.map_err(NetworkError::Netlink)? {
            for nla in &msg.nlas {
                if let LinkNla::IfName(name) = nla {
                    if name.starts_with(prefix) {
                        names.push(name.clone());
                    }
                }
            }
        }

        Ok(names)
    }

    /// List all ports (link names) attached to a bridge
    pub async fn list_bridge_ports(&self, bridge_index: u32) -> NetworkResult<Vec<(String, u32)>> {
        let mut links = self.handle.link().get().execute();
        let mut ports = Vec::new();

        while let Some(msg) = links.try_next().await.map_err(NetworkError::Netlink)? {
            let mut name = None;
            let mut is_port = false;

            for nla in &msg.nlas {
                match nla {
                    LinkNla::IfName(n) => name = Some(n.clone()),
                    LinkNla::Master(master_idx) if *master_idx == bridge_index => is_port = true,
                    _ => {}
                }
            }

            if is_port {
                if let Some(n) = name {
                    ports.push((n, msg.header.index));
                }
            }
        }

        Ok(ports)
    }
}

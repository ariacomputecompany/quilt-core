use crate::icc::network::error::{NetworkError, NetworkResult};
use std::net::Ipv4Addr;

/// nftables-based firewall with O(1) session lookup via sets.
///
/// Architecture:
/// - Single `inet quilt` table with:
///   - `set session_ips { type ipv4_addr; }` — O(1) add/remove per session
///   - `chain forward` — policy drop, allow only @session_ips egress
///   - `chain postrouting` — masquerade for 10.42.0.0/16
///   - `chain prerouting` — DNS DNAT redirect
///
/// Replaces the per-container iptables chains (O(N) POSTROUTING walk).
#[allow(dead_code)]
pub struct Firewall {
    bridge_name: String,
    bridge_ip: Ipv4Addr,
    subnet_cidr: String,
    dns_port: u16,
    /// Whether nft is available (detected at startup)
    nft_available: bool,
}

#[allow(dead_code)]
impl Firewall {
    pub fn new(bridge_name: &str, bridge_ip: Ipv4Addr, subnet_cidr: &str, dns_port: u16) -> Self {
        let nft_available = std::process::Command::new("nft")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false);

        Self {
            bridge_name: bridge_name.to_string(),
            bridge_ip,
            subnet_cidr: subnet_cidr.to_string(),
            dns_port,
            nft_available,
        }
    }

    /// Initialize nftables table and chains at startup. Idempotent.
    pub async fn initialize(&self) -> NetworkResult<()> {
        if self.nft_available {
            self.initialize_nft().await?;
        } else {
            tracing::warn!("nft not available, falling back to iptables");
            self.initialize_iptables().await?;
        }

        // Always ensure iptables doesn't interfere with quilt traffic.
        // Docker (and other tools) inject DOCKER-USER/DOCKER-FORWARD chains
        // into iptables FORWARD that intercept ALL forwarded packets before
        // any user rules. We must insert bypass rules so quilt bridge traffic
        // is never dropped by foreign iptables chains.
        self.ensure_iptables_bypass().await;

        Ok(())
    }

    /// Ensure iptables FORWARD chain and any Docker chains don't block quilt traffic.
    /// This is critical on hosts with Docker installed — Docker's DOCKER-USER chain
    /// processes ALL forwarded packets first and can silently drop container traffic.
    async fn ensure_iptables_bypass(&self) {
        let bridge = &self.bridge_name;
        let subnet = &self.subnet_cidr;

        // 1. If DOCKER-USER chain exists, insert RETURN rules for quilt traffic
        //    so Docker skips processing our packets entirely.
        let docker_user_cmds = vec![
            format!(
                "iptables -C DOCKER-USER -i {} -j RETURN 2>/dev/null || iptables -I DOCKER-USER 1 -i {} -j RETURN",
                bridge, bridge
            ),
            format!(
                "iptables -C DOCKER-USER -o {} -j RETURN 2>/dev/null || iptables -I DOCKER-USER 1 -o {} -j RETURN",
                bridge, bridge
            ),
        ];
        for cmd in &docker_user_cmds {
            if let Err(e) = self.run_shell(cmd).await {
                // DOCKER-USER chain might not exist — that's fine
                tracing::debug!("Docker bypass rule skipped (chain may not exist): {}", e);
            }
        }

        // 2. Ensure top-of-chain ACCEPT rules in iptables FORWARD for quilt bridge.
        //    These must come BEFORE any Docker chains to guarantee our traffic passes.
        let forward_cmds = vec![
            format!(
                "iptables -C FORWARD -i {} -j ACCEPT 2>/dev/null || iptables -I FORWARD 1 -i {} -j ACCEPT",
                bridge, bridge
            ),
            format!(
                "iptables -C FORWARD -o {} -j ACCEPT 2>/dev/null || iptables -I FORWARD 1 -o {} -j ACCEPT",
                bridge, bridge
            ),
        ];
        for cmd in &forward_cmds {
            if let Err(e) = self.run_shell(cmd).await {
                tracing::warn!("Failed to insert iptables FORWARD bypass: {}", e);
            }
        }

        // 3. Ensure MASQUERADE in iptables nat table (backup for nftables)
        let masq_cmd = format!(
            "iptables -t nat -C POSTROUTING -s {} ! -o {} -j MASQUERADE 2>/dev/null || \
             iptables -t nat -A POSTROUTING -s {} ! -o {} -j MASQUERADE",
            subnet, bridge, subnet, bridge
        );
        if let Err(e) = self.run_shell(&masq_cmd).await {
            tracing::warn!("Failed to ensure iptables MASQUERADE: {}", e);
        }

        tracing::info!(
            "iptables bypass configured for bridge {} (Docker-safe)",
            bridge
        );
    }

    async fn initialize_nft(&self) -> NetworkResult<()> {
        // Create table + set + chains in a single atomic nft command
        let ruleset = format!(
            r#"
table inet quilt {{
    set session_ips {{
        type ipv4_addr
    }}

    chain forward {{
        type filter hook forward priority 0; policy drop;
        ct state established,related accept
        iifname "{bridge}" accept
        oifname "{bridge}" ip daddr @session_ips accept
        iifname "{bridge}" ip saddr @session_ips accept
    }}

    chain postrouting {{
        type nat hook postrouting priority 100;
        ip saddr {subnet} oifname != "{bridge}" masquerade
    }}

    chain prerouting {{
        type nat hook prerouting priority -100;
        iifname "{bridge}" udp dport 53 dnat ip to {bridge_ip}:{dns_port}
        iifname "{bridge}" tcp dport 53 dnat ip to {bridge_ip}:{dns_port}
    }}
}}
"#,
            bridge = self.bridge_name,
            subnet = self.subnet_cidr,
            bridge_ip = self.bridge_ip,
            dns_port = self.dns_port,
        );

        // Flush existing table first (idempotent)
        let _ = self.run_nft("delete table inet quilt").await;

        // Apply the full ruleset
        self.run_nft_stdin(&ruleset).await?;

        tracing::info!(
            "nftables initialized: table=inet quilt, bridge={}, subnet={}",
            self.bridge_name,
            self.subnet_cidr
        );
        Ok(())
    }

    async fn initialize_iptables(&self) -> NetworkResult<()> {
        // Fallback: use iptables static rules (legacy path)
        let cmds = vec![
            "iptables -P FORWARD DROP".to_string(),
            "iptables -A FORWARD -m state --state ESTABLISHED,RELATED -j ACCEPT".to_string(),
            format!(
                "iptables -A FORWARD -i {} -j ACCEPT",
                self.bridge_name
            ),
            format!(
                "iptables -A FORWARD -o {} -j ACCEPT",
                self.bridge_name
            ),
            format!(
                "iptables -t nat -C POSTROUTING -s {} ! -o {} -j MASQUERADE 2>/dev/null || \
                 iptables -t nat -A POSTROUTING -s {} ! -o {} -j MASQUERADE",
                self.subnet_cidr,
                self.bridge_name,
                self.subnet_cidr,
                self.bridge_name
            ),
            format!(
                "iptables -t nat -A PREROUTING -i {} -p udp --dport 53 -j DNAT --to-destination {}:{}",
                self.bridge_name, self.bridge_ip, self.dns_port
            ),
            format!(
                "iptables -t nat -A PREROUTING -i {} -p tcp --dport 53 -j DNAT --to-destination {}:{}",
                self.bridge_name, self.bridge_ip, self.dns_port
            ),
        ];

        for cmd in &cmds {
            let _ = self.run_shell(cmd).await;
        }

        tracing::info!("iptables initialized (nft unavailable)");
        Ok(())
    }

    /// Add a session IP to the allow set. O(1).
    pub async fn add_session(&self, ip: Ipv4Addr) -> NetworkResult<()> {
        if self.nft_available {
            let cmd = format!("add element inet quilt session_ips {{ {} }}", ip);
            self.run_nft(&cmd).await?;
        }
        // iptables fallback: no per-session rules needed (FORWARD ACCEPT for bridge)
        tracing::debug!("Firewall: added session {}", ip);
        Ok(())
    }

    /// Remove a session IP from the allow set. O(1).
    pub async fn remove_session(&self, ip: Ipv4Addr) -> NetworkResult<()> {
        if self.nft_available {
            let cmd = format!("delete element inet quilt session_ips {{ {} }}", ip);
            // Ignore errors (IP might not be in set)
            let _ = self.run_nft(&cmd).await;
        }
        tracing::debug!("Firewall: removed session {}", ip);
        Ok(())
    }

    /// List all session IPs currently in the firewall set (for reconciliation)
    pub async fn list_sessions(&self) -> NetworkResult<Vec<Ipv4Addr>> {
        if !self.nft_available {
            return Ok(Vec::new());
        }

        let output = tokio::process::Command::new("nft")
            .args(["list", "set", "inet", "quilt", "session_ips"])
            .output()
            .await
            .map_err(NetworkError::Io)?;

        if !output.status.success() {
            return Ok(Vec::new());
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut ips = Vec::new();

        // Parse nft output: "elements = { 10.42.0.2, 10.42.0.3 }"
        if let Some(start) = stdout.find("elements = {") {
            if let Some(end) = stdout[start..].find('}') {
                let elements = &stdout[start + 12..start + end];
                for part in elements.split(',') {
                    let trimmed = part.trim();
                    if let Ok(ip) = trimmed.parse::<Ipv4Addr>() {
                        ips.push(ip);
                    }
                }
            }
        }

        Ok(ips)
    }

    /// Cleanup all firewall rules (for shutdown)
    pub async fn cleanup(&self) -> NetworkResult<()> {
        if self.nft_available {
            let _ = self.run_nft("delete table inet quilt").await;
        }
        tracing::info!("Firewall cleaned up");
        Ok(())
    }

    // ── Helpers ───────────────────────────────────────────────────────

    async fn run_nft(&self, args: &str) -> NetworkResult<()> {
        let output = tokio::process::Command::new("nft")
            .args(args.split_whitespace())
            .output()
            .await
            .map_err(NetworkError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(NetworkError::Command {
                cmd: format!("nft {}", args),
                stderr: stderr.to_string(),
            });
        }
        Ok(())
    }

    async fn run_nft_stdin(&self, ruleset: &str) -> NetworkResult<()> {
        use tokio::io::AsyncWriteExt;

        let mut child = tokio::process::Command::new("nft")
            .arg("-f")
            .arg("-")
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(NetworkError::Io)?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin
                .write_all(ruleset.as_bytes())
                .await
                .map_err(NetworkError::Io)?;
        }

        let output = child.wait_with_output().await.map_err(NetworkError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(NetworkError::Command {
                cmd: "nft -f -".to_string(),
                stderr: stderr.to_string(),
            });
        }
        Ok(())
    }

    async fn run_shell(&self, cmd: &str) -> NetworkResult<()> {
        let output = tokio::process::Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .output()
            .await
            .map_err(NetworkError::Io)?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(NetworkError::Command {
                cmd: cmd.to_string(),
                stderr: stderr.to_string(),
            });
        }
        Ok(())
    }
}

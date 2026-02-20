// Bridge management module
// Handles Linux bridge creation, configuration, and lifecycle management

use crate::utils::command::CommandExecutor;
use crate::utils::console::ConsoleLogger;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

/// Lock-free bridge state tracking with atomic operations
pub struct AtomicBridgeState {
    pub exists: AtomicBool,
    pub has_ip: AtomicBool,
    pub is_up: AtomicBool,
    pub last_verified_millis: AtomicU64,
}

#[allow(dead_code)]
impl AtomicBridgeState {
    pub fn new() -> Self {
        Self {
            exists: AtomicBool::new(false),
            has_ip: AtomicBool::new(false),
            is_up: AtomicBool::new(false),
            // Force initial verification by setting timestamp 60s in the past
            last_verified_millis: AtomicU64::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .saturating_sub(60_000) as u64,
            ),
        }
    }

    pub fn is_fully_configured(&self) -> bool {
        self.exists.load(Ordering::Relaxed)
            && self.has_ip.load(Ordering::Relaxed)
            && self.is_up.load(Ordering::Relaxed)
    }

    pub fn needs_verification(&self, cache_duration: Duration) -> bool {
        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let last_verified = self.last_verified_millis.load(Ordering::Relaxed);
        (now_millis - last_verified) > cache_duration.as_millis() as u64
    }

    pub fn mark_verified(&self) {
        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.last_verified_millis
            .store(now_millis, Ordering::Relaxed);
    }

    pub fn update_state(&self, exists: bool, has_ip: bool, is_up: bool) {
        self.exists.store(exists, Ordering::Relaxed);
        self.has_ip.store(has_ip, Ordering::Relaxed);
        self.is_up.store(is_up, Ordering::Relaxed);
        self.mark_verified();
    }
}

/// Bridge management functionality
pub struct BridgeManager {
    pub bridge_name: String,
    pub bridge_ip: String,
    pub bridge_state: std::sync::Arc<AtomicBridgeState>,
    pub bridge_ready: AtomicBool,
}

#[allow(dead_code)]
impl BridgeManager {
    pub fn new(bridge_name: String, bridge_ip: String) -> Self {
        Self {
            bridge_name,
            bridge_ip,
            bridge_state: std::sync::Arc::new(AtomicBridgeState::new()),
            bridge_ready: AtomicBool::new(false),
        }
    }

    pub fn ensure_bridge_ready(&self) -> Result<(), String> {
        ConsoleLogger::progress(&format!(
            "Initializing network bridge: {}",
            self.bridge_name
        ));

        // Always check if bridge actually exists on the system (no caching bullshit)
        if self.bridge_exists_and_configured() {
            ConsoleLogger::success(&format!(
                "Bridge {} already properly configured",
                self.bridge_name
            ));
            return Ok(());
        }

        // Create bridge atomically if it doesn't exist
        if !self.bridge_exists() {
            self.create_bridge_atomic()?;
        } else {
            // Bridge exists but may not be configured, verify and fix
            self.configure_bridge_ip()?;
            self.bring_bridge_up()?;
        }

        Ok(())
    }

    pub fn bridge_exists(&self) -> bool {
        let check_cmd = format!("ip link show {}", self.bridge_name);
        ConsoleLogger::debug(&format!("Checking bridge existence: {}", check_cmd));

        // Add namespace debugging
        ConsoleLogger::debug(&format!("🔍 Current PID: {}", std::process::id()));

        match CommandExecutor::execute_shell(&check_cmd) {
            Ok(result) => {
                let exists = result.success && result.stdout.contains(&self.bridge_name);
                ConsoleLogger::debug(&format!(
                    "Bridge {} existence check: {} (success: {})",
                    self.bridge_name, exists, result.success
                ));

                if !exists && result.success {
                    // Command succeeded but bridge not found - this is normal
                    ConsoleLogger::debug(&format!(
                        "Bridge {} does not exist (normal condition)",
                        self.bridge_name
                    ));
                } else if !result.success {
                    // Command failed - this might indicate a system issue
                    ConsoleLogger::debug(&format!(
                        "Bridge existence check failed: stderr: {}, stdout: {}",
                        result.stderr, result.stdout
                    ));
                }

                exists
            }
            Err(e) => {
                ConsoleLogger::debug(&format!("Bridge existence check error: {}", e));
                false
            }
        }
    }

    fn bridge_exists_and_configured(&self) -> bool {
        // Fast path: Check cached bridge ready flag first
        if self.bridge_ready.load(Ordering::Relaxed) {
            // Check if we need to re-verify based on extended cache duration (60s during startup bursts)
            if !self
                .bridge_state
                .needs_verification(Duration::from_secs(60))
            {
                ConsoleLogger::debug(&format!(
                    "✅ [BRIDGE-CACHE] Bridge {} verified from lock-free cache",
                    self.bridge_name
                ));
                return true;
            }
        }

        // Perform full verification
        let (exists, has_ip, is_up) = self.verify_bridge_state_full();

        // Update atomic state
        self.bridge_state.update_state(exists, has_ip, is_up);

        let fully_configured = exists && has_ip && is_up;
        if fully_configured {
            self.bridge_ready.store(true, Ordering::Relaxed);
        }

        fully_configured
    }

    fn verify_bridge_state_full(&self) -> (bool, bool, bool) {
        ConsoleLogger::debug(&format!(
            "🔍 [BRIDGE-VERIFY-FULL] Full verification for bridge {}",
            self.bridge_name
        ));

        // Check 1: Bridge device exists
        let bridge_exists =
            match CommandExecutor::execute_shell(&format!("ip link show {}", self.bridge_name)) {
                Ok(result) => result.success && result.stdout.contains(&self.bridge_name),
                Err(_) => false,
            };

        if !bridge_exists {
            ConsoleLogger::debug(&format!(
                "❌ [BRIDGE-VERIFY] Bridge {} does not exist",
                self.bridge_name
            ));
            return (false, false, false);
        }

        // Check 2: Bridge is UP
        let bridge_is_up = match CommandExecutor::execute_shell(&format!(
            "ip link show {} | grep -q 'state UP'",
            self.bridge_name
        )) {
            Ok(result) => result.success,
            Err(_) => false,
        };

        // Check 3: Bridge has IP address
        let bridge_has_ip = self.verify_bridge_ip_with_retry();

        ConsoleLogger::debug(&format!(
            "🔍 [BRIDGE-VERIFY-FULL] Bridge {} state: exists={}, up={}, has_ip={}",
            self.bridge_name, bridge_exists, bridge_is_up, bridge_has_ip
        ));

        (bridge_exists, bridge_has_ip, bridge_is_up)
    }

    fn verify_bridge_ip_with_retry(&self) -> bool {
        ConsoleLogger::debug(&format!(
            "🔍 [IP-VERIFY] Verifying IP {} on bridge {} with retry logic",
            self.bridge_ip, self.bridge_name
        ));

        // Try multiple verification methods with retry
        for attempt in 1..=3 {
            ConsoleLogger::debug(&format!("🔍 [IP-VERIFY] Attempt {} of 3", attempt));

            // Method 1: ip addr show
            let ip_check_1 = format!(
                "ip addr show {} | grep {}",
                self.bridge_name, self.bridge_ip
            );
            if let Ok(result) = CommandExecutor::execute_shell(&ip_check_1) {
                if result.success && result.stdout.contains(&self.bridge_ip) {
                    ConsoleLogger::debug(&format!(
                        "✅ [IP-VERIFY] Method 1 success: IP {} found on {}",
                        self.bridge_ip, self.bridge_name
                    ));
                    return true;
                }
            }

            // Method 2: ip route get (to check if IP is routable via bridge)
            let route_check = format!(
                "ip route get {} | grep {}",
                self.bridge_ip, self.bridge_name
            );
            if let Ok(result) = CommandExecutor::execute_shell(&route_check) {
                if result.success && result.stdout.contains(&self.bridge_name) {
                    ConsoleLogger::debug(&format!(
                        "✅ [IP-VERIFY] Method 2 success: {} routable via {}",
                        self.bridge_ip, self.bridge_name
                    ));
                    return true;
                }
            }

            // Method 3: ping self test
            let ping_check = format!("ping -c 1 -W 1 {} >/dev/null 2>&1", self.bridge_ip);
            if let Ok(result) = CommandExecutor::execute_shell(&ping_check) {
                if result.success {
                    ConsoleLogger::debug(&format!(
                        "✅ [IP-VERIFY] Method 3 success: {} is pingable",
                        self.bridge_ip
                    ));
                    return true;
                }
            }

            if attempt < 3 {
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        ConsoleLogger::debug(&format!(
            "❌ [IP-VERIFY] All methods failed - IP {} not found on {}",
            self.bridge_ip, self.bridge_name
        ));
        false
    }

    fn create_bridge_atomic(&self) -> Result<(), String> {
        ConsoleLogger::debug(&format!("Creating bridge atomically: {}", self.bridge_name));

        // ELITE: Single compound command for complete bridge setup
        let bridge_cidr = format!("{}/16", self.bridge_ip);
        let atomic_bridge_cmd = format!(
            "ip link add name {} type bridge && ip addr add {} dev {} && ip link set {} up",
            self.bridge_name, bridge_cidr, self.bridge_name, self.bridge_name
        );

        ConsoleLogger::debug(&format!(
            "Executing atomic bridge creation: {}",
            atomic_bridge_cmd
        ));
        let result = CommandExecutor::execute_shell(&atomic_bridge_cmd)?;

        if !result.success {
            return Err(format!("Atomic bridge creation failed: {}", result.stderr));
        }

        // Verify bridge was created successfully
        self.verify_bridge_created()?;

        // Mark as ready in cache
        self.bridge_ready.store(true, Ordering::Relaxed);
        self.bridge_state.update_state(true, true, true);

        ConsoleLogger::success(&format!(
            "Bridge {} created and configured atomically",
            self.bridge_name
        ));
        Ok(())
    }

    fn configure_bridge_ip(&self) -> Result<(), String> {
        let bridge_cidr = format!("{}/16", self.bridge_ip);
        let check_cmd = format!(
            "ip addr show {} | grep {}",
            self.bridge_name, self.bridge_ip
        );

        ConsoleLogger::debug(&format!(
            "Checking if bridge IP already assigned: {}",
            check_cmd
        ));
        if CommandExecutor::execute_shell(&check_cmd).map_or(false, |r| r.success) {
            ConsoleLogger::debug(&format!(
                "Bridge IP {} already assigned to {}",
                self.bridge_ip, self.bridge_name
            ));
            return Ok(());
        }

        let assign_cmd = format!("ip addr add {} dev {}", bridge_cidr, self.bridge_name);
        ConsoleLogger::debug(&format!("Assigning IP to bridge: {}", assign_cmd));

        let result = CommandExecutor::execute_shell(&assign_cmd)?;
        if !result.success && !result.stderr.contains("File exists") {
            return Err(format!("Failed to assign IP to bridge: {}", result.stderr));
        }

        ConsoleLogger::success(&format!(
            "IP {} assigned to bridge {}",
            bridge_cidr, self.bridge_name
        ));
        Ok(())
    }

    fn bring_bridge_up(&self) -> Result<(), String> {
        let up_cmd = format!("ip link set {} up", self.bridge_name);
        ConsoleLogger::debug(&format!("Executing: {}", up_cmd));

        let result = CommandExecutor::execute_shell(&up_cmd)?;
        if !result.success {
            return Err(format!("Failed to bring bridge up: {}", result.stderr));
        }

        // Verify bridge is actually UP
        self.verify_bridge_up()?;

        ConsoleLogger::success(&format!("Bridge {} is UP", self.bridge_name));
        Ok(())
    }

    fn verify_bridge_created(&self) -> Result<(), String> {
        for attempt in 1..=10 {
            // Fast polling instead of single 100ms delay
            if self.bridge_exists() {
                return Ok(());
            }
            if attempt < 10 {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        Err(format!(
            "Bridge {} was not created after verification attempts",
            self.bridge_name
        ))
    }

    pub fn verify_bridge_up(&self) -> Result<(), String> {
        let check_cmd = format!("ip link show {} | grep -q '<.*UP.*>'", self.bridge_name);
        for attempt in 1..=10 {
            // Fast polling instead of single 100ms delay
            if CommandExecutor::execute_shell(&check_cmd).map_or(false, |r| r.success) {
                return Ok(());
            }
            if attempt < 10 {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        Err(format!(
            "Bridge {} is not UP after verification attempts",
            self.bridge_name
        ))
    }

    pub fn ensure_bridge_ready_for_attachment(&self) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "🔍 [BRIDGE-VERIFY] Checking if bridge {} exists and is ready for veth attachment",
            self.bridge_name
        ));

        // Check if bridge exists and is properly configured
        if self.bridge_exists_and_configured() {
            ConsoleLogger::debug(&format!(
                "✅ [BRIDGE-VERIFY] Bridge {} is ready for attachment",
                self.bridge_name
            ));
            return Ok(());
        }

        // If bridge doesn't exist or isn't configured, try to fix it
        ConsoleLogger::debug(&format!(
            "🔧 [BRIDGE-REPAIR] Bridge {} needs configuration repair",
            self.bridge_name
        ));

        // First try diagnosis to understand what's wrong
        if let Err(e) = self.diagnose_bridge_issues() {
            ConsoleLogger::warning(&format!("Bridge diagnosis failed: {}", e));
        }

        // Try to repair the configuration
        self.repair_bridge_configuration()?;

        // Verify it's now ready
        if self.bridge_exists_and_configured() {
            ConsoleLogger::success(&format!(
                "✅ [BRIDGE-REPAIR] Bridge {} successfully repaired and ready",
                self.bridge_name
            ));
            Ok(())
        } else {
            Err(format!(
                "Bridge {} could not be made ready for attachment",
                self.bridge_name
            ))
        }
    }

    fn diagnose_bridge_issues(&self) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "🔍 [BRIDGE-DIAG] Diagnosing bridge issues for {}",
            self.bridge_name
        ));

        // Check if bridge exists
        if let Ok(result) =
            CommandExecutor::execute_shell(&format!("ip link show {}", self.bridge_name))
        {
            if result.success {
                ConsoleLogger::debug(&format!(
                    "✅ [BRIDGE-DIAG] Bridge {} exists",
                    self.bridge_name
                ));
                ConsoleLogger::debug(&format!(
                    "ℹ️ [BRIDGE-DIAG] Bridge state: {}",
                    result.stdout.trim()
                ));

                // Check if it's UP
                if result.stdout.contains("state UP") {
                    ConsoleLogger::debug(&format!(
                        "✅ [BRIDGE-DIAG] Bridge {} is UP",
                        self.bridge_name
                    ));
                } else {
                    ConsoleLogger::debug(&format!(
                        "❌ [BRIDGE-DIAG] Bridge {} is not UP",
                        self.bridge_name
                    ));
                }

                // Check IP configuration
                if let Ok(ip_result) =
                    CommandExecutor::execute_shell(&format!("ip addr show {}", self.bridge_name))
                {
                    if ip_result.stdout.contains(&self.bridge_ip) {
                        ConsoleLogger::debug(&format!(
                            "✅ [BRIDGE-DIAG] Bridge {} has IP {}",
                            self.bridge_name, self.bridge_ip
                        ));
                    } else {
                        ConsoleLogger::debug(&format!(
                            "❌ [BRIDGE-DIAG] Bridge {} missing IP {}",
                            self.bridge_name, self.bridge_ip
                        ));
                    }
                }
            } else {
                ConsoleLogger::debug(&format!(
                    "❌ [BRIDGE-DIAG] Bridge {} does not exist",
                    self.bridge_name
                ));
            }
        }

        Ok(())
    }

    fn repair_bridge_configuration(&self) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "🔧 [BRIDGE-REPAIR] Repairing bridge {} configuration",
            self.bridge_name
        ));

        // Try to add IP address if missing
        let bridge_cidr = format!("{}/16", self.bridge_ip);
        let add_ip_cmd = format!(
            "ip addr add {} dev {} 2>/dev/null || true",
            bridge_cidr, self.bridge_name
        );
        let _ = CommandExecutor::execute_shell(&add_ip_cmd);

        // Try to bring bridge up if it's down
        let bring_up_cmd = format!("ip link set {} up 2>/dev/null || true", self.bridge_name);
        let _ = CommandExecutor::execute_shell(&bring_up_cmd);

        // Wait a moment for changes to take effect
        std::thread::sleep(Duration::from_millis(100));

        // Update our cache state
        let (exists, has_ip, is_up) = self.verify_bridge_state_full();
        self.bridge_state.update_state(exists, has_ip, is_up);

        if exists && has_ip && is_up {
            self.bridge_ready.store(true, Ordering::Relaxed);
            ConsoleLogger::debug(&format!(
                "✅ [BRIDGE-REPAIR] Bridge {} successfully repaired",
                self.bridge_name
            ));
            Ok(())
        } else {
            Err(format!(
                "Bridge {} repair failed: exists={}, has_ip={}, is_up={}",
                self.bridge_name, exists, has_ip, is_up
            ))
        }
    }

    pub fn verify_bridge_ready_for_attachment_fast(&self) -> Result<(), String> {
        ConsoleLogger::debug(&format!(
            "⚡ [BRIDGE-FAST] Fast bridge readiness check for {}",
            self.bridge_name
        ));

        // Use lock-free cached state if available and recent (extended 60s cache during startup bursts)
        if self.bridge_ready.load(Ordering::Relaxed) {
            if !self
                .bridge_state
                .needs_verification(Duration::from_secs(60))
            {
                ConsoleLogger::debug(&format!(
                    "✅ [BRIDGE-FAST] Bridge {} ready (cached)",
                    self.bridge_name
                ));
                return Ok(());
            }
        }

        // Fast verification: only check if bridge exists and is up
        if self.bridge_exists() && self.verify_bridge_up().is_ok() {
            self.bridge_ready.store(true, Ordering::Relaxed);
            ConsoleLogger::debug(&format!(
                "✅ [BRIDGE-FAST] Bridge {} verified as ready",
                self.bridge_name
            ));
            Ok(())
        } else {
            Err(format!(
                "Bridge {} not ready for attachment",
                self.bridge_name
            ))
        }
    }
}

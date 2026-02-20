//! Strict isolation hardening: seccomp BPF, capability dropping, no_new_privs.
//!
//! Applied inside the container child process after chroot/chdir but before exec.
//! Uses raw libc syscalls — no external crate dependencies.

/// Apply all strict isolation hardening measures.
/// Must be called after chroot/chdir (mount operations already done) and before exec.
/// Fails closed: returns Err if any critical hardening step fails.
pub fn apply_strict_hardening() -> Result<(), String> {
    // 1. Restrictive umask
    unsafe {
        libc::umask(0o077);
    }

    // 2. PR_SET_NO_NEW_PRIVS — prevents suid/sgid privilege escalation
    let ret = unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) };
    if ret != 0 {
        return Err(format!(
            "Failed to set PR_SET_NO_NEW_PRIVS: {}",
            std::io::Error::last_os_error()
        ));
    }

    // 3. Drop all capabilities from bounding set
    drop_all_capabilities()?;

    // 4. Install seccomp BPF filter
    install_seccomp_filter()?;

    Ok(())
}

/// Drop all Linux capabilities from the bounding set.
/// Iterates from 0 to CAP_LAST_CAP (detected by EINVAL).
fn drop_all_capabilities() -> Result<(), String> {
    for cap in 0..64u64 {
        let ret = unsafe { libc::prctl(libc::PR_CAPBSET_DROP, cap, 0, 0, 0) };
        if ret != 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINVAL) {
                // Past CAP_LAST_CAP — we're done
                break;
            }
            return Err(format!("Failed to drop capability {}: {}", cap, err));
        }
    }
    Ok(())
}

// ============================================================================
// Seccomp BPF filter
// ============================================================================

// BPF instruction classes
const BPF_LD: u16 = 0x00;
const BPF_JMP: u16 = 0x05;
const BPF_RET: u16 = 0x06;

// BPF ld/st modes
const BPF_W: u16 = 0x00;
const BPF_ABS: u16 = 0x20;

// BPF alu/jmp fields
const BPF_JEQ: u16 = 0x10;
const BPF_K: u16 = 0x00;

// Seccomp return values
const SECCOMP_RET_ALLOW: u32 = 0x7fff_0000;
const SECCOMP_RET_ERRNO: u32 = 0x0005_0000;

// seccomp_data field offsets
const SECCOMP_DATA_NR_OFFSET: u32 = 0; // offsetof(seccomp_data, nr)
const SECCOMP_DATA_ARCH_OFFSET: u32 = 4; // offsetof(seccomp_data, arch)

/// Construct a BPF statement (no jump).
fn bpf_stmt(code: u16, k: u32) -> libc::sock_filter {
    libc::sock_filter {
        code,
        jt: 0,
        jf: 0,
        k,
    }
}

/// Construct a BPF jump instruction.
fn bpf_jump(code: u16, k: u32, jt: u8, jf: u8) -> libc::sock_filter {
    libc::sock_filter { code, jt, jf, k }
}

/// Blocked syscalls for x86_64.
#[cfg(target_arch = "x86_64")]
fn blocked_syscalls() -> Vec<u32> {
    vec![
        246, // kexec_load
        169, // reboot
        165, // mount
        166, // umount2
        167, // swapon
        168, // swapoff
        175, // init_module
        176, // delete_module
        313, // finit_module
        304, // open_by_handle_at (container escape vector)
        310, // kexec_file_load
        155, // pivot_root
        163, // acct
        134, // uselib
        171, // setdomainname
        170, // sethostname (already in UTS ns but belt-and-suspenders)
    ]
}

/// AUDIT_ARCH constant for x86_64.
#[cfg(target_arch = "x86_64")]
const AUDIT_ARCH: u32 = 0xC000_003E;

/// Blocked syscalls for aarch64.
#[cfg(target_arch = "aarch64")]
fn blocked_syscalls() -> Vec<u32> {
    vec![
        // aarch64 syscall numbers (from asm-generic/unistd.h)
        104, // kexec_load (not implemented on aarch64 but block anyway)
        142, // reboot
        40,  // mount
        39,  // umount2
        224, // swapon
        225, // swapoff
        105, // init_module
        106, // delete_module
        273, // finit_module
        265, // open_by_handle_at
        294, // kexec_file_load
        41,  // pivot_root
        89,  // acct
        // uselib doesn't exist on aarch64
        162, // setdomainname
        161, // sethostname
    ]
}

/// AUDIT_ARCH constant for aarch64.
#[cfg(target_arch = "aarch64")]
const AUDIT_ARCH: u32 = 0xC000_00B7;

/// Install a seccomp BPF filter that blocks dangerous syscalls with EPERM.
/// Uses a blocklist approach: all syscalls allowed except explicitly blocked ones.
/// This is safe for minit, shell, and general container workloads.
///
/// BPF program structure:
///   1. Load arch from seccomp_data, verify it matches
///   2. Load syscall number
///   3. For each blocked syscall: JEQ → jump to DENY
///   4. Fall through to ALLOW
///   5. DENY returns ERRNO(EPERM)
fn install_seccomp_filter() -> Result<(), String> {
    let blocked = blocked_syscalls();
    let num_blocked = blocked.len();

    // Build BPF program
    let mut filter: Vec<libc::sock_filter> = Vec::with_capacity(num_blocked + 5);

    // Instruction 0: Load architecture
    filter.push(bpf_stmt(BPF_LD | BPF_W | BPF_ABS, SECCOMP_DATA_ARCH_OFFSET));

    // Instruction 1: Check arch matches — if yes skip to instruction 2, if no jump to ALLOW
    // (Allow unknown architectures rather than breaking — defense in depth, not only defense)
    let jump_to_allow = (num_blocked + 1) as u8; // skip past all checks + load_nr to ALLOW
    filter.push(bpf_jump(
        BPF_JMP | BPF_JEQ | BPF_K,
        AUDIT_ARCH,
        0,
        jump_to_allow,
    ));

    // Instruction 2: Load syscall number
    filter.push(bpf_stmt(BPF_LD | BPF_W | BPF_ABS, SECCOMP_DATA_NR_OFFSET));

    // Instructions 3..3+N-1: Check each blocked syscall
    for (i, &syscall_nr) in blocked.iter().enumerate() {
        let remaining_checks = num_blocked - i - 1;
        // If match: jump past remaining checks + ALLOW instruction to DENY
        let jump_to_deny = (remaining_checks + 1) as u8;
        // If no match: fall through to next check (jf=0)
        filter.push(bpf_jump(
            BPF_JMP | BPF_JEQ | BPF_K,
            syscall_nr,
            jump_to_deny,
            0,
        ));
    }

    // Instruction 3+N: ALLOW (default)
    filter.push(bpf_stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW));

    // Instruction 3+N+1: DENY with EPERM
    filter.push(bpf_stmt(
        BPF_RET | BPF_K,
        SECCOMP_RET_ERRNO | (libc::EPERM as u32),
    ));

    // Assemble sock_fprog
    let prog = libc::sock_fprog {
        len: filter.len() as u16,
        filter: filter.as_ptr() as *mut libc::sock_filter,
    };

    // Install via prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &prog)
    // Note: PR_SET_NO_NEW_PRIVS must be set first (done in apply_strict_hardening)
    let ret = unsafe {
        libc::syscall(
            libc::SYS_seccomp,
            1i32, // SECCOMP_SET_MODE_FILTER
            0i32, // flags
            &prog as *const libc::sock_fprog,
        )
    };

    if ret != 0 {
        return Err(format!(
            "seccomp(SECCOMP_SET_MODE_FILTER) failed: {}",
            std::io::Error::last_os_error()
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bpf_stmt_construction() {
        let stmt = bpf_stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW);
        assert_eq!(stmt.code, BPF_RET | BPF_K);
        assert_eq!(stmt.jt, 0);
        assert_eq!(stmt.jf, 0);
        assert_eq!(stmt.k, SECCOMP_RET_ALLOW);
    }

    #[test]
    fn test_bpf_jump_construction() {
        let jmp = bpf_jump(BPF_JMP | BPF_JEQ | BPF_K, 165, 3, 0);
        assert_eq!(jmp.code, BPF_JMP | BPF_JEQ | BPF_K);
        assert_eq!(jmp.jt, 3);
        assert_eq!(jmp.jf, 0);
        assert_eq!(jmp.k, 165);
    }

    #[test]
    fn test_blocked_syscalls_not_empty() {
        let blocked = blocked_syscalls();
        assert!(!blocked.is_empty());
        // Should have at least the critical ones: mount, reboot, kexec, open_by_handle_at
        assert!(blocked.len() >= 10);
    }

    #[test]
    fn test_filter_program_structure() {
        let blocked = blocked_syscalls();
        let num_blocked = blocked.len();

        // Expected instructions: load_arch(1) + check_arch(1) + load_nr(1) + checks(N) + allow(1) + deny(1)
        let expected_len = 3 + num_blocked + 2;

        // Build the filter to verify structure
        let mut filter: Vec<libc::sock_filter> = Vec::new();
        filter.push(bpf_stmt(BPF_LD | BPF_W | BPF_ABS, SECCOMP_DATA_ARCH_OFFSET));
        let jump_to_allow = (num_blocked + 1) as u8;
        filter.push(bpf_jump(
            BPF_JMP | BPF_JEQ | BPF_K,
            AUDIT_ARCH,
            0,
            jump_to_allow,
        ));
        filter.push(bpf_stmt(BPF_LD | BPF_W | BPF_ABS, SECCOMP_DATA_NR_OFFSET));

        for (i, &syscall_nr) in blocked.iter().enumerate() {
            let remaining = num_blocked - i - 1;
            let jump_to_deny = (remaining + 1) as u8;
            filter.push(bpf_jump(
                BPF_JMP | BPF_JEQ | BPF_K,
                syscall_nr,
                jump_to_deny,
                0,
            ));
        }

        filter.push(bpf_stmt(BPF_RET | BPF_K, SECCOMP_RET_ALLOW));
        filter.push(bpf_stmt(
            BPF_RET | BPF_K,
            SECCOMP_RET_ERRNO | (libc::EPERM as u32),
        ));

        assert_eq!(filter.len(), expected_len);

        // Verify first instruction loads arch
        assert_eq!(filter[0].code, BPF_LD | BPF_W | BPF_ABS);
        assert_eq!(filter[0].k, SECCOMP_DATA_ARCH_OFFSET);

        // Verify last two are ALLOW and DENY
        assert_eq!(filter[filter.len() - 2].code, BPF_RET | BPF_K);
        assert_eq!(filter[filter.len() - 2].k, SECCOMP_RET_ALLOW);
        assert_eq!(filter[filter.len() - 1].code, BPF_RET | BPF_K);
        assert_eq!(
            filter[filter.len() - 1].k,
            SECCOMP_RET_ERRNO | (libc::EPERM as u32)
        );
    }

    #[test]
    fn test_jump_offsets_in_bounds() {
        // Verify that all jump offsets in the BPF program are within bounds
        let blocked = blocked_syscalls();
        let num_blocked = blocked.len();
        let total_insns = 3 + num_blocked + 2;

        // Check arch jump: from instruction 1, jf should reach ALLOW at index (3 + num_blocked)
        let arch_jf = (num_blocked + 1) as usize; // jump forward this many
        let arch_target = 1 + 1 + arch_jf; // current pos + 1 + offset
        assert_eq!(arch_target, 3 + num_blocked); // should land on ALLOW
        assert!(arch_target < total_insns);

        // Check each syscall jump: from instruction (3+i), jt should reach DENY
        for i in 0..num_blocked {
            let remaining = num_blocked - i - 1;
            let jt = (remaining + 1) as usize;
            let target = (3 + i) + 1 + jt; // current pos + 1 + offset
            assert_eq!(target, 3 + num_blocked + 1); // should land on DENY
            assert!(target < total_insns);
        }
    }
}

use std::net::Ipv4Addr;

/// Allocate non-overlapping per-node PodCIDRs carved from a cluster PodCIDR.
///
/// This is IPv4-only and intentionally simple/rigid for production correctness:
/// - `cluster_cidr` must be a valid IPv4 CIDR base.
/// - `node_prefix_len` must be >= cluster prefix and <= 30.
/// - Allocation chooses the lowest available subnet by scanning from the base.
pub fn allocate_next_pod_cidr(
    cluster_cidr: &str,
    node_prefix_len: u8,
    already_allocated: &[String],
) -> Result<String, String> {
    let (cluster_base, cluster_prefix) = parse_ipv4_cidr(cluster_cidr)?;
    if node_prefix_len < cluster_prefix {
        return Err(format!(
            "node prefix /{} must be >= cluster prefix /{}",
            node_prefix_len, cluster_prefix
        ));
    }
    if node_prefix_len > 30 {
        return Err(format!(
            "node prefix /{} is too small; must be <= /30",
            node_prefix_len
        ));
    }

    let cluster_base_u32 = u32::from(cluster_base);
    let cluster_mask = prefix_mask(cluster_prefix);
    if (cluster_base_u32 & cluster_mask) != cluster_base_u32 {
        return Err(format!(
            "cluster PodCIDR must be a network base address, got {}",
            cluster_cidr
        ));
    }

    let node_mask = prefix_mask(node_prefix_len);
    let cluster_size = 1u64 << (32 - cluster_prefix);
    let node_size = 1u64 << (32 - node_prefix_len);
    if node_size == 0 || node_size > cluster_size {
        return Err("invalid CIDR sizing".to_string());
    }

    let mut allocated: Vec<(u32, u32)> = Vec::new();
    for cidr in already_allocated {
        let (base, prefix) = parse_ipv4_cidr(cidr)?;
        if prefix != node_prefix_len {
            // Be strict: mixed prefix allocations are not supported.
            return Err(format!(
                "mixed node PodCIDR prefixes not supported (expected /{}, got {} in {})",
                node_prefix_len, prefix, cidr
            ));
        }
        allocated.push((u32::from(base), prefix_mask(prefix)));
    }

    // Scan from cluster base in node-sized increments.
    let mut offset: u64 = 0;
    while offset + node_size <= cluster_size {
        let candidate = cluster_base_u32
            .checked_add(offset as u32)
            .ok_or_else(|| "CIDR overflow".to_string())?;
        // Candidate must be aligned to node prefix.
        if (candidate & node_mask) != candidate {
            offset += node_size;
            continue;
        }

        // Ensure candidate is within the cluster CIDR.
        if (candidate & cluster_mask) != cluster_base_u32 {
            break;
        }

        let mut conflict = false;
        for (base, _) in &allocated {
            if *base == candidate {
                conflict = true;
                break;
            }
        }
        if !conflict {
            return Ok(format!("{}/{}", Ipv4Addr::from(candidate), node_prefix_len));
        }

        offset += node_size;
    }

    Err("no available PodCIDR blocks remaining".to_string())
}

fn parse_ipv4_cidr(cidr: &str) -> Result<(Ipv4Addr, u8), String> {
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
    if prefix_len > 32 {
        return Err(format!("invalid prefix length /{}", prefix_len));
    }
    Ok((ip, prefix_len))
}

fn prefix_mask(prefix_len: u8) -> u32 {
    if prefix_len == 0 {
        0
    } else {
        (!0u32) << (32 - prefix_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_first_block() {
        let cidr = allocate_next_pod_cidr("10.42.0.0/16", 24, &[]).unwrap();
        assert_eq!(cidr, "10.42.0.0/24");
    }

    #[test]
    fn skips_allocated_blocks() {
        let cidr = allocate_next_pod_cidr(
            "10.42.0.0/16",
            24,
            &["10.42.0.0/24".to_string(), "10.42.1.0/24".to_string()],
        )
        .unwrap();
        assert_eq!(cidr, "10.42.2.0/24");
    }

    #[test]
    fn errors_when_exhausted_small_range() {
        let cluster = "10.0.0.0/30";
        // /30 cluster can only fit one /30 node.
        let first = allocate_next_pod_cidr(cluster, 30, &[]).unwrap();
        assert_eq!(first, "10.0.0.0/30");
        let err = allocate_next_pod_cidr(cluster, 30, &[first]).unwrap_err();
        assert!(err.contains("no available"));
    }
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::ptr;
use std::sync::OnceLock;

use crate::common::app_config::ServerConfig;
use crate::novarocks_config::config as novarocks_app_config;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Cidr {
    network: IpAddr,
    prefix_len: u8,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LocalAddress {
    ip: IpAddr,
    is_loopback: bool,
}

pub(crate) fn advertise_host() -> Result<String, String> {
    static ADVERTISE_HOST: OnceLock<Result<String, String>> = OnceLock::new();
    ADVERTISE_HOST
        .get_or_init(|| {
            let cfg = novarocks_app_config().map_err(|e| e.to_string())?;
            advertise_host_for_server(&cfg.server)
        })
        .clone()
}

pub fn advertise_host_for_server(server: &ServerConfig) -> Result<String, String> {
    let candidates = local_address_candidates()?;
    choose_advertise_host(&server.host, &server.priority_networks, &candidates)
}

pub(crate) fn format_host_for_url(host: &str) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]")
    } else {
        host.to_string()
    }
}

fn choose_advertise_host(
    configured_host: &str,
    priority_networks: &str,
    candidates: &[LocalAddress],
) -> Result<String, String> {
    let priority_networks = priority_networks.trim();
    if !priority_networks.is_empty() {
        let cidrs = parse_priority_cidrs(priority_networks)?;
        let matched = candidates
            .iter()
            .filter(|candidate| cidrs.iter().any(|cidr| cidr.contains(candidate.ip)))
            .map(|candidate| candidate.ip.to_string())
            .collect::<Vec<_>>();
        if matched.len() > 1 {
            return Err(format!(
                "priority_networks is ambiguous; matched multiple local addresses: {}",
                matched.join(", ")
            ));
        }
        if let Some(host) = matched.first() {
            return Ok(host.clone());
        }
        tracing::warn!(
            priority_networks = priority_networks,
            "priority_networks did not match any local address; falling back to default selection"
        );
    } else {
        let configured_host = configured_host.trim();
        if !configured_host.is_empty() && !is_unspecified_bind_host(configured_host) {
            return Ok(configured_host.to_string());
        }
    }

    choose_default_candidate(candidates)
}

fn parse_priority_cidrs(input: &str) -> Result<Vec<Cidr>, String> {
    input
        .split(';')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(Cidr::parse)
        .collect()
}

fn choose_default_candidate(candidates: &[LocalAddress]) -> Result<String, String> {
    if let Some(candidate) = candidates
        .iter()
        .find(|candidate| !candidate.is_loopback && candidate.ip.is_ipv4())
    {
        return Ok(candidate.ip.to_string());
    }
    if let Some(candidate) = candidates.iter().find(|candidate| !candidate.is_loopback) {
        return Ok(candidate.ip.to_string());
    }
    if let Some(candidate) = candidates.first() {
        return Ok(candidate.ip.to_string());
    }
    Err("failed to discover any local IP address".to_string())
}

fn local_address_candidates() -> Result<Vec<LocalAddress>, String> {
    let mut ifaddrs: *mut libc::ifaddrs = ptr::null_mut();
    let rc = unsafe { libc::getifaddrs(&mut ifaddrs) };
    if rc != 0 {
        return Err(format!(
            "getifaddrs failed: {}",
            std::io::Error::last_os_error()
        ));
    }

    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut current = ifaddrs;
    while !current.is_null() {
        let ifa = unsafe { &*current };
        current = ifa.ifa_next;

        if ifa.ifa_addr.is_null() {
            continue;
        }
        if (ifa.ifa_flags & libc::IFF_UP as u32) == 0 {
            continue;
        }

        let Some(ip) = ip_from_sockaddr(ifa.ifa_addr) else {
            continue;
        };
        if !seen.insert(ip) {
            continue;
        }

        out.push(LocalAddress {
            ip,
            is_loopback: (ifa.ifa_flags & libc::IFF_LOOPBACK as u32) != 0,
        });
    }

    unsafe { libc::freeifaddrs(ifaddrs) };
    Ok(out)
}

fn ip_from_sockaddr(addr: *const libc::sockaddr) -> Option<IpAddr> {
    let family = unsafe { (*addr).sa_family as i32 };
    match family {
        libc::AF_INET => {
            let addr_in = unsafe { &*(addr as *const libc::sockaddr_in) };
            Some(IpAddr::V4(Ipv4Addr::from(u32::from_be(
                addr_in.sin_addr.s_addr,
            ))))
        }
        libc::AF_INET6 => {
            let addr_in6 = unsafe { &*(addr as *const libc::sockaddr_in6) };
            Some(IpAddr::V6(Ipv6Addr::from(addr_in6.sin6_addr.s6_addr)))
        }
        _ => None,
    }
}

fn is_unspecified_bind_host(host: &str) -> bool {
    matches!(host.trim(), "" | "0.0.0.0" | "::" | "[::]")
}

impl Cidr {
    fn parse(input: &str) -> Result<Self, String> {
        let (network, prefix_len) = input
            .split_once('/')
            .ok_or_else(|| format!("invalid priority_networks entry '{input}', expected CIDR"))?;
        let network = network
            .trim()
            .parse::<IpAddr>()
            .map_err(|e| format!("invalid CIDR address '{network}': {e}"))?;
        let prefix_len = prefix_len
            .trim()
            .parse::<u8>()
            .map_err(|e| format!("invalid CIDR prefix in '{input}': {e}"))?;
        let max_prefix_len = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix_len > max_prefix_len {
            return Err(format!(
                "invalid CIDR prefix in '{input}': prefix must be <= {max_prefix_len}"
            ));
        }
        Ok(Self {
            network,
            prefix_len,
        })
    }

    fn contains(&self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(ip)) => {
                mask_v4(network, self.prefix_len) == mask_v4(ip, self.prefix_len)
            }
            (IpAddr::V6(network), IpAddr::V6(ip)) => {
                mask_v6(network, self.prefix_len) == mask_v6(ip, self.prefix_len)
            }
            _ => false,
        }
    }
}

fn mask_v4(addr: Ipv4Addr, prefix_len: u8) -> u32 {
    if prefix_len == 0 {
        return 0;
    }
    let mask = u32::MAX << (32 - u32::from(prefix_len));
    u32::from(addr) & mask
}

fn mask_v6(addr: Ipv6Addr, prefix_len: u8) -> u128 {
    if prefix_len == 0 {
        return 0;
    }
    let mask = u128::MAX << (128 - u32::from(prefix_len));
    u128::from_be_bytes(addr.octets()) & mask
}

#[cfg(test)]
mod tests {
    use super::{LocalAddress, choose_advertise_host, format_host_for_url};
    use std::net::IpAddr;

    fn addr(ip: &str, is_loopback: bool) -> LocalAddress {
        LocalAddress {
            ip: ip.parse::<IpAddr>().expect("parse test ip"),
            is_loopback,
        }
    }

    #[test]
    fn choose_advertise_host_uses_priority_networks_match() {
        let host = choose_advertise_host(
            "0.0.0.0",
            "192.168.108.23/24",
            &[
                addr("127.0.0.1", true),
                addr("10.0.0.9", false),
                addr("192.168.108.43", false),
            ],
        )
        .expect("choose host");
        assert_eq!(host, "192.168.108.43");
    }

    #[test]
    fn choose_advertise_host_errors_when_priority_networks_match_multiple_ips() {
        let err = choose_advertise_host(
            "0.0.0.0",
            "10.10.10.0/24",
            &[addr("10.10.10.9", false), addr("10.10.10.10", false)],
        )
        .expect_err("multiple matches should fail");
        assert!(err.contains("matched multiple local addresses"));
    }

    #[test]
    fn choose_advertise_host_prefers_explicit_host_without_priority_networks() {
        let host = choose_advertise_host(
            "be.internal.example",
            "",
            &[addr("10.0.0.9", false), addr("127.0.0.1", true)],
        )
        .expect("choose host");
        assert_eq!(host, "be.internal.example");
    }

    #[test]
    fn choose_advertise_host_prefers_non_loopback_ipv4_for_wildcard_bind() {
        let host = choose_advertise_host(
            "0.0.0.0",
            "",
            &[
                addr("127.0.0.1", true),
                addr("fe80::1", false),
                addr("192.168.1.20", false),
            ],
        )
        .expect("choose host");
        assert_eq!(host, "192.168.1.20");
    }

    #[test]
    fn choose_advertise_host_falls_back_to_non_loopback_ipv6_when_no_ipv4_exists() {
        let host = choose_advertise_host(
            "0.0.0.0",
            "",
            &[addr("127.0.0.1", true), addr("2001:db8::10", false)],
        )
        .expect("choose host");
        assert_eq!(host, "2001:db8::10");
    }

    #[test]
    fn format_host_for_url_wraps_ipv6() {
        assert_eq!(format_host_for_url("2001:db8::1"), "[2001:db8::1]");
        assert_eq!(format_host_for_url("10.0.0.9"), "10.0.0.9");
    }
}

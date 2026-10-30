// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Neutral facts about selected shared dependencies. These values carry no
//! quota, credentials, execution identity or right to perform an operation.
//! Providers and filesystem adapters project the route they actually selected;
//! governance consumes that fact without resolving catalog names or DNS aliases.

mod retention;
pub use retention::{ExistingOwnerRetentionLease, RetentionOwner, RetentionPinError};

use std::{fmt, sync::Arc};

/// A network service's HTTP origin, independent of catalog alias, object path,
/// credentials and credential epoch. HTTP default ports use the URL author's
/// origin normalization. Distinct DNS names are not guessed to be equivalent.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NetworkServiceOrigin(Arc<str>);

/// The HTTP service protocol already selected by the original transport.
/// This is a fact, not TLS material, an admission or permission to dial.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NetworkServiceScheme {
    Http,
    Https,
}
impl NetworkServiceScheme {
    fn as_str(self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }
    fn default_port(self) -> u16 {
        match self {
            Self::Http => 80,
            Self::Https => 443,
        }
    }
}

// Existing URL hosts retain their exact original serialization. Typed IPs use
// that same URL crate's Host Display, including IPv4-mapped IPv6. No new host
// parser, DNS lookup or Native-specific dependency enters this neutral crate.
#[derive(Clone, Copy, Debug)]
enum OriginHost<'a> {
    Serialized(&'a str),
    Ipv4(std::net::Ipv4Addr),
    Ipv6(std::net::Ipv6Addr),
}
impl fmt::Display for OriginHost<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Serialized(host) => f.write_str(host),
            Self::Ipv4(ip) => fmt::Display::fmt(&url::Host::<&str>::Ipv4(*ip), f),
            Self::Ipv6(ip) => fmt::Display::fmt(&url::Host::<&str>::Ipv6(*ip), f),
        }
    }
}

/// A validated, allocation-free view of an already selected route's origin.
/// Input parsing, normalization and custody stay with that original route
/// owner. Keeping this view grants no resource or execution authority.
#[derive(Clone, Copy, Debug)]
pub struct SelectedNetworkOrigin<'a> {
    scheme: &'a str,
    host: OriginHost<'a>,
    port: Option<u16>,
    bytes: usize,
}
impl<'a> SelectedNetworkOrigin<'a> {
    pub fn from_selected_url(selected: &'a url::Url) -> Result<Self, DependencyFactError> {
        let scheme = selected.scheme();
        if !matches!(scheme, "http" | "https") || selected.host().is_none() {
            return Err(DependencyFactError::InvalidNetworkOrigin);
        }
        let host = selected
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or(DependencyFactError::InvalidNetworkOrigin)?;
        Self::checked(scheme, OriginHost::Serialized(host), selected.port())
    }

    /// Borrow the typed host and explicit port already selected/normalized by
    /// the original route owner. Native callers pass their actual canonical
    /// reference host and actual TLS policy, never a Tonic dummy origin.
    ///
    /// Domain input must already be canonical lowercase ASCII (A-labels).
    /// Reject delimiters, user information, path, percent escapes and whitespace
    /// rather than interpreting or normalizing a hand-built Domain value.
    /// This does not authenticate the caller's route choice or grant a dial.
    pub fn from_selected_host(
        scheme: NetworkServiceScheme,
        host: url::Host<&'a str>,
        port: u16,
    ) -> Result<Self, DependencyFactError> {
        let host = match host {
            url::Host::Domain(host) => {
                if host.is_empty()
                    || !host.bytes().all(|byte| {
                        byte.is_ascii_lowercase()
                            || byte.is_ascii_digit()
                            || matches!(byte, b'-' | b'.' | b'_')
                    })
                {
                    return Err(DependencyFactError::InvalidNetworkOrigin);
                }
                OriginHost::Serialized(host)
            }
            url::Host::Ipv4(ip) => OriginHost::Ipv4(ip),
            url::Host::Ipv6(ip) => OriginHost::Ipv6(ip),
        };
        Self::checked(
            scheme.as_str(),
            host,
            (port != scheme.default_port()).then_some(port),
        )
    }

    fn checked(
        scheme: &'a str,
        host: OriginHost<'a>,
        port: Option<u16>,
    ) -> Result<Self, DependencyFactError> {
        let mut selected = Self {
            scheme,
            host,
            port,
            bytes: 0,
        };
        let mut count = OriginCounter(0);
        selected
            .write_origin(&mut count)
            .map_err(|_| DependencyFactError::EndpointTooLong)?;
        selected.bytes = count.0;
        Ok(selected)
    }

    fn write_origin(self, target: &mut impl fmt::Write) -> fmt::Result {
        write!(target, "{}://{}", self.scheme, self.host)?;
        if let Some(port) = self.port {
            write!(target, ":{port}")?;
        }
        Ok(())
    }

    /// Allocate the canonical output only after its original owner has prepaid
    /// both the String and final Arc that coexist during this conversion.
    pub fn into_owned(self) -> NetworkServiceOrigin {
        let mut origin = String::with_capacity(self.bytes);
        self.write_origin(&mut origin)
            .expect("writing to a bounded String cannot fail");
        debug_assert_eq!(origin.len(), self.bytes);
        debug_assert!(origin.is_ascii());
        NetworkServiceOrigin(Arc::from(origin))
    }
}

struct OriginCounter(usize);
impl fmt::Write for OriginCounter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0 = self
            .0
            .checked_add(value.len())
            .filter(|bytes| *bytes <= NetworkServiceOrigin::MAX_BYTES)
            .ok_or(fmt::Error)?;
        Ok(())
    }
}
struct OriginComparison<'a>(&'a str);
impl fmt::Write for OriginComparison<'_> {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0 = self.0.strip_prefix(value).ok_or(fmt::Error)?;
        Ok(())
    }
}
impl NetworkServiceOrigin {
    /// Maximum canonical origin output from this fact author, in bytes.
    pub const MAX_BYTES: usize = 1024;

    /// Project the HTTP(S) URL already parsed by the route's original owner.
    /// This does not parse, resolve DNS, acquire credentials or authorize I/O.
    /// Path, user information, query and fragment are never copied or retained.
    pub fn from_selected_url(selected: &url::Url) -> Result<Self, DependencyFactError> {
        SelectedNetworkOrigin::from_selected_url(selected).map(SelectedNetworkOrigin::into_owned)
    }

    /// Compare a validated selected origin without allocating another String
    /// or Arc. Both sides use the same URL author's host/default-port facts.
    pub fn matches_selected(&self, selected: SelectedNetworkOrigin<'_>) -> bool {
        if self.0.len() != selected.bytes {
            return false;
        }
        let mut comparison = OriginComparison(&self.0);
        selected.write_origin(&mut comparison).is_ok() && comparison.0.is_empty()
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The HDFS service selected by its original ClientBuilder URL. Logical HA
/// nameservices remain service identities; user/path and DNS aliases are not.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct HdfsServiceOrigin(Arc<str>);
impl HdfsServiceOrigin {
    pub const MAX_BYTES: usize = 1024;
    pub fn from_selected_url(selected: &url::Url) -> Result<Self, DependencyFactError> {
        if selected.scheme() != "hdfs" {
            return Err(DependencyFactError::InvalidServiceOrigin);
        }
        let host = selected
            .host_str()
            .filter(|h| !h.is_empty())
            .ok_or(DependencyFactError::InvalidServiceOrigin)?;
        // Do not invent a default port: an unported logical nameservice is
        // interpreted using the original Hadoop configuration by ClientBuilder.
        let suffix = selected.port().map(|p| format!(":{p}")).unwrap_or_default();
        let bytes = 7usize
            .checked_add(host.len())
            .and_then(|n| n.checked_add(suffix.len()))
            .filter(|n| *n <= Self::MAX_BYTES)
            .ok_or(DependencyFactError::EndpointTooLong)?;
        let mut value = String::with_capacity(bytes);
        value.push_str("hdfs://");
        value.push_str(host);
        value.push_str(&suffix);
        Ok(Self(Arc::from(value)))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The same actual Thrift service is shared by framed and buffered codecs.
/// A codec changes wire framing, not the endpoint's failure domain. No HTTP
/// reinterpretation, DNS resolution, default port or catalog alias enters it.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ThriftServiceOrigin(Arc<str>);

/// Borrow the original transport's selected host/address and explicit port.
/// Selection and original URI ownership remain with that transport owner.
#[derive(Clone, Copy, Debug)]
pub struct SelectedThriftServiceOrigin<'a> {
    host: &'a str,
    port: u16,
}
impl<'a> SelectedThriftServiceOrigin<'a> {
    pub fn from_selected_address(address: &'a str) -> Result<Self, DependencyFactError> {
        let (host, port) = address
            .rsplit_once(':')
            .ok_or(DependencyFactError::InvalidServiceOrigin)?;
        let port = port
            .parse::<u16>()
            .ok()
            .filter(|port| *port != 0)
            .ok_or(DependencyFactError::InvalidServiceOrigin)?;
        if host.is_empty()
            || !host.is_ascii()
            || host.bytes().any(|byte| {
                byte.is_ascii_whitespace() || matches!(byte, b'/' | b'@' | b'?' | b'#' | b'%')
            })
        {
            return Err(DependencyFactError::InvalidServiceOrigin);
        }
        if host.starts_with('[') {
            host.strip_prefix('[')
                .and_then(|value| value.strip_suffix(']'))
                .and_then(|value| value.parse::<std::net::Ipv6Addr>().ok())
                .ok_or(DependencyFactError::InvalidServiceOrigin)?;
        } else if host.contains(':')
            || host
                .bytes()
                .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'.' | b'-' | b'_'))
        {
            return Err(DependencyFactError::InvalidServiceOrigin);
        }
        let bytes = 9usize
            .checked_add(host.len())
            .and_then(|n| n.checked_add(1 + port.ilog10() as usize + 1))
            .ok_or(DependencyFactError::EndpointTooLong)?;
        if bytes > ThriftServiceOrigin::MAX_BYTES {
            return Err(DependencyFactError::EndpointTooLong);
        }
        Ok(Self { host, port })
    }
    pub fn into_owned(self) -> ThriftServiceOrigin {
        use std::fmt::Write;
        // One bounded String and its final Arc coexist, exactly as the
        // original service-fact fee model. Lowercase without a third buffer.
        let mut value =
            String::with_capacity(ThriftServiceOrigin::MAX_BYTES.min(9 + self.host.len() + 6));
        value.push_str("thrift://");
        if self.host.starts_with('[') {
            let address = self.host[1..self.host.len() - 1]
                .parse::<std::net::Ipv6Addr>()
                .expect("validated selected IPv6");
            write!(&mut value, "[{address}]").expect("bounded String write");
        } else {
            for byte in self.host.bytes() {
                value.push(byte.to_ascii_lowercase() as char);
            }
        }
        write!(&mut value, ":{}", self.port).expect("bounded String write");
        ThriftServiceOrigin(Arc::from(value))
    }
}
impl ThriftServiceOrigin {
    pub const MAX_BYTES: usize = 1024;
    pub fn from_selected_address(address: &str) -> Result<Self, DependencyFactError> {
        SelectedThriftServiceOrigin::from_selected_address(address)
            .map(SelectedThriftServiceOrigin::into_owned)
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A shared host-local dependency is intentionally independent of directory or
/// catalog name. Network dependencies name the actual selected service origin.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SharedDependency {
    Network(NetworkServiceOrigin),
    Hdfs(HdfsServiceOrigin),
    Thrift(ThriftServiceOrigin),
    LocalHost,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DependencyFactError {
    EndpointTooLong,
    InvalidNetworkOrigin,
    InvalidServiceOrigin,
}
impl fmt::Display for DependencyFactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EndpointTooLong => "shared dependency origin exceeds its bounded output profile",
            Self::InvalidServiceOrigin => {
                "shared dependency requires an actual selected service origin"
            }
            Self::InvalidNetworkOrigin => {
                "shared dependency requires a selected HTTP(S) service origin"
            }
        })
    }
}
impl std::error::Error for DependencyFactError {}

#[cfg(test)]
mod tests {
    use super::*;

    // Parsing belongs to the fixture's original route owner, outside the fact
    // constructor. Production callers must supply their already-selected Url.
    fn origin(endpoint: &str) -> NetworkServiceOrigin {
        let selected = url::Url::parse(endpoint).expect("fixture route must parse");
        NetworkServiceOrigin::from_selected_url(&selected).expect("fixture origin must fit")
    }

    #[test]
    fn selected_routes_share_one_origin_without_retaining_path_or_secrets() {
        let first =
            origin("HTTPS://user:password@Example.org:443/v1/catalog?credential=secret#private");
        let second = origin("https://example.org/oauth/token");
        assert_eq!(first, second);
        assert_eq!(first.as_str(), "https://example.org");
        assert_eq!(
            format!("{first:?}"),
            "NetworkServiceOrigin(\"https://example.org\")"
        );
        assert_ne!(first, origin("https://example.org:8443"));
        assert_ne!(first, origin("https://other-alias.example.org"));
    }

    #[test]
    fn original_url_author_controls_host_and_default_port_normalization() {
        for input in [
            "http://EXAMPLE.org:80/path",
            "https://EXAMPLE.org:443/path",
            "http://[0:0:0:0:0:0:0:1]:80/path",
            "https://[2001:0DB8:0:0:0:0:0:1]:8443/path",
            "http://127.0.0.1:80/path",
            "https://bücher.example:443/path",
            "https://example.org:0/path",
            "https://example.org:65535/path",
        ] {
            let selected = url::Url::parse(input).unwrap();
            let fact = NetworkServiceOrigin::from_selected_url(&selected).unwrap();
            // The existing URL origin author is the independent semantic
            // oracle; its allocations are confined to this test.
            assert_eq!(fact.as_str(), selected.origin().ascii_serialization());
        }
        assert_eq!(origin("http://[::1]:80").as_str(), "http://[::1]");
        assert_eq!(
            origin("https://bücher.example").as_str(),
            "https://xn--bcher-kva.example"
        );
        assert_ne!(origin("http://[::1]"), origin("https://[::1]"));
    }

    #[test]
    fn origin_projection_borrows_the_selected_url_after_original_mutation() {
        let mut selected = url::Url::parse("https://before.example/path").unwrap();
        selected.set_host(Some("AFTER.example")).unwrap();
        selected.set_port(Some(8443)).unwrap();
        selected.set_username("private-user").unwrap();
        selected.set_password(Some("private-password")).unwrap();
        selected.set_query(Some("credential=private-query"));
        let fact = NetworkServiceOrigin::from_selected_url(&selected).unwrap();
        assert_eq!(fact.as_str(), "https://after.example:8443");
        assert!(!format!("{fact:?}").contains("private"));
    }

    #[test]
    fn unsupported_parsed_routes_have_no_default_network_domain() {
        for input in [
            "file:///tmp/table",
            "s3://bucket/key",
            "ftp://example.org/path",
            "data:text/plain,private",
        ] {
            let selected = url::Url::parse(input).unwrap();
            assert_eq!(
                NetworkServiceOrigin::from_selected_url(&selected),
                Err(DependencyFactError::InvalidNetworkOrigin)
            );
        }
    }

    #[test]
    fn accepted_origin_limit_includes_every_port_width_before_output_allocation() {
        for port in [
            None,
            Some(0u16),
            Some(9),
            Some(10),
            Some(99),
            Some(100),
            Some(999),
            Some(1000),
            Some(9999),
            Some(10000),
            Some(65535),
        ] {
            let suffix = port.map_or_else(String::new, |port| format!(":{port}"));
            let overhead = "https://".len() + suffix.len();
            let host = "a".repeat(NetworkServiceOrigin::MAX_BYTES - overhead);
            // url 2.5.7 intentionally ignores DNS length in its URL host
            // parser. No DNS lookup or network operation occurs here.
            let selected = url::Url::parse(&format!("https://{host}{suffix}/path")).unwrap();
            let fact = NetworkServiceOrigin::from_selected_url(&selected).unwrap();
            assert_eq!(fact.as_str().len(), NetworkServiceOrigin::MAX_BYTES);
            let too_long = url::Url::parse(&format!("https://{host}a{suffix}/path")).unwrap();
            assert_eq!(
                NetworkServiceOrigin::from_selected_url(&too_long),
                Err(DependencyFactError::EndpointTooLong)
            );
        }
    }

    #[test]
    fn large_non_origin_parts_remain_with_the_original_url_owner() {
        let path = "x".repeat(32 * 1024);
        let selected = url::Url::parse(&format!(
            "https://user:password@example.org/{path}?credential=secret#private"
        ))
        .unwrap();
        let fact = NetworkServiceOrigin::from_selected_url(&selected).unwrap();
        assert_eq!(fact.as_str(), "https://example.org");
        assert_eq!(
            format!("{fact:?}"),
            "NetworkServiceOrigin(\"https://example.org\")"
        );
    }
    #[test]
    fn borrowed_selected_views_share_the_exact_original_canonical_author() {
        for input in [
            "HTTP://user:secret@EXAMPLE.org:80/path?q=secret#private",
            "https://EXAMPLE.org:443/other",
            "http://[::1]:65535/path",
            "https://bücher.example:8443/",
            "https://example.org:0/",
        ] {
            let selected = url::Url::parse(input).unwrap();
            let view = SelectedNetworkOrigin::from_selected_url(&selected).unwrap();
            let fact = view.into_owned();
            assert!(fact.matches_selected(view));
            assert_eq!(fact.as_str(), selected.origin().ascii_serialization());
            for different in ["https://different.example", "http://different.example:8443"] {
                let different = url::Url::parse(different).unwrap();
                assert!(!fact.matches_selected(
                    SelectedNetworkOrigin::from_selected_url(&different).unwrap()
                ));
            }
            let detail = format!("{view:?}");
            assert!(!detail.contains("secret") && !detail.contains("private"));
        }
        let owned = origin("https://example.org:8443/path");
        for input in [
            "http://example.org:8443/path",
            "https://example.org/path",
            "https://example.org.evil:8443/",
        ] {
            let selected = url::Url::parse(input).unwrap();
            assert!(
                !owned
                    .matches_selected(SelectedNetworkOrigin::from_selected_url(&selected).unwrap())
            );
        }
    }

    #[test]
    fn typed_selected_hosts_match_the_original_url_origin_without_input_reparsing() {
        for input in [
            "http://EXAMPLE.org:80/path?token=private",
            "https://EXAMPLE.org:443/other#private",
            "http://127.0.0.1:8080/path",
            "https://[2001:0DB8::1]:443/path",
            "http://[::ffff:192.0.2.1]:80/path",
            "https://[::ffff:192.0.2.1]:8443/path",
            "https://xn--bcher-kva.example:443/path",
            "https://example.org:0/path",
            "https://example.org:65535/path",
        ] {
            let selected = url::Url::parse(input).unwrap();
            let scheme = if selected.scheme() == "https" {
                NetworkServiceScheme::Https
            } else {
                NetworkServiceScheme::Http
            };
            let view = SelectedNetworkOrigin::from_selected_host(
                scheme,
                selected.host().unwrap(),
                selected.port_or_known_default().unwrap(),
            )
            .unwrap();
            let fact = view.into_owned();
            assert_eq!(fact.as_str(), selected.origin().ascii_serialization());
            assert!(fact.matches_selected(view));
            assert!(
                fact.matches_selected(SelectedNetworkOrigin::from_selected_url(&selected).unwrap())
            );
            assert_eq!(
                fact,
                NetworkServiceOrigin::from_selected_url(&selected).unwrap()
            );
        }
    }

    #[test]
    fn typed_domain_input_rejects_non_origin_bytes_and_preserves_exact_output_limit() {
        for host in [
            "",
            "UPPER.example",
            "user@host",
            "host/path",
            "host?token=x",
            "host#token",
            "host:443",
            "host%2eexample",
            " host",
            "host\n",
            "[::1]",
            "bücher.example",
        ] {
            assert_eq!(
                SelectedNetworkOrigin::from_selected_host(
                    NetworkServiceScheme::Https,
                    url::Host::Domain(host),
                    443
                )
                .unwrap_err(),
                DependencyFactError::InvalidNetworkOrigin
            );
        }
        for port in [443, 8443, 0, 65535] {
            let suffix = if port == 443 {
                String::new()
            } else {
                format!(":{port}")
            };
            let host =
                "a".repeat(NetworkServiceOrigin::MAX_BYTES - "https://".len() - suffix.len());
            let view = SelectedNetworkOrigin::from_selected_host(
                NetworkServiceScheme::Https,
                url::Host::Domain(&host),
                port,
            )
            .unwrap();
            let selected =
                url::Url::parse(&format!("https://{host}{suffix}/not-retained")).unwrap();
            assert_eq!(
                view.into_owned().as_str(),
                selected.origin().ascii_serialization()
            );
            let too_long = format!("{host}a");
            assert_eq!(
                SelectedNetworkOrigin::from_selected_host(
                    NetworkServiceScheme::Https,
                    url::Host::Domain(&too_long),
                    port
                )
                .unwrap_err(),
                DependencyFactError::EndpointTooLong
            );
        }
        let selected = SelectedNetworkOrigin::from_selected_host(
            NetworkServiceScheme::Https,
            url::Host::Domain("selected.example"),
            443,
        )
        .unwrap()
        .into_owned();
        let alias = SelectedNetworkOrigin::from_selected_host(
            NetworkServiceScheme::Https,
            url::Host::Domain("alias.example"),
            443,
        )
        .unwrap()
        .into_owned();
        assert_ne!(selected, alias, "DNS alias equivalence is not guessed");
    }
}

#[cfg(test)]
mod thrift_service_origin_tests {
    use super::*;
    #[test]
    fn selected_thrift_preserves_actual_port_and_shared_service_domain() {
        let framed = ThriftServiceOrigin::from_selected_address("HMS.Example:9083").unwrap();
        let buffered = SelectedThriftServiceOrigin::from_selected_address("hms.example:9083")
            .unwrap()
            .into_owned();
        assert_eq!(framed, buffered);
        assert_eq!(framed.as_str(), "thrift://hms.example:9083");
        assert_ne!(
            framed,
            ThriftServiceOrigin::from_selected_address("hms.example:9084").unwrap()
        );
        assert_eq!(
            ThriftServiceOrigin::from_selected_address("[2001:db8::1]:19083")
                .unwrap()
                .as_str(),
            "thrift://[2001:db8::1]:19083"
        );
    }
    #[test]
    fn thrift_requires_selected_address_without_defaults_or_http_reinterpretation() {
        for address in [
            "hms.example",
            "hms.example:",
            "hms.example:0",
            "hms.example:65536",
            "thrift://hms.example:9083",
            "http://hms.example:9083",
            "hms.example:9083/path",
            "u@hms.example:9083",
        ] {
            assert!(
                SelectedThriftServiceOrigin::from_selected_address(address).is_err(),
                "{address}"
            );
        }
    }
}

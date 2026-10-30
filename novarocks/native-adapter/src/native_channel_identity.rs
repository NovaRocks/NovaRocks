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

//! Allocation-free exact identity for the closed BE-origin channel domains.

use std::io;
use std::net::IpAddr;

use novarocks_proto_codec::native_rpc::{NativeRpcMethod, NativeTrafficClass};
use novarocks_types::{BackendProcessId, NativeEndpoint, NativeReferenceHost};

const MAX_HOST_BYTES: usize = 253;

/// Method routing stays exact in the manifest; the three unit methods share
/// one physical Exchange connection owner instead of multiplying peer quotas.
pub(crate) const fn connection_owner_method(method: NativeRpcMethod) -> NativeRpcMethod {
    match method {
        NativeRpcMethod::RegisterExchangeDemand | NativeRpcMethod::SubscribeExchangeGrants => {
            NativeRpcMethod::DeliverExchangeUnit
        }
        _ => method,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum InlineNativeReferenceHostKind {
    Ipv4,
    Ipv6,
    Dns,
}

/// Inline storage retains exact endpoint equality, rather than a hash receipt.
/// Its enclosing original owner must prepay this actual Rust layout. This value
/// owns no allocation, transport capability, membership fact or generation.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) struct InlineNativeChannelIdentity {
    peer: Option<BackendProcessId>,
    host: [u8; MAX_HOST_BYTES],
    host_len: u8,
    port: u16,
    reference_host_kind: InlineNativeReferenceHostKind,
    method: NativeRpcMethod,
}

impl InlineNativeChannelIdentity {
    /// Borrow and copy only validated canonical bytes; no endpoint clone occurs.
    pub(crate) fn from_parts(
        peer: Option<BackendProcessId>,
        endpoint: &NativeEndpoint,
        method: NativeRpcMethod,
    ) -> io::Result<Self> {
        match (method.contract().traffic, peer) {
            (NativeTrafficClass::Exchange | NativeTrafficClass::RuntimeFilter, Some(peer)) => {
                BackendProcessId::try_from_uuid(peer.as_uuid())
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            }
            (NativeTrafficClass::Membership, None) => {}
            _ => return Err(io::ErrorKind::InvalidInput.into()),
        }
        let source = endpoint.host().as_bytes();
        if source.is_empty() || source.len() > MAX_HOST_BYTES || !source.is_ascii() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let reference_host_kind = match endpoint.reference_host() {
            NativeReferenceHost::Ip(IpAddr::V4(_)) => InlineNativeReferenceHostKind::Ipv4,
            NativeReferenceHost::Ip(IpAddr::V6(_)) => InlineNativeReferenceHostKind::Ipv6,
            NativeReferenceHost::Dns(_) => InlineNativeReferenceHostKind::Dns,
        };
        let mut host = [0; MAX_HOST_BYTES];
        host[..source.len()].copy_from_slice(source);
        Ok(Self {
            peer,
            host,
            host_len: source.len() as u8,
            port: endpoint.port(),
            reference_host_kind,
            method: connection_owner_method(method),
        })
    }

    pub(crate) const fn peer(&self) -> Option<BackendProcessId> {
        self.peer
    }

    /// Nil bytes identify only the admitted Membership None domain.
    #[cfg(test)]
    pub(crate) const fn peer_bytes(&self) -> [u8; 16] {
        match self.peer {
            Some(peer) => peer.to_bytes(),
            None => [0; 16],
        }
    }

    pub(crate) const fn method(&self) -> NativeRpcMethod {
        self.method
    }

    #[cfg(test)]
    pub(crate) fn host_bytes(&self) -> &[u8] {
        &self.host[..usize::from(self.host_len)]
    }

    #[cfg(test)]
    pub(crate) fn host(&self) -> &str {
        // The private constructor copies only canonical ASCII host bytes.
        std::str::from_utf8(self.host_bytes()).expect("validated inline Native host")
    }

    #[cfg(test)]
    pub(crate) const fn port(&self) -> u16 {
        self.port
    }

    #[cfg(test)]
    pub(crate) const fn reference_host_kind(&self) -> InlineNativeReferenceHostKind {
        self.reference_host_kind
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_proto_codec::native_rpc::NATIVE_METHODS;
    use novarocks_types::CanonicalDnsName;

    fn peer(suffix: u8) -> BackendProcessId {
        let mut bytes = [0; 16];
        bytes[6] = 0x70;
        bytes[8] = 0x80;
        bytes[15] = suffix;
        BackendProcessId::try_from_bytes(bytes).unwrap()
    }

    #[test]
    fn exact_peer_endpoint_and_manifest_lane_remain_distinct() {
        let endpoint = NativeEndpoint::from_host_port("example.com", 9000).unwrap();
        let other_port = NativeEndpoint::from_host_port("example.com", 9001).unwrap();
        let other_host = NativeEndpoint::from_host_port("other.example.com", 9000).unwrap();
        let key = InlineNativeChannelIdentity::from_parts(
            Some(peer(1)),
            &endpoint,
            NativeRpcMethod::DeliverExchangeUnit,
        )
        .unwrap();
        for other in [
            InlineNativeChannelIdentity::from_parts(
                Some(peer(2)),
                &endpoint,
                NativeRpcMethod::DeliverExchangeUnit,
            )
            .unwrap(),
            InlineNativeChannelIdentity::from_parts(
                Some(peer(1)),
                &other_port,
                NativeRpcMethod::DeliverExchangeUnit,
            )
            .unwrap(),
            InlineNativeChannelIdentity::from_parts(
                Some(peer(1)),
                &other_host,
                NativeRpcMethod::DeliverExchangeUnit,
            )
            .unwrap(),
            InlineNativeChannelIdentity::from_parts(
                Some(peer(1)),
                &endpoint,
                NativeRpcMethod::TransmitRuntimeFilterEnvelope,
            )
            .unwrap(),
        ] {
            assert_ne!(key, other);
        }
        let copied = key;
        assert_eq!(copied, key);
        assert_eq!(key.peer(), Some(peer(1)));
        assert_eq!(key.peer_bytes(), peer(1).to_bytes());
        assert_eq!(key.host(), "example.com");
        assert_eq!(key.host_bytes(), b"example.com");
        assert_eq!(key.port(), 9000);
        assert_eq!(key.method(), NativeRpcMethod::DeliverExchangeUnit);
    }

    #[test]
    fn canonical_equivalence_matches_the_original_endpoint() {
        for (first, second) in [
            ("EXAMPLE.Com", "example.com"),
            ("2001:0db8:0:0:0:0:0:1", "2001:db8::1"),
            ("127.0.0.1", "127.0.0.1"),
        ] {
            let first = NativeEndpoint::from_host_port(first, 9010).unwrap();
            let second = NativeEndpoint::from_host_port(second, 9010).unwrap();
            assert_eq!(first, second);
            assert_eq!(
                InlineNativeChannelIdentity::from_parts(
                    Some(peer(1)),
                    &first,
                    NativeRpcMethod::DeliverExchangeUnit
                )
                .unwrap(),
                InlineNativeChannelIdentity::from_parts(
                    Some(peer(1)),
                    &second,
                    NativeRpcMethod::DeliverExchangeUnit
                )
                .unwrap(),
            );
        }
    }

    #[test]
    fn same_printed_host_does_not_erase_dns_versus_ip_identity() {
        let ip = NativeEndpoint::from_host_port("127.0.0.1", 9000).unwrap();
        let dns = NativeEndpoint::new(
            NativeReferenceHost::Dns(CanonicalDnsName::parse("127.0.0.1").unwrap()),
            9000,
        )
        .unwrap();
        assert_eq!(ip.host(), dns.host());
        assert_ne!(ip, dns);
        let ip = InlineNativeChannelIdentity::from_parts(
            Some(peer(1)),
            &ip,
            NativeRpcMethod::DeliverExchangeUnit,
        )
        .unwrap();
        let dns = InlineNativeChannelIdentity::from_parts(
            Some(peer(1)),
            &dns,
            NativeRpcMethod::DeliverExchangeUnit,
        )
        .unwrap();
        assert_ne!(ip, dns);
        assert_eq!(
            ip.reference_host_kind(),
            InlineNativeReferenceHostKind::Ipv4
        );
        assert_eq!(
            dns.reference_host_kind(),
            InlineNativeReferenceHostKind::Dns
        );
    }

    #[test]
    fn only_the_closed_outbound_peer_domains_are_admitted() {
        let endpoint = NativeEndpoint::from_host_port("example.com", 9000).unwrap();
        for entry in NATIVE_METHODS {
            let with_peer =
                InlineNativeChannelIdentity::from_parts(Some(peer(1)), &endpoint, entry.method);
            let without_peer =
                InlineNativeChannelIdentity::from_parts(None, &endpoint, entry.method);
            match entry.method {
                NativeRpcMethod::DeliverExchangeUnit
                | NativeRpcMethod::RegisterExchangeDemand
                | NativeRpcMethod::SubscribeExchangeGrants
                | NativeRpcMethod::TransmitRuntimeFilterEnvelope => {
                    assert!(with_peer.is_ok());
                    assert_eq!(
                        without_peer.unwrap_err().kind(),
                        io::ErrorKind::InvalidInput
                    );
                }
                NativeRpcMethod::AnnounceBackend => {
                    assert_eq!(with_peer.unwrap_err().kind(), io::ErrorKind::InvalidInput);
                    let membership = without_peer.unwrap();
                    assert_eq!(membership.peer(), None);
                    assert_eq!(membership.peer_bytes(), [0; 16]);
                }
                _ => {
                    assert_eq!(with_peer.unwrap_err().kind(), io::ErrorKind::InvalidInput);
                    assert_eq!(
                        without_peer.unwrap_err().kind(),
                        io::ErrorKind::InvalidInput
                    );
                }
            }
        }
    }

    #[test]
    fn maximum_canonical_dns_host_fits_without_endpoint_storage_aliases() {
        let host = format!(
            "{}.{}.{}.{}",
            "a".repeat(63),
            "b".repeat(63),
            "c".repeat(63),
            "d".repeat(61)
        );
        assert_eq!(host.len(), MAX_HOST_BYTES);
        let endpoint = NativeEndpoint::from_host_port(&host, u16::MAX).unwrap();
        let key = InlineNativeChannelIdentity::from_parts(
            Some(peer(1)),
            &endpoint,
            NativeRpcMethod::DeliverExchangeUnit,
        )
        .unwrap();
        assert_eq!(key.host_bytes(), host.as_bytes());
        assert_ne!(key.host_bytes().as_ptr(), endpoint.host().as_ptr());
        drop(endpoint);
        assert_eq!(key.host(), host);
        assert_eq!(key.port(), u16::MAX);
    }
}

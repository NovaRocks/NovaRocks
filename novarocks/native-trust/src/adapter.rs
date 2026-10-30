// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use std::{
    fmt,
    future::Future,
    io,
    net::{SocketAddr, ToSocketAddrs},
    pin::Pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll},
};

use hyper_util::rt::TokioIo;
use novarocks_types::{NativeEndpoint, NativeReferenceHost};
use rustls::{ClientConfig, ServerConfig, pki_types::ServerName};
use tokio::{net::TcpStream, sync::Semaphore};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tower::Service;

use novarocks_resource_contract::{
    DependencyFactError, NetworkServiceScheme, SelectedNetworkOrigin,
};
use novarocks_transport_io::BoxedIo;

use crate::{AutomaticTlsMaterial, NativeTlsMaterial, NativeTransportMode, NativeTrustFailureKind};

/// How many Native DNS resolutions may run at once in one process.
///
/// Each position is one blocking `getaddrinfo` call in a NovaRocks-owned
/// `spawn_blocking` closure, holding at most one resolver descriptor. The
/// count is process-local: it is not advertised and does not size any peer.
/// The resolver library's own memory is outside this bound (spec v6 §5.4.4).
pub const NATIVE_DNS_RESOLUTION_POSITIONS: usize = 4;

static PROCESS_DNS_RESOLVER: LazyLock<NativeDnsResolver> =
    LazyLock::new(|| NativeDnsResolver::new(NATIVE_DNS_RESOLUTION_POSITIONS));

/// Bounded resolution of a Native endpoint's reference host.
///
/// An IP-literal endpoint is used as is. A DNS endpoint is resolved with the
/// standard library resolver inside a closure NovaRocks hands to
/// `spawn_blocking`; the concurrency position is moved into that closure, so
/// it is returned only when the blocking call itself has returned, even if
/// the future that awaited it was cancelled first. Resolved addresses are only
/// dial targets: the reference host stays the TLS and cache identity.
#[derive(Clone)]
pub struct NativeDnsResolver {
    positions: Arc<Semaphore>,
    limit: usize,
}

impl fmt::Debug for NativeDnsResolver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeDnsResolver")
            .field("limit", &self.limit)
            .field("available", &self.positions.available_permits())
            .finish()
    }
}

impl NativeDnsResolver {
    pub fn new(positions: usize) -> Self {
        Self {
            positions: Arc::new(Semaphore::new(positions)),
            limit: positions,
        }
    }

    /// The one resolver every Native connector in this process uses.
    pub fn process() -> &'static Self {
        &PROCESS_DNS_RESOLVER
    }

    pub fn positions(&self) -> usize {
        self.limit
    }

    pub fn available_positions(&self) -> usize {
        self.positions.available_permits()
    }

    pub async fn resolve(&self, endpoint: &NativeEndpoint) -> io::Result<Vec<SocketAddr>> {
        self.resolve_with(endpoint, |host, port| {
            (host, port)
                .to_socket_addrs()
                .map(|addresses| addresses.collect())
        })
        .await
    }

    /// `lookup` is the blocking resolver call; production passes the
    /// standard library resolver. It runs only for DNS reference hosts.
    pub(crate) async fn resolve_with<F>(
        &self,
        endpoint: &NativeEndpoint,
        lookup: F,
    ) -> io::Result<Vec<SocketAddr>>
    where
        F: FnOnce(&str, u16) -> io::Result<Vec<SocketAddr>> + Send + 'static,
    {
        let port = endpoint.port();
        let host = match endpoint.reference_host() {
            NativeReferenceHost::Ip(address) => return Ok(vec![SocketAddr::new(*address, port)]),
            NativeReferenceHost::Dns(name) => name.as_str().to_owned(),
        };
        let position = Arc::clone(&self.positions)
            .acquire_owned()
            .await
            .map_err(|_| io::Error::other("native DNS resolver is closed"))?;
        let resolved = tokio::task::spawn_blocking(move || {
            // Returned when this closure returns, not when its awaiting
            // future is dropped.
            let _position = position;
            lookup(&host, port)
        })
        .await
        .map_err(|error| io::Error::other(format!("native DNS resolution panicked: {error}")))??;
        if resolved.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "native DNS resolution returned no address",
            ));
        }
        Ok(resolved)
    }
}

#[derive(Clone)]
pub struct NativeEndpointConnector {
    endpoint: NativeEndpoint,
    client_tls: Option<Arc<ClientConfig>>,
}

impl NativeEndpointConnector {
    pub fn plaintext(endpoint: NativeEndpoint) -> Self {
        Self {
            endpoint,
            client_tls: None,
        }
    }

    pub fn pem(endpoint: NativeEndpoint, material: &NativeTlsMaterial) -> Self {
        Self {
            endpoint,
            client_tls: Some(material.client_config()),
        }
    }

    pub fn automatic(
        endpoint: NativeEndpoint,
        material: &AutomaticTlsMaterial,
    ) -> Result<Self, NativeTrustFailureKind> {
        Ok(Self {
            client_tls: Some(material.client_config_for(&endpoint)?),
            endpoint,
        })
    }

    pub fn endpoint(&self) -> &NativeEndpoint {
        &self.endpoint
    }

    /// Pure selected reference-host facts from this connector's real TLS state.
    /// No URL allocation, DNS, credential acquisition or dial occurs here.
    pub fn selected_network_origin(
        &self,
    ) -> Result<SelectedNetworkOrigin<'_>, DependencyFactError> {
        Self::selected_origin_for(
            &self.endpoint,
            if self.client_tls.is_some() {
                NativeTransportMode::Pem
            } else {
                NativeTransportMode::Disabled
            },
        )
    }

    /// Project the original endpoint under the route owner's already frozen
    /// transport mode without constructing a connector or TLS configuration.
    /// This is a fact only; it neither validates TLS material nor grants I/O.
    pub fn selected_origin_for(
        endpoint: &NativeEndpoint,
        mode: NativeTransportMode,
    ) -> Result<SelectedNetworkOrigin<'_>, DependencyFactError> {
        let scheme = match mode {
            NativeTransportMode::Disabled => NetworkServiceScheme::Http,
            NativeTransportMode::Automatic | NativeTransportMode::Pem => {
                NetworkServiceScheme::Https
            }
        };
        // NativeEndpoint already normalized this exact reference identity.
        // Never replace it with a DNS result or reparse its display as a URL.
        let host = match endpoint.reference_host() {
            NativeReferenceHost::Ip(std::net::IpAddr::V4(ip)) => url::Host::Ipv4(*ip),
            NativeReferenceHost::Ip(std::net::IpAddr::V6(ip)) => url::Host::Ipv6(*ip),
            NativeReferenceHost::Dns(name) => url::Host::Domain(name.as_str()),
        };
        SelectedNetworkOrigin::from_selected_host(scheme, host, endpoint.port())
    }

    pub async fn connect(&self) -> Result<BoxedIo, NativeConnectFailure> {
        self.connect_with(NativeDnsResolver::process()).await
    }

    /// Resolve through `resolver`, then connect only to the resolved
    /// addresses. TLS still verifies the endpoint's reference host.
    pub async fn connect_with(
        &self,
        resolver: &NativeDnsResolver,
    ) -> Result<BoxedIo, NativeConnectFailure> {
        let addresses = resolver
            .resolve(&self.endpoint)
            .await
            .map_err(NativeConnectFailure::Resolution)?;
        // A refused or timed-out connect is the peer being gone, not this
        // deployment being misconfigured.
        let stream = TcpStream::connect(addresses.as_slice())
            .await
            .map_err(NativeConnectFailure::Unreachable)?;
        match &self.client_tls {
            None => Ok(Box::new(stream)),
            Some(config) => {
                let server_name = ServerName::try_from(self.endpoint.host().to_owned())
                    .map_err(|_| NativeConnectFailure::UnverifiableServerName)?;
                let stream = TlsConnector::from(config.clone())
                    .connect(server_name, stream)
                    .await
                    .map_err(NativeConnectFailure::Handshake)?;
                Ok(Box::new(stream))
            }
        }
    }
}

/// Why one attempt to reach a native endpoint produced no stream.
///
/// The classification alone is not enough to act on: "native peer is
/// unreachable" is true for a refused connection and for an exhausted
/// descriptor table, and those send an operator to opposite places. The
/// operating system already said which one it was, so this keeps that answer
/// instead of discarding it at the point that produced it.
///
/// The kept detail is an `io::Error` from `connect` or from the TLS record
/// layer. Neither carries key material, a token or a payload, so this does not
/// widen what a failure discloses.
#[derive(Debug)]
pub enum NativeConnectFailure {
    /// The endpoint's DNS reference host did not resolve to an address.
    Resolution(io::Error),
    /// The TCP connection could not be established.
    Unreachable(io::Error),
    /// The endpoint's reference host is not a name TLS can verify against.
    /// This one really is configuration.
    UnverifiableServerName,
    /// The TCP connection formed but the TLS handshake did not complete.
    Handshake(io::Error),
}

impl NativeConnectFailure {
    /// The redacted trust vocabulary this failure answers to.
    pub fn kind(&self) -> NativeTrustFailureKind {
        match self {
            Self::Resolution(_) | Self::Unreachable(_) => {
                NativeTrustFailureKind::TransportUnreachable
            }
            Self::UnverifiableServerName => NativeTrustFailureKind::TransportConfiguration,
            Self::Handshake(_) => NativeTrustFailureKind::TransportHandshake,
        }
    }
}

impl fmt::Display for NativeConnectFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resolution(error) => {
                write!(formatter, "{}: name resolution: {error}", self.kind())
            }
            Self::Unreachable(error) => write!(formatter, "{}: {error}", self.kind()),
            Self::UnverifiableServerName => write!(formatter, "{}", self.kind()),
            Self::Handshake(error) => write!(formatter, "{}: {error}", self.kind()),
        }
    }
}

impl std::error::Error for NativeConnectFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Resolution(error) | Self::Unreachable(error) | Self::Handshake(error) => {
                Some(error)
            }
            Self::UnverifiableServerName => None,
        }
    }
}

impl Service<http::Uri> for NativeEndpointConnector {
    type Response = TokioIo<BoxedIo>;
    type Error = io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: http::Uri) -> Self::Future {
        let connector = self.clone();
        Box::pin(async move {
            connector
                .connect()
                .await
                .map(TokioIo::new)
                .map_err(io::Error::other)
        })
    }
}

#[derive(Clone)]
pub struct NativeIncomingAdapter {
    mode: NativeTransportMode,
    server_tls: Option<Arc<ServerConfig>>,
}

impl NativeIncomingAdapter {
    pub fn plaintext() -> Self {
        Self {
            mode: NativeTransportMode::Disabled,
            server_tls: None,
        }
    }
    pub fn pem(material: &NativeTlsMaterial) -> Self {
        Self {
            mode: NativeTransportMode::Pem,
            server_tls: Some(material.server_config()),
        }
    }
    pub fn automatic(material: &AutomaticTlsMaterial) -> Self {
        Self {
            mode: NativeTransportMode::Automatic,
            server_tls: Some(material.server_config()),
        }
    }
    pub fn mode(&self) -> NativeTransportMode {
        self.mode
    }

    pub async fn accept(&self, stream: TcpStream) -> Result<BoxedIo, NativeTrustFailureKind> {
        match &self.server_tls {
            None => Ok(Box::new(stream)),
            Some(config) => TlsAcceptor::from(config.clone())
                .accept(stream)
                .await
                .map(|stream| Box::new(stream) as BoxedIo)
                .map_err(|_| NativeTrustFailureKind::TransportConfiguration),
        }
    }
}

#[cfg(test)]
mod dns_tests {
    use std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, Mutex, mpsc},
        time::Duration,
    };

    use novarocks_types::NativeEndpoint;

    use super::NativeDnsResolver;

    const WATCHDOG: Duration = Duration::from_secs(5);

    async fn available_becomes(resolver: &NativeDnsResolver, expected: usize) {
        tokio::time::timeout(WATCHDOG, async {
            while resolver.available_positions() != expected {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("DNS positions reach the expected count");
    }

    type Lookup = Box<dyn FnOnce(&str, u16) -> std::io::Result<Vec<SocketAddr>> + Send>;

    /// A blocking lookup that reports entry and returns only when released.
    fn held_lookup() -> (Lookup, mpsc::Receiver<String>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let release_rx = Arc::new(Mutex::new(release_rx));
        let lookup = move |host: &str, port: u16| {
            entered_tx.send(host.to_owned()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(vec![SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 9)),
                port,
            )])
        };
        (Box::new(lookup), entered_rx, release_tx)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dns_closure_keeps_its_position_until_the_blocking_lookup_returns() {
        let resolver = NativeDnsResolver::new(1);
        let endpoint: NativeEndpoint = "be-0.resolver.test:9070".parse().unwrap();
        let (lookup, entered, release) = held_lookup();
        let waiting = {
            let resolver = resolver.clone();
            let endpoint = endpoint.clone();
            tokio::spawn(async move { resolver.resolve_with(&endpoint, lookup).await })
        };
        let host = tokio::task::spawn_blocking(move || entered.recv_timeout(WATCHDOG))
            .await
            .unwrap()
            .expect("the blocking lookup started");
        assert_eq!(host, "be-0.resolver.test");
        assert_eq!(resolver.available_positions(), 0);
        // Cancel the future that awaited the closure: the closure is still
        // running, so its position must not come back yet.
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            resolver.available_positions(),
            0,
            "a cancelled awaiter cannot return the position of a running lookup"
        );
        release.send(()).unwrap();
        available_becomes(&resolver, 1).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dns_positions_bound_concurrent_lookups_and_release_on_return() {
        let resolver = NativeDnsResolver::new(1);
        let endpoint: NativeEndpoint = "be-1.resolver.test:9070".parse().unwrap();
        let (lookup, entered, release) = held_lookup();
        let first = {
            let resolver = resolver.clone();
            let endpoint = endpoint.clone();
            tokio::spawn(async move { resolver.resolve_with(&endpoint, lookup).await })
        };
        tokio::task::spawn_blocking(move || entered.recv_timeout(WATCHDOG))
            .await
            .unwrap()
            .unwrap();
        let second_entered = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let observed = Arc::clone(&second_entered);
        let mut second = {
            let resolver = resolver.clone();
            let endpoint = endpoint.clone();
            tokio::spawn(async move {
                resolver
                    .resolve_with(&endpoint, move |_, port| {
                        observed.store(true, std::sync::atomic::Ordering::SeqCst);
                        Ok(vec![SocketAddr::new(
                            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 10)),
                            port,
                        )])
                    })
                    .await
            })
        };
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut second)
                .await
                .is_err(),
            "the second lookup waits for a position"
        );
        assert!(!second_entered.load(std::sync::atomic::Ordering::SeqCst));
        release.send(()).unwrap();
        let first = first.await.unwrap().unwrap();
        assert_eq!(first[0].ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 9)));
        let second = tokio::time::timeout(WATCHDOG, second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(second[0].port(), 9070);
        available_becomes(&resolver, 1).await;
    }

    #[tokio::test]
    async fn ip_literal_endpoints_skip_resolution_and_take_no_position() {
        let resolver = NativeDnsResolver::new(0);
        for (text, expected) in [
            ("127.0.0.1:9070", "127.0.0.1:9070"),
            ("[::1]:9071", "[::1]:9071"),
        ] {
            let endpoint: NativeEndpoint = text.parse().unwrap();
            let resolved = resolver
                .resolve_with(&endpoint, |_, _| -> std::io::Result<Vec<SocketAddr>> {
                    panic!("an IP literal must not reach the resolver")
                })
                .await
                .unwrap();
            assert_eq!(resolved, vec![expected.parse::<SocketAddr>().unwrap()]);
        }
    }

    #[tokio::test]
    async fn an_empty_resolution_is_an_error_not_an_empty_dial() {
        let resolver = NativeDnsResolver::new(1);
        let endpoint: NativeEndpoint = "nothing.resolver.test:9070".parse().unwrap();
        let error = resolver
            .resolve_with(&endpoint, |_, _| Ok(Vec::new()))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
        available_becomes(&resolver, 1).await;
    }
}

#[cfg(test)]
mod selected_origin_tests {
    use super::NativeEndpointConnector;
    use crate::{
        AutomaticTlsMaterial, DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust,
        PemTransportMaterial, ValidatedSharedSecret,
    };
    use novarocks_secret::SecretValue;
    use novarocks_types::NativeEndpoint;

    #[test]
    fn original_native_endpoint_projection_matches_url_origin_without_io() {
        for (host, port) in [
            ("127.0.0.1", 80),
            ("EXAMPLE.org", 8443),
            ("2001:0DB8:0:0::1", 443),
            ("::ffff:192.0.2.1", 8443),
        ] {
            let endpoint = NativeEndpoint::from_host_port(host, port).unwrap();
            for mode in [
                NativeTransportMode::Disabled,
                NativeTransportMode::Automatic,
                NativeTransportMode::Pem,
            ] {
                let scheme = if mode == NativeTransportMode::Disabled {
                    "http"
                } else {
                    "https"
                };
                let selected =
                    NativeEndpointConnector::selected_origin_for(&endpoint, mode).unwrap();
                let oracle = url::Url::parse(&format!(
                    "{scheme}://{endpoint}/path?token=not-retained#private"
                ))
                .unwrap();
                assert_eq!(
                    selected.into_owned().as_str(),
                    oracle.origin().ascii_serialization()
                );
            }
            let connector = NativeEndpointConnector::plaintext(endpoint.clone());
            assert_eq!(
                connector.selected_network_origin().unwrap().into_owned(),
                NativeEndpointConnector::selected_origin_for(
                    &endpoint,
                    NativeTransportMode::Disabled
                )
                .unwrap()
                .into_owned()
            );
        }
        let first = NativeEndpoint::from_host_port("same.example", 19060).unwrap();
        let alias = NativeEndpoint::from_host_port("alias.example", 19060).unwrap();
        assert_ne!(
            NativeEndpointConnector::selected_origin_for(&first, NativeTransportMode::Disabled)
                .unwrap()
                .into_owned(),
            NativeEndpointConnector::selected_origin_for(&alias, NativeTransportMode::Disabled)
                .unwrap()
                .into_owned()
        );
        assert!(NativeEndpoint::from_host_port("user@host/path", 19060).is_err());
    }

    #[test]
    fn actual_plaintext_automatic_and_pem_connector_state_owns_the_scheme_without_io() {
        let endpoint = NativeEndpoint::from_host_port("LOCALHOST", 443).unwrap();
        let trust = NativeTrust::new(
            DeploymentId::parse("selected-origin-pure").unwrap(),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .unwrap(),
            NativeCallerSubject::parse("fe@localhost:19040").unwrap(),
            NativeTransportMode::Automatic,
        );
        let material = AutomaticTlsMaterial::for_endpoint(trust, endpoint.clone()).unwrap();
        let automatic = NativeEndpointConnector::automatic(endpoint.clone(), &material).unwrap();
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let certificate = certified.cert.pem();
        let pem = PemTransportMaterial::new(
            certificate.clone().into_bytes(),
            certified.key_pair.serialize_pem().into_bytes(),
            certificate.into_bytes(),
        )
        .unwrap()
        .tls_material()
        .unwrap();
        let pem = NativeEndpointConnector::pem(endpoint.clone(), &pem);
        let plaintext = NativeEndpointConnector::plaintext(endpoint);
        assert_eq!(
            automatic
                .selected_network_origin()
                .unwrap()
                .into_owned()
                .as_str(),
            "https://localhost"
        );
        assert_eq!(
            pem.selected_network_origin().unwrap().into_owned().as_str(),
            "https://localhost"
        );
        assert_eq!(
            plaintext
                .selected_network_origin()
                .unwrap()
                .into_owned()
                .as_str(),
            "http://localhost:443"
        );
        // No connect(), DNS resolver, listener, runtime, service or socket exists.
    }
}

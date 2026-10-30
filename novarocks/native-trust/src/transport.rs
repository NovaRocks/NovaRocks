// Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.
// See the NOTICE file distributed with this work for additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

use std::{fmt, sync::Arc};

use ed25519_dalek::{SigningKey, pkcs8::EncodePrivateKey};
use novarocks_types::NativeEndpoint;
use rcgen::{CertificateParams, ExtendedKeyUsagePurpose, KeyPair, KeyUsagePurpose, PKCS_ED25519};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    client::Resumption,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, pem::PemObject},
    version::TLS13,
};

use crate::{NativeTrust, NativeTrustFailureKind};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NativeTransportMode {
    Disabled,
    Automatic,
    Pem,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct NativeTransportProfile {
    mode: NativeTransportMode,
}

impl NativeTransportProfile {
    pub fn new(mode: NativeTransportMode) -> Self {
        Self { mode }
    }
    pub fn mode(&self) -> NativeTransportMode {
        self.mode
    }
    pub fn uses_tls(&self) -> bool {
        self.mode != NativeTransportMode::Disabled
    }
    pub fn tls13_only(&self) -> bool {
        self.uses_tls()
    }
    pub fn alpn_protocols(&self) -> &'static [&'static [u8]] {
        if self.uses_tls() { &[b"h2"] } else { &[] }
    }
    pub fn permits_early_data(&self) -> bool {
        false
    }
    pub fn permits_resumption(&self) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct PemTransportMaterial {
    certificate_chain: Vec<u8>,
    private_key: Vec<u8>,
    trust_roots: Vec<u8>,
}

impl PemTransportMaterial {
    pub fn new(
        certificate_chain: Vec<u8>,
        private_key: Vec<u8>,
        trust_roots: Vec<u8>,
    ) -> Result<Self, NativeTrustFailureKind> {
        let material = Self {
            certificate_chain,
            private_key,
            trust_roots,
        };
        material.parse_parts().map(|_| material)
    }

    pub fn tls_material(&self) -> Result<NativeTlsMaterial, NativeTrustFailureKind> {
        let (certificates, private_key, roots) = self.parse_parts()?;
        NativeTlsMaterial::from_parts(certificates, private_key, roots)
    }

    fn parse_parts(
        &self,
    ) -> Result<
        (
            Vec<CertificateDer<'static>>,
            PrivateKeyDer<'static>,
            RootCertStore,
        ),
        NativeTrustFailureKind,
    > {
        let certificates = CertificateDer::pem_slice_iter(&self.certificate_chain)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        if certificates.is_empty() {
            return Err(NativeTrustFailureKind::TransportConfiguration);
        }
        let private_key = PrivateKeyDer::from_pem_slice(&self.private_key)
            .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        let roots = CertificateDer::pem_slice_iter(&self.trust_roots)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        if roots.is_empty() {
            return Err(NativeTrustFailureKind::TransportConfiguration);
        }
        let mut store = RootCertStore::empty();
        for root in roots {
            store
                .add(root)
                .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        }
        Ok((certificates, private_key, store))
    }
}

impl fmt::Debug for PemTransportMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PemTransportMaterial(REDACTED)")
    }
}

#[derive(Clone)]
pub struct NativeTlsMaterial {
    server: Arc<ServerConfig>,
    client: Arc<ClientConfig>,
}

impl NativeTlsMaterial {
    fn from_parts(
        certificates: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
        roots: RootCertStore,
    ) -> Result<Self, NativeTrustFailureKind> {
        let mut server = ServerConfig::builder_with_protocol_versions(&[&TLS13])
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        server.alpn_protocols = vec![b"h2".to_vec()];
        server.max_early_data_size = 0;
        server.send_tls13_tickets = 0;
        let mut client = ClientConfig::builder_with_protocol_versions(&[&TLS13])
            .with_root_certificates(roots)
            .with_no_client_auth();
        client.alpn_protocols = vec![b"h2".to_vec()];
        client.enable_early_data = false;
        client.resumption = Resumption::disabled();
        Ok(Self {
            server: Arc::new(server),
            client: Arc::new(client),
        })
    }
    pub(crate) fn server_config(&self) -> Arc<ServerConfig> {
        self.server.clone()
    }
    pub(crate) fn client_config(&self) -> Arc<ClientConfig> {
        self.client.clone()
    }
}

impl fmt::Debug for NativeTlsMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("NativeTlsMaterial(REDACTED)")
    }
}

#[derive(Clone, Debug)]
pub struct AutomaticTlsMaterial {
    local_endpoint: NativeEndpoint,
    local: NativeTlsMaterial,
    trust: NativeTrust,
}

impl AutomaticTlsMaterial {
    pub fn for_endpoint(
        trust: NativeTrust,
        local_endpoint: NativeEndpoint,
    ) -> Result<Self, NativeTrustFailureKind> {
        let local = Self::material_for_seed(trust.automatic_tls_seed(), &local_endpoint)?;
        Ok(Self {
            local_endpoint,
            local,
            trust,
        })
    }
    pub fn local_endpoint(&self) -> &NativeEndpoint {
        &self.local_endpoint
    }
    pub(crate) fn server_config(&self) -> Arc<ServerConfig> {
        self.local.server_config()
    }
    pub(crate) fn client_config_for(
        &self,
        remote: &NativeEndpoint,
    ) -> Result<Arc<ClientConfig>, NativeTrustFailureKind> {
        Self::material_for_seed(self.trust.automatic_tls_seed(), remote)
            .map(|material| material.client_config())
    }

    fn material_for_seed(
        seed: &[u8; 32],
        endpoint: &NativeEndpoint,
    ) -> Result<NativeTlsMaterial, NativeTrustFailureKind> {
        let signing_key = SigningKey::from_bytes(seed);
        let key_der = signing_key
            .to_pkcs8_der()
            .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        let key_pair = KeyPair::from_pkcs8_der_and_sign_algo(
            &PrivatePkcs8KeyDer::from(key_der.as_bytes().to_vec()),
            &PKCS_ED25519,
        )
        .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        let mut parameters = CertificateParams::new(vec![endpoint.host().to_string()])
            .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        parameters.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        parameters.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let certificate = parameters
            .self_signed(&key_pair)
            .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        let certificate_der = CertificateDer::from(certificate.der().to_vec());
        let mut roots = RootCertStore::empty();
        roots
            .add(certificate_der.clone())
            .map_err(|_| NativeTrustFailureKind::TransportConfiguration)?;
        NativeTlsMaterial::from_parts(
            vec![certificate_der],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_der.as_bytes().to_vec())),
            roots,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use http::Request;
    use novarocks_secret::SecretValue;
    use novarocks_types::NativeEndpoint;
    use rcgen::generate_simple_self_signed;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    use super::{AutomaticTlsMaterial, PemTransportMaterial};
    use crate::{
        ManualClock, NativeCallerSubject, NativeTransportMode, NativeTrust, NativeTrustFailureKind,
        ValidatedSharedSecret,
        adapter::{NativeEndpointConnector, NativeIncomingAdapter},
        deployment::DeploymentId,
    };

    fn trust() -> NativeTrust {
        NativeTrust::new_with_clock(
            DeploymentId::parse("analytics-prod").unwrap(),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .unwrap(),
            NativeCallerSubject::parse("be@localhost:9080").unwrap(),
            NativeTransportMode::Automatic,
            Arc::new(ManualClock::new(1_700_000_000)),
        )
    }

    #[tokio::test]
    async fn automatic_tls_is_tls13_h2_and_binds_the_exact_reference_identity() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let endpoint: NativeEndpoint = format!("localhost:{port}").parse().unwrap();
        let material = AutomaticTlsMaterial::for_endpoint(trust(), endpoint.clone()).unwrap();
        let server = NativeIncomingAdapter::automatic(&material);
        let client = NativeEndpointConnector::automatic(endpoint, &material).unwrap();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let stream = server.accept(stream).await.unwrap();
            let mut connection = h2::server::handshake(stream).await.unwrap();
            let (request, mut respond) = connection.accept().await.unwrap().unwrap();
            assert_eq!(request.uri().path(), "/native");
            let response = http::Response::builder().status(200).body(()).unwrap();
            let mut send = respond.send_response(response, false).unwrap();
            send.send_data(Bytes::from_static(b"h2ok"), true).unwrap();
            // Continue polling the connection so its response frames flush.
            while connection.accept().await.is_some() {}
        });
        let stream = client.connect().await.unwrap();
        let (mut sender, connection) = h2::client::handshake(stream).await.unwrap();
        let driver = tokio::spawn(async move {
            // The test drops the TLS peer after its one response. Rustls
            // reports missing close_notify as an IO error; it is not an H2
            // protocol or authentication acceptance signal.
            let _ = connection.await;
        });
        let request = Request::builder()
            .method("POST")
            .uri("/native")
            .body(())
            .unwrap();
        let (response, _) = sender.send_request(request, true).unwrap();
        assert_eq!(response.await.unwrap().status(), 200);
        drop(sender);
        let _ = driver.await;
        server_task.await.unwrap();
        assert_eq!(
            material.server_config().alpn_protocols,
            vec![b"h2".to_vec()]
        );
        assert_eq!(
            material
                .client_config_for(material.local_endpoint())
                .unwrap()
                .alpn_protocols,
            vec![b"h2".to_vec()]
        );
    }

    /// A connection that never formed must say why, in the operating system's
    /// own words.
    ///
    /// The classification alone does not tell an operator what to do: a refused
    /// connection and an exhausted descriptor table are both "native peer is
    /// unreachable", and they send them to opposite places.
    #[tokio::test]
    async fn a_refused_connection_keeps_the_operating_system_reason() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let endpoint: NativeEndpoint = format!("127.0.0.1:{port}").parse().unwrap();
        let failure = NativeEndpointConnector::plaintext(endpoint)
            .connect()
            .await
            .err()
            .expect("nothing is listening on a port that was just released");

        let source = std::error::Error::source(&failure)
            .expect("a refused connection has an operating system reason")
            .downcast_ref::<std::io::Error>()
            .expect("that reason is an io error");
        assert_eq!(
            source.kind(),
            std::io::ErrorKind::ConnectionRefused,
            "unexpected reason: {failure}"
        );
        assert!(
            format!("{failure}").contains("native peer is unreachable"),
            "the classification must survive alongside the reason: {failure}"
        );
        assert_eq!(failure.kind(), NativeTrustFailureKind::TransportUnreachable);
    }

    #[tokio::test]
    async fn automatic_tls_rejects_the_wrong_reference_leaf() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let endpoint: NativeEndpoint = format!("localhost:{port}").parse().unwrap();
        let wrong: NativeEndpoint = format!("127.0.0.1:{port}").parse().unwrap();
        let material = AutomaticTlsMaterial::for_endpoint(trust(), endpoint.clone()).unwrap();
        let server = NativeIncomingAdapter::automatic(&material);
        let client = NativeEndpointConnector::automatic(wrong, &material).unwrap();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            assert!(server.accept(stream).await.is_err());
        });
        assert!(client.connect().await.is_err());
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn pem_material_requires_private_roots_and_completes_tls13_loopback() {
        assert!(PemTransportMaterial::new(b"not pem".to_vec(), vec![], vec![]).is_err());
        let certified = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let certificate = certified.cert.pem();
        let private_key = certified.key_pair.serialize_pem();
        let material = PemTransportMaterial::new(
            certificate.clone().into_bytes(),
            private_key.into_bytes(),
            certificate.into_bytes(),
        )
        .unwrap();
        let tls = material.tls_material().unwrap();
        assert_eq!(tls.server_config().alpn_protocols, vec![b"h2".to_vec()]);
        assert!(!tls.client_config().enable_early_data);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = NativeIncomingAdapter::pem(&tls);
        let client =
            NativeEndpointConnector::pem(format!("localhost:{port}").parse().unwrap(), &tls);
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = server.accept(stream).await.unwrap();
            let mut value = [0_u8; 1];
            stream.read_exact(&mut value).await.unwrap();
            assert_eq!(value, [7]);
        });
        let mut stream = client.connect().await.unwrap();
        stream.write_all(&[7]).await.unwrap();
        server_task.await.unwrap();
    }

    #[test]
    fn pem_material_rejects_missing_and_truncated_private_keys() {
        let certified = generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
        let certificate = certified.cert.pem();
        let missing_key = PemTransportMaterial::new(
            certificate.clone().into_bytes(),
            certificate.clone().into_bytes(),
            certificate.clone().into_bytes(),
        );
        assert_eq!(
            missing_key.unwrap_err(),
            NativeTrustFailureKind::TransportConfiguration
        );
        let private_key = certified.key_pair.serialize_pem();
        let truncated_key = private_key.lines().take(2).collect::<Vec<_>>().join("\n");
        assert!(truncated_key.starts_with("-----BEGIN "));
        assert!(!truncated_key.contains("-----END "));
        let truncated = PemTransportMaterial::new(
            certificate.clone().into_bytes(),
            truncated_key.into_bytes(),
            certificate.into_bytes(),
        );
        assert_eq!(
            truncated.unwrap_err(),
            NativeTrustFailureKind::TransportConfiguration
        );
    }
}

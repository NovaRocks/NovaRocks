// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership and limitations under the License.

use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, Mac};
use http::HeaderMap;
use novarocks_types::{BackendProcessId, FrontendProcessId};
use serde::{Deserialize, Deserializer};
use sha2::Sha256;
use tonic::{Request, Status, service::Interceptor};
use tower::{Layer, Service};

use crate::{DeploymentId, NativeTransportMode, NativeTrustFailureKind, ValidatedSharedSecret};

const JWT_KEY_INFO: &[u8] = b"novarocks/native-trust/jwt-hs256-key/v1";
const AUTOMATIC_TLS_KEY_INFO: &[u8] = b"novarocks/native-trust/automatic-tls-key/v1";
const MAX_TOKEN_BYTES: usize = 2048;
const MAX_CLOCK_SKEW_SECONDS: i64 = 5 * 60;
const TOKEN_REFRESH_THRESHOLD_SECONDS: i64 = 5 * 60;
/// Native caller tokens are intentionally short-lived and have no grace after expiry.
pub const TOKEN_LIFETIME_SECONDS: i64 = 6 * 60;

type HmacSha256 = Hmac<Sha256>;

/// Injectable wall clock boundary. It has no authority other than providing a
/// Unix timestamp; callers cannot use it to disable token verification.
pub trait NativeTrustClock: Send + Sync + fmt::Debug {
    fn unix_seconds(&self) -> i64;
}

/// Production wall clock.
#[derive(Debug)]
pub struct SystemClock;

impl NativeTrustClock for SystemClock {
    fn unix_seconds(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or_default()
    }
}

/// Deterministic clock for tests and controlled integration fixtures.
#[derive(Debug)]
pub struct ManualClock(Mutex<i64>);

impl ManualClock {
    pub fn new(unix_seconds: i64) -> Self {
        Self(Mutex::new(unix_seconds))
    }

    pub fn set_unix_seconds(&self, unix_seconds: i64) {
        *self
            .0
            .lock()
            .expect("manual native trust clock mutex poisoned") = unix_seconds;
    }
}

impl NativeTrustClock for ManualClock {
    fn unix_seconds(&self) -> i64 {
        *self
            .0
            .lock()
            .expect("manual native trust clock mutex poisoned")
    }
}

/// A bounded diagnostic subject. It is proof of neither topology membership
/// nor authorization; its only public use is local diagnostics after a token
/// has already authenticated the deployment.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct NativeCallerSubject(String);

impl NativeCallerSubject {
    pub fn parse(value: impl Into<String>) -> Result<Self, NativeTrustFailureKind> {
        let value = value.into();
        let bytes = value.as_bytes();
        if !(1..=256).contains(&bytes.len())
            || !bytes.iter().all(|byte| (0x21..=0x7e).contains(byte))
            || bytes.iter().any(|byte| matches!(*byte, b'"' | b'\\'))
        {
            return Err(NativeTrustFailureKind::InvalidCallerSubject);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for NativeCallerSubject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("NativeCallerSubject")
            .field(&self.0)
            .finish()
    }
}

impl fmt::Display for NativeCallerSubject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// An exact process identity supplied by the role's composition owner.
///
/// This is a signed caller assertion, not topology membership, health, or
/// permission to operate on a query context. It neither mints an identity nor
/// derives a role from the diagnostic subject.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum NativeProcessIdentity {
    Frontend(FrontendProcessId),
    Backend(BackendProcessId),
}

/// Successful deployment authentication and its optional signed process claim.
/// Membership and operation authorization remain with their application owners.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuthenticatedNativeCaller {
    subject: NativeCallerSubject,
    process_identity: Option<NativeProcessIdentity>,
}

impl AuthenticatedNativeCaller {
    pub fn subject(&self) -> &NativeCallerSubject {
        &self.subject
    }

    /// Legacy unbound tokens carry no process assertion. A consumer requiring
    /// exact process admission must reject `None`, never interpret the subject.
    pub const fn process_identity(&self) -> Option<NativeProcessIdentity> {
        self.process_identity
    }
}

#[derive(Clone)]
pub struct NativeTrust {
    inner: Arc<NativeTrustInner>,
}

struct NativeTrustInner {
    deployment_id: DeploymentId,
    local_subject: NativeCallerSubject,
    jwt_hmac_key: [u8; 32],
    automatic_ed25519_seed: [u8; 32],
    transport_mode: NativeTransportMode,
    clock: Arc<dyn NativeTrustClock>,
    authentication: Mutex<AuthenticationState>,
}

struct AuthenticationState {
    process_identity: Option<NativeProcessIdentity>,
    cache: Option<CachedToken>,
}

struct CachedToken {
    value: String,
    expires_at: i64,
}

impl fmt::Debug for NativeTrust {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NativeTrust")
            .field("deployment_id", &self.inner.deployment_id)
            .field("local_subject", &self.inner.local_subject)
            .field("transport_mode", &self.inner.transport_mode)
            .field("jwt_hmac_key", &"REDACTED")
            .field(
                "automatic_ed25519_seed",
                &if self.automatic_tls_seed().len() == 32 {
                    "REDACTED"
                } else {
                    "UNAVAILABLE"
                },
            )
            .finish()
    }
}

impl NativeTrust {
    pub fn new(
        deployment_id: DeploymentId,
        shared_secret: ValidatedSharedSecret,
        local_subject: NativeCallerSubject,
        transport_mode: NativeTransportMode,
    ) -> Self {
        Self::new_with_clock(
            deployment_id,
            shared_secret,
            local_subject,
            transport_mode,
            Arc::new(SystemClock),
        )
    }

    pub fn new_with_clock(
        deployment_id: DeploymentId,
        shared_secret: ValidatedSharedSecret,
        local_subject: NativeCallerSubject,
        transport_mode: NativeTransportMode,
        clock: Arc<dyn NativeTrustClock>,
    ) -> Self {
        let prk = hmac_sha256(
            deployment_id.as_str().as_bytes(),
            shared_secret.expose_for_kdf(),
        );
        Self {
            inner: Arc::new(NativeTrustInner {
                deployment_id,
                local_subject,
                jwt_hmac_key: hkdf_expand_32(&prk, JWT_KEY_INFO),
                automatic_ed25519_seed: hkdf_expand_32(&prk, AUTOMATIC_TLS_KEY_INFO),
                transport_mode,
                clock,
                authentication: Mutex::new(AuthenticationState {
                    process_identity: None,
                    cache: None,
                }),
            }),
        }
    }

    pub fn deployment_id(&self) -> &DeploymentId {
        &self.inner.deployment_id
    }

    pub fn local_subject(&self) -> &NativeCallerSubject {
        &self.inner.local_subject
    }

    /// Bind the existing role-owned process identity exactly once.
    ///
    /// All trust clones share this binding. The first binding atomically clears
    /// any cached legacy token under the same lock used for token issuance.
    /// Repeating the exact identity is idempotent; another role or process is a
    /// conflict and leaves both the original identity and token cache unchanged.
    pub fn bind_process_identity(
        &self,
        identity: NativeProcessIdentity,
    ) -> Result<(), NativeTrustFailureKind> {
        let mut authentication = self
            .inner
            .authentication
            .lock()
            .expect("native trust authentication state mutex poisoned");
        match authentication.process_identity {
            Some(existing) if existing == identity => Ok(()),
            Some(_) => Err(NativeTrustFailureKind::ProcessIdentityConflict),
            None => {
                authentication.cache = None;
                authentication.process_identity = Some(identity);
                Ok(())
            }
        }
    }

    /// The shared local binding, independent of a remote token's signed claim.
    pub fn local_process_identity(&self) -> Option<NativeProcessIdentity> {
        self.inner
            .authentication
            .lock()
            .expect("native trust authentication state mutex poisoned")
            .process_identity
    }

    pub fn transport_mode(&self) -> NativeTransportMode {
        self.inner.transport_mode
    }

    pub fn client_interceptor(&self) -> NativeClientAuthInterceptor {
        NativeClientAuthInterceptor::new(self.clone())
    }

    /// Produces a listener-wide admission capability. Role applications install
    /// it outside their complete Tonic route set so unknown paths are subject
    /// to the same authentication decision as generated RPC methods.
    pub fn server_admission(&self) -> NativeServerAdmission {
        NativeServerAdmission::new(self.clone())
    }

    /// Internal input for the automatic TLS material builder. It is deliberately
    /// crate-private so role applications can never obtain the derived seed.
    pub(crate) fn automatic_tls_seed(&self) -> &[u8; 32] {
        &self.inner.automatic_ed25519_seed
    }

    /// Applies exactly one current `authorization: Bearer ...` metadata value
    /// to a newly created outbound RPC. Channel state is never authentication
    /// state; later RPCs call this again and may refresh the cache.
    pub fn apply_client_authorization(
        &self,
        metadata: &mut tonic::metadata::MetadataMap,
    ) -> Result<(), NativeTrustFailureKind> {
        let token = self.current_token()?;
        let value = tonic::metadata::MetadataValue::try_from(format!("Bearer {token}"))
            .map_err(|_| NativeTrustFailureKind::MalformedAuthorization)?;
        metadata.insert("authorization", value);
        Ok(())
    }

    pub fn verify_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedNativeCaller, NativeTrustFailureKind> {
        let values: Vec<_> = headers
            .get_all(http::header::AUTHORIZATION)
            .iter()
            .collect();
        if values.is_empty() {
            return Err(NativeTrustFailureKind::MissingAuthorization);
        }
        if values.len() != 1 {
            return Err(NativeTrustFailureKind::DuplicateAuthorization);
        }
        let authorization = values[0]
            .to_str()
            .map_err(|_| NativeTrustFailureKind::MalformedAuthorization)?;
        self.verify_authorization_value(authorization)
    }

    pub fn verify_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<AuthenticatedNativeCaller, NativeTrustFailureKind> {
        let values: Vec<_> = metadata.get_all("authorization").iter().collect();
        if values.is_empty() {
            return Err(NativeTrustFailureKind::MissingAuthorization);
        }
        if values.len() != 1 {
            return Err(NativeTrustFailureKind::DuplicateAuthorization);
        }
        let authorization = values[0]
            .to_str()
            .map_err(|_| NativeTrustFailureKind::MalformedAuthorization)?;
        self.verify_authorization_value(authorization)
    }

    fn current_token(&self) -> Result<String, NativeTrustFailureKind> {
        let now = self.inner.clock.unix_seconds();
        let mut authentication = self
            .inner
            .authentication
            .lock()
            .expect("native trust authentication state mutex poisoned");
        if let Some(cached) = authentication.cache.as_ref()
            && now < cached.expires_at - TOKEN_REFRESH_THRESHOLD_SECONDS
        {
            return Ok(cached.value.clone());
        }
        let expires_at = now
            .checked_add(TOKEN_LIFETIME_SECONDS)
            .ok_or(NativeTrustFailureKind::InvalidTokenTime)?;
        let token = self.encode_token(now, expires_at, authentication.process_identity)?;
        authentication.cache = Some(CachedToken {
            value: token.clone(),
            expires_at,
        });
        Ok(token)
    }

    fn encode_token(
        &self,
        issued_at: i64,
        expires_at: i64,
        identity: Option<NativeProcessIdentity>,
    ) -> Result<String, NativeTrustFailureKind> {
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let claims = match identity {
            None => format!(
                r#"{{"aud":"{}","sub":"{}","iat":{},"exp":{}}}"#,
                self.inner.deployment_id.as_str(),
                self.inner.local_subject.as_str(),
                issued_at,
                expires_at
            ),
            Some(NativeProcessIdentity::Frontend(process)) => format!(
                r#"{{"aud":"{}","sub":"{}","iat":{},"exp":{},"role":"frontend","process_id":"{}"}}"#,
                self.inner.deployment_id.as_str(),
                self.inner.local_subject.as_str(),
                issued_at,
                expires_at,
                process
            ),
            Some(NativeProcessIdentity::Backend(process)) => format!(
                r#"{{"aud":"{}","sub":"{}","iat":{},"exp":{},"role":"backend","process_id":"{}"}}"#,
                self.inner.deployment_id.as_str(),
                self.inner.local_subject.as_str(),
                issued_at,
                expires_at,
                process
            ),
        };
        let encoded_claims = URL_SAFE_NO_PAD.encode(claims.as_bytes());
        let signing_input = format!("{header}.{encoded_claims}");
        let signature = hmac_sha256(&self.inner.jwt_hmac_key, signing_input.as_bytes());
        Ok(format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature)
        ))
    }

    fn verify_authorization_value(
        &self,
        authorization: &str,
    ) -> Result<AuthenticatedNativeCaller, NativeTrustFailureKind> {
        let token = authorization
            .strip_prefix("Bearer ")
            .filter(|token| !token.is_empty() && token.is_ascii() && token.trim() == *token)
            .ok_or(NativeTrustFailureKind::MalformedAuthorization)?;
        if token.len() > MAX_TOKEN_BYTES {
            return Err(NativeTrustFailureKind::TokenTooLarge);
        }
        let mut components = token.split('.');
        let header = components
            .next()
            .ok_or(NativeTrustFailureKind::MalformedToken)?;
        let claims_segment = components
            .next()
            .ok_or(NativeTrustFailureKind::MalformedToken)?;
        let signature = components
            .next()
            .ok_or(NativeTrustFailureKind::MalformedToken)?;
        if components.next().is_some()
            || header.is_empty()
            || claims_segment.is_empty()
            || signature.is_empty()
            || !header
                .bytes()
                .chain(claims_segment.bytes())
                .chain(signature.bytes())
                .all(is_unpadded_base64url_byte)
        {
            return Err(NativeTrustFailureKind::MalformedToken);
        }

        let header_bytes = decode_base64url(header)?;
        let jose: JoseHeader = serde_json::from_slice(&header_bytes)
            .map_err(|_| NativeTrustFailureKind::InvalidJoseHeader)?;
        if jose.alg != "HS256" || jose.typ != "JWT" {
            return Err(NativeTrustFailureKind::InvalidJoseHeader);
        }
        let claims_bytes = decode_base64url(claims_segment)?;
        let claims: JwtClaims = serde_json::from_slice(&claims_bytes)
            .map_err(|_| NativeTrustFailureKind::InvalidClaims)?;
        let process_identity = claims.process_identity()?;
        let subject = NativeCallerSubject::parse(claims.sub)
            .map_err(|_| NativeTrustFailureKind::InvalidClaims)?;
        if claims.aud != self.inner.deployment_id.as_str() {
            return Err(NativeTrustFailureKind::InvalidClaims);
        }

        let signature = decode_base64url(signature)?;
        let signing_input = format!("{header}.{claims_segment}");
        let mut verifier = HmacSha256::new_from_slice(&self.inner.jwt_hmac_key)
            .map_err(|_| NativeTrustFailureKind::InvalidSignature)?;
        verifier.update(signing_input.as_bytes());
        verifier
            .verify_slice(&signature)
            .map_err(|_| NativeTrustFailureKind::InvalidSignature)?;

        let lifetime = claims
            .exp
            .checked_sub(claims.iat)
            .ok_or(NativeTrustFailureKind::InvalidTokenTime)?;
        let now = self.inner.clock.unix_seconds();
        if !(1..=TOKEN_LIFETIME_SECONDS).contains(&lifetime)
            || claims.iat > now.saturating_add(MAX_CLOCK_SKEW_SECONDS)
        {
            return Err(NativeTrustFailureKind::InvalidTokenTime);
        }
        if now >= claims.exp {
            return Err(NativeTrustFailureKind::ExpiredToken);
        }
        Ok(AuthenticatedNativeCaller {
            subject,
            process_identity,
        })
    }

    #[cfg(test)]
    fn jwt_hmac_key_for_vector(&self) -> [u8; 32] {
        self.inner.jwt_hmac_key
    }

    #[cfg(test)]
    fn automatic_ed25519_seed_for_vector(&self) -> [u8; 32] {
        *self.automatic_tls_seed()
    }
}

/// Tonic interceptor that obtains a fresh metadata value per RPC. It is safe
/// to clone with a role-scoped `NativeTrust` and has no option to omit auth.
#[derive(Clone, Debug)]
pub struct NativeClientAuthInterceptor {
    trust: NativeTrust,
}

impl NativeClientAuthInterceptor {
    pub fn new(trust: NativeTrust) -> Self {
        Self { trust }
    }
}

impl Interceptor for NativeClientAuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        self.trust
            .apply_client_authorization(request.metadata_mut())
            .map_err(|_| Status::unauthenticated("native caller authentication failed"))?;
        Ok(request)
    }
}

/// Adapter-neutral server admission capability. It has one deliberately low
/// information remote status for every authentication failure; listener owners
/// retain route composition and bounded role-local observability.
#[derive(Clone, Debug)]
pub struct NativeServerAdmission {
    trust: NativeTrust,
}

impl NativeServerAdmission {
    pub fn new(trust: NativeTrust) -> Self {
        Self { trust }
    }

    pub fn admit_headers(
        &self,
        headers: &HeaderMap,
    ) -> Result<AuthenticatedNativeCaller, Box<Status>> {
        self.trust.verify_headers(headers).map_err(|_| {
            Box::new(Status::unauthenticated(
                "native caller authentication failed",
            ))
        })
    }

    pub fn admit_metadata(
        &self,
        metadata: &tonic::metadata::MetadataMap,
    ) -> Result<AuthenticatedNativeCaller, Box<Status>> {
        self.trust.verify_metadata(metadata).map_err(|_| {
            Box::new(Status::unauthenticated(
                "native caller authentication failed",
            ))
        })
    }

    pub fn listener_layer(&self) -> NativeListenerAuthLayer {
        NativeListenerAuthLayer {
            admission: self.clone(),
            on_authentication_failure: None,
        }
    }
}

/// Tower layer for installation around the complete Native route set. It
/// authenticates before the generated service or unknown-path fallback sees a
/// request; every rejection has the same public gRPC status.
#[derive(Clone, Debug)]
pub struct NativeListenerAuthLayer {
    admission: NativeServerAdmission,
    on_authentication_failure: Option<AuthenticationFailureObserver>,
}

#[derive(Clone)]
struct AuthenticationFailureObserver(Arc<dyn Fn() + Send + Sync>);

impl std::fmt::Debug for AuthenticationFailureObserver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthenticationFailureObserver")
            .finish_non_exhaustive()
    }
}

impl NativeListenerAuthLayer {
    /// The listener contributes metrics without reimplementing authentication
    /// or deciding what authenticated process claim reaches the application.
    pub fn with_authentication_failure_observer(
        mut self,
        observer: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        self.on_authentication_failure = Some(AuthenticationFailureObserver(observer));
        self
    }
}

impl<S> Layer<S> for NativeListenerAuthLayer {
    type Service = NativeListenerAuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        NativeListenerAuthService {
            admission: self.admission.clone(),
            on_authentication_failure: self.on_authentication_failure.clone(),
            inner,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NativeListenerAuthService<S> {
    admission: NativeServerAdmission,
    on_authentication_failure: Option<AuthenticationFailureObserver>,
    inner: S,
}

impl<S, Body> Service<http::Request<Body>> for NativeListenerAuthService<S>
where
    S: Service<http::Request<Body>, Response = http::Response<tonic::body::BoxBody>> + Send,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = http::Response<tonic::body::BoxBody>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: http::Request<Body>) -> Self::Future {
        let caller = match self.admission.admit_headers(request.headers()) {
            Ok(caller) => caller,
            Err(_) => {
                if let Some(observer) = &self.on_authentication_failure {
                    (observer.0)();
                }
                return Box::pin(async {
                    Ok(Status::unauthenticated("native caller authentication failed").into_http())
                });
            }
        };
        // The authentication owner publishes the verified claim exactly once.
        // Application owners still authorize its exact process and operation.
        request.extensions_mut().insert(caller);
        Box::pin(self.inner.call(request))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JoseHeader {
    alg: String,
    typ: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JwtClaims {
    aud: String,
    sub: String,
    iat: i64,
    exp: i64,
    #[serde(default)]
    role: ProcessStringClaim,
    #[serde(default)]
    process_id: ProcessStringClaim,
}

// A default applies only to a missing object field. Explicit null and other
// non-string values invoke String::deserialize and are rejected, rather than
// silently being treated as a legacy absent claim by Option<String>.
#[derive(Default)]
enum ProcessStringClaim {
    #[default]
    Missing,
    Present(String),
}

impl<'de> Deserialize<'de> for ProcessStringClaim {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::Present)
    }
}

impl JwtClaims {
    fn process_identity(&self) -> Result<Option<NativeProcessIdentity>, NativeTrustFailureKind> {
        match (&self.role, &self.process_id) {
            (ProcessStringClaim::Missing, ProcessStringClaim::Missing) => Ok(None),
            (ProcessStringClaim::Present(role), ProcessStringClaim::Present(process)) => {
                let identity = match role.as_str() {
                    "frontend" => NativeProcessIdentity::Frontend(
                        process
                            .parse()
                            .map_err(|_| NativeTrustFailureKind::InvalidClaims)?,
                    ),
                    "backend" => NativeProcessIdentity::Backend(
                        process
                            .parse()
                            .map_err(|_| NativeTrustFailureKind::InvalidClaims)?,
                    ),
                    _ => return Err(NativeTrustFailureKind::InvalidClaims),
                };
                Ok(Some(identity))
            }
            _ => Err(NativeTrustFailureKind::InvalidClaims),
        }
    }
}

fn decode_base64url(value: &str) -> Result<Vec<u8>, NativeTrustFailureKind> {
    URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| NativeTrustFailureKind::MalformedToken)
}

fn is_unpadded_base64url_byte(byte: u8) -> bool {
    byte.is_ascii_uppercase()
        || byte.is_ascii_lowercase()
        || byte.is_ascii_digit()
        || matches!(byte, b'-' | b'_')
}

fn hmac_sha256(key: &[u8], input: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts arbitrary key lengths");
    mac.update(input);
    mac.finalize().into_bytes().into()
}

fn hkdf_expand_32(prk: &[u8; 32], info: &[u8]) -> [u8; 32] {
    // RFC 5869 expand for a single SHA-256 block. The fixed 32-byte output
    // needs exactly one block, so no loop or variable-length API can drift.
    let mut mac = HmacSha256::new_from_slice(prk).expect("SHA-256 PRK has valid length");
    mac.update(info);
    mac.update(&[1]);
    mac.finalize().into_bytes().into()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use http::{HeaderMap, HeaderValue, header::AUTHORIZATION};
    use novarocks_secret::SecretValue;

    use super::{
        DeploymentId, ManualClock, NativeCallerSubject, NativeProcessIdentity, NativeTransportMode,
        NativeTrust, NativeTrustFailureKind, TOKEN_LIFETIME_SECONDS, ValidatedSharedSecret,
    };

    const REFERENCE_TOKEN: &str = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJhbmFseXRpY3MtcHJvZCIsInN1YiI6ImZlQDEyNy4wLjAuMTo5MDgwIiwiaWF0IjoxNzAwMDAwMDAwLCJleHAiOjE3MDAwMDAzNjB9.PFzl2xOOm_UE6NWzXZCdz8-OuujaqQY1CeC5B5K-1YM";

    fn trust(clock: Arc<ManualClock>) -> NativeTrust {
        NativeTrust::new_with_clock(
            DeploymentId::parse("analytics-prod").unwrap(),
            ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
                .unwrap(),
            NativeCallerSubject::parse("fe@127.0.0.1:9080").unwrap(),
            NativeTransportMode::Disabled,
            clock,
        )
    }

    fn authorization(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    fn frontend_identity() -> NativeProcessIdentity {
        NativeProcessIdentity::Frontend(
            "018bcfe5-6800-7000-8000-000000000001"
                .parse()
                .expect("explicit UUIDv7 frontend fixture"),
        )
    }

    fn backend_identity() -> NativeProcessIdentity {
        NativeProcessIdentity::Backend(
            "018bcfe5-6800-7000-8000-000000000002"
                .parse()
                .expect("explicit UUIDv7 backend fixture"),
        )
    }

    fn signed_claims(trust: &NativeTrust, claims: &str) -> String {
        let header = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9";
        let claims = URL_SAFE_NO_PAD.encode(claims.as_bytes());
        let signing_input = format!("{header}.{claims}");
        let signature =
            super::hmac_sha256(&trust.jwt_hmac_key_for_vector(), signing_input.as_bytes());
        format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature))
    }

    #[derive(Clone)]
    struct CaptureCaller(Arc<std::sync::Mutex<Vec<super::AuthenticatedNativeCaller>>>);

    impl tower::Service<http::Request<()>> for CaptureCaller {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = std::future::Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(
            &mut self,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: http::Request<()>) -> Self::Future {
            // Tonic preserves these HTTP extensions on the generated Request.
            let request = tonic::Request::from_http(request);
            self.0.lock().unwrap().push(
                request
                    .extensions()
                    .get::<super::AuthenticatedNativeCaller>()
                    .unwrap()
                    .clone(),
            );
            std::future::ready(Ok(http::Response::new(tonic::body::empty_body())))
        }
    }

    #[tokio::test]
    async fn listener_preserves_the_verified_caller_for_operation_authorization() {
        use tower::{Layer, Service};

        let trust = trust(Arc::new(ManualClock::new(1_700_000_000)));
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut service = trust
            .server_admission()
            .listener_layer()
            .layer(CaptureCaller(observed.clone()));
        for identity in [None, Some(backend_identity())] {
            if let Some(identity) = identity {
                trust.bind_process_identity(identity).unwrap();
            }
            let headers = authorization(&trust.current_token().unwrap());
            let expected = trust.verify_headers(&headers).unwrap();
            let mut request = http::Request::new(());
            *request.headers_mut() = headers;
            // A stale local extension cannot override the authenticated headers.
            request
                .extensions_mut()
                .insert(super::AuthenticatedNativeCaller {
                    subject: NativeCallerSubject::parse("stale-local-value").unwrap(),
                    process_identity: Some(frontend_identity()),
                });
            service.call(request).await.unwrap();
            assert_eq!(observed.lock().unwrap().last(), Some(&expected));
            assert_eq!(expected.process_identity(), identity);
        }
    }

    #[tokio::test]
    async fn listener_does_not_publish_a_caller_for_rejected_authentication() {
        use tower::{Layer, Service};

        let trust = trust(Arc::new(ManualClock::new(1_700_000_000)));
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut service = trust
            .server_admission()
            .listener_layer()
            .layer(CaptureCaller(observed.clone()));
        let response = service.call(http::Request::new(())).await.unwrap();
        assert_eq!(response.headers()["grpc-status"], "16");
        assert!(observed.lock().unwrap().is_empty());
    }

    #[test]
    fn process_binding_clears_legacy_cache_is_shared_and_conflicts_without_mutation() {
        let trust = trust(Arc::new(ManualClock::new(1_700_000_000)));
        let clone = trust.clone();
        let legacy = trust.current_token().unwrap();
        assert_eq!(legacy, REFERENCE_TOKEN);
        assert_eq!(trust.local_process_identity(), None);
        assert_eq!(
            trust
                .verify_headers(&authorization(&legacy))
                .unwrap()
                .process_identity(),
            None
        );

        clone.bind_process_identity(frontend_identity()).unwrap();
        let bound = trust.current_token().unwrap();
        assert_ne!(bound, legacy);
        assert_eq!(trust.local_process_identity(), Some(frontend_identity()));
        let authenticated = clone.verify_headers(&authorization(&bound)).unwrap();
        assert_eq!(authenticated.process_identity(), Some(frontend_identity()));
        assert_eq!(authenticated.subject().as_str(), "fe@127.0.0.1:9080");
        let payload = URL_SAFE_NO_PAD
            .decode(bound.split('.').nth(1).unwrap())
            .unwrap();
        let claims: serde_json::Value = serde_json::from_slice(&payload).unwrap();
        assert_eq!(claims["role"], "frontend");
        assert_eq!(claims["process_id"], "018bcfe5-6800-7000-8000-000000000001");

        clone.bind_process_identity(frontend_identity()).unwrap();
        assert_eq!(clone.current_token().unwrap(), bound);
        assert_eq!(
            clone.bind_process_identity(backend_identity()),
            Err(NativeTrustFailureKind::ProcessIdentityConflict)
        );
        let another_frontend = NativeProcessIdentity::Frontend(
            "018bcfe5-6800-7000-8000-000000000003".parse().unwrap(),
        );
        assert_eq!(
            trust.bind_process_identity(another_frontend),
            Err(NativeTrustFailureKind::ProcessIdentityConflict)
        );
        assert_eq!(clone.local_process_identity(), Some(frontend_identity()));
        assert_eq!(trust.current_token().unwrap(), bound);
    }

    #[test]
    fn bound_token_refresh_preserves_exact_identity_and_signature() {
        let clock = Arc::new(ManualClock::new(1_700_000_000));
        let issuer = trust(clock.clone());
        issuer.bind_process_identity(backend_identity()).unwrap();
        let verifier = trust(clock.clone());
        verifier.bind_process_identity(frontend_identity()).unwrap();
        let first = issuer.current_token().unwrap();
        assert_eq!(
            verifier
                .verify_headers(&authorization(&first))
                .unwrap()
                .process_identity(),
            Some(backend_identity())
        );
        clock.set_unix_seconds(1_700_000_059);
        assert_eq!(issuer.current_token().unwrap(), first);
        clock.set_unix_seconds(1_700_000_060);
        let renewed = issuer.clone().current_token().unwrap();
        assert_ne!(renewed, first);
        let authenticated = verifier.verify_headers(&authorization(&renewed)).unwrap();
        assert_eq!(authenticated.process_identity(), Some(backend_identity()));
        // The fixture subject says "fe"; it remains diagnostic and does not
        // override the signed, composition-supplied Backend identity.
        assert_eq!(authenticated.subject().as_str(), "fe@127.0.0.1:9080");
        clock.set_unix_seconds(1_700_000_000 + TOKEN_LIFETIME_SECONDS);
        assert_eq!(
            verifier.verify_headers(&authorization(&first)),
            Err(NativeTrustFailureKind::ExpiredToken)
        );
        assert_eq!(
            verifier
                .verify_headers(&authorization(&renewed))
                .unwrap()
                .process_identity(),
            Some(backend_identity())
        );
    }

    #[test]
    fn actual_signed_process_claims_reject_partial_null_duplicate_and_invalid_identity() {
        let trust = trust(Arc::new(ManualClock::new(1_700_000_001)));
        let malformed = [
            r#", "role":"frontend""#,
            r#", "process_id":"018bcfe5-6800-7000-8000-000000000001""#,
            r#", "role":null, "process_id":null"#,
            r#", "role":null"#,
            r#", "process_id":null"#,
            r#", "role":"frontend", "process_id":null"#,
            r#", "role":0, "process_id":"018bcfe5-6800-7000-8000-000000000001""#,
            r#", "role":"frontend", "process_id":[]"#,
            r#", "role":"fe", "process_id":"018bcfe5-6800-7000-8000-000000000001""#,
            r#", "role":"unknown", "process_id":"018bcfe5-6800-7000-8000-000000000001""#,
            r#", "role":"frontend", "process_id":"00000000-0000-0000-0000-000000000000""#,
            r#", "role":"backend", "process_id":"018bcfe5-6800-4000-8000-000000000001""#,
            r#", "role":"backend", "process_id":"not-a-uuid""#,
            r#", "role":"frontend", "process_id":"""#,
            r#", "role":"frontend", "role":"backend", "process_id":"018bcfe5-6800-7000-8000-000000000001""#,
            r#", "role":"frontend", "process_id":"018bcfe5-6800-7000-8000-000000000001", "process_id":"018bcfe5-6800-7000-8000-000000000002""#,
            r#", "role":"frontend", "process_id":"018bcfe5-6800-7000-8000-000000000001", "extra":true"#,
        ];
        for extra in malformed {
            let claims = format!(
                r#"{{"aud":"analytics-prod","sub":"fe@127.0.0.1:9080","iat":1700000000,"exp":1700000360{extra}}}"#
            );
            let token = signed_claims(&trust, &claims);
            assert_eq!(
                trust.verify_headers(&authorization(&token)),
                Err(NativeTrustFailureKind::InvalidClaims),
                "signed malformed claim suffix: {extra}"
            );
        }
    }

    #[test]
    fn valid_process_claim_substitution_requires_a_new_signature() {
        let trust = trust(Arc::new(ManualClock::new(1_700_000_001)));
        trust.bind_process_identity(frontend_identity()).unwrap();
        let token = trust.current_token().unwrap();
        let parts: Vec<_> = token.split('.').collect();
        let payload = String::from_utf8(URL_SAFE_NO_PAD.decode(parts[1]).unwrap()).unwrap();
        let changed = payload.replace(
            "018bcfe5-6800-7000-8000-000000000001",
            "018bcfe5-6800-7000-8000-000000000003",
        );
        let tampered = format!(
            "{}.{}.{}",
            parts[0],
            URL_SAFE_NO_PAD.encode(changed.as_bytes()),
            parts[2]
        );
        assert_eq!(
            trust.verify_headers(&authorization(&tampered)),
            Err(NativeTrustFailureKind::InvalidSignature)
        );
        let signed = signed_claims(&trust, &changed);
        assert_eq!(
            trust
                .verify_headers(&authorization(&signed))
                .unwrap()
                .process_identity(),
            Some(NativeProcessIdentity::Frontend(
                "018bcfe5-6800-7000-8000-000000000003".parse().unwrap()
            ))
        );
    }

    #[test]
    fn competing_clone_bindings_choose_one_identity_and_all_later_tokens_use_it() {
        let trust = trust(Arc::new(ManualClock::new(1_700_000_000)));
        trust.current_token().unwrap();
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let mut threads = Vec::new();
        for identity in [frontend_identity(), backend_identity()] {
            let clone = trust.clone();
            let barrier = Arc::clone(&barrier);
            threads.push(std::thread::spawn(move || {
                barrier.wait();
                (identity, clone.bind_process_identity(identity))
            }));
        }
        barrier.wait();
        let outcomes: Vec<_> = threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .collect();
        assert_eq!(
            outcomes.iter().filter(|(_, result)| result.is_ok()).count(),
            1
        );
        assert_eq!(
            outcomes
                .iter()
                .filter(
                    |(_, result)| *result == Err(NativeTrustFailureKind::ProcessIdentityConflict)
                )
                .count(),
            1
        );
        let winner = outcomes
            .iter()
            .find(|(_, result)| result.is_ok())
            .unwrap()
            .0;
        assert_eq!(trust.local_process_identity(), Some(winner));
        for clone in [trust.clone(), trust.clone()] {
            let token = clone.current_token().unwrap();
            assert_eq!(
                trust
                    .verify_headers(&authorization(&token))
                    .unwrap()
                    .process_identity(),
                Some(winner)
            );
        }
    }

    #[test]
    fn derives_frozen_hkdf_vectors_and_exact_jwt() {
        let clock = Arc::new(ManualClock::new(1_700_000_000));
        let trust = trust(clock);
        assert_eq!(
            hex::encode(trust.jwt_hmac_key_for_vector()),
            "533ac6e603e3d233e61bdb905dfdbab26fc479d05b3a34cabff154e0a243da11"
        );
        assert_eq!(
            hex::encode(trust.automatic_ed25519_seed_for_vector()),
            "469d8b1b1ff5a80e55c4d2189b1080789bf72e7cf8bfcd687c2ddcd4bf22dd87"
        );
        assert_eq!(trust.current_token().unwrap(), REFERENCE_TOKEN);
    }

    #[test]
    fn verifies_exact_token_and_rejects_duplicate_unknown_and_time_claims() {
        let clock = Arc::new(ManualClock::new(1_700_000_001));
        let trust = trust(clock);
        assert_eq!(
            trust
                .verify_headers(&authorization(REFERENCE_TOKEN))
                .unwrap()
                .subject()
                .as_str(),
            "fe@127.0.0.1:9080"
        );

        let mut duplicate = authorization(REFERENCE_TOKEN);
        duplicate.append(AUTHORIZATION, HeaderValue::from_static("Bearer x"));
        assert_eq!(
            trust.verify_headers(&duplicate),
            Err(NativeTrustFailureKind::DuplicateAuthorization)
        );

        let unknown_claim = REFERENCE_TOKEN.replacen(
            "eyJhdWQiOiJhbmFseXRpY3MtcHJvZCIsInN1YiI6ImZlQDEyNy4wLjAuMTo5MDgwIiwiaWF0IjoxNzAwMDAwMDAwLCJleHAiOjE3MDAwMDAzNjB9",
            "eyJhdWQiOiJhbmFseXRpY3MtcHJvZCIsInN1YiI6ImZlQDEyNy4wLjAuMTo5MDgwIiwiaWF0IjoxNzAwMDAwMDAwLCJleHAiOjE3MDAwMDAzNjAsIngiOjF9",
            1,
        );
        assert!(matches!(
            trust.verify_headers(&authorization(&unknown_claim)),
            Err(NativeTrustFailureKind::InvalidSignature | NativeTrustFailureKind::InvalidClaims)
        ));

        let duplicate_claims = URL_SAFE_NO_PAD.encode(
            br#"{"aud":"analytics-prod","aud":"analytics-prod","sub":"fe@127.0.0.1:9080","iat":1700000000,"exp":1700000360}"#,
        );
        let duplicate_token =
            format!("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.{duplicate_claims}.invalid");
        assert_eq!(
            trust.verify_headers(&authorization(&duplicate_token)),
            Err(NativeTrustFailureKind::InvalidClaims)
        );
    }

    #[test]
    fn cache_refreshes_before_expiry_and_never_accepts_expired_tokens() {
        let clock = Arc::new(ManualClock::new(1_700_000_000));
        let trust = trust(clock.clone());
        let first = trust.current_token().unwrap();
        clock.set_unix_seconds(1_700_000_059);
        assert_eq!(trust.current_token().unwrap(), first);
        clock.set_unix_seconds(1_700_000_060);
        assert_ne!(trust.current_token().unwrap(), first);
        clock.set_unix_seconds(1_700_000_000 + TOKEN_LIFETIME_SECONDS);
        assert_eq!(
            trust.verify_headers(&authorization(REFERENCE_TOKEN)),
            Err(NativeTrustFailureKind::ExpiredToken)
        );
    }

    #[test]
    fn redacts_secret_and_rejects_malformed_input_without_panicking() {
        let clock = Arc::new(ManualClock::new(1));
        let trust = trust(clock);
        assert!(!format!("{trust:?}").contains("0123456789abcdef0123456789abcdef"));
        for authorization_value in [
            "Bearer",
            "Bearer  ",
            "bearer x",
            "Bearer a.b",
            "Bearer a.b.c.d",
            "Bearer a=.b.c",
            "Bearer a.b.c ",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(authorization_value).unwrap(),
            );
            assert!(trust.verify_headers(&headers).is_err());
        }
    }

    #[test]
    fn server_admission_has_one_remote_failure_for_every_auth_problem() {
        let trust = trust(Arc::new(ManualClock::new(1_700_000_001)));
        let error = trust
            .server_admission()
            .admit_headers(&HeaderMap::new())
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::Unauthenticated);
        assert_eq!(error.message(), "native caller authentication failed");
    }
}

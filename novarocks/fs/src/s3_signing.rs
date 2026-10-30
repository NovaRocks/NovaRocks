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

//! Provider signing over the actual OpenDAL S3 request.
//!
//! This is not joint admission. The selected route is a pure fact; the original
//! governed caller must bind all dependencies and physical resources before
//! credential acquisition or HTTP dispatch. Cross-origin redirect admission
//! remains deliberately unimplemented here.

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use http::{Method, Request, Response, StatusCode, Uri, header};
use opendal::raw::{
    Layer, OpCopier, OpCopy, OpCreateDir, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
    PresignedRequest, RpCreateDir, RpPresign, RpRename, RpStat, Service, ServiceInfo, Servicer,
    oio,
};
use opendal::{Buffer, Error, ErrorKind, HttpBody, HttpTransport, OperationContext};
use url::Url;

use super::{
    AuthoritySigningSource, CredentialDenialLatch, ObjectStoreCredentialSource,
    ObjectStoreEndpointIdentity,
};
use crate::{FileError, FileResult};

// Preserve reqwest 0.12's default finite redirect depth, with explicit signing.
const MAX_REDIRECTS: usize = 10;

/// Pure facts from the actual request, not a grant or a reconstructed endpoint.
/// A future joint owner may pass service_base() to NetworkServiceOrigin.
pub(super) struct SelectedS3Request {
    actual: Url,
    service_base: Url,
}
impl SelectedS3Request {
    pub(super) fn actual(&self) -> &Url {
        &self.actual
    }
    pub(super) fn service_base(&self) -> &Url {
        &self.service_base
    }
}

enum SigningMaterial {
    Static(reqsign::AwsCredential),
    Authority(AuthoritySigningSource),
}

pub(super) struct S3RequestSigner {
    bucket: String,
    use_path_style: bool,
    signer: reqsign::AwsV4Signer,
    material: SigningMaterial,
    denial: Arc<CredentialDenialLatch>,
}
impl fmt::Debug for S3RequestSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3RequestSigner").finish_non_exhaustive()
    }
}
impl S3RequestSigner {
    pub(super) fn new(
        endpoint: &ObjectStoreEndpointIdentity,
        source: &ObjectStoreCredentialSource,
        denial: Arc<CredentialDenialLatch>,
    ) -> FileResult<Self> {
        let material = match source {
            ObjectStoreCredentialSource::Static {
                material: value, ..
            } => {
                let access_key_id = value.access_key_id.expose_secret().to_owned();
                let secret_access_key = value.access_key_secret.expose_secret().to_owned();
                if access_key_id.is_empty() || secret_access_key.is_empty() {
                    return Err(FileError::invalid("static S3 signing requires both keys"));
                }
                SigningMaterial::Static(reqsign::AwsCredential {
                    access_key_id,
                    secret_access_key,
                    session_token: value
                        .session_token
                        .as_ref()
                        .map(|token| token.expose_secret().to_owned()),
                    expires_in: None,
                })
            }
            ObjectStoreCredentialSource::Authority(authority) => {
                SigningMaterial::Authority(AuthoritySigningSource {
                    authority: Arc::clone(authority),
                    denial: Arc::clone(&denial),
                    budget: Duration::from_millis(endpoint.timeout_ms),
                })
            }
        };
        Ok(Self {
            bucket: endpoint.bucket.clone(),
            use_path_style: endpoint.use_path_style,
            signer: reqsign::AwsV4Signer::new("s3", &endpoint.region),
            material,
            denial,
        })
    }

    pub(super) fn selected_request(&self, uri: &Uri) -> opendal::Result<SelectedS3Request> {
        let actual = Url::parse(&uri.to_string())
            .map_err(|error| invalid("invalid actual S3 request URI").set_source(error))?;
        if !matches!(actual.scheme(), "http" | "https")
            || actual.host_str().is_none()
            || !actual.username().is_empty()
            || actual.password().is_some()
        {
            return Err(invalid(
                "S3 request requires an HTTP origin without user information",
            ));
        }
        if actual.query_pairs().any(|(name, _)| {
            [
                "X-Amz-Algorithm",
                "X-Amz-Credential",
                "X-Amz-Signature",
                "X-Amz-Date",
                "X-Amz-Expires",
                "X-Amz-SignedHeaders",
                "X-Amz-Security-Token",
            ]
            .iter()
            .any(|reserved| name.eq_ignore_ascii_case(reserved))
        }) {
            return Err(invalid(
                "an SDK S3 request must not already contain query authentication",
            ));
        }
        let mut service_base = actual.clone();
        if !self.use_path_style {
            // Inspect the original URI host, preserving the exact configured
            // bucket prefix added by this SDK's virtual-host renderer.
            let host = uri
                .host()
                .ok_or_else(|| invalid("S3 request has no host"))?;
            let prefix = format!("{}.", self.bucket);
            let base = host
                .strip_prefix(&prefix)
                .filter(|base| !base.is_empty())
                .ok_or_else(|| {
                    invalid("actual virtual S3 host does not match its frozen bucket")
                })?;
            service_base
                .set_host(Some(base))
                .map_err(|error| invalid("invalid actual S3 service host").set_source(error))?;
        }
        service_base.set_path("/");
        service_base.set_query(None);
        service_base.set_fragment(None);
        Ok(SelectedS3Request {
            actual,
            service_base,
        })
    }

    /// ECS roles can have wider IAM rights than this deployment binding. Check
    /// the actual SDK target on every signing path, before acquiring material.
    fn authorize_node_local_target(
        &self,
        uri: &Uri,
        headers: &http::HeaderMap,
        body: Option<&Buffer>,
    ) -> opendal::Result<()> {
        let SigningMaterial::Authority(source) = &self.material else {
            return Ok(());
        };
        let id = source.authority.id();
        if !matches!(
            id.capability(),
            crate::storage_authority::AuthorityCapabilityPath::NodeLocalProvider { .. }
        ) {
            return Ok(());
        }
        let scope =
            Url::parse(id.scope().as_str()).map_err(|_| invalid("invalid bound ECS scope"))?;
        if scope.host_str() != Some(self.bucket.as_str()) {
            return Err(scope_denied());
        }
        // FsLocation preserves literal key bytes; the SDK encodes them once.
        // Decode only the emitted request, never the configured logical key.
        let prefix = scope.path().as_bytes();
        let check = |path: &[u8]| -> opendal::Result<()> {
            if path.contains(&b'\\')
                || path
                    .split(|byte| *byte == b'/')
                    .any(|part| matches!(part, b"." | b".."))
            {
                return Err(scope_denied());
            }
            if path.starts_with(prefix) {
                Ok(())
            } else {
                Err(scope_denied())
            }
        };
        let raw_path = if self.use_path_style {
            uri.path()
                .strip_prefix(&format!("/{}", self.bucket))
                .filter(|path| path.is_empty() || path.starts_with('/'))
                .ok_or_else(scope_denied)?
        } else {
            uri.path()
        };
        let path = percent_encoding::percent_decode_str(raw_path).collect::<Vec<_>>();
        let query = url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .collect::<Vec<_>>();
        if path.is_empty() || path == b"/" {
            if query.iter().any(|(key, _)| key == "delete") {
                #[derive(serde::Deserialize)]
                #[serde(rename = "Delete", deny_unknown_fields)]
                struct Delete {
                    #[serde(rename = "Quiet", default)]
                    _quiet: Option<bool>,
                    #[serde(rename = "Object", default)]
                    objects: Vec<DeleteObject>,
                }
                #[derive(serde::Deserialize)]
                #[serde(deny_unknown_fields)]
                struct DeleteObject {
                    #[serde(rename = "Key")]
                    key: String,
                    #[serde(rename = "VersionId", default)]
                    _version: Option<String>,
                }
                let body = body.ok_or_else(scope_denied)?.to_bytes();
                let text = std::str::from_utf8(&body).map_err(|_| scope_denied())?;
                let delete: Delete = quick_xml::de::from_str(text).map_err(|_| scope_denied())?;
                if delete.objects.is_empty() {
                    return Err(scope_denied());
                }
                for object in delete.objects {
                    check(format!("/{}", object.key).as_bytes())?;
                }
            } else {
                let prefixes = query
                    .iter()
                    .filter(|(key, _)| key == "prefix")
                    .collect::<Vec<_>>();
                if prefixes.len() > 1 {
                    return Err(scope_denied());
                }
                let key_prefix = prefixes
                    .first()
                    .map(|(_, value)| value.as_ref())
                    .unwrap_or("");
                check(format!("/{key_prefix}").as_bytes())?;
            }
        } else {
            check(&path)?;
        }
        if let Some(copy) = headers.get("x-amz-copy-source") {
            let copy = copy.to_str().map_err(|_| scope_denied())?;
            let copy = copy.split('?').next().unwrap_or_default();
            let copy = percent_encoding::percent_decode_str(copy).collect::<Vec<_>>();
            let bucket = format!("{}/", self.bucket);
            let copy = copy.strip_prefix(b"/").unwrap_or(&copy);
            let key = copy
                .strip_prefix(bucket.as_bytes())
                .ok_or_else(scope_denied)?;
            let mut path = Vec::with_capacity(key.len() + 1);
            path.push(b'/');
            path.extend_from_slice(key);
            check(&path)?;
        }
        Ok(())
    }

    async fn credential_in(
        &self,
        request: Option<&novarocks_spi::connector::ConnectorRequestContext>,
    ) -> opendal::Result<Cow<'_, reqsign::AwsCredential>> {
        match &self.material {
            SigningMaterial::Static(value) => Ok(Cow::Borrowed(value)),
            SigningMaterial::Authority(loader) => {
                // Call the original authority on every real sign, including
                // each replay. No SDK cache or invented wall-clock expiry.
                match loader.credential_for_request_in(request).await {
                    Ok(value) => Ok(Cow::Owned(value)),
                    Err(error) => Err(self.denial.classify(
                        Error::new(ErrorKind::Unexpected, "acquire S3 signing material")
                            .set_temporary()
                            .set_source(error),
                    )),
                }
            }
        }
    }

    async fn sign_header(&self, request: &mut Request<Buffer>) -> opendal::Result<()> {
        let selected = self.selected_request(request.uri())?;
        // Pure selected facts only. No admission has occurred in this slice.
        let _service_base = selected.service_base();
        self.authorize_node_local_target(request.uri(), request.headers(), Some(request.body()))?;
        clear_signing_headers(request.headers_mut());
        let context = request
            .extensions()
            .get::<novarocks_spi::connector::ConnectorRequestContext>();
        let credential = self.credential_in(context).await?;
        self.signer
            .sign(request, credential.as_ref())
            .map_err(|error| invalid("sign actual S3 request").set_source(error))?;
        // Match the SDK's original HTTP/2-compatible host handling.
        request.headers_mut().remove(header::HOST);
        Ok(())
    }

    async fn presign(
        &self,
        original: PresignedRequest,
        expiry: Duration,
    ) -> opendal::Result<PresignedRequest> {
        let mut request: Request<()> = original.into();
        let selected = self.selected_request(request.uri())?;
        let _service_base = selected.service_base();
        self.authorize_node_local_target(request.uri(), request.headers(), None)?;
        clear_signing_headers(request.headers_mut());
        let credential = self.credential_in(None).await?;
        self.signer
            .sign_query(&mut request, expiry, credential.as_ref())
            .map_err(|error| invalid("presign actual S3 request").set_source(error))?;
        request.headers_mut().remove(header::HOST);
        let (parts, ()) = request.into_parts();
        Ok(PresignedRequest::new(
            parts.method,
            parts.uri,
            parts.headers,
        ))
    }
}

fn invalid(message: &'static str) -> Error {
    Error::new(ErrorKind::Unexpected, message).set_permanent()
}
fn scope_denied() -> Error {
    Error::new(
        ErrorKind::PermissionDenied,
        "S3 request leaves its configured ECS scope",
    )
    .set_permanent()
}
fn clear_signing_headers(headers: &mut http::HeaderMap) {
    for name in [
        "authorization",
        "host",
        "x-amz-date",
        "x-amz-security-token",
    ] {
        headers.remove(name);
    }
}

/// The supplied inner client must have automatic redirection disabled.
pub(super) struct S3SigningTransport<T> {
    inner: T,
    signer: Arc<S3RequestSigner>,
}
impl<T> S3SigningTransport<T> {
    pub(super) fn new(inner: T, signer: Arc<S3RequestSigner>) -> Self {
        Self { inner, signer }
    }
}
impl<T: HttpTransport> HttpTransport for S3SigningTransport<T> {
    async fn fetch(&self, mut request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        // Redirects cannot acquire a different object, version or subresource
        // using the original handle's permission.
        let frozen_target = request.uri().path_and_query().cloned();
        for redirects in 0..=MAX_REDIRECTS {
            let selected = self.signer.selected_request(request.uri())?;
            let original = request
                .extensions()
                .get::<novarocks_spi::connector::ConnectorRequestContext>()
                .cloned();
            let permit = match original.as_ref() {
                Some(context) => {
                    let dependency =
                        novarocks_spi::connector::NetworkServiceOrigin::from_selected_url(
                            selected.service_base(),
                        )
                        .map(novarocks_spi::connector::SharedDependency::Network)
                        .map_err(|e| invalid("invalid selected S3 service origin").set_source(e))?;
                    context
                        .admit_selected_io(dependency)
                        .await
                        .map_err(|e| invalid("original S3 I/O admission refused").set_source(e))?
                }
                None => None,
            };
            // Nested credential traffic extends THIS paid phase atomically.
            if let (Some(context), Some(permit)) = (original.as_ref(), permit.as_ref()) {
                request
                    .extensions_mut()
                    .insert(context.clone().with_io_phase(permit.retain()));
            }
            self.signer.sign_header(&mut request).await?;
            // Buffer clone shares the original payload; no detached byte copy.
            let mut replay = Request::new(request.body().clone());
            *replay.method_mut() = request.method().clone();
            *replay.uri_mut() = request.uri().clone();
            *replay.version_mut() = request.version();
            *replay.headers_mut() = request.headers().clone();
            if let Some(context) = original {
                replay.extensions_mut().insert(context);
            }
            let response = self.inner.fetch(request).await?;
            let response = retain_response_phase(response, permit);
            if !matches!(
                response.status(),
                StatusCode::MOVED_PERMANENTLY
                    | StatusCode::FOUND
                    | StatusCode::SEE_OTHER
                    | StatusCode::TEMPORARY_REDIRECT
                    | StatusCode::PERMANENT_REDIRECT
            ) {
                return Ok(response);
            }
            let Some(location) = response.headers().get(header::LOCATION) else {
                return Ok(response);
            };
            if redirects == MAX_REDIRECTS {
                return Err(invalid("S3 redirect limit exceeded"));
            }
            // Do not silently turn a write into a GET after 301/302/303.
            if matches!(
                response.status(),
                StatusCode::MOVED_PERMANENTLY | StatusCode::FOUND | StatusCode::SEE_OTHER
            ) && !matches!(*replay.method(), Method::GET | Method::HEAD)
            {
                return Err(invalid(
                    "S3 redirect cannot change a mutating request method",
                ));
            }
            let location = location
                .to_str()
                .map_err(|error| invalid("invalid S3 redirect Location").set_source(error))?;
            let mut next = selected
                .actual()
                .join(location)
                .map_err(|error| invalid("invalid S3 redirect target").set_source(error))?;
            next.set_fragment(None);
            // A future original joint owner must authorize a new service route
            // before credentials or dispatch. Do not leak authentication now.
            if next.origin() != selected.actual().origin()
                || !next.username().is_empty()
                || next.password().is_some()
            {
                return Err(Error::new(
                    ErrorKind::Unsupported,
                    "cross-origin S3 redirect requires governed target admission",
                )
                .set_permanent());
            }
            *replay.uri_mut() = next
                .as_str()
                .parse()
                .map_err(|error| invalid("invalid S3 redirect request URI").set_source(error))?;
            if replay.uri().path_and_query() != frozen_target.as_ref() {
                return Err(Error::new(ErrorKind::Unsupported,
                    "S3 redirect changes the frozen object target; scoped target authorization is required")
                    .set_permanent());
            }
            clear_signing_headers(replay.headers_mut());
            // Exit the actual response stream before issuing its replacement.
            // This does not manufacture a physical-exit receipt or a grant.
            drop(response);
            request = replay;
        }
        unreachable!("finite redirect loop returns on its final turn")
    }
}

/// SDK skip_signature also affects presign, which never calls HttpTransport.
/// Forward the SDK's actual unsigned request through the same Provider signer.
#[derive(Clone, Debug)]
pub(super) struct S3PresignLayer {
    signer: Arc<S3RequestSigner>,
}
impl S3PresignLayer {
    pub(super) fn new(signer: Arc<S3RequestSigner>) -> Self {
        Self { signer }
    }
}
impl Layer for S3PresignLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(S3PresignService {
            inner,
            signer: Arc::clone(&self.signer),
        })
    }
}
#[derive(Debug)]
struct S3PresignService {
    inner: Servicer,
    signer: Arc<S3RequestSigner>,
}
impl Service for S3PresignService {
    type Reader = oio::Reader;
    type Writer = oio::Writer;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::Copier;
    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }
    fn capability(&self) -> opendal::Capability {
        self.inner.capability()
    }
    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> opendal::Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }
    fn read(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpRead,
    ) -> opendal::Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }
    fn write(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpWrite,
    ) -> opendal::Result<Self::Writer> {
        self.inner.write(ctx, path, args)
    }
    async fn stat(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpStat,
    ) -> opendal::Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }
    fn delete(&self, ctx: &OperationContext) -> opendal::Result<Self::Deleter> {
        self.inner.delete(ctx)
    }
    fn list(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpList,
    ) -> opendal::Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }
    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
        opts: OpCopier,
    ) -> opendal::Result<Self::Copier> {
        self.inner.copy(ctx, from, to, args, opts)
    }
    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> opendal::Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }
    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> opendal::Result<RpPresign> {
        let expiry = args.expire();
        let unsigned = self
            .inner
            .presign(ctx, path, args)
            .await?
            .into_presigned_request();
        Ok(RpPresign::new(self.signer.presign(unsigned, expiry).await?))
    }
}

/// Explicit per-request context over the same pooled transport. The typed
/// extension belongs to this actual HTTP Request, not an authority lookup.
pub(super) struct GovernedRequestTransport {
    inner: opendal::HttpTransporter,
    request: novarocks_spi::connector::ConnectorRequestContext,
}
impl GovernedRequestTransport {
    pub(super) fn new(
        inner: opendal::HttpTransporter,
        request: novarocks_spi::connector::ConnectorRequestContext,
    ) -> Self {
        Self { inner, request }
    }
}
impl HttpTransport for GovernedRequestTransport {
    async fn fetch(&self, mut request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
        if request
            .extensions()
            .get::<novarocks_spi::connector::ConnectorRequestContext>()
            .is_none()
        {
            request.extensions_mut().insert(self.request.clone());
        }
        self.inner.fetch(request).await
    }
}
fn retain_response_phase(
    response: Response<HttpBody>,
    phase: Option<novarocks_spi::connector::ConnectorIoPermit>,
) -> Response<HttpBody> {
    let Some(phase) = phase else {
        return response;
    };
    let (parts, body) = response.into_parts();
    // The original body validates content length. The wrapper retains its
    // paid alias through actual body/F Drop, including partial reads/unwind.
    let stream = futures::stream::try_unfold((body, phase), |(mut body, phase)| async move {
        let chunk = opendal::raw::oio::ReadStream::read(&mut body).await?;
        if chunk.is_empty() {
            Ok(None)
        } else {
            Ok(Some((chunk, (body, phase))))
        }
    });
    Response::from_parts(parts, HttpBody::new(Box::pin(stream), None))
}

#[cfg(test)]
mod tests {
    use super::super::{ObjectStoreEndpointConfig, ObjectStoreSecretMaterial};
    use super::*;
    use crate::SecretValue;
    use crate::storage_authority::{
        AcquisitionFailure, AuthorityCapabilityPath, AuthorityMaterial, AuthorityMaterialSource,
        RefreshExecutor, RefreshPolicy, StorageAuthority, StorageAuthorityId,
    };
    use novarocks_spi::connector::{
        CatalogHandle, CatalogVersion, ConnectorInstanceId, StorageCredentialScopePrefix,
    };
    use opendal::{HttpTransporter, Operator};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::time::Instant;

    type ReplyQueue = VecDeque<(StatusCode, Option<&'static str>)>;

    #[derive(Clone, Default)]
    struct CapturedHttp {
        requests: Arc<Mutex<Vec<(Uri, http::HeaderMap)>>>,
        replies: Arc<Mutex<ReplyQueue>>,
        rotate_on_redirect: Arc<Mutex<Option<Arc<StorageAuthority>>>>,
    }
    impl HttpTransport for CapturedHttp {
        async fn fetch(&self, request: Request<Buffer>) -> opendal::Result<Response<HttpBody>> {
            self.requests
                .lock()
                .unwrap()
                .push((request.uri().clone(), request.headers().clone()));
            let (status, location) = self
                .replies
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or((StatusCode::OK, None));
            if status == StatusCode::TEMPORARY_REDIRECT {
                let rotate = self.rotate_on_redirect.lock().unwrap().take();
                if let Some(authority) = rotate {
                    install(&authority, "redirect-next");
                }
            }
            let mut response = Response::new(HttpBody::new(
                futures::stream::empty::<opendal::Result<Buffer>>(),
                Some(0),
            ));
            *response.status_mut() = status;
            response
                .headers_mut()
                .insert(header::CONTENT_LENGTH, "0".parse().unwrap());
            response
                .headers_mut()
                .insert(header::ETAG, "\"fixture\"".parse().unwrap());
            response.headers_mut().insert(
                header::LAST_MODIFIED,
                "Mon, 01 Jan 2024 00:00:00 GMT".parse().unwrap(),
            );
            if let Some(location) = location {
                response
                    .headers_mut()
                    .insert(header::LOCATION, location.parse().unwrap());
            }
            Ok(response)
        }
    }
    fn endpoint(bucket: &str, address: &str, path_style: bool) -> ObjectStoreEndpointIdentity {
        ObjectStoreEndpointIdentity::try_new(
            bucket,
            &ObjectStoreEndpointConfig {
                endpoint: address.to_owned(),
                enable_path_style_access: Some(path_style),
                region: Some("us-east-1".to_owned()),
                retry_max_times: None,
                retry_min_delay_ms: None,
                retry_max_delay_ms: None,
                timeout_ms: None,
                io_timeout_ms: None,
            },
        )
        .unwrap()
    }
    fn static_material() -> ObjectStoreCredentialSource {
        ObjectStoreCredentialSource::Static {
            reference: novarocks_spi::connector::StaticCredentialReference::try_new(
                "signing-fixture",
                "v1",
            )
            .unwrap(),
            material: ObjectStoreSecretMaterial {
                access_key_id: SecretValue::new("access-static"),
                access_key_secret: SecretValue::new("secret-static"),
                session_token: Some(SecretValue::new("token-static")),
            },
        }
    }
    fn sdk_operator(
        endpoint: &ObjectStoreEndpointIdentity,
        material: &ObjectStoreCredentialSource,
        capture: CapturedHttp,
    ) -> (Operator, Arc<S3RequestSigner>) {
        let signer = Arc::new(
            S3RequestSigner::new(
                endpoint,
                material,
                Arc::new(CredentialDenialLatch::default()),
            )
            .unwrap(),
        );
        let mut builder = opendal::services::S3::default()
            .bucket(&endpoint.bucket)
            .endpoint(&endpoint.endpoint)
            .region(&endpoint.region)
            .skip_signature()
            .disable_config_load()
            .disable_ec2_metadata();
        if !endpoint.use_path_style {
            builder = builder.enable_virtual_host_style();
        }
        let operator = Operator::new(builder)
            .unwrap()
            .layer(S3PresignLayer::new(Arc::clone(&signer)));
        let context = operator
            .base_context()
            .with_http_transport(HttpTransporter::new(S3SigningTransport::new(
                capture,
                Arc::clone(&signer),
            )));
        (operator.with_context(context), signer)
    }

    /// The oracle observes requests emitted by the real SDK, not hand-made URIs.
    #[tokio::test]
    async fn real_sdk_requests_keep_one_service_base_across_buckets_and_addressing() {
        for (address, path_style, expected) in [
            ("http://127.0.0.1:9000", true, "http://127.0.0.1:9000/"),
            ("http://[::1]:9000", true, "http://[::1]:9000/"),
            (
                "https://objects.example.test:9443",
                true,
                "https://objects.example.test:9443/",
            ),
            (
                "https://objects.example.test:9443",
                false,
                "https://objects.example.test:9443/",
            ),
            (
                "https://s3.amazonaws.com",
                false,
                "https://s3.us-east-1.amazonaws.com/",
            ),
        ] {
            let mut bases = Vec::new();
            for bucket in ["one", "two"] {
                let capture = CapturedHttp::default();
                let (operator, signer) = sdk_operator(
                    &endpoint(bucket, address, path_style),
                    &static_material(),
                    capture.clone(),
                );
                operator.stat("object").await.unwrap();
                let requests = capture.requests.lock().unwrap();
                assert_eq!(requests.len(), 1);
                let (uri, headers) = &requests[0];
                assert!(
                    headers[header::AUTHORIZATION]
                        .to_str()
                        .unwrap()
                        .contains("Credential=access-static/")
                );
                assert_eq!(headers["x-amz-security-token"], "token-static");
                let selected = signer.selected_request(uri).unwrap();
                assert_eq!(selected.service_base().as_str(), expected);
                if !path_style {
                    assert!(uri.host().unwrap().starts_with(&format!("{bucket}.")));
                }
                bases.push(selected.service_base().clone());
            }
            assert_eq!(bases[0], bases[1]);
        }
    }

    struct NeverAcquires;
    impl AuthorityMaterialSource for NeverAcquires {
        fn acquire(&self, _: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
            Err(AcquisitionFailure::NoRenewalCapability)
        }
    }
    impl RefreshExecutor for NeverAcquires {
        fn execute(&self, _: Box<dyn FnOnce() + Send + 'static>) {
            panic!("seeded signing fixture must not schedule acquisition");
        }
    }
    fn authority() -> Arc<StorageAuthority> {
        authority_for_scope("s3://one/")
    }
    fn authority_for_scope(scope: &str) -> Arc<StorageAuthority> {
        Arc::new(StorageAuthority::new(
            StorageAuthorityId::new(
                CatalogHandle::new(
                    ConnectorInstanceId::parse("lake").unwrap(),
                    CatalogVersion::from_bytes([0x11; 32]),
                ),
                StorageCredentialScopePrefix::try_from_normalized(scope).unwrap(),
                AuthorityCapabilityPath::SeededWithoutRenewal { grant: None },
            ),
            Arc::new(NeverAcquires),
            Arc::new(NeverAcquires),
            RefreshPolicy::default(),
        ))
    }
    fn install(authority: &StorageAuthority, suffix: &str) {
        authority.install_material(AuthorityMaterial::new(
            SecretValue::new(format!("access-{suffix}")),
            SecretValue::new(format!("secret-{suffix}")),
            Some(SecretValue::new(format!("token-{suffix}"))),
            Instant::now() + Duration::from_secs(3600),
        ));
    }

    fn node_local_authority(scope: &str) -> Arc<StorageAuthority> {
        use novarocks_spi::connector::{CatalogCredentialPurpose, NodeLocalCredentialReference};
        Arc::new(StorageAuthority::new(
            StorageAuthorityId::new(
                authority_for_scope(scope).id().catalog().clone(),
                StorageCredentialScopePrefix::try_from_normalized(scope).unwrap(),
                AuthorityCapabilityPath::NodeLocalProvider {
                    purpose: CatalogCredentialPurpose::ObjectStoreData,
                    principal: NodeLocalCredentialReference::try_new("ecs", "v1").unwrap(),
                    provider: crate::AliyunEcsRamRoleConfig::try_new(
                        "role",
                        vec![scope.to_owned()],
                    )
                    .unwrap(),
                },
            ),
            Arc::new(NeverAcquires),
            Arc::new(NeverAcquires),
            RefreshPolicy::default(),
        ))
    }

    #[tokio::test]
    async fn node_local_signer_refuses_every_escaped_target_before_acquisition_or_http() {
        for path_style in [true, false] {
            let scope = "s3://one/allowed/";
            let authority = node_local_authority(scope);
            let capture = CapturedHttp::default();
            let (operator, signer) = sdk_operator(
                &endpoint("one", "https://objects.example.test", path_style),
                &ObjectStoreCredentialSource::Authority(Arc::clone(&authority)),
                capture.clone(),
            );
            for error in [
                operator.stat("outside/key").await.unwrap_err(),
                operator.stat("allowed").await.unwrap_err(),
                operator.stat("allowed/../outside/key").await.unwrap_err(),
                operator.list("outside/").await.unwrap_err(),
                operator.list("allowed-sibling/").await.unwrap_err(),
                operator.write("outside/key", "data").await.unwrap_err(),
                operator.delete("outside/key").await.unwrap_err(),
                operator
                    .presign_read("outside/key", Duration::from_secs(30))
                    .await
                    .unwrap_err(),
                operator
                    .copy("outside/key", "allowed/copied")
                    .await
                    .unwrap_err(),
            ] {
                assert_eq!(error.kind(), ErrorKind::PermissionDenied, "{error}");
            }
            assert!(capture.requests.lock().unwrap().is_empty());
            // Cover multipart and a mixed-key batch emitted at the bucket URL.
            let base = if path_style {
                "https://objects.example.test/one"
            } else {
                "https://one.objects.example.test"
            };
            for (target, body) in [
                ("/outside/key?uploads", ""),
                ("/allowed/../outside/key", ""),
                ("/allowed/%2e%2e/outside/key", ""),
                ("/allowed/%2E/outside/key", ""),
                ("/allowed/%5c..%5coutside/key", ""),
                ("/outside/key?uploadId=id&partNumber=1", ""),
                (
                    "/?delete",
                    "<Delete><Quiet>true</Quiet><Object><Key>allowed/a</Key></Object><Object><Key>outside/b</Key></Object></Delete>",
                ),
            ] {
                let mut request = Request::builder()
                    .method(Method::POST)
                    .uri(format!("{base}{target}"))
                    .body(Buffer::from(body.to_owned()))
                    .unwrap();
                assert_eq!(
                    signer.sign_header(&mut request).await.unwrap_err().kind(),
                    ErrorKind::PermissionDenied
                );
            }
            for target in ["/allowed/../outside", "/allowed/%2e%2e/outside", "/allowed"] {
                let request = PresignedRequest::new(
                    Method::GET,
                    format!("{base}{target}").parse().unwrap(),
                    http::HeaderMap::new(),
                );
                assert_eq!(
                    signer
                        .presign(request, Duration::from_secs(30))
                        .await
                        .unwrap_err()
                        .kind(),
                    ErrorKind::PermissionDenied
                );
            }
            // In-scope targets continue through the original signer and token.
            install(&authority, "scoped");
            operator.stat("allowed/key").await.unwrap();
            operator
                .presign_read("allowed/key", Duration::from_secs(30))
                .await
                .unwrap();
            let uri: Uri = format!("{base}/?delete").parse().unwrap();
            assert!(signer.authorize_node_local_target(&uri, &http::HeaderMap::new(), Some(&Buffer::from("<Delete><Quiet>true</Quiet><Object><Key>allowed/a&amp;b</Key></Object></Delete>"))).is_ok());
            assert_eq!(capture.requests.lock().unwrap().len(), 1);
            for copy_source in ["one/allowed/source", "/one/allowed/source"] {
                let mut headers = http::HeaderMap::new();
                headers.insert("x-amz-copy-source", copy_source.parse().unwrap());
                let uri: Uri = format!("{base}/allowed/target?uploadId=id&partNumber=1")
                    .parse()
                    .unwrap();
                assert!(
                    signer
                        .authorize_node_local_target(&uri, &headers, None)
                        .is_ok()
                );
            }
            // The mock HTTP body does not implement CopyObject's XML result;
            // observing its signed request proves the actual SDK header passed.
            let _ = operator.copy("allowed/source", "allowed/target").await;
            let requests = capture.requests.lock().unwrap();
            assert!(requests.len() >= 2);
            assert!(requests.iter().any(|(_, headers)| {
                headers
                    .get("x-amz-copy-source")
                    .is_some_and(|value| value == "one/allowed/source")
            }));
        }
    }

    #[tokio::test]
    async fn node_local_scope_preserves_the_original_literal_percent_key() {
        let scope = "s3://one/allowed%20/";
        let config =
            crate::AliyunEcsRamRoleConfig::try_new("role", vec![scope.to_owned()]).unwrap();
        assert!(config.scope_for_locations(&[format!("{scope}key")]).is_ok());
        let authority = node_local_authority(scope);
        install(&authority, "percent");
        for path_style in [true, false] {
            let capture = CapturedHttp::default();
            let (operator, _) = sdk_operator(
                &endpoint("one", "https://objects.example.test", path_style),
                &ObjectStoreCredentialSource::Authority(Arc::clone(&authority)),
                capture.clone(),
            );
            operator.stat("allowed%20/key").await.unwrap();
            assert_eq!(
                operator.stat("allowed /key").await.unwrap_err().kind(),
                ErrorKind::PermissionDenied
            );
            let requests = capture.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(requests[0].0.path().contains("allowed%2520/key"));
        }
    }

    #[tokio::test]
    async fn real_sdk_signing_observes_authority_rotation_presign_and_original_denial() {
        let authority = authority();
        install(&authority, "one");
        let capture = CapturedHttp::default();
        let (operator, signer) = sdk_operator(
            &endpoint("one", "https://objects.example.test", true),
            &ObjectStoreCredentialSource::Authority(Arc::clone(&authority)),
            capture.clone(),
        );
        operator.stat("first").await.unwrap();
        install(&authority, "two");
        operator.stat("second").await.unwrap();
        {
            let requests = capture.requests.lock().unwrap();
            assert_eq!(requests.len(), 2);
            for (index, suffix) in ["one", "two"].into_iter().enumerate() {
                let headers = &requests[index].1;
                assert!(
                    headers[header::AUTHORIZATION]
                        .to_str()
                        .unwrap()
                        .contains(&format!("Credential=access-{suffix}/"))
                );
                assert_eq!(
                    headers["x-amz-security-token"].to_str().unwrap(),
                    format!("token-{suffix}")
                );
            }
        }
        assert_eq!(
            authority.metrics().cache_hits,
            2,
            "each real SDK request reaches material_for_request"
        );
        install(&authority, "three");
        let signed = operator
            .presign_read("signed", Duration::from_secs(60))
            .await
            .unwrap();
        let signed = Url::parse(&signed.uri().to_string()).unwrap();
        assert!(
            signed
                .query_pairs()
                .any(|(name, value)| name == "X-Amz-Credential"
                    && value.starts_with("access-three/"))
        );
        assert!(
            signed
                .query_pairs()
                .any(|(name, value)| name == "X-Amz-Security-Token" && value == "token-three")
        );
        assert!(
            signed
                .query_pairs()
                .any(|(name, value)| name == "X-Amz-Signature" && !value.is_empty())
        );
        assert_eq!(authority.metrics().cache_hits, 3);
        assert_eq!(
            capture.requests.lock().unwrap().len(),
            2,
            "real SDK presign is signed by the service layer without HTTP"
        );
        authority.close(AcquisitionFailure::Denied(
            "signing capability revoked".to_owned(),
        ));
        let failure = operator.stat("denied").await.unwrap_err();
        assert_eq!(failure.kind(), ErrorKind::PermissionDenied);
        assert!(!failure.is_temporary());
        assert!(signer.denial.observed().is_some());
        assert_eq!(
            capture.requests.lock().unwrap().len(),
            2,
            "denial cannot dispatch the SDK request"
        );
    }

    #[tokio::test]
    async fn real_sdk_redirect_requires_the_original_object_target_before_resigning() {
        for (location, allowed, expected_calls) in [
            ("/one/allowed/original", true, 2),
            ("/other-bucket/allowed/original", false, 1),
            ("/one/outside/original", false, 1),
            ("/one/allowed/another-object", false, 1),
            ("/one/allowed/original?versionId=other", false, 1),
            (
                "https://foreign.example.test/one/allowed/original",
                false,
                1,
            ),
        ] {
            let authority = authority_for_scope("s3://one/allowed/");
            install(&authority, "redirect");
            let capture = CapturedHttp::default();
            capture
                .replies
                .lock()
                .unwrap()
                .push_back((StatusCode::TEMPORARY_REDIRECT, Some(location)));
            *capture.rotate_on_redirect.lock().unwrap() = Some(Arc::clone(&authority));
            let (operator, _) = sdk_operator(
                &endpoint("one", "https://objects.example.test", true),
                &ObjectStoreCredentialSource::Authority(Arc::clone(&authority)),
                capture.clone(),
            );
            let result = operator.stat("allowed/original").await;
            assert_eq!(result.is_ok(), allowed);
            if !allowed {
                assert_eq!(result.unwrap_err().kind(), ErrorKind::Unsupported);
            }
            let requests = capture.requests.lock().unwrap();
            assert_eq!(requests.len(), expected_calls);
            assert!(
                requests
                    .iter()
                    .all(|(_, headers)| headers.contains_key(header::AUTHORIZATION))
            );
            if allowed {
                assert_eq!(
                    requests[0].0.path_and_query(),
                    requests[1].0.path_and_query()
                );
                assert!(
                    requests[0].1[header::AUTHORIZATION]
                        .to_str()
                        .unwrap()
                        .contains("Credential=access-redirect/")
                );
                assert!(
                    requests[1].1[header::AUTHORIZATION]
                        .to_str()
                        .unwrap()
                        .contains("Credential=access-redirect-next/")
                );
                assert_eq!(requests[1].1["x-amz-security-token"], "token-redirect-next");
                assert_ne!(
                    requests[0].1[header::AUTHORIZATION],
                    requests[1].1[header::AUTHORIZATION],
                    "the same authorized target is freshly signed with the authority's new material"
                );
            }
            assert_eq!(
                authority.metrics().cache_hits,
                expected_calls as u64,
                "a changed bucket, scope, object or query must not trigger a new sign"
            );
        }
    }
}

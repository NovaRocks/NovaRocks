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

//! Explicit, role-local ECS acquisition. StorageAuthority alone owns renewal.
// Design: ADR-0201 (docs/adr/ADR-0201-role-local-node-credential-sources.md)

use std::sync::{Arc, Mutex};
use std::time::{Instant, SystemTime};

use bytes::Bytes;
use novarocks_spi::connector::{
    ConnectorRequestContext, NetworkServiceOrigin, SharedDependency, StorageCredentialScopePrefix,
};
use reqsign_aliyun_oss::EcsRamRoleCredentialProvider;
use reqsign_core::{Context, HttpSend, ProvideCredential};

use crate::{
    AcquisitionFailure, AuthorityMaterial, AuthorityMaterialSource, FileError, FileIoRuntime,
    FileResult, FsLocation, FsScheme, SecretValue,
};

const METADATA_ORIGIN: &str = "http://100.100.100.200";

/// Immutable deployment definition, never a snapshot of temporary material.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AliyunEcsRamRoleConfig {
    role_name: Arc<str>,
    scope_prefixes: Arc<[StorageCredentialScopePrefix]>,
}

impl AliyunEcsRamRoleConfig {
    pub fn try_new(role_name: &str, prefixes: Vec<String>) -> FileResult<Self> {
        if role_name.is_empty()
            || !role_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'@'))
        {
            return Err(FileError::invalid("invalid ECS RAM role name"));
        }
        if prefixes.is_empty() {
            return Err(FileError::invalid(
                "ECS RAM role requires explicit storage scope prefixes",
            ));
        }
        let mut scope_prefixes = prefixes
            .iter()
            .map(|prefix| {
                if !prefix.ends_with('/') {
                    return Err(FileError::invalid(
                        "ECS storage scope must be a directory prefix",
                    ));
                }
                StorageCredentialScopePrefix::try_from_normalized(prefix)
                    .map_err(|error| FileError::invalid(error.to_string()))
            })
            .collect::<FileResult<Vec<_>>>()?;
        scope_prefixes.sort();
        if scope_prefixes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(FileError::invalid("duplicate ECS storage scope prefix"));
        }
        Ok(Self {
            role_name: Arc::from(role_name),
            scope_prefixes: scope_prefixes.into(),
        })
    }

    pub fn role_name(&self) -> &str {
        &self.role_name
    }

    pub fn scope_prefixes(&self) -> &[StorageCredentialScopePrefix] {
        &self.scope_prefixes
    }

    /// The handle's locations must all fit one explicitly configured scope.
    /// No common ancestor is invented to enlarge the deployment definition.
    pub fn scope_for_locations(
        &self,
        locations: &[String],
    ) -> FileResult<StorageCredentialScopePrefix> {
        let canonical = locations
            .iter()
            .map(|value| {
                let location = FsLocation::parse(value)?;
                if location.scheme() != FsScheme::ObjectStore {
                    return Err(FileError::invalid(
                        "ECS credentials require object-store locations",
                    ));
                }
                let value = format!(
                    "s3://{}{}",
                    location.authority().unwrap_or_default(),
                    location.path()
                );
                StorageCredentialScopePrefix::try_from_normalized(&value)
                    .map_err(|error| FileError::invalid(error.to_string()))?;
                Ok(value)
            })
            .collect::<FileResult<Vec<_>>>()?;
        if canonical.is_empty() {
            return Err(FileError::invalid("ECS storage operation has no location"));
        }
        self.scope_prefixes
            .iter()
            .filter(|prefix| {
                canonical.iter().all(|value| {
                    value.starts_with(prefix.as_str())
                        || value == prefix.as_str().trim_end_matches('/')
                })
            })
            .max_by_key(|prefix| prefix.as_str().len())
            .cloned()
            .ok_or_else(|| {
                FileError::new(
                    crate::FileErrorKind::Permission,
                    "object-store operation leaves its configured ECS scope",
                )
            })
    }

    pub fn material_source(
        &self,
        runtime: Arc<dyn FileIoRuntime>,
    ) -> Arc<dyn AuthorityMaterialSource> {
        Arc::new(EcsMaterialSource {
            config: self.clone(),
            runtime,
        })
    }
}

struct EcsMaterialSource {
    config: AliyunEcsRamRoleConfig,
    runtime: Arc<dyn FileIoRuntime>,
}

impl EcsMaterialSource {
    async fn acquire_material(
        config: AliyunEcsRamRoleConfig,
        deadline: Instant,
        request: Option<ConnectorRequestContext>,
    ) -> Result<AuthorityMaterial, AcquisitionFailure> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| {
                AcquisitionFailure::Transient(
                    "ECS metadata HTTP client could not be constructed".into(),
                )
            })?;
        let failure = Arc::new(Mutex::new(None));
        let transport = EcsHttpSend {
            client,
            deadline,
            request,
            failure: failure.clone(),
        };
        let context = Context::new().with_http_send(transport);
        Self::acquire_with_context(config, deadline, context, failure).await
    }

    async fn acquire_with_context(
        config: AliyunEcsRamRoleConfig,
        deadline: Instant,
        context: Context,
        failure: Arc<Mutex<Option<MetadataFailure>>>,
    ) -> Result<AuthorityMaterial, AcquisitionFailure> {
        let provider = EcsRamRoleCredentialProvider::new()
            .with_endpoint(METADATA_ORIGIN)
            .with_role_name(config.role_name())
            .with_disable_imdsv1(true);
        // A context with no ambient environment, files or commands is used.
        let result = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            provider.provide_credential(&context),
        )
        .await
        .map_err(|_| {
            AcquisitionFailure::Transient("ECS metadata acquisition deadline elapsed".into())
        })?;
        let credential = result
            .map_err(
                |error| match failure.lock().unwrap_or_else(|p| p.into_inner()).as_ref() {
                    Some(MetadataFailure::Status(401 | 403)) => AcquisitionFailure::Denied(
                        "ECS metadata denied role credential access".into(),
                    ),
                    Some(MetadataFailure::Status(404)) => AcquisitionFailure::Denied(
                        "configured ECS RAM role is not bound to this instance".into(),
                    ),
                    Some(MetadataFailure::Unreachable) => AcquisitionFailure::MetadataUnreachable(
                        "ECS metadata endpoint cannot be reached from this process".into(),
                    ),
                    Some(MetadataFailure::Status(status)) => AcquisitionFailure::Transient(
                        format!("ECS metadata returned HTTP {status}"),
                    ),
                    Some(MetadataFailure::Cancelled) => AcquisitionFailure::Transient(
                        "ECS metadata acquisition was cancelled".into(),
                    ),
                    Some(MetadataFailure::Transport) => {
                        AcquisitionFailure::Transient("ECS metadata transport failed".into())
                    }
                    None => AcquisitionFailure::Denied(format!(
                        "ECS metadata returned invalid credential material ({:?})",
                        error.kind()
                    )),
                },
            )?
            .ok_or_else(|| {
                AcquisitionFailure::Denied(
                    "explicit ECS provider returned no credential material".into(),
                )
            })?;
        let expiration = credential.expires_in.ok_or_else(|| {
            AcquisitionFailure::Denied("ECS credential material has no expiration".into())
        })?;
        let remaining = expiration
            .as_system_time()
            .duration_since(SystemTime::now())
            .map_err(|_| {
                AcquisitionFailure::Transient(
                    "ECS metadata returned expired credential material".into(),
                )
            })?;
        let now = Instant::now();
        let not_after = now.checked_add(remaining).ok_or_else(|| {
            AcquisitionFailure::Denied("ECS credential expiration cannot be represented".into())
        })?;
        Ok(AuthorityMaterial::new(
            SecretValue::new(credential.access_key_id),
            SecretValue::new(credential.access_key_secret),
            credential.security_token.map(SecretValue::new),
            not_after,
        ))
    }
}

impl AuthorityMaterialSource for EcsMaterialSource {
    fn acquire(&self, deadline: Instant) -> Result<AuthorityMaterial, AcquisitionFailure> {
        self.runtime
            .block_on_authority_material(Box::pin(Self::acquire_material(
                self.config.clone(),
                deadline,
                None,
            )))
    }

    fn acquire_async<'a>(
        &'a self,
        deadline: Instant,
        request: ConnectorRequestContext,
    ) -> futures::future::BoxFuture<'a, Result<AuthorityMaterial, AcquisitionFailure>> {
        Box::pin(Self::acquire_material(
            self.config.clone(),
            deadline.min(request.deadline()),
            Some(request),
        ))
    }
}

#[derive(Clone, Debug)]
enum MetadataFailure {
    Status(u16),
    Unreachable,
    Transport,
    Cancelled,
}

#[derive(Clone)]
struct EcsHttpSend {
    client: reqwest::Client,
    deadline: Instant,
    request: Option<ConnectorRequestContext>,
    failure: Arc<Mutex<Option<MetadataFailure>>>,
}

impl std::fmt::Debug for EcsHttpSend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EcsHttpSend")
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

impl EcsHttpSend {
    fn fail(&self, failure: MetadataFailure) -> reqsign_core::Error {
        *self.failure.lock().unwrap_or_else(|p| p.into_inner()) = Some(failure);
        reqsign_core::Error::unexpected("ECS metadata request failed")
    }
}

impl HttpSend for EcsHttpSend {
    async fn http_send(
        &self,
        request: http::Request<Bytes>,
    ) -> reqsign_core::Result<http::Response<Bytes>> {
        let (mut parts, body) = request.into_parts();
        let url = url::Url::parse(&parts.uri.to_string())
            .map_err(|_| self.fail(MetadataFailure::Transport))?;
        if url.origin().ascii_serialization() != METADATA_ORIGIN {
            return Err(self.fail(MetadataFailure::Transport));
        }
        if let Some(value) = parts.headers.get_mut("X-aliyun-ecs-metadata-token") {
            value.set_sensitive(true);
        }
        let operation = async {
            let _permit = if let Some(context) = &self.request {
                let dependency = NetworkServiceOrigin::from_selected_url(&url)
                    .map(SharedDependency::Network)
                    .map_err(|_| self.fail(MetadataFailure::Transport))?;
                context
                    .admit_selected_io(dependency)
                    .await
                    .map_err(|_| self.fail(MetadataFailure::Cancelled))?
            } else {
                None
            };
            let response = self
                .client
                .request(parts.method, url)
                .headers(parts.headers)
                .body(body)
                .send()
                .await
                .map_err(|error| {
                    self.fail(if error.is_connect() {
                        MetadataFailure::Unreachable
                    } else {
                        MetadataFailure::Transport
                    })
                })?;
            let status = response.status();
            let bytes = if status == http::StatusCode::OK {
                response
                    .bytes()
                    .await
                    .map_err(|_| self.fail(MetadataFailure::Transport))?
            } else {
                // Upstream error formatting includes bodies. Never forward a
                // metadata body into an SDK error or the public reason chain.
                *self.failure.lock().unwrap_or_else(|p| p.into_inner()) =
                    Some(MetadataFailure::Status(status.as_u16()));
                Bytes::new()
            };
            http::Response::builder()
                .status(status)
                .body(bytes)
                .map_err(|_| self.fail(MetadataFailure::Transport))
        };
        if let Some(context) = &self.request {
            tokio::select! {
                result = tokio::time::timeout_at(tokio::time::Instant::from_std(self.deadline), operation) => result.map_err(|_| self.fail(MetadataFailure::Transport))?,
                _ = context.stop().stopped() => Err(self.fail(MetadataFailure::Cancelled)),
            }
        } else {
            tokio::time::timeout_at(tokio::time::Instant::from_std(self.deadline), operation)
                .await
                .map_err(|_| self.fail(MetadataFailure::Transport))?
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::time::Duration;

    #[derive(Clone, Debug)]
    struct MetadataSequence {
        responses: Arc<Mutex<VecDeque<(u16, String)>>>,
        requests: Arc<Mutex<Vec<(String, String, Option<String>)>>>,
        failure: Arc<Mutex<Option<MetadataFailure>>>,
    }
    impl HttpSend for MetadataSequence {
        async fn http_send(
            &self,
            request: http::Request<Bytes>,
        ) -> reqsign_core::Result<http::Response<Bytes>> {
            self.requests.lock().unwrap().push((
                request.method().to_string(),
                request.uri().to_string(),
                request
                    .headers()
                    .get("X-aliyun-ecs-metadata-token")
                    .map(|v| v.to_str().unwrap().to_string()),
            ));
            let (status, body) = self
                .responses
                .lock()
                .unwrap()
                .pop_front()
                .expect("expected metadata request");
            if status != 200 {
                *self.failure.lock().unwrap() = Some(MetadataFailure::Status(status));
            }
            Ok(http::Response::builder()
                .status(status)
                .body(Bytes::from(body))
                .unwrap())
        }
    }
    fn configuration() -> AliyunEcsRamRoleConfig {
        AliyunEcsRamRoleConfig::try_new("test-role", vec!["s3://bucket/allowed/".into()]).unwrap()
    }
    fn credential_response(id: &str, lifetime: Duration) -> String {
        let expiration = (reqsign_core::time::Timestamp::now() + lifetime).format_rfc3339_zulu();
        serde_json::json!({"Code":"Success","AccessKeyId":id,"AccessKeySecret":"secret","SecurityToken":format!("token-{id}"),"Expiration":expiration}).to_string()
    }
    fn sequence(responses: Vec<(u16, String)>) -> MetadataSequence {
        MetadataSequence {
            responses: Arc::new(Mutex::new(responses.into())),
            requests: Arc::default(),
            failure: Arc::default(),
        }
    }
    async fn acquire(sequence: &MetadataSequence) -> Result<AuthorityMaterial, AcquisitionFailure> {
        EcsMaterialSource::acquire_with_context(
            configuration(),
            Instant::now() + Duration::from_secs(5),
            Context::new().with_http_send(sequence.clone()),
            sequence.failure.clone(),
        )
        .await
    }

    #[test]
    fn deployment_scope_is_explicit_and_component_bound() {
        let config = configuration();
        assert!(
            config
                .scope_for_locations(&["s3://bucket/allowed/data".into()])
                .is_ok()
        );
        assert!(
            config
                .scope_for_locations(&["s3a://bucket/allowed/data".into()])
                .is_ok()
        );
        for path in [
            "s3://bucket/allowed-sibling/data",
            "s3://other/allowed/data",
            "s3://bucket/allowed/../outside",
            "file:///tmp/data",
        ] {
            assert!(
                config.scope_for_locations(&[path.into()]).is_err(),
                "{path}"
            );
        }
        assert!(AliyunEcsRamRoleConfig::try_new("test-role", vec![]).is_err());
        assert!(
            AliyunEcsRamRoleConfig::try_new("test-role", vec!["s3://bucket/allowed".into()])
                .is_err()
        );
        assert!(
            AliyunEcsRamRoleConfig::try_new("other/path", vec!["s3://bucket/".into()]).is_err()
        );
    }

    #[tokio::test]
    async fn direct_sdk_acquisition_keeps_imdsv2_and_observes_rotation() {
        let sequence = sequence(vec![
            (200, "imds-token-one".into()),
            (200, credential_response("first", Duration::from_secs(600))),
            (200, "imds-token-two".into()),
            (200, credential_response("second", Duration::from_secs(600))),
        ]);
        let first = acquire(&sequence).await.unwrap();
        let second = acquire(&sequence).await.unwrap();
        assert_eq!(first.access_key_id().expose_secret(), "first");
        assert_eq!(second.access_key_id().expose_secret(), "second");
        assert_eq!(
            second.session_token().unwrap().expose_secret(),
            "token-second"
        );
        let requests = sequence.requests.lock().unwrap();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].0, "PUT");
        assert!(requests[1].1.ends_with("/test-role"));
        assert_eq!(requests[1].2.as_deref(), Some("imds-token-one"));
        assert_eq!(requests[3].2.as_deref(), Some("imds-token-two"));
        assert!(!format!("{second:?}").contains("token-second"));
    }

    #[tokio::test]
    async fn upstream_short_credentials_are_judged_by_original_authority_policy() {
        let sequence = sequence(vec![
            (200, "imds-token".into()),
            (200, credential_response("short", Duration::from_secs(10))),
        ]);
        let material = acquire(&sequence).await.unwrap();
        assert!(material.not_after() > Instant::now() + Duration::from_secs(5));
    }

    #[tokio::test]
    async fn imdsv2_denial_never_falls_back_or_displays_metadata_body() {
        let sequence = sequence(vec![(403, "private-response-canary".into())]);
        let failure = acquire(&sequence).await.unwrap_err();
        assert!(matches!(failure, AcquisitionFailure::Denied(_)));
        assert_eq!(sequence.requests.lock().unwrap().len(), 1);
        assert!(!format!("{failure:?}").contains("private-response-canary"));
    }

    #[tokio::test]
    async fn missing_role_is_distinct_from_transient_metadata_failure() {
        for (status, denied) in [(404, true), (503, false)] {
            let sequence = sequence(vec![
                (200, "imds-token".into()),
                (status, "unused-error-body".into()),
            ]);
            let failure = acquire(&sequence).await.unwrap_err();
            assert_eq!(matches!(failure, AcquisitionFailure::Denied(_)), denied);
            if !denied {
                assert!(matches!(failure, AcquisitionFailure::Transient(_)));
            }
        }
    }
}

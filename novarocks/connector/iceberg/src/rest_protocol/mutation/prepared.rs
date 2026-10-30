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

//! A REST request prepared entirely before entering its evidence interval.

use super::*;
use crate::catalog::publication::EvidenceContext;
use crate::catalog::publication_dispatch::{PreparationFailure, PreparationResult};
use crate::commit::attempt::observation::{DispatchEvidence, ProtocolObservation};
use crate::commit::publication_descriptor::{
    PublicationDescriptor, RecoveryBinding, RecoveryPrincipalScope,
};
use novarocks_spi::connector::{
    ConnectorOperationControl, ConnectorRequestContext, ProviderBindingEpoch,
};
use std::sync::Arc;

/// Only the catalog owner constructs this route from its admitted operation.
#[derive(Clone, Debug)]
pub(crate) enum MutationRoute {
    CommitTable(TableIdent),
    CreateTable(NamespaceIdent),
    CreateNamespace,
    DropNamespace(NamespaceIdent),
    DropTable(TableIdent),
    CreateView(NamespaceIdent),
    ReplaceView(TableIdent),
    DropView(TableIdent),
}
impl MutationRoute {
    fn request(&self, binding: &SessionBinding) -> (Method, String, bool) {
        match self {
            Self::CommitTable(table) => (Method::POST, binding.table_url(table), true),
            Self::CreateTable(namespace) => (Method::POST, binding.tables_url(namespace), true),
            Self::CreateNamespace => (Method::POST, binding.namespaces_url(), true),
            Self::DropNamespace(namespace) => {
                (Method::DELETE, binding.namespace_url(namespace), false)
            }
            Self::DropTable(table) => (Method::DELETE, binding.table_url(table), false),
            Self::CreateView(namespace) => (Method::POST, binding.views_url(namespace), true),
            Self::ReplaceView(view) => (Method::POST, binding.view_url(view), true),
            Self::DropView(view) => (Method::DELETE, binding.view_url(view), false),
        }
    }
}

pub(crate) struct PreparedRestMutation {
    request: RestRequest,
    binding: RecoveryBinding,
    expectation: AttributionExpectation,
    owner: Arc<crate::rest_protocol::auth::RestAuthOwner>,
    keep_document: bool,
    original: ConnectorRequestContext,
}
impl std::fmt::Debug for PreparedRestMutation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedRestMutation")
            .field("method", self.request.method())
            .field("keep_document", &self.keep_document)
            .finish_non_exhaustive()
    }
}

impl PreparedRestMutation {
    pub(crate) async fn prepare(
        endpoints: Arc<RestEndpoints>,
        route: MutationRoute,
        descriptor: &PublicationDescriptor,
        generation: ProviderBindingEpoch,
        preparation: &ConnectorRequestContext,
        expectation: AttributionExpectation,
        keep_document: bool,
        delegation: Option<HeaderValue>,
    ) -> PreparationResult<Self> {
        use crate::iceberg::{Error, ErrorKind};
        let stopped = PreparationFailure::from;
        let binding_failure = |step| {
            PreparationFailure(novarocks_spi::connector::ConnectorMutationFailure::new(
                crate::rest_protocol::evidence::not_written_kind(NotWrittenCause::Binding(step)),
                "REST publication session or authentication preparation failed",
            ))
        };
        preparation.check_active().map_err(stopped)?;
        endpoints.publication_generation.require(generation)?;
        descriptor.validate()?;
        // These awaits are session/authentication reads. Their shared attempt
        // remains driven by other waiters if this caller stops waiting.
        let request_context = RequestContext::from_connector(preparation);
        let session = tokio::select! {
            result = endpoints.session_in(&request_context) => result.map_err(|failure| match failure.original { Some(original) => PreparationFailure::from(original), None => binding_failure(failure.step) })?,
            _ = preparation.stop().stopped() => return Err(PreparationFailure(novarocks_spi::connector::ConnectorMutationFailure::new(novarocks_spi::connector::ConnectorMutationFailureKind::Cancelled, "REST publication preparation cancelled"))),
            _ = tokio::time::sleep_until(preparation.deadline().into()) => return Err(PreparationFailure(novarocks_spi::connector::ConnectorMutationFailure::new(novarocks_spi::connector::ConnectorMutationFailureKind::DeadlineExceeded, "REST publication preparation deadline elapsed"))),
        };
        let mut headers = session.request_headers(delegation);
        tokio::select! {
            result = endpoints.owner().authorize_in(&mut headers, &request_context) => result.map_err(|failure| match failure.original { Some(original) => PreparationFailure::from(original), None => binding_failure(failure.step) })?,
            _ = preparation.stop().stopped() => return Err(PreparationFailure(novarocks_spi::connector::ConnectorMutationFailure::new(novarocks_spi::connector::ConnectorMutationFailureKind::Cancelled, "REST publication preparation cancelled"))),
            _ = tokio::time::sleep_until(preparation.deadline().into()) => return Err(PreparationFailure(novarocks_spi::connector::ConnectorMutationFailure::new(novarocks_spi::connector::ConnectorMutationFailureKind::DeadlineExceeded, "REST publication preparation deadline elapsed"))),
        }
        preparation.check_active().map_err(stopped)?;
        let (method, endpoint, has_body) = route.request(&session);
        let parsed = reqwest::Url::parse(&endpoint).map_err(|_| {
            Error::new(ErrorKind::DataInvalid, "Invalid prepared REST mutation URL")
        })?;
        let lifetime = match session.idempotency_capability() {
            crate::rest_protocol::configuration::IdempotencyCapability::Supported { lifetime } => {
                headers.insert(
                    "Idempotency-Key",
                    HeaderValue::from_str(&descriptor.key.to_string())
                        .expect("UUIDv7 is a valid header value"),
                );
                Some(*lifetime)
            }
            crate::rest_protocol::configuration::IdempotencyCapability::Absent
            | crate::rest_protocol::configuration::IdempotencyCapability::Invalid { .. } => None,
        };
        let request = RestRequest::new(method.clone(), endpoint.clone(), headers);
        let request = if has_body {
            if descriptor.request.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Prepared REST mutation requires its frozen JSON body",
                )
                .into());
            }
            request.with_encoded_json_body(descriptor.request.clone())
        } else {
            if !descriptor.request.is_empty() {
                return Err(Error::new(
                    ErrorKind::DataInvalid,
                    "Bodyless REST mutation has unexpected frozen bytes",
                )
                .into());
            }
            request
        };
        let principal_scope = if endpoints.owner().is_anonymous() {
            RecoveryPrincipalScope::Anonymous
        } else {
            // This records an exact constraint, without claiming knowledge of
            // the subject hidden behind an opaque token. Recovery must retain
            // this generation's immutable identity, never just the auth mode.
            RecoveryPrincipalScope::CatalogGeneration {
                generation: generation.to_bytes(),
            }
        };
        let warehouse = session.warehouse().map(str::to_owned);
        let domain = serde_json::to_string(&(
            parsed.origin().ascii_serialization(),
            &warehouse,
            &principal_scope,
        ))
        .map_err(|error| {
            Error::new(
                ErrorKind::DataInvalid,
                "Cannot encode REST deduplication domain",
            )
            .with_source(error)
        })?;
        let binding = RecoveryBinding {
            generation: generation.to_bytes(),
            method: method.to_string(),
            endpoint,
            warehouse,
            query: parsed
                .query_pairs()
                .map(|(key, value)| (key.into_owned(), value.into_owned()))
                .collect(),
            principal_scope,
            deduplication_domain: domain,
            lifetime,
        };
        Ok(Self {
            request,
            binding,
            expectation,
            owner: Arc::clone(endpoints.owner()),
            keep_document,
            original: preparation.clone(),
        })
    }

    pub(crate) fn binding(&self) -> &RecoveryBinding {
        &self.binding
    }

    /// Starts no new session/auth request and serializes no request data.
    pub(crate) async fn dispatch(
        self,
        context: EvidenceContext,
    ) -> Result<PreparedRestOutcome, novarocks_spi::connector::ConnectorError> {
        // First publication is independent of statement cancellation after
        // dispatch. A nested necessary publication retains its phase's own
        // stop and the deadline already bounded by the original journal.
        let stop = if self
            .original
            .necessary_finalization_deadline_cap()
            .is_some()
        {
            self.original.stop().clone()
        } else {
            novarocks_spi::connector::ConnectorStopOwner::new().view()
        };
        let original = self
            .original
            .for_existing_obligation(context.deadline(), stop);
        let request_context = RequestContext::from_connector(&original);
        let (stage, body) = self
            .owner
            .http()
            .send_once(
                Ok(self.request),
                self.expectation.endpoint().success_statuses(),
                &request_context,
            )
            .await?;
        let (read, document) = match (self.expectation.endpoint().response_shape(), body) {
            (Some(shape), Some(body)) => {
                read_view(shape, &self.expectation, body, self.keep_document).await
            }
            (None, Some(body)) => (read_empty(body).await, None),
            _ => (BodyRead::NotRead, None),
        };
        let evidence = classify(&stage, read, &self.expectation);
        let document = document.filter(|_| matches!(evidence, RestEvidence::Applied { .. }));
        Ok(PreparedRestOutcome {
            evidence,
            stage,
            document,
        })
    }
}

pub(crate) struct PreparedRestOutcome {
    pub evidence: RestEvidence,
    pub stage: DispatchStage,
    pub document: Option<Vec<u8>>,
}
impl PreparedRestOutcome {
    /// Mapping is a mechanism observation, not a retryable-error inference.
    pub(crate) fn dispatch_evidence(&self) -> DispatchEvidence {
        use novarocks_spi::connector::{ConnectorMutationFailure, ConnectorMutationFailureKind};
        let observation = match &self.stage {
            DispatchStage::NotWritten(_) => ProtocolObservation {
                adapter: "rest",
                stage: "not-written",
                ..Default::default()
            },
            DispatchStage::WrittenNoStatus(_) => ProtocolObservation {
                adapter: "rest",
                stage: "written-no-status",
                ..Default::default()
            },
            DispatchStage::Status { code, header_names } => ProtocolObservation {
                adapter: "rest",
                stage: "status",
                status_code: Some(*code),
                header_names: header_names
                    .iter()
                    .map(|name| name.as_str().to_owned())
                    .collect(),
            },
        };
        match &self.evidence {
            RestEvidence::NotDispatched { kind, .. } => DispatchEvidence::NotDispatched {
                failure: ConnectorMutationFailure::new(*kind, self.evidence.message()),
            },
            RestEvidence::Rejected { kind, .. } => DispatchEvidence::Rejected {
                failure: ConnectorMutationFailure::new(*kind, self.evidence.message()),
                observation,
            },
            RestEvidence::Unknown { diagnosis } => DispatchEvidence::Unknown {
                failure: ConnectorMutationFailure::new(
                    if matches!(
                        diagnosis.reason,
                        super::super::evidence::UnknownReason::NoStatus(
                            super::super::evidence::NoStatusCause::DeadlineExceeded
                        ) | super::super::evidence::UnknownReason::ProtocolAnomaly(
                            super::super::evidence::AnomalyCause::Read(
                                super::super::evidence::BodyReadFailure::DeadlineExceeded
                            )
                        )
                    ) {
                        ConnectorMutationFailureKind::DeadlineExceeded
                    } else {
                        ConnectorMutationFailureKind::Unavailable
                    },
                    self.evidence.message(),
                ),
                observation,
            },
            RestEvidence::Applied { receipt } => {
                let mut proof =
                    crate::catalog::transaction::CommitProof::applied(receipt.snapshot_id);
                if let Some(uuid) = receipt.uuid {
                    proof = proof.with_table_uuid(uuid.to_string());
                }
                proof.metadata_location = receipt.metadata_location.as_deref().map(Arc::from);
                proof.metadata_predecessor = receipt.metadata_predecessor.clone();
                DispatchEvidence::Applied { proof }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::publication::CatalogEvidencePolicy;
    use crate::commit::publication_descriptor::AttributionPlan;
    use crate::rest_protocol::test_server::{Reply, Script, TestServer};
    use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
    use std::time::{Duration, Instant};

    async fn endpoints(server: &TestServer) -> Arc<RestEndpoints> {
        crate::catalog_runtime::build_vended_rest_generation_for_test(
            std::collections::HashMap::from([("uri".into(), server.url())]),
            None,
            crate::iceberg::Runtime::try_current().unwrap(),
        )
        .await
        .unwrap()
    }
    fn context(stop: &ConnectorStopOwner) -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(5),
            stop.view(),
            64,
            128,
        )
        .unwrap()
    }
    fn descriptor() -> PublicationDescriptor {
        PublicationDescriptor::catalog(
            [1; 16],
            vec!["db.t".into()],
            b"\n{\"requirements\":[],\"updates\":[]}\n".to_vec(),
            AttributionPlan::ObserveOnly,
        )
        .unwrap()
    }
    fn expectation() -> AttributionExpectation {
        AttributionExpectation::table_commit(Uuid::nil(), None).unwrap()
    }
    fn route() -> MutationRoute {
        MutationRoute::CommitTable(TableIdent::new(
            NamespaceIdent::new("db".into()),
            "t".into(),
        ))
    }

    #[tokio::test]
    async fn exact_wire_body_and_supported_key_survive_statement_stop_until_evidence() {
        let reply = serde_json::json!({"metadata-location":"s3://warehouse/t/metadata/v2.json", "metadata":{"table-uuid":Uuid::nil()}});
        let server = TestServer::start(Script::new()
            .on("GET", "/v1/config", Reply::json(200, &serde_json::json!({"defaults":{},"overrides":{"header.Idempotency-Key":"server-forgery"},"idempotency-key-lifetime":"PT0.000000001S"})))
            .on("POST", "/v1/namespaces/db/tables/t", Reply::json(200, &reply).with_delay(Duration::from_millis(50))));
        let endpoints = endpoints(&server).await;
        let stop = ConnectorStopOwner::new();
        let preparation = context(&stop);
        let descriptor = descriptor();
        let generation = ProviderBindingEpoch::new();
        endpoints.publication_generation.bind(generation).unwrap();
        let prepared = PreparedRestMutation::prepare(
            endpoints,
            route(),
            &descriptor,
            generation,
            &preparation,
            expectation(),
            false,
            None,
        )
        .await
        .unwrap();
        assert_eq!(prepared.binding().generation, generation.to_bytes());
        assert_eq!(prepared.binding().lifetime, Some(Duration::from_nanos(1)));
        prepared
            .binding()
            .validate_generation(generation.to_bytes())
            .unwrap();
        let policy = CatalogEvidencePolicy {
            evidence: Duration::from_secs(1),
            ..Default::default()
        };
        let granted = policy.begin_evidence(&preparation).unwrap();
        let task = tokio::spawn(prepared.dispatch(granted));
        tokio::time::timeout(Duration::from_secs(1), async {
            while server.count("POST", "/v1/namespaces/db/tables/t") == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        stop.request_stop();
        let outcome = task.await.unwrap().unwrap();
        assert!(matches!(
            outcome.dispatch_evidence(),
            DispatchEvidence::Applied { .. }
        ));
        let requests = server.requests_to("POST", "/v1/namespaces/db/tables/t");
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body, descriptor.request);
        assert_eq!(
            requests[0].header("idempotency-key"),
            Some(descriptor.key.to_string().as_str())
        );
        assert_eq!(server.count("GET", "/v1/config"), 1);
    }

    #[tokio::test]
    async fn missing_or_invalid_capability_sends_once_without_key_and_stall_exits_unknown() {
        for lifetime in [None, Some(serde_json::json!("invalid"))] {
            let mut config = serde_json::json!({"defaults":{},"overrides":{}});
            if let Some(value) = lifetime {
                config["idempotency-key-lifetime"] = value;
            }
            let server = TestServer::start(
                Script::new()
                    .on("GET", "/v1/config", Reply::json(200, &config))
                    .on("POST", "/v1/namespaces/db/tables/t", Reply::NeverAnswer),
            );
            let stop = ConnectorStopOwner::new();
            let preparation = context(&stop);
            let endpoints = endpoints(&server).await;
            let generation = ProviderBindingEpoch::new();
            endpoints.publication_generation.bind(generation).unwrap();
            let prepared = PreparedRestMutation::prepare(
                endpoints,
                route(),
                &descriptor(),
                generation,
                &preparation,
                expectation(),
                false,
                None,
            )
            .await
            .unwrap();
            assert!(prepared.binding().lifetime.is_none());
            let policy = CatalogEvidencePolicy {
                evidence: Duration::from_millis(30),
                ..Default::default()
            };
            let outcome = prepared
                .dispatch(policy.begin_evidence(&preparation).unwrap())
                .await
                .unwrap();
            assert!(matches!(
                outcome.dispatch_evidence(),
                DispatchEvidence::Unknown { .. }
            ));
            let requests = server.requests_to("POST", "/v1/namespaces/db/tables/t");
            assert_eq!(requests.len(), 1);
            assert!(requests[0].header("idempotency-key").is_none());
        }
    }
    #[tokio::test]
    async fn a_callers_epoch_cannot_relabel_an_existing_rest_session() {
        let server = TestServer::start(Script::new().on(
            "GET",
            "/v1/config",
            Reply::json(200, &serde_json::json!({"defaults":{},"overrides":{}})),
        ));
        let endpoints = endpoints(&server).await;
        let original = ProviderBindingEpoch::new();
        endpoints.publication_generation.bind(original).unwrap();
        let before = server.requests().len();
        let stop = ConnectorStopOwner::new();
        let error = PreparedRestMutation::prepare(
            endpoints.clone(),
            route(),
            &descriptor(),
            ProviderBindingEpoch::new(),
            &context(&stop),
            expectation(),
            false,
            None,
        )
        .await
        .unwrap_err();
        assert_eq!(
            error.0.kind(),
            novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest
        );
        assert!(
            endpoints
                .publication_generation
                .bind(ProviderBindingEpoch::new())
                .is_err()
        );
        assert_eq!(
            server.requests().len(),
            before,
            "foreign generation must fail before session/auth or mutation I/O"
        );
    }
    #[tokio::test]
    async fn catalog_owner_checks_noop_identity_and_exact_bytes_before_any_http() {
        use crate::catalog::admission::CatalogOperation;
        use crate::commit::model::{
            AttemptArtifacts, AttemptToken, BaseIdentity, FrozenRequest, FrozenRequestParts,
            OperationToken, RequestShape,
        };
        let server = TestServer::start(Script::new().on(
            "GET",
            "/v1/config",
            Reply::json(200, &serde_json::json!({"defaults":{},"overrides":{}})),
        ));
        let endpoints = endpoints(&server).await;
        let owner = crate::catalog::factory::NovaRocksCatalogFactory::adopt_vended_rest_for_test(
            endpoints,
            "s3://warehouse",
        );
        let generation = ProviderBindingEpoch::new();
        owner.bind_publication_generation(generation).unwrap();
        let operation = OperationToken::from_mutation(
            novarocks_spi::connector::ConnectorMutationOperationId::new(),
        );
        let target = TableIdent::new(NamespaceIdent::new("db".into()), "t".into());
        let request = || {
            FrozenRequest::new(FrozenRequestParts {
                shape: RequestShape::MetadataOnly,
                target: target.clone(),
                target_ref: "main".into(),
                base: BaseIdentity::Existing {
                    uuid: Uuid::nil(),
                    parent: None,
                    metadata_location: "s3://warehouse/t/metadata/v1.json".into(),
                },
                requirements: vec![crate::iceberg::TableRequirement::UuidMatch {
                    uuid: Uuid::nil(),
                }],
                updates: vec![],
                artifacts: AttemptArtifacts::empty(AttemptToken::new(operation, 0)),
            })
            .unwrap()
        };
        let stop = ConnectorStopOwner::new();
        let before = server.requests().len();
        let descriptor = PublicationDescriptor::frozen(&request()).unwrap();
        let prepared = owner
            .prepare_frozen_publication(
                request(),
                CatalogOperation::AlterProperties,
                None,
                descriptor.clone(),
                generation,
                context(&stop),
            )
            .await
            .unwrap();
        assert!(matches!(
            prepared,
            crate::catalog::publication_dispatch::PreparedPublication::NoOp
        ));
        let error = owner
            .prepare_frozen_publication(
                request(),
                CatalogOperation::AlterProperties,
                None,
                descriptor.clone(),
                ProviderBindingEpoch::new(),
                context(&stop),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.0.kind(),
            novarocks_spi::connector::ConnectorMutationFailureKind::InvalidRequest
        );
        let wrong_body = PublicationDescriptor::catalog(
            operation.to_bytes(),
            vec![target.to_string()],
            br#"{"requirements":[],"updates":[]}"#.to_vec(),
            AttributionPlan::ObserveOnly,
        )
        .unwrap();
        assert!(
            owner
                .prepare_frozen_publication(
                    request(),
                    CatalogOperation::AlterProperties,
                    None,
                    wrong_body,
                    generation,
                    context(&stop)
                )
                .await
                .is_err()
        );
        let fixture = crate::commit::overwrite::preparer_tests::Fixture::new();
        let metadata = fixture.metadata(crate::iceberg::spec::FormatVersion::V3);
        let create = FrozenRequest::new(FrozenRequestParts {
            shape: RequestShape::Create,
            target,
            target_ref: "main".into(),
            base: BaseIdentity::Create {
                staged: crate::commit::model::StagedCreateIdentity::new(
                    operation,
                    Arc::new(metadata),
                ),
            },
            requirements: vec![crate::iceberg::TableRequirement::NotExist],
            updates: vec![],
            artifacts: AttemptArtifacts::empty(AttemptToken::new(operation, 1)),
        })
        .unwrap();
        let descriptor = PublicationDescriptor::frozen(&create).unwrap();
        assert!(
            owner
                .prepare_frozen_publication(
                    create,
                    CatalogOperation::CreateTable(
                        crate::catalog::CatalogCreateIntent::CreateTableAsSelect
                    ),
                    None,
                    descriptor,
                    generation,
                    context(&stop)
                )
                .await
                .is_err(),
            "an empty create must not become a local NoOp"
        );
        assert_eq!(server.requests().len(), before);
    }
}

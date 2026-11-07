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

//! The former SDK adapter's independent protocol properties on the actual
//! prepared owner and sole journal. Each case owns its loopback server and
//! awaits actual publication exit before the server joins its actors.
use super::test_server::{Reply, Script, TestServer};
use crate::catalog::admission::CatalogOperation;
use crate::catalog::catalog_request::CatalogPublicationRequest;
use crate::catalog::error::CatalogOutcome;
use crate::catalog::transaction::CommitProof;
use crate::catalog::{CatalogNamespaceName, CatalogTableName, NovaRocksCatalog};
use crate::commit::model::{
    AttemptArtifacts, AttemptToken, BaseIdentity, FrozenRequest, FrozenRequestParts,
    OperationToken, RequestShape,
};
use crate::iceberg::{NamespaceIdent, TableIdent, TableRequirement, TableUpdate};
use novarocks_spi::connector::{
    ConnectorInstanceDescriptor, ConnectorInstanceId, ConnectorMutationFailureKind as Kind,
    ConnectorMutationOperationId, ConnectorProviderId, ConnectorRequestContext, ConnectorStopOwner,
    ExternalMutationEffect, ExternalMutationFinalization, ProviderBindingEpoch,
};
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

const TABLE: Uuid = Uuid::from_u128(7);
struct Fixture {
    owner: Arc<dyn NovaRocksCatalog>,
    context: ConnectorRequestContext,
    binding: crate::access_binding::IcebergReadBinding,
}
impl Fixture {
    async fn new(server: &TestServer) -> Self {
        let endpoints = crate::catalog_runtime::build_vended_rest_generation_for_test(
            HashMap::from([("uri".into(), server.url())]),
            None,
            crate::iceberg::Runtime::try_current().unwrap(),
        )
        .await
        .unwrap();
        let owner = crate::catalog::factory::NovaRocksCatalogFactory::adopt_vended_rest_for_test(
            endpoints,
            "s3://warehouse",
        );
        let context = ConnectorRequestContext::try_new(
            std::time::Instant::now() + std::time::Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
            64 * 1024,
            1024 * 1024,
        )
        .unwrap();
        let runtime = tokio::runtime::Handle::current();
        let binding = crate::access_binding::IcebergReadBinding::new(
            None,
            novarocks_fs::FsAccessResolver::new(),
            Arc::new(novarocks_fs::TokioFileIoRuntime::new(runtime.clone())),
            novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
            Arc::new(novarocks_fs::TokioFileIoSpawner::new(runtime)),
        )
        .for_request(context.clone());
        Self {
            owner,
            context,
            binding,
        }
    }
    async fn commit(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        crate::catalog::admission_test_support::publish_frozen(
            Arc::clone(&self.owner),
            ConnectorInstanceDescriptor {
                provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
                instance_id: ConnectorInstanceId::parse("rest-publication-test").unwrap(),
            },
            ProviderBindingEpoch::from_bytes([66; 16]),
            request,
            CatalogOperation::AlterProperties,
            self.context.clone(),
            None,
        )
        .await
    }
    async fn catalog(&self, request: CatalogPublicationRequest) -> CatalogOutcome<CommitProof> {
        let operation_id = ConnectorMutationOperationId::new();
        let request = request.freeze(operation_id.to_bytes()).unwrap();
        crate::catalog::admission_test_support::publish_catalog(
            Arc::clone(&self.owner),
            request,
            operation_id,
            ProviderBindingEpoch::from_bytes([66; 16]),
            self.context.clone(),
            self.binding.clone(),
        )
        .await
    }
}
fn parts(requirements: Vec<TableRequirement>) -> FrozenRequestParts {
    let token = OperationToken::from_mutation(ConnectorMutationOperationId::new());
    FrozenRequestParts {
        shape: RequestShape::MetadataOnly,
        target: TableIdent::new(NamespaceIdent::new("db".into()), "t".into()),
        target_ref: "main".into(),
        base: BaseIdentity::Existing {
            uuid: TABLE,
            parent: None,
            metadata_location: "s3://warehouse/db/t/metadata/1.json".into(),
        },
        requirements,
        updates: vec![TableUpdate::SetProperties {
            updates: HashMap::from([("fixture".into(), "true".into())]),
        }],
        artifacts: AttemptArtifacts::empty(AttemptToken::new(token, 0)),
    }
}
fn frozen() -> FrozenRequest {
    FrozenRequest::new(parts(vec![TableRequirement::UuidMatch { uuid: TABLE }])).unwrap()
}
fn scripted(method: &str, path: &str, reply: Reply) -> TestServer {
    TestServer::start(
        Script::new()
            .on("GET", "/v1/config", Reply::config(&[], &[], None))
            .on("GET", "/v1/namespaces/db", Reply::status(404))
            .on(method, path, reply),
    )
}

#[tokio::test]
async fn each_endpoint_keeps_typed_rejection_and_unknown_evidence() {
    for family in ["commit", "namespace", "drop"] {
        let (method, path) = match family {
            "namespace" => ("POST", "/v1/namespaces"),
            "drop" => ("DELETE", "/v1/namespaces/db/tables/t"),
            _ => ("POST", "/v1/namespaces/db/tables/t"),
        };
        for status in [409, 401, 403, 404, 400, 500, 419] {
            let server = scripted(method, path, Reply::status(status));
            let fixture = Fixture::new(&server).await;
            let outcome = match family {
                "namespace" => {
                    fixture
                        .catalog(CatalogPublicationRequest::CreateNamespace {
                            namespace: CatalogNamespaceName::new("db"),
                            properties: HashMap::new(),
                            no_op_if_exists: false,
                        })
                        .await
                }
                "drop" => {
                    fixture
                        .catalog(CatalogPublicationRequest::DropTable {
                            table: CatalogTableName::new("db", "t"),
                            no_op_if_missing: false,
                            purge: false,
                            capture: None,
                        })
                        .await
                }
                _ => fixture.commit(frozen()).await,
            };
            let expected = match status {
                409 if family == "namespace" => Some(Kind::AlreadyExists),
                409 => Some(Kind::Conflict),
                401 | 419 => Some(Kind::Unauthenticated),
                403 => Some(Kind::PermissionDenied),
                _ => None,
            };
            match (expected, outcome) {
                (Some(expected), CatalogOutcome::KnownUncommitted { failure }) => {
                    assert_eq!(failure.kind(), expected, "{family} {status}")
                }
                (None, CatalogOutcome::CommitUnknown { .. }) => {}
                (expected, outcome) => {
                    panic!("{family} HTTP {status}: expected {expected:?}, got {outcome:?}")
                }
            }
            assert_eq!(server.count(method, path), 1, "{family} {status}");
            assert_eq!(server.count("GET", "/v1/config"), 1);
        }
    }
}
#[tokio::test]
async fn a_404_remains_unknown_after_the_real_prepared_dispatch() {
    let server = scripted("POST", "/v1/namespaces/db/tables/t", Reply::status(404));
    let fixture = Fixture::new(&server).await;
    assert!(matches!(
        fixture.commit(frozen()).await,
        CatalogOutcome::CommitUnknown { .. }
    ));
    assert_eq!(server.count("POST", "/v1/namespaces/db/tables/t"), 1);
}
#[tokio::test]
async fn missing_frozen_uuid_refuses_before_session_or_mutation_io() {
    let server = scripted("POST", "/v1/namespaces/db/tables/t", Reply::status(200));
    let _fixture = Fixture::new(&server).await;
    let error = FrozenRequest::new(parts(vec![])).unwrap_err();
    assert_eq!(error.kind(), crate::iceberg::ErrorKind::DataInvalid);
    assert!(server.requests().is_empty());
}
#[tokio::test]
async fn a_response_for_another_incarnation_is_a_protocol_anomaly() {
    let response = serde_json::json!({"metadata-location":"s3://warehouse/db/t/metadata/2.json","metadata":{"table-uuid":Uuid::from_u128(8)}});
    let server = scripted(
        "POST",
        "/v1/namespaces/db/tables/t",
        Reply::json(200, &response),
    );
    assert!(matches!(
        Fixture::new(&server).await.commit(frozen()).await,
        CatalogOutcome::CommitUnknown { .. }
    ));
    assert_eq!(server.count("POST", "/v1/namespaces/db/tables/t"), 1);
}
#[tokio::test]
async fn a_vended_success_needs_no_sdk_table_object_to_remain_applied() {
    let response = serde_json::json!({"metadata-location":"s3://warehouse/db/t/metadata/2.json","metadata":{"table-uuid":TABLE}});
    let server = scripted(
        "POST",
        "/v1/namespaces/db/tables/t",
        Reply::json(200, &response),
    );
    let outcome = Fixture::new(&server).await.commit(frozen()).await;
    let CatalogOutcome::KnownCommitted {
        effect: ExternalMutationEffect::Applied,
        receipt,
        finalization: ExternalMutationFinalization::Complete,
    } = outcome
    else {
        panic!("exact positive evidence requires no SDK materialization: {outcome:?}")
    };
    assert_eq!(
        receipt.table_uuid.as_deref(),
        Some(TABLE.to_string().as_str())
    );
    assert_eq!(
        receipt.metadata_location.as_deref(),
        Some("s3://warehouse/db/t/metadata/2.json")
    );
    assert_eq!(server.count("POST", "/v1/namespaces/db/tables/t"), 1);
}

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

use super::*;
use crate::rest_protocol::test_server::{Reply, Script, TestServer};
use novarocks_failure_contract::FailureProjection;
use novarocks_failure_contract::connector_reason::ICEBERG_FILESYSTEM_ERROR;
use novarocks_spi::connector::{
    ConnectorCatalogMutation, ConnectorCatalogMutationOperation, ConnectorCatalogMutationRequest,
    DdlPropertyValue, ExternalMutationOutcome,
};
use serde_json::json;

fn catalog_server(
    namespace: Option<&str>,
    assigned: String,
    plant_file_in_stage_location: bool,
) -> TestServer {
    let namespace = namespace.map_or_else(|| json!({}), |location| json!({"location": location}));
    TestServer::start(
        Script::new()
            .on("GET", "/v1/config", Reply::config(&[], &[], None))
            .on("GET", "/v1/namespaces/db/tables/t", Reply::status(404))
            .on(
                "GET",
                "/v1/namespaces/db",
                Reply::json(200, &json!({"namespace": ["db"], "properties": namespace})),
            )
            .on_fn("POST", "/v1/namespaces/db/tables", move |request, _| {
                let request: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
                let location = request["location"].as_str().unwrap_or(&assigned);
                if request["stage-create"] != true {
                    return Reply::status(409);
                }
                if plant_file_in_stage_location {
                    let local = std::path::Path::new(location.strip_prefix("file://").unwrap());
                    std::fs::create_dir_all(local).unwrap();
                    std::fs::write(local.join("other-writer.parquet"), b"prior content").unwrap();
                }
                let schema = Schema::builder()
                    .with_fields(vec![Arc::new(crate::iceberg::spec::NestedField::required(
                        1,
                        "id",
                        crate::iceberg::spec::Type::Primitive(
                            crate::iceberg::spec::PrimitiveType::Long,
                        ),
                    ))])
                    .build()
                    .unwrap();
                let metadata = crate::iceberg::spec::TableMetadataBuilder::new(
                    schema,
                    crate::iceberg::spec::PartitionSpec::unpartition_spec(),
                    crate::iceberg::spec::SortOrder::unsorted_order(),
                    location.to_owned(),
                    FormatVersion::V2,
                    HashMap::new(),
                )
                .unwrap()
                .build()
                .unwrap()
                .metadata;
                Reply::json(
                    200,
                    &json!({"metadata-location": null, "metadata": metadata}),
                )
            }),
    )
}

fn request_with_properties(
    adapter: &IcebergStagedCreateAdapter,
    location: Option<&str>,
    version: i32,
) -> ConnectorStagedCreatePrepareRequest {
    let original = stage_http_request(adapter, context());
    let lease = ordinary_ctas_lease(adapter);
    let mut values = ctas_defaults(&lease);
    values
        .iter_mut()
        .find(|(key, _)| key.as_ref() == "format_version")
        .unwrap()
        .1 = DdlPropertyValue::Integer(version);
    if let Some(location) = location {
        values.push((
            "location".into(),
            DdlPropertyValue::Varchar(location.into()),
        ));
    }
    let storage = lease
        .prepare_ctas_properties(values.into(), None, &original.context)
        .unwrap();
    lease
        .prepare_typed_request(
            original.publication_id,
            original.operation_id,
            original.table,
            original.columns,
            storage,
            original.policy,
            original.context,
        )
        .unwrap()
}

fn recorded_location(
    request: &crate::rest_protocol::test_server::RecordedRequest,
) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(&request.body).unwrap()["location"]
        .as_str()
        .map(str::to_owned)
}

fn assert_computed_location(location: &str, namespace: &str) {
    let suffix = location.strip_prefix(&format!("{namespace}/t-")).unwrap();
    assert_eq!(suffix.len(), 32);
    assert!(
        suffix
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
}

// Observes the original stage's location binding before any writer is opened.
#[test]
fn staged_create_uses_its_explicit_or_unique_location_and_leaves_catalog_default_unchecked() {
    let root = tempfile::tempdir().unwrap();
    let explicit = format!("file://{}/explicit", root.path().display());
    let assigned = format!("file://{}/assigned", root.path().display());
    std::fs::create_dir(root.path().join("assigned")).unwrap();
    std::fs::write(
        root.path().join("assigned/existing.parquet"),
        b"owned by another writer",
    )
    .unwrap();
    let namespace = format!("file://{}/namespace", root.path().display());

    for (namespace, explicit) in [
        (Some(namespace.as_str()), Some(explicit.as_str())),
        (Some(namespace.as_str()), None),
        (None, None),
    ] {
        let server = catalog_server(namespace, assigned.clone(), false);
        let (executor, adapter) = stage_http_adapter(&server);
        let request = request_with_properties(&adapter, explicit, 2);
        let request_context = request.context.clone();
        let outcome = executor.block_on(adapter.prepare_async(request)).unwrap();
        let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } = outcome else {
            panic!("rest stage with an admitted table location: {outcome:?}");
        };
        adapter
            .plan_write(ConnectorStagedWritePlanningRequest {
                handle,
                context: request_context,
            })
            .unwrap();
        let posts = server.requests_to("POST", "/v1/namespaces/db/tables");
        assert_eq!(posts.len(), 1);
        let recorded = recorded_location(&posts[0]);
        match (explicit, namespace) {
            (Some(explicit), _) => assert_eq!(recorded.as_deref(), Some(explicit)),
            (None, Some(namespace)) => {
                assert_computed_location(recorded.as_deref().unwrap(), namespace)
            }
            (None, None) => assert!(recorded.is_none()),
        }
    }
}

// Observes a stage that succeeded and owns a handle, but cannot lend a writer.
#[test]
fn staged_create_rejects_a_nonempty_location_before_lending_a_writer() {
    let root = tempfile::tempdir().unwrap();
    let existing = root.path().join("existing.parquet");
    std::fs::write(&existing, b"prior content").unwrap();
    let location = format!("file://{}", root.path().display());
    let server = catalog_server(None, location.clone(), false);
    let (executor, adapter) = stage_http_adapter(&server);
    let request = request_with_properties(&adapter, Some(&location), 2);
    let context = request.context.clone();
    let operation_id = request.operation_id;
    let ConnectorStagedCreatePrepareOutcome::Prepared {
        handle, receipt, ..
    } = executor.block_on(adapter.prepare_async(request)).unwrap()
    else {
        panic!("staged response was accepted before listing");
    };
    assert_eq!(receipt.effect(), ExternalMutationEffect::Applied);
    let error = adapter
        .plan_write(ConnectorStagedWritePlanningRequest {
            handle: handle.clone(),
            context: context.clone(),
        })
        .err()
        .unwrap();
    assert_eq!(error.failure_fact().reason(), ICEBERG_FILESYSTEM_ERROR);
    assert!(
        error
            .message()
            .starts_with("Cannot create a table on a non-empty location: ")
    );
    let outcome = executor
        .block_on(adapter.abort_async(ConnectorStagedCreateAbortRequest {
            operation_id,
            handle,
            write: None,
            context,
        }))
        .unwrap();
    assert!(matches!(
        outcome,
        ConnectorStagedCreateAbortOutcome::Aborted {
            finalization: ExternalMutationFinalization::Complete,
            ..
        }
    ));
    assert_eq!(std::fs::read(&existing).unwrap(), b"prior content");
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}

// Observes a calculated location that becomes non-empty before the stage response.
#[test]
fn staged_create_checks_its_calculated_location_after_the_stage_response() {
    let root = tempfile::tempdir().unwrap();
    let namespace = format!("file://{}", root.path().display());
    let server = catalog_server(Some(&namespace), namespace.clone(), true);
    let (executor, adapter) = stage_http_adapter(&server);
    let request = request_with_properties(&adapter, None, 2);
    let context = request.context.clone();
    let ConnectorStagedCreatePrepareOutcome::Prepared { handle, .. } =
        executor.block_on(adapter.prepare_async(request)).unwrap()
    else {
        panic!("stage succeeds before consulting storage");
    };
    let refusal = adapter
        .plan_write(ConnectorStagedWritePlanningRequest { handle, context })
        .err()
        .unwrap();
    assert_eq!(refusal.failure_fact().reason(), ICEBERG_FILESYSTEM_ERROR);
    let posts = server.requests_to("POST", "/v1/namespaces/db/tables");
    let location = recorded_location(&posts[0]).unwrap();
    assert_computed_location(&location, &namespace);
    let sentinel = std::path::Path::new(location.strip_prefix("file://").unwrap())
        .join("other-writer.parquet");
    assert_eq!(std::fs::read(sentinel).unwrap(), b"prior content");
}

// Observes the ordinary request's frozen REST location and the storage-first refusal.
#[test]
fn ordinary_create_uses_the_same_location_author_in_its_catalog_publication() {
    let root = tempfile::tempdir().unwrap();
    let namespace = format!("file://{}/namespace", root.path().display());
    let explicit = format!("file://{}/explicit", root.path().display());
    let nonempty = format!("file://{}/nonempty", root.path().display());
    std::fs::create_dir(root.path().join("nonempty")).unwrap();
    let sentinel = root.path().join("nonempty/existing.parquet");
    std::fs::write(&sentinel, b"prior content").unwrap();

    for (namespace, explicit, refuses) in [
        (Some(namespace.as_str()), Some(explicit.as_str()), false),
        (Some(namespace.as_str()), None, false),
        (None, None, false),
        (None, Some(nonempty.as_str()), true),
    ] {
        let server = catalog_server(namespace, nonempty.clone(), false);
        let (_executor, adapter) = stage_http_adapter(&server);
        let original = request_with_properties(&adapter, explicit, 2);
        let properties = original
            .ordinary_typed_properties()
            .unwrap()
            .unwrap()
            .properties()
            .to_vec();
        let request = ConnectorCatalogMutationRequest {
            operation_id: original.operation_id,
            target: original.owner,
            operation: ConnectorCatalogMutationOperation::CreateTable {
                table: original.table,
                columns: original.columns,
                comment: None,
                properties,
                policy: CreatePolicy::FailIfExists,
            },
            context: original.context,
        };
        let outcome =
            ConnectorCatalogMutation::execute(adapter.provider.as_ref(), request).unwrap();
        let ExternalMutationOutcome::KnownUncommitted { failure, .. } = outcome else {
            panic!("the spy only responds with a commit rejection");
        };
        let posts = server.requests_to("POST", "/v1/namespaces/db/tables");
        if refuses {
            assert_eq!(failure.failure_fact().reason(), ICEBERG_FILESYSTEM_ERROR);
            assert!(posts.is_empty());
            assert_eq!(std::fs::read(&sentinel).unwrap(), b"prior content");
        } else {
            assert_eq!(failure.kind(), ConnectorMutationFailureKind::AlreadyExists);
            assert_eq!(posts.len(), 1);
            let recorded = recorded_location(&posts[0]);
            match (explicit, namespace) {
                (Some(explicit), _) => assert_eq!(recorded.as_deref(), Some(explicit)),
                (None, Some(namespace)) => {
                    assert_computed_location(recorded.as_deref().unwrap(), namespace)
                }
                (None, None) => assert!(recorded.is_none()),
            }
        }
    }
}

#[test]
fn ctas_accepts_all_create_table_format_versions() {
    let server = TestServer::start(Script::new());
    let (_executor, adapter) = stage_http_adapter(&server);
    for (number, format) in [
        (1, FormatVersion::V1),
        (2, FormatVersion::V2),
        (3, FormatVersion::V3),
    ] {
        let request = request_with_properties(&adapter, None, number);
        let prepared = prepare_typed_staged_table(&request, "").unwrap();
        assert_eq!(prepared.format_version, format);
    }
}

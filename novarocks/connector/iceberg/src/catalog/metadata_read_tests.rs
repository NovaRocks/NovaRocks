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

//! Complete metadata reads through actual admitted catalog owners.

use super::{CatalogMetadataLoad, CatalogTableName, NovaRocksCatalog};
use crate::access_binding::IcebergReadBinding;
use crate::catalog_runtime::RestAccessDelegationMode;
use crate::rest_protocol::test_server::{BodyPlan, Reply, Script, TestServer};
use novarocks_spi::connector::{ConnectorErrorKind, ConnectorRequestContext, ConnectorStopOwner};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn request_context(budget: Duration, stop: &ConnectorStopOwner) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(Instant::now() + budget, stop.view(), 65536, 262144).unwrap()
}
fn binding(context: ConnectorRequestContext) -> IcebergReadBinding {
    let handle = tokio::runtime::Handle::current();
    IcebergReadBinding::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
        Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle)),
    )
    .for_request(context)
}
fn metadata() -> crate::iceberg::spec::TableMetadata {
    use crate::iceberg::spec::{
        FormatVersion, NestedField, PrimitiveType, Schema, SortOrder, TableMetadataBuilder, Type,
        UnboundPartitionSpec,
    };
    TableMetadataBuilder::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .build()
            .unwrap(),
        UnboundPartitionSpec::default(),
        SortOrder::unsorted_order(),
        "s3://warehouse/db/t".into(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}
fn response() -> serde_json::Value {
    let mut complete = serde_json::to_value(metadata()).unwrap();
    complete["last-sequence-number"] = serde_json::json!(2);
    complete["last-updated-ms"] = serde_json::json!(2);
    complete["current-snapshot-id"] = serde_json::json!(22);
    complete["snapshots"] = serde_json::json!([
        {"snapshot-id":11,"sequence-number":1,"timestamp-ms":1,
            "manifest-list":"s3://warehouse/db/t/list-11.avro",
            "summary":{"operation":"append"},"schema-id":0},
        {"snapshot-id":22,"parent-snapshot-id":11,"sequence-number":2,"timestamp-ms":2,
            "manifest-list":"s3://warehouse/db/t/list-22.avro",
            "summary":{"operation":"append"},"schema-id":0}
    ]);
    complete["snapshot-log"] = serde_json::json!([
        {"timestamp-ms":1,"snapshot-id":11},{"timestamp-ms":2,"snapshot-id":22}
    ]);
    complete["refs"] = serde_json::json!({"main":{"snapshot-id":22,"type":"branch"}});
    serde_json::json!({"metadata-location":"s3://warehouse/db/t/metadata/v1.json",
        "metadata":complete,
        "storage-credentials":[{"prefix":"s3://warehouse/db/t",
            "config":{"s3.access-key-id":"credential-canary"}}]})
}
async fn owner(server: &TestServer, extra: &[(&str, &str)]) -> Arc<dyn NovaRocksCatalog> {
    owner_with_identity(server, extra, None).await
}
async fn owner_with_identity(
    server: &TestServer,
    extra: &[(&str, &str)],
    identity: Option<crate::access_binding::IcebergRestAuthMaterial>,
) -> Arc<dyn NovaRocksCatalog> {
    let mut properties = HashMap::from([("uri".to_owned(), server.url())]);
    properties.extend(
        extra
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string())),
    );
    let endpoints = crate::catalog_runtime::build_vended_rest_generation_for_test(
        properties,
        identity,
        crate::iceberg::Runtime::try_current().unwrap(),
    )
    .await
    .unwrap();
    Arc::new(super::rest::NovaRocksRestCatalog::new(
        endpoints,
        None,
        RestAccessDelegationMode::Vended,
    ))
}
fn table() -> CatalogTableName {
    CatalogTableName::new("db", "t")
}

#[tokio::test]
async fn real_vended_owner_reads_all_and_returns_typed_304_without_storage_materialization() {
    let server = TestServer::start(
        Script::new()
            .on("GET", "/v1/config", Reply::config(&[], &[], None))
            .on("POST", "/v1/oauth/tokens", Reply::token("gc-role-token"))
            .on_sequence(
                "GET",
                "/v1/namespaces/db/tables/t",
                vec![
                    Reply::json(200, &response()).with_header("etag", "W/\"exact-all\""),
                    Reply::bytes(304, b"invalid-json-credential-canary".to_vec()),
                ],
            ),
    );
    let catalog = owner_with_identity(
        &server,
        &[("header.x-tenant", "tenant-a")],
        Some(crate::rest_protocol::test_server::oauth2_identity(
            "fe-gc", "secret",
        )),
    )
    .await;
    let stop = ConnectorStopOwner::new();
    let read = binding(request_context(Duration::from_secs(5), &stop));
    let CatalogMetadataLoad::Complete {
        base,
        validator: Some(validator),
    } = catalog
        .load_complete_metadata_for_read(table(), read.clone(), None)
        .await
        .unwrap()
    else {
        panic!("first complete load requires the actual ETag");
    };
    assert_eq!(
        base.metadata_location,
        "s3://warehouse/db/t/metadata/v1.json"
    );
    let mut ids = base
        .metadata
        .snapshots()
        .map(|snapshot| snapshot.snapshot_id())
        .collect::<Vec<_>>();
    ids.sort_unstable();
    assert_eq!(
        ids,
        [11, 22],
        "full metadata must retain the unreferenced historical snapshot"
    );
    assert!(
        base.metadata
            .refs()
            .values()
            .all(|reference| reference.snapshot_id != 11)
    );
    assert!(matches!(
        catalog
            .load_complete_metadata_for_read(table(), read, Some(&validator))
            .await
            .unwrap(),
        CatalogMetadataLoad::NotModified
    ));
    let loads = server.requests_to("GET", "/v1/namespaces/db/tables/t");
    assert_eq!(loads.len(), 2);
    for request in &loads {
        assert_eq!(request.target, "/v1/namespaces/db/tables/t?snapshots=all");
        assert_eq!(request.header("x-tenant"), Some("tenant-a"));
        assert_eq!(
            request.header("authorization"),
            Some("Bearer gc-role-token")
        );
        assert_eq!(request.header("x-iceberg-access-delegation"), None);
    }
    assert_eq!(loads[1].header("if-none-match"), Some("W/\"exact-all\""));
    assert_eq!(server.count("GET", "/v1/config"), 1);
    assert_eq!(server.count("POST", "/v1/oauth/tokens"), 1);
    assert_eq!(
        server.count("GET", "/v1/namespaces/db/tables/t/credentials"),
        0
    );
    assert!(!format!("{validator:?}").contains("exact-all"));
}

#[tokio::test]
async fn validators_never_cross_generation_warehouse_prefix_or_table_scope() {
    let server = TestServer::start(
        Script::new()
            .on("GET", "/v1/config", Reply::config(&[], &[], None))
            .on(
                "GET",
                "/v1/namespaces/db/tables/t",
                Reply::json(200, &response()).with_header("etag", "\"shared\""),
            ),
    );
    let stop = ConnectorStopOwner::new();
    let read = binding(request_context(Duration::from_secs(5), &stop));
    let original = owner(&server, &[("warehouse", "warehouse-a")]).await;
    let CatalogMetadataLoad::Complete {
        validator: Some(validator),
        ..
    } = original
        .load_complete_metadata_for_read(table(), read.clone(), None)
        .await
        .unwrap()
    else {
        panic!("complete validator");
    };
    let before = server.requests().len();
    for extra in [
        vec![],
        vec![("warehouse", "warehouse-b")],
        vec![("prefix", "other")],
        vec![("header.x-tenant", "tenant-b")],
    ] {
        let other = owner(&server, &extra).await;
        let error = other
            .load_complete_metadata_for_read(table(), read.clone(), Some(&validator))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    }
    let error = original
        .load_complete_metadata_for_read(
            CatalogTableName::new("db", "other"),
            read,
            Some(&validator),
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
    // Every mismatched validator is refused before config or table I/O.
    assert_eq!(server.requests().len(), before);
}

#[tokio::test]
async fn no_etag_stays_unconditional_and_unsolicited_304_is_refused() {
    let server = TestServer::start(
        Script::new()
            .on("GET", "/v1/config", Reply::config(&[], &[], None))
            .on_sequence(
                "GET",
                "/v1/namespaces/db/tables/t",
                vec![Reply::json(200, &response()), Reply::status(304)],
            ),
    );
    let catalog = owner(&server, &[]).await;
    let stop = ConnectorStopOwner::new();
    let read = binding(request_context(Duration::from_secs(5), &stop));
    assert!(matches!(
        catalog
            .load_complete_metadata_for_read(table(), read.clone(), None)
            .await
            .unwrap(),
        CatalogMetadataLoad::Complete {
            validator: None,
            ..
        }
    ));
    assert_eq!(
        catalog
            .load_complete_metadata_for_read(table(), read, None)
            .await
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::Unavailable
    );
    assert!(
        server
            .requests_to("GET", "/v1/namespaces/db/tables/t")
            .iter()
            .all(|request| request.header("if-none-match").is_none())
    );
}

#[tokio::test]
async fn original_stop_prohibits_io_and_original_deadline_bounds_streamed_body() {
    let response_bytes = response().to_string().into_bytes();
    let server = TestServer::start(
        Script::new()
            .on("GET", "/v1/config", Reply::config(&[], &[], None))
            .on(
                "GET",
                "/v1/namespaces/db/tables/t",
                Reply::status(200).with_body(BodyPlan::Chunked(vec![
                    (Duration::ZERO, response_bytes[..1].to_vec()),
                    (Duration::from_millis(400), response_bytes[1..].to_vec()),
                ])),
            ),
    );
    let catalog = owner(&server, &[]).await;
    let stopped = ConnectorStopOwner::new();
    stopped.request_stop();
    let error = catalog
        .load_complete_metadata_for_read(
            table(),
            binding(request_context(Duration::from_secs(5), &stopped)),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ConnectorErrorKind::Cancelled);
    assert!(server.requests().is_empty());
    // Establish the unchanged session before the short read interval.
    let warm_stop = ConnectorStopOwner::new();
    let warm = binding(request_context(Duration::from_secs(5), &warm_stop));
    let _ = catalog.table_exists_for_read(table(), warm).await;
    let stop = ConnectorStopOwner::new();
    let started = Instant::now();
    assert!(
        catalog
            .load_complete_metadata_for_read(
                table(),
                binding(request_context(Duration::from_millis(100), &stop)),
                None
            )
            .await
            .is_err()
    );
    assert!(
        started.elapsed() < Duration::from_millis(350),
        "read acquired a fresh deadline"
    );
}

#[tokio::test]
async fn default_hadoop_path_uses_real_request_bound_metadata_without_a_validator() {
    use crate::iceberg::{NamespaceIdent, TableCreation};
    let directory = tempfile::tempdir().unwrap();
    let location = directory.path().to_str().unwrap();
    let stop = ConnectorStopOwner::new();
    let read = binding(request_context(Duration::from_secs(5), &stop));
    let client = Arc::new(
        crate::hadoop_catalog::HadoopFileSystemCatalog::new_with_binding(
            crate::fs_io::build_file_io_for_location(location, read.clone()),
            location.to_owned(),
            read.clone(),
            crate::iceberg::Runtime::try_current().unwrap(),
        ),
    );
    let created = client
        .create_fixture_table(
            &NamespaceIdent::new("db".into()),
            TableCreation::builder()
                .name("t".to_string())
                .schema(metadata().current_schema().as_ref().clone())
                .build(),
        )
        .await
        .unwrap();
    let catalog = super::hadoop::NovaRocksHadoopCatalog::new(client);
    let CatalogMetadataLoad::Complete {
        base,
        validator: None,
    } = catalog
        .load_complete_metadata_for_read(table(), read, None)
        .await
        .unwrap()
    else {
        panic!("Hadoop returns authoritative full metadata, never a synthetic validator");
    };
    assert_eq!(base.metadata.uuid(), created.metadata().uuid());
    assert_eq!(
        Some(base.metadata_location.as_str()),
        created.metadata_location()
    );
}

#[tokio::test]
async fn missing_or_expired_original_context_is_refused_before_session_io() {
    let server = TestServer::start(Script::new());
    let catalog = owner(&server, &[]).await;
    let stop = ConnectorStopOwner::new();
    let with_context = binding(request_context(Duration::from_millis(20), &stop));
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        catalog
            .load_complete_metadata_for_read(table(), with_context, None)
            .await
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::DeadlineExceeded
    );
    let handle = tokio::runtime::Handle::current();
    let without_context = IcebergReadBinding::new(
        None,
        novarocks_fs::FsAccessResolver::new(),
        Arc::new(novarocks_fs::TokioFileIoRuntime::new(handle.clone())),
        Arc::new(novarocks_fs::TokioFileIoSpawner::new(handle)),
    );
    assert_eq!(
        catalog
            .load_complete_metadata_for_read(table(), without_context, None)
            .await
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::InvalidRequest
    );
    assert!(server.requests().is_empty());
}

#[test]
fn complete_metadata_requires_an_exact_nonempty_location() {
    for location in [None, Some("")] {
        assert_eq!(
            super::metadata_read::complete_metadata(metadata(), location, None)
                .unwrap_err()
                .kind(),
            ConnectorErrorKind::CorruptData
        );
    }
}

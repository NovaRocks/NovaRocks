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

//! Independent Java REST evidence for canonical operation requests.
//! These ignored tests require a pinned fixture publication; they are not native SQL acceptance.

use super::*;
use crate::access_binding::IcebergReadBinding;
use crate::catalog::error::{CatalogCommitEvidence, CatalogOutcome};
use crate::catalog::transaction::CommitProof;
use crate::commit::attempt::{self, Publisher, RetryPolicy};
use crate::commit::dependency::ValidationInputs;
use crate::commit::fast_append::FastAppendPreparer;
use crate::commit::model::*;
use crate::commit::operation::{IcebergCommitAttempt, IcebergCommitOperation, OperationLimits};
use crate::commit::recovery::FrozenPublicationFacts;
use crate::commit::rewrite_data_files::RewriteDataFilesPreparer;
use crate::commit::statistics::StatisticsPreparer;
use crate::iceberg::spec::*;
use crate::iceberg::{
    Error, ErrorKind, NamespaceIdent, Result, TableIdent, TableRequirement, TableUpdate,
};
use crate::iceberg_catalog_rest::{
    CommitTableRequest, CommitTableResponse, CreateTableRequest, LoadTableResult,
};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use novarocks_fs::{
    FsAccessResolver, ObjectStoreConfig, SecretValue, TokioFileIoRuntime, TokioFileTaskSpawner,
};
use novarocks_spi::connector::{
    ConnectorMutationFailureKind, ConnectorRequestContext, ConnectorStopOwner,
    ConnectorWriteOperationId, ExternalMutationEffect, MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
    MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES, StatisticsArtifactDraft,
};
use parquet::arrow::ArrowWriter;
use reqwest::{Client, StatusCode};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use url::Url;
use uuid::Uuid;

fn required(key: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| panic!("Pinned interop driver must set {key}"))
}

fn error(message: impl Into<String>) -> Error {
    Error::new(ErrorKind::Unexpected, message.into())
}

fn schema() -> Schema {
    Schema::builder()
        .with_fields(vec![Arc::new(NestedField::required(
            1,
            "id",
            Type::Primitive(PrimitiveType::Long),
        ))])
        .build()
        .unwrap()
}

fn semantic_facts(metadata: &TableMetadata) -> Value {
    let mut facts = serde_json::to_value(metadata).unwrap();
    for field in ["last-updated-ms", "metadata-log", "snapshot-log"] {
        facts.as_object_mut().unwrap().remove(field);
    }
    for field in [
        "schemas",
        "partition-specs",
        "sort-orders",
        "snapshots",
        "statistics",
    ] {
        if let Some(values) = facts[field].as_array_mut() {
            values.sort_by_key(Value::to_string);
        }
    }
    facts
}

#[derive(Clone)]
struct Java {
    client: Client,
    root: Url,
    prefix: Vec<String>,
    namespace: NamespaceIdent,
    binding: IcebergReadBinding,
    artifacts: PathBuf,
}

impl Java {
    async fn new(case: &str) -> Self {
        let client = Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let mut root = Url::parse(&required("NOVAROCKS_ICEBERG_REST_URI")).unwrap();
        assert!(root.query().is_none() && root.fragment().is_none());
        assert!(root.username().is_empty() && root.password().is_none());
        root.set_path(&format!("{}/", root.path().trim_end_matches('/')));
        let config_url = root.join("v1/config").unwrap();
        let response = client
            .get(config_url)
            .query(&[("warehouse", required("NOVAROCKS_ICEBERG_REST_WAREHOUSE"))])
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let config: Value = response.json().await.unwrap();
        let mut props = BTreeMap::new();
        for section in ["defaults", "overrides"] {
            if let Some(values) = config[section].as_object() {
                for (key, value) in values {
                    props.insert(
                        key.clone(),
                        value.as_str().expect("string REST config").to_owned(),
                    );
                }
            }
        }
        let prefix = props
            .get("prefix")
            .map(|p| {
                p.split('/')
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let namespace = NamespaceIdent::new(format!(
            "{}_{}",
            required("NR_IRU5_INTEROP_NAMESPACE"),
            case
        ));
        let artifacts = PathBuf::from(required("NR_IRU5_INTEROP_ARTIFACT_DIR")).join(case);
        std::fs::create_dir(&artifacts).unwrap();
        let runtime = tokio::runtime::Handle::current();
        let binding = IcebergReadBinding::new(
            Some(ObjectStoreConfig {
                endpoint: required("AWS_S3_ENDPOINT"),
                access_key_id: SecretValue::new(required("AWS_S3_ACCESS_KEY_ID")),
                access_key_secret: SecretValue::new(required("AWS_S3_SECRET_ACCESS_KEY")),
                session_token: std::env::var("AWS_SESSION_TOKEN")
                    .ok()
                    .map(SecretValue::new),
                enable_path_style_access: Some(true),
                region: std::env::var("AWS_REGION").ok(),
                retry_max_times: None,
                retry_min_delay_ms: None,
                retry_max_delay_ms: None,
                timeout_ms: None,
                io_timeout_ms: None,
            }),
            FsAccessResolver::new(),
            Arc::new(TokioFileIoRuntime::new(runtime.clone())),
            Arc::new(TokioFileTaskSpawner::new(runtime)),
        );
        let java = Self {
            client,
            root,
            prefix,
            namespace,
            binding,
            artifacts,
        };
        let result = java
            .client
            .post(java.endpoint(&["namespaces"]))
            .json(&json!({"namespace": java.namespace.as_ref(), "properties": {}}))
            .send()
            .await
            .unwrap();
        assert_eq!(
            result.status(),
            StatusCode::OK,
            "unique namespace must be created exactly once"
        );
        java.record("fixture", &json!({"publication": required("NOVA_ENV_REST_ENV_FILE"), "prefix": java.prefix, "namespace": java.namespace.as_ref()}));
        java
    }

    fn independent(&self) -> Self {
        let mut independent = self.clone();
        independent.client = Client::builder()
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        independent
    }

    fn endpoint(&self, parts: &[&str]) -> Url {
        let mut url = self.root.clone();
        url.path_segments_mut()
            .unwrap()
            .pop_if_empty()
            .push("v1")
            .extend(self.prefix.iter().map(String::as_str))
            .extend(parts.iter().copied());
        url
    }

    fn table_url(&self, ident: &TableIdent) -> Url {
        self.endpoint(&[
            "namespaces",
            &ident.namespace.to_url_string(),
            "tables",
            &ident.name,
        ])
    }

    fn ident(&self, name: &str) -> TableIdent {
        TableIdent::new(self.namespace.clone(), name.to_owned())
    }

    fn record(&self, name: &str, value: &Value) {
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(self.artifacts.join(format!("{name}.json")))
            .unwrap();
        file.write_all(&serde_json::to_vec_pretty(value).unwrap())
            .unwrap();
    }

    async fn create(&self, name: &str, staged: bool) -> LoadTableResult {
        let response = self
            .client
            .post(self.endpoint(&["namespaces", &self.namespace.to_url_string(), "tables"]))
            .json(&CreateTableRequest {
                name: name.to_owned(),
                location: None,
                schema: schema(),
                partition_spec: Some(PartitionSpec::unpartition_spec().into_unbound()),
                write_order: Some(SortOrder::unsorted_order()),
                stage_create: Some(staged),
                properties: HashMap::from([
                    ("format-version".into(), "3".into()),
                    ("write.row-lineage".into(), "true".into()),
                ]),
            })
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let loaded: LoadTableResult = response.json().await.unwrap();
        assert_eq!(loaded.metadata.format_version(), FormatVersion::V3);
        assert!(
            loaded.metadata.location().starts_with("s3://"),
            "use actual Java S3 location"
        );
        assert_eq!(loaded.metadata_location.is_none(), staged);
        self.record(&format!("{name}-initial"), &json!({"staged": staged, "metadata": loaded.metadata, "metadata-location": loaded.metadata_location}));
        loaded
    }

    async fn load(&self, ident: &TableIdent) -> Result<LoadTableResult> {
        let response = self
            .client
            .get(self.table_url(ident))
            .send()
            .await
            .map_err(|e| error(format!("Java load failed: {e}")))?;
        if response.status() != StatusCode::OK {
            return Err(error(format!("Java load returned {}", response.status())));
        }
        response
            .json()
            .await
            .map_err(|e| error(format!("Java load decode failed: {e}")))
    }

    async fn post(&self, label: &str, request: &FrozenRequest) -> Result<(StatusCode, Bytes)> {
        let envelope = CommitTableRequest {
            identifier: Some(request.identifier().clone()),
            requirements: request.requirements().to_vec(),
            updates: request.updates().to_vec(),
        };
        let body = serde_json::to_value(&envelope).unwrap();
        assert_eq!(body, request.to_rest_json().unwrap());
        self.record(&format!("{label}-request"), &body);
        // One POST only. Unknown transport/5xx outcomes are never replayed by this helper.
        let response = self
            .client
            .post(self.table_url(request.identifier()))
            .json(&envelope)
            .send()
            .await
            .map_err(|e| error(format!("Java commit transport outcome unknown: {e}")))?;
        let status = response.status();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| error(format!("Java commit body outcome unknown: {e}")))?;
        let observed = if status == StatusCode::OK {
            let result: CommitTableResponse = serde_json::from_slice(&bytes)
                .map_err(|e| error(format!("Java committed response decode unknown: {e}")))?;
            json!({"status": status.as_u16(), "metadata": result.metadata, "metadata-location": result.metadata_location})
        } else {
            // Retain the wire status without persisting arbitrary server error text or headers.
            json!({"status": status.as_u16(), "error-body-bytes": bytes.len()})
        };
        self.record(&format!("{label}-response"), &observed);
        Ok((status, bytes))
    }

    fn operation(&self, metadata: &TableMetadata) -> IcebergCommitOperation {
        let stop = ConnectorStopOwner::new();
        IcebergCommitOperation::new(
            OperationToken::from_write(ConnectorWriteOperationId::from_bytes(
                *Uuid::now_v7().as_bytes(),
            )),
            metadata.location(),
            self.binding.clone(),
            ConnectorRequestContext::try_new(
                Instant::now() + Duration::from_secs(120),
                stop.view(),
                MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
                MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
            )
            .unwrap(),
            crate::resources::IcebergCatalogRuntime::new(tokio::runtime::Handle::current()),
            OperationLimits::default(),
        )
        .unwrap()
    }

    async fn data(
        &self,
        operation: &IcebergCommitOperation,
        metadata: &TableMetadata,
        values: &[i64],
    ) -> DataFile {
        let field = Field::new("id", DataType::Int64, false)
            .with_metadata(HashMap::from([("PARQUET:field_id".into(), "1".into())]));
        let arrow = Arc::new(ArrowSchema::new(vec![field]));
        let batch = RecordBatch::try_new(
            arrow.clone(),
            vec![Arc::new(Int64Array::from(values.to_vec()))],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut bytes, arrow, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        // Session writers produce immutable data before commit admission; the operation adopts that exact object.
        let object = ObjectIdentity::new(format!(
            "{}/data/{}-{}.parquet",
            metadata.location().trim_end_matches('/'),
            operation.token().path_component(),
            Uuid::now_v7()
        ))
        .unwrap();
        let io = crate::fs_io::build_file_io_for_location(object.path(), self.binding.clone());
        let mut output = io
            .new_output(object.path())
            .unwrap()
            .writer()
            .await
            .unwrap();
        output.write(Bytes::copy_from_slice(&bytes)).await.unwrap();
        output.close().await.unwrap();
        operation.adopt_session_data(object.clone()).unwrap();
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(object.path().into())
            .file_format(DataFileFormat::Parquet)
            .partition(Struct::empty())
            .record_count(values.len() as u64)
            .file_size_in_bytes(bytes.len() as u64)
            .build()
            .unwrap()
    }
}

fn intent(
    java: &Java,
    name: &str,
    metadata: &TableMetadata,
    operation: &IcebergCommitOperation,
    shape: RequestShape,
    added: Vec<AddedContent>,
    removed: Vec<FrozenEntry>,
    dependency: Dependency,
) -> OperationIntent {
    OperationIntent::new(OperationIntentParts {
        target: TableTarget {
            ident: java.ident(name),
            uuid: (shape != RequestShape::Create).then_some(metadata.uuid()),
        },
        target_ref: "main".into(),
        start: metadata.current_snapshot().map(|s| StartSnapshot {
            snapshot_id: s.snapshot_id(),
            sequence_number: s.sequence_number(),
        }),
        changes: FileChanges { added, removed },
        dependencies: vec![dependency],
        isolation: IsolationLevel::Snapshot,
        shape,
        summary: BTreeMap::from([("iru5-interop".into(), operation.token().path_component())]),
        token: operation.token(),
    })
    .unwrap()
}

fn base(loaded: &LoadTableResult) -> StagingBase {
    StagingBase::Existing {
        metadata: loaded.metadata.clone(),
        metadata_location: loaded.metadata_location.clone().unwrap(),
    }
}

fn added_references(intent: &OperationIntent) -> Vec<ObjectIdentity> {
    intent
        .changes()
        .added
        .iter()
        .map(|a| ObjectIdentity::new(a.file().file_path()).unwrap())
        .collect()
}

fn bounded_preflight(request: &FrozenRequest, operation: &IcebergCommitOperation) -> Result<()> {
    let facts = FrozenPublicationFacts::from_request(request, operation)?;
    let bytes = serde_json::to_vec(&facts).map_err(|e| error(e.to_string()))?;
    if bytes.len() > MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES {
        return Err(error("Recovery ledger exceeds handle payload bound"));
    }
    let decoded: FrozenPublicationFacts =
        serde_json::from_slice(&bytes).map_err(|e| error(e.to_string()))?;
    decoded.validate(operation.token())?;
    Ok(())
}

async fn freeze_append(
    loaded: &LoadTableResult,
    intent: &OperationIntent,
    attempt: &IcebergCommitAttempt,
) -> (TableMetadata, FrozenRequest) {
    let mut engine = StagingEngine::begin(base(loaded), intent, attempt).unwrap();
    engine.stage(&FastAppendPreparer).await.unwrap();
    let expected = engine.metadata().clone();
    let request = engine.freeze(&added_references(intent)).unwrap();
    (expected, request)
}

async fn publish(
    java: &Java,
    label: &str,
    expected: &TableMetadata,
    request: &FrozenRequest,
    operation: &IcebergCommitOperation,
) -> LoadTableResult {
    bounded_preflight(request, operation).unwrap();
    let (status, _) = java.post(label, request).await.unwrap();
    assert_eq!(status, StatusCode::OK);
    let actual = java.load(request.identifier()).await.unwrap();
    assert_eq!(
        semantic_facts(&actual.metadata),
        semantic_facts(expected),
        "Java is the independent metadata oracle"
    );
    java.record(&format!("{label}-readback"), &json!({"facts": semantic_facts(&actual.metadata), "metadata-location": actual.metadata_location}));
    actual
}

async fn statistics(attempt: &IcebergCommitAttempt, snapshot: &Snapshot) -> StatisticsFile {
    let draft = StatisticsArtifactDraft::try_new(
        vec![1],
        "iru5/test-statistics-v1",
        Bytes::from_static(b"independent-interop"),
        BTreeMap::new(),
    )
    .unwrap();
    crate::stats_assembler::write_puffin_artifacts_allocated(
        attempt,
        snapshot.snapshot_id(),
        snapshot.sequence_number(),
        &[draft],
    )
    .await
    .unwrap()
    .unwrap()
}

async fn physical(
    java: &Java,
    label: &str,
    metadata: &TableMetadata,
    snapshot_id: i64,
    attempt: &IcebergCommitAttempt,
) -> Vec<(ManifestFile, ManifestEntry)> {
    let snapshot = metadata.snapshot_by_id(snapshot_id).unwrap();
    let list = snapshot
        .load_manifest_list(attempt.file_io(), metadata)
        .await
        .unwrap();
    let mut raw = Vec::new();
    for manifest in list.entries() {
        let bytes = attempt
            .file_io()
            .new_input(&manifest.manifest_path)
            .unwrap()
            .read()
            .await
            .unwrap();
        for entry in Manifest::parse_avro(&bytes).unwrap().entries() {
            raw.push((manifest.clone(), entry.as_ref().clone()));
        }
    }
    java.record(
        label,
        &json!({
            "snapshot": {
                "snapshot-id": snapshot.snapshot_id(),
                "parent-snapshot-id": snapshot.parent_snapshot_id(),
                "sequence-number": snapshot.sequence_number(),
                "timestamp-ms": snapshot.timestamp_ms(),
                "manifest-list": snapshot.manifest_list(),
                "schema-id": snapshot.schema_id(),
                "first-row-id": snapshot.first_row_id(),
                "added-rows": snapshot.added_rows_count(),
                "operation": format!("{:?}", snapshot.summary().operation),
                "summary": snapshot.summary().additional_properties,
            },
            "manifest-list": list.entries().iter().map(|manifest| json!({
                "manifest-path": manifest.manifest_path,
                "manifest-length": manifest.manifest_length,
                "partition-spec-id": manifest.partition_spec_id,
                "content": format!("{:?}", manifest.content),
                "sequence-number": manifest.sequence_number,
                "min-sequence-number": manifest.min_sequence_number,
                "added-snapshot-id": manifest.added_snapshot_id,
                "added-files-count": manifest.added_files_count,
                "existing-files-count": manifest.existing_files_count,
                "deleted-files-count": manifest.deleted_files_count,
                "added-rows-count": manifest.added_rows_count,
                "existing-rows-count": manifest.existing_rows_count,
                "deleted-rows-count": manifest.deleted_rows_count,
                "first-row-id": manifest.first_row_id,
            })).collect::<Vec<_>>(),
            "entries": raw.iter().map(|(manifest, entry)| json!({
                "manifest": manifest.manifest_path,
                "spec-id": manifest.partition_spec_id,
                "status": format!("{:?}", entry.status()),
                "path": entry.data_file().file_path(),
                "data-sequence": entry.sequence_number,
                "file-sequence": entry.file_sequence_number,
                "first-row-id": entry.data_file().first_row_id(),
            })).collect::<Vec<_>>(),
        }),
    );
    raw
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru5_java_rest_three_shapes_historic_spec_and_composition() {
    let java = Java::new("shapes").await;
    let staged = java.create("created", true).await;
    let operation = java.operation(&staged.metadata);
    let file = java.data(&operation, &staged.metadata, &[10, 11]).await;
    let create = intent(
        &java,
        "created",
        &staged.metadata,
        &operation,
        RequestShape::Create,
        vec![
            AddedContent::new_logical_data(file, staged.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let attempt = operation.begin_attempt().unwrap();
    let mut engine = StagingEngine::begin(
        StagingBase::Create {
            staged: StagedCreateIdentity::new(operation.token(), Arc::new(staged.metadata.clone())),
            initialization_updates: staged
                .metadata
                .staged_create_initialization_updates()
                .unwrap(),
        },
        &create,
        &attempt,
    )
    .unwrap();
    engine.stage(&FastAppendPreparer).await.unwrap();
    let expected = engine.metadata().clone();
    let request = engine.freeze(&added_references(&create)).unwrap();
    assert_eq!(request.requirements(), &[TableRequirement::NotExist]);
    let created = publish(&java, "create", &expected, &request, &operation).await;
    assert_eq!(
        created
            .metadata
            .current_snapshot()
            .unwrap()
            .added_rows_count(),
        Some(2)
    );

    let operation = java.operation(&created.metadata);
    let file = java.data(&operation, &created.metadata, &[12]).await;
    let append = intent(
        &java,
        "created",
        &created.metadata,
        &operation,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(file, created.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let attempt = operation.begin_attempt().unwrap();
    let mut engine = StagingEngine::begin(base(&created), &append, &attempt).unwrap();
    let historical = created.metadata.default_partition_spec_id();
    let partitioned: UnboundPartitionSpec = serde_json::from_value(json!({"spec-id":1,"fields":[{"source-id":1,"field-id":1000,"name":"id_part","transform":"identity"}]})).unwrap();
    engine
        .stage_change(PreparedChange {
            updates: vec![
                TableUpdate::AddSpec { spec: partitioned },
                TableUpdate::SetDefaultSpec { spec_id: -1 },
            ],
            requirements: vec![],
        })
        .unwrap();
    assert_ne!(engine.metadata().default_partition_spec_id(), historical);
    java.record(
        "composition-partition-prefix",
        &semantic_facts(engine.metadata()),
    );
    engine
        .stage_change(PreparedChange {
            updates: vec![
                TableUpdate::AddSpec {
                    spec: created
                        .metadata
                        .default_partition_spec()
                        .as_ref()
                        .clone()
                        .into_unbound(),
                },
                TableUpdate::SetDefaultSpec { spec_id: -1 },
            ],
            requirements: vec![],
        })
        .unwrap();
    assert_eq!(engine.metadata().default_partition_spec_id(), historical);
    assert!(engine.updates().contains(&TableUpdate::SetDefaultSpec {
        spec_id: historical
    }));
    java.record(
        "composition-historical-prefix",
        &semantic_facts(engine.metadata()),
    );
    engine.stage(&FastAppendPreparer).await.unwrap();
    java.record(
        "composition-append-prefix",
        &semantic_facts(engine.metadata()),
    );
    let stats = statistics(&attempt, engine.metadata().current_snapshot().unwrap()).await;
    engine
        .stage(&StatisticsPreparer {
            statistics: stats.clone(),
        })
        .await
        .unwrap();
    java.record(
        "composition-statistics-prefix",
        &semantic_facts(engine.metadata()),
    );
    let expected = engine.metadata().clone();
    // Attempt-owned Puffin is frozen through the attempt ledger; only session data crosses attempts.
    let request = engine.freeze(&added_references(&append)).unwrap();
    assert!(
        request
            .artifacts()
            .attempt_owned()
            .iter()
            .any(|object| object.path() == stats.statistics_path)
    );
    let appended = publish(&java, "composition", &expected, &request, &operation).await;
    assert_eq!(appended.metadata.statistics_iter().len(), 1);
    assert_eq!(
        appended
            .metadata
            .current_snapshot()
            .unwrap()
            .added_rows_count(),
        Some(1)
    );

    let operation = java.operation(&appended.metadata);
    let snapshot = appended.metadata.current_snapshot().unwrap();
    let measured = intent(
        &java,
        "created",
        &appended.metadata,
        &operation,
        RequestShape::MetadataOnly,
        vec![],
        vec![],
        Dependency::MeasuredSnapshotExists(snapshot.snapshot_id()),
    );
    let attempt = operation.begin_attempt().unwrap();
    let parent = Some(snapshot.snapshot_id());
    assert_eq!(
        crate::commit::dependency::validate(&measured, &appended.metadata, parent, &attempt)
            .await
            .unwrap(),
        crate::commit::dependency::Verdict::Holds
    );
    let stats = statistics(&attempt, snapshot).await;
    let mut engine = StagingEngine::begin(base(&appended), &measured, &attempt).unwrap();
    engine
        .stage(&StatisticsPreparer {
            statistics: stats.clone(),
        })
        .await
        .unwrap();
    let expected = engine.metadata().clone();
    // This metadata-only request owns no cross-attempt/session references.
    let request = engine.freeze(&[]).unwrap();
    assert!(
        request
            .artifacts()
            .attempt_owned()
            .iter()
            .any(|object| object.path() == stats.statistics_path)
    );
    assert!(
        !request
            .requirements()
            .iter()
            .any(|r| matches!(r, TableRequirement::RefSnapshotIdMatch { .. }))
    );
    assert!(!request.updates().iter().any(|u| matches!(
        u,
        TableUpdate::AddSnapshot { .. } | TableUpdate::SetSnapshotRef { .. }
    )));
    let actual = publish(&java, "metadata-only", &expected, &request, &operation).await;
    assert_eq!(
        actual.metadata.current_snapshot_id(),
        appended.metadata.current_snapshot_id()
    );
    assert_eq!(actual.metadata.snapshots().len(), 2);
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru5_java_rest_d12_inheritance_and_physical_rewrite() {
    let java = Java::new("d12").await;
    let loaded = java.create("data", false).await;
    let operation = java.operation(&loaded.metadata);
    let file = java.data(&operation, &loaded.metadata, &[20, 21, 22]).await;
    let append = intent(
        &java,
        "data",
        &loaded.metadata,
        &operation,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(file, loaded.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let attempt = operation.begin_attempt().unwrap();
    let (expected, request) = freeze_append(&loaded, &append, &attempt).await;
    let added = publish(&java, "append", &expected, &request, &operation).await;
    let source = added.metadata.current_snapshot().unwrap();
    let raw = physical(
        &java,
        "append-d12",
        &added.metadata,
        source.snapshot_id(),
        &attempt,
    )
    .await;
    assert_eq!(raw.len(), 1);
    assert_eq!(raw[0].1.sequence_number, None);
    assert_eq!(raw[0].1.file_sequence_number, None);
    assert_eq!(raw[0].1.data_file().first_row_id(), None);
    assert_eq!(raw[0].0.first_row_id, source.first_row_id());
    let inherited = raw[0].0.load_manifest(attempt.file_io()).await.unwrap();
    assert_eq!(
        inherited.entries()[0].sequence_number,
        Some(source.sequence_number())
    );
    assert_eq!(
        inherited.entries()[0].file_sequence_number,
        Some(source.sequence_number())
    );

    let operation = java.operation(&added.metadata);
    let attempt = operation.begin_attempt().unwrap();
    let mut inputs = ValidationInputs::new(&added.metadata, Some(source.snapshot_id()), &attempt);
    let live = inputs.live_set().await.unwrap();
    let frozen: Vec<_> = live.values().map(|v| v.frozen.clone()).collect();
    assert_eq!(frozen.len(), 1);
    let first = frozen[0].facts().first_row_id.unwrap();
    let file = java.data(&operation, &added.metadata, &[20, 21, 22]).await;
    let file = crate::commit::data_file::clone_data_file_with_first_row_id(
        &file,
        frozen[0].facts().partition_spec_id,
        Some(first),
    )
    .unwrap();
    let rewrite = intent(
        &java,
        "data",
        &added.metadata,
        &operation,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::rewritten_data(
                file,
                frozen[0].facts().partition_spec_id,
                source.sequence_number(),
            )
            .unwrap(),
        ],
        frozen,
        Dependency::RefUnchanged,
    );
    let mut engine = StagingEngine::begin(base(&added), &rewrite, &attempt).unwrap();
    engine.stage(&RewriteDataFilesPreparer).await.unwrap();
    let expected = engine.metadata().clone();
    let request = engine.freeze(&added_references(&rewrite)).unwrap();
    let rewritten = publish(&java, "rewrite", &expected, &request, &operation).await;
    let snapshot = rewritten.metadata.current_snapshot().unwrap();
    assert_eq!(snapshot.added_rows_count(), Some(0));
    assert_eq!(
        rewritten.metadata.next_row_id(),
        added.metadata.next_row_id()
    );
    let raw = physical(
        &java,
        "rewrite-d12",
        &rewritten.metadata,
        snapshot.snapshot_id(),
        &attempt,
    )
    .await;
    let replacement = raw
        .iter()
        .find(|(_, entry)| entry.status() == ManifestStatus::Added)
        .unwrap();
    assert_eq!(
        replacement.1.sequence_number,
        Some(source.sequence_number())
    );
    assert_eq!(replacement.1.file_sequence_number, None);
    assert_eq!(replacement.1.data_file().first_row_id(), Some(first));
    let inherited = replacement
        .0
        .load_manifest(attempt.file_io())
        .await
        .unwrap();
    let replacement = inherited
        .entries()
        .iter()
        .find(|e| e.status() == ManifestStatus::Added)
        .unwrap();
    assert_eq!(replacement.sequence_number, Some(source.sequence_number()));
    assert_eq!(
        replacement.file_sequence_number,
        Some(snapshot.sequence_number())
    );
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru5_java_rest_two_canonical_clients_real_cas() {
    let java = Java::new("cas").await;
    let other = java.independent();
    let loaded = java.create("race", false).await;
    let left = java.operation(&loaded.metadata);
    let right = other.operation(&loaded.metadata);
    let left_data = java.data(&left, &loaded.metadata, &[31]).await;
    let right_data = other.data(&right, &loaded.metadata, &[32]).await;
    let left_intent = intent(
        &java,
        "race",
        &loaded.metadata,
        &left,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(left_data, loaded.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let right_intent = intent(
        &other,
        "race",
        &loaded.metadata,
        &right,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(right_data, loaded.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let left_attempt = left.begin_attempt().unwrap();
    let right_attempt = right.begin_attempt().unwrap();
    let (expected, left_request) = freeze_append(&loaded, &left_intent, &left_attempt).await;
    let (_, right_request) = freeze_append(&loaded, &right_intent, &right_attempt).await;
    assert_ne!(left.token(), right.token());
    assert_eq!(left_request.requirements(), right_request.requirements());
    assert!(
        matches!((left_request.base(),right_request.base()), (BaseIdentity::Existing { uuid:a,parent:b,metadata_location:c },BaseIdentity::Existing { uuid:d,parent:e,metadata_location:f }) if a==d && b==e && c==f)
    );
    bounded_preflight(&right_request, &right).unwrap();
    publish(&java, "winner", &expected, &left_request, &left).await;
    let (status, _) = other.post("stale-loser", &right_request).await.unwrap();
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "require real Java requirement rejection"
    );
    let actual = java.load(&java.ident("race")).await.unwrap();
    assert_eq!(semantic_facts(&actual.metadata), semantic_facts(&expected));
    assert_eq!(actual.metadata.snapshots().len(), 1);
    let mut inputs = ValidationInputs::new(
        &actual.metadata,
        actual.metadata.current_snapshot_id(),
        &left_attempt,
    );
    let live = inputs.live_set().await.unwrap();
    assert_eq!(live.len(), 1);
    assert_eq!(
        live.values().next().unwrap().file.file_path(),
        left_intent.changes().added[0].file().file_path()
    );
    assert_ne!(
        live.values().next().unwrap().file.file_path(),
        right_intent.changes().added[0].file().file_path()
    );
}

struct InterleavingPublisher {
    java: Java,
    ident: TableIdent,
    operation: IcebergCommitOperation,
    competitor: Mutex<Option<(Java, IcebergCommitOperation, TableMetadata, FrozenRequest)>>,
    dispatched: Mutex<Vec<Value>>,
}

#[async_trait]
impl Publisher for InterleavingPublisher {
    async fn load_target(&self, attempt: &IcebergCommitAttempt) -> Result<StagingBase> {
        attempt.check_active()?;
        let loaded = self.java.load(&self.ident).await?;
        attempt.check_active()?;
        Ok(base(&loaded))
    }

    fn preflight_recovery(
        &self,
        request: &FrozenRequest,
        operation: &IcebergCommitOperation,
    ) -> Result<()> {
        bounded_preflight(request, operation)
    }

    async fn dispatch_once(&self, request: FrozenRequest) -> CatalogOutcome<CommitProof> {
        let competitor = self.competitor.lock().unwrap().take();
        if let Some((java, operation, expected, competing)) = competitor {
            // The first canonical request is already frozen. An independent client now wins its real CAS.
            publish(&java, "competitor", &expected, &competing, &operation).await;
        }
        let ordinal = request.artifacts().attempt().ordinal();
        let snapshot = request.ref_snapshot_after("main");
        let envelope = request.to_rest_json().unwrap();
        self.dispatched
            .lock()
            .unwrap()
            .push(json!({"attempt":ordinal,"snapshot":snapshot,"request":envelope}));
        let evidence = CatalogCommitEvidence::for_target(format!(
            "{}.{}",
            self.ident.namespace, self.ident.name
        ))
        .with_commit_uuid(self.operation.token().path_component());
        let (status, body) = match self
            .java
            .post(&format!("attempt-{ordinal}"), &request)
            .await
        {
            Ok(result) => result,
            Err(error) => return CatalogOutcome::unknown(error.to_string(), evidence),
        };
        if status == StatusCode::CONFLICT {
            return CatalogOutcome::uncommitted(
                ConnectorMutationFailureKind::Conflict,
                "Java REST rejected stale requirements",
            );
        }
        if status.is_server_error() {
            return CatalogOutcome::unknown(format!("Java REST returned {status}"), evidence);
        }
        if status != StatusCode::OK {
            return CatalogOutcome::uncommitted(
                ConnectorMutationFailureKind::InvalidRequest,
                format!("Java REST returned {status}"),
            );
        }
        let response: CommitTableResponse = match serde_json::from_slice(&body) {
            Ok(response) => response,
            Err(error) => return CatalogOutcome::unknown(error.to_string(), evidence),
        };
        if response.metadata.current_snapshot_id() != snapshot {
            return CatalogOutcome::unknown(
                "Java response did not match exact frozen snapshot",
                evidence,
            );
        }
        CatalogOutcome::committed(
            CommitProof::applied(snapshot).with_table_uuid(response.metadata.uuid().to_string()),
            ExternalMutationEffect::Applied,
        )
    }
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru5_java_rest_attempt_runner_reloads_and_reprepares_after_real_cas() {
    let java = Java::new("reprepare").await;
    let other = java.independent();
    let loaded = java.create("race", false).await;
    let operation = java.operation(&loaded.metadata);
    let competitor = other.operation(&loaded.metadata);
    let data = java.data(&operation, &loaded.metadata, &[41]).await;
    let other_data = other.data(&competitor, &loaded.metadata, &[42]).await;
    let append = intent(
        &java,
        "race",
        &loaded.metadata,
        &operation,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(data, loaded.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let other_intent = intent(
        &other,
        "race",
        &loaded.metadata,
        &competitor,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(other_data, loaded.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoReadDependency,
    );
    let other_attempt = competitor.begin_attempt().unwrap();
    let (other_expected, other_request) =
        freeze_append(&loaded, &other_intent, &other_attempt).await;
    let other_snapshot = other_request.ref_snapshot_after("main").unwrap();
    let publisher = InterleavingPublisher {
        java: java.clone(),
        ident: java.ident("race"),
        operation: operation.clone(),
        competitor: Mutex::new(Some((other, competitor, other_expected, other_request))),
        dispatched: Mutex::new(vec![]),
    };
    let policy = RetryPolicy::from_properties(&HashMap::from([
        ("commit.retry.num-retries".into(), "1".into()),
        ("commit.retry.min-wait-ms".into(), "1".into()),
        ("commit.retry.max-wait-ms".into(), "2".into()),
        ("commit.retry.total-timeout-ms".into(), "60000".into()),
    ]))
    .unwrap();
    let report = attempt::run(
        &operation,
        &append,
        &PreparedChange::default(),
        &[&FastAppendPreparer],
        &publisher,
        policy,
    )
    .await;
    let proof = match report.publication {
        PublicationOutcome::Committed(proof) => proof,
        other => panic!("real retry must commit: {other:?}"),
    };
    assert!(matches!(
        report.cleanup,
        IcebergCleanupReport::Complete { .. }
    ));
    let traces = publisher.dispatched.lock().unwrap().clone();
    assert_eq!(traces.len(), 2);
    assert_eq!(traces[0]["attempt"], 0);
    assert_eq!(traces[1]["attempt"], 1);
    assert_ne!(traces[0]["snapshot"], traces[1]["snapshot"]);
    let snapshots = |trace: &Value| {
        trace["request"]["updates"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|u| u["action"] == "add-snapshot")
            .map(|u| u["snapshot"].clone())
            .collect::<Vec<_>>()
    };
    let first = snapshots(&traces[0]);
    let second = snapshots(&traces[1]);
    assert_eq!(first.len(), 1);
    assert_eq!(second.len(), 1);
    assert_ne!(first[0]["manifest-list"], second[0]["manifest-list"]);
    assert_eq!(second[0]["parent-snapshot-id"], other_snapshot);
    let actual = java.load(&java.ident("race")).await.unwrap();
    assert_eq!(
        actual.metadata.snapshots().len(),
        2,
        "failed first attempt must not publish a snapshot"
    );
    assert_eq!(actual.metadata.current_snapshot_id(), proof.snapshot_id);
    assert_eq!(
        actual
            .metadata
            .current_snapshot()
            .unwrap()
            .parent_snapshot_id(),
        Some(other_snapshot)
    );
    assert!(
        actual
            .metadata
            .snapshot_by_id(first[0]["snapshot-id"].as_i64().unwrap())
            .is_none()
    );
    assert_eq!(actual.metadata.next_row_id(), 2);
    let attempt = operation.begin_attempt().unwrap();
    let mut inputs = ValidationInputs::new(
        &actual.metadata,
        actual.metadata.current_snapshot_id(),
        &attempt,
    );
    let paths: BTreeSet<_> = inputs
        .live_set()
        .await
        .unwrap()
        .values()
        .map(|v| v.file.file_path().to_owned())
        .collect();
    assert_eq!(
        paths,
        BTreeSet::from([
            append.changes().added[0].file().file_path().to_owned(),
            other_intent.changes().added[0]
                .file()
                .file_path()
                .to_owned()
        ])
    );
    for record in operation.artifacts().unwrap().iter().filter(|r| {
        r.class == ArtifactClass::Attempt && r.attempt.is_some_and(|t| t.ordinal() == 0)
    }) {
        assert!(
            !attempt
                .file_io()
                .new_input(record.object.path())
                .unwrap()
                .exists()
                .await
                .unwrap(),
            "failed attempt object must be removed"
        );
    }
    java.record("retry-traces",&json!({"dispatches":traces,"metadata":semantic_facts(&actual.metadata),"cleanup":format!("{:?}",report.cleanup)}));
}

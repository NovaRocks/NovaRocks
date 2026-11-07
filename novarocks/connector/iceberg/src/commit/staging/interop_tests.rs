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
use crate::rest_protocol::wire::{
    CommitTableRequest, CommitTableResponse, CreateTableRequest, LoadTableResult,
};
use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use bytes::Bytes;
use novarocks_fs::{
    FsAccessResolver, ObjectStoreConfig, SecretValue, TokioFileIoRuntime, TokioFileIoSpawner,
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

// The finite driver cases share one original run and use distinct suffixes
// short enough for the scratch sweeper's complete resource-name grammar.
const INTEROP_CASES: &[(&str, &str, &str)] = &[
    ("NR_IRU5", "shapes", "j501"),
    ("NR_IRU5", "d12", "j502"),
    ("NR_IRU5", "cas", "j503"),
    ("NR_IRU5", "reprepare", "j504"),
    ("NR_IRU13A", "lifecycle", "a001"),
    ("NR_IRU13A", "commits", "a002"),
    ("NR_IRU13A", "ctas", "a003"),
    ("NR_IRU13A", "views", "a004"),
    ("NR_IRU13A", "recreated", "a005"),
    ("NR_IRU13B", "ctas", "b001"),
    ("NR_IRU13B", "schema", "b002"),
    ("NR_IRU13B", "replace", "b003"),
    ("NR_IRU13B", "reuse", "b004"),
    ("NR_IRU13B", "dialects", "b005"),
];

fn original_scratch() -> Option<Value> {
    let value: Value = serde_json::from_str(
        &std::env::var("NOVA_FIXTURE_RUN_CONTEXT").unwrap_or_else(|_| "null".into()),
    )
    .expect("original fixture run context must be JSON");
    (!value.is_null()).then_some(value)
}

fn canonical_interop_s3(uri: &str) -> Option<String> {
    let body = uri.strip_prefix("s3://")?;
    let (bucket, path) = body.split_once('/').unwrap_or((body, ""));
    let edge = |value: u8| value.is_ascii_lowercase() || value.is_ascii_digit();
    if !(3..=63).contains(&bucket.len())
        || !edge(*bucket.as_bytes().first()?)
        || !edge(*bucket.as_bytes().last()?)
        || !bucket
            .bytes()
            .all(|value| edge(value) || matches!(value, b'.' | b'-'))
        || uri.contains(['%', '*', '?', '#'])
        || uri.chars().any(|value| value.is_control() || value <= ' ')
        || path.starts_with('/')
        || path.contains("//")
    {
        return None;
    }
    let path = path.trim_matches('/');
    if !path.is_empty() && path.split('/').any(|part| matches!(part, "" | "." | "..")) {
        return None;
    }
    Some(if path.is_empty() {
        format!("s3://{bucket}")
    } else {
        format!("s3://{bucket}/{path}")
    })
}

fn interop_descendant(root: &str, location: &str) -> bool {
    match (canonical_interop_s3(root), canonical_interop_s3(location)) {
        (Some(root), Some(location)) => location.starts_with(&format!("{root}/")),
        _ => false,
    }
}

fn interop_storage_belongs(storage: &Value, context: &Value, location: &str) -> bool {
    if storage["schema"] != 1
        || storage["identity"] != context["identity"]
        || storage["machine"]["daemon_id"] != storage["storage_owner"]["daemon_id"]
        || storage["storage_owner"]["minio_volume"]
            .as_str()
            .is_none_or(str::is_empty)
    {
        return false;
    }
    let normalized = location
        .strip_prefix("s3a://")
        .map(|body| format!("s3://{body}"));
    let Some(location) = canonical_interop_s3(normalized.as_deref().unwrap_or(location)) else {
        return false;
    };
    let Some(root) = context["identity"]["scope_root"].as_str() else {
        return false;
    };
    if storage["ranges"]["run_root"].as_str() != Some(format!("{root}/").as_str()) {
        return false;
    }
    if location == root || interop_descendant(root, &location) {
        return true;
    }
    let Some(run) = context["identity"]["run_id"].as_str() else {
        return false;
    };
    for range in ["rest", "rest_mv"] {
        let Some(warehouse) = storage["ranges"][range]["warehouse"].as_str() else {
            return false;
        };
        if storage["ranges"][range]["namespace_prefix"].as_str() != Some(format!("{run}_").as_str())
            || canonical_interop_s3(warehouse).is_none_or(|v| format!("{v}/") != warehouse)
        {
            return false;
        }
        if let Some(relative) = location.strip_prefix(warehouse) {
            let namespace = relative.split('/').next().unwrap_or_default();
            if let Some(purpose) = namespace.strip_prefix(&format!("{run}_"))
                && namespace.len() <= 63
                && !purpose.is_empty()
                && purpose.as_bytes()[0].is_ascii_lowercase()
                && purpose
                    .bytes()
                    .all(|v| v.is_ascii_lowercase() || v.is_ascii_digit())
            {
                return true;
            }
        }
    }
    false
}

fn assert_original_storage(location: &str) {
    if let Some(context) = original_scratch() {
        let storage: Value = serde_json::from_str(&required("NOVA_FIXTURE_RUN_STORAGE"))
            .expect("original fixture storage projection must be JSON");
        assert!(
            interop_storage_belongs(&storage, &context, location),
            "actual Java location must belong to the original run storage ranges"
        );
    }
}

fn scratch_namespace(context: &Value, driver: &str, case: &str) -> String {
    let identity = &context["identity"];
    let run = identity["run_id"]
        .as_str()
        .expect("original scratch run ID");
    let pieces: Vec<_> = run.split('_').collect();
    assert!(
        pieces.len() == 4
            && pieces[0] == "nr"
            && pieces[1] == "scratch"
            && pieces[2].len() == 13
            && pieces[2].bytes().all(|v| v.is_ascii_digit())
            && pieces[3].len() == 32
            && pieces[3]
                .bytes()
                .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v)),
        "original scratch run ID must retain the complete frozen grammar"
    );
    assert_eq!(
        identity["created_unix_ms"].as_u64(),
        pieces[2].parse::<u64>().ok()
    );
    let root = identity["scope_root"]
        .as_str()
        .expect("original scratch root");
    assert!(
        canonical_interop_s3(root).as_deref() == Some(root)
            && root.ends_with(&format!("/scratch/{run}"))
    );
    assert_eq!(context["schema"].as_u64(), Some(1));
    let suffix = INTEROP_CASES
        .iter()
        .find(|(d, c, _)| *d == driver && *c == case)
        .expect("shared interop cases require a unique frozen ordinal")
        .2;
    let namespace = format!("{run}_{suffix}");
    assert!(namespace.len() <= 63);
    namespace
}

fn interop_namespace(driver: &str, case: &str) -> NamespaceIdent {
    NamespaceIdent::new(match original_scratch() {
        Some(context) => scratch_namespace(&context, driver, case),
        None => format!(
            "{}_{}",
            required(&format!("{driver}_INTEROP_NAMESPACE")),
            case
        ),
    })
}

fn scratch_location(namespace: &NamespaceIdent, name: Option<&str>) -> Option<String> {
    original_scratch().map(|context| {
        let run = context["identity"]["run_id"]
            .as_str()
            .expect("original scratch run ID");
        let namespace = namespace.join(".");
        assert!(namespace.starts_with(&format!("{run}_")) && namespace.len() <= 63);
        let base = format!(
            "{}/rest/{namespace}",
            context["identity"]["scope_root"]
                .as_str()
                .expect("original scratch root")
        );
        name.map_or_else(|| base.clone(), |name| format!("{base}/{name}"))
    })
}

fn scratch_namespace_properties(_namespace: &NamespaceIdent) -> HashMap<String, String> {
    HashMap::new()
}

#[test]
fn shared_interop_case_ordinals_preserve_complete_scratch_identity() {
    let run = "nr_scratch_1792530000000_0123456789abcdef0123456789abcdef";
    let context = json!({"schema":1,"identity":{"run_id":run,"created_unix_ms":1792530000000u64,
        "scope_root":format!("s3://novarocks/worktree/scratch/{run}")}});
    let mut names = BTreeSet::new();
    for (driver, case, suffix) in INTEROP_CASES {
        assert!(suffix.len() <= 5);
        let name = scratch_namespace(&context, driver, case);
        assert!(name.starts_with(&format!("{run}_")) && name.len() <= 63);
        assert!(
            names.insert(name),
            "case ordinals must be globally unique per run"
        );
    }
    let mut custom = context.clone();
    custom["identity"]["scope_root"] =
        json!(format!("s3://custom-bucket/another/prefix/scratch/{run}"));
    assert_eq!(
        scratch_namespace(&custom, "NR_IRU5", "shapes"),
        format!("{run}_j501")
    );
    let base = format!("s3://custom-bucket/another/prefix/scratch/{run}");
    assert!(interop_descendant(&base, &format!("{base}/rest/ns/table")));
    for location in [
        format!("{base}-sibling/t"),
        format!("{base}/../escaped"),
        format!("{base}/%2e%2e/escaped"),
        format!("{base}//table"),
        format!("s3://custom-bucket//another/prefix/scratch/{run}/table"),
        format!("{base}/table?query"),
        format!("{base}/table#fragment"),
        format!("{base}/table{}invalid", char::from(10)),
    ] {
        assert!(
            !interop_descendant(&base, &location),
            "noncanonical or sibling locations are not descendants"
        );
    }
    custom["identity"]["scope_root"] = json!(format!("s3://custom-bucket/../scratch/{run}"));
    assert!(std::panic::catch_unwind(|| scratch_namespace(&custom, "NR_IRU5", "shapes")).is_err());
    let mut malformed = context.clone();
    malformed["identity"]["run_id"] = json!(format!("{run}_truncated"));
    assert!(
        std::panic::catch_unwind(|| scratch_namespace(&malformed, "NR_IRU5", "shapes")).is_err()
    );
}

#[test]
fn shared_interop_storage_membership_matches_fixture_scope_vectors() {
    let vectors: Value = serde_json::from_str(include_str!(
        "../../../../../../docker/iceberg-rest/tests/run_storage_scope_vectors.json"
    ))
    .unwrap();
    let context = json!({"identity":vectors["identity"]});
    for case in vectors["membership"].as_array().unwrap() {
        assert_eq!(
            interop_storage_belongs(
                &vectors["projection"],
                &context,
                case["uri"].as_str().unwrap()
            ),
            case["belongs"].as_bool().unwrap_or(false),
            "{}",
            case["uri"]
        );
    }
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
        let namespace = interop_namespace("NR_IRU5", case);
        let artifacts = PathBuf::from(required("NR_IRU5_INTEROP_ARTIFACT_DIR")).join(case);
        let java = Self::attach(namespace, artifacts).await;
        let result = java
            .client
            .post(java.endpoint(&["namespaces"]))
            .json(&json!({"namespace": java.namespace.as_ref(), "properties": scratch_namespace_properties(&java.namespace)}))
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

    /// Attach the independent raw-HTTP client to the pinned server without
    /// creating `namespace`.
    async fn attach(namespace: NamespaceIdent, artifacts: PathBuf) -> Self {
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
            novarocks_fs::TokioRefreshExecutor::shared(runtime.clone()),
            Arc::new(TokioFileIoSpawner::new(runtime)),
        );
        Self {
            client,
            root,
            prefix,
            namespace,
            binding,
            artifacts,
        }
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
        assert_original_storage(loaded.metadata.location());
        if let Some(location) = loaded.metadata_location.as_deref() {
            assert_original_storage(location);
        }
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
        let loaded: LoadTableResult = response
            .json()
            .await
            .map_err(|e| error(format!("Java load decode failed: {e}")))?;
        assert_original_storage(loaded.metadata.location());
        if let Some(location) = loaded.metadata_location.as_deref() {
            assert_original_storage(location);
        }
        Ok(loaded)
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

async fn probe_writer_sizes(java: &Java, metadata: &TableMetadata) {
    // This operation is never frozen or dispatched. Its test-only objects cannot
    // become publication references, and only its own ledger authorizes cleanup.
    let operation = java.operation(metadata);
    let attempt = operation.begin_attempt().unwrap();
    let mut objects = Vec::new();
    let observed: Result<Vec<Value>> = async {
        let cases = [
            ("empty", vec![]),
            ("single", vec![Bytes::from_static(b"single-write")]),
            (
                "multiple",
                vec![Bytes::from_static(b"first-"), Bytes::from_static(b"second")],
            ),
        ];
        let mut observations = Vec::new();
        for (label, chunks) in cases {
            let object = attempt.allocate(ArtifactClass::Attempt, ArtifactKind::Metadata)?;
            assert_original_storage(object.path());
            objects.push(object.clone());
            let mut writer = attempt.file_io().new_output(object.path())?.writer().await?;
            let mut expected = Vec::new();
            for chunk in chunks {
                expected.extend_from_slice(&chunk);
                writer.write(chunk).await?;
            }
            let closed = writer.close().await?;
            drop(writer);
            let input = attempt.file_io().new_input(object.path())?;
            let stored = input.metadata().await?;
            let actual = input.read().await?;
            let expected_size = u64::try_from(expected.len())
                .map_err(|e| error(format!("Probe input length is not representable: {e}")))?;
            if closed.size != expected_size
                || stored.size != expected_size
                || actual.as_ref() != expected.as_slice()
            {
                return Err(error(format!(
                    "MinIO writer size differs for {label}: expected={expected_size}, close={}, stat={}, read={}",
                    closed.size, stored.size, actual.len()
                )));
            }
            observations.push(json!({
                "case": label,
                "path": object.path(),
                "expected-size": expected_size,
                "close-size": closed.size,
                "stat-size": stored.size,
                "read-size": actual.len(),
            }));
        }
        let artifacts = operation.artifacts()?;
        if artifacts.len() != 3
            || artifacts.iter().any(|record| record.write_state != ArtifactWriteState::Written)
        {
            return Err(error("MinIO probes must retain all three completed objects in their original ledger"));
        }
        Ok(observations)
    }
    .await;
    // Every writer above has actually exited before the original operation
    // cleans its never-published objects, including after a probe I/O failure.
    drop(attempt);
    let cleanup = operation.cleanup(CleanupScope::EntireOperation).await;
    java.record(
        "writer-size-probes",
        &json!({
            "operation": operation.token().path_component(),
            "observed": observed.as_ref().ok(),
            "error": observed.as_ref().err().map(ToString::to_string),
            "cleanup": format!("{cleanup:?}"),
        }),
    );
    let _ = observed.unwrap();
    assert_eq!(cleanup, IcebergCleanupReport::Complete { deleted: 3 });
    assert!(operation.artifacts().unwrap().is_empty());
    let independent_io =
        crate::fs_io::build_file_io_for_location(metadata.location(), java.binding.clone());
    for object in objects {
        assert!(
            !independent_io
                .new_input(object.path())
                .unwrap()
                .exists()
                .await
                .unwrap()
        );
    }
}

async fn physical(
    java: &Java,
    label: &str,
    metadata: &TableMetadata,
    snapshot_id: i64,
    attempt: &IcebergCommitAttempt,
) -> Vec<(ManifestFile, ManifestEntry)> {
    let snapshot = metadata.snapshot_by_id(snapshot_id).unwrap();
    let list = crate::manifest::load_manifest_list(snapshot, attempt.file_io(), metadata)
        .await
        .unwrap();
    let mut raw = Vec::new();
    let mut lengths = Vec::new();
    for manifest in list.entries() {
        let input = attempt
            .file_io()
            .new_input(&manifest.manifest_path)
            .unwrap();
        let stored = input.metadata().await.unwrap();
        let bytes = input.read().await.unwrap();
        let recorded = u64::try_from(manifest.manifest_length).unwrap();
        assert_eq!(
            recorded, stored.size,
            "Manifest length must match MinIO stat"
        );
        assert_eq!(
            recorded,
            u64::try_from(bytes.len()).unwrap(),
            "Manifest length must match actual bytes"
        );
        lengths.push(json!({
            "path": manifest.manifest_path,
            "manifest-length": recorded,
            "stat-size": stored.size,
            "read-size": bytes.len(),
        }));
        for entry in Manifest::parse_avro(&bytes).unwrap().entries() {
            raw.push((manifest.clone(), entry.as_ref().clone()));
        }
    }
    java.record(
        label,
        &json!({
            "manifest-sizes": lengths,
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
    probe_writer_sizes(&java, &staged.metadata).await;
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
        Dependency::NoRead,
    );
    let attempt = operation.begin_attempt().unwrap();
    let mut engine = StagingEngine::begin(
        StagingBase::Create {
            staged: StagedCreateIdentity::new(operation.token(), Arc::new(staged.metadata.clone())),
            initialization_updates: super::staged_create_updates(&staged.metadata).unwrap(),
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
        Dependency::NoRead,
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
        Dependency::NoRead,
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
    let inherited = crate::manifest::load_manifest(&raw[0].0, attempt.file_io())
        .await
        .unwrap();
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
    let inherited = crate::manifest::load_manifest(&replacement.0, attempt.file_io())
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
        Dependency::NoRead,
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
        Dependency::NoRead,
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
        Dependency::NoRead,
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
        Dependency::NoRead,
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

// ---------------------------------------------------------------------------
// IRU-13a: every NovaRocks-issued REST request against the Java reference
// server, judged by NovaRocks' own D13 evidence and cross-checked through the
// independent raw-HTTP client above.
// ---------------------------------------------------------------------------

use crate::catalog::view_request::{ValidatedViewReplace, ViewCreate, ViewDefinition};
use crate::catalog_runtime::RestAccessDelegationMode;

use crate::catalog::NovaRocksCatalog;
use crate::catalog::admission::CatalogOperation;
use crate::rest_protocol::endpoints::RestEndpoints;
use crate::rest_protocol::evidence::{
    AttributionExpectation, ExpectedSnapshot, MutationEndpoint, RestEvidence, RestReceipt,
    UnknownReason,
};
use crate::rest_protocol::mutation::MutationOutcome;
use novarocks_spi::connector::ConnectorErrorKind;

/// One NovaRocks REST generation built exactly as production builds a static
/// catalog, plus the independent Java oracle for the same namespace.
// A fixture-only finalizer runs after the real Java response. It preserves
// the adapter's evidence and any original owner work; only the added work fails.
struct JavaFinalizationProbeDispatch {
    inner: Box<dyn crate::catalog::publication_dispatch::PreparedCatalogDispatch>,
    journal: crate::commit::attempt::observation::PublicationJournal<()>,
    polls: Arc<std::sync::atomic::AtomicUsize>,
}
#[async_trait]
impl crate::catalog::publication_dispatch::PreparedCatalogDispatch
    for JavaFinalizationProbeDispatch
{
    async fn dispatch_once(
        self: Box<Self>,
        context: crate::catalog::publication::EvidenceContext,
    ) -> crate::catalog::publication_dispatch::DispatchCompletion {
        let mut completion = self.inner.dispatch_once(context).await;
        completion.finalization = Some(Box::new(JavaFinalizationProbe {
            inner: completion.finalization,
            journal: self.journal,
            polls: self.polls,
        }));
        completion
    }
}
struct JavaFinalizationProbe {
    inner: Option<Box<dyn crate::catalog::publication_dispatch::FinalizationWork>>,
    journal: crate::commit::attempt::observation::PublicationJournal<()>,
    polls: Arc<std::sync::atomic::AtomicUsize>,
}
#[async_trait]
impl crate::catalog::publication_dispatch::FinalizationWork for JavaFinalizationProbe {
    async fn run(
        self: Box<Self>,
        context: crate::catalog::publication::EvidenceContext,
    ) -> novarocks_spi::connector::ExternalMutationFinalization {
        assert!(
            matches!(
                self.journal
                    .commit_outcome(self.journal.current_entry().unwrap()),
                CatalogOutcome::KnownCommitted {
                    effect: ExternalMutationEffect::Applied,
                    ..
                }
            ),
            "Actual Java Applied evidence must be recorded before the first finalization poll"
        );
        self.polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(inner) = self.inner {
            assert_eq!(
                inner.run(context).await,
                novarocks_spi::connector::ExternalMutationFinalization::Complete
            );
        }
        novarocks_spi::connector::ExternalMutationFinalization::Failed(
            novarocks_spi::connector::ConnectorMutationFailure::new(
                ConnectorMutationFailureKind::Internal,
                "Injected finalization failure after actual Java publication",
            ),
        )
    }
}

struct NovaRocksRest {
    java: Java,
    endpoints: Arc<RestEndpoints>,
    owner: Arc<dyn NovaRocksCatalog>,
    descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor,
    generation: novarocks_spi::connector::ProviderBindingEpoch,
}

impl NovaRocksRest {
    /// The namespace is created later, by a NovaRocks request. `driver` is
    /// the environment prefix of the interop driver that owns this run, e.g.
    /// `NR_IRU13A`.
    async fn new(driver: &str, case: &str) -> Self {
        let namespace = interop_namespace(driver, case);
        let artifacts =
            PathBuf::from(required(&format!("{driver}_INTEROP_ARTIFACT_DIR"))).join(case);
        let java = Java::attach(namespace, artifacts).await;
        let configuration = crate::catalog_config::parse_catalog_configuration(
            "iru13a",
            &[
                ("type".to_string(), "iceberg".to_string()),
                ("iceberg.catalog.type".to_string(), "rest".to_string()),
                ("uri".to_string(), required("NOVAROCKS_ICEBERG_REST_URI")),
                (
                    "warehouse".to_string(),
                    required("NOVAROCKS_ICEBERG_REST_WAREHOUSE"),
                ),
            ],
        )
        .unwrap();
        let client = crate::catalog_runtime::build_catalog_client(
            &configuration,
            java.binding.clone(),
            crate::iceberg::Runtime::try_current().unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(
            client.rest_access_delegation(),
            RestAccessDelegationMode::Static
        );
        let endpoints = Arc::clone(client.rest_endpoints().expect("REST generation endpoints"));
        java.record("fixture", &json!({"publication": required("NOVA_ENV_REST_ENV_FILE"), "prefix": java.prefix, "namespace": java.namespace.as_ref()}));
        let owner =
            crate::catalog::factory::NovaRocksCatalogFactory::adopt(&configuration, &client)
                .unwrap();
        Self {
            java,
            endpoints,
            owner,
            descriptor: novarocks_spi::connector::ConnectorInstanceDescriptor {
                provider_id: novarocks_spi::connector::ConnectorProviderId::parse("iceberg")
                    .unwrap(),
                instance_id: novarocks_spi::connector::ConnectorInstanceId::parse("iru13a")
                    .unwrap(),
            },
            generation: novarocks_spi::connector::ProviderBindingEpoch::from_bytes([13; 16]),
        }
    }

    async fn publish(
        &self,
        label: &str,
        request: FrozenRequest,
        operation: CatalogOperation,
        context: ConnectorRequestContext,
        staged: Option<crate::catalog::StagedPublicationExpectation>,
    ) -> CatalogOutcome<CommitProof> {
        let outcome = crate::catalog::admission_test_support::publish_frozen(
            Arc::clone(&self.owner),
            self.descriptor.clone(),
            self.generation,
            request,
            operation,
            context,
            staged,
        )
        .await;
        self.java
            .record(label, &json!({"journal-outcome":format!("{outcome:?}")}));
        outcome
    }

    async fn publish_with_failed_finalization(
        &self,
        request: FrozenRequest,
        context: ConnectorRequestContext,
    ) -> CatalogOutcome<CommitProof> {
        use crate::catalog::publication_dispatch::PreparedPublication;
        let token = request.artifacts().attempt().operation();
        let evidence = novarocks_spi::connector::ExternalMutationEvidence::try_new(
            2,
            self.descriptor.clone(),
            self.generation,
            novarocks_spi::connector::ConnectorMutationOperationId::from_bytes(token.to_bytes()),
            "java_finalization_probe",
            Bytes::from_static(b"{}"),
        )
        .unwrap();
        let journal = crate::commit::attempt::observation::PublicationJournal::new(token);
        journal.record(evidence, (), &request).unwrap();
        let owner = Arc::clone(&self.owner);
        let policy = owner.evidence_policy();
        let probe_journal = journal.clone();
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let probe_polls = Arc::clone(&polls);
        let entry = journal
            .publish_once(
                context,
                policy,
                move |descriptor, generation, preparation| async move {
                    let prepared = owner
                        .prepare_frozen_for_admission_test(
                            request,
                            CatalogOperation::AlterProperties,
                            None,
                            descriptor,
                            generation,
                            preparation,
                        )
                        .await?;
                    Ok(match prepared {
                        PreparedPublication::Mutation { binding, dispatch } => {
                            PreparedPublication::Mutation {
                                binding,
                                dispatch: Box::new(JavaFinalizationProbeDispatch {
                                    inner: dispatch,
                                    journal: probe_journal,
                                    polls: probe_polls,
                                }),
                            }
                        }
                        PreparedPublication::NoOp => {
                            panic!("Property change must dispatch a real mutation")
                        }
                    })
                },
            )
            .await
            .unwrap();
        assert_eq!(polls.load(std::sync::atomic::Ordering::SeqCst), 1);
        let outcome = journal.commit_outcome(entry);
        self.java.record(
            "failed-finalization",
            &json!({"journal-outcome":format!("{outcome:?}")}),
        );
        outcome
    }

    fn namespace(&self) -> &NamespaceIdent {
        &self.java.namespace
    }

    /// Record the evidence of one request; its `Debug` carries no server text.
    fn record(&self, label: &str, outcome: &MutationOutcome) {
        self.java.record(
            label,
            &json!({"evidence": format!("{:?}", outcome.evidence)}),
        );
    }

    /// Use the same frozen catalog owner and journal as production. Retain the
    /// real response only after its evidence and finalization are recorded.
    async fn publish_catalog(
        &self,
        label: &str,
        request: crate::catalog::catalog_request::CatalogPublicationRequest,
    ) -> (CatalogOutcome<CommitProof>, Option<Vec<u8>>) {
        use crate::catalog::catalog_request::{
            CatalogCreatePublicationRequest, CatalogPublicationRequest,
        };
        let operation_id = novarocks_spi::connector::ConnectorMutationOperationId::new();
        let frozen = match request {
            CatalogPublicationRequest::CreateTable {
                namespace,
                creation,
                no_op_if_exists,
            } => self
                .owner
                .freeze_create_publication(CatalogCreatePublicationRequest {
                    operation_id: operation_id.to_bytes(),
                    namespace,
                    creation,
                    no_op_if_exists,
                })
                .unwrap(),
            request => request.freeze(operation_id.to_bytes()).unwrap(),
        };
        let context = ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(30),
            ConnectorStopOwner::new().view(),
            MAX_CONNECTOR_HANDLE_PAYLOAD_BYTES,
            MAX_CONNECTOR_TOTAL_PAYLOAD_BYTES,
        )
        .unwrap();
        let result = crate::catalog::admission_test_support::publish_catalog_with_document(
            Arc::clone(&self.owner),
            frozen,
            operation_id,
            self.generation,
            context.clone(),
            self.java.binding.for_request(context),
        )
        .await;
        self.java
            .record(label, &json!({"journal-outcome":format!("{:?}", result.0)}));
        result
    }

    async fn create_namespace(&self) {
        let (outcome, _) = self
            .publish_catalog(
                "create-namespace",
                crate::catalog::catalog_request::CatalogPublicationRequest::CreateNamespace {
                    namespace: crate::catalog::CatalogNamespaceName::new(
                        self.namespace().join("."),
                    ),
                    properties: scratch_namespace_properties(self.namespace()),
                    no_op_if_exists: false,
                },
            )
            .await;
        committed_catalog(&outcome);
    }

    async fn drop_namespace(&self) {
        let (outcome, _) = self
            .publish_catalog(
                "drop-namespace",
                crate::catalog::catalog_request::CatalogPublicationRequest::DropNamespace {
                    namespace: crate::catalog::CatalogNamespaceName::new(
                        self.namespace().join("."),
                    ),
                    no_op_if_missing: false,
                },
            )
            .await;
        committed_catalog(&outcome);
    }

    /// Create a table immediately (static access) and return the applied
    /// response document; Java independently checks the receipt's identity.
    async fn create_table(&self, label: &str, name: &str) -> LoadTableResult {
        let request = create_request(self.namespace(), name);
        let (outcome, document) = self
            .publish_catalog(
                label,
                crate::catalog::catalog_request::CatalogPublicationRequest::CreateTable {
                    namespace: crate::catalog::CatalogNamespaceName::new(
                        self.namespace().join("."),
                    ),
                    creation: crate::iceberg::TableCreation::builder()
                        .name(request.name)
                        .location_opt(request.location)
                        .schema(request.schema)
                        .partition_spec(request.partition_spec.unwrap())
                        .sort_order(request.write_order.unwrap())
                        .properties(request.properties)
                        .build(),
                    no_op_if_exists: false,
                },
            )
            .await;
        let receipt = committed_catalog(&outcome);
        assert!(
            document.is_none(),
            "Ordinary create retains minimal evidence only"
        );
        let (loaded, _) = self
            .endpoints
            .load_table(&self.java.ident(name), false)
            .await
            .unwrap();
        // Static fixture metadata comes from the original Provider session;
        // storage authority remains the original explicit fixture binding.
        let created = LoadTableResult {
            metadata_location: loaded.metadata_location().map(str::to_owned),
            metadata: loaded.metadata().clone(),
            config: HashMap::new(),
            storage_credentials: None,
        };
        assert_eq!(
            receipt.table_uuid.as_deref(),
            Some(created.metadata.uuid().to_string().as_str())
        );
        assert!(receipt.metadata_location.is_some());
        assert_eq!(
            receipt.metadata_location.as_deref(),
            created.metadata_location.as_deref()
        );
        assert_eq!(created.metadata.format_version(), FormatVersion::V3);
        let oracle = self.java.load(&self.java.ident(name)).await.unwrap();
        assert_eq!(oracle.metadata.uuid(), created.metadata.uuid());
        assert_eq!(oracle.metadata_location, created.metadata_location);
        created
    }

    async fn drop_table(&self, label: &str, name: &str) {
        let (outcome, _) = self
            .publish_catalog(
                label,
                crate::catalog::catalog_request::CatalogPublicationRequest::DropTable {
                    table: crate::catalog::CatalogTableName::new(self.namespace().join("."), name),
                    no_op_if_missing: false,
                    purge: false,
                    capture: None,
                },
            )
            .await;
        committed_catalog(&outcome);
    }

    /// The raw status of an independent load.
    async fn load_status(&self, ident: &TableIdent) -> StatusCode {
        self.java
            .client
            .get(self.java.table_url(ident))
            .send()
            .await
            .unwrap()
            .status()
    }
}

/// The receipt of an applied request; anything else fails with its evidence.
fn applied(outcome: &MutationOutcome, endpoint: MutationEndpoint) -> RestReceipt {
    match &outcome.evidence {
        RestEvidence::Applied { receipt } if receipt.endpoint == endpoint => receipt.clone(),
        other => panic!("Java REST must apply the NovaRocks {endpoint:?} request: {other:?}"),
    }
}

fn committed_catalog(outcome: &CatalogOutcome<CommitProof>) -> CommitProof {
    match outcome {
        CatalogOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            finalization: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            receipt,
        } => receipt.clone(),
        other => panic!("Java must acknowledge the actual prepared owner: {other:?}"),
    }
}

fn freeze_metadata_fixture(
    java: &Java,
    ident: TableIdent,
    base: &LoadTableResult,
    change: PreparedChange,
) -> (FrozenRequest, ConnectorRequestContext) {
    let operation = java.operation(&base.metadata);
    let context = operation.request_context().clone();
    let request = super::freeze_existing_change(
        ident,
        "main",
        &base.metadata,
        base.metadata_location
            .as_deref()
            .expect("authoritative Java metadata location"),
        RequestShape::MetadataOnly,
        AttemptToken::new(operation.token(), 0),
        change,
        &context,
    )
    .unwrap();
    (request, context)
}

fn create_request(namespace: &NamespaceIdent, name: &str) -> CreateTableRequest {
    CreateTableRequest {
        name: name.to_owned(),
        location: scratch_location(namespace, Some(name)),
        schema: schema(),
        partition_spec: Some(PartitionSpec::unpartition_spec().into_unbound()),
        write_order: Some(SortOrder::unsorted_order()),
        stage_create: None,
        properties: HashMap::from([
            ("format-version".into(), "3".into()),
            ("write.row-lineage".into(), "true".into()),
        ]),
    }
}

fn set_property(key: &str, value: &str) -> TableUpdate {
    TableUpdate::SetProperties {
        updates: HashMap::from([(key.to_owned(), value.to_owned())]),
    }
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13a_java_rest_namespace_table_lifecycle_and_interleaved_reads() {
    let rest = NovaRocksRest::new("NR_IRU13A", "lifecycle").await;
    let java = &rest.java;
    let endpoints = &rest.endpoints;
    let namespace = rest.namespace();

    // Provider reads and prepared publications share the original session binding.
    let upstream = Arc::clone(&rest.owner);
    assert!(
        !upstream
            .namespace_exists(crate::catalog::CatalogNamespaceName::new(
                namespace.join(".")
            ))
            .await
            .unwrap()
    );
    rest.create_namespace().await;
    let (no_op, _) = rest
        .publish_catalog(
            "create-namespace-no-op",
            crate::catalog::catalog_request::CatalogPublicationRequest::CreateNamespace {
                namespace: crate::catalog::CatalogNamespaceName::new(namespace.join(".")),
                properties: HashMap::new(),
                no_op_if_exists: true,
            },
        )
        .await;
    assert!(matches!(
        no_op,
        CatalogOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            finalization: novarocks_spi::connector::ExternalMutationFinalization::Complete,
            ..
        }
    ));
    assert!(
        upstream
            .namespace_exists(crate::catalog::CatalogNamespaceName::new(
                namespace.join(".")
            ))
            .await
            .unwrap()
    );
    let again = endpoints.create_namespace(namespace, HashMap::new()).await;
    rest.record("create-namespace-again", &again);
    assert_eq!(
        again.evidence,
        RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::AlreadyExists
        }
    );

    // Single-page namespace listing, following the server's tokens.
    let mut seen = BTreeSet::new();
    let mut found = false;
    let mut token: Option<String> = None;
    loop {
        let page = endpoints
            .list_namespaces_page(token.as_deref(), 50)
            .await
            .unwrap();
        assert!(page.namespaces.len() <= 50);
        found |= page.namespaces.contains(namespace);
        match page.next_page_token {
            Some(next) => {
                assert!(seen.insert(next.clone()), "server repeated a page token");
                token = Some(next);
            }
            None => break,
        }
    }
    assert!(
        found,
        "paged namespace listing must contain the new namespace"
    );

    let names = ["t0", "t1", "t2"];
    let mut created = BTreeMap::new();
    for name in names {
        assert!(!endpoints.table_exists(&java.ident(name)).await.unwrap());
        let table = rest.create_table(&format!("create-{name}"), name).await;
        assert!(endpoints.table_exists(&java.ident(name)).await.unwrap());
        created.insert(name, table);
    }
    assert!(!endpoints.table_exists(&java.ident("absent")).await.unwrap());

    // Paged table listing with a page smaller than the namespace.
    let mut listed = Vec::new();
    let mut pages = 0;
    let mut seen = BTreeSet::new();
    let mut token: Option<String> = None;
    loop {
        let page = endpoints
            .list_tables_page(namespace, token.as_deref(), 2)
            .await
            .unwrap();
        pages += 1;
        assert!(page.identifiers.len() <= 2);
        listed.extend(page.identifiers.into_iter().map(|ident| ident.name));
        match page.next_page_token {
            Some(next) => {
                assert!(seen.insert(next.clone()), "server repeated a page token");
                token = Some(next);
            }
            None => break,
        }
    }
    assert!(pages >= 2, "a page size of two must page three tables");
    listed.sort();
    assert_eq!(listed, names);
    let mut upstream_listed = upstream
        .list_tables(
            crate::catalog::CatalogNamespaceName::new(namespace.join(".")),
            novarocks_spi::connector::ConnectorListingBound::V1,
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    upstream_listed.sort();
    assert_eq!(upstream_listed, listed);

    // Drop never requests a purge: the dropped table's metadata file survives.
    let dropped_location = created["t0"].metadata_location.clone().unwrap();
    rest.drop_table("drop-t0", "t0").await;
    assert!(!endpoints.table_exists(&java.ident("t0")).await.unwrap());
    assert_eq!(
        rest.load_status(&java.ident("t0")).await,
        StatusCode::NOT_FOUND
    );
    let io = crate::fs_io::build_file_io_for_location(&dropped_location, java.binding.clone());
    assert!(
        io.exists(&dropped_location).await.unwrap(),
        "drop without purgeRequested keeps the table files"
    );
    let mut remaining = upstream
        .list_tables(
            crate::catalog::CatalogNamespaceName::new(namespace.join(".")),
            novarocks_spi::connector::ConnectorListingBound::V1,
        )
        .await
        .unwrap()
        .into_iter()
        .collect::<Vec<_>>();
    remaining.sort();
    assert_eq!(remaining, ["t1", "t2"]);

    rest.drop_table("drop-t1", "t1").await;
    rest.drop_table("drop-t2", "t2").await;
    let empty = endpoints
        .list_tables_page(namespace, None, 2)
        .await
        .unwrap();
    assert!(empty.identifiers.is_empty());
    assert!(
        upstream
            .list_tables(
                crate::catalog::CatalogNamespaceName::new(namespace.join(".")),
                novarocks_spi::connector::ConnectorListingBound::V1
            )
            .await
            .unwrap()
            .is_empty()
    );
    rest.drop_namespace().await;
    assert!(
        !upstream
            .namespace_exists(crate::catalog::CatalogNamespaceName::new(
                namespace.join(".")
            ))
            .await
            .unwrap()
    );
    // D13: a drop's 404 may follow a publication, so it is unknown.
    let missing = endpoints.drop_namespace(namespace).await;
    rest.record("drop-namespace-again", &missing);
    assert!(matches!(
        &missing.evidence,
        RestEvidence::Unknown { diagnosis }
            if diagnosis.reason == UnknownReason::PostPublishPossible
                && diagnosis.status == Some(404)
    ));
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13a_java_rest_table_commit_shapes() {
    let rest = NovaRocksRest::new("NR_IRU13A", "commits").await;
    let java = &rest.java;
    let endpoints = &rest.endpoints;
    rest.create_namespace().await;
    let created = rest.create_table("create", "shapes").await;
    let ident = java.ident("shapes");
    let uuid = created.metadata.uuid();

    // Snapshot-producing: an append adding S, attributed by S and the UUID.
    let operation = java.operation(&created.metadata);
    let file = java.data(&operation, &created.metadata, &[1, 2]).await;
    let append = intent(
        java,
        "shapes",
        &created.metadata,
        &operation,
        RequestShape::SnapshotProducing,
        vec![
            AddedContent::new_logical_data(file, created.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoRead,
    );
    let attempt = operation.begin_attempt().unwrap();
    let (expected, request) = freeze_append(&created, &append, &attempt).await;
    assert!(
        request
            .requirements()
            .contains(&TableRequirement::UuidMatch { uuid })
    );
    let snapshot = request.ref_snapshot_after("main").unwrap();
    let outcome = rest
        .publish(
            "append",
            request,
            CatalogOperation::Append,
            operation.request_context().clone(),
            None,
        )
        .await;
    let receipt = committed_catalog(&outcome);
    assert_eq!(receipt.snapshot_id, Some(snapshot));
    assert_eq!(
        receipt.table_uuid.as_deref(),
        Some(uuid.to_string().as_str())
    );
    let appended = java.load(&ident).await.unwrap();
    assert_eq!(
        semantic_facts(&appended.metadata),
        semantic_facts(&expected),
        "Java is the independent metadata oracle"
    );
    assert_eq!(
        appended.metadata_location.as_deref(),
        receipt.metadata_location.as_deref()
    );
    assert_eq!(
        appended
            .metadata
            .snapshot_for_ref("main")
            .map(|snapshot| snapshot.snapshot_id()),
        Some(snapshot)
    );

    // Metadata-only: attributed by the frozen UUID alone.
    let (request, context) = freeze_metadata_fixture(
        java,
        ident.clone(),
        &appended,
        PreparedChange {
            requirements: vec![],
            updates: vec![set_property("iru13a.metadata-only", "applied")],
        },
    );
    let outcome = rest
        .publish(
            "metadata-only",
            request,
            CatalogOperation::AlterProperties,
            context,
            None,
        )
        .await;
    let receipt = committed_catalog(&outcome);
    assert_eq!(receipt.snapshot_id, None);
    assert_eq!(
        receipt.table_uuid.as_deref(),
        Some(uuid.to_string().as_str())
    );
    let updated = java.load(&ident).await.unwrap();
    assert_eq!(
        updated
            .metadata
            .properties()
            .get("iru13a.metadata-only")
            .map(String::as_str),
        Some("applied")
    );
    assert_eq!(updated.metadata.current_snapshot_id(), Some(snapshot));
    assert_eq!(
        updated.metadata_location.as_deref(),
        receipt.metadata_location.as_deref()
    );

    // A failing requirement: produced before the append, when main was absent.
    let commit = CommitTableRequest {
        identifier: Some(ident.clone()),
        requirements: vec![
            TableRequirement::UuidMatch { uuid },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".into(),
                snapshot_id: None,
            },
        ],
        updates: vec![set_property("iru13a.stale", "applied")],
    };
    let expect = AttributionExpectation::table_commit(uuid, None).unwrap();
    let outcome = endpoints.commit_table(commit, &expect, false).await;
    rest.record("stale-requirement", &outcome);
    assert_eq!(
        outcome.evidence,
        RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::Conflict
        }
    );
    let unchanged = java.load(&ident).await.unwrap();
    assert_eq!(unchanged.metadata_location, updated.metadata_location);
    assert!(!unchanged.metadata.properties().contains_key("iru13a.stale"));

    // RefRetention is a distinct frozen shape: Java checks both the reference
    // and its full policy, then its explicit CAS removal. No snapshot is added.
    let retained_ref = SnapshotReference::new(
        snapshot,
        SnapshotRetention::branch(Some(2), Some(3_600_000), Some(7_200_000)),
    );
    let operation = java.operation(&unchanged.metadata);
    let context = operation.request_context().clone();
    let request = super::freeze_existing_change(
        ident.clone(),
        "main",
        &unchanged.metadata,
        unchanged.metadata_location.as_deref().unwrap(),
        RequestShape::RefRetention,
        AttemptToken::new(operation.token(), 0),
        PreparedChange {
            requirements: vec![],
            updates: vec![TableUpdate::SetSnapshotRef {
                ref_name: "retained".into(),
                reference: retained_ref.clone(),
            }],
        },
        &context,
    )
    .unwrap();
    assert!(
        request
            .requirements()
            .contains(&TableRequirement::UuidMatch { uuid })
    );
    assert!(
        request
            .requirements()
            .contains(&TableRequirement::RefSnapshotIdMatch {
                r#ref: "retained".into(),
                snapshot_id: None,
            })
    );
    let outcome = rest
        .publish(
            "ref-retention-set",
            request,
            CatalogOperation::CreateBranch,
            context,
            None,
        )
        .await;
    let receipt = committed_catalog(&outcome);
    let retained = java.load(&ident).await.unwrap();
    assert_eq!(
        retained.metadata.refs().get("retained"),
        Some(&retained_ref)
    );
    assert_eq!(retained.metadata.current_snapshot_id(), Some(snapshot));
    assert_eq!(
        retained.metadata.snapshots().count(),
        unchanged.metadata.snapshots().count()
    );
    assert_eq!(
        receipt.table_uuid.as_deref(),
        Some(uuid.to_string().as_str())
    );
    assert_eq!(
        receipt.metadata_location.as_deref(),
        retained.metadata_location.as_deref()
    );
    let operation = java.operation(&retained.metadata);
    let context = operation.request_context().clone();
    let request = super::freeze_existing_change(
        ident.clone(),
        "main",
        &retained.metadata,
        retained.metadata_location.as_deref().unwrap(),
        RequestShape::RefRetention,
        AttemptToken::new(operation.token(), 0),
        PreparedChange {
            requirements: vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: "retained".into(),
                snapshot_id: Some(snapshot),
            }],
            updates: vec![TableUpdate::RemoveSnapshotRef {
                ref_name: "retained".into(),
            }],
        },
        &context,
    )
    .unwrap();
    let outcome = rest
        .publish(
            "ref-retention-remove",
            request,
            CatalogOperation::DropBranch,
            context,
            None,
        )
        .await;
    let receipt = committed_catalog(&outcome);
    let removed = java.load(&ident).await.unwrap();
    assert!(!removed.metadata.refs().contains_key("retained"));
    assert_eq!(removed.metadata.current_snapshot_id(), Some(snapshot));
    assert_eq!(
        semantic_facts(&removed.metadata),
        semantic_facts(&unchanged.metadata)
    );
    assert_eq!(
        receipt.metadata_location.as_deref(),
        removed.metadata_location.as_deref()
    );

    let (request, context) = freeze_metadata_fixture(
        java,
        ident.clone(),
        &removed,
        PreparedChange {
            requirements: vec![],
            updates: vec![set_property("finalization-probe", "committed")],
        },
    );
    let outcome = rest
        .publish_with_failed_finalization(request, context)
        .await;
    let CatalogOutcome::KnownCommitted {
        effect: ExternalMutationEffect::Applied,
        receipt,
        finalization: novarocks_spi::connector::ExternalMutationFinalization::Failed(failure),
    } = outcome
    else {
        panic!("Actual Java commit must remain Applied with separate failed finalization");
    };
    assert_eq!(failure.kind(), ConnectorMutationFailureKind::Internal);
    assert_eq!(
        failure.message(),
        "Injected finalization failure after actual Java publication"
    );
    let finalized = java.load(&ident).await.unwrap();
    assert_eq!(finalized.metadata.uuid(), uuid);
    assert_eq!(finalized.metadata.current_snapshot_id(), Some(snapshot));
    assert_eq!(
        finalized
            .metadata
            .properties()
            .get("finalization-probe")
            .map(String::as_str),
        Some("committed")
    );
    assert_eq!(
        finalized.metadata_location.as_deref(),
        receipt.metadata_location.as_deref()
    );

    rest.drop_table("drop", "shapes").await;
    rest.drop_namespace().await;
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13a_java_rest_stage_create_and_ctas_final_commit() {
    let rest = NovaRocksRest::new("NR_IRU13A", "ctas").await;
    let java = &rest.java;
    let endpoints = &rest.endpoints;
    rest.create_namespace().await;
    let ident = java.ident("ctas");

    let outcome = endpoints
        .stage_create(rest.namespace(), create_request(rest.namespace(), "ctas"))
        .await;
    rest.record("stage", &outcome);
    let receipt = applied(&outcome, MutationEndpoint::StageCreate);
    assert_eq!(receipt.metadata_location, None);
    let staged: LoadTableResult =
        serde_json::from_slice(outcome.document.as_deref().expect("applied document")).unwrap();
    assert!(staged.metadata_location.is_none());
    let staged_uuid = staged.metadata.uuid();
    assert_eq!(receipt.uuid, Some(staged_uuid));
    // A stage publishes nothing.
    assert!(!endpoints.table_exists(&ident).await.unwrap());

    // The connector derives the initialization updates, as the stage flow does.
    let initialization_updates = super::staged_create_updates(&staged.metadata).unwrap();
    let operation = java.operation(&staged.metadata);
    let file = java.data(&operation, &staged.metadata, &[7, 8]).await;
    let create = intent(
        java,
        "ctas",
        &staged.metadata,
        &operation,
        RequestShape::Create,
        vec![
            AddedContent::new_logical_data(file, staged.metadata.default_partition_spec_id())
                .unwrap(),
        ],
        vec![],
        Dependency::NoRead,
    );
    let attempt = operation.begin_attempt().unwrap();
    let mut engine = StagingEngine::begin(
        StagingBase::Create {
            staged: StagedCreateIdentity::new(operation.token(), Arc::new(staged.metadata.clone())),
            initialization_updates,
        },
        &create,
        &attempt,
    )
    .unwrap();
    engine.stage(&FastAppendPreparer).await.unwrap();
    let marker = (
        "novarocks.ctas.operation-id".to_string(),
        operation.token().path_component(),
    );
    engine
        .stage_change(PreparedChange {
            requirements: vec![],
            updates: vec![set_property(&marker.0, &marker.1)],
        })
        .unwrap();
    let request = engine.freeze(&added_references(&create)).unwrap();
    assert_eq!(request.requirements(), &[TableRequirement::NotExist]);
    let snapshot = request.ref_snapshot_after("main").unwrap();

    let expect = AttributionExpectation::ctas_final_commit(
        staged_uuid,
        Some(ExpectedSnapshot::new(snapshot, Some("main".into()))),
        vec![marker.clone()],
    )
    .unwrap();
    // An explicit protocol-level duplicate is retained solely for the
    // assert-create negative oracle; the production journal never replays it.
    let duplicate = CommitTableRequest {
        identifier: Some(request.identifier().clone()),
        requirements: request.requirements().to_vec(),
        updates: request.updates().to_vec(),
    };
    let outcome = rest
        .publish(
            "ctas-final-commit",
            request,
            CatalogOperation::CreateTable(crate::catalog::CatalogCreateIntent::CreateTableAsSelect),
            operation.request_context().clone(),
            Some(crate::catalog::StagedPublicationExpectation {
                staged_table_uuid: staged_uuid,
                snapshot_id: Some(snapshot),
                properties: vec![marker.clone()],
            }),
        )
        .await;
    let receipt = committed_catalog(&outcome);
    assert_eq!(receipt.snapshot_id, Some(snapshot));
    assert_eq!(
        receipt.table_uuid.as_deref(),
        Some(staged_uuid.to_string().as_str())
    );
    let published = java.load(&ident).await.unwrap();
    assert_eq!(published.metadata.uuid(), staged_uuid);
    assert_eq!(published.metadata.current_snapshot_id(), Some(snapshot));
    assert_eq!(
        published.metadata.properties().get(&marker.0),
        Some(&marker.1)
    );
    assert_eq!(
        published
            .metadata
            .current_snapshot()
            .unwrap()
            .added_rows_count(),
        Some(2)
    );
    assert_eq!(
        published.metadata_location.as_deref(),
        receipt.metadata_location.as_deref()
    );
    assert_eq!(
        published
            .metadata
            .snapshot_for_ref("main")
            .map(|s| s.snapshot_id()),
        Some(snapshot)
    );

    // The same final commit again: `assert-create` now fails on the server.
    let outcome = endpoints.commit_table(duplicate, &expect, false).await;
    rest.record("ctas-final-commit-again", &outcome);
    assert_eq!(
        outcome.evidence,
        RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::AlreadyExists
        }
    );
    let unchanged = java.load(&ident).await.unwrap();
    assert_eq!(unchanged.metadata_location, published.metadata_location);

    rest.drop_table("drop", "ctas").await;
    rest.drop_namespace().await;
}

fn view_definition(namespace: &NamespaceIdent, schema: Schema, sql: &str) -> ViewDefinition {
    ViewDefinition {
        schema,
        sql: sql.to_owned(),
        summary: HashMap::from([("engine-name".to_owned(), "novarocks".to_owned())]),
        default_catalog: Some("iru13a".to_owned()),
        default_namespace: namespace.clone(),
        timestamp_ms: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
    }
}

/// `metadata` as if it belonged to another view incarnation.
fn with_view_uuid(metadata: &ViewMetadata, uuid: Uuid) -> ViewMetadata {
    let mut json = serde_json::to_value(metadata).unwrap();
    json["view-uuid"] = json!(uuid);
    serde_json::from_value(json).unwrap()
}

/// The replacement's columns differ from the original's, so it adds a schema.
fn replaced_view_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "name",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .unwrap()
}

fn current_sql(metadata: &ViewMetadata) -> String {
    match metadata.current_version().representations().iter().next() {
        Some(ViewRepresentation::Sql(sql)) => sql.sql.clone(),
        None => panic!("view version has no representation"),
    }
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13a_java_rest_view_create_replace_drop() {
    let rest = NovaRocksRest::new("NR_IRU13A", "views").await;
    let endpoints = &rest.endpoints;
    rest.create_namespace().await;
    let view = rest.java.ident("v");
    let creation = || {
        ViewCreate::validate(
            "v".to_owned(),
            view_definition(rest.namespace(), schema(), "select 1 as id"),
            HashMap::new(),
        )
        .unwrap()
    };

    assert!(!endpoints.view_exists(&view).await.unwrap());
    let outcome = endpoints.create_view(rest.namespace(), creation()).await;
    rest.record("create-view", &outcome);
    let receipt = applied(&outcome, MutationEndpoint::CreateView);
    let view_uuid = receipt.uuid.expect("view UUID");
    assert!(receipt.metadata_location.is_some());
    assert!(endpoints.view_exists(&view).await.unwrap());
    let current = endpoints.load_view(&view).await.unwrap();
    assert_eq!(current.uuid(), view_uuid);
    assert_original_storage(current.location());
    assert_eq!(current_sql(&current), "select 1 as id");

    let again = endpoints.create_view(rest.namespace(), creation()).await;
    rest.record("create-view-again", &again);
    assert_eq!(
        again.evidence,
        RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::AlreadyExists
        }
    );

    // Replace exactly as the view statement does: a new current version,
    // with the frozen view UUID as requirement and attribution.
    let replace = |current: &ViewMetadata| {
        ValidatedViewReplace::validate(
            view.clone(),
            current,
            view_definition(
                rest.namespace(),
                replaced_view_schema(),
                "select 2 as id, 'x' as name",
            ),
            HashMap::new(),
        )
        .unwrap()
    };
    let outcome = endpoints
        .replace_view(
            &replace(&current),
            &AttributionExpectation::replace_view(view_uuid),
        )
        .await;
    rest.record("replace-view", &outcome);
    let receipt = applied(&outcome, MutationEndpoint::ReplaceView);
    assert_eq!(receipt.uuid, Some(view_uuid));
    let replaced = endpoints.load_view(&view).await.unwrap();
    assert_eq!(replaced.uuid(), view_uuid);
    assert_eq!(current_sql(&replaced), "select 2 as id, 'x' as name");
    assert_eq!(
        replaced.current_version().schema_id(),
        replaced
            .schemas_iter()
            .map(|schema| schema.schema_id())
            .max()
            .unwrap()
    );

    // A replacement produced from another view incarnation is refused.
    let other = Uuid::now_v7();
    let outcome = endpoints
        .replace_view(
            &replace(&with_view_uuid(&replaced, other)),
            &AttributionExpectation::replace_view(other),
        )
        .await;
    rest.record("replace-view-other-incarnation", &outcome);
    assert_eq!(
        outcome.evidence,
        RestEvidence::Rejected {
            status: 409,
            kind: ConnectorMutationFailureKind::Conflict
        }
    );
    let unchanged = endpoints.load_view(&view).await.unwrap();
    assert_eq!(
        unchanged.current_version_id(),
        replaced.current_version_id()
    );

    let outcome = endpoints.drop_view(&view).await;
    rest.record("drop-view", &outcome);
    applied(&outcome, MutationEndpoint::DropView);
    let missing = endpoints.load_view(&view).await.unwrap_err();
    assert_eq!(missing.kind(), ConnectorErrorKind::NotFound);
    assert!(!endpoints.view_exists(&view).await.unwrap());
    // D13: a drop's 404 may follow a publication, so it is unknown.
    let outcome = endpoints.drop_view(&view).await;
    rest.record("drop-view-again", &outcome);
    assert!(matches!(
        &outcome.evidence,
        RestEvidence::Unknown { diagnosis }
            if diagnosis.reason == UnknownReason::PostPublishPossible
                && diagnosis.status == Some(404)
    ));
    rest.drop_namespace().await;
}

/// The partition evolution the `ALTER TABLE ... ADD PARTITION COLUMN` path
/// builds from `base`: identity on `id`, guarded by the base default spec.
fn partition_evolution(base: &TableMetadata) -> PreparedChange {
    let base_spec_id = base.default_partition_spec_id();
    let spec = UnboundPartitionSpecBuilder::new()
        .add_partition_fields([UnboundPartitionField {
            source_id: 1,
            field_id: None,
            name: "id_part".to_owned(),
            transform: Transform::Identity,
        }])
        .unwrap()
        .build();
    let build = TableMetadataBuilder::new_from_metadata(base.clone(), None)
        .add_default_partition_spec(spec)
        .unwrap()
        .build()
        .unwrap();
    let new_spec_id = build.metadata.default_partition_spec_id();
    assert_ne!(new_spec_id, base_spec_id);
    let mut updates = build.changes;
    if let Some(TableUpdate::AddSpec { spec }) = updates.first_mut() {
        *spec = build
            .metadata
            .partition_spec_by_id(new_spec_id)
            .unwrap()
            .as_ref()
            .clone()
            .into_unbound();
    }
    PreparedChange {
        requirements: vec![TableRequirement::DefaultSpecIdMatch {
            default_spec_id: base_spec_id,
        }],
        updates,
    }
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13a_java_rest_recreated_table_rejects_stale_partition_evolution() {
    let rest = NovaRocksRest::new("NR_IRU13A", "recreated").await;
    let java = &rest.java;
    rest.create_namespace().await;
    let ident = java.ident("recreated");
    rest.create_table("create-old", "recreated").await;
    let old = java.load(&ident).await.unwrap();
    rest.drop_table("drop-old", "recreated").await;
    rest.create_table("create-new", "recreated").await;
    let before = java.load(&ident).await.unwrap();
    assert_ne!(before.metadata.uuid(), old.metadata.uuid());
    assert_eq!(
        before.metadata.current_schema(),
        old.metadata.current_schema()
    );
    assert_eq!(
        before.metadata.default_partition_spec_id(),
        old.metadata.default_partition_spec_id()
    );
    assert_eq!(
        before.metadata.default_partition_spec(),
        old.metadata.default_partition_spec()
    );

    // Produced from the dropped incarnation: every requirement but the frozen
    // UUID also holds on the recreated table.
    let (request, context) = freeze_metadata_fixture(
        java,
        ident.clone(),
        &old,
        partition_evolution(&old.metadata),
    );
    let outcome = rest
        .publish(
            "stale-partition-evolution",
            request,
            CatalogOperation::AlterPartitionSpec,
            context,
            None,
        )
        .await;
    let CatalogOutcome::KnownUncommitted { failure } = outcome else {
        panic!("the stale incarnation must be rejected");
    };
    assert_eq!(failure.kind(), ConnectorMutationFailureKind::Conflict);
    // Preserve the exact wire-status oracle separately from the typed verdict.
    assert_eq!(
        failure.message(),
        "REST catalog rejected the request with HTTP 409"
    );
    let after = java.load(&ident).await.unwrap();
    assert_eq!(after.metadata_location, before.metadata_location);
    assert_eq!(
        semantic_facts(&after.metadata),
        semantic_facts(&before.metadata)
    );

    // Control: the same evolution produced from the recreated table applies.
    let (request, context) = freeze_metadata_fixture(
        java,
        ident.clone(),
        &before,
        partition_evolution(&before.metadata),
    );
    let outcome = rest
        .publish(
            "current-partition-evolution",
            request,
            CatalogOperation::AlterPartitionSpec,
            context,
            None,
        )
        .await;
    let proof = committed_catalog(&outcome);
    assert_eq!(
        proof.table_uuid.as_deref(),
        Some(before.metadata.uuid().to_string().as_str())
    );
    let evolved = java.load(&ident).await.unwrap();
    assert_ne!(
        evolved.metadata.default_partition_spec_id(),
        before.metadata.default_partition_spec_id()
    );
    java.record(
        "current-partition-evolution-result",
        &json!({"default-spec-id": evolved.metadata.default_partition_spec_id()}),
    );

    rest.drop_table("drop-new", "recreated").await;
    rest.drop_namespace().await;
}

// IRU-13b: NovaRocks owns the REST request vocabulary that core patches used
// to provide. Java fixture read-back is the independent oracle.

const IRU13B: &str = "NR_IRU13B";

/// `id` (1), `a` (2), `b` (3): the highest field is the last one.
fn three_column_schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            )),
            Arc::new(NestedField::optional(
                2,
                "a",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                3,
                "b",
                Type::Primitive(PrimitiveType::String),
            )),
        ])
        .build()
        .unwrap()
}

fn three_column_request(
    namespace: &NamespaceIdent,
    name: &str,
    format_version: u8,
) -> CreateTableRequest {
    CreateTableRequest {
        name: name.to_owned(),
        location: scratch_location(namespace, Some(name)),
        schema: three_column_schema(),
        partition_spec: Some(PartitionSpec::unpartition_spec().into_unbound()),
        write_order: Some(SortOrder::unsorted_order()),
        stage_create: None,
        properties: HashMap::from([("format-version".into(), format_version.to_string())]),
    }
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13b_java_rest_ctas_publishes_staged_metadata_for_every_format_version() {
    let rest = NovaRocksRest::new(IRU13B, "ctas").await;
    let java = &rest.java;
    let endpoints = &rest.endpoints;
    rest.create_namespace().await;

    for (format_version, number) in [
        (FormatVersion::V1, 1),
        (FormatVersion::V2, 2),
        (FormatVersion::V3, 3),
    ] {
        let name = format!("ctas_v{number}");
        let ident = java.ident(&name);
        let outcome = endpoints
            .stage_create(
                rest.namespace(),
                three_column_request(rest.namespace(), &name, number),
            )
            .await;
        rest.record(&format!("{name}-stage"), &outcome);
        applied(&outcome, MutationEndpoint::StageCreate);
        let staged: LoadTableResult =
            serde_json::from_slice(outcome.document.as_deref().expect("applied document")).unwrap();
        let staged = staged.metadata;
        assert_eq!(staged.format_version(), format_version);

        let initialization_updates = super::staged_create_updates(&staged).unwrap();
        let operation = java.operation(&staged);
        let file = java.data(&operation, &staged, &[7, 8]).await;
        let create = intent(
            java,
            &name,
            &staged,
            &operation,
            RequestShape::Create,
            vec![AddedContent::new_logical_data(file, staged.default_partition_spec_id()).unwrap()],
            vec![],
            Dependency::NoRead,
        );
        let attempt = operation.begin_attempt().unwrap();
        let mut engine = StagingEngine::begin(
            StagingBase::Create {
                staged: StagedCreateIdentity::new(operation.token(), Arc::new(staged.clone())),
                initialization_updates,
            },
            &create,
            &attempt,
        )
        .unwrap();
        engine.stage(&FastAppendPreparer).await.unwrap();
        let marker = (
            "novarocks.ctas.operation-id".to_string(),
            operation.token().path_component(),
        );
        engine
            .stage_change(PreparedChange {
                requirements: vec![],
                updates: vec![set_property(&marker.0, &marker.1)],
            })
            .unwrap();
        let request = engine.freeze(&added_references(&create)).unwrap();
        let body = request.to_rest_json().unwrap();
        java.record(&format!("{name}-request"), &body);
        let add_schema = body["updates"]
            .as_array()
            .unwrap()
            .iter()
            .find(|update| update["action"] == "add-schema")
            .expect("initialization adds the staged schema");
        assert!(add_schema.get("last-column-id").is_none(), "{add_schema}");
        assert!(
            body["updates"]
                .as_array()
                .unwrap()
                .iter()
                .any(|update| update["action"] == "upgrade-format-version"
                    && update["format-version"] == number),
            "every format version is selected explicitly"
        );

        let snapshot = request.ref_snapshot_after("main").unwrap();

        let outcome = rest
            .publish(
                &format!("{name}-final-commit"),
                request,
                CatalogOperation::CreateTable(
                    crate::catalog::CatalogCreateIntent::CreateTableAsSelect,
                ),
                operation.request_context().clone(),
                Some(crate::catalog::StagedPublicationExpectation {
                    staged_table_uuid: staged.uuid(),
                    snapshot_id: Some(snapshot),
                    properties: vec![marker.clone()],
                }),
            )
            .await;
        let receipt = committed_catalog(&outcome);
        assert_eq!(receipt.snapshot_id, Some(snapshot));

        let published = java.load(&ident).await.unwrap().metadata;
        assert_eq!(published.format_version(), format_version);
        assert_eq!(published.uuid(), staged.uuid());
        assert_eq!(published.location(), staged.location());
        assert_eq!(published.current_schema(), staged.current_schema());
        assert_eq!(
            published.default_partition_spec(),
            staged.default_partition_spec()
        );
        assert_eq!(published.default_sort_order(), staged.default_sort_order());
        let mut properties = published.properties().clone();
        assert_eq!(properties.remove(&marker.0), Some(marker.1.clone()));
        assert_eq!(&properties, staged.properties());
        assert_eq!(
            published.last_column_id(),
            staged.current_schema().highest_field_id()
        );
        assert_eq!(published.last_column_id(), 3);
        assert_eq!(published.current_snapshot_id(), Some(snapshot));
        java.record(
            &format!("{name}-published"),
            &json!({"format-version": number, "last-column-id": published.last_column_id()}),
        );
        rest.drop_table(&format!("{name}-drop"), &name).await;
    }
    rest.drop_namespace().await;
}

fn root_column(name: &str) -> novarocks_spi::connector::ConnectorColumnPath {
    novarocks_spi::connector::ConnectorColumnPath {
        segments: vec![name.into()],
    }
}

fn drop_column(name: &str) -> novarocks_spi::connector::ConnectorSchemaChange {
    novarocks_spi::connector::ConnectorSchemaChange::DropColumn {
        path: root_column(name),
    }
}

impl NovaRocksRest {
    /// Apply one schema change exactly as `ALTER TABLE` builds it, and return
    /// the metadata Java holds afterwards.
    async fn alter(
        &self,
        label: &str,
        name: &str,
        change: novarocks_spi::connector::ConnectorSchemaChange,
    ) -> TableMetadata {
        let ident = self.java.ident(name);
        let base = self.java.load(&ident).await.unwrap();
        let change = crate::catalog_control::catalog_mutation::prepare_schema_updates(
            &base.metadata,
            &change,
        )
        .unwrap();
        assert!(!change.updates.is_empty(), "the change is not a no-op");
        let (request, context) = freeze_metadata_fixture(&self.java, ident.clone(), &base, change);
        let outcome = self
            .publish(label, request, CatalogOperation::AlterSchema, context, None)
            .await;
        committed_catalog(&outcome);
        let altered = self.java.load(&ident).await.unwrap().metadata;
        self.java.record(
            &format!("{label}-result"),
            &json!({
                "last-column-id": altered.last_column_id(),
                "schema": altered.current_schema().as_ref(),
            }),
        );
        altered
    }

    async fn create_three_column_table(&self, name: &str) {
        let outcome = self
            .endpoints
            .create_table(
                self.namespace(),
                three_column_request(self.namespace(), name, 2),
            )
            .await;
        self.record(&format!("{name}-create"), &outcome);
        applied(&outcome, MutationEndpoint::CreateTable);
    }
}

fn field_ids(metadata: &TableMetadata) -> Vec<(String, i32)> {
    metadata
        .current_schema()
        .as_struct()
        .fields()
        .iter()
        .map(|field| (field.name.clone(), field.id))
        .collect()
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13b_java_rest_schema_changes_keep_the_column_high_water_mark() {
    let rest = NovaRocksRest::new(IRU13B, "schema").await;
    rest.create_namespace().await;

    // Dropping only the highest field: the high-water mark stays.
    rest.create_three_column_table("drop_highest").await;
    let dropped = rest
        .alter("drop-highest", "drop_highest", drop_column("b"))
        .await;
    assert_eq!(field_ids(&dropped), vec![("id".into(), 1), ("a".into(), 2)]);
    assert_eq!(dropped.last_column_id(), 3);

    // Adding after the drop: the new field is numbered past the old mark.
    rest.create_three_column_table("drop_then_add").await;
    rest.alter("drop-then-add-drop", "drop_then_add", drop_column("b"))
        .await;
    let added = rest
        .alter(
            "drop-then-add-add",
            "drop_then_add",
            novarocks_spi::connector::ConnectorSchemaChange::AddColumn {
                parent: novarocks_spi::connector::ConnectorColumnPath { segments: vec![] },
                column: novarocks_spi::connector::ConnectorColumnDefinition {
                    name: "c".into(),
                    data_type: novarocks_spi::connector::ConnectorDataType::String,
                    nullable: true,
                    aggregation: None,
                    default: None,
                },
                position: novarocks_spi::connector::ConnectorColumnPosition::Default,
            },
        )
        .await;
    assert_eq!(
        field_ids(&added),
        vec![("id".into(), 1), ("a".into(), 2), ("c".into(), 4)]
    );
    assert_eq!(added.last_column_id(), 4);

    // Renaming after the drop adds no field; the mark is unchanged.
    rest.create_three_column_table("drop_then_rename").await;
    rest.alter(
        "drop-then-rename-drop",
        "drop_then_rename",
        drop_column("b"),
    )
    .await;
    let renamed = rest
        .alter(
            "drop-then-rename-rename",
            "drop_then_rename",
            novarocks_spi::connector::ConnectorSchemaChange::RenameColumn {
                path: root_column("a"),
                to: "a2".into(),
            },
        )
        .await;
    assert_eq!(
        field_ids(&renamed),
        vec![("id".into(), 1), ("a2".into(), 2)]
    );
    assert_eq!(renamed.last_column_id(), 3);

    for name in ["drop_highest", "drop_then_add", "drop_then_rename"] {
        rest.drop_table(&format!("{name}-drop"), name).await;
    }
    rest.drop_namespace().await;
}

impl Java {
    fn view_url(&self, name: &str) -> Url {
        self.endpoint(&["namespaces", &self.namespace.to_url_string(), "views", name])
    }

    /// One raw view request by another writer; returns its status.
    async fn raw_view_request(&self, label: &str, url: Url, body: &Value) -> StatusCode {
        self.record(&format!("{label}-request"), body);
        let status = self
            .client
            .post(url)
            .json(body)
            .send()
            .await
            .unwrap()
            .status();
        self.record(
            &format!("{label}-status"),
            &json!({"status": status.as_u16()}),
        );
        status
    }
}

/// A raw view version as another writer would send it.
fn raw_view_version(version_id: i32, schema_id: i32, sql: &str, dialects: &[&str]) -> Value {
    json!({
        "version-id": version_id,
        "schema-id": schema_id,
        "timestamp-ms": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        "summary": {"engine-name": "other-writer"},
        "representations": dialects
            .iter()
            .map(|dialect| json!({"type": "sql", "sql": sql, "dialect": dialect}))
            .collect::<Vec<_>>(),
        "default-namespace": [],
    })
}

fn version_sql(version: &ViewVersion) -> String {
    match version.representations().iter().next() {
        Some(ViewRepresentation::Sql(sql)) => sql.sql.clone(),
        None => panic!("view version has no representation"),
    }
}

fn operation_id(version: &ViewVersion) -> Option<String> {
    version
        .summary()
        .get(crate::catalog::view_request::OPERATION_ID_SUMMARY_KEY)
        .cloned()
}

impl NovaRocksRest {
    async fn create_novarocks_view(&self, name: &str, schema: Schema, sql: &str) -> ViewMetadata {
        let creation = ViewCreate::validate(
            name.to_owned(),
            view_definition(self.namespace(), schema, sql),
            HashMap::new(),
        )
        .unwrap();
        let outcome = self.endpoints.create_view(self.namespace(), creation).await;
        self.record(&format!("{name}-create"), &outcome);
        applied(&outcome, MutationEndpoint::CreateView);
        let loaded = self
            .endpoints
            .load_view(&self.java.ident(name))
            .await
            .unwrap();
        assert_original_storage(loaded.location());
        loaded
    }

    fn validate_replace(
        &self,
        current: &ViewMetadata,
        name: &str,
        schema: Schema,
        sql: &str,
        properties: &[(&str, &str)],
    ) -> std::result::Result<ValidatedViewReplace, String> {
        ValidatedViewReplace::validate(
            self.java.ident(name),
            current,
            view_definition(self.namespace(), schema, sql),
            properties
                .iter()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
        )
    }

    /// Dispatch one validated replacement and check that Java now holds it
    /// as a new current version carrying this replacement's identity.
    async fn replace_and_check(
        &self,
        label: &str,
        replace: &ValidatedViewReplace,
        before: &ViewMetadata,
    ) -> ViewMetadata {
        let outcome = self
            .endpoints
            .replace_view(
                replace,
                &AttributionExpectation::replace_view(replace.uuid()),
            )
            .await;
        self.record(label, &outcome);
        let receipt = applied(&outcome, MutationEndpoint::ReplaceView);
        assert_eq!(receipt.uuid, Some(before.uuid()));
        let after = self.endpoints.load_view(replace.ident()).await.unwrap();
        let current = after.current_version();
        assert!(
            before
                .versions()
                .all(|version| version.version_id() != current.version_id()),
            "{label}: a replacement adds a version"
        );
        assert_eq!(
            operation_id(current),
            replace
                .version()
                .summary
                .get(crate::catalog::view_request::OPERATION_ID_SUMMARY_KEY)
                .cloned(),
            "{label}: the current version is the one this replacement added"
        );
        assert_eq!(
            version_sql(current),
            replace.version().representations[0].sql,
            "{label}"
        );
        self.java.record(
            &format!("{label}-result"),
            &json!({
                "current-version-id": after.current_version_id(),
                "versions": after.versions().map(|version| version.version_id()).collect::<Vec<_>>(),
                "schema-id": current.schema_id(),
            }),
        );
        after
    }
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13b_java_rest_view_replacements_always_add_the_selected_version() {
    let rest = NovaRocksRest::new(IRU13B, "replace").await;
    rest.create_namespace().await;
    let first = rest
        .create_novarocks_view("v", schema(), "select 1 as id")
        .await;

    // Unchanged columns, new SQL: the existing schema is referenced by ID.
    let replace = rest
        .validate_replace(&first, "v", schema(), "select 2 as id", &[])
        .unwrap();
    assert!(matches!(
        replace.schema(),
        crate::catalog::view_request::ViewSchema::Existing(_)
    ));
    let unchanged_columns = rest
        .replace_and_check("unchanged-columns", &replace, &first)
        .await;
    assert_eq!(unchanged_columns.schemas_iter().len(), 1);

    // Changed columns: the commit adds the schema and the version uses it.
    let replace = rest
        .validate_replace(
            &unchanged_columns,
            "v",
            replaced_view_schema(),
            "select 3 as id, 'x' as name",
            &[],
        )
        .unwrap();
    let changed_columns = rest
        .replace_and_check("changed-columns", &replace, &unchanged_columns)
        .await;
    assert_eq!(changed_columns.schemas_iter().len(), 2);
    assert_eq!(
        changed_columns
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<_>>(),
        vec!["id".to_string(), "name".to_string()]
    );

    // The same definition as the current version.
    let replace = rest
        .validate_replace(
            &changed_columns,
            "v",
            replaced_view_schema(),
            "select 3 as id, 'x' as name",
            &[],
        )
        .unwrap();
    let same_as_current = rest
        .replace_and_check("same-as-current", &replace, &changed_columns)
        .await;

    // The same definition as a historical version.
    let replace = rest
        .validate_replace(&same_as_current, "v", schema(), "select 2 as id", &[])
        .unwrap();
    let same_as_history = rest
        .replace_and_check("same-as-history", &replace, &same_as_current)
        .await;
    assert_eq!(same_as_history.versions().len(), 5);

    let outcome = rest.endpoints.drop_view(&rest.java.ident("v")).await;
    rest.record("drop", &outcome);
    applied(&outcome, MutationEndpoint::DropView);
    rest.drop_namespace().await;
}

/// Spec 1.4: after A loads and validates its replacement, another writer
/// expires history and reuses version ID 2 for another definition. A still
/// selects its own definition, because it never names a version by number.
#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13b_java_rest_view_replacement_survives_version_id_reuse() {
    let rest = NovaRocksRest::new(IRU13B, "reuse").await;
    let java = &rest.java;
    rest.create_namespace().await;
    // {1: X, 2: Y, 3: W}, current W.
    let x = rest
        .create_novarocks_view("v", schema(), "select 1 as id")
        .await;
    let replace = rest
        .validate_replace(&x, "v", schema(), "select 2 as id", &[])
        .unwrap();
    let y = rest.replace_and_check("publish-y", &replace, &x).await;
    let replace = rest
        .validate_replace(&y, "v", schema(), "select 3 as id", &[])
        .unwrap();
    let w = rest.replace_and_check("publish-w", &replace, &y).await;
    assert_eq!(
        w.versions()
            .map(|v| v.version_id())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([1, 2, 3])
    );

    // A loads and validates "replace with Y", but does not dispatch yet.
    let a = rest
        .validate_replace(&w, "v", schema(), "select 2 as id", &[])
        .unwrap();

    // B: back to X and keep one version. Shrinking retention while W is
    // current would keep only W, so both happen in one commit.
    let identifier = json!({"namespace": java.namespace.as_ref(), "name": "v"});
    let uuid = json!({"type": "assert-view-uuid", "uuid": w.uuid()});
    let status = java
        .raw_view_request(
            "b-back-to-x",
            java.view_url("v"),
            &json!({
                "identifier": identifier,
                "requirements": [uuid],
                "updates": [
                    {"action": "set-properties", "updates": {"version.history.num-entries": "1"}},
                    {"action": "set-current-view-version", "view-version-id": 1},
                ],
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    // B: keep two versions and publish Z, requesting ID 2: the server takes
    // the larger of the request and its maximum ID plus one, so ID 2 is
    // reused.
    let schema_id = w.current_version().schema_id();
    let status = java
        .raw_view_request(
            "b-publish-z",
            java.view_url("v"),
            &json!({
                "identifier": identifier,
                "requirements": [uuid],
                "updates": [
                    {"action": "set-properties", "updates": {"version.history.num-entries": "2"}},
                    {"action": "add-view-version", "view-version": raw_view_version(2, schema_id, "select 26 as id", &["starrocks"])},
                    {"action": "set-current-view-version", "view-version-id": -1},
                ],
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let reused = rest.endpoints.load_view(&java.ident("v")).await.unwrap();
    let held = reused
        .versions()
        .map(|version| (version.version_id(), version_sql(version)))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        held,
        BTreeMap::from([
            (1, "select 1 as id".to_string()),
            (2, "select 26 as id".to_string())
        ]),
        "ID 2 now denotes Z"
    );
    // B: publish W again.
    let status = java
        .raw_view_request(
            "b-publish-w",
            java.view_url("v"),
            &json!({
                "identifier": identifier,
                "requirements": [uuid],
                "updates": [
                    {"action": "add-view-version", "view-version": raw_view_version(3, schema_id, "select 3 as id", &["starrocks"])},
                    {"action": "set-current-view-version", "view-version-id": -1},
                ],
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let before = rest.endpoints.load_view(&java.ident("v")).await.unwrap();
    assert_eq!(
        before
            .versions()
            .map(|version| (version.version_id(), version_sql(version)))
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([
            (2, "select 26 as id".to_string()),
            (3, "select 3 as id".to_string())
        ])
    );

    // A dispatches: the current definition is the one A asked for.
    let after = rest.replace_and_check("a-dispatch", &a, &before).await;
    assert_eq!(version_sql(after.current_version()), "select 2 as id");

    let outcome = rest.endpoints.drop_view(&java.ident("v")).await;
    rest.record("drop", &outcome);
    applied(&outcome, MutationEndpoint::DropView);
    rest.drop_namespace().await;
}

#[tokio::test]
#[ignore = "requires pinned Java REST and MinIO fixture publication"]
async fn iru13b_java_rest_view_replacement_keeps_dialects_unless_allowed() {
    let rest = NovaRocksRest::new(IRU13B, "dialects").await;
    let java = &rest.java;
    rest.create_namespace().await;
    // Another engine's view, defined for both StarRocks and Spark.
    let status = java
        .raw_view_request(
            "create-two-dialects",
            java.endpoint(&["namespaces", &java.namespace.to_url_string(), "views"]),
            &json!({
                "name": "v",
                "schema": serde_json::to_value(schema()).unwrap(),
                "view-version": raw_view_version(1, schema().schema_id(), "select 1 as id", &["starrocks", "spark"]),
                "properties": {},
            }),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let current = rest.endpoints.load_view(&java.ident("v")).await.unwrap();
    assert_original_storage(current.location());

    // Without permission the replacement is refused before dispatch.
    let error = rest
        .validate_replace(&current, "v", schema(), "select 2 as id", &[])
        .expect_err("dropping the Spark definition must be refused locally");
    assert!(error.contains("spark"), "{error}");
    java.record("refused-locally", &json!({"error": error}));
    let unchanged = rest.endpoints.load_view(&java.ident("v")).await.unwrap();
    assert_eq!(unchanged.current_version_id(), current.current_version_id());
    assert_eq!(unchanged.versions().len(), 1);

    // The statement allows the drop: accepted.
    let replace = rest
        .validate_replace(
            &current,
            "v",
            schema(),
            "select 2 as id",
            &[("replace.drop-dialect.allowed", "true")],
        )
        .unwrap();
    let replaced = rest
        .replace_and_check("allowed-drop", &replace, &current)
        .await;
    assert_eq!(replaced.current_version().representations().len(), 1);
    assert_eq!(
        replaced
            .properties()
            .get("replace.drop-dialect.allowed")
            .map(String::as_str),
        Some("true")
    );

    let outcome = rest.endpoints.drop_view(&java.ident("v")).await;
    rest.record("drop", &outcome);
    applied(&outcome, MutationEndpoint::DropView);
    rest.drop_namespace().await;
}

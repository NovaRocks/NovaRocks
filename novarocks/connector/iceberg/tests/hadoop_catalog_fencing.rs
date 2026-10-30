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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use novarocks_connector_iceberg::access_binding::{IcebergCredentialResolver, IcebergReadBinding};
use novarocks_connector_iceberg::catalog_config::{
    IcebergCatalogConfiguration, parse_catalog_configuration_with_object_store_binding,
};
use novarocks_connector_iceberg::catalog_runtime::build_hadoop_catalog;
use novarocks_connector_iceberg::connector_factory::IcebergConnectorFactory;
use novarocks_connector_iceberg::iceberg::spec::TableMetadata;
use novarocks_connector_iceberg::iceberg::{Catalog, TableIdent};
use novarocks_connector_iceberg::novarocks_fs::{
    FsAccessResolver, FsAccessResources, ObjectStoreConfig, ObjectStoreProviderPool,
    ObjectStoreSecretMaterial, SecretValue, TokioFileIoRuntime, TokioFileTaskSpawner,
};
use novarocks_connector_iceberg::resources::IcebergMetadataResources;
use novarocks_spi::connector::{
    CatalogCredentialBinding, CatalogCredentialMode, CatalogCredentialPurpose, CatalogHandle,
    CatalogProperties, CatalogProperty, CatalogVersion, ConnectorCatalogMutationOperation,
    ConnectorCatalogMutationRequest, ConnectorColumnDefinition, ConnectorControlCreation,
    ConnectorControlFactory, ConnectorControlFactoryRequest, ConnectorDataType, ConnectorError,
    ConnectorErrorKind, ConnectorInstanceId, ConnectorMutationFailureKind,
    ConnectorMutationOperationId, ConnectorNamespaceIdentity, ConnectorProviderBindingKey,
    ConnectorProviderId, ConnectorRequestContext, ConnectorStopOwner, ConnectorTableIdentity,
    CreatePolicy, CredentialConsumerRole, ExternalMutationEffect, ExternalMutationFinalization,
    ExternalMutationOutcome, StaticCredentialReference,
};
use novarocks_test_support::ManagedCommand;
use sha2::{Digest, Sha256};

#[path = "support/hadoop_fencing_run.rs"]
mod hadoop_fencing_run;
use hadoop_fencing_run::{RunOwner, child_work_deadline};

const CHILD_ENV: &str = "NR_HADOOP_FENCE_CHILD";
const WAREHOUSE_ENV: &str = "NR_HADOOP_FENCE_WAREHOUSE";
const NAMESPACE_ENV: &str = "NR_HADOOP_FENCE_NAMESPACE";
const TABLE_ENV: &str = "NR_HADOOP_FENCE_TABLE";
const POLICY_ENV: &str = "NR_HADOOP_FENCE_POLICY";
const BARRIER_ENV: &str = "NR_HADOOP_FENCE_BARRIER";
const CHILD_INDEX_ENV: &str = "NR_HADOOP_FENCE_CHILD_INDEX";
const RESULT_PREFIX: &str = "HADOOP_FENCE_RESULT ";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Policy {
    Strict,
    NoOp,
}

impl Policy {
    fn as_str(self) -> &'static str {
        match self {
            Self::Strict => "strict",
            Self::NoOp => "noop",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "strict" => Self::Strict,
            "noop" => Self::NoOp,
            other => panic!("unknown child create policy: {other}"),
        }
    }
}

#[derive(Debug)]
struct ChildResult {
    pid: u32,
    outcome: String,
    uuid: String,
    location: String,
    digest: String,
}

#[derive(Debug)]
struct PrefixAudit {
    uuid: String,
    location: String,
    digest: String,
    entries: Vec<String>,
}

#[test]
fn local_strict_two_process_fencing() -> anyhow::Result<()> {
    let warehouse = tempfile::tempdir().expect("local warehouse");
    RunOwner::local()?.run(|owner| {
        run_race(
            owner,
            &warehouse.path().to_string_lossy(),
            "local_strict",
            Policy::Strict,
        )
    })
}

#[test]
fn local_noop_two_process_fencing() -> anyhow::Result<()> {
    let warehouse = tempfile::tempdir().expect("local warehouse");
    RunOwner::local()?.run(|owner| {
        run_race(
            owner,
            &warehouse.path().to_string_lossy(),
            "local_noop",
            Policy::NoOp,
        )
    })
}

#[test]
#[ignore = "requires the shared MinIO fixture"]
fn minio_strict_and_noop_two_process_fencing() -> anyhow::Result<()> {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../..");
    RunOwner::minio(&repo)?.run(|owner| {
        let warehouse = owner.warehouse()?.to_owned();
        run_race(owner, &warehouse, "minio_strict", Policy::Strict)?;
        run_race(owner, &warehouse, "minio_noop", Policy::NoOp)
    })
}

/// The parent process invokes this exact libtest entry in two independent
/// processes. It requires the parent's inputs and is excluded from ordinary
/// test invocations.
#[test]
#[ignore = "internal helper invoked exactly by the two-process fencing tests"]
fn hadoop_fencing_child_process_entry() {
    assert_eq!(required_env(CHILD_ENV), "1", "invalid child process marker");

    let warehouse = required_env(WAREHOUSE_ENV);
    let namespace = required_env(NAMESPACE_ENV);
    let table_name = required_env(TABLE_ENV);
    let policy = Policy::parse(&required_env(POLICY_ENV));
    let barrier = PathBuf::from(required_env(BARRIER_ENV));
    let child_index = required_env(CHILD_INDEX_ENV);

    let config = catalog_configuration(&warehouse, object_store_config(&warehouse));
    let runtime = tokio::runtime::Runtime::new().expect("child runtime");
    // Each child constructs its own production control generation. The
    // mutation capability freezes and publishes through that owner's journal.
    let creation = child_control(&config, &runtime);
    let binding = creation.binding();
    let mutation = binding.mutation().expect("catalog mutation capability");
    let target = ConnectorProviderBindingKey {
        instance_id: binding.descriptor().instance_id.clone(),
        incarnation: binding.incarnation(),
    };
    let namespace_result = mutation
        .execute(ConnectorCatalogMutationRequest {
            operation_id: ConnectorMutationOperationId::new(),
            target: target.clone(),
            operation: ConnectorCatalogMutationOperation::CreateNamespace {
                namespace: ConnectorNamespaceIdentity {
                    instance_id: target.instance_id.clone(),
                    namespace: namespace.clone().into(),
                },
                policy: CreatePolicy::NoOpIfExists,
            },
            context: request_context(),
        })
        .expect("execute namespace publication");
    assert!(
        matches!(
            namespace_result,
            ExternalMutationOutcome::KnownCommitted {
                finalization: ExternalMutationFinalization::Complete,
                ..
            }
        ),
        "namespace must exist before the table race: {namespace_result:?}"
    );

    std::fs::write(barrier.join(format!("ready-{child_index}")), b"ready\n")
        .expect("publish child barrier readiness");
    wait_for_both_children(&barrier);

    let result = mutation
        .execute(ConnectorCatalogMutationRequest {
            operation_id: ConnectorMutationOperationId::new(),
            target: target.clone(),
            operation: ConnectorCatalogMutationOperation::CreateTable {
                table: ConnectorTableIdentity {
                    instance_id: target.instance_id.clone(),
                    namespace: namespace.clone().into(),
                    table: table_name.clone().into(),
                },
                columns: vec![ConnectorColumnDefinition {
                    name: "id".into(),
                    data_type: ConnectorDataType::BigInt,
                    nullable: false,
                    aggregation: None,
                    default: None,
                }],
                key: None,
                partitioning: Vec::new(),
                properties: vec![("format-version".into(), "2".into())],
                policy: match policy {
                    Policy::Strict => CreatePolicy::FailIfExists,
                    Policy::NoOp => CreatePolicy::NoOpIfExists,
                },
            },
            context: request_context(),
        })
        .expect("execute prepared create publication");
    let (outcome, applied_version) = match result {
        ExternalMutationOutcome::KnownCommitted {
            cleanup: _,
            effect,
            receipt,
            finalization,
        } => {
            assert_eq!(finalization, ExternalMutationFinalization::Complete);
            assert_eq!(receipt.descriptor().instance_id, target.instance_id);
            assert_eq!(receipt.incarnation(), target.incarnation);
            match effect {
                ExternalMutationEffect::Applied => (
                    "applied",
                    Some(
                        serde_json::from_slice::<serde_json::Value>(
                            receipt.provider_version().expect("applied create identity"),
                        )
                        .expect("decode applied create identity"),
                    ),
                ),
                ExternalMutationEffect::NoOp => {
                    assert_eq!(policy, Policy::NoOp);
                    assert!(receipt.provider_version().is_none());
                    ("noop", None)
                }
            }
        }
        ExternalMutationOutcome::KnownUncommitted { failure, cleanup } => {
            assert_eq!(policy, Policy::Strict);
            assert_eq!(failure.kind(), ConnectorMutationFailureKind::AlreadyExists);
            assert_eq!(cleanup, ExternalMutationFinalization::Complete);
            ("conflict", None)
        }
        other => panic!("child create returned an unexpected outcome: {other:?}"),
    };
    // Read the actual canonical object independently from the publication
    // receipt, so a fabricated receipt cannot establish the storage oracle.
    let location = format!(
        "{}/{namespace}/{table_name}/metadata/v1.metadata.json",
        config.warehouse_uri.trim_end_matches('/')
    );
    let deadline = child_work_deadline().expect("parent work deadline");
    let read_binding = catalog_binding(&config).for_request(read_request_context(deadline));
    let access =
        novarocks_connector_iceberg::fs_io::resolve_access_for_location(&location, &read_binding)
            .expect("resolve child v1 access");
    let bytes = runtime
        .block_on(async {
            tokio::time::timeout_at(deadline.into(), async {
                access
                    .operator()
                    .read(access.single_relative_path().expect("v1 path"))
                    .await
            })
            .await
        })
        .expect("child metadata read exceeded original work deadline")
        .expect("read authoritative v1 after child create")
        .to_bytes();
    let metadata: TableMetadata = serde_json::from_slice(&bytes).expect("decode child v1");
    let table_uuid = metadata.uuid().to_string();
    let digest = hex_digest(&bytes);
    if let Some(version) = applied_version {
        assert_eq!(version["table_uuid"].as_str(), Some(table_uuid.as_str()));
        assert_eq!(
            version["metadata_location"].as_str(),
            Some(location.as_str())
        );
        assert_eq!(version["metadata_digest"].as_str(), Some(digest.as_str()));
    }
    println!(
        "{RESULT_PREFIX}pid={} outcome={outcome} uuid={table_uuid} location={location} digest={digest}",
        std::process::id(),
    );
}

fn run_race(
    owner: &mut RunOwner,
    warehouse: &str,
    table_name: &str,
    policy: Policy,
) -> anyhow::Result<()> {
    owner.check_work()?;
    let namespace = "fencing";
    // Configuration parsing canonicalizes local paths to file:// URIs. Use
    // that same identity for child inputs, receipts and the independent audit.
    let config = catalog_configuration(warehouse, owner.object_store_config());
    let warehouse = config.warehouse_uri.as_str();
    let metadata_location = format!(
        "{}/{}/{}/metadata/v1.metadata.json",
        warehouse.trim_end_matches('/'),
        namespace,
        table_name
    );
    let binding = catalog_binding(&config).for_request(read_request_context(owner.work_deadline()));
    let access = novarocks_connector_iceberg::fs_io::resolve_access_for_location(
        &metadata_location,
        &binding,
    )
    .expect("resolve v1 metadata access");
    assert!(
        access.supports_conditional_create(),
        "storage must advertise native create-if-absent for {metadata_location}"
    );

    let barrier = tempfile::tempdir().expect("process barrier");
    let commands = std::array::from_fn(|index| {
        let mut command =
            ManagedCommand::new(std::env::current_exe().expect("current test executable"));
        command
            .args([
                "--ignored",
                "--exact",
                "hadoop_fencing_child_process_entry",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_ENV, "1")
            .env(WAREHOUSE_ENV, warehouse)
            .env(NAMESPACE_ENV, namespace)
            .env(TABLE_ENV, table_name)
            .env(POLICY_ENV, policy.as_str())
            .env(BARRIER_ENV, barrier.path())
            .env(CHILD_INDEX_ENV, index.to_string());
        command
    });
    let captures = owner.capture_pair(commands)?;
    let results = captures
        .iter()
        .map(|capture| parse_child_result(&capture.output))
        .collect::<Vec<_>>();
    assert_ne!(results[0].pid, results[1].pid, "children must be processes");
    assert_eq!(
        results
            .iter()
            .filter(|result| result.outcome == "applied")
            .count(),
        1,
        "exactly one process must own v1: {results:?}"
    );
    let expected_other = match policy {
        Policy::Strict => "conflict",
        Policy::NoOp => "noop",
    };
    assert_eq!(
        results
            .iter()
            .filter(|result| result.outcome == expected_other)
            .count(),
        1,
        "loser outcome must follow create policy: {results:?}"
    );
    assert_eq!(results[0].uuid, results[1].uuid);
    assert_eq!(results[0].location, results[1].location);
    assert_eq!(results[0].digest, results[1].digest);

    owner.check_work()?;
    let audit = audit_prefix(&config, namespace, table_name, owner.work_deadline());
    assert_eq!(audit.uuid, results[0].uuid);
    assert_eq!(audit.location, results[0].location);
    assert_eq!(audit.digest, results[0].digest);
    assert!(
        audit.location.ends_with(&format!(
            "/{namespace}/{table_name}/metadata/v1.metadata.json"
        )),
        "canonical metadata location must retain the Hadoop v1 suffix: {}",
        audit.location
    );
    assert_eq!(
        audit.entries,
        vec!["v1.metadata.json", "version-hint.text"],
        "the successful create must leave one v1 and its hint"
    );
    println!(
        "HADOOP_FENCE_AUDIT policy={} warehouse={} pids={},{} uuid={} location={} digest={} entries={}",
        policy.as_str(),
        warehouse,
        results[0].pid,
        results[1].pid,
        audit.uuid,
        audit.location,
        audit.digest,
        audit.entries.join(",")
    );
    owner.check_work()?;
    for capture in captures {
        capture.release_diagnostics()?;
    }
    Ok(())
}

fn audit_prefix(
    config: &IcebergCatalogConfiguration,
    namespace: &str,
    table_name: &str,
    deadline: Instant,
) -> PrefixAudit {
    let runtime = tokio::runtime::Runtime::new().expect("audit runtime");
    let catalog = build_hadoop_catalog(
        config,
        catalog_binding(config).for_request(read_request_context(deadline)),
        novarocks_connector_iceberg::iceberg::Runtime::new(&runtime),
    )
    .expect("build audit Hadoop catalog");
    let ident = TableIdent::from_strs([namespace, table_name]).expect("audit table identity");
    let table = runtime
        .block_on(async {
            tokio::time::timeout_at(deadline.into(), catalog.load_table(&ident)).await
        })
        .expect("audit table load exceeded original work deadline")
        .expect("load authoritative table for audit");
    let location = table
        .metadata_location()
        .expect("audit table metadata location")
        .to_string();
    let binding = catalog_binding(config).for_request(read_request_context(deadline));
    let access =
        novarocks_connector_iceberg::fs_io::resolve_access_for_location(&location, &binding)
            .expect("resolve audit metadata access");
    let relative_path = access
        .single_relative_path()
        .expect("single audit metadata path")
        .to_string();
    let metadata_dir = Path::new(&relative_path)
        .parent()
        .expect("metadata parent")
        .to_string_lossy()
        .replace('\\', "/");
    let metadata_dir = format!("{}/", metadata_dir.trim_end_matches('/'));
    let operator = access.operator();
    let bytes = runtime
        .block_on(async {
            tokio::time::timeout_at(deadline.into(), async {
                operator.read(&relative_path).await
            })
            .await
        })
        .expect("audit metadata read exceeded original work deadline")
        .expect("read authoritative v1")
        .to_bytes();
    let decoded: TableMetadata =
        serde_json::from_slice(&bytes).expect("decode authoritative v1 metadata");
    let mut entries = runtime
        .block_on(async {
            tokio::time::timeout_at(deadline.into(), async {
                operator.list(&metadata_dir).await
            })
            .await
        })
        .expect("audit listing exceeded original work deadline")
        .expect("list metadata prefix")
        .into_iter()
        .filter(|entry| entry.metadata().mode().is_file())
        .map(|entry| {
            entry
                .path()
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .expect("metadata entry name")
                .to_string()
        })
        .collect::<Vec<_>>();
    entries.sort();
    PrefixAudit {
        uuid: decoded.uuid().to_string(),
        location,
        digest: hex_digest(&bytes),
        entries,
    }
}

fn parse_child_result(output: &Output) -> ChildResult {
    if !output.status.success() {
        panic!(
            "catalog child failed with {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let stdout = String::from_utf8(output.stdout.clone()).expect("child stdout is UTF-8");
    let line = stdout
        .lines()
        .find_map(|line| line.find(RESULT_PREFIX).map(|offset| &line[offset..]))
        .unwrap_or_else(|| panic!("child emitted no machine-readable result:\n{stdout}"));
    let fields = line[RESULT_PREFIX.len()..]
        .split_ascii_whitespace()
        .map(|field| {
            field
                .split_once('=')
                .unwrap_or_else(|| panic!("invalid child result field: {field}"))
        })
        .collect::<BTreeMap<_, _>>();
    ChildResult {
        pid: fields["pid"].parse().expect("child pid"),
        outcome: fields["outcome"].to_string(),
        uuid: fields["uuid"].to_string(),
        location: fields["location"].to_string(),
        digest: fields["digest"].to_string(),
    }
}

fn wait_for_both_children(barrier: &Path) {
    let deadline = child_work_deadline()
        .expect("parent work deadline")
        .min(Instant::now() + Duration::from_secs(30));
    while !(barrier.join("ready-0").exists() && barrier.join("ready-1").exists()) {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for both independent child processes"
        );
        // This sleep only avoids busy-polling the readiness barrier. Both
        // children pass the same barrier before the storage request, so it
        // cannot select or order the eventual v1 owner.
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn catalog_configuration(
    warehouse: &str,
    object_store_config: Option<ObjectStoreConfig>,
) -> IcebergCatalogConfiguration {
    let properties = vec![
        ("type".to_string(), "iceberg".to_string()),
        ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
        (
            "iceberg.catalog.warehouse".to_string(),
            warehouse.to_string(),
        ),
    ];
    parse_catalog_configuration_with_object_store_binding(
        "hadoop_fencing",
        &properties,
        object_store_config.as_ref(),
    )
    .expect("parse Hadoop catalog configuration")
}

fn catalog_binding(config: &IcebergCatalogConfiguration) -> IcebergReadBinding {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    let runtime = RUNTIME
        .get_or_init(|| tokio::runtime::Runtime::new().expect("test runtime"))
        .handle()
        .clone();
    IcebergReadBinding::new(
        config.object_store_config.clone(),
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.clone())),
        Arc::new(TokioFileTaskSpawner::new(runtime)),
    )
}

fn object_store_config(warehouse: &str) -> Option<ObjectStoreConfig> {
    if !warehouse.starts_with("s3://") {
        return None;
    }
    Some(ObjectStoreConfig {
        endpoint: required_env("AWS_S3_ENDPOINT"),
        access_key_id: SecretValue::new(required_env("AWS_S3_ACCESS_KEY_ID")),
        access_key_secret: SecretValue::new(required_env("AWS_S3_SECRET_ACCESS_KEY")),
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
    })
}

fn request_context() -> ConnectorRequestContext {
    read_request_context(
        child_work_deadline()
            .expect("parent work deadline")
            .min(Instant::now() + Duration::from_secs(30)),
    )
}

fn read_request_context(deadline: Instant) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        deadline,
        ConnectorStopOwner::new().view(),
        64 * 1024,
        256 * 1024,
    )
    .expect("mutation request context")
}

struct ChildCredentials(Option<ObjectStoreSecretMaterial>);

impl IcebergCredentialResolver for ChildCredentials {
    fn resolve_object_store_static(
        &self,
        _reference: &StaticCredentialReference,
    ) -> Result<ObjectStoreSecretMaterial, ConnectorError> {
        Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            "fencing child only owns frontend metadata credentials",
        ))
    }

    fn resolve_object_store_metadata_static(
        &self,
        reference: &StaticCredentialReference,
    ) -> Result<ObjectStoreSecretMaterial, ConnectorError> {
        assert_eq!(reference.name(), "fencing-metadata");
        assert_eq!(reference.generation(), "v1");
        self.0.clone().ok_or_else(|| {
            ConnectorError::new(
                ConnectorErrorKind::Unsupported,
                "local fencing child has no object-store credentials",
            )
        })
    }
}

fn child_control(
    config: &IcebergCatalogConfiguration,
    runtime: &tokio::runtime::Runtime,
) -> ConnectorControlCreation {
    let mut properties = vec![
        ("type".to_string(), "iceberg".to_string()),
        ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
        (
            "iceberg.catalog.warehouse".to_string(),
            config.warehouse_uri.clone(),
        ),
    ];
    let mut credentials = Vec::new();
    if let Some(object_store) = &config.object_store_config {
        properties.push(("aws.s3.endpoint".to_string(), object_store.endpoint.clone()));
        properties.push((
            "aws.s3.enable_path_style_access".to_string(),
            "true".to_string(),
        ));
        if let Some(region) = &object_store.region {
            properties.push(("aws.s3.region".to_string(), region.clone()));
        }
        credentials.push(
            CatalogCredentialBinding::try_new(
                CatalogCredentialPurpose::ObjectStoreMetadata,
                CredentialConsumerRole::Frontend,
                CatalogCredentialMode::Static(
                    StaticCredentialReference::try_new("fencing-metadata", "v1")
                        .expect("metadata credential reference"),
                ),
            )
            .expect("metadata credential binding"),
        );
    }
    let instance_id = ConnectorInstanceId::parse("hadoop_fencing").expect("instance identity");
    let provider_id = ConnectorProviderId::parse("iceberg").expect("provider identity");
    let catalog_properties = CatalogProperties::new(
        CatalogHandle::new(instance_id.clone(), CatalogVersion::from_bytes([1; 32])),
        provider_id.clone(),
        1,
        properties
            .iter()
            .map(|(key, value)| CatalogProperty::new(key, value).expect("non-secret property"))
            .collect(),
        credentials,
    )
    .expect("typed catalog properties");
    let resources = FsAccessResources::new(
        Arc::new(ObjectStoreProviderPool::new(Default::default()).expect("child provider pool")),
        FsAccessResolver::new(),
        Arc::new(TokioFileIoRuntime::new(runtime.handle().clone())),
        Arc::new(TokioFileTaskSpawner::new(runtime.handle().clone())),
    );
    let planning_binding = IcebergReadBinding::with_metadata_credential_resolver(
        resources,
        Arc::new(ChildCredentials(
            config
                .object_store_config
                .as_ref()
                .map(ObjectStoreConfig::secret_material),
        )),
    );
    let factory = IcebergConnectorFactory::new(IcebergMetadataResources::new(
        planning_binding,
        runtime.handle().clone(),
    ));
    let request = ConnectorControlFactoryRequest::try_new(provider_id, instance_id, properties)
        .and_then(|request| request.with_catalog_properties(catalog_properties))
        .expect("child control factory request");
    factory
        .create_control(request)
        .expect("build independent child control generation")
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("required environment variable {name} is unset"))
}

fn hex_digest(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

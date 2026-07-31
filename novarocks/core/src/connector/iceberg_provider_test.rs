// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License.  You may obtain a copy of the License
// at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arrow::datatypes::DataType;
use novarocks_spi::connector::{
    ConnectorBatchBudget, ConnectorBeginScanRequest, ConnectorCancellation,
    ConnectorCatalogMutationOperation, ConnectorCatalogMutationRequest, ConnectorColumnDefinition,
    ConnectorDataType, ConnectorExecutionBinding, ConnectorExecutionBindingKey,
    ConnectorExecutionInstaller, ConnectorInstanceId, ConnectorListTablesRequest,
    ConnectorNamespaceIdentity, ConnectorOpenReaderRequest, ConnectorReadSelector,
    ConnectorRequestContext, ConnectorSplitPlanningRequest, ConnectorTableIdentity,
    ConnectorTableRequest, ConnectorTableResolution, CreatePolicy, ExternalMutationEffect,
    ExternalMutationFinalization, ExternalMutationOutcome, StatisticsAccuracy,
    StatisticsCollectionRequest, StatisticsCollectionResult, StatisticsCoverage,
    StatisticsDataVersion, StatisticsEvidence, StatisticsEvidenceRevision, StatisticsMetric,
    StatisticsMetricRequest, StatisticsMetricState, StatisticsMetricValue, StatisticsProvenance,
    StatisticsPublishPreparationRequest, StatisticsPublishRequest, StatisticsReadRequest,
    StatisticsReader,
};

use super::iceberg::catalog::registry::{create_table, drop_table, insert_rows, load_table};
use super::iceberg::catalog::{IcebergCatalogRegistry, create_namespace};
use super::iceberg::provider::{
    IcebergConnectorInstaller, IcebergControlProvider, IcebergReadBinding,
};
use crate::query_execution::statistics::{StatisticsCollectionFinalizer, ThetaSketchPartial};
use crate::sql::{Literal, TableColumnDef};
use novarocks_catalog::schema::SqlType;

struct NotCancelled;

impl ConnectorCancellation for NotCancelled {
    fn is_cancelled(&self) -> bool {
        false
    }
}

fn context() -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        Arc::new(NotCancelled),
        1024 * 1024,
        4 * 1024 * 1024,
    )
    .expect("request context")
}

fn registry_with_table() -> (Arc<RwLock<IcebergCatalogRegistry>>, tempfile::TempDir) {
    let warehouse = tempfile::Builder::new()
        .prefix("novarocks_spi_iceberg_provider_")
        .tempdir()
        .expect("warehouse tempdir");
    let registry = Arc::new(RwLock::new(IcebergCatalogRegistry::default()));
    {
        let mut guard = registry.write().expect("iceberg catalog write lock");
        guard
            .create_catalog(
                "ice",
                &[
                    ("type".to_string(), "iceberg".to_string()),
                    ("iceberg.catalog.type".to_string(), "hadoop".to_string()),
                    (
                        "iceberg.catalog.warehouse".to_string(),
                        format!("file://{}", warehouse.path().join("warehouse").display()),
                    ),
                ],
            )
            .expect("create catalog");
    }
    let entry = registry
        .read()
        .expect("iceberg catalog read lock")
        .get("ice")
        .expect("catalog entry");
    create_namespace(&entry, "db").expect("create namespace");
    create_table(
        &entry,
        "db",
        "orders",
        &[TableColumnDef {
            name: "id".to_string(),
            data_type: SqlType::Int,
            nullable: false,
            aggregation: None,
            default: None,
        }],
        None,
        &[],
        &[],
    )
    .expect("create table");
    insert_rows(&entry, "db", "orders", &[vec![Literal::Int(7)]]).expect("insert table row");
    (registry, warehouse)
}

fn install_execution(
    control: &novarocks_spi::connector::ConnectorControlBinding,
) -> ConnectorExecutionBinding {
    let declaration = control
        .execution_declaration(&context())
        .expect("create secret-free declaration");
    IcebergConnectorInstaller::new(
        IcebergReadBinding::default_binding(None).expect("build read binding"),
    )
    .install(&declaration, &context())
    .expect("install read-only Iceberg execution binding")
}

#[test]
fn iceberg_distribution_installs_a_metadata_free_read_only_instance() {
    let (registry, _warehouse) = registry_with_table();
    let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
    let control = IcebergControlProvider::new_control(instance_id, registry)
        .expect("planning Iceberg control binding");

    let declaration = control
        .execution_declaration(&context())
        .expect("create secret-free declaration");
    let declaration_debug = format!("{declaration:?}");
    assert!(!declaration_debug.contains("warehouse"));
    assert!(!declaration_debug.contains("access_key"));

    let execution = install_execution(&control);
    assert_eq!(execution.key(), &declaration.binding_key());
    assert_eq!(
        execution.provider_id(),
        &declaration.descriptor().provider_id
    );
}

#[test]
fn iceberg_statistics_reader_requires_the_metadata_data_version_pin() {
    let (registry, _warehouse) = registry_with_table();
    let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
    let control = IcebergControlProvider::new_control(instance_id.clone(), registry)
        .expect("planning Iceberg control binding");
    let metadata = control
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context(),
        })
        .expect("load table metadata");
    let data_version = metadata
        .statistics_data_version
        .clone()
        .expect("Iceberg metadata supplies a separate data-version pin");
    let metrics = StatisticsMetricRequest::try_new(vec![
        StatisticsMetric::RowCount,
        StatisticsMetric::ThetaNdv {
            column: Arc::from("id"),
        },
    ])
    .expect("statistics metric request");
    let evidence = control
        .statistics()
        .expect("statistics capability")
        .read_statistics(StatisticsReadRequest {
            table: metadata.table.clone(),
            data_version: data_version.clone(),
            metrics: metrics.clone(),
            context: context(),
        })
        .expect("read pinned statistics");
    assert_eq!(evidence.data_version, data_version);
    assert_eq!(evidence.coverage, StatisticsCoverage::Subset);
    assert_eq!(evidence.accuracy, StatisticsAccuracy::Approximate);

    let collection = control
        .statistics()
        .expect("statistics capability")
        .collection()
        .expect("Iceberg supports collection preparation")
        .prepare_collection(StatisticsCollectionRequest {
            operation_id: Default::default(),
            table: metadata.table.clone(),
            data_version: data_version.clone(),
            metrics: metrics.clone(),
            context: context(),
        })
        .expect("prepare pinned collection");
    assert_eq!(collection.data_version, data_version);
    assert_eq!(collection.table(), &metadata.table);
    assert_eq!(collection.metrics.metrics(), metrics.metrics());
    assert_eq!(collection.scan_projection(), &[0]);
    assert_eq!(collection.scan_columns().len(), 1);
    assert_eq!(collection.scan_columns()[0].ordinal(), 0);
    assert_eq!(collection.scan_columns()[0].name(), "id");
    assert_eq!(collection.scan_columns()[0].data_type(), &DataType::Int32);
    assert!(!collection.scan_columns()[0].nullable());

    let wrong_version = StatisticsDataVersion::try_new(bytes::Bytes::from_static(b"wrong"))
        .expect("bounded test version");
    let error = control
        .statistics()
        .expect("statistics capability")
        .read_statistics(StatisticsReadRequest {
            table: metadata.table,
            data_version: wrong_version,
            metrics,
            context: context(),
        })
        .expect_err("statistics must reject a data-version drift");
    assert!(error.to_string().contains("data version"));
}

#[test]
fn iceberg_statistics_publish_uses_a_pinned_operation_specific_puffin() {
    let (registry, _warehouse) = registry_with_table();
    let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
    let control = IcebergControlProvider::new_control(instance_id.clone(), Arc::clone(&registry))
        .expect("planning Iceberg control binding");
    let metadata = control
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context(),
        })
        .expect("load table metadata");
    let data_version = metadata
        .statistics_data_version
        .clone()
        .expect("Iceberg metadata supplies data version");
    let metric = StatisticsMetric::ThetaNdv {
        column: Arc::from("id"),
    };
    let partial =
        ThetaSketchPartial::try_from_i64_values(12, [7_i64]).expect("build collection theta");
    let artifact = StatisticsCollectionFinalizer::default()
        .with_theta("id", partial)
        .try_visible_row_artifact(&data_version)
        .expect("encode Core artifact");
    let result = StatisticsCollectionResult::try_new(
        StatisticsEvidence {
            data_version: data_version.clone(),
            evidence_revision: StatisticsEvidenceRevision::try_new(bytes::Bytes::from_static(
                b"visible-row/v1",
            ))
            .expect("revision"),
            coverage: StatisticsCoverage::Full,
            accuracy: StatisticsAccuracy::Exact,
            interval: None,
            provenance: StatisticsProvenance::VisibleRows,
            metrics: BTreeMap::from([(
                metric,
                StatisticsMetricState::Available(StatisticsMetricValue::F64(1.0)),
            )]),
        },
        artifact,
    )
    .expect("collection result");
    let operation_id = Default::default();
    let outcome = control
        .statistics()
        .expect("statistics capability")
        .collection()
        .expect("statistics collection capability")
        .prepare_publish(StatisticsPublishPreparationRequest {
            operation_id,
            table: metadata.table.clone(),
            result: result.clone(),
            context: context(),
        })
        .expect("prepare pinned Iceberg statistics publication");
    let outcome = control
        .statistics()
        .expect("statistics capability")
        .collection()
        .expect("statistics collection capability")
        .publish_statistics(StatisticsPublishRequest {
            operation_id,
            table: metadata.table.clone(),
            result,
            context: context(),
            evidence: outcome,
        })
        .expect("publish pinned Iceberg statistics");
    let receipt = match outcome {
        ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::Applied,
            receipt,
            ..
        } => receipt,
        other => panic!("expected committed statistics publication, got {other:?}"),
    };
    let path = std::str::from_utf8(receipt.provider_payload()).expect("receipt path");
    assert!(path.contains(&uuid::Uuid::from_bytes(operation_id.to_bytes()).to_string()));
    let entry = registry
        .read()
        .expect("registry read")
        .get("ice")
        .expect("catalog entry");
    let loaded = load_table(&entry, "db", "orders").expect("reload table metadata");
    assert!(
        loaded
            .table
            .metadata()
            .statistics_iter()
            .any(|file| file.statistics_path == path)
    );
    let evidence = control
        .statistics()
        .expect("statistics capability")
        .read_statistics(StatisticsReadRequest {
            table: metadata.table,
            data_version,
            metrics: StatisticsMetricRequest::try_new(vec![StatisticsMetric::ThetaNdv {
                column: Arc::from("id"),
            }])
            .expect("metric request"),
            context: context(),
        })
        .expect("read published statistics evidence");
    assert!(
        evidence
            .evidence_revision
            .as_bytes()
            .windows(path.len())
            .any(|window| window == path.as_bytes())
    );
}

#[test]
fn iceberg_control_mutation_honors_create_policy_without_implicit_namespace_creation() {
    let (registry, _warehouse) = registry_with_table();
    let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
    let control = IcebergControlProvider::new_control(instance_id.clone(), registry)
        .expect("Iceberg control binding");
    let mutation = control.mutation().expect("mutation capability");
    let target = ConnectorExecutionBindingKey {
        instance_id: instance_id.clone(),
        incarnation: control.incarnation(),
    };
    let namespace = ConnectorNamespaceIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from("db"),
    };

    let no_op = mutation
        .execute(ConnectorCatalogMutationRequest {
            operation_id: Default::default(),
            target: target.clone(),
            operation: ConnectorCatalogMutationOperation::CreateNamespace {
                namespace,
                policy: CreatePolicy::NoOpIfExists,
            },
            context: context(),
        })
        .expect("mutation contract");
    assert!(matches!(
        no_op,
        ExternalMutationOutcome::KnownCommitted {
            effect: ExternalMutationEffect::NoOp,
            finalization: ExternalMutationFinalization::Complete,
            ..
        }
    ));

    let created = mutation
        .execute(ConnectorCatalogMutationRequest {
            operation_id: Default::default(),
            target,
            operation: ConnectorCatalogMutationOperation::CreateTable {
                table: ConnectorTableIdentity {
                    instance_id,
                    namespace: Arc::from("missing"),
                    table: Arc::from("must_not_create_namespace"),
                },
                columns: vec![ConnectorColumnDefinition {
                    name: Arc::from("id"),
                    data_type: ConnectorDataType::Int,
                    nullable: false,
                    aggregation: None,
                    default: None,
                }],
                key: None,
                partitioning: Vec::new(),
                properties: Vec::new(),
                policy: CreatePolicy::FailIfExists,
            },
            context: context(),
        })
        .expect("mutation contract");
    assert!(matches!(
        created,
        ExternalMutationOutcome::KnownUncommitted { .. }
    ));
}

#[test]
fn installed_iceberg_instance_reads_a_planned_split_without_catalog_metadata() {
    let (registry, _warehouse) = registry_with_table();
    let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
    let planning = IcebergControlProvider::new_control(instance_id.clone(), registry)
        .expect("planning Iceberg control binding");
    let resolved = planning
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id,
                namespace: Arc::from("db"),
                table: Arc::from("orders"),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context(),
        })
        .expect("load table");
    let scan = planning
        .planning()
        .begin_scan(
            &resolved.table,
            ConnectorBeginScanRequest {
                projection: vec![0],
                static_predicates: Vec::new(),
                selector: ConnectorReadSelector::Current,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
                    max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero bytes"),
                },
                context: context(),
            },
        )
        .expect("begin scan");
    let split = planning
        .planning()
        .plan_splits(
            &scan.handle,
            ConnectorSplitPlanningRequest {
                target_parallelism: NonZeroUsize::new(1).expect("parallelism"),
                max_split_bytes: None,
                context: context(),
            },
        )
        .expect("plan split")
        .splits
        .remove(0);
    let installed = install_execution(&planning);

    let mut reader = installed
        .read()
        .expect("read capability")
        .open_reader(
            &split,
            ConnectorOpenReaderRequest {
                expected_schema: resolved.schema,
                batch: ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
                    max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero bytes"),
                },
                context: context(),
            },
        )
        .expect("read-only instance opens planned split");
    assert_eq!(
        reader
            .next_batch()
            .expect("read batch")
            .expect("one batch")
            .num_rows(),
        1
    );
    reader.close().expect("close reader");
}

fn remove_snapshot_manifest_files(path: &std::path::Path) -> usize {
    let mut removed = 0;
    for entry in std::fs::read_dir(path).expect("read fixture directory") {
        let entry = entry.expect("read fixture entry");
        let path = entry.path();
        if path.is_dir() {
            removed += remove_snapshot_manifest_files(&path);
        } else if path
            .extension()
            .is_some_and(|extension| extension == "avro")
        {
            std::fs::remove_file(&path).expect("remove snapshot manifest");
            removed += 1;
        }
    }
    removed
}

fn replace_json_number(value: &mut serde_json::Value, from: i64, to: i64) {
    match value {
        serde_json::Value::Array(values) => {
            for value in values {
                replace_json_number(value, from, to);
            }
        }
        serde_json::Value::Object(values) => {
            for value in values.values_mut() {
                replace_json_number(value, from, to);
            }
        }
        serde_json::Value::Number(number) if number.as_i64() == Some(from) => {
            *number = serde_json::Number::from(to);
        }
        _ => {}
    }
}

fn force_current_snapshot_id(
    warehouse: &std::path::Path,
    namespace: &str,
    table: &str,
    from: i64,
    to: i64,
) {
    let metadata_dir = warehouse
        .join("warehouse")
        .join(namespace)
        .join(table)
        .join("metadata");
    let metadata_path = std::fs::read_dir(&metadata_dir)
        .expect("read recreated table metadata")
        .filter_map(|entry| {
            let path = entry.ok()?.path();
            let name = path.file_name()?.to_str()?;
            let version = name
                .strip_prefix('v')?
                .strip_suffix(".metadata.json")?
                .parse::<u64>()
                .ok()?;
            Some((version, path))
        })
        .max_by_key(|(version, _)| *version)
        .map(|(_, path)| path)
        .expect("latest recreated table metadata");
    let mut metadata: serde_json::Value = serde_json::from_slice(
        &std::fs::read(&metadata_path).expect("read recreated table metadata JSON"),
    )
    .expect("decode recreated table metadata JSON");
    replace_json_number(&mut metadata, from, to);
    std::fs::write(
        &metadata_path,
        serde_json::to_vec(&metadata).expect("encode recreated table metadata JSON"),
    )
    .expect("rewrite recreated table snapshot ID");
}

#[test]
fn iceberg_instance_resolves_metadata_and_plans_a_snapshot_split() {
    let (registry, warehouse) = registry_with_table();
    let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
    let instance = IcebergControlProvider::new_control(instance_id.clone(), registry)
        .expect("Iceberg control binding");
    let metadata = instance.metadata();
    let namespace = ConnectorNamespaceIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from("db"),
    };
    let table = ConnectorTableIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from("db"),
        table: Arc::from("orders"),
    };

    assert_eq!(
        metadata
            .list_tables(ConnectorListTablesRequest {
                namespace: namespace.clone(),
                context: context(),
            })
            .expect("list tables"),
        vec![table.clone()]
    );
    let resolved = metadata
        .load_table(ConnectorTableRequest {
            table,
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context(),
        })
        .expect("load table");
    assert_eq!(resolved.table.owner(), &instance_id);
    assert_eq!(resolved.schema.fields().len(), 1);

    let scan = instance
        .planning()
        .begin_scan(
            &resolved.table,
            ConnectorBeginScanRequest {
                projection: vec![0],
                static_predicates: Vec::new(),
                selector: ConnectorReadSelector::Current,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
                    max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero bytes"),
                },
                context: context(),
            },
        )
        .expect("begin scan");
    let splits = instance
        .planning()
        .plan_splits(
            &scan.handle,
            ConnectorSplitPlanningRequest {
                target_parallelism: NonZeroUsize::new(1).expect("parallelism"),
                max_split_bytes: None,
                context: context(),
            },
        )
        .expect("plan splits")
        .splits;
    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].owner(), &instance_id);
    assert!(splits[0].estimated_bytes().is_some_and(|bytes| bytes > 0));
    assert!(
        remove_snapshot_manifest_files(warehouse.path()) > 0,
        "fixture must contain snapshot manifests before reader opens"
    );
    let execution = install_execution(&instance);
    for _ in 0..2 {
        let mut reader = execution
            .read()
            .expect("read capability")
            .open_reader(
                &splits[0],
                ConnectorOpenReaderRequest {
                    expected_schema: Arc::clone(&resolved.schema),
                    batch: ConnectorBatchBudget {
                        max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
                        max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero bytes"),
                    },
                    context: context(),
                },
            )
            .expect("open reader without re-reading snapshot manifests");
        let batch = reader
            .next_batch()
            .expect("read batch")
            .expect("expected one batch");
        assert_eq!(batch.num_rows(), 1);
        assert!(reader.next_batch().expect("read EOS").is_none());
        reader.close().expect("close reader");
    }
}

#[test]
fn drop_recreate_with_same_snapshot_id_rejects_stale_split() {
    let (registry, warehouse) = registry_with_table();
    let entry = registry
        .read()
        .expect("iceberg catalog read lock")
        .get("ice")
        .expect("catalog entry");
    let instance_id = ConnectorInstanceId::parse("ice").expect("instance ID");
    let instance = IcebergControlProvider::new_control(instance_id.clone(), Arc::clone(&registry))
        .expect("Iceberg control binding");
    let table_identity = ConnectorTableIdentity {
        instance_id: instance_id.clone(),
        namespace: Arc::from("db"),
        table: Arc::from("orders"),
    };
    let resolved = instance
        .metadata()
        .load_table(ConnectorTableRequest {
            table: table_identity.clone(),
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context(),
        })
        .expect("load original table");
    let scan = instance
        .planning()
        .begin_scan(
            &resolved.table,
            ConnectorBeginScanRequest {
                projection: vec![0],
                static_predicates: Vec::new(),
                selector: ConnectorReadSelector::Current,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
                    max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero bytes"),
                },
                context: context(),
            },
        )
        .expect("begin original scan");
    let stale_split = instance
        .planning()
        .plan_splits(
            &scan.handle,
            ConnectorSplitPlanningRequest {
                target_parallelism: NonZeroUsize::new(1).expect("parallelism"),
                max_split_bytes: None,
                context: context(),
            },
        )
        .expect("plan original splits")
        .splits
        .remove(0);
    let original = load_table(&entry, "db", "orders").expect("load original table");
    let reused_snapshot_id = original
        .table
        .metadata()
        .current_snapshot_id()
        .expect("original snapshot");
    let original_uuid = original.table.metadata().uuid().to_string();

    drop_table(&entry, "db", "orders").expect("drop original table");
    create_table(
        &entry,
        "db",
        "orders",
        &[TableColumnDef {
            name: "id".to_string(),
            data_type: SqlType::Int,
            nullable: false,
            aggregation: None,
            default: None,
        }],
        None,
        &[],
        &[],
    )
    .expect("recreate table");
    insert_rows(&entry, "db", "orders", &[vec![Literal::Int(9)]])
        .expect("insert recreated table row");
    entry.invalidate_table_cache("db", "orders");
    let recreated = load_table(&entry, "db", "orders").expect("load recreated table");
    let generated_snapshot_id = recreated
        .table
        .metadata()
        .current_snapshot_id()
        .expect("recreated snapshot");
    let recreated_uuid = recreated.table.metadata().uuid().to_string();
    assert_ne!(
        recreated_uuid, original_uuid,
        "drop/recreate must produce a new table incarnation"
    );
    force_current_snapshot_id(
        warehouse.path(),
        "db",
        "orders",
        generated_snapshot_id,
        reused_snapshot_id,
    );
    entry.invalidate_table_cache("db", "orders");
    let recreated = load_table(&entry, "db", "orders").expect("reload recreated table");
    assert_eq!(
        recreated.table.metadata().current_snapshot_id(),
        Some(reused_snapshot_id),
        "fixture must reproduce numeric snapshot-ID reuse"
    );

    let execution = install_execution(&instance);
    let error = match execution.read().expect("read capability").open_reader(
        &stale_split,
        ConnectorOpenReaderRequest {
            expected_schema: Arc::clone(&resolved.schema),
            batch: ConnectorBatchBudget {
                max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
                max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero bytes"),
            },
            context: context(),
        },
    ) {
        Ok(_) => panic!("stale split from the dropped table incarnation must be rejected"),
        Err(error) => error,
    };
    assert_eq!(
        error.kind(),
        novarocks_spi::connector::ConnectorErrorKind::CorruptData
    );

    let current = instance
        .metadata()
        .load_table(ConnectorTableRequest {
            table: table_identity,
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: context(),
        })
        .expect("load current table");
    let current_scan = instance
        .planning()
        .begin_scan(
            &current.table,
            ConnectorBeginScanRequest {
                projection: vec![0],
                static_predicates: Vec::new(),
                selector: ConnectorReadSelector::Current,
                limit: None,
                batch: ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
                    max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero bytes"),
                },
                context: context(),
            },
        )
        .expect("begin current scan");
    let current_split = instance
        .planning()
        .plan_splits(
            &current_scan.handle,
            ConnectorSplitPlanningRequest {
                target_parallelism: NonZeroUsize::new(1).expect("parallelism"),
                max_split_bytes: None,
                context: context(),
            },
        )
        .expect("plan current splits")
        .splits
        .remove(0);
    let mut reader = execution
        .read()
        .expect("read capability")
        .open_reader(
            &current_split,
            ConnectorOpenReaderRequest {
                expected_schema: Arc::clone(&current.schema),
                batch: ConnectorBatchBudget {
                    max_rows: NonZeroUsize::new(1024).expect("nonzero rows"),
                    max_bytes: NonZeroUsize::new(1024 * 1024).expect("nonzero bytes"),
                },
                context: context(),
            },
        )
        .expect("open current split");
    assert_eq!(
        reader
            .next_batch()
            .expect("read current batch")
            .expect("current batch")
            .num_rows(),
        1
    );
}

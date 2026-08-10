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

use super::super::collect_scan_bindings;
use super::*;
use novarocks_spi::connector::{
    ConnectorControlResolver, ConnectorInstanceId, ConnectorReadSelector, ConnectorTableIdentity,
    ConnectorTableRequest, ConnectorTableResolution,
};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

struct RejectResolver;

impl ScanBindingResolver for RejectResolver {
    fn resolve_scan(
        &self,
        node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        panic!("ordinary Iceberg scan unexpectedly invoked resolver for node {node_id}")
    }
}

struct ErrorResolver;

impl ScanBindingResolver for ErrorResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        Err("boom".to_string())
    }
}

struct EmptyResolver;

impl ScanBindingResolver for EmptyResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        _scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        Ok(None)
    }
}

struct JoinRefreshDeltaResolver;

impl ScanBindingResolver for JoinRefreshDeltaResolver {
    fn resolve_scan(
        &self,
        _node_id: i32,
        scan: &PlanScanNode,
    ) -> Result<Option<ResolvedScanExecution>, String> {
        let ScanSource::Sql(source) = &scan.table.source;
        if !matches!(
            &source.kind,
            crate::sql::planner::table::SqlScanKind::Delta { .. }
        ) {
            return Err("join refresh fixture resolver received a non-delta scan".to_string());
        }
        Ok(Some(ResolvedScanExecution::IcebergDelta(
            crate::query_execution::preparation::scan::ResolvedIcebergDeltaScan {
                runtime_plan:
                    crate::query_execution::preparation::scan::IcebergDeltaScanRuntimePlan {
                        table_location: format!(
                            "s3://sqlx2-coalesce/{}/{}/{}",
                            source.table.catalog, source.table.namespace, source.table.table
                        ),
                        data_columns: Vec::new(),
                        change_files: Vec::new(),
                        delete_side: None,
                    },
            },
        )))
    }
}

fn collect_coalesce_scan_tables(
    node: &crate::sql::planner::distributed::DistributedNode,
    scans: &mut std::collections::BTreeMap<String, crate::sql::planner::table::TableDef>,
    frozen_snapshots: &mut std::collections::BTreeMap<String, std::collections::BTreeSet<i64>>,
    node_ids: &mut Vec<i32>,
) {
    if let DistributedNodeKind::Scan(scan) = &node.payload {
        let ScanSource::Sql(source) = &scan.table.source;
        if let crate::sql::planner::table::SqlScanKind::FrozenInputSet {
            version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(snapshot_id),
        } = &source.kind
        {
            frozen_snapshots
                .entry(source.table.table.clone())
                .or_default()
                .insert(*snapshot_id);
        }
        let previous = scans.insert(source.table.table.clone(), scan.table.clone());
        if let Some(previous) = previous {
            let ScanSource::Sql(previous) = &previous.source;
            assert_eq!(
                previous.binding, source.binding,
                "repeated join-refresh scan for {} must keep its exact token",
                source.table.table
            );
            assert_eq!(
                previous.table, source.table,
                "repeated join-refresh scan for {} must keep its base identity",
                source.table.table
            );
        }
        node_ids.push(node.node_id);
    }
    for child in &node.children {
        collect_coalesce_scan_tables(child, scans, frozen_snapshots, node_ids);
    }
}

fn coalesce_materialization(
    table: &crate::sql::planner::table::TableDef,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
) -> crate::engine::query_planning::bindings::QueryScanMaterialization {
    let metadata = planning_lease
        .binding()
        .metadata()
        .load_table(ConnectorTableRequest {
            table: ConnectorTableIdentity {
                instance_id: ConnectorInstanceId::parse("ice").expect("fixture catalog"),
                namespace: Arc::from("db"),
                table: Arc::from(table.name.as_str()),
            },
            resolution: ConnectorTableResolution::StrictBaseTable,
            context: crate::connector::test_request_context(),
        })
        .expect("fixture read admission");
    crate::engine::query_planning::bindings::QueryScanMaterialization {
        table: metadata.table,
        schema: metadata.schema,
        selector: ConnectorReadSelector::Current,
        statistics_pin: None,
        planning_lease,
    }
}

fn coalesce_binding(
    table: crate::sql::planner::table::TableDef,
    materialization: crate::engine::query_planning::bindings::QueryScanMaterialization,
    planning_lease: novarocks_spi::connector::ConnectorControlPlanningLease,
    frozen_snapshot_ids: &std::collections::BTreeSet<i64>,
) -> crate::engine::query_planning::bindings::QueryTableBinding {
    let mv_target_read = match &table.source {
        ScanSource::Sql(source) => match &source.kind {
            crate::sql::planner::table::SqlScanKind::MvTargetState { facts } => Some(
                crate::engine::query_planning::bindings::MvTargetReadAdmission {
                    full: materialization.clone(),
                    affected_partitions: materialization.clone(),
                    target_table_uuid: facts.target_table_uuid.clone(),
                    frozen_snapshot_id: facts.target_snapshot_id,
                },
            ),
            crate::sql::planner::table::SqlScanKind::MvTargetLocator { facts } => Some(
                crate::engine::query_planning::bindings::MvTargetReadAdmission {
                    full: materialization.clone(),
                    affected_partitions: materialization.clone(),
                    target_table_uuid: facts.target_table_uuid.clone(),
                    frozen_snapshot_id: facts.target_snapshot_id,
                },
            ),
            _ => None,
        },
        _ => None,
    };
    let frozen_snapshot_materializations = frozen_snapshot_ids
        .iter()
        .map(|snapshot_id| {
            let mut frozen = materialization.clone();
            frozen.selector = ConnectorReadSelector::SnapshotId(*snapshot_id);
            (*snapshot_id, frozen)
        })
        .collect();
    crate::engine::query_planning::bindings::QueryTableBinding {
        resolved: crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
            Some("ice"),
            "db",
            table,
        ),
        statistics_pin: None,
        admission: crate::engine::query_planning::bindings::QueryTableBindingAdmission::Exact(
            planning_lease,
        ),
        scan_materialization: Some(materialization),
        write_target_admission: None,
        mv_target_read,
        frozen_snapshot_materializations,
        delta_runtime_plans: std::collections::BTreeMap::new(),
    }
}

#[test]
fn sqlx2_join_refresh_coalesce_tokenized_materialization_lowers_native_bundle() {
    use std::num::NonZeroU64;

    use crate::engine::query_planning::bindings::{
        QueryScanMaterialization, QueryTableBindingKey, QueryTableBindingStore,
    };
    use crate::sql::planner::table::SqlScanKind;

    let bindings = QueryTableBindingStore::try_new_with_scope_for_test(
        NonZeroU64::new(79).expect("fixture scope"),
    );
    let (optimized, tokens) = crate::sql::planner::imv_rewrite::entrypoint::tests::tests_support::build_tokenized_join_refresh_coalesce_plan_for_lowering(bindings.scope());
    let physical = crate::sql::planner::optimizer_bridge::to_physical_plan(&optimized)
        .expect("coalesce fixture physical plan");
    let distributed = crate::sql::planner::pipeline::build_distributed_plan(physical)
        .expect("coalesce fixture distributed plan");

    let mut scan_tables = std::collections::BTreeMap::new();
    let mut frozen_snapshots = std::collections::BTreeMap::new();
    let mut scan_node_ids = Vec::new();
    for fragment in distributed.fragments() {
        collect_coalesce_scan_tables(
            &fragment.root,
            &mut scan_tables,
            &mut frozen_snapshots,
            &mut scan_node_ids,
        );
    }
    assert_eq!(scan_node_ids.len(), 9, "coalesce fixture scan count");
    assert_eq!(scan_tables.len(), 3, "coalesce fixture binding identities");

    let registry = ConnectorRegistry::new();
    crate::connector::iceberg::provider::register_planned_files_fixture(
        &registry,
        "ice",
        vec![data_file("s3://sqlx2-coalesce/frozen.parquet")],
        None,
    );
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let lease = controls
        .acquire_current(&ConnectorInstanceId::parse("ice").expect("fixture connector"))
        .expect("fixture planning lease");
    let no_frozen_snapshots = std::collections::BTreeSet::new();

    let left = scan_tables.remove("l").expect("left base scan");
    let left_id = bindings
        .resolve_or_insert_with_id(QueryTableBindingKey::strict_base("ice", "db", "l"), |id| {
            assert_eq!(id, tokens.left);
            Ok(coalesce_binding(
                left.clone(),
                coalesce_materialization(&left, lease.clone()),
                lease.clone(),
                frozen_snapshots.get("l").unwrap_or(&no_frozen_snapshots),
            ))
        })
        .expect("left binding");
    let right = scan_tables.remove("r").expect("right base scan");
    let right_id = bindings
        .resolve_or_insert_with_id(QueryTableBindingKey::strict_base("ice", "db", "r"), |id| {
            assert_eq!(id, tokens.right);
            Ok(coalesce_binding(
                right.clone(),
                coalesce_materialization(&right, lease.clone()),
                lease.clone(),
                frozen_snapshots.get("r").unwrap_or(&no_frozen_snapshots),
            ))
        })
        .expect("right binding");
    let target = scan_tables.remove("mv").expect("target locator scan");
    let target_id = bindings
        .resolve_or_insert_with_id(
            QueryTableBindingKey::mv_target("ice", "db", "mv", "uuid-tgt", Some(99)),
            |id| {
                assert_eq!(id, tokens.target);
                Ok(coalesce_binding(
                    target.clone(),
                    coalesce_materialization(&target, lease.clone()),
                    lease.clone(),
                    frozen_snapshots.get("mv").unwrap_or(&no_frozen_snapshots),
                ))
            },
        )
        .expect("target binding");
    assert_eq!(
        (left_id, right_id, target_id),
        (tokens.left, tokens.right, tokens.target)
    );
    assert!(scan_tables.is_empty());

    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        &controls,
        &crate::connector::test_request_context(),
        Some(&bindings),
        Some(&JoinRefreshDeltaResolver),
        super::super::ScanPreparationOptions::single_backend_fixture(),
    )
    .expect("tokenized coalesce scans must prepare from exact bindings");
    for node_id in &scan_node_ids {
        assert!(
            prepared.scan_bindings().binding(*node_id).is_some(),
            "prepared binding missing coalesce scan node {node_id}"
        );
    }
    let native =
        crate::protocol::native::encode::encode_native_fragment_bundle(&distributed, &prepared)
            .expect("tokenized coalesce plan must lower to native fragments");
    assert_eq!(
        native.fragment_ids().count(),
        distributed.fragments().len(),
        "native bundle must contain every prepared coalesce fragment"
    );

    let kinds = distributed
        .fragments()
        .iter()
        .flat_map(|fragment| {
            let mut sources = Vec::new();
            fn visit(
                node: &crate::sql::planner::distributed::DistributedNode,
                out: &mut Vec<SqlScanKind>,
            ) {
                if let DistributedNodeKind::Scan(scan) = &node.payload {
                    let ScanSource::Sql(source) = &scan.table.source;
                    out.push(source.kind.clone());
                }
                for child in &node.children {
                    visit(child, out);
                }
            }
            visit(&fragment.root, &mut sources);
            sources
        })
        .collect::<Vec<_>>();
    assert!(
        kinds
            .iter()
            .any(|kind| matches!(kind, SqlScanKind::MvTargetLocator { .. }))
    );
    assert!(
        kinds
            .iter()
            .any(|kind| matches!(kind, SqlScanKind::Delta { .. }))
    );
    assert!(
        kinds
            .iter()
            .any(|kind| matches!(kind, SqlScanKind::FrozenInputSet { .. }))
    );
}

#[test]
fn scan_preparation_propagates_caller_cancellation() {
    let context =
        crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(true)))
            .expect("cancelled request context");
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(registry.clone());
    let plan = plan(scan_node(10, IcebergDataFileBinding::CurrentSnapshot));
    let query_bindings = fixture_query_table_bindings(&plan, &controls);
    let err = match super::super::prepare_scan_bindings(
        &plan,
        &controls,
        &context,
        Some(&query_bindings),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
    ) {
        Ok(_) => panic!("caller cancellation must reach the connector provider"),
        Err(err) => err,
    };

    assert!(
        err.contains("Cancelled: connector request was cancelled"),
        "{err}"
    );
}

#[test]
fn sqlx2_preparation_uses_request_local_scan_materialization_without_reacquiring_current() {
    let mut root = scan_node(10, IcebergDataFileBinding::CurrentSnapshot);
    let DistributedNodeKind::Scan(scan) = &root.payload else {
        panic!("fixture root must be a scan");
    };
    let table = iceberg_table();
    let source_table = scan.table.clone();
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let lease = controls
        .acquire_current(
            &ConnectorInstanceId::parse(&table.catalog).expect("fixture catalog instance"),
        )
        .expect("fixture planning lease");
    let bindings = crate::engine::query_planning::bindings::QueryTableBindingStore::try_new()
        .expect("binding store");
    let binding_id = bindings
        .resolve_or_insert_with_id(
            crate::engine::query_planning::bindings::QueryTableBindingKey::strict_base(
                &table.catalog,
                &table.namespace,
                &table.table,
            ),
            |id| {
                let mut resolved = source_table.clone();
                resolved.source = ScanSource::Sql(crate::sql::planner::table::SqlScanSource::new(
                    id,
                    crate::sql::planner::table::SqlTableIdentity {
                        catalog: table.catalog.clone(),
                        namespace: table.namespace.clone(),
                        table: table.table.clone(),
                    },
                    crate::sql::planner::table::SqlScanKind::Data {
                        version: crate::sql::planner::table::SqlTableVersionSelector::Current,
                    },
                ));
                let metadata = lease
                    .binding()
                    .metadata()
                    .load_table(ConnectorTableRequest {
                        table: ConnectorTableIdentity {
                            instance_id: ConnectorInstanceId::parse(&table.catalog)
                                .expect("fixture catalog instance"),
                            namespace: Arc::from(table.namespace.as_str()),
                            table: Arc::from(table.table.as_str()),
                        },
                        resolution: ConnectorTableResolution::StrictBaseTable,
                        context: crate::connector::test_request_context(),
                    })
                    .expect("fixture read admission");
                let binding = crate::engine::query_planning::bindings::QueryTableBinding {
                    resolved: crate::sql::catalog::ResolvedAnalyzerTable::from_planner(
                        Some(&table.catalog),
                        "default",
                        resolved,
                    ),
                    statistics_pin: None,
                    admission:
                        crate::engine::query_planning::bindings::QueryTableBindingAdmission::Exact(
                            lease.clone(),
                        ),
                    scan_materialization: Some(
                        crate::engine::query_planning::bindings::QueryScanMaterialization {
                            table: metadata.table,
                            schema: metadata.schema,
                            selector: ConnectorReadSelector::Current,
                            statistics_pin: None,
                            planning_lease: lease.clone(),
                        },
                    ),
                    write_target_admission: None,
                    mv_target_read: None,
                    frozen_snapshot_materializations: std::collections::BTreeMap::new(),
                    delta_runtime_plans: std::collections::BTreeMap::new(),
                };
                Ok(binding)
            },
        )
        .expect("binding token");
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("fixture root must be a scan");
    };
    scan.table.source = ScanSource::Sql(crate::sql::planner::table::SqlScanSource::new(
        binding_id,
        crate::sql::planner::table::SqlTableIdentity {
            catalog: table.catalog.clone(),
            namespace: table.namespace.clone(),
            table: table.table.clone(),
        },
        crate::sql::planner::table::SqlScanKind::Data {
            version: crate::sql::planner::table::SqlTableVersionSelector::Current,
        },
    ));

    let prepared = super::super::prepare_scan_bindings(
        &plan(root),
        &controls,
        &crate::connector::test_request_context(),
        Some(&bindings),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
    )
    .expect("exact query binding must plan the scan");
    assert!(
        !prepared
            .connector_read(0, 10)
            .expect("prepared connector read")
            .splits
            .is_empty(),
        "preparation must use files retained by the binding, not stale SQL plan files"
    );
    let retained = prepared
        .connector_read(0, 10)
        .expect("prepared connector read")
        .planning_lease
        .clone();
    assert_eq!(
        retained.binding().incarnation(),
        lease.binding().incarnation(),
        "preparation must retain the query binding generation"
    );
}

#[test]
fn sqlx1_preparation_rejects_unbound_binding_instead_of_reacquiring_current() {
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let bindings = crate::engine::query_planning::bindings::QueryTableBindingStore::try_new()
        .expect("binding store");
    let error = match super::super::prepare_scan_bindings(
        &plan(scan_node(10, IcebergDataFileBinding::CurrentSnapshot)),
        &controls,
        &crate::connector::test_request_context(),
        Some(&bindings),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
    ) {
        Ok(_) => panic!("unbound binding must fail before a current-generation acquire"),
        Err(error) => error,
    };
    assert!(
        error.contains("SQL table binding token is missing from this request")
            || error.contains("SQL table binding token belongs to a different request"),
        "{error}"
    );
}

#[test]
fn ordinary_current_snapshot_is_immutable_and_does_not_invoke_resolver() {
    let plan = plan(scan_node(10, IcebergDataFileBinding::CurrentSnapshot));
    let before = format!("{plan:#?}");
    let bindings = prepare_scan_bindings(
        &plan,
        &registry(vec![data_file("s3://bucket/current.parquet")]),
        Some(&RejectResolver),
    )
    .expect("prepare current-snapshot scan");

    assert_eq!(format!("{plan:#?}"), before);
    assert!(bindings.binding(10).is_some());
    assert!(bindings.scan_ranges(0, 10).expect("ranges").is_empty());
    assert_eq!(
        bindings
            .connector_read(0, 10)
            .expect("opaque connector read")
            .splits
            .len(),
        1
    );
}

#[test]
fn duplicate_scan_node_defense_reports_exact_error() {
    let root = scan_node(10, IcebergDataFileBinding::ExplicitFiles);
    let registry = registry(vec![data_file("s3://bucket/explicit.parquet")]);
    let mut seen_scan_node_ids = std::collections::BTreeSet::new();
    let mut bindings = crate::query_execution::preparation::scan::ScanExecutionBindings::default();
    let context = crate::connector::test_request_context();
    let controls = crate::connector::FixtureControlResolver::new(registry.clone());
    let query_bindings = fixture_query_table_bindings(&plan(root.clone()), &controls);

    collect_scan_bindings(
        0,
        &root,
        &controls,
        &context,
        Some(&query_bindings),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
        &mut seen_scan_node_ids,
        &mut bindings,
    )
    .expect("first scan preparation");
    let err = collect_scan_bindings(
        0,
        &root,
        &controls,
        &context,
        Some(&query_bindings),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
        &mut seen_scan_node_ids,
        &mut bindings,
    )
    .expect_err("duplicate scan node must fail before re-planning");

    assert_eq!(err, "duplicate scan node_id=10");
}

#[test]
fn refresh_only_sources_require_resolver_with_kind_and_node_id() {
    for (source, expected_kind) in [(
        crate::sql::planner::table::test_sql_scan_source(
            crate::sql::planner::table::SqlScanKind::Delta {
                from_snapshot_id: 6,
                to_snapshot_id: 7,
            },
        ),
        "SqlDelta",
    )] {
        let mut root = scan_node(37, IcebergDataFileBinding::ExplicitFiles);
        replace_scan_source(&mut root, source);

        let err = match prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), None) {
            Ok(_) => panic!("{expected_kind} without resolver must fail"),
            Err(err) => err,
        };

        assert!(err.contains("requires scan binding resolver"), "{err}");
        assert!(err.contains(expected_kind), "{err}");
        assert!(err.contains("node_id=37"), "{err}");
    }
}

#[test]
fn resolver_error_reports_source_kind_node_id_and_cause() {
    let mut root = scan_node(47, IcebergDataFileBinding::ExplicitFiles);
    replace_scan_source(
        &mut root,
        crate::sql::planner::table::test_sql_scan_source(
            crate::sql::planner::table::SqlScanKind::Delta {
                from_snapshot_id: 6,
                to_snapshot_id: 7,
            },
        ),
    );

    let err =
        match prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), Some(&ErrorResolver)) {
            Ok(_) => panic!("resolver error must fail preparation"),
            Err(err) => err,
        };

    assert_eq!(
        err,
        "scan binding resolver failed for required source SqlDelta from_snapshot_id=6 to_snapshot_id=7 node_id=47: boom"
    );
}

#[test]
fn resolver_ok_none_reports_exact_required_source_error() {
    let mut root = scan_node(48, IcebergDataFileBinding::ExplicitFiles);
    replace_scan_source(
        &mut root,
        crate::sql::planner::table::test_sql_scan_source(
            crate::sql::planner::table::SqlScanKind::Delta {
                from_snapshot_id: 6,
                to_snapshot_id: 7,
            },
        ),
    );

    let err =
        match prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), Some(&EmptyResolver)) {
            Ok(_) => panic!("empty resolver result must fail preparation"),
            Err(err) => err,
        };

    assert_eq!(
        err,
        "scan binding resolver returned no binding for required source SqlDelta from_snapshot_id=6 to_snapshot_id=7 node_id=48"
    );
}

#[test]
fn resolver_failure_precedes_invalid_physical_projection() {
    let mut root = scan_node(49, IcebergDataFileBinding::ExplicitFiles);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.columns[0].name = "missing".to_string();
    scan.table.source = crate::sql::planner::table::test_sql_scan_source(
        crate::sql::planner::table::SqlScanKind::Delta {
            from_snapshot_id: 6,
            to_snapshot_id: 7,
        },
    );

    let err =
        match prepare_scan_bindings(&plan(root), &ConnectorRegistry::new(), Some(&ErrorResolver)) {
            Ok(_) => panic!("resolver error must win over physical projection error"),
            Err(err) => err,
        };

    assert_eq!(
        err,
        "scan binding resolver failed for required source SqlDelta from_snapshot_id=6 to_snapshot_id=7 node_id=49: boom"
    );
}

#[test]
fn target_state_and_locator_reject_equality_deletes() {
    use novarocks_execution::exec::row_position::{
        ICEBERG_FILE_PATH_COL, ICEBERG_LAST_UPDATED_SEQ_COL, ICEBERG_ROW_ID_COL,
        ICEBERG_ROW_POS_COL,
    };

    let sources = [
        (
            crate::sql::planner::table::test_sql_scan_source(crate::sql::planner::table::SqlScanKind::MvTargetLocator { facts: crate::sql::planner::table::SqlMvTargetLocatorScan {
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(6),
                apply_key_column: "id".to_string(),
                branch_id_column: None,
            }}),
            "target-locator",
        ),
        (
            crate::sql::planner::table::test_sql_scan_source(crate::sql::planner::table::SqlScanKind::MvTargetState { facts: crate::sql::planner::table::SqlMvTargetStateScan {
                target_table_uuid: "00000000-0000-0000-0000-000000000001".to_string(),
                target_snapshot_id: Some(6),
                aggregate_state_layout_version: 1,
                columns: vec![source_column("id", DataType::Int32, false)],
                group_key_names: vec!["id".to_string()],
                aggregate_state_names: Vec::new(),
                physical_column_names: vec!["id".to_string()],
                row_id_column_name: ICEBERG_ROW_ID_COL.to_string(),
                row_filter:
                    crate::sql::planner::table::SqlMvTargetStateRowFilter::DeltaInputRowIds {
                        row_id_column_name: ICEBERG_ROW_ID_COL.to_string(),
                        branch_scope: None,
                    },
                partition_constraint:
                    crate::sql::planner::table::SqlMvTargetStatePartitionConstraint::Unpartitioned,
            }}),
            "target-state",
        ),
    ];

    for (source, expected_kind) in sources {
        let mut root = scan_node(39, IcebergDataFileBinding::ExplicitFiles);
        let DistributedNodeKind::Scan(scan) = &mut root.payload else {
            panic!("test root must be a scan");
        };
        scan.table
            .columns
            .push(source_column("category", DataType::Utf8, true));
        scan.table.iceberg_row_lineage_metadata_columns = vec![
            source_column(ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
            source_column(ICEBERG_ROW_POS_COL, DataType::Int64, false),
            source_column(ICEBERG_ROW_ID_COL, DataType::Int64, false),
            source_column(ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
        ];
        scan.columns.extend([
            column(11, ICEBERG_FILE_PATH_COL, DataType::Utf8, false),
            column(12, ICEBERG_ROW_POS_COL, DataType::Int64, false),
            column(13, ICEBERG_ROW_ID_COL, DataType::Int64, false),
            column(14, ICEBERG_LAST_UPDATED_SEQ_COL, DataType::Int64, true),
        ]);
        scan.table.source = source;
        let mut file = data_file("s3://bucket/target-data.parquet");
        file.delete_files = vec![equality_delete_file(Vec::new(), vec![3])];
        let controls = crate::connector::FixtureControlResolver::new(registry(vec![file.clone()]));
        let err = match prepare_scan_bindings_with_materialized_files(
            &plan(root),
            &controls,
            None,
            vec![file],
        ) {
            Ok(_) => panic!("{expected_kind} equality-delete scan must fail"),
            Err(err) => err,
        };

        assert!(err.contains(expected_kind), "{err}");
        assert!(err.contains("does not support equality deletes"), "{err}");
    }
}

#[test]
fn resolver_execution_kind_must_match_semantic_source() {
    let mut version = scan_node(41, IcebergDataFileBinding::ExplicitFiles);
    replace_scan_source(
        &mut version,
        crate::sql::planner::table::test_sql_scan_source(
            crate::sql::planner::table::SqlScanKind::ConnectorRead,
        ),
    );
    let resolver = StaticResolver {
        execution: resolved_delta(),
    };

    let err =
        match prepare_scan_bindings(&plan(version), &ConnectorRegistry::new(), Some(&resolver)) {
            Ok(_) => panic!("connector scan must reject delta execution"),
            Err(err) => err,
        };

    assert!(err.contains("SqlConnectorRead"), "{err}");
    assert!(err.contains("requires ConnectorRead execution"), "{err}");
    assert!(err.contains("node_id=41"), "{err}");
}

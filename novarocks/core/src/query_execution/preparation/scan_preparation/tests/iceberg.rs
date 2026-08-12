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
use crate::connector::scan_model::{FixtureColumnStats, FixturePartitionValue};

/// A read unit that carries min/max statistics for `id`. The fixture never
/// decodes the bounds; they exist so the unit models a statistics-bearing file
/// whose bytes must survive Core untouched.
fn data_file_with_i32_stats(path: &str, min: i32, max: i32) -> FixtureScanFile {
    let mut file = data_file(path);
    file.column_stats = std::collections::BTreeMap::from([(
        "id".to_string(),
        FixtureColumnStats {
            null_count: Some(0),
            value_count: Some(10),
            lower_bound: Some(min.to_le_bytes().to_vec()),
            upper_bound: Some(max.to_le_bytes().to_vec()),
        },
    )]);
    file
}

fn identity_partition_file(path: &str, id: i32) -> FixtureScanFile {
    let mut file = data_file(path);
    file.partition_values = vec![FixturePartitionValue {
        field_name: "id".to_string(),
        transform: "identity".to_string(),
        value: Some(id.to_string()),
    }];
    file
}

fn id_eq(value: i64) -> crate::sql::analysis::TypedExpr {
    use crate::sql::analysis::{BinOp, ExprKind, LiteralValue, TypedExpr};

    TypedExpr {
        kind: ExprKind::BinaryOp {
            left: Box::new(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: ColumnId::new_for_test(1),
                    qualifier: Some("ice_t".to_string()),
                    column: "id".to_string(),
                },
                data_type: DataType::Int32,
                nullable: false,
            }),
            op: BinOp::Eq,
            right: Box::new(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(value)),
                data_type: DataType::Int32,
                nullable: false,
            }),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn unsupported_id_predicate() -> crate::sql::analysis::TypedExpr {
    use crate::sql::analysis::{ExprKind, TypedExpr};

    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: "abs".to_string(),
            args: vec![id_eq(12)],
            distinct: false,
            volatility: crate::sql::functions::FunctionVolatility::Immutable,
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn planned_data_files(
    bindings: &crate::query_execution::preparation::scan::ScanExecutionBindings,
    node_id: i32,
) -> Vec<FixtureScanFile> {
    let planned = bindings
        .connector_read(0, node_id)
        .expect("opaque connector read");
    planned
        .splits
        .iter()
        .map(|split| {
            crate::connector::scan_model::planned_split_file_for_test(split)
                .expect("decode fixture split")
        })
        .collect()
}

/// Predicate pushdown must reach the connector while Core keeps the matching
/// residual and returns the connector's own unit verbatim.
///
/// Whether a statistics bound excludes a unit is provider semantics, so the
/// fixture connector never prunes and this test supplies only the unit a
/// pruning provider would have selected. The pruning decision itself is
/// asserted by the provider's file-pruning unit tests.
#[test]
fn ordinary_iceberg_scan_uses_opaque_connector_read_and_preserves_residual() {
    let mut root = scan_node(10);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.predicates = vec![id_eq(12)];
    let bindings = prepare_scan_bindings(
        &plan(root),
        &registry(vec![data_file_with_i32_stats(
            "s3://bucket/id-10-20.parquet",
            10,
            20,
        )]),
        None,
    )
    .expect("prepare pruned scan");
    assert!(
        bindings
            .scan_ranges(0, 10)
            .is_some_and(|ranges| ranges.is_empty())
    );
    let read = bindings
        .connector_read(0, 10)
        .expect("opaque connector read");
    assert_eq!(read.splits.len(), 1);
    assert_eq!(
        planned_data_files(&bindings, 10)[0].path,
        "s3://bucket/id-10-20.parquet"
    );
    assert_eq!(read.static_predicates.len(), 1);
    assert_eq!(
        format!("{:?}", read.residual_predicates),
        format!("{:?}", vec![id_eq(12)])
    );
    assert!(read.predicate_dispositions.iter().all(|disposition| {
        disposition.kind == novarocks_spi::connector::ConnectorPredicateDispositionKind::PruningOnly
    }));
}

#[test]
fn delta_scan_uses_opaque_connector_read() {
    let mut root = scan_node(40);
    replace_scan_source(
        &mut root,
        crate::sql::planner::table::test_sql_scan_source(
            crate::sql::planner::table::SqlScanKind::Delta {
                from_snapshot_id: 6,
                to_snapshot_id: 7,
            },
        ),
    );
    let resolver = StaticResolver {
        execution: resolved_data_delta(),
    };

    let bindings = prepare_scan_bindings(
        &plan(root),
        &registry(vec![data_file("s3://bucket/delta.parquet")]),
        Some(&resolver),
    )
    .expect("prepare delta scan");

    assert!(matches!(
        bindings.binding(40).expect("binding").execution,
        ResolvedScanExecution::SealedConnectorScan(_)
    ));
    assert!(
        bindings
            .scan_ranges(0, 40)
            .expect("delta ranges")
            .is_empty()
    );
    let planned = bindings
        .connector_read(0, 40)
        .expect("delta connector read");
    assert_eq!(
        planned.declaration.descriptor().provider_id.as_str(),
        "fixture",
        "the planned read must carry the declaring connector's own provider identity"
    );
    assert_eq!(planned.splits.len(), 1);
    assert_eq!(planned.splits[0].split_id(), "fixture-0");
}

#[test]
fn explicit_files_plan_opaque_connector_splits() {
    let plan = plan(scan_node(10));
    let bindings = prepare_scan_bindings(
        &plan,
        &registry(vec![data_file("s3://bucket/explicit.parquet")]),
        None,
    )
    .expect("prepare explicit scan");
    let ranges = bindings.scan_ranges(0, 10).expect("ranges");
    assert!(ranges.is_empty());
    let planned = bindings.connector_read(0, 10).expect("connector read");
    assert_eq!(
        planned.declaration.descriptor().provider_id.as_str(),
        "fixture"
    );
    assert_eq!(
        planned.declaration.descriptor().instance_id.as_str(),
        "test_catalog"
    );
    assert_eq!(planned.splits.len(), 1);
    assert_eq!(planned.splits[0].split_id(), "fixture-0");
    assert_eq!(planned.splits[0].owner().as_str(), "test_catalog");
}

#[test]
fn sqlx2_frozen_snapshot_scan_uses_its_exact_admitted_file_set() {
    let mut root = scan_node(10);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("fixture root must be a scan");
    };
    let ScanSource::Sql(source) = &mut scan.table.source;
    source.kind = crate::sql::planner::table::SqlScanKind::FrozenInputSet {
        version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(11),
    };
    let plan = plan(root);
    let controls = crate::connector::FixtureControlResolver::new(registry(vec![data_file(
        "s3://bucket/current.parquet",
    )]));
    let store = fixture_query_table_bindings(&plan, &controls);
    let DistributedNodeKind::Scan(scan) = &plan.fragments()[0].root.payload else {
        panic!("fixture root must remain a scan");
    };
    let ScanSource::Sql(source) = &scan.table.source;
    let selected = store
        .frozen_snapshot_materialization(source.binding, 11)
        .expect("select admitted snapshot files");
    let crate::engine::query_planning::bindings::QueryScanMaterialization { selector, .. } =
        selected
    else {
        panic!("frozen snapshot must retain neutral connector materialization");
    };

    assert_eq!(
        selector,
        novarocks_spi::connector::ConnectorReadSelector::SnapshotId(11),
        "FrozenInputSet must retain its admitted snapshot selector"
    );
    super::super::prepare_scan_bindings(
        &plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&store),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
    )
    .expect("prepare selected frozen snapshot scan");
}

#[test]
fn sqlx2_frozen_snapshot_scan_rejects_a_selector_without_admitted_files() {
    let mut root = scan_node(10);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("fixture root must be a scan");
    };
    let ScanSource::Sql(source) = &mut scan.table.source;
    source.kind = crate::sql::planner::table::SqlScanKind::FrozenInputSet {
        version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(11),
    };
    let controls = crate::connector::FixtureControlResolver::new(registry(vec![data_file(
        "s3://bucket/current.parquet",
    )]));
    let store = fixture_query_table_bindings(&plan(root.clone()), &controls);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("fixture root must remain a scan");
    };
    let ScanSource::Sql(source) = &mut scan.table.source;
    source.kind = crate::sql::planner::table::SqlScanKind::FrozenInputSet {
        version: crate::sql::planner::table::SqlTableVersionSelector::Snapshot(12),
    };
    let plan = plan(root);

    let error = match super::super::prepare_scan_bindings(
        &plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&store),
        None,
        super::super::ScanPreparationOptions::single_backend_fixture(),
    ) {
        Ok(_) => panic!("unadmitted frozen snapshot must fail before split planning"),
        Err(error) => error,
    };
    assert!(
        error.contains("snapshot 12 has no admitted connector materialization"),
        "{error}"
    );
}

/// A partition-valued unit takes the same opaque path as any other: Core keeps
/// the residual and never reads the partition values.
///
/// Identity-partition exclusion is provider semantics, so the fixture does not
/// prune and this test supplies only the surviving unit; the exclusion itself is
/// asserted by the provider's file-pruning unit tests.
#[test]
fn identity_partition_predicate_stays_on_opaque_connector_path() {
    let mut root = scan_node(10);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.predicates = vec![id_eq(12)];
    let bindings = prepare_scan_bindings(
        &plan(root),
        &registry(vec![identity_partition_file(
            "s3://bucket/id-12.parquet",
            12,
        )]),
        None,
    )
    .expect("prepare connector scan");
    let read = bindings
        .connector_read(0, 10)
        .expect("opaque connector read");
    assert_eq!(read.splits.len(), 1);
    assert_eq!(
        planned_data_files(&bindings, 10)[0].path,
        "s3://bucket/id-12.parquet"
    );
    assert_eq!(
        format!("{:?}", read.residual_predicates),
        format!("{:?}", vec![id_eq(12)])
    );
}

#[test]
fn large_plain_file_preserves_provider_owned_split_and_byte_estimate() {
    let plan = plan(scan_node(10));
    let mut file = data_file("s3://bucket/large.parquet");
    file.size = 300 * 1024 * 1024;
    let bindings =
        prepare_scan_bindings(&plan, &registry(vec![file]), None).expect("prepare large-file scan");
    assert!(bindings.scan_ranges(0, 10).expect("ranges").is_empty());
    let planned = bindings.connector_read(0, 10).expect("connector read");

    assert_eq!(planned.splits.len(), 1);
    assert_eq!(planned.splits[0].estimated_bytes(), Some(300 * 1024 * 1024));
    let file = crate::connector::scan_model::planned_split_file_for_test(&planned.splits[0])
        .expect("decode fixture split");
    assert_eq!(file.path, "s3://bucket/large.parquet");
    assert_eq!(file.size, 300 * 1024 * 1024);
}

/// A connector split-planning failure must reach the caller verbatim, carrying
/// the connector's own error kind and message under the scan node that asked
/// for it. Core must neither reword nor reclassify a provider refusal.
#[test]
fn connector_planning_error_is_preserved_exactly_with_scan_node_context() {
    let plan = plan(scan_node(10));
    // The scan carrier names `test_table`; registering units only for another
    // table makes the connector refuse to plan with its own NotFound.
    let registry = registry_for_tables(HashMap::from([(
        "other_table".to_string(),
        vec![data_file("s3://bucket/other.parquet")],
    )]));

    let err = match prepare_scan_bindings(&plan, &registry, None) {
        Ok(_) => panic!("a connector that cannot plan the scan must fail preparation"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        "scan preparation node_id=10: NotFound: no planned files for fixture table test_table"
    );
}

#[test]
fn unsupported_predicate_does_not_guess_pruning() {
    let mut root = scan_node(10);
    let DistributedNodeKind::Scan(scan) = &mut root.payload else {
        panic!("test root must be a scan");
    };
    scan.predicates = vec![unsupported_id_predicate()];
    let bindings = prepare_scan_bindings(
        &plan(root),
        &registry(vec![
            data_file_with_i32_stats("s3://bucket/id-1-5.parquet", 1, 5),
            data_file_with_i32_stats("s3://bucket/id-10-20.parquet", 10, 20),
        ]),
        None,
    )
    .expect("unsupported pruning predicate must preserve scan semantics");

    let read = bindings
        .connector_read(0, 10)
        .expect("opaque connector read");
    assert!(read.static_predicates.is_empty());
    assert_eq!(
        format!("{:?}", read.residual_predicates),
        format!("{:?}", vec![unsupported_id_predicate()])
    );
    assert_eq!(read.splits.len(), 2);
}

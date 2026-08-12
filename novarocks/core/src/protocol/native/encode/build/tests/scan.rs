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

//! Native scan-encoding tests over the provider-neutral read fixture.
//!
//! Every assertion here is about Core: which columns the native `ScanNode`
//! publishes, which residual predicates it keeps, and that the opaque connector
//! facts it forwards come back byte-for-byte. None of it needs a concrete
//! provider, so none of it names one.
//!
//! Provider semantics that used to be asserted through this encoder now live
//! beside the provider implementation; Core tests cover only the neutral scan
//! contract encoded here.

use std::collections::BTreeMap;

use super::*;
use crate::connector::scan_model::{
    FixtureColumnStats, FixtureDeleteFile, FixtureScanFile,
    planned_split_file_for_test as fixture_split_file,
    planned_split_required_physical_columns_for_test as fixture_split_required_columns,
    register_planned_files_fixture,
};

/// Provider-private bytes the fixture attaches to every read unit. Core has no
/// vocabulary for them, so it must return them untouched.
const OPAQUE_PROVIDER_PAYLOAD: &[u8] = b"provider-private-equality-key-state";

/// One equality-delete descriptor, identified by column name, by field ID, or
/// by both. The fixture never interprets either form.
fn equality_delete(
    equality_column_names: &[&str],
    equality_field_ids: &[i32],
) -> FixtureDeleteFile {
    FixtureDeleteFile::equality(
        "s3://bucket/eq-delete.parquet",
        equality_column_names,
        equality_field_ids,
    )
}

/// One read unit carrying provider-private bytes plus the supplied deletes.
fn fixture_file(deletes: Vec<FixtureDeleteFile>) -> FixtureScanFile {
    let mut file = FixtureScanFile::new("s3://bucket/data.parquet");
    file.partition_spec_id = Some(0);
    file.sequence_number = Some(1);
    file.deletes = deletes;
    file.opaque_payload = OPAQUE_PROVIDER_PAYLOAD.to_vec();
    file
}

/// A read unit whose `id` statistics are present but stay encoded: only a
/// provider decodes bounds, so Core must not act on them.
fn i32_stats_file(path: &str, min: i32, max: i32) -> FixtureScanFile {
    let mut file = FixtureScanFile::new(path);
    file.column_stats = BTreeMap::from([(
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

fn fixture_registry(files: Vec<FixtureScanFile>) -> ConnectorRegistry {
    let registry = ConnectorRegistry::new();
    register_planned_files_fixture(&registry, "test_catalog", files, None);
    registry
}

fn set_scan_predicates(plan: DistributedPlan, predicates: Vec<TypedExpr>) -> DistributedPlan {
    crate::sql::planner::distributed::test_support::rebuild_test_plan(
        plan,
        Default::default(),
        |draft| {
            let DistributedNodeKind::Scan(scan) = &mut draft.fragments_mut()[0].root.payload else {
                panic!("root must be scan");
            };
            scan.predicates = predicates;
        },
    )
}

fn id_eq_literal(value: i64) -> TypedExpr {
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
            op: crate::sql::analysis::BinOp::Eq,
            right: Box::new(TypedExpr {
                kind: ExprKind::Literal(crate::sql::analysis::LiteralValue::Int(value)),
                data_type: DataType::Int32,
                nullable: false,
            }),
        },
        data_type: DataType::Boolean,
        nullable: false,
    }
}

fn native_root_scan(
    result: &(
        PreparedFragmentSet,
        NativeFragmentBundle,
        Vec<BoundarySchemaReport>,
    ),
) -> &novarocks_protocol::plan::ScanNode {
    let root_fragment_id = result.0.scheduling_view().execution_anchor();
    let root = result
        .1
        .get(root_fragment_id)
        .expect("native root fragment")
        .root
        .as_ref()
        .expect("root node");
    let novarocks_protocol::plan::distributed_node::Payload::Physical(physical) =
        root.payload.as_ref().expect("root payload")
    else {
        panic!("root must be physical");
    };
    let novarocks_protocol::plan::plan_node::Kind::Scan(scan) =
        physical.kind.as_ref().expect("physical kind")
    else {
        panic!("root must be scan");
    };
    scan
}

fn native_connector_splits(
    result: &(
        PreparedFragmentSet,
        NativeFragmentBundle,
        Vec<BoundarySchemaReport>,
    ),
) -> &[novarocks_spi::connector::ConnectorSplit] {
    result
        .0
        .scheduling_view()
        .connector_read(0, 10)
        .expect("opaque connector read")
        .splits
        .as_slice()
}

/// Read the neutral facts back out of the planned splits. Anything other than a
/// verbatim round trip means Core reinterpreted a provider fact.
fn native_planned_files(
    result: &(
        PreparedFragmentSet,
        NativeFragmentBundle,
        Vec<BoundarySchemaReport>,
    ),
) -> Vec<FixtureScanFile> {
    native_connector_splits(result)
        .iter()
        .map(|split| fixture_split_file(split).expect("decode neutral fixture split"))
        .collect()
}

fn native_planned_paths(
    result: &(
        PreparedFragmentSet,
        NativeFragmentBundle,
        Vec<BoundarySchemaReport>,
    ),
) -> Vec<String> {
    native_planned_files(result)
        .into_iter()
        .map(|file| file.path)
        .collect()
}

/// A key expressed only as a provider field ID has no name Core could bind, so
/// the SQL projection must stay exactly as planned and the whole provider fact
/// must survive the round trip untouched.
#[test]
fn equality_delete_field_ids_remain_provider_owned() {
    let plan = iceberg_scan_plan(Some(vec!["id"]));
    let file = fixture_file(vec![equality_delete(&[], &[3])]);
    let registry = fixture_registry(vec![file.clone()]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build native connector scan");

    assert_eq!(native_root_scan(&result).required_columns, vec!["id"]);
    assert_eq!(
        native_planned_files(&result),
        vec![file],
        "Core must forward the provider fact byte-for-byte"
    );
}

/// Naming the key by column instead of by field ID must not make it Core's:
/// the provider still owns it, and the SQL projection is unchanged.
#[test]
fn equality_delete_column_names_remain_provider_owned() {
    let plan = iceberg_scan_plan(Some(vec!["id"]));
    let file = fixture_file(vec![equality_delete(&["category"], &[])]);
    let registry = fixture_registry(vec![file.clone()]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build native connector scan");

    assert_eq!(native_root_scan(&result).required_columns, vec!["id"]);
    assert_eq!(
        native_planned_files(&result),
        vec![file],
        "Core must forward the provider fact byte-for-byte"
    );
}

/// The provider declares a physical column it must read for its own delete
/// application. That declaration travels inside the opaque split and must reach
/// neither the native scan's required columns nor its query outputs.
#[test]
fn equality_delete_key_from_planned_splits_is_hidden_from_query_projection() {
    let plan = iceberg_scan_plan(Some(vec!["id"]));
    let registry = fixture_registry(vec![fixture_file(vec![equality_delete(
        &["category"],
        &[2],
    )])]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build native connector scan");
    let scan = native_root_scan(&result);

    assert_eq!(scan.required_columns, vec!["id"]);
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id"]
    );
    assert_eq!(
        fixture_split_required_columns(&native_connector_splits(&result)[0])
            .expect("decode neutral fixture split"),
        vec!["category".to_string()],
        "the key stays a provider-owned physical read requirement"
    );
}

#[test]
fn equality_delete_with_non_key_projection_rejects_unbound_hidden_key() {
    // A sealed SQL scan may not introduce a connector-only physical key
    // without a planner ColumnId. The enclosing planner must model that key
    // explicitly before the native encoder receives the artifact.
    let plan = iceberg_scan_plan_with_outputs(Some(vec!["id", "category"]), &["id"]);
    let registry = fixture_registry(vec![fixture_file(vec![equality_delete(&[], &[3])])]);

    let error = match build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    )) {
        Ok(_) => panic!("unbound hidden equality key must fail before encoding"),
        Err(error) => error,
    };

    assert!(
        error.contains("required physical column 'category'"),
        "{error}"
    );
    assert!(error.contains("no planner ColumnId"), "{error}");
}

#[test]
fn equality_delete_with_unrestricted_select_all_preserves_all_query_outputs() {
    let plan = iceberg_scan_plan_with_outputs(None, &["id", "category"]);
    let registry = fixture_registry(vec![fixture_file(vec![equality_delete(&[], &[3])])]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build unrestricted SELECT * connector scan");
    let scan = native_root_scan(&result);

    assert_eq!(scan.required_columns, vec!["id", "category"]);
    assert_eq!(
        scan.columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["id", "category"]
    );
}

/// Core does not split a large read unit: sizing is the provider's call, so the
/// unit stays whole and its size becomes the split's byte estimate.
#[test]
fn native_scan_keeps_large_file_in_provider_owned_split() {
    let plan = iceberg_scan_plan(None);
    let mut file = FixtureScanFile::new("s3://bucket/large.parquet");
    file.size = 300 * 1024 * 1024;
    let registry = fixture_registry(vec![file.clone()]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("build native connector scan");
    let splits = native_connector_splits(&result);

    assert_eq!(splits.len(), 1);
    assert_eq!(splits[0].estimated_bytes(), Some(300 * 1024 * 1024));
    assert_eq!(
        fixture_split_file(&splits[0]).expect("decode neutral fixture split"),
        file
    );
}

/// A predicate no provider accepted must not gain a pruning effect on the way
/// through Core: every read unit survives and the residual stays on the native
/// scan node so execution still filters.
#[test]
fn native_scan_unsupported_predicate_does_not_guess_pruning() {
    let plan = set_scan_predicates(
        iceberg_scan_plan(None),
        vec![TypedExpr {
            kind: ExprKind::FunctionCall {
                name: "abs".to_string(),
                args: vec![id_eq_literal(12)],
                distinct: false,
                volatility: crate::sql::functions::FunctionVolatility::Immutable,
            },
            data_type: DataType::Boolean,
            nullable: false,
        }],
    );
    let registry = fixture_registry(vec![
        i32_stats_file("s3://bucket/id-1-5.parquet", 1, 5),
        i32_stats_file("s3://bucket/id-10-20.parquet", 10, 20),
    ]);

    let result = build_for_test(TestBuildRequest::result(
        &plan,
        &EmptyCatalog,
        &registry,
        None,
    ))
    .expect("unsupported pruning predicate must preserve scan semantics");

    assert_eq!(
        native_planned_paths(&result),
        vec![
            "s3://bucket/id-1-5.parquet".to_string(),
            "s3://bucket/id-10-20.parquet".to_string(),
        ],
        "no read unit may be dropped for a predicate the provider never accepted"
    );
    assert_eq!(
        native_root_scan(&result).predicates.len(),
        1,
        "the residual must survive so execution still applies it"
    );
}

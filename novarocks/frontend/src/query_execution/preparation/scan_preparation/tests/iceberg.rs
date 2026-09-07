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

//! Typed connector lowering of Iceberg scans.

use super::*;

/// `ScanExecutionBindings` holds connector leases and split managers, so it is
/// deliberately not `Debug`; a refusal is therefore asserted through a match.
fn expect_preparation_error(
    result: Result<crate::query_execution::preparation::scan::ScanExecutionBindings, String>,
    expectation: &str,
) -> String {
    match result {
        Ok(_) => panic!("{expectation}"),
        Err(error) => error,
    }
}

/// The one scan node every single-scan fixture plan has.
pub(super) fn only_scan_node(
    bindings: &crate::query_execution::preparation::scan::ScanExecutionBindings,
) -> (novarocks_sql::plan_read::FragmentId, i32) {
    let mut keys = bindings.typed_scan_keys().collect::<Vec<_>>();
    assert_eq!(keys.len(), 1, "the fixture plan has exactly one scan");
    keys.pop().expect("one typed scan")
}

/// A provider-frozen cohort read reaches the connector as exactly the file set
/// the cohort froze -- no more, no fewer, and in one canonical spelling. The
/// scan's own SQL identity is query-local and synthetic, so the relation the
/// connector is asked for comes from the pinned set rather than from the name
/// the statement planned under.
#[test]
fn a_pinned_cohort_read_freezes_the_relation_the_provider_named() {
    use novarocks_spi::connector::{
        ConnectorControlResolver, ConnectorInstanceId, ConnectorPinnedFileSet,
    };

    let plan = native_scan_plan(NativeScanFixture::PinnedFileSet).expect("sealed pinned fixture");
    let controls = crate::connector::FixtureControlResolver::new(registry(vec![data_file(
        "s3://bucket/current.parquet",
    )]));
    let instance_id = ConnectorInstanceId::parse("test_catalog").expect("fixture instance");
    let lease = controls
        .acquire_current(&instance_id)
        .expect("fixture planning lease");
    // Offered unsorted and out of order: one pinned read must have one
    // spelling, whoever assembled the list.
    let pinned = ConnectorPinnedFileSet::try_new(
        "db",
        "orders",
        12,
        ["s3://bucket/b.parquet", "s3://bucket/a.parquet"],
    )
    .expect("pinned file set");
    let resolver = super::StaticResolver {
        execution:
            crate::query_execution::preparation::scan::ResolvedScanExecution::AdmittedPinnedFileSet(
                crate::query_execution::preparation::scan::QueryPinnedFileSetRead {
                    pinned: pinned.clone(),
                    owner: instance_id,
                    planning_lease: lease,
                },
            ),
    };

    let bindings = super::prepare_scan_bindings_with_controls(&plan, &controls, Some(&resolver))
        .expect("pinned cohort scan preparation");
    let (fragment_id, node_id) = only_scan_node(&bindings);
    let typed = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan");
    assert_eq!(
        typed.prepared.table_scan.table().relation_kind(),
        novarocks_spi::connector::read_stack::ConnectorReadRelationKind::Table
    );
    assert_eq!(
        typed
            .prepared
            .table_scan
            .table()
            .catalog()
            .catalog_name()
            .as_str(),
        "test_catalog",
        "the SPI relation remains bound to the cohort's admitted catalog"
    );
}

#[test]
fn an_ordinary_iceberg_scan_lowers_to_a_typed_data_relation() {
    let bindings = prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
            .expect("sealed ordinary iceberg fixture"),
        &registry(vec![data_file("s3://bucket/data.parquet")]),
        None,
    )
    .expect("typed scan preparation");

    let (fragment_id, node_id) = only_scan_node(&bindings);
    let typed = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan");
    assert_eq!(
        typed.prepared.table_scan.table().relation_kind(),
        novarocks_spi::connector::read_stack::ConnectorReadRelationKind::Table
    );
    assert_eq!(
        typed
            .prepared
            .table_scan
            .table()
            .catalog()
            .catalog_name()
            .as_str(),
        typed.catalog_properties.handle().catalog_name().as_str(),
        "the frozen relation and its declaration name one generation"
    );
}

/// Preparation must hand the connector's enumerator on untouched. Nothing here
/// may produce a split set, and in particular the opaque carrier — the only
/// thing that ever carried one — must have no entry at all.
#[test]
fn preparation_enumerates_no_split() {
    let bindings = prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
            .expect("sealed ordinary iceberg fixture"),
        &registry(vec![
            data_file("s3://bucket/a.parquet"),
            data_file("s3://bucket/b.parquet"),
        ]),
        None,
    )
    .expect("typed scan preparation");

    let (fragment_id, node_id) = only_scan_node(&bindings);
    let typed = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan");
    let source = typed.prepared.table_scan.source();
    // The fixture control fails any enumeration attempt, so a prepared scan
    // proves preparation never called it.
    assert!(
        typed
            .prepared
            .split_manager
            .get_splits(
                &novarocks_spi::connector::read_stack::ConnectorSession::try_new(
                    "probe",
                    "probe",
                    "UTC",
                    "en_US",
                    std::time::SystemTime::UNIX_EPOCH,
                )
                .expect("probe session"),
                typed.prepared.table_scan.table().relation().table(),
                source.assignments(),
                &typed.prepared.table_scan.dynamic_filter_columns(),
                &typed.prepared.constraint,
            )
            .is_err(),
        "the fixture enumerator refuses, so preparation cannot have used it"
    );
}

/// The ordered assignments are the output-order authority, so they follow the
/// scan's own output order and are never sorted into the connector's order.
#[test]
fn assignments_follow_the_scan_output_order() {
    let bindings = prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
            .expect("sealed ordinary iceberg fixture"),
        &registry(vec![data_file("s3://bucket/data.parquet")]),
        None,
    )
    .expect("typed scan preparation");
    let (fragment_id, node_id) = only_scan_node(&bindings);
    let typed = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan");
    let source = typed.prepared.table_scan.source();
    let assignments = source.assignments();
    assert!(!assignments.is_empty());
    for (ordinal, assignment) in assignments.iter().enumerate() {
        assert_eq!(assignment.variable(), format!("v{ordinal}"));
    }
}

/// An MV target scan is an ordinary pinned DATA read: the lane it came from is
/// not an execution difference the typed stack can observe.
#[test]
fn an_mv_target_scan_lowers_to_an_ordinary_pinned_data_handle() {
    for fixture in [
        NativeScanFixture::TargetStateProjection,
        NativeScanFixture::TargetLocatorProjection,
    ] {
        let plan = native_scan_plan(fixture).expect("sealed MV target fixture");
        let bindings = prepare_scan_bindings(
            &plan,
            &registry(vec![data_file("s3://bucket/target.parquet")]),
            None,
        )
        .unwrap_or_else(|error| panic!("MV target lowering for {fixture:?}: {error}"));
        let (fragment_id, node_id) = only_scan_node(&bindings);
        let typed = bindings
            .typed_scan(fragment_id, node_id)
            .expect("typed connector scan");
        assert_eq!(
            typed.prepared.table_scan.table().relation_kind(),
            novarocks_spi::connector::read_stack::ConnectorReadRelationKind::Table,
            "an MV target lane must not produce a specialized relation"
        );
    }
}

/// A change-window read is its own relation family. It lowers to a typed
/// change-window relation pinned to both endpoints -- the set difference of
/// the rows visible at each, never a replay of the manifests between them.
#[test]
fn a_change_window_scan_lowers_to_a_typed_change_window_relation() {
    let bindings = prepare_scan_bindings_with_delta_resolver(
        &native_scan_plan(NativeScanFixture::DeltaForPreparedBinding)
            .expect("sealed delta fixture"),
        &registry(vec![data_file("s3://bucket/data.parquet")]),
    )
    .expect("typed change-window preparation");

    let (fragment_id, node_id) = only_scan_node(&bindings);
    let typed = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan");
    assert_eq!(
        typed.prepared.table_scan.table().relation_kind(),
        novarocks_spi::connector::read_stack::ConnectorReadRelationKind::ChangeWindow
    );
}

/// A change-window lane that reaches preparation with no resolver has no
/// query-local admission to freeze, and must say so rather than reading the
/// relation whole.
#[test]
fn a_change_window_scan_without_its_query_local_admission_fails_closed() {
    let error = expect_preparation_error(
        prepare_scan_bindings(
            &native_scan_plan(NativeScanFixture::DeltaForPreparedBinding)
                .expect("sealed delta fixture"),
            &registry(vec![data_file("s3://bucket/data.parquet")]),
            None,
        ),
        "a change-window scan has no admission without its resolver",
    );
    assert!(
        error.contains("SqlDelta") && error.contains("requires scan binding resolver"),
        "unexpected error: {error}"
    );
}

/// A synthetic VARIANT output has no connector column. Only the physical
/// columns are assigned; the backend materializes the synthetic one on top of
/// those read slots.
#[test]
fn a_variant_scan_assigns_only_its_physical_column() {
    let plan =
        native_scan_plan(NativeScanFixture::VariantProjection).expect("sealed VARIANT fixture");
    let bindings = prepare_scan_bindings(
        &plan,
        &registry(vec![data_file("s3://bucket/variant.parquet")]),
        None,
    )
    .expect("typed scan preparation");

    let (fragment_id, node_id) = only_scan_node(&bindings);
    let source = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan")
        .prepared
        .table_scan
        .source();
    let assignments = source.assignments();
    assert_eq!(
        assignments.len(),
        1,
        "a synthetic VARIANT output is not a connector column"
    );
    assert_eq!(assignments[0].variable(), "v0");
}

/// A pre-pinned opaque read has no relation name or version left to freeze, so
/// it fails closed rather than emitting the carrier it used to.
///
/// The refusal is on the scan's own category, before any resolver is
/// consulted, so no execution the resolver could supply changes the outcome.
#[test]
fn a_pre_pinned_opaque_read_fails_closed() {
    let error = expect_preparation_error(
        prepare_scan_bindings(
            &native_scan_plan(NativeScanFixture::ConnectorRead)
                .expect("sealed connector-read fixture"),
            &registry(vec![data_file("s3://bucket/data.parquet")]),
            None,
        ),
        "a pre-pinned opaque read has no typed lowering",
    );
    assert!(
        error.contains("pre-pinned opaque connector read"),
        "unexpected error: {error}"
    );
}

/// A scan whose exact catalog handle is not installed must fail closed: an
/// absent typed control is never permission to reach another catalog version.
#[test]
fn a_scan_whose_catalog_handle_does_not_resolve_fails_closed() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary iceberg fixture");
    let connectors = registry(vec![data_file("s3://bucket/data.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    let query_bindings = fixture_query_table_bindings(&plan, &controls);
    // An empty role host stands for "this catalog handle was never installed".
    let empty = Arc::new(crate::connector::ConnectorControlHost::new());
    let error = expect_preparation_error(
        super::super::prepare_scan_bindings(
            &plan,
            &controls,
            &crate::connector::test_request_context(),
            Some(&query_bindings),
            None,
            &fixture_scan_preparation_options(empty),
            &[],
        ),
        "an uninstalled catalog handle cannot be planned",
    );
    assert!(
        error.contains("no complete typed control generation"),
        "unexpected error: {error}"
    );
}

/// Preparation without the statement's typed control inputs is a contract
/// error, not permission to fall back to the opaque carrier.
#[test]
fn preparation_without_typed_inputs_refuses_instead_of_falling_back() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary iceberg fixture");
    let connectors = registry(vec![data_file("s3://bucket/data.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(connectors.clone());
    let query_bindings = fixture_query_table_bindings(&plan, &controls);
    let error = expect_preparation_error(
        super::super::prepare_scan_bindings(
            &plan,
            &controls,
            &crate::connector::test_request_context(),
            Some(&query_bindings),
            None,
            &super::super::ScanPreparationOptions::single_backend_fixture(),
            &[],
        ),
        "no typed control registry was threaded in",
    );
    assert!(
        error.contains("typed control registry"),
        "unexpected error: {error}"
    );
}

/// A frozen snapshot selector reaches the connector as an exact pin.
#[test]
fn a_frozen_snapshot_scan_pins_its_admitted_snapshot() {
    let bindings = prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::FrozenSnapshotEleven)
            .expect("sealed frozen-snapshot fixture"),
        &registry(vec![data_file("s3://bucket/data.parquet")]),
        None,
    )
    .expect("typed scan preparation");
    let (fragment_id, node_id) = only_scan_node(&bindings);
    let typed = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan");
    assert_eq!(
        typed.prepared.table_scan.table().relation_kind(),
        novarocks_spi::connector::read_stack::ConnectorReadRelationKind::Table
    );
}

/// One scan-domain request the frontend resolves for a scan.
fn source_scan_request(
    binding_id: u32,
    fragment_id: novarocks_sql::plan_read::FragmentId,
    node_id: i32,
    column_id: u32,
    data_type: arrow::datatypes::DataType,
    nullable: bool,
) -> novarocks_sql::planning::query_execution::SqlRuntimeFilterSourceScanRequest {
    novarocks_sql::planning::query_execution::SqlRuntimeFilterSourceScanRequest {
        binding_id,
        fragment_id,
        node_id,
        column_id: novarocks_sql::plan_read::ColumnId(column_id),
        data_type,
        nullable,
    }
}

/// A runtime filter reaches the reader by naming a column, not a position.
///
/// The fixture scan outputs two columns, and the filter names the second one.
/// The carrier must therefore bind the filter's id to the assignment that
/// holds that column's own `ColumnHandle` -- the Iceberg field ID -- rather
/// than to the first assignment or to any ordinal derived from the filter.
#[test]
fn a_runtime_filter_binds_to_the_assignment_holding_its_column_handle() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergAllColumns)
        .expect("sealed all-columns iceberg fixture");
    let bindings = prepare_scan_bindings_with_runtime_filters(
        &plan,
        &registry(vec![data_file("s3://bucket/data.parquet")]),
        &[source_scan_request(
            6,
            0,
            10,
            3,
            arrow::datatypes::DataType::Utf8,
            true,
        )],
    )
    .expect("typed scan preparation");

    let (fragment_id, node_id) = only_scan_node(&bindings);
    let source = bindings
        .typed_scan(fragment_id, node_id)
        .expect("typed connector scan")
        .prepared
        .table_scan
        .source();
    let assignments = source.assignments();
    assert_eq!(
        assignments.len(),
        2,
        "the fixture scan outputs id, category"
    );

    let carried = source.dynamic_filters();
    assert_eq!(carried.len(), 1);
    assert_eq!(carried[0].filter_id(), 6);
    let bound = assignments
        .iter()
        .find(|assignment| assignment.variable() == carried[0].variable())
        .expect("the binding names an assignment of this scan");
    // Identity, not position: the bound assignment is the one carrying the
    // named column's handle, and it is not the scan's first assignment.
    assert_eq!(bound.column(), assignments[1].column());
    assert_ne!(bound.column(), assignments[0].column());
}

/// A filter naming a column the scan does not output is refused, never dropped:
/// a dropped binding would leave its producer publishing into a scan that can
/// never apply it.
#[test]
fn a_runtime_filter_naming_an_unprojected_column_is_refused() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary iceberg fixture");
    let error = expect_preparation_error(
        prepare_scan_bindings_with_runtime_filters(
            &plan,
            &registry(vec![data_file("s3://bucket/data.parquet")]),
            &[source_scan_request(
                6,
                0,
                10,
                3,
                arrow::datatypes::DataType::Utf8,
                true,
            )],
        ),
        "a filter naming an unprojected column cannot be silently dropped",
    );
    assert!(
        error.contains("runtime filter binding id=6")
            && error.contains("does not resolve to exactly one physical output"),
        "unexpected error: {error}"
    );
}

/// Resolution runs against the typed scans preparation just froze and needs no
/// provider field ordinal at all.
#[test]
fn scan_domain_resolution_confirms_the_typed_carrier_binding() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergAllColumns)
        .expect("sealed all-columns iceberg fixture");
    let request = source_scan_request(6, 0, 10, 3, arrow::datatypes::DataType::Utf8, true);
    let bindings = prepare_scan_bindings_with_runtime_filters(
        &plan,
        &registry(vec![data_file("s3://bucket/data.parquet")]),
        std::slice::from_ref(&request),
    )
    .expect("typed scan preparation");

    let resolutions =
        crate::query_execution::preparation::runtime_filter_binding::resolve_runtime_filter_source_targets(
            [request],
            &bindings,
        )
        .expect("scan-domain resolution");
    assert_eq!(resolutions.len(), 1);
    assert_eq!(resolutions[0].binding_id, 6);
    assert_eq!(resolutions[0].data_type, arrow::datatypes::DataType::Utf8);
    assert!(resolutions[0].nullable);
}

/// A request the scan never bound is refused rather than resolved: the carrier
/// is the only thing the backend reads.
#[test]
fn scan_domain_resolution_refuses_a_filter_the_scan_never_bound() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergAllColumns)
        .expect("sealed all-columns iceberg fixture");
    let bindings = prepare_scan_bindings_with_runtime_filters(
        &plan,
        &registry(vec![data_file("s3://bucket/data.parquet")]),
        &[],
    )
    .expect("typed scan preparation");

    let error =
        crate::query_execution::preparation::runtime_filter_binding::resolve_runtime_filter_source_targets(
            [source_scan_request(6, 0, 10, 3, arrow::datatypes::DataType::Utf8, true)],
            &bindings,
        )
        .expect_err("an unbound filter cannot resolve");
    assert!(
        error.contains("runtime filter binding id=6") && error.contains("never offered"),
        "unexpected error: {error}"
    );
}

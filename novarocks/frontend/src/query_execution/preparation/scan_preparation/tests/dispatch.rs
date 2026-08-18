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
        let facts = scan_preparation_facts(scan);
        if facts.category() != SqlScanPreparationCategory::Delta {
            return Err("join refresh fixture resolver received a non-delta scan".to_string());
        }
        let delta = facts
            .delta_window()
            .ok_or_else(|| "join refresh fixture delta lacks a snapshot window".to_string())?;
        Ok(Some(ResolvedScanExecution::SealedConnectorScan(
            fixture_sealed_change_scan(
                facts.identity().catalog(),
                facts.identity().table(),
                delta.from_snapshot_id(),
                delta.to_snapshot_id(),
            ),
        )))
    }
}

#[test]
fn sqlx2_join_refresh_coalesce_tokenized_materialization_lowers_native_bundle() {
    fn collect(
        node: &novarocks_sql::plan_read::DistributedNode,
        facts: &mut Vec<(i32, SqlScanPreparationCategory)>,
    ) {
        if let DistributedNodeKind::Scan(scan) = &node.payload {
            facts.push((node.node_id, scan_preparation_facts(scan).category()));
        }
        for child in &node.children {
            collect(child, facts);
        }
    }

    let distributed = native_scan_plan(NativeScanFixture::JoinRefreshCoalesce)
        .expect("sealed join-refresh coalesce fixture");
    let mut scan_facts = Vec::new();
    for fragment in distributed.fragments() {
        collect(&fragment.root, &mut scan_facts);
    }
    assert_eq!(scan_facts.len(), 9, "coalesce fixture scan count");
    assert!(
        scan_facts
            .iter()
            .any(|(_, category)| *category == SqlScanPreparationCategory::MvTargetLocator)
    );
    assert!(
        scan_facts
            .iter()
            .any(|(_, category)| *category == SqlScanPreparationCategory::Delta)
    );
    assert!(
        scan_facts.iter().any(|(_, category)| {
            *category == SqlScanPreparationCategory::AdmittedFrozenSnapshot
        })
    );

    let registry = ConnectorRegistry::new();
    novarocks::connector::scan_model::register_planned_files_fixture(
        &registry,
        "ice",
        vec![data_file("s3://sqlx2-coalesce/frozen.parquet")],
        None,
    );
    let controls = novarocks::connector::FixtureControlResolver::new(registry);
    let bindings = fixture_query_table_bindings(&distributed, &controls);
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        &controls,
        &novarocks::connector::test_request_context(),
        Some(&bindings),
        Some(&JoinRefreshDeltaResolver),
        super::super::ScanPreparationOptions::single_backend_fixture(),
    )
    .expect("tokenized coalesce scans must prepare from exact bindings");
    for (node_id, _) in &scan_facts {
        assert!(prepared.scan_bindings().binding(*node_id).is_some());
    }
    let expected_ids = distributed
        .fragments()
        .iter()
        .map(|fragment| fragment.fragment_id)
        .collect::<std::collections::BTreeSet<_>>();
    let native = crate::query_execution::native_fragment::native_fragment_attachment_for_test(
        expected_ids
            .iter()
            .copied()
            .map(|fragment_id| novarocks_protocol::plan::PlanFragment {
                fragment_id,
                ..Default::default()
            }),
        &expected_ids,
        None,
    )
    .expect("tokenized coalesce fixture must seal native fragment IDs");
    assert_eq!(native.fragment_ids().count(), distributed.fragments().len());
}

#[test]
fn scan_preparation_propagates_caller_cancellation() {
    let context =
        novarocks::connector::connector_request_context(None, Arc::new(AtomicBool::new(true)))
            .expect("cancelled request context");
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = novarocks::connector::FixtureControlResolver::new(registry.clone());
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
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
        err.contains("Cancelled: read fixture observed caller cancellation"),
        "{err}"
    );
}

#[test]
fn sqlx2_preparation_uses_request_local_scan_materialization_without_reacquiring_current() {
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = novarocks::connector::FixtureControlResolver::new(registry);
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let bindings = fixture_query_table_bindings(&plan, &controls);

    let prepared = super::super::prepare_scan_bindings(
        &plan,
        &controls,
        &novarocks::connector::test_request_context(),
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
    let DistributedNodeKind::Scan(scan) = &plan.fragments()[0].root.payload else {
        panic!("sealed fixture must retain its scan root");
    };
    let expected = bindings
        .exact_planning_lease(scan_preparation_facts(scan).binding())
        .expect("fixture exact lease");
    assert_eq!(
        retained.binding().incarnation(),
        expected.binding().incarnation()
    );
}

#[test]
fn sqlx1_preparation_rejects_unbound_binding_instead_of_reacquiring_current() {
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = novarocks::connector::FixtureControlResolver::new(registry);
    let bindings = crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
        .expect("binding store");
    let error = match super::super::prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
            .expect("sealed ordinary fixture"),
        &controls,
        &novarocks::connector::test_request_context(),
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
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
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
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let root = plan.fragments()[0].root.clone();
    let registry = registry(vec![data_file("s3://bucket/explicit.parquet")]);
    let mut seen_scan_node_ids = std::collections::BTreeSet::new();
    let mut bindings = crate::query_execution::preparation::scan::ScanExecutionBindings::default();
    let context = novarocks::connector::test_request_context();
    let controls = novarocks::connector::FixtureControlResolver::new(registry.clone());
    let query_bindings = fixture_query_table_bindings(&plan, &controls);

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
    let err = match prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::DeltaForPreparedBinding)
            .expect("sealed delta fixture"),
        &ConnectorRegistry::new(),
        None,
    ) {
        Ok(_) => panic!("SqlDelta without resolver must fail"),
        Err(err) => err,
    };

    assert!(err.contains("requires scan binding resolver"), "{err}");
    assert!(err.contains("SqlDelta"), "{err}");
    assert!(err.contains("node_id=10"), "{err}");
}

#[test]
fn resolver_error_reports_source_kind_node_id_and_cause() {
    let err = match prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::DeltaForPreparedBinding)
            .expect("sealed delta fixture"),
        &ConnectorRegistry::new(),
        Some(&ErrorResolver),
    ) {
        Ok(_) => panic!("resolver error must fail preparation"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        "scan binding resolver failed for required source SqlDelta from_snapshot_id=6 to_snapshot_id=7 node_id=10: boom"
    );
}

#[test]
fn resolver_ok_none_reports_exact_required_source_error() {
    let err = match prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::DeltaForPreparedBinding)
            .expect("sealed delta fixture"),
        &ConnectorRegistry::new(),
        Some(&EmptyResolver),
    ) {
        Ok(_) => panic!("empty resolver result must fail preparation"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        "scan binding resolver returned no binding for required source SqlDelta from_snapshot_id=6 to_snapshot_id=7 node_id=10"
    );
}

#[test]
fn resolver_failure_precedes_invalid_physical_projection() {
    let err = match prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::DeltaWithInvalidProjection)
            .expect("sealed invalid delta fixture"),
        &ConnectorRegistry::new(),
        Some(&ErrorResolver),
    ) {
        Ok(_) => panic!("resolver error must win over physical projection error"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        "scan binding resolver failed for required source SqlDelta from_snapshot_id=6 to_snapshot_id=7 node_id=10: boom"
    );
}

#[test]
fn target_state_and_locator_reject_equality_deletes() {
    for (fixture, expected_kind) in [
        (NativeScanFixture::TargetLocatorProjection, "target-locator"),
        (NativeScanFixture::TargetStateProjection, "target-state"),
    ] {
        let plan = native_scan_plan(fixture).expect("sealed target fixture");
        let mut file = data_file("s3://bucket/target-data.parquet");
        file.deletes = vec![equality_delete_file(Vec::new(), vec![3])];
        let controls = novarocks::connector::FixtureControlResolver::new(registry(vec![file]));
        let err = match prepare_scan_bindings_with_controls(&plan, &controls, None) {
            Ok(_) => panic!("{expected_kind} equality-delete scan must fail"),
            Err(err) => err,
        };

        assert!(err.contains(expected_kind), "{err}");
        assert!(err.contains("does not support equality deletes"), "{err}");
    }
}

#[test]
fn resolver_execution_kind_must_match_semantic_source() {
    let resolver = StaticResolver {
        execution: resolved_delta(),
    };

    let err = match prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::ConnectorRead).expect("sealed connector-read fixture"),
        &ConnectorRegistry::new(),
        Some(&resolver),
    ) {
        Ok(_) => panic!("connector scan must reject delta execution"),
        Err(err) => err,
    };

    assert!(err.contains("SqlConnectorRead"), "{err}");
    assert!(err.contains("requires ConnectorRead execution"), "{err}");
    assert!(err.contains("node_id=10"), "{err}");
}

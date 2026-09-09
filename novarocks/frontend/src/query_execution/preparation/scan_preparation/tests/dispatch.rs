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

/// A topology-only planning pass must not reacquire a connector's current
/// control generation. It has to use the exact table handle and planning
/// lease admitted with the statement's query-local binding store instead.
struct RejectCurrentControlResolver;

impl novarocks_spi::connector::ConnectorControlResolver for RejectCurrentControlResolver {
    fn observe_current_binding(
        &self,
        _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorProviderBindingKey,
        novarocks_spi::connector::ConnectorError,
    > {
        panic!("topology-only re-planning must not observe a current connector binding")
    }

    fn observe_current_control_runtime(
        &self,
        _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlRuntimeId,
        novarocks_spi::connector::ConnectorError,
    > {
        panic!("topology-only re-planning must not observe a current control runtime")
    }

    fn acquire_current(
        &self,
        _instance_id: &novarocks_spi::connector::ConnectorInstanceId,
    ) -> Result<
        novarocks_spi::connector::ConnectorControlPlanningLease,
        novarocks_spi::connector::ConnectorError,
    > {
        panic!("topology-only re-planning must not acquire a current connector binding")
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

    let registry = FixtureConnectorRegistry::new();
    crate::connector::scan_model::register_planned_files_fixture(
        &registry,
        "ice",
        vec![data_file("s3://sqlx2-coalesce/frozen.parquet")],
        None,
    );
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let bindings = fixture_query_table_bindings(&distributed, &controls);
    // The delta lane resolves through the production query-local resolver, so
    // the fixture exercises exactly the lookup a refresh performs.
    let delta_resolver =
        crate::query_execution::planning::delta_scan::QueryTableBindingScanResolver::new(&bindings);
    let prepared = crate::query_execution::preparation::prepare_fragments(
        &distributed,
        &controls,
        &crate::connector::test_request_context(),
        Some(&bindings),
        Some(&delta_resolver),
        fixture_scan_preparation_options(fixture_control_role_host(&distributed, &controls)),
    )
    .expect("tokenized coalesce scans must prepare from exact bindings");
    for (node_id, _) in &scan_facts {
        assert!(prepared.scan_bindings().binding(*node_id).is_some());
    }
    assert_eq!(
        prepared.scan_bindings().negotiation_receipts().count(),
        scan_facts.len(),
        "every production typed scan must carry one frozen negotiation outcome"
    );
    let scheduling = prepared.scheduling_view();
    for (fragment_id, node_id, _) in prepared.scan_bindings().typed_scans() {
        assert!(
            scheduling.has_typed_connector_scan(fragment_id, node_id),
            "the scheduling projection must retain typed connector scan presence"
        );
    }
    let expected_ids = distributed
        .fragments()
        .iter()
        .map(|fragment| fragment.fragment_id)
        .collect::<std::collections::BTreeSet<_>>();
    let native = crate::query_execution::native_fragment::native_fragment_attachment_for_test(
        expected_ids.iter().copied().map(|fragment_id| {
            novarocks_proto_models::plan::PlanFragment {
                fragment_id,
                ..Default::default()
            }
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
        crate::connector::connector_request_context(None, Arc::new(AtomicBool::new(true)))
            .expect("cancelled request context");
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(registry.clone());
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let query_bindings = fixture_query_table_bindings(&plan, &controls);
    let err = match super::super::prepare_scan_bindings(
        &plan,
        &controls,
        &context,
        Some(&query_bindings),
        None,
        &fixture_scan_preparation_options(fixture_control_role_host(&plan, &controls)),
        &[],
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
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let bindings = fixture_query_table_bindings(&plan, &controls);

    let prepared = super::super::prepare_scan_bindings(
        &plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&bindings),
        None,
        &fixture_scan_preparation_options(fixture_control_role_host(&plan, &controls)),
        &[],
    )
    .expect("exact query binding must plan the scan");
    let typed = prepared.typed_scan(0, 10).expect("prepared typed scan");
    let DistributedNodeKind::Scan(scan) = &plan.fragments()[0].root.payload else {
        panic!("sealed fixture must retain its scan root");
    };
    let expected = bindings
        .exact_planning_lease(scan_preparation_facts(scan).binding())
        .expect("fixture exact lease");
    // The lease preparation retained and the catalog content the frozen
    // relation names are all the one the query already admitted -- never a
    // generation acquired now.
    assert_eq!(
        typed.planning_lease.binding().incarnation(),
        expected.binding().incarnation()
    );
    assert_eq!(
        typed.prepared.table_scan.table().catalog(),
        expected
            .binding()
            .catalog_handle()
            .expect("fixture lease carries a catalog handle")
    );
}

#[test]
fn negotiation_rejects_handle_from_another_admitted_runtime_generation() {
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let bindings = fixture_query_table_bindings(&plan, &controls);

    let error = match super::super::prepare_scan_bindings(
        &plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&bindings),
        None,
        &fixture_scan_preparation_options(foreign_fixture_control_role_host(&plan, &controls)),
        &[],
    ) {
        Ok(_) => {
            panic!("a foreign typed handle must not be relabeled as this query's admitted scan")
        }
        Err(error) => error,
    };

    assert!(
        error.contains("another admitted runtime generation"),
        "{error}"
    );
}

#[test]
fn frozen_description_binding_is_jointly_resolved_from_plan_token_and_statement_store() {
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let bindings = fixture_query_table_bindings(&plan, &controls);
    let sealed_plan = novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(plan);
    bindings.seal_for_topology_replan();
    let receipts = bindings
        .sealed_exact_binding_receipts()
        .expect("binding receipts are semantically sealed");

    let resolved = novarocks_query_application::preparation::resolve_plan_scan_bindings(
        &sealed_plan,
        &receipts,
    )
    .expect("sealed scan token must resolve through its exact statement store");
    let contract = sealed_plan
        .scan_contracts()
        .expect("sealed scan contracts")
        .remove(0);
    assert_eq!(resolved.len(), 1);
    assert_eq!(resolved[0].node_id(), 10);
    let object_parts = resolved[0].occurrence().binding().object().parts();
    assert_eq!(object_parts.len(), 3);
    assert_eq!(object_parts[0].as_ref(), contract.catalog());
    assert_eq!(object_parts[1].as_ref(), contract.namespace());
    assert_eq!(object_parts[2].as_ref(), contract.table());
}

#[test]
fn sqlx1_preparation_rejects_unbound_binding_instead_of_reacquiring_current() {
    let registry = registry(vec![data_file("s3://bucket/current.parquet")]);
    let controls = crate::connector::FixtureControlResolver::new(registry);
    let bindings = crate::catalog_application::query_bindings::QueryTableBindingStore::try_new()
        .expect("binding store");
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let error = match super::super::prepare_scan_bindings(
        &plan,
        &controls,
        &crate::connector::test_request_context(),
        Some(&bindings),
        None,
        &fixture_scan_preparation_options(fixture_control_role_host(&plan, &controls)),
        &[],
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
    assert!(
        bindings.typed_scan(0, 10).is_some(),
        "an ordinary current-snapshot scan lowers onto the typed stack"
    );
}

#[test]
fn topology_only_replanning_reuses_the_first_admitted_current_binding() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let registry = FixtureConnectorRegistry::new();
    crate::connector::scan_model::register_planned_files_fixture(
        &registry,
        "test_catalog",
        vec![data_file("s3://bucket/first-admission.parquet")],
        None,
    );
    let admission_controls = crate::connector::FixtureControlResolver::new(registry.clone());
    let bindings = fixture_query_table_bindings(&plan, &admission_controls);
    let DistributedNodeKind::Scan(scan) = &plan.fragments()[0].root.payload else {
        panic!("ordinary fixture must retain a scan root");
    };
    let admitted = bindings
        .binding(scan_preparation_facts(scan).binding())
        .expect("first admission binding");
    assert_eq!(
        admitted
            .scan_materialization
            .as_ref()
            .expect("first admission scan materialization")
            .selector,
        novarocks_spi::connector::ConnectorReadSelector::Current
    );
    let typed_control = fixture_control_role_host(&plan, &admission_controls);

    // A second registration stands for a catalog that moved on. The re-plan
    // must retain the table handle and exact planning lease stored above, so
    // it cannot observe this replacement at all.
    crate::connector::scan_model::register_planned_files_fixture(
        &registry,
        "test_catalog",
        vec![data_file("s3://bucket/new-current.parquet")],
        None,
    );
    let reject_current = RejectCurrentControlResolver;
    let context = crate::connector::test_request_context();

    let first = super::super::prepare_scan_bindings(
        &plan,
        &reject_current,
        &context,
        Some(&bindings),
        None,
        &fixture_scan_preparation_options(Arc::clone(&typed_control)),
        &[],
    )
    .expect("first preparation must use its admitted binding");
    let second = super::super::prepare_scan_bindings(
        &plan,
        &reject_current,
        &context,
        Some(&bindings),
        None,
        &super::super::ScanPreparationOptions::new(
            true,
            std::num::NonZeroUsize::new(3).expect("non-zero topology target"),
            None,
        )
        .with_typed_connector_control(
            typed_control,
            novarocks_spi::connector::read_stack::ConnectorSession::try_new(
                "fixture-query",
                "fixture-user",
                "UTC",
                "en_US",
                std::time::SystemTime::UNIX_EPOCH,
            )
            .expect("fixture connector session"),
        ),
        &[],
    )
    .expect("topology-only re-planning must reuse its admitted binding");

    // Preparation enumerates no split, so the observable retention is the
    // frozen generation itself: both passes install the exact generation the
    // first admission acquired, and the second pass never reached the
    // replacement registered above.
    let first_typed = first.typed_scan(0, 10).expect("first typed scan");
    let second_typed = second.typed_scan(0, 10).expect("second typed scan");
    let admitted_catalog = admitted
        .admission
        .exact_planning_lease()
        .expect("first admission lease")
        .binding()
        .catalog_handle()
        .expect("first admission has catalog handle")
        .clone();
    assert_eq!(first_typed.catalog_properties.handle(), &admitted_catalog);
    assert_eq!(second_typed.catalog_properties.handle(), &admitted_catalog);
    let first_relation = first_typed.prepared.table_scan.table().relation();
    let second_relation = second_typed.prepared.table_scan.table().relation();
    assert_eq!(second_relation.kind(), first_relation.kind());
    assert_eq!(
        second_relation.table().binding(),
        first_relation.table().binding(),
        "a topology-only re-plan must retain the same provider generation"
    );
    assert_eq!(
        second_relation.transaction().binding(),
        first_relation.transaction().binding(),
        "a topology-only re-plan must retain the same provider transaction generation"
    );
}

#[test]
fn duplicate_scan_node_defense_reports_exact_error() {
    let plan = native_scan_plan(NativeScanFixture::OrdinaryIcebergIdProjection)
        .expect("sealed ordinary fixture");
    let root = plan.fragments()[0].root.clone();
    let sealed_plan =
        novarocks_sql::planning::query_execution::SealedPreparationPlan::seal(plan.clone());
    let registry = registry(vec![data_file("s3://bucket/explicit.parquet")]);
    let mut seen_scan_node_ids = std::collections::BTreeSet::new();
    let mut bindings =
        crate::query_execution::preparation::scan::ScanExecutionBindings::for_sealed_plan(
            sealed_plan.clone(),
        );
    let context = crate::connector::test_request_context();
    let controls = crate::connector::FixtureControlResolver::new(registry.clone());
    let query_bindings = fixture_query_table_bindings(&plan, &controls);
    let limits = novarocks_query_application::observation::PreparationLimits::new(
        novarocks_query_application::observation::PreparationCountLimits::new(
            std::num::NonZeroU32::new(8).unwrap(),
            std::num::NonZeroUsize::new(8).unwrap(),
            std::num::NonZeroUsize::new(8).unwrap(),
            std::num::NonZeroUsize::new(8).unwrap(),
        ),
        novarocks_query_application::observation::PreparationByteLimits::new(
            std::num::NonZeroUsize::new(1024).unwrap(),
            std::num::NonZeroUsize::new(1024).unwrap(),
            std::num::NonZeroUsize::new(64 * 1024).unwrap(),
        ),
        context.deadline(),
    );
    let options = fixture_scan_preparation_options(fixture_control_role_host(&plan, &controls))
        .with_preparation_budget(
            novarocks_query_application::observation::PreparationBudget::new(limits),
        );

    query_bindings.seal_for_topology_replan();

    collect_scan_bindings(
        0,
        &root,
        &controls,
        &context,
        Some(&query_bindings),
        None,
        &options,
        &sealed_plan,
        &[],
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
        &options,
        &sealed_plan,
        &[],
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
        &FixtureConnectorRegistry::new(),
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
        &FixtureConnectorRegistry::new(),
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
        &FixtureConnectorRegistry::new(),
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
        &FixtureConnectorRegistry::new(),
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

/// A resolver may not hand one lane another lane's execution: a delta scan
/// answered with an ordinary admitted read would silently become a full read
/// of the relation instead of the difference between two snapshots.
#[test]
fn resolver_execution_kind_must_match_semantic_source() {
    let resolver = StaticResolver {
        execution: ResolvedScanExecution::AdmittedConnectorRead(
            crate::query_execution::preparation::scan::fixture_query_scan_materialization(
                "test_catalog",
            ),
        ),
    };

    let err = match prepare_scan_bindings(
        &native_scan_plan(NativeScanFixture::DeltaForPreparedBinding)
            .expect("sealed delta fixture"),
        &FixtureConnectorRegistry::new(),
        Some(&resolver),
    ) {
        Ok(_) => panic!("a delta scan must reject an ordinary admitted read"),
        Err(err) => err,
    };

    assert!(err.contains("SqlDelta"), "{err}");
    assert!(
        err.contains("requires AdmittedChangeWindow execution"),
        "{err}"
    );
    assert!(err.contains("node_id=10"), "{err}");
}

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

//! Original catalogue ownership through real completion paths.

use super::tests::{
    FrozenOrdersCatalog, answer_catalog, answer_exact_statistics, answer_provider,
    available_statistics_evidence, incomplete, provider_contract, request,
};
use super::*;
use crate::compiler::{
    SessionOptimizerSettings, builtin_sql_function_catalog, noop_constant_evaluator,
};
use std::sync::{
    Weak,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Debug, Default)]
struct CatalogObservations {
    snapshots: AtomicUsize,
    aggregate_bindings: AtomicUsize,
    exact_selections: AtomicUsize,
    latest: std::sync::Mutex<Option<Weak<dyn SqlFunctionCatalog>>>,
}
#[derive(Debug)]
struct CountingCatalog {
    inner: Arc<dyn SqlFunctionCatalog>,
    observations: Arc<CatalogObservations>,
    assert_capture_before_binding: bool,
}
impl CountingCatalog {
    fn new(inner: Arc<dyn SqlFunctionCatalog>, assert_capture_before_binding: bool) -> Arc<Self> {
        Arc::new(Self {
            inner,
            observations: Arc::new(CatalogObservations::default()),
            assert_capture_before_binding,
        })
    }
    fn count(&self) -> usize {
        self.observations.snapshots.load(Ordering::SeqCst)
    }
    fn last_snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        self.observations
            .latest
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .upgrade()
            .expect("the original analyzed snapshot must remain owned")
    }
}
impl SqlFunctionCatalog for CountingCatalog {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        self.observations.snapshots.fetch_add(1, Ordering::SeqCst);
        // Each capture deliberately has a distinct outer owner. Actual installed
        // semantics delegate unchanged to the one immutable underlying engine.
        let captured: Arc<dyn SqlFunctionCatalog> = Arc::new(Self {
            inner: self.inner.clone(),
            observations: self.observations.clone(),
            assert_capture_before_binding: self.assert_capture_before_binding,
        });
        *self.observations.latest.lock().unwrap() = Some(Arc::downgrade(&captured));
        captured
    }
    fn snapshot_for_scalar_presence(&self) -> Arc<dyn SqlFunctionCatalog> {
        // Keep the observer outside the real admitted catalogue. Snapshot
        // observations must identify the actual wrapper used by optimization,
        // rather than its unwrapped metadata source. The scope does not prepare
        // an owner or replace any binding/implementation declaration.
        Arc::new(Self {
            inner: self.inner.snapshot_for_scalar_presence(),
            observations: self.observations.clone(),
            assert_capture_before_binding: self.assert_capture_before_binding,
        })
    }
    fn admit_bound_lifecycle_observed(
        &self,
        binding: &novarocks_functions::ResolvedFunctionBinding,
        lifecycle: novarocks_functions::PureCallLifecycle,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        self.inner
            .admit_bound_lifecycle_observed(binding, lifecycle, control)
    }
    fn admit_bound_environment_observed(
        &self,
        binding: &novarocks_functions::ResolvedFunctionBinding,
        environment: &[novarocks_type_contract::SemanticParameterRef],
        parameters: &novarocks_type_contract::SemanticParameters,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        self.inner
            .admit_bound_environment_observed(binding, environment, parameters, control)
    }
    fn admit_authored_environment_observed(
        &self,
        binding: &crate::binding::SqlFunctionBinding,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        self.inner
            .admit_authored_environment_observed(binding, control)
    }
    fn admit_native_bitnot_source_observed(
        &self,
        source: &novarocks_functions::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), novarocks_functions::FunctionBindingError> {
        self.inner
            .admit_native_bitnot_source_observed(source, control)
    }
    fn select_exact_overload_observed(
        &self,
        function_id: &novarocks_functions::FunctionId,
        kind: novarocks_functions::FunctionKind,
        overload: &novarocks_functions::FunctionOverloadId,
        request: novarocks_functions::FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        Arc<novarocks_functions::FunctionBindingSelection>,
        novarocks_functions::FunctionBindingError,
    > {
        self.observations
            .exact_selections
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .select_exact_overload_observed(function_id, kind, overload, request, control)
    }
    fn pure_overload_declaration_observed<'a>(
        &'a self,
        function_id: &novarocks_functions::FunctionId,
        kind: novarocks_functions::FunctionKind,
        overload: &novarocks_functions::FunctionOverloadId,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureOverloadDeclaration<'a>,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        self.inner
            .pure_overload_declaration_observed(function_id, kind, overload, control)
    }
    fn prepare_fresh_selected(
        &self,
        input: novarocks_functions::CallEffectInput<'_>,
        selected: Arc<novarocks_functions::FunctionBindingSelection>,
        options: novarocks_functions::PureCallPreparation,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::PureCallSpecialization,
        novarocks_functions::FunctionSpecializationFailure,
    > {
        self.inner
            .prepare_fresh_selected(input, selected, options, control)
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner
            .resolve_scalar_signature(name, arg_types, control)
    }
    fn resolve_scalar_binding(
        &self,
        name: &str,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner.resolve_scalar_binding(name, arguments, control)
    }
    fn resolve_scalar_binding_with_expected_result(
        &self,
        name: &str,
        arguments: &[novarocks_functions::FunctionArgument],
        expected: &novarocks_functions::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner
            .resolve_scalar_binding_with_expected_result(name, arguments, expected, control)
    }
    fn resolve_value_conversion_binding(
        &self,
        argument: &novarocks_functions::FunctionArgument,
        target: &novarocks_functions::FunctionValueType,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner
            .resolve_value_conversion_binding(argument, target, control)
    }
    fn resolve_window_binding(
        &self,
        name: &str,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner.resolve_window_binding(name, arguments, control)
    }
    fn resolve_table_binding(
        &self,
        name: &str,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        self.inner.resolve_table_binding(name, arguments, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.inner.contains_aggregate(name)
    }
    fn resolve_aggregate_binding(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        if self.assert_capture_before_binding {
            assert!(self.observations.snapshots.load(Ordering::SeqCst) > 0);
        }
        self.observations
            .aggregate_bindings
            .fetch_add(1, Ordering::SeqCst);
        self.inner
            .resolve_aggregate_binding(name, logical_argument_count, arguments, control)
    }
    fn resolve_aggregate_binding_trusted(
        &self,
        name: &str,
        logical_argument_count: usize,
        arguments: &[novarocks_functions::FunctionArgument],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedFunctionBinding,
        novarocks_functions::FunctionBindingError,
    > {
        if self.assert_capture_before_binding {
            assert!(self.observations.snapshots.load(Ordering::SeqCst) > 0);
        }
        self.observations
            .aggregate_bindings
            .fetch_add(1, Ordering::SeqCst);
        self.inner.resolve_aggregate_binding_trusted(
            name,
            logical_argument_count,
            arguments,
            control,
        )
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner
            .resolve_aggregate_signature(name, arg_types, control)
    }
    fn resolve_aggregate_update_signature(
        &self,
        name: &str,
        logical_arg_types: &[arrow::datatypes::DataType],
        update_arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner.resolve_aggregate_update_signature(
            name,
            logical_arg_types,
            update_arg_types,
            control,
        )
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        arg_types: &[arrow::datatypes::DataType],
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<
        novarocks_functions::ResolvedAggregateSignature,
        novarocks_functions::FunctionResolutionError,
    > {
        self.inner
            .resolve_aggregate_trusted(name, arg_types, control)
    }
    fn volatility(&self, name: &str) -> novarocks_functions::FunctionVolatility {
        self.inner.volatility(name)
    }
}

fn tracked_request(sql: &str, catalog: &Arc<CountingCatalog>) -> SqlFinalPlanCompileRequest {
    let template = request(sql, SqlCompileIntent::Query);
    SqlFinalPlanCompileRequest::new(
        template.version,
        template.statement,
        template.intent,
        template.session,
        template.environment,
        catalog.clone(),
        template.constant_evaluator,
        template.constant_policy,
        template.control,
        template.dop_domain,
        template.scan_read_budget,
        template.limits,
    )
}

#[test]
fn catalogue_query_no_io_retains_the_analyzed_owner_through_clone_and_parts() {
    let catalog = CountingCatalog::new(builtin_sql_function_catalog().snapshot(), false);
    let input = tracked_request("SELECT ABS(CAST(-4 AS BIGINT))", &catalog);
    let request_snapshot = input.functions.clone();
    assert_eq!(catalog.count(), 1);
    let completed = SqlCompiler::start(
        input.try_into_completion().expect("completion seed"),
        &SqlCompileControl::unbounded(),
    )
    .expect("actual completion")
    .into_complete()
    .expect("no external observations");
    let analyzed_snapshot = catalog.last_snapshot();
    assert!(!Arc::ptr_eq(&request_snapshot, &analyzed_snapshot));
    assert_eq!(
        catalog.count(),
        2,
        "the original analysis capture remains unchanged"
    );
    let (owner, _, _) = completed.into_parts();
    let owner_clone = owner.clone();
    assert!(Arc::ptr_eq(owner.function_catalog(), &analyzed_snapshot));
    assert!(Arc::ptr_eq(
        owner_clone.function_catalog(),
        &analyzed_snapshot
    ));
    assert!(Arc::ptr_eq(owner.plan_arc(), owner_clone.plan_arc()));
    assert_eq!(catalog.count(), 2, "publication and clone never recapture");
    assert!(
        catalog.observations.exact_selections.load(Ordering::SeqCst) > 0,
        "the retained original owner must author actual late selections"
    );
}

#[test]
fn catalogue_query_provider_completion_moves_the_actual_optimizer_snapshot() {
    let catalog = CountingCatalog::new(builtin_sql_function_catalog().snapshot(), false);
    let input = tracked_request("SELECT MIN(order_key) FROM orders", &catalog);
    let request_snapshot = input.functions.clone();
    let pending = incomplete(
        SqlCompiler::start(
            input.try_into_completion().expect("completion seed"),
            &SqlCompileControl::unbounded(),
        )
        .expect("catalogue need"),
    );
    let statistics = incomplete(answer_catalog(pending));
    let provider = incomplete(answer_exact_statistics(statistics, 13));
    let optimizer_snapshot = catalog.last_snapshot();
    let captures = catalog.count();
    assert_eq!(captures, 2);
    assert!(!Arc::ptr_eq(&request_snapshot, &optimizer_snapshot));
    let owner = answer_provider(provider)
        .into_complete()
        .expect("complete")
        .into_plan();
    assert!(Arc::ptr_eq(owner.function_catalog(), &optimizer_snapshot));
    assert!(
        catalog
            .observations
            .aggregate_bindings
            .load(Ordering::SeqCst)
            > 0
    );
    let clone = owner.clone();
    drop(owner);
    assert!(Arc::ptr_eq(clone.function_catalog(), &optimizer_snapshot));
    assert_eq!(
        catalog.count(),
        captures,
        "provider and final lowering reuse the admitted owner"
    );
}

#[test]
fn catalogue_dml_read_completion_retains_its_optimized_owner_without_recapture() {
    let catalog = CountingCatalog::new(builtin_sql_function_catalog().snapshot(), false);
    let control = SqlCompileControl::unbounded();
    let analyzed = SqlCompiler::analyze(crate::compiler::SqlAnalyzeRequest::new(
        SqlStatementInput::parsed_query(crate::compiler::test_query(
            "SELECT MIN(order_key) FROM orders",
        )),
        SqlCompileIntent::Query,
        request("SELECT 1", SqlCompileIntent::Query).session,
        SqlPlanningEnvironment::Distributed,
        &FrozenOrdersCatalog,
        catalog.as_ref(),
        noop_constant_evaluator(),
        None,
        crate::constant::test_constant_policy(),
        control.clone(),
    ))
    .expect("actual analysis")
    .into_pending()
    .expect("analyzed source");
    let statistics = DmlStatisticsSnapshot::from_evidence([available_statistics_evidence(13)]);
    let (completion, needs) = crate::planning::dml::begin_final_dml_read_plan(
        crate::compiler::SqlOptimizeRequest::new(analyzed, &statistics, control.clone()),
        &SessionOptimizerSettings::default(),
    )
    .expect("actual DML optimization");
    let optimizer_snapshot = catalog.last_snapshot();
    let captures = catalog.count();
    assert_eq!(captures, 1);
    let reads =
        crate::planning::dml::DmlFinalizedProviderReadSet::try_new(needs.iter().map(|need| {
            crate::planning::dml::DmlFinalizedProviderRead {
                fact: ProviderReadFact::negotiated(need, provider_contract(need))
                    .expect("provider fact"),
                read_budget: ScanReadBudget {
                    max_batch_rows: MAX_SCAN_BATCH_ROWS,
                    max_batch_bytes: MAX_SCAN_BATCH_BYTES,
                },
            }
        }))
        .expect("actual provider reads");
    let owner = completion
        .finish(
            PlanVersionId::try_new([24; 16]).expect("version"),
            PipelineDopDomain {
                min: 1,
                max: 8,
                requires_power_of_two: true,
            },
            reads,
            &control,
        )
        .expect("actual DML final plan");
    assert!(Arc::ptr_eq(owner.function_catalog(), &optimizer_snapshot));
    assert_eq!(catalog.count(), captures);
    assert!(
        owner
            .plan()
            .fragments()
            .values()
            .any(|fragment| fragment.nodes().values().any(|node| {
                matches!(
                    &node.kind,
                    novarocks_physical_plan::NodeKind::Aggregate { .. }
                )
            }))
    );
}

#[test]
fn catalogue_analyze_captures_once_before_bindings_and_retains_that_exact_owner() {
    use novarocks_spi::connector::{
        StatisticsArtifactIdentity, StatisticsRequiredAggregation, StatisticsScanColumn,
    };
    let exact = crate::functions::test_installed_blob_aggregate_catalog();
    let inner: Arc<dyn SqlFunctionCatalog> = Arc::new(exact);
    let catalog = CountingCatalog::new(inner, true);
    let input_type = novarocks_type_contract::FunctionValueType::new(DataType::Int64, true);
    let requirement = StatisticsRequiredAggregation::try_new(
        StatisticsScanColumn::try_new(0, "id", input_type.clone()).expect("input"),
        novarocks_connector_iceberg_functions::ICEBERG_THETA_AGGREGATE_NAME,
        StatisticsArtifactIdentity::try_new(vec![7], "test-blob-v1").expect("artifact"),
    )
    .expect("requirement");
    let scan = crate::planning::dml::StatisticsConnectorScan {
        binding: crate::binding::SqlTableBindingId::new_for_test(1),
        catalog: "iceberg".into(),
        namespace: "db".into(),
        table: "t".into(),
        version_ordinal: 42,
        columns: vec![StatisticsScanColumn::try_new(0, "id", input_type).expect("column")],
    };
    let owner = crate::planning::dml::build_final_statistics_connector_plan(
        scan,
        &[requirement],
        catalog.as_ref(),
        &SessionOptimizerSettings::default(),
        crate::planning::dml::statistics_final_context_for_test(),
        novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
        false,
        crate::constant::test_constant_policy(),
        &SqlCompileControl::unbounded(),
    )
    .expect("actual generated ANALYZE source");
    let snapshot = catalog.last_snapshot();
    let original: Arc<dyn SqlFunctionCatalog> = catalog.clone();
    assert!(!Arc::ptr_eq(&original, &snapshot));
    assert!(Arc::ptr_eq(owner.function_catalog(), &snapshot));
    assert_eq!(catalog.count(), 1);
    assert!(
        catalog
            .observations
            .aggregate_bindings
            .load(Ordering::SeqCst)
            > 0
    );
    let clone = owner.clone();
    assert!(Arc::ptr_eq(clone.function_catalog(), &snapshot));
    assert_eq!(catalog.count(), 1);
}

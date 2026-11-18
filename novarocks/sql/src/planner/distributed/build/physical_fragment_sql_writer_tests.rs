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

//! Actual Writer statistics publication through the retained SQL catalogue.

use super::super::contract_lowering::{NodeExpressionEffects, pure_node_effects_for_test};
use super::super::{
    contract_lowering::lowered_writer_state_tests::{authored, authored_with_catalog, final_call},
    expression_occurrences::author_physical_occurrences_observed,
    lowered_draft::AggregateStateTransport,
};
use super::*;
use crate::compiler::SqlAuthoredPhysicalPlan;

use arrow::datatypes::DataType;
use novarocks_connector_iceberg_functions::{
    ICEBERG_THETA_STATE_FORMAT_IDENTITY, iceberg_theta_registration,
};
use novarocks_functions::{
    CallEffectInput, EngineFunctionCatalogBuilder, FunctionArgument, FunctionBindingError,
    FunctionBindingRequest, FunctionBindingSelection, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResolutionError, FunctionSpecializationFailure, FunctionVolatility,
    PureCallPreparation, PureCallSpecialization, PureOverloadDeclaration,
    ResolvedAggregateSignature, ResolvedFunctionBinding, ResolvedFunctionSignature,
};
use novarocks_type_contract::{
    AggregateStateFormatId, ArgumentControl, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, ObservableEffects,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let index = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(index <= stop, "callback after originating control refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if index == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn causes() -> [CompileControlError; 3] {
    [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ]
}
fn writer_call(
    node: &PhysicalNode,
    site: PhysicalCallSite,
) -> &novarocks_physical_plan::WriterAggregateCall {
    match (site, &node.kind) {
        (PhysicalCallSite::WriterPartial { node: id, call }, NodeKind::TableWriter { target })
            if id == node.id =>
        {
            &target.partial_aggregates[call as usize]
        }
        (PhysicalCallSite::WriterFinal { node: id, call }, NodeKind::TableFinish(finish))
            if id == node.id =>
        {
            &finish.final_aggregates[call as usize]
        }
        _ => panic!("not an actual Writer lifecycle"),
    }
}
fn writer_fragments(owner: &SqlAuthoredPhysicalPlan) -> impl Iterator<Item = &Fragment> {
    owner.plan().fragments().values().filter(|fragment| {
        fragment.nodes().values().any(|node| match &node.kind {
            NodeKind::TableWriter { target } => !target.partial_aggregates.is_empty(),
            NodeKind::TableFinish(finish) => !finish.final_aggregates.is_empty(),
            _ => false,
        })
    })
}
fn scopes<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    occurrences: &AuthoredPhysicalOccurrences<'_>,
) -> (
    ConstantPolicy,
    BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'a>>,
) {
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let mut policy = None;
    let mut result = BTreeMap::new();
    for &(site, context) in &occurrences.relational_contexts {
        let node_id = match site {
            PhysicalCallSite::WriterPartial { node, .. }
            | PhysicalCallSite::WriterFinal { node, .. } => node,
            _ => panic!("the genuine Writer fixture contains a different call family"),
        };
        let node = &fragment.nodes()[&node_id];
        let call = writer_call(node, site);
        let entry = owner
            .checked_writer_aggregate_source_observed(fragment, node, site, call, &mut work)
            .unwrap();
        let captured = entry.captured();
        match policy {
            None => policy = Some(captured.constant_policy()),
            Some(original) => assert_eq!(original, captured.constant_policy()),
        }
        result.insert(
            site,
            PhysicalRelationalCallSourceScope {
                source: node,
                decimal_overflow_policy: captured.binding().decimal_overflow_policy(),
                // The installed original Theta declaration has no environment keys.
                environment: &[],
                proof_scope: CallProofScope::Relation(context.identity),
            },
        );
    }
    work.finish().unwrap();
    (policy.expect("actual Writer statistics"), result)
}
fn input<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    occurrences: &'a AuthoredPhysicalOccurrences<'a>,
    policy: ConstantPolicy,
    relational: &'a BTreeMap<PhysicalCallSite, PhysicalRelationalCallSourceScope<'a>>,
    expressions: &'a BTreeMap<novarocks_physical_plan::NodeId, NodeExpressionEffects>,
) -> PhysicalFragmentEffectsInput<'a> {
    PhysicalFragmentEffectsInput {
        fragment,
        occurrences,
        constants: owner.plan().constants(),
        parameters: owner.plan().parameters(),
        literal_policy: policy,
        node_effects: expressions,
        relational_scopes: relational,
    }
}
fn prefixes(
    mut invoke: impl FnMut(
        &Control,
    ) -> Result<AuthoredPhysicalFragmentEffects, PhysicalFragmentEffectsError>,
    success: bool,
) {
    let baseline = Control::default();
    let result = invoke(&baseline);
    assert_eq!(result.is_ok(), success, "{result:?}");
    let expected = baseline.trace.into_inner().unwrap();
    assert!(expected.len() >= 2);
    for stop in 0..expected.len() {
        for cause in causes() {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(invoke(&control), Err(PhysicalFragmentEffectsError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=stop]);
        }
    }
}
fn assert_fresh(owner: &SqlAuthoredPhysicalPlan, expected: (usize, usize)) {
    let mut partial = 0;
    let mut final_count = 0;
    for fragment in writer_fragments(owner) {
        let occurrences = author_physical_occurrences_observed(
            fragment,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        assert!(std::ptr::eq(occurrences.fragment(), fragment));
        let (policy, relational) = scopes(owner, fragment, &occurrences);
        let expressions = pure_node_effects_for_test(fragment);
        let result = author_sql_fragment_effects_observed(
            owner.relational_sources(),
            input(
                owner,
                fragment,
                &occurrences,
                policy,
                &relational,
                &expressions,
            ),
            &Control::default(),
        )
        .unwrap();
        assert_eq!(
            result.calls.entries().len(),
            occurrences.relational_contexts.len()
        );
        let writer_uses = occurrences.writer_value_uses().collect::<Vec<_>>();
        assert_eq!(writer_uses.len(), occurrences.relational_contexts.len());
        for &(site, context) in &occurrences.relational_contexts {
            let id = match site {
                PhysicalCallSite::WriterPartial { node, .. } => {
                    partial += 1;
                    node
                }
                PhysicalCallSite::WriterFinal { node, .. } => {
                    final_count += 1;
                    node
                }
                _ => panic!("Writer"),
            };
            let call = writer_call(&fragment.nodes()[&id], site);
            let &(value_site, value, value_context) =
                writer_uses.iter().find(|entry| entry.0 == site).unwrap();
            assert_eq!(value_site, site);
            assert!(std::ptr::eq(value, &fragment.values()[&call.input]));
            assert_eq!(value_context.identity.call, context.identity.call);
            assert_ne!(value_context.identity.role, context.identity.role);
            assert!(value_context.is_materialized_input_of(context));
            assert_eq!(
                value_context.demand,
                novarocks_type_contract::EvaluationDemand::Value
            );
            // Materialized relation inputs have their own typed role. They
            // cannot masquerade as scalar uses or callable relation contexts.
            assert!(
                !novarocks_type_contract::CallEffectContext::Relation(value_context)
                    .matches_kind(FunctionKind::Scalar)
            );
            assert!(!value_context.is_call());
            assert_eq!(
                call.binding.state_format.as_str(),
                ICEBERG_THETA_STATE_FORMAT_IDENTITY
            );
            let frozen = &result.calls.entries()[&site];
            assert_eq!(frozen.site, site);
            assert_eq!(frozen.context, context);
            let original = occurrences
                .writer_value_uses()
                .find(|(actual, _, _)| *actual == site)
                .unwrap()
                .2;
            assert_eq!(frozen.materialized_writer_input, Some(original));
            assert!(original.is_materialized_input_of(frozen.context));
            assert!(
                !novarocks_type_contract::CallEffectContext::Relation(original)
                    .matches_kind(FunctionKind::Aggregate)
            );

            assert_eq!(
                frozen.decimal_overflow_policy,
                relational[&site].decimal_overflow_policy
            );
            assert_eq!(
                frozen.effects.value_stability,
                FunctionVolatility::Immutable
            );
            assert_eq!(
                frozen.effects.own_row_error,
                FunctionIntrinsicRowError::NotRowEvaluated
            );
            assert_eq!(
                frozen.effects.failure_behavior,
                FunctionFailureBehavior::Propagate
            );
            assert_eq!(
                frozen.effects.null_behavior,
                FunctionNullBehavior::CalledOnNull
            );
            assert_eq!(frozen.effects.argument_control, ArgumentControl::Aggregate);
            assert_eq!(
                frozen.effects.instance_state,
                FunctionInstanceState::AggregateInstance
            );
            assert_eq!(frozen.effects.observable_effects, ObservableEffects::NONE);
            assert!(frozen.effects.environment.is_empty());
            assert_eq!(
                frozen.effects.proof_scope,
                CallProofScope::Relation(context.identity)
            );
        }
        result
            .calls
            .validate_fragment(fragment, &Control::default())
            .unwrap();
    }
    assert_eq!((partial, final_count), expected);
}

#[test]
fn real_writer_statistics_single_stream_publishes_partial_and_final_fresh_facts() {
    let owner = authored(&[1], &[false]);
    assert_fresh(&owner, (1, 1));
    let cloned = owner.clone();
    assert!(Arc::ptr_eq(owner.plan_arc(), cloned.plan_arc()));
    assert!(Arc::ptr_eq(
        owner.function_catalog(),
        cloned.function_catalog()
    ));
    assert_fresh(&cloned, (1, 1));
}

#[test]
fn real_writer_shared_channel_accepts_both_independent_original_contributors() {
    let owner = authored(&[1, 1], &[false, false]);
    assert_fresh(&owner, (2, 1));
    let (fragment, node, site, call) = final_call(&owner, 0);
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let final_entry = owner
        .checked_writer_aggregate_source_observed(fragment, node, site, call, &mut work)
        .unwrap();
    let mut emissions = Vec::new();
    let mut routes = Vec::new();
    final_entry
        .state_inputs_observed(&mut work)
        .unwrap()
        .visit_observed(
            &mut work,
            |producer, endpoint, _| {
                assert!(
                    producer
                        .canonical()
                        .unwrap()
                        .belongs_to(producer.captured())
                );
                assert!(matches!(
                    &producer.captured().request().arguments[0],
                    FunctionArgument::Value { constant: None, .. }
                ));
                emissions.push((
                    producer.captured().logical_identity().clone(),
                    std::ptr::from_ref(producer.captured()),
                    endpoint,
                ));
                Ok::<_, SqlSourceJournalError>(())
            },
            |_, kind, links, _| {
                routes.push((kind, links.len()));
                Ok(())
            },
            |_, _| panic!("both real Writers contribute to this auxiliary channel"),
        )
        .unwrap();
    work.finish().unwrap();
    assert_eq!(emissions.len(), 2);
    assert!(!emissions[0].0.same_lineage(&emissions[1].0));
    assert!(
        emissions
            .iter()
            .all(|entry| entry.1 != std::ptr::from_ref(final_entry.captured()))
    );
    assert_eq!(routes.len(), 3);
    assert!(matches!(routes[0], (AggregateStateTransport::UnionAll, 2)));
    assert!(
        routes[1..]
            .iter()
            .all(|entry| matches!(entry, (AggregateStateTransport::Stream(_), 1)))
    );
}

#[test]
fn real_writer_sparse_repeated_channels_preserve_no_contribution_without_fake_arguments() {
    let owner = authored(&[2, 1], &[false, false]);
    assert_fresh(&owner, (3, 2));
    let (fragment, node, site, call) = final_call(&owner, 1);
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let entry = owner
        .checked_writer_aggregate_source_observed(fragment, node, site, call, &mut work)
        .unwrap();
    let mut emissions = 0;
    let mut absent = 0;
    entry
        .state_inputs_observed(&mut work)
        .unwrap()
        .visit_observed(
            &mut work,
            |producer, _, _| {
                emissions += 1;
                assert!(matches!(
                    &producer.captured().request().arguments[0],
                    FunctionArgument::Value { constant: None, .. }
                ));
                Ok::<_, SqlSourceJournalError>(())
            },
            |_, _, _, _| Ok(()),
            |_, _| {
                absent += 1;
                Ok(())
            },
        )
        .unwrap();
    work.finish().unwrap();
    assert_eq!((emissions, absent), (1, 1));
    assert!(matches!(
        &entry.captured().request().arguments[0],
        FunctionArgument::Value { constant: None, .. }
    ));
}

#[test]
fn real_writer_fragment_all_success_and_policy_refusal_callbacks_keep_original_control() {
    let owner = authored(&[1], &[false]);
    for fragment in writer_fragments(&owner) {
        let occurrences = author_physical_occurrences_observed(
            fragment,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        let (policy, mut relational) = scopes(&owner, fragment, &occurrences);
        let expressions = pure_node_effects_for_test(fragment);
        prefixes(
            |control| {
                author_sql_fragment_effects_observed(
                    owner.relational_sources(),
                    input(
                        &owner,
                        fragment,
                        &occurrences,
                        policy,
                        &relational,
                        &expressions,
                    ),
                    control,
                )
            },
            true,
        );
        for source in relational.values_mut() {
            source.decimal_overflow_policy = match source.decimal_overflow_policy {
                DecimalOverflowPolicy::OutputNull => DecimalOverflowPolicy::ReportError,
                DecimalOverflowPolicy::ReportError => DecimalOverflowPolicy::OutputNull,
            };
        }
        assert!(
            author_sql_fragment_effects_observed(
                owner.relational_sources(),
                input(
                    &owner,
                    fragment,
                    &occurrences,
                    policy,
                    &relational,
                    &expressions
                ),
                &Control::default()
            )
            .is_err()
        );
        prefixes(
            |control| {
                author_sql_fragment_effects_observed(
                    owner.relational_sources(),
                    input(
                        &owner,
                        fragment,
                        &occurrences,
                        policy,
                        &relational,
                        &expressions,
                    ),
                    control,
                )
            },
            false,
        );
    }
}

#[test]
fn real_writer_fragment_refuses_equal_foreign_occurrence_source_and_missing_scopes() {
    let owner = authored(&[1], &[false]);
    for fragment in writer_fragments(&owner) {
        let occurrences = author_physical_occurrences_observed(
            fragment,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        let (policy, mut relational) = scopes(&owner, fragment, &occurrences);
        let expressions = pure_node_effects_for_test(fragment);
        let foreign = fragment.clone();
        assert_eq!(foreign.id(), fragment.id());
        assert_eq!(foreign.nodes(), fragment.nodes());
        let foreign_occurrences = author_physical_occurrences_observed(
            &foreign,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        let mut source = input(
            &owner,
            fragment,
            &occurrences,
            policy,
            &relational,
            &expressions,
        );
        source.occurrences = &foreign_occurrences;
        assert!(
            author_sql_fragment_effects_observed(
                owner.relational_sources(),
                source,
                &Control::default()
            )
            .is_err()
        );
        prefixes(
            |control| {
                let mut source = input(
                    &owner,
                    fragment,
                    &occurrences,
                    policy,
                    &relational,
                    &expressions,
                );
                source.occurrences = &foreign_occurrences;
                author_sql_fragment_effects_observed(owner.relational_sources(), source, control)
            },
            false,
        );
        let (missing, _) = relational.pop_first().unwrap();
        assert!(
            matches!(author_sql_fragment_effects_observed(owner.relational_sources(),
            input(&owner, fragment, &occurrences, policy, &relational, &expressions), &Control::default()),
            Err(PhysicalFragmentEffectsError::MissingScope(site)) if site == missing)
        );
    }
}

#[derive(Clone, Copy, Debug)]
enum SelectionFault {
    StateFormat,
    NominalState,
}
#[derive(Debug, Default)]
struct SelectionObservations {
    fault: Option<(usize, SelectionFault)>,
    requests: usize,
    bad_returns: usize,
}
#[derive(Debug)]
struct LateSelectionCatalog {
    inner: Arc<dyn SqlFunctionCatalog>,
    observations: Arc<Mutex<SelectionObservations>>,
}
impl LateSelectionCatalog {
    fn arm(&self, at: usize, fault: SelectionFault) {
        *self.observations.lock().unwrap() = SelectionObservations {
            fault: Some((at, fault)),
            ..Default::default()
        };
    }
}
// This adversarial return fixture keeps the original real Theta catalogue and
// source construction. It is not a new implementation or a valid declaration.
impl SqlFunctionCatalog for LateSelectionCatalog {
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(Self {
            inner: self.inner.clone(),
            observations: self.observations.clone(),
        })
    }
    fn select_exact_overload_observed(
        &self,
        function: &FunctionId,
        kind: FunctionKind,
        overload: &FunctionOverloadId,
        request: FunctionBindingRequest<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError> {
        let selected = self
            .inner
            .select_exact_overload_observed(function, kind, overload, request, control)?;
        let fault = {
            let mut observations = self.observations.lock().unwrap();
            observations.requests += 1;
            observations
                .fault
                .filter(|(at, _)| *at == observations.requests)
                .map(|(_, fault)| {
                    observations.bad_returns += 1;
                    fault
                })
        };
        let Some(fault) = fault else {
            return Ok(selected);
        };
        let mut bad = selected.as_ref().clone();
        let state = bad.aggregate.as_mut().unwrap();
        match fault {
            SelectionFault::StateFormat => {
                state.state_format =
                    AggregateStateFormatId::try_new("test/adversarial-late-state/v1").unwrap()
            }
            SelectionFault::NominalState => {
                state.intermediate_type =
                    novarocks_type_contract::FunctionValueType::try_with_logical_type(
                        state.intermediate_type.data_type().clone(),
                        state.intermediate_type.nullable(),
                        novarocks_type_contract::ValueLogicalType::Bitmap,
                    )
                    .unwrap()
            }
        }
        Ok(Arc::new(bad))
    }
    fn pure_overload_declaration_observed<'a>(
        &'a self,
        function: &FunctionId,
        kind: FunctionKind,
        overload: &FunctionOverloadId,
        control: &dyn PureCompileControl,
    ) -> Result<PureOverloadDeclaration<'a>, FunctionSpecializationFailure> {
        self.inner
            .pure_overload_declaration_observed(function, kind, overload, control)
    }
    fn prepare_fresh_selected(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        self.inner
            .prepare_fresh_selected(input, selected, options, control)
    }
    fn resolve_scalar_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionSignature, FunctionResolutionError> {
        self.inner.resolve_scalar_signature(name, args, control)
    }
    fn resolve_scalar_binding(
        &self,
        name: &str,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.inner.resolve_scalar_binding(name, args, control)
    }
    fn resolve_scalar_binding_with_expected_result(
        &self,
        name: &str,
        args: &[FunctionArgument],
        expected: &novarocks_type_contract::FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.inner
            .resolve_scalar_binding_with_expected_result(name, args, expected, control)
    }
    fn resolve_value_conversion_binding(
        &self,
        arg: &FunctionArgument,
        target: &novarocks_type_contract::FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.inner
            .resolve_value_conversion_binding(arg, target, control)
    }
    fn resolve_window_binding(
        &self,
        name: &str,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.inner.resolve_window_binding(name, args, control)
    }
    fn resolve_table_binding(
        &self,
        name: &str,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.inner.resolve_table_binding(name, args, control)
    }
    fn contains_aggregate(&self, name: &str) -> bool {
        self.inner.contains_aggregate(name)
    }
    fn resolve_aggregate_binding(
        &self,
        name: &str,
        logical: usize,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.inner
            .resolve_aggregate_binding(name, logical, args, control)
    }
    fn resolve_aggregate_binding_trusted(
        &self,
        name: &str,
        logical: usize,
        args: &[FunctionArgument],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedFunctionBinding, FunctionBindingError> {
        self.inner
            .resolve_aggregate_binding_trusted(name, logical, args, control)
    }
    fn resolve_aggregate_signature(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.inner.resolve_aggregate_signature(name, args, control)
    }
    fn resolve_aggregate_update_signature(
        &self,
        name: &str,
        logical: &[DataType],
        update: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.inner
            .resolve_aggregate_update_signature(name, logical, update, control)
    }
    fn resolve_aggregate_trusted(
        &self,
        name: &str,
        args: &[DataType],
        control: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.inner.resolve_aggregate_trusted(name, args, control)
    }
    fn volatility(&self, name: &str) -> FunctionVolatility {
        self.inner.volatility(name)
    }
}
#[test]
fn real_writer_fragment_checks_late_contributor_state_and_publishes_no_partial_table() {
    let registration = iceberg_theta_registration().unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(registration.definition().clone()).unwrap();
    let functions = Arc::new(LateSelectionCatalog {
        inner: Arc::new(builder.seal_bound().unwrap()),
        observations: Arc::new(Mutex::new(SelectionObservations::default())),
    });
    let owner = authored_with_catalog(&[1, 1], &[false, false], functions.clone());
    // Both genuine producers and the original consumer were emitted without
    // any fault. The adversarial catalogue is activated only after setup.
    assert_fresh(&owner, (2, 1));
    let (fragment, _, final_site, _) = final_call(&owner, 0);
    let occurrences = author_physical_occurrences_observed(
        fragment,
        owner.function_catalog().as_ref(),
        &Control::default(),
    )
    .unwrap();
    let (policy, relational) = scopes(&owner, fragment, &occurrences);
    let expressions = pure_node_effects_for_test(fragment);
    let earlier_updates = occurrences
        .relational_contexts
        .iter()
        .take_while(|entry| entry.0 != final_site)
        .filter(|entry| matches!(entry.0, PhysicalCallSite::WriterPartial { .. }))
        .count();
    // The Final checks its own request, then BOTH ordered producer requests.
    let bad_request = earlier_updates + 3;
    for fault in [SelectionFault::StateFormat, SelectionFault::NominalState] {
        functions.arm(bad_request, fault);
        let refusal = author_sql_fragment_effects_observed(
            owner.relational_sources(),
            input(
                &owner,
                fragment,
                &occurrences,
                policy,
                &relational,
                &expressions,
            ),
            &Control::default(),
        );
        assert!(
            matches!(refusal, Err(PhysicalFragmentEffectsError::WriterRequest(_))),
            "{refusal:?}"
        );
        let observed = functions.observations.lock().unwrap();
        assert_eq!(observed.requests, bad_request);
        assert_eq!(observed.bad_returns, 1);
        drop(observed);
        prefixes(
            |control| {
                functions.arm(bad_request, fault);
                author_sql_fragment_effects_observed(
                    owner.relational_sources(),
                    input(
                        &owner,
                        fragment,
                        &occurrences,
                        policy,
                        &relational,
                        &expressions,
                    ),
                    control,
                )
            },
            false,
        );
    }
}

#[test]
fn real_writer_original_nullable_channels_keep_own_captures_and_operational_none() {
    let owner = authored(&[1, 1], &[false, true]);
    // Original planner keys separate these full logical signatures. Each lane
    // keeps its own contributor and the other branch's actual no-contribution.
    assert_fresh(&owner, (2, 2));
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    for fragment in writer_fragments(&owner) {
        visit_relational_calls_observed(fragment, &mut work, |site, _, work| {
            let node = match site {
                PhysicalCallSite::WriterPartial { node, .. }
                | PhysicalCallSite::WriterFinal { node, .. } => &fragment.nodes()[&node],
                _ => panic!("Writer source"),
            };
            let entry = owner.checked_writer_aggregate_source_observed(
                fragment,
                node,
                site,
                writer_call(node, site),
                work,
            )?;
            assert!(entry.captured().request().expected_result_type.is_some());
            assert!(matches!(
                entry.captured().request().arguments[0],
                FunctionArgument::Value { constant: None, .. }
            ));
            if let Some(canonical) = entry.canonical() {
                assert!(canonical.request().expected_result_type.is_none());
                assert!(matches!(
                    canonical.request().arguments[0],
                    FunctionArgument::Value { constant: None, .. }
                ));
            }
            Ok::<_, PhysicalFragmentEffectsError>(())
        })
        .unwrap();
    }
    work.finish().unwrap();
}

#[test]
fn real_writer_wide_fresh_calls_sample_actual_control_quantums_without_losing_sparse_lane() {
    let owner = authored(&[320, 319], &[false, false]);
    assert_fresh(&owner, (639, 320));
    for fragment in writer_fragments(&owner) {
        let occurrences = author_physical_occurrences_observed(
            fragment,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        let (policy, relational) = scopes(&owner, fragment, &occurrences);
        let expressions = pure_node_effects_for_test(fragment);
        let baseline = Control::default();
        author_sql_fragment_effects_observed(
            owner.relational_sources(),
            input(
                &owner,
                fragment,
                &occurrences,
                policy,
                &relational,
                &expressions,
            ),
            &baseline,
        )
        .unwrap();
        let trace = baseline.trace.into_inner().unwrap();
        let quantum = trace
            .iter()
            .position(|(_, units)| *units == 256)
            .expect("actual fresh graph work reaches its original quantum");
        let mut stops = vec![
            0,
            quantum.saturating_sub(1),
            quantum,
            quantum + 1,
            trace.len() / 2,
            trace.len() - 1,
        ];
        stops.sort_unstable();
        stops.dedup();
        for stop in stops {
            if stop >= trace.len() {
                continue;
            }
            for cause in causes() {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((stop, cause.clone())),
                };
                let result = author_sql_fragment_effects_observed(
                    owner.relational_sources(),
                    input(
                        &owner,
                        fragment,
                        &occurrences,
                        policy,
                        &relational,
                        &expressions,
                    ),
                    &control,
                );
                assert!(
                    matches!(result,Err(PhysicalFragmentEffectsError::Control(actual)) if actual==cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn real_writer_frozen_materialized_receipt_rejects_absence_foreign_domain_and_duplicate_use() {
    let owner = authored(&[2], &[false]);
    for fragment in writer_fragments(&owner) {
        let occurrences = author_physical_occurrences_observed(
            fragment,
            owner.function_catalog().as_ref(),
            &Control::default(),
        )
        .unwrap();
        let (policy, relational) = scopes(&owner, fragment, &occurrences);
        let expressions = pure_node_effects_for_test(fragment);
        let source = input(
            &owner,
            fragment,
            &occurrences,
            policy,
            &relational,
            &expressions,
        );
        let authored = author_sql_fragment_effects_observed(
            owner.relational_sources(),
            source,
            &Control::default(),
        )
        .unwrap();
        let original = authored
            .calls
            .entries()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        assert!(original.len() >= 2);
        let check = |calls| FrozenFragmentCalls::try_new(fragment, calls, &Control::default());
        check(original.clone()).unwrap();
        let mut absent = original.clone();
        absent[0].materialized_writer_input = None;
        assert!(matches!(check(absent), Err(FrozenCallError::WrongContext)));
        let mut foreign = original.clone();
        let foreign_call = novarocks_type_contract::RelationCallId::new(u32::MAX);
        assert!(
            !occurrences
                .relational_contexts
                .iter()
                .any(|(_, context)| context.identity.call == foreign_call)
        );
        foreign[0]
            .materialized_writer_input
            .as_mut()
            .unwrap()
            .identity
            .call = foreign_call;
        assert!(matches!(check(foreign), Err(FrozenCallError::WrongContext)));
        let mut duplicate = original.clone();
        duplicate[1]
            .materialized_writer_input
            .as_mut()
            .unwrap()
            .identity
            .call = original[0].materialized_writer_input.unwrap().identity.call;
        assert!(matches!(
            check(duplicate),
            Err(FrozenCallError::WrongContext)
        ));
        let mut shared_call = original.clone();
        shared_call[1].context.identity.call = original[0].context.identity.call;
        shared_call[1]
            .materialized_writer_input
            .as_mut()
            .unwrap()
            .identity
            .call = original[0].context.identity.call;
        assert!(matches!(
            check(shared_call),
            Err(FrozenCallError::DuplicateIdentity)
        ));
    }
}

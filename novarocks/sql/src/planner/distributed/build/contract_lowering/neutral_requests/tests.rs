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

//! Genuine SQL emitter sources, not a fabricated request journal.

use super::super::{
    ROOT_FRAGMENT_ID, lower_final_physical_plan, lowered_scalar_source_tests as scalar,
    lowered_window_table_source_tests as relational, lowered_writer_state_tests,
    tests::{column, dop, stats, values, version},
};
use super::*;
use crate::analysis::{ExprKind as TypedKind, TypedExpr};
use crate::compiler::SqlAuthoredPhysicalPlan;
use crate::planner::payload::PlanProjectNode;
use crate::planner::physical::{PhysicalPlanKind, PhysicalPlanNode};
use arrow::datatypes::DataType;
use novarocks_functions::FunctionResultType;
use novarocks_type_contract::{CompilePhase, DecimalOverflowPolicy, PureCompileControl};
use std::sync::{Arc, Mutex};
use {
    novarocks_expression_contract::ExprKind,
    novarocks_physical_plan::{AggregatePhase, NodeKind, PhysicalCallSite},
};

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn assert_request(
    owner: &SqlAuthoredPhysicalPlan,
    original: FunctionBindingRequest<'_>,
    policy: ConstantPolicy,
    actual: &PhysicalCallRequest,
    work: &mut CompileCheckpoints<'_>,
) {
    assert_request_in_pools(owner.plan().constants(), original, policy, actual, work);
}

fn assert_request_in_pools(
    pools: &novarocks_physical_plan::ConstantPools,
    original: FunctionBindingRequest<'_>,
    policy: ConstantPolicy,
    actual: &PhysicalCallRequest,
    work: &mut CompileCheckpoints<'_>,
) {
    assert_eq!(
        actual.logical_argument_count,
        original.logical_argument_count
    );
    assert_eq!(
        actual.expected_result_type.as_ref(),
        original.expected_result_type
    );
    assert_eq!(actual.constant_policy, policy);
    assert_eq!(actual.arguments.len(), original.arguments.len());
    for (expected, actual) in original.arguments.iter().zip(&actual.arguments) {
        match (expected, actual) {
            (
                FunctionArgument::Value {
                    value_type,
                    constant,
                },
                StaticFunctionArgument::Value {
                    value_type: actual_type,
                    constant: reference,
                },
            ) => {
                assert_eq!(actual_type, value_type);
                match (constant, reference) {
                    (None, None) => {}
                    (Some(expected), Some(reference)) => {
                        let resolved = pools
                            .resolve_observed(*reference, actual_type, work)
                            .unwrap();
                        assert_eq!(resolved.ordinal(), expected.ordinal());
                        assert_eq!(resolved.value_type(), expected.value_type());
                        assert_eq!(
                            resolved.pool().backing_identity(),
                            expected.pool().backing_identity()
                        );
                        assert!(Arc::ptr_eq(
                            resolved.pool().array(),
                            expected.pool().array()
                        ));
                        assert!(Arc::ptr_eq(
                            resolved.pool().field_ref(),
                            expected.pool().field_ref()
                        ));
                    }
                    _ => panic!("original constant presence changed"),
                }
            }
            (
                FunctionArgument::Lambda {
                    parameter_types,
                    result_type,
                },
                StaticFunctionArgument::Lambda {
                    parameter_types: actual_parameters,
                    result_type: actual_result,
                },
            ) => {
                assert_eq!(actual_parameters, parameter_types);
                assert_eq!(actual_result, result_type);
            }
            _ => panic!("original argument class changed"),
        }
    }
}

/// Check every actual expression definition via the retained owner loan. Frame
/// offsets and lexical scopes remain separate original physical facts.
fn assert_expression_requests(owner: &SqlAuthoredPhysicalPlan) {
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let mut calls = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            if let Some(unit) = &node.expressions {
                for source in unit.definitions() {
                    if !matches!(source.kind, ExprKind::FunctionCall { .. }) {
                        continue;
                    }
                    let entry = owner
                        .checked_expression_call_source_observed(fragment, node, source, &mut work)
                        .unwrap();
                    let canonical = entry.canonical_operational().unwrap();
                    let actual = unit.requests().get(&source.id).unwrap();
                    assert_request(
                        owner,
                        canonical.request(),
                        entry.captured().constant_policy(),
                        actual,
                        &mut work,
                    );
                    calls += 1;
                }
            }
            if let NodeKind::Window(spec) = &node.kind {
                for (ordinal, source) in spec.calls.iter().enumerate() {
                    let site = PhysicalCallSite::Window {
                        node: node.id,
                        call: u32::try_from(ordinal).unwrap(),
                    };
                    let entry = owner
                        .checked_window_source_observed(fragment, node, site, source, &mut work)
                        .unwrap();
                    let actual = fragment.call_requests().get(site).unwrap();
                    assert_request(
                        owner,
                        entry.canonical_operational().request(),
                        entry.captured().constant_policy(),
                        actual,
                        &mut work,
                    );
                    calls += 1;
                }
            }
        }
    }
    assert!(calls > 0);
    work.finish().unwrap();
}

#[test]
fn neutral_sql_scalar_none_folded_constants_and_nonzero_cv_keep_original_identity() {
    for argument in [
        TypedExpr {
            kind: TypedKind::Nested(novarocks_parser::ast::AstBox::new(scalar::text("MiXeD"))),
            value_type: ValueType::new(DataType::Utf8, false),
        },
        TypedExpr {
            kind: TypedKind::Cast {
                cast_mode: None,
                expr: novarocks_parser::ast::AstBox::new(scalar::text("MiXeD")),
                target: DataType::Utf8,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            value_type: ValueType::new(DataType::Utf8, false),
        },
    ] {
        let owner = scalar::authored(scalar::lower_call(
            argument,
            DecimalOverflowPolicy::OutputNull,
        ));
        assert_expression_requests(&owner);
        let (_fragment, unit_node, source) = scalar::scalar_source(&owner);
        let ExprKind::FunctionCall { args, .. } = &source.kind else {
            unreachable!()
        };
        assert!(matches!(
            unit_node
                .expressions
                .as_ref()
                .unwrap()
                .definition(args[0])
                .unwrap()
                .kind,
            ExprKind::Constant(_)
        ));
        let actual = unit_node
            .expressions
            .as_ref()
            .unwrap()
            .requests()
            .get(&source.id)
            .unwrap();
        assert!(matches!(
            actual.arguments[0],
            StaticFunctionArgument::Value { constant: None, .. }
        ));
        assert!(actual.expected_result_type.is_none());
    }
    let (owner, pool, field) = scalar::operational_tests::selected_source_owner();
    assert_expression_requests(&owner);
    let (_fragment, unit_node, source) = scalar::scalar_source(&owner);
    let actual = unit_node
        .expressions
        .as_ref()
        .unwrap()
        .requests()
        .get(&source.id)
        .unwrap();
    let StaticFunctionArgument::Value {
        constant: Some(reference),
        ..
    } = &actual.arguments[0]
    else {
        panic!("original selected CV")
    };
    assert_eq!(reference.ordinal, 1);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let resolved = owner
        .plan()
        .constants()
        .resolve_source_observed(*reference, &mut work)
        .unwrap();
    assert_eq!(resolved.try_utf8().unwrap(), Some("MiXeD-中国"));
    assert!(Arc::ptr_eq(resolved.pool().array(), pool.array()));
    assert!(Arc::ptr_eq(resolved.pool().field_ref(), &field));
    work.finish().unwrap();

    let owner = scalar::authored(scalar::lower_call(
        TypedExpr {
            kind: TypedKind::Literal(crate::analysis::LiteralValue::Null),
            value_type: ValueType::new(DataType::Null, true),
        },
        DecimalOverflowPolicy::OutputNull,
    ));
    assert_expression_requests(&owner);
    let (fragment, unit_node, source) = scalar::scalar_source(&owner);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let entry = owner
        .checked_expression_call_source_observed(fragment, unit_node, source, &mut work)
        .unwrap();
    let original_request = entry.captured().request();
    let FunctionArgument::Value {
        constant: Some(original_null),
        ..
    } = &original_request.arguments[0]
    else {
        panic!("original bare NULL")
    };
    let actual = unit_node
        .expressions
        .as_ref()
        .unwrap()
        .requests()
        .get(&source.id)
        .unwrap();
    let StaticFunctionArgument::Value {
        value_type,
        constant: Some(reference),
    } = &actual.arguments[0]
    else {
        panic!("actual canonical typed NULL")
    };
    assert_eq!(
        original_null.value_type(),
        &ValueType::new(DataType::Null, true)
    );
    assert_eq!(value_type, &ValueType::new(DataType::Utf8, true));
    let canonical_null = owner
        .plan()
        .constants()
        .resolve_observed(*reference, value_type, &mut work)
        .unwrap();
    assert_eq!(canonical_null.try_utf8().unwrap(), None);
    assert_ne!(
        canonical_null.pool().backing_identity(),
        original_null.pool().backing_identity()
    );
    work.finish().unwrap();
}

#[test]
fn neutral_sql_lambda_window_order_and_left_table_keep_distinct_original_channels() {
    use super::super::original_emission_test_observer::{
        OriginalEmissionObserver, OriginalEmissionView, lower_read_with_original_emission_observer,
    };
    struct ObserveTransfer {
        calls: usize,
        window: bool,
    }
    impl OriginalEmissionObserver for ObserveTransfer {
        fn observe(
            &mut self,
            view: OriginalEmissionView<'_>,
            work: &mut CompileCheckpoints<'_>,
        ) -> Result<(), ContractLoweringError> {
            self.calls += 1;
            // The pure transfer component owns its actual output registry.
            // This is neither the source environment nor a published plan.
            let mut output = super::super::constant_emission::SqlConstantEmission::new();
            let transferred =
                transfer_neutral_requests_observed(view.journal(), work, |value, work| {
                    output.register(value, value.value_type(), work)
                })?;
            let mut lambda = false;
            let mut calls = 0;
            let mut windows = 0;
            for (&fragment, builder) in view.fragments() {
                for (&node, actual_node) in builder.construction_nodes() {
                    if let Some(unit) = builder.construction_unit(node) {
                        for definition in unit.definitions() {
                            if !matches!(definition.kind, ExprKind::FunctionCall { .. }) {
                                continue;
                            }
                            let source =
                                view.checked_scalar_source(fragment, node, definition.id, work)?;
                            assert!(std::ptr::eq(source.source(), definition));
                            let target = RequestTarget::Unit {
                                node,
                                definition: definition.id,
                            };
                            let actual = &transferred[&fragment]
                                .iter()
                                .find(|(actual, _)| *actual == target)
                                .unwrap()
                                .1;
                            assert_request_in_pools(
                                output.pools(),
                                source.canonical_operational().unwrap().request(),
                                source.captured().constant_policy(),
                                actual,
                                work,
                            );
                            lambda |= actual.arguments.iter().any(|argument| {
                                matches!(argument, StaticFunctionArgument::Lambda { .. })
                            });
                            calls += 1;
                        }
                    }
                    if let NodeKind::Window(spec) = &actual_node.kind {
                        for (ordinal, call) in spec.calls.iter().enumerate() {
                            let site = PhysicalCallSite::Window {
                                node,
                                call: u32::try_from(ordinal).unwrap(),
                            };
                            let emission = view.window_source(fragment, site, work)?;
                            assert!(std::ptr::eq(emission.source(), call));
                            let actual = &transferred[&fragment]
                                .iter()
                                .find(|(target, _)| *target == RequestTarget::Relation(site))
                                .unwrap()
                                .1;
                            assert_request_in_pools(
                                output.pools(),
                                emission.canonical().request(),
                                emission.captured().constant_policy(),
                                actual,
                                work,
                            );
                            assert_eq!(actual.logical_argument_count, 1);
                            assert_eq!(actual.arguments.len(), 2);
                            windows += 1;
                        }
                    }
                }
            }
            assert_eq!(calls, view.journal().expression_entries.len());
            assert_eq!(windows, view.journal().window_entries.len());
            assert_eq!(
                calls + windows,
                transferred.values().map(Vec::len).sum::<usize>()
            );
            assert!(calls + windows > 0);
            if self.window {
                assert_eq!(windows, 1);
            } else {
                assert!(lambda);
            }

            Ok(())
        }
    }
    let order = crate::analysis::SortItem {
        expr: relational::integer(9),
        asc: false,
        nulls_first: true,
    };
    let (window_source, _) = relational::window_fixture(vec![relational::integer(7)], vec![order]);
    for (source, window, overload) in [
        (
            plan(scalar::analyzed_lambda_expression_for_test()),
            false,
            "builtin.scalar/array_map/dynamic-v1",
        ),
        (
            window_source,
            true,
            "builtin.aggregate/array_agg/derived-v1",
        ),
    ] {
        let control = Control::default();
        let mut observer = ObserveTransfer { calls: 0, window };
        let result = lower_read_with_original_emission_observer(
            &source,
            version(),
            dop(),
            None,
            crate::functions::builtin_sql_function_catalog().snapshot(),
            false,
            scalar::policy(),
            &control,
            &mut observer,
        );
        assert_eq!(observer.calls, 1);
        assert!(matches!(result, Err(ContractLoweringError::FunctionBinding(
            novarocks_functions::FunctionBindingError::UnavailableImplementation(ref actual)
        )) if actual.as_str() == overload));
    }
    let (source, binding) = relational::table_fixture(vec![relational::list_value(1)], true);
    let owner = relational::finish(&source);
    let (fragment, source) = relational::table_source(&owner);
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let entry = owner
        .checked_table_source_observed(fragment, source, &mut work)
        .unwrap();
    let actual = fragment
        .call_requests()
        .get(PhysicalCallSite::Table { node: source.id })
        .unwrap();
    assert_request(
        &owner,
        entry.canonical_operational().request(),
        entry.captured().constant_policy(),
        actual,
        &mut work,
    );
    assert!(actual.expected_result_type.is_none());
    assert!(matches!(
        binding.selected.result_type,
        FunctionResultType::Relation(_)
    ));
    assert_eq!(actual.logical_argument_count, 1);
    work.finish().unwrap();
}

#[test]
fn neutral_sql_partial_final_own_literal_requests_survive_fragment_pool_projection() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT MIN(7), MAX(9), COUNT(*) FROM orders",
    );
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut partial = 0;
    let mut final_calls = 0;
    let mut request_only = 0;
    for fragment in owner.plan().fragments().values() {
        let projected = novarocks_physical_plan::project_fragment_constants_observed(
            owner.plan().constants(),
            fragment,
            &mut work,
        )
        .unwrap();
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, source) in calls.iter().enumerate() {
                let site = PhysicalCallSite::Aggregate {
                    node: node.id,
                    call: u32::try_from(ordinal).unwrap(),
                };
                let entry = owner
                    .checked_aggregate_source_observed(fragment, node, site, source, &mut work)
                    .unwrap();
                let actual = fragment.call_requests().get(site).unwrap();
                let original = if source.binding.phase.consumes_logical_arguments() {
                    partial += 1;
                    let request = entry.canonical().unwrap().request();
                    assert!(request.expected_result_type.is_none());
                    request
                } else {
                    final_calls += 1;
                    assert!(matches!(source.binding.phase, AggregatePhase::Final { .. }));
                    let canonical = entry.canonical().unwrap();
                    assert!(canonical.belongs_to(entry.captured()));
                    // The independent logical capture keeps its original SQL
                    // result; the current operational request is unconstrained.
                    assert!(entry.captured().request().expected_result_type.is_some());
                    let request = canonical.request();
                    assert!(request.expected_result_type.is_none());
                    assert_eq!(
                        canonical.selected().argument_types.as_ref(),
                        source.binding.function.argument_types.as_ref()
                    );
                    request
                };
                assert_request(
                    &owner,
                    original,
                    entry.captured().constant_policy(),
                    actual,
                    &mut work,
                );
                for argument in &actual.arguments {
                    if let StaticFunctionArgument::Value {
                        value_type,
                        constant: Some(reference),
                    } = argument
                    {
                        let resolved = projected
                            .resolve_observed(*reference, value_type, &mut work)
                            .unwrap();
                        assert!(matches!(
                            resolved
                                .signed_integer_observed(CompilePhase::Validate, &control)
                                .unwrap(),
                            Some(7 | 9)
                        ));
                        if !fragment.nodes().values().filter_map(|node| node.expressions.as_ref()).flat_map(|unit| unit.definitions()).any(|expression| matches!(expression.kind, ExprKind::Constant(found) if found == *reference)) {
                            request_only += 1;
                        }
                    }
                }
            }
        }
    }
    assert_eq!(partial, 3);
    assert_eq!(final_calls, 3);
    assert!(
        request_only >= 2,
        "Final original literal CVs must survive without dummy Constant Exprs"
    );
    work.finish().unwrap();
}

#[test]
fn neutral_sql_writer_final_keeps_own_capture_and_omissions_are_not_calls() {
    let owner = lowered_writer_state_tests::authored(&[2, 1], &[false, false]);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let mut updates = 0;
    let mut merges = 0;
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let (calls, final_phase) = match &node.kind {
                NodeKind::TableWriter { target } => (&target.partial_aggregates, false),
                NodeKind::TableFinish(finish) => (&finish.final_aggregates, true),
                _ => continue,
            };
            for (ordinal, source) in calls.iter().enumerate() {
                let ordinal = u32::try_from(ordinal).unwrap();
                let site = if final_phase {
                    PhysicalCallSite::WriterFinal {
                        node: node.id,
                        call: ordinal,
                    }
                } else {
                    PhysicalCallSite::WriterPartial {
                        node: node.id,
                        call: ordinal,
                    }
                };
                let entry = owner
                    .checked_writer_aggregate_source_observed(
                        fragment, node, site, source, &mut work,
                    )
                    .unwrap();
                let actual = fragment.call_requests().get(site).unwrap();
                let request = if final_phase {
                    merges += 1;
                    let canonical = entry.canonical().unwrap();
                    assert!(canonical.belongs_to(entry.captured()));
                    assert!(entry.captured().request().expected_result_type.is_some());
                    assert_eq!(
                        canonical.selected().argument_types.as_ref(),
                        source.binding.function.argument_types.as_ref()
                    );
                    canonical.request()
                } else {
                    updates += 1;
                    entry.canonical().unwrap().request()
                };
                assert_request(
                    &owner,
                    request,
                    entry.captured().constant_policy(),
                    actual,
                    &mut work,
                );
            }
            // Each actual call ordinal is present exactly once. Empty/sparse
            // auxiliary channels cannot become a fabricated neutral call.
            assert_eq!(
                fragment
                    .call_requests()
                    .entries()
                    .keys()
                    .filter(|definition| match definition {
                        PhysicalCallSite::WriterPartial { node: id, .. }
                        | PhysicalCallSite::WriterFinal { node: id, .. } => *id == node.id,
                        _ => false,
                    })
                    .count(),
                calls.len()
            );
        }
    }
    assert!(updates > merges && merges > 0);
    work.finish().unwrap();
}

fn plan(expression: TypedExpr) -> PhysicalPlanNode {
    let mut output = column(
        71,
        "source_result",
        expression.value_type.data_type().clone(),
        expression.value_type.nullable(),
    );
    output.value_type = expression.value_type.clone();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Project(PlanProjectNode {
            items: vec![crate::analysis::ProjectItem {
                expr: expression,
                output_name: "source_result".into(),
                output_column_id: output.column_id,
            }],
            output_qualifier: None,
        }),
        children: vec![values(vec![], vec![vec![]])],
        output_columns: vec![output],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}
fn finish(source: &PhysicalPlanNode, control: &Control) -> Result<(), ContractLoweringError> {
    let owner = lower_final_physical_plan(
        source,
        version(),
        dop(),
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        scalar::policy(),
        control,
    )?
    .finish_observed(control)
    .map_err(ContractLoweringError::from)?;
    let fragment = owner.plan().fragments().get(&ROOT_FRAGMENT_ID).unwrap();
    assert_eq!(fragment.call_requests().entries().len(), 0);
    assert_eq!(
        fragment
            .nodes()
            .values()
            .filter_map(|node| node.expressions.as_deref())
            .map(|unit| unit.requests().len())
            .sum::<usize>(),
        1
    );
    Ok(())
}
#[test]
fn neutral_sql_transfer_success_and_ordinary_refusal_preserve_every_original_control_prefix() {
    let source = plan(scalar::lower_call(
        scalar::text("MiXeD"),
        DecimalOverflowPolicy::OutputNull,
    ));
    let mut invalid = source.clone();
    invalid.output_columns[0].value_type = ValueType::new(DataType::Int64, false);
    for (source, succeeds) in [(&source, true), (&invalid, false)] {
        let baseline = Control::default();
        let result = finish(source, &baseline);
        assert_eq!(result.is_ok(), succeeds);
        assert!(!matches!(result, Err(ContractLoweringError::Control(_))));
        let expected = baseline.trace();
        assert!(!expected.is_empty());
        for at in 0..expected.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((at, cause.clone())),
                };
                assert!(
                    matches!(finish(source, &control), Err(ContractLoweringError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.trace(), expected[..=at]);
            }
        }
    }
}

// This negative fixture corrupts an actual completed emitter journal. It does
// not mint a checked source or weaken the ordinary production source gate.
#[derive(Clone, Copy, Debug)]
enum MissingCanonicalGate {
    FinishDraft,
    NeutralTransfer,
}
fn without_actual_canonical_source_at(
    source: &PhysicalPlanNode,
    functions: &Arc<dyn crate::compiler::SqlFunctionCatalog>,
    control: &Control,
    gate: MissingCanonicalGate,
) -> Result<(), ContractLoweringError> {
    use super::super::{
        ContractLoweringVisitor, FragmentSink, OutputPort, ResultPort, result_fields,
        unstatable_runtime_filters,
    };
    let mut visitor = ContractLoweringVisitor::new(
        version(),
        dop(),
        None,
        Arc::clone(functions),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        scalar::policy(),
        control,
    )?;
    visitor.unstatable_runtime_filters = unstatable_runtime_filters(source, &mut visitor.work)?;
    let root = visitor.lower_node(source)?;
    let result_types = root
        .output
        .iter()
        .map(|value| visitor.value_declared_type(*value))
        .collect::<Result<Vec<_>, _>>()?;
    visitor.work.flush()?;
    let fields = result_fields(
        source,
        &root.output,
        &result_types,
        &root.display_names,
        control,
    )?;
    let result_port = ResultPort {
        scalar_schema: None,
        fragment: root.fragment,
        output: OutputPort {
            node: root.node,
            columns: root.output.clone(),
        },
        fields,
    };
    visitor.complete_fragment(root.fragment, root.node, FragmentSink::Result)?;
    assert_eq!(visitor.call_sources.expression_entries.len(), 1);
    let entry = visitor
        .call_sources
        .expression_entries
        .values_mut()
        .next()
        .unwrap();
    assert!(entry.canonical_operational.is_some());
    entry.canonical_operational = None;
    match gate {
        MissingCanonicalGate::FinishDraft => visitor.finish_draft(result_port).map(|_| ()),
        MissingCanonicalGate::NeutralTransfer => {
            // This is the original leaf before publication, borrowing the real
            // emitter journal, pool registry and same visitor-owned work.
            // It is not a replacement path through finish_draft's source gate.
            let result = transfer_neutral_requests_observed(
                &visitor.call_sources,
                &mut visitor.work,
                |value, work| {
                    visitor
                        .constant_emission
                        .register(value, value.value_type(), work)
                },
            )
            .map(|_| ());
            if matches!(&result, Err(ContractLoweringError::Control(_))) {
                return result;
            }
            // Original counting/reservation/entry lookup leaves genuine
            // pending work on this ordinary refusal; do not inject steps.
            visitor.work.finish()?;
            result
        }
    }
}

#[test]
fn neutral_sql_finish_draft_missing_canonical_completes_pending_ordinary_tail_and_preserves_all_causes()
 {
    // Resolve the real installed source/catalogue before the observed visitor
    // invocation. The invocation never replaces its original control/meter.
    let source = plan(scalar::lower_call(
        scalar::text("MiXeD"),
        DecimalOverflowPolicy::OutputNull,
    ));
    let functions = crate::functions::builtin_sql_function_catalog().snapshot();
    for gate in [
        MissingCanonicalGate::FinishDraft,
        MissingCanonicalGate::NeutralTransfer,
    ] {
        let baseline = Control::default();
        let result = without_actual_canonical_source_at(&source, &functions, &baseline, gate);
        match gate {
            MissingCanonicalGate::FinishDraft => {
                assert!(matches!(
                    result,
                    Err(ContractLoweringError::Semantics(
                        crate::planner::distributed::build::PackageSemanticsError::InvalidSource(
                            "original scalar source has no canonical request"
                        )
                    ))
                ));
            }
            MissingCanonicalGate::NeutralTransfer => {
                assert!(matches!(result,
                    Err(ContractLoweringError::InvalidFunctionBinding { ref detail })
                    if detail == "neutral expression request has no canonical original source"));
            }
        }
        let expected = baseline.trace();
        let &(phase, units) = expected
            .last()
            .expect("actual original completion callback");
        assert_eq!(phase, CompilePhase::Validate);
        match gate {
            MissingCanonicalGate::FinishDraft => assert_eq!(
                units, 0,
                "the original checked-source loan already flushed before missing canonical admission"
            ),
            MissingCanonicalGate::NeutralTransfer => assert!(
                units > 0,
                "ordinary neutral transfer must complete its actually pending source work"
            ),
        }
        for at in 0..expected.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    refusal: Some((at, cause.clone())),
                };
                assert!(
                    matches!(without_actual_canonical_source_at(&source, &functions, &control, gate),
                    Err(ContractLoweringError::Control(actual)) if actual == cause),
                    "gate {gate:?}, at {at}: {cause:?}"
                );
                assert_eq!(control.trace(), expected[..=at]);
            }
        }
    }
}

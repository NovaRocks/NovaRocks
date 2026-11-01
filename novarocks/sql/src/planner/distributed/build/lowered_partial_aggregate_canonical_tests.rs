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
use crate::planner::distributed::build::lowered_draft::{
    AggregateRuntimeDemand, CheckedAggregateLogicalSourceEntry,
};
use crate::planner::distributed::build::physical_aggregate_requests::{
    PhysicalAggregateRequestError, author_physical_aggregate_update_request_from_journal_observed,
};

pub(super) fn chain(
    name: &str,
    arguments: Vec<TypedExpr>,
    order: Vec<SortItem>,
    child: PhysicalPlanNode,
) -> PhysicalPlanNode {
    let mut local = aggregate(name, arguments, order, child);
    let PhysicalPlanKind::HashAggregate(spec) = &mut local.kind else {
        unreachable!()
    };
    let source = spec.aggregates[0].source.clone();
    let selected = &source.binding().selected;
    let intermediate = selected
        .aggregate
        .as_ref()
        .unwrap()
        .intermediate_type
        .clone();
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        unreachable!()
    };
    let result = result.clone();
    let mut state = column(
        91,
        "partial_state",
        intermediate.data_type().clone(),
        intermediate.nullable(),
    );
    state.value_type = intermediate;
    spec.mode = AggMode::Local;
    spec.aggregates[0].result_type = state.value_type.clone();
    spec.output_layout = AggregateOutputLayout::new(vec![], vec![state.clone()]);
    spec.output_columns = vec![state.clone()];
    local.output_columns = vec![state.clone()];
    let mut output = column(
        92,
        "final_result",
        result.data_type().clone(),
        result.nullable(),
    );
    output.value_type = result.clone();
    let gather = PhysicalPlanNode {
        kind: PhysicalPlanKind::Redistribute(crate::planner::physical::RedistributeNode {
            mode: RedistributeMode::Gather,
            partition_exprs: vec![],
            output_columns: vec![state.clone()],
        }),
        children: vec![local],
        output_columns: vec![state],
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    PhysicalPlanNode {
        kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Global,
            group_by: vec![],
            aggregates: vec![AggregateCall {
                name: name.into(),
                source,
                distinct: false,
                result_type: result.clone(),
                output_column_id: output.column_id,
            }],
            is_merge: vec![true],
            output_layout: AggregateOutputLayout::new(vec![], vec![output.clone()]),
            output_columns: vec![output.clone()],
            topn_runtime_filter_builds: vec![],
        })),
        children: vec![gather],
        output_columns: vec![output],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}
pub(super) fn entry(
    owner: &SqlAuthoredPhysicalPlan,
    partial: bool,
) -> CheckedAggregateLogicalSourceEntry<'_> {
    for fragment in owner.plan().fragments().values() {
        for node in fragment.nodes().values() {
            let NodeKind::Aggregate { calls, .. } = &node.kind else {
                continue;
            };
            for (ordinal, call) in calls.iter().enumerate() {
                let matches = if partial {
                    matches!(call.binding.phase, AggregatePhase::Partial { .. })
                } else {
                    matches!(call.binding.phase, AggregatePhase::Final { .. })
                };
                if !matches {
                    continue;
                }
                let control = Control::default();
                let mut work =
                    CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)
                        .unwrap();
                let result = owner
                    .checked_aggregate_source_observed(
                        fragment,
                        node,
                        novarocks_physical_plan::PhysicalCallSite::Aggregate {
                            node: node.id,
                            call: u32::try_from(ordinal).unwrap(),
                        },
                        call,
                        &mut work,
                    )
                    .unwrap();
                work.finish().unwrap();
                return result;
            }
        }
    }
    panic!("actual chain must publish its requested Partial/Final occurrence");
}
fn verify_update_and_final(owner: &SqlAuthoredPhysicalPlan) {
    let partial = entry(owner, true);
    let final_entry = entry(owner, false);
    let canonical = partial.canonical().unwrap();
    assert!(canonical.belongs_to(partial.captured()));
    assert_eq!(partial.runtime(), AggregateRuntimeDemand::Update);
    let final_canonical = final_entry.canonical().unwrap();
    assert!(final_canonical.belongs_to(final_entry.captured()));
    assert!(
        !Arc::ptr_eq(final_canonical, canonical),
        "Final authors its own canonical receipt from its original capture"
    );
    assert_eq!(
        final_canonical.selected().argument_types.as_ref(),
        final_entry
            .source()
            .binding
            .function
            .argument_types
            .as_ref()
    );
    assert_eq!(
        final_canonical.selected().result_type,
        FunctionResultType::Scalar(final_entry.source().binding.function.result_type.clone())
    );
    assert!(
        partial
            .captured()
            .logical_identity()
            .same_revision(final_entry.captured().logical_identity())
    );
    assert!(std::ptr::eq(
        partial.captured().binding().resolved(),
        final_entry.captured().binding().resolved()
    ));
    let AggregatePhase::Partial { sequence } = partial.phase() else {
        unreachable!()
    };
    assert_eq!(final_entry.phase(), AggregatePhase::Final { sequence });
    let final_call = final_entry.source();
    assert_eq!(final_call.arguments.len(), 1);
    assert!(final_call.order_by.is_empty());
    assert_eq!(
        final_entry.runtime(),
        AggregateRuntimeDemand::ExpressionState(final_call.arguments[0])
    );
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let request =
        author_physical_aggregate_update_request_from_journal_observed(&partial, &mut work)
            .unwrap();
    work.finish().unwrap();
    let expected = control.trace.into_inner().unwrap();
    for at in 0..expected.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Default::default(),
                refusal: Some((at, cause.clone())),
            };
            let invoke = || {
                let mut work =
                    CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization)?;
                let result = author_physical_aggregate_update_request_from_journal_observed(
                    &partial, &mut work,
                );
                if matches!(&result, Err(PhysicalAggregateRequestError::Control(_))) {
                    return result;
                }
                work.finish()?;
                result
            };
            assert!(
                matches!(invoke(), Err(PhysicalAggregateRequestError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
        }
    }
    assert!(Arc::ptr_eq(request.selected(), canonical.selected()));
    assert!(std::ptr::eq(
        request.request().arguments,
        canonical.request().arguments
    ));
    assert!(request.request().expected_result_type.is_none());
    assert_eq!(
        request.captured_constant_policy(),
        Some(partial.captured().constant_policy())
    );
    assert_eq!(
        request.captured_decimal_overflow_policy(),
        Some(DecimalOverflowPolicy::ReportError)
    );
    assert_eq!(
        canonical.selected().argument_types.as_ref(),
        partial.source().binding.function.argument_types.as_ref()
    );
    assert_eq!(
        canonical
            .selected()
            .aggregate
            .as_ref()
            .unwrap()
            .state_format,
        partial.source().binding.state_format
    );
    assert_eq!(
        partial.source().binding.state_format,
        final_call.binding.state_format
    );
    assert_eq!(
        partial
            .fragment()
            .values()
            .get(&partial.source().output)
            .unwrap()
            .ty,
        canonical
            .selected()
            .aggregate
            .as_ref()
            .unwrap()
            .intermediate_type
    );
    assert_eq!(
        final_entry
            .fragment()
            .values()
            .get(&final_call.output)
            .unwrap()
            .ty,
        final_call.binding.function.result_type
    );
}

#[test]
fn partial_count_star_column_min_literal_max_keep_canonical_update_and_own_final_capture() {
    let input = column(7, "number", DataType::Int64, false);
    for (name, args, child) in [
        ("count", vec![], values(vec![], vec![vec![]])),
        (
            "count",
            vec![reference(&input)],
            values(vec![input.clone()], vec![vec![integer(7)]]),
        ),
        ("min", vec![integer(7)], values(vec![], vec![vec![]])),
        (
            "max",
            vec![reference(&input)],
            values(vec![input.clone()], vec![vec![integer(7)]]),
        ),
    ] {
        let source = chain(name, args, vec![], child);
        let owner = authored(&source, &Control::default()).unwrap();
        verify_update_and_final(&owner);
        let partial = entry(&owner, true);
        assert_eq!(
            partial
                .canonical()
                .unwrap()
                .request()
                .logical_argument_count,
            if name == "count" && partial.source().arguments.is_empty() {
                0
            } else {
                1
            }
        );
    }
}

#[test]
fn partial_avg_local_output_uses_intermediate_utf8_not_final_float64() {
    let input = column(7, "number", DataType::Int64, false);
    let source = chain(
        "avg",
        vec![reference(&input)],
        vec![],
        values(vec![input], vec![vec![integer(7)]]),
    );
    let owner = authored(&source, &Control::default()).unwrap();
    verify_update_and_final(&owner);
    let partial = entry(&owner, true);
    let final_entry = entry(&owner, false);
    let selected = partial.canonical().unwrap().selected();
    // These independent carrier oracles match the actual installed AVG author.
    assert_eq!(
        selected
            .aggregate
            .as_ref()
            .unwrap()
            .intermediate_type
            .data_type()
            .clone(),
        DataType::Utf8
    );
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        unreachable!()
    };
    assert_eq!(result.data_type().clone(), DataType::Float64);
    assert!(result.nullable());
    assert_ne!(
        result,
        &selected.aggregate.as_ref().unwrap().intermediate_type
    );
    assert_eq!(
        partial
            .fragment()
            .values()
            .get(&partial.source().output)
            .unwrap()
            .ty,
        selected.aggregate.as_ref().unwrap().intermediate_type
    );
    assert_eq!(
        final_entry
            .fragment()
            .expressions()
            .get(final_entry.source().arguments[0])
            .unwrap()
            .ty,
        selected.aggregate.as_ref().unwrap().intermediate_type
    );
    assert_eq!(final_entry.source().binding.function.result_type, *result);
    // Metadata/state transport only: this does not install a pure AVG kernel.
}

#[test]
fn partial_array_agg_cv_order_retains_original_ordinals_backing_and_final_logical_source() {
    let ty = ValueType::new(DataType::Int64, true);
    let field = Arc::new(
        Field::new("original_cv", DataType::Int64, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider-origin".into(),
                "original".into(),
            )]),
        ),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        Int64Array::from(vec![Some(101), Some(202), Some(303)]).to_data(),
        policy(),
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap();
    let expr = |ordinal| TypedExpr {
        kind: ExprKind::Constant(pool.value(ordinal).unwrap()),
        value_type: ty.clone(),
    };
    let source = chain(
        "array_agg",
        vec![expr(1)],
        vec![SortItem {
            expr: expr(2),
            asc: false,
            nulls_first: true,
        }],
        values(vec![], vec![vec![]]),
    );
    let owner = authored(&source, &Control::default()).unwrap();
    verify_update_and_final(&owner);
    let partial = entry(&owner, true);
    let final_entry = entry(&owner, false);
    let canonical = partial.canonical().unwrap();
    assert_eq!(canonical.request().logical_argument_count, 1);
    assert_eq!(canonical.request().arguments.len(), 2);
    assert_eq!(final_entry.captured().request().arguments.len(), 2);
    assert_eq!(
        partial.source().order_by[0].direction,
        SortDirection::Descending
    );
    assert_eq!(
        partial.source().order_by[0].null_ordering,
        NullOrdering::First
    );
    for (position, ordinal) in [(0, 1), (1, 2)] {
        for argument in [
            &canonical.request().arguments[position],
            &partial.captured().request().arguments[position],
            &final_entry.captured().request().arguments[position],
        ] {
            let constant = value(argument).1.unwrap();
            assert_eq!(constant.ordinal(), ordinal);
            assert!(Arc::ptr_eq(constant.pool().field_ref(), &field));
            assert!(Arc::ptr_eq(constant.pool().array(), pool.array()));
            assert_eq!(constant.pool().backing_identity(), pool.backing_identity());
        }
        let expr = if position == 0 {
            partial.source().arguments[0]
        } else {
            partial.source().order_by[0].expr
        };
        let ContractExprKind::Constant(reference) =
            partial.fragment().expressions().get(expr).unwrap().kind
        else {
            panic!("actual retained Partial CV address");
        };
        let control = Control::default();
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        let actual = owner
            .plan()
            .constants()
            .resolve_source_observed(reference, &mut work)
            .unwrap();
        work.finish().unwrap();
        assert_eq!(actual.ordinal(), ordinal);
        assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
        assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
    }
    // Final consumes one actual state; its retained logical ORDER is not a runtime ORDER argument.
    assert_eq!(final_entry.source().arguments.len(), 1);
    assert!(final_entry.source().order_by.is_empty());
}

#[test]
fn partial_elided_cast_nested_constants_keep_none_in_operational_and_final_capture() {
    for arg in [
        TypedExpr {
            kind: ExprKind::Nested(Box::new(integer(7))),
            value_type: ValueType::new(DataType::Int64, false),
        },
        TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(integer(9)),
                target: DataType::Int64,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            value_type: ValueType::new(DataType::Int64, false),
        },
    ] {
        let source = chain("min", vec![arg], vec![], values(vec![], vec![vec![]]));
        let owner = authored(&source, &Control::default()).unwrap();
        verify_update_and_final(&owner);
        let partial = entry(&owner, true);
        let final_entry = entry(&owner, false);
        assert!(
            value(&partial.canonical().unwrap().request().arguments[0])
                .1
                .is_none()
        );
        assert!(
            value(&partial.captured().request().arguments[0])
                .1
                .is_none()
        );
        assert!(
            value(&final_entry.captured().request().arguments[0])
                .1
                .is_none()
        );
        assert!(matches!(
            partial
                .fragment()
                .expressions()
                .get(partial.source().arguments[0])
                .unwrap()
                .kind,
            ContractExprKind::Constant(_)
        ));
        assert!(matches!(
            final_entry
                .fragment()
                .expressions()
                .get(final_entry.source().arguments[0])
                .unwrap()
                .kind,
            ContractExprKind::Value(_)
        ));
    }
}

#[test]
fn partial_success_layout_source_and_late_nullable_state_compatibility_keep_original_control_prefixes()
 {
    let input = column(7, "number", DataType::Int64, false);
    let good = chain(
        "count",
        vec![reference(&input)],
        vec![],
        values(vec![input.clone()], vec![vec![integer(7)]]),
    );
    let mut bad_layout = good.clone();
    let PhysicalPlanKind::HashAggregate(spec) = &mut bad_layout.kind else {
        unreachable!()
    };
    let wrong = column(92, "final_result", DataType::Utf8, true);
    spec.output_layout = AggregateOutputLayout::new(vec![], vec![wrong.clone()]);
    spec.output_columns = vec![wrong.clone()];
    bad_layout.output_columns = vec![wrong];
    let mut bad_source = good.clone();
    let PhysicalPlanKind::HashAggregate(spec) = &mut bad_source.kind else {
        unreachable!()
    };
    let original = spec.aggregates[0].source.binding().clone();
    spec.aggregates[0].source = AggregateArgumentSource::logical_update(vec![], vec![], original);
    let late_nullable = chain(
        "count",
        vec![reference(&input)],
        vec![],
        repeat(values(vec![input.clone()], vec![vec![integer(7)]]), &input),
    );
    for (case, source) in [
        (0, &good),
        (1, &bad_layout),
        (2, &bad_source),
        (3, &late_nullable),
    ] {
        let baseline = Control::default();
        let result = authored(source, &baseline);
        match (case, result) {
            (0 | 3, Ok(owner)) => verify_update_and_final(&owner),
            (
                1,
                Err(ContractLoweringError::OutputColumnMismatch {
                    node: "HashAggregate",
                    ..
                }),
            ) => {}
            (
                2,
                Err(ContractLoweringError::InvalidAggregate {
                    detail: "binding logical/ORDER BY arity differs from the call",
                }),
            ) => {}
            (_, actual) => panic!("unexpected exact outcome for case {case}: {actual:?}"),
        }
        let expected = baseline.trace.into_inner().unwrap();
        assert!(!expected.is_empty());
        for at in 0..expected.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    trace: Default::default(),
                    refusal: Some((at, cause.clone())),
                };
                assert!(
                    matches!(authored(source, &control), Err(ContractLoweringError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
            }
        }
    }
}

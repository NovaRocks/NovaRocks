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

use super::super::lowered_draft::CheckedAggregateLogicalSourceEntry;
use super::tests::{column, dop, stats, values, version};
use super::*;
use crate::analysis::SortItem;
use crate::binding::AggregateArgumentSource;
use crate::compiler::{SqlAuthoredPhysicalPlan, SqlFunctionCatalog};
use crate::planner::payload::{AggregateCall, PlanRepeatNode};
use crate::planner::physical::{AggregateOutputLayout, PhysicalHashAggregateNode};
use arrow::array::{Array, Int64Array};
use arrow::datatypes::Field;
use novarocks_constant_contract::ConstantPool;
use novarocks_functions::{FunctionArgument, FunctionResultType};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::sync::Mutex;

#[path = "lowered_partial_aggregate_canonical_tests.rs"]
mod lowered_partial_aggregate_canonical_tests;

#[path = "lowered_aggregate_state_sources_tests.rs"]
mod lowered_aggregate_state_sources_tests;

#[path = "lowered_aggregate_state_compatibility_tests.rs"]
mod lowered_aggregate_state_compatibility_tests;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after the original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> novarocks_functions::ConstantPolicy {
    crate::constant::test_constant_policy()
}
fn reference(source: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: source.column_id,
            qualifier: None,
            column: source.name.clone(),
        },
        value_type: source.value_type.clone(),
    }
}
fn integer(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        value_type: ValueType::new(DataType::Int64, false),
    }
}
fn aggregate(
    name: &str,
    arguments: Vec<TypedExpr>,
    order: Vec<SortItem>,
    child: PhysicalPlanNode,
) -> PhysicalPlanNode {
    let control = Control::default();
    let request = arguments
        .iter()
        .chain(order.iter().map(|item| &item.expr))
        .map(|arg| crate::analysis::function_argument(arg, policy(), &control).unwrap())
        .collect::<Vec<_>>();
    let resolved = crate::functions::builtin_engine_function_catalog()
        .resolve_aggregate_binding(name, arguments.len(), &request, &control)
        .unwrap();
    let FunctionResultType::Scalar(result) = &resolved.selected.result_type else {
        panic!("actual Aggregate scalar result");
    };
    let mut output = column(
        91,
        "aggregate_result",
        result.data_type().clone(),
        result.nullable(),
    );
    output.value_type = result.clone();
    let binding = SqlFunctionBinding::new(resolved, DecimalOverflowPolicy::ReportError);
    PhysicalPlanNode {
        kind: PhysicalPlanKind::HashAggregate(Box::new(PhysicalHashAggregateNode {
            mode: AggMode::Single,
            group_by: vec![],
            aggregates: vec![AggregateCall {
                name: name.into(),
                source: AggregateArgumentSource::logical_update(arguments, order, binding),
                distinct: false,
                result_type: output.value_type.clone(),
                output_column_id: output.column_id,
            }],
            is_merge: vec![false],
            output_layout: AggregateOutputLayout::new(vec![], vec![output.clone()]),
            output_columns: vec![output.clone()],
            topn_runtime_filter_builds: vec![],
        })),
        children: vec![child],
        output_columns: vec![output],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}
fn authored(
    plan: &PhysicalPlanNode,
    control: &dyn PureCompileControl,
) -> Result<SqlAuthoredPhysicalPlan, ContractLoweringError> {
    lower_final_physical_plan(
        plan,
        version(),
        dop(),
        crate::functions::builtin_sql_function_catalog().snapshot(),
        false,
        policy(),
        control,
    )?
    .finish_observed(control)
    .map_err(ContractLoweringError::from)
}
fn loan(owner: &SqlAuthoredPhysicalPlan) -> CheckedAggregateLogicalSourceEntry<'_> {
    let fragment = owner.plan().fragments().get(&ROOT_FRAGMENT_ID).unwrap();
    let node = fragment.nodes().get(&fragment.root()).unwrap();
    let NodeKind::Aggregate { calls, .. } = &node.kind else {
        panic!("actual Single aggregate root");
    };
    assert_eq!(calls.len(), 1);
    let control = Control::default();
    let mut work =
        CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
    let entry = owner
        .checked_aggregate_source_observed(
            fragment,
            node,
            novarocks_physical_plan::PhysicalCallSite::Aggregate {
                node: node.id,
                call: 0,
            },
            &calls[0],
            &mut work,
        )
        .unwrap();
    work.finish().unwrap();
    assert_eq!(entry.phase(), AggregatePhase::Single);
    assert!(entry.canonical().unwrap().belongs_to(entry.captured()));
    entry
}
fn value(argument: &FunctionArgument) -> (&ValueType, Option<&novarocks_functions::ConstantValue>) {
    let FunctionArgument::Value {
        value_type,
        constant,
    } = argument
    else {
        panic!("actual Value argument");
    };
    (value_type, constant.as_ref())
}
fn repeat(child: PhysicalPlanNode, original: &OutputColumn) -> PhysicalPlanNode {
    let grouping = column(72, "grouping", DataType::Int64, false);
    let mut repeated = original.clone();
    repeated.value_type = repeated.value_type.with_nullable(true).unwrap();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Repeat(PlanRepeatNode {
            repeat_column_ref_list: vec![vec![original.name.clone()], vec![]],
            repeat_column_ref_ids: vec![vec![original.column_id], vec![]],
            grouping_ids: vec![0, 1],
            all_rollup_columns: vec![original.name.clone()],
            all_rollup_column_ids: vec![original.column_id],
            grouping_key_aliases: vec![],
            grouping_fn_args: vec![("grouping".into(), vec![original.name.clone()])],
            grouping_fn_arg_ids: vec![vec![original.column_id]],
            grouping_fn_ids: vec![("grouping".into(), grouping.column_id)],
            virtual_tuple_id: None,
        }),
        children: vec![child],
        output_columns: vec![repeated, grouping],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}

#[test]
fn single_count_star_and_repeat_nullable_channels_publish_actual_selected_request() {
    let star = aggregate("count", vec![], vec![], values(vec![], vec![vec![]]));
    let owner = authored(&star, &Control::default()).unwrap();
    let entry = loan(&owner);
    let canonical = entry.canonical().unwrap();
    assert_eq!(canonical.request().logical_argument_count, 0);
    assert!(canonical.request().arguments.is_empty());
    assert!(canonical.request().expected_result_type.is_none());
    assert_eq!(canonical.selected().argument_types.len(), 0);
    assert_eq!(
        entry.source().binding.function.result_type,
        ValueType::new(DataType::Int64, false)
    );

    let input = column(7, "original_nonnullable", DataType::Int64, false);
    let child = repeat(values(vec![input.clone()], vec![vec![integer(7)]]), &input);
    let plan = aggregate("count", vec![reference(&input)], vec![], child);
    let owner = authored(&plan, &Control::default()).unwrap();
    let entry = loan(&owner);
    let captured = value(&entry.captured().request().arguments[0]);
    let canonical = entry.canonical().unwrap();
    let operational = value(&canonical.request().arguments[0]);
    assert!(!captured.0.nullable());
    assert!(operational.0.nullable());
    assert!(captured.1.is_none() && operational.1.is_none());
    assert!(!input.value_type.nullable());
    assert!(canonical.request().expected_result_type.is_none());
    assert_eq!(
        canonical.selected().argument_types.as_ref(),
        entry.source().binding.function.argument_types.as_ref()
    );
    let actual = entry
        .fragment()
        .expressions()
        .get(entry.source().arguments[0])
        .unwrap();
    assert_eq!(&actual.ty, operational.0);
}

#[test]
fn single_min_keeps_nested_field_metadata_and_nominal_json_source_facts() {
    let nested = DataType::Struct(
        vec![Arc::new(
            Field::new("中国", DataType::Utf8, true).with_metadata(std::collections::HashMap::<
                String,
                String,
            >::from([(
                "provider-origin".into(),
                "original".into(),
            )])),
        )]
        .into(),
    );
    for ty in [
        ValueType::new(nested, false),
        ValueType::try_with_logical_type(
            DataType::Utf8,
            false,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap(),
    ] {
        let mut input = column(7, "full_source", ty.data_type().clone(), ty.nullable());
        input.value_type = ty.clone();
        // Empty Values publishes a real typed channel without fabricating a
        // nested literal or claiming a pure MIN kernel for the Struct profile.
        let plan = aggregate(
            "min",
            vec![reference(&input)],
            vec![],
            values(vec![input], vec![]),
        );
        let owner = authored(&plan, &Control::default()).unwrap();
        let entry = loan(&owner);
        let canonical = entry.canonical().unwrap();
        assert_eq!(value(&canonical.request().arguments[0]).0, &ty);
        assert_eq!(value(&entry.captured().request().arguments[0]).0, &ty);
        let mut result = ty.clone();
        result = result.with_nullable(true).unwrap();
        assert_eq!(entry.source().binding.function.result_type, result);
        assert_eq!(entry.source().binding.intermediate_type, result);
        assert_eq!(
            canonical
                .selected()
                .aggregate
                .as_ref()
                .unwrap()
                .intermediate_type,
            result
        );
        assert_eq!(
            canonical
                .selected()
                .aggregate
                .as_ref()
                .unwrap()
                .state_format,
            entry.source().binding.state_format
        );
        assert!(canonical.request().expected_result_type.is_none());
    }
}

#[test]
fn single_array_agg_original_nonzero_cv_and_order_keep_backing_field_and_policy() {
    let ty = ValueType::new(DataType::Int64, true);
    let field = Arc::new(
        Field::new("original_cv", DataType::Int64, true).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider-origin".into(),
                "source".into(),
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
    let plan = aggregate(
        "array_agg",
        vec![expr(1)],
        vec![SortItem {
            expr: expr(2),
            asc: false,
            nulls_first: true,
        }],
        values(vec![], vec![vec![]]),
    );
    let owner = authored(&plan, &Control::default()).unwrap();
    let entry = loan(&owner);
    let canonical = entry.canonical().unwrap();
    assert_eq!(canonical.request().logical_argument_count, 1);
    assert_eq!(canonical.request().arguments.len(), 2);
    assert_eq!(entry.source().order_by.len(), 1);
    assert_eq!(
        entry.source().order_by[0].direction,
        SortDirection::Descending
    );
    assert_eq!(
        entry.source().order_by[0].null_ordering,
        NullOrdering::First
    );
    assert_eq!(entry.captured().constant_policy(), policy());
    assert_eq!(
        entry.captured().binding().decimal_overflow_policy(),
        DecimalOverflowPolicy::ReportError
    );
    for (position, ordinal) in [(0, 1), (1, 2)] {
        let original = value(&entry.captured().request().arguments[position])
            .1
            .unwrap();
        let actual = value(&canonical.request().arguments[position]).1.unwrap();
        assert_eq!(actual.ordinal(), ordinal);
        assert_eq!(original.ordinal(), ordinal);
        assert!(Arc::ptr_eq(actual.pool().field_ref(), &field));
        assert!(Arc::ptr_eq(actual.pool().array(), pool.array()));
        assert_eq!(actual.pool().backing_identity(), pool.backing_identity());
        let expression = if position == 0 {
            entry.source().arguments[0]
        } else {
            entry.source().order_by[0].expr
        };
        let ContractExprKind::Constant(reference) =
            entry.fragment().expressions().get(expression).unwrap().kind
        else {
            panic!("actual emitted CV reference");
        };
        let control = Control::default();
        let mut work =
            CompileCheckpoints::try_new(&control, CompilePhase::FunctionSpecialization).unwrap();
        let emitted = owner
            .plan()
            .constants()
            .resolve_source_observed(reference, &mut work)
            .unwrap();
        work.finish().unwrap();
        assert_eq!(emitted.ordinal(), ordinal);
        assert!(Arc::ptr_eq(emitted.pool().field_ref(), &field));
        assert_eq!(emitted.pool().backing_identity(), pool.backing_identity());
    }
    // This checks installed metadata selection, not pure ARRAY_AGG ORDER execution.
    assert!(canonical.request().expected_result_type.is_none());
}

#[test]
fn single_nested_and_elided_cast_constants_remain_original_none_requests() {
    for argument in [
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
        let plan = aggregate("min", vec![argument], vec![], values(vec![], vec![vec![]]));
        let owner = authored(&plan, &Control::default()).unwrap();
        let entry = loan(&owner);
        assert!(value(&entry.captured().request().arguments[0]).1.is_none());
        assert!(
            value(&entry.canonical().unwrap().request().arguments[0])
                .1
                .is_none()
        );
        assert!(matches!(
            entry
                .fragment()
                .expressions()
                .get(entry.source().arguments[0])
                .unwrap()
                .kind,
            ContractExprKind::Constant(_)
        ));
    }
}

#[test]
fn single_original_layout_refusal_and_success_preserve_every_original_control_prefix() {
    let good = aggregate("count", vec![], vec![], values(vec![], vec![vec![]]));
    let mut bad = good.clone();
    let PhysicalPlanKind::HashAggregate(spec) = &mut bad.kind else {
        unreachable!()
    };
    let malformed = column(91, "aggregate_result", DataType::Utf8, true);
    spec.output_layout = AggregateOutputLayout::new(vec![], vec![malformed.clone()]);
    spec.output_columns = vec![malformed.clone()];
    bad.output_columns = vec![malformed];
    let mut bad_binding = good.clone();
    let PhysicalPlanKind::HashAggregate(spec) = &mut bad_binding.kind else {
        unreachable!()
    };
    let original = spec.aggregates[0].source.binding().clone();
    spec.aggregates[0].source =
        AggregateArgumentSource::logical_update(vec![integer(7)], vec![], original);
    for (plan, success) in [(&good, true), (&bad, false), (&bad_binding, false)] {
        let baseline = Control::default();
        let result = authored(plan, &baseline);
        assert_eq!(result.is_ok(), success);
        if !success {
            if std::ptr::eq(plan, &bad) {
                assert!(matches!(
                    result,
                    Err(ContractLoweringError::OutputColumnMismatch {
                        node: "HashAggregate",
                        ..
                    })
                ));
            } else {
                assert!(matches!(
                    result,
                    Err(ContractLoweringError::InvalidAggregate {
                        detail: "binding logical/ORDER BY arity differs from the call"
                    })
                ));
            }
        }
        let expected = baseline.trace.into_inner().unwrap();
        assert!(!expected.is_empty());
        if success {
            assert!(expected.iter().any(|(_, units)| *units > 0));
        }
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
                    matches!(authored(plan, &control), Err(ContractLoweringError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
            }
        }
    }
}

#[test]
fn single_journal_reader_borrows_same_request_selection_and_preserves_control_prefixes() {
    use super::super::physical_aggregate_requests::{
        PhysicalAggregateRequestError,
        author_physical_aggregate_update_request_from_journal_observed,
    };
    let input = column(7, "original_nonnullable", DataType::Int64, false);
    let child = repeat(values(vec![input.clone()], vec![vec![integer(7)]]), &input);
    let plans = [
        aggregate("count", vec![], vec![], values(vec![], vec![vec![]])),
        aggregate("count", vec![reference(&input)], vec![], child),
        aggregate(
            "min",
            vec![integer(7)],
            vec![],
            values(vec![], vec![vec![]]),
        ),
    ];
    for plan in plans {
        let owner = authored(&plan, &Control::default()).unwrap();
        let entry = loan(&owner);
        let canonical = entry.canonical().unwrap();
        let invoke = |control: &Control| {
            let mut work =
                CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
            let result =
                author_physical_aggregate_update_request_from_journal_observed(&entry, &mut work);
            if matches!(&result, Err(PhysicalAggregateRequestError::Control(_))) {
                return result;
            }
            work.finish()?;
            result
        };
        let control = Control::default();
        let request = invoke(&control).unwrap();
        assert!(Arc::ptr_eq(request.selected(), canonical.selected()));
        assert!(std::ptr::eq(
            request.request().arguments,
            canonical.request().arguments,
        ));
        assert!(request.request().expected_result_type.is_none());
        assert!(std::ptr::eq(request.source(), entry.source()));
        assert!(std::ptr::eq(request.node(), entry.node()));
        assert_eq!(request.site(), entry.site());
        assert_eq!(
            request.request().logical_argument_count,
            entry.source().arguments.len()
        );
        assert_eq!(
            request.captured_constant_policy(),
            Some(entry.captured().constant_policy()),
        );
        assert_eq!(
            request.captured_decimal_overflow_policy(),
            Some(entry.captured().binding().decimal_overflow_policy()),
        );
        let expected = control.trace.into_inner().unwrap();
        assert!(expected.len() > 1);
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
                    matches!(invoke(&control), Err(PhysicalAggregateRequestError::Control(actual)) if actual == cause)
                );
                assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
            }
        }
    }
}

#[test]
fn single_wide_nested_metadata_emission_observes_bounded_original_control() {
    let fields = (0..513)
        .map(|ordinal| {
            Arc::new(
                Field::new(
                    format!("field_{ordinal}"),
                    DataType::Int64,
                    ordinal % 2 == 0,
                )
                .with_metadata(std::collections::HashMap::<String, String>::from(
                    [("provider-origin".into(), format!("source_{ordinal}"))],
                )),
            )
        })
        .collect::<Vec<_>>();
    let ty = ValueType::new(DataType::Struct(fields.into()), false);
    let mut input = column(7, "wide_source", ty.data_type().clone(), ty.nullable());
    input.value_type = ty.clone();
    let plan = aggregate(
        "min",
        vec![reference(&input)],
        vec![],
        values(vec![input], vec![]),
    );
    let baseline = Control::default();
    let owner = authored(&plan, &baseline).unwrap();
    let entry = loan(&owner);
    assert_eq!(
        value(&entry.canonical().unwrap().request().arguments[0]).0,
        &ty
    );
    let expected = baseline.trace.into_inner().unwrap();
    assert!(expected.iter().any(|(_, units)| *units == 256));
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
                matches!(authored(&plan, &control), Err(ContractLoweringError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), expected[..=at]);
        }
    }
}

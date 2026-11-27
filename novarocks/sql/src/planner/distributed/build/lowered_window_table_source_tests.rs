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

#[path = "lowered_window_table_canonical_tests.rs"]
mod lowered_window_table_canonical_tests;

use super::original_emission_test_observer::{
    OriginalEmissionObserver, OriginalEmissionView, OriginalWindowEmission,
    lower_read_with_original_emission_observer,
};
use super::tests::{column, dop, stats, values, version};
use super::*;
use crate::analysis::{SortItem, WindowBound, WindowFrame, WindowFrameType};
use crate::binding::SqlFunctionBinding;
use crate::compiler::SqlAuthoredPhysicalPlan;
use crate::planner::distributed::build::lowered_draft::{
    CheckedTableLogicalSourceEntry, CheckedWindowLogicalSourceEntry, SqlOperationalProjectionError,
    SqlSourceJournalError,
};
use crate::planner::payload::{PlanTableFunctionNode, PlanWindowNode, WindowExpr};
use arrow::array::{Array, Int64Array, ListArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use novarocks_constant_contract::ConstantPool;
use novarocks_functions::{FunctionArgument, FunctionKind, FunctionResultType};
use novarocks_type_contract::DecimalOverflowPolicy;
use std::{collections::HashMap, sync::Mutex};

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
pub(in crate::planner::distributed::build) fn policy() -> novarocks_functions::ConstantPolicy {
    crate::constant::test_constant_policy()
}
fn project(
    control: &dyn PureCompileControl,
    projection: impl FnOnce(
        &mut CompileCheckpoints<'_>,
    ) -> Result<Box<[FunctionArgument]>, SqlOperationalProjectionError>,
) -> Result<Box<[FunctionArgument]>, SqlOperationalProjectionError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
    let result = projection(&mut work);
    if matches!(&result, Err(SqlOperationalProjectionError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
pub(in crate::planner::distributed::build) fn integer(value: i64) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(value)),
        value_type: ValueType::new(DataType::Int64, false),
    }
}
fn reference(column: &OutputColumn) -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: column.column_id,
            qualifier: None,
            column: column.name.clone(),
        },
        value_type: column.value_type.clone(),
    }
}
pub(in crate::planner::distributed::build) fn syntax_null() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        value_type: ValueType::new(
            DataType::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int64,
                true,
            ))),
            true,
        ),
    }
}
fn argument(source: &TypedExpr) -> FunctionArgument {
    crate::analysis::function_argument(source, policy(), &Control::default()).unwrap()
}
pub(in crate::planner::distributed::build) fn finish(
    source: &PhysicalPlanNode,
) -> SqlAuthoredPhysicalPlan {
    let control = Control::default();
    lower_final_physical_plan(
        source,
        version(),
        dop(),
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        policy(),
        &control,
    )
    .unwrap()
    .finish_observed(&control)
    .unwrap()
}
/// Run the real driver and inspect its original Window before mandatory
/// implementation admission. The result is still the driver's final result.
pub(in crate::planner::distributed::build) fn observe_window_emission<F>(
    source: &PhysicalPlanNode,
    control: &dyn PureCompileControl,
    mut inspect: F,
) -> Result<(), ContractLoweringError>
where
    F: FnMut(
        OriginalWindowEmission<'_>,
        &mut CompileCheckpoints<'_>,
    ) -> Result<(), ContractLoweringError>,
{
    struct Observer<'inspect, F>(&'inspect mut F);
    impl<F> OriginalEmissionObserver for Observer<'_, F>
    where
        F: FnMut(
            OriginalWindowEmission<'_>,
            &mut CompileCheckpoints<'_>,
        ) -> Result<(), ContractLoweringError>,
    {
        fn observe(
            &mut self,
            view: OriginalEmissionView<'_>,
            work: &mut CompileCheckpoints<'_>,
        ) -> Result<(), ContractLoweringError> {
            assert_eq!(view.journal().window_entries.len(), 1);
            let (&(fragment, site), _) = view.journal().window_entries.first_key_value().unwrap();
            (self.0)(view.window_source(fragment, site, work)?, work)
        }
    }
    let mut observer = Observer(&mut inspect);
    lower_read_with_original_emission_observer(
        source,
        version(),
        dop(),
        None,
        crate::functions::builtin_sql_function_catalog().snapshot(),
        false,
        policy(),
        control,
        &mut observer,
    )
    .map(|_| ())
}
pub(in crate::planner::distributed::build) fn window_source(
    owner: &SqlAuthoredPhysicalPlan,
) -> (
    &Fragment,
    &novarocks_physical_plan::PhysicalNode,
    novarocks_physical_plan::PhysicalCallSite,
    &novarocks_physical_plan::WindowCallSpec,
) {
    owner
        .plan()
        .fragments()
        .values()
        .find_map(|fragment| {
            fragment.nodes().values().find_map(|node| {
                let NodeKind::Window(spec) = &node.kind else {
                    return None;
                };
                spec.calls
                    .iter()
                    .enumerate()
                    .next()
                    .map(|(ordinal, source)| {
                        (
                            fragment,
                            node,
                            novarocks_physical_plan::PhysicalCallSite::Window {
                                node: node.id,
                                call: u32::try_from(ordinal).unwrap(),
                            },
                            source,
                        )
                    })
            })
        })
        .expect("actual relational window emission")
}
pub(in crate::planner::distributed::build) fn table_source(
    owner: &SqlAuthoredPhysicalPlan,
) -> (&Fragment, &novarocks_physical_plan::PhysicalNode) {
    owner
        .plan()
        .fragments()
        .values()
        .find_map(|fragment| {
            fragment.nodes().values().find_map(|source| {
                matches!(source.kind, NodeKind::TableFunction { .. }).then_some((fragment, source))
            })
        })
        .expect("actual table emission")
}
fn window_loan<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    unit_node: &'a novarocks_physical_plan::PhysicalNode,
    window_site: novarocks_physical_plan::PhysicalCallSite,
    source: &'a novarocks_physical_plan::WindowCallSpec,
    control: &dyn PureCompileControl,
) -> Result<CheckedWindowLogicalSourceEntry<'a>, SqlSourceJournalError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result =
        owner.checked_window_source_observed(fragment, unit_node, window_site, source, &mut work);
    if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn table_loan<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    source: &'a novarocks_physical_plan::PhysicalNode,
    control: &dyn PureCompileControl,
) -> Result<CheckedTableLogicalSourceEntry<'a>, SqlSourceJournalError> {
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    let result = owner.checked_table_source_observed(fragment, source, &mut work);
    if matches!(&result, Err(SqlSourceJournalError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn constant(source: &FunctionArgument) -> Option<&novarocks_functions::ConstantValue> {
    let FunctionArgument::Value { constant, .. } = source else {
        panic!("value channel")
    };
    constant.as_ref()
}
fn emitted(
    owner: &SqlAuthoredPhysicalPlan,
    unit_node: &novarocks_physical_plan::PhysicalNode,
    id: ExprId,
) -> novarocks_functions::ConstantValue {
    let source = unit_node
        .expressions
        .as_ref()
        .unwrap()
        .definition(id)
        .unwrap();
    let ContractExprKind::Constant(reference) = source.kind else {
        panic!("actual selected constant")
    };
    reference_constant(owner, reference)
}
fn reference_constant(
    owner: &SqlAuthoredPhysicalPlan,
    reference: novarocks_constant_contract::ConstantReference,
) -> novarocks_functions::ConstantValue {
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    let value = owner
        .plan()
        .constants()
        .resolve_source_observed(reference, &mut work)
        .unwrap();
    work.finish().unwrap();
    value
}
fn root_definitions(
    fragment: &Fragment,
    node: NodeId,
    roots: &[novarocks_expression_contract::RootOrdinal],
) -> Vec<ExprId> {
    roots
        .iter()
        .map(|root| fragment.root_expression(node, *root).unwrap().id)
        .collect()
}
pub(in crate::planner::distributed::build) fn list_value(ordinal: u32) -> TypedExpr {
    let field = Arc::new(arrow::datatypes::Field::new("item", DataType::Int64, true));
    let array = ListArray::new(
        field,
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 3])),
        Arc::new(Int64Array::from(vec![Some(999), Some(7), None])),
        Some(NullBuffer::from(vec![true, true, false])),
    );
    let ty = ValueType::new(array.data_type().clone(), true);
    let root = Arc::new(ty.try_to_field("original.list").unwrap());
    let pool = ConstantPool::try_new(
        root,
        ty.clone(),
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    TypedExpr {
        kind: ExprKind::Constant(pool.value(ordinal).unwrap()),
        value_type: ty,
    }
}
// Actual installed resolver, already-typed emitter fixture. This is not a
// certificate of a parser, optimizer or lifecycle preparation capability.
pub(in crate::planner::distributed::build) fn table_fixture(
    args: Vec<TypedExpr>,
    left: bool,
) -> (PhysicalPlanNode, SqlFunctionBinding) {
    let arguments = args.iter().map(argument).collect::<Vec<_>>();
    let resolved = crate::functions::builtin_sql_function_catalog()
        .resolve_table_binding("unnest", &arguments, &Control::default())
        .unwrap();
    let FunctionResultType::Relation(results) = &resolved.selected.result_type else {
        panic!("whole relation")
    };
    let mut outputs = Vec::new();
    for (ordinal, result) in results.iter().enumerate() {
        let mut output = column(
            81 + ordinal as u32,
            "item",
            result.data_type().clone(),
            result.nullable() || left,
        );
        output.value_type = result.clone();
        output.value_type = output
            .value_type
            .with_nullable(output.value_type.nullable() || (left))
            .unwrap();
        outputs.push(output);
    }
    let binding = SqlFunctionBinding::from_plan(
        crate::planner::BINDING_AUTHORITY,
        resolved,
        DecimalOverflowPolicy::ReportError,
    );
    let input = column(1, "outer", DataType::Int64, false);
    let source = PhysicalPlanNode {
        kind: PhysicalPlanKind::TableFunction(PlanTableFunctionNode {
            function_name: "unnest".into(),
            args,
            binding: binding.clone(),
            output_columns: outputs.clone(),
            alias: None,
            is_left_join: left,
        }),
        children: vec![values(vec![input.clone()], vec![vec![integer(3)]])],
        output_columns: std::iter::once(input).chain(outputs).collect(),
        stats: stats(),
        probe_runtime_filters: vec![],
    };
    (source, binding)
}
pub(in crate::planner::distributed::build) fn window_fixture(
    args: Vec<TypedExpr>,
    function_order_by: Vec<SortItem>,
) -> (PhysicalPlanNode, SqlFunctionBinding) {
    let inputs = args
        .iter()
        .chain(function_order_by.iter().map(|item| &item.expr))
        .map(argument)
        .collect::<Vec<_>>();
    let name = if function_order_by.is_empty() {
        "min"
    } else {
        "array_agg"
    };
    let resolved = crate::functions::builtin_sql_function_catalog()
        .resolve_aggregate_binding(name, args.len(), &inputs, &Control::default())
        .unwrap();
    let FunctionResultType::Scalar(result) = &resolved.selected.result_type else {
        panic!("aggregate window scalar")
    };
    let result = result.clone();
    let order_keys = function_order_by
        .iter()
        .map(|order| novarocks_type_contract::AggregateStateOrderKey {
            ascending: order.asc,
            nulls_first: order.nulls_first,
        })
        .collect();
    let binding = SqlFunctionBinding::from_plan(
        crate::planner::BINDING_AUTHORITY,
        resolved,
        DecimalOverflowPolicy::ReportError,
    )
    .with_aggregate_state_source(novarocks_type_contract::AggregateStateInterpretation {
        distinct: false,
        order_keys: novarocks_type_contract::AggregateStateOrderKeys::from_pure_boxed(order_keys),
    });
    let input = column(1, "k", DataType::Int64, false);
    let mut output = column(
        2,
        "window_result",
        result.data_type().clone(),
        result.nullable(),
    );
    output.value_type = result.clone();
    let window = WindowExpr {
        name: name.into(),
        args,
        distinct: false,
        binding: binding.clone(),
        function_order_by,
        aggregate_binding: Some(binding.clone()),
        partition_by: vec![reference(&input)],
        order_by: vec![SortItem {
            expr: reference(&input),
            asc: true,
            nulls_first: false,
        }],
        window_frame: Some(WindowFrame {
            frame_type: WindowFrameType::Rows,
            start: WindowBound::Preceding(2),
            end: WindowBound::CurrentRow,
        }),
        result_type: result.data_type().clone(),
        output_name: output.name.clone(),
        output_column_id: output.column_id,
        ignore_nulls: false,
    };
    (
        PhysicalPlanNode {
            kind: PhysicalPlanKind::Window(PlanWindowNode {
                window_exprs: vec![window],
                output_columns: vec![input.clone(), output.clone()],
            }),
            children: vec![values(vec![input.clone()], vec![vec![integer(9)]])],
            output_columns: vec![input, output],
            stats: stats(),
            probe_runtime_filters: vec![],
        },
        binding,
    )
}

#[test]
fn window_journal_real_completion_keeps_row_number_and_min_requests() {
    for (sql, count, kind) in [
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY order_key) FROM orders",
            0,
            FunctionKind::Window,
        ),
        (
            "SELECT MIN(order_key) OVER (ORDER BY order_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM orders",
            1,
            FunctionKind::Aggregate,
        ),
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        let clone = owner.clone();
        let (fragment, unit_node, window_site, source) = window_source(&owner);
        let original = window_loan(
            &owner,
            fragment,
            unit_node,
            window_site,
            source,
            &Control::default(),
        )
        .unwrap();
        let copied = window_loan(
            &clone,
            fragment,
            unit_node,
            window_site,
            source,
            &Control::default(),
        )
        .unwrap();
        assert!(Arc::ptr_eq(owner.plan_arc(), clone.plan_arc()));
        assert!(std::ptr::eq(original.captured(), copied.captured()));
        assert!(std::ptr::eq(original.fragment(), fragment));
        assert!(std::ptr::eq(original.source(), source));
        assert_eq!(original.captured().binding().kind, kind);
        assert_eq!(original.captured().request().logical_argument_count, count);
        assert_eq!(original.captured().request().arguments.len(), count);
        assert_eq!(original.captured().constant_policy(), policy());
        let FunctionResultType::Scalar(result) =
            &original.captured().binding().selected.result_type
        else {
            panic!("original full window result")
        };
        assert!(std::ptr::eq(
            original.captured().request().expected_result_type.unwrap(),
            result
        ));
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        assert!(matches!(
            owner.checked_window_source_observed(
                fragment,
                unit_node,
                novarocks_physical_plan::PhysicalCallSite::Table { node: unit_node.id },
                source,
                &mut work
            ),
            Err(SqlSourceJournalError::InvalidSource(
                "window journal loans a different actual call slot"
            ))
        ));
        work.finish().unwrap();
        for argument in original.captured().request().arguments {
            assert!(constant(argument).is_none());
        }
    }
}

#[test]
fn table_journal_real_parser_completion_keeps_whole_relation_request() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT u.* FROM orders, UNNEST([order_key]) u",
    );
    let clone = owner.clone();
    let (fragment, source) = table_source(&owner);
    let unit_node = source;
    let receipt = table_loan(&owner, fragment, source, &Control::default()).unwrap();
    let other = table_loan(&clone, fragment, source, &Control::default()).unwrap();
    assert!(std::ptr::eq(receipt.captured(), other.captured()));
    assert!(std::ptr::eq(receipt.fragment(), fragment));
    assert!(std::ptr::eq(receipt.source(), source));
    assert!(receipt.captured().request().expected_result_type.is_none());
    let FunctionResultType::Relation(result) = &receipt.captured().binding().selected.result_type
    else {
        panic!("relation")
    };
    let NodeKind::TableFunction {
        function,
        arguments,
        ..
    } = &source.kind
    else {
        unreachable!()
    };
    assert_eq!(function.result_types.as_ref(), result.as_ref());
    assert_eq!(
        receipt.arguments(),
        root_definitions(fragment, unit_node.id, arguments).as_slice()
    );
    assert_eq!(result.len(), 1);
}

#[test]
fn window_journal_orders_only_logical_and_function_order_channels_with_original_cv() {
    let ty = ValueType::new(DataType::Int64, false);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("ordered.window").unwrap()),
        ty.clone(),
        Int64Array::from(vec![999, 7, 41]).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let selected = |ordinal| TypedExpr {
        kind: ExprKind::Constant(pool.value(ordinal).unwrap()),
        value_type: ty.clone(),
    };
    let (source, binding) = window_fixture(
        vec![selected(1)],
        vec![SortItem {
            expr: selected(2),
            asc: false,
            nulls_first: true,
        }],
    );
    let control = Control::default();
    let mut seen = 0;
    let result = observe_window_emission(&source, &control, |receipt, work| {
        seen += 1;
        let unit_node = receipt.node();
        let source = receipt.source();
        assert!(std::ptr::eq(
            receipt.captured().binding().resolved(),
            binding.resolved()
        ));
        assert_eq!(
            receipt.captured().binding().decimal_overflow_policy(),
            DecimalOverflowPolicy::ReportError
        );
        let request = receipt.captured().request();
        assert_eq!(request.logical_argument_count, 1);
        assert_eq!(request.arguments.len(), 2);
        let novarocks_physical_plan::WindowCallSpec {
            arguments: args,
            order_by: function_order_by,
            frame,
            ..
        } = source;
        assert_eq!(
            receipt.arguments(),
            &[
                receipt.definitions().root_definition(args[0]).unwrap().id,
                receipt
                    .definitions()
                    .root_definition(function_order_by[0].expr)
                    .unwrap()
                    .id
            ]
        );
        assert_eq!(function_order_by[0].direction, SortDirection::Descending);
        assert_eq!(function_order_by[0].null_ordering, NullOrdering::First);
        let projected = receipt.operational_arguments(&control)?;
        assert_eq!(projected.len(), 2);
        for (index, ordinal) in [1, 2].into_iter().enumerate() {
            let captured = constant(&request.arguments[index]).unwrap();
            let operational = constant(&projected[index]).unwrap();
            assert_eq!(operational.ordinal(), ordinal);
            assert_eq!(
                operational.pool().backing_identity(),
                captured.pool().backing_identity()
            );
            let actual = receipt.emitted_constant(receipt.arguments()[index], work)?;
            assert_eq!(actual.ordinal(), ordinal);
            assert!(Arc::ptr_eq(captured.pool().array(), pool.array()));
            assert!(Arc::ptr_eq(actual.pool().array(), captured.pool().array()));
            assert!(Arc::ptr_eq(
                actual.pool().field_ref(),
                captured.pool().field_ref()
            ));
        }
        let Some(ContractWindowFrame {
            start: ContractWindowBound::Preceding(offset),
            ..
        }) = frame
        else {
            panic!("frame offset")
        };
        assert!(receipt.arguments().iter().all(|id| !matches!(receipt.definitions().definition(*id).unwrap().kind, ContractExprKind::Constant(reference) if reference == *offset)));
        let node = unit_node;
        let NodeKind::Window(spec) = &node.kind else {
            unreachable!()
        };
        assert!(
            !receipt.arguments().contains(
                &receipt
                    .definitions()
                    .root_definition(spec.partition_by[0].expr)
                    .unwrap()
                    .id
            )
        );
        assert!(
            !receipt.arguments().contains(
                &receipt
                    .definitions()
                    .root_definition(spec.order_by[0].expr)
                    .unwrap()
                    .id
            )
        );
        Ok(())
    });
    assert_eq!(seen, 1);
    result.unwrap();
}

#[test]
fn table_journal_selected_list_ordinal_and_left_whole_relation_keep_full_source() {
    let args = vec![list_value(1), list_value(2)];
    let (source, binding) = table_fixture(args, true);
    let owner = finish(&source);
    let (fragment, node) = table_source(&owner);
    let unit_node = node;
    let receipt = table_loan(&owner, fragment, node, &Control::default()).unwrap();
    assert!(std::ptr::eq(
        receipt.captured().binding().resolved(),
        binding.resolved()
    ));
    assert_eq!(
        receipt.captured().binding().decimal_overflow_policy(),
        DecimalOverflowPolicy::ReportError
    );
    assert_eq!(receipt.captured().constant_policy(), policy());
    let request = receipt.captured().request();
    assert_eq!(request.arguments.len(), 2);
    assert!(request.expected_result_type.is_none());
    let FunctionResultType::Relation(results) = &binding.selected.result_type else {
        panic!("relation")
    };
    let NodeKind::TableFunction {
        function,
        left_outer,
        ..
    } = &node.kind
    else {
        unreachable!()
    };
    assert!(*left_outer);
    assert_eq!(results.len(), 2);
    assert_eq!(function.result_types.as_ref(), results.as_ref());
    let projected = project(&Control::default(), |work| {
        receipt.operational_arguments_observed(owner.plan().constants(), work)
    })
    .unwrap();
    assert_eq!(projected.len(), 2);
    for (index, ordinal) in [1, 2].into_iter().enumerate() {
        let captured = constant(&request.arguments[index]).unwrap();
        let operational = constant(&projected[index]).unwrap();
        assert_eq!(operational.ordinal(), ordinal);
        assert_eq!(
            operational.pool().backing_identity(),
            captured.pool().backing_identity()
        );
        assert_eq!(operational.value_type(), captured.value_type());
        let actual = emitted(&owner, unit_node, receipt.arguments()[index]);
        assert_eq!(actual.ordinal(), ordinal);
        assert!(Arc::ptr_eq(actual.pool().array(), captured.pool().array()));
        assert!(Arc::ptr_eq(
            actual.pool().field_ref(),
            captured.pool().field_ref()
        ));
        let DataType::List(field) = &captured.value_type().data_type() else {
            unreachable!()
        };
        assert_eq!(field.name(), "item");
        assert!(field.metadata().is_empty());
        assert_eq!(
            actual
                .is_null_observed(CompilePhase::Validate, &Control::default())
                .unwrap(),
            ordinal == 2
        );
    }
}

#[test]
fn window_and_table_journals_keep_none_elision_and_typed_null_source_policy() {
    let literal = integer(7);
    for input in [
        TypedExpr {
            kind: ExprKind::Nested(novarocks_parser::ast::AstBox::new(literal.clone())),
            value_type: literal.value_type.clone(),
        },
        TypedExpr {
            kind: ExprKind::Cast {
                cast_mode: None,
                expr: novarocks_parser::ast::AstBox::new(literal.clone()),
                target: DataType::Int64,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            value_type: literal.value_type.clone(),
        },
    ] {
        let (source, _) = window_fixture(vec![input], vec![]);
        let owner = finish(&source);
        let (fragment, unit_node, window_site, source) = window_source(&owner);
        let receipt = window_loan(
            &owner,
            fragment,
            unit_node,
            window_site,
            source,
            &Control::default(),
        )
        .unwrap();
        assert!(constant(&receipt.captured().request().arguments[0]).is_none());
        assert_eq!(
            emitted(&owner, unit_node, receipt.arguments()[0])
                .try_i64()
                .unwrap(),
            Some(7)
        );
    }
    for input in [
        syntax_null(),
        TypedExpr {
            value_type: list_value(1).value_type,
            kind: ExprKind::Nested(novarocks_parser::ast::AstBox::new(list_value(1))),
        },
    ] {
        let is_nested = matches!(input.kind, ExprKind::Nested(_));
        let (source, _) = table_fixture(vec![input], false);
        let owner = finish(&source);
        let (fragment, node) = table_source(&owner);
        let unit_node = node;
        let receipt = table_loan(&owner, fragment, node, &Control::default()).unwrap();
        let captured_request = receipt.captured().request();
        let captured = constant(&captured_request.arguments[0]);
        assert_eq!(captured.is_none(), is_nested);
        let actual = emitted(&owner, unit_node, receipt.arguments()[0]);
        assert_eq!(
            actual
                .is_null_observed(CompilePhase::Validate, &Control::default())
                .unwrap(),
            !is_nested
        );
        if let Some(captured) = captured {
            assert!(Arc::ptr_eq(actual.pool().array(), captured.pool().array()));
        }
    }
}

#[test]
fn window_and_table_journal_foreign_missing_and_all_small_control_prefixes_are_exact() {
    let (source, _) = window_fixture(vec![integer(7)], vec![]);
    let window_owner = finish(&source);
    let (wf, wn, original_site, ws) = window_source(&window_owner);
    let foreign_window = ws.clone();
    let foreign_wf = wf.clone();
    let NodeKind::Window(spec) = &wn.kind else {
        unreachable!()
    };
    // There is no expression-shaped Window source in this API. The absent
    // actual slot is the negative membership boundary for a relational call.
    let missing_site = novarocks_physical_plan::PhysicalCallSite::Window {
        node: wn.id,
        call: u32::try_from(spec.calls.len()).unwrap(),
    };
    for (fragment, source, site) in [
        (wf, ws, original_site),
        (wf, &foreign_window, original_site),
        (&foreign_wf, ws, original_site),
        (wf, ws, missing_site),
    ] {
        let control = Control::default();
        let result = window_loan(&window_owner, fragment, wn, site, source, &control);
        if std::ptr::eq(source, ws) && std::ptr::eq(fragment, wf) && site == original_site {
            assert!(result.is_ok());
        } else {
            assert!(matches!(
                result,
                Err(SqlSourceJournalError::InvalidSource(_))
            ));
        }
        let trace = control.trace();
        assert_eq!(trace[0], (CompilePhase::Validate, 0));
        assert!(
            trace
                .iter()
                .all(|(phase, _)| *phase == CompilePhase::Validate)
        );
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(window_loan(&window_owner,fragment,wn,site,source,&control),Err(SqlSourceJournalError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..=stop]);
            }
        }
    }
    let (source, _) = table_fixture(vec![list_value(1)], true);
    let table_owner = finish(&source);
    let (tf, ts) = table_source(&table_owner);
    let foreign_table = ts.clone();
    let foreign_tf = tf.clone();
    let missing_table = tf
        .nodes()
        .values()
        .find(|node| !matches!(node.kind, NodeKind::TableFunction { .. }))
        .unwrap();
    for (fragment, source) in [
        (tf, ts),
        (tf, &foreign_table),
        (&foreign_tf, ts),
        (tf, missing_table),
    ] {
        let control = Control::default();
        let result = table_loan(&table_owner, fragment, source, &control);
        if std::ptr::eq(source, ts) && std::ptr::eq(fragment, tf) {
            assert!(result.is_ok());
        } else if std::ptr::eq(source, missing_table) {
            assert!(matches!(result, Err(SqlSourceJournalError::MissingEntry)));
        } else {
            assert!(matches!(
                result,
                Err(SqlSourceJournalError::InvalidSource(_))
            ));
        }
        let trace = control.trace();
        assert_eq!(trace[0], (CompilePhase::Validate, 0));
        assert!(
            trace
                .iter()
                .all(|(phase, _)| *phase == CompilePhase::Validate)
        );
        for stop in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control {
                    refusal: Some((stop, cause.clone())),
                    ..Control::default()
                };
                assert!(
                    matches!(table_loan(&table_owner,fragment,source,&control),Err(SqlSourceJournalError::Control(actual)) if actual==cause)
                );
                assert_eq!(control.trace(), trace[..=stop]);
            }
        }
    }
}

#[test]
fn window_and_table_actual_capture_recording_preserve_every_lowering_control_prefix() {
    let (window, _) = window_fixture(
        vec![integer(7)],
        vec![SortItem {
            expr: integer(41),
            asc: false,
            nulls_first: true,
        }],
    );
    let (table, _) = table_fixture(vec![list_value(1), list_value(2)], true);
    let invoke_window = |control: &Control, seen: &mut usize| {
        observe_window_emission(&window, control, |emission, _| {
            *seen += 1;
            let projected = emission.operational_arguments(control)?;
            assert_eq!(projected.len(), 2);
            assert_eq!(emission.captured().request().logical_argument_count, 1);
            assert!(emission.canonical().belongs_to(emission.captured()));
            Ok(())
        })
    };
    let control = Control::default();
    let mut seen = 0;
    invoke_window(&control, &mut seen).unwrap();
    assert_eq!(seen, 1);
    let baseline = control.trace();
    assert!(
        baseline
            .iter()
            .any(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
    );
    for stop in 0..baseline.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            let mut seen = 0;
            assert!(
                matches!(invoke_window(&control, &mut seen), Err(ContractLoweringError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), baseline[..=stop]);
        }
    }
    // The supported table branch still completes the original driver.
    let functions = crate::functions::builtin_sql_function_catalog().snapshot();
    let invoke_table = |control: &Control| {
        lower_final_physical_plan(
            &table,
            version(),
            dop(),
            functions.clone(),
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            false,
            policy(),
            control,
        )
    };
    let control = Control::default();
    assert!(invoke_table(&control).is_ok());
    let baseline = control.trace();
    assert!(
        baseline
            .iter()
            .any(|(phase, _)| *phase == CompilePhase::FunctionSpecialization)
    );
    for stop in 0..baseline.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(invoke_table(&control), Err(ContractLoweringError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), baseline[..=stop]);
        }
    }
}

#[test]
fn aggregate_window_argument_limit_refuses_before_any_channel_emission() {
    // Deliberately overlong selected/emission metadata exercises admission;
    // it is not an installed call selection or a source capability certificate.
    let key = SortItem {
        expr: integer(11),
        asc: true,
        nulls_first: false,
    };
    let (mut source, original) = window_fixture(vec![integer(7)], vec![key.clone()]);
    let mut resolved = original.resolved().clone();
    resolved.selected.argument_types = novarocks_type_contract::TypedFactSlice::from_pure_boxed(
        vec![
            FunctionArgumentType::Value(ValueType::new(DataType::Int64, false));
            novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS + 1
        ]
        .into_boxed_slice(),
    );
    let binding = SqlFunctionBinding::from_plan(
        crate::planner::BINDING_AUTHORITY,
        resolved,
        DecimalOverflowPolicy::ReportError,
    );
    let PhysicalPlanKind::Window(payload) = &mut source.kind else {
        unreachable!()
    };
    let window = &mut payload.window_exprs[0];
    window.binding = binding.clone();
    window.aggregate_binding = Some(binding);
    window.function_order_by = vec![key; novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS];
    let control = Control::default();
    let mut visitor = ContractLoweringVisitor::new(
        version(),
        dop(),
        None,
        crate::functions::builtin_sql_function_catalog().snapshot(),
        novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
        false,
        policy(),
        &control,
    )
    .unwrap();
    assert!(matches!(
        visitor.lower_window_call(NodeId::new(99), window, &BTreeMap::new()),
        Err(ContractLoweringError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert!(visitor.call_sources.expression_entries.is_empty());
    assert!(visitor.call_sources.window_entries.is_empty());
    assert!(
        visitor.fragments[&ROOT_FRAGMENT_ID]
            .construction_unit(NodeId::new(99))
            .is_none_or(|unit| unit.is_empty())
    );
    assert_eq!(
        control.trace().last(),
        Some(&(CompilePhase::FunctionSpecialization, 0))
    );
}

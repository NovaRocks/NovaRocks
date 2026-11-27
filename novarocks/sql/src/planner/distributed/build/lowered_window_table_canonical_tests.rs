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
use crate::planner::payload::PlanRepeatNode;
use novarocks_physical_plan::{AggregatePhase, TableFunctionOutput};

fn checked_window<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    unit_node: &'a novarocks_physical_plan::PhysicalNode,
    window_site: novarocks_physical_plan::PhysicalCallSite,
    source: &'a novarocks_physical_plan::WindowCallSpec,
) -> CheckedWindowLogicalSourceEntry<'a> {
    let receipt = window_loan(
        owner,
        fragment,
        unit_node,
        window_site,
        source,
        &Control::default(),
    )
    .unwrap();
    let selected = receipt.canonical_selection();
    let original = receipt.captured().binding().resolved();
    let novarocks_physical_plan::WindowCallSpec {
        binding: function,
        aggregate_binding,
        ..
    } = source;
    assert_eq!(function.function_id, original.function_id);
    assert_eq!(function.kind, original.kind);
    assert_eq!(function.overload, original.selected.overload);
    assert_eq!(function.overload, selected.overload);
    assert_eq!(function.argument_types, selected.argument_types);
    let FunctionResultType::Scalar(result) = &selected.result_type else {
        panic!("window scalar result");
    };
    assert_eq!(&source.binding.result_type, result);
    assert_eq!(&function.result_type, result);
    let projected = project(&Control::default(), |work| {
        receipt.operational_arguments_observed(owner.plan().constants(), work)
    })
    .unwrap();
    assert_eq!(
        selected.argument_types.as_ref(),
        projected
            .iter()
            .map(FunctionArgument::argument_type)
            .collect::<Vec<_>>()
            .as_slice()
    );
    match (&selected.aggregate, aggregate_binding) {
        (Some(state), Some(binding)) => {
            assert_eq!(binding.phase, AggregatePhase::Single);
            assert_eq!(binding.function, *function);
            assert_eq!(
                binding.logical_argument_count as usize,
                receipt.captured().request().logical_argument_count
            );
            assert_eq!(binding.intermediate_type, state.intermediate_type);
            assert_eq!(binding.state_format, state.state_format);
        }
        (None, None) => assert_eq!(function.kind, FunctionKind::Window),
        _ => panic!("aggregate metadata must come from the same selection"),
    }
    let node = unit_node;
    let NodeKind::Window(spec) = &node.kind else {
        unreachable!()
    };
    let output = source.output;
    assert!(spec.calls.iter().any(|call| std::ptr::eq(call, source)));
    let value = fragment.values().get(&output).unwrap();
    assert_eq!(&value.ty, result);
    assert!(
        matches!(value.origin, ValueOrigin::NodeOutput { node: owner, output_ordinal }
        if owner == unit_node.id && unit_node.output.columns[output_ordinal as usize] == output)
    );
    let clone = owner.clone();
    let copied = window_loan(
        &clone,
        fragment,
        unit_node,
        window_site,
        source,
        &Control::default(),
    )
    .unwrap();
    assert!(std::ptr::eq(receipt.captured(), copied.captured()));
    assert!(Arc::ptr_eq(selected, copied.canonical_selection()));
    receipt
}

fn checked_table<'a>(
    owner: &'a SqlAuthoredPhysicalPlan,
    fragment: &'a Fragment,
    node: &'a novarocks_physical_plan::PhysicalNode,
) -> CheckedTableLogicalSourceEntry<'a> {
    let receipt = table_loan(owner, fragment, node, &Control::default()).unwrap();
    let selected = receipt.canonical_selection();
    let NodeKind::TableFunction {
        function,
        arguments,
        outputs,
        left_outer,
    } = &node.kind
    else {
        unreachable!()
    };
    let original = receipt.captured().binding().resolved();
    assert_eq!(function.function_id, original.function_id);
    assert_eq!(function.overload, original.selected.overload);
    assert_eq!(function.overload, selected.overload);
    assert_eq!(function.argument_types, selected.argument_types);
    assert!(selected.aggregate.is_none());
    let FunctionResultType::Relation(results) = &selected.result_type else {
        panic!("whole relation");
    };
    assert_eq!(function.result_types.as_ref(), results.as_ref());
    assert_eq!(
        receipt.arguments(),
        root_definitions(fragment, node.id, arguments).as_slice()
    );
    assert!(receipt.captured().request().expected_result_type.is_none());
    assert!(receipt.captured().binding().result_constraint().is_none());
    let projected = project(&Control::default(), |work| {
        receipt.operational_arguments_observed(owner.plan().constants(), work)
    })
    .unwrap();
    assert_eq!(
        selected.argument_types.as_ref(),
        projected
            .iter()
            .map(FunctionArgument::argument_type)
            .collect::<Vec<_>>()
            .as_slice()
    );
    for (ordinal, output) in outputs.iter().enumerate() {
        let value = fragment.values().get(&output.value()).unwrap();
        match output {
            TableFunctionOutput::PassThrough(input) => {
                assert_eq!(value.id, *input);
                assert!(
                    fragment
                        .nodes()
                        .get(&node.inputs[0])
                        .unwrap()
                        .output
                        .columns
                        .contains(input)
                );
            }
            TableFunctionOutput::FunctionResult { result_ordinal, .. } => {
                let result = &results[*result_ordinal as usize];
                let mut expected = result.clone();
                expected = expected
                    .with_nullable(expected.nullable() || (*left_outer))
                    .unwrap();
                assert_eq!(value.ty, expected);
                assert!(
                    matches!(value.origin, ValueOrigin::NodeOutput { node: owner, output_ordinal }
                    if owner == node.id && output_ordinal == ordinal as u32)
                );
            }
        }
    }
    let clone = owner.clone();
    let copied = table_loan(&clone, fragment, node, &Control::default()).unwrap();
    assert!(std::ptr::eq(receipt.captured(), copied.captured()));
    assert!(Arc::ptr_eq(selected, copied.canonical_selection()));
    receipt
}

fn assert_result(owner: &SqlAuthoredPhysicalPlan) {
    let result = owner.plan().result_port().unwrap();
    let fragment = owner.plan().fragments().get(&result.fragment).unwrap();
    for field in &result.fields {
        assert_eq!(field.ty, fragment.values().get(&field.value).unwrap().ty);
        assert!(result.output.columns.contains(&field.value));
    }
}

fn repeat(input: OutputColumn, row: TypedExpr) -> PhysicalPlanNode {
    let mut output = input.clone();
    output.value_type = output.value_type.with_nullable(true).unwrap();
    PhysicalPlanNode {
        kind: PhysicalPlanKind::Repeat(PlanRepeatNode {
            repeat_column_ref_list: vec![vec![input.name.clone()], vec![]],
            repeat_column_ref_ids: vec![vec![input.column_id], vec![]],
            grouping_ids: vec![0, 1],
            all_rollup_columns: vec![input.name.clone()],
            all_rollup_column_ids: vec![input.column_id],
            grouping_key_aliases: vec![],
            grouping_fn_args: vec![],
            grouping_fn_arg_ids: vec![],
            grouping_fn_ids: vec![],
            virtual_tuple_id: None,
        }),
        children: vec![values(vec![input], vec![vec![row]])],
        output_columns: vec![output],
        stats: stats(),
        probe_runtime_filters: vec![],
    }
}

#[test]
fn canonical_window_real_row_number_and_min_over_use_one_emitted_selection() {
    for (sql, kind, args) in [
        (
            "SELECT ROW_NUMBER() OVER (ORDER BY order_key) AS w FROM orders",
            FunctionKind::Window,
            0,
        ),
        (
            "SELECT MIN(order_key) OVER (ORDER BY order_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS w FROM orders",
            FunctionKind::Aggregate,
            1,
        ),
    ] {
        let owner = crate::compiler::compile_authored_aggregate_for_test(sql);
        let (fragment, unit_node, window_site, source) = window_source(&owner);
        let receipt = checked_window(&owner, fragment, unit_node, window_site, source);
        assert_eq!(receipt.captured().binding().kind, kind);
        assert_eq!(receipt.captured().request().arguments.len(), args);
        assert!(receipt.captured().binding().result_constraint().is_none());
        // Both installed owners retain a nullable Int64 selected carrier.
        let expected = ValueType::new(DataType::Int64, true);
        assert_eq!(source.binding.result_type, expected);
        assert_result(&owner);
        assert_eq!(owner.plan().result_port().unwrap().fields[0].ty, expected);
    }
    let (mut malformed, _) = window_fixture(vec![integer(7)], vec![]);
    malformed.output_columns[1].value_type = malformed.output_columns[1]
        .value_type
        .with_nullable(false)
        .unwrap();
    let PhysicalPlanKind::Window(window) = &mut malformed.kind else {
        unreachable!()
    };
    window.output_columns[1].value_type = window.output_columns[1]
        .value_type
        .with_nullable(false)
        .unwrap();
    assert!(matches!(
        lower_final_physical_plan(
            &malformed,
            version(),
            dop(),
            crate::functions::builtin_sql_function_catalog().snapshot(),
            novarocks_functions::testkit::shared_installed_builtin_owner_catalogue(),
            policy(),
            &Control::default(),
        ),
        Err(ContractLoweringError::OutputColumnMismatch { .. })
    ));
}

#[test]
fn canonical_window_cv_order_and_frame_remain_independent_ordered_channels() {
    let ty = ValueType::new(DataType::Int64, false);
    let root = Arc::new(
        ty.try_to_field("original.window.channels")
            .unwrap()
            .with_metadata(HashMap::from([("source.field".into(), "kept".into())])),
    );
    let pool = ConstantPool::try_new(
        root.clone(),
        ty.clone(),
        Int64Array::from(vec![999, 7, 41]).to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let cell = |ordinal| TypedExpr {
        kind: ExprKind::Constant(pool.value(ordinal).unwrap()),
        value_type: ty.clone(),
    };
    let (plan, binding) = window_fixture(
        vec![cell(1)],
        vec![SortItem {
            expr: cell(2),
            asc: false,
            nulls_first: true,
        }],
    );
    let control = Control::default();
    let mut seen = 0;
    let result = observe_window_emission(&plan, &control, |receipt, work| {
        seen += 1;
        let unit_node = receipt.node();
        let source = receipt.source();
        let selected = receipt.canonical().selected();
        assert!(receipt.canonical().belongs_to(receipt.captured()));
        let original_binding = receipt.captured().binding().resolved();
        let function = &source.binding;
        assert_eq!(function.function_id, original_binding.function_id);
        assert_eq!(function.kind, original_binding.kind);
        assert_eq!(function.overload, original_binding.selected.overload);
        assert_eq!(function.overload, selected.overload);
        assert_eq!(function.argument_types, selected.argument_types);
        let FunctionResultType::Scalar(selected_result) = &selected.result_type else {
            panic!("window scalar result");
        };
        assert_eq!(&function.result_type, selected_result);
        let (Some(state), Some(aggregate)) = (&selected.aggregate, &source.aggregate_binding)
        else {
            panic!("same canonical aggregate state");
        };
        assert_eq!(aggregate.phase, AggregatePhase::Single);
        assert_eq!(aggregate.function, *function);
        assert_eq!(
            aggregate.logical_argument_count as usize,
            receipt.captured().request().logical_argument_count
        );
        assert_eq!(aggregate.intermediate_type, state.intermediate_type);
        assert_eq!(aggregate.state_format, state.state_format);
        assert_eq!(
            aggregate.state_interpretation.as_ref(),
            receipt.captured().binding().aggregate_state_source(),
        );

        let output = receipt.builder().value(source.output).unwrap();
        assert_eq!(&output.ty, selected_result);
        assert!(
            matches!(output.origin, ValueOrigin::NodeOutput { node: owner, output_ordinal }
        if owner == unit_node.id && unit_node.output.columns[output_ordinal as usize] == source.output)
        );
        assert!(std::ptr::eq(
            receipt.captured().binding().resolved(),
            binding.resolved()
        ));
        let original = receipt.captured().request();
        assert_eq!(original.logical_argument_count, 1);
        assert_eq!(original.arguments.len(), 2);
        let projected = receipt.operational_arguments(&control)?;
        assert_eq!(
            selected.argument_types.as_ref(),
            projected
                .iter()
                .map(FunctionArgument::argument_type)
                .collect::<Vec<_>>()
                .as_slice()
        );
        for (index, (ordinal, expected)) in [(1, 7), (2, 41)].into_iter().enumerate() {
            let cv = constant(&projected[index]).unwrap();
            assert_eq!(cv.ordinal(), ordinal);
            assert_eq!(cv.value_type(), &ty);
            assert_eq!(cv.try_i64().unwrap(), Some(expected));
            assert!(Arc::ptr_eq(cv.pool().field_ref(), &root));
            assert!(Arc::ptr_eq(cv.pool().array(), pool.array()));
            let actual = receipt.emitted_constant(receipt.arguments()[index], work)?;
            assert_eq!(actual.ordinal(), ordinal);
            assert!(Arc::ptr_eq(actual.pool().field_ref(), &root));
        }
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
        let Some(ContractWindowFrame {
            start: ContractWindowBound::Preceding(offset),
            end: ContractWindowBound::CurrentRow,
            ..
        }) = frame
        else {
            panic!("original ROWS frame");
        };
        assert!(receipt.arguments().iter().all(|id| !matches!(receipt.definitions().definition(*id).unwrap().kind, ContractExprKind::Constant(reference) if reference == *offset)));
        assert_eq!(
            receipt
                .reference_constant(*offset, work)?
                .try_i64()
                .unwrap(),
            Some(2)
        );
        let NodeKind::Window(spec) = &unit_node.kind else {
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
fn canonical_window_table_none_and_null_keep_original_operational_presence() {
    for kind in [
        ExprKind::Nested(novarocks_parser::ast::AstBox::new(integer(7))),
        ExprKind::Cast {
            cast_mode: None,
            expr: novarocks_parser::ast::AstBox::new(integer(7)),
            target: DataType::Int64,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
    ] {
        let input = TypedExpr {
            kind,
            value_type: ValueType::new(DataType::Int64, false),
        };
        let (plan, _) = window_fixture(vec![input], vec![]);
        let owner = finish(&plan);
        let (fragment, unit_node, window_site, source) = window_source(&owner);
        let receipt = checked_window(&owner, fragment, unit_node, window_site, source);
        assert!(constant(&receipt.captured().request().arguments[0]).is_none());
        let projected = project(&Control::default(), |work| {
            receipt.operational_arguments_observed(owner.plan().constants(), work)
        })
        .unwrap();
        assert!(constant(&projected[0]).is_none());
        assert_eq!(
            emitted(&owner, unit_node, receipt.arguments()[0])
                .try_i64()
                .unwrap(),
            Some(7)
        );
    }
    let input = TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Null),
        value_type: ValueType::new(DataType::Int64, true),
    };
    let (plan, _) = window_fixture(vec![input], vec![]);
    let owner = finish(&plan);
    let (fragment, unit_node, window_site, source) = window_source(&owner);
    let receipt = checked_window(&owner, fragment, unit_node, window_site, source);
    let original = receipt.captured().request();
    assert_eq!(
        constant(&original.arguments[0]).unwrap().try_i64().unwrap(),
        None
    );
    assert_eq!(
        emitted(&owner, unit_node, receipt.arguments()[0])
            .try_i64()
            .unwrap(),
        None
    );
    for input in [
        syntax_null(),
        TypedExpr {
            value_type: list_value(1).value_type,
            kind: ExprKind::Nested(novarocks_parser::ast::AstBox::new(list_value(1))),
        },
    ] {
        let none = matches!(input.kind, ExprKind::Nested(_));
        let (plan, _) = table_fixture(vec![input], false);
        let owner = finish(&plan);
        let (fragment, node) = table_source(&owner);
        let unit_node = node;
        let receipt = checked_table(&owner, fragment, node);
        let original = receipt.captured().request();
        let projected = project(&Control::default(), |work| {
            receipt.operational_arguments_observed(owner.plan().constants(), work)
        })
        .unwrap();
        assert_eq!(constant(&original.arguments[0]).is_none(), none);
        assert_eq!(constant(&projected[0]).is_none(), none);
        let actual = emitted(&owner, unit_node, receipt.arguments()[0]);
        if !none {
            assert!(Arc::ptr_eq(
                actual.pool().array(),
                constant(&original.arguments[0]).unwrap().pool().array()
            ));
        }
    }
}

#[test]
fn canonical_table_real_unnest_and_left_cv_keep_whole_relation_and_ordinals() {
    let owner = crate::compiler::compile_authored_aggregate_for_test(
        "SELECT u.* FROM orders, UNNEST([order_key]) u",
    );
    let (fragment, node) = table_source(&owner);
    let receipt = checked_table(&owner, fragment, node);
    let FunctionResultType::Relation(results) = &receipt.canonical_selection().result_type else {
        unreachable!()
    };
    assert_eq!(results.as_ref(), &[ValueType::new(DataType::Int64, true)]);
    assert_result(&owner);
    let (plan, binding) = table_fixture(vec![list_value(1), list_value(2)], true);
    let owner = finish(&plan);
    let (fragment, node) = table_source(&owner);
    let unit_node = node;
    let receipt = checked_table(&owner, fragment, node);
    assert!(std::ptr::eq(
        receipt.captured().binding().resolved(),
        binding.resolved()
    ));
    let NodeKind::TableFunction {
        left_outer,
        outputs,
        ..
    } = &node.kind
    else {
        unreachable!()
    };
    assert!(*left_outer);
    assert!(matches!(outputs[0], TableFunctionOutput::PassThrough(_)));
    assert_eq!(outputs.len(), 3);
    let original = receipt.captured().request();
    for (index, ordinal) in [1, 2].into_iter().enumerate() {
        let cv = constant(&original.arguments[index]).unwrap();
        let actual = emitted(&owner, unit_node, receipt.arguments()[index]);
        assert_eq!(actual.ordinal(), ordinal);
        assert!(Arc::ptr_eq(actual.pool().array(), cv.pool().array()));
        assert!(Arc::ptr_eq(
            actual.pool().field_ref(),
            cv.pool().field_ref()
        ));
        let DataType::List(item) = &cv.value_type().data_type() else {
            unreachable!()
        };
        assert_eq!(item.name(), "item");
        assert!(item.metadata().is_empty());
        assert_eq!(
            actual
                .is_null_observed(CompilePhase::Validate, &Control::default())
                .unwrap(),
            ordinal == 2
        );
    }
    assert_result(&owner);
}

#[test]
fn canonical_table_struct_relation_retains_nested_field_identity() {
    let leaf = Arc::new(arrow::datatypes::Field::new(
        "nested.payload",
        DataType::Int64,
        false,
    ));
    let children = arrow::array::StructArray::new(
        vec![leaf.clone()].into(),
        vec![Arc::new(Int64Array::from(vec![999, 7, 8]))],
        None,
    );
    let item = Arc::new(arrow::datatypes::Field::new(
        "item",
        children.data_type().clone(),
        false,
    ));
    let array = ListArray::new(
        item.clone(),
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3])),
        Arc::new(children),
        None,
    );
    let ty = ValueType::new(array.data_type().clone(), false);
    let field = Arc::new(ty.try_to_field("selected.records").unwrap());
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    let input = TypedExpr {
        kind: ExprKind::Constant(pool.value(1).unwrap()),
        value_type: ty,
    };
    let (plan, _) = table_fixture(vec![input], true);
    let owner = finish(&plan);
    let (fragment, node) = table_source(&owner);
    let receipt = checked_table(&owner, fragment, node);
    let request = receipt.captured().request();
    let cv = constant(&request.arguments[0]).unwrap();
    assert_eq!(cv.ordinal(), 1);
    assert!(Arc::ptr_eq(cv.pool().field_ref(), &field));
    let FunctionResultType::Relation(results) = &receipt.canonical_selection().result_type else {
        unreachable!()
    };
    assert_eq!(results.len(), 1);
    assert!(results[0].nullable());
    let DataType::Struct(fields) = &results[0].data_type() else {
        panic!("whole nested relation result");
    };
    assert_eq!(fields[0].name(), "nested.payload");
    assert!(fields[0].metadata().is_empty());
    let DataType::List(authored_item) = cv.value_type().data_type() else {
        unreachable!()
    };
    let DataType::Struct(authored_fields) = authored_item.data_type() else {
        unreachable!()
    };
    assert!(Arc::ptr_eq(&fields[0], &authored_fields[0]));
    let DataType::List(selected_item) = &cv.value_type().data_type() else {
        unreachable!()
    };
    assert!(Arc::ptr_eq(selected_item, authored_item));
    assert_result(&owner);
}

#[test]
fn canonical_window_table_repeat_nullable_source_changes_selected_arguments_before_emission() {
    // These are actual typed emitter fixtures using the existing Repeat author;
    // they do not assert optimizer placement or lifecycle runtime preparation.
    let input = column(1, "k", DataType::Int64, false);
    let (mut plan, binding) = window_fixture(vec![reference(&input)], vec![]);
    plan.children = vec![repeat(input, integer(9))];
    plan.output_columns[0].value_type = plan.output_columns[0]
        .value_type
        .with_nullable(true)
        .unwrap();
    let PhysicalPlanKind::Window(window) = &mut plan.kind else {
        unreachable!()
    };
    window.output_columns[0].value_type = window.output_columns[0]
        .value_type
        .with_nullable(true)
        .unwrap();
    let owner = finish(&plan);
    let (fragment, unit_node, window_site, source) = window_source(&owner);
    let receipt = checked_window(&owner, fragment, unit_node, window_site, source);
    assert!(std::ptr::eq(
        receipt.captured().binding().resolved(),
        binding.resolved()
    ));
    assert!(
        matches!(&receipt.captured().request().arguments[0],FunctionArgument::Value{value_type,constant:None}
        if value_type==&ValueType::new(DataType::Int64,false))
    );
    assert_eq!(
        receipt.canonical_selection().argument_types.as_ref(),
        &[novarocks_functions::FunctionArgumentType::Value(
            ValueType::new(DataType::Int64, true)
        )]
    );
    let child = unit_node
        .expressions
        .as_ref()
        .unwrap()
        .definition(receipt.arguments()[0])
        .unwrap();
    let Some(value) = child.kind.input_value() else {
        unreachable!()
    };
    assert!(matches!(
        fragment.values().get(&value).unwrap().origin,
        ValueOrigin::NullExtended { .. }
    ));
    assert_result(&owner);

    let row = list_value(1);
    // The selected ordinal is non-NULL; narrowing its checked source type would
    // retag a CV. Rebuild a lawful nonnullable pool from that same Arrow source
    // instead of changing its original full type.
    let ExprKind::Constant(original) = &row.kind else {
        unreachable!()
    };
    let array = ListArray::new(
        match &row.value_type.data_type() {
            DataType::List(item) => item.clone(),
            _ => unreachable!(),
        },
        OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3])),
        Arc::new(Int64Array::from(vec![Some(999), Some(7), None])),
        None,
    );
    let ty = ValueType::new(array.data_type().clone(), false);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("repeat.list").unwrap()),
        ty.clone(),
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(pool.value(1).unwrap().ordinal(), original.ordinal());
    let row = TypedExpr {
        kind: ExprKind::Constant(pool.value(1).unwrap()),
        value_type: ty.clone(),
    };
    let mut input = column(1, "outer_list", ty.data_type().clone(), false);
    input.value_type = ty.clone();
    let (mut plan, binding) = table_fixture(vec![reference(&input)], true);
    plan.children = vec![repeat(input.clone(), row)];
    input.value_type = input.value_type.with_nullable(true).unwrap();
    plan.output_columns[0] = input;
    let owner = finish(&plan);
    let (fragment, node) = table_source(&owner);
    let unit_node = node;
    let receipt = checked_table(&owner, fragment, node);
    assert!(std::ptr::eq(
        receipt.captured().binding().resolved(),
        binding.resolved()
    ));
    assert!(
        matches!(&receipt.captured().request().arguments[0],FunctionArgument::Value{value_type,constant:None}
        if value_type==&ty)
    );
    let novarocks_functions::FunctionArgumentType::Value(selected) =
        &receipt.canonical_selection().argument_types[0]
    else {
        unreachable!()
    };
    let mut expected = ty;
    expected = expected.with_nullable(true).unwrap();
    assert_eq!(selected, &expected);
    let child = unit_node
        .expressions
        .as_ref()
        .unwrap()
        .definition(receipt.arguments()[0])
        .unwrap();
    let Some(value) = child.kind.input_value() else {
        unreachable!()
    };
    assert!(matches!(
        fragment.values().get(&value).unwrap().origin,
        ValueOrigin::NullExtended { .. }
    ));
    assert_result(&owner);
}

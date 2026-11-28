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
use crate::binding::SqlFunctionBinding;
use crate::column_id::ColumnId;
use crate::common::{JoinKind, LiteralValue, OutputColumn};
use crate::optimizer::operator::{
    Operator, PhysicalNestLoopJoinOp, ProjectOp, ScalarProjectItem, ValuesOp,
};
use crate::optimizer::optimized_tree::{OptimizedOperatorNode, PlanExecutionProps};
use crate::optimizer::scalar::HashableLiteral;
use crate::optimizer::{BINDING_AUTHORITY, test_optimizer_control};
use arrow::datatypes::Field;
use novarocks_functions::{FunctionBindingRequest, FunctionKind, FunctionResultType};
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::sync::Arc;

fn column(arena: &mut ScalarArena, ordinal: u32, ty: FunctionValueType) -> ScalarId {
    arena.intern(ScalarNode::ColumnRef(ColumnId(ordinal)), ty)
}

fn literal(arena: &mut ScalarArena, value: LiteralValue, ty: FunctionValueType) -> ScalarId {
    arena.intern(ScalarNode::Literal(HashableLiteral(value)), ty)
}

fn add(arena: &mut ScalarArena, left: ScalarId, right: ScalarId) -> ScalarId {
    let ty = FunctionValueType::new(
        DataType::Float64,
        arena.nullable(left) || arena.nullable(right),
    );
    arena.intern(
        ScalarNode::BinaryOp {
            op: BinOp::Add,
            left,
            right,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        ty,
    )
}

#[test]
fn truth_reader_and_value_reader_share_an_exact_nullable_boolean() {
    let mut arena = ScalarArena::new();
    let left = column(
        &mut arena,
        1,
        FunctionValueType::new(DataType::Float64, true),
    );
    let right = column(
        &mut arena,
        2,
        FunctionValueType::new(DataType::Float64, true),
    );
    let common = arena.intern(
        ScalarNode::BinaryOp {
            op: BinOp::Gt,
            left,
            right,
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
        FunctionValueType::new(DataType::Boolean, true),
    );

    let sites = UseSites::inspect(
        &arena,
        [
            UseRoot::new(common, EvaluationDemand::TruthOnly),
            UseRoot::value(common),
        ],
        test_optimizer_control(),
    )
    .unwrap();
    let commons = sites.candidates(&arena);
    assert_eq!(commons, [common]);
    let (items, replacements) = super::super::build_commons(
        &mut arena,
        &mut crate::column_id::ColumnRefFactory::new(),
        &commons,
        test_optimizer_control(),
    )
    .unwrap();
    assert!(arena.nullable(items[0].expr));
    assert!(arena.nullable(replacements[&common]));
}

#[test]
fn one_guarded_use_refuses_a_definition_wide_replacement() {
    let mut arena = ScalarArena::new();
    let float = FunctionValueType::new(DataType::Float64, true);
    let left = column(&mut arena, 1, float.clone());
    let right = column(&mut arena, 2, float.clone());
    let common = add(&mut arena, left, right);
    let when = column(
        &mut arena,
        3,
        FunctionValueType::new(DataType::Boolean, true),
    );
    let case = arena.intern(
        ScalarNode::Case {
            ieee_float_equality: false,
            operand: None,
            when_then: vec![(when, common)],
            else_expr: Some(left),
        },
        float,
    );
    let sites = UseSites::inspect(
        &arena,
        [
            UseRoot::value(common),
            UseRoot::value(common),
            UseRoot::value(case),
        ],
        test_optimizer_control(),
    )
    .unwrap();
    assert!(!sites.candidates(&arena).contains(&common));
}

fn call(arena: &mut ScalarArena, name: &str, args: Vec<ScalarId>) -> ScalarId {
    let arguments = args
        .iter()
        .map(|&id| {
            let expr = crate::planner::optimizer_bridge::scalar::materialize(arena, id);
            crate::analysis::function_argument(
                &expr,
                arena.constant_policy(),
                test_optimizer_control(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let binding = crate::functions::builtin_engine_function_catalog()
        .resolve_bound_trusted(
            name,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments: &arguments,
                logical_argument_count: args.len(),
            },
            test_optimizer_control(),
        )
        .unwrap();
    let FunctionResultType::Scalar(output) = binding.selected.result_type.clone() else {
        panic!("expected a scalar result");
    };
    let volatility = binding.semantics.volatility;
    let binding = SqlFunctionBinding::from_plan(
        BINDING_AUTHORITY,
        binding,
        DecimalOverflowPolicy::OutputNull,
    );
    arena.intern(
        ScalarNode::FunctionCall {
            name: name.into(),
            args,
            distinct: false,
            binding,
            volatility,
        },
        output,
    )
}

#[test]
fn no_arguments_call_contributes_no_uses_and_keeps_its_original_arguments() {
    let mut arena = ScalarArena::new();
    let float = FunctionValueType::new(DataType::Float64, true);
    let left = column(&mut arena, 1, float.clone());
    let right = column(&mut arena, 2, float);
    let common = add(&mut arena, left, right);
    let row = DataType::Struct(vec![Field::new("item", DataType::Int32, true)].into());
    let list = column(
        &mut arena,
        3,
        FunctionValueType::new(
            DataType::List(Arc::new(Field::new("item", row, true))),
            true,
        ),
    );
    let field_name = literal(
        &mut arena,
        LiteralValue::String("item".into()),
        FunctionValueType::new(DataType::Utf8, false),
    );
    let no_arguments = call(
        &mut arena,
        "__array_struct_subfield",
        vec![list, field_name, common],
    );
    assert_eq!(
        arena
            .call_argument_control(no_arguments, test_optimizer_control())
            .unwrap(),
        Some(novarocks_type_contract::ArgumentControl::NoArguments)
    );

    let inspect = |arena: &ScalarArena, roots| {
        UseSites::inspect(arena, roots, test_optimizer_control()).unwrap()
    };
    let sites = inspect(
        &arena,
        vec![UseRoot::value(common), UseRoot::value(no_arguments)],
    );
    assert!(sites.candidates(&arena).is_empty());
    let sites = inspect(
        &arena,
        vec![
            UseRoot::value(common),
            UseRoot::value(common),
            UseRoot::value(no_arguments),
        ],
    );
    assert_eq!(sites.candidates(&arena), [common]);
    let replacements = HashMap::from([(common, left)]);
    assert_eq!(
        super::super::Substitution {
            replacements: &replacements,
            unchanged_arguments: sites.unchanged_call_arguments(),
            control: test_optimizer_control(),
        }
        .apply(&mut arena, no_arguments)
        .unwrap(),
        no_arguments
    );
}

fn output_column(ordinal: u32, nullable: bool) -> OutputColumn {
    OutputColumn {
        column_id: ColumnId(ordinal),
        name: format!("c{ordinal}"),
        value_type: FunctionValueType::new(DataType::Float64, nullable),
        is_internal: false,
    }
}

fn node(
    op: Operator,
    output_columns: Vec<OutputColumn>,
    children: Vec<OptimizedOperatorNode>,
) -> OptimizedOperatorNode {
    OptimizedOperatorNode {
        op,
        output_columns,
        children,
        stats: Default::default(),
        explain_stats: Default::default(),
        execution_props: PlanExecutionProps::default(),
    }
}

fn input(columns: Vec<OutputColumn>) -> OptimizedOperatorNode {
    node(
        Operator::PhysicalValues(ValuesOp {
            rows: vec![],
            columns: columns.clone(),
        }),
        columns,
        vec![],
    )
}

#[test]
fn outer_join_output_value_cannot_bind_below_null_extension() {
    let mut arena = ScalarArena::new();
    let nullable_right = column(
        &mut arena,
        2,
        FunctionValueType::new(DataType::Float64, true),
    );
    let zero = literal(
        &mut arena,
        LiteralValue::Float(0.0),
        FunctionValueType::new(DataType::Float64, false),
    );
    let common = call(&mut arena, "coalesce", vec![nullable_right, zero]);
    let left = input(vec![output_column(1, false)]);
    let right = input(vec![output_column(2, false)]);
    let join = node(
        Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
            join_type: JoinKind::LeftOuter,
            condition: None,
        }),
        vec![output_column(1, false), output_column(2, true)],
        vec![left, right],
    );
    let sites = UseSites::inspect(
        &arena,
        [UseRoot::value(common), UseRoot::value(common)],
        test_optimizer_control(),
    )
    .unwrap();
    assert_eq!(
        super::super::candidates_for_input(&arena, &sites, &join),
        [common]
    );
    assert!(super::super::candidates_for_input(&arena, &sites, &join.children[1]).is_empty());
}

#[test]
fn a_project_reusing_a_column_id_for_a_different_value_is_not_passthrough() {
    let mut arena = ScalarArena::new();
    let ty = FunctionValueType::new(DataType::Float64, true);
    let original = column(&mut arena, 1, ty.clone());
    let one = literal(
        &mut arena,
        LiteralValue::Float(1.0),
        FunctionValueType::new(DataType::Float64, false),
    );
    let computed = add(&mut arena, original, one);
    let project = ProjectOp {
        items: vec![ScalarProjectItem {
            expr: computed,
            output_name: "c1".into(),
            output_column_id: ColumnId(1),
            expr_display: None,
        }],
        output_qualifier: None,
    };
    let input = input(vec![output_column(1, true)]);
    assert!(!super::super::binds_to_input(
        &arena,
        computed,
        &input,
        &HashSet::from([ColumnId(1)]),
        Some(&project),
    ));
}

#[test]
fn outer_join_projection_keeps_its_anchor_when_source_types_are_already_nullable() {
    let mut arena = ScalarArena::new();
    let nullable_right = column(
        &mut arena,
        2,
        FunctionValueType::new(DataType::Float64, true),
    );
    let zero = literal(
        &mut arena,
        LiteralValue::Float(0.0),
        FunctionValueType::new(DataType::Float64, false),
    );
    let common = call(&mut arena, "coalesce", vec![nullable_right, zero]);
    let left = input(vec![output_column(1, true)]);
    let right = input(vec![output_column(2, true)]);
    let join = node(
        Operator::PhysicalNestLoopJoin(PhysicalNestLoopJoinOp {
            join_type: JoinKind::LeftOuter,
            condition: None,
        }),
        vec![output_column(1, true), output_column(2, true)],
        vec![left, right],
    );

    let item = |ordinal| ScalarProjectItem {
        expr: common,
        output_name: format!("c{ordinal}"),
        output_column_id: ColumnId(ordinal),
        expr_display: None,
    };
    let mut plan = node(
        Operator::PhysicalProject(ProjectOp {
            items: vec![item(3), item(4)],
            output_qualifier: None,
        }),
        vec![
            output_column(3, arena.nullable(common)),
            output_column(4, arena.nullable(common)),
        ],
        vec![join],
    );

    super::super::rewrite(
        &mut plan,
        &mut arena,
        &mut crate::column_id::ColumnRefFactory::new(),
        &crate::optimizer::options::OptimizerOptions::default_settings(),
        test_optimizer_control(),
    )
    .unwrap();

    let prelude = &plan.children[0];
    let Operator::PhysicalProject(project) = &prelude.op else {
        panic!("expected shared evaluation above the join");
    };
    assert!(project.items.iter().any(|item| item.expr == common));
    let join = &prelude.children[0];
    assert!(matches!(join.op, Operator::PhysicalNestLoopJoin(_)));
    assert!(
        join.children
            .iter()
            .all(|child| matches!(child.op, Operator::PhysicalValues(_)))
    );
}

#[test]
fn flattened_boolean_syntax_does_not_supply_another_runtime_use() {
    let mut arena = ScalarArena::new();
    let boolean = FunctionValueType::new(DataType::Boolean, true);
    let left = column(&mut arena, 1, boolean.clone());
    let right = column(&mut arena, 2, boolean.clone());
    let third = column(&mut arena, 3, boolean.clone());
    let and = |arena: &mut ScalarArena, left, right| {
        arena.intern(
            ScalarNode::BinaryOp {
                op: BinOp::And,
                left,
                right,
                decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
            },
            boolean.clone(),
        )
    };
    let common = and(&mut arena, left, right);
    let flattened = and(&mut arena, common, third);
    let inspect = |arena: &ScalarArena, roots| {
        UseSites::inspect(arena, roots, test_optimizer_control()).unwrap()
    };
    let sites = inspect(
        &arena,
        vec![UseRoot::value(common), UseRoot::value(flattened)],
    );
    assert!(sites.candidates(&arena).is_empty());

    let sites = inspect(
        &arena,
        vec![
            UseRoot::value(common),
            UseRoot::value(common),
            UseRoot::value(flattened),
        ],
    );
    assert_eq!(sites.candidates(&arena), [common]);
    let replacements = HashMap::from([(common, third)]);
    let substitution = super::super::Substitution {
        replacements: &replacements,
        unchanged_arguments: sites.unchanged_call_arguments(),
        control: test_optimizer_control(),
    };
    assert_eq!(substitution.apply(&mut arena, common).unwrap(), third);
    assert_eq!(
        substitution.apply(&mut arena, flattened).unwrap(),
        flattened
    );
}

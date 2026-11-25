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

use super::{fixtures::*, *};
use crate::common::{JoinKind, LiteralValue};
use crate::optimizer::operator::{FilterOp, LogicalJoinOp, SortOp, ValuesOp};
use crate::optimizer::scalar::HashableLiteral;
use arrow::datatypes::TimeUnit;

#[test]
fn does_not_fold_expression_referencing_a_column() {
    let mut fixture = Fixture::new();
    let column = fixture.column(7);
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, column, one);
    let plan = project(sum);

    assert!(matches!(fixture.apply(plan), RewriteResult::Unchanged));
    assert_eq!(fixture.calls(), 0, "a column child is never foldable");
}

#[test]
fn folds_constant_subtree_under_a_column_reference() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let two = fixture.int_literal(2);
    let column = fixture.column(7);
    let constant_sum = fixture.binary(BinOp::Add, one, two);
    let outer = fixture.binary(BinOp::Add, constant_sum, column);
    let plan = project(outer);

    let rewritten = changed(fixture.apply(plan));
    let folded = project_expr(&rewritten);

    // `1 + 2 + col` becomes `3 + col`: the constant sub-tree collapsed but
    // the outer node still references a column and stays put.
    let ScalarNode::BinaryOp {
        op, left, right, ..
    } = fixture.node(folded)
    else {
        panic!("expected a BinaryOp root, got {:?}", fixture.node(folded));
    };
    assert_eq!(op, BinOp::Add);
    assert_int_literal(&fixture, left, 3);
    assert!(matches!(fixture.node(right), ScalarNode::ColumnRef(_)));
}

#[test]
fn does_not_fold_volatile_function_call() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let call = fixture.function_call(
        "rand",
        vec![one],
        false,
        FunctionVolatility::Volatile,
        DataType::Int64,
        false,
    );
    let plan = project(call);

    assert!(matches!(fixture.apply(plan), RewriteResult::Unchanged));
    assert_eq!(
        fixture.calls(),
        0,
        "a volatile call never creates a fold unit"
    );
}

#[test]
fn does_not_fold_side_effecting_sleep_from_catalog_volatility() {
    // `sleep(10)` has every surface property of a foldable constant: an
    // immutable literal argument and a `Boolean` output that encodes onto
    // the wire. Its only observable behavior is the delay it imposes on
    // whichever thread evaluates it, so folding it on the frontend blocked
    // the planner for the sleep duration and then shipped a bare `true` to
    // the backends -- the delay vanished from execution.
    //
    // The gate is the catalog's volatility classification, so read it from
    // the catalog here instead of hardcoding `Volatile`: this fails if
    // `sleep` ever drifts back to `Immutable`.
    for name in ["sleep", "SLEEP"] {
        let mut fixture = Fixture::new();
        let ten = fixture.int_literal(10);
        let call = fixture.function_call(
            name,
            vec![ten],
            false,
            crate::functions::builtin_function_volatility(name),
            DataType::Boolean,
            false,
        );
        let plan = project(call);

        assert!(
            matches!(fixture.apply(plan), RewriteResult::Unchanged),
            "{name} must not be folded"
        );
        assert_eq!(fixture.calls(), 0, "{name} must not create a fold unit");
    }
}

#[test]
fn does_not_fold_distinct_function_call() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let call = fixture.function_call(
        "some_agg_like_call",
        vec![one],
        true,
        FunctionVolatility::Immutable,
        DataType::Int64,
        false,
    );
    let plan = project(call);

    assert!(matches!(fixture.apply(plan), RewriteResult::Unchanged));
    assert_eq!(fixture.calls(), 0);
}

#[test]
fn does_not_fold_denylisted_function_even_when_marked_immutable() {
    for name in ["get_variant_date", "GET_VARIANT_DATE", "get_variant_time"] {
        let mut fixture = Fixture::new();
        let one = fixture.int_literal(1);
        let call = fixture.function_call(
            name,
            vec![one],
            false,
            // Deliberately Immutable: the denylist, not volatility, is
            // what must stop this fold.
            FunctionVolatility::Immutable,
            DataType::Int64,
            false,
        );
        let plan = project(call);

        assert!(
            matches!(fixture.apply(plan), RewriteResult::Unchanged),
            "{name} must not be folded"
        );
        assert_eq!(fixture.calls(), 0, "{name} must not create a fold unit");
    }
}

#[test]
fn does_not_fold_node_whose_output_type_is_not_wire_encodable() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary_typed(
        BinOp::Add,
        one,
        one,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        false,
    );
    let plan = project(sum);

    assert!(matches!(fixture.apply(plan), RewriteResult::Unchanged));
    assert_eq!(
        fixture.calls(),
        0,
        "the wire whitelist is checked before unit compilation"
    );
}

#[test]
fn wire_whitelist_accepts_exactly_the_decodable_literal_types() {
    for accepted in [
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Date32,
        DataType::Decimal128(38, 9),
        DataType::Decimal256(76, 10),
        DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
    ] {
        assert!(
            is_wire_encodable_literal_type(&FunctionValueType::new(accepted.clone(), true)),
            "{accepted:?} must be foldable"
        );
    }

    for rejected in [
        DataType::Timestamp(TimeUnit::Microsecond, None),
        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        DataType::FixedSizeBinary(14),
        DataType::Date64,
        DataType::Null,
        DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int64,
            true,
        ))),
        DataType::Struct(arrow::datatypes::Fields::empty()),
    ] {
        assert!(
            !is_wire_encodable_literal_type(&FunctionValueType::new(rejected.clone(), true)),
            "{rejected:?} must not be foldable"
        );
    }
    use novarocks_type_contract::{TemporalPrecision, TemporalType};
    for temporal in [
        TemporalType::Timestamp(TemporalPrecision::MICROSECONDS),
        TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS),
    ] {
        let typed = novarocks_expression::typing::temporal_function_value_type(temporal, true)
            .expect("exact temporal declaration");
        assert!(is_wire_encodable_literal_type(&typed));
    }
}

#[test]
fn matches_requires_at_least_one_scalar_field() {
    let fixture = Fixture::new();
    let empty_values = OptExpr::leaf(Operator::LogicalValues(ValuesOp {
        rows: vec![],
        columns: vec![],
    }));
    assert!(!fixture.matches(&empty_values));
}

#[test]
fn folds_filter_predicate() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    let plan = OptExpr::leaf(Operator::LogicalFilter(FilterOp { predicate: sum }));

    let rewritten = changed(fixture.apply(plan));
    let Operator::LogicalFilter(filter) = &rewritten.op else {
        panic!("expected LogicalFilter");
    };
    assert_int_literal(&fixture, filter.predicate, 2);
}

#[test]
fn folds_values_rows_join_condition_and_sort_keys() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let two = fixture.int_literal(2);
    let sum = fixture.binary(BinOp::Add, one, two);

    let values = OptExpr::leaf(Operator::LogicalValues(ValuesOp {
        rows: vec![vec![sum]],
        columns: vec![],
    }));
    let rewritten = changed(fixture.apply(values));
    let Operator::LogicalValues(values) = &rewritten.op else {
        panic!("expected LogicalValues");
    };
    assert_int_literal(&fixture, values.rows[0][0], 3);

    let join = OptExpr::new(
        Operator::LogicalJoin(LogicalJoinOp {
            naa_author: None,
            join_type: JoinKind::Inner,
            condition: Some(sum),
        }),
        vec![],
    );
    let rewritten = changed(fixture.apply(join));
    let Operator::LogicalJoin(join) = &rewritten.op else {
        panic!("expected LogicalJoin");
    };
    assert_int_literal(&fixture, join.condition.unwrap(), 3);

    let sort = OptExpr::leaf(Operator::LogicalSort(SortOp {
        items: vec![SortKey {
            expr: sum,
            asc: true,
            nulls_first: false,
            display: None,
        }],
        analytic_partition_exprs: vec![sum],
        partition_limit: None,
        topn_type: None,
    }));
    let rewritten = changed(fixture.apply(sort));
    let Operator::LogicalSort(sort) = &rewritten.op else {
        panic!("expected LogicalSort");
    };
    assert_int_literal(&fixture, sort.items[0].expr, 3);
    assert_int_literal(&fixture, sort.analytic_partition_exprs[0], 3);
}

#[test]
fn collapsing_nested_constant_preserves_the_authored_json_domain() {
    use arrow::array::{Array, LargeBinaryArray};
    use novarocks_type_contract::{CompilePhase, FunctionValueType, ValueLogicalType};
    use std::sync::Arc;

    let mut fixture = Fixture::new();
    let value_type = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Json,
    )
    .unwrap();
    let pool = novarocks_functions::ConstantPool::try_new(
        Arc::new(value_type.try_to_field("original_json").unwrap()),
        value_type.clone(),
        LargeBinaryArray::from(vec![b"\x01\x44\x01x".as_slice()]).to_data(),
        crate::constant::test_constant_policy(),
        CompilePhase::Validate,
        crate::optimizer::test_optimizer_control(),
    )
    .unwrap();
    let constant = fixture.intern(
        ScalarNode::Constant(pool.value(0).unwrap()),
        value_type.clone(),
    );
    let nested = fixture.intern(ScalarNode::Nested(constant), value_type.clone());
    let folded = project_expr(&changed(fixture.apply(project(nested))));
    assert_eq!(fixture.arena.borrow().value_type(folded), &value_type);
    assert_eq!(fixture.calls(), 0);
}

#[test]
fn folds_through_nested_wrapper() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    let nested = fixture.intern(
        ScalarNode::Nested(sum),
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
    );
    let plan = project(nested);

    let rewritten = changed(fixture.apply(plan));
    assert_int_literal(&fixture, project_expr(&rewritten), 2);
}

#[test]
fn folds_constants_inside_a_case_without_folding_the_case_itself() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let two = fixture.int_literal(2);
    let column = fixture.column(7);
    let when = fixture.binary_typed(BinOp::Eq, column, one, DataType::Boolean, true);
    let then = fixture.binary(BinOp::Add, one, two);
    let case = fixture.intern(
        ScalarNode::Case {
            ieee_float_equality: false,
            operand: None,
            when_then: vec![(when, then)],
            else_expr: None,
        },
        novarocks_type_contract::FunctionValueType::new(DataType::Int64, true),
    );
    let plan = project(case);

    let rewritten = changed(fixture.apply(plan));
    let folded = project_expr(&rewritten);
    let ScalarNode::Case { when_then, .. } = fixture.node(folded) else {
        panic!("Case must not be folded away in v1");
    };
    assert_int_literal(&fixture, when_then[0].1, 3);
}

#[test]
fn second_pass_over_a_folded_plan_reports_unchanged() {
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let sum = fixture.binary(BinOp::Add, one, one);
    let plan = project(sum);

    let rewritten = changed(fixture.apply(plan));
    assert!(
        matches!(fixture.apply(rewritten), RewriteResult::Unchanged),
        "folding must reach a fixed point in one pass"
    );
}

#[test]
fn short_circuit_does_not_fold_over_an_unfolded_child() {
    use novarocks_type_contract::FunctionValueType;
    let mut fixture = Fixture::new();
    let one = fixture.int_literal(1);
    let zero = fixture.int_literal(0);
    let division = fixture.binary(BinOp::Div, one, zero);
    let bool_type = FunctionValueType::new(DataType::Boolean, false);
    let falsity = fixture.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(false))),
        bool_type.clone(),
    );
    let truth = fixture.intern(
        ScalarNode::Literal(HashableLiteral(LiteralValue::Bool(true))),
        bool_type,
    );
    let comparison = fixture.binary_typed(BinOp::Eq, division, one, DataType::Boolean, true);
    let conjunction =
        fixture.binary_typed(BinOp::And, falsity, comparison, DataType::Boolean, true);
    assert!(matches!(
        fixture.apply(project(conjunction)),
        RewriteResult::Unchanged
    ));
    let nested = fixture.binary_typed(BinOp::And, truth, falsity, DataType::Boolean, false);
    let outer = fixture.binary_typed(BinOp::And, nested, comparison, DataType::Boolean, true);
    let folded = project_expr(&changed(fixture.apply(project(outer))));
    let ScalarNode::BinaryOp { left, right, .. } = fixture.node(folded) else {
        panic!("outer conjunction must stay")
    };
    assert!([left, right].contains(&comparison));
    assert!(
        [left, right]
            .iter()
            .any(|id| matches!(fixture.node(*id), ScalarNode::Constant(_)))
    );
    for (name, args) in [
        ("coalesce", vec![one, division]),
        ("if", vec![truth, one, division]),
    ] {
        let conditional = fixture.function_call(
            name,
            args,
            false,
            FunctionVolatility::Immutable,
            DataType::Int64,
            false,
        );
        assert!(matches!(
            fixture.apply(project(conditional)),
            RewriteResult::Unchanged
        ));
    }
}

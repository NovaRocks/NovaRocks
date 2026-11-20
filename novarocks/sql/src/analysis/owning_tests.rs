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

//! Owning-carrier tests only. Structural bindings do not claim execution support.
use super::*;
use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};
use std::fmt::{self, Write};

const DEPTH: usize = 20_000;
const SLOTS: usize = 27;

#[derive(Default)]
struct DebugSink(usize);
impl Write for DebugSink {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        self.0 += text.len();
        Ok(())
    }
}
fn value_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn leaf() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::ColumnRef {
            column_id: ColumnId(73),
            qualifier: Some("source".into()),
            column: "value".into(),
        },
        value_type: value_type(),
    }
}
fn marker() -> TypedExpr {
    TypedExpr {
        kind: ExprKind::Literal(LiteralValue::Int(-19)),
        value_type: value_type(),
    }
}
fn sort(expr: TypedExpr) -> SortItem {
    SortItem {
        expr,
        asc: false,
        nulls_first: true,
    }
}
fn check_marker(expr: &TypedExpr) {
    assert_eq!(expr.value_type, value_type());
    assert!(matches!(
        expr.kind,
        ExprKind::Literal(LiteralValue::Int(-19))
    ));
}
fn binding() -> crate::binding::SqlFunctionBinding {
    test_function_binding(
        "owning",
        &[marker(), marker()],
        DataType::Int64,
        false,
        crate::functions::FunctionVolatility::Immutable,
    )
}
fn wrap(slot: usize, child: TypedExpr) -> TypedExpr {
    let boxed = AstBox::new;
    let policy = DecimalOverflowPolicy::ReportError;
    let kind = match slot {
        0 => ExprKind::BinaryOp {
            left: boxed(child),
            right: boxed(marker()),
            op: BinOp::Add,
            decimal_overflow_policy: policy,
        },
        1 => ExprKind::BinaryOp {
            left: boxed(marker()),
            right: boxed(child),
            op: BinOp::Add,
            decimal_overflow_policy: policy,
        },
        2 => ExprKind::UnaryOp {
            op: UnOp::Negate,
            expr: boxed(child),
        },
        3 => ExprKind::LambdaFunction {
            params: vec![],
            body: boxed(child),
        },
        4 => ExprKind::Cast {
            expr: boxed(child),
            target: DataType::Int64,
            array_failure_mode: None,
            decimal_overflow_policy: policy,
        },
        5 => ExprKind::IsNull {
            expr: boxed(child),
            negated: true,
        },
        6 => ExprKind::InList {
            expr: boxed(child),
            list: vec![marker()].into(),
            negated: true,
        },
        7 => ExprKind::Between {
            expr: boxed(child),
            low: boxed(marker()),
            high: boxed(marker()),
            negated: true,
        },
        8 => ExprKind::Between {
            expr: boxed(marker()),
            low: boxed(child),
            high: boxed(marker()),
            negated: true,
        },
        9 => ExprKind::Between {
            expr: boxed(marker()),
            low: boxed(marker()),
            high: boxed(child),
            negated: true,
        },
        10 => ExprKind::Like {
            expr: boxed(child),
            pattern: boxed(marker()),
            negated: true,
        },
        11 => ExprKind::Like {
            expr: boxed(marker()),
            pattern: boxed(child),
            negated: true,
        },
        12 => ExprKind::Case {
            operand: Some(boxed(child)),
            when_then: vec![(marker(), marker())].into(),
            else_expr: Some(boxed(marker())),
        },
        13 => ExprKind::Case {
            operand: Some(boxed(marker())),
            when_then: vec![(marker(), marker())].into(),
            else_expr: Some(boxed(child)),
        },
        14 => ExprKind::IsTruthValue {
            expr: boxed(child),
            value: false,
            negated: true,
        },
        15 => ExprKind::Nested(boxed(child)),
        16 => ExprKind::Lambda {
            params: vec!["parameter".into()],
            body: boxed(child),
        },
        17 => ExprKind::FunctionCall {
            name: "owning".into(),
            args: vec![marker(), child].into(),
            distinct: false,
            binding: binding(),
            volatility: crate::functions::FunctionVolatility::Immutable,
        },
        18 => ExprKind::AggregateCall {
            name: "owning".into(),
            args: vec![marker(), child].into(),
            distinct: true,
            order_by: vec![sort(marker())].into(),
            resolved: binding(),
        },
        19 => ExprKind::AggregateCall {
            name: "owning".into(),
            args: vec![marker()].into(),
            distinct: true,
            order_by: vec![sort(marker()), sort(child)].into(),
            resolved: binding(),
        },
        20 => ExprKind::InList {
            expr: boxed(marker()),
            list: vec![marker(), child].into(),
            negated: true,
        },
        21 => ExprKind::Case {
            operand: Some(boxed(marker())),
            when_then: vec![(marker(), marker()), (child, marker())].into(),
            else_expr: Some(boxed(marker())),
        },
        22 => ExprKind::Case {
            operand: Some(boxed(marker())),
            when_then: vec![(marker(), marker()), (marker(), child)].into(),
            else_expr: Some(boxed(marker())),
        },
        23..=26 => {
            let (args, function_order_by, partition_by, order_by) = match slot {
                23 => (
                    vec![marker(), child],
                    vec![sort(marker())],
                    vec![marker()],
                    vec![sort(marker())],
                ),
                24 => (
                    vec![marker()],
                    vec![sort(marker()), sort(child)],
                    vec![marker()],
                    vec![sort(marker())],
                ),
                25 => (
                    vec![marker()],
                    vec![sort(marker())],
                    vec![marker(), child],
                    vec![sort(marker())],
                ),
                _ => (
                    vec![marker()],
                    vec![sort(marker())],
                    vec![marker()],
                    vec![sort(marker()), sort(child)],
                ),
            };
            ExprKind::WindowCall {
                name: "owning".into(),
                args: args.into(),
                function_order_by: function_order_by.into(),
                partition_by: partition_by.into(),
                order_by: order_by.into(),
                distinct: true,
                binding: binding(),
                aggregate_binding: None,
                window_frame: Some(WindowFrame {
                    frame_type: WindowFrameType::Rows,
                    start: WindowBound::Preceding(2),
                    end: WindowBound::CurrentRow,
                }),
                ignore_nulls: true,
            }
        }
        _ => unreachable!(),
    };
    TypedExpr {
        kind,
        value_type: value_type(),
    }
}
// These checks assert original sibling order and flags as well as depth.
fn child(slot: usize, node: &TypedExpr) -> &TypedExpr {
    assert_eq!(node.value_type, value_type());
    match (&node.kind, slot) {
        (
            ExprKind::BinaryOp {
                left,
                right,
                op: BinOp::Add,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            },
            0,
        ) => {
            check_marker(right);
            left
        }
        (
            ExprKind::BinaryOp {
                left,
                right,
                op: BinOp::Add,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            },
            1,
        ) => {
            check_marker(left);
            right
        }
        (
            ExprKind::UnaryOp {
                op: UnOp::Negate,
                expr,
            },
            2,
        ) => expr,
        (ExprKind::LambdaFunction { params, body }, 3) => {
            assert!(params.is_empty());
            body
        }
        (
            ExprKind::Cast {
                expr,
                target: DataType::Int64,
                array_failure_mode: None,
                decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            },
            4,
        ) => expr,
        (
            ExprKind::IsNull {
                expr,
                negated: true,
            },
            5,
        ) => expr,
        (
            ExprKind::InList {
                expr,
                list,
                negated: true,
            },
            6,
        ) => {
            assert_eq!(list.len(), 1);
            check_marker(&list[0]);
            expr
        }
        (
            ExprKind::Between {
                expr,
                low,
                high,
                negated: true,
            },
            7..=9,
        ) => match slot {
            7 => {
                check_marker(low);
                check_marker(high);
                expr
            }
            8 => {
                check_marker(expr);
                check_marker(high);
                low
            }
            _ => {
                check_marker(expr);
                check_marker(low);
                high
            }
        },
        (
            ExprKind::Like {
                expr,
                pattern,
                negated: true,
            },
            10,
        ) => {
            check_marker(pattern);
            expr
        }
        (
            ExprKind::Like {
                expr,
                pattern,
                negated: true,
            },
            11,
        ) => {
            check_marker(expr);
            pattern
        }
        (
            ExprKind::Case {
                operand: Some(operand),
                when_then,
                else_expr: Some(else_expr),
            },
            12 | 13 | 21 | 22,
        ) => {
            check_marker(&when_then[0].0);
            check_marker(&when_then[0].1);
            match slot {
                12 => {
                    assert_eq!(when_then.len(), 1);
                    check_marker(else_expr);
                    operand
                }
                13 => {
                    assert_eq!(when_then.len(), 1);
                    check_marker(operand);
                    else_expr
                }
                21 => {
                    assert_eq!(when_then.len(), 2);
                    check_marker(operand);
                    check_marker(else_expr);
                    check_marker(&when_then[1].1);
                    &when_then[1].0
                }
                _ => {
                    assert_eq!(when_then.len(), 2);
                    check_marker(operand);
                    check_marker(else_expr);
                    check_marker(&when_then[1].0);
                    &when_then[1].1
                }
            }
        }
        (
            ExprKind::IsTruthValue {
                expr,
                value: false,
                negated: true,
            },
            14,
        ) => expr,
        (ExprKind::Nested(expr), 15) => expr,
        (ExprKind::Lambda { params, body }, 16) => {
            assert_eq!(params, &["parameter"]);
            body
        }
        (
            ExprKind::FunctionCall {
                name,
                args,
                distinct: false,
                ..
            },
            17,
        ) => {
            assert_eq!(name, "owning");
            assert_eq!(args.len(), 2);
            check_marker(&args[0]);
            &args[1]
        }
        (
            ExprKind::AggregateCall {
                args,
                order_by,
                distinct: true,
                ..
            },
            18,
        ) => {
            assert_eq!(args.len(), 2);
            check_marker(&args[0]);
            check_marker(&order_by[0].expr);
            &args[1]
        }
        (
            ExprKind::AggregateCall {
                args,
                order_by,
                distinct: true,
                ..
            },
            19,
        ) => {
            assert_eq!(order_by.len(), 2);
            check_marker(&args[0]);
            check_marker(&order_by[0].expr);
            assert!(!order_by[1].asc && order_by[1].nulls_first);
            &order_by[1].expr
        }
        (
            ExprKind::InList {
                expr,
                list,
                negated: true,
            },
            20,
        ) => {
            assert_eq!(list.len(), 2);
            check_marker(expr);
            check_marker(&list[0]);
            &list[1]
        }
        (
            ExprKind::WindowCall {
                args,
                function_order_by,
                partition_by,
                order_by,
                ignore_nulls: true,
                distinct: true,
                window_frame: Some(frame),
                ..
            },
            23..=26,
        ) => {
            assert_eq!(frame.start, WindowBound::Preceding(2));
            assert_eq!(frame.end, WindowBound::CurrentRow);
            for sort in function_order_by.iter().chain(order_by.iter()) {
                assert!(!sort.asc && sort.nulls_first);
            }
            match slot {
                23 => {
                    assert_eq!(args.len(), 2);
                    check_marker(&args[0]);
                    &args[1]
                }
                24 => {
                    assert_eq!(function_order_by.len(), 2);
                    check_marker(&function_order_by[0].expr);
                    &function_order_by[1].expr
                }
                25 => {
                    assert_eq!(partition_by.len(), 2);
                    check_marker(&partition_by[0]);
                    &partition_by[1]
                }
                _ => {
                    assert_eq!(order_by.len(), 2);
                    check_marker(&order_by[0].expr);
                    &order_by[1].expr
                }
            }
        }
        _ => panic!("unexpected owning slot {slot}"),
    }
}
fn take_child(slot: usize, node: TypedExpr) -> TypedExpr {
    match (node.kind, slot) {
        (ExprKind::BinaryOp { left, .. }, 0) => left.into_inner(),
        (ExprKind::BinaryOp { right, .. }, 1) => right.into_inner(),
        (ExprKind::UnaryOp { expr, .. }, 2)
        | (ExprKind::Cast { expr, .. }, 4)
        | (ExprKind::IsNull { expr, .. }, 5)
        | (ExprKind::InList { expr, .. }, 6)
        | (ExprKind::IsTruthValue { expr, .. }, 14) => expr.into_inner(),
        (ExprKind::LambdaFunction { body, .. }, 3) | (ExprKind::Lambda { body, .. }, 16) => {
            body.into_inner()
        }
        (ExprKind::Between { expr, .. }, 7) => expr.into_inner(),
        (ExprKind::Between { low, .. }, 8) => low.into_inner(),
        (ExprKind::Between { high, .. }, 9) => high.into_inner(),
        (ExprKind::Like { expr, .. }, 10) => expr.into_inner(),
        (ExprKind::Like { pattern, .. }, 11) => pattern.into_inner(),
        (
            ExprKind::Case {
                operand: Some(expr),
                ..
            },
            12,
        )
        | (
            ExprKind::Case {
                else_expr: Some(expr),
                ..
            },
            13,
        ) => expr.into_inner(),
        (ExprKind::Nested(expr), 15) => expr.into_inner(),
        (ExprKind::FunctionCall { mut args, .. }, 17)
        | (ExprKind::AggregateCall { mut args, .. }, 18)
        | (ExprKind::WindowCall { mut args, .. }, 23) => args.pop().unwrap(),
        (ExprKind::AggregateCall { mut order_by, .. }, 19)
        | (ExprKind::WindowCall { mut order_by, .. }, 26) => order_by.pop().unwrap().expr,
        (ExprKind::InList { mut list, .. }, 20) => list.pop().unwrap(),
        (ExprKind::Case { mut when_then, .. }, 21) => when_then.pop().unwrap().0,
        (ExprKind::Case { mut when_then, .. }, 22) => when_then.pop().unwrap().1,
        (
            ExprKind::WindowCall {
                mut function_order_by,
                ..
            },
            24,
        ) => function_order_by.pop().unwrap().expr,
        (
            ExprKind::WindowCall {
                mut partition_by, ..
            },
            25,
        ) => partition_by.pop().unwrap(),
        _ => panic!("unexpected owning slot {slot}"),
    }
}
fn check_leaf(node: &TypedExpr) {
    assert_eq!(node.value_type, value_type());
    assert!(
        matches!(&node.kind, ExprKind::ColumnRef { column_id: ColumnId(73), qualifier: Some(q), column } if q == "source" && column == "value")
    );
}

#[test]
fn recursive_typed_expression_owners_keep_every_child_slot_and_original_allocations() {
    let original = Box::new(leaf());
    let address = original.as_ref() as *const TypedExpr;
    let edge = AstBox::from_box(original);
    assert_eq!(edge.as_ref() as *const TypedExpr, address);
    let original = edge.into_box();
    assert_eq!(original.as_ref() as *const TypedExpr, address);
    drop(original);
    let mut original = Vec::with_capacity(11);
    original.push(leaf());
    let (address, capacity) = (original.as_ptr(), original.capacity());
    let edge = AstVec::from(original);
    assert_eq!(edge.as_ptr(), address);
    assert_eq!(edge.capacity(), capacity);
    let original = edge.into_inner();
    assert_eq!(original.as_ptr(), address);
    assert_eq!(original.capacity(), capacity);
    drop(original);
    // Each slot independently forms the entire owning cycle; no other protected
    // variant on a mixed path can hide a missing carrier at this slot.
    for slot in 0..SLOTS {
        let mut tree = leaf();
        for _ in 0..DEPTH {
            tree = wrap(slot, tree);
        }
        let mut copy = tree.clone();
        let mut cursor = &copy;
        for _ in 0..DEPTH {
            cursor = child(slot, cursor);
        }
        check_leaf(cursor);
        let mut sink = DebugSink::default();
        write!(&mut sink, "{tree:?}").unwrap();
        assert!(sink.0 > DEPTH);
        for _ in 0..DEPTH {
            copy = take_child(slot, copy);
        }
        check_leaf(&copy);
        drop(copy);
        drop(tree);
    }
}

fn output() -> AnalyzedOutputField {
    AnalyzedOutputField {
        column_id: ColumnId(73),
        name: "internal".into(),
        field_name: Some("visible".into()),
        value_type: value_type(),
        is_internal: false,
    }
}
fn query(body: QueryBody) -> ResolvedQuery {
    ResolvedQuery {
        body,
        order_by: vec![sort(marker())],
        limit: Some(7),
        offset: Some(2),
        output_columns: vec![output()],
        local_cte_ids: vec![11],
    }
}
fn base_query() -> ResolvedQuery {
    query(QueryBody::Values(ResolvedValues {
        rows: vec![vec![leaf()]],
        output_columns: vec![output()],
    }))
}
fn select() -> ResolvedSelect {
    ResolvedSelect {
        from: None,
        filter: None,
        group_by: vec![],
        having: None,
        projection: vec![],
        has_aggregation: false,
        distinct: true,
        repeat: None,
        apply_specs: vec![].into(),
        predicate_apply_specs: vec![].into(),
    }
}
fn subquery(inner: ResolvedQuery) -> Relation {
    Relation::Subquery {
        query: AstBox::new(inner),
        alias: "derived".into(),
        output_columns: vec![output()],
    }
}
fn wrap_query(slot: usize, inner: ResolvedQuery) -> ResolvedQuery {
    let mut select = select();
    match slot {
        0 => select.from = Some(subquery(inner)),
        1 => {
            return query(QueryBody::SetOperation(ResolvedSetOp {
                kind: SetOpKind::Except,
                all: false,
                left: AstBox::new(inner),
                right: AstBox::new(base_query()),
            }));
        }
        2 => {
            return query(QueryBody::SetOperation(ResolvedSetOp {
                kind: SetOpKind::Intersect,
                all: true,
                left: AstBox::new(base_query()),
                right: AstBox::new(inner),
            }));
        }
        3 => {
            select.apply_specs = vec![ApplyScalarSpec {
                mark_probe: None,
                mark_key_bindings: None,
                subquery_id: 17,
                clause: ApplyClause::Projection,
                output_column: output(),
                inner,
                correlation_column_ids: vec![ColumnId(73)],
                need_check_max_rows: true,
                subquery_text: "source SQL".into(),
            }]
            .into()
        }
        4 => {
            select.predicate_apply_specs = vec![ApplyPredicateSpec {
                subquery_id: 19,
                kind: SubqueryKind::Exists { negated: true },
                clause: ApplyClause::Where,
                output_column: output(),
                inner,
                correlation_column_ids: vec![ColumnId(73)],
                in_lhs: Some(leaf()),
                use_semi_anti: true,
                subquery_text: "source SQL".into(),
            }]
            .into()
        }
        _ => unreachable!(),
    }
    query(QueryBody::Select(select))
}
fn check_output(query: &ResolvedQuery) {
    assert_eq!(query.limit, Some(7));
    assert_eq!(query.offset, Some(2));
    assert_eq!(query.local_cte_ids, [11]);
    assert_eq!(query.output_columns.len(), 1);
    let field = &query.output_columns[0];
    assert_eq!(field.column_id, ColumnId(73));
    assert_eq!(field.name, "internal");
    assert_eq!(field.field_name.as_deref(), Some("visible"));
    assert_eq!(field.value_type, value_type());
    assert!(!field.is_internal);
    assert_eq!(query.order_by.len(), 1);
    assert!(!query.order_by[0].asc && query.order_by[0].nulls_first);
    check_marker(&query.order_by[0].expr);
}
fn query_child(slot: usize, query: &ResolvedQuery) -> &ResolvedQuery {
    check_output(query);
    match (&query.body, slot) {
        (QueryBody::SetOperation(set), 1) => {
            assert!(matches!(set.kind, SetOpKind::Except));
            assert!(!set.all);
            check_output(&set.right);
            &set.left
        }
        (QueryBody::SetOperation(set), 2) => {
            assert!(matches!(set.kind, SetOpKind::Intersect));
            assert!(set.all);
            check_output(&set.left);
            &set.right
        }
        (QueryBody::Select(select), _) => {
            assert!(select.distinct);
            assert!(select.projection.is_empty());
            assert!(select.group_by.is_empty());
            match slot {
                0 => {
                    let Some(Relation::Subquery {
                        query,
                        alias,
                        output_columns,
                    }) = &select.from
                    else {
                        panic!("actual subquery edge")
                    };
                    assert_eq!(alias, "derived");
                    assert_eq!(output_columns[0].column_id, ColumnId(73));
                    query
                }
                3 => {
                    assert_eq!(select.apply_specs.len(), 1);
                    let spec = &select.apply_specs[0];
                    assert_eq!(spec.subquery_id, 17);
                    assert!(matches!(spec.clause, ApplyClause::Projection));
                    assert!(spec.need_check_max_rows);
                    assert_eq!(spec.correlation_column_ids, [ColumnId(73)]);
                    &spec.inner
                }
                4 => {
                    assert_eq!(select.predicate_apply_specs.len(), 1);
                    let spec = &select.predicate_apply_specs[0];
                    assert_eq!(spec.subquery_id, 19);
                    assert!(matches!(spec.kind, SubqueryKind::Exists { negated: true }));
                    assert!(spec.use_semi_anti);
                    check_leaf(spec.in_lhs.as_ref().unwrap());
                    &spec.inner
                }
                _ => unreachable!(),
            }
        }
        _ => panic!("actual query owning slot"),
    }
}
fn take_query_child(slot: usize, query: ResolvedQuery) -> ResolvedQuery {
    match (query.body, slot) {
        (QueryBody::SetOperation(set), 1) => set.left.into_inner(),
        (QueryBody::SetOperation(set), 2) => set.right.into_inner(),
        (QueryBody::Select(mut select), 0) => {
            let Some(Relation::Subquery { query, .. }) = select.from.take() else {
                panic!("actual subquery edge")
            };
            query.into_inner()
        }
        (QueryBody::Select(mut select), 3) => select.apply_specs.pop().unwrap().inner,
        (QueryBody::Select(mut select), 4) => select.predicate_apply_specs.pop().unwrap().inner,
        _ => panic!("actual query owning slot"),
    }
}
#[test]
fn recursive_query_and_relation_owners_keep_original_query_fields_and_move_children() {
    for slot in 0..5 {
        let mut tree = base_query();
        for _ in 0..DEPTH {
            tree = wrap_query(slot, tree);
        }
        let mut copy = tree.clone();
        let mut cursor = &copy;
        for _ in 0..DEPTH {
            cursor = query_child(slot, cursor);
        }
        check_output(cursor);
        let QueryBody::Values(values) = &cursor.body else {
            panic!("original values")
        };
        check_leaf(&values.rows[0][0]);
        let mut sink = DebugSink::default();
        write!(&mut sink, "{tree:?}").unwrap();
        assert!(sink.0 > DEPTH);
        for _ in 0..DEPTH {
            copy = take_query_child(slot, copy);
        }
        check_output(&copy);
        drop(copy);
        drop(tree);
    }
    // The pure Relation -> JoinRelation -> Relation cycle is independent of Query.
    let mut tree = subquery(base_query());
    for level in 0..DEPTH {
        let (left, right) = if level % 2 == 0 {
            (tree, subquery(base_query()))
        } else {
            (subquery(base_query()), tree)
        };
        tree = Relation::Join(AstBox::new(JoinRelation {
            naa_author: None,
            left,
            right,
            join_type: JoinKind::Cross,
            condition: Some(marker()),
        }));
    }
    let mut copy = tree.clone();
    let mut cursor = &copy;
    for level in (0..DEPTH).rev() {
        let Relation::Join(join) = cursor else {
            panic!("actual Join carrier")
        };
        assert!(matches!(join.join_type, JoinKind::Cross));
        check_marker(join.condition.as_ref().unwrap());
        cursor = if level % 2 == 0 {
            &join.left
        } else {
            &join.right
        };
    }
    let Relation::Subquery { query, alias, .. } = cursor else {
        panic!("actual leaf relation")
    };
    assert_eq!(alias, "derived");
    check_output(query);
    let mut sink = DebugSink::default();
    write!(&mut sink, "{tree:?}").unwrap();
    assert!(sink.0 > DEPTH);
    for level in (0..DEPTH).rev() {
        let Relation::Join(join) = copy else {
            panic!("actual Join move")
        };
        let join = join.into_inner();
        copy = if level % 2 == 0 {
            join.left
        } else {
            join.right
        };
    }
    drop(copy);
    drop(tree);
}

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

use novarocks_execution::exec::chunk::ChunkSchema;
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode, LiteralValue};
use novarocks_execution::exec::min_max_predicate::{MinMaxPredicate, MinMaxPredicateValue};

#[derive(Clone, Copy)]
enum Comparison {
    Eq,
    Lt,
    Le,
    Gt,
    Ge,
}

pub(crate) fn extract_min_max_predicates(
    arena: &ExprArena,
    root: ExprId,
    scan_schema: &ChunkSchema,
) -> Vec<MinMaxPredicate> {
    let mut out = Vec::new();
    collect(arena, root, scan_schema, &mut out);
    out
}

fn collect(
    arena: &ExprArena,
    id: ExprId,
    scan_schema: &ChunkSchema,
    out: &mut Vec<MinMaxPredicate>,
) {
    match arena.node(id) {
        Some(ExprNode::And(left, right)) => {
            collect(arena, *left, scan_schema, out);
            collect(arena, *right, scan_schema, out);
        }
        Some(ExprNode::Eq(left, right)) => {
            push_comparison(arena, *left, *right, Comparison::Eq, scan_schema, out)
        }
        Some(ExprNode::Lt(left, right)) => {
            push_comparison(arena, *left, *right, Comparison::Lt, scan_schema, out)
        }
        Some(ExprNode::Le(left, right)) => {
            push_comparison(arena, *left, *right, Comparison::Le, scan_schema, out)
        }
        Some(ExprNode::Gt(left, right)) => {
            push_comparison(arena, *left, *right, Comparison::Gt, scan_schema, out)
        }
        Some(ExprNode::Ge(left, right)) => {
            push_comparison(arena, *left, *right, Comparison::Ge, scan_schema, out)
        }
        _ => {}
    }
}

fn push_comparison(
    arena: &ExprArena,
    left: ExprId,
    right: ExprId,
    comparison: Comparison,
    scan_schema: &ChunkSchema,
    out: &mut Vec<MinMaxPredicate>,
) {
    if let Some(predicate) =
        comparison_from_slot_literal(arena, left, right, comparison, scan_schema)
    {
        out.push(predicate);
        return;
    }
    let reversed = match comparison {
        Comparison::Eq => Comparison::Eq,
        Comparison::Lt => Comparison::Gt,
        Comparison::Le => Comparison::Ge,
        Comparison::Gt => Comparison::Lt,
        Comparison::Ge => Comparison::Le,
    };
    if let Some(predicate) = comparison_from_slot_literal(arena, right, left, reversed, scan_schema)
    {
        out.push(predicate);
    }
}

fn comparison_from_slot_literal(
    arena: &ExprArena,
    slot_expr: ExprId,
    literal_expr: ExprId,
    comparison: Comparison,
    scan_schema: &ChunkSchema,
) -> Option<MinMaxPredicate> {
    let ExprNode::SlotId(slot_id) = arena.node(slot_expr)? else {
        return None;
    };
    let ExprNode::Literal(literal) = arena.node(literal_expr)? else {
        return None;
    };
    let slot_type = arena.data_type(slot_expr)?;
    if arena.data_type(literal_expr)? != slot_type {
        return None;
    }
    let ordinal = scan_schema.index_of(*slot_id)?;
    if scan_schema.slots().get(ordinal)?.data_type() != slot_type {
        return None;
    }
    let value = min_max_value(literal)?;
    let column = ordinal.to_string();
    Some(match comparison {
        Comparison::Eq => MinMaxPredicate::Eq { column, value },
        Comparison::Lt => MinMaxPredicate::Lt { column, value },
        Comparison::Le => MinMaxPredicate::Le { column, value },
        Comparison::Gt => MinMaxPredicate::Gt { column, value },
        Comparison::Ge => MinMaxPredicate::Ge { column, value },
    })
}

fn min_max_value(literal: &LiteralValue) -> Option<MinMaxPredicateValue> {
    Some(match literal {
        LiteralValue::Null | LiteralValue::Decimal256 { .. } => return None,
        LiteralValue::Int8(value) => MinMaxPredicateValue::Int32(i32::from(*value)),
        LiteralValue::Int16(value) => MinMaxPredicateValue::Int32(i32::from(*value)),
        LiteralValue::Int32(value) => MinMaxPredicateValue::Int32(*value),
        LiteralValue::Int64(value) => MinMaxPredicateValue::Int64(*value),
        LiteralValue::LargeInt(value) => MinMaxPredicateValue::LargeInt(*value),
        LiteralValue::Float32(value) => MinMaxPredicateValue::Float(*value),
        LiteralValue::Float64(value) => MinMaxPredicateValue::Double(*value),
        LiteralValue::Bool(value) => MinMaxPredicateValue::Boolean(*value),
        LiteralValue::Utf8(value) => MinMaxPredicateValue::ByteArray(value.as_bytes().to_vec()),
        LiteralValue::Binary(value) => MinMaxPredicateValue::ByteArray(value.clone()),
        LiteralValue::Date32(value) => MinMaxPredicateValue::Date32(*value),
        LiteralValue::Decimal128 {
            value,
            precision,
            scale,
        } => MinMaxPredicateValue::Decimal128 {
            value: *value,
            precision: *precision,
            scale: *scale,
        },
    })
}

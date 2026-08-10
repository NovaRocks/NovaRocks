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

pub(super) fn native_scan_min_max_predicates(
    predicates: &[crate::sql::analysis::TypedExpr],
) -> Vec<novarocks_execution::exec::min_max_predicate::MinMaxPredicate> {
    let mut out = Vec::new();
    for predicate in predicates {
        collect_native_min_max_predicates(predicate, &mut out);
    }
    out
}

fn collect_native_min_max_predicates(
    expr: &crate::sql::analysis::TypedExpr,
    out: &mut Vec<novarocks_execution::exec::min_max_predicate::MinMaxPredicate>,
) {
    use crate::sql::analysis::{BinOp, ExprKind};

    match &expr.kind {
        ExprKind::Nested(inner) => collect_native_min_max_predicates(inner, out),
        ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => {
            collect_native_min_max_predicates(left, out);
            collect_native_min_max_predicates(right, out);
        }
        ExprKind::BinaryOp { left, op, right } => {
            if let Some(predicate) = native_min_max_comparison(left, *op, right) {
                out.push(predicate);
            } else if let Some(predicate) =
                native_min_max_comparison(right, reverse_comparison(*op), left)
            {
                out.push(predicate);
            }
        }
        _ => {}
    }
}

fn reverse_comparison(op: crate::sql::analysis::BinOp) -> crate::sql::analysis::BinOp {
    use crate::sql::analysis::BinOp;
    match op {
        BinOp::Lt => BinOp::Gt,
        BinOp::Le => BinOp::Ge,
        BinOp::Gt => BinOp::Lt,
        BinOp::Ge => BinOp::Le,
        other => other,
    }
}

fn native_min_max_comparison(
    column: &crate::sql::analysis::TypedExpr,
    op: crate::sql::analysis::BinOp,
    literal: &crate::sql::analysis::TypedExpr,
) -> Option<novarocks_execution::exec::min_max_predicate::MinMaxPredicate> {
    use crate::sql::analysis::{BinOp, ExprKind};
    use novarocks_execution::exec::min_max_predicate::MinMaxPredicate;

    let ExprKind::ColumnRef { column: name, .. } = &column.kind else {
        return None;
    };
    if column.data_type != literal.data_type {
        return None;
    }
    let value = native_min_max_literal(literal)?;
    Some(match op {
        BinOp::Eq => MinMaxPredicate::Eq {
            column: name.clone(),
            value,
        },
        BinOp::Lt => MinMaxPredicate::Lt {
            column: name.clone(),
            value,
        },
        BinOp::Le => MinMaxPredicate::Le {
            column: name.clone(),
            value,
        },
        BinOp::Gt => MinMaxPredicate::Gt {
            column: name.clone(),
            value,
        },
        BinOp::Ge => MinMaxPredicate::Ge {
            column: name.clone(),
            value,
        },
        _ => return None,
    })
}

fn native_min_max_literal(
    expr: &crate::sql::analysis::TypedExpr,
) -> Option<novarocks_execution::exec::min_max_predicate::MinMaxPredicateValue> {
    use crate::sql::analysis::{ExprKind, LiteralValue};
    use arrow::datatypes::{DataType, TimeUnit};
    use novarocks_execution::exec::min_max_predicate::MinMaxPredicateValue;

    let ExprKind::Literal(literal) = &expr.kind else {
        return None;
    };
    match (&expr.data_type, literal) {
        (DataType::Boolean, LiteralValue::Bool(value)) => {
            Some(MinMaxPredicateValue::Boolean(*value))
        }
        (DataType::Int8 | DataType::Int16 | DataType::Int32, LiteralValue::Int(value)) => {
            i32::try_from(*value).ok().map(MinMaxPredicateValue::Int32)
        }
        (DataType::Int64, LiteralValue::Int(value)) => Some(MinMaxPredicateValue::Int64(*value)),
        (DataType::Float32, LiteralValue::Float(value)) if value.is_finite() => {
            Some(MinMaxPredicateValue::Float(*value as f32))
        }
        (DataType::Float64, LiteralValue::Float(value)) if value.is_finite() => {
            Some(MinMaxPredicateValue::Double(*value))
        }
        (DataType::Utf8 | DataType::LargeUtf8, LiteralValue::String(value)) => {
            Some(MinMaxPredicateValue::ByteArray(value.as_bytes().to_vec()))
        }
        (DataType::Binary | DataType::LargeBinary, LiteralValue::Binary(value)) => {
            Some(MinMaxPredicateValue::ByteArray(value.clone()))
        }
        (DataType::Date32, LiteralValue::Int(value)) => {
            i32::try_from(*value).ok().map(MinMaxPredicateValue::Date32)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, _), LiteralValue::Int(value)) => {
            Some(MinMaxPredicateValue::DateTimeMicros(*value))
        }
        (DataType::Timestamp(TimeUnit::Nanosecond, _), LiteralValue::Int(value)) => {
            Some(MinMaxPredicateValue::DateTimeNanos(*value))
        }
        _ => None,
    }
}

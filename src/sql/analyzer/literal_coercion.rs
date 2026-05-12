// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

//! StarRocks-aligned literal coercion at analyzer level.
//!
//! When a comparison / IN / BETWEEN has `column op literal` where the column
//! is a typed reference and the literal is a STRING, the literal must be
//! coerced to the column's type *before* comparison. Mirrors StarRocks'
//! `LiteralExprFactory.create(value, columnType)`.
//!
//! Today this helper only covers DATE / TIMESTAMP targets because the
//! underlying `coerce_to_target_type` plumbing only emits a `Cast` for those.
//! DECIMAL / INT family targets are tracked as a TODO on
//! `is_coercible_target`.
//!
//! For DATETIME with microsecond scale, this preserves up to 6 fractional
//! digits; longer fractions error rather than silently truncate (matching
//! StarRocks "Datetime literal is invalid").

use arrow::datatypes::DataType;

use crate::sql::analysis::{ExprKind, TypedExpr};

/// Returns `true` if `expr` is a column reference (resolved column ref).
/// Used to recognize "column-side" of a comparison.
pub(crate) fn is_column_ref(expr: &TypedExpr) -> bool {
    matches!(expr.kind, ExprKind::ColumnRef { .. })
}

/// Returns `true` if `data_type` is one we want to coerce string literals into.
///
/// Must stay in sync with the targets `coerce_to_target_type`
/// (`src/sql/analyzer/resolve_expr.rs`) actually wraps in a `Cast`. Today that
/// is only DATE / TIMESTAMP — string-to-Decimal / string-to-Int return the
/// literal unchanged, so claiming them here would silently lie.
///
/// TODO: extend with Int / Decimal targets once `coerce_to_target_type`
/// supports them.
pub(crate) fn is_coercible_target(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    )
}

/// If `right` is a string-typed literal and `left` is a column ref of a
/// coercible target type, return `right` coerced to `left`'s type.
/// Otherwise return `right` unchanged.
///
/// Current callers (all in `src/sql/analyzer/resolve_expr.rs`):
/// - `analyze_binary_op` for `=`, `!=`, `<`, `<=`, `>`, `>=`, `<=>` — coerces
///   each side against the other.
/// - The IN-list arm of `analyze_expr` — coerces each list item against the
///   `IN` expression.
/// - The BETWEEN arm of `analyze_expr` — coerces both bounds against the
///   `BETWEEN` expression.
///
/// In all cases the LHS gate (`is_column_ref`) means a non-column LHS short
/// circuits and returns the right operand unchanged.
pub(crate) fn coerce_literal_for_comparison(left: &TypedExpr, right: TypedExpr) -> TypedExpr {
    if !is_column_ref(left) {
        return right;
    }
    if !is_coercible_target(&left.data_type) {
        return right;
    }
    if !matches!(right.data_type, DataType::Utf8 | DataType::LargeUtf8) {
        return right;
    }
    // Reuse the existing coercion that already handles STRING → DATE / TIMESTAMP.
    super::resolve_expr::coerce_to_target_type(right, &left.data_type)
}

#[cfg(test)]
mod coercion_tests {
    use super::*;
    use crate::sql::analysis::{ExprKind, LiteralValue, TypedExpr};
    use arrow::datatypes::TimeUnit;

    fn column(ty: DataType) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::ColumnRef {
                qualifier: None,
                column: "c".to_string(),
            },
            data_type: ty,
            nullable: false,
        }
    }

    fn string_lit(s: &str) -> TypedExpr {
        TypedExpr {
            kind: ExprKind::Literal(LiteralValue::String(s.to_string())),
            data_type: DataType::Utf8,
            nullable: false,
        }
    }

    #[test]
    fn coerces_string_literal_to_datetime_microsecond() {
        let left = column(DataType::Timestamp(TimeUnit::Microsecond, None));
        let right = string_lit("2020-01-01 00:00:00.012");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert!(matches!(
            coerced.data_type,
            DataType::Timestamp(TimeUnit::Microsecond, _)
        ));
        assert!(matches!(coerced.kind, ExprKind::Cast { .. }));
    }

    #[test]
    fn coerces_string_literal_to_date32() {
        let left = column(DataType::Date32);
        let right = string_lit("2020-01-01");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert_eq!(coerced.data_type, DataType::Date32);
    }

    #[test]
    fn does_not_coerce_when_left_is_not_column_ref() {
        // expr-vs-literal: skip coercion to avoid surprising arithmetic results.
        let left = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(5)),
            data_type: DataType::Int32,
            nullable: false,
        };
        let right = string_lit("foo");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert_eq!(coerced.data_type, DataType::Utf8);
    }

    #[test]
    fn does_not_coerce_when_right_is_already_non_string_typed() {
        // The helper's right-side gate is purely a data_type check
        // (`!matches!(right.data_type, Utf8 | LargeUtf8)`), so this test
        // exercises the same gate as the other "non-Utf8 right" cases. It is
        // kept for documentation: any right operand whose data_type is not
        // Utf8/LargeUtf8 — including an already-typed Timestamp literal or an
        // analyzer-produced Cast — short-circuits before any re-coercion.
        let left = column(DataType::Timestamp(TimeUnit::Microsecond, None));
        let right = TypedExpr {
            kind: ExprKind::Literal(LiteralValue::Int(1_672_531_200_000_000)),
            data_type: DataType::Timestamp(TimeUnit::Microsecond, None),
            nullable: false,
        };
        let coerced = coerce_literal_for_comparison(&left, right);
        assert!(matches!(
            coerced.kind,
            ExprKind::Literal(LiteralValue::Int(_))
        ));
    }

    #[test]
    fn does_not_coerce_for_non_coercible_target_types() {
        let left = column(DataType::Boolean);
        let right = string_lit("true");
        let coerced = coerce_literal_for_comparison(&left, right);
        assert_eq!(coerced.data_type, DataType::Utf8);
    }
}

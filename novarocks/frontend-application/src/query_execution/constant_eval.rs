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

//! Frontend adapter that answers the SQL compiler's constant-evaluation port
//! with the real execution kernels.
//!
//! The Frontend is the only owner allowed to bridge these two crates:
//! `novarocks-sql` must never depend on `novarocks-execution`, so a folded
//! literal can only stay bit-identical to runtime output if the Frontend
//! builds a one-node `ExprArena` and runs it through `ExprArena::eval`.
//!
//! This adapter is a dumb per-node calculator. Recursion, volatility gating,
//! foldable-shape policy, and the fail-open decision all live on the SQL side;
//! here the only decisions are "can this literal/node shape be represented
//! faithfully?" (`Ok(None)` when not) and "what did the kernel return?".

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, RecordBatch, RecordBatchOptions, StringArray,
};
use arrow::compute::kernels::cast_utils::parse_decimal;
use arrow::datatypes::{DataType, Decimal128Type, Decimal256Type, DecimalType};
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema};
use novarocks_execution::exec::expr::function::lookup_function;
use novarocks_execution::exec::expr::{
    ExprArena, ExprId, ExprNode, LiteralValue as ExecLiteralValue,
};
use novarocks_sql::compiler::{
    BinOp, FoldNodeKind, FoldRequest, LiteralValue as SqlLiteralValue, SqlConstantEvaluator, UnOp,
};
use novarocks_types::largeint;
use std::sync::Arc;

/// Zero-sized, stateless evaluator: it owns no session, catalog, or runtime
/// state, so one process-lifetime instance serves every compilation.
#[derive(Debug)]
struct ExecutionConstantEvaluator;

static EXECUTION_CONSTANT_EVALUATOR: ExecutionConstantEvaluator = ExecutionConstantEvaluator;

/// The Frontend-owned constant evaluator handed to the SQL compiler.
///
/// Frontend is the only crate that sees both the SQL compiler boundary and the
/// execution kernels, so it owns this adapter. Every compile request built here
/// passes it, which is what lets the optimizer fold constants with exactly the
/// semantics the runtime would have produced.
// Design: ADR-0100 (docs/adr/ADR-0100-constant-folding-reuses-execution-kernels-through-an-injected-port.md)
pub(crate) fn constant_evaluator() -> &'static dyn SqlConstantEvaluator {
    &EXECUTION_CONSTANT_EVALUATOR
}

impl SqlConstantEvaluator for ExecutionConstantEvaluator {
    fn eval_scalar(&self, request: &FoldRequest) -> Result<Option<SqlLiteralValue>, String> {
        let mut arena = ExprArena::default();
        let mut arg_ids: Vec<ExprId> = Vec::with_capacity(request.args.len());
        for arg in &request.args {
            let Some(literal) = sql_literal_to_exec(&arg.value, &arg.data_type) else {
                return Ok(None);
            };
            arg_ids.push(arena.push_typed(ExprNode::Literal(literal), arg.data_type.clone()));
        }

        let Some(root_node) = root_node_for(&request.kind, &arg_ids) else {
            return Ok(None);
        };
        let root = arena.push_typed(root_node, request.out_type.clone());

        let chunk = single_row_chunk()?;
        let output = arena.eval(root, &chunk)?;
        read_back_row0(&output, &request.out_type)
    }
}

/// A schemaless chunk with exactly one row.
///
/// Constant folding never reads a slot, so the chunk carries no columns; the
/// explicit row count is what makes every literal kernel materialize a
/// length-1 array.
fn single_row_chunk() -> Result<Chunk, String> {
    let chunk_schema = Arc::new(ChunkSchema::empty());
    let batch = RecordBatch::try_new_with_options(
        chunk_schema.arrow_schema_ref(),
        Vec::new(),
        &RecordBatchOptions::new().with_row_count(Some(1)),
    )
    .map_err(|error| format!("constant folding failed to build a 1-row chunk: {error}"))?;
    Chunk::try_new_with_chunk_schema(batch, chunk_schema)
}

/// Maps one SQL fold node onto the execution expression it must reuse.
///
/// Returns `None` for any shape without a direct execution counterpart: the
/// adapter must never emulate a missing kernel.
fn root_node_for(kind: &FoldNodeKind, args: &[ExprId]) -> Option<ExprNode> {
    match kind {
        FoldNodeKind::BinaryOp(op) => {
            let [lhs, rhs] = args else {
                return None;
            };
            let (lhs, rhs) = (*lhs, *rhs);
            Some(match op {
                BinOp::Add => ExprNode::Add(lhs, rhs),
                BinOp::Sub => ExprNode::Sub(lhs, rhs),
                BinOp::Mul => ExprNode::Mul(lhs, rhs),
                BinOp::Div => ExprNode::Div(lhs, rhs),
                BinOp::Mod => ExprNode::Mod(lhs, rhs),
                BinOp::Eq => ExprNode::Eq(lhs, rhs),
                BinOp::Ne => ExprNode::Ne(lhs, rhs),
                BinOp::Lt => ExprNode::Lt(lhs, rhs),
                BinOp::Le => ExprNode::Le(lhs, rhs),
                BinOp::Gt => ExprNode::Gt(lhs, rhs),
                BinOp::Ge => ExprNode::Ge(lhs, rhs),
                BinOp::EqForNull => ExprNode::EqForNull(lhs, rhs),
                BinOp::And => ExprNode::And(lhs, rhs),
                BinOp::Or => ExprNode::Or(lhs, rhs),
            })
        }
        FoldNodeKind::UnaryOp(op) => {
            let [child] = args else {
                return None;
            };
            match op {
                UnOp::Not => Some(ExprNode::Not(*child)),
                // Execution has no negation or bitwise-not expression node.
                // Emulating either here would reimplement semantics the
                // Frontend does not own, so decline instead.
                UnOp::Negate | UnOp::BitwiseNot => None,
            }
        }
        FoldNodeKind::Cast => {
            let [child] = args else {
                return None;
            };
            // The cast target is the node's own data type, which the caller
            // attaches through `push_typed(.., out_type)`.
            Some(ExprNode::Cast(*child))
        }
        FoldNodeKind::Function { name } => {
            let kind = lookup_function(name)?;
            Some(ExprNode::FunctionCall {
                kind,
                args: args.to_vec(),
            })
        }
    }
}

/// Turns one already-folded SQL literal into the execution literal the kernels
/// expect, driven by the argument's declared Arrow type.
///
/// Every arm is exact: a value that cannot round-trip through the target
/// representation yields `None` so the caller keeps the original expression.
fn sql_literal_to_exec(value: &SqlLiteralValue, data_type: &DataType) -> Option<ExecLiteralValue> {
    match (value, data_type) {
        // A typed NULL literal keeps its declared type: `ExprArena::eval`
        // materializes `new_null_array(out_type)` for a `Null` literal.
        (SqlLiteralValue::Null, _) => Some(ExecLiteralValue::Null),
        (SqlLiteralValue::Bool(v), DataType::Boolean) => Some(ExecLiteralValue::Bool(*v)),
        (SqlLiteralValue::Int(v), DataType::Int8) => {
            i8::try_from(*v).ok().map(ExecLiteralValue::Int8)
        }
        (SqlLiteralValue::Int(v), DataType::Int16) => {
            i16::try_from(*v).ok().map(ExecLiteralValue::Int16)
        }
        (SqlLiteralValue::Int(v), DataType::Int32) => {
            i32::try_from(*v).ok().map(ExecLiteralValue::Int32)
        }
        (SqlLiteralValue::Int(v), DataType::Int64) => Some(ExecLiteralValue::Int64(*v)),
        (SqlLiteralValue::Int(v), DataType::Date32) => {
            i32::try_from(*v).ok().map(ExecLiteralValue::Date32)
        }
        (SqlLiteralValue::LargeInt(v), dt) if largeint::is_largeint_data_type(dt) => {
            Some(ExecLiteralValue::LargeInt(*v))
        }
        (SqlLiteralValue::Float(v), DataType::Float64) => Some(ExecLiteralValue::Float64(*v)),
        (SqlLiteralValue::Float(v), DataType::Float32) => {
            // Accept only a FLOAT literal that survives the f64 -> f32 round
            // trip. Everything this adapter reads back from a Float32 column
            // does survive it, so the guard rejects exactly the widened values
            // the runtime would never have produced.
            let narrowed = *v as f32;
            (f64::from(narrowed).to_bits() == v.to_bits())
                .then_some(ExecLiteralValue::Float32(narrowed))
        }
        (SqlLiteralValue::String(v), DataType::Utf8) => Some(ExecLiteralValue::Utf8(v.clone())),
        (SqlLiteralValue::Binary(v), DataType::Binary) => Some(ExecLiteralValue::Binary(v.clone())),
        (SqlLiteralValue::Decimal(text), DataType::Decimal128(precision, scale)) => {
            exact_decimal_text(text, *scale)?;
            parse_decimal::<Decimal128Type>(text, *precision, *scale)
                .ok()
                .map(|value| ExecLiteralValue::Decimal128 {
                    value,
                    precision: *precision,
                    scale: *scale,
                })
        }
        (SqlLiteralValue::Decimal(text), DataType::Decimal256(precision, scale)) => {
            exact_decimal_text(text, *scale)?;
            parse_decimal::<Decimal256Type>(text, *precision, *scale)
                .ok()
                .map(|value| ExecLiteralValue::Decimal256 {
                    value,
                    precision: *precision,
                    scale: *scale,
                })
        }
        _ => None,
    }
}

/// Guards `parse_decimal`, which silently truncates surplus fraction digits
/// and accepts e-notation. Both would fold to a value the runtime never
/// produced, so only plain notation that fits the column scale is accepted.
fn exact_decimal_text(text: &str, scale: i8) -> Option<()> {
    let scale = usize::try_from(scale).ok()?;
    if text.contains(['e', 'E']) {
        return None;
    }
    match text.split_once('.') {
        Some((_, fraction)) => (fraction.len() <= scale).then_some(()),
        None => Some(()),
    }
}

/// Reads the single produced row back into the SQL literal vocabulary.
///
/// `Err` means the kernel produced something that contradicts the frozen node
/// type; `Ok(None)` means the output type has no faithful SQL literal form.
fn read_back_row0(
    output: &ArrayRef,
    out_type: &DataType,
) -> Result<Option<SqlLiteralValue>, String> {
    if output.len() != 1 {
        return Err(format!(
            "constant folding produced {} rows, expected exactly 1",
            output.len()
        ));
    }
    if !is_readable_output_type(out_type) {
        return Ok(None);
    }
    if output.data_type() != out_type {
        return Err(format!(
            "constant folding produced {:?}, expected {:?}",
            output.data_type(),
            out_type
        ));
    }
    if output.is_null(0) {
        return Ok(Some(SqlLiteralValue::Null));
    }

    let literal = match out_type {
        DataType::Boolean => SqlLiteralValue::Bool(downcast::<BooleanArray>(output)?.value(0)),
        DataType::Int8 => SqlLiteralValue::Int(i64::from(downcast::<Int8Array>(output)?.value(0))),
        DataType::Int16 => {
            SqlLiteralValue::Int(i64::from(downcast::<Int16Array>(output)?.value(0)))
        }
        DataType::Int32 => {
            SqlLiteralValue::Int(i64::from(downcast::<Int32Array>(output)?.value(0)))
        }
        DataType::Int64 => SqlLiteralValue::Int(downcast::<Int64Array>(output)?.value(0)),
        DataType::Date32 => {
            SqlLiteralValue::Int(i64::from(downcast::<Date32Array>(output)?.value(0)))
        }
        DataType::FixedSizeBinary(_) => SqlLiteralValue::LargeInt(largeint::i128_from_be_bytes(
            downcast::<FixedSizeBinaryArray>(output)?.value(0),
        )?),
        DataType::Float32 => {
            SqlLiteralValue::Float(f64::from(downcast::<Float32Array>(output)?.value(0)))
        }
        DataType::Float64 => SqlLiteralValue::Float(downcast::<Float64Array>(output)?.value(0)),
        // `StringArray::value` converts without revalidating, so a kernel is
        // able to hand back bytes that are not valid UTF-8. The plan encodes a
        // literal string as a protobuf string field, which would re-encode
        // those bytes, so decline rather than fold something the runtime would
        // not reproduce. (Byte-carrying string families such as `aes_encrypt`
        // are valid UTF-8 and are excluded by the SQL-side foldable list.)
        DataType::Utf8 => {
            let Some(text) = utf8_round_trippable(downcast::<StringArray>(output)?.value(0)) else {
                return Ok(None);
            };
            SqlLiteralValue::String(text)
        }
        DataType::LargeUtf8 => {
            let Some(text) = utf8_round_trippable(downcast::<LargeStringArray>(output)?.value(0))
            else {
                return Ok(None);
            };
            SqlLiteralValue::String(text)
        }
        DataType::Binary => {
            SqlLiteralValue::Binary(downcast::<BinaryArray>(output)?.value(0).to_vec())
        }
        DataType::LargeBinary => {
            SqlLiteralValue::Binary(downcast::<LargeBinaryArray>(output)?.value(0).to_vec())
        }
        // Arrow renders the unscaled value exactly at the column's scale, so
        // the folded text matches what the runtime would have printed — but
        // only while the value still fits the declared precision. A decimal
        // kernel is allowed to return a result wider than its own declared
        // precision (an overflowed multiply or a rounding cast such as
        // `CAST(99999.999 AS DECIMAL(7,2))` -> 100000.00), and rendering that
        // through the declared precision drops the leading digit. Decline the
        // fold instead, so the runtime keeps producing whatever it produces
        // today for out-of-range decimals.
        DataType::Decimal128(precision, _) => {
            let array = downcast::<Decimal128Array>(output)?;
            if !Decimal128Type::is_valid_decimal_precision(array.value(0), *precision) {
                return Ok(None);
            }
            SqlLiteralValue::Decimal(array.value_as_string(0))
        }
        DataType::Decimal256(precision, _) => {
            let array = downcast::<Decimal256Array>(output)?;
            if !Decimal256Type::is_valid_decimal_precision(array.value(0), *precision) {
                return Ok(None);
            }
            SqlLiteralValue::Decimal(array.value_as_string(0))
        }
        // `is_readable_output_type` already filtered everything else.
        _ => return Ok(None),
    };
    Ok(Some(literal))
}

/// Returns the string only when its bytes really are valid UTF-8.
///
/// `StringArray::value` converts without revalidating, so a kernel that stores
/// raw bytes in a Utf8 array yields a `&str` whose bytes would change the first
/// time they are re-encoded.
fn utf8_round_trippable(value: &str) -> Option<String> {
    std::str::from_utf8(value.as_bytes())
        .ok()
        .map(str::to_string)
}

/// Output types with an exact SQL literal representation.
fn is_readable_output_type(out_type: &DataType) -> bool {
    match out_type {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Date32
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary => true,
        // LARGEINT is the only FixedSizeBinary the SQL literal vocabulary can
        // express; any other width is opaque bytes with no literal form.
        DataType::FixedSizeBinary(_) => largeint::is_largeint_data_type(out_type),
        // A negative scale would render a text this adapter refuses to read
        // back in, so never fold into one.
        DataType::Decimal128(_, scale) | DataType::Decimal256(_, scale) => *scale >= 0,
        _ => false,
    }
}

fn downcast<T: 'static>(output: &ArrayRef) -> Result<&T, String> {
    output.as_any().downcast_ref::<T>().ok_or_else(|| {
        format!(
            "constant folding could not read a {:?} result as {}",
            output.data_type(),
            std::any::type_name::<T>()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_sql::compiler::FoldArg;

    fn arg(value: SqlLiteralValue, data_type: DataType) -> FoldArg {
        FoldArg {
            value,
            data_type,
            nullable: false,
        }
    }

    fn fold(
        kind: FoldNodeKind,
        args: Vec<FoldArg>,
        out_type: DataType,
    ) -> Result<Option<SqlLiteralValue>, String> {
        constant_evaluator().eval_scalar(&FoldRequest {
            kind,
            args,
            out_type,
            out_nullable: true,
        })
    }

    #[test]
    fn folds_int32_addition() {
        let folded = fold(
            FoldNodeKind::BinaryOp(BinOp::Add),
            vec![
                arg(SqlLiteralValue::Int(1), DataType::Int32),
                arg(SqlLiteralValue::Int(1), DataType::Int32),
            ],
            DataType::Int32,
        );
        assert_eq!(folded, Ok(Some(SqlLiteralValue::Int(2))));
    }

    #[test]
    fn folds_date_format_of_date32_literal() {
        // 18262 is 2020-01-01 in days since the epoch. The kernel translates
        // the MySQL format through `mysql_format_to_chrono`, so `%Y-%m-%d`
        // renders as chrono `%Y-%m-%d`.
        let folded = fold(
            FoldNodeKind::Function {
                name: "date_format".to_string(),
            },
            vec![
                arg(SqlLiteralValue::Int(18262), DataType::Date32),
                arg(
                    SqlLiteralValue::String("%Y-%m-%d".to_string()),
                    DataType::Utf8,
                ),
            ],
            DataType::Utf8,
        );
        assert_eq!(
            folded,
            Ok(Some(SqlLiteralValue::String("2020-01-01".to_string())))
        );
    }

    #[test]
    fn folds_cast_of_utf8_to_date32() {
        // 1970-01-01 -> 2020-01-01 spans 50 years with 12 leap days
        // (1972..=2016 step 4, 2000 included), i.e. 50 * 365 + 12 = 18262.
        let folded = fold(
            FoldNodeKind::Cast,
            vec![arg(
                SqlLiteralValue::String("2020-01-01".to_string()),
                DataType::Utf8,
            )],
            DataType::Date32,
        );
        assert_eq!(folded, Ok(Some(SqlLiteralValue::Int(18262))));
    }

    #[test]
    fn folds_decimal_multiplication_keeping_scale() {
        // 1.25 * 4.00 with an output scale equal to the sum of the input
        // scales, so the kernel neither rescales nor rounds: 125 * 400 = 50000
        // at scale 4.
        let folded = fold(
            FoldNodeKind::BinaryOp(BinOp::Mul),
            vec![
                arg(
                    SqlLiteralValue::Decimal("1.25".to_string()),
                    DataType::Decimal128(10, 2),
                ),
                arg(
                    SqlLiteralValue::Decimal("4.00".to_string()),
                    DataType::Decimal128(10, 2),
                ),
            ],
            DataType::Decimal128(20, 4),
        );
        assert_eq!(
            folded,
            Ok(Some(SqlLiteralValue::Decimal("5.0000".to_string())))
        );
    }

    #[test]
    fn declines_decimal_result_wider_than_its_declared_precision() {
        // `CAST(99999.999 AS DECIMAL(7,2))` rounds to 100000.00, which needs
        // precision 8. The kernel still returns it, but rendering that through
        // the declared precision 7 drops the leading digit and the fold would
        // read 10000.00 — ten times smaller than what the runtime produces.
        let folded = fold(
            FoldNodeKind::Cast,
            vec![arg(
                SqlLiteralValue::Decimal("99999.999".to_string()),
                DataType::Decimal128(8, 3),
            )],
            DataType::Decimal128(7, 2),
        );
        assert_eq!(folded, Ok(None));
    }

    #[test]
    fn folds_decimal_cast_that_still_fits_its_precision() {
        // Same rounding shape as above, one digit of headroom: the guard must
        // not reject a result the declared precision can hold.
        let folded = fold(
            FoldNodeKind::Cast,
            vec![arg(
                SqlLiteralValue::Decimal("99999.999".to_string()),
                DataType::Decimal128(8, 3),
            )],
            DataType::Decimal128(8, 2),
        );
        assert_eq!(
            folded,
            Ok(Some(SqlLiteralValue::Decimal("100000.00".to_string())))
        );
    }

    #[test]
    fn declines_unknown_function() {
        let folded = fold(
            FoldNodeKind::Function {
                name: "no_such_novarocks_function".to_string(),
            },
            vec![arg(SqlLiteralValue::Int(1), DataType::Int32)],
            DataType::Int32,
        );
        assert_eq!(folded, Ok(None));
    }

    #[test]
    fn declines_unary_negate() {
        let folded = fold(
            FoldNodeKind::UnaryOp(UnOp::Negate),
            vec![arg(SqlLiteralValue::Int(1), DataType::Int32)],
            DataType::Int32,
        );
        assert_eq!(folded, Ok(None));

        // `NOT` is the one unary op with a direct execution node.
        let folded_not = fold(
            FoldNodeKind::UnaryOp(UnOp::Not),
            vec![arg(SqlLiteralValue::Bool(true), DataType::Boolean)],
            DataType::Boolean,
        );
        assert_eq!(folded_not, Ok(Some(SqlLiteralValue::Bool(false))));
    }

    #[test]
    fn declines_unmappable_argument_literal() {
        // A string value carried on an INT slot has no faithful execution
        // literal: the adapter must not parse or coerce it.
        let mismatched_kind = fold(
            FoldNodeKind::BinaryOp(BinOp::Add),
            vec![
                arg(SqlLiteralValue::String("7".to_string()), DataType::Int32),
                arg(SqlLiteralValue::Int(1), DataType::Int32),
            ],
            DataType::Int32,
        );
        assert_eq!(mismatched_kind, Ok(None));

        // An INT literal that does not fit its declared width is equally
        // unmappable.
        let out_of_range = fold(
            FoldNodeKind::BinaryOp(BinOp::Add),
            vec![
                arg(SqlLiteralValue::Int(i64::MAX), DataType::Int32),
                arg(SqlLiteralValue::Int(1), DataType::Int32),
            ],
            DataType::Int32,
        );
        assert_eq!(out_of_range, Ok(None));

        // More fraction digits than the column scale would be truncated, so
        // the decimal is declined rather than folded to a different value.
        let lossy_decimal = fold(
            FoldNodeKind::BinaryOp(BinOp::Add),
            vec![
                arg(
                    SqlLiteralValue::Decimal("1.239".to_string()),
                    DataType::Decimal128(10, 2),
                ),
                arg(
                    SqlLiteralValue::Decimal("1.00".to_string()),
                    DataType::Decimal128(10, 2),
                ),
            ],
            DataType::Decimal128(10, 2),
        );
        assert_eq!(lossy_decimal, Ok(None));
    }

    #[test]
    fn division_by_zero_folds_to_null() {
        // Observed behavior: `arithmetic::eval_div` nullifies zero divisors
        // (matching StarRocks), so the kernel yields NULL rather than `Err`.
        // The adapter therefore folds `1 / 0` to a typed NULL literal instead
        // of surfacing an evaluation failure.
        let folded = fold(
            FoldNodeKind::BinaryOp(BinOp::Div),
            vec![
                arg(SqlLiteralValue::Int(1), DataType::Int32),
                arg(SqlLiteralValue::Int(0), DataType::Int32),
            ],
            DataType::Float64,
        );
        assert_eq!(folded, Ok(Some(SqlLiteralValue::Null)));
    }
}

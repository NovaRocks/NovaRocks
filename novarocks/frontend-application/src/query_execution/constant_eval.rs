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
    Array, ArrayRef, Decimal128Array, Decimal256Array, RecordBatch, RecordBatchOptions,
};
use arrow::datatypes::{DataType, Decimal128Type, Decimal256Type, DecimalType};
use novarocks_execution::exec::chunk::{Chunk, ChunkSchema};
use novarocks_execution::exec::expr::function::lookup_function;
use novarocks_execution::exec::expr::{ExprArena, ExprId, ExprNode};
use novarocks_functions::{ConstantPool, ConstantValue, validate_function_value_type_observed};
use novarocks_sql::compiler::{
    BinOp, FoldNodeKind, FoldRequest, SqlConstantEvaluationError, SqlConstantEvaluator, UnOp,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, FunctionValueType, PureCompileControl, ValueKind,
    ValueLogicalType, ValueTypeRef,
};
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
    fn eval_scalar(
        &self,
        request: &FoldRequest,
        control: &dyn PureCompileControl,
    ) -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let evaluated = (|| -> Result<Option<ConstantValue>, SqlConstantEvaluationError> {
            // Validate all frozen source facts even when a supported-domain
            // check will decline an earlier argument. No payload enum is read.
            let mut supported = legacy_value_type_supported(&request.result_type, &mut work)?;
            for arg in &request.args {
                supported &= legacy_value_type_supported(&arg.value_type, &mut work)?;
                if !arg
                    .value_type
                    .exactly_equals_observed(arg.value.value_type(), || {
                        work.step().map_err(SqlConstantEvaluationError::from)
                    })?
                {
                    return Err(novarocks_functions::ConstantError::Invalid(
                        "fold constant source differs from its exact frozen value type",
                    )
                    .into());
                }
                work.step()?;
            }
            if !supported {
                return Ok(None);
            }
            work.flush()?;
            let mut arena = ExprArena::default();
            let mut arg_ids: Vec<ExprId> = Vec::with_capacity(request.args.len());
            for arg in &request.args {
                arg_ids.push(arena.push_typed(
                    ExprNode::Constant(arg.value.clone()),
                    arg.value_type.data_type().clone(),
                ));
                work.step()?;
            }
            work.step()?;
            let Some(root_node) = root_node_for(&request.kind, &arg_ids) else {
                return Ok(None);
            };
            let root = arena.push_typed(root_node, request.result_type.data_type().clone());
            if let Some(source) = request.regexp_count_pattern_source {
                arena.set_regexp_count_pattern_source(root, source)?;
            }
            work.flush()?;
            let chunk = single_row_chunk()?;
            work.flush()?;
            // This staged adapter reuses the legacy runtime. Its opaque
            // internal loops/allocations are not a pure-kernel or MEM proof.
            let output = arena.eval(root, &chunk)?;
            work.flush()?;
            if output.len() != 1 {
                return Err(format!(
                    "constant folding produced {} rows, expected exactly 1",
                    output.len()
                )
                .into());
            }
            if !novarocks_type_contract::arrow_data_types_exact_observed::<
                SqlConstantEvaluationError,
            >(output.data_type(), request.result_type.data_type(), || {
                work.step()?;
                Ok(())
            })? {
                return Err("constant folding output differs from its frozen carrier"
                    .to_owned()
                    .into());
            }
            if output.is_null(0) && !request.result_type.nullable() {
                return Ok(None);
            }
            // Preserve the old optional fold refusal for decimal results that
            // the runtime emits outside their declared precision. CV creation
            // must not reinterpret that result or truncate a display string.
            work.flush()?;
            let utf8_fits = utf8_output_fits(&output, &mut work)?;
            if !utf8_fits {
                return Ok(None);
            }
            let decimal_fits = decimal_output_fits(&output)?;
            work.flush()?;
            if !decimal_fits {
                return Ok(None);
            }
            let field = Arc::new(arrow::datatypes::Field::new(
                "literal",
                request.result_type.data_type().clone(),
                request.result_type.nullable(),
            ));
            let data = output.to_data();
            work.flush()?;
            let pool = ConstantPool::try_new(
                field,
                request.result_type.clone(),
                data,
                request.constant_policy,
                CompilePhase::FunctionSpecialization,
                control,
            )?;
            work.step()?;
            Ok(Some(pool.value(0)?))
        })();
        if matches!(
            &evaluated,
            Err(SqlConstantEvaluationError::Control(_))
                | Err(SqlConstantEvaluationError::Constant(
                    novarocks_functions::ConstantError::Limit(_)
                ))
        ) {
            return evaluated;
        }
        work.finish()?;
        evaluated
    }
}

// Inspect bytes before any StringArray::value call can form an invalid &str.
// Only malformed UTF8 preserves the legacy optional fold decline; invalid
// layout remains an ordinary evaluation error, not an Arrow-error fallback.
fn utf8_output_fits(
    output: &ArrayRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlConstantEvaluationError> {
    if output.is_null(0) {
        return Ok(true);
    }
    macro_rules! inspect {
        ($array:ty) => {{
            let array = output.as_any().downcast_ref::<$array>().ok_or_else(|| {
                SqlConstantEvaluationError::Evaluation("invalid UTF8 output class".into())
            })?;
            let offsets = array.value_offsets();
            let start = usize::try_from(offsets[0]).map_err(|_| {
                SqlConstantEvaluationError::Evaluation("invalid UTF8 output offset".into())
            })?;
            let end = usize::try_from(offsets[1]).map_err(|_| {
                SqlConstantEvaluationError::Evaluation("invalid UTF8 output offset".into())
            })?;
            let bytes = array.value_data().get(start..end).ok_or_else(|| {
                SqlConstantEvaluationError::Evaluation("invalid UTF8 output range".into())
            })?;
            // Account real bounded visits before the opaque UTF8 validator.
            for byte in bytes {
                std::hint::black_box(byte);
                work.step()?;
            }
            work.flush()?;
            let valid = std::str::from_utf8(bytes).is_ok();
            work.flush()?;
            Ok(valid)
        }};
    }
    match output.data_type() {
        DataType::Utf8 => inspect!(arrow::array::StringArray),
        DataType::LargeUtf8 => inspect!(arrow::array::LargeStringArray),
        _ => Ok(true),
    }
}

fn decimal_output_fits(output: &ArrayRef) -> Result<bool, String> {
    if output.is_null(0) {
        return Ok(true);
    }
    match output.data_type() {
        DataType::Decimal128(precision, _) => {
            let array = output
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    "constant folding has an invalid Decimal128 result class".to_owned()
                })?;
            Ok(Decimal128Type::is_valid_decimal_precision(
                array.value(0),
                *precision,
            ))
        }
        DataType::Decimal256(precision, _) => {
            let array = output
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    "constant folding has an invalid Decimal256 result class".to_owned()
                })?;
            Ok(Decimal256Type::is_valid_decimal_precision(
                array.value(0),
                *precision,
            ))
        }
        _ => Ok(true),
    }
}

/// The legacy arena carries Arrow types only. Its one exact non-Physical
/// scalar representation is the existing LARGEINT literal/kernel protocol;
/// all other semantic domains need the full-value evaluator migration.
fn legacy_value_type_supported(
    value_type: &FunctionValueType,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, SqlConstantEvaluationError> {
    work.step()?;
    value_type
        .logical_type()
        .validate_carrier(value_type.data_type())?;
    // The function owner supplies the actual type/metadata resource limits.
    // Its borrowed walk observes entries, not opaque cloning or kernel work.
    validate_function_value_type_observed(value_type, work)?;
    fn supported(
        value: &ValueTypeRef,
        root: bool,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<bool, SqlConstantEvaluationError> {
        work.step()?;
        let mut allowed = match value.kind() {
            ValueKind::Nominal(ValueLogicalType::LargeInt) => root,
            ValueKind::Nominal(_) => false,
            ValueKind::Scalar(DataType::FixedSizeBinary(_)) => false,
            _ => true,
        };
        for ordinal in 0..value.child_count() {
            work.step()?;
            let child = value
                .child_type_at(ordinal)
                .expect("immutable value node exposes every declared child");
            allowed &= supported(child, false, work)?;
        }
        Ok(allowed)
    }
    supported(value_type.value_type(), true, work)
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
        FoldNodeKind::BinaryOp(op, decimal_overflow_policy) => {
            let [lhs, rhs] = args else {
                return None;
            };
            let (lhs, rhs) = (*lhs, *rhs);
            Some(match op {
                BinOp::Add => ExprNode::Add(lhs, rhs, *decimal_overflow_policy),
                BinOp::Sub => ExprNode::Sub(lhs, rhs, *decimal_overflow_policy),
                BinOp::Mul => ExprNode::Mul(lhs, rhs, *decimal_overflow_policy),
                BinOp::Div => ExprNode::Div(lhs, rhs, *decimal_overflow_policy),
                BinOp::Mod => ExprNode::Mod(lhs, rhs, *decimal_overflow_policy),
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
        FoldNodeKind::Cast(decimal_overflow_policy) => {
            let [child] = args else {
                return None;
            };
            // The cast target is the node's own data type, which the caller
            // attaches through `push_typed(.., out_type)`.
            Some(ExprNode::Cast(*child, *decimal_overflow_policy))
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

#[cfg(test)]
#[path = "constant_eval/cv_tests.rs"]
mod cv_tests;

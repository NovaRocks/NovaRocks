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
use crate::compiler::{
    FoldArg, FoldRequest, FrozenFoldIntrinsic, FrozenFoldNode, SqlConstantEvaluationError,
    SqlConstantEvaluator,
};
use crate::optimizer::scalar::HashableLiteral;
use novarocks_functions::ConstantValue;

pub(super) struct FoldServices<'a> {
    pub(super) calculator: &'a dyn SqlConstantEvaluator,
    pub(super) functions: &'a dyn crate::compiler::SqlFunctionCatalog,
    pub(super) allow_throw_exception: bool,
}

/// Try to replace one node (whose children are already folded) with a literal.
///
/// Returns `Ok(None)` when a safety gate fails or the legacy evaluator declines
/// or reports an evaluation error. Caller control failures remain typed errors.
fn evaluate_legacy(
    arena: &ScalarArena,
    id: ScalarId,
    evaluator: &FoldServices<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<Option<ConstantValue>, SqlCompileError> {
    work.step()?;
    let node = arena.node(id).clone();
    let out_type = arena.data_type(id).clone();

    // Gate 2: a volatile or DISTINCT function is never a constant.
    // Gate 3: an environment-sensitive function must stay on the backend.
    match &node {
        ScalarNode::BinaryOp { .. } | ScalarNode::UnaryOp { .. } | ScalarNode::Cast { .. } => {}
        ScalarNode::FunctionCall {
            name,
            distinct,
            volatility,
            ..
        } => {
            if *distinct || *volatility != FunctionVolatility::Immutable {
                return Ok(None);
            }
            if is_environment_sensitive_function(name) || is_byte_carrying_string_function(name) {
                return Ok(None);
            }
        }
        // Every other node shape stays unfolded under the existing policy; its children were
        // already folded above.
        _ => return Ok(None),
    };

    // Gate 4: retain the existing literal-type folding coverage boundary.
    if !is_wire_encodable_literal_type(&out_type) {
        return Ok(None);
    }

    let children: Vec<ScalarId> = match &node {
        ScalarNode::BinaryOp { left, right, .. } => vec![*left, *right],
        ScalarNode::UnaryOp { child, .. } | ScalarNode::Cast { child, .. } => vec![*child],
        ScalarNode::FunctionCall { args, .. } => args.clone(),
        _ => return Ok(None),
    };

    // Gate 1: every child must already be a literal.
    let mut args = Vec::with_capacity(children.len());
    for child in children {
        work.step()?;
        let value = match arena.node(child) {
            ScalarNode::Constant(value) => value.clone(),
            ScalarNode::Literal(HashableLiteral(value)) => {
                work.flush()?;
                let value = crate::constant::admit_syntax_constant(
                    value,
                    arena.value_type(child),
                    arena.constant_policy(),
                    work.control(),
                )?;
                work.flush()?;
                value
            }
            _ => return Ok(None),
        };
        args.push(FoldArg {
            value,
            value_type: arena.value_type(child).clone(),
        });
    }

    // Preparation shares the exact call/transition authors with package lowering.
    // Its failure stays fatal; only evaluator row errors use fail-open below.
    use crate::compiler::selected_scalar_call as selected;
    let intrinsic = |kind| FrozenFoldNode::Intrinsic(kind);
    let conversion =
        |arg: &FoldArg, target: &novarocks_type_contract::FunctionValueType, policy| {
            selected::freeze_conversion(
                evaluator.functions,
                arg,
                target,
                policy,
                evaluator.allow_throw_exception,
                arena.constant_policy(),
                work.control(),
            )
        };
    let kind = match &node {
        ScalarNode::FunctionCall { binding, .. } => {
            let Some(call) = selected::freeze_fold_call(
                evaluator.functions,
                binding,
                &args,
                arena.constant_policy(),
                true,
                work.control(),
            )?
            else {
                return Ok(None);
            };
            FrozenFoldNode::Call(call)
        }
        ScalarNode::Cast {
            decimal_overflow_policy,
            ..
        } => intrinsic(FrozenFoldIntrinsic::Cast {
            plan: conversion(&args[0], arena.value_type(id), *decimal_overflow_policy)?,
        }),
        ScalarNode::UnaryOp { op, .. } => intrinsic(match op {
            crate::common::UnOp::Not => FrozenFoldIntrinsic::Not,
            crate::common::UnOp::Negate => FrozenFoldIntrinsic::Negate,
            crate::common::UnOp::BitwiseNot => FrozenFoldIntrinsic::BitNot,
        }),
        ScalarNode::BinaryOp {
            op,
            decimal_overflow_policy,
            ..
        } => {
            if let Some(operator) = selected::arithmetic_operator(*op) {
                intrinsic(FrozenFoldIntrinsic::Arithmetic {
                    operator,
                    decimal_overflow_policy: *decimal_overflow_policy,
                    allow_throw_exception: evaluator.allow_throw_exception,
                })
            } else if matches!(op, crate::common::BinOp::And | crate::common::BinOp::Or) {
                intrinsic(if *op == crate::common::BinOp::And {
                    FrozenFoldIntrinsic::Conjunction
                } else {
                    FrozenFoldIntrinsic::Disjunction
                })
            } else {
                let compared = selected::comparison_type(&args[0].value_type, &args[1].value_type)
                    .map_err(|cause| {
                        SqlCompileError::CompilationSource(
                            novarocks_failure_contract::SharedFailureCause::new(cause),
                        )
                    })?;
                let left = conversion(
                    &args[0],
                    &selected::comparison_target(&compared, &args[0].value_type),
                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                )?;
                let right = conversion(
                    &args[1],
                    &selected::comparison_target(&compared, &args[1].value_type),
                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                )?;
                intrinsic(if let Some(operator) = selected::comparison_operator(*op) {
                    FrozenFoldIntrinsic::Comparison {
                        operator,
                        left,
                        right,
                    }
                } else {
                    FrozenFoldIntrinsic::NullSafeEq { left, right }
                })
            }
        }
        _ => unreachable!("foldability gates checked the node"),
    };
    let request = FoldRequest {
        kind,
        args,
        result_type: arena.value_type(id).clone(),
        constant_policy: arena.constant_policy(),
    };

    work.step()?;
    work.flush()?;
    let source = crate::compiler::SqlFoldDependencySource::from_node(&request.kind);
    let input = crate::compiler::SqlFoldDependencyInput {
        source,
        request: &request,
    };
    let evaluated = match crate::compiler::evaluate_fold_dependency_observed(
        evaluator.calculator,
        input,
        work.control(),
    ) {
        Ok(value) => Ok(value),
        Err(SqlConstantEvaluationError::Source(source)) => {
            use novarocks_failure_contract::{FailureClass, FailureProjection};
            if source.failure_fact().class() == FailureClass::Data {
                // Optional evaluation of data may decline folding. The
                // original expression remains and owns runtime evaluation.
                Err(source.detail().to_owned())
            } else {
                // Resource/control/invariant failures are not an optional
                // data result, and must retain their actual projected source.
                return Err(SqlCompileError::CompilationSource(source));
            }
        }
        Err(SqlConstantEvaluationError::Outer(source)) => {
            return Err(SqlCompileError::CompilationSource(source));
        }
        Err(SqlConstantEvaluationError::Evaluation(error)) => Err(error),
        Err(SqlConstantEvaluationError::Control(error)) => return Err(error.into()),
        Err(SqlConstantEvaluationError::Constant(error)) => return Err(error.into()),
    };
    work.step()?;
    Ok(evaluated.ok().flatten())
}

pub(super) fn compare(
    arena: &ScalarArena,
    input: ScalarId,
    ctx: &RewriteContext<'_>,
    current: &Result<Option<ConstantValue>, SqlCompileError>,
) {
    use novarocks_failpoint::observation_marker::{
        EXL03_FOLD_PARITY_ENABLED, EXL03_FOLD_PARITY_MISMATCH,
    };
    use std::sync::Once;

    let Some(calculator) = ctx
        .constant_evaluator()
        .filter(|evaluator| evaluator.is_legacy_fold_reference())
    else {
        return;
    };
    if matches!(
        current,
        Err(SqlCompileError::Cancelled
            | SqlCompileError::DeadlineExceeded
            | SqlCompileError::ResourceExhausted)
    ) {
        return;
    }
    static ENABLED: Once = Once::new();
    ENABLED.call_once(|| tracing::warn!("{} probe=fold", EXL03_FOLD_PARITY_ENABLED));
    let control = crate::compiler::SqlCompileControl::unbounded();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate)
        .expect("unbounded probe control");
    let previous = evaluate_legacy(
        arena,
        input,
        &FoldServices {
            calculator,
            functions: ctx.function_catalog(),
            allow_throw_exception: false,
        },
        &mut work,
    );
    let same = match (&previous, current) {
        (Ok(None), Ok(None)) => true,
        (Ok(Some(previous)), Ok(Some(current))) => previous
            .equals_observed(current, CompilePhase::Validate, &control)
            .expect("admitted constant values can be compared"),
        (Err(_), Err(_)) => true,
        _ => false,
    };
    if !same {
        tracing::warn!(
            "{} field=outcome old={:?} new={:?} input={:?}",
            EXL03_FOLD_PARITY_MISMATCH,
            previous,
            current,
            arena.node(input),
        );
        if std::env::var_os("NOVAROCKS_EXL03_PARITY_PANIC").is_some_and(|value| value == "1") {
            panic!("EXL-03 fold parity mismatch for {:?}", arena.node(input));
        }
    }
}

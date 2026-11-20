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

use novarocks_functions::FunctionResultType;
use novarocks_type_contract::OperatorFunction;

use crate::analysis::{ExprKind, TypedExpr};
use crate::binding::SqlFunctionBinding;
use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

pub(crate) fn typed_call(binding: SqlFunctionBinding, operand: TypedExpr) -> TypedExpr {
    let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
        unreachable!("negation has a scalar result");
    };
    let value_type = result.clone();
    TypedExpr {
        kind: ExprKind::FunctionCall {
            name: OperatorFunction::Negation.catalog_name().to_owned(),
            volatility: binding.semantics.volatility,
            args: vec![operand].into(),
            distinct: false,
            binding,
        },
        value_type,
    }
}

pub(crate) fn typed_operand(expression: &TypedExpr) -> Option<&TypedExpr> {
    let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
        return None;
    };
    let [operand] = &args[..] else {
        return None;
    };
    is_negation(&binding.function_id).then_some(operand)
}

pub(crate) fn scalar_operand(arena: &ScalarArena, expression: ScalarId) -> Option<ScalarId> {
    let ScalarNode::FunctionCall { binding, args, .. } = arena.node(expression) else {
        return None;
    };
    let [operand] = args.as_slice() else {
        return None;
    };
    is_negation(&binding.function_id).then_some(*operand)
}

pub(crate) fn is_negation(function: &novarocks_type_contract::FunctionId) -> bool {
    use novarocks_type_contract::OperatorFunction;

    OperatorFunction::from_function_id(function) == Some(OperatorFunction::Negation)
}

pub(crate) fn typed_operand_has_lower_precedence(operand: &crate::analysis::TypedExpr) -> bool {
    use crate::analysis::ExprKind;

    matches!(
        operand.kind,
        ExprKind::BinaryOp { .. }
            | ExprKind::UnaryOp { .. }
            | ExprKind::IsNull { .. }
            | ExprKind::IsTruthValue { .. }
            | ExprKind::InList { .. }
            | ExprKind::Between { .. }
            | ExprKind::Like { .. }
            | ExprKind::LambdaFunction { .. }
            | ExprKind::Lambda { .. }
    )
}

pub(crate) fn negation_delimiters(
    needs_parentheses: bool,
    child_start: &str,
) -> (&'static str, &'static str) {
    // Numeric spellings and a second minus change how the lexer reads prefix '-'.
    let lexical_boundary = matches!(
        child_start.as_bytes().first(),
        Some(b'-' | b'+' | b'.' | b'0'..=b'9')
    );
    if needs_parentheses || lexical_boundary {
        ("-(", ")")
    } else {
        ("-", "")
    }
}

pub(crate) fn render_negation(child: &str, needs_parentheses: bool) -> String {
    let (prefix, suffix) = negation_delimiters(needs_parentheses, child);
    format!("{prefix}{child}{suffix}")
}

/// Inspect the first emitted piece without materializing the child outside the
/// caller's rendering budget. The child keeps its original writer and errors.
pub(crate) fn write_negation(
    out: &mut dyn std::fmt::Write,
    needs_parentheses: bool,
    child: impl FnOnce(&mut dyn std::fmt::Write) -> std::fmt::Result,
) -> std::fmt::Result {
    struct Operand<'a> {
        out: &'a mut dyn std::fmt::Write,
        needs_parentheses: bool,
        suffix: Option<&'static str>,
    }

    impl std::fmt::Write for Operand<'_> {
        fn write_str(&mut self, piece: &str) -> std::fmt::Result {
            if piece.is_empty() {
                return Ok(());
            }
            if self.suffix.is_none() {
                let (prefix, suffix) = negation_delimiters(self.needs_parentheses, piece);
                self.out.write_str(prefix)?;
                self.suffix = Some(suffix);
            }
            self.out.write_str(piece)
        }
    }

    let mut operand = Operand {
        out,
        needs_parentheses,
        suffix: None,
    };
    child(&mut operand)?;
    if let Some(suffix) = operand.suffix {
        operand.out.write_str(suffix)
    } else {
        let (prefix, suffix) = negation_delimiters(needs_parentheses, "");
        operand.out.write_str(prefix)?;
        operand.out.write_str(suffix)
    }
}

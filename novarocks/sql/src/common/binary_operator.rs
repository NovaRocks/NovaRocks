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

use arrow::datatypes::DataType;
use novarocks_functions::FunctionResultType;
use novarocks_type_contract::{OperatorFunction, ValueKind, ValueLogicalType, ValueTypeRef};

use crate::analysis::{ExprKind, TypedExpr};
use crate::binding::SqlFunctionBinding;
use crate::common::BinOp;
use crate::optimizer::scalar::{ScalarArena, ScalarId, ScalarNode};

pub(crate) fn comparison_op(operator: OperatorFunction) -> BinOp {
    match operator {
        OperatorFunction::Equal => BinOp::Eq,
        OperatorFunction::NotEqual => BinOp::Ne,
        OperatorFunction::LessThan => BinOp::Lt,
        OperatorFunction::LessThanOrEqual => BinOp::Le,
        OperatorFunction::GreaterThan => BinOp::Gt,
        OperatorFunction::GreaterThanOrEqual => BinOp::Ge,
        OperatorFunction::Identical | OperatorFunction::Distinct => BinOp::EqForNull,
        _ => unreachable!("comparison syntax selected a comparison"),
    }
}

pub(crate) fn requires_legacy_wide(value: &ValueTypeRef) -> bool {
    match value.kind() {
        ValueKind::Nominal(ValueLogicalType::LargeInt) => true,
        ValueKind::Scalar(DataType::Decimal256(precision, _)) => *precision > 38,
        ValueKind::List(item) | ValueKind::FixedSizeList { item, .. } => requires_legacy_wide(item),
        ValueKind::Struct(fields) => fields
            .iter()
            .any(|field| requires_legacy_wide(field.value_type())),
        ValueKind::Map { key, value } => requires_legacy_wide(key) || requires_legacy_wide(value),
        ValueKind::Union(fields) => fields
            .iter()
            .any(|field| requires_legacy_wide(field.field.value_type())),
        _ => false,
    }
}

/// Composite predicates retain their exact element comparison recipe.
pub(crate) fn uses_structural_comparison(value: &ValueTypeRef) -> bool {
    matches!(
        value.kind(),
        ValueKind::List(_)
            | ValueKind::FixedSizeList { .. }
            | ValueKind::Struct(_)
            | ValueKind::Map { .. }
    )
}

pub(crate) fn typed_call(
    operator: OperatorFunction,
    binding: SqlFunctionBinding,
    operands: Vec<TypedExpr>,
) -> TypedExpr {
    let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
        unreachable!("binary operator has a scalar result");
    };
    TypedExpr {
        value_type: result.clone(),
        kind: ExprKind::FunctionCall {
            name: operator.catalog_name().to_owned(),
            volatility: binding.semantics.volatility,
            args: operands.into(),
            distinct: false,
            binding,
        },
    }
}

pub(crate) fn typed_operands(
    expression: &TypedExpr,
) -> Option<(OperatorFunction, [&TypedExpr; 2])> {
    let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
        return None;
    };
    let [left, right] = &args[..] else {
        return None;
    };
    Some((
        OperatorFunction::from_function_id(&binding.function_id)
            .filter(|operator| operator.is_infix())?,
        [left, right],
    ))
}

pub(crate) fn scalar_operands(
    arena: &ScalarArena,
    expression: ScalarId,
) -> Option<(OperatorFunction, [ScalarId; 2])> {
    let ScalarNode::FunctionCall { binding, args, .. } = arena.node(expression) else {
        return None;
    };
    let [left, right] = args.as_slice() else {
        return None;
    };
    Some((
        OperatorFunction::from_function_id(&binding.function_id)
            .filter(|operator| operator.is_infix())?,
        [*left, *right],
    ))
}

pub(crate) fn typed_in_list(expression: &TypedExpr) -> Option<(&TypedExpr, &[TypedExpr])> {
    let ExprKind::FunctionCall { binding, args, .. } = &expression.kind else {
        return None;
    };
    if OperatorFunction::from_function_id(&binding.function_id) != Some(OperatorFunction::InList) {
        return None;
    }
    args.split_first()
        .filter(|(_, candidates)| !candidates.is_empty())
}

/// Read an expanded inclusive range only when both comparisons use the same
/// explicit probe. A local value under Let is outside this proof.
pub(crate) fn scalar_inclusive_range(
    arena: &ScalarArena,
    expression: ScalarId,
) -> Option<(ScalarId, ScalarId, ScalarId)> {
    let ScalarNode::BinaryOp {
        left,
        op: BinOp::And | BinOp::ExpandedConjunction | BinOp::ExpandedNegatedConjunction,
        right,
        ..
    } = arena.node(expression)
    else {
        return None;
    };
    let (BinOp::Le, [lower, probe]) = scalar_comparison(arena, *left)? else {
        return None;
    };
    let (BinOp::Le, [other_probe, upper]) = scalar_comparison(arena, *right)? else {
        return None;
    };
    (probe == other_probe).then_some((probe, lower, upper))
}

pub(crate) fn scalar_in_list(
    arena: &ScalarArena,
    expression: ScalarId,
) -> Option<(ScalarId, &[ScalarId])> {
    let ScalarNode::FunctionCall { binding, args, .. } = arena.node(expression) else {
        return None;
    };
    if OperatorFunction::from_function_id(&binding.function_id) != Some(OperatorFunction::InList) {
        return None;
    }
    args.split_first()
        .filter(|(_, candidates)| !candidates.is_empty())
        .map(|(probe, candidates)| (*probe, candidates))
}

pub(crate) fn typed_comparison(expression: &TypedExpr) -> Option<(BinOp, [&TypedExpr; 2])> {
    if let Some((operator, operands)) = typed_operands(expression) {
        return comparison_kind(operator).map(|op| (op, operands));
    }
    let ExprKind::BinaryOp {
        left, op, right, ..
    } = &expression.kind
    else {
        return None;
    };
    is_comparison(*op).then_some((*op, [left, right]))
}

pub(crate) fn scalar_comparison(
    arena: &ScalarArena,
    expression: ScalarId,
) -> Option<(BinOp, [ScalarId; 2])> {
    if let Some((operator, operands)) = scalar_operands(arena, expression) {
        return comparison_kind(operator).map(|op| (op, operands));
    }
    let ScalarNode::BinaryOp {
        left, op, right, ..
    } = arena.node(expression)
    else {
        return None;
    };
    is_comparison(*op).then_some((*op, [*left, *right]))
}

fn comparison_kind(operator: OperatorFunction) -> Option<BinOp> {
    (operator.comparison().is_some() || operator == OperatorFunction::Identical)
        .then(|| comparison_op(operator))
}

fn is_comparison(operator: BinOp) -> bool {
    matches!(
        operator,
        BinOp::Eq | BinOp::EqForNull | BinOp::Ne | BinOp::Lt | BinOp::Le | BinOp::Gt | BinOp::Ge
    )
}

pub(crate) fn permits_equality_substitution(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

#[cfg(test)]
pub(crate) fn test_predicate(operator: OperatorFunction, operands: Vec<TypedExpr>) -> TypedExpr {
    use novarocks_functions::{FunctionArgument, FunctionBindingRequest, FunctionCandidate};

    let control = crate::optimizer::test_optimizer_control();
    let policy = crate::constant::test_constant_policy();
    let catalog = crate::functions::builtin_sql_function_catalog();
    let overflow = novarocks_type_contract::DecimalOverflowPolicy::OutputNull;
    let binding = if operator == OperatorFunction::InList {
        let (probe, candidates) = operands.split_first().expect("test IN probe");
        let members = candidates
            .iter()
            .map(|candidate| {
                let FunctionArgument::Value {
                    value_type,
                    constant,
                } = crate::analysis::function_argument(candidate, policy, control).unwrap()
                else {
                    panic!("test IN candidate value");
                };
                FunctionCandidate {
                    value_type,
                    constant,
                }
            })
            .collect::<Box<[_]>>();
        let nullable = members.iter().any(|member| member.value_type.nullable());
        let arguments = [
            crate::analysis::function_argument(probe, policy, control).unwrap(),
            FunctionArgument::CandidateGroup {
                element_type: probe.value_type.with_nullable(nullable).unwrap(),
                members,
            },
        ];
        let binding = catalog
            .resolve_scalar_operator_binding(
                operator,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 2,
                    expected_result_type: None,
                },
                control,
            )
            .unwrap();
        SqlFunctionBinding::from_test(binding, overflow)
    } else {
        SqlFunctionBinding::resolve_operator_for_plan(
            crate::binding::PlanBindingAuthority::Test,
            catalog,
            operator,
            &operands,
            overflow,
            policy,
            control,
        )
        .unwrap()
    };
    typed_call(operator, binding, operands)
}

#[cfg(test)]
pub(crate) fn test_inclusive_range(probe: TypedExpr, low: TypedExpr, high: TypedExpr) -> TypedExpr {
    use novarocks_parser::ast::AstBox;
    use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};

    // These optimizer fixtures include synthetic comparisons outside the
    // catalogue so that unrelated comparison-domain safeguards can be tested.
    let comparison = |left: TypedExpr, right: TypedExpr| TypedExpr {
        value_type: FunctionValueType::new(
            DataType::Boolean,
            left.value_type.nullable() || right.value_type.nullable(),
        ),
        kind: ExprKind::BinaryOp {
            left: AstBox::new(left),
            op: BinOp::Le,
            right: AstBox::new(right),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
    };
    let left = comparison(low, probe.clone());
    let right = comparison(probe, high);
    TypedExpr {
        value_type: FunctionValueType::new(
            DataType::Boolean,
            left.value_type.nullable() || right.value_type.nullable(),
        ),
        kind: ExprKind::BinaryOp {
            left: AstBox::new(left),
            op: BinOp::ExpandedConjunction,
            right: AstBox::new(right),
            decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
        },
    }
}

#[cfg(test)]
pub(crate) fn intern_test_predicate(
    arena: &mut ScalarArena,
    operator: OperatorFunction,
    operands: Vec<ScalarId>,
) -> ScalarId {
    let typed = crate::planner::optimizer_bridge::scalar::materialize_exprs(arena, &operands);
    let typed = test_predicate(operator, typed);
    let ExprKind::FunctionCall {
        name,
        binding,
        volatility,
        distinct,
        ..
    } = typed.kind
    else {
        unreachable!("selected test predicate");
    };
    arena.intern(
        ScalarNode::FunctionCall {
            name,
            args: operands,
            binding,
            volatility,
            distinct,
        },
        typed.value_type,
    )
}

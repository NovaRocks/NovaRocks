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

/// Subquery membership keys use grouping equality for floating-point
/// values while retaining SQL NULL propagation.
pub(crate) fn subquery_equality(left: &DataType, right: &DataType) -> OperatorFunction {
    if [left, right]
        .into_iter()
        .any(|ty| matches!(ty, DataType::Float32 | DataType::Float64))
    {
        OperatorFunction::SubqueryFloatEqual
    } else {
        OperatorFunction::Equal
    }
}

pub(crate) fn contains_internal_wide_type(value: &ValueTypeRef) -> bool {
    match value.kind() {
        ValueKind::Nominal(ValueLogicalType::LargeInt) => true,
        ValueKind::Scalar(DataType::Decimal256(precision, _)) => *precision > 38,
        ValueKind::List(item) | ValueKind::FixedSizeList { item, .. } => {
            contains_internal_wide_type(item)
        }
        ValueKind::Struct(fields) => fields
            .iter()
            .any(|field| contains_internal_wide_type(field.value_type())),
        ValueKind::Map { key, value } => {
            contains_internal_wide_type(key) || contains_internal_wide_type(value)
        }
        ValueKind::Union(fields) => fields
            .iter()
            .any(|field| contains_internal_wide_type(field.field.value_type())),
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

pub(crate) fn typed_call(binding: SqlFunctionBinding, operands: Vec<TypedExpr>) -> TypedExpr {
    let FunctionResultType::Scalar(result) = &binding.selected.result_type else {
        unreachable!("binary operator has a scalar result");
    };
    let operator = OperatorFunction::from_function_id(&binding.function_id)
        .expect("operator typing selected an installed operator identity");
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
    let ScalarNode::BooleanConnective {
        left,
        op:
            crate::common::BooleanOp::And
            | crate::common::BooleanOp::ExpandedConjunction
            | crate::common::BooleanOp::ExpandedNegatedConjunction,
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
    let (operator, operands) = typed_operands(expression)?;
    comparison_kind(operator).map(|op| (op, operands))
}

pub(crate) fn scalar_comparison(
    arena: &ScalarArena,
    expression: ScalarId,
) -> Option<(BinOp, [ScalarId; 2])> {
    let (operator, operands) = scalar_operands(arena, expression)?;
    comparison_kind(operator).map(|op| (op, operands))
}

fn comparison_kind(operator: OperatorFunction) -> Option<BinOp> {
    (operator.comparison().is_some() || operator == OperatorFunction::Identical)
        .then(|| comparison_op(operator))
}

pub(crate) fn permits_equality_substitution(data_type: &DataType) -> bool {
    !matches!(
        data_type,
        DataType::Float16 | DataType::Float32 | DataType::Float64
    )
}

#[cfg(test)]
pub(crate) enum TestOperator {
    Boolean(crate::common::BooleanOp),
    Call(OperatorFunction),
}

#[cfg(test)]
impl From<crate::common::BooleanOp> for TestOperator {
    fn from(operator: crate::common::BooleanOp) -> Self {
        Self::Boolean(operator)
    }
}

#[cfg(test)]
impl From<BinOp> for TestOperator {
    fn from(operator: BinOp) -> Self {
        Self::Call(match operator {
            BinOp::Add => OperatorFunction::Add,
            BinOp::Sub => OperatorFunction::Subtract,
            BinOp::Mul => OperatorFunction::Multiply,
            BinOp::Div => OperatorFunction::Divide,
            BinOp::Mod => OperatorFunction::Modulo,
            BinOp::Eq => OperatorFunction::Equal,
            BinOp::Ne => OperatorFunction::NotEqual,
            BinOp::Lt => OperatorFunction::LessThan,
            BinOp::Le => OperatorFunction::LessThanOrEqual,
            BinOp::Gt => OperatorFunction::GreaterThan,
            BinOp::Ge => OperatorFunction::GreaterThanOrEqual,
            BinOp::EqForNull => OperatorFunction::Identical,
        })
    }
}

#[cfg(test)]
pub(crate) fn test_operator(
    op: impl Into<TestOperator>,
    left: TypedExpr,
    right: TypedExpr,
    policy: novarocks_type_contract::DecimalOverflowPolicy,
) -> TypedExpr {
    use novarocks_type_contract::FunctionValueType;

    let operator = match op.into() {
        TestOperator::Call(operator) => operator,
        TestOperator::Boolean(op) => {
            return TypedExpr {
                value_type: FunctionValueType::new(
                    DataType::Boolean,
                    left.value_type.nullable() || right.value_type.nullable(),
                ),
                kind: ExprKind::BooleanConnective {
                    left: novarocks_parser::ast::AstBox::new(left),
                    op,
                    right: novarocks_parser::ast::AstBox::new(right),
                },
            };
        }
    };
    crate::analyzer::type_optimizer_operator(
        crate::functions::builtin_sql_function_catalog(),
        operator,
        [left, right],
        policy,
        crate::constant::test_constant_policy(),
        crate::optimizer::test_optimizer_control(),
    )
    .unwrap()
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
    typed_call(binding, operands)
}

#[cfg(test)]
pub(crate) fn test_inclusive_range(probe: TypedExpr, low: TypedExpr, high: TypedExpr) -> TypedExpr {
    use novarocks_parser::ast::AstBox;
    use novarocks_type_contract::{DecimalOverflowPolicy, FunctionValueType};

    // These optimizer fixtures include synthetic comparisons outside the
    // catalogue so that unrelated comparison-domain safeguards can be tested.
    let comparison = |left: TypedExpr, right: TypedExpr| {
        crate::common::binary_operator::test_operator(
            BinOp::Le,
            left,
            right,
            DecimalOverflowPolicy::OutputNull,
        )
    };
    let left = comparison(low, probe.clone());
    let right = comparison(probe, high);
    TypedExpr {
        value_type: FunctionValueType::new(
            DataType::Boolean,
            left.value_type.nullable() || right.value_type.nullable(),
        ),
        kind: ExprKind::BooleanConnective {
            left: AstBox::new(left),
            op: crate::common::BooleanOp::ExpandedConjunction,
            right: AstBox::new(right),
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

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

/// Preserve the original mixed-domain coercion and comparison when an
/// admitted textual or Boolean operand participates in a numeric predicate.
pub(crate) fn requires_legacy_mixed_comparison(
    left: &novarocks_type_contract::FunctionValueType,
    right: &novarocks_type_contract::FunctionValueType,
) -> bool {
    let text_or_bool = |value: &novarocks_type_contract::FunctionValueType| {
        value.logical_type() == ValueLogicalType::Physical
            && matches!(
                value.data_type(),
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Boolean
            )
    };
    (text_or_bool(left) && novarocks_type_contract::is_numeric_value_type(right))
        || (text_or_bool(right) && novarocks_type_contract::is_numeric_value_type(left))
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
        OperatorFunction::from_function_id(&binding.function_id)?,
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
        OperatorFunction::from_function_id(&binding.function_id)?,
        [*left, *right],
    ))
}

/// Borrow the original operands and operator for optimizer facts. Distinct
/// has no equivalent in the legacy binary vocabulary.
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

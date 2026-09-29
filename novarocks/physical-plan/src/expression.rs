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

use std::collections::{BTreeMap, BTreeSet};

use arrow_schema::DataType;

use novarocks_type_contract::{
    AggregateStateFormatId, FunctionArgumentEvaluation, FunctionFailureBehavior, FunctionId,
    FunctionKind, FunctionOverloadId, FunctionVolatility,
};

use crate::{ExprId, FunctionArgumentType, ValueId, ValueType};

#[derive(Clone, Debug, PartialEq)]
pub struct BoundFunction {
    pub function_id: FunctionId,
    pub overload: FunctionOverloadId,
    pub kind: FunctionKind,
    pub argument_types: Box<[FunctionArgumentType]>,
    pub result_type: ValueType,
    pub volatility: FunctionVolatility,
    pub argument_evaluation: FunctionArgumentEvaluation,
    pub failure_behavior: FunctionFailureBehavior,
    pub intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError,
}

/// Exact binding for a function whose result is a relation rather than a
/// scalar value. Keeping this separate prevents outer-input pass-through
/// columns from being mistaken for function result columns.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundTableFunction {
    pub function_id: FunctionId,
    pub overload: FunctionOverloadId,
    pub argument_types: Box<[FunctionArgumentType]>,
    pub result_types: Box<[ValueType]>,
    pub volatility: FunctionVolatility,
    pub argument_evaluation: FunctionArgumentEvaluation,
    pub failure_behavior: FunctionFailureBehavior,
    pub intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AggregatePhase {
    Single,
    Partial {
        sequence: crate::AggregateSequenceId,
    },
    Intermediate {
        sequence: crate::AggregateSequenceId,
    },
    Final {
        sequence: crate::AggregateSequenceId,
    },
}

impl AggregatePhase {
    pub const fn sequence(self) -> Option<crate::AggregateSequenceId> {
        match self {
            Self::Single => None,
            Self::Partial { sequence }
            | Self::Intermediate { sequence }
            | Self::Final { sequence } => Some(sequence),
        }
    }

    pub const fn consumes_logical_arguments(self) -> bool {
        matches!(self, Self::Single | Self::Partial { .. })
    }

    pub const fn produces_final_result(self) -> bool {
        matches!(self, Self::Single | Self::Final { .. })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AggregateBinding {
    pub function: BoundFunction,
    pub phase: AggregatePhase,
    /// Number of logical SQL arguments at the front of `argument_types`.
    /// Remaining channels are aggregate-owned ORDER BY inputs.
    pub logical_argument_count: u32,
    pub intermediate_type: ValueType,
    pub state_format: AggregateStateFormatId,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum NullOrdering {
    First,
    Last,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SortExpr {
    pub expr: ExprId,
    pub direction: SortDirection,
    pub null_ordering: NullOrdering,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    BitwiseNot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Eq,
    EqForNull,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    BitAnd,
    BitOr,
    BitXor,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LiteralValue {
    Null,
    Boolean(bool),
    Int64(i64),
    UInt64(u64),
    Float64Bits(u64),
    LargeInt(i128),
    Decimal128(i128),
    /// A 256-bit decimal's unscaled value, big-endian two's complement.
    ///
    /// There is no 256-bit integer in this crate's vocabulary, and the bytes
    /// are what every reader of the value wants anyway.
    Decimal256([u8; 32]),
    Utf8(Box<str>),
    Binary(Box<[u8]>),
    Date32(i32),
    Time64(i64),
    Timestamp(i64),
    IntervalMonthDayNano(i128),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Clone, Debug, PartialEq)]
pub enum WindowBound {
    UnboundedPreceding,
    Preceding(ExprId),
    CurrentRow,
    Following(ExprId),
    UnboundedFollowing,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start: WindowBound,
    pub end: WindowBound,
    pub exclusion: WindowFrameExclusion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowFrameExclusion {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ExprKind {
    Value(ValueId),
    LambdaParameter {
        lambda: ExprId,
        ordinal: u32,
    },
    Literal(LiteralValue),
    Unary {
        op: UnaryOperator,
        expr: ExprId,
    },
    Binary {
        left: ExprId,
        op: BinaryOperator,
        right: ExprId,
    },
    /// SQL `AND` over an ordered argument list.
    ///
    /// Boolean connectives are n-ary rather than binary so that the number of
    /// conjuncts a query writes does not become expression depth. A filter
    /// panel that emits 300 predicates is an ordinary query, not a deep one,
    /// and the depth bound exists to stop pathological nesting rather than to
    /// cap how many conditions a user may write.
    ///
    /// `args` is evaluation order. Three-valued `AND` is associative and
    /// commutative, but its arguments are not: they may fail or be volatile,
    /// so a consumer must evaluate left to right and stop at the first `false`.
    /// Any parenthesisation that preserves this order is observably equal,
    /// which is what lets a codec shape the list into a balanced tree.
    Conjunction {
        args: Box<[ExprId]>,
    },
    /// SQL `OR` over an ordered argument list. Mirrors [`ExprKind::Conjunction`],
    /// stopping at the first `true`.
    Disjunction {
        args: Box<[ExprId]>,
    },
    FunctionCall {
        function: BoundFunction,
        args: Box<[ExprId]>,
    },
    Lambda {
        parameter_types: Box<[ValueType]>,
        body: ExprId,
    },
    Cast {
        expr: ExprId,
        target: DataType,
    },
    IsNull {
        expr: ExprId,
        negated: bool,
    },
    InList {
        expr: ExprId,
        list: Box<[ExprId]>,
        negated: bool,
    },
    Between {
        expr: ExprId,
        low: ExprId,
        high: ExprId,
        negated: bool,
    },
    Like {
        expr: ExprId,
        pattern: ExprId,
        negated: bool,
    },
    Case {
        operand: Option<ExprId>,
        when_then: Box<[(ExprId, ExprId)]>,
        else_expr: Option<ExprId>,
    },
    IsTruthValue {
        expr: ExprId,
        value: bool,
        negated: bool,
    },
    WindowCall {
        function: BoundFunction,
        distinct: bool,
        args: Box<[ExprId]>,
        function_order_by: Box<[SortExpr]>,
        frame: Option<WindowFrame>,
        ignore_nulls: bool,
        aggregate_binding: Option<AggregateBinding>,
    },
}

impl ExprKind {
    pub(crate) fn expression_references(&self, output: &mut Vec<ExprId>) {
        match self {
            Self::Value(_) | Self::LambdaParameter { .. } | Self::Literal(_) => {}
            Self::Unary { expr, .. }
            | Self::Cast { expr, .. }
            | Self::IsNull { expr, .. }
            | Self::IsTruthValue { expr, .. } => output.push(*expr),
            Self::Binary { left, right, .. } => output.extend([*left, *right]),
            Self::Conjunction { args } | Self::Disjunction { args } => {
                output.extend(args.iter().copied());
            }
            Self::FunctionCall { args, .. } => {
                output.extend(args.iter().copied());
            }
            Self::Lambda { body, .. } => output.push(*body),
            Self::InList { expr, list, .. } => {
                output.push(*expr);
                output.extend(list.iter().copied());
            }
            Self::Between {
                expr, low, high, ..
            } => output.extend([*expr, *low, *high]),
            Self::Like { expr, pattern, .. } => output.extend([*expr, *pattern]),
            Self::Case {
                operand,
                when_then,
                else_expr,
            } => {
                output.extend(*operand);
                for (when, then) in when_then {
                    output.extend([*when, *then]);
                }
                output.extend(*else_expr);
            }
            Self::WindowCall {
                args,
                function_order_by,
                frame,
                ..
            } => {
                output.extend(args.iter().copied());
                output.extend(function_order_by.iter().map(|item| item.expr));
                if let Some(frame) = frame {
                    for bound in [&frame.start, &frame.end] {
                        if let WindowBound::Preceding(expr) | WindowBound::Following(expr) = bound {
                            output.push(*expr);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExprNode {
    pub id: ExprId,
    /// Physical node whose evaluation scope owns this expression.
    ///
    /// Expressions may be shared by multiple roots of the same node, but they
    /// never cross node scopes. This makes value visibility a local, linear
    /// validation instead of a repeated transitive graph walk.
    pub owner: crate::NodeId,
    /// Innermost enclosing lambda. `None` denotes the physical node scope.
    pub lambda_scope: Option<ExprId>,
    pub ty: ValueType,
    pub kind: ExprKind,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ExprArena {
    nodes: BTreeMap<ExprId, ExprNode>,
}

impl ExprArena {
    pub fn get(&self, id: ExprId) -> Option<&ExprNode> {
        self.nodes.get(&id)
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&ExprId, &ExprNode)> {
        self.nodes.iter()
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub(crate) fn insert(&mut self, node: ExprNode) -> Option<ExprNode> {
        self.nodes.insert(node.id, node)
    }
}

/// Return whether every expression has replica-deterministic semantics.
///
/// Missing expression definitions fail closed. `allow_values` is explicit
/// because some ownership checks require closed expressions, while relational
/// operators may safely read identical input values on every replica.
pub fn expressions_are_replica_deterministic(
    arena: &ExprArena,
    expressions: impl IntoIterator<Item = ExprId>,
    allow_values: bool,
) -> bool {
    let mut pending = expressions.into_iter().collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    while let Some(expression) = pending.pop() {
        if !visited.insert(expression) {
            continue;
        }
        let Some(expression) = arena.get(expression) else {
            return false;
        };
        let immutable = match &expression.kind {
            ExprKind::Value(_) => allow_values,
            ExprKind::FunctionCall { function, .. } | ExprKind::WindowCall { function, .. } => {
                function.volatility == FunctionVolatility::Immutable
            }
            ExprKind::Literal(_)
            | ExprKind::LambdaParameter { .. }
            | ExprKind::Unary { .. }
            | ExprKind::Binary { .. }
            | ExprKind::Conjunction { .. }
            | ExprKind::Disjunction { .. }
            | ExprKind::Lambda { .. }
            | ExprKind::Cast { .. }
            | ExprKind::IsNull { .. }
            | ExprKind::InList { .. }
            | ExprKind::Between { .. }
            | ExprKind::Like { .. }
            | ExprKind::Case { .. }
            | ExprKind::IsTruthValue { .. } => true,
        };
        if !immutable {
            return false;
        }
        expression.kind.expression_references(&mut pending);
    }
    true
}

/// Value a sort key reads directly, when it reads one at all.
///
/// An ordering key names a value, not an arbitrary computation: a sort over a
/// derived expression orders rows but establishes no ordering another operator
/// can rely on.
pub fn expression_value(arena: &ExprArena, expression: ExprId) -> Option<ValueId> {
    match &arena.get(expression)?.kind {
        ExprKind::Value(value) => Some(*value),
        _ => None,
    }
}

/// The value one join key reads, seen through a widening conversion.
///
/// A join states the type it compares, so a key whose two sides met at a
/// wider type reads its column through a conversion. Which value the key
/// comes from is unchanged by that: widening is injective, so a row that
/// matches after the conversion is exactly a row that matches before, and a
/// filter built on the key still prunes the column it was read from.
pub fn join_key_source_value(arena: &ExprArena, expression: ExprId) -> Option<ValueId> {
    let node = arena.get(expression)?;
    match &node.kind {
        ExprKind::Value(value) => Some(*value),
        ExprKind::Cast { expr, target } => {
            let operand = arena.get(*expr)?;
            widening_keeps_key_identity(&operand.ty.data_type, target)
                .then(|| join_key_source_value(arena, *expr))
                .flatten()
        }
        _ => None,
    }
}

/// Whether a conversion maps distinct values to distinct values.
///
/// Only a wider signed integer qualifies, which is what reconciling two
/// integer keys produces. A conversion to a float or a decimal can map two
/// keys onto one, and then a filter built on the converted value would prune
/// a row that the original key would have kept.
fn widening_keeps_key_identity(from: &DataType, to: &DataType) -> bool {
    const fn signed_integer_width(data_type: &DataType) -> Option<u8> {
        match data_type {
            DataType::Int8 => Some(1),
            DataType::Int16 => Some(2),
            DataType::Int32 => Some(4),
            DataType::Int64 => Some(8),
            _ => None,
        }
    }
    if from == to {
        return true;
    }
    match (signed_integer_width(from), signed_integer_width(to)) {
        (Some(from), Some(to)) => to >= from,
        _ => false,
    }
}

/// Ordering a sort establishes, partition keys first.
///
/// Returns `None` when any key is not a direct value reference, which is the
/// case where no ordering can be claimed downstream.
pub fn ordering_keys(
    arena: &ExprArena,
    partition_by: &[SortExpr],
    order_by: &[SortExpr],
) -> Option<Vec<crate::OrderingKey>> {
    partition_by
        .iter()
        .chain(order_by)
        .map(|item| {
            expression_value(arena, item.expr).map(|value| crate::OrderingKey {
                value,
                direction: item.direction,
                null_ordering: item.null_ordering,
            })
        })
        .collect()
}

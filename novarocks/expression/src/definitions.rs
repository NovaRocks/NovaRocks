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

//! Immutable expression facts shared by local-program instances.
use arrow::datatypes::DataType;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    PureCompileControl, ValueTypeRef,
};
use std::{collections::HashSet, fmt, sync::Arc};

/// Matches the native-v1 expanded-expression budget. This bounds the arena
/// independently of the encoded descriptor's byte budget.
pub const MAX_STATIC_EXPRESSIONS: usize = novarocks_type_contract::MAX_CONTROL_DEFINITIONS;
pub const MAX_STATIC_EXPRESSION_DYNAMIC_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_STATIC_EXPRESSION_DEPTH: usize = novarocks_type_contract::MAX_CONTROL_DEPTH;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CompiledExprId(usize);

impl CompiledExprId {
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn index(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug)]
pub enum StaticExprKind {
    /// A checked value retaining its complete field/type and pool ordinal.
    Constant(novarocks_functions::ConstantValue),
    Input,
    /// Cast retaining its exact operation and failure mode.
    PreparedCast {
        operation: novarocks_functions::CastOperation,
        child: CompiledExprId,
        cast_mode: novarocks_type_contract::TrinoCastMode,
    },
    /// Intrinsic arithmetic retaining its exact admitted semantic parameter.
    PreparedArithmetic {
        operator: novarocks_type_contract::ArithmeticOperator,
        left: CompiledExprId,
        right: CompiledExprId,
        decimal_overflow_policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
    },
    Eq(CompiledExprId, CompiledExprId),
    /// Compiled null-safe equality owns a mandatory recipe.
    PreparedNullSafeComparison {
        left: CompiledExprId,
        right: CompiledExprId,
    },
    Ne(CompiledExprId, CompiledExprId),
    Lt(CompiledExprId, CompiledExprId),
    Le(CompiledExprId, CompiledExprId),
    Gt(CompiledExprId, CompiledExprId),
    Ge(CompiledExprId, CompiledExprId),
    /// Ordered physical Boolean occurrences, without introducing binary
    /// intermediate definitions or changing the checked invocation graph.
    NaryAnd {
        args: Vec<CompiledExprId>,
    },
    NaryOr {
        args: Vec<CompiledExprId>,
    },
    /// One checked original Eager2 LIKE source; negative expansion is explicit.
    PreparedLike {
        text: CompiledExprId,
        pattern: CompiledExprId,
        negated: bool,
    },
    /// Three checked definitions, four distinct ordered invocation edges.
    PreparedBetween {
        plan: novarocks_type_contract::NativeBetweenPlan,
        operand: CompiledExprId,
        low: CompiledExprId,
        high: CompiledExprId,
    },
    Not(CompiledExprId),
    IsNull(CompiledExprId),
    IsNotNull(CompiledExprId),
    /// Checked ordered native IN with a mandatory per-use recipe.
    PreparedInList {
        child: CompiledExprId,
        values: Vec<CompiledExprId>,
        is_not_in: bool,
    },
    Case {
        ieee_float_equality: bool,
        has_case_expr: bool,
        has_else_expr: bool,
        children: Vec<CompiledExprId>,
    },
    /// Compiled call shape. Its exact contract, implementation and control
    /// belong to the mandatory per-use ProgramResolvedCalls table.
    BoundCall {
        args: Vec<CompiledExprId>,
    },
}

impl StaticExprKind {
    /// Exact ordinary comparison identity and ordered source references.
    /// Null-safe comparison has its own semantics and is excluded.
    pub const fn ordinary_comparison(
        &self,
    ) -> Option<(
        novarocks_functions::ComparisonOperator,
        CompiledExprId,
        CompiledExprId,
    )> {
        use novarocks_functions::ComparisonOperator;
        let (operator, left, right) = match self {
            Self::Eq(left, right) => (ComparisonOperator::Eq, *left, *right),
            Self::Ne(left, right) => (ComparisonOperator::Ne, *left, *right),
            Self::Lt(left, right) => (ComparisonOperator::Lt, *left, *right),
            Self::Le(left, right) => (ComparisonOperator::Le, *left, *right),
            Self::Gt(left, right) => (ComparisonOperator::Gt, *left, *right),
            Self::Ge(left, right) => (ComparisonOperator::Ge, *left, *right),
            _ => return None,
        };
        Some((operator, left, right))
    }

    pub const fn from_comparison(
        operator: novarocks_functions::ComparisonOperator,
        left: CompiledExprId,
        right: CompiledExprId,
    ) -> Self {
        use novarocks_functions::ComparisonOperator;
        match operator {
            ComparisonOperator::Eq => Self::Eq(left, right),
            ComparisonOperator::Ne => Self::Ne(left, right),
            ComparisonOperator::Lt => Self::Lt(left, right),
            ComparisonOperator::Le => Self::Le(left, right),
            ComparisonOperator::Gt => Self::Gt(left, right),
            ComparisonOperator::Ge => Self::Ge(left, right),
        }
    }

    pub fn decimal_overflow_policy(&self) -> Option<DecimalOverflowPolicy> {
        match self {
            Self::PreparedArithmetic {
                decimal_overflow_policy,
                ..
            } => Some(*decimal_overflow_policy),
            _ => None,
        }
    }
    fn try_for_each_reference<E>(
        &self,
        mut visit: impl FnMut(CompiledExprId) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Constant(_) | Self::Input => {}
            Self::PreparedCast { child, .. }
            | Self::Not(child)
            | Self::IsNull(child)
            | Self::IsNotNull(child) => visit(*child)?,
            Self::PreparedLike {
                text: a,
                pattern: b,
                ..
            }
            | Self::PreparedArithmetic {
                left: a, right: b, ..
            }
            | Self::PreparedNullSafeComparison { left: a, right: b }
            | Self::Eq(a, b)
            | Self::Ne(a, b)
            | Self::Lt(a, b)
            | Self::Le(a, b)
            | Self::Gt(a, b)
            | Self::Ge(a, b) => {
                visit(*a)?;
                visit(*b)?;
            }
            Self::PreparedBetween {
                operand, low, high, ..
            } => {
                visit(*operand)?;
                visit(*low)?;
                visit(*high)?;
            }
            Self::PreparedInList { child, values, .. } => {
                visit(*child)?;
                for id in values {
                    visit(*id)?;
                }
            }
            Self::NaryAnd { args: children }
            | Self::NaryOr { args: children }
            | Self::Case { children, .. }
            | Self::BoundCall { args: children } => {
                for id in children {
                    visit(*id)?;
                }
            }
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct StaticExprNode {
    kind: StaticExprKind,
    value_type: novarocks_functions::FunctionValueType,
}
impl StaticExprNode {
    /// Save the exact semantic and physical author with both original holds.
    pub fn new_value_typed(
        kind: StaticExprKind,
        value_type: novarocks_functions::FunctionValueType,
    ) -> Self {
        Self { kind, value_type }
    }
    pub fn value_type(&self) -> Option<&novarocks_functions::FunctionValueType> {
        Some(&self.value_type)
    }
    pub const fn kind(&self) -> &StaticExprKind {
        &self.kind
    }
    pub fn data_type(&self) -> &DataType {
        self.value_type.data_type()
    }
    pub fn field_schema(&self) -> Option<&ValueTypeRef> {
        Some(self.value_type.value_type())
    }
}
#[derive(Clone, Debug)]
pub struct ImmutableExpressions {
    nodes: Arc<[StaticExprNode]>,
    storage: novarocks_type_contract::ValueTypeStorageLifetime,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticExpressionError {
    RuntimeBoundArena,
    TooManyNodes,
    InvalidReference,
    TooDeep,
    TooManyBytes,
    InvalidMetadataArity,
    ConstantTypeMismatch,
}
impl fmt::Display for StaticExpressionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid static expression graph: {self:?}")
    }
}
impl std::error::Error for StaticExpressionError {}
/// Original expression failures and original request-control interruptions stay
/// distinct at pure compilation. The result owns no control capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExpressionsCompileError {
    Expression(StaticExpressionError),
    Control(CompileControlError),
}
impl From<StaticExpressionError> for ExpressionsCompileError {
    fn from(error: StaticExpressionError) -> Self {
        Self::Expression(error)
    }
}
impl From<CompileControlError> for ExpressionsCompileError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for ExpressionsCompileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Expression(error) => error.fmt(formatter),
            Self::Control(error) => error.fmt(formatter),
        }
    }
}
impl std::error::Error for ExpressionsCompileError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Expression(error) => error,
            Self::Control(error) => error,
        })
    }
}

#[derive(Clone, Copy)]
enum ExpressionObservation {
    Step,
    OpaqueBoundary,
}
impl ImmutableExpressions {
    /// Pure compilation uses the original request control and preserves its first refusal.
    pub fn try_new_for_compile(
        nodes: Vec<StaticExprNode>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ExpressionsCompileError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = Self::try_new_core(nodes, |operation| match operation {
            ExpressionObservation::Step => work.step().map_err(ExpressionsCompileError::Control),
            ExpressionObservation::OpaqueBoundary => {
                work.flush().map_err(ExpressionsCompileError::Control)
            }
        });
        if matches!(&result, Err(ExpressionsCompileError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }
    fn try_new_core<E: From<StaticExpressionError>>(
        nodes: Vec<StaticExprNode>,
        mut observe: impl FnMut(ExpressionObservation) -> Result<(), E>,
    ) -> Result<Self, E> {
        let too_many = nodes.len() > MAX_STATIC_EXPRESSIONS;
        observe(ExpressionObservation::Step)?;
        if too_many {
            return Err(StaticExpressionError::TooManyNodes.into());
        }
        let mut depths = Vec::with_capacity(nodes.len());
        let mut dynamic_bytes = 0usize;
        let mut charged_constant_arrays = HashSet::new();
        for (index, node) in nodes.iter().enumerate() {
            if let StaticExprKind::NaryAnd { args } | StaticExprKind::NaryOr { args } = &node.kind {
                let empty = args.is_empty();
                observe(ExpressionObservation::Step)?;
                if empty {
                    return Err(StaticExpressionError::InvalidMetadataArity.into());
                }
            }
            let mut depth = 1usize;
            node.kind.try_for_each_reference(|child| -> Result<(), E> {
                let valid = child.index() < index;
                if valid {
                    depth = depth.max(depths[child.index()] + 1);
                }
                observe(ExpressionObservation::Step)?;
                if !valid {
                    return Err(StaticExpressionError::InvalidReference.into());
                }
                Ok(())
            })?;
            let too_deep = depth > MAX_STATIC_EXPRESSION_DEPTH;
            observe(ExpressionObservation::Step)?;
            if too_deep {
                return Err(StaticExpressionError::TooDeep.into());
            }
            depths.push(depth);
            observe(ExpressionObservation::Step)?;
            if let StaticExprKind::Constant(value) = &node.kind {
                observe(ExpressionObservation::OpaqueBoundary)?;
                let same = novarocks_type_contract::arrow_data_types_exact(
                    node.data_type(),
                    value.value_type().data_type(),
                );
                observe(ExpressionObservation::OpaqueBoundary)?;
                if !same {
                    return Err(StaticExpressionError::ConstantTypeMismatch.into());
                }
                let array = Arc::as_ptr(value.pool().array()) as *const () as usize;
                let first = charged_constant_arrays.insert(array);
                observe(ExpressionObservation::Step)?;
                if first {
                    let facts = value.pool().resource_facts();
                    let retained = facts
                        .retained_buffer_capacity_bytes
                        .checked_add(facts.metadata_bytes)
                        .and_then(|bytes| usize::try_from(bytes).ok());
                    observe(ExpressionObservation::Step)?;
                    let retained = retained.ok_or(StaticExpressionError::TooManyBytes)?;
                    let next = dynamic_bytes.checked_add(retained);
                    observe(ExpressionObservation::Step)?;
                    dynamic_bytes = next.ok_or(StaticExpressionError::TooManyBytes)?;
                }
            }
            let too_many_bytes = dynamic_bytes > MAX_STATIC_EXPRESSION_DYNAMIC_BYTES;
            observe(ExpressionObservation::Step)?;
            if too_many_bytes {
                return Err(StaticExpressionError::TooManyBytes.into());
            }
        }
        observe(ExpressionObservation::OpaqueBoundary)?;
        let result = Self {
            nodes: Arc::from(nodes),
            storage: novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        };
        observe(ExpressionObservation::OpaqueBoundary)?;
        Ok(result)
    }
    pub(crate) fn with_storage_lifetime(
        mut self,
        storage: novarocks_type_contract::ValueTypeStorageLifetime,
    ) -> Self {
        self.storage = storage;
        self
    }
    pub fn nodes(&self) -> &[StaticExprNode] {
        &self.nodes
    }
    pub fn node(&self, id: CompiledExprId) -> Option<&StaticExprNode> {
        self.nodes.get(id.index())
    }
}

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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use arrow_buffer::i256;
use arrow_schema::DataType;
use novarocks_type_contract::ValueTypeRef;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, DecimalOverflowPolicy,
    PureCompileControl,
};
use novarocks_types::SlotId;

/// Matches the native-v1 expanded-expression budget. This bounds the arena
/// independently of the encoded descriptor's byte budget.
pub const MAX_STATIC_EXPRESSIONS: usize = novarocks_type_contract::MAX_CONTROL_DEFINITIONS;
pub const MAX_STATIC_EXPRESSION_DYNAMIC_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_STATIC_EXPRESSION_DEPTH: usize = novarocks_type_contract::MAX_CONTROL_DEPTH;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ProgramExprId(usize);

impl ProgramExprId {
    pub const fn new(index: usize) -> Self {
        Self(index)
    }

    pub const fn index(self) -> usize {
        self.0
    }
}

/// A literal has no Chunk accounting or provider output lease.
#[derive(Clone, Debug)]
pub enum StaticLiteral {
    Null,
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    LargeInt(i128),
    Float32(f32),
    Float64(f64),
    Bool(bool),
    Utf8(Arc<str>),
    Binary(Arc<[u8]>),
    Date32(i32),
    Decimal128 {
        value: i128,
        precision: u8,
        scale: i8,
    },
    Decimal256 {
        value: i256,
        precision: u8,
        scale: i8,
    },
}

impl StaticLiteral {
    fn dynamic_bytes(&self) -> usize {
        match self {
            Self::Utf8(value) => value.len(),
            Self::Binary(value) => value.len(),
            _ => 0,
        }
    }
}

/// The closed family matches the current Execution function dispatch. Names
/// inside a family are resolved by the kernel ABI before a program is run.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StaticFunctionKind {
    ArrayMap,
    Substring,
    Like,
    Upper,
    Split,
    Year,
    HourFromUnixtime,
    AssertTrue,
    If,
    IfNull,
    Coalesce,
    IsNull,
    IsNotNull,
    Abs,
    Round,
    /// The original default host's exact opaque Binary leaf; no name alias.
    PercentileToBinary,
    /// Existing default same-carrier JSON-to-text conversion after frozen validation.
    JsonToText,
    Date(&'static str),
    Array(&'static str),
    Map(&'static str),
    StructFn(&'static str),
    Math(&'static str),
    String(&'static str),
    Bit(&'static str),
    Matching(&'static str),
    Encryption(&'static str),
    Variant(&'static str),
    Object(&'static str),
    MvState(&'static str),
    NullIf,
    IcebergTransformIdentity,
    IcebergTransformVoid,
    IcebergTransformYear,
    IcebergTransformMonth,
    IcebergTransformDay,
    IcebergTransformHour,
    IcebergTransformBucket,
    IcebergTransformTruncate,
}

#[derive(Clone, Debug)]
pub enum StaticExprKind {
    /// A checked value retaining its complete field/type and pool ordinal.
    Constant(novarocks_functions::ConstantValue),
    /// Construction-path literal pending the production compiler migration.
    Literal(StaticLiteral),
    SlotId(SlotId),
    ArrayExpr {
        elements: Vec<ProgramExprId>,
    },
    StructExpr {
        fields: Vec<ProgramExprId>,
    },
    LambdaFunction {
        body: ProgramExprId,
        arg_slots: Vec<SlotId>,
        common_sub_exprs: Vec<(SlotId, ProgramExprId)>,
        is_nondeterministic: bool,
    },
    DictDecode {
        child: ProgramExprId,
        dict: Arc<HashMap<i32, Vec<u8>>>,
    },
    Cast(ProgramExprId, DecimalOverflowPolicy),
    CastTime(ProgramExprId, DecimalOverflowPolicy),
    CastTimeFromDatetime(ProgramExprId, DecimalOverflowPolicy),
    /// Cast retaining its exact operation and admitted semantic parameter.
    PreparedCast {
        operation: novarocks_functions::CastOperation,
        child: ProgramExprId,
        decimal_overflow_policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
    },
    /// Intrinsic arithmetic retaining its exact admitted semantic parameter.
    /// Legacy construction tags below confer no prepared recipe authority.
    PreparedNativeNegate(ProgramExprId),
    /// Exact admitted native BitwiseNot recipe, never ordinary scalar dispatch.
    PreparedNativeBitNot(ProgramExprId),
    PreparedArithmetic {
        operator: novarocks_type_contract::ArithmeticOperator,
        left: ProgramExprId,
        right: ProgramExprId,
        decimal_overflow_policy: DecimalOverflowPolicy,
        allow_throw_exception: bool,
    },
    Add(ProgramExprId, ProgramExprId, DecimalOverflowPolicy),
    Sub(ProgramExprId, ProgramExprId, DecimalOverflowPolicy),
    Mul(ProgramExprId, ProgramExprId, DecimalOverflowPolicy),
    Div(ProgramExprId, ProgramExprId, DecimalOverflowPolicy),
    Mod(ProgramExprId, ProgramExprId, DecimalOverflowPolicy),
    Eq(ProgramExprId, ProgramExprId),
    EqForNull(ProgramExprId, ProgramExprId),
    /// Compiled null-safe equality owns a mandatory recipe. The legacy
    /// construction tag above cannot carry that authority into ExprArena.
    PreparedNullSafeComparison {
        left: ProgramExprId,
        right: ProgramExprId,
    },
    Ne(ProgramExprId, ProgramExprId),
    Lt(ProgramExprId, ProgramExprId),
    Le(ProgramExprId, ProgramExprId),
    Gt(ProgramExprId, ProgramExprId),
    Ge(ProgramExprId, ProgramExprId),
    And(ProgramExprId, ProgramExprId),
    Or(ProgramExprId, ProgramExprId),
    /// Ordered physical Boolean occurrences, without introducing binary
    /// intermediate definitions or changing the checked invocation graph.
    NaryAnd {
        args: Vec<ProgramExprId>,
    },
    NaryOr {
        args: Vec<ProgramExprId>,
    },
    /// One checked original Eager2 LIKE source; negative expansion is explicit.
    PreparedLike {
        text: ProgramExprId,
        pattern: ProgramExprId,
        negated: bool,
    },
    /// Three checked definitions, four distinct ordered invocation edges.
    PreparedBetween {
        plan: novarocks_type_contract::NativeBetweenPlan,
        operand: ProgramExprId,
        low: ProgramExprId,
        high: ProgramExprId,
    },
    Not(ProgramExprId),
    IsNull(ProgramExprId),
    IsNotNull(ProgramExprId),
    /// Checked ordered native IN with a mandatory per-use recipe. The legacy
    /// In tag below cannot authorize a compiled invocation.
    PreparedInList {
        child: ProgramExprId,
        values: Vec<ProgramExprId>,
        is_not_in: bool,
    },
    In {
        child: ProgramExprId,
        values: Vec<ProgramExprId>,
        is_not_in: bool,
    },
    Case {
        has_case_expr: bool,
        has_else_expr: bool,
        children: Vec<ProgramExprId>,
    },
    /// Compiled call shape. Its exact contract, implementation and control
    /// belong to the mandatory per-use ProgramResolvedCalls table.
    BoundCall {
        args: Vec<ProgramExprId>,
    },
    /// Legacy construction bridge only. A dispatch tag does not resolve a
    /// compiled function or confer effects, demand or implementation authority.
    FunctionCall {
        kind: StaticFunctionKind,
        args: Vec<ProgramExprId>,
    },
    Clone(ProgramExprId),
}

impl StaticExprKind {
    /// Exact ordinary comparison identity and ordered source references.
    /// Null-safe comparison has its own semantics and is excluded.
    pub const fn ordinary_comparison(
        &self,
    ) -> Option<(
        novarocks_functions::ComparisonOperator,
        ProgramExprId,
        ProgramExprId,
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
        left: ProgramExprId,
        right: ProgramExprId,
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
            Self::PreparedCast {
                decimal_overflow_policy,
                ..
            }
            | Self::PreparedArithmetic {
                decimal_overflow_policy,
                ..
            } => Some(*decimal_overflow_policy),
            Self::Cast(_, policy)
            | Self::CastTime(_, policy)
            | Self::CastTimeFromDatetime(_, policy)
            | Self::Add(_, _, policy)
            | Self::Sub(_, _, policy)
            | Self::Mul(_, _, policy)
            | Self::Div(_, _, policy)
            | Self::Mod(_, _, policy) => Some(*policy),
            _ => None,
        }
    }

    fn try_for_each_reference<E>(
        &self,
        mut visit: impl FnMut(ProgramExprId) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Constant(_) | Self::Literal(_) | Self::SlotId(_) => {}
            Self::ArrayExpr { elements } | Self::StructExpr { fields: elements } => {
                for id in elements {
                    visit(*id)?;
                }
            }
            Self::LambdaFunction {
                body,
                common_sub_exprs,
                ..
            } => {
                // Dependency validation retains the original body-first order;
                // invocation/common order belongs to the checked control graph.
                visit(*body)?;
                for (_, id) in common_sub_exprs {
                    visit(*id)?;
                }
            }
            Self::PreparedCast { child, .. }
            | Self::DictDecode { child, .. }
            | Self::Cast(child, _)
            | Self::CastTime(child, _)
            | Self::CastTimeFromDatetime(child, _)
            | Self::PreparedNativeNegate(child)
            | Self::PreparedNativeBitNot(child)
            | Self::Not(child)
            | Self::IsNull(child)
            | Self::IsNotNull(child)
            | Self::Clone(child) => visit(*child)?,
            Self::PreparedLike {
                text: a,
                pattern: b,
                ..
            }
            | Self::PreparedArithmetic {
                left: a, right: b, ..
            }
            | Self::PreparedNullSafeComparison { left: a, right: b }
            | Self::Add(a, b, _)
            | Self::Sub(a, b, _)
            | Self::Mul(a, b, _)
            | Self::Div(a, b, _)
            | Self::Mod(a, b, _)
            | Self::Eq(a, b)
            | Self::EqForNull(a, b)
            | Self::Ne(a, b)
            | Self::Lt(a, b)
            | Self::Le(a, b)
            | Self::Gt(a, b)
            | Self::Ge(a, b)
            | Self::And(a, b)
            | Self::Or(a, b) => {
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
            Self::In { child, values, .. } | Self::PreparedInList { child, values, .. } => {
                visit(*child)?;
                for id in values {
                    visit(*id)?;
                }
            }
            Self::NaryAnd { args: children }
            | Self::NaryOr { args: children }
            | Self::Case { children, .. }
            | Self::FunctionCall { args: children, .. }
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
    data_type: DataType,
    field_schema: Option<ValueTypeRef>,
    complete_type: Option<novarocks_functions::FunctionValueType>,
    regexp_count_pattern_source: Option<novarocks_type_contract::RegexpCountPatternSource>,
}

impl StaticExprNode {
    pub fn new(
        kind: StaticExprKind,
        data_type: DataType,
        field_schema: Option<ValueTypeRef>,
    ) -> Self {
        Self {
            kind,
            data_type,
            field_schema,
            complete_type: None,
            regexp_count_pattern_source: None,
        }
    }

    /// Save the exact semantic and physical author with both original holds.
    /// The bare carrier/schema constructor remains an expansion-only adapter.
    pub fn new_value_typed(
        kind: StaticExprKind,
        value_type: novarocks_functions::FunctionValueType,
    ) -> Self {
        Self {
            kind,
            data_type: value_type.data_type().clone(),
            field_schema: None,
            complete_type: Some(value_type),
            regexp_count_pattern_source: None,
        }
    }

    /// Preserve the same call-local legacy fact; no compiled-call authority is
    /// supplied here. Complete child admission is checked by the graph owner.
    pub fn with_regexp_count_pattern_source(
        mut self,
        source: novarocks_type_contract::RegexpCountPatternSource,
    ) -> Result<Self, StaticExpressionError> {
        if !matches!(&self.kind, StaticExprKind::FunctionCall { kind: StaticFunctionKind::String("regexp_count"), args } if args.len() == 2)
        {
            return Err(StaticExpressionError::InvalidRegexpCountSource);
        }
        if self
            .regexp_count_pattern_source
            .is_some_and(|previous| previous != source)
        {
            return Err(StaticExpressionError::InvalidRegexpCountSource);
        }
        self.regexp_count_pattern_source = Some(source);
        Ok(self)
    }

    pub fn regexp_count_pattern_source(
        &self,
    ) -> Option<novarocks_type_contract::RegexpCountPatternSource> {
        self.regexp_count_pattern_source
    }

    pub fn value_type(&self) -> Option<&novarocks_functions::FunctionValueType> {
        self.complete_type.as_ref()
    }

    pub const fn kind(&self) -> &StaticExprKind {
        &self.kind
    }

    pub const fn data_type(&self) -> &DataType {
        &self.data_type
    }

    pub fn field_schema(&self) -> Option<&ValueTypeRef> {
        self.complete_type
            .as_ref()
            .map(|ty| ty.value_type())
            .or(self.field_schema.as_ref())
    }
}

#[derive(Clone, Debug)]
pub struct ImmutableExpressions {
    nodes: Arc<[StaticExprNode]>,
    allow_throw_exception: bool,
    query_global_dicts: Arc<HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>>>,
    session_time_zone: Option<Arc<str>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StaticExpressionError {
    RuntimeBoundArena,
    TooManyNodes,
    InvalidReference,
    TooDeep,
    TooManyBytes,
    DuplicateLambdaSlot,
    InvalidMetadataArity,
    UnsupportedDecimalCastPolicy,
    ConstantTypeMismatch,
    InvalidRegexpCountSource,
}

impl fmt::Display for StaticExpressionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid static expression graph: {self:?}")
    }
}

impl std::error::Error for StaticExpressionError {}

/// Original expression failures and original request-control interruptions stay
/// distinct at pure compilation. The result owns no control capability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
    pub fn try_new(
        nodes: Vec<StaticExprNode>,
        allow_throw_exception: bool,
        query_global_dicts: HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>>,
        session_time_zone: Option<Arc<str>>,
    ) -> Result<Self, StaticExpressionError> {
        Self::try_new_core(
            nodes,
            allow_throw_exception,
            query_global_dicts,
            session_time_zone,
            |_| Ok(()),
        )
    }

    /// Pure compilation uses the original request control. The legacy entry
    /// shares the same validation core without adding a runtime control source.
    /// SDK policy traversal and final Arc allocation have observations around
    /// opaque work; this does not establish internal cooperation or MEM grants.
    pub fn try_new_for_compile(
        nodes: Vec<StaticExprNode>,
        allow_throw_exception: bool,
        query_global_dicts: HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>>,
        session_time_zone: Option<Arc<str>>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ExpressionsCompileError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::LowerProgram)?;
        let result = Self::try_new_core(
            nodes,
            allow_throw_exception,
            query_global_dicts,
            session_time_zone,
            |operation| match operation {
                ExpressionObservation::Step => {
                    work.step().map_err(ExpressionsCompileError::Control)
                }
                ExpressionObservation::OpaqueBoundary => {
                    work.flush().map_err(ExpressionsCompileError::Control)
                }
            },
        );
        if matches!(&result, Err(ExpressionsCompileError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    fn try_new_core<E: From<StaticExpressionError>>(
        nodes: Vec<StaticExprNode>,
        allow_throw_exception: bool,
        query_global_dicts: HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>>,
        session_time_zone: Option<Arc<str>>,
        mut observe: impl FnMut(ExpressionObservation) -> Result<(), E>,
    ) -> Result<Self, E> {
        let too_many = nodes.len() > MAX_STATIC_EXPRESSIONS;
        observe(ExpressionObservation::Step)?;
        if too_many {
            return Err(StaticExpressionError::TooManyNodes.into());
        }
        let mut depths = Vec::with_capacity(nodes.len());
        let mut dynamic_bytes = session_time_zone.as_ref().map_or(0, |zone| zone.len());
        let mut charged_dicts = HashSet::new();
        let mut charged_constant_arrays = HashSet::new();
        for (index, node) in nodes.iter().enumerate() {
            if let StaticExprKind::NaryAnd { args } | StaticExprKind::NaryOr { args } = &node.kind {
                let empty = args.is_empty();
                observe(ExpressionObservation::Step)?;
                if empty {
                    return Err(StaticExpressionError::InvalidMetadataArity.into());
                }
            }
            let mut depth = 1_usize;
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
            if let Some(source) = node.regexp_count_pattern_source {
                let StaticExprKind::FunctionCall {
                    kind: StaticFunctionKind::String("regexp_count"),
                    args,
                } = &node.kind
                else {
                    return Err(StaticExpressionError::InvalidRegexpCountSource.into());
                };
                let [_, pattern] = args.as_slice() else {
                    return Err(StaticExpressionError::InvalidRegexpCountSource.into());
                };
                let pattern = &nodes[pattern.index()];
                let exact_physical_utf8 = |ty: &novarocks_functions::FunctionValueType| {
                    ty.logical_type() == novarocks_type_contract::ValueLogicalType::Physical
                        && ty.data_type() == &DataType::Utf8
                };
                // Saved declared facts do not replace the checked pool's domain.
                if pattern.data_type != DataType::Utf8
                    || pattern
                        .value_type()
                        .is_some_and(|ty| !exact_physical_utf8(ty))
                    || pattern.field_schema().is_some_and(|schema| {
                        !matches!(schema.kind(), novarocks_type_contract::ValueKind::String)
                    })
                    || matches!(pattern.kind(), StaticExprKind::Constant(value) if !exact_physical_utf8(value.value_type()))
                    || (source.invalid_pattern_is_error()
                        && !matches!(
                            pattern.kind(),
                            StaticExprKind::Constant(_)
                                | StaticExprKind::Literal(StaticLiteral::Utf8(_))
                        ))
                {
                    return Err(StaticExpressionError::InvalidRegexpCountSource.into());
                }
                observe(ExpressionObservation::Step)?;
            }
            if let StaticExprKind::Cast(child, policy)
            | StaticExprKind::CastTime(child, policy)
            | StaticExprKind::CastTimeFromDatetime(child, policy) = &node.kind
            {
                observe(ExpressionObservation::OpaqueBoundary)?;
                let supported = novarocks_type_contract::decimal_error_policy_cast_supported(
                    &nodes[child.index()].data_type,
                    &node.data_type,
                    *policy,
                );
                observe(ExpressionObservation::OpaqueBoundary)?;
                if !supported {
                    return Err(StaticExpressionError::UnsupportedDecimalCastPolicy.into());
                }
            }
            let too_deep = depth > MAX_STATIC_EXPRESSION_DEPTH;
            observe(ExpressionObservation::Step)?;
            if too_deep {
                return Err(StaticExpressionError::TooDeep.into());
            }
            depths.push(depth);
            observe(ExpressionObservation::Step)?;
            if let StaticExprKind::Literal(literal) = &node.kind {
                let next = dynamic_bytes.checked_add(literal.dynamic_bytes());
                observe(ExpressionObservation::Step)?;
                dynamic_bytes = next.ok_or(StaticExpressionError::TooManyBytes)?;
            }
            if let StaticExprKind::Constant(value) = &node.kind {
                // Keep the original exact comparison and admission semantics.
                // Its type/metadata traversal is opaque in this slice; a flush
                // is an interruption boundary, not completed internal work.
                observe(ExpressionObservation::OpaqueBoundary)?;
                let same = novarocks_type_contract::arrow_data_types_exact(
                    &node.data_type,
                    &value.value_type().data_type(),
                );
                observe(ExpressionObservation::OpaqueBoundary)?;
                if !same {
                    return Err(StaticExpressionError::ConstantTypeMismatch.into());
                }
                // Ownership deduplication is not semantic equality; a selected
                // ordinal retains its whole already checked immutable pool.
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
            if let StaticExprKind::DictDecode { dict, .. } = &node.kind {
                let first = charged_dicts.insert(Arc::as_ptr(dict));
                observe(ExpressionObservation::Step)?;
                if first {
                    for value in dict.values() {
                        let next = dynamic_bytes.checked_add(value.len());
                        observe(ExpressionObservation::Step)?;
                        dynamic_bytes = next.ok_or(StaticExpressionError::TooManyBytes)?;
                    }
                }
            }
            if let StaticExprKind::LambdaFunction {
                arg_slots,
                common_sub_exprs,
                ..
            } = &node.kind
            {
                let mut slots = BTreeSet::new();
                for slot in arg_slots
                    .iter()
                    .chain(common_sub_exprs.iter().map(|(slot, _)| slot))
                {
                    let duplicate = !slots.insert(*slot);
                    observe(ExpressionObservation::Step)?;
                    if duplicate {
                        return Err(StaticExpressionError::DuplicateLambdaSlot.into());
                    }
                }
            }
            let too_many_bytes = dynamic_bytes > MAX_STATIC_EXPRESSION_DYNAMIC_BYTES;
            observe(ExpressionObservation::Step)?;
            if too_many_bytes {
                return Err(StaticExpressionError::TooManyBytes.into());
            }
        }
        for dict in query_global_dicts.values() {
            let first = charged_dicts.insert(Arc::as_ptr(dict));
            observe(ExpressionObservation::Step)?;
            if first {
                for value in dict.values() {
                    let next = dynamic_bytes.checked_add(value.len());
                    observe(ExpressionObservation::Step)?;
                    dynamic_bytes = next.ok_or(StaticExpressionError::TooManyBytes)?;
                    let too_many_bytes = dynamic_bytes > MAX_STATIC_EXPRESSION_DYNAMIC_BYTES;
                    observe(ExpressionObservation::Step)?;
                    if too_many_bytes {
                        return Err(StaticExpressionError::TooManyBytes.into());
                    }
                }
            }
        }
        observe(ExpressionObservation::OpaqueBoundary)?;
        let result = Self {
            nodes: Arc::from(nodes),
            allow_throw_exception,
            query_global_dicts: Arc::new(query_global_dicts),
            session_time_zone,
        };
        observe(ExpressionObservation::OpaqueBoundary)?;
        Ok(result)
    }

    pub fn nodes(&self) -> &[StaticExprNode] {
        &self.nodes
    }

    pub fn node(&self, id: ProgramExprId) -> Option<&StaticExprNode> {
        self.nodes.get(id.index())
    }

    pub const fn allow_throw_exception(&self) -> bool {
        self.allow_throw_exception
    }

    pub fn query_global_dict(&self, slot: SlotId) -> Option<&Arc<HashMap<i32, Vec<u8>>>> {
        self.query_global_dicts.get(&slot)
    }

    pub fn query_global_dicts(&self) -> &HashMap<SlotId, Arc<HashMap<i32, Vec<u8>>>> {
        &self.query_global_dicts
    }

    pub fn session_time_zone(&self) -> Option<&str> {
        self.session_time_zone.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_forward_references_and_cycles_before_freeze() {
        let nodes = vec![StaticExprNode::new(
            StaticExprKind::Not(ProgramExprId::new(0)),
            DataType::Boolean,
            None,
        )];
        assert!(matches!(
            ImmutableExpressions::try_new(nodes, false, HashMap::new(), None),
            Err(StaticExpressionError::InvalidReference)
        ));
    }

    #[test]
    fn shares_frozen_dict_backing_without_rebuilding() {
        let dict = Arc::new(HashMap::from([(1, vec![b'a'])]));
        let nodes = vec![
            StaticExprNode::new(
                StaticExprKind::SlotId(SlotId::new(1)),
                DataType::Int32,
                None,
            ),
            StaticExprNode::new(
                StaticExprKind::DictDecode {
                    child: ProgramExprId::new(0),
                    dict: Arc::clone(&dict),
                },
                DataType::Utf8,
                None,
            ),
        ];
        let expressions = ImmutableExpressions::try_new(nodes, false, HashMap::new(), None)
            .expect("valid expressions");
        let cloned = expressions.clone();
        assert!(Arc::ptr_eq(&expressions.nodes, &cloned.nodes));
        let StaticExprKind::DictDecode { dict: stored, .. } = cloned.nodes()[1].kind() else {
            panic!("expected dict decode")
        };
        assert!(Arc::ptr_eq(&dict, stored));
    }
    #[test]
    fn static_binding_rejects_nested_reporting_decimal_casts() {
        use arrow_schema::{Field, Fields};
        use novarocks_type_contract::DecimalOverflowPolicy as Policy;
        fn containers(decimal: DataType) -> Vec<DataType> {
            let field = Arc::new(Field::new("element", decimal.clone(), true));
            let entries = Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", decimal.clone(), true),
                ])),
                false,
            );
            vec![
                DataType::List(field.clone()),
                DataType::LargeList(field.clone()),
                DataType::FixedSizeList(field, 2),
                DataType::Struct(Fields::from(vec![Field::new("value", decimal, true)])),
                DataType::Map(Arc::new(entries), false),
            ]
        }
        for (source, target) in containers(DataType::Decimal128(10, 2))
            .into_iter()
            .zip(containers(DataType::Decimal128(12, 3)))
        {
            for (result_type, policy, accepted) in [
                (target.clone(), Policy::ReportError, false),
                (target, Policy::OutputNull, true),
                (source.clone(), Policy::ReportError, true),
            ] {
                let result = ImmutableExpressions::try_new(
                    vec![
                        StaticExprNode::new(
                            StaticExprKind::SlotId(SlotId::new(1)),
                            source.clone(),
                            None,
                        ),
                        StaticExprNode::new(
                            StaticExprKind::Cast(ProgramExprId::new(0), policy),
                            result_type,
                            None,
                        ),
                    ],
                    false,
                    HashMap::new(),
                    None,
                );
                if accepted {
                    result.unwrap();
                } else {
                    assert!(matches!(
                        result,
                        Err(StaticExpressionError::UnsupportedDecimalCastPolicy)
                    ));
                }
            }
        }
    }
    struct TestControl {
        trace: std::sync::Mutex<Vec<u32>>,
        stop: Option<(usize, CompileControlError)>,
    }
    impl TestControl {
        fn new(stop: Option<(usize, CompileControlError)>) -> Self {
            Self {
                trace: std::sync::Mutex::new(Vec::new()),
                stop,
            }
        }
        fn trace(&self) -> Vec<u32> {
            self.trace.lock().unwrap().clone()
        }
    }
    impl PureCompileControl for TestControl {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::LowerProgram);
            assert!(units <= 256);
            let mut trace = self.trace.lock().unwrap();
            let index = trace.len();
            trace.push(units);
            if let Some((at, cause)) = self.stop
                && index == at
            {
                return Err(cause);
            }
            Ok(())
        }
    }
    fn slot() -> StaticExprNode {
        StaticExprNode::new(
            StaticExprKind::SlotId(SlotId::new(1)),
            DataType::Int32,
            None,
        )
    }
    fn assert_every_checkpoint_refuses(nodes: &[StaticExprNode]) -> Vec<u32> {
        let baseline = TestControl::new(None);
        ImmutableExpressions::try_new_for_compile(
            nodes.to_vec(),
            false,
            HashMap::new(),
            None,
            &baseline,
        )
        .unwrap();
        let trace = baseline.trace();
        for at in 0..trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = TestControl::new(Some((at, cause)));
                let result = ImmutableExpressions::try_new_for_compile(
                    nodes.to_vec(),
                    false,
                    HashMap::new(),
                    None,
                    &control,
                );
                assert!(
                    matches!(result, Err(ExpressionsCompileError::Control(actual)) if actual == cause)
                );
                assert_eq!(
                    control.trace(),
                    trace[..=at],
                    "no callback after the first interruption"
                );
            }
        }
        trace
    }

    #[test]
    fn compile_reference_loop_observes_real_quantum_and_all_tails() {
        // Only two nodes: the positive quantum necessarily includes actual
        // dependency visits rather than a wide arena's outer-node loop.
        let nodes = vec![
            slot(),
            StaticExprNode::new(
                StaticExprKind::ArrayExpr {
                    elements: vec![ProgramExprId::new(0); 320],
                },
                DataType::List(Arc::new(arrow_schema::Field::new(
                    "item",
                    DataType::Int32,
                    true,
                ))),
                None,
            ),
        ];
        let trace = assert_every_checkpoint_refuses(&nodes);
        assert_eq!(trace.first(), Some(&0));
        assert!(trace.contains(&256));
        assert!(trace.iter().any(|units| *units > 0 && *units < 256));
        assert_eq!(trace.last(), Some(&0));
    }

    #[test]
    fn compile_dictionary_loop_observes_real_quantum_without_rebuilding_backing() {
        let dict = Arc::new(
            (0..320)
                .map(|key| (key, vec![b'x']))
                .collect::<HashMap<_, _>>(),
        );
        let nodes = vec![
            slot(),
            StaticExprNode::new(
                StaticExprKind::DictDecode {
                    child: ProgramExprId::new(0),
                    dict: dict.clone(),
                },
                DataType::Utf8,
                None,
            ),
        ];
        assert!(assert_every_checkpoint_refuses(&nodes).contains(&256));
        let control = TestControl::new(None);
        let result = ImmutableExpressions::try_new_for_compile(
            nodes,
            true,
            HashMap::from([(SlotId::new(9), dict.clone())]),
            Some(Arc::from("UTC")),
            &control,
        )
        .unwrap();
        let StaticExprKind::DictDecode { dict: retained, .. } = result.nodes()[1].kind() else {
            panic!("dict decode");
        };
        assert!(Arc::ptr_eq(&dict, retained));
        assert!(Arc::ptr_eq(
            &dict,
            result.query_global_dict(SlotId::new(9)).unwrap()
        ));
        assert!(result.allow_throw_exception());
        assert_eq!(result.session_time_zone(), Some("UTC"));
    }

    #[test]
    fn compile_ordinary_error_tail_preserves_original_control_and_legacy_error() {
        let nodes = vec![StaticExprNode::new(
            StaticExprKind::Not(ProgramExprId::new(0)),
            DataType::Boolean,
            None,
        )];
        let control = TestControl::new(None);
        assert!(matches!(
            ImmutableExpressions::try_new_for_compile(
                nodes.clone(),
                false,
                HashMap::new(),
                None,
                &control
            ),
            Err(ExpressionsCompileError::Expression(
                StaticExpressionError::InvalidReference
            ))
        ));
        assert_eq!(control.trace(), [0, 2]);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = TestControl::new(Some((1, cause)));
            assert!(
                matches!(ImmutableExpressions::try_new_for_compile(nodes.clone(), false, HashMap::new(), None, &control),
                Err(ExpressionsCompileError::Control(actual)) if actual == cause)
            );
            assert_eq!(control.trace(), [0, 2]);
        }
        assert!(matches!(
            ImmutableExpressions::try_new(nodes, false, HashMap::new(), None),
            Err(StaticExpressionError::InvalidReference)
        ));
    }

    #[test]
    fn compile_lambda_slot_loop_preserves_first_duplicate_and_control_quantum() {
        let args = (0..320).map(SlotId::new).collect::<Vec<_>>();
        let valid = vec![
            slot(),
            StaticExprNode::new(
                StaticExprKind::LambdaFunction {
                    body: ProgramExprId::new(0),
                    arg_slots: args.clone(),
                    common_sub_exprs: Vec::new(),
                    is_nondeterministic: false,
                },
                DataType::Int32,
                None,
            ),
        ];
        assert!(assert_every_checkpoint_refuses(&valid).contains(&256));
        let mut duplicate = args;
        duplicate.push(SlotId::new(0));
        let invalid = vec![
            slot(),
            StaticExprNode::new(
                StaticExprKind::LambdaFunction {
                    body: ProgramExprId::new(0),
                    arg_slots: duplicate,
                    common_sub_exprs: Vec::new(),
                    is_nondeterministic: false,
                },
                DataType::Int32,
                None,
            ),
        ];
        let control = TestControl::new(None);
        assert!(matches!(
            ImmutableExpressions::try_new_for_compile(
                invalid.clone(),
                false,
                HashMap::new(),
                None,
                &control
            ),
            Err(ExpressionsCompileError::Expression(
                StaticExpressionError::DuplicateLambdaSlot
            ))
        ));
        assert!(matches!(
            ImmutableExpressions::try_new(invalid, false, HashMap::new(), None),
            Err(StaticExpressionError::DuplicateLambdaSlot)
        ));
    }

    #[test]
    fn compile_and_legacy_preserve_shared_dictionary_retained_byte_boundary() {
        let dict = Arc::new(HashMap::from([(
            1,
            vec![0; MAX_STATIC_EXPRESSION_DYNAMIC_BYTES],
        )]));
        let nodes = vec![
            slot(),
            StaticExprNode::new(
                StaticExprKind::DictDecode {
                    child: ProgramExprId::new(0),
                    dict: dict.clone(),
                },
                DataType::Utf8,
                None,
            ),
        ];
        let globals = HashMap::from([(SlotId::new(1), dict.clone()), (SlotId::new(2), dict)]);
        let control = TestControl::new(None);
        ImmutableExpressions::try_new_for_compile(
            nodes.clone(),
            false,
            globals.clone(),
            None,
            &control,
        )
        .unwrap();
        ImmutableExpressions::try_new(nodes.clone(), false, globals.clone(), None).unwrap();
        let mut over = globals;
        over.insert(SlotId::new(3), Arc::new(HashMap::from([(1, vec![1])])));
        let control = TestControl::new(None);
        assert!(matches!(
            ImmutableExpressions::try_new_for_compile(
                nodes.clone(),
                false,
                over.clone(),
                None,
                &control
            ),
            Err(ExpressionsCompileError::Expression(
                StaticExpressionError::TooManyBytes
            ))
        ));
        assert!(matches!(
            ImmutableExpressions::try_new(nodes, false, over, None),
            Err(StaticExpressionError::TooManyBytes)
        ));
    }
}

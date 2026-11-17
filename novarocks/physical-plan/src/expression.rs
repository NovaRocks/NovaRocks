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

use std::collections::{BTreeMap, BTreeSet, btree_map::Entry};
use std::fmt;

use arrow_schema::DataType;

use novarocks_type_contract::{
    AggregateStateFormatId, CompileCheckpoints, CompileControlError, CompilePhase,
    FunctionArgumentEvaluation, FunctionArgumentTypes, FunctionFailureBehavior, FunctionId,
    FunctionKind, FunctionOverloadId, FunctionVolatility, PureCompileControl, SemanticParameterRef,
    TypedFactSlice,
};

use crate::{ExprId, ValueId, ValueType};

/// One expression use occurrence. Shared ExprIds retain independent demand;
/// this value does not authorize caching across uses or control domains.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExprUse {
    pub expr: ExprId,
    pub demand: novarocks_type_contract::EvaluationDemand,
}

#[derive(Clone, Debug, PartialEq)]
pub struct LegacyBindingMetadata {
    /// Original producer metadata for the remaining legacy publication path.
    /// This is not an occurrence effect declaration or an environment proof.
    pub volatility: FunctionVolatility,
    pub argument_evaluation: FunctionArgumentEvaluation,
    pub failure_behavior: FunctionFailureBehavior,
    pub intrinsic_row_error: novarocks_type_contract::FunctionIntrinsicRowError,
    /// Legacy producer/v1 projection, not the new occurrence's environment.
    /// FragmentPackage's mandatory call table is the only new-path authority.
    pub semantic_parameters: Box<[novarocks_type_contract::SemanticParameterRef]>,
}

/// Missing legacy provenance cannot be filled from a selected signature.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MissingLegacyBindingMetadata;
impl fmt::Display for MissingLegacyBindingMetadata {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("exact signature has no legacy binding metadata")
    }
}
impl std::error::Error for MissingLegacyBindingMetadata {}

#[derive(Clone, Debug, PartialEq)]
pub struct BoundFunction {
    pub function_id: FunctionId,
    pub overload: FunctionOverloadId,
    pub kind: FunctionKind,
    pub argument_types: FunctionArgumentTypes,
    pub result_type: ValueType,
    /// Explicit migration provenance. V2 never manufactures this metadata;
    /// its occurrence effects and environment belong to FrozenFragmentCalls.
    pub legacy_metadata: Option<LegacyBindingMetadata>,
}

impl BoundFunction {
    /// Materialize only the exact signature. This does not certify an installed
    /// implementation, original request, effects or an executable occurrence.
    pub fn from_exact_signature(
        function_id: FunctionId,
        overload: FunctionOverloadId,
        kind: FunctionKind,
        argument_types: FunctionArgumentTypes,
        result_type: ValueType,
    ) -> Self {
        Self {
            function_id,
            overload,
            kind,
            argument_types,
            result_type,
            legacy_metadata: None,
        }
    }

    pub fn require_legacy_metadata(
        &self,
    ) -> Result<&LegacyBindingMetadata, MissingLegacyBindingMetadata> {
        self.legacy_metadata
            .as_ref()
            .ok_or(MissingLegacyBindingMetadata)
    }

    /// Structural correspondence of the selected signature. Occurrence
    /// effects and environments are independent and require their own proof.
    pub(crate) fn signature_matches(&self, other: &Self) -> bool {
        self.function_id == other.function_id
            && self.overload == other.overload
            && self.kind == other.kind
            && self.argument_types == other.argument_types
            && self.result_type == other.result_type
    }
}

/// Exact binding for a function whose result is a relation rather than a
/// scalar value. Keeping this separate prevents outer-input pass-through
/// columns from being mistaken for function result columns.
#[derive(Clone, Debug, PartialEq)]
pub struct BoundTableFunction {
    pub function_id: FunctionId,
    pub overload: FunctionOverloadId,
    pub argument_types: FunctionArgumentTypes,
    pub result_types: TypedFactSlice<ValueType>,
    /// Original legacy producer provenance, absent on a V2 exact signature.
    pub legacy_metadata: Option<LegacyBindingMetadata>,
}
impl BoundTableFunction {
    /// Relation signature only; pass-through columns and actual occurrence
    /// effects are separate facts, never inferred from these result types.
    pub fn from_exact_signature(
        function_id: FunctionId,
        overload: FunctionOverloadId,
        argument_types: FunctionArgumentTypes,
        result_types: TypedFactSlice<ValueType>,
    ) -> Self {
        Self {
            function_id,
            overload,
            argument_types,
            result_types,
            legacy_metadata: None,
        }
    }

    pub fn require_legacy_metadata(
        &self,
    ) -> Result<&LegacyBindingMetadata, MissingLegacyBindingMetadata> {
        self.legacy_metadata
            .as_ref()
            .ok_or(MissingLegacyBindingMetadata)
    }
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
    /// Original state-row policy, independent of this phase's input channels.
    pub state_interpretation: Option<novarocks_type_contract::AggregateStateInterpretation>,
    pub state_argument_contract: novarocks_type_contract::AggregateStateArgumentContract,
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

impl BinaryOperator {
    /// Borrow the original neutral arithmetic identity. This mapping does not
    /// authorize a type profile or supply primitive effects.
    pub const fn arithmetic_operator(self) -> Option<novarocks_type_contract::ArithmeticOperator> {
        use novarocks_type_contract::ArithmeticOperator as A;
        Some(match self {
            Self::Add => A::Add,
            Self::Subtract => A::Subtract,
            Self::Multiply => A::Multiply,
            Self::Divide => A::Divide,
            Self::Modulo => A::Modulo,
            _ => return None,
        })
    }

    /// Borrow the original ordinary comparison identity. NULL-safe equality
    /// has its own owner and deliberately supplies no ordinary operator here.
    pub const fn comparison_operator(self) -> Option<novarocks_type_contract::ComparisonOperator> {
        use novarocks_type_contract::ComparisonOperator as C;
        Some(match self {
            Self::Eq => C::Eq,
            Self::NotEq => C::Ne,
            Self::Lt => C::Lt,
            Self::LtEq => C::Le,
            Self::Gt => C::Gt,
            Self::GtEq => C::Ge,
            _ => return None,
        })
    }
}

pub use novarocks_type_contract::{WindowFrameExclusion, WindowFrameUnits};
pub type WindowBound = novarocks_type_contract::WindowBound<ExprId>;
pub type WindowFrame = novarocks_type_contract::WindowFrame<ExprId>;

#[derive(Clone, Debug, PartialEq)]
pub enum ExprKind {
    Value(ValueId),
    LambdaParameter {
        lambda: ExprId,
        ordinal: u32,
    },
    /// Exact address in the sole immutable plan/package checked pool table.
    Constant(crate::ConstantReference),
    Unary {
        op: UnaryOperator,
        expr: ExprId,
    },
    Binary {
        left: ExprId,
        op: BinaryOperator,
        right: ExprId,
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
        /// Required for arithmetic; absent for comparisons and bitwise operators.
        /// The value is supplied by the plan's immutable parameter table.
        allow_throw_exception: Option<SemanticParameterRef>,
    },
    /// SQL `AND` over an ordered argument list.
    ///
    /// Boolean connectives are n-ary rather than binary so that the number of
    /// conjuncts a query writes does not become expression depth. A filter
    /// panel that emits 300 predicates is an ordinary query, not a deep one,
    /// and the depth bound exists to stop pathological nesting rather than to
    /// cap how many conditions a user may write.
    ///
    /// The list preserves source occurrences. A pure Boolean region may be
    /// scheduled using exact effect facts and buffer row data errors. Value
    /// demand is decided by FALSE; TruthOnly demand is decided by FALSE or
    /// NULL. Observable state/effects and strong conditional domains constrain
    /// movement independently of Boolean associativity.
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
        decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy,
        allow_throw_exception: SemanticParameterRef,
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
        aggregate_binding: Option<Box<AggregateBinding>>,
    },
}

impl ExprKind {
    /// Intrinsic invocation topology from the original definition vocabulary.
    /// Function calls deliberately return None: their exact installed owner
    /// supplies control, independently of legacy binding metadata. This proves
    /// neither installed capability nor complete occurrence effects.
    pub fn intrinsic_control_shape(
        &self,
    ) -> Result<
        Option<novarocks_type_contract::ControlShape>,
        novarocks_type_contract::ExpressionControlFlowError,
    > {
        use crate::ExprKind;
        use novarocks_type_contract::ControlShape;
        Ok(match self {
            ExprKind::Conjunction { .. } => Some(ControlShape::Conjunction),
            ExprKind::Disjunction { .. } => Some(ControlShape::Disjunction),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => Some(ControlShape::Case {
                simple: operand.is_some(),
                arms: u32::try_from(when_then.len()).map_err(|_| {
                    novarocks_type_contract::ExpressionControlFlowError::TooManyItems
                })?,
                has_else: else_expr.is_some(),
            }),
            ExprKind::Between { negated, .. } => Some(ControlShape::Between { negated: *negated }),
            ExprKind::InList { negated, .. } => {
                Some(ControlShape::Membership { negated: *negated })
            }
            ExprKind::Lambda { .. } => Some(ControlShape::LambdaBody),
            ExprKind::FunctionCall { .. } => None,
            ExprKind::Value(_)
            | ExprKind::LambdaParameter { .. }
            | ExprKind::Constant(_)
            | ExprKind::Unary { .. }
            | ExprKind::Binary { .. }
            | ExprKind::Cast { .. }
            | ExprKind::IsNull { .. }
            | ExprKind::Like { .. }
            | ExprKind::IsTruthValue { .. }
            | ExprKind::WindowCall { .. } => Some(ControlShape::Eager),
        })
    }

    /// Original ordered evaluation occurrences. A definition edge is not a
    /// memoized invocation: BETWEEN reads its operand in both comparisons.
    pub fn invocation_references_observed<E>(
        &self,
        mut visit: impl FnMut(ExprId) -> Result<(), E>,
    ) -> Result<(), E> {
        if let Self::Between {
            expr,
            low,
            high,
            negated,
        } = self
        {
            for role in novarocks_type_contract::NativeBetweenPlan::new(*negated).sources() {
                visit(match role {
                    novarocks_type_contract::BetweenSourceRole::Operand => *expr,
                    novarocks_type_contract::BetweenSourceRole::Lower => *low,
                    novarocks_type_contract::BetweenSourceRole::Upper => *high,
                })?;
            }
            Ok(())
        } else {
            self.expression_references_observed(visit)
        }
    }

    /// Actual primitive parameter consumers, independent of function-call
    /// occurrence environments and legacy binding migration fields.
    pub fn intrinsic_parameter_references(&self) -> impl Iterator<Item = &SemanticParameterRef> {
        match self {
            Self::Binary {
                allow_throw_exception,
                ..
            } => allow_throw_exception.as_ref(),
            Self::Cast {
                allow_throw_exception,
                ..
            } => Some(allow_throw_exception),
            _ => None,
        }
        .into_iter()
    }

    pub(crate) fn expression_references(&self, output: &mut Vec<ExprId>) {
        let result = self.expression_references_observed::<std::convert::Infallible>(|id| {
            output.push(id);
            Ok(())
        });
        match result {
            Ok(()) => {}
            Err(never) => match never {},
        }
    }
    /// Visit actual ordered children without copying the DAG edge table.
    /// The caller meters each callback and admits its own output allocations.
    /// This loan uses the same ordered children as definition correspondence.
    pub fn expression_references_observed<E>(
        &self,
        mut visit: impl FnMut(ExprId) -> Result<(), E>,
    ) -> Result<(), E> {
        match self {
            Self::Value(_) | Self::LambdaParameter { .. } | Self::Constant(_) => {}
            Self::Unary { expr, .. }
            | Self::Cast { expr, .. }
            | Self::IsNull { expr, .. }
            | Self::IsTruthValue { expr, .. } => visit(*expr)?,
            Self::Binary { left, right, .. } => {
                visit(*left)?;
                visit(*right)?;
            }
            Self::Conjunction { args }
            | Self::Disjunction { args }
            | Self::FunctionCall { args, .. } => {
                for id in args {
                    visit(*id)?;
                }
            }
            Self::Lambda { body, .. } => visit(*body)?,
            Self::InList { expr, list, .. } => {
                visit(*expr)?;
                for id in list {
                    visit(*id)?;
                }
            }
            Self::Between {
                expr, low, high, ..
            } => {
                visit(*expr)?;
                visit(*low)?;
                visit(*high)?;
            }
            Self::Like { expr, pattern, .. } => {
                visit(*expr)?;
                visit(*pattern)?;
            }
            Self::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(id) = operand {
                    visit(*id)?;
                }
                for (when, then) in when_then {
                    visit(*when)?;
                    visit(*then)?;
                }
                if let Some(id) = else_expr {
                    visit(*id)?;
                }
            }
            Self::WindowCall {
                args,
                function_order_by,
                frame,
                ..
            } => {
                for id in args {
                    visit(*id)?;
                }
                for item in function_order_by {
                    visit(item.expr)?;
                }
                if let Some(frame) = frame {
                    for bound in [&frame.start, &frame.end] {
                        if let WindowBound::Preceding(id) | WindowBound::Following(id) = bound {
                            visit(*id)?;
                        }
                    }
                }
            }
        }
        Ok(())
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

/// Failures of sparse definition ownership, before fragment semantics are checked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExprArenaConstructionError {
    Control(CompileControlError),
    TooManyDefinitions,
    DefinitionCountMismatch { declared: usize, actual: usize },
    DuplicateDefinition(ExprId),
}
impl From<CompileControlError> for ExprArenaConstructionError {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl fmt::Display for ExprArenaConstructionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(error) => error.fmt(formatter),
            Self::TooManyDefinitions => {
                formatter.write_str("expression definition count exceeds its fragment limit")
            }
            Self::DefinitionCountMismatch { declared, actual } => {
                write!(
                    formatter,
                    "expression iterator declared {declared} definitions but yielded {actual}"
                )
            }
            Self::DuplicateDefinition(id) => {
                write!(formatter, "duplicate expression definition {}", id.get())
            }
        }
    }
}
impl std::error::Error for ExprArenaConstructionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(error) => Some(error),
            _ => None,
        }
    }
}

impl ExprArena {
    pub(crate) fn get_mut_for_construction(&mut self, id: ExprId) -> Option<&mut ExprNode> {
        self.nodes.get_mut(&id)
    }
    /// Own already-authored definitions while preserving every sparse ID.
    /// Zero and MAX are ordinary sparse keys; no next-ID allocator is involved.
    ///
    /// This checks definition count and duplicates only. The original fragment
    /// validators still own types, references, lexical scopes and graph shape.
    /// The caller must admit iterator production/clones and arena allocations
    /// before calling; this constructor is not a resource grant or preflight.
    /// The declared length is checked against actual pulls before inserting an
    /// excess entry, and short iterators are rejected before publication.
    pub fn try_from_definitions_observed(
        definitions: impl ExactSizeIterator<Item = ExprNode>,
        limits: &crate::PlanLimits,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ExprArenaConstructionError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
        let result = Self::try_from_definitions_in(definitions, limits, &mut work);
        if matches!(&result, Err(ExprArenaConstructionError::Control(_))) {
            return result;
        }
        work.finish()?;
        result
    }

    /// Compose the same checked sparse construction in the caller's original
    /// phase and checkpoint scope. The caller owns entry, ordinary/success
    /// finish, and prior admission of iterator production and tree allocations.
    /// Standard-library entry/insertion is an opaque operation bracketed by
    /// actual checkpoints; its internal movement is not cooperative work.
    pub fn try_from_definitions_in(
        mut definitions: impl ExactSizeIterator<Item = ExprNode>,
        limits: &crate::PlanLimits,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ExprArenaConstructionError> {
        let count = definitions.len();
        work.step()?;
        if count > limits.fragment_expressions {
            return Err(ExprArenaConstructionError::TooManyDefinitions);
        }
        let mut nodes = BTreeMap::new();
        let mut actual_count = 0usize;
        for definition in &mut definitions {
            let next_count = actual_count.checked_add(1);
            // The source iterator has completed its pull. Count it even
            // when the actual count check is about to reject the source.
            work.step()?;
            actual_count = next_count.ok_or(ExprArenaConstructionError::TooManyDefinitions)?;
            if actual_count > count {
                return Err(ExprArenaConstructionError::DefinitionCountMismatch {
                    declared: count,
                    actual: actual_count,
                });
            }
            let id = definition.id;
            work.flush()?;
            let inserted = match nodes.entry(id) {
                Entry::Vacant(entry) => {
                    entry.insert(definition);
                    Ok(())
                }
                Entry::Occupied(_) => Err(ExprArenaConstructionError::DuplicateDefinition(id)),
            };
            work.step()?;
            work.flush()?;
            inserted?;
        }
        if actual_count != count {
            return Err(ExprArenaConstructionError::DefinitionCountMismatch {
                declared: count,
                actual: actual_count,
            });
        }
        Ok(Self { nodes })
    }
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

impl novarocks_type_contract::ExpressionDefinitionMembership<ExprId> for ExprArena {
    fn definition_count(&self) -> usize {
        self.len()
    }
    fn contains_definition(&self, id: ExprId) -> bool {
        self.get(id).is_some()
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
) -> Result<bool, MissingLegacyBindingMetadata> {
    check_definition_properties(arena, expressions, allow_values, true)
}

/// Check the existing closed-definition prerequisite independently of runtime
/// effect claims. This does not establish replica equivalence: exact actual
/// occurrences must separately provide their complete frozen call facts.
pub(crate) fn expressions_have_closed_value_scope(
    arena: &ExprArena,
    expressions: impl IntoIterator<Item = ExprId>,
) -> bool {
    // This branch never reads legacy provenance or certifies effects.
    matches!(
        check_definition_properties(arena, expressions, false, false),
        Ok(true)
    )
}

fn check_definition_properties(
    arena: &ExprArena,
    expressions: impl IntoIterator<Item = ExprId>,
    allow_values: bool,
    check_legacy_stability: bool,
) -> Result<bool, MissingLegacyBindingMetadata> {
    let mut pending = expressions.into_iter().collect::<Vec<_>>();
    let mut visited = BTreeSet::new();
    while let Some(expression) = pending.pop() {
        if !visited.insert(expression) {
            continue;
        }
        let Some(expression) = arena.get(expression) else {
            return Ok(false);
        };
        let immutable = match &expression.kind {
            ExprKind::Value(_) => allow_values,
            ExprKind::FunctionCall { function, .. } | ExprKind::WindowCall { function, .. } => {
                !check_legacy_stability
                    || function.require_legacy_metadata()?.volatility
                        == FunctionVolatility::Immutable
            }
            ExprKind::Constant(_)
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
            return Ok(false);
        }
        expression.kind.expression_references(&mut pending);
    }
    Ok(true)
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
        ExprKind::Cast { expr, target, .. } => {
            let operand = arena.get(*expr)?;
            widening_keeps_key_identity(operand.ty.data_type(), target)
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

#[cfg(test)]
#[path = "expression_arena_tests.rs"]
mod arena_construction_tests;

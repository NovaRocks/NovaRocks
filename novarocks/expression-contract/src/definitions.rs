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

use crate::{ExprId, ValueId, ValueType};
use arrow_schema::DataType;
use novarocks_type_contract::{
    FunctionArgumentEvaluation, FunctionArgumentTypes, FunctionFailureBehavior, FunctionId,
    FunctionKind, FunctionOverloadId, FunctionVolatility, SemanticParameterRef,
};
use std::fmt;

/// One expression use occurrence. Shared ExprIds retain independent demand;
/// this value does not authorize caching across uses or evaluation regions.
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
    /// The unit's per-use call claims are the occurrence authority.
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
    /// its occurrence effects and environment belong to the unit call claims.
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
    pub fn signature_matches(&self, other: &Self) -> bool {
        self.function_id == other.function_id
            && self.overload == other.overload
            && self.kind == other.kind
            && self.argument_types == other.argument_types
            && self.result_type == other.result_type
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnaryOperator {
    Not,
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
    /// The list preserves source occurrences. A demanded row error ends
    /// evaluation for that row. Value demand is decided by FALSE; TruthOnly
    /// demand is decided by FALSE or NULL. Reordering requires proof that it
    /// preserves row demand, failures and observable effects.
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
        cast_mode: novarocks_type_contract::TrinoCastMode,
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
        /// Use ordinary IEEE equality for the original floating comparison domain.
        ieee_float_equality: bool,
        operand: Option<ExprId>,
        when_then: Box<[(ExprId, ExprId)]>,
        else_expr: Option<ExprId>,
    },
    IsTruthValue {
        expr: ExprId,
        value: bool,
        negated: bool,
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
                ..
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
            | ExprKind::IsTruthValue { .. } => Some(ControlShape::Eager),
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
            _ => None,
        }
        .into_iter()
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
                ..
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
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ExprNode {
    pub id: ExprId,
    /// Innermost enclosing lambda; None denotes this unit root scope.
    pub lambda_scope: Option<ExprId>,
    pub ty: ValueType,
    pub kind: ExprKind,
}

/// Complete the Boolean NULL law using the original owner's actual constant
/// view. Neutral units retain references, so their structural gate cannot
/// decide this value-dependent proof. The caller supplies checked source
/// lookup and actual immediate CVs; computed expressions are never folded here.
/// This borrows the original work without opening an entry, scope or footer.
pub fn boolean_connective_nullability_observed<'source, E>(
    source: &ExprNode,
    definition: impl Fn(ExprId) -> Option<&'source ExprNode>,
    mut actual_constant: impl FnMut(
        &ExprNode,
        &mut novarocks_type_contract::CompileCheckpoints<'_>,
    ) -> Result<Option<bool>, E>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<bool, E>
where
    E: From<novarocks_type_contract::CompileControlError>,
{
    let (args, deciding) = match &source.kind {
        ExprKind::Conjunction { args } => (args, false),
        ExprKind::Disjunction { args } => (args, true),
        _ => return Ok(false),
    };
    if args.len() < 2
        || source.ty.data_type() != &arrow_schema::DataType::Boolean
        || source.ty.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
    {
        return Ok(false);
    }
    let mut nullable = false;
    let mut constant_decides = false;
    for id in args {
        let Some(operand) = definition(*id) else {
            return Ok(false);
        };
        work.step()?;
        if operand.ty.data_type() != &arrow_schema::DataType::Boolean
            || operand.ty.logical_type() != novarocks_type_contract::ValueLogicalType::Physical
        {
            return Ok(false);
        }
        nullable |= operand.ty.nullable();
        if matches!(operand.kind, ExprKind::Constant(_)) {
            constant_decides |= actual_constant(operand, work)? == Some(deciding);
            work.step()?;
        }
    }
    Ok(source.ty.nullable() || !nullable || constant_decides)
}

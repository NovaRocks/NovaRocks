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

//! Ordinary SQL comparison and total identity over complete composite types.

use std::sync::Arc;

use arrow_schema::DataType;
use novarocks_failure_contract::kernel_reason::MAP_KEY_INDETERMINATE;
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ExpressionEffectContext, ExpressionEffects,
    FunctionValueType, KeyComparison, KeyRelation, PureCompileControl, ValueKind, ValueTypeRef,
    key_domains_match_observed,
};
use novarocks_value_arrays::{
    ValueFailure,
    hash_eq::{BoundKeyArray, KeyPreparationFailure, KeyRows, PreparedKeyOperations},
};

use super::{ComparisonPrepareError, checked_address};
use crate::{
    AggregateStateAllocator, DataFailureReason, EvaluatedArgument, KernelEvaluationControl,
    KernelFailure, RowDataError, ScopedExpressionEffects,
    aggregate_host_allocator::HostAggregateAllocator,
    kernel_control::{internal, invalid},
    kernel_input::{EvaluationCheckpoints, validate_type_observed},
};

/// Scratch allocation uses the expression caller's existing host authority.
pub struct ComparisonAllocator(HostAggregateAllocator);

impl ComparisonAllocator {
    pub fn try_new(host: Arc<dyn AggregateStateAllocator>) -> Result<Self, KernelFailure> {
        Ok(Self(HostAggregateAllocator::try_new(host)?))
    }

    pub(crate) const fn retained_bytes() -> usize {
        HostAggregateAllocator::metadata_allocation_bytes()
    }
}

/// Final outcome of one demanded composite predicate. Key indeterminacy remains
/// distinct from SQL NULL until the caller places it in its row error journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompositeComparisonResult {
    Value(Option<bool>),
    Indeterminate,
}

impl CompositeComparisonResult {
    pub fn row_error(self, selected_ordinal: usize) -> Option<RowDataError> {
        match self {
            Self::Indeterminate => Some(RowDataError::with_reason(
                selected_ordinal,
                DataFailureReason::Domain(MAP_KEY_INDETERMINATE),
                "Map key comparison is indeterminate",
            )),
            Self::Value(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct ComparisonFacts {
    nullable: bool,
    may_raise_row_error: bool,
    requires_allocator: bool,
}

impl ComparisonFacts {
    fn join(self, other: Self) -> Self {
        Self {
            nullable: self.nullable || other.nullable,
            may_raise_row_error: self.may_raise_row_error || other.may_raise_row_error,
            requires_allocator: self.requires_allocator || other.requires_allocator,
        }
    }
}

// Design: ADR-0270 (docs/adr/ADR-0270-recursive-casts-and-composite-sql-predicates.md)
/// Exact SQL composite relation after the original type author has frozen any
/// needed coercions. Each operand retains its own layout and NULL declarations.
#[derive(Clone, Debug)]
pub struct PreparedCompositeComparisonRecipe {
    left: FunctionValueType,
    right: FunctionValueType,
    left_keys: PreparedKeyOperations,
    right_keys: PreparedKeyOperations,
    facts: ComparisonFacts,
}

impl PartialEq for PreparedCompositeComparisonRecipe {
    fn eq(&self, other: &Self) -> bool {
        self.relation() == other.relation() && self.left == other.left && self.right == other.right
    }
}

impl Eq for PreparedCompositeComparisonRecipe {}

impl PreparedCompositeComparisonRecipe {
    pub fn is_composite(ty: &FunctionValueType) -> bool {
        matches!(
            ty.value_type().kind(),
            ValueKind::List(_)
                | ValueKind::FixedSizeList { .. }
                | ValueKind::Struct(_)
                | ValueKind::Map { .. }
        )
    }

    pub fn try_new(
        relation: KeyRelation,
        left: &FunctionValueType,
        right: &FunctionValueType,
        control: &dyn PureCompileControl,
    ) -> Result<Self, ComparisonPrepareError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)?;
        let result = (|| {
            validate_type_observed(left, &mut work).map_err(ComparisonPrepareError::Kernel)?;
            validate_type_observed(right, &mut work).map_err(ComparisonPrepareError::Kernel)?;
            if !Self::is_composite(left) && !Self::is_composite(right) {
                return Err(ComparisonPrepareError::Unsupported);
            }
            if !key_domains_match_observed::<ComparisonPrepareError>(
                left.value_type(),
                right.value_type(),
                |units| observe_compile(&mut work, units).map_err(Into::into),
            )? {
                return Err(ComparisonPrepareError::TypeMismatch);
            }
            let mut observe = || work.step();
            let mut facts = comparison_facts(left.value_type(), &mut observe)?
                .join(comparison_facts(right.value_type(), &mut observe)?);
            if relation == KeyRelation::Identical {
                facts.nullable = false;
                facts.may_raise_row_error = false;
            }
            let mut prepare = |ty| {
                PreparedKeyOperations::prepare_observed(ty, relation, &mut |units| {
                    observe_compile(&mut work, units)
                })
                .map_err(|failure| match failure {
                    KeyPreparationFailure::Caller(error) => error.into(),
                    KeyPreparationFailure::Unsupported(_) => ComparisonPrepareError::Unsupported,
                })
            };
            Ok(Self {
                left_keys: prepare(left)?,
                right_keys: prepare(right)?,
                left: left.clone(),
                right: right.clone(),
                facts,
            })
        })();
        if result
            .as_ref()
            .is_err_and(|error: &ComparisonPrepareError| error.control_error().is_some())
        {
            return result;
        }
        work.finish()?;
        result
    }

    /// Row error possible for a selected ordinary equality domain.
    /// This is the same structural fact used by checked recipe preparation.
    pub fn equal_may_raise_row_error(ty: &FunctionValueType) -> bool {
        match comparison_facts(ty.value_type(), &mut || {
            Ok::<_, std::convert::Infallible>(())
        }) {
            Ok(facts) => facts.may_raise_row_error,
            Err(never) => match never {},
        }
    }

    pub fn left_type(&self) -> &FunctionValueType {
        &self.left
    }

    pub fn right_type(&self) -> &FunctionValueType {
        &self.right
    }

    pub fn relation(&self) -> KeyRelation {
        self.left_keys.relation()
    }

    pub fn nullable_result(&self) -> bool {
        self.facts.nullable
    }

    pub fn may_raise_row_error(&self) -> bool {
        self.facts.may_raise_row_error
    }

    pub fn requires_allocator(&self) -> bool {
        self.facts.requires_allocator
    }

    pub fn own_effects(&self, context: ExpressionEffectContext) -> ScopedExpressionEffects {
        ScopedExpressionEffects::primitive(context, self.static_own_effects())
    }

    pub fn static_own_effects(&self) -> ExpressionEffects {
        ExpressionEffects {
            may_raise_row_error: self.may_raise_row_error(),
            ..ExpressionEffects::PURE_VALUE
        }
    }

    /// The caller supplies one demanded pair and excludes inherited row errors.
    /// An allocator is needed only when this pair actually constructs MAP scratch.
    #[allow(clippy::too_many_arguments)]
    pub fn compare_rows(
        &self,
        left: EvaluatedArgument<'_>,
        left_ordinal: usize,
        left_batch_row: usize,
        right: EvaluatedArgument<'_>,
        right_ordinal: usize,
        right_batch_row: usize,
        allocator: Option<&ComparisonAllocator>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<CompositeComparisonResult, KernelFailure> {
        control.checkpoint(0)?;
        let mut work = EvaluationCheckpoints::new(control);
        let result = (|| {
            let l = checked_address(left, left_ordinal, left_batch_row, &mut work)?;
            let r = checked_address(right, right_ordinal, right_batch_row, &mut work)?;
            let left = bind(&self.left_keys, &self.left, left, l, &mut work)?;
            let right = bind(&self.right_keys, &self.right, right, r, &mut work)?;
            work.flush()?;
            let mut observe = |units| observe_evaluation(&mut work, units);
            let comparison = if let Some(allocator) = allocator {
                self.left_keys
                    .compare_in_observed(&left, &right, (l, r), &allocator.0, &mut observe)
                    .map_err(|cause| {
                        allocator
                            .0
                            .take_recorded_failure()
                            .unwrap_or_else(|| value_failure(cause))
                    })?
            } else {
                self.left_keys
                    .compare_observed(
                        &left,
                        l,
                        &right,
                        r,
                        &mut |_| Err(invalid("composite comparison requires its allocation host")),
                        &mut observe,
                    )
                    .map_err(value_failure)?
            };
            Ok(match (self.relation(), comparison) {
                (_, KeyComparison::True) => CompositeComparisonResult::Value(Some(true)),
                (_, KeyComparison::False) => CompositeComparisonResult::Value(Some(false)),
                (KeyRelation::Equal, KeyComparison::Unknown) => {
                    CompositeComparisonResult::Value(None)
                }
                (KeyRelation::Equal, KeyComparison::Indeterminate) => {
                    CompositeComparisonResult::Indeterminate
                }
                (KeyRelation::Identical, _) => {
                    return Err(internal("composite identity produced an uncertain result"));
                }
            })
        })();
        work.finish_result(result)
    }
}

fn comparison_facts<E>(
    ty: &ValueTypeRef,
    observe: &mut impl FnMut() -> Result<(), E>,
) -> Result<ComparisonFacts, E> {
    observe()?;
    let root = ComparisonFacts {
        nullable: ty.nullable() || matches!(ty.kind(), ValueKind::Scalar(DataType::Null)),
        ..ComparisonFacts::default()
    };
    Ok(match ty.kind() {
        ValueKind::List(child) | ValueKind::FixedSizeList { item: child, .. } => {
            root.join(comparison_facts(child, observe)?)
        }
        ValueKind::Struct(fields) => {
            let mut result = root;
            for field in fields.iter() {
                result = result.join(comparison_facts(field.value_type(), observe)?);
            }
            result
        }
        ValueKind::Map { key, .. } if matches!(key.kind(), ValueKind::Scalar(DataType::Null)) => {
            // MAP(UNKNOWN, V) cannot have an actual non-NULL entry key.
            root
        }
        ValueKind::Map { key, value } => {
            let keys = comparison_facts(key, observe)?;
            let values = comparison_facts(value, observe)?;
            ComparisonFacts {
                nullable: root.nullable || values.nullable,
                may_raise_row_error: keys.nullable
                    || keys.may_raise_row_error
                    || values.may_raise_row_error,
                requires_allocator: true,
            }
        }
        _ => root,
    })
}

fn bind<'a>(
    keys: &'a PreparedKeyOperations,
    expected: &FunctionValueType,
    argument: EvaluatedArgument<'a>,
    row: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<BoundKeyArray<'a>, KernelFailure> {
    if let Some(constant) = argument.constant_value() {
        let actual = constant.value_type();
        if (actual.nullable() && !expected.nullable())
            || !actual.same_physical_binding_ignoring_root_nullability_observed(expected, || {
                work.step()
            })?
        {
            return Err(invalid(
                "composite comparison constant differs from its frozen value type",
            ));
        }
    }
    keys.bind_observed(
        argument.array(),
        KeyRows::Range(row..row + 1),
        &mut |units| observe_evaluation(work, units),
    )
    .map_err(value_failure)
}

fn observe_compile(
    work: &mut CompileCheckpoints<'_>,
    units: usize,
) -> Result<(), novarocks_type_contract::CompileControlError> {
    for _ in 0..units {
        work.step()?;
    }
    Ok(())
}

fn observe_evaluation(
    work: &mut EvaluationCheckpoints<'_>,
    units: usize,
) -> Result<(), KernelFailure> {
    for _ in 0..units {
        work.step()?;
    }
    Ok(())
}

fn value_failure(cause: ValueFailure<KernelFailure>) -> KernelFailure {
    match cause {
        ValueFailure::Caller(error) => error,
        ValueFailure::Value(error) => KernelFailure::from(
            crate::array_projection::ArrayProjectionFailure::Value(error),
        ),
    }
}

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

//! Distinct implementation, call-site and expression/region effects. These
//! facts describe semantics; they never hold a mutable evaluation instance.
use crate::{
    FunctionFailureBehavior, FunctionIntrinsicRowError, FunctionKind, FunctionVolatility,
    SemanticParameterKey, SemanticParameterRef,
};
use std::{collections::BTreeSet, fmt};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EvaluationRegionId(u32);
impl EvaluationRegionId {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ExpressionUseId(u32);
impl ExpressionUseId {
    pub const fn new(id: u32) -> Self {
        Self(id)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Exact argument control, after signature binding. NULLIF evaluates both
/// operands; it is not the IF or COALESCE control protocol.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ArgumentControl {
    Eager,
    /// Invoke the exact owner without evaluating any argument value. Full
    /// logical definitions/types remain checked; this is not TypeOnly.
    NoArguments,
    /// Validate argument types without creating any value-evaluation demand.
    TypeOnly,
    If,
    Coalesce,
    Try,
    /// Demand each regexp argument only after the previous one succeeded and
    /// produced a non-NULL value. Compile the pattern at argument ordinal 1.
    OrderedRegexp,
    /// The exact installed owner demands original emitted source occurrences.
    TemporalSource(crate::TemporalSourceKind),
    SimpleCase,
    SearchedCase,
    /// The exact overload owner declares the lambda argument and the body's
    /// consumption context. Boolean-returning bodies are not automatically
    /// TruthOnly: map/materialization and truth filtering have different uses.
    HigherOrder {
        body_ordinal: u32,
        body_demand: crate::EvaluationDemand,
    },
    Aggregate,
    Window,
    Table,
}

impl ArgumentControl {
    /// Match the invocation protocol to already verified exact owner facts.
    /// Arity, ordered child definitions, guards and types remain separate
    /// obligations. This comparison does not authenticate an owner declaration
    /// or authorize effect movement. Relational lifecycle protocols deliberately
    /// do not become scalar Eager invocations.
    pub const fn matches_scalar_shape(self, shape: crate::ControlShape) -> bool {
        use crate::ControlShape;
        match (self, shape) {
            (Self::Eager, ControlShape::Eager)
            | (Self::TypeOnly, ControlShape::TypeOnly)
            | (Self::NoArguments, ControlShape::NoArguments)
            | (Self::If, ControlShape::If)
            | (Self::Coalesce, ControlShape::Coalesce)
            | (Self::Try, ControlShape::Try)
            | (Self::OrderedRegexp, ControlShape::OrderedRegexp)
            | (Self::SimpleCase, ControlShape::Case { simple: true, .. })
            | (Self::SearchedCase, ControlShape::Case { simple: false, .. }) => true,
            (
                Self::HigherOrder {
                    body_ordinal,
                    body_demand,
                },
                ControlShape::HigherOrder {
                    body_ordinal: actual_ordinal,
                    body_demand: actual_demand,
                },
            ) => {
                body_ordinal == actual_ordinal
                    && matches!(
                        (body_demand, actual_demand),
                        (
                            crate::EvaluationDemand::Value,
                            crate::EvaluationDemand::Value
                        ) | (
                            crate::EvaluationDemand::TruthOnly,
                            crate::EvaluationDemand::TruthOnly
                        )
                    )
            }
            (
                Self::TemporalSource(crate::TemporalSourceKind::TimeFormat),
                ControlShape::TemporalSource(
                    crate::TemporalSourceShape::FormatOrdinary
                    | crate::TemporalSourceShape::FormatUtf8Override,
                ),
            )
            | (
                Self::TemporalSource(crate::TemporalSourceKind::TimeToSec),
                ControlShape::TemporalSource(
                    crate::TemporalSourceShape::SecondsDirect
                    | crate::TemporalSourceShape::SecondsCastString
                    | crate::TemporalSourceShape::SecondsCastOther
                    | crate::TemporalSourceShape::SecondsRoundtrip,
                ),
            ) => true,
            _ => false,
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FunctionNullBehavior {
    /// NULL inputs produce NULL without invoking the row implementation.
    Strict,
    /// The implementation receives NULL inputs and owns its result rule.
    CalledOnNull,
    /// The exact control protocol owns argument demand and NULL decisions.
    ControlDefined,
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FunctionInstanceState {
    None,
    ScalarInstance,
    AggregateInstance,
    WindowPartition,
    TableInstance,
}
/// Independent observables; volatility alone cannot express these boundaries.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ObservableEffects {
    pub rng_sampling: bool,
    pub warnings: bool,
    pub controlled_wait: bool,
}
impl ObservableEffects {
    pub const NONE: Self = Self {
        rng_sampling: false,
        warnings: false,
        controlled_wait: false,
    };
    pub const fn is_empty(self) -> bool {
        !self.rng_sampling && !self.warnings && !self.controlled_wait
    }
    pub const fn union(self, other: Self) -> Self {
        Self {
            rng_sampling: self.rng_sampling || other.rng_sampling,
            warnings: self.warnings || other.warnings,
            controlled_wait: self.controlled_wait || other.controlled_wait,
        }
    }
}

/// The implementation owner's base facts for one exact overload. Environment
/// keys state possible dependencies; a bound call must freeze actual refs.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FunctionEffectDeclaration {
    pub value_stability: FunctionVolatility,
    pub own_row_error: FunctionIntrinsicRowError,
    pub failure_behavior: FunctionFailureBehavior,
    pub null_behavior: FunctionNullBehavior,
    pub argument_control: ArgumentControl,
    pub instance_state: FunctionInstanceState,
    pub observable_effects: ObservableEffects,
    pub environment_dependencies: Box<[SemanticParameterKey]>,
}
impl FunctionEffectDeclaration {
    pub fn validate(&self, kind: FunctionKind) -> Result<(), EffectContractError> {
        if self.environment_dependencies.len() > crate::MAX_SEMANTIC_PARAMETERS {
            return Err(EffectContractError::InvalidEnvironmentReference);
        }
        if kind != FunctionKind::Scalar && self.null_behavior == FunctionNullBehavior::Strict {
            return Err(EffectContractError::KindMismatch);
        }
        if !self.own_row_error.is_valid_for_kind(kind) {
            return Err(EffectContractError::KindMismatch);
        }
        let valid = match kind {
            FunctionKind::Scalar => {
                matches!(
                    self.argument_control,
                    ArgumentControl::Eager
                        | ArgumentControl::NoArguments
                        | ArgumentControl::TypeOnly
                        | ArgumentControl::If
                        | ArgumentControl::Coalesce
                        | ArgumentControl::Try
                        | ArgumentControl::OrderedRegexp
                        | ArgumentControl::TemporalSource(_)
                        | ArgumentControl::SimpleCase
                        | ArgumentControl::SearchedCase
                        | ArgumentControl::HigherOrder { .. }
                ) && matches!(
                    self.instance_state,
                    FunctionInstanceState::None | FunctionInstanceState::ScalarInstance
                )
            }
            FunctionKind::Aggregate => {
                self.argument_control == ArgumentControl::Aggregate
                    && self.instance_state == FunctionInstanceState::AggregateInstance
            }
            FunctionKind::Window => {
                self.argument_control == ArgumentControl::Window
                    && self.instance_state == FunctionInstanceState::WindowPartition
            }
            FunctionKind::Table => {
                self.argument_control == ArgumentControl::Table
                    && self.instance_state == FunctionInstanceState::TableInstance
            }
        };
        if !valid {
            return Err(EffectContractError::KindMismatch);
        }
        if matches!(
            self.argument_control,
            ArgumentControl::If
                | ArgumentControl::Coalesce
                | ArgumentControl::Try
                | ArgumentControl::OrderedRegexp
                | ArgumentControl::TemporalSource(_)
                | ArgumentControl::SimpleCase
                | ArgumentControl::SearchedCase
        ) && self.null_behavior != FunctionNullBehavior::ControlDefined
        {
            return Err(EffectContractError::ControlMismatch);
        }
        if self.argument_control == ArgumentControl::TypeOnly
            && (self.own_row_error != FunctionIntrinsicRowError::NoRowError
                || self.instance_state != FunctionInstanceState::None
                || !self.observable_effects.is_empty())
        {
            return Err(EffectContractError::ControlMismatch);
        }
        // The closed key vocabulary needs no heap-backed membership set.
        // Each exhaustive match arm names an actual semantic key, not an ID
        // range or a new limit on declarations.
        let mut statement_start_utc = false;
        let mut time_zone = false;
        let mut allow_throw_exception = false;
        let mut group_concat_legacy = false;
        let mut group_concat_max_len = false;
        for key in &self.environment_dependencies {
            let seen = match key {
                SemanticParameterKey::StatementStartUtc => &mut statement_start_utc,
                SemanticParameterKey::TimeZone => &mut time_zone,
                SemanticParameterKey::AllowThrowException => &mut allow_throw_exception,
                SemanticParameterKey::GroupConcatLegacy => &mut group_concat_legacy,
                SemanticParameterKey::GroupConcatMaxLen => &mut group_concat_max_len,
            };
            if std::mem::replace(seen, true) {
                return Err(EffectContractError::DuplicateEnvironmentKey);
            }
        }
        Ok(())
    }
}

/// Assumptions belong to one proven scalar region or exact relation identity.
/// This identity grants no permission to hoist, cache or change a lifecycle.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CallProofScope {
    Unconditional,
    Region(EvaluationRegionId),
    Relation(crate::RelationEffectId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CallEffects {
    pub value_stability: FunctionVolatility,
    pub own_row_error: FunctionIntrinsicRowError,
    pub failure_behavior: FunctionFailureBehavior,
    pub null_behavior: FunctionNullBehavior,
    pub argument_control: ArgumentControl,
    pub instance_state: FunctionInstanceState,
    pub observable_effects: ObservableEffects,
    pub environment: Box<[SemanticParameterRef]>,
    pub proof_scope: CallProofScope,
}
/// A selected original demand-zero is the only added specialization. This
/// validates an owner fact; it never infers that fact from arity or payload.
pub fn validate_selected_argument_control(
    base: ArgumentControl,
    selected: ArgumentControl,
) -> Result<(), EffectContractError> {
    if base == selected
        || (base == ArgumentControl::Eager && selected == ArgumentControl::NoArguments)
    {
        Ok(())
    } else {
        Err(EffectContractError::ControlMismatch)
    }
}
impl CallEffects {
    /// Refinement may prove fewer row errors or environment dependencies. It
    /// cannot change the control protocol or silently erase observable effects.
    pub fn validate_refinement(
        &self,
        base: &FunctionEffectDeclaration,
        scope: CallProofScope,
    ) -> Result<(), EffectContractError> {
        self.validate_refinement_with_argument_control(base, scope, base.argument_control)
    }
    /// The selected control was authenticated by the exact installed owner
    /// before runtime child uses were authored. Other refinements are unchanged.
    pub fn validate_refinement_with_argument_control(
        &self,
        base: &FunctionEffectDeclaration,
        scope: CallProofScope,
        selected_control: ArgumentControl,
    ) -> Result<(), EffectContractError> {
        validate_selected_argument_control(base.argument_control, selected_control)?;
        if self.proof_scope != scope {
            return Err(EffectContractError::ProofScopeMismatch);
        }
        if self.failure_behavior != base.failure_behavior
            || self.null_behavior != base.null_behavior
            || self.argument_control != selected_control
            || self.observable_effects != base.observable_effects
        {
            return Err(EffectContractError::ControlMismatch);
        }
        if self.value_stability > base.value_stability {
            return Err(EffectContractError::InvalidRefinement);
        }
        if !matches!(
            (base.own_row_error, self.own_row_error),
            (
                FunctionIntrinsicRowError::MayRaise,
                FunctionIntrinsicRowError::NoRowError | FunctionIntrinsicRowError::MayRaise
            ) | (
                FunctionIntrinsicRowError::NoRowError,
                FunctionIntrinsicRowError::NoRowError
            ) | (
                FunctionIntrinsicRowError::NotRowEvaluated,
                FunctionIntrinsicRowError::NotRowEvaluated
            )
        ) {
            return Err(EffectContractError::InvalidRefinement);
        }
        // Stateful scalar declarations may specialize to a stateless per-row
        // implementation (e.g. nonconstant rand(seed)); RNG remains observable.
        if self.instance_state != base.instance_state
            && !(base.instance_state == FunctionInstanceState::ScalarInstance
                && self.instance_state == FunctionInstanceState::None)
        {
            return Err(EffectContractError::InvalidRefinement);
        }
        if self.environment.len() > base.environment_dependencies.len() {
            return Err(EffectContractError::InvalidEnvironmentReference);
        }
        let mut refs = BTreeSet::new();
        let mut keys = BTreeSet::new();
        if self.environment.iter().any(|reference| {
            !base
                .environment_dependencies
                .contains(&reference.expected_key)
                || !refs.insert(*reference)
                || !keys.insert(reference.expected_key)
        }) {
            return Err(EffectContractError::InvalidEnvironmentReference);
        }
        Ok(())
    }
}

/// Conservative effects of an entire expression occurrence. Child errors are
/// composed independently of a parent's ReturnsNull policy. Operator lifecycle
/// failures remain separate from selected scalar row-data errors.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpressionEffects {
    pub value_stability: FunctionVolatility,
    pub may_raise_row_error: bool,
    /// Whole invocation Data is atomic, cannot be masked as a row error, and
    /// forbids reordering, replay or evaluation in an earlier region.
    pub may_raise_invocation_data: bool,
    pub has_instance_state: bool,
    pub observable_effects: ObservableEffects,
}
impl ExpressionEffects {
    pub const PURE_VALUE: Self = Self {
        value_stability: FunctionVolatility::Immutable,
        may_raise_row_error: false,
        may_raise_invocation_data: false,
        has_instance_state: false,
        observable_effects: ObservableEffects::NONE,
    };
    #[cfg(test)]
    fn with_call(self, call: &CallEffects) -> Self {
        self.join(Self {
            value_stability: call.value_stability,
            may_raise_row_error: call.own_row_error == FunctionIntrinsicRowError::MayRaise,
            may_raise_invocation_data: false,
            has_instance_state: call.instance_state != FunctionInstanceState::None,
            observable_effects: call.observable_effects,
        })
    }
    pub fn join(self, other: Self) -> Self {
        Self {
            value_stability: self.value_stability.max(other.value_stability),
            may_raise_row_error: self.may_raise_row_error || other.may_raise_row_error,
            may_raise_invocation_data: self.may_raise_invocation_data
                || other.may_raise_invocation_data,
            has_instance_state: self.has_instance_state || other.has_instance_state,
            observable_effects: self.observable_effects.union(other.observable_effects),
        }
    }
    /// Eligible only inside the already-proven pure Boolean region/region.
    /// Row errors are buffered there, rather than erased by this fact.
    pub const fn permits_boolean_reordering(self) -> bool {
        !matches!(self.value_stability, FunctionVolatility::Volatile)
            && !self.may_raise_invocation_data
            && !self.has_instance_state
            && self.observable_effects.is_empty()
    }
    /// Replaying a pure kernel for the same selected row may recreate a row
    /// error. This fact never permits repeating a successful state/effect.
    pub const fn permits_same_row_kernel_replay(self) -> bool {
        self.permits_boolean_reordering()
    }
    /// This is only an effect prerequisite. Relational, NULL-extension and
    /// control-region proofs are still required before earlier evaluation.
    pub const fn permits_earlier_evaluation(self) -> bool {
        self.permits_boolean_reordering() && !self.may_raise_row_error
    }
}

/// A summary stays attached to its use and region. Frozen facts must first be
/// recomputed by the exact owner, including complete environment dependencies.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExpressionEffectContext {
    pub use_id: ExpressionUseId,
    pub region: EvaluationRegionId,
    pub demand: crate::EvaluationDemand,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EffectContractError {
    KindMismatch,
    ControlMismatch,
    DuplicateEnvironmentKey,
    ProofScopeMismatch,
    CallIdentityMismatch,
    InvalidRefinement,
    InvalidEnvironmentReference,
}
impl fmt::Display for EffectContractError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid effect contract: {self:?}")
    }
}
impl std::error::Error for EffectContractError {}

#[cfg(test)]
mod tests {
    use super::*;
    fn base() -> FunctionEffectDeclaration {
        FunctionEffectDeclaration {
            value_stability: FunctionVolatility::Immutable,
            own_row_error: FunctionIntrinsicRowError::MayRaise,
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: FunctionNullBehavior::CalledOnNull,
            argument_control: ArgumentControl::Eager,
            instance_state: FunctionInstanceState::None,
            observable_effects: ObservableEffects::NONE,
            environment_dependencies: Box::default(),
        }
    }
    fn call() -> CallEffects {
        let base = base();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::default(),
            proof_scope: CallProofScope::Unconditional,
        }
    }
    #[test]
    fn returns_null_never_erases_child_errors() {
        let child = ExpressionEffects {
            may_raise_row_error: true,
            ..ExpressionEffects::PURE_VALUE
        };
        let call = CallEffects {
            failure_behavior: FunctionFailureBehavior::ReturnsNull,
            ..call()
        };
        assert!(child.with_call(&call).may_raise_row_error);
        assert!(
            ExpressionEffects::PURE_VALUE
                .with_call(&call)
                .may_raise_row_error
        );
        let handled = CallEffects {
            own_row_error: FunctionIntrinsicRowError::NoRowError,
            ..call
        };
        assert!(
            !ExpressionEffects::PURE_VALUE
                .with_call(&handled)
                .may_raise_row_error
        );
        assert!(child.with_call(&handled).may_raise_row_error);
    }
    #[test]
    fn state_and_observables_block_reordering_even_for_deterministic_values() {
        for observable_effects in [
            ObservableEffects {
                rng_sampling: true,
                ..ObservableEffects::NONE
            },
            ObservableEffects {
                warnings: true,
                ..ObservableEffects::NONE
            },
            ObservableEffects {
                controlled_wait: true,
                ..ObservableEffects::NONE
            },
        ] {
            let effects = ExpressionEffects {
                observable_effects,
                ..ExpressionEffects::PURE_VALUE
            };
            assert!(!effects.permits_boolean_reordering());
            assert!(!effects.permits_same_row_kernel_replay());
            assert!(!effects.permits_earlier_evaluation());
        }
        assert!(
            !ExpressionEffects {
                has_instance_state: true,
                ..ExpressionEffects::PURE_VALUE
            }
            .permits_boolean_reordering()
        );
        let may_fail = ExpressionEffects::PURE_VALUE.with_call(&call());
        assert!(may_fail.permits_boolean_reordering());
        assert!(may_fail.permits_same_row_kernel_replay());
        assert!(!may_fail.permits_earlier_evaluation());
    }
    #[test]
    fn refinement_cannot_escape_its_region_or_erase_rng() {
        let mut base = base();
        base.observable_effects.rng_sampling = true;
        base.instance_state = FunctionInstanceState::ScalarInstance;
        let mut call = call();
        call.observable_effects = base.observable_effects;
        let scope = CallProofScope::Region(EvaluationRegionId::new(u32::MAX));
        call.proof_scope = scope;
        call.validate_refinement(&base, scope).unwrap();
        assert_eq!(
            call.validate_refinement(&base, CallProofScope::Unconditional),
            Err(EffectContractError::ProofScopeMismatch)
        );
        call.observable_effects = ObservableEffects::NONE;
        assert_eq!(
            call.validate_refinement(&base, scope),
            Err(EffectContractError::ControlMismatch)
        );
    }

    #[test]
    fn exact_control_matches_shape_without_inventing_a_scalar_lifecycle() {
        use crate::{ControlShape, EvaluationDemand};
        let controls = [
            ArgumentControl::Eager,
            ArgumentControl::TypeOnly,
            ArgumentControl::If,
            ArgumentControl::Coalesce,
            ArgumentControl::Try,
            ArgumentControl::OrderedRegexp,
            ArgumentControl::SimpleCase,
            ArgumentControl::SearchedCase,
            ArgumentControl::HigherOrder {
                body_ordinal: 2,
                body_demand: EvaluationDemand::Value,
            },
            ArgumentControl::HigherOrder {
                body_ordinal: 2,
                body_demand: EvaluationDemand::TruthOnly,
            },
            ArgumentControl::Aggregate,
            ArgumentControl::Window,
            ArgumentControl::Table,
        ];
        let shapes = [
            ControlShape::Eager,
            ControlShape::TypeOnly,
            ControlShape::If,
            ControlShape::Coalesce,
            ControlShape::Try,
            ControlShape::OrderedRegexp,
            ControlShape::Case {
                simple: true,
                arms: 1,
                has_else: true,
            },
            ControlShape::Case {
                simple: false,
                arms: 1,
                has_else: true,
            },
            ControlShape::HigherOrder {
                body_ordinal: 2,
                body_demand: EvaluationDemand::Value,
            },
            ControlShape::HigherOrder {
                body_ordinal: 2,
                body_demand: EvaluationDemand::TruthOnly,
            },
        ];
        for (index, control) in controls.into_iter().enumerate() {
            for (shape_index, shape) in shapes.into_iter().enumerate() {
                assert_eq!(control.matches_scalar_shape(shape), index == shape_index);
            }
            assert!(!control.matches_scalar_shape(ControlShape::Conjunction));
            assert!(!control.matches_scalar_shape(ControlShape::Disjunction));
            assert!(!control.matches_scalar_shape(ControlShape::LambdaBody));
            assert!(!control.matches_scalar_shape(ControlShape::HigherOrder {
                body_ordinal: 1,
                body_demand: EvaluationDemand::Value
            }));
        }
    }

    #[test]
    fn higher_order_refinement_cannot_change_body_position_or_demand() {
        use crate::EvaluationDemand;
        let mut base = base();
        base.argument_control = ArgumentControl::HigherOrder {
            body_ordinal: 1,
            body_demand: EvaluationDemand::Value,
        };
        let mut refined = call();
        refined.argument_control = base.argument_control;
        refined
            .validate_refinement(&base, CallProofScope::Unconditional)
            .unwrap();
        for control in [
            ArgumentControl::HigherOrder {
                body_ordinal: 0,
                body_demand: EvaluationDemand::Value,
            },
            ArgumentControl::HigherOrder {
                body_ordinal: 1,
                body_demand: EvaluationDemand::TruthOnly,
            },
        ] {
            refined.argument_control = control;
            assert_eq!(
                refined.validate_refinement(&base, CallProofScope::Unconditional),
                Err(EffectContractError::ControlMismatch)
            );
        }
    }
}

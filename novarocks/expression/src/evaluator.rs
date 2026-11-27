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

//! Evaluation of checked unit-local root occurrences.
//!
//! This selected expression controller keeps state per root use and driver. The
//! host must install its memory scopes and allocation admission before calling
//! it, as required by the neutral kernel ABI. These controls do not mint a
//! memory grant. Nested body frames borrow the original host at their use.

use input_source::FrameInput;

use crate::{ComparisonSite, CompiledExprSet, StaticExprKind};
use arrow::{
    array::{ArrayRef, UInt64Array, new_empty_array, new_null_array},
    compute::{TakeOptions, take},
    datatypes::DataType,
};
use novarocks_expression_contract::{RootOrdinal, RootScope};
use novarocks_functions::{
    EvaluatedArgument, FunctionArgumentType, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, PreparedPureKernel, RowDataError, ScalarInvocationScope,
    ScopedExpressionEffects, SelectedValues, Selection, validate_evaluated_argument_observed,
    visit_selected_nulls,
};
use novarocks_type_contract::{
    ControlShape, ExpressionUseId, FunctionNullBehavior,
    arrow_fields_exact_with_nullability_observed,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

/// One operator/driver root owns its instances, even when definitions and
/// immutable preparation backing are shared with another root or driver.
pub struct CompiledExpressionInstance {
    program: Arc<CompiledExprSet>,
    root: RootOrdinal,
    /// The actual scope retained by this compiled unit. No graph or channel
    /// role is consulted by the evaluator, including for an empty scope.
    input: RootScope,
    instances: BTreeMap<ExpressionUseId, scalar_invocation::CallInstance>,
    json_constants: novarocks_functions::native_inlist::JsonConstantKeyCache,
    has_invocation_data: bool,
    effects: BTreeMap<ExpressionUseId, ScopedExpressionEffects>,
    failed: bool,
    #[cfg(test)]
    slot_source_observer: Option<Arc<input_source::SlotSourceObserver>>,
    allocator: Option<Arc<dyn novarocks_functions::AggregateStateAllocator>>,
}

fn invalid(message: &str) -> KernelFailure {
    KernelFailure::InvalidProgram(KernelDiagnostic::new(message))
}
fn internal(message: &str) -> KernelFailure {
    KernelFailure::Internal(KernelDiagnostic::new(message))
}

// Observe originating refusals even when nested helpers return a category
// other than the three interruption/resource variants. Never retry them.
struct ObservedControl<'a> {
    original: &'a dyn KernelEvaluationControl,
    refused: AtomicBool,
    // A real leaf Kernel Err is atomic even when it originated at a host
    // allocator rather than a cooperative control checkpoint.
    operation_aborted: AtomicBool,
}
impl KernelEvaluationControl for ObservedControl<'_> {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        let result = self.original.checkpoint(work);
        if result.is_err() {
            self.refused.store(true, Ordering::Relaxed);
        }
        result
    }
    fn wait(&self, duration: Duration) -> Result<(), KernelFailure> {
        let result = self.original.wait(duration);
        if result.is_err() {
            self.refused.store(true, Ordering::Relaxed);
        }
        result
    }
    fn regexp_search_progress(&self, offset: usize) -> Result<(), KernelFailure> {
        let result = self.original.regexp_search_progress(offset);
        if result.is_err() {
            self.refused.store(true, Ordering::Relaxed);
        }
        result
    }
    fn regexp_search_stopped(&self, offset: usize) {
        self.original.regexp_search_stopped(offset);
    }
}
struct Work<'a, 'scope, R = std::convert::Infallible> {
    control: &'a ObservedControl<'a>,
    pending: u32,
    scalar_host: Option<&'scope mut (dyn ScalarCallHost<Refusal = R> + 'scope)>,
}
impl<R> Work<'_, '_, R> {
    fn step(&mut self) -> Result<(), KernelFailure> {
        self.pending += 1;
        if self.pending == novarocks_functions::MAX_UNOBSERVED_KERNEL_WORK {
            self.flush()?;
        }
        Ok(())
    }
    fn flush(&mut self) -> Result<(), KernelFailure> {
        self.control.checkpoint(self.pending)?;
        self.pending = 0;
        Ok(())
    }
    fn finish<T>(&mut self, result: Result<T, KernelFailure>) -> Result<T, KernelFailure> {
        // Every originating failure is final for fallible completion work.
        // Resource settlement remains the responsibility of its actual scope.
        if result.is_err()
            || self.control.refused.load(Ordering::Relaxed)
            || self.control.operation_aborted.load(Ordering::Relaxed)
        {
            return result;
        }
        self.flush()?;
        result
    }
    fn finish_evaluation<T>(
        &mut self,
        result: Result<T, ExprEvaluationFailure<R>>,
    ) -> Result<T, ExprEvaluationFailure<R>> {
        // Preserve the first failure. A completion checkpoint can only
        // interrupt a successful frame.
        match result {
            Err(error) => Err(error),
            Ok(value) => self.finish(Ok(value)).map_err(Into::into),
        }
    }
}

// Preserve explicit constant/input shape through ordinary calls. A one-row
// column is never promoted to a scalar, and constants are not expanded just
// to call a kernel. Only a materialized root or narrowed computed child needs
// a gather.
enum Value<'a> {
    Constant(novarocks_functions::ConstantValue),
    ConstantView(constant_projection::CanonicalConstant),
    Column(ArrayRef),
    Selected(SelectedValues<'a>),
    Mapped(mapped::MappedValues),
    Continued(Box<Self>, input_source::ArrayContinuation),
}

// The array itself is wrapped before this value escapes. The explicit sidecar
// additionally transports its actual creator through later Arrow copy authors.
struct MaterializedValue<'a> {
    value: SelectedValues<'a>,
    continuation: Option<input_source::ArrayContinuation>,
}
impl<'a> MaterializedValue<'a> {
    fn into_parts(self) -> (Selection<'a>, ArrayRef, Box<[RowDataError]>) {
        self.value.into_parts()
    }
}
impl<'a> std::ops::Deref for MaterializedValue<'a> {
    type Target = SelectedValues<'a>;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}
impl Value<'_> {
    fn argument(&self) -> Result<EvaluatedArgument<'_>, KernelFailure> {
        Ok(match self {
            Self::Constant(value) => EvaluatedArgument::Constant(value),
            Self::ConstantView(value) => value.argument(),
            Self::Column(array) => EvaluatedArgument::Column(array),
            Self::Selected(value) => EvaluatedArgument::SelectedColumn(value),
            Self::Mapped(_) => {
                return Err(invalid(
                    "mapped expression value requires its selected-copy or broadcast author",
                ));
            }
            Self::Continued(value, _) => return value.argument(),
        })
    }
    fn data_type(&self) -> Result<&DataType, KernelFailure> {
        match self.uncontinued() {
            Self::Mapped(value) => Ok(value.data_type()),
            value => Ok(value.argument()?.array().data_type()),
        }
    }
    fn errors(&self) -> &[RowDataError] {
        match self {
            Self::Selected(value) => value.errors(),
            Self::Mapped(value) => value.errors(),
            Self::Continued(value, _) => value.errors(),
            _ => &[],
        }
    }
    fn continuation(&self) -> Option<input_source::ArrayContinuation> {
        match self {
            Self::Continued(_, owner) => Some(owner.clone()),
            _ => None,
        }
    }
    fn uncontinued(&self) -> &Self {
        match self {
            Self::Continued(value, _) => value.uncontinued(),
            _ => self,
        }
    }
    fn retaining_continuation(
        self,
        owner: input_source::ArrayContinuation,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<Self, KernelFailure> {
        let value = match self {
            Self::Constant(value) => Self::Constant(value),
            Self::ConstantView(value) => Self::ConstantView(value),
            Self::Column(array) => Self::Column(owner.retain(array, work.control)?),
            Self::Mapped(value) => Self::Mapped(value),
            Self::Selected(value) => {
                let (selection, array, errors) = value.into_parts();
                let array = owner.retain(array, work.control)?;
                let ty = array.data_type().clone();
                Self::Selected(SelectedValues::try_new_observed(
                    selection,
                    &ty,
                    array,
                    errors,
                    || work.step(),
                )?)
            }
            Self::Continued(value, _) => return value.retaining_continuation(owner, work),
        };
        Ok(Self::Continued(Box::new(value), owner))
    }
    fn materialize<'a>(
        self,
        selection: Selection<'a>,
        ty: &DataType,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<MaterializedValue<'a>, KernelFailure>
    where
        Self: 'a,
    {
        let value = match self {
            Self::Continued(value, owner) => {
                let output = value.materialize(selection, ty, work)?;
                let (_, array, errors) = output.into_parts();
                let array = owner.retain(array, work.control)?;
                return Ok(MaterializedValue {
                    value: materialized_source_values(selection, ty, array, errors, work)?,
                    continuation: Some(owner),
                });
            }
            value => value,
        };
        let (array, errors) = match value {
            Self::Selected(value) => {
                let (_, array, errors) = value.into_parts();
                (array, errors)
            }
            Self::Column(array) if selection.is_all() => (array, Box::default()),
            Self::Mapped(value) => {
                if value.len() != selection.len() {
                    return Err(invalid(
                        "mapped value differs from its actual selected domain",
                    ));
                }
                let array = value.materialize(0..selection.len(), work)?;
                (array, value.into_errors())
            }
            value => {
                let argument = value.argument()?;
                let mut indices = Vec::with_capacity(selection.len());
                for (ordinal, row) in selection.iter().enumerate() {
                    indices.push(Some(
                        u64::try_from(argument.value_row(ordinal, row))
                            .map_err(|_| KernelFailure::ResourceExhausted)?,
                    ));
                    work.step()?;
                }
                (gather(argument.array(), &indices, work)?, Box::default())
            }
        };
        Ok(MaterializedValue {
            value: materialized_source_values(selection, ty, array, errors, work)?,
            continuation: None,
        })
    }
}

// Source carriers may widen physical child NULL flags without changing their
// frozen logical binding. Materializing or gathering such a value keeps that
// carrier and its original custody; the final port's construction owner authors
// any required layout rebuild. Kernel result authors remain exact.
fn materialized_source_values<'a>(
    selection: Selection<'a>,
    expected: &DataType,
    array: ArrayRef,
    errors: Box<[RowDataError]>,
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    if !novarocks_type_contract::arrow_data_types_exact_with_profile_and_nullability_observed::<
        KernelFailure,
    >(
        expected,
        array.data_type(),
        novarocks_type_contract::ArrowTypeProfile::Physical,
        novarocks_type_contract::CarrierNullabilityPolicy::Permissive,
        || work.step(),
    )? {
        return Err(novarocks_functions::EvaluationContractError::ResultType.into());
    }
    let actual = array.data_type().clone();
    SelectedValues::try_new_observed(selection, &actual, array, errors, || work.step())
}

impl CompiledExpressionInstance {
    /// Validate the complete actual root before creating any mutable kernel
    /// instance. Static signatures are not reselected or guessed here.
    pub fn try_new(
        program: Arc<CompiledExprSet>,
        root: RootOrdinal,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        let observed = ObservedControl {
            original: control,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        observed.checkpoint(0)?;
        let mut work: Work<'_, '_> = Work {
            control: &observed,
            pending: 0,
            scalar_host: None,
        };
        let result = Self::prepare(program, root, &mut work);
        work.finish(result)
    }
    pub fn try_new_with_allocator(
        program: Arc<CompiledExprSet>,
        root: RootOrdinal,
        control: &dyn KernelEvaluationControl,
        allocator: Option<Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    ) -> Result<Self, KernelFailure> {
        let mut instance = Self::try_new(program, root, control)?;
        instance.allocator = allocator;
        Ok(instance)
    }
    fn prepare(
        program: Arc<CompiledExprSet>,
        root: RootOrdinal,
        work: &mut Work<'_, '_, impl Sized>,
    ) -> Result<Self, KernelFailure> {
        let compiled_root = program
            .root(root)
            .ok_or_else(|| invalid("missing actual compiled expression root"))?;
        let input = compiled_root.scope;
        program
            .scope(input)
            .ok_or_else(|| invalid("root input scope has no frozen layout"))?;
        let root_use = compiled_root.use_id;
        let flow = program.control_flow();
        let definitions = program.definitions();
        let mut stack = vec![(root_use, false)];
        let mut seen = BTreeSet::new();
        let mut effects = BTreeMap::<ExpressionUseId, ScopedExpressionEffects>::new();
        let mut has_invocation_data = false;
        while let Some((use_id, exiting)) = stack.pop() {
            work.step()?;
            let invocation = flow
                .uses()
                .get(&use_id)
                .ok_or_else(|| invalid("missing actual expression occurrence"))?;
            let occurrence = use_id;
            if exiting {
                let node = definitions
                    .node(invocation.definition)
                    .ok_or_else(|| invalid("missing actual expression definition"))?;
                let summary = match node.kind() {
                    StaticExprKind::BoundCall { .. } => {
                        let call = program
                            .calls()
                            .get(&occurrence)
                            .ok_or_else(|| invalid("missing actual prepared scalar occurrence"))?;
                        has_invocation_data |=
                            matches!(call.prepared(), PreparedPureKernel::ScalarInvocation(_));
                        let summary = *program
                            .effects()
                            .get(&occurrence)
                            .ok_or_else(|| invalid("missing prepared call effects"))?;
                        summary.for_use(invocation.context).map_err(|_| {
                            invalid("prepared call effects differ from actual occurrence")
                        })?;
                        summary
                    }
                    StaticExprKind::Constant(_)
                    | StaticExprKind::Input
                    | StaticExprKind::Lambda { .. }
                    | StaticExprKind::LambdaParameter { .. }
                    | StaticExprKind::NaryAnd { .. }
                    | StaticExprKind::NaryOr { .. }
                    | StaticExprKind::Not(_)
                    | StaticExprKind::IsNull(_)
                    | StaticExprKind::IsNotNull(_)
                    | StaticExprKind::Case { .. }
                    | StaticExprKind::PreparedCast { .. }
                    | StaticExprKind::PreparedBetween { .. }
                    | StaticExprKind::PreparedLike { .. }
                    | StaticExprKind::PreparedInList { .. }
                    | StaticExprKind::PreparedArithmetic { .. }
                    | StaticExprKind::PreparedNullSafeComparison { .. }
                    | StaticExprKind::Eq(..)
                    | StaticExprKind::Ne(..)
                    | StaticExprKind::Lt(..)
                    | StaticExprKind::Le(..)
                    | StaticExprKind::Gt(..)
                    | StaticExprKind::Ge(..) => {
                        let mut summary =
                            if matches!(node.kind(), StaticExprKind::PreparedArithmetic { .. }) {
                                program
                                    .recipes()
                                    .arithmetic(occurrence)
                                    .ok_or_else(|| {
                                        invalid("missing mandatory arithmetic effect recipe")
                                    })?
                                    .own_effects(invocation.context)
                            } else if matches!(node.kind(), StaticExprKind::PreparedLike { .. }) {
                                program
                                    .recipes()
                                    .native_like(occurrence)
                                    .ok_or_else(|| invalid("missing mandatory LIKE effect recipe"))?
                                    .own_effects(invocation.context)
                            } else if matches!(node.kind(), StaticExprKind::PreparedInList { .. }) {
                                program
                                    .recipes()
                                    .native_inlist(occurrence)
                                    .ok_or_else(|| invalid("missing mandatory IN effect recipe"))?
                                    .own_effects(invocation.context)
                            } else if let ControlShape::Case {
                                simple: true, arms, ..
                            } = invocation.control
                            {
                                let mut own =
                                    novarocks_type_contract::ExpressionEffects::PURE_VALUE;
                                for arm in 0..arms {
                                    work.step()?;
                                    let recipe = program
                                        .recipes()
                                        .case_comparison(occurrence, arm)
                                        .ok_or_else(|| {
                                            invalid("missing mandatory CASE WHEN effect recipe")
                                        })?;
                                    if let crate::primitives::PreparedNodeComparison::Composite(
                                        recipe,
                                    ) = recipe
                                    {
                                        own.may_raise_row_error |= recipe.may_raise_row_error();
                                    }
                                }
                                ScopedExpressionEffects::primitive(invocation.context, own)
                            } else if let StaticExprKind::PreparedBetween { plan, .. } = node.kind()
                            {
                                let lower = program
                                    .recipes()
                                    .comparison(ComparisonSite::BetweenLower(occurrence))
                                    .ok_or_else(|| {
                                        invalid("missing mandatory BETWEEN lower effect recipe")
                                    })?;
                                let upper = program
                                    .recipes()
                                    .comparison(ComparisonSite::BetweenUpper(occurrence))
                                    .ok_or_else(|| {
                                        invalid("missing mandatory BETWEEN upper effect recipe")
                                    })?;
                                if lower.operator() != plan.lower()
                                    || upper.operator() != plan.upper()
                                {
                                    return Err(invalid(
                                        "BETWEEN comparison effects differ from original expansion",
                                    ));
                                }
                                lower.own_effects(invocation.context)
                            } else if matches!(node.kind(), StaticExprKind::PreparedCast { .. }) {
                                program
                                    .recipes()
                                    .cast(occurrence)
                                    .ok_or_else(|| invalid("missing mandatory cast effect recipe"))?
                                    .own_effects(invocation.context)
                            } else if matches!(
                                node.kind(),
                                StaticExprKind::PreparedNullSafeComparison { .. }
                            ) {
                                program
                                    .recipes()
                                    .null_safe_comparison(occurrence)
                                    .ok_or_else(|| {
                                        invalid("missing mandatory null-safe effect recipe")
                                    })?
                                    .own_effects(invocation.context)
                            } else {
                                ScopedExpressionEffects::pure_value(invocation.context)
                            };
                        for (ordinal, child) in invocation.arguments.iter().enumerate() {
                            let child = *effects.get(child).ok_or_else(|| {
                                invalid("missing prepared ordered operand effects")
                            })?;
                            summary = summary
                                .join_control_argument(child, flow, ordinal)
                                .map_err(|_| {
                                    invalid("operand effects differ from actual ordered context")
                                })?;
                            work.step()?;
                        }
                        summary
                    }
                };
                effects.insert(occurrence, summary);
                continue;
            }
            if !seen.insert(use_id) {
                return Err(invalid("expression occurrence is shared or cyclic"));
            }
            let node = definitions
                .node(invocation.definition)
                .ok_or_else(|| invalid("missing actual expression definition"))?;
            admit_node_result(node.data_type())?;
            match node.kind() {
                StaticExprKind::Constant(_)
                | StaticExprKind::Input
                | StaticExprKind::LambdaParameter { .. }
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.is_empty() => {}
                StaticExprKind::Lambda { .. }
                    if invocation.control == ControlShape::LambdaBody
                        && invocation.arguments.len() == 1 => {}
                StaticExprKind::NaryAnd { .. } | StaticExprKind::NaryOr { .. }
                    if invocation.control
                        == (if matches!(node.kind(), StaticExprKind::NaryAnd { .. }) {
                            ControlShape::Conjunction
                        } else {
                            ControlShape::Disjunction
                        }) => {}
                StaticExprKind::Not(_)
                | StaticExprKind::IsNull(_)
                | StaticExprKind::IsNotNull(_)
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.len() == 1 => {}
                StaticExprKind::PreparedCast { .. }
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.len() == 1
                        && program.recipes().cast(occurrence).is_some() => {}
                StaticExprKind::PreparedInList {
                    values, is_not_in, ..
                } if invocation.control
                    == (ControlShape::Membership {
                        negated: *is_not_in,
                    })
                    && invocation.arguments.len() == values.len() + 1
                    && program.recipes().native_inlist(occurrence).is_some() => {}
                StaticExprKind::PreparedLike { .. }
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.len() == 2
                        && program.recipes().native_like(occurrence).is_some() => {}
                StaticExprKind::PreparedBetween { plan, .. }
                    if invocation.control
                        == (ControlShape::Between {
                            negated: plan.negated(),
                        })
                        && invocation.arguments.len() == 4
                        && program
                            .recipes()
                            .comparison(ComparisonSite::BetweenLower(occurrence))
                            .is_some()
                        && program
                            .recipes()
                            .comparison(ComparisonSite::BetweenUpper(occurrence))
                            .is_some() => {}
                StaticExprKind::PreparedArithmetic { .. }
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.len() == 2
                        && program.recipes().arithmetic(occurrence).is_some() => {}
                StaticExprKind::PreparedNullSafeComparison { .. }
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.len() == 2
                        && program.recipes().null_safe_comparison(occurrence).is_some() => {}
                kind if kind.ordinary_comparison().is_some()
                    && invocation.control == ControlShape::Eager
                    && invocation.arguments.len() == 2
                    && program
                        .recipes()
                        .comparison(ComparisonSite::Binary(occurrence))
                        .is_some() => {}
                StaticExprKind::Case { .. }
                    if matches!(invocation.control, ControlShape::Case { .. })
                        && guarded::supports_result(node.data_type()) => {}
                StaticExprKind::BoundCall { .. } => {
                    let call = program
                        .calls()
                        .get(&occurrence)
                        .ok_or_else(|| invalid("missing actual prepared scalar occurrence"))?;
                    admit_call_protocol(invocation.control, call.prepared(), node.data_type())?;
                }
                _ => {
                    return Err(invalid(
                        "expression requires its dedicated compiled protocol",
                    ));
                }
            }
            stack.push((use_id, true));
            for &child in invocation.arguments.iter().rev() {
                stack.push((child, false));
                work.step()?;
            }
        }
        Ok(Self {
            program,
            root,
            input,
            instances: BTreeMap::new(),
            json_constants: Default::default(),
            has_invocation_data,
            effects,
            failed: false,
            #[cfg(test)]
            slot_source_observer: None,
            allocator: None,
        })
    }
    /// Evaluate a checked root in the actual domain authored by its caller.
    /// Selected output carries only the independent demand lifetime.
    pub fn evaluate<'a, R>(
        &mut self,
        input: EvaluationInput<'_>,
        invocation: ScalarInvocationScope<'a>,
        activation: novarocks_functions::ScalarInvocationActivation,
        control: &dyn KernelEvaluationControl,
        host: &mut dyn ScalarCallHost<Refusal = R>,
    ) -> Result<SelectedValues<'a>, ExprEvaluationFailure<R>> {
        self.evaluate_input_controlled(
            FrameInput::new(input),
            invocation.actual_domain(),
            invocation.demand(),
            activation,
            control,
            host,
        )
    }
    #[cfg(test)]
    pub(crate) fn observe_slot_source(&mut self, observer: Arc<input_source::SlotSourceObserver>) {
        self.slot_source_observer = Some(observer);
    }
    fn evaluate_input_controlled<'a, R>(
        &mut self,
        input: FrameInput<'_>,
        actual_domain: Selection<'a>,
        selection: Selection<'a>,
        activation: novarocks_functions::ScalarInvocationActivation,
        control: &dyn KernelEvaluationControl,
        host: &mut dyn ScalarCallHost<Refusal = R>,
    ) -> Result<SelectedValues<'a>, ExprEvaluationFailure<R>> {
        use novarocks_functions::ScalarInvocationActivation;
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        if activation == ScalarInvocationActivation::ValidateOnly && !selection.is_empty() {
            self.failed = true;
            return Err(invalid("nonempty root demand cannot be validation-only").into());
        }
        #[cfg(test)]
        let source_observer = self.slot_source_observer.clone();
        #[cfg(test)]
        let input = if let Some(observer) = source_observer.as_deref() {
            input.observed(observer)
        } else {
            input
        };
        let observed = ObservedControl {
            original: control,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        let mut work = Work {
            control: &observed,
            pending: 0,
            scalar_host: Some(host),
        };
        let result = (|| {
            observed.checkpoint(0)?;
            self.evaluate_once::<R>(input, actual_domain, selection, activation, &mut work)
        })();
        // A positively installed whole-invocation root preserves every first
        // failure, including entry validation before any leaf is created.
        let result = if self.has_invocation_data && result.is_err() {
            result
        } else {
            work.finish_evaluation(result)
        };
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn evaluate_once<'a, R>(
        &mut self,
        input: FrameInput<'_>,
        actual_domain: Selection<'a>,
        selection: Selection<'a>,
        activation: novarocks_functions::ScalarInvocationActivation,
        work: &mut Work<'_, '_, R>,
    ) -> Result<SelectedValues<'a>, ExprEvaluationFailure<R>> {
        if input.num_rows() != selection.batch_rows() {
            return Err(invalid("selection differs from actual input batch rows").into());
        }
        let layout = self
            .program
            .scope(self.input)
            .ok_or_else(|| invalid("root input scope has no frozen layout"))?;
        if self.program.scopes().is_empty() {
            // Preserve the explicit empty one-row input contract.
            let schema = input.schema();
            work.flush()?;
            let empty =
                schema.fields().is_empty() && schema.metadata().is_empty() && input.num_rows() == 1;
            work.flush()?;
            if !empty {
                return Err(
                    invalid("actual input differs from the root's empty one-row port").into(),
                );
            }
            return self.evaluate_root::<R>(input, actual_domain, selection, activation, work);
        }
        work.flush()?;
        let schema = input.schema();
        let frozen = layout.schema();
        // The original exact Field author checks all names, metadata, encoding
        // attributes and parameters; only physical NULL flags may widen. Active
        // logical requiredness is checked below against the frozen channel FVT.
        let same_metadata = schema.metadata() == frozen.metadata();
        work.flush()?;
        if !same_metadata || schema.fields().len() != frozen.fields().len() {
            return Err(invalid("actual input fields differ from the frozen root port").into());
        }
        for (actual, expected) in schema.fields().iter().zip(frozen.fields()) {
            if !arrow_fields_exact_with_nullability_observed(
                expected,
                actual,
                novarocks_type_contract::CarrierNullabilityPolicy::Permissive,
                || work.step(),
            )? {
                return Err(invalid("actual input fields differ from the frozen root port").into());
            }
        }
        if input.is_projected() {
            for ordinal in self.required_input_columns() {
                work.step()?;
                if !input.has_materialized_column(ordinal) {
                    return Err(
                        invalid("projected root input omits a required frozen column").into(),
                    );
                }
            }
        }
        // Whole inputs still validate every column. An explicit projected port
        // validates every materialized column at its original frozen ordinal;
        // its complete logical declaration was checked above. Missing Slot
        // materialization remains an error, including under empty selection.
        for (ordinal, array) in input.columns_with_ordinals() {
            let ty = layout
                .ordinal_type(ordinal)
                .ok_or_else(|| invalid("missing exact incoming scope type"))?;
            work.flush()?;
            validate_evaluated_argument_observed(
                EvaluatedArgument::Column(array),
                selection,
                ty,
                work.control,
            )?;
            work.step()?;
        }
        self.evaluate_root::<R>(input, actual_domain, selection, activation, work)
    }
    /// Original checked lexical Slot facts for this actual root. Repeated
    /// occurrences may name one ordinal; consumers deduplicate coordinates.
    /// This exposes no evaluator, value algorithm or resource authority.
    pub fn required_input_columns(&self) -> impl Iterator<Item = usize> + '_ {
        self.effects.keys().filter_map(|occurrence| {
            self.program
                .input(*occurrence)
                .filter(|source| source.scope == self.input)
                .map(|source| source.ordinal as usize)
        })
    }
    fn evaluate_root<'a, R>(
        &mut self,
        input: FrameInput<'_>,
        actual_domain: Selection<'a>,
        selection: Selection<'a>,
        activation: novarocks_functions::ScalarInvocationActivation,
        work: &mut Work<'_, '_, R>,
    ) -> Result<SelectedValues<'a>, ExprEvaluationFailure<R>> {
        let result = guarded::evaluate_tree::<R>(
            &self.program,
            self.root,
            input,
            self.input,
            actual_domain,
            selection,
            activation,
            &mut self.instances,
            &mut self.json_constants,
            self.allocator.as_ref(),
            work,
        )?;
        let root = self
            .program
            .root(self.root)
            .ok_or_else(|| invalid("missing actual compiled expression root"))?;
        let FunctionArgumentType::Value(ty) = self
            .program
            .types()
            .get(root.definition.index())
            .ok_or_else(|| invalid("missing actual root value type"))?
        else {
            return Err(invalid("actual root is not a value").into());
        };
        finish_root_value(result, ty, selection, self.allocator.as_ref(), work).map_err(Into::into)
    }
}

fn finish_root_value<'a>(
    result: Value<'a>,
    ty: &novarocks_functions::FunctionValueType,
    selection: Selection<'a>,
    allocator: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<SelectedValues<'a>, KernelFailure> {
    let result = result.materialize(selection, ty.data_type(), work)?;
    let (_, array, errors) = result.into_parts();
    let array = input_source::retain_result_type(array, ty, allocator, work.control)?;
    let result = materialized_source_values(selection, ty.data_type(), array, errors, work)?;
    if !ty.nullable() {
        let mut errors = result.errors().iter().peekable();
        work.flush()?;
        visit_selected_nulls(
            EvaluatedArgument::SelectedColumn(&result),
            selection,
            work.control,
            |ordinal, _, is_null| {
                if errors
                    .peek()
                    .is_some_and(|error| error.selected_ordinal() == ordinal)
                {
                    errors.next();
                } else if is_null {
                    return Err(internal(
                        "non-null compiled root returned a successful SQL NULL",
                    ));
                }
                Ok(())
            },
        )?;
    }
    Ok(result)
}

fn evaluate_scalar<'a, R, K: Ord + Clone>(
    occurrence: K,
    prepared: scalar_invocation::PreparedCall<'_>,
    children: &[Value<'a>],
    selection: Selection<'a>,
    activation: novarocks_functions::ScalarInvocationActivation,
    parent_row: Option<scalar_groups::ParentRow<'_>>,
    instances: &mut BTreeMap<K, scalar_invocation::CallInstance>,
    allocator: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    work: &mut Work<'_, '_, R>,
) -> Result<SelectedValues<'a>, ExprEvaluationFailure<R>> {
    let mut blocked = Vec::with_capacity(selection.len());
    for _ in 0..selection.len() {
        blocked.push(false);
        work.step()?;
    }
    let mut inherited = BTreeMap::<usize, RowDataError>::new();
    let effects = prepared.contract().effects();
    // Ordered arguments arrive on the leaf's already narrowed selection.
    let arguments_already_screened =
        effects.argument_control == novarocks_type_contract::ArgumentControl::OrderedNull;
    for (position, child) in children.iter().enumerate() {
        for error in child.errors() {
            let ordinal = error.selected_ordinal();
            blocked[ordinal] = true;
            // Ordered required children give stable first-error precedence.
            inherited.entry(ordinal).or_insert_with(|| error.clone());
            work.step()?;
        }
        if !arguments_already_screened && effects.null_behavior.rejects_null_at(position) {
            if let Value::Mapped(value) = child.uncontinued() {
                for (ordinal, blocked) in blocked.iter_mut().enumerate() {
                    *blocked |= value.is_null(ordinal, work)?;
                }
                continue;
            }
            work.flush()?;
            visit_selected_nulls(
                child.argument()?,
                selection,
                work.control,
                |ordinal, _, is_null| {
                    if is_null {
                        blocked[ordinal] = true;
                    }
                    Ok(())
                },
            )?;
        }
    }
    let mut active_rows = Vec::new();
    let mut active_ordinals = Vec::new();
    for (ordinal, row) in selection.iter().enumerate() {
        if !blocked[ordinal] {
            active_rows.push(row);
            active_ordinals.push(ordinal);
        }
        work.step()?;
    }
    if active_rows.is_empty()
        && !(prepared.is_invocation()
            && selection.is_empty()
            && activation == novarocks_functions::ScalarInvocationActivation::Activated)
    {
        work.flush()?;
        let values = new_null_array(
            prepared.contract().result_type().data_type(),
            selection.len(),
        );
        work.flush()?;
        let mut errors = Vec::with_capacity(inherited.len());
        for error in inherited.into_values() {
            errors.push(error);
            work.step()?;
        }
        return SelectedValues::try_new_observed(
            selection,
            prepared.contract().result_type().data_type(),
            values,
            errors.into_boxed_slice(),
            || work.step(),
        )
        .map_err(ExprEvaluationFailure::Kernel);
    }
    let full_call = active_rows.len() == selection.len();
    let call_selection = if full_call {
        selection
    } else {
        Selection::try_sparse_observed(selection.batch_rows(), &active_rows, || work.step())?
    };
    let groups = scalar_groups::Groups::plan(
        prepared,
        children,
        &active_ordinals,
        selection,
        parent_row,
        allocator,
        work,
    )?;
    let mut narrowed = Vec::new();
    if groups.is_none() {
        let mut indices = Vec::with_capacity(active_ordinals.len());
        if !full_call {
            for &ordinal in &active_ordinals {
                indices.push(Some(
                    u64::try_from(ordinal).map_err(|_| KernelFailure::ResourceExhausted)?,
                ));
                work.step()?;
            }
        }
        for child in children {
            let output = match child.uncontinued() {
                Value::Mapped(value) => {
                    Some(value.materialize(active_ordinals.iter().copied(), work)?)
                }
                Value::Selected(child_values) if !full_call => Some(gather_continued(
                    child_values.values(),
                    &indices,
                    child.continuation().as_ref(),
                    work,
                )?),
                _ => None,
            };
            let output = if let Some(array) = output {
                Some(SelectedValues::try_new_observed::<KernelFailure>(
                    call_selection,
                    array.data_type(),
                    Arc::clone(&array),
                    Box::default(),
                    || work.step(),
                )?)
            } else {
                None
            };
            narrowed.push(output);
            work.step()?;
        }
    }
    let mut arguments = Vec::with_capacity(children.len());
    if groups.is_none() {
        for (index, child) in children.iter().enumerate() {
            arguments.push(if let Some(output) = &narrowed[index] {
                EvaluatedArgument::SelectedColumn(output)
            } else {
                child.argument()?
            });
            work.step()?;
        }
    }
    work.flush()?;
    if let std::collections::btree_map::Entry::Vacant(entry) = instances.entry(occurrence.clone()) {
        // The actual host must authorize this instance's immutable lifetime
        // bound before entry. Representability/postchecks are not that grant.
        let instance = match prepared.instantiate(allocator.cloned()) {
            Ok(instance) => instance,
            Err(cause) => {
                work.control
                    .operation_aborted
                    .store(true, Ordering::Relaxed);
                return Err(cause.into());
            }
        };
        work.control.checkpoint(0)?;
        entry.insert(instance);
    }
    let instance = instances
        .get_mut(&occurrence)
        .ok_or_else(|| internal("scalar instance was not installed"))?;
    let array = if let Some(groups) = groups {
        groups
            .evaluate(
                instance,
                prepared,
                children,
                selection,
                activation,
                &mut inherited,
                allocator,
                work,
            )
            .inspect_err(|_| {
                work.control
                    .operation_aborted
                    .store(true, Ordering::Relaxed)
            })?
    } else {
        let output = scalar_invocation::invoke(
            instance,
            call_selection,
            &arguments,
            activation,
            allocator,
            work,
        )
        .inspect_err(|_| {
            work.control
                .operation_aborted
                .store(true, Ordering::Relaxed)
        })?;
        let (_, array, own_errors) = output.into_parts();
        for error in own_errors {
            let original = active_ordinals[error.selected_ordinal()];
            inherited.insert(original, error.with_selected_ordinal(original));
            work.step()?;
        }
        if full_call {
            array
        } else {
            let mut indices = Vec::with_capacity(selection.len());
            for _ in 0..selection.len() {
                indices.push(None);
                work.step()?;
            }
            for (call_ordinal, &original) in active_ordinals.iter().enumerate() {
                indices[original] = Some(
                    u64::try_from(call_ordinal).map_err(|_| KernelFailure::ResourceExhausted)?,
                );
                work.step()?;
            }
            gather(&array, &indices, work)?
        }
    };
    let mut errors = Vec::with_capacity(inherited.len());
    for error in inherited.into_values() {
        errors.push(error);
        work.step()?;
    }
    work.flush()?;
    SelectedValues::try_new_observed(
        selection,
        prepared.contract().result_type().data_type(),
        array,
        errors.into_boxed_slice(),
        || work.step(),
    )
    .map_err(ExprEvaluationFailure::Kernel)
}

fn gather_continued(
    array: &ArrayRef,
    indices: &[Option<u64>],
    continuation: Option<&input_source::ArrayContinuation>,
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<ArrayRef, KernelFailure> {
    let output = gather(array, indices, work)?;
    match continuation {
        Some(owner) => owner.retain(output, work.control),
        None => Ok(output),
    }
}

fn gather(
    array: &ArrayRef,
    indices: &[Option<u64>],
    work: &mut Work<'_, '_, impl Sized>,
) -> Result<ArrayRef, KernelFailure> {
    work.flush()?;
    if indices.is_empty() {
        let output = new_empty_array(array.data_type());
        work.flush()?;
        return Ok(output);
    }
    novarocks_functions::selected_copy::preflight_take(array.as_ref(), indices, |boundary| {
        if boundary { work.flush() } else { work.step() }
    })
    .map_err(novarocks_functions::selected_copy::CopyError::into_kernel)?;
    work.flush()?;
    let indices = UInt64Array::from_iter(indices.iter().copied());
    work.flush()?;
    let result = take(
        array.as_ref(),
        &indices,
        Some(TakeOptions { check_bounds: true }),
    )
    .map_err(|_| internal("checked compiled selection could not be gathered"));
    work.flush()?;
    result
}

fn admit_node_result(ty: &DataType) -> Result<(), KernelFailure> {
    // Error-placeholder NULLs require an outer validity carrier.
    if matches!(ty, DataType::Union(..) | DataType::RunEndEncoded(..)) {
        return Err(invalid(
            "encoded root result requires its dedicated error carrier",
        ));
    }
    Ok(())
}
fn admit_call_protocol(
    shape: ControlShape,
    call: &PreparedPureKernel,
    result: &DataType,
) -> Result<(), KernelFailure> {
    let supported = match (shape, call) {
        (
            ControlShape::Eager | ControlShape::TypeOnly | ControlShape::NoArguments,
            PreparedPureKernel::Scalar(_) | PreparedPureKernel::ScalarInvocation(_),
        ) => call.call_contract().effects().null_behavior != FunctionNullBehavior::ControlDefined,
        (
            ControlShape::If
            | ControlShape::Coalesce
            | ControlShape::Try
            | ControlShape::TemporalSource(_),
            PreparedPureKernel::ControlIntrinsic(_),
        ) => {
            call.call_contract().effects().null_behavior == FunctionNullBehavior::ControlDefined
                && guarded::supports_result(result)
        }
        (ControlShape::OrderedNull, PreparedPureKernel::Scalar(_)) => {
            call.call_contract().effects().null_behavior != FunctionNullBehavior::ControlDefined
        }
        (ControlShape::OrderedRegexp, PreparedPureKernel::OrderedRegexp(_))
        | (ControlShape::NullIf, PreparedPureKernel::NullIf(_)) => {
            call.call_contract().effects().null_behavior == FunctionNullBehavior::ControlDefined
                && guarded::supports_result(result)
        }
        (
            ControlShape::HigherOrder {
                body_ordinal,
                body_demand,
            },
            PreparedPureKernel::HigherOrder(prepared),
        ) => {
            prepared.contract().body_ordinal() == body_ordinal as usize
                && prepared.contract().body_demand() == body_demand
                && call.call_contract().effects().null_behavior
                    == FunctionNullBehavior::ControlDefined
        }
        _ => false,
    };
    if supported {
        Ok(())
    } else {
        Err(invalid(
            "expression requires its dedicated control or carrier protocol",
        ))
    }
}

mod failure;
mod host;
mod input;

pub use failure::{ExprEvaluationFailure, RequiredExpressionRowError, ScalarCallFailure};
pub use host::{ScalarCallHost, ScalarCallPhase, ScalarCallRequestFacts};
pub use input::{EvaluationInput, InputColumnProjection, InputSourceStorage, InputStorageFailure};

mod boolean_region;
mod constant_projection;
mod filter_conjunction;
mod guarded;
mod input_source;
mod mapped;
mod node_recipes;
mod scalar_groups;
pub(crate) mod scalar_invocation;
mod unary;

pub use filter_conjunction::CompiledFilterConjunctionInstance;

#[cfg(test)]
mod lambda_observation;
#[cfg(test)]
mod tests;

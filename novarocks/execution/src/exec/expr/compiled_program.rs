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
//! Evaluation of actual checked root occurrences, without thawing ExprArena.
//!
//! This selected expression controller keeps state per root use and driver. The
//! host must install its memory scopes and allocation admission before calling
//! it, as required by the neutral kernel ABI. These controls do not mint a
//! memory grant. Higher-order and encoded guarded-result protocols remain separate.

use arrow::{
    array::{ArrayRef, UInt64Array, new_empty_array, new_null_array},
    compute::{TakeOptions, take},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use novarocks_functions::{
    EvaluatedArgument, FunctionArgumentType, KernelDiagnostic, KernelEvaluationControl,
    KernelFailure, PreparedPureKernel, RowDataError, ScalarEvaluationInstance,
    ScopedExpressionEffects, SelectedValues, Selection, validate_evaluated_argument_observed,
    visit_selected_nulls,
};
use novarocks_local_program::{
    LocalProgram, ProgramCallSite, ProgramChannelLayoutRole, ProgramChannelSite,
    ProgramExpressionRootSite, ProgramLexicalSource, ProgramNodeId, ProgramRootInput,
    ProgramUseRef, StaticExprKind, root_input_layout,
};
use novarocks_type_contract::{ControlShape, FunctionNullBehavior, arrow_fields_exact_observed};
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
    program: Arc<LocalProgram>,
    root: ProgramExpressionRootSite,
    /// The one input layout this root reads, named by its node and layout
    /// role, from the same authority the lexical bindings were checked
    /// against; `None` is the explicit empty port of a root that reads no
    /// input. A join key reads its own side's layout and a join residual the
    /// join scope; every other root reads a node output.
    input: Option<(ProgramNodeId, ProgramChannelLayoutRole)>,
    instances: BTreeMap<ProgramUseRef, scalar_invocation::CallInstance>,
    has_invocation_data: bool,
    effects: BTreeMap<ProgramUseRef, ScopedExpressionEffects>,
    failed: bool,
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
}
struct Work<'a, 'scope> {
    control: &'a ObservedControl<'a>,
    pending: u32,
    scalar_scope: Option<
        &'scope mut (dyn crate::runtime::scalar_memory::RuntimeScalarOperationScope + 'scope),
    >,
}
impl Work<'_, '_> {
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
        if self.control.refused.load(Ordering::Relaxed)
            || self.control.operation_aborted.load(Ordering::Relaxed)
            || matches!(
                &result,
                Err(KernelFailure::Cancelled
                    | KernelFailure::DeadlineExceeded
                    | KernelFailure::ResourceExhausted)
            )
        {
            return result;
        }
        self.flush()?;
        result
    }
}

// Preserve explicit constant/input shape through ordinary calls. A one-row
// column is never promoted to a scalar, and constants are not expanded just
// to call a kernel. Only a materialized root or narrowed computed child needs
// a gather.
enum Value<'a> {
    Constant(novarocks_functions::ConstantValue),
    Column(ArrayRef),
    Selected(SelectedValues<'a>),
}
impl Value<'_> {
    fn argument(&self) -> EvaluatedArgument<'_> {
        match self {
            Self::Constant(value) => EvaluatedArgument::Constant(value),
            Self::Column(array) => EvaluatedArgument::Column(array),
            Self::Selected(value) => EvaluatedArgument::SelectedColumn(value),
        }
    }
    fn errors(&self) -> &[RowDataError] {
        match self {
            Self::Selected(value) => value.errors(),
            _ => &[],
        }
    }
    fn materialize<'a>(
        self,
        selection: Selection<'a>,
        ty: &DataType,
        work: &mut Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, KernelFailure>
    where
        Self: 'a,
    {
        let (array, errors) = match self {
            Self::Selected(value) => {
                let (_, array, errors) = value.into_parts();
                (array, errors)
            }
            Self::Column(array) if selection.is_all() => (array, Box::default()),
            value => {
                let argument = value.argument();
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
        SelectedValues::try_new_observed(selection, ty, array, errors, || work.step())
    }
}

impl CompiledExpressionInstance {
    /// Validate the complete actual root before creating any mutable kernel
    /// instance. Static signatures are not reselected or guessed here.
    pub fn try_new(
        program: Arc<LocalProgram>,
        root: ProgramExpressionRootSite,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self, KernelFailure> {
        let observed = ObservedControl {
            original: control,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        observed.checkpoint(0)?;
        let mut work = Work {
            control: &observed,
            pending: 0,
            scalar_scope: None,
        };
        let result = Self::prepare(program, root, &mut work);
        work.finish(result)
    }
    pub fn try_new_with_allocator(
        program: Arc<LocalProgram>,
        root: ProgramExpressionRootSite,
        control: &dyn KernelEvaluationControl,
        allocator: Option<Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    ) -> Result<Self, KernelFailure> {
        let mut instance = Self::try_new(program, root, control)?;
        instance.allocator = allocator;
        Ok(instance)
    }
    fn prepare(
        program: Arc<LocalProgram>,
        root: ProgramExpressionRootSite,
        work: &mut Work<'_, '_>,
    ) -> Result<Self, KernelFailure> {
        let checked = program.checked();
        let snapshot = checked.channels().expressions().resolved_calls().snapshot();
        let input = match root_input_layout(program.graph(), root)
            .map_err(|_| invalid("root has no actual compiled input port"))?
        {
            ProgramRootInput::Layout { node, role } => {
                // The frozen port must exist before any instance is created.
                checked
                    .channels()
                    .channel_layout(node, role)
                    .ok_or_else(|| invalid("root input port has no frozen layout"))?;
                Some((node, role))
            }
            ProgramRootInput::Empty => None,
        };
        let root_use = *snapshot
            .bindings()
            .get(&root)
            .ok_or_else(|| invalid("missing actual compiled expression root"))?;
        let arena = root.arena();
        let flow = snapshot
            .flows()
            .get(&arena)
            .ok_or_else(|| invalid("missing actual root control arena"))?;
        let definitions = snapshot
            .roots()
            .arenas()
            .get(&arena)
            .ok_or_else(|| invalid("missing actual root definitions"))?;
        let mut stack = vec![(root_use, false)];
        let mut seen = BTreeSet::new();
        let mut effects = BTreeMap::<ProgramUseRef, ScopedExpressionEffects>::new();
        let mut has_invocation_data = false;
        while let Some((use_id, exiting)) = stack.pop() {
            work.step()?;
            let invocation = flow
                .uses()
                .get(&use_id)
                .ok_or_else(|| invalid("missing actual expression occurrence"))?;
            let occurrence = ProgramUseRef { arena, use_id };
            if exiting {
                let node = definitions
                    .node(invocation.definition)
                    .ok_or_else(|| invalid("missing actual expression definition"))?;
                let summary = match node.kind() {
                    StaticExprKind::BoundCall { .. } => {
                        let call = &checked.channels().expressions().resolved_calls().calls()
                            [&ProgramCallSite::Expression(occurrence)];
                        has_invocation_data |= matches!(
                            call.specialization().prepared(),
                            PreparedPureKernel::ScalarInvocation(_)
                        );
                        let summary = call.effects();
                        summary.for_use(invocation.context).map_err(|_| {
                            invalid("prepared call effects differ from actual occurrence")
                        })?;
                        summary
                    }
                    StaticExprKind::Constant(_)
                    | StaticExprKind::SlotId(_)
                    | StaticExprKind::NaryAnd { .. }
                    | StaticExprKind::NaryOr { .. }
                    | StaticExprKind::Not(_)
                    | StaticExprKind::IsNull(_)
                    | StaticExprKind::IsNotNull(_)
                    | StaticExprKind::Case { .. }
                    | StaticExprKind::PreparedCast { .. }
                    | StaticExprKind::PreparedNativeNegate(_)
                    | StaticExprKind::PreparedNativeBitNot(_)
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
                                    .arithmetic_recipe(occurrence)
                                    .ok_or_else(|| {
                                        invalid("missing mandatory arithmetic effect recipe")
                                    })?
                                    .own_effects(invocation.context)
                            } else if matches!(node.kind(), StaticExprKind::PreparedNativeNegate(_))
                            {
                                program
                                    .native_negate_recipe(occurrence)
                                    .ok_or_else(|| {
                                        invalid("missing mandatory native negate effect recipe")
                                    })?
                                    .own_effects(invocation.context)
                            } else if matches!(node.kind(), StaticExprKind::PreparedNativeBitNot(_))
                            {
                                program
                                    .native_bitnot_recipe(occurrence)
                                    .ok_or_else(|| {
                                        invalid("missing mandatory native BitwiseNot effect recipe")
                                    })?
                                    .own_effects(invocation.context)
                            } else if matches!(node.kind(), StaticExprKind::PreparedLike { .. }) {
                                program
                                    .native_like_recipe(occurrence)
                                    .ok_or_else(|| invalid("missing mandatory LIKE effect recipe"))?
                                    .own_effects(invocation.context)
                            } else if matches!(node.kind(), StaticExprKind::PreparedInList { .. }) {
                                program
                                    .native_inlist_recipe(occurrence)
                                    .ok_or_else(|| invalid("missing mandatory IN effect recipe"))?
                                    .own_effects(invocation.context)
                            } else if let StaticExprKind::PreparedBetween { plan, .. } = node.kind()
                            {
                                let lower = program
                                .comparison_recipe(
                                    novarocks_local_program::ProgramComparisonSite::BetweenLower(
                                        occurrence,
                                    ),
                                )
                                .ok_or_else(|| {
                                    invalid("missing mandatory BETWEEN lower effect recipe")
                                })?;
                                let upper = program
                                .comparison_recipe(
                                    novarocks_local_program::ProgramComparisonSite::BetweenUpper(
                                        occurrence,
                                    ),
                                )
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
                                    .cast_recipe(occurrence)
                                    .ok_or_else(|| invalid("missing mandatory cast effect recipe"))?
                                    .own_effects(invocation.context)
                            } else if matches!(
                                node.kind(),
                                StaticExprKind::PreparedNullSafeComparison { .. }
                            ) {
                                program
                                    .null_safe_comparison_recipe(occurrence)
                                    .ok_or_else(|| {
                                        invalid("missing mandatory null-safe effect recipe")
                                    })?
                                    .own_effects(invocation.context)
                            } else {
                                ScopedExpressionEffects::pure_value(invocation.context)
                            };
                        for (ordinal, child) in invocation.arguments.iter().enumerate() {
                            let child = *effects
                                .get(&ProgramUseRef {
                                    arena,
                                    use_id: *child,
                                })
                                .ok_or_else(|| {
                                    invalid("missing prepared ordered operand effects")
                                })?;
                            summary = summary
                                .join_control_argument(child, flow.shared_flow(), ordinal)
                                .map_err(|_| {
                                    invalid("operand effects differ from actual ordered context")
                                })?;
                            work.step()?;
                        }
                        summary
                    }
                    _ => return Err(invalid("expression has no admitted own-effect author")),
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
            // Error-placeholder NULLs require an outer validity carrier. These
            // encoded result protocols need their dedicated representation;
            // never fabricate error bitmaps or change a frozen result type.
            if matches!(
                node.data_type(),
                DataType::Union(..) | DataType::RunEndEncoded(..)
            ) {
                return Err(invalid(
                    "encoded root result requires its dedicated error carrier",
                ));
            }
            match node.kind() {
                StaticExprKind::Constant(_) | StaticExprKind::SlotId(_)
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.is_empty() => {}
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
                        && program.cast_recipe(occurrence).is_some() => {}
                StaticExprKind::PreparedNativeNegate(_)
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.len() == 1
                        && program.native_negate_recipe(occurrence).is_some() => {}
                StaticExprKind::PreparedNativeBitNot(_)
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.len() == 1
                        && program.native_bitnot_recipe(occurrence).is_some() => {}
                StaticExprKind::PreparedInList {
                    values, is_not_in, ..
                } if invocation.control
                    == (ControlShape::Membership {
                        negated: *is_not_in,
                    })
                    && invocation.arguments.len() == values.len() + 1
                    && program.native_inlist_recipe(occurrence).is_some() => {}
                StaticExprKind::PreparedLike { .. }
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.len() == 2
                        && program.native_like_recipe(occurrence).is_some() => {}
                StaticExprKind::PreparedBetween { plan, .. }
                    if invocation.control
                        == (ControlShape::Between {
                            negated: plan.negated(),
                        })
                        && invocation.arguments.len() == 4
                        && program
                            .comparison_recipe(
                                novarocks_local_program::ProgramComparisonSite::BetweenLower(
                                    occurrence,
                                ),
                            )
                            .is_some()
                        && program
                            .comparison_recipe(
                                novarocks_local_program::ProgramComparisonSite::BetweenUpper(
                                    occurrence,
                                ),
                            )
                            .is_some() => {}
                StaticExprKind::PreparedArithmetic { .. }
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.len() == 2
                        && program.arithmetic_recipe(occurrence).is_some() => {}
                StaticExprKind::PreparedNullSafeComparison { .. }
                    if invocation.control == ControlShape::Eager
                        && invocation.arguments.len() == 2
                        && program.null_safe_comparison_recipe(occurrence).is_some() => {}
                kind if kind.ordinary_comparison().is_some()
                    && invocation.control == ControlShape::Eager
                    && invocation.arguments.len() == 2
                    && program
                        .comparison_recipe(novarocks_local_program::ProgramComparisonSite::Binary(
                            occurrence,
                        ))
                        .is_some() => {}
                StaticExprKind::Case { .. }
                    if matches!(invocation.control, ControlShape::Case { .. })
                        && guarded::supports_result(node.data_type()) => {}
                StaticExprKind::BoundCall { .. } => {
                    let call = checked
                        .channels()
                        .expressions()
                        .resolved_calls()
                        .calls()
                        .get(&ProgramCallSite::Expression(occurrence))
                        .ok_or_else(|| invalid("missing actual prepared scalar occurrence"))?;
                    let supported = match (invocation.control, call.specialization().prepared()) {
                        (
                            ControlShape::Eager
                            | ControlShape::TypeOnly
                            | ControlShape::NoArguments,
                            PreparedPureKernel::Scalar(_) | PreparedPureKernel::ScalarInvocation(_),
                        ) => {
                            call.call_contract().effects().null_behavior
                                != FunctionNullBehavior::ControlDefined
                        }
                        (
                            ControlShape::If
                            | ControlShape::Coalesce
                            | ControlShape::TemporalSource(_),
                            PreparedPureKernel::ControlIntrinsic(_),
                        ) => {
                            call.call_contract().effects().null_behavior
                                == FunctionNullBehavior::ControlDefined
                                && guarded::supports_result(node.data_type())
                        }
                        _ => false,
                    };
                    if !supported {
                        return Err(invalid(
                            "expression requires its dedicated control or carrier protocol",
                        ));
                    }
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
            has_invocation_data,
            effects,
            failed: false,
            allocator: None,
        })
    }
    /// The caller supplies this root's actual input port, retaining the full
    /// frozen field order and metadata. Selection always names its batch rows.
    /// An empty-port root is evaluated over exactly one row with no column.
    pub fn evaluate<'a>(
        &mut self,
        input: &RecordBatch,
        selection: Selection<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        if self.failed {
            return Err(KernelFailure::InstanceFailed);
        }
        if self.has_invocation_data {
            self.failed = true;
            return Err(invalid(
                "whole-invocation scalar requires the lossless evaluation entry",
            ));
        }
        let observed = ObservedControl {
            original: control,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        let mut work = Work {
            control: &observed,
            pending: 0,
            scalar_scope: None,
        };
        let result = observed.checkpoint(0).and_then(|()| {
            self.evaluate_once::<KernelFailure>(
                input,
                selection,
                novarocks_functions::ScalarInvocationActivation::Activated,
                &mut work,
            )
        });
        let result = work.finish(result);
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    /// Actual caller demand, independent of empty Selection. This entry owns
    /// atomic invocation Data while the old adapter remains Kernel-only.
    pub fn evaluate_evaluation<'a>(
        &mut self,
        input: &RecordBatch,
        selection: Selection<'a>,
        activation: novarocks_functions::ScalarInvocationActivation,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, novarocks_functions::ScalarInvocationFailure> {
        self.evaluate_controlled::<novarocks_functions::ScalarInvocationFailure>(
            input, selection, activation, control, None,
        )
    }
    /// Production runtime callers explicitly loan their original synchronous
    /// operation scope. This uses the SAME Frame tree and demand author.
    pub fn evaluate_runtime<'a>(
        &mut self,
        input: &RecordBatch,
        selection: Selection<'a>,
        activation: novarocks_functions::ScalarInvocationActivation,
        control: &dyn KernelEvaluationControl,
        scope: &mut dyn crate::runtime::scalar_memory::RuntimeScalarOperationScope,
    ) -> Result<SelectedValues<'a>, crate::runtime::scalar_memory::RuntimeScalarEvaluationFailure>
    {
        self.evaluate_controlled(input, selection, activation, control, Some(scope))
    }
    fn evaluate_controlled<'a, E: scalar_invocation::FrameFailure>(
        &mut self,
        input: &RecordBatch,
        selection: Selection<'a>,
        activation: novarocks_functions::ScalarInvocationActivation,
        control: &dyn KernelEvaluationControl,
        scope: Option<&mut dyn crate::runtime::scalar_memory::RuntimeScalarOperationScope>,
    ) -> Result<SelectedValues<'a>, E> {
        use novarocks_functions::ScalarInvocationActivation;
        if self.failed {
            return Err(KernelFailure::InstanceFailed.into());
        }
        if activation == ScalarInvocationActivation::ValidateOnly && !selection.is_empty() {
            self.failed = true;
            return Err(invalid("nonempty root demand cannot be validation-only").into());
        }
        let observed = ObservedControl {
            original: control,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        let mut work = Work {
            control: &observed,
            pending: 0,
            scalar_scope: scope,
        };
        let result = (|| {
            observed.checkpoint(0)?;
            self.evaluate_once::<E>(input, selection, activation, &mut work)
        })();
        // A positively installed whole-invocation root preserves every first
        // failure, including entry validation before any leaf is created.
        let result = if self.has_invocation_data && result.is_err() {
            result
        } else {
            E::finish(&mut work, result)
        };
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn evaluate_once<'a, E: scalar_invocation::FrameFailure>(
        &mut self,
        input: &RecordBatch,
        selection: Selection<'a>,
        activation: novarocks_functions::ScalarInvocationActivation,
        work: &mut Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, E> {
        if input.num_rows() != selection.batch_rows() {
            return Err(invalid("selection differs from actual input batch rows").into());
        }
        let Some((input_node, input_role)) = self.input else {
            // The explicit empty port: no field, no metadata and one row, so
            // the root is evaluated exactly once per call.
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
            return self.evaluate_root::<E>(input, None, selection, activation, work);
        };
        let channels = self.program.checked().channels();
        let layout = channels
            .channel_layout(input_node, input_role)
            .ok_or_else(|| invalid("root input port has no frozen layout"))?;
        work.flush()?;
        let schema = input.schema();
        let frozen = layout.schema();
        // Metadata equality is an opaque library operation over the admitted
        // frozen domain. Field traversal uses the complete type owner's author,
        // including root dictionary attributes ignored by ordinary Arrow Eq.
        let same_metadata = schema.metadata() == frozen.metadata();
        work.flush()?;
        if !same_metadata || schema.fields().len() != frozen.fields().len() {
            return Err(invalid("actual input fields differ from the frozen root port").into());
        }
        for (actual, expected) in schema.fields().iter().zip(frozen.fields()) {
            if !arrow_fields_exact_observed(actual, expected, || work.step())? {
                return Err(invalid("actual input fields differ from the frozen root port").into());
            }
        }
        // Validate the entire incoming port, including columns not referenced
        // by this root. Empty selection still validates its shape and type.
        for (ordinal, array) in input.columns().iter().enumerate() {
            let ordinal = u32::try_from(ordinal).map_err(|_| KernelFailure::ResourceExhausted)?;
            let ty = channels
                .channel_type(ProgramChannelSite::Layout {
                    node: input_node,
                    role: input_role,
                    ordinal,
                })
                .ok_or_else(|| invalid("missing exact incoming channel type"))?;
            work.flush()?;
            validate_evaluated_argument_observed(
                EvaluatedArgument::Column(array),
                selection,
                ty,
                work.control,
            )?;
            work.step()?;
        }
        self.evaluate_root::<E>(
            input,
            Some((input_node, input_role)),
            selection,
            activation,
            work,
        )
    }
    fn evaluate_root<'a, E: scalar_invocation::FrameFailure>(
        &mut self,
        input: &RecordBatch,
        input_node: Option<(ProgramNodeId, ProgramChannelLayoutRole)>,
        selection: Selection<'a>,
        activation: novarocks_functions::ScalarInvocationActivation,
        work: &mut Work<'_, '_>,
    ) -> Result<SelectedValues<'a>, E> {
        let checked = self.program.checked();
        let typed = checked.channels().expressions();
        let snapshot = typed.resolved_calls().snapshot();
        let flow = &snapshot.flows()[&self.root.arena()];
        let result = guarded::evaluate_tree::<E>(
            &self.program,
            self.root,
            input,
            input_node,
            selection,
            activation,
            &mut self.instances,
            self.allocator.as_ref(),
            &self.effects,
            work,
        )?;
        let root_use = snapshot.bindings()[&self.root];
        let invocation = &flow.uses()[&root_use];
        let FunctionArgumentType::Value(ty) = typed
            .definition_type(self.root.arena(), invocation.definition)
            .ok_or_else(|| invalid("missing actual root value type"))?
        else {
            return Err(invalid("actual root is not a value").into());
        };
        let result = result.materialize(selection, &ty.data_type, work)?;
        if !ty.nullable {
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
                        )
                        .into());
                    }
                    Ok(())
                },
            )?;
        }
        Ok(result)
    }
}

fn evaluate_scalar<'a, E: scalar_invocation::FrameFailure>(
    occurrence: ProgramUseRef,
    prepared: scalar_invocation::PreparedCall<'_>,
    children: &[Value<'a>],
    selection: Selection<'a>,
    activation: novarocks_functions::ScalarInvocationActivation,
    instances: &mut BTreeMap<ProgramUseRef, scalar_invocation::CallInstance>,
    allocator: Option<&Arc<dyn novarocks_functions::AggregateStateAllocator>>,
    work: &mut Work<'_, '_>,
) -> Result<SelectedValues<'a>, E> {
    let mut blocked = Vec::with_capacity(selection.len());
    for _ in 0..selection.len() {
        blocked.push(false);
        work.step()?;
    }
    let mut inherited = BTreeMap::<usize, RowDataError>::new();
    for child in children {
        for error in child.errors() {
            let ordinal = error.selected_ordinal();
            blocked[ordinal] = true;
            // Ordered required children give stable first-error precedence.
            inherited.entry(ordinal).or_insert_with(|| error.clone());
            work.step()?;
        }
        if prepared.contract().effects().null_behavior == FunctionNullBehavior::Strict {
            work.flush()?;
            visit_selected_nulls(
                child.argument(),
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
        && !(E::ACTIVATE_EMPTY
            && prepared.is_invocation()
            && selection.is_empty()
            && activation == novarocks_functions::ScalarInvocationActivation::Activated)
    {
        work.flush()?;
        let values = new_null_array(
            &prepared.contract().result_type().data_type,
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
            &prepared.contract().result_type().data_type,
            values,
            errors.into_boxed_slice(),
            || work.step(),
        )
        .map_err(E::from);
    }
    let full_call = active_rows.len() == selection.len();
    let call_selection = if full_call {
        selection
    } else {
        Selection::try_sparse_observed(selection.batch_rows(), &active_rows, || work.step())?
    };
    let mut narrowed = Vec::new();
    if !full_call {
        let mut indices = Vec::with_capacity(active_ordinals.len());
        for &ordinal in &active_ordinals {
            indices.push(Some(
                u64::try_from(ordinal).map_err(|_| KernelFailure::ResourceExhausted)?,
            ));
            work.step()?;
        }
        for child in children {
            let output = if let Value::Selected(child) = child {
                let array = gather(child.values(), &indices, work)?;
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
    for (index, child) in children.iter().enumerate() {
        arguments.push(if !full_call && let Some(output) = &narrowed[index] {
            EvaluatedArgument::SelectedColumn(output)
        } else {
            child.argument()
        });
        work.step()?;
    }
    work.flush()?;
    if let std::collections::btree_map::Entry::Vacant(entry) = instances.entry(occurrence) {
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
    let output = match E::invoke(
        instances
            .get_mut(&occurrence)
            .ok_or_else(|| internal("scalar instance was not installed"))?,
        call_selection,
        &arguments,
        activation,
        work,
    ) {
        Ok(output) => output,
        Err(cause) => {
            // A real typed leaf failure owns the first cause. Do not add a
            // completion callback that can replace it. Row Data stays Ok and
            // retains the original lawful downstream masking path.
            work.control
                .operation_aborted
                .store(true, Ordering::Relaxed);
            return Err(cause.into());
        }
    };
    let (_, array, own_errors) = output.into_parts();
    for error in own_errors {
        let original = active_ordinals[error.selected_ordinal()];
        inherited.insert(original, error.with_selected_ordinal(original));
        work.step()?;
    }
    let array = if full_call {
        array
    } else {
        let mut indices = Vec::with_capacity(selection.len());
        for _ in 0..selection.len() {
            indices.push(None);
            work.step()?;
        }
        for (call_ordinal, &original) in active_ordinals.iter().enumerate() {
            indices[original] =
                Some(u64::try_from(call_ordinal).map_err(|_| KernelFailure::ResourceExhausted)?);
            work.step()?;
        }
        gather(&array, &indices, work)?
    };
    let mut errors = Vec::with_capacity(inherited.len());
    for error in inherited.into_values() {
        errors.push(error);
        work.step()?;
    }
    work.flush()?;
    SelectedValues::try_new_observed(
        selection,
        &prepared.contract().result_type().data_type,
        array,
        errors.into_boxed_slice(),
        || work.step(),
    )
    .map_err(E::from)
}

fn gather(
    array: &ArrayRef,
    indices: &[Option<u64>],
    work: &mut Work<'_, '_>,
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
    .map_err(|error| match error {
        novarocks_functions::selected_copy::CopyError::Control(error) => error,
        novarocks_functions::selected_copy::CopyError::Extent => KernelFailure::ResourceExhausted,
        novarocks_functions::selected_copy::CopyError::Invalid(_)
        | novarocks_functions::selected_copy::CopyError::Unsupported(_) => {
            invalid("selected Arrow copy requires a different carrier protocol")
        }
    })?;
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

#[cfg(test)]
mod row_error_tests;
#[cfg(test)]
pub(crate) mod tests;

#[cfg(test)]
mod copy_tests;

mod guarded;
mod scalar_invocation;

#[cfg(test)]
pub(crate) mod guarded_tests;

mod boolean_region;

mod unary;

#[cfg(test)]
mod nary_tests;

mod filter_conjunction;
pub(crate) use filter_conjunction::CompiledFilterConjunctionInstance;

#[cfg(test)]
mod runtime_scalar_memory_tests;

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

//! Real frozen catalogue attachments to actual local operator sites. The CPU
//! fixtures execute only to verify borrowed handle identity and lifecycle;
//! they are not production migration, native, or memory authorization evidence.

use super::*;
use crate::*;
use arrow_array::{ArrayRef, Int32Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use novarocks_connector_contract::WriteTargetOrdinal;
use novarocks_functions::*;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, DecimalOverflowPolicy, EvaluationDomainId,
    ExpressionEffectContext, FunctionEffectDeclaration, FunctionInstanceState,
    FunctionNullBehavior, ObservableEffects, SemanticParameters, WindowFrame as CommonWindowFrame,
};
use std::{
    collections::HashMap,
    mem::MaybeUninit,
    num::NonZeroUsize,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Compile(Option<CompileControlError>);
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert!(work <= 256);
        self.0.clone().map_or(Ok(()), Err)
    }
}
const COMPILE: Compile = Compile(None);
struct Runtime;
impl KernelEvaluationControl for Runtime {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        Ok(())
    }
}
const RUNTIME: Runtime = Runtime;

#[derive(Debug, Default)]
pub(crate) struct Counts {
    resolve: AtomicUsize,
    legacy: AtomicUsize,
    refine: AtomicUsize,
    aggregate: AtomicUsize,
    adapter: AtomicUsize,
    window: AtomicUsize,
    table: AtomicUsize,
    state_create: AtomicUsize,
    state_drop: AtomicUsize,
    update: AtomicUsize,
    merge: AtomicUsize,
    begin: AtomicUsize,
    finish: AtomicUsize,
    instance_drop: AtomicUsize,
}

fn value_type(data_type: DataType) -> FunctionValueType {
    FunctionValueType::new(data_type, false)
}
fn state_format() -> AggregateStateFormatIdentity {
    AggregateStateFormatIdentity::try_new("fixture/catalogue/sum-state-v1").unwrap()
}
fn base(kind: FunctionKind) -> FunctionEffectDeclaration {
    let (argument_control, instance_state, own_row_error) = match kind {
        FunctionKind::Aggregate => (
            ArgumentControl::Aggregate,
            FunctionInstanceState::AggregateInstance,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ),
        FunctionKind::Window => (
            ArgumentControl::Window,
            FunctionInstanceState::WindowPartition,
            FunctionIntrinsicRowError::NotRowEvaluated,
        ),
        FunctionKind::Table => (
            ArgumentControl::Table,
            FunctionInstanceState::TableInstance,
            FunctionIntrinsicRowError::NoRowError,
        ),
        FunctionKind::Scalar => (
            ArgumentControl::Eager,
            FunctionInstanceState::None,
            FunctionIntrinsicRowError::NoRowError,
        ),
    };
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control,
        instance_state,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::default(),
    }
}

pub(crate) struct Owner {
    pub(crate) declaration: FunctionBindingDeclaration,
    pub(crate) implementations: Vec<PureImplementationDeclaration>,
    pub(crate) selections: Vec<Arc<FunctionBindingSelection>>,
    pub(crate) counts: Arc<Counts>,
}
impl Owner {
    pub(crate) fn new(kind: FunctionKind, abis: &[PureKernelAbi]) -> Self {
        let id = FunctionId::try_new(format!("fixture/catalogue/{kind:?}-v1")).unwrap();
        let mut overloads = Vec::new();
        let mut implementations = Vec::new();
        let mut selections = Vec::new();
        for (ordinal, &abi) in abis.iter().enumerate() {
            let overload = FunctionOverloadId::try_new(format!("fixture/typed-{ordinal}")).unwrap();
            let argument = value_type(DataType::Int64);
            overloads.push(FunctionOverloadDeclaration::from_effects(
                overload.clone(),
                format!("{:?}", argument.data_type()),
                "Int64",
                (kind == FunctionKind::Aggregate).then(|| AggregateBindingDeclaration {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_pattern: "Int64".into(),
                    state_format: state_format(),
                }),
                base(kind),
            ));
            implementations.push(PureImplementationDeclaration {
                overload: overload.clone(),
                implementation: PureImplementationId::try_new(format!(
                    "fixture/catalogue/cpu-{ordinal}-v1"
                ))
                .unwrap(),
                abi,
            });
            let output = value_type(DataType::Int64);
            selections.push(Arc::new(FunctionBindingSelection {
                overload,
                argument_types: vec![FunctionArgumentType::Value(argument)].into(),
                result_type: if kind == FunctionKind::Table {
                    FunctionResultType::Relation(vec![output].into())
                } else {
                    FunctionResultType::Scalar(output)
                },
                aggregate: (kind == FunctionKind::Aggregate).then(|| AggregateBindingSelection {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_type: value_type(DataType::Int64),
                    state_format: state_format(),
                }),
            }));
        }
        Self {
            declaration: FunctionBindingDeclaration::try_new_complete(id, kind, overloads).unwrap(),
            implementations,
            selections,
            counts: Arc::default(),
        }
    }
    pub(crate) fn frozen(&self, selected: &FunctionBindingSelection) -> CallEffects {
        let declaration = self
            .declaration
            .effect_declaration(&selected.overload)
            .unwrap();
        CallEffects {
            value_stability: declaration.value_stability,
            own_row_error: declaration.own_row_error,
            failure_behavior: declaration.failure_behavior,
            null_behavior: declaration.null_behavior,
            argument_control: declaration.argument_control,
            instance_state: declaration.instance_state,
            observable_effects: declaration.observable_effects,
            environment: Box::default(),
            proof_scope: CallProofScope::Unconditional,
        }
    }
    pub(crate) fn manifest(&self) -> Vec<InstalledPureKernel> {
        // Fixture installation inventory, independent of the sealed catalogue.
        self.implementations
            .iter()
            .map(|implementation| InstalledPureKernel {
                function: self.declaration.function_id().clone(),
                kind: self.declaration.kind(),
                implementation: implementation.clone(),
                aggregate_state_format: (self.declaration.kind() == FunctionKind::Aggregate)
                    .then(state_format),
            })
            .collect()
    }
}
impl PureFunctionMetadataOwner for Owner {
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}
impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.counts.resolve.fetch_add(1, Ordering::Relaxed);
        self.selections
            .iter()
            .find(|selected| {
                selected.argument_types.as_ref()
                    == request
                        .arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Vec<_>>()
            })
            .map(|selected| selected.as_ref().clone())
            .ok_or(FunctionBindingError::NoMatchingOverload)
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if request.logical_argument_count != 1
            || !self
                .selections
                .iter()
                .any(|candidate| candidate.as_ref() == selected)
            || request.arguments.len() != 1
            || request.arguments[0].argument_type() != selected.argument_types[0]
        {
            return Err(FunctionBindingError::NoMatchingOverload);
        }
        Ok(())
    }
}
impl FunctionEffectOwner for Owner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if function != self.declaration.function_id() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        self.declaration.effect_declaration(&selected.overload)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 1)
            .map_err(FunctionEffectOwnerError::Control)?;
        self.validate_selected(input.selected, input.request, control)?;
        self.counts.refine.fetch_add(1, Ordering::Relaxed);
        Ok(self.frozen(input.selected))
    }
}
impl AggregateSignatureResolver for Owner {
    fn resolve_aggregate(
        &self,
        arguments: &[DataType],
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        self.counts.legacy.fetch_add(1, Ordering::Relaxed);
        let selected = self
            .selections
            .iter()
            .find(|selected| {
                matches!(&selected.argument_types[0], FunctionArgumentType::Value(ty)
                    if arguments == [ty.data_type().clone()])
            })
            .ok_or(FunctionResolutionError::NoMatchingSignature {
                candidates: self.selections.len(),
                binding_enforced: true,
            })?;
        Ok(ResolvedAggregateSignature {
            overload: AggregateOverloadIdentity::try_new(selected.overload.as_str()).unwrap(),
            argument_types: arguments.to_vec(),
            intermediate_type: DataType::Int64,
            output_type: DataType::Int64,
            state_format: state_format(),
        })
    }
    fn produces_null(&self) -> bool {
        false
    }
}

pub(crate) struct Call<'owner> {
    owner: &'owner Owner,
    pub(crate) selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: [Option<ExpressionUseId>; 1],
    parameters: SemanticParameters,
    pub(crate) context_id: u32,
    state_input_type: FunctionValueType,
}
impl<'owner> Call<'owner> {
    pub(crate) fn new(owner: &'owner Owner, ordinal: usize) -> Self {
        let selected = owner.selections[ordinal].clone();
        let arguments = selected
            .argument_types
            .iter()
            .map(|argument| match argument {
                FunctionArgumentType::Value(value_type) => FunctionArgument::Value {
                    value_type: value_type.clone(),
                    constant: None,
                },
                FunctionArgumentType::Lambda { .. } => unreachable!("typed fixture uses values"),
            })
            .collect();
        Self {
            owner,
            selected,
            arguments,
            uses: [Some(ExpressionUseId::new(1))],
            parameters: SemanticParameters::default(),
            context_id: u32::MAX,
            state_input_type: value_type(DataType::Int64),
        }
    }
    pub(crate) fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(self.context_id),
                domain: EvaluationDomainId::new(1),
                demand: EvaluationDemand::Value,
            },
            argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: self.owner.declaration.function_id(),
            kind: self.owner.declaration.kind(),
            selected: &self.selected,
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &self.arguments,
                logical_argument_count: 1,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        }
    }
    pub(crate) fn input_for_phase(&self, phase: AggregateKernelPhase) -> CallEffectInput<'_> {
        let mut input = self.input();
        if !phase.consumes_logical_arguments() {
            input.argument_uses = novarocks_functions::CallArgumentUses::AggregateMerge {
                phase,
                state_context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(0),
                    domain: EvaluationDomainId::new(2),
                    demand: EvaluationDemand::Value,
                },
                state_input_type: &self.state_input_type,
            };
        }
        input
    }
    pub(crate) fn children(&self) -> ScopedExpressionEffects {
        ScopedExpressionEffects::pure_value(self.input().context)
    }
}
fn aggregate_options(phase: AggregateKernelPhase) -> AggregatePreparationOptions {
    AggregatePreparationOptions {
        state_interpretation: None,
        phase,
        distinct: false,
        order_keys: Arc::from([]),
        state_input_type: None,
    }
}
pub(crate) fn window_options() -> WindowCallOptions {
    WindowCallOptions::try_new(
        Some(CommonWindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::UnboundedPreceding,
            end: WindowBound::CurrentRow,
            exclusion: WindowFrameExclusion::NoOthers,
        }),
        false,
        &COMPILE,
    )
    .unwrap()
}
pub(crate) fn over_prepare(call: &Call<'_>) -> PureCallPreparation {
    PureCallPreparation::AggregateWindow {
        arguments: call.children(),
        options: AggregateWindowPreparationOptions {
            aggregate: aggregate_options(AggregateKernelPhase::Single),
            window: window_options(),
        },
    }
}

impl PureAggregateImplementation for Owner {
    type Kernel = Sum;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        _: &dyn PureCompileControl,
    ) -> Result<Arc<Sum>, KernelFailure> {
        self.counts.aggregate.fetch_add(1, Ordering::Relaxed);
        assert_eq!(input.function_id, self.declaration.function_id());
        assert!(std::ptr::eq(input.selected, contract.call().selected()));
        Ok(Arc::new(Sum {
            contract,
            counts: self.counts.clone(),
        }))
    }
}
#[derive(Debug)]
pub(crate) struct Sum {
    contract: Arc<AggregateCallContract>,
    pub(crate) counts: Arc<Counts>,
}
pub(crate) struct SumState {
    value: i64,
    pub(crate) counts: Arc<Counts>,
}
impl Drop for SumState {
    fn drop(&mut self) {
        self.counts.state_drop.fetch_add(1, Ordering::Relaxed);
    }
}
fn number(argument: EvaluatedArgument<'_>, ordinal: usize, row: usize) -> i64 {
    let at = argument.value_row(ordinal, row);
    match argument.array().data_type() {
        DataType::Int32 => i64::from(
            argument
                .array()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(at),
        ),
        DataType::Int64 => argument
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(at),
        _ => unreachable!("exact fixture signature checked integer inputs"),
    }
}
impl PreparedAggregateKernel for Sum {
    type State = SumState;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = SelectedAggregateMergeInput<'a, 'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::FixedZero
    }
    fn create_state(
        &self,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SumState, KernelFailure> {
        control.checkpoint(1)?;
        self.counts.state_create.fetch_add(1, Ordering::Relaxed);
        Ok(SumState {
            value: 0,
            counts: self.counts.clone(),
        })
    }
    fn prepare_update<'a>(
        &'a self,
        input: SelectedAggregateUpdateInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'a>, KernelFailure> {
        control.checkpoint(1)?;
        Ok(input)
    }
    fn update_row<'a>(
        &self,
        state: &mut SumState,
        input: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        control.checkpoint(1)?;
        self.counts.update.fetch_add(1, Ordering::Relaxed);
        state.value += number(
            input.logical_arguments()[0],
            ordinal,
            input.selection().row(ordinal).unwrap(),
        );
        Ok(())
    }
    fn prepare_merge<'a>(
        &'a self,
        input: SelectedAggregateMergeInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        control.checkpoint(1)?;
        Ok(input)
    }
    fn merge_row<'a>(
        &self,
        state: &mut SumState,
        input: &Self::PreparedMergeBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        control.checkpoint(1)?;
        self.counts.merge.fetch_add(1, Ordering::Relaxed);
        state.value += number(
            input.state(),
            ordinal,
            input.selection().row(ordinal).unwrap(),
        );
        Ok(())
    }
    fn build_intermediate<'a, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'a SumState>,
    {
        control.checkpoint(1)?;
        Ok(Arc::new(Int64Array::from(
            states.map(|state| state.value).collect::<Vec<_>>(),
        )))
    }
    fn build_final<'a, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'a SumState>,
    {
        self.build_intermediate(states, control)
    }
    fn retained_bytes(&self, _: &SumState) -> usize {
        0
    }
}

impl PureAggregateWindowImplementation for Owner {
    fn prepare_aggregate_window(
        &self,
        aggregate: Arc<Sum>,
        contract: Arc<WindowCallContract>,
        _: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        self.counts.adapter.fetch_add(1, Ordering::Relaxed);
        assert!(Arc::ptr_eq(&self.counts, &aggregate.counts));
        assert_eq!(aggregate.contract().phase(), AggregateKernelPhase::Single);
        assert!(Arc::ptr_eq(
            contract.aggregate().unwrap(),
            aggregate.contract()
        ));
        Ok(Arc::new(Window {
            contract,
            aggregate: Some(aggregate),
            counts: self.counts.clone(),
        }))
    }
}
impl PureWindowImplementation for Owner {
    fn prepare_window(
        &self,
        _: CallEffectInput<'_>,
        contract: Arc<WindowCallContract>,
        _: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        self.counts.window.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(Window {
            contract,
            aggregate: None,
            counts: self.counts.clone(),
        }))
    }
}
#[derive(Debug)]
struct Window {
    contract: Arc<WindowCallContract>,
    aggregate: Option<Arc<Sum>>,
    pub(crate) counts: Arc<Counts>,
}
struct Partition {
    results: Vec<i64>,
    pub(crate) counts: Arc<Counts>,
}
impl Drop for Partition {
    fn drop(&mut self) {
        self.counts.instance_drop.fetch_add(1, Ordering::Relaxed);
    }
}
impl PreparedWindowKernel for Window {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }
    fn partition_retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure> {
        Ok(size_of::<Partition>() + rows * size_of::<i64>())
    }
    fn begin_partition<'a>(
        self: Arc<Self>,
        input: WindowPartitionInput<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'a>, KernelFailure> {
        self.counts.begin.fetch_add(1, Ordering::Relaxed);
        let full = input.full_input();
        let mut results = Vec::with_capacity(full.partition_rows());
        for (row, frame) in input.frames().iter().enumerate() {
            control.checkpoint(1)?;
            if let Some(aggregate) = &self.aggregate {
                let rows = (frame.start..frame.end).collect::<Vec<_>>();
                let selected = Selection::try_sparse(full.partition_rows(), &rows).unwrap();
                let values = SelectedAggregateUpdateInput::try_new(
                    aggregate.contract(),
                    selected,
                    full.logical_arguments(),
                    full.order_arguments(),
                    control,
                )?;
                let mut state = create_aggregate_state(aggregate.as_ref(), control)?;
                let mut invocation =
                    AggregateUpdateInvocation::try_new(aggregate.as_ref(), values, control)?;
                while invocation.next_selected_ordinal().is_some() {
                    invocation.update_next(&mut state, control)?;
                }
                let output = emit_aggregate(aggregate.as_ref(), [&state].into_iter(), 1, control)?;
                results.push(
                    output
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                );
            } else {
                results.push(number(full.logical_arguments()[0], row, row));
            }
        }
        Ok(Box::new(Partition {
            results,
            counts: self.counts.clone(),
        }))
    }
}
impl WindowKernelPartition for Partition {
    fn evaluate<'a>(
        &mut self,
        selection: Selection<'a>,
        _: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(1)?;
        SelectedValues::try_new(
            selection,
            &DataType::Int64,
            Arc::new(Int64Array::from(
                selection
                    .iter()
                    .map(|row| self.results[row])
                    .collect::<Vec<_>>(),
            )),
            Box::default(),
        )
        .map_err(|_| {
            KernelFailure::Internal(KernelDiagnostic::new("fixture window output invalid"))
        })
    }
    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        control.checkpoint(1)?;
        self.counts.finish.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.results.capacity() * size_of::<i64>()
    }
}

impl PureTableImplementation for Owner {
    fn prepare_table(
        &self,
        _: CallEffectInput<'_>,
        contract: Arc<TableCallContract>,
        _: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedTableKernel>, KernelFailure> {
        self.counts.table.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(Table {
            contract,
            counts: self.counts.clone(),
        }))
    }
}
#[derive(Debug)]
struct Table {
    contract: Arc<TableCallContract>,
    pub(crate) counts: Arc<Counts>,
}
struct Cursor<'a> {
    input: SelectedTableInput<'a, 'a>,
    pub(crate) counts: Arc<Counts>,
    position: usize,
}
impl Drop for Cursor<'_> {
    fn drop(&mut self) {
        self.counts.instance_drop.fetch_add(1, Ordering::Relaxed);
    }
}
impl PreparedTableKernel for Table {
    fn contract(&self) -> &Arc<TableCallContract> {
        &self.contract
    }
    fn cursor_retained_upper_bound(&self, _: usize) -> Result<usize, KernelFailure> {
        Ok(size_of::<Cursor<'_>>())
    }
    fn begin_selected<'a>(
        self: Arc<Self>,
        input: SelectedTableInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn TableKernelCursor + 'a>, KernelFailure> {
        control.checkpoint(1)?;
        self.counts.begin.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(Cursor {
            input,
            counts: self.counts.clone(),
            position: 0,
        }))
    }
}
impl TableKernelCursor for Cursor<'_> {
    fn next(
        &mut self,
        capacity: TableStepCapacity,
        control: &dyn KernelEvaluationControl,
    ) -> Result<TableCursorStep, KernelFailure> {
        control.checkpoint(1)?;
        let selection = self.input.selection();
        if capacity.page.rows == 0 || capacity.page.completions == 0 {
            return Ok(TableCursorStep::CapacityRequired(
                TableCapacityRequirements {
                    row: capacity.page.rows == 0,
                    completion: capacity.page.completions == 0,
                    parent_error: false,
                },
            ));
        }
        let end = selection
            .len()
            .min(self.position + capacity.page.rows.min(capacity.page.completions));
        let output: ArrayRef = Arc::new(Int64Array::from(
            (self.position..end)
                .map(|ordinal| {
                    number(
                        self.input.arguments()[0],
                        ordinal,
                        selection.row(ordinal).unwrap(),
                    )
                })
                .collect::<Vec<_>>(),
        ));
        let parents: Box<[usize]> = (self.position..end).collect();
        self.position = end;
        Ok(TableCursorStep::Page(OwnedTableOutputPage {
            columns: vec![output].into(),
            parent_ordinals: parents.clone(),
            completed_parents: parents,
            parent_errors: Box::default(),
            eof: end == selection.len(),
        }))
    }
    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        control.checkpoint(1)?;
        self.counts.finish.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn retained_bytes(&self) -> usize {
        size_of::<Self>()
    }
}

fn seal(
    definition: FunctionDefinition,
    manifest: Vec<InstalledPureKernel>,
) -> Result<PureEngineFunctionCatalog, PureCatalogError> {
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    builder.seal_pure(manifest)
}
pub(crate) fn aggregate_catalog(owner: Arc<Owner>) -> PureEngineFunctionCatalog {
    seal(
        FunctionDefinition::try_new_pure_aggregate_window(
            "typed_sum",
            FunctionVisibility::Public,
            owner.clone(),
        )
        .unwrap(),
        owner.manifest(),
    )
    .unwrap()
}

fn literal_arena(ty: DataType) -> Arc<ImmutableExpressions> {
    Arc::new(
        ImmutableExpressions::try_new(
            vec![StaticExprNode::new(
                StaticExprKind::Literal(StaticLiteral::Int64(7)),
                ty,
                None,
            )],
            false,
            HashMap::new(),
            None,
        )
        .unwrap(),
    )
}
fn layout(slot: u32, ty: DataType) -> StaticLayout {
    StaticLayout::try_new(
        Arc::new(Schema::new(vec![Field::new("value", ty, false)])),
        Arc::from([SlotId::new(slot)]),
    )
    .unwrap()
}
fn source(layout: &StaticLayout) -> ProgramNode {
    let array: ArrayRef = match layout.schema().field(0).data_type() {
        DataType::Int64 => Arc::new(Int64Array::from(vec![3, 999, 5])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![3, 999, 5])),
        _ => unreachable!("fixture source integer carrier"),
    };
    let batch = RecordBatch::try_new(layout.schema().clone(), vec![array]).unwrap();
    ProgramNode::new(
        10,
        ProgramNodeKind::Values {
            values: StaticValues::try_new(batch, layout.clone()).unwrap(),
        },
        layout.clone(),
    )
}
fn program(
    nodes: Vec<ProgramNode>,
    arena: Arc<ImmutableExpressions>,
    requirements: Vec<BindingRequirement>,
) -> LocalProgramGraph {
    let root = ProgramNodeId::new(nodes.len() - 1);
    let output = nodes[root.index()].output_layout();
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        output.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    );
    LocalProgramGraph::try_new(
        nodes,
        root,
        arena,
        profile,
        BindingRequirements::try_new(requirements).unwrap(),
    )
    .unwrap()
}
pub(crate) fn snapshot(program: LocalProgramGraph) -> ProgramRootControlBindings {
    let roots = ProgramExpressionRoots::collect(&program, &COMPILE).unwrap();
    let mut flows = BTreeMap::new();
    let mut bindings = Vec::new();
    for (arena, definitions) in roots.arenas() {
        let mut uses = Vec::new();
        for (site, root) in roots
            .sites()
            .iter()
            .filter(|(site, _)| site.arena() == *arena)
        {
            let use_id = ExpressionUseId::new(u32::try_from(uses.len()).unwrap());
            uses.push(ProgramExpressionUse {
                context: ExpressionEffectContext {
                    use_id,
                    domain: EvaluationDomainId::new(1),
                    demand: root.demand,
                },
                definition: root.definition,
                control: ControlShape::Eager,
                arguments: Box::default(),
            });
            bindings.push(ProgramRootUseBinding {
                site: *site,
                use_id,
            });
        }
        flows.insert(
            *arena,
            ProgramControlFlow::try_new(
                vec![ProgramEvaluationDomain {
                    id: EvaluationDomainId::new(1),
                    parent: None,
                    guard: None,
                }],
                uses,
                definitions.nodes().len(),
                &COMPILE,
            )
            .unwrap(),
        );
    }
    ProgramRootControlBindings::try_new(program, flows, bindings, &COMPILE).unwrap()
}
fn resolved_signature(owner: &Owner) -> ResolvedAggregateSignature {
    let selected = &owner.selections[0];
    ResolvedAggregateSignature {
        overload: AggregateOverloadIdentity::try_new(selected.overload.as_str()).unwrap(),
        argument_types: selected
            .argument_types
            .iter()
            .map(|arg| match arg {
                FunctionArgumentType::Value(ty) => ty.data_type().clone(),
                FunctionArgumentType::Lambda { .. } => unreachable!(),
            })
            .collect(),
        intermediate_type: DataType::Int64,
        output_type: DataType::Int64,
        state_format: state_format(),
    }
}
/// The actual fixture catalogue already owns this complete selection. Its
/// static calls retain that exact source; display aliases and raw signatures
/// are not authorities for reconstructing the selected semantics.
fn frozen_selection(owner: &Owner) -> Arc<FrozenAggregateSelection> {
    Arc::new(
        FrozenAggregateSelection::try_new(
            owner.declaration.function_id().clone(),
            owner.selections[0].clone(),
            1,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    )
}

pub(crate) fn aggregate_node(
    owner: &Owner,
    intermediate: bool,
    finalize: bool,
    order: StaticAggregateOrder,
    group_roots: usize,
) -> LocalProgramGraph {
    let output = layout(1, DataType::Int64);
    program(
        vec![
            source(&output),
            ProgramNode::new(
                20,
                ProgramNodeKind::Aggregate {
                    input: ProgramNodeId::new(0),
                    group_by: vec![ProgramExprId::new(0); group_roots],
                    functions: vec![StaticAggregateCall {
                        frozen: Some(frozen_selection(owner)),
                        state_interpretation: None,
                        // Deliberately unrelated display text: it is never implementation authority.
                        name: "display_alias_without_catalogue_entry".into(),
                        inputs: vec![ProgramExprId::new(0)],
                        input_is_intermediate: intermediate,
                        types: None,
                        order,
                        resolved: resolved_signature(owner),
                    }],
                    need_finalize: finalize,
                    input_is_intermediate: intermediate,
                    topn_filters: vec![].into(),
                    streaming_preaggregation_mode: None,
                },
                output,
            ),
        ],
        literal_arena(DataType::Int64),
        vec![],
    )
}
pub(crate) fn prepare_aggregate_token(
    catalog: &PureEngineFunctionCatalog,
    call: &Call<'_>,
    phase: AggregateKernelPhase,
) -> PureCallSpecialization {
    let mut options = aggregate_options(phase);
    if !phase.consumes_logical_arguments() {
        options.state_input_type = Some(call.state_input_type.clone());
    }
    catalog
        .prepare_frozen(
            call.input_for_phase(phase),
            call.selected.clone(),
            &call.owner.frozen(&call.selected),
            PureCallPreparation::Aggregate {
                arguments: call.children(),
                options,
            },
            &COMPILE,
        )
        .unwrap()
}
pub(crate) fn aggregate_site() -> ProgramCallSite {
    ProgramCallSite::Aggregate {
        node: ProgramNodeId::new(1),
        call: 0,
    }
}
fn integers(array: &ArrayRef) -> Vec<i64> {
    array
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}
#[repr(C, align(64))]
struct Storage([MaybeUninit<u8>; 128]);

pub(crate) fn run_aggregate(handle: &PreparedAggregateHandle, phase: AggregateKernelPhase) {
    let mut storage = Storage([MaybeUninit::uninit(); 128]);
    let mut states = [handle.initialize_in(&mut storage.0, &RUNTIME).unwrap()];
    let array: ArrayRef = Arc::new(Int64Array::from(vec![3, 999, 5]));
    let args = [EvaluatedArgument::Column(&array)];
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    if phase.consumes_logical_arguments() {
        let input = SelectedAggregateUpdateInput::try_new(
            handle.contract(),
            selection,
            &args,
            &[],
            &RUNTIME,
        )
        .unwrap();
        let mut frame = handle
            .prepare_update_batch(&mut states, &[0, 0], input, &RUNTIME)
            .unwrap();
        frame.run(&RUNTIME).unwrap();
        assert_eq!(frame.rows_processed(), 2);
    } else {
        let input =
            SelectedAggregateMergeInput::try_new(handle.contract(), selection, args[0], &RUNTIME)
                .unwrap();
        let mut frame = handle
            .prepare_merge_batch(&mut states, &[0, 0], input, &RUNTIME)
            .unwrap();
        frame.run(&RUNTIME).unwrap();
        assert_eq!(frame.rows_processed(), 2);
    }
    assert_eq!(
        integers(&handle.emit(&states, &[0], 1, &RUNTIME).unwrap()),
        [8]
    );
}

#[test]
fn four_aggregate_phases_borrow_the_actual_frozen_handle_and_run_its_state() {
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateWindowV1],
    ));
    let catalog = aggregate_catalog(owner.clone());
    let call = Call::new(&owner, 0);
    for (intermediate, finalize, phase) in [
        (false, false, AggregateKernelPhase::Partial),
        (false, true, AggregateKernelPhase::Single),
        (true, false, AggregateKernelPhase::Intermediate),
        (true, true, AggregateKernelPhase::Final),
    ] {
        let token = prepare_aggregate_token(&catalog, &call, phase);
        let original_call = token.call_contract() as *const FunctionCallContract;
        let resolved = ProgramResolvedCalls::try_new(
            snapshot(aggregate_node(
                &owner,
                intermediate,
                finalize,
                StaticAggregateOrder::default(),
                0,
            )),
            vec![(aggregate_site(), token)],
            &COMPILE,
        )
        .unwrap();
        let entry = &resolved.calls()[&aggregate_site()];
        assert_eq!(entry.call_contract() as *const _, original_call);
        assert!(Arc::ptr_eq(
            entry.call_contract().selected_owner(),
            &call.selected
        ));
        assert_eq!(entry.implementation(), &owner.implementations[0]);
        let ProgramStateTemplate::Aggregate { scope, kernel } = entry.state_template() else {
            panic!("aggregate state scope")
        };
        assert_eq!(
            scope,
            ProgramAggregateCallScope::Aggregate {
                node: ProgramNodeId::new(1),
                call: 0
            }
        );
        let PreparedPureKernel::Aggregate(actual) = entry.specialization().prepared() else {
            panic!("actual aggregate")
        };
        assert!(std::ptr::eq(kernel, actual));
        assert_eq!(kernel.state_layout(), std::alloc::Layout::new::<SumState>());
        assert_eq!(kernel.contract().phase(), phase);
        run_aggregate(kernel, phase);
    }
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 4);
    assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), 4);
    assert_eq!(owner.counts.state_create.load(Ordering::Relaxed), 4);
    assert_eq!(owner.counts.state_drop.load(Ordering::Relaxed), 4);
    assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 0);
}

pub(crate) fn window_program(
    owner: &Owner,
    aggregate: bool,
    frame: Option<WindowFrame>,
    ignore_nulls: bool,
) -> LocalProgramGraph {
    let output = layout(1, DataType::Int64);
    program(
        vec![
            source(&output),
            ProgramNode::new(
                20,
                ProgramNodeKind::Analytic {
                    input: ProgramNodeId::new(0),
                    partition_exprs: vec![],
                    order_by_exprs: vec![],
                    functions: vec![StaticWindowFunction {
                        frozen_aggregate: aggregate.then(|| frozen_selection(owner)),
                        kind: if aggregate {
                            WindowFunctionKind::Sum
                        } else {
                            WindowFunctionKind::FirstValue
                        },
                        args: vec![ProgramExprId::new(0)],
                        return_type: DataType::Int64,
                        aggregate_binding: aggregate
                            .then(|| (Arc::from("alias"), resolved_signature(owner))),
                        frame,
                        ignore_nulls,
                    }],
                    output_columns: vec![AnalyticOutputColumn::Window(0)],
                },
                output,
            ),
        ],
        literal_arena(DataType::Int64),
        vec![],
    )
}
/// One compiled-only prepared call over the fixture source, with `frame`.
fn prepared_window_graph(
    frame: Option<WindowFrame>,
) -> Result<LocalProgramGraph, LocalProgramError> {
    let output = layout(1, DataType::Int64);
    let nodes = vec![
        source(&output),
        ProgramNode::new(
            20,
            ProgramNodeKind::Analytic {
                input: ProgramNodeId::new(0),
                partition_exprs: vec![],
                order_by_exprs: vec![],
                functions: vec![StaticWindowFunction {
                    // This fixture declares a window-only prepared call.
                    frozen_aggregate: None,
                    kind: WindowFunctionKind::Prepared,
                    args: vec![ProgramExprId::new(0)],
                    return_type: DataType::Int64,
                    aggregate_binding: None,
                    frame,
                    ignore_nulls: false,
                }],
                output_columns: vec![AnalyticOutputColumn::Window(0)],
            },
            output.clone(),
        ),
    ];
    let profile = CompileProfile::new(
        NonZeroUsize::new(1).unwrap(),
        None,
        output.identity().unwrap(),
        KernelAbiVersion::CURRENT,
    );
    LocalProgramGraph::try_new(
        nodes,
        ProgramNodeId::new(1),
        literal_arena(DataType::Int64),
        profile,
        BindingRequirements::try_new(vec![]).unwrap(),
    )
}
pub(crate) fn local_frame() -> WindowFrame {
    WindowFrame {
        start: None,
        end: Some(WindowBoundary::CurrentRow),
        window_type: WindowType::Rows,
    }
}
pub(crate) fn window_site() -> ProgramCallSite {
    ProgramCallSite::Window {
        node: ProgramNodeId::new(1),
        call: 0,
    }
}
pub(crate) fn run_window(kernel: Arc<dyn PreparedWindowKernel>, expected: &[i64]) {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![3, 4, 5]));
    let args = [EvaluatedArgument::Column(&array)];
    let contract = kernel.contract().clone();
    let full = FullPartitionWindowInput::try_new(&contract, 3, &args, &[], &RUNTIME).unwrap();
    let peers = [WindowRowRange { start: 0, end: 3 }];
    let frames = [
        WindowRowRange { start: 0, end: 1 },
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 0, end: 3 },
    ];
    let input = WindowPartitionInput::try_new(full, &peers, &frames, &RUNTIME).unwrap();
    let mut partition = WindowEvaluationPartition::begin(kernel, input, &RUNTIME).unwrap();
    let rows = [0, 2];
    let output = partition
        .evaluate(Selection::try_sparse(3, &rows).unwrap(), 2, &RUNTIME)
        .unwrap();
    assert_eq!(integers(output.values()), expected);
    partition.finish(&RUNTIME).unwrap();
}
#[test]
fn window_and_aggregate_over_keep_different_identity_with_the_same_partition_lifecycle() {
    for aggregate in [false, true] {
        let kind = if aggregate {
            FunctionKind::Aggregate
        } else {
            FunctionKind::Window
        };
        let owner = Arc::new(Owner::new(
            kind,
            &[if aggregate {
                PureKernelAbi::AggregateWindowV1
            } else {
                PureKernelAbi::WindowV1
            }],
        ));
        let catalog = if aggregate {
            aggregate_catalog(owner.clone())
        } else {
            seal(
                FunctionDefinition::try_new_pure_window(
                    "fixture_window",
                    FunctionVisibility::Public,
                    owner.clone(),
                )
                .unwrap(),
                owner.manifest(),
            )
            .unwrap()
        };
        let call = Call::new(&owner, 0);
        let options = if aggregate {
            over_prepare(&call)
        } else {
            PureCallPreparation::Window {
                arguments: call.children(),
                options: window_options(),
            }
        };
        let token = catalog
            .prepare_frozen(
                call.input(),
                call.selected.clone(),
                &owner.frozen(&call.selected),
                options,
                &COMPILE,
            )
            .unwrap();
        let resolved = ProgramResolvedCalls::try_new(
            snapshot(window_program(
                &owner,
                aggregate,
                Some(local_frame()),
                false,
            )),
            vec![(window_site(), token)],
            &COMPILE,
        )
        .unwrap();
        let entry = &resolved.calls()[&window_site()];
        assert_eq!(entry.call_contract().kind(), kind);
        let ProgramStateTemplate::WindowPartition {
            node,
            call: ordinal,
            kernel,
        } = entry.state_template()
        else {
            panic!("partition lifecycle")
        };
        assert_eq!((node, ordinal), (ProgramNodeId::new(1), 0));
        let PreparedPureKernel::Window(actual) = entry.specialization().prepared() else {
            panic!("actual partition")
        };
        assert!(std::ptr::eq(kernel, actual));
        assert_eq!(kernel.contract().aggregate().is_some(), aggregate);
        run_window(kernel.clone(), if aggregate { &[3, 12] } else { &[3, 5] });
        assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.instance_drop.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 0);
    }
}

pub(crate) fn table_program(
    param_type: DataType,
    result_type: DataType,
    param_slot: u32,
) -> LocalProgramGraph {
    let input = layout(1, DataType::Int64);
    let output = layout(2, result_type.clone());
    program(
        vec![
            source(&input),
            ProgramNode::new(
                20,
                ProgramNodeKind::TableFunction {
                    input: ProgramNodeId::new(0),
                    function_name: "display_table_alias".into(),
                    param_slots: vec![SlotId::new(param_slot)],
                    outer_slots: vec![],
                    fn_result_slots: vec![SlotId::new(2)],
                    fn_result_required: true,
                    is_left_join: false,
                    param_types: vec![param_type],
                    ret_types: vec![result_type],
                    output_slot_sources: vec![TableFunctionOutputSlot::Result { index: 0 }],
                },
                output,
            ),
        ],
        literal_arena(DataType::Int64),
        vec![],
    )
}
pub(crate) fn table_fixture() -> (Arc<Owner>, PureEngineFunctionCatalog) {
    let owner = Arc::new(Owner::new(FunctionKind::Table, &[PureKernelAbi::TableV1]));
    let catalog = seal(
        FunctionDefinition::try_new_pure_table(
            "fixture_table",
            FunctionVisibility::Public,
            owner.clone(),
        )
        .unwrap(),
        owner.manifest(),
    )
    .unwrap();
    (owner, catalog)
}
pub(crate) fn prepare_table_token(
    catalog: &PureEngineFunctionCatalog,
    call: &Call<'_>,
) -> PureCallSpecialization {
    catalog
        .prepare_frozen(
            call.input(),
            call.selected.clone(),
            &call.owner.frozen(&call.selected),
            PureCallPreparation::Table {
                arguments: call.children(),
            },
            &COMPILE,
        )
        .unwrap()
}
pub(crate) fn table_site() -> ProgramCallSite {
    ProgramCallSite::Table {
        node: ProgramNodeId::new(1),
    }
}
#[test]
fn table_scope_borrows_the_exact_frozen_cursor_and_samples_only_selected_parent_rows() {
    let (owner, catalog) = table_fixture();
    let call = Call::new(&owner, 0);
    let token = prepare_table_token(&catalog, &call);
    let resolved = ProgramResolvedCalls::try_new(
        snapshot(table_program(DataType::Int64, DataType::Int64, 1)),
        vec![(table_site(), token)],
        &COMPILE,
    )
    .unwrap();
    assert!(
        resolved.snapshot().flows()[&ProgramExpressionArena::Main]
            .uses()
            .is_empty()
    );
    let entry = &resolved.calls()[&table_site()];
    let ProgramStateTemplate::TableCursor { node, kernel } = entry.state_template() else {
        panic!("cursor template")
    };
    assert_eq!(node, ProgramNodeId::new(1));
    let PreparedPureKernel::Table(actual) = entry.specialization().prepared() else {
        panic!("actual table")
    };
    assert!(std::ptr::eq(kernel, actual));
    let contract = kernel.contract().clone();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![3, 999, 5]));
    let args = [EvaluatedArgument::Column(&array)];
    let rows = [0, 2];
    let input = SelectedTableInput::try_new(
        &contract,
        Selection::try_sparse(3, &rows).unwrap(),
        &args,
        &RUNTIME,
    )
    .unwrap();
    let mut cursor = TableEvaluationCursor::begin(kernel.clone(), input, &RUNTIME).unwrap();
    let capacity = TableStepCapacity {
        page: TablePageCapacity {
            rows: 1,
            completions: 1,
        },
        parent_errors: 0,
    };
    for (ordinal, value, eof) in [(0, 3, false), (1, 5, true)] {
        let TableCursorStep::Page(page) = cursor.next(capacity, &RUNTIME).unwrap() else {
            panic!("actual CPU page")
        };
        assert_eq!(integers(&page.columns[0]), [value]);
        assert_eq!(page.parent_ordinals.as_ref(), [ordinal]);
        assert_eq!(page.completed_parents.as_ref(), [ordinal]);
        assert_eq!(page.eof, eof);
    }
    cursor.finish(&RUNTIME).unwrap();
    drop(cursor);
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.instance_drop.load(Ordering::Relaxed), 1);
}

pub(crate) fn writer_program(
    owner: &Owner,
    partial_input_slot: u32,
    final_input_slot: u32,
) -> LocalProgramGraph {
    let data = layout(1, DataType::Int64);
    let intermediate = layout(2, DataType::Int64);
    let final_layout = layout(999, DataType::Int64);
    let arena = literal_arena(DataType::Int64);
    let target = WriteTargetOrdinal::try_new(0).unwrap();
    let nodes = vec![
        source(&data),
        ProgramNode::new(
            20,
            ProgramNodeKind::TableWriter {
                input: ProgramNodeId::new(0),
                target,
                expected_layout: data.clone(),
                projection: StaticWriterProjection {
                    arena: arena.clone(),
                    expressions: vec![ProgramExprId::new(0)],
                    layout: data,
                },
                writer_multiplex_layout: intermediate.clone(),
                partial_aggregates: vec![WriterPartialAggregateCall {
                    frozen: Some(frozen_selection(owner)),
                    input_slot_id: SlotId::new(partial_input_slot),
                    function_name: "writer_alias".into(),
                    resolved: resolved_signature(owner),
                    intermediate_slot_id: SlotId::new(2),
                }],
                partial_preparation: None,
            },
            intermediate.clone(),
        ),
        ProgramNode::new(
            30,
            ProgramNodeKind::TableFinish {
                count_mode: novarocks_result_contract::FinishCountMode::WriterReported,
                input_roles: novarocks_result_contract::FinishInputs {
                    writer_results: 0,
                    summaries: Default::default(),
                },
                logical_summary_layout: None,
                inputs: vec![ProgramNodeId::new(1)],
                expected_targets: vec![target],
                writer_multiplex_layout: intermediate.clone(),
                root_result_layout: final_layout.clone(),
                final_aggregates: WriterFinalAggregatePlan {
                    prepared: None,
                    calls: vec![WriterFinalAggregateCall {
                        frozen: Some(frozen_selection(owner)),
                        function_name: "finish_alias".into(),
                        resolved: resolved_signature(owner),
                        intermediate_input_slot_id: SlotId::new(final_input_slot),
                        final_output_slot_id: SlotId::new(999),
                    }],
                    unpivot: None,
                },
            },
            final_layout.clone(),
        ),
    ];
    program(
        nodes,
        arena,
        vec![
            BindingRequirement::TableWriter {
                node: ProgramNodeId::new(1),
                layout: intermediate,
            },
            BindingRequirement::TableFinish {
                node: ProgramNodeId::new(2),
                layout: final_layout,
            },
        ],
    )
}
pub(crate) fn partial_site() -> ProgramCallSite {
    ProgramCallSite::WriterPartial {
        node: ProgramNodeId::new(1),
        call: 0,
    }
}
pub(crate) fn final_site() -> ProgramCallSite {
    ProgramCallSite::WriterFinal {
        node: ProgramNodeId::new(2),
        call: 0,
    }
}
pub(crate) fn writer_tokens(
    catalog: &PureEngineFunctionCatalog,
    owner: &Owner,
) -> Vec<(ProgramCallSite, PureCallSpecialization)> {
    let mut partial = Call::new(owner, 0);
    partial.context_id = u32::MAX - 1;
    let final_call = Call::new(owner, 0);
    vec![
        (
            partial_site(),
            prepare_aggregate_token(catalog, &partial, AggregateKernelPhase::Partial),
        ),
        (
            final_site(),
            prepare_aggregate_token(catalog, &final_call, AggregateKernelPhase::Final),
        ),
    ]
}
#[test]
fn writer_partial_and_final_keep_exact_state_scopes_without_requiring_internal_final_slot_in_input()
{
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateWindowV1],
    ));
    let catalog = aggregate_catalog(owner.clone());
    let program = writer_program(&owner, 1, 2);
    let ProgramNodeKind::TableFinish {
        writer_multiplex_layout,
        final_aggregates,
        ..
    } = program.nodes()[2].kind()
    else {
        unreachable!()
    };
    assert_eq!(
        final_aggregates.calls[0].final_output_slot_id,
        SlotId::new(999)
    );
    assert!(!writer_multiplex_layout.slots().contains(&SlotId::new(999)));
    let resolved =
        ProgramResolvedCalls::try_new(snapshot(program), writer_tokens(&catalog, &owner), &COMPILE)
            .unwrap();
    for (site, expected_scope, phase) in [
        (
            partial_site(),
            ProgramAggregateCallScope::WriterPartial {
                node: ProgramNodeId::new(1),
                call: 0,
            },
            AggregateKernelPhase::Partial,
        ),
        (
            final_site(),
            ProgramAggregateCallScope::WriterFinal {
                node: ProgramNodeId::new(2),
                call: 0,
            },
            AggregateKernelPhase::Final,
        ),
    ] {
        let entry = &resolved.calls()[&site];
        let ProgramStateTemplate::Aggregate { scope, kernel } = entry.state_template() else {
            panic!("writer state template")
        };
        assert_eq!(scope, expected_scope);
        let PreparedPureKernel::Aggregate(actual) = entry.specialization().prepared() else {
            panic!("writer aggregate")
        };
        assert!(std::ptr::eq(kernel, actual));
        run_aggregate(kernel, phase);
    }
    assert_eq!(owner.counts.state_create.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.state_drop.load(Ordering::Relaxed), 2);
}

#[test]
fn relational_sites_reject_wrong_phase_ordinal_order_coverage_and_lifecycle() {
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateWindowV1],
    ));
    let catalog = aggregate_catalog(owner.clone());
    let call = Call::new(&owner, 0);
    let make = || {
        snapshot(aggregate_node(
            &owner,
            false,
            false,
            StaticAggregateOrder::default(),
            0,
        ))
    };
    let single = prepare_aggregate_token(&catalog, &call, AggregateKernelPhase::Single);
    assert_eq!(
        ProgramResolvedCalls::try_new(make(), vec![(aggregate_site(), single)], &COMPILE)
            .unwrap_err(),
        ProgramResolvedCallsError::WrongPhase
    );
    let partial = prepare_aggregate_token(&catalog, &call, AggregateKernelPhase::Partial);
    assert_eq!(
        ProgramResolvedCalls::try_new(
            make(),
            vec![(
                ProgramCallSite::Aggregate {
                    node: ProgramNodeId::new(1),
                    call: 1
                },
                partial.clone()
            )],
            &COMPILE
        )
        .unwrap_err(),
        ProgramResolvedCallsError::MissingSite(aggregate_site())
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(make(), vec![], &COMPILE).unwrap_err(),
        ProgramResolvedCallsError::MissingSite(aggregate_site())
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(
            make(),
            vec![
                (aggregate_site(), partial.clone()),
                (aggregate_site(), partial.clone())
            ],
            &COMPILE
        )
        .unwrap_err(),
        ProgramResolvedCallsError::DuplicateSite
    );
    let order = StaticAggregateOrder {
        is_asc_order: vec![false],
        nulls_first: vec![true],
        ..Default::default()
    };
    assert_eq!(
        ProgramResolvedCalls::try_new(
            snapshot(aggregate_node(&owner, false, false, order, 0)),
            vec![(aggregate_site(), partial.clone())],
            &COMPILE
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongOrder
    );
    let over = catalog
        .prepare_frozen(
            call.input(),
            call.selected.clone(),
            &owner.frozen(&call.selected),
            over_prepare(&call),
            &COMPILE,
        )
        .unwrap();
    assert_eq!(
        ProgramResolvedCalls::try_new(make(), vec![(aggregate_site(), over)], &COMPILE)
            .unwrap_err(),
        ProgramResolvedCallsError::WrongLifecycle
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(
            snapshot(window_program(&owner, true, Some(local_frame()), false)),
            vec![(window_site(), partial)],
            &COMPILE
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongLifecycle
    );
}
#[test]
fn wrong_window_geometry_and_table_ordered_carriers_are_rejected() {
    let owner = Arc::new(Owner::new(FunctionKind::Window, &[PureKernelAbi::WindowV1]));
    let catalog = seal(
        FunctionDefinition::try_new_pure_window(
            "fixture_window",
            FunctionVisibility::Public,
            owner.clone(),
        )
        .unwrap(),
        owner.manifest(),
    )
    .unwrap();
    let call = Call::new(&owner, 0);
    let token = catalog
        .prepare_frozen(
            call.input(),
            call.selected.clone(),
            &owner.frozen(&call.selected),
            PureCallPreparation::Window {
                arguments: call.children(),
                options: window_options(),
            },
            &COMPILE,
        )
        .unwrap();
    for p in [
        window_program(&owner, false, None, false),
        window_program(&owner, false, Some(local_frame()), true),
    ] {
        assert_eq!(
            ProgramResolvedCalls::try_new(
                snapshot(p),
                vec![(window_site(), token.clone())],
                &COMPILE
            )
            .unwrap_err(),
            ProgramResolvedCallsError::WrongWindow
        );
    }
    let (owner, catalog) = table_fixture();
    let call = Call::new(&owner, 0);
    let token = prepare_table_token(&catalog, &call);
    for (p, error) in [
        (
            table_program(DataType::Int32, DataType::Int64, 1),
            ProgramResolvedCallsError::TypeMismatch,
        ),
        (
            table_program(DataType::Int64, DataType::Int32, 1),
            ProgramResolvedCallsError::TypeMismatch,
        ),
        (
            table_program(DataType::Int64, DataType::Int64, 99),
            ProgramResolvedCallsError::WrongArguments,
        ),
    ] {
        assert_eq!(
            ProgramResolvedCalls::try_new(
                snapshot(p),
                vec![(table_site(), token.clone())],
                &COMPILE
            )
            .unwrap_err(),
            error
        );
    }
}
#[test]
fn writer_wrong_phase_and_absent_actual_input_slot_fail_while_internal_output_is_not_an_input() {
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateWindowV1],
    ));
    let catalog = aggregate_catalog(owner.clone());
    for p in [writer_program(&owner, 99, 2), writer_program(&owner, 1, 99)] {
        assert_eq!(
            ProgramResolvedCalls::try_new(snapshot(p), writer_tokens(&catalog, &owner), &COMPILE)
                .unwrap_err(),
            ProgramResolvedCallsError::WrongArguments
        );
    }
    let mut tokens = writer_tokens(&catalog, &owner);
    let mut call = Call::new(&owner, 0);
    call.context_id = u32::MAX - 1;
    tokens[0].1 = prepare_aggregate_token(&catalog, &call, AggregateKernelPhase::Single);
    assert_eq!(
        ProgramResolvedCalls::try_new(snapshot(writer_program(&owner, 1, 2)), tokens, &COMPILE)
            .unwrap_err(),
        ProgramResolvedCallsError::WrongPhase
    );
    let mut tokens = writer_tokens(&catalog, &owner);
    tokens[1].1 = prepare_aggregate_token(
        &catalog,
        &Call::new(&owner, 0),
        AggregateKernelPhase::Intermediate,
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(snapshot(writer_program(&owner, 1, 2)), tokens, &COMPILE)
            .unwrap_err(),
        ProgramResolvedCallsError::WrongPhase
    );
    let call = Call::new(&owner, 0);
    let shared = vec![
        (
            partial_site(),
            prepare_aggregate_token(&catalog, &call, AggregateKernelPhase::Partial),
        ),
        (
            final_site(),
            prepare_aggregate_token(&catalog, &call, AggregateKernelPhase::Final),
        ),
    ];
    assert_eq!(
        ProgramResolvedCalls::try_new(snapshot(writer_program(&owner, 1, 2)), shared, &COMPILE)
            .unwrap_err(),
        ProgramResolvedCallsError::SharedUse
    );
}

struct Stop {
    at: u32,
    error: CompileControlError,
    observed: Mutex<Vec<u32>>,
}
impl PureCompileControl for Stop {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::LowerProgram);
        assert!(work <= 256);
        self.observed.lock().unwrap().push(work);
        if work == self.at {
            Err(self.error.clone())
        } else {
            Ok(())
        }
    }
}
#[test]
fn resolved_relational_entry_and_positive_quantum_keep_all_three_typed_controls() {
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateWindowV1],
    ));
    let catalog = aggregate_catalog(owner.clone());
    let call = Call::new(&owner, 0);
    let token = prepare_aggregate_token(&catalog, &call, AggregateKernelPhase::Partial);
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [0, 256] {
            let stop = Stop {
                at,
                error: error.clone(),
                observed: Mutex::default(),
            };
            let result = ProgramResolvedCalls::try_new(
                snapshot(aggregate_node(
                    &owner,
                    false,
                    false,
                    StaticAggregateOrder::default(),
                    300,
                )),
                vec![(aggregate_site(), token.clone())],
                &stop,
            );
            assert_eq!(
                result.unwrap_err(),
                ProgramResolvedCallsError::Control(error.clone())
            );
            assert!(stop.observed.lock().unwrap().contains(&at));
        }
    }
}
#[test]
fn graph_reference_and_actual_relational_call_share_one_combined_near_over_budget() {
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateWindowV1],
    ));
    let catalog = aggregate_catalog(owner.clone());
    let call = Call::new(&owner, 0);
    let token = prepare_aggregate_token(&catalog, &call, AggregateKernelPhase::Partial);
    // One actual argument root plus group occurrences, then one actual call.
    let near = snapshot(aggregate_node(
        &owner,
        false,
        false,
        StaticAggregateOrder::default(),
        MAX_CONTROL_USE_REFERENCES - 2,
    ));
    assert_eq!(
        near.flows()[&ProgramExpressionArena::Main].use_reference_count(),
        MAX_CONTROL_USE_REFERENCES - 1
    );
    ProgramResolvedCalls::try_new(near, vec![(aggregate_site(), token.clone())], &COMPILE).unwrap();
    let over = snapshot(aggregate_node(
        &owner,
        false,
        false,
        StaticAggregateOrder::default(),
        MAX_CONTROL_USE_REFERENCES - 1,
    ));
    assert_eq!(
        over.flows()[&ProgramExpressionArena::Main].use_reference_count(),
        MAX_CONTROL_USE_REFERENCES
    );
    assert_eq!(
        ProgramResolvedCalls::try_new(over, vec![(aggregate_site(), token)], &COMPILE).unwrap_err(),
        ProgramResolvedCallsError::TooManyItems
    );
}

#[test]
fn prepared_window_calls_carry_their_own_explicit_frame() {
    let owner = Arc::new(Owner::new(FunctionKind::Window, &[PureKernelAbi::WindowV1]));
    let catalog = seal(
        FunctionDefinition::try_new_pure_window(
            "fixture_window",
            FunctionVisibility::Public,
            owner.clone(),
        )
        .unwrap(),
        owner.manifest(),
    )
    .unwrap();
    let call = Call::new(&owner, 0);
    let prepare = |options| {
        catalog
            .prepare_frozen(
                call.input(),
                call.selected.clone(),
                &owner.frozen(&call.selected),
                PureCallPreparation::Window {
                    arguments: call.children(),
                    options,
                },
                &COMPILE,
            )
            .unwrap()
    };
    // Explicit ROWS UNBOUNDED PRECEDING .. CURRENT ROW, and an absent frame.
    let explicit = prepare(window_options());
    let absent = prepare(WindowCallOptions::try_new(None, false, &COMPILE).unwrap());
    let running = WindowFrame {
        start: None,
        end: Some(WindowBoundary::CurrentRow),
        window_type: WindowType::Range,
    };
    let offset = WindowFrame {
        start: Some(WindowBoundary::Preceding(1)),
        end: Some(WindowBoundary::CurrentRow),
        window_type: WindowType::Rows,
    };
    let resolve = |frame, token: &PureCallSpecialization| {
        ProgramResolvedCalls::try_new(
            snapshot(prepared_window_graph(Some(frame)).unwrap()),
            vec![(window_site(), token.clone())],
            &COMPILE,
        )
    };
    // An explicit prepared frame is the call's frame exactly; an absent one
    // stands for the compiler's offset-free default derivation.
    resolve(local_frame(), &explicit).unwrap();
    resolve(running, &absent).unwrap();
    for (frame, token) in [(running, &explicit), (offset, &absent)] {
        assert_eq!(
            resolve(frame, token).unwrap_err(),
            ProgramResolvedCallsError::WrongWindow
        );
    }
    // A legacy call keeps exact equality: its explicit frame never stands for
    // an absent prepared one.
    assert_eq!(
        ProgramResolvedCalls::try_new(
            snapshot(window_program(&owner, false, Some(local_frame()), false)),
            vec![(window_site(), absent.clone())],
            &COMPILE,
        )
        .unwrap_err(),
        ProgramResolvedCallsError::WrongWindow
    );
    // A prepared call always carries its explicit frame.
    assert_eq!(
        prepared_window_graph(None).unwrap_err(),
        LocalProgramError::InvalidNodeShape
    );
}

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

//! Real CPU fixtures for atomic catalogue ownership. These do not install
//! production kernels, authorize MEM allocations or prove native acceptance.

use super::tests::{assert_preparation_provenance, checked_into_parts};
use super::*;
use arrow_array::{ArrayRef, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileControlError, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, SemanticParameters, WindowBound,
    WindowFrame, WindowFrameExclusion, WindowFrameUnits,
};
use std::{
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

struct Compile(Option<CompileControlError>);
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert!(work <= 256);
        self.0.map_or(Ok(()), Err)
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
struct Counts {
    resolve: AtomicUsize,
    legacy: AtomicUsize,
    refine: AtomicUsize,
    aggregate: AtomicUsize,
    window_prepare: AtomicUsize,
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
    FunctionValueType::from_legacy_declared_projection(data_type, false)
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

struct Owner {
    declaration: FunctionBindingDeclaration,
    implementations: Vec<PureImplementationDeclaration>,
    selections: Vec<Arc<FunctionBindingSelection>>,
    counts: Arc<Counts>,
    window_prepare_failure: Option<KernelFailure>,
    window_foreign_contract: bool,
}
impl Owner {
    fn new(kind: FunctionKind, abis: &[PureKernelAbi]) -> Self {
        let id = FunctionId::try_new(format!("fixture/catalogue/{kind:?}-v1")).unwrap();
        let mut overloads = Vec::new();
        let mut implementations = Vec::new();
        let mut selections = Vec::new();
        for (ordinal, &abi) in abis.iter().enumerate() {
            let overload = FunctionOverloadId::try_new(format!("fixture/typed-{ordinal}")).unwrap();
            let argument = value_type(if kind == FunctionKind::Aggregate && ordinal == 0 {
                DataType::Int32
            } else {
                DataType::Int64
            });
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
            window_prepare_failure: None,
            window_foreign_contract: false,
        }
    }
    fn frozen(&self, selected: &FunctionBindingSelection) -> CallEffects {
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
    fn manifest(&self) -> Vec<InstalledPureKernel> {
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

struct Call<'owner> {
    owner: &'owner Owner,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: [Option<ExpressionUseId>; 1],
    parameters: SemanticParameters,
}
impl<'owner> Call<'owner> {
    fn new(owner: &'owner Owner, ordinal: usize) -> Self {
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
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                domain: EvaluationDomainId::new(1),
                demand: EvaluationDemand::Value,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
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
    fn children(&self) -> ScopedExpressionEffects {
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
fn window_options() -> WindowCallOptions {
    WindowCallOptions::try_new(
        Some(WindowFrame {
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
fn aggregate_prepare(call: &Call<'_>, phase: AggregateKernelPhase) -> PureCallPreparation {
    PureCallPreparation::Aggregate {
        arguments: call.children(),
        options: aggregate_options(phase),
    }
}
fn over_prepare(call: &Call<'_>) -> PureCallPreparation {
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
struct Sum {
    contract: Arc<AggregateCallContract>,
    counts: Arc<Counts>,
}
struct SumState {
    value: i64,
    counts: Arc<Counts>,
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
    fn prepare_aggregate_for_window(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Sum>, KernelFailure> {
        self.counts.window_prepare.fetch_add(1, Ordering::Relaxed);
        if let Some(failure) = &self.window_prepare_failure {
            return Err(failure.clone());
        }
        let contract = if self.window_foreign_contract {
            Arc::new((*contract).clone())
        } else {
            contract
        };
        self.prepare_aggregate(input, contract, control)
    }

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
    counts: Arc<Counts>,
}
struct Partition {
    results: Vec<i64>,
    counts: Arc<Counts>,
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
        .map_err(|_| crate::kernel_control::internal("fixture window output invalid"))
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
    counts: Arc<Counts>,
}
struct Cursor<'a> {
    input: SelectedTableInput<'a, 'a>,
    counts: Arc<Counts>,
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
fn aggregate_catalog(owner: Arc<Owner>) -> PureEngineFunctionCatalog {
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
fn integers(output: &ArrayRef) -> Vec<i64> {
    output
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .values()
        .to_vec()
}

#[repr(C, align(64))]
struct Storage([MaybeUninit<u8>; 128]);

#[test]
fn registered_typed_aggregate_erases_real_cpu_and_drops_real_state() {
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateV1],
    ));
    let catalog = aggregate_catalog(owner.clone());
    let call = Call::new(&owner, 0);
    let legacy = catalog
        .metadata()
        .resolve_aggregate_user("typed_sum", &[DataType::Int32])
        .unwrap();
    assert_eq!(legacy.overload.as_str(), call.selected.overload.as_str());
    assert_eq!(legacy.state_format, state_format());
    assert_eq!(owner.counts.legacy.load(Ordering::Relaxed), 1);
    let bound = catalog
        .metadata()
        .resolve_bound_user(
            "typed_sum",
            FunctionKind::Aggregate,
            call.input().request,
            crate::binding_test_control(),
        )
        .unwrap();
    assert_eq!(bound.selected, *call.selected);
    let resolves = owner.counts.resolve.load(Ordering::Relaxed);
    let prepared = catalog
        .prepare_frozen(
            call.input(),
            call.selected.clone(),
            &owner.frozen(&call.selected),
            aggregate_prepare(&call, AggregateKernelPhase::Partial),
            &COMPILE,
        )
        .unwrap();
    assert_preparation_provenance(
        &prepared,
        &owner.implementations[0],
        PurePreparationSource::Frozen,
    );
    // A state initialized through the pre-consumption clone must remain usable
    // by the moved handle; equal call contracts alone do not prove ownership.
    let PreparedPureKernel::Aggregate(original_handle) = prepared.prepared().clone() else {
        panic!("aggregate handle expected");
    };
    let PreparedPureKernel::Aggregate(handle) =
        checked_into_parts(prepared, call.input(), &owner.frozen(&call.selected))
    else {
        panic!("aggregate handle expected");
    };
    assert_eq!(handle.contract().phase(), AggregateKernelPhase::Partial);
    assert_eq!(handle.state_layout(), std::alloc::Layout::new::<SumState>());
    let mut storage = Storage([MaybeUninit::uninit(); 128]);
    let mut states = [original_handle
        .initialize_in(&mut storage.0, &RUNTIME)
        .unwrap()];
    let input: ArrayRef = Arc::new(Int32Array::from(vec![3, 900, 5]));
    let arguments = [EvaluatedArgument::Column(&input)];
    let rows = [0, 2];
    let selection = Selection::try_sparse(3, &rows).unwrap();
    let checked = SelectedAggregateUpdateInput::try_new(
        handle.contract(),
        selection,
        &arguments,
        &[],
        &RUNTIME,
    )
    .unwrap();
    {
        let mut frame = handle
            .prepare_update_batch(&mut states, &[0, 0], checked, &RUNTIME)
            .unwrap();
        frame.run(&RUNTIME).unwrap();
        assert_eq!(frame.rows_processed(), 2);
    }
    assert_eq!(
        integers(&handle.emit(&states, &[0], 1, &RUNTIME).unwrap()),
        [8]
    );
    drop(states);
    assert_eq!(owner.counts.state_create.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.state_drop.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.update.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), resolves);
}

fn run_window(prepared: Arc<dyn PreparedWindowKernel>, values: &[i64], expected: &[i64]) {
    let array: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
    let arguments = [EvaluatedArgument::Column(&array)];
    let contract = prepared.contract().clone();
    let full =
        FullPartitionWindowInput::try_new(&contract, values.len(), &arguments, &[], &RUNTIME)
            .unwrap();
    let peers = [WindowRowRange {
        start: 0,
        end: values.len(),
    }];
    let frames = (0..values.len())
        .map(|row| WindowRowRange {
            start: 0,
            end: row + 1,
        })
        .collect::<Vec<_>>();
    let input = WindowPartitionInput::try_new(full, &peers, &frames, &RUNTIME).unwrap();
    let mut partition = WindowEvaluationPartition::begin(prepared, input, &RUNTIME).unwrap();
    let rows = [0, values.len() - 1];
    let output = partition
        .evaluate(
            Selection::try_sparse(values.len(), &rows).unwrap(),
            2,
            &RUNTIME,
        )
        .unwrap();
    assert_eq!(integers(output.values()), expected);
    partition.finish(&RUNTIME).unwrap();
}

#[test]
fn mixed_aggregate_capabilities_gate_over_before_private_prepare() {
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateV1, PureKernelAbi::AggregateWindowV1],
    ));
    assert!(matches!(
        FunctionDefinition::try_new_pure_aggregate(
            "typed_sum",
            FunctionVisibility::Public,
            owner.clone()
        ),
        Err(PureCatalogError::InvalidAbi { .. })
    ));
    let catalog = aggregate_catalog(owner.clone());
    let basic = Call::new(&owner, 0);
    assert!(matches!(
        catalog.prepare_fresh(
            basic.input(),
            basic.selected.clone(),
            over_prepare(&basic),
            &COMPILE
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.adapter.load(Ordering::Relaxed), 0);
    let window = Call::new(&owner, 1);
    for frozen in [false, true] {
        let specialization = if frozen {
            catalog.prepare_frozen(
                window.input(),
                window.selected.clone(),
                &owner.frozen(&window.selected),
                over_prepare(&window),
                &COMPILE,
            )
        } else {
            catalog.prepare_fresh(
                window.input(),
                window.selected.clone(),
                over_prepare(&window),
                &COMPILE,
            )
        }
        .unwrap();
        let source = if frozen {
            PurePreparationSource::Frozen
        } else {
            PurePreparationSource::Fresh
        };
        assert_preparation_provenance(&specialization, &owner.implementations[1], source);
        let ordinary = if frozen {
            catalog.prepare_frozen(
                window.input(),
                window.selected.clone(),
                &owner.frozen(&window.selected),
                aggregate_prepare(&window, AggregateKernelPhase::Single),
                &COMPILE,
            )
        } else {
            catalog.prepare_fresh(
                window.input(),
                window.selected.clone(),
                aggregate_prepare(&window, AggregateKernelPhase::Single),
                &COMPILE,
            )
        }
        .unwrap();
        assert_preparation_provenance(&ordinary, &owner.implementations[1], source);
        assert_eq!(
            ordinary.implementation().abi,
            PureKernelAbi::AggregateWindowV1
        );
        assert_eq!(
            specialization.implementation().abi,
            PureKernelAbi::AggregateWindowV1
        );
        assert!(std::ptr::eq(
            ordinary.implementation(),
            specialization.implementation()
        ));
        assert!(matches!(
            ordinary.prepared(),
            PreparedPureKernel::Aggregate(_)
        ));
        assert!(matches!(
            specialization.prepared(),
            PreparedPureKernel::Window(_)
        ));
        let PreparedPureKernel::Aggregate(ordinary_handle) =
            checked_into_parts(ordinary, window.input(), &owner.frozen(&window.selected))
        else {
            panic!("ordinary aggregate handle expected");
        };
        assert_eq!(
            ordinary_handle.contract().phase(),
            AggregateKernelPhase::Single
        );
        let PreparedPureKernel::Window(handle) = checked_into_parts(
            specialization,
            window.input(),
            &owner.frozen(&window.selected),
        ) else {
            panic!("window handle expected");
        };
        assert_eq!(handle.contract().call().kind(), FunctionKind::Aggregate);
        assert_eq!(
            handle.contract().aggregate().unwrap().phase(),
            AggregateKernelPhase::Single
        );
        run_window(handle, &[2, 7, 4], &[2, 13]);
    }
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 4);
    assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), 4);
    assert_eq!(owner.counts.adapter.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.window.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.state_create.load(Ordering::Relaxed), 6);
    assert_eq!(owner.counts.state_drop.load(Ordering::Relaxed), 6);
    assert_eq!(owner.counts.instance_drop.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 0);
}

#[test]
fn pure_window_fresh_and_frozen_prepare_real_partition() {
    let owner = Arc::new(Owner::new(FunctionKind::Window, &[PureKernelAbi::WindowV1]));
    let catalog = seal(
        FunctionDefinition::try_new_pure_window(
            "typed_window",
            FunctionVisibility::Public,
            owner.clone(),
        )
        .unwrap(),
        owner.manifest(),
    )
    .unwrap();
    let call = Call::new(&owner, 0);
    for frozen in [false, true] {
        let options = PureCallPreparation::Window {
            arguments: call.children(),
            options: window_options(),
        };
        let specialization = if frozen {
            catalog.prepare_frozen(
                call.input(),
                call.selected.clone(),
                &owner.frozen(&call.selected),
                options,
                &COMPILE,
            )
        } else {
            catalog.prepare_fresh(call.input(), call.selected.clone(), options, &COMPILE)
        }
        .unwrap();
        let source = if frozen {
            PurePreparationSource::Frozen
        } else {
            PurePreparationSource::Fresh
        };
        assert_preparation_provenance(&specialization, &owner.implementations[0], source);
        let PreparedPureKernel::Window(handle) =
            checked_into_parts(specialization, call.input(), &owner.frozen(&call.selected))
        else {
            panic!("window handle expected");
        };
        assert!(std::ptr::eq(
            handle.contract().call().selected(),
            call.selected.as_ref()
        ));
        run_window(handle, &[4, 8, 11], &[4, 11]);
    }
    assert_eq!(owner.counts.window.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.instance_drop.load(Ordering::Relaxed), 2);
}

#[test]
fn pure_table_fresh_and_frozen_prepare_selected_cursor() {
    let owner = Arc::new(Owner::new(FunctionKind::Table, &[PureKernelAbi::TableV1]));
    let catalog = seal(
        FunctionDefinition::try_new_pure_table(
            "typed_table",
            FunctionVisibility::Public,
            owner.clone(),
        )
        .unwrap(),
        owner.manifest(),
    )
    .unwrap();
    let call = Call::new(&owner, 0);
    for frozen in [false, true] {
        let options = PureCallPreparation::Table {
            arguments: call.children(),
        };
        let specialization = if frozen {
            catalog.prepare_frozen(
                call.input(),
                call.selected.clone(),
                &owner.frozen(&call.selected),
                options,
                &COMPILE,
            )
        } else {
            catalog.prepare_fresh(call.input(), call.selected.clone(), options, &COMPILE)
        }
        .unwrap();
        let source = if frozen {
            PurePreparationSource::Frozen
        } else {
            PurePreparationSource::Fresh
        };
        assert_preparation_provenance(&specialization, &owner.implementations[0], source);
        let PreparedPureKernel::Table(handle) =
            checked_into_parts(specialization, call.input(), &owner.frozen(&call.selected))
        else {
            panic!("table handle expected");
        };
        assert!(std::ptr::eq(
            handle.contract().call().selected(),
            call.selected.as_ref()
        ));
        let contract = handle.contract().clone();
        let array: ArrayRef = Arc::new(Int64Array::from(vec![4, 999, 8]));
        let arguments = [EvaluatedArgument::Column(&array)];
        let rows = [0, 2];
        let input = SelectedTableInput::try_new(
            &contract,
            Selection::try_sparse(3, &rows).unwrap(),
            &arguments,
            &RUNTIME,
        )
        .unwrap();
        let mut cursor = TableEvaluationCursor::begin(handle, input, &RUNTIME).unwrap();
        let capacity = TableStepCapacity {
            page: TablePageCapacity {
                rows: 2,
                completions: 2,
            },
            parent_errors: 0,
        };
        let TableCursorStep::Page(page) = cursor.next(capacity, &RUNTIME).unwrap() else {
            panic!("actual page expected");
        };
        assert_eq!(integers(&page.columns[0]), [4, 8]);
        assert_eq!(page.parent_ordinals.as_ref(), [0, 1]);
        assert_eq!(page.completed_parents.as_ref(), [0, 1]);
        assert!(page.eof && page.parent_errors.is_empty());
        cursor.finish(&RUNTIME).unwrap();
    }
    assert_eq!(owner.counts.table.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.instance_drop.load(Ordering::Relaxed), 2);
}

#[test]
fn installed_manifest_requires_exact_aggregate_format_and_complete_coverage() {
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateV1, PureKernelAbi::AggregateWindowV1],
    ));
    for change in 0..4 {
        let mut installed = owner.manifest();
        match change {
            0 => {
                installed.pop();
            }
            1 => {
                installed[0].aggregate_state_format =
                    Some(AggregateStateFormatIdentity::try_new("fixture/wrong-format").unwrap());
            }
            2 => {
                let mut extra = installed[0].clone();
                extra.implementation.overload =
                    FunctionOverloadId::try_new("fixture/not-declared").unwrap();
                installed.push(extra);
            }
            3 => {
                installed[0].kind = FunctionKind::Window;
            }
            _ => unreachable!(),
        }
        let definition = FunctionDefinition::try_new_pure_aggregate_window(
            "typed_sum",
            FunctionVisibility::Public,
            owner.clone(),
        )
        .unwrap();
        assert!(matches!(
            seal(definition, installed),
            Err(PureCatalogError::InstalledManifestMismatch)
        ));
    }
    assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), 0);
}

#[test]
fn typed_registration_rejects_wrong_kind_and_control() {
    let table = Arc::new(Owner::new(FunctionKind::Table, &[PureKernelAbi::TableV1]));
    assert!(matches!(
        FunctionDefinition::try_new_pure_window("wrong", FunctionVisibility::Public, table),
        Err(PureCatalogError::InvalidAbi { .. })
    ));
    let scalar = Arc::new(Owner::new(FunctionKind::Scalar, &[PureKernelAbi::WindowV1]));
    assert!(matches!(
        FunctionDefinition::try_new_pure_window("wrong", FunctionVisibility::Public, scalar),
        Err(PureCatalogError::InvalidAbi { .. })
    ));
    let window = Arc::new(Owner::new(FunctionKind::Window, &[PureKernelAbi::WindowV1]));
    assert!(matches!(
        FunctionDefinition::try_new_pure_aggregate_window(
            "wrong",
            FunctionVisibility::Public,
            window
        ),
        Err(PureCatalogError::InvalidAbi { .. })
    ));
}

#[test]
fn frozen_mismatch_unknown_identity_and_control_do_not_invoke_cpu_prepare() {
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateV1],
    ));
    let catalog = aggregate_catalog(owner.clone());
    let call = Call::new(&owner, 0);
    let mut frozen = owner.frozen(&call.selected);
    frozen.failure_behavior = FunctionFailureBehavior::ReturnsNull;
    assert!(
        catalog
            .prepare_frozen(
                call.input(),
                call.selected.clone(),
                &frozen,
                aggregate_prepare(&call, AggregateKernelPhase::Single),
                &COMPILE
            )
            .is_err()
    );
    assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), 0);
    let unknown = FunctionId::try_new("fixture/unknown-function").unwrap();
    let mut input = call.input();
    input.function_id = &unknown;
    assert!(matches!(
        catalog.prepare_fresh(
            input,
            call.selected.clone(),
            aggregate_prepare(&call, AggregateKernelPhase::Single),
            &COMPILE
        ),
        Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction
        ))
    ));
    for category in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        assert!(
            matches!(catalog.prepare_frozen(call.input(), call.selected.clone(), &owner.frozen(&call.selected), aggregate_prepare(&call, AggregateKernelPhase::Single), &Compile(Some(category))), Err(FunctionSpecializationFailure::Control(error)) if error == category)
        );
    }
    assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 0);
}

#[test]
fn aggregate_abi_changes_digest_without_changing_base_or_implementation_identity() {
    let basic = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateV1],
    ));
    let window = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateWindowV1],
    ));
    assert_eq!(basic.declaration, window.declaration);
    assert_eq!(
        basic.implementations[0].overload,
        window.implementations[0].overload
    );
    assert_eq!(
        basic.implementations[0].implementation,
        window.implementations[0].implementation
    );
    let first = aggregate_catalog(basic);
    let second = aggregate_catalog(window);
    assert_ne!(first.digest(), second.digest());
}

#[test]
fn c02_p1_preparation_registered_and_direct_hooks_are_once_per_use_point() {
    for registered in [false, true] {
        for frozen in [false, true] {
            for window_first in [false, true] {
                let owner = Arc::new(Owner::new(
                    FunctionKind::Aggregate,
                    &[PureKernelAbi::AggregateWindowV1],
                ));
                let catalog = aggregate_catalog(owner.clone());
                let call = Call::new(&owner, 0);
                for (ordinal, window) in [window_first, !window_first].into_iter().enumerate() {
                    if registered {
                        let options = if window {
                            over_prepare(&call)
                        } else {
                            aggregate_prepare(&call, AggregateKernelPhase::Single)
                        };
                        let result = if frozen {
                            catalog.prepare_frozen(
                                call.input(),
                                call.selected.clone(),
                                &owner.frozen(&call.selected),
                                options,
                                &COMPILE,
                            )
                        } else {
                            catalog.prepare_fresh(
                                call.input(),
                                call.selected.clone(),
                                options,
                                &COMPILE,
                            )
                        }
                        .unwrap();
                        assert_eq!(
                            matches!(result.prepared(), PreparedPureKernel::Window(_)),
                            window
                        );
                        assert_eq!(
                            matches!(result.prepared(), PreparedPureKernel::Aggregate(_)),
                            !window
                        );
                    } else if window {
                        let options = AggregateWindowPreparationOptions {
                            aggregate: aggregate_options(AggregateKernelPhase::Single),
                            window: window_options(),
                        };
                        let result = if frozen {
                            specialize_frozen_aggregate_window(
                                owner.as_ref(),
                                call.input(),
                                call.selected.clone(),
                                &owner.frozen(&call.selected),
                                call.children(),
                                options,
                                &COMPILE,
                            )
                        } else {
                            specialize_aggregate_window(
                                owner.as_ref(),
                                call.input(),
                                call.selected.clone(),
                                call.children(),
                                options,
                                &COMPILE,
                            )
                        }
                        .unwrap();
                        assert_eq!(
                            result.prepared().contract().aggregate().unwrap().phase(),
                            AggregateKernelPhase::Single
                        );
                    } else {
                        let result = if frozen {
                            specialize_frozen_aggregate(
                                owner.as_ref(),
                                call.input(),
                                call.selected.clone(),
                                &owner.frozen(&call.selected),
                                call.children(),
                                aggregate_options(AggregateKernelPhase::Single),
                                &COMPILE,
                            )
                        } else {
                            specialize_aggregate(
                                owner.as_ref(),
                                call.input(),
                                call.selected.clone(),
                                call.children(),
                                aggregate_options(AggregateKernelPhase::Single),
                                &COMPILE,
                            )
                        }
                        .unwrap();
                        assert_eq!(
                            result.prepared().contract().phase(),
                            AggregateKernelPhase::Single
                        );
                    }
                    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), ordinal + 1);
                    assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), ordinal + 1);
                    let windows = usize::from(window_first || ordinal == 1);
                    assert_eq!(owner.counts.window_prepare.load(Ordering::Relaxed), windows);
                    assert_eq!(owner.counts.adapter.load(Ordering::Relaxed), windows);
                    assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 0);
                }
            }
        }
    }
}

#[test]
fn c02_p1_preparation_hook_failure_never_retries_ordinary_or_creates_adapter() {
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("window prepare original"),
        internal("window prepare original"),
        KernelFailure::Operational(KernelDiagnostic::new("window prepare original")),
        KernelFailure::InstanceFailed,
    ] {
        for frozen in [false, true] {
            let mut owner =
                Owner::new(FunctionKind::Aggregate, &[PureKernelAbi::AggregateWindowV1]);
            owner.window_prepare_failure = Some(failure.clone());
            let owner = Arc::new(owner);
            let catalog = aggregate_catalog(owner.clone());
            let call = Call::new(&owner, 0);
            let result = if frozen {
                catalog.prepare_frozen(
                    call.input(),
                    call.selected.clone(),
                    &owner.frozen(&call.selected),
                    over_prepare(&call),
                    &COMPILE,
                )
            } else {
                catalog.prepare_fresh(
                    call.input(),
                    call.selected.clone(),
                    over_prepare(&call),
                    &COMPILE,
                )
            };
            assert!(
                matches!(result, Err(FunctionSpecializationFailure::Kernel(actual)) if actual == failure)
            );
            assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 1);
            assert_eq!(owner.counts.window_prepare.load(Ordering::Relaxed), 1);
            assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), 0);
            assert_eq!(owner.counts.adapter.load(Ordering::Relaxed), 0);
        }
    }
}

#[test]
fn c02_p1_preparation_hook_cannot_replace_exact_aggregate_contract() {
    for frozen in [false, true] {
        let mut owner = Owner::new(FunctionKind::Aggregate, &[PureKernelAbi::AggregateWindowV1]);
        owner.window_foreign_contract = true;
        let owner = Arc::new(owner);
        let catalog = aggregate_catalog(owner.clone());
        let call = Call::new(&owner, 0);
        let result = if frozen {
            catalog.prepare_frozen(
                call.input(),
                call.selected.clone(),
                &owner.frozen(&call.selected),
                over_prepare(&call),
                &COMPILE,
            )
        } else {
            catalog.prepare_fresh(
                call.input(),
                call.selected.clone(),
                over_prepare(&call),
                &COMPILE,
            )
        };
        assert!(matches!(
            result,
            Err(FunctionSpecializationFailure::Kernel(
                KernelFailure::Internal(_)
            ))
        ));
        assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.window_prepare.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.adapter.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn c02_p1_preparation_by_domains_follow_the_same_registered_owner_use_point() {
    for name in ["max_by", "min_by"] {
        for frozen in [false, true] {
            for window_first in [false, true] {
                let catalog =
                    crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
                let arguments = [DataType::LargeUtf8, DataType::Utf8].map(|data_type| {
                    FunctionArgument::Value {
                        value_type: FunctionValueType::new(data_type, true),
                        constant: None,
                    }
                });
                let request = FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 2,
                    expected_result_type: None,
                };
                let bound = catalog
                    .resolve_bound_user(name, FunctionKind::Aggregate, request, &COMPILE)
                    .unwrap();
                let selected = Arc::new(bound.selected);
                let declaration = catalog
                    .definition(name, FunctionKind::Aggregate)
                    .unwrap()
                    .binding_declaration()
                    .unwrap()
                    .effect_declaration(&selected.overload)
                    .unwrap();
                let context = ExpressionEffectContext {
                    use_id: ExpressionUseId::new(7),
                    domain: EvaluationDomainId::new(11),
                    demand: EvaluationDemand::Value,
                };
                let parameters = SemanticParameters::default();
                let uses = [Some(ExpressionUseId::new(8)), Some(ExpressionUseId::new(9))];
                let input = CallEffectInput {
                    function_id: &bound.function_id,
                    kind: FunctionKind::Aggregate,
                    selected: &selected,
                    request,
                    argument_uses: crate::CallArgumentUses::SelectedChannels(&uses),
                    context,
                    parameters: &parameters,
                    environment: &[],
                    decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
                    proof_scope: CallProofScope::Domain(context.domain),
                };
                let effects = CallEffects {
                    value_stability: declaration.value_stability,
                    own_row_error: declaration.own_row_error,
                    failure_behavior: declaration.failure_behavior,
                    null_behavior: declaration.null_behavior,
                    argument_control: declaration.argument_control,
                    instance_state: declaration.instance_state,
                    observable_effects: declaration.observable_effects,
                    environment: Box::default(),
                    proof_scope: input.proof_scope,
                };
                for window in [window_first, !window_first] {
                    let preparation = if window {
                        PureCallPreparation::AggregateWindow {
                            arguments: ScopedExpressionEffects::pure_value(context),
                            options: AggregateWindowPreparationOptions {
                                aggregate: aggregate_options(AggregateKernelPhase::Single),
                                window: window_options(),
                            },
                        }
                    } else {
                        PureCallPreparation::Aggregate {
                            arguments: ScopedExpressionEffects::pure_value(context),
                            options: aggregate_options(AggregateKernelPhase::Single),
                        }
                    };
                    // Exercise the installed public frozen/fresh author at the
                    // original aggregate or window preparation use point.
                    let result = prepare_selected(
                        &catalog,
                        input,
                        selected.clone(),
                        frozen.then_some(&effects),
                        preparation,
                        &COMPILE,
                    );
                    if window {
                        let result = result.unwrap();
                        let PreparedPureKernel::Window(prepared) = result.prepared() else {
                            panic!("window handle");
                        };
                        assert_eq!(
                            prepared.contract().aggregate().unwrap().phase(),
                            AggregateKernelPhase::Single
                        );
                        assert_eq!(prepared.contract().call().selected(), selected.as_ref());
                    } else {
                        let expected = format!(
                            "{} has no installed max_by/min_by input profile for {}",
                            bound.function_id.as_str(),
                            "[Value(FunctionValueType { data_type: LargeUtf8, nullable: true, logical_type: Physical }), Value(FunctionValueType { data_type: Utf8, nullable: true, logical_type: Physical })]"
                        );
                        assert!(
                            matches!(&result, Err(FunctionSpecializationFailure::Kernel(KernelFailure::InvalidProgram(actual))) if actual.message() == expected),
                            "name={name}, frozen={frozen}, window_first={window_first}, selected={:?}, expected={expected:?}, error={:?}, success={}",
                            selected.argument_types,
                            result.as_ref().err(),
                            result.is_ok(),
                        );
                    }
                }
            }
        }
        let public = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let arguments = [DataType::Int64, DataType::Int64].map(|ty| FunctionArgument::Value {
            value_type: value_type(ty),
            constant: None,
        });
        let request = FunctionBindingRequest {
            arguments: &arguments,
            logical_argument_count: 2,
            expected_result_type: None,
        };
        let bound = public
            .resolve_bound_user(name, FunctionKind::Aggregate, request, &COMPILE)
            .unwrap();
        let profile = public
            .pure_overload_declaration_observed(
                &bound.function_id,
                FunctionKind::Aggregate,
                &bound.selected.overload,
                &COMPILE,
            )
            .unwrap();
        assert_eq!(
            profile.implementation().abi,
            PureKernelAbi::AggregateWindowV1
        );
        for lifecycle in [
            PureCallLifecycle::Aggregate,
            PureCallLifecycle::AggregateWindow,
        ] {
            profile
                .admit_selected_lifecycle_observed(&bound.selected, 2, lifecycle, &COMPILE)
                .unwrap();
        }
    }
}

#[test]
fn c02_p1_preparation_frozen_mismatch_never_reaches_override() {
    let owner = Arc::new(Owner::new(
        FunctionKind::Aggregate,
        &[PureKernelAbi::AggregateWindowV1],
    ));
    let catalog = aggregate_catalog(owner.clone());
    let call = Call::new(&owner, 0);
    let mut effects = owner.frozen(&call.selected);
    effects.observable_effects.controlled_wait = true;
    assert!(matches!(
        catalog.prepare_frozen(
            call.input(),
            call.selected.clone(),
            &effects,
            over_prepare(&call),
            &COMPILE
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.window_prepare.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.aggregate.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.adapter.load(Ordering::Relaxed), 0);
}

#[test]
fn c02_p1_preparation_controls_stop_before_and_after_override_without_adapter() {
    struct Stop<'a> {
        counts: &'a Counts,
        after_hook: bool,
        failure: CompileControlError,
    }
    impl PureCompileControl for Stop<'_> {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            if !self.after_hook || self.counts.window_prepare.load(Ordering::Relaxed) > 0 {
                Err(self.failure)
            } else {
                Ok(())
            }
        }
    }
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for frozen in [false, true] {
            for after_hook in [false, true] {
                let owner = Arc::new(Owner::new(
                    FunctionKind::Aggregate,
                    &[PureKernelAbi::AggregateWindowV1],
                ));
                let catalog = aggregate_catalog(owner.clone());
                let call = Call::new(&owner, 0);
                let stop = Stop {
                    counts: &owner.counts,
                    after_hook,
                    failure,
                };
                let result = if frozen {
                    catalog.prepare_frozen(
                        call.input(),
                        call.selected.clone(),
                        &owner.frozen(&call.selected),
                        over_prepare(&call),
                        &stop,
                    )
                } else {
                    catalog.prepare_fresh(
                        call.input(),
                        call.selected.clone(),
                        over_prepare(&call),
                        &stop,
                    )
                };
                assert!(
                    matches!(result, Err(FunctionSpecializationFailure::Control(actual)) if actual == failure)
                );
                assert_eq!(
                    owner.counts.refine.load(Ordering::Relaxed),
                    usize::from(after_hook)
                );
                assert_eq!(
                    owner.counts.window_prepare.load(Ordering::Relaxed),
                    usize::from(after_hook)
                );
                assert_eq!(
                    owner.counts.aggregate.load(Ordering::Relaxed),
                    usize::from(after_hook)
                );
                assert_eq!(owner.counts.adapter.load(Ordering::Relaxed), 0);
            }
        }
    }
}

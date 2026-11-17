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

use super::*;
use crate::{
    AggregateBindingSelection, AggregateCallContract, AggregateKernelPhase, AggregateOrderKey,
    AggregateStateFormatIdentity, AggregateStateMemoryPolicy, AggregateUpdateInvocation,
    EvaluatedArgument, FunctionArgument, FunctionBindingRequest, FunctionEffectOwnerError,
    FunctionId, FunctionKind, FunctionOverloadId, FunctionResultType, KernelDiagnostic,
    SelectedAggregateMergeInput, SelectedAggregateUpdateInput, create_aggregate_state,
    emit_aggregate,
};
use arrow_array::{ArrayRef, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileControlError, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, FunctionEffectDeclaration,
    FunctionFailureBehavior, FunctionInstanceState, FunctionIntrinsicRowError,
    FunctionNullBehavior, FunctionValueType, FunctionVolatility, ObservableEffects,
    SemanticParameters, WindowBound, WindowFrame, WindowFrameExclusion, WindowFrameUnits,
};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

#[derive(Debug, Default)]
struct Counts {
    refine: AtomicUsize,
    aggregate: AtomicUsize,
    adapter: AtomicUsize,
    begin: AtomicUsize,
    update: AtomicUsize,
    evaluate: AtomicUsize,
    finish: AtomicUsize,
}
struct Compile;
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        Ok(())
    }
}
struct Runtime;
impl KernelEvaluationControl for Runtime {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("fixture does not wait")
    }
}
struct Owner {
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: [FunctionArgument; 2],
    uses: [Option<ExpressionUseId>; 2],
    declaration: FunctionEffectDeclaration,
    parameters: SemanticParameters,
    counts: Arc<Counts>,
    foreign: bool,
}
impl Owner {
    fn new() -> Self {
        let arguments =
            [DataType::Int64, DataType::Int32].map(|data_type| FunctionArgument::Value {
                value_type: FunctionValueType::from_legacy_declared_projection(data_type, false),
                constant: None,
            });
        Self {
            id: FunctionId::try_new("fixture/aggregate-window/sum-v1").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/aggregate-window/int64-v1").unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Box<[_]>>(),
                ),
                result_type: FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Int64,
                    false,
                )),
                aggregate: Some(AggregateBindingSelection {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_type: FunctionValueType::new(DataType::Int64, false),
                    state_format: AggregateStateFormatIdentity::try_new(
                        "fixture/aggregate-window/state-v1",
                    )
                    .unwrap(),
                }),
            }),
            arguments,
            uses: [Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))],
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::Aggregate,
                instance_state: FunctionInstanceState::AggregateInstance,
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            parameters: SemanticParameters::default(),
            counts: Arc::default(),
            foreign: false,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                region: EvaluationRegionId::new(7),
                demand: EvaluationDemand::Value,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.id,
            kind: FunctionKind::Aggregate,
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
    fn frozen(&self) -> CallEffects {
        CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: CallProofScope::Unconditional,
        }
    }
    fn options(&self) -> AggregatePreparationOptions {
        AggregatePreparationOptions {
            state_interpretation: None,
            phase: AggregateKernelPhase::Single,
            distinct: true,
            order_keys: vec![AggregateOrderKey {
                ascending: false,
                nulls_first: true,
            }]
            .into(),
            state_input_type: None,
        }
    }
    fn specialize(
        &self,
        frozen: bool,
        control: &dyn PureCompileControl,
    ) -> Result<WindowSpecialization, FunctionSpecializationFailure> {
        let input = self.input();
        let children = ScopedExpressionEffects::pure_value(input.context);
        if frozen {
            specialize_frozen_aggregate_window(
                self,
                input,
                self.selected.clone(),
                &self.frozen(),
                children,
                AggregateWindowPreparationOptions {
                    aggregate: self.options(),
                    window: window_options(),
                },
                control,
            )
        } else {
            specialize_aggregate_window(
                self,
                input,
                self.selected.clone(),
                children,
                AggregateWindowPreparationOptions {
                    aggregate: self.options(),
                    window: window_options(),
                },
                control,
            )
        }
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
        &Compile,
    )
    .unwrap()
}
impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        _: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        panic!("aggregate OVER must not reselect or resolve by name")
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if !std::ptr::eq(selected, self.selected.as_ref())
            || !crate::binding::arguments_equal_for_test(
                request.arguments,
                &self.arguments,
                control,
            )?
            || request.logical_argument_count != 1
        {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(())
    }
}
impl FunctionEffectOwner for Owner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != &self.id || !std::ptr::eq(selected, self.selected.as_ref()) {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(&self.declaration)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.counts.refine.fetch_add(1, Ordering::Relaxed);
        self.validate_selected(input.selected, input.request, control)?;
        assert_eq!(input.kind, FunctionKind::Aggregate);
        assert_eq!(input.function_id, &self.id);
        assert!(std::ptr::eq(input.parameters, &self.parameters));
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 1)
            .map_err(FunctionEffectOwnerError::Control)?;
        Ok(self.frozen())
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
        assert_eq!(input.kind, FunctionKind::Aggregate);
        assert!(std::ptr::eq(input.selected, contract.call().selected()));
        Ok(Arc::new(Sum {
            contract,
            counts: self.counts.clone(),
        }))
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
        assert!(Arc::ptr_eq(
            contract.aggregate().unwrap(),
            aggregate.contract()
        ));
        assert_eq!(contract.call().kind(), FunctionKind::Aggregate);
        assert_eq!(aggregate.contract().phase(), AggregateKernelPhase::Single);
        assert!(aggregate.contract().distinct());
        assert_eq!(
            aggregate.contract().order_keys(),
            self.options().order_keys.as_ref()
        );
        assert_eq!(
            contract.function_order_keys(),
            self.options().order_keys.as_ref()
        );
        assert_eq!(contract.call().selected(), self.selected.as_ref());
        assert_eq!(contract.options(), &window_options());
        let contract = if self.foreign {
            Arc::new((*contract).clone())
        } else {
            contract
        };
        Ok(Arc::new(Adapter {
            aggregate,
            contract,
        }))
    }
}
#[derive(Debug)]
struct Sum {
    contract: Arc<AggregateCallContract>,
    counts: Arc<Counts>,
}
#[derive(Default)]
struct State {
    sum: i64,
    seen: Vec<i64>,
}
impl PreparedAggregateKernel for Sum {
    type State = State;
    type PreparedUpdateBatch<'a> = SelectedAggregateUpdateInput<'a, 'a>;
    type PreparedMergeBatch<'a> = SelectedAggregateMergeInput<'a, 'a>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::BoundedRetained {
            max_retained_bytes_per_state: 1024,
        }
    }
    fn create_state(&self, control: &dyn KernelEvaluationControl) -> Result<State, KernelFailure> {
        control.checkpoint(1)?;
        Ok(State::default())
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
        state: &mut State,
        input: &Self::PreparedUpdateBatch<'a>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        control.checkpoint(1)?;
        self.counts.update.fetch_add(1, Ordering::Relaxed);
        let row = input.selection().row(ordinal).unwrap();
        let value = input.logical_arguments()[0];
        let value = value
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(value.value_row(ordinal, row));
        let order = input.order_arguments()[0];
        assert_eq!(
            order
                .array()
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(order.value_row(ordinal, row)),
            row as i32
        );
        if value == -9 {
            return Err(KernelFailure::Operational(KernelDiagnostic::new(
                "required later frame update failed",
            )));
        }
        if !self.contract.distinct() || !state.seen.contains(&value) {
            state.sum += value;
            state.seen.push(value);
        }
        Ok(())
    }
    fn prepare_merge<'a>(
        &'a self,
        _: SelectedAggregateMergeInput<'a, 'a>,
        _: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'a>, KernelFailure> {
        panic!("Single aggregate OVER must not merge states")
    }
    fn merge_row<'a>(
        &self,
        _: &mut State,
        _: &Self::PreparedMergeBatch<'a>,
        _: usize,
        _: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        panic!("Single aggregate OVER must not merge states")
    }
    fn build_intermediate<'a, I>(
        &self,
        _: I,
        _: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'a State>,
    {
        panic!("Single aggregate OVER must emit final values")
    }
    fn build_final<'a, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'a State>,
    {
        control.checkpoint(1)?;
        Ok(Arc::new(Int64Array::from(
            states.map(|state| state.sum).collect::<Vec<_>>(),
        )))
    }
    fn retained_bytes(&self, state: &State) -> usize {
        state.seen.capacity() * size_of::<i64>()
    }
}
#[derive(Debug)]
struct Adapter {
    aggregate: Arc<Sum>,
    contract: Arc<WindowCallContract>,
}
impl PreparedWindowKernel for Adapter {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }
    fn partition_retained_upper_bound(&self, rows: usize) -> Result<usize, KernelFailure> {
        Ok(size_of::<Partition<'_>>() + rows * size_of::<i64>())
    }
    fn begin_partition<'a>(
        self: Arc<Self>,
        input: WindowPartitionInput<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'a>, KernelFailure> {
        self.aggregate.counts.begin.fetch_add(1, Ordering::Relaxed);
        let full = input.full_input();
        let mut results = Vec::with_capacity(full.partition_rows());
        // Fixture host: complete every required frame before any output Selection exists.
        // This exercises typed dispatch and error responsibility, not a production frame algorithm or MEM authorization.
        for frame in input.frames() {
            let rows = (frame.start..frame.end).collect::<Vec<_>>();
            let selection = Selection::try_sparse(full.partition_rows(), &rows).unwrap();
            let update = SelectedAggregateUpdateInput::try_new(
                self.aggregate.contract(),
                selection,
                full.logical_arguments(),
                full.order_arguments(),
                control,
            )?;
            let mut state = create_aggregate_state(self.aggregate.as_ref(), control)?;
            let mut invocation =
                AggregateUpdateInvocation::try_new(self.aggregate.as_ref(), update, control)?;
            for _ in &rows {
                invocation.update_next(&mut state, control)?;
            }
            let result = emit_aggregate(self.aggregate.as_ref(), [&state].into_iter(), 1, control)?;
            results.push(
                result
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(0),
            );
        }
        Ok(Box::new(Partition {
            owner: self,
            _input: input,
            results,
        }))
    }
}
struct Partition<'a> {
    owner: Arc<Adapter>,
    _input: WindowPartitionInput<'a>,
    results: Vec<i64>,
}
impl WindowKernelPartition for Partition<'_> {
    fn evaluate<'a>(
        &mut self,
        selection: Selection<'a>,
        _: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'a>, KernelFailure> {
        control.checkpoint(1)?;
        self.owner
            .aggregate
            .counts
            .evaluate
            .fetch_add(1, Ordering::Relaxed);
        let values: ArrayRef = Arc::new(Int64Array::from(
            (0..selection.len())
                .map(|i| self.results[selection.row(i).unwrap()])
                .collect::<Vec<_>>(),
        ));
        SelectedValues::try_new(selection, &DataType::Int64, values, Box::default())
            .map_err(|_| internal("fixture output contract drift"))
    }
    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        control.checkpoint(1)?;
        self.owner
            .aggregate
            .counts
            .finish
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.results.capacity() * size_of::<i64>()
    }
}
fn count(value: &AtomicUsize) -> usize {
    value.load(Ordering::Relaxed)
}

#[test]
fn exact_aggregate_owner_prepares_once_and_sparse_output_keeps_single_order_distinct_contract() {
    for frozen in [false, true] {
        let owner = Owner::new();
        let prepared = owner.specialize(frozen, &Compile).unwrap().into_prepared();
        assert_eq!(
            (
                count(&owner.counts.refine),
                count(&owner.counts.aggregate),
                count(&owner.counts.adapter)
            ),
            (1, 1, 1)
        );
        assert_eq!(prepared.contract().call().kind(), FunctionKind::Aggregate);
        assert_eq!(
            prepared.contract().aggregate().unwrap().phase(),
            AggregateKernelPhase::Single
        );
        let values: ArrayRef = Arc::new(Int64Array::from(vec![2, 2, 5]));
        let order: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2]));
        let logical = [EvaluatedArgument::Column(&values)];
        let orders = [EvaluatedArgument::Column(&order)];
        let peers = [WindowRowRange { start: 0, end: 3 }];
        let frames = [
            WindowRowRange { start: 0, end: 1 },
            WindowRowRange { start: 0, end: 2 },
            WindowRowRange { start: 0, end: 3 },
        ];
        let full =
            FullPartitionWindowInput::try_new(prepared.contract(), 3, &logical, &orders, &Runtime)
                .unwrap();
        let input = WindowPartitionInput::try_new(full, &peers, &frames, &Runtime).unwrap();
        let mut partition =
            WindowEvaluationPartition::begin(prepared.clone(), input, &Runtime).unwrap();
        assert_eq!(count(&owner.counts.update), 6);
        let selected = [0, 2];
        let selection = Selection::try_sparse(3, &selected).unwrap();
        let output = partition.evaluate(selection, 2, &Runtime).unwrap();
        assert_eq!(output.selection(), selection);
        assert_eq!(
            output
                .values()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[2, 7]
        );
        partition.finish(&Runtime).unwrap();
        assert_eq!(
            (
                count(&owner.counts.begin),
                count(&owner.counts.evaluate),
                count(&owner.counts.finish)
            ),
            (1, 1, 1)
        );
    }
}

#[test]
fn empty_output_still_performs_every_required_frame_and_later_errors_fail_begin() {
    for later_error in [false, true] {
        let owner = Owner::new();
        let prepared = owner.specialize(false, &Compile).unwrap().into_prepared();
        let values: ArrayRef = Arc::new(Int64Array::from(vec![
            2,
            3,
            if later_error { -9 } else { 5 },
        ]));
        let order: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2]));
        let logical = [EvaluatedArgument::Column(&values)];
        let orders = [EvaluatedArgument::Column(&order)];
        let peers = [WindowRowRange { start: 0, end: 3 }];
        let frames = [
            WindowRowRange { start: 0, end: 1 },
            WindowRowRange { start: 0, end: 2 },
            WindowRowRange { start: 0, end: 3 },
        ];
        let full =
            FullPartitionWindowInput::try_new(prepared.contract(), 3, &logical, &orders, &Runtime)
                .unwrap();
        let input = WindowPartitionInput::try_new(full, &peers, &frames, &Runtime).unwrap();
        let result = WindowEvaluationPartition::begin(prepared.clone(), input, &Runtime);
        assert_eq!(count(&owner.counts.update), 6);
        if later_error {
            assert!(matches!(result, Err(KernelFailure::Operational(_))));
        } else {
            let mut partition = result.unwrap();
            let empty = Selection::try_sparse(3, &[]).unwrap();
            assert!(
                partition
                    .evaluate(empty, 0, &Runtime)
                    .unwrap()
                    .values()
                    .is_empty()
            );
            partition.finish(&Runtime).unwrap();
            assert_eq!(count(&owner.counts.finish), 1);
        }
        assert_eq!(count(&owner.counts.evaluate), 0);
    }
}

#[test]
fn non_single_or_state_input_is_rejected_before_any_owner_work() {
    for phase in [
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
        AggregateKernelPhase::Single,
    ] {
        let owner = Owner::new();
        let mut options = owner.options();
        options.phase = phase;
        if phase == AggregateKernelPhase::Single {
            options.state_input_type = Some(FunctionValueType::new(DataType::Int64, false));
        }
        let input = owner.input();
        let result = specialize_aggregate_window(
            &owner,
            input,
            owner.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context),
            AggregateWindowPreparationOptions {
                aggregate: options,
                window: window_options(),
            },
            &Compile,
        );
        assert!(matches!(
            result,
            Err(FunctionSpecializationFailure::Kernel(
                KernelFailure::InvalidProgram(_)
            ))
        ));
        assert_eq!(
            (
                count(&owner.counts.refine),
                count(&owner.counts.aggregate),
                count(&owner.counts.adapter)
            ),
            (0, 0, 0)
        );
    }
}
#[test]
fn forged_frozen_effects_cannot_reach_aggregate_or_window_adapter() {
    let owner = Owner::new();
    let input = owner.input();
    let mut frozen = owner.frozen();
    frozen.observable_effects.controlled_wait = true;
    let result = specialize_frozen_aggregate_window(
        &owner,
        input,
        owner.selected.clone(),
        &frozen,
        ScopedExpressionEffects::pure_value(input.context),
        AggregateWindowPreparationOptions {
            aggregate: owner.options(),
            window: window_options(),
        },
        &Compile,
    );
    assert!(matches!(
        result,
        Err(FunctionSpecializationFailure::InvalidInput(
            "frozen call effects differ from exact local refinement"
        ))
    ));
    assert_eq!(
        (
            count(&owner.counts.refine),
            count(&owner.counts.aggregate),
            count(&owner.counts.adapter)
        ),
        (1, 0, 0)
    );
}
#[test]
fn equal_but_foreign_window_contract_is_an_internal_fault() {
    for frozen in [false, true] {
        let mut owner = Owner::new();
        owner.foreign = true;
        assert!(matches!(
            owner.specialize(frozen, &Compile),
            Err(FunctionSpecializationFailure::Kernel(
                KernelFailure::Internal(_)
            ))
        ));
        assert_eq!(
            (
                count(&owner.counts.refine),
                count(&owner.counts.aggregate),
                count(&owner.counts.adapter)
            ),
            (1, 1, 1)
        );
    }
}
#[derive(Clone, Copy)]
enum StopAt {
    Entry,
    AggregatePrepared,
    AdapterPrepared,
}
struct Stop {
    at: StopAt,
    counts: Arc<Counts>,
    failure: CompileControlError,
}
impl PureCompileControl for Stop {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        let stop = match self.at {
            StopAt::Entry => true,
            StopAt::AggregatePrepared => count(&self.counts.aggregate) > 0,
            StopAt::AdapterPrepared => count(&self.counts.adapter) > 0,
        };
        if stop {
            Err(self.failure.clone())
        } else {
            Ok(())
        }
    }
}
#[test]
fn controls_before_and_after_adapter_preserve_typed_failures_and_no_publication() {
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in [
            StopAt::Entry,
            StopAt::AggregatePrepared,
            StopAt::AdapterPrepared,
        ] {
            for frozen in [false, true] {
                let owner = Owner::new();
                let control = Stop {
                    at,
                    counts: owner.counts.clone(),
                    failure: failure.clone(),
                };
                assert!(
                    matches!(owner.specialize(frozen, &control), Err(FunctionSpecializationFailure::Control(actual)) if actual == failure)
                );
                let expected = match at {
                    StopAt::Entry => (0, 0, 0),
                    StopAt::AggregatePrepared => (1, 1, 0),
                    StopAt::AdapterPrepared => (1, 1, 1),
                };
                assert_eq!(
                    (
                        count(&owner.counts.refine),
                        count(&owner.counts.aggregate),
                        count(&owner.counts.adapter)
                    ),
                    expected
                );
                assert_eq!(count(&owner.counts.begin), 0);
            }
        }
    }
}

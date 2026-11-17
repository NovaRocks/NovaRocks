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
    AggregateBindingSelection, AggregateStateFormatIdentity, FunctionArgument,
    FunctionBindingRequest, FunctionEffectOwnerError, FunctionId, FunctionKind, FunctionOverloadId,
    FunctionResultType, KernelDiagnostic,
};
use arrow_array::{Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallProofScope, CompileCheckpoints, CompileControlError,
    DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
    ExpressionUseId, FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    SemanticParameters,
};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Debug, Default)]
struct Counts {
    refine: AtomicUsize,
    prepare: AtomicUsize,
    create: AtomicUsize,
    drop: AtomicUsize,
    update: AtomicUsize,
    merge: AtomicUsize,
    batches: AtomicUsize,
    intermediate: AtomicUsize,
    final_emit: AtomicUsize,
}
#[derive(Clone, Debug, Default)]
enum Emit {
    #[default]
    Good,
    WrongRows,
    WrongType,
    Null,
    Grow(Option<KernelFailure>),
    Failure(KernelFailure),
}
#[derive(Clone, Debug)]
struct Behavior {
    bound: usize,
    foreign: bool,
    create_failure: Option<KernelFailure>,
    create_growth: bool,
    row_failure: Option<(usize, KernelFailure)>,
    row_growth: bool,
    emit: Emit,
}
impl Default for Behavior {
    fn default() -> Self {
        Self {
            bound: 0,
            foreign: false,
            create_failure: None,
            create_growth: false,
            row_failure: None,
            row_growth: false,
            emit: Emit::Good,
        }
    }
}
#[derive(Default)]
struct CompileControl {
    failure: Option<CompileControlError>,
    quantum_only: bool,
    seen: Mutex<Vec<u32>>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(work <= 256);
        self.seen.lock().unwrap().push(work);
        if !self.quantum_only || work == 256 {
            self.failure.clone().map_or(Ok(()), Err)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct RuntimeControl {
    failure: Option<KernelFailure>,
    positive: bool,
    seen: Mutex<Vec<u32>>,
}
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        self.seen.lock().unwrap().push(work);
        if !self.positive || work > 0 {
            self.failure.clone().map_or(Ok(()), Err)
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("fixture does not wait")
    }
}
struct Owner {
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    logical: usize,
    declaration: FunctionEffectDeclaration,
    parameters: SemanticParameters,
    counts: Arc<Counts>,
    behavior: Behavior,
}
impl Owner {
    fn new(logical: usize, ordered: bool) -> Self {
        let mut types = vec![FunctionValueType::new(DataType::Int64, false); logical];
        if ordered {
            types.push(FunctionValueType::new(DataType::Int32, false));
        }
        let arguments = types
            .into_iter()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect::<Vec<_>>();
        Self {
            id: FunctionId::try_new("fixture/aggregate-lifecycle/exact-owner").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/aggregate-lifecycle/signature")
                    .unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Box<[_]>>(),
                ),
                result_type: FunctionResultType::Scalar(FunctionValueType::new(
                    DataType::Int32,
                    false,
                )),
                aggregate: Some(AggregateBindingSelection {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_type: FunctionValueType::new(DataType::Int64, false),
                    state_format: AggregateStateFormatIdentity::try_new(
                        "fixture/aggregate-lifecycle/state-v1",
                    )
                    .unwrap(),
                }),
            }),
            uses: (0..arguments.len())
                .map(|n| Some(ExpressionUseId::new(n as u32 + 1)))
                .collect(),
            arguments,
            logical,
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
            counts: Arc::new(Counts::default()),
            behavior: Behavior::default(),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                region: EvaluationRegionId::new(9),
                demand: EvaluationDemand::Value,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.id,
            kind: FunctionKind::Aggregate,
            selected: &self.selected,
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &self.arguments,
                logical_argument_count: self.logical,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        }
    }
    fn input_for_phase(&self, phase: AggregateKernelPhase) -> CallEffectInput<'_> {
        let mut input = self.input();
        if !phase.consumes_logical_arguments() {
            input.argument_uses = crate::CallArgumentUses::AggregateMerge {
                phase,
                state_context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(u32::MAX),
                    region: EvaluationRegionId::new(8),
                    demand: EvaluationDemand::Value,
                },
                state_input_type: &self.selected.aggregate.as_ref().unwrap().intermediate_type,
            };
        }
        input
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
    fn options(&self, phase: AggregateKernelPhase) -> AggregatePreparationOptions {
        let update = phase.consumes_logical_arguments();
        AggregatePreparationOptions {
            state_interpretation: None,
            phase,
            distinct: update,
            order_keys: if update {
                vec![
                    AggregateOrderKey {
                        ascending: false,
                        nulls_first: true
                    };
                    self.arguments.len() - self.logical
                ]
                .into()
            } else {
                Arc::from([])
            },
            state_input_type: (!update).then(|| {
                self.selected
                    .aggregate
                    .as_ref()
                    .unwrap()
                    .intermediate_type
                    .clone()
            }),
        }
    }
    fn specialize(&self, phase: AggregateKernelPhase) -> AggregateSpecialization<Kernel> {
        let input = self.input_for_phase(phase);
        specialize_aggregate(
            self,
            input,
            self.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context),
            self.options(phase),
            &CompileControl::default(),
        )
        .unwrap()
    }
}
impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        _: FunctionBindingRequest<'_>,
        _control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        panic!("must not reselect")
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if !std::ptr::eq(selected, self.selected.as_ref())
            || request.logical_argument_count != self.logical
            || !crate::binding::arguments_equal_for_test(
                request.arguments,
                &self.arguments,
                control,
            )?
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
        if input.function_id != &self.id
            || input.kind != FunctionKind::Aggregate
            || !input.environment.is_empty()
            || !std::ptr::eq(input.parameters, &self.parameters)
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        for _ in input.request.arguments {
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(self.frozen())
    }
}
impl PureAggregateImplementation for Owner {
    type Kernel = Kernel;
    fn prepare_aggregate(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<AggregateCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<Kernel>, KernelFailure> {
        self.counts.prepare.fetch_add(1, Ordering::Relaxed);
        assert!(std::ptr::eq(contract.call().selected(), input.selected));
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(crate::kernel_control::compile_failure)?;
        let contract = if self.behavior.foreign {
            Arc::new((*contract).clone())
        } else {
            contract
        };
        Ok(Arc::new(Kernel {
            contract,
            counts: self.counts.clone(),
            behavior: self.behavior.clone(),
        }))
    }
}
#[derive(Debug)]
struct Kernel {
    contract: Arc<AggregateCallContract>,
    counts: Arc<Counts>,
    behavior: Behavior,
}
#[derive(Debug)]
struct State {
    sum: i64,
    last_row: Option<usize>,
    last_order: Option<i32>,
    heap: Mutex<Vec<u8>>,
    counts: Arc<Counts>,
}
impl Drop for State {
    fn drop(&mut self) {
        self.counts.drop.fetch_add(1, Ordering::Relaxed);
    }
}
impl Kernel {
    fn row(
        &self,
        state: &mut State,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        control.checkpoint(256)?;
        if self.behavior.row_growth {
            state.heap.lock().unwrap().push(1);
        }
        if let Some((at, error)) = &self.behavior.row_failure
            && *at == ordinal
        {
            return Err(error.clone());
        }
        Ok(())
    }
    fn emit<'s, I>(
        &self,
        states: I,
        final_result: bool,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        I: ExactSizeIterator<Item = &'s State>,
    {
        control.checkpoint(0)?;
        let mut values = Vec::with_capacity(states.len());
        for state in states {
            control.checkpoint(1)?;
            if let Emit::Grow(_) = self.behavior.emit {
                state.heap.lock().unwrap().push(1);
            }
            values.push(state.sum);
        }
        match &self.behavior.emit {
            Emit::Grow(Some(error)) | Emit::Failure(error) => return Err(error.clone()),
            Emit::WrongRows => values.push(0),
            Emit::WrongType => {
                return Ok(Arc::new(arrow_array::BooleanArray::from(vec![
                    true;
                    values.len()
                ])));
            }
            Emit::Null => {
                return Ok(if final_result {
                    Arc::new(Int32Array::from(vec![None; values.len()]))
                } else {
                    Arc::new(Int64Array::from(vec![None; values.len()]))
                });
            }
            _ => {}
        }
        Ok(if final_result {
            Arc::new(Int32Array::from(
                values.iter().map(|v| (*v * 10) as i32).collect::<Vec<_>>(),
            ))
        } else {
            Arc::new(Int64Array::from(values))
        })
    }
}
impl PreparedAggregateKernel for Kernel {
    type State = State;
    type PreparedUpdateBatch<'b> = SelectedAggregateUpdateInput<'b, 'b>;
    type PreparedMergeBatch<'b> = SelectedAggregateMergeInput<'b, 'b>;
    fn contract(&self) -> &Arc<AggregateCallContract> {
        &self.contract
    }
    fn memory_policy(&self) -> AggregateStateMemoryPolicy {
        AggregateStateMemoryPolicy::BoundedRetained {
            max_retained_bytes_per_state: self.behavior.bound,
        }
    }
    fn create_state(&self, control: &dyn KernelEvaluationControl) -> Result<State, KernelFailure> {
        self.counts.create.fetch_add(1, Ordering::Relaxed);
        control.checkpoint(1)?;
        let state = State {
            sum: 0,
            last_row: None,
            last_order: None,
            heap: Mutex::new(if self.behavior.create_growth {
                vec![1]
            } else {
                vec![]
            }),
            counts: self.counts.clone(),
        };
        if let Some(error) = &self.behavior.create_failure {
            return Err(error.clone());
        }
        Ok(state)
    }
    fn prepare_update<'b>(
        &'b self,
        input: SelectedAggregateUpdateInput<'b, 'b>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedUpdateBatch<'b>, KernelFailure> {
        control.checkpoint(1)?;
        self.counts.batches.fetch_add(1, Ordering::Relaxed);
        Ok(input)
    }
    fn update_row<'b>(
        &self,
        state: &mut State,
        input: &Self::PreparedUpdateBatch<'b>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        self.counts.update.fetch_add(1, Ordering::Relaxed);
        self.row(state, ordinal, control)?;
        let row = input.selection().row(ordinal).unwrap();
        state.last_row = Some(row);
        for arg in input.logical_arguments() {
            let array = arg.array().as_any().downcast_ref::<Int64Array>().unwrap();
            state.sum += array.value(arg.value_row(ordinal, row));
        }
        for arg in input.order_arguments() {
            let array = arg.array().as_any().downcast_ref::<Int32Array>().unwrap();
            state.last_order = Some(array.value(arg.value_row(ordinal, row)));
        }
        Ok(())
    }
    fn prepare_merge<'b>(
        &'b self,
        input: SelectedAggregateMergeInput<'b, 'b>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Self::PreparedMergeBatch<'b>, KernelFailure> {
        control.checkpoint(1)?;
        self.counts.batches.fetch_add(1, Ordering::Relaxed);
        Ok(input)
    }
    fn merge_row<'b>(
        &self,
        state: &mut State,
        input: &Self::PreparedMergeBatch<'b>,
        ordinal: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<(), KernelFailure> {
        self.counts.merge.fetch_add(1, Ordering::Relaxed);
        self.row(state, ordinal, control)?;
        let row = input.selection().row(ordinal).unwrap();
        let arg = input.state();
        state.sum += arg
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(arg.value_row(ordinal, row));
        state.last_row = Some(row);
        Ok(())
    }
    fn build_intermediate<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        State: 's,
        I: ExactSizeIterator<Item = &'s State>,
    {
        self.counts.intermediate.fetch_add(1, Ordering::Relaxed);
        self.emit(states, false, control)
    }
    fn build_final<'s, I>(
        &self,
        states: I,
        control: &dyn KernelEvaluationControl,
    ) -> Result<ArrayRef, KernelFailure>
    where
        State: 's,
        I: ExactSizeIterator<Item = &'s State>,
    {
        self.counts.final_emit.fetch_add(1, Ordering::Relaxed);
        self.emit(states, true, control)
    }
    fn retained_bytes(&self, state: &State) -> usize {
        state.heap.lock().unwrap().capacity()
    }
}
fn operational() -> KernelFailure {
    KernelFailure::Operational(KernelDiagnostic::new("fixture data failure"))
}
fn failures() -> Vec<KernelFailure> {
    vec![
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        operational(),
        internal("fixture fault"),
    ]
}
fn value_array() -> ArrayRef {
    Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]))
}
fn order_array() -> ArrayRef {
    Arc::new(Int32Array::from(vec![5, 4, 3, 2, 1]))
}

#[test]
fn fresh_and_frozen_prepare_once_without_state_and_preserve_exact_phase_channels() {
    let owner = Owner::new(1, true);
    for phase in [
        AggregateKernelPhase::Single,
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        for frozen in [false, true] {
            owner.counts.refine.store(0, Ordering::Relaxed);
            owner.counts.prepare.store(0, Ordering::Relaxed);
            let input = owner.input_for_phase(phase);
            let child = ScopedExpressionEffects::pure_value(input.context);
            let prepared = if frozen {
                specialize_frozen_aggregate(
                    &owner,
                    input,
                    owner.selected.clone(),
                    &owner.frozen(),
                    child,
                    owner.options(phase),
                    &CompileControl::default(),
                )
            } else {
                specialize_aggregate(
                    &owner,
                    input,
                    owner.selected.clone(),
                    child,
                    owner.options(phase),
                    &CompileControl::default(),
                )
            }
            .unwrap();
            assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 1);
            assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 1);
            assert_eq!(owner.counts.create.load(Ordering::Relaxed), 0);
            assert_eq!(prepared.state_layout(), Layout::new::<State>());
            assert!(std::ptr::eq(
                owner.selected.as_ref(),
                prepared.prepared().contract.call().selected()
            ));
            assert_eq!(prepared.prepared().contract.phase(), phase);
            assert_eq!(
                prepared.prepared().contract.distinct(),
                phase.consumes_logical_arguments()
            );
            assert_eq!(
                prepared.prepared().contract.logical_argument_types().len(),
                1
            );
            assert_eq!(prepared.prepared().contract.order_argument_types().len(), 1);
            assert_eq!(
                prepared.prepared().contract.order_keys().len(),
                usize::from(phase.consumes_logical_arguments())
            );
            assert!(
                prepared
                    .effects()
                    .for_use(input.context)
                    .unwrap()
                    .has_instance_state
            );
        }
    }
}
#[test]
fn four_phases_emit_actual_intermediate_or_final_values_with_sparse_repeated_group_borrows() {
    let owner = Owner::new(1, true);
    let values = value_array();
    let orders = order_array();
    let rows = [1, 3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let runtime = RuntimeControl::default();
    for phase in [
        AggregateKernelPhase::Single,
        AggregateKernelPhase::Partial,
        AggregateKernelPhase::Intermediate,
        AggregateKernelPhase::Final,
    ] {
        let kernel = owner.specialize(phase).into_prepared();
        let mut state = create_aggregate_state(kernel.as_ref(), &runtime).unwrap();
        if phase.consumes_logical_arguments() {
            let logical = [EvaluatedArgument::Column(&values)];
            let order = [EvaluatedArgument::Column(&orders)];
            let input = SelectedAggregateUpdateInput::try_new(
                &kernel.contract,
                selection,
                &logical,
                &order,
                &runtime,
            )
            .unwrap();
            let mut invocation =
                AggregateUpdateInvocation::try_new(kernel.as_ref(), input, &runtime).unwrap();
            assert_eq!(invocation.next_batch_row(), Some(1));
            invocation.update_next(&mut state, &runtime).unwrap();
            assert_eq!(state.sum, 20);
            assert_eq!(state.last_order, Some(4));
            assert_eq!(invocation.next_selected_ordinal(), Some(1));
            assert_eq!(invocation.next_batch_row(), Some(3));
            invocation.update_next(&mut state, &runtime).unwrap();
            assert_eq!(state.last_order, Some(2));
            assert_eq!(invocation.next_selected_ordinal(), None);
        } else {
            let input = SelectedAggregateMergeInput::try_new(
                &kernel.contract,
                selection,
                EvaluatedArgument::Column(&values),
                &runtime,
            )
            .unwrap();
            let mut invocation =
                AggregateMergeInvocation::try_new(kernel.as_ref(), input, &runtime).unwrap();
            assert_eq!(invocation.next_batch_row(), Some(1));
            invocation.merge_next(&mut state, &runtime).unwrap();
            assert_eq!(state.sum, 20);
            assert_eq!(invocation.next_batch_row(), Some(3));
            invocation.merge_next(&mut state, &runtime).unwrap();
            assert_eq!(invocation.next_selected_ordinal(), None);
        }
        assert_eq!(state.sum, 60);
        assert_eq!(state.last_row, Some(3));
        let output = emit_aggregate(kernel.as_ref(), [&state].into_iter(), 1, &runtime).unwrap();
        if phase.produces_final_result() {
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .values(),
                &[600]
            );
        } else {
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values(),
                &[60]
            );
        }
    }
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 4);
    assert_eq!(owner.counts.final_emit.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.intermediate.load(Ordering::Relaxed), 2);
}

#[test]
fn independent_states_and_empty_selection_perform_no_updates() {
    let owner = Owner::new(1, false);
    let kernel = owner
        .specialize(AggregateKernelPhase::Single)
        .into_prepared();
    let runtime = RuntimeControl::default();
    let mut first = create_aggregate_state(kernel.as_ref(), &runtime).unwrap();
    let second = create_aggregate_state(kernel.as_ref(), &runtime).unwrap();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    let rows = [3];
    let selected = Selection::try_sparse(5, &rows).unwrap();
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, selected, &args, &[], &runtime)
            .unwrap();
    AggregateUpdateInvocation::try_new(kernel.as_ref(), input, &runtime)
        .unwrap()
        .update_next(&mut first, &runtime)
        .unwrap();
    assert_eq!(first.sum, 40);
    assert_eq!(second.sum, 0);
    let empty_rows = [];
    let empty = Selection::try_sparse(5, &empty_rows).unwrap();
    let input =
        SelectedAggregateUpdateInput::try_new(&kernel.contract, empty, &args, &[], &runtime)
            .unwrap();
    let mut call = AggregateUpdateInvocation::try_new(kernel.as_ref(), input, &runtime).unwrap();
    assert_eq!(call.next_selected_ordinal(), None);
    assert!(matches!(
        call.update_next(&mut first, &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(owner.counts.update.load(Ordering::Relaxed), 1);
    assert_eq!(first.sum, 40);
    drop(first);
    drop(second);
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 2);
}

#[test]
fn equal_foreign_contracts_are_rejected_before_batch_or_state_creation() {
    let mut owner = Owner::new(1, false);
    owner.behavior.foreign = true;
    let input = owner.input();
    let result = specialize_aggregate(
        &owner,
        input,
        owner.selected.clone(),
        ScopedExpressionEffects::pure_value(input.context),
        owner.options(AggregateKernelPhase::Single),
        &CompileControl::default(),
    );
    assert!(matches!(
        result,
        Err(FunctionSpecializationFailure::Kernel(
            KernelFailure::Internal(_)
        ))
    ));
    assert_eq!(owner.counts.create.load(Ordering::Relaxed), 0);
    owner.behavior.foreign = false;
    let runtime = RuntimeControl::default();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Final] {
        let kernel = owner.specialize(phase).into_prepared();
        let foreign = (*kernel.contract).clone();
        assert_eq!(foreign, *kernel.contract);
        let batches = owner.counts.batches.load(Ordering::Relaxed);
        if phase.consumes_logical_arguments() {
            let input = SelectedAggregateUpdateInput::try_new(
                &foreign,
                Selection::all(5),
                &args,
                &[],
                &runtime,
            )
            .unwrap();
            assert!(matches!(
                AggregateUpdateInvocation::try_new(kernel.as_ref(), input, &runtime),
                Err(KernelFailure::InvalidProgram(_))
            ));
        } else {
            let input = SelectedAggregateMergeInput::try_new(
                &foreign,
                Selection::all(5),
                args[0],
                &runtime,
            )
            .unwrap();
            assert!(matches!(
                AggregateMergeInvocation::try_new(kernel.as_ref(), input, &runtime),
                Err(KernelFailure::InvalidProgram(_))
            ));
        }
        assert_eq!(owner.counts.batches.load(Ordering::Relaxed), batches);
    }
}

#[test]
fn update_and_merge_failure_latch_preserves_successful_prefix_without_replay() {
    for failure in failures() {
        let mut owner = Owner::new(1, false);
        owner.behavior.row_failure = Some((1, failure.clone()));
        let runtime = RuntimeControl::default();
        let values = value_array();
        let args = [EvaluatedArgument::Column(&values)];
        let rows = [1, 3];
        let selected = Selection::try_sparse(5, &rows).unwrap();
        for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Final] {
            let kernel = owner.specialize(phase).into_prepared();
            let mut state = create_aggregate_state(kernel.as_ref(), &runtime).unwrap();
            if phase.consumes_logical_arguments() {
                let input = SelectedAggregateUpdateInput::try_new(
                    &kernel.contract,
                    selected,
                    &args,
                    &[],
                    &runtime,
                )
                .unwrap();
                let mut call =
                    AggregateUpdateInvocation::try_new(kernel.as_ref(), input, &runtime).unwrap();
                call.update_next(&mut state, &runtime).unwrap();
                assert_eq!(state.sum, 20);
                assert_eq!(call.update_next(&mut state, &runtime), Err(failure.clone()));
                assert_eq!(call.next_selected_ordinal(), None);
                assert_eq!(call.next_batch_row(), None);
                assert_eq!(
                    call.update_next(&mut state, &runtime),
                    Err(KernelFailure::InstanceFailed)
                );
                assert_eq!(owner.counts.update.load(Ordering::Relaxed), 2);
            } else {
                let input = SelectedAggregateMergeInput::try_new(
                    &kernel.contract,
                    selected,
                    args[0],
                    &runtime,
                )
                .unwrap();
                let mut call =
                    AggregateMergeInvocation::try_new(kernel.as_ref(), input, &runtime).unwrap();
                call.merge_next(&mut state, &runtime).unwrap();
                assert_eq!(state.sum, 20);
                assert_eq!(call.merge_next(&mut state, &runtime), Err(failure.clone()));
                assert_eq!(call.next_selected_ordinal(), None);
                assert_eq!(call.next_batch_row(), None);
                assert_eq!(
                    call.merge_next(&mut state, &runtime),
                    Err(KernelFailure::InstanceFailed)
                );
                assert_eq!(owner.counts.merge.load(Ordering::Relaxed), 2);
            }
            assert_eq!(state.sum, 20);
            assert_eq!(state.last_row, Some(1));
            drop(state);
        }
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 2);
        // The host still owns its whole-operator latch; a new invocation is not tested as a retry.
    }
}

#[test]
fn construction_raii_and_retained_overflow_precede_publication() {
    let runtime = RuntimeControl::default();
    for failure in failures() {
        let mut owner = Owner::new(0, false);
        owner.behavior.create_failure = Some(failure.clone());
        let kernel = owner
            .specialize(AggregateKernelPhase::Single)
            .into_prepared();
        assert_eq!(
            create_aggregate_state(kernel.as_ref(), &runtime).unwrap_err(),
            failure
        );
        assert_eq!(owner.counts.create.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
    }
    let mut owner = Owner::new(0, false);
    owner.behavior.bound = usize::MAX;
    let input = owner.input();
    let result = specialize_aggregate(
        &owner,
        input,
        owner.selected.clone(),
        ScopedExpressionEffects::pure_value(input.context),
        owner.options(AggregateKernelPhase::Single),
        &CompileControl::default(),
    );
    assert!(matches!(
        result,
        Err(FunctionSpecializationFailure::Kernel(
            KernelFailure::ResourceExhausted
        ))
    ));
    assert_eq!(owner.counts.create.load(Ordering::Relaxed), 0);
    owner.behavior.bound = 0;
    let prepared = owner
        .specialize(AggregateKernelPhase::Single)
        .into_prepared();
    let overflow = Kernel {
        contract: prepared.contract.clone(),
        counts: owner.counts.clone(),
        behavior: Behavior {
            bound: usize::MAX,
            ..Behavior::default()
        },
    };
    assert!(matches!(
        create_aggregate_state(&overflow, &runtime),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(owner.counts.create.load(Ordering::Relaxed), 0);
    let growth = Kernel {
        contract: prepared.contract.clone(),
        counts: owner.counts.clone(),
        behavior: Behavior {
            create_growth: true,
            ..Behavior::default()
        },
    };
    assert!(matches!(
        create_aggregate_state(&growth, &runtime),
        Err(KernelFailure::Internal(_))
    ));
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
}

#[test]
fn lifetime_bound_checks_success_and_preserves_every_originating_error_and_cleanup() {
    let runtime = RuntimeControl::default();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Final] {
        for failure in [
            None,
            Some(operational()),
            Some(actual_aggregate_scalar_source()),
            Some(internal("primary")),
            Some(KernelFailure::Cancelled),
            Some(KernelFailure::DeadlineExceeded),
            Some(KernelFailure::ResourceExhausted),
        ] {
            let mut owner = Owner::new(1, false);
            owner.behavior.row_growth = true;
            owner.behavior.row_failure = failure.clone().map(|error| (0, error));
            let kernel = owner.specialize(phase).into_prepared();
            let mut state = create_aggregate_state(kernel.as_ref(), &runtime).unwrap();
            let result = if phase.consumes_logical_arguments() {
                let input = SelectedAggregateUpdateInput::try_new(
                    &kernel.contract,
                    Selection::all(5),
                    &args,
                    &[],
                    &runtime,
                )
                .unwrap();
                let mut invocation =
                    AggregateUpdateInvocation::try_new(kernel.as_ref(), input, &runtime).unwrap();
                let result = invocation.update_next(&mut state, &runtime);
                assert_eq!(
                    invocation.update_next(&mut state, &runtime),
                    Err(KernelFailure::InstanceFailed)
                );
                result
            } else {
                let input = SelectedAggregateMergeInput::try_new(
                    &kernel.contract,
                    Selection::all(5),
                    args[0],
                    &runtime,
                )
                .unwrap();
                let mut invocation =
                    AggregateMergeInvocation::try_new(kernel.as_ref(), input, &runtime).unwrap();
                let result = invocation.merge_next(&mut state, &runtime);
                assert_eq!(
                    invocation.merge_next(&mut state, &runtime),
                    Err(KernelFailure::InstanceFailed)
                );
                result
            };
            match failure {
                Some(error) => assert_eq!(result, Err(error)),
                None => assert!(matches!(result, Err(KernelFailure::Internal(_)))),
            }
            assert!(kernel.retained_bytes(&state) > 0);
            drop(state);
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        }
    }
}

#[test]
fn emission_carrier_faults_are_internal_and_capacity_precedes_emitter() {
    let runtime = RuntimeControl::default();
    for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
        for emit in [Emit::WrongRows, Emit::WrongType, Emit::Null] {
            let mut owner = Owner::new(0, false);
            owner.behavior.emit = emit;
            let kernel = owner.specialize(phase).into_prepared();
            let state = create_aggregate_state(kernel.as_ref(), &runtime).unwrap();
            assert_eq!(
                emit_aggregate(kernel.as_ref(), [&state].into_iter(), 0, &runtime).unwrap_err(),
                KernelFailure::ResourceExhausted
            );
            assert_eq!(
                owner.counts.intermediate.load(Ordering::Relaxed)
                    + owner.counts.final_emit.load(Ordering::Relaxed),
                0
            );
            assert!(matches!(
                emit_aggregate(kernel.as_ref(), [&state].into_iter(), 1, &runtime),
                Err(ref cause) if crate::kernel_control::test_contract::output(cause)
            ));
        }
        for failure in failures() {
            let mut owner = Owner::new(0, false);
            owner.behavior.emit = Emit::Failure(failure.clone());
            let kernel = owner.specialize(phase).into_prepared();
            let state = create_aggregate_state(kernel.as_ref(), &runtime).unwrap();
            assert_eq!(
                emit_aggregate(kernel.as_ref(), [&state].into_iter(), 1, &runtime).unwrap_err(),
                failure
            );
        }
    }
}

#[test]
fn emission_checks_successful_growth_and_preserves_every_originating_error() {
    let runtime = RuntimeControl::default();
    for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
        for failure in [
            None,
            Some(operational()),
            Some(actual_aggregate_scalar_source()),
            Some(KernelFailure::Cancelled),
            Some(KernelFailure::DeadlineExceeded),
            Some(KernelFailure::ResourceExhausted),
        ] {
            let mut owner = Owner::new(0, false);
            owner.behavior.emit = Emit::Grow(failure.clone());
            let kernel = owner.specialize(phase).into_prepared();
            let state = create_aggregate_state(kernel.as_ref(), &runtime).unwrap();
            let result = emit_aggregate(kernel.as_ref(), [&state].into_iter(), 1, &runtime);
            match failure {
                Some(error) => assert_eq!(result.unwrap_err(), error),
                None => assert!(matches!(result, Err(KernelFailure::Internal(_)))),
            }
            drop(state);
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        }
    }
}

#[test]
fn compile_entry_and_positive_quantum_controls_remain_typed_before_preparation() {
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive in [false, true] {
            let owner = Owner::new(if positive { 300 } else { 1 }, false);
            let input = owner.input();
            let control = CompileControl {
                failure: Some(failure.clone()),
                quantum_only: positive,
                ..CompileControl::default()
            };
            for frozen in [false, true] {
                let child = ScopedExpressionEffects::pure_value(input.context);
                let result = if frozen {
                    specialize_frozen_aggregate(
                        &owner,
                        input,
                        owner.selected.clone(),
                        &owner.frozen(),
                        child,
                        owner.options(AggregateKernelPhase::Single),
                        &control,
                    )
                } else {
                    specialize_aggregate(
                        &owner,
                        input,
                        owner.selected.clone(),
                        child,
                        owner.options(AggregateKernelPhase::Single),
                        &control,
                    )
                };
                assert!(
                    matches!(result,Err(FunctionSpecializationFailure::Control(actual)) if actual==failure)
                );
                assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
                assert_eq!(owner.counts.create.load(Ordering::Relaxed), 0);
            }
            if positive {
                assert!(control.seen.lock().unwrap().contains(&256));
            } else {
                assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 0);
            }
        }
    }
}

#[test]
fn runtime_entry_and_positive_work_controls_reach_every_lifecycle_with_cleanup() {
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let owner = Owner::new(1, false);
        let ok = RuntimeControl::default();
        let values = value_array();
        let args = [EvaluatedArgument::Column(&values)];
        let entry = RuntimeControl {
            failure: Some(failure.clone()),
            ..RuntimeControl::default()
        };
        let positive = RuntimeControl {
            failure: Some(failure.clone()),
            positive: true,
            ..RuntimeControl::default()
        };
        for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Final] {
            let kernel = owner.specialize(phase).into_prepared();
            let created = owner.counts.create.load(Ordering::Relaxed);
            assert_eq!(
                create_aggregate_state(kernel.as_ref(), &entry).unwrap_err(),
                failure
            );
            assert_eq!(owner.counts.create.load(Ordering::Relaxed), created);
            assert_eq!(
                create_aggregate_state(kernel.as_ref(), &positive).unwrap_err(),
                failure
            );
            let mut state = create_aggregate_state(kernel.as_ref(), &ok).unwrap();
            if phase.consumes_logical_arguments() {
                let input = SelectedAggregateUpdateInput::try_new(
                    &kernel.contract,
                    Selection::all(5),
                    &args,
                    &[],
                    &ok,
                )
                .unwrap();
                assert!(
                    matches!(AggregateUpdateInvocation::try_new(kernel.as_ref(),input,&entry),Err(actual) if actual==failure)
                );
                assert!(
                    matches!(AggregateUpdateInvocation::try_new(kernel.as_ref(),input,&positive),Err(actual) if actual==failure)
                );
                for control in [&entry, &positive] {
                    let mut call =
                        AggregateUpdateInvocation::try_new(kernel.as_ref(), input, &ok).unwrap();
                    assert_eq!(call.update_next(&mut state, control), Err(failure.clone()));
                    assert_eq!(
                        call.update_next(&mut state, &ok),
                        Err(KernelFailure::InstanceFailed)
                    );
                }
            } else {
                let input = SelectedAggregateMergeInput::try_new(
                    &kernel.contract,
                    Selection::all(5),
                    args[0],
                    &ok,
                )
                .unwrap();
                assert!(
                    matches!(AggregateMergeInvocation::try_new(kernel.as_ref(),input,&entry),Err(actual) if actual==failure)
                );
                assert!(
                    matches!(AggregateMergeInvocation::try_new(kernel.as_ref(),input,&positive),Err(actual) if actual==failure)
                );
                for control in [&entry, &positive] {
                    let mut call =
                        AggregateMergeInvocation::try_new(kernel.as_ref(), input, &ok).unwrap();
                    assert_eq!(call.merge_next(&mut state, control), Err(failure.clone()));
                    assert_eq!(
                        call.merge_next(&mut state, &ok),
                        Err(KernelFailure::InstanceFailed)
                    );
                }
            }
            assert_eq!(state.sum, 0);
            for control in [&entry, &positive] {
                assert_eq!(
                    emit_aggregate(kernel.as_ref(), [&state].into_iter(), 1, control).unwrap_err(),
                    failure
                );
            }
            drop(state);
        }
        assert!(positive.seen.lock().unwrap().contains(&256));
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 2);
    }
}

#[test]
fn empty_update_and_merge_skip_private_batch_preparation_and_do_not_read_carriers() {
    let owner = Owner::new(1, false);
    let ok = RuntimeControl::default();
    let rows = [];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    let reject_private_work = RuntimeControl {
        failure: Some(KernelFailure::Cancelled),
        positive: true,
        ..RuntimeControl::default()
    };
    for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Final] {
        let kernel = owner.specialize(phase).into_prepared();
        let mut state = create_aggregate_state(kernel.as_ref(), &ok).unwrap();
        let before = owner.counts.batches.load(Ordering::Relaxed);
        if phase.consumes_logical_arguments() {
            let input =
                SelectedAggregateUpdateInput::try_new(&kernel.contract, selection, &args, &[], &ok)
                    .unwrap();
            let mut call =
                AggregateUpdateInvocation::try_new(kernel.as_ref(), input, &reject_private_work)
                    .unwrap();
            assert_eq!(call.next_selected_ordinal(), None);
            assert_eq!(call.next_batch_row(), None);
            assert!(matches!(
                call.update_next(&mut state, &ok),
                Err(KernelFailure::InvalidProgram(_))
            ));
        } else {
            let input =
                SelectedAggregateMergeInput::try_new(&kernel.contract, selection, args[0], &ok)
                    .unwrap();
            let mut call =
                AggregateMergeInvocation::try_new(kernel.as_ref(), input, &reject_private_work)
                    .unwrap();
            assert_eq!(call.next_selected_ordinal(), None);
            assert_eq!(call.next_batch_row(), None);
            assert!(matches!(
                call.merge_next(&mut state, &ok),
                Err(KernelFailure::InvalidProgram(_))
            ));
        }
        assert_eq!(owner.counts.batches.load(Ordering::Relaxed), before);
        assert_eq!(state.sum, 0);
    }
    assert_eq!(owner.counts.update.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.merge.load(Ordering::Relaxed), 0);
    assert!(
        reject_private_work
            .seen
            .lock()
            .unwrap()
            .iter()
            .all(|work| *work == 0)
    );
}

struct FinishControl {
    failure: KernelFailure,
    zeroes: AtomicUsize,
}
impl KernelEvaluationControl for FinishControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        if work == 0 && self.zeroes.fetch_add(1, Ordering::Relaxed) == 1 {
            Err(self.failure.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("fixture does not wait")
    }
}
#[test]
fn finishing_controls_drop_unpublished_initialized_state_and_reject_prepared_batches() {
    let runtime = RuntimeControl::default();
    let values = value_array();
    let args = [EvaluatedArgument::Column(&values)];
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let owner = Owner::new(1, false);
        for phase in [AggregateKernelPhase::Single, AggregateKernelPhase::Final] {
            let kernel = owner.specialize(phase).into_prepared();
            let control = FinishControl {
                failure: failure.clone(),
                zeroes: AtomicUsize::new(0),
            };
            let drops = owner.counts.drop.load(Ordering::Relaxed);
            assert_eq!(
                create_aggregate_state(kernel.as_ref(), &control).unwrap_err(),
                failure
            );
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), drops + 1);
            let control = FinishControl {
                failure: failure.clone(),
                zeroes: AtomicUsize::new(0),
            };
            let batches = owner.counts.batches.load(Ordering::Relaxed);
            if phase.consumes_logical_arguments() {
                let input = SelectedAggregateUpdateInput::try_new(
                    &kernel.contract,
                    Selection::all(5),
                    &args,
                    &[],
                    &runtime,
                )
                .unwrap();
                assert!(
                    matches!(AggregateUpdateInvocation::try_new(kernel.as_ref(),input,&control),Err(actual) if actual==failure)
                );
            } else {
                let input = SelectedAggregateMergeInput::try_new(
                    &kernel.contract,
                    Selection::all(5),
                    args[0],
                    &runtime,
                )
                .unwrap();
                assert!(
                    matches!(AggregateMergeInvocation::try_new(kernel.as_ref(),input,&control),Err(actual) if actual==failure)
                );
            }
            assert_eq!(owner.counts.batches.load(Ordering::Relaxed), batches + 1);
        }
    }
}

#[test]
fn forged_frozen_effects_or_wrong_child_use_never_prepare_aggregate() {
    let owner = Owner::new(1, false);
    let input = owner.input();
    let mut frozen = owner.frozen();
    frozen.observable_effects.controlled_wait = true;
    assert!(matches!(
        specialize_frozen_aggregate(
            &owner,
            input,
            owner.selected.clone(),
            &frozen,
            ScopedExpressionEffects::pure_value(input.context),
            owner.options(AggregateKernelPhase::Single),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::InvalidInput(
            "frozen call effects differ from exact local refinement"
        ))
    ));
    let mut other = input.context;
    other.use_id = ExpressionUseId::new(99);
    assert!(matches!(
        specialize_aggregate(
            &owner,
            input,
            owner.selected.clone(),
            ScopedExpressionEffects::pure_value(other),
            owner.options(AggregateKernelPhase::Single),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::Effects(_))
    ));
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.create.load(Ordering::Relaxed), 0);
}

#[test]
fn prepared_batches_read_compact_ordinals_and_explicit_scalar_broadcast() {
    let owner = Owner::new(1, true);
    let runtime = RuntimeControl::default();
    let rows = [1, 3];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let compact: ArrayRef = Arc::new(Int64Array::from(vec![200, 400]));
    let selected = crate::SelectedValues::try_new(
        selection,
        compact.data_type(),
        compact.clone(),
        Box::new([]),
    )
    .unwrap();
    let order: ArrayRef = Arc::new(Int32Array::from(vec![7]));
    let logical = [EvaluatedArgument::SelectedColumn(&selected)];
    let orders = [EvaluatedArgument::Scalar(&order)];
    let kernel = owner
        .specialize(AggregateKernelPhase::Partial)
        .into_prepared();
    let mut state = create_aggregate_state(kernel.as_ref(), &runtime).unwrap();
    let input = SelectedAggregateUpdateInput::try_new(
        &kernel.contract,
        selection,
        &logical,
        &orders,
        &runtime,
    )
    .unwrap();
    let mut update = AggregateUpdateInvocation::try_new(kernel.as_ref(), input, &runtime).unwrap();
    update.update_next(&mut state, &runtime).unwrap();
    update.update_next(&mut state, &runtime).unwrap();
    assert_eq!(state.sum, 600);
    assert_eq!(state.last_row, Some(3));
    assert_eq!(state.last_order, Some(7));
    let merge_kernel = owner
        .specialize(AggregateKernelPhase::Final)
        .into_prepared();
    let mut merged = create_aggregate_state(merge_kernel.as_ref(), &runtime).unwrap();
    let input = SelectedAggregateMergeInput::try_new(
        &merge_kernel.contract,
        selection,
        logical[0],
        &runtime,
    )
    .unwrap();
    let mut merge =
        AggregateMergeInvocation::try_new(merge_kernel.as_ref(), input, &runtime).unwrap();
    merge.merge_next(&mut merged, &runtime).unwrap();
    merge.merge_next(&mut merged, &runtime).unwrap();
    assert_eq!(merged.sum, 600);
    assert_eq!(merged.last_row, Some(3));
}

fn actual_aggregate_scalar_source() -> KernelFailure {
    let source = crate::aggregate_scalar::build_scalar_array(
        &DataType::Boolean,
        vec![Some(crate::aggregate_scalar::AggScalarValue::Int64(1))],
        &mut crate::aggregate_scalar::ScalarWork::new(None),
    )
    .unwrap_err();
    let fact = novarocks_failure_contract::FailureProjection::failure_fact(&source);
    assert_eq!(
        fact.class(),
        novarocks_failure_contract::FailureClass::Invariant
    );
    assert_eq!(fact.reason().code(), 315);
    assert!(matches!(&source, KernelFailure::Source(_)));
    source
}

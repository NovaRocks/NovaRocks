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
    AggregateBindingSelection, AggregateStateFormatIdentity, CallEffectInput, FunctionArgument,
    FunctionBindingError, FunctionBindingSelection, FunctionEffectOwner, FunctionEffectOwnerError,
    FunctionId, FunctionOverloadId, RowDataError, SelectedValues, refine_call_effects,
};
use crate::{
    EvaluatedArgument, FunctionBindingRequest, FunctionKind, FunctionResultType, FunctionValueType,
    KernelDiagnostic,
};
use arrow_array::{ArrayRef, Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompileControlError,
    DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
    ExpressionUseId, FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    SemanticParameters, WindowBound, WindowFrame, WindowFrameExclusion, WindowFrameUnits,
};
use std::{
    sync::{
        Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
struct CompileControl {
    failure: Option<CompileControlError>,
    positive_only: bool,
    work: Mutex<Vec<u32>>,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= novarocks_type_contract::MAX_UNOBSERVED_COMPILE_WORK);
        self.work.lock().unwrap().push(units);
        if (!self.positive_only || units > 0)
            && let Some(failure) = self.failure.clone()
        {
            Err(failure)
        } else {
            Ok(())
        }
    }
}
#[derive(Default)]
struct RuntimeControl {
    failure: Option<KernelFailure>,
    positive_only: bool,
    work: Mutex<Vec<u32>>,
}
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= crate::MAX_UNOBSERVED_KERNEL_WORK);
        self.work.lock().unwrap().push(units);
        if (!self.positive_only || units > 0)
            && let Some(failure) = &self.failure
        {
            Err(failure.clone())
        } else {
            Ok(())
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("window contract validation must not wait")
    }
}
#[derive(Debug, Default)]
struct Counts {
    refine: AtomicUsize,
    prepare: AtomicUsize,
    begin: AtomicUsize,
    setup_rows: AtomicUsize,
    evaluate: AtomicUsize,
    finish: AtomicUsize,
    drop: AtomicUsize,
    kernel_drop: AtomicUsize,
    drop_live_owner: AtomicUsize,
    drift: AtomicUsize,
}
#[derive(Clone, Debug, Default)]
enum Output {
    #[default]
    Good,
    ForeignSelection,
    WrongType,
    Null,
    RowError,
    WrongRows,
}
#[derive(Debug, Default)]
struct Behavior {
    foreign_contract: bool,
    begin_failure: Option<KernelFailure>,
    begin_failure_at: Option<usize>,
    begin_growth: bool,
    begin_drift: bool,
    evaluate_failure: Option<KernelFailure>,
    evaluate_growth: bool,
    evaluate_drift: bool,
    finish_failure: Option<KernelFailure>,
    finish_growth: bool,
    finish_drift: bool,
    output: Output,
}
struct Fixture {
    counts: Arc<Counts>,
    behavior: Arc<Behavior>,
    kind: FunctionKind,
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    argument_uses: Vec<Option<ExpressionUseId>>,
    logical: usize,
    declaration: FunctionEffectDeclaration,
    parameters: SemanticParameters,
}
impl Fixture {
    fn new(logical: &[FunctionValueType], order: &[FunctionValueType], kind: FunctionKind) -> Self {
        let arguments = logical
            .iter()
            .chain(order)
            .cloned()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            kind,
            FunctionKind::Window | FunctionKind::Aggregate
        ));
        Self {
            counts: Arc::new(Counts::default()),
            behavior: Arc::new(Behavior::default()),
            kind,
            id: FunctionId::try_new("fixture/window-contract/exact-owner").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new(
                    "fixture/window-contract/exact-logical-signature",
                )
                .unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Box<[_]>>(),
                ),
                result_type: FunctionResultType::Scalar(i64_type(false)),
                aggregate: (kind == FunctionKind::Aggregate).then(|| AggregateBindingSelection {
                    state_argument_contract:
                        novarocks_type_contract::AggregateStateArgumentContract::ExactSignature,
                    intermediate_type: i64_type(false),
                    state_format: AggregateStateFormatIdentity::try_new(
                        "fixture/window-contract/state-v1",
                    )
                    .unwrap(),
                }),
            }),
            argument_uses: (0..arguments.len())
                .map(|ordinal| Some(ExpressionUseId::new(ordinal as u32 + 1)))
                .collect(),
            arguments,
            logical: logical.len(),
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::NotRowEvaluated,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: if kind == FunctionKind::Window {
                    ArgumentControl::Window
                } else {
                    ArgumentControl::Aggregate
                },
                instance_state: if kind == FunctionKind::Window {
                    FunctionInstanceState::WindowPartition
                } else {
                    FunctionInstanceState::AggregateInstance
                },
                observable_effects: ObservableEffects::NONE,
                environment_dependencies: Box::new([]),
            },
            parameters: SemanticParameters::default(),
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: ExpressionEffectContext {
                use_id: ExpressionUseId::new(0),
                region: EvaluationRegionId::new(9),
                demand: EvaluationDemand::Value,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.argument_uses),
            function_id: &self.id,
            kind: self.kind,
            selected: self.selected.as_ref(),
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
    fn call(&self) -> Arc<FunctionCallContract> {
        let input = self.input();
        let receipt = refine_call_effects(self, input, &CompileControl::default()).unwrap();
        Arc::new(
            FunctionCallContract::from_refined(
                input,
                &receipt,
                self.selected.clone(),
                &CompileControl::default(),
            )
            .unwrap(),
        )
    }
}
impl FunctionEffectOwner for Fixture {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        id: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if id != &self.id || selected != self.selected.as_ref() {
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
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        if input.function_id != &self.id
            || input.kind != self.kind
            || input.selected != self.selected.as_ref()
            || input.request.logical_argument_count != self.logical
            || input.request.arguments.len() != self.arguments.len()
            || !input.environment.is_empty()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "fixture exact window or aggregate call differs".into(),
            )
            .into());
        }
        for (argument, expected) in input.request.arguments.iter().zip(&self.arguments) {
            if !argument.equals_observed(expected, CompilePhase::FunctionSpecialization, control)? {
                return Err(FunctionBindingError::InvalidBinding(
                    "fixture exact argument differs".into(),
                )
                .into());
            }
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(CallEffects {
            value_stability: self.declaration.value_stability,
            own_row_error: self.declaration.own_row_error,
            failure_behavior: self.declaration.failure_behavior,
            null_behavior: self.declaration.null_behavior,
            argument_control: self.declaration.argument_control,
            instance_state: self.declaration.instance_state,
            observable_effects: self.declaration.observable_effects,
            environment: Box::new([]),
            proof_scope: input.proof_scope,
        })
    }
}

fn i64_type(nullable: bool) -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, nullable)
}
fn options() -> WindowCallOptions {
    WindowCallOptions::try_new(None, false, &CompileControl::default()).unwrap()
}

impl FunctionBindingResolver for Fixture {
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
            || !crate::binding::arguments_equal_for_test(
                request.arguments,
                &self.arguments,
                control,
            )?
            || request.logical_argument_count != self.logical
        {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(())
    }
}
impl Fixture {
    fn prepared(&self) -> Arc<dyn PreparedWindowKernel> {
        let input = self.input();
        specialize_window(
            self,
            input,
            self.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context),
            options(),
            &CompileControl::default(),
        )
        .unwrap()
        .into_prepared()
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
}
impl PureWindowImplementation for Fixture {
    fn prepare_window(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<WindowCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedWindowKernel>, KernelFailure> {
        self.counts.prepare.fetch_add(1, Ordering::Relaxed);
        assert!(std::ptr::eq(contract.call().selected(), input.selected));
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(crate::kernel_control::compile_failure)?;
        let contract = if self.behavior.foreign_contract {
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
    contract: Arc<WindowCallContract>,
    counts: Arc<Counts>,
    behavior: Arc<Behavior>,
}
impl Drop for Kernel {
    fn drop(&mut self) {
        self.counts.kernel_drop.fetch_add(1, Ordering::Relaxed);
    }
}
struct Instance<'a> {
    owner: Weak<Kernel>,
    input: WindowPartitionInput<'a>,
    counts: Arc<Counts>,
    heap: Vec<u8>,
}
impl Drop for Instance<'_> {
    fn drop(&mut self) {
        self.counts.drop.fetch_add(1, Ordering::Relaxed);
        if self.owner.upgrade().is_some() {
            self.counts.drop_live_owner.fetch_add(1, Ordering::Relaxed);
        }
    }
}
impl PreparedWindowKernel for Kernel {
    fn contract(&self) -> &Arc<WindowCallContract> {
        &self.contract
    }
    fn partition_retained_upper_bound(&self, _: usize) -> Result<usize, KernelFailure> {
        Ok(size_of::<Instance<'static>>() + self.counts.drift.load(Ordering::Relaxed))
    }
    fn begin_partition<'a>(
        self: Arc<Self>,
        input: WindowPartitionInput<'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn WindowKernelPartition + 'a>, KernelFailure> {
        self.counts.begin.fetch_add(1, Ordering::Relaxed);
        control.checkpoint(1)?;
        let instance = Instance {
            owner: Arc::downgrade(&self),
            input,
            counts: self.counts.clone(),
            heap: if self.behavior.begin_growth {
                vec![1]
            } else {
                vec![]
            },
        };
        // Required complete-input setup executes before any output Selection exists.
        let mut work = EvaluationCheckpoints::new(control);
        for row in 0..input.full_input().partition_rows() {
            for argument in input.full_input().logical_arguments() {
                let array = argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let _ = array.value(argument.value_row(row, row));
            }
            self.counts.setup_rows.fetch_add(1, Ordering::Relaxed);
            if self.behavior.begin_failure_at == Some(row) {
                return Err(operational());
            }
            work.step()?;
        }
        work.finish()?;
        if self.behavior.begin_drift {
            self.counts.drift.store(1, Ordering::Relaxed);
        }
        if let Some(failure) = &self.behavior.begin_failure {
            return Err(failure.clone());
        }
        Ok(Box::new(instance))
    }
}
impl WindowKernelPartition for Instance<'_> {
    fn evaluate<'s>(
        &mut self,
        selection: Selection<'s>,
        _: usize,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'s>, KernelFailure> {
        self.counts.evaluate.fetch_add(1, Ordering::Relaxed);
        control.checkpoint(0)?;
        let owner = self
            .owner
            .upgrade()
            .expect("exact owner lives through instance");
        if owner.behavior.evaluate_growth {
            self.heap.push(1);
        }
        if owner.behavior.evaluate_drift {
            self.counts.drift.store(1, Ordering::Relaxed);
        }
        if let Some(error) = &owner.behavior.evaluate_failure {
            return Err(error.clone());
        }
        let mut work = EvaluationCheckpoints::new(control);
        let mut values = Vec::with_capacity(selection.len());
        // Real FIRST_VALUE reads full-input frame starts, not selected output rows.
        for ordinal in 0..selection.len() {
            let row = selection.row(ordinal).unwrap();
            let frame = self.input.frames()[row];
            let argument = self.input.full_input().logical_arguments()[0];
            let value = if frame.start == frame.end {
                None
            } else {
                let array = argument
                    .array()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                Some(array.value(argument.value_row(frame.start, frame.start)))
            };
            values.push(value);
            work.step()?;
        }
        work.finish()?;
        let actual = if matches!(owner.behavior.output, Output::ForeignSelection) {
            Selection::all(selection.batch_rows())
        } else {
            selection
        };
        let array: ArrayRef = match owner.behavior.output {
            Output::ForeignSelection => Arc::new(Int64Array::from(vec![Some(0); actual.len()])),
            Output::WrongType => Arc::new(Int32Array::from(vec![0; actual.len()])),
            Output::Null | Output::RowError => Arc::new(Int64Array::from(vec![None; actual.len()])),
            Output::WrongRows => {
                values.push(Some(0));
                Arc::new(Int64Array::from(values))
            }
            Output::Good => Arc::new(Int64Array::from(values)),
        };
        let errors: Box<[RowDataError]> = if matches!(owner.behavior.output, Output::RowError) {
            Box::from([RowDataError::new(0, "fixture input failure")])
        } else {
            Box::new([])
        };
        SelectedValues::try_new(actual, array.data_type(), array.clone(), errors)
            .map_err(|_| internal("fixture malformed result carrier"))
    }
    fn finish(&mut self, control: &dyn KernelEvaluationControl) -> Result<(), KernelFailure> {
        self.counts.finish.fetch_add(1, Ordering::Relaxed);
        control.checkpoint(1)?;
        let owner = self.owner.upgrade().unwrap();
        if owner.behavior.finish_growth {
            self.heap.push(1);
        }
        if owner.behavior.finish_drift {
            self.counts.drift.store(1, Ordering::Relaxed);
        }
        owner.behavior.finish_failure.clone().map_or(Ok(()), Err)
    }
    fn retained_bytes(&self) -> usize {
        size_of::<Self>() + self.heap.capacity()
    }
}
fn fixture() -> Fixture {
    Fixture::new(&[i64_type(false)], &[], FunctionKind::Window)
}
fn full<'a>(
    contract: &'a WindowCallContract,
    arguments: &'a [EvaluatedArgument<'a>],
    rows: usize,
) -> FullPartitionWindowInput<'a, 'a> {
    FullPartitionWindowInput::try_new(contract, rows, arguments, &[], &RuntimeControl::default())
        .unwrap()
}
fn ranges(rows: usize) -> (Vec<WindowRowRange>, Vec<WindowRowRange>) {
    (
        (rows > 0)
            .then_some(WindowRowRange {
                start: 0,
                end: rows,
            })
            .into_iter()
            .collect(),
        vec![
            WindowRowRange {
                start: 0,
                end: rows
            };
            rows
        ],
    )
}
fn operational() -> KernelFailure {
    KernelFailure::Operational(KernelDiagnostic::new("fixture lifecycle data failure"))
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
fn values() -> arrow_array::ArrayRef {
    Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]))
}

#[test]
fn sparse_first_value_reads_unselected_full_arguments_and_preserves_independent_output() {
    let owner = fixture();
    let kernel = owner.prepared();
    let array = values();
    let args = [EvaluatedArgument::Column(&array)];
    let peers = [
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 2, end: 5 },
    ];
    let frames = [
        WindowRowRange { start: 0, end: 1 },
        WindowRowRange { start: 0, end: 2 },
        WindowRowRange { start: 1, end: 3 },
        WindowRowRange { start: 1, end: 4 },
        WindowRowRange { start: 2, end: 5 },
    ];
    let input = WindowPartitionInput::try_new(
        full(kernel.contract(), &args, 5),
        &peers,
        &frames,
        &RuntimeControl::default(),
    )
    .unwrap();
    assert!(std::ptr::eq(input.frames().as_ptr(), frames.as_ptr()));
    assert_eq!(input.full_input().partition_rows(), 5);
    let mut partition =
        WindowEvaluationPartition::begin(kernel.clone(), input, &RuntimeControl::default())
            .unwrap();
    let rows = [1, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    let output = partition
        .evaluate(selection, 2, &RuntimeControl::default())
        .unwrap();
    assert_eq!(output.selection(), selection);
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[10, 30]
    );
    assert!(partition.retained_bytes().unwrap() <= partition.retained_upper_bound());
    partition.finish(&RuntimeControl::default()).unwrap();
    assert_eq!(
        partition.finish(&RuntimeControl::default()),
        Err(KernelFailure::InstanceFailed)
    );
    assert_eq!(
        partition
            .evaluate(selection, 2, &RuntimeControl::default())
            .unwrap_err(),
        KernelFailure::InstanceFailed
    );
    drop(partition);
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.evaluate.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
}

#[test]
fn empty_output_runs_complete_begin_and_finish_but_never_private_evaluate() {
    let owner = fixture();
    let kernel = owner.prepared();
    let array = values();
    let args = [EvaluatedArgument::Column(&array)];
    let (peers, frames) = ranges(5);
    let runtime = RuntimeControl::default();
    let input =
        WindowPartitionInput::try_new(full(kernel.contract(), &args, 5), &peers, &frames, &runtime)
            .unwrap();
    let mut partition = WindowEvaluationPartition::begin(kernel.clone(), input, &runtime).unwrap();
    let empty_rows = [];
    let output = partition
        .evaluate(Selection::try_sparse(5, &empty_rows).unwrap(), 0, &runtime)
        .unwrap();
    assert_eq!(output.values().len(), 0);
    assert_eq!(owner.counts.setup_rows.load(Ordering::Relaxed), 5);
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.evaluate.load(Ordering::Relaxed), 0);
    partition.finish(&runtime).unwrap();
    assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 1);
    drop(partition);
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
    let owner = fixture();
    let kernel = owner.prepared();
    let empty: ArrayRef = Arc::new(Int64Array::from(Vec::<i64>::new()));
    let args = [EvaluatedArgument::Column(&empty)];
    let input =
        WindowPartitionInput::try_new(full(kernel.contract(), &args, 0), &[], &[], &runtime)
            .unwrap();
    let mut partition = WindowEvaluationPartition::begin(kernel.clone(), input, &runtime).unwrap();
    assert_eq!(
        partition
            .evaluate(Selection::all(0), 0, &runtime)
            .unwrap()
            .values()
            .len(),
        0
    );
    partition.finish(&runtime).unwrap();
}

#[test]
fn required_unselected_late_input_failure_survives_empty_output_demand_and_cleans_partial_instance()
{
    let mut owner = fixture();
    Arc::get_mut(&mut owner.behavior).unwrap().begin_failure_at = Some(4);
    let kernel = owner.prepared();
    let array = values();
    let args = [EvaluatedArgument::Column(&array)];
    let (peers, frames) = ranges(5);
    let runtime = RuntimeControl::default();
    let input =
        WindowPartitionInput::try_new(full(kernel.contract(), &args, 5), &peers, &frames, &runtime)
            .unwrap();
    assert!(
        matches!(WindowEvaluationPartition::begin(kernel.clone(),input,&runtime),Err(actual) if actual==operational())
    );
    assert_eq!(owner.counts.setup_rows.load(Ordering::Relaxed), 5);
    assert_eq!(owner.counts.evaluate.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
}

#[test]
fn geometry_rejects_holes_overlap_empty_peers_frame_count_and_range_without_beginning() {
    let owner = fixture();
    let kernel = owner.prepared();
    let array = values();
    let args = [EvaluatedArgument::Column(&array)];
    let runtime = RuntimeControl::default();
    let input = full(kernel.contract(), &args, 5);
    let (_, frames) = ranges(5);
    for peers in [
        vec![],
        vec![WindowRowRange { start: 1, end: 5 }],
        vec![
            WindowRowRange { start: 0, end: 2 },
            WindowRowRange { start: 3, end: 5 },
        ],
        vec![
            WindowRowRange { start: 0, end: 3 },
            WindowRowRange { start: 2, end: 5 },
        ],
        vec![
            WindowRowRange { start: 0, end: 0 },
            WindowRowRange { start: 0, end: 5 },
        ],
        vec![WindowRowRange { start: 0, end: 6 }],
    ] {
        assert!(matches!(
            WindowPartitionInput::try_new(input, &peers, &frames, &runtime),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    let peers = [WindowRowRange { start: 0, end: 5 }];
    for bad in [
        vec![WindowRowRange { start: 0, end: 5 }; 4],
        vec![WindowRowRange { start: 3, end: 2 }; 5],
        vec![WindowRowRange { start: 0, end: 6 }; 5],
    ] {
        assert!(matches!(
            WindowPartitionInput::try_new(input, &peers, &bad, &runtime),
            Err(KernelFailure::InvalidProgram(_))
        ));
    }
    let empty_frames = vec![WindowRowRange { start: 2, end: 2 }; 5];
    assert!(WindowPartitionInput::try_new(input, &peers, &empty_frames, &runtime).is_ok());
    let excluded = WindowCallOptions::try_new(
        Some(WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::UnboundedPreceding,
            end: WindowBound::CurrentRow,
            exclusion: WindowFrameExclusion::Ties,
        }),
        false,
        &CompileControl::default(),
    )
    .unwrap();
    let contract =
        WindowCallContract::try_window(owner.call(), excluded, &CompileControl::default()).unwrap();
    assert!(matches!(
        WindowPartitionInput::try_new(full(&contract, &args, 5), &peers, &frames, &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
}

#[test]
fn value_equal_foreign_contract_rejects_before_begin_and_owner_lives_until_typed_drop() {
    let owner = fixture();
    let kernel = owner.prepared();
    let weak = Arc::downgrade(&kernel);
    let contract = kernel.contract().clone();
    let foreign = (*contract).clone();
    assert_eq!(foreign, *contract);
    let array = values();
    let args = [EvaluatedArgument::Column(&array)];
    let (peers, frames) = ranges(5);
    let runtime = RuntimeControl::default();
    let input =
        WindowPartitionInput::try_new(full(&foreign, &args, 5), &peers, &frames, &runtime).unwrap();
    assert!(matches!(
        WindowEvaluationPartition::begin(kernel.clone(), input, &runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
    let input = WindowPartitionInput::try_new(full(&contract, &args, 5), &peers, &frames, &runtime)
        .unwrap();
    let partition = WindowEvaluationPartition::begin(kernel.clone(), input, &runtime).unwrap();
    drop(kernel);
    assert!(weak.upgrade().is_some());
    drop(partition);
    assert!(weak.upgrade().is_none());
    assert_eq!(owner.counts.drop_live_owner.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.kernel_drop.load(Ordering::Relaxed), 1);
}

#[test]
fn wrong_output_shape_selection_type_null_error_and_capacity_latch_partition() {
    let array = values();
    let args = [EvaluatedArgument::Column(&array)];
    let (peers, frames) = ranges(5);
    let runtime = RuntimeControl::default();
    let rows = [1, 4];
    let selection = Selection::try_sparse(5, &rows).unwrap();
    for output in [
        Output::ForeignSelection,
        Output::WrongType,
        Output::Null,
        Output::RowError,
        Output::WrongRows,
    ] {
        let mut owner = fixture();
        Arc::get_mut(&mut owner.behavior).unwrap().output = output;
        let kernel = owner.prepared();
        let input = WindowPartitionInput::try_new(
            full(kernel.contract(), &args, 5),
            &peers,
            &frames,
            &runtime,
        )
        .unwrap();
        let mut partition =
            WindowEvaluationPartition::begin(kernel.clone(), input, &runtime).unwrap();
        assert!(matches!(
            partition.evaluate(selection, 2, &runtime),
            Err(ref cause) if crate::kernel_control::test_contract::output(cause)
        ));
        assert_eq!(
            partition.evaluate(selection, 2, &runtime).unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(
            partition.finish(&runtime),
            Err(KernelFailure::InstanceFailed)
        );
        assert_eq!(owner.counts.evaluate.load(Ordering::Relaxed), 1);
    }
    for (selection, capacity) in [(selection, 1), (Selection::all(6), 6)] {
        let owner = fixture();
        let kernel = owner.prepared();
        let input = WindowPartitionInput::try_new(
            full(kernel.contract(), &args, 5),
            &peers,
            &frames,
            &runtime,
        )
        .unwrap();
        let mut partition =
            WindowEvaluationPartition::begin(kernel.clone(), input, &runtime).unwrap();
        assert!(matches!(
            partition.evaluate(selection, capacity, &runtime),
            Err(ref cause) if crate::kernel_control::test_contract::input(cause)
        ));
        assert_eq!(owner.counts.evaluate.load(Ordering::Relaxed), 0);
        assert_eq!(
            partition.finish(&runtime),
            Err(KernelFailure::InstanceFailed)
        );
    }
}

#[test]
fn lifecycle_failures_preserve_categories_and_finish_runs_once_without_replay() {
    let array = values();
    let args = [EvaluatedArgument::Column(&array)];
    let (peers, frames) = ranges(5);
    let runtime = RuntimeControl::default();
    for failure in failures() {
        let mut owner = fixture();
        Arc::get_mut(&mut owner.behavior).unwrap().begin_failure = Some(failure.clone());
        let kernel = owner.prepared();
        let input = WindowPartitionInput::try_new(
            full(kernel.contract(), &args, 5),
            &peers,
            &frames,
            &runtime,
        )
        .unwrap();
        assert!(
            matches!(WindowEvaluationPartition::begin(kernel.clone(),input,&runtime),Err(actual) if actual==failure)
        );
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        for finish in [false, true] {
            let mut owner = fixture();
            let behavior = Arc::get_mut(&mut owner.behavior).unwrap();
            if finish {
                behavior.finish_failure = Some(failure.clone())
            } else {
                behavior.evaluate_failure = Some(failure.clone())
            };
            let kernel = owner.prepared();
            let input = WindowPartitionInput::try_new(
                full(kernel.contract(), &args, 5),
                &peers,
                &frames,
                &runtime,
            )
            .unwrap();
            let mut partition =
                WindowEvaluationPartition::begin(kernel.clone(), input, &runtime).unwrap();
            if finish {
                assert_eq!(partition.finish(&runtime), Err(failure.clone()));
                assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 1);
            } else {
                assert_eq!(
                    partition
                        .evaluate(Selection::all(5), 5, &runtime)
                        .unwrap_err(),
                    failure
                );
            }
            assert_eq!(
                partition.finish(&runtime),
                Err(KernelFailure::InstanceFailed)
            );
            assert_eq!(
                partition
                    .evaluate(Selection::all(5), 5, &runtime)
                    .unwrap_err(),
                KernelFailure::InstanceFailed
            );
            drop(partition);
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        }
    }
}

#[test]
fn retained_growth_and_metadata_drift_cover_success_error_and_finish_exits() {
    let array = values();
    let args = [EvaluatedArgument::Column(&array)];
    let (peers, frames) = ranges(5);
    let runtime = RuntimeControl::default();
    for drift in [false, true] {
        let mut owner = fixture();
        let behavior = Arc::get_mut(&mut owner.behavior).unwrap();
        if drift {
            behavior.begin_drift = true
        } else {
            behavior.begin_growth = true
        };
        let kernel = owner.prepared();
        let input = WindowPartitionInput::try_new(
            full(kernel.contract(), &args, 5),
            &peers,
            &frames,
            &runtime,
        )
        .unwrap();
        assert!(matches!(
            WindowEvaluationPartition::begin(kernel.clone(), input, &runtime),
            Err(KernelFailure::Internal(_))
        ));
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        for finish in [false, true] {
            for failure in [
                None,
                Some(operational()),
                Some(actual_lifecycle_source()),
                Some(KernelFailure::Cancelled),
                Some(KernelFailure::DeadlineExceeded),
                Some(KernelFailure::ResourceExhausted),
            ] {
                let mut owner = fixture();
                let behavior = Arc::get_mut(&mut owner.behavior).unwrap();
                if finish {
                    behavior.finish_failure = failure.clone();
                    behavior.finish_growth = !drift;
                    behavior.finish_drift = drift;
                } else {
                    behavior.evaluate_failure = failure.clone();
                    behavior.evaluate_growth = !drift;
                    behavior.evaluate_drift = drift;
                }
                let kernel = owner.prepared();
                let input = WindowPartitionInput::try_new(
                    full(kernel.contract(), &args, 5),
                    &peers,
                    &frames,
                    &runtime,
                )
                .unwrap();
                let mut partition =
                    WindowEvaluationPartition::begin(kernel.clone(), input, &runtime).unwrap();
                let result = if finish {
                    partition.finish(&runtime)
                } else {
                    partition
                        .evaluate(Selection::all(5), 5, &runtime)
                        .map(|_| ())
                };
                if let Some(error) = failure {
                    // Accepted first-cause migration: actual source identity,
                    // including an ordinary outer refusal, wins over late drift.
                    assert_eq!(result, Err(error));
                } else {
                    assert!(matches!(result, Err(KernelFailure::Internal(_))));
                }
                assert_eq!(
                    partition.finish(&runtime),
                    Err(KernelFailure::InstanceFailed)
                );
                assert_eq!(
                    owner.counts.evaluate.load(Ordering::Relaxed),
                    usize::from(!finish)
                );
                assert_eq!(
                    owner.counts.finish.load(Ordering::Relaxed),
                    usize::from(finish)
                );
                drop(partition);
                assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
                assert_eq!(owner.counts.drop_live_owner.load(Ordering::Relaxed), 1);
            }
        }
    }
}

struct QuantumControl {
    failure: KernelFailure,
    work: Mutex<Vec<u32>>,
}
impl KernelEvaluationControl for QuantumControl {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        self.work.lock().unwrap().push(units);
        if units == 256 {
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
fn geometry_begin_and_selected_evaluation_interrupt_at_positive_256_with_typed_errors() {
    let runtime = RuntimeControl::default();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![9; 300]));
    let args = [EvaluatedArgument::Column(&array)];
    let (peers, frames) = ranges(300);
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let owner = fixture();
        let kernel = owner.prepared();
        let control = QuantumControl {
            failure: failure.clone(),
            work: Mutex::new(vec![]),
        };
        assert!(
            matches!(WindowPartitionInput::try_new(full(kernel.contract(),&args,300),&peers,&frames,&control),Err(actual) if actual==failure)
        );
        assert!(control.work.lock().unwrap().contains(&256));
        assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
        let input = WindowPartitionInput::try_new(
            full(kernel.contract(), &args, 300),
            &peers,
            &frames,
            &runtime,
        )
        .unwrap();
        let control = QuantumControl {
            failure: failure.clone(),
            work: Mutex::new(vec![]),
        };
        assert!(
            matches!(WindowEvaluationPartition::begin(kernel.clone(),input,&control),Err(actual) if actual==failure)
        );
        assert_eq!(owner.counts.setup_rows.load(Ordering::Relaxed), 256);
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        let mut partition =
            WindowEvaluationPartition::begin(kernel.clone(), input, &runtime).unwrap();
        let control = QuantumControl {
            failure: failure.clone(),
            work: Mutex::new(vec![]),
        };
        assert_eq!(
            partition
                .evaluate(Selection::all(300), 300, &control)
                .unwrap_err(),
            failure
        );
        assert!(control.work.lock().unwrap().contains(&256));
        assert_eq!(owner.counts.evaluate.load(Ordering::Relaxed), 1);
        assert_eq!(
            partition
                .evaluate(Selection::all(300), 300, &runtime)
                .unwrap_err(),
            KernelFailure::InstanceFailed
        );
        drop(partition);
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 2);
    }
}

struct AfterPositiveControl {
    failure: KernelFailure,
    arm_at: u32,
    armed: std::sync::atomic::AtomicBool,
}
impl KernelEvaluationControl for AfterPositiveControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        if work == self.arm_at {
            self.armed.store(true, Ordering::Relaxed);
            Ok(())
        } else if work == 0 && self.armed.load(Ordering::Relaxed) {
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
fn lifecycle_entry_and_post_work_controls_cleanup_and_latch_without_skipping_drop() {
    let array = values();
    let args = [EvaluatedArgument::Column(&array)];
    let (peers, frames) = ranges(5);
    let runtime = RuntimeControl::default();
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let owner = fixture();
        let kernel = owner.prepared();
        let input = WindowPartitionInput::try_new(
            full(kernel.contract(), &args, 5),
            &peers,
            &frames,
            &runtime,
        )
        .unwrap();
        let entry = RuntimeControl {
            failure: Some(failure.clone()),
            ..RuntimeControl::default()
        };
        assert!(
            matches!(WindowEvaluationPartition::begin(kernel.clone(),input,&entry),Err(actual) if actual==failure)
        );
        assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
        let post = AfterPositiveControl {
            failure: failure.clone(),
            arm_at: 5,
            armed: std::sync::atomic::AtomicBool::new(false),
        };
        assert!(
            matches!(WindowEvaluationPartition::begin(kernel.clone(),input,&post),Err(actual) if actual==failure)
        );
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        for finish in [false, true] {
            for after in [false, true] {
                let mut partition =
                    WindowEvaluationPartition::begin(kernel.clone(), input, &runtime).unwrap();
                let post = AfterPositiveControl {
                    failure: failure.clone(),
                    arm_at: if finish { 1 } else { 5 },
                    armed: std::sync::atomic::AtomicBool::new(false),
                };
                let control: &dyn KernelEvaluationControl = if after { &post } else { &entry };
                if finish {
                    assert_eq!(partition.finish(control), Err(failure.clone()));
                } else {
                    assert_eq!(
                        partition
                            .evaluate(Selection::all(5), 5, control)
                            .unwrap_err(),
                        failure
                    );
                }
                assert_eq!(
                    partition.finish(&runtime),
                    Err(KernelFailure::InstanceFailed)
                );
                drop(partition);
            }
        }
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 5);
        assert_eq!(owner.counts.drop_live_owner.load(Ordering::Relaxed), 5);
    }
}

#[test]
fn fresh_and_frozen_specialization_refine_prepare_once_preserve_exact_options_and_no_instance() {
    let owner = fixture();
    let prepared_options = WindowCallOptions::try_new(
        Some(WindowFrame {
            units: WindowFrameUnits::Rows,
            start: WindowBound::UnboundedPreceding,
            end: WindowBound::CurrentRow,
            exclusion: WindowFrameExclusion::NoOthers,
        }),
        true,
        &CompileControl::default(),
    )
    .unwrap();
    let mut previous = None;
    for frozen in [false, true] {
        owner.counts.refine.store(0, Ordering::Relaxed);
        owner.counts.prepare.store(0, Ordering::Relaxed);
        let input = owner.input();
        let arguments = ScopedExpressionEffects::pure_value(input.context);
        let result = if frozen {
            specialize_frozen_window(
                &owner,
                input,
                owner.selected.clone(),
                &owner.frozen(),
                arguments,
                prepared_options,
                &CompileControl::default(),
            )
        } else {
            specialize_window(
                &owner,
                input,
                owner.selected.clone(),
                arguments,
                prepared_options,
                &CompileControl::default(),
            )
        }
        .unwrap();
        assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
        assert!(std::ptr::eq(
            result.prepared().contract().call().selected(),
            owner.selected.as_ref()
        ));
        assert_eq!(result.prepared().contract().options(), &prepared_options);
        let effects = result.effects().for_use(input.context).unwrap();
        if let Some(previous) = previous {
            assert_eq!(previous, effects);
        }
        previous = Some(effects);
        assert!(effects.has_instance_state);
    }
}

#[test]
fn wrong_kind_frozen_scope_and_returned_contract_reject_at_correct_preparation_boundary() {
    let owner = Fixture::new(&[i64_type(false)], &[], FunctionKind::Aggregate);
    let input = owner.input();
    assert!(matches!(
        specialize_window(
            &owner,
            input,
            owner.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context),
            options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::Kernel(
            KernelFailure::InvalidProgram(_)
        ))
    ));
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
    let owner = fixture();
    let input = owner.input();
    let mut frozen = owner.frozen();
    frozen.observable_effects.warnings = true;
    assert!(matches!(
        specialize_frozen_window(
            &owner,
            input,
            owner.selected.clone(),
            &frozen,
            ScopedExpressionEffects::pure_value(input.context),
            options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    let mut context = input.context;
    context.region = EvaluationRegionId::new(99);
    assert!(matches!(
        specialize_window(
            &owner,
            input,
            owner.selected.clone(),
            ScopedExpressionEffects::pure_value(context),
            options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::Effects(_))
    ));
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
    let mut owner = fixture();
    Arc::get_mut(&mut owner.behavior).unwrap().foreign_contract = true;
    let input = owner.input();
    assert!(matches!(
        specialize_window(
            &owner,
            input,
            owner.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context),
            options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::Kernel(
            KernelFailure::Internal(_)
        ))
    ));
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
}

#[test]
fn compile_entry_and_positive_quantum_preserve_typed_controls_before_prepare() {
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive_only in [false, true] {
            let owner = Fixture::new(
                &vec![i64_type(false); if positive_only { 300 } else { 1 }],
                &[],
                FunctionKind::Window,
            );
            let input = owner.input();
            let control = CompileControl {
                failure: Some(failure.clone()),
                positive_only,
                ..CompileControl::default()
            };
            for frozen in [false, true] {
                let arguments = ScopedExpressionEffects::pure_value(input.context);
                let result = if frozen {
                    specialize_frozen_window(
                        &owner,
                        input,
                        owner.selected.clone(),
                        &owner.frozen(),
                        arguments,
                        options(),
                        &control,
                    )
                } else {
                    specialize_window(
                        &owner,
                        input,
                        owner.selected.clone(),
                        arguments,
                        options(),
                        &control,
                    )
                };
                assert!(
                    matches!(result,Err(FunctionSpecializationFailure::Control(actual)) if actual==failure)
                );
                assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
                assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
            }
            if positive_only {
                assert!(control.work.lock().unwrap().contains(&256));
            } else {
                assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 0);
            }
        }
    }
}

fn actual_lifecycle_source() -> KernelFailure {
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

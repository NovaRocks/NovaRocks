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
    CallEffectInput, FunctionArgument, FunctionBindingError, FunctionBindingRequest,
    FunctionBindingSelection, FunctionEffectOwner, FunctionEffectOwnerError, FunctionId,
    FunctionOverloadId,
};
use crate::{
    EvaluatedArgument, FunctionKind, FunctionResultType, FunctionValueType, KernelDiagnostic,
    Selection,
};
use arrow_array::{Int32Array, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    ArgumentControl, CallEffects, CallProofScope, CompileCheckpoints, CompileControlError,
    DecimalOverflowPolicy, EvaluationDemand, EvaluationRegionId, ExpressionEffectContext,
    ExpressionUseId, FunctionEffectDeclaration, FunctionFailureBehavior, FunctionInstanceState,
    FunctionIntrinsicRowError, FunctionNullBehavior, FunctionVolatility, ObservableEffects,
    SemanticParameters,
};
use std::{
    sync::{
        Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct CompileControl(Option<CompileControlError>);
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
        self.0.clone().map_or(Ok(()), Err)
    }
}
struct RuntimeControl {
    failure: Option<KernelFailure>,
    positive_only: bool,
    work: Mutex<Vec<u32>>,
}
impl RuntimeControl {
    fn normal() -> Self {
        Self {
            failure: None,
            positive_only: false,
            work: Mutex::default(),
        }
    }
    fn fail(failure: KernelFailure, positive_only: bool) -> Self {
        Self {
            failure: Some(failure),
            positive_only,
            work: Mutex::default(),
        }
    }
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
        panic!("table input/page validation must not wait")
    }
}

#[derive(Debug, Default)]
struct Counts {
    refine: AtomicUsize,
    prepare: AtomicUsize,
    begin: AtomicUsize,
    next: AtomicUsize,
    parsed: AtomicUsize,
    finish: AtomicUsize,
    drop: AtomicUsize,
    drop_live: AtomicUsize,
    kernel_drop: AtomicUsize,
    drift: AtomicUsize,
}
#[derive(Debug, Default)]
enum Fault {
    #[default]
    None,
    EmptyMore,
    EmptyRequirements,
    GrantedRequirements,
    WrongType,
    Null,
    BadParent,
    TooManyRows,
    DuplicateComplete,
    CompletedErrorOverlap,
    LargeCompletions,
}
#[derive(Debug, Default)]
struct Behavior {
    fault: Fault,
    foreign_contract: bool,
    begin_failure: Option<KernelFailure>,
    begin_growth: bool,
    begin_drift: bool,
    next_failure: Option<KernelFailure>,
    next_growth: bool,
    next_drift: bool,
    finish_failure: Option<KernelFailure>,
    finish_growth: bool,
    finish_drift: bool,
}
struct Fixture {
    counts: Arc<Counts>,
    behavior: Arc<Behavior>,
    id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: Vec<Option<ExpressionUseId>>,
    declaration: FunctionEffectDeclaration,
    parameters: SemanticParameters,
}
impl Fixture {
    fn new(arguments: &[FunctionValueType], results: &[FunctionValueType]) -> Self {
        let arguments = arguments
            .iter()
            .cloned()
            .map(|value_type| FunctionArgument::Value {
                value_type,
                constant: None,
            })
            .collect::<Vec<_>>();
        Self {
            counts: Arc::new(Counts::default()),
            behavior: Arc::new(Behavior::default()),
            id: FunctionId::try_new("fixture/table/exact-owner").unwrap(),
            selected: Arc::new(FunctionBindingSelection {
                overload: FunctionOverloadId::try_new("fixture/table/exact-relation").unwrap(),
                argument_types: novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                    arguments
                        .iter()
                        .map(FunctionArgument::argument_type)
                        .collect::<Box<[_]>>(),
                ),
                result_type: FunctionResultType::Relation(
                    novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                        results.to_vec().into_boxed_slice(),
                    ),
                ),
                aggregate: None,
            }),
            uses: (0..arguments.len())
                .map(|ordinal| Some(ExpressionUseId::new(ordinal as u32 + 1)))
                .collect(),
            arguments,
            declaration: FunctionEffectDeclaration {
                value_stability: FunctionVolatility::Immutable,
                own_row_error: FunctionIntrinsicRowError::MayRaise,
                failure_behavior: FunctionFailureBehavior::Propagate,
                null_behavior: FunctionNullBehavior::CalledOnNull,
                argument_control: ArgumentControl::Table,
                instance_state: FunctionInstanceState::TableInstance,
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
                region: EvaluationRegionId::new(7),
                demand: EvaluationDemand::Value,
            },
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.id,
            kind: FunctionKind::Table,
            selected: self.selected.as_ref(),
            request: FunctionBindingRequest {
                expected_result_type: None,
                arguments: &self.arguments,
                logical_argument_count: self.arguments.len(),
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Unconditional,
        }
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
            || input.kind != FunctionKind::Table
            || input.selected != self.selected.as_ref()
            || input.request.logical_argument_count != self.arguments.len()
            || input.request.arguments.len() != self.arguments.len()
            || !input.environment.is_empty()
        {
            return Err(FunctionBindingError::InvalidBinding(
                "fixture exact table call differs".into(),
            )
            .into());
        }
        for (argument, expected) in input.request.arguments.iter().zip(&self.arguments) {
            if !argument.equals_observed(expected, CompilePhase::FunctionSpecialization, control)? {
                return Err(FunctionBindingError::InvalidBinding(
                    "fixture exact table argument differs".into(),
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
            || request.logical_argument_count != self.arguments.len()
        {
            return Err(FunctionBindingError::UnknownFunction);
        }
        Ok(())
    }
}
impl Fixture {
    fn prepared(&self) -> Arc<dyn PreparedTableKernel> {
        let input = self.input();
        specialize_table(
            self,
            input,
            self.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context),
            &CompileControl(None),
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
impl PureTableImplementation for Fixture {
    fn prepare_table(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<TableCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedTableKernel>, KernelFailure> {
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
    contract: Arc<TableCallContract>,
    counts: Arc<Counts>,
    behavior: Arc<Behavior>,
}
impl Drop for Kernel {
    fn drop(&mut self) {
        self.counts.kernel_drop.fetch_add(1, Ordering::Relaxed);
    }
}
struct Cursor<'a> {
    owner: Weak<Kernel>,
    input: SelectedTableInput<'a, 'a>,
    counts: Arc<Counts>,
    parent: usize,
    loaded: Option<i64>,
    offset: i64,
    heap: Vec<u8>,
}
impl Drop for Cursor<'_> {
    fn drop(&mut self) {
        self.counts.drop.fetch_add(1, Ordering::Relaxed);
        if self.owner.upgrade().is_some() {
            self.counts.drop_live.fetch_add(1, Ordering::Relaxed);
        }
    }
}
impl PreparedTableKernel for Kernel {
    fn contract(&self) -> &Arc<TableCallContract> {
        &self.contract
    }
    fn cursor_retained_upper_bound(&self, _: usize) -> Result<usize, KernelFailure> {
        Ok(size_of::<Cursor<'static>>() + self.counts.drift.load(Ordering::Relaxed))
    }
    fn begin_selected<'a>(
        self: Arc<Self>,
        input: SelectedTableInput<'a, 'a>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn TableKernelCursor + 'a>, KernelFailure> {
        self.counts.begin.fetch_add(1, Ordering::Relaxed);
        control.checkpoint(1)?;
        let cursor = Cursor {
            owner: Arc::downgrade(&self),
            input,
            counts: self.counts.clone(),
            parent: 0,
            loaded: None,
            offset: 0,
            heap: if self.behavior.begin_growth {
                vec![1]
            } else {
                vec![]
            },
        };
        if self.behavior.begin_drift {
            self.counts.drift.store(1, Ordering::Relaxed);
        }
        if let Some(error) = &self.behavior.begin_failure {
            return Err(error.clone());
        }
        Ok(Box::new(cursor))
    }
}
fn page(
    values: Vec<Option<i64>>,
    parents: Vec<usize>,
    complete: Vec<usize>,
    errors: Vec<RowDataError>,
    eof: bool,
) -> TableCursorStep {
    TableCursorStep::Page(OwnedTableOutputPage {
        columns: Box::from([Arc::new(Int64Array::from(values)) as ArrayRef]),
        parent_ordinals: parents.into_boxed_slice(),
        completed_parents: complete.into_boxed_slice(),
        parent_errors: errors.into_boxed_slice(),
        eof,
    })
}
impl TableKernelCursor for Cursor<'_> {
    fn next(
        &mut self,
        capacity: TableStepCapacity,
        control: &dyn KernelEvaluationControl,
    ) -> Result<TableCursorStep, KernelFailure> {
        self.counts.next.fetch_add(1, Ordering::Relaxed);
        control.checkpoint(1)?;
        let owner = self.owner.upgrade().unwrap();
        if owner.behavior.next_growth {
            self.heap.push(1);
        }
        if owner.behavior.next_drift {
            self.counts.drift.store(1, Ordering::Relaxed);
        }
        if let Some(error) = &owner.behavior.next_failure {
            return Err(error.clone());
        }
        match owner.behavior.fault {
            Fault::EmptyMore => return Ok(page(vec![], vec![], vec![], vec![], false)),
            Fault::EmptyRequirements => {
                return Ok(TableCursorStep::CapacityRequired(
                    TableCapacityRequirements {
                        row: false,
                        completion: false,
                        parent_error: false,
                    },
                ));
            }
            Fault::GrantedRequirements => {
                return Ok(TableCursorStep::CapacityRequired(
                    TableCapacityRequirements {
                        row: true,
                        completion: false,
                        parent_error: false,
                    },
                ));
            }
            Fault::WrongType => {
                return Ok(TableCursorStep::Page(OwnedTableOutputPage {
                    columns: Box::from([Arc::new(Int32Array::from(vec![0])) as ArrayRef]),
                    parent_ordinals: Box::from([0]),
                    completed_parents: Box::new([]),
                    parent_errors: Box::new([]),
                    eof: true,
                }));
            }
            Fault::Null => return Ok(page(vec![None], vec![0], vec![], vec![], true)),
            Fault::BadParent => {
                return Ok(page(
                    vec![Some(0)],
                    vec![self.input.selection().len()],
                    vec![],
                    vec![],
                    true,
                ));
            }
            Fault::TooManyRows => {
                return Ok(page(
                    vec![Some(0); capacity.page.rows + 1],
                    vec![0; capacity.page.rows + 1],
                    vec![],
                    vec![],
                    true,
                ));
            }
            Fault::DuplicateComplete => return Ok(page(vec![], vec![], vec![0, 0], vec![], true)),
            Fault::CompletedErrorOverlap => {
                return Ok(page(
                    vec![],
                    vec![],
                    vec![0],
                    vec![RowDataError::new(0, "fixture failure")],
                    true,
                ));
            }
            Fault::LargeCompletions => {
                return Ok(page(
                    vec![],
                    vec![],
                    (0..self.input.selection().len()).collect(),
                    vec![],
                    true,
                ));
            }
            Fault::None => {}
        }
        let parents = self.input.selection().len();
        if self.parent == parents {
            return Ok(page(vec![], vec![], vec![], vec![], true));
        }
        // Parse only one currently selected parent, never an unselected input row.
        let count = if let Some(count) = self.loaded {
            count
        } else {
            let row = self.input.selection().row(self.parent).unwrap();
            let arg = self.input.arguments()[0];
            let value = arg
                .array()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(arg.value_row(self.parent, row));
            self.counts.parsed.fetch_add(1, Ordering::Relaxed);
            self.loaded = Some(value);
            value
        };
        let required = TableCapacityRequirements {
            row: count > 0,
            completion: count == 0 || (count > 0 && self.offset + 1 == count),
            parent_error: count < 0,
        };
        if (required.row && capacity.page.rows == 0)
            || (required.completion && capacity.page.completions == 0)
            || (required.parent_error && capacity.parent_errors == 0)
        {
            return Ok(TableCursorStep::CapacityRequired(required));
        }
        let ordinal = self.parent;
        if count < 0 {
            self.parent += 1;
            self.loaded = None;
            return Ok(page(
                vec![],
                vec![],
                vec![],
                vec![RowDataError::new(ordinal, "negative expansion size")],
                self.parent == parents,
            ));
        }
        if count == 0 {
            self.parent += 1;
            self.loaded = None;
            return Ok(page(
                vec![],
                vec![],
                vec![ordinal],
                vec![],
                self.parent == parents,
            ));
        }
        let value = self.offset;
        self.offset += 1;
        let complete = if self.offset == count {
            self.parent += 1;
            self.loaded = None;
            self.offset = 0;
            vec![ordinal]
        } else {
            vec![]
        };
        Ok(page(
            vec![Some(value)],
            vec![ordinal],
            complete,
            vec![],
            self.parent == parents,
        ))
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
    Fixture::new(&[i64_type(false)], &[i64_type(false)])
}
fn input<'a>(
    contract: &'a TableCallContract,
    selection: Selection<'a>,
    args: &'a [EvaluatedArgument<'a>],
) -> SelectedTableInput<'a, 'a> {
    SelectedTableInput::try_new(contract, selection, args, &RuntimeControl::normal()).unwrap()
}
fn capacity(rows: usize, completions: usize, parent_errors: usize) -> TableStepCapacity {
    TableStepCapacity {
        page: TablePageCapacity { rows, completions },
        parent_errors,
    }
}
fn operational() -> KernelFailure {
    KernelFailure::Operational(KernelDiagnostic::new("fixture failure"))
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
fn output(step: TableCursorStep) -> OwnedTableOutputPage {
    match step {
        TableCursorStep::Page(page) => page,
        other => panic!("expected data page: {other:?}"),
    }
}

#[test]
fn actual_sparse_lazy_expansion_has_bounded_pages_parent_mapping_and_independent_completion_error()
{
    let owner = fixture();
    let kernel = owner.prepared();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![-999, 2, 0, -1, 1, 999]));
    let args = [EvaluatedArgument::Column(&array)];
    let rows = [1, 2, 3, 4];
    let selected = Selection::try_sparse(6, &rows).unwrap();
    let runtime = RuntimeControl::normal();
    let mut cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(kernel.contract(), selected, &args),
        &runtime,
    )
    .unwrap();
    assert_eq!(owner.counts.parsed.load(Ordering::Relaxed), 0);
    let grant = capacity(1, 1, 1);
    let mut produced = Vec::new();
    let mut completes = Vec::new();
    let mut errors = Vec::new();
    let mut pages = 0;
    loop {
        let page = output(cursor.next(grant, &runtime).unwrap());
        pages += 1;
        assert!(page.parent_ordinals.len() <= 1);
        assert!(page.completed_parents.len() <= 1);
        assert!(page.parent_errors.len() <= 1);
        let values = page.columns[0]
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for (i, parent) in page.parent_ordinals.iter().enumerate() {
            produced.push((selected.row(*parent).unwrap(), values.value(i)));
        }
        completes.extend_from_slice(&page.completed_parents);
        errors.extend(
            page.parent_errors
                .iter()
                .map(RowDataError::selected_ordinal),
        );
        if page.eof {
            break;
        }
    }
    assert_eq!(produced, [(1, 0), (1, 1), (4, 0)]);
    assert_eq!(completes, [0, 1, 3]);
    assert_eq!(errors, [2]);
    assert_eq!(pages, 5);
    assert_eq!(owner.counts.parsed.load(Ordering::Relaxed), 4);
    assert_eq!(
        cursor.next(grant, &runtime).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    cursor.finish(&runtime).unwrap();
    assert_eq!(cursor.finish(&runtime), Err(KernelFailure::InstanceFailed));
    assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 1);
    assert!(cursor.retained_bytes().unwrap() <= cursor.retained_upper_bound());
    drop(cursor);
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
}

#[test]
fn atomic_missing_grants_wait_without_reinvocation_then_funded_steps_progress() {
    let runtime = RuntimeControl::normal();
    for value in [1, 0, -1] {
        let owner = fixture();
        let kernel = owner.prepared();
        let array: ArrayRef = Arc::new(Int64Array::from(vec![value]));
        let args = [EvaluatedArgument::Column(&array)];
        let mut cursor = TableEvaluationCursor::begin(
            kernel.clone(),
            input(kernel.contract(), Selection::all(1), &args),
            &runtime,
        )
        .unwrap();
        let missing = capacity(0, 0, 0);
        let expected = TableCapacityRequirements {
            row: value > 0,
            completion: value >= 0,
            parent_error: value < 0,
        };
        for _ in 0..3 {
            assert!(
                matches!(cursor.next(missing,&runtime).unwrap(),TableCursorStep::CapacityRequired(actual) if actual==expected)
            );
        }
        assert_eq!(owner.counts.next.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.parsed.load(Ordering::Relaxed), 1);
        if value == 1 {
            assert!(
                matches!(cursor.next(capacity(1,0,0),&runtime).unwrap(),TableCursorStep::CapacityRequired(actual) if actual==expected)
            );
            assert_eq!(owner.counts.next.load(Ordering::Relaxed), 1);
        }
        let page = output(cursor.next(capacity(1, 1, 1), &runtime).unwrap());
        assert!(page.eof);
        assert_eq!(page.parent_ordinals.len(), usize::from(value > 0));
        assert_eq!(page.completed_parents.len(), usize::from(value >= 0));
        assert_eq!(page.parent_errors.len(), usize::from(value < 0));
        assert_eq!(owner.counts.next.load(Ordering::Relaxed), 2);
        assert_eq!(owner.counts.parsed.load(Ordering::Relaxed), 1);
        cursor.finish(&runtime).unwrap();
    }
}

#[test]
fn no_row_error_owner_cannot_request_impossible_error_capacity() {
    let mut owner = fixture();
    owner.declaration.own_row_error = FunctionIntrinsicRowError::NoRowError;
    let kernel = owner.prepared();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![-1]));
    let args = [EvaluatedArgument::Column(&array)];
    let runtime = RuntimeControl::normal();
    let mut cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(kernel.contract(), Selection::all(1), &args),
        &runtime,
    )
    .unwrap();
    assert!(matches!(
        cursor.next(capacity(1, 1, 0), &runtime),
        Err(KernelFailure::Internal(_))
    ));
    assert_eq!(
        cursor.next(capacity(1, 1, 1), &runtime).unwrap_err(),
        KernelFailure::InstanceFailed
    );
}

#[test]
fn empty_selected_input_has_explicit_eof_without_private_cursor_lifecycle() {
    let owner = fixture();
    let kernel = owner.prepared();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![-99, 99]));
    let args = [EvaluatedArgument::Column(&array)];
    let rows = [];
    let runtime = RuntimeControl::normal();
    let mut cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(
            kernel.contract(),
            Selection::try_sparse(2, &rows).unwrap(),
            &args,
        ),
        &runtime,
    )
    .unwrap();
    let page = output(cursor.next(capacity(0, 0, 0), &runtime).unwrap());
    assert!(page.eof);
    assert_eq!(page.columns.len(), 1);
    assert_eq!(page.columns[0].len(), 0);
    assert!(page.parent_ordinals.is_empty());
    assert!(page.completed_parents.is_empty());
    assert!(page.parent_errors.is_empty());
    cursor.finish(&runtime).unwrap();
    drop(cursor);
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.next.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 0);
}

#[test]
fn completion_only_and_error_only_nonterminal_progress_are_distinct_from_empty_more() {
    let owner = fixture();
    let kernel = owner.prepared();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![0, -1, 1]));
    let args = [EvaluatedArgument::Column(&array)];
    let runtime = RuntimeControl::normal();
    let mut cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(kernel.contract(), Selection::all(3), &args),
        &runtime,
    )
    .unwrap();
    let page = output(cursor.next(capacity(0, 1, 0), &runtime).unwrap());
    assert!(!page.eof);
    assert_eq!(page.completed_parents.as_ref(), &[0]);
    assert_eq!(page.columns[0].len(), 0);
    assert!(page.parent_errors.is_empty());
    let page = output(cursor.next(capacity(0, 0, 1), &runtime).unwrap());
    assert!(!page.eof);
    assert_eq!(page.parent_errors[0].selected_ordinal(), 1);
    assert!(page.completed_parents.is_empty());
    assert_eq!(page.columns[0].len(), 0);
    let page = output(cursor.next(capacity(1, 1, 0), &runtime).unwrap());
    assert!(page.eof);
    assert_eq!(page.parent_ordinals.as_ref(), &[2]);
    cursor.finish(&runtime).unwrap();
}

#[test]
fn malformed_capacity_requests_empty_more_and_producer_pages_are_internal_and_latched() {
    let runtime = RuntimeControl::normal();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let args = [EvaluatedArgument::Column(&array)];
    for fault in [
        Fault::EmptyMore,
        Fault::EmptyRequirements,
        Fault::GrantedRequirements,
        Fault::WrongType,
        Fault::Null,
        Fault::BadParent,
        Fault::TooManyRows,
        Fault::DuplicateComplete,
        Fault::CompletedErrorOverlap,
    ] {
        let mut owner = fixture();
        Arc::get_mut(&mut owner.behavior).unwrap().fault = fault;
        let kernel = owner.prepared();
        let mut cursor = TableEvaluationCursor::begin(
            kernel.clone(),
            input(kernel.contract(), Selection::all(1), &args),
            &runtime,
        )
        .unwrap();
        assert!(matches!(
            cursor.next(capacity(1, 2, 1), &runtime),
            Err(KernelFailure::Internal(_))
        ));
        assert_eq!(
            cursor.next(capacity(1, 2, 1), &runtime).unwrap_err(),
            KernelFailure::InstanceFailed
        );
        assert_eq!(cursor.finish(&runtime), Err(KernelFailure::InstanceFailed));
        assert_eq!(owner.counts.next.load(Ordering::Relaxed), 1);
        drop(cursor);
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn early_finish_rejects_without_private_finish_and_exact_owner_survives_typed_drop() {
    let owner = fixture();
    let kernel = owner.prepared();
    let contract = kernel.contract().clone();
    let foreign = (*contract).clone();
    let weak = Arc::downgrade(&kernel);
    let runtime = RuntimeControl::normal();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let args = [EvaluatedArgument::Column(&array)];
    assert!(matches!(
        TableEvaluationCursor::begin(
            kernel.clone(),
            input(&foreign, Selection::all(1), &args),
            &runtime
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
    let mut cursor = TableEvaluationCursor::begin(
        kernel.clone(),
        input(&contract, Selection::all(1), &args),
        &runtime,
    )
    .unwrap();
    drop(kernel);
    assert!(weak.upgrade().is_some());
    assert!(matches!(
        cursor.finish(&runtime),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 0);
    assert_eq!(
        cursor.next(capacity(1, 1, 1), &runtime).unwrap_err(),
        KernelFailure::InstanceFailed
    );
    drop(cursor);
    assert!(weak.upgrade().is_none());
    assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.drop_live.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.kernel_drop.load(Ordering::Relaxed), 1);
}

#[test]
fn lifecycle_errors_and_partial_construction_cleanup_preserve_all_failure_categories() {
    let runtime = RuntimeControl::normal();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let args = [EvaluatedArgument::Column(&array)];
    for failure in failures() {
        let mut owner = fixture();
        Arc::get_mut(&mut owner.behavior).unwrap().begin_failure = Some(failure.clone());
        let kernel = owner.prepared();
        assert!(
            matches!(TableEvaluationCursor::begin(kernel.clone(),input(kernel.contract(),Selection::all(1),&args),&runtime),Err(actual) if actual==failure)
        );
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        for finish in [false, true] {
            let mut owner = fixture();
            let behavior = Arc::get_mut(&mut owner.behavior).unwrap();
            if finish {
                behavior.finish_failure = Some(failure.clone());
            } else {
                behavior.next_failure = Some(failure.clone());
            }
            let kernel = owner.prepared();
            let mut cursor = TableEvaluationCursor::begin(
                kernel.clone(),
                input(kernel.contract(), Selection::all(1), &args),
                &runtime,
            )
            .unwrap();
            if finish {
                assert!(output(cursor.next(capacity(1, 1, 1), &runtime).unwrap()).eof);
                assert_eq!(cursor.finish(&runtime), Err(failure.clone()));
                assert_eq!(owner.counts.finish.load(Ordering::Relaxed), 1);
            } else {
                assert_eq!(
                    cursor.next(capacity(1, 1, 1), &runtime).unwrap_err(),
                    failure
                );
            }
            assert_eq!(cursor.finish(&runtime), Err(KernelFailure::InstanceFailed));
            assert_eq!(
                cursor.next(capacity(1, 1, 1), &runtime).unwrap_err(),
                KernelFailure::InstanceFailed
            );
            drop(cursor);
            assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        }
    }
}

#[test]
fn retained_growth_and_bound_drift_cover_initialization_next_and_finish_error_exits() {
    let runtime = RuntimeControl::normal();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let args = [EvaluatedArgument::Column(&array)];
    for drift in [false, true] {
        let mut owner = fixture();
        let behavior = Arc::get_mut(&mut owner.behavior).unwrap();
        if drift {
            behavior.begin_drift = true
        } else {
            behavior.begin_growth = true
        };
        let kernel = owner.prepared();
        assert!(matches!(
            TableEvaluationCursor::begin(
                kernel.clone(),
                input(kernel.contract(), Selection::all(1), &args),
                &runtime
            ),
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
                    behavior.next_failure = failure.clone();
                    behavior.next_growth = !drift;
                    behavior.next_drift = drift
                };
                let kernel = owner.prepared();
                let mut cursor = TableEvaluationCursor::begin(
                    kernel.clone(),
                    input(kernel.contract(), Selection::all(1), &args),
                    &runtime,
                )
                .unwrap();
                let result = if finish {
                    assert!(output(cursor.next(capacity(1, 1, 1), &runtime).unwrap()).eof);
                    cursor.finish(&runtime)
                } else {
                    cursor.next(capacity(1, 1, 1), &runtime).map(|_| ())
                };
                if let Some(error) = failure {
                    // Accepted first-cause migration: actual source identity,
                    // including an ordinary outer refusal, wins over late drift.
                    assert_eq!(result, Err(error));
                } else {
                    assert!(matches!(result, Err(KernelFailure::Internal(_))));
                }
                assert_eq!(cursor.finish(&runtime), Err(KernelFailure::InstanceFailed));
                assert_eq!(owner.counts.next.load(Ordering::Relaxed), 1);
                assert_eq!(
                    owner.counts.finish.load(Ordering::Relaxed),
                    usize::from(finish)
                );
                drop(cursor);
                assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
                assert_eq!(owner.counts.drop_live.load(Ordering::Relaxed), 1);
            }
        }
    }
}

struct AfterPositiveControl {
    failure: KernelFailure,
    armed: std::sync::atomic::AtomicBool,
}
impl KernelEvaluationControl for AfterPositiveControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        if work > 0 {
            self.armed.store(true, Ordering::Relaxed);
            Ok(())
        } else if self.armed.load(Ordering::Relaxed) {
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
fn entry_and_post_work_controls_cleanup_partial_cursor_and_latch_next_finish() {
    let runtime = RuntimeControl::normal();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let args = [EvaluatedArgument::Column(&array)];
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let owner = fixture();
        let kernel = owner.prepared();
        let selected = input(kernel.contract(), Selection::all(1), &args);
        let entry = RuntimeControl::fail(failure.clone(), false);
        assert!(
            matches!(TableEvaluationCursor::begin(kernel.clone(),selected,&entry),Err(actual) if actual==failure)
        );
        assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
        let post = AfterPositiveControl {
            failure: failure.clone(),
            armed: std::sync::atomic::AtomicBool::new(false),
        };
        assert!(
            matches!(TableEvaluationCursor::begin(kernel.clone(),selected,&post),Err(actual) if actual==failure)
        );
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
        for finish in [false, true] {
            for after in [false, true] {
                let mut cursor =
                    TableEvaluationCursor::begin(kernel.clone(), selected, &runtime).unwrap();
                if finish {
                    assert!(output(cursor.next(capacity(1, 1, 1), &runtime).unwrap()).eof);
                }
                let post = AfterPositiveControl {
                    failure: failure.clone(),
                    armed: std::sync::atomic::AtomicBool::new(false),
                };
                let control: &dyn KernelEvaluationControl = if after { &post } else { &entry };
                if finish {
                    assert_eq!(cursor.finish(control), Err(failure.clone()));
                } else {
                    assert_eq!(
                        cursor.next(capacity(1, 1, 1), control).unwrap_err(),
                        failure
                    );
                }
                assert_eq!(cursor.finish(&runtime), Err(KernelFailure::InstanceFailed));
                assert_eq!(
                    cursor.next(capacity(1, 1, 1), &runtime).unwrap_err(),
                    KernelFailure::InstanceFailed
                );
                drop(cursor);
            }
        }
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 5);
        assert_eq!(owner.counts.drop_live.load(Ordering::Relaxed), 5);
    }
}

struct QuantumControl {
    failure: KernelFailure,
    work: Mutex<Vec<u32>>,
}
impl KernelEvaluationControl for QuantumControl {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= 256);
        self.work.lock().unwrap().push(work);
        if work == 256 {
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
fn large_parent_metadata_stops_at_256_preserving_three_outer_control_categories() {
    let runtime = RuntimeControl::normal();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![0; 300]));
    let args = [EvaluatedArgument::Column(&array)];
    for failure in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
    ] {
        let mut owner = fixture();
        Arc::get_mut(&mut owner.behavior).unwrap().fault = Fault::LargeCompletions;
        let kernel = owner.prepared();
        let mut cursor = TableEvaluationCursor::begin(
            kernel.clone(),
            input(kernel.contract(), Selection::all(300), &args),
            &runtime,
        )
        .unwrap();
        // A dedicated page fixture isolates framework metadata validation from private parsing work.
        let control = QuantumControl {
            failure: failure.clone(),
            work: Mutex::new(vec![]),
        };
        assert_eq!(
            cursor.next(capacity(0, 300, 0), &control).unwrap_err(),
            failure
        );
        assert!(control.work.lock().unwrap().contains(&256));
        assert_eq!(owner.counts.next.load(Ordering::Relaxed), 1);
        assert_eq!(
            cursor.next(capacity(0, 300, 0), &runtime).unwrap_err(),
            KernelFailure::InstanceFailed
        );
        drop(cursor);
        assert_eq!(owner.counts.drop.load(Ordering::Relaxed), 1);
    }
}

#[test]
fn fresh_frozen_owner_refines_prepares_once_without_cursor_and_rejects_forged_identity() {
    let owner = fixture();
    let input = owner.input();
    let mut previous = None;
    for frozen in [false, true] {
        owner.counts.refine.store(0, Ordering::Relaxed);
        owner.counts.prepare.store(0, Ordering::Relaxed);
        let arguments = ScopedExpressionEffects::pure_value(input.context);
        let prepared = if frozen {
            specialize_frozen_table(
                &owner,
                input,
                owner.selected.clone(),
                &owner.frozen(),
                arguments,
                &CompileControl(None),
            )
        } else {
            specialize_table(
                &owner,
                input,
                owner.selected.clone(),
                arguments,
                &CompileControl(None),
            )
        }
        .unwrap();
        assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 1);
        assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
        assert!(std::ptr::eq(
            prepared.prepared().contract().call().selected(),
            owner.selected.as_ref()
        ));
        let effects = prepared.effects().for_use(input.context).unwrap();
        if let Some(previous) = previous {
            assert_eq!(previous, effects);
        }
        previous = Some(effects);
        assert!(effects.has_instance_state);
    }
    owner.counts.prepare.store(0, Ordering::Relaxed);
    let mut frozen = owner.frozen();
    frozen.observable_effects.warnings = true;
    assert!(matches!(
        specialize_frozen_table(
            &owner,
            input,
            owner.selected.clone(),
            &frozen,
            ScopedExpressionEffects::pure_value(input.context),
            &CompileControl(None)
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    let mut context = input.context;
    context.use_id = ExpressionUseId::new(99);
    assert!(matches!(
        specialize_table(
            &owner,
            input,
            owner.selected.clone(),
            ScopedExpressionEffects::pure_value(context),
            &CompileControl(None)
        ),
        Err(FunctionSpecializationFailure::Effects(_))
    ));
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
    let mut owner = fixture();
    Arc::get_mut(&mut owner.behavior).unwrap().foreign_contract = true;
    let input = owner.input();
    assert!(matches!(
        specialize_table(
            &owner,
            input,
            owner.selected.clone(),
            ScopedExpressionEffects::pure_value(input.context),
            &CompileControl(None)
        ),
        Err(FunctionSpecializationFailure::Kernel(
            KernelFailure::Internal(_)
        ))
    ));
    assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
}

#[test]
fn compile_entry_errors_remain_typed_before_owner_refinement_or_preparation() {
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let owner = fixture();
        let input = owner.input();
        for frozen in [false, true] {
            let arguments = ScopedExpressionEffects::pure_value(input.context);
            let result = if frozen {
                specialize_frozen_table(
                    &owner,
                    input,
                    owner.selected.clone(),
                    &owner.frozen(),
                    arguments,
                    &CompileControl(Some(failure.clone())),
                )
            } else {
                specialize_table(
                    &owner,
                    input,
                    owner.selected.clone(),
                    arguments,
                    &CompileControl(Some(failure.clone())),
                )
            };
            assert!(
                matches!(result,Err(FunctionSpecializationFailure::Control(actual)) if actual==failure)
            );
            assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 0);
            assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
            assert_eq!(owner.counts.begin.load(Ordering::Relaxed), 0);
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

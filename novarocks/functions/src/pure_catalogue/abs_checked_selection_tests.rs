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
use arrow_array::{Array, ArrayRef, Int32Array};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationRegionId, ExpressionEffectContext, ExpressionUseId, SemanticParameterId,
    SemanticParameterKey, SemanticParameterRef, SemanticParameters, ValueSchemaCreationScope,
    ValueTypeStorageHold, ValueTypeStorageLifetime,
};
use std::{alloc::Layout, sync::Mutex, time::Duration};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Control(u32),
    Exact(usize, usize),
    Opaque(usize),
}
#[derive(Default)]
struct Trace {
    events: Mutex<Vec<Event>>,
}
struct Control {
    trace: Arc<Trace>,
    stop: Option<(usize, CompileControlError)>,
}
impl Control {
    fn new() -> Self {
        Self {
            trace: Arc::new(Trace::default()),
            stop: None,
        }
    }
    fn events(&self) -> Vec<Event> {
        self.trace.events.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut events = self.trace.events.lock().unwrap();
        let index = events.len();
        if let Some((stop, _)) = self.stop {
            assert!(index <= stop, "callback after first control refusal");
        }
        events.push(Event::Control(units));
        match self.stop.clone() {
            Some((stop, cause)) if stop == index => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Debug, Eq, PartialEq)]
struct PrimaryH {
    token: Arc<()>,
}
struct Life(Arc<std::sync::atomic::AtomicUsize>);
impl ValueTypeStorageHold for Life {}
impl Drop for Life {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
struct Scope {
    trace: Arc<Trace>,
    stop: Option<usize>,
    h: Arc<()>,
    life: Arc<Life>,
    operation_bytes: usize,
}
impl Scope {
    fn new(control: &Control) -> Self {
        Self {
            trace: control.trace.clone(),
            stop: None,
            h: Arc::new(()),
            life: Arc::new(Life(Arc::new(std::sync::atomic::AtomicUsize::new(0)))),
            operation_bytes: 0,
        }
    }
    fn observe(&mut self, event: Event, bytes: usize) -> Result<(), PrimaryH> {
        let mut events = self.trace.events.lock().unwrap();
        let index = events.len();
        if let Some(stop) = self.stop {
            assert!(index <= stop, "request after primary H");
        }
        events.push(event);
        // This bounded test observation port records the original operation
        // requests; it is not a physical allocation receipt or a new grant.
        let next = self
            .operation_bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= 65536);
        match next {
            Some(bytes) if self.stop != Some(index) => {
                self.operation_bytes = bytes;
                Ok(())
            }
            _ => Err(PrimaryH {
                token: self.h.clone(),
            }),
        }
    }
}
impl ValueSchemaCreationScope<PrimaryH> for Scope {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self.life.clone())
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), PrimaryH> {
        self.observe(Event::Exact(layout.size(), layout.align()), layout.size())
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), PrimaryH> {
        self.observe(Event::Opaque(bytes), bytes)
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 32,
        max_array_nodes: 512,
        max_logical_elements: 4096,
        max_retained_buffer_bytes: 8 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 8,
        max_metadata_bytes: 4 * 1024 * 1024,
        max_library_validation_work: 128 * 1024 * 1024,
        max_library_validation_bytes: 64 * 1024 * 1024,
    }
}
struct Fixture {
    catalog: EngineFunctionCatalog,
    function: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    uses: [Option<ExpressionUseId>; 1],
    parameters: SemanticParameters,
}
impl Fixture {
    fn new() -> Self {
        let control = Control::new();
        let catalog = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let policy = policy();
        let ty = FunctionValueType::new(DataType::Int32, false);
        let pool = ConstantPool::try_new(
            Arc::new(Field::new("actual-source", DataType::Int32, false)),
            ty.clone(),
            Int32Array::from(vec![77, -9]).to_data(),
            policy,
            CompilePhase::Validate,
            &control,
        )
        .unwrap();
        let constant = pool.value(1).unwrap();
        assert_eq!(constant.ordinal(), 1);
        let arguments = vec![FunctionArgument::Value {
            value_type: ty,
            constant: Some(constant),
        }];
        let bound = catalog
            .resolve_bound_user(
                "abs",
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &control,
            )
            .unwrap();
        Self {
            catalog,
            function: bound.function_id,
            selected: Arc::new(bound.selected),
            arguments,
            uses: [Some(ExpressionUseId::new(42))],
            parameters: SemanticParameters::try_new([]).unwrap(),
        }
    }
    fn context(&self) -> ExpressionEffectContext {
        ExpressionEffectContext {
            use_id: ExpressionUseId::new(41),
            region: EvaluationRegionId::new(7),
            demand: EvaluationDemand::Value,
        }
    }
    fn input(&self) -> CallEffectInput<'_> {
        CallEffectInput {
            context: (self.context()).into(),
            argument_uses: crate::CallArgumentUses::SelectedChannels(&self.uses),
            function_id: &self.function,
            kind: FunctionKind::Scalar,
            selected: &self.selected,
            request: FunctionBindingRequest {
                arguments: &self.arguments,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            environment: &[],
            parameters: &self.parameters,
            decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Region(self.context().region),
        }
    }
    fn options(&self) -> PureCallPreparation {
        PureCallPreparation::Scalar {
            arguments: ScopedExpressionEffects::pure_value(self.context()),
        }
    }
    fn checked(
        &self,
        control: &Control,
        scope: &mut Scope,
        frozen: Option<&CallEffects>,
    ) -> Result<PureCallSpecialization, CheckedCallPreparationFailure<PrimaryH>> {
        self.catalog.prepare_selected_checked_with_creator_in(
            self.input(),
            self.selected.clone(),
            frozen,
            self.options(),
            scope,
            control,
        )
    }
}
struct EvaluationControl;
impl KernelEvaluationControl for EvaluationControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("ABS does not wait")
    }
}
fn assert_original_abs_values(prepared: &PureCallSpecialization) {
    assert_eq!(
        prepared.implementation().implementation.as_str(),
        "builtin.scalar/abs/selected-v1"
    );
    let PreparedPureKernel::Scalar(kernel) = prepared.prepared() else {
        panic!("scalar ABI")
    };
    assert_eq!(
        kernel.contract().result_type(),
        &FunctionValueType::new(DataType::Int32, false)
    );
    let mut instance = crate::ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap();
    let array: ArrayRef = Arc::new(Int32Array::from(vec![i32::MIN, 5, -7]));
    let values = [crate::EvaluatedArgument::Column(&array)];
    let rows = [0, 2];
    let output = instance
        .evaluate(
            crate::Selection::try_sparse(3, &rows).unwrap(),
            &values,
            &EvaluationControl,
        )
        .unwrap();
    assert_eq!(output.errors().len(), 1);
    let error = &output.errors()[0];
    assert_eq!(error.selected_ordinal(), 0);
    assert_eq!(error.reason(), crate::DataFailureReason::NumericOutOfRange);
    let output = output
        .values()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap();
    assert!(output.is_null(0));
    assert_eq!(output.value(1), 7);
}
#[test]
fn abs_checked_registered_adapter_preserves_actual_fresh_frozen_values_and_exact_contract() {
    let fixture = Fixture::new();
    let pure = fixture
        .catalog
        .prepare_fresh_selected(
            fixture.input(),
            fixture.selected.clone(),
            fixture.options(),
            &Control::new(),
        )
        .unwrap();
    let control = Control::new();
    let mut scope = Scope::new(&control);
    let created = fixture.checked(&control, &mut scope, None).unwrap();
    assert_eq!(created.source(), PurePreparationSource::Fresh);
    assert_eq!(created.call_contract(), pure.call_contract());
    assert_eq!(created.effects(), pure.effects());
    assert!(Arc::ptr_eq(
        created.call_contract().selected_owner(),
        &fixture.selected
    ));
    assert_original_abs_values(&created);
    let frozen = created.call_contract().effects().clone();
    let sealed_control = Control::new();
    let mut sealed_scope = Scope::new(&sealed_control);
    let sealed = fixture
        .checked(&sealed_control, &mut sealed_scope, Some(&frozen))
        .unwrap();
    assert_eq!(sealed.source(), PurePreparationSource::Frozen);
    assert_original_abs_values(&sealed);
    let mut inaccurate = frozen;
    inaccurate.proof_scope = CallProofScope::Region(EvaluationRegionId::new(8));
    assert!(matches!(
        fixture.catalog.prepare_frozen_selected(
            fixture.input(),
            fixture.selected.clone(),
            &inaccurate,
            fixture.options(),
            &control,
        ),
        Err(FunctionSpecializationFailure::InvalidInput(
            "frozen call effects differ from exact local refinement"
        ))
    ));
    assert!(matches!(
        fixture.checked(&control, &mut scope, Some(&inaccurate)),
        Err(CheckedCallPreparationFailure::Preparation(
            crate::CheckedOwnerFailure::Original(FunctionSpecializationFailure::InvalidInput(
                "frozen call effects differ from exact local refinement"
            ))
        ))
    ));
}
#[test]
fn abs_checked_loan_keeps_exact_original_input_catalogue_registered_owner_and_policy() {
    let fixture = Fixture::new();
    let control = Control::new();
    let mut scope = Scope::new(&control);
    let checked = fixture
        .catalog
        .check_call_effect_input_with_creator_in(fixture.input(), &mut scope, &control)
        .unwrap();
    checked.require_input(fixture.input()).unwrap();
    let other = Fixture::new();
    assert!(checked.require_catalog(&other.catalog).is_err());
    let definition = fixture
        .catalog
        .definition("abs", FunctionKind::Scalar)
        .unwrap();
    let declaration = definition.binding_declaration().unwrap();
    checked
        .require_registered(declaration, fixture.input())
        .unwrap();
    assert!(
        checked
            .require_registered(&declaration.clone(), fixture.input())
            .is_err()
    );
    let other_declaration = other
        .catalog
        .definition("abs", FunctionKind::Scalar)
        .unwrap()
        .binding_declaration()
        .unwrap();
    assert!(
        checked
            .require_registered(other_declaration, fixture.input())
            .is_err()
    );
    let foreign_attachment = other
        .catalog
        .definition("abs", FunctionKind::Scalar)
        .unwrap()
        .binding
        .as_ref()
        .unwrap()
        .pure
        .as_ref()
        .unwrap();
    let foreign_error = foreign_attachment
        .owner
        .prepare_checked(
            &checked,
            fixture.input(),
            fixture.selected.clone(),
            None,
            fixture.options(),
            &control,
        )
        .unwrap_err();
    assert!(matches!(
        foreign_error,
        crate::CheckedOwnerFailure::Original(FunctionSpecializationFailure::Binding(
            FunctionBindingError::InvalidBinding(_)
        ))
    ));
    let copied_arguments = fixture.arguments.clone();
    let copied_selected = (*fixture.selected).clone();
    let copied_function = fixture.function.clone();
    let copied_parameters = fixture.parameters.clone();
    let copied_uses = fixture.uses;
    let expected = FunctionValueType::new(DataType::Int64, false);
    let environment = [SemanticParameterRef {
        id: SemanticParameterId::new(9),
        expected_key: SemanticParameterKey::TimeZone,
    }];
    for fault in 0..13 {
        let mut input = fixture.input();
        match fault {
            0 => {
                input.context = novarocks_type_contract::ExpressionEffectContext {
                    use_id: ExpressionUseId::new(43),
                    ..input.context.expression().unwrap()
                }
                .into()
            }
            1 => {
                input.context = novarocks_type_contract::ExpressionEffectContext {
                    region: EvaluationRegionId::new(8),
                    ..input.context.expression().unwrap()
                }
                .into()
            }
            2 => input.argument_uses = crate::CallArgumentUses::SelectedChannels(&copied_uses),
            3 => input.function_id = &copied_function,
            4 => input.kind = FunctionKind::Window,
            5 => input.selected = &copied_selected,
            6 => input.request.arguments = &copied_arguments,
            7 => input.request.logical_argument_count = 0,
            8 => input.request.expected_result_type = Some(&expected),
            9 => input.environment = &environment,
            10 => input.parameters = &copied_parameters,
            11 => input.decimal_overflow_policy = DecimalOverflowPolicy::OutputNull,
            12 => input.proof_scope = CallProofScope::Unconditional,
            _ => unreachable!(),
        }
        assert!(checked.require_input(input).is_err(), "fault {fault}");
    }
    // The loan does not certify refinement: legitimate original ABS proof/env
    // guards still execute after successful full signature admission.
    let mut invalid = fixture.input();
    invalid.proof_scope = CallProofScope::Region(EvaluationRegionId::new(8));
    assert!(
        fixture
            .catalog
            .prepare_selected_checked_with_creator_in(
                invalid,
                fixture.selected.clone(),
                None,
                fixture.options(),
                &mut scope,
                &control
            )
            .is_err()
    );
}
#[test]
fn abs_checked_mint_only_after_original_footer_and_primary_h_has_no_callback_tail() {
    let fixture = Fixture::new();
    let good = Control::new();
    let mut scope = Scope::new(&good);
    fixture
        .catalog
        .check_call_effect_input_with_creator_in(fixture.input(), &mut scope, &good)
        .unwrap();
    let trace = good.events();
    assert!(
        matches!(trace.last(), Some(Event::Control(_))),
        "completed validation footer before mint"
    );
    let full_control = Control::new();
    let mut full_scope = Scope::new(&full_control);
    fixture
        .checked(&full_control, &mut full_scope, None)
        .unwrap();
    let full_trace = full_control.events();
    let construction = |events: &[Event]| {
        events
            .iter()
            .filter(|event| !matches!(event, Event::Control(_)))
            .cloned()
            .collect::<Vec<_>>()
    };
    assert_eq!(
        construction(&full_trace),
        construction(&trace),
        "registered effect/preparation reuses ONE completed original selected author"
    );
    let request_indices: Vec<_> = full_trace
        .iter()
        .enumerate()
        .filter_map(|(i, event)| (!matches!(event, Event::Control(_))).then_some(i))
        .collect();
    assert!(
        !request_indices.is_empty(),
        "actual selected type constructor requested resources"
    );
    for stop in request_indices {
        let control = Control::new();
        let mut scope = Scope::new(&control);
        scope.stop = Some(stop);
        let token = scope.h.clone();
        let error = fixture.checked(&control, &mut scope, None).unwrap_err();
        let CheckedCallPreparationFailure::Binding(crate::FunctionBindingCreationFailure::Creator(
            cause,
        )) = error
        else {
            panic!("primary H preserved")
        };
        assert!(Arc::ptr_eq(&token, &cause.token));
        assert_eq!(control.events(), full_trace[..=stop]);
    }
    let footer = trace.len() - 1;
    let control = Control {
        trace: Arc::new(Trace::default()),
        stop: Some((footer, CompileControlError::Cancelled)),
    };
    let mut scope = Scope::new(&control);
    assert!(matches!(
        fixture.catalog.check_call_effect_input_with_creator_in(
            fixture.input(),
            &mut scope,
            &control
        ),
        Err(crate::FunctionBindingCreationFailure::Binding(
            FunctionBindingError::Control(CompileControlError::Cancelled)
        ))
    ));
    assert_eq!(control.events(), trace);
}
fn control_cause(error: CheckedCallPreparationFailure<PrimaryH>) -> Option<CompileControlError> {
    match error {
        CheckedCallPreparationFailure::Binding(crate::FunctionBindingCreationFailure::Binding(
            FunctionBindingError::Control(error),
        )) => Some(error),
        CheckedCallPreparationFailure::Preparation(crate::CheckedOwnerFailure::Original(
            FunctionSpecializationFailure::Control(error),
        )) => Some(error),
        CheckedCallPreparationFailure::Preparation(crate::CheckedOwnerFailure::Original(
            FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled),
        )) => Some(CompileControlError::Cancelled),
        CheckedCallPreparationFailure::Preparation(crate::CheckedOwnerFailure::Original(
            FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded),
        )) => Some(CompileControlError::DeadlineExceeded),
        CheckedCallPreparationFailure::Preparation(crate::CheckedOwnerFailure::Original(
            FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted),
        )) => Some(CompileControlError::ResourceExhausted),
        _ => None,
    }
}
#[test]
fn abs_checked_registered_every_original_control_failure_has_exact_primary_prefix() {
    let fixture = Fixture::new();
    let good = Control::new();
    let mut scope = Scope::new(&good);
    fixture.checked(&good, &mut scope, None).unwrap();
    let trace = good.events();
    for (stop, event) in trace.iter().enumerate() {
        if !matches!(event, Event::Control(_)) {
            continue;
        }
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Arc::new(Trace::default()),
                stop: Some((stop, cause.clone())),
            };
            let mut scope = Scope::new(&control);
            assert_eq!(
                control_cause(fixture.checked(&control, &mut scope, None).unwrap_err()),
                Some(cause)
            );
            assert_eq!(control.events(), trace[..=stop]);
        }
    }
}
#[test]
fn abs_checked_receipt_does_not_keep_validation_scope_or_catalogue_in_prepared_kernel() {
    let fixture = Fixture::new();
    let control = Control::new();
    let mut scope = Scope::new(&control);
    let exits = scope.life.0.clone();
    let value = fixture.checked(&control, &mut scope, None).unwrap();
    assert!(
        control
            .events()
            .iter()
            .any(|event| !matches!(event, Event::Control(_)))
    );
    drop(scope);
    assert_eq!(exits.load(std::sync::atomic::Ordering::SeqCst), 1);
    drop(fixture);
    drop(control);
    assert_original_abs_values(&value);
    drop(value);
    assert_eq!(exits.load(std::sync::atomic::Ordering::SeqCst), 1);
}
// Preserve an explicitly unconverted installed boundary even when all builtin
// scalar owners join the checked route. Its original ABS CPU stays real.
struct UnconvertedInstalledAbs {
    original: Arc<dyn InstalledPureOwner>,
    pure_prepares: Arc<std::sync::atomic::AtomicUsize>,
}
impl InstalledPureOwner for UnconvertedInstalledAbs {
    fn accepts_installed_abi(&self, abi: PureKernelAbi) -> bool {
        self.original.accepts_installed_abi(abi)
    }
    fn prepare(
        &self,
        input: CallEffectInput<'_>,
        selected: Arc<FunctionBindingSelection>,
        frozen: Option<&CallEffects>,
        options: PureCallPreparation,
        control: &dyn PureCompileControl,
    ) -> Result<PreparedPureCallDraft, FunctionSpecializationFailure> {
        self.pure_prepares
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.original
            .prepare(input, selected, frozen, options, control)
    }
    // The original explicit UnconvertedOwner checked default is deliberate.
}
#[test]
fn abs_pilot_unconverted_installed_scalar_refuses_checked_and_original_pure_remains_available() {
    let mut fixture = Fixture::new();
    let pure_prepares = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let index = fixture.catalog.identities[&fixture.function];
    let attachment = fixture.catalog.definitions[index]
        .binding
        .as_mut()
        .unwrap()
        .pure
        .as_mut()
        .unwrap();
    attachment.owner = Arc::new(UnconvertedInstalledAbs {
        original: attachment.owner.clone(),
        pure_prepares: pure_prepares.clone(),
    });
    let pure = fixture
        .catalog
        .prepare_fresh_selected(
            fixture.input(),
            fixture.selected.clone(),
            fixture.options(),
            &Control::new(),
        )
        .unwrap();
    assert_eq!(pure.implementation().abi, PureKernelAbi::ScalarV1);
    assert_eq!(pure.source(), PurePreparationSource::Fresh);
    assert!(Arc::ptr_eq(
        pure.call_contract().selected_owner(),
        &fixture.selected
    ));
    assert_original_abs_values(&pure);
    assert_eq!(pure_prepares.load(std::sync::atomic::Ordering::SeqCst), 1);
    let control = Control::new();
    let mut scope = Scope::new(&control);
    assert!(matches!(
        fixture.checked(&control, &mut scope, None),
        Err(CheckedCallPreparationFailure::Preparation(
            crate::CheckedOwnerFailure::UnconvertedOwner
        ))
    ));
    assert!(
        control
            .events()
            .iter()
            .any(|event| !matches!(event, Event::Control(_))),
        "actual ABS signature creator precedes installed staging refusal"
    );
    assert_eq!(
        pure_prepares.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "checked staging refusal must not retry the original pure preparation"
    );
    let after = fixture
        .catalog
        .prepare_fresh_selected(
            fixture.input(),
            fixture.selected.clone(),
            fixture.options(),
            &Control::new(),
        )
        .unwrap();
    assert_eq!(after.implementation().abi, PureKernelAbi::ScalarV1);
    assert_eq!(after.call_contract(), pure.call_contract());
    assert_original_abs_values(&after);
    assert_eq!(pure_prepares.load(std::sync::atomic::Ordering::SeqCst), 2);
}

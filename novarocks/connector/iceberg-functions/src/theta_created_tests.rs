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

//! Original Theta resolver construction through the staged actual creator port.
use super::*;
use novarocks_functions::{
    EngineFunctionCatalog, FunctionArgument, FunctionArgumentType, FunctionBindingCreationFailure,
    FunctionBindingError, FunctionBindingRequest, FunctionKind, FunctionOverloadId,
    FunctionResultType,
};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, PureCompileControl, ValueSchemaCreationScope,
    ValueTypeStorageHold, ValueTypeStorageLifetime,
};
use std::{
    alloc::Layout,
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct Control {
    calls: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let mut calls = self.calls.lock().unwrap();
        calls.push((phase, units));
        match self.stop.clone() {
            Some((at, cause)) if calls.len() == at => Err(cause),
            _ => Ok(()),
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
struct Cause;
#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Exact(usize, usize),
    Opaque(usize),
}
#[derive(Default)]
struct Facts {
    events: Mutex<Vec<Event>>,
    held: AtomicUsize,
    exits: AtomicUsize,
}
struct Hold(Arc<Facts>);
impl ValueTypeStorageHold for Hold {}
impl Drop for Hold {
    fn drop(&mut self) {
        self.0.held.store(0, Ordering::SeqCst);
        self.0.exits.fetch_add(1, Ordering::SeqCst);
    }
}
struct Scope {
    hold: Arc<Hold>,
    refuse: Option<usize>,
    cause: Arc<Cause>,
}
impl Scope {
    fn new(refuse: Option<usize>) -> (Arc<Facts>, Self) {
        let facts = Arc::new(Facts::default());
        (
            facts.clone(),
            Self {
                hold: Arc::new(Hold(facts)),
                refuse,
                cause: Arc::new(Cause),
            },
        )
    }
    fn admit(&mut self, event: Event, bytes: usize) -> Result<(), Arc<Cause>> {
        let mut events = self.hold.0.events.lock().unwrap();
        events.push(event);
        if self.refuse == Some(events.len()) {
            return Err(self.cause.clone());
        }
        let prior = self.hold.0.held.load(Ordering::SeqCst);
        let next = prior
            .checked_add(bytes)
            .filter(|n| *n <= 65536)
            .ok_or_else(|| self.cause.clone())?;
        self.hold.0.held.store(next, Ordering::SeqCst);
        Ok(())
    }
}
impl ValueSchemaCreationScope<Arc<Cause>> for Scope {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self.hold.clone())
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), Arc<Cause>> {
        self.admit(Event::Exact(layout.size(), layout.align()), layout.size())
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), Arc<Cause>> {
        self.admit(Event::Opaque(bytes), bytes)
    }
}

fn catalog() -> EngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    crate::IcebergFunctionBundle
        .contribute(&mut builder)
        .unwrap();
    builder.seal().unwrap()
}
fn argument(value_type: FunctionValueType) -> [FunctionArgument; 1] {
    [FunctionArgument::Value {
        value_type,
        constant: None,
    }]
}
fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    }
}
fn validate(
    catalog: &EngineFunctionCatalog,
    bound: &novarocks_functions::ResolvedFunctionBinding,
    arguments: &[FunctionArgument],
    scope: &mut Scope,
    control: &dyn PureCompileControl,
) -> Result<(), FunctionBindingCreationFailure<Arc<Cause>>> {
    catalog.validate_frozen_selection_with_creator_in(
        &bound.function_id,
        bound.kind,
        &bound.selected,
        request(arguments),
        scope,
        control,
    )
}
#[test]
fn theta_created_validation_keeps_original_loan_full_domains_state_law_and_temporary_exit() {
    let family = IcebergThetaAggregateFamily::try_new().unwrap();
    let catalog = catalog();
    for nullable in [false, true] {
        for (carrier, domain) in [
            (DataType::Int32, ValueLogicalType::Physical),
            (DataType::FixedSizeBinary(16), ValueLogicalType::Uuid),
            (DataType::FixedSizeBinary(16), ValueLogicalType::LargeInt),
            (DataType::Binary, ValueLogicalType::Hll),
            (DataType::Binary, ValueLogicalType::Bitmap),
            (DataType::LargeBinary, ValueLogicalType::Variant),
        ] {
            let values =
                [FunctionValueType::try_with_logical_type(carrier, nullable, domain).unwrap()];
            let args = argument(values[0].clone());
            let control = Control::default();
            let bound = catalog
                .resolve_bound_trusted(
                    ICEBERG_THETA_AGGREGATE_NAME,
                    FunctionKind::Aggregate,
                    request(&args),
                    &control,
                )
                .unwrap();
            let loan = family
                .borrow_update_signature(bound.selected.overload.as_str(), &values)
                .unwrap()
                .unwrap();
            let original = family
                .overloads()
                .iter()
                .find(|decl| decl.identity.as_str() == bound.selected.overload.as_str())
                .unwrap();
            assert!(std::ptr::eq(loan.original, original));
            assert!(std::ptr::eq(&loan.argument_types[0], values[0].data_type()));
            let signature = family
                .resolve_update_signature(&original.identity, loan.argument_types)
                .unwrap();
            assert_eq!(signature.state_format, original.state_format);
            assert_eq!(signature.overload, original.identity);
            let FunctionArgumentType::Value(actual) = &bound.selected.argument_types[0] else {
                panic!("value");
            };
            assert_eq!(actual, &values[0]);
            let FunctionResultType::Scalar(result) = &bound.selected.result_type else {
                panic!("scalar");
            };
            assert_eq!(result.data_type(), &DataType::Binary);
            assert!(!result.nullable());
            let state = bound.selected.aggregate.as_ref().unwrap();
            assert_eq!(state.state_format, original.state_format);
            assert_eq!(state.state_argument_contract,
                novarocks_functions::AggregateStateArgumentContract::ValueRootNullabilityIndependent);
            catalog
                .validate_frozen_selection(
                    &bound.function_id,
                    bound.kind,
                    &bound.selected,
                    request(&args),
                    &control,
                )
                .unwrap();
            for _ in 0..3 {
                let (facts, mut scope) = Scope::new(None);
                validate(&catalog, &bound, &args, &mut scope, &control).unwrap();
                let events = facts.events.lock().unwrap().clone();
                assert!(events.iter().any(|event| matches!(event,Event::Exact(bytes,_) if *bytes==original.identity.as_str().len())));
                assert!(events.iter().any(|event| matches!(event,Event::Exact(bytes,_) if *bytes==std::mem::size_of::<DataType>())));
                assert!(events.iter().any(|event| matches!(event,Event::Exact(bytes,_) if *bytes==novarocks_type_contract::FunctionArgumentTypes::storage_allocation_layout().unwrap().size())));
                assert!(facts.held.load(Ordering::SeqCst) > 0);
                drop(scope);
                assert_eq!(facts.held.load(Ordering::SeqCst), 0);
                assert_eq!(facts.exits.load(Ordering::SeqCst), 1);
                assert_eq!(actual.logical_type(), domain);
            }
        }
    }
}
#[test]
fn theta_created_validation_preserves_every_first_h_and_control_prefix() {
    let catalog = catalog();
    let args = argument(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    );
    let bound = catalog
        .resolve_bound_trusted(
            ICEBERG_THETA_AGGREGATE_NAME,
            FunctionKind::Aggregate,
            request(&args),
            &Control::default(),
        )
        .unwrap();
    let (facts, mut scope) = Scope::new(None);
    let control = Control::default();
    validate(&catalog, &bound, &args, &mut scope, &control).unwrap();
    let events = facts.events.lock().unwrap().clone();
    let calls = control.calls.lock().unwrap().clone();
    drop(scope);
    assert!(events.len() > 2);
    for at in 1..=events.len() {
        let (facts, mut scope) = Scope::new(Some(at));
        let first = scope.cause.clone();
        let control = Control::default();
        match validate(&catalog, &bound, &args, &mut scope, &control) {
            Err(FunctionBindingCreationFailure::Creator(actual)) => {
                assert!(Arc::ptr_eq(&actual, &first))
            }
            other => panic!("original H lost: {other:?}"),
        }
        assert_eq!(*facts.events.lock().unwrap(), events[..at]);
        let observed = control.calls.lock().unwrap().clone();
        assert_eq!(observed, calls[..observed.len()]);
        assert!(observed.len() < calls.len());
        drop(scope);
        assert_eq!(facts.held.load(Ordering::SeqCst), 0);
    }
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at in 1..=calls.len() {
            let (facts, mut scope) = Scope::new(None);
            let control = Control {
                stop: Some((at, cause.clone())),
                ..Default::default()
            };
            assert!(
                matches!(validate(&catalog,&bound,&args,&mut scope,&control),
                Err(FunctionBindingCreationFailure::Binding(FunctionBindingError::Control(actual))) if actual==cause)
            );
            assert_eq!(*control.calls.lock().unwrap(), calls[..at]);
            let observed = facts.events.lock().unwrap().clone();
            assert_eq!(observed, events[..observed.len()]);
            drop(scope);
            assert_eq!(facts.held.load(Ordering::SeqCst), 0);
        }
    }
}
#[test]
fn theta_created_validation_rejects_exact_identity_arity_state_and_same_carrier_forgery() {
    let catalog = catalog();
    let args = argument(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    );
    let bound = catalog
        .resolve_bound_trusted(
            ICEBERG_THETA_AGGREGATE_NAME,
            FunctionKind::Aggregate,
            request(&args),
            &Control::default(),
        )
        .unwrap();
    for axis in 0..4 {
        let mut wrong = bound.clone();
        match axis {
            0 => wrong.selected.overload = FunctionOverloadId::try_new("foreign/theta/v1").unwrap(),
            1 => {
                wrong.selected.argument_types =
                    novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(Box::new([]))
            }
            2 => {
                wrong.selected.aggregate.as_mut().unwrap().state_format =
                    AggregateStateFormatIdentity::try_new("foreign/theta/state-v1").unwrap()
            }
            3 => {
                wrong.selected.argument_types =
                    novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
                        vec![FunctionArgumentType::Value(FunctionValueType::new(
                            DataType::FixedSizeBinary(16),
                            false,
                        ))]
                        .into_boxed_slice(),
                    )
            }
            _ => unreachable!(),
        }
        let (facts, mut scope) = Scope::new(None);
        let result = validate(&catalog, &wrong, &args, &mut scope, &Control::default());
        if axis == 0 {
            assert!(matches!(
                result,
                Err(FunctionBindingCreationFailure::Binding(
                    FunctionBindingError::UnknownOverload(_)
                ))
            ));
        } else {
            assert!(matches!(
                result,
                Err(FunctionBindingCreationFailure::Binding(
                    FunctionBindingError::InvalidBinding(_)
                ))
            ));
        }
        if axis < 3 {
            assert!(facts.events.lock().unwrap().is_empty());
        }
        drop(scope);
        assert_eq!(facts.held.load(Ordering::SeqCst), 0);
    }
    let foreign_args = argument(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    );
    let mut selected = bound.clone();
    selected.selected.overload = FunctionOverloadId::try_new("iceberg/theta-stat/int/v1").unwrap();
    let (facts, mut scope) = Scope::new(None);
    assert!(matches!(
        validate(
            &catalog,
            &selected,
            &foreign_args,
            &mut scope,
            &Control::default()
        ),
        Err(FunctionBindingCreationFailure::Binding(
            FunctionBindingError::InvalidBinding(_)
        ))
    ));
    drop(scope);
    assert_eq!(facts.held.load(Ordering::SeqCst), 0);
}

// The same provider bundle authors both the metadata and installed pure owner.
// Freeze effects through that real owner; no test-only resolver is installed.
struct WriterFixture {
    catalog: EngineFunctionCatalog,
    function: novarocks_functions::FunctionId,
    selected: Arc<novarocks_functions::FunctionBindingSelection>,
    arguments: [FunctionArgument; 1],
    value_type: FunctionValueType,
    call: novarocks_type_contract::RelationEffectContext,
    input: novarocks_type_contract::RelationEffectContext,
    parameters: novarocks_type_contract::SemanticParameters,
    frozen: novarocks_type_contract::CallEffects,
}
impl WriterFixture {
    fn new(value_type: FunctionValueType) -> Self {
        use novarocks_functions::*;
        use novarocks_type_contract::{
            CallProofScope, DecimalOverflowPolicy, RelationCallId, RelationEffectContext,
            SemanticParameters,
        };
        let catalog = catalog();
        let arguments = argument(value_type.clone());
        let bound = catalog
            .resolve_bound_trusted(
                ICEBERG_THETA_AGGREGATE_NAME,
                FunctionKind::Aggregate,
                request(&arguments),
                &Control::default(),
            )
            .unwrap();
        let function = bound.function_id;
        let selected = Arc::new(bound.selected);
        let call = RelationEffectContext::call(RelationCallId::new(41));
        let input = RelationEffectContext::materialized_input(call.identity.call);
        let parameters = SemanticParameters::try_new([]).unwrap();
        let authored = catalog
            .prepare_fresh_selected(
                CallEffectInput {
                    context: call.into(),
                    argument_uses: CallArgumentUses::MaterializedInput {
                        context: input,
                        input_type: &value_type,
                    },
                    function_id: &function,
                    kind: FunctionKind::Aggregate,
                    selected: &selected,
                    request: request(&arguments),
                    environment: &[],
                    parameters: &parameters,
                    decimal_overflow_policy: DecimalOverflowPolicy::OutputNull,
                    proof_scope: CallProofScope::Relation(call.identity),
                },
                Arc::clone(&selected),
                PureCallPreparation::Aggregate {
                    arguments: ScopedRelationalEffects::primitive(
                        call,
                        ScopedRelationalEffects::pure_value(input)
                            .for_use(input)
                            .unwrap(),
                    ),
                    options: AggregatePreparationOptions {
                        state_interpretation: None,
                        phase: AggregateKernelPhase::Partial,
                        distinct: false,
                        order_keys: Arc::from([]),
                        state_input_type: None,
                    },
                },
                &Control::default(),
            )
            .unwrap();
        let frozen = authored.call_contract().effects().clone();
        Self {
            catalog,
            function,
            selected,
            arguments,
            value_type,
            call,
            input,
            parameters,
            frozen,
        }
    }
    fn loan(
        &self,
        phase: novarocks_functions::AggregateKernelPhase,
    ) -> novarocks_functions::FrozenWriterAggregatePreparation<'_> {
        novarocks_functions::FrozenWriterAggregatePreparation {
            function_id: &self.function,
            selected: &self.selected,
            original_request: request(&self.arguments),
            call_context: self.call,
            materialized_input_context: self.input,
            frozen_effects: &self.frozen,
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            parameters: &self.parameters,
            actual_input_type: if phase.consumes_logical_arguments() {
                &self.value_type
            } else {
                &self.selected.aggregate.as_ref().unwrap().intermediate_type
            },
            phase,
        }
    }
}
fn writer_constant_policy() -> novarocks_functions::ConstantPolicy {
    // There are no ConstantValues or pool sources in this genuine request.
    // Its original caller loans a zero-source policy, never a default grant.
    novarocks_functions::ConstantPolicy {
        max_rows: 0,
        max_array_nodes: 0,
        max_logical_elements: 0,
        max_retained_buffer_bytes: 0,
        max_type_depth: 0,
        max_type_nodes: 0,
        max_dictionary_depth: 0,
        max_metadata_bytes: 0,
        max_library_validation_work: 0,
        max_library_validation_bytes: 0,
    }
}
fn prepare_writer(
    source: &WriterFixture,
    phase: novarocks_functions::AggregateKernelPhase,
    scope: &mut Scope,
    control: &dyn PureCompileControl,
) -> Result<
    novarocks_functions::PureCallSpecialization,
    novarocks_functions::CheckedCallPreparationFailure<Arc<Cause>>,
> {
    novarocks_functions::prepare_frozen_writer_aggregate_with_creator_in(
        &source.catalog,
        source.loan(phase),
        &writer_constant_policy(),
        scope,
        control,
    )
}
fn writer_control_cause(
    failure: novarocks_functions::CheckedCallPreparationFailure<Arc<Cause>>,
) -> CompileControlError {
    use novarocks_functions::{
        CheckedCallPreparationFailure as P, CheckedOwnerFailure as O,
        FunctionSpecializationFailure as S, KernelFailure,
    };
    match failure {
        P::Binding(FunctionBindingCreationFailure::Binding(FunctionBindingError::Control(
            cause,
        )))
        | P::Preparation(O::Original(S::Control(cause))) => cause,
        P::Preparation(O::Original(S::Kernel(KernelFailure::Cancelled))) => {
            CompileControlError::Cancelled
        }
        P::Preparation(O::Original(S::Kernel(KernelFailure::DeadlineExceeded))) => {
            CompileControlError::DeadlineExceeded
        }
        P::Preparation(O::Original(S::Kernel(KernelFailure::ResourceExhausted))) => {
            CompileControlError::ResourceExhausted
        }
        other => panic!("original writer compile cause changed: {other:?}"),
    }
}

#[test]
fn theta_created_writer_uses_original_full_uuid_phase_null_and_namespace_contracts() {
    use novarocks_functions::{
        AggregateKernelPhase, CheckedCallPreparationFailure as P, CheckedOwnerFailure as O,
        FunctionSpecializationFailure as S, PreparedPureKernel,
    };
    let source = WriterFixture::new(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    );
    assert_eq!(
        source.selected.argument_types[0],
        FunctionArgumentType::Value(source.value_type.clone())
    );
    for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
        let (facts, mut scope) = Scope::new(None);
        let prepared = prepare_writer(&source, phase, &mut scope, &Control::default()).unwrap();
        let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
            panic!("actual Theta aggregate");
        };
        assert_eq!(handle.contract().phase(), phase);
        assert_eq!(
            handle.contract().call().context(),
            novarocks_type_contract::CallEffectContext::Relation(source.call)
        );
        assert_eq!(handle.contract().call().effects(), &source.frozen);
        assert_eq!(
            handle.contract().final_type().data_type(),
            &DataType::Binary
        );
        assert!(!handle.contract().final_type().nullable());
        assert!(facts.events.lock().unwrap().len() > 2);
        drop(scope);
        assert_eq!(facts.held.load(Ordering::SeqCst), 0);
        assert_eq!(facts.exits.load(Ordering::SeqCst), 1);
        // The validation expected objects are temporary. The prepared handle
        // borrows the independently frozen selected declaration after exit.
        assert_eq!(handle.contract().phase(), phase);
        assert_eq!(source.value_type.logical_type(), ValueLogicalType::Uuid);
    }
    let nullable_source = WriterFixture::new(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    );
    let mut good = nullable_source.loan(AggregateKernelPhase::Partial);
    good.actual_input_type = &source.value_type;
    let (facts, mut scope) = Scope::new(None);
    let prepared = novarocks_functions::prepare_frozen_writer_aggregate_with_creator_in(
        &nullable_source.catalog,
        good,
        &writer_constant_policy(),
        &mut scope,
        &Control::default(),
    )
    .unwrap();
    let PreparedPureKernel::Aggregate(handle) = prepared.prepared() else {
        panic!("actual Theta aggregate");
    };
    assert_eq!(handle.contract().phase(), AggregateKernelPhase::Partial);
    drop(scope);
    assert_eq!(facts.held.load(Ordering::SeqCst), 0);

    for actual in [
        FunctionValueType::new(DataType::FixedSizeBinary(16), false),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    ] {
        assert_eq!(actual.data_type(), source.value_type.data_type());
        let mut wrong = source.loan(AggregateKernelPhase::Partial);
        wrong.actual_input_type = &actual;
        let (facts, mut scope) = Scope::new(None);
        assert!(matches!(
            novarocks_functions::prepare_frozen_writer_aggregate_with_creator_in(
                &source.catalog,
                wrong,
                &writer_constant_policy(),
                &mut scope,
                &Control::default(),
            ),
            Err(P::Preparation(O::Original(S::InvalidInput(
                "writer materialized input differs from its complete logical channel"
            ))))
        ));
        assert!(facts.events.lock().unwrap().is_empty());
        drop(scope);
        assert_eq!(facts.held.load(Ordering::SeqCst), 0);
    }
    let (facts, mut scope) = Scope::new(None);
    let mut wrong = source.loan(AggregateKernelPhase::Partial);
    wrong.materialized_input_context = source.call;
    assert!(matches!(
        novarocks_functions::prepare_frozen_writer_aggregate_with_creator_in(
            &source.catalog,
            wrong,
            &writer_constant_policy(),
            &mut scope,
            &Control::default(),
        ),
        Err(P::Preparation(O::Original(S::InvalidInput(
            "writer preparation changes its original phase, scalar channel or Value scope"
        ))))
    ));
    assert!(facts.events.lock().unwrap().is_empty());
    drop(scope);
    assert_eq!(facts.held.load(Ordering::SeqCst), 0);
}

#[test]
fn theta_created_writer_preserves_every_actual_first_h_and_control_prefix_without_tail() {
    use novarocks_functions::{AggregateKernelPhase, CheckedCallPreparationFailure as P};
    let source = WriterFixture::new(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Uuid,
        )
        .unwrap(),
    );
    for phase in [AggregateKernelPhase::Partial, AggregateKernelPhase::Final] {
        let (facts, mut scope) = Scope::new(None);
        let control = Control::default();
        prepare_writer(&source, phase, &mut scope, &control).unwrap();
        let events = facts.events.lock().unwrap().clone();
        let calls = control.calls.lock().unwrap().clone();
        assert!(events.len() > 2);
        assert!(calls.len() > 1);
        drop(scope);
        assert_eq!(facts.held.load(Ordering::SeqCst), 0);
        for at in 1..=events.len() {
            let (facts, mut scope) = Scope::new(Some(at));
            let original_h = Arc::clone(&scope.cause);
            let control = Control::default();
            match prepare_writer(&source, phase, &mut scope, &control) {
                Err(P::Binding(FunctionBindingCreationFailure::Creator(actual))) => {
                    assert!(Arc::ptr_eq(&actual, &original_h));
                }
                other => panic!("actual Theta Writer creator cause changed: {other:?}"),
            }
            assert_eq!(*facts.events.lock().unwrap(), events[..at]);
            let prefix = control.calls.lock().unwrap().clone();
            assert_eq!(prefix, calls[..prefix.len()]);
            assert!(prefix.len() < calls.len(), "footer after originating H");
            drop(scope);
            assert_eq!(facts.held.load(Ordering::SeqCst), 0);
            assert_eq!(facts.exits.load(Ordering::SeqCst), 1);
        }
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 1..=calls.len() {
                let (facts, mut scope) = Scope::new(None);
                let control = Control {
                    stop: Some((at, cause.clone())),
                    ..Default::default()
                };
                let actual = writer_control_cause(
                    prepare_writer(&source, phase, &mut scope, &control).unwrap_err(),
                );
                assert_eq!(actual, cause);
                assert_eq!(*control.calls.lock().unwrap(), calls[..at]);
                let prefix = facts.events.lock().unwrap().clone();
                assert_eq!(prefix, events[..prefix.len()]);
                drop(scope);
                assert_eq!(facts.held.load(Ordering::SeqCst), 0);
                assert_eq!(facts.exits.load(Ordering::SeqCst), 1);
            }
        }
    }
}

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Public compiler/preparation owner checks using the existing frozen RAND fixture.
use super::*;
use arrow_array::Array;
use novarocks_functions::{
    CheckedCallPreparationFailure, CheckedOwnerFailure, FunctionBindingCreationFailure,
    FunctionBindingError, FunctionSpecializationFailure,
};
use novarocks_local_program::LocalProgram;
use novarocks_type_contract::{
    ValueSchemaCreation, ValueSchemaCreationFactory, ValueSchemaCreationScope,
    ValueSchemaScopeBeginFailure, ValueTypeStorageHold, ValueTypeStorageLifetime,
};
use std::{
    alloc::Layout,
    cell::RefCell,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[derive(Clone, Debug, Eq, PartialEq)]
enum Event {
    Control(CompilePhase, u32),
    Holder(usize, usize),
    Exact(usize, usize, usize),
    Opaque(usize, usize),
}
#[derive(Default)]
struct Observations {
    events: Mutex<Vec<Event>>,
    stopped: AtomicBool,
    control_stop: Mutex<Option<(usize, CompileControlError)>>,
    request_stop: Mutex<Option<usize>>,
    next_scope: AtomicUsize,
    held: AtomicUsize,
    scope_exits: AtomicUsize,
    factory_exits: AtomicUsize,
    scope_limit: AtomicUsize,
}
impl Observations {
    fn start(&self) {
        self.events.lock().unwrap().clear();
        self.stopped.store(false, Ordering::SeqCst);
    }
    fn events(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }
    fn quote(
        &self,
        event: Event,
        bytes: usize,
        held: &AtomicUsize,
        token: &Arc<()>,
    ) -> Result<(), FirstH> {
        assert!(
            !self.stopped.load(Ordering::SeqCst),
            "request after primary refusal"
        );
        let mut trace = self.events.lock().unwrap();
        let at = trace.len();
        trace.push(event);
        let next = held
            .load(Ordering::SeqCst)
            .checked_add(bytes)
            .filter(|bytes| *bytes <= self.scope_limit.load(Ordering::SeqCst));
        if *self.request_stop.lock().unwrap() == Some(at) || next.is_none() {
            self.stopped.store(true, Ordering::SeqCst);
            return Err(FirstH(token.clone()));
        }
        held.store(next.unwrap(), Ordering::SeqCst);
        self.held.fetch_add(bytes, Ordering::SeqCst);
        Ok(())
    }
}
impl PureCompileControl for Observations {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        assert!(
            !self.stopped.load(Ordering::SeqCst),
            "control after primary refusal"
        );
        let mut events = self.events.lock().unwrap();
        let at = events.len();
        events.push(Event::Control(phase, units));
        match self.control_stop.lock().unwrap().clone() {
            Some((stop, cause)) if stop == at => {
                self.stopped.store(true, Ordering::SeqCst);
                Err(cause)
            }
            _ => Ok(()),
        }
    }
}
#[derive(Debug)]
struct FirstH(Arc<()>);
struct Hold {
    observations: Arc<Observations>,
    bytes: AtomicUsize,
}
impl ValueTypeStorageHold for Hold {}
impl Drop for Hold {
    fn drop(&mut self) {
        self.observations
            .held
            .fetch_sub(self.bytes.load(Ordering::SeqCst), Ordering::SeqCst);
        self.observations.scope_exits.fetch_add(1, Ordering::SeqCst);
    }
}
struct Scope {
    id: usize,
    hold: Arc<Hold>,
    token: Arc<()>,
}
impl ValueSchemaCreationScope<FirstH> for Scope {
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(self.hold.clone())
    }
    fn request_layout(&mut self, layout: Layout) -> Result<(), FirstH> {
        self.hold.observations.quote(
            Event::Exact(self.id, layout.size(), layout.align()),
            layout.size(),
            &self.hold.bytes,
            &self.token,
        )
    }
    fn observe_opaque_upper_bound(&mut self, bytes: usize) -> Result<(), FirstH> {
        self.hold.observations.quote(
            Event::Opaque(self.id, bytes),
            bytes,
            &self.hold.bytes,
            &self.token,
        )
    }
}
struct Factory {
    observations: Arc<Observations>,
    token: Arc<()>,
}
impl Factory {
    fn new() -> Self {
        Self::with_scope_limit(65536)
    }
    fn with_scope_limit(scope_limit: usize) -> Self {
        Self {
            observations: Arc::new(Observations {
                scope_limit: AtomicUsize::new(scope_limit),
                ..Observations::default()
            }),
            token: Arc::new(()),
        }
    }
    fn make(&self) -> Result<Scope, FirstH> {
        let id = self.observations.next_scope.fetch_add(1, Ordering::SeqCst);
        let layout =
            novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Hold>())
                .unwrap();
        // The real test holder Arc is quoted before allocation; quotes are
        // conservative operations, not physical allocator receipts or grants.
        let bytes = AtomicUsize::new(0);
        self.observations.quote(
            Event::Holder(id, layout.size()),
            layout.size(),
            &bytes,
            &self.token,
        )?;
        Ok(Scope {
            id,
            hold: Arc::new(Hold {
                observations: self.observations.clone(),
                bytes,
            }),
            token: self.token.clone(),
        })
    }
}
impl Drop for Factory {
    fn drop(&mut self) {
        self.observations
            .factory_exits
            .fetch_add(1, Ordering::SeqCst);
    }
}
impl ValueSchemaCreationFactory<FirstH> for Factory {
    type Scope<'a> = Scope;
    fn begin_scope(&self) -> Result<Scope, FirstH> {
        self.make()
    }
    fn begin_scope_observed<O>(
        &self,
        observe: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Scope, ValueSchemaScopeBeginFailure<O, FirstH>> {
        let layout =
            novarocks_type_contract::owned_resources::layout::arc_layout(Layout::new::<Hold>())
                .unwrap();
        observe(layout).map_err(ValueSchemaScopeBeginFailure::Observer)?;
        self.make().map_err(ValueSchemaScopeBeginFailure::Creator)
    }
}
fn compile_created(
    source: Arc<FragmentPackage>,
    functions: &PureEngineFunctionCatalog,
    factory: &Factory,
) -> Result<LocalProgram, FragmentCreationError<FirstH>> {
    compile_created_validated(providers(source), functions, factory, options(1))
}

fn compile_created_validated(
    source: crate::ProviderValidatedFragment,
    functions: &PureEngineFunctionCatalog,
    factory: &Factory,
    compile_options: LocalCompileOptions,
) -> Result<LocalProgram, FragmentCreationError<FirstH>> {
    let main = RefCell::new(
        factory
            .begin_scope()
            .map_err(FragmentCreationError::Creator)?,
    );
    let mut temporary = factory
        .begin_scope()
        .map_err(FragmentCreationError::Creator)?;
    let storage = main.borrow().storage_lifetime();
    // These are short exclusive reborrows of the actual caller scope, not H
    // capture/recovery channels. No borrow is held across another callback.
    let mut request = |layout| main.borrow_mut().request_layout(layout);
    let mut opaque = |bytes| main.borrow_mut().observe_opaque_upper_bound(bytes);
    let mut creation = ValueSchemaCreation::new(storage, &mut request, &mut opaque);
    compile_fragment_with_creation(
        source,
        functions,
        compile_options,
        &mut creation,
        &mut temporary,
        factory,
        factory.observations.as_ref(),
    )
}
fn with_actual_call<R>(
    source: &FragmentPackage,
    program: &LocalProgram,
    body: impl FnOnce(
        CallEffectInput<'_>,
        Arc<novarocks_functions::FunctionBindingSelection>,
        &novarocks_type_contract::CallEffects,
    ) -> R,
) -> R {
    let project = program
        .graph()
        .nodes()
        .iter()
        .find(|node| node.physical_sources().iter().any(|id| id.get() == 41))
        .unwrap();
    let ProgramNodeKind::Project { exprs, .. } = project.kind() else {
        panic!("actual RAND project")
    };
    let reference = exprs[0];
    let compiled = program.unit(reference.unit).unwrap();
    let root = compiled.root(reference.root).unwrap();
    let call = &compiled.calls()[&root.use_id];
    let contract = call.call_contract();
    let unit = source.fragment().nodes()[&NodeId::new(41)]
        .expressions
        .as_ref()
        .unwrap();
    let frozen = &unit.claims()[&root.use_id];
    let invocation = &unit.control_flow().uses()[&frozen.context.use_id];
    let definition = unit.definition(invocation.definition).unwrap();
    let ExprKind::FunctionCall { function, args } = &definition.kind else {
        panic!("actual frozen RAND call")
    };
    assert!(args.is_empty());
    assert!(invocation.arguments.is_empty());
    let original = &unit.requests()[&invocation.definition];
    let mut work =
        CompileCheckpoints::try_new(&FixtureControl, CompilePhase::FunctionSpecialization).unwrap();
    let request = crate::original_requests::materialize_call_request_observed(
        original,
        source.constants(),
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(request.request().logical_argument_count, 0);
    assert_eq!(function.function_id, *contract.function_id());
    assert_eq!(
        novarocks_type_contract::CallEffectContext::from(frozen.context),
        contract.context()
    );
    let selected = contract.selected_owner().clone();
    let input = CallEffectInput {
        context: frozen.context.into(),
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(&[]),
        function_id: &function.function_id,
        kind: function.kind,
        selected: selected.as_ref(),
        request: request.request(),
        environment: &frozen.effects.environment,
        parameters: source.parameters(),
        decimal_overflow_policy: frozen.decimal_overflow_policy,
        proof_scope: frozen.effects.proof_scope,
    };
    body(input, selected.clone(), &frozen.effects)
}
fn preparation(
    functions: &PureEngineFunctionCatalog,
    input: CallEffectInput<'_>,
    selected: Arc<novarocks_functions::FunctionBindingSelection>,
    effects: &novarocks_type_contract::CallEffects,
    scope: &mut Scope,
    control: &dyn PureCompileControl,
) -> Result<novarocks_functions::PureCallSpecialization, CheckedCallPreparationFailure<FirstH>> {
    functions.prepare_frozen_with_creator_in(
        input,
        selected,
        effects,
        PureCallPreparation::Scalar {
            arguments: ScopedExpressionEffects::pure_value(
                input.context.expression().expect("actual scalar context"),
            ),
        },
        scope,
        control,
    )
}
fn assert_rand(prepared: &novarocks_functions::PureCallSpecialization) {
    assert_eq!(prepared.source(), PurePreparationSource::Frozen);
    assert_eq!(prepared.implementation().abi, PureKernelAbi::ScalarV1);
    assert_eq!(
        prepared.implementation().implementation.as_str(),
        "builtin.scalar/rand/selected-v1"
    );
    let novarocks_functions::PreparedPureKernel::Scalar(kernel) = prepared.prepared() else {
        panic!("actual RAND scalar")
    };
    assert_eq!(
        kernel.contract().result_type(),
        &FunctionValueType::new(DataType::Float64, false)
    );
    let mut instance = ScalarEvaluationInstance::instantiate(kernel.clone()).unwrap();
    let result = instance
        .evaluate(Selection::all(3), &[], &EvaluationControl)
        .unwrap();
    assert!(result.errors().is_empty());
    let values = result
        .values()
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(values.len(), 3);
    assert!(
        values
            .values()
            .iter()
            .all(|value| (0.0..1.0).contains(value))
    );
}
fn operation_quotes(events: &[Event]) -> Vec<(usize, usize)> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Exact(_, size, align) => Some((*size, *align)),
            Event::Opaque(_, bytes) => Some((*bytes, 0)),
            _ => None,
        })
        .collect()
}
#[test]
fn actual_public_frozen_preparation_uses_one_completed_signature_author_and_no_scope_escape() {
    let functions = rng_subset();
    let source = package(&functions, 2);
    let program = compile_fragment(
        providers(source.clone()),
        &functions,
        options(1),
        &FixtureControl,
    )
    .unwrap();
    let late = with_actual_call(&source, &program, |input, selected, effects| {
        let pure = functions
            .prepare_frozen(
                input,
                selected.clone(),
                effects,
                PureCallPreparation::Scalar {
                    arguments: ScopedExpressionEffects::pure_value(
                        input.context.expression().expect("actual scalar context"),
                    ),
                },
                &FixtureControl,
            )
            .unwrap();
        let factory = Factory::new();
        let observations = factory.observations.clone();
        let mut scope = factory.begin_scope().unwrap();
        observations.start();
        functions
            .metadata()
            .validate_frozen_selection_with_creator_in(
                input.function_id,
                input.kind,
                &selected,
                input.request,
                &mut scope,
                observations.as_ref(),
            )
            .unwrap();
        let signature = operation_quotes(&observations.events());
        assert!(!signature.is_empty());
        drop(scope);
        assert_eq!(observations.held.load(Ordering::SeqCst), 0);
        let mut scope = factory.begin_scope().unwrap();
        observations.start();
        let prepared = preparation(
            &functions,
            input,
            selected,
            effects,
            &mut scope,
            observations.as_ref(),
        )
        .unwrap();
        assert_eq!(
            operation_quotes(&observations.events()),
            signature,
            "effect and prepare replay do not construct a second selected type"
        );
        assert_eq!(prepared.call_contract(), pure.call_contract());
        assert_eq!(prepared.effects(), pure.effects());
        drop(scope);
        assert_eq!(observations.held.load(Ordering::SeqCst), 0);
        assert_eq!(observations.scope_exits.load(Ordering::SeqCst), 2);
        drop(factory);
        assert_eq!(observations.factory_exits.load(Ordering::SeqCst), 1);
        assert_rand(&prepared);
        prepared
    });
    drop(program);
    drop(source);
    drop(functions);
    assert_rand(&late);
}
#[test]
fn actual_public_frozen_preparation_primary_h_preserves_each_request_prefix() {
    let functions = rng_subset();
    let source = package(&functions, 1);
    let program = compile_fragment(
        providers(source.clone()),
        &functions,
        options(1),
        &FixtureControl,
    )
    .unwrap();
    with_actual_call(&source, &program, |input, selected, effects| {
        let good = Factory::new();
        let mut scope = good.begin_scope().unwrap();
        good.observations.start();
        preparation(
            &functions,
            input,
            selected.clone(),
            effects,
            &mut scope,
            good.observations.as_ref(),
        )
        .unwrap();
        let trace = good.observations.events();
        let requests: Vec<_> = trace
            .iter()
            .enumerate()
            .filter_map(|(at, event)| {
                matches!(event, Event::Exact(..) | Event::Opaque(..)).then_some(at)
            })
            .collect();
        assert!(!requests.is_empty());
        for at in requests {
            let factory = Factory::new();
            let mut scope = factory.begin_scope().unwrap();
            factory.observations.start();
            *factory.observations.request_stop.lock().unwrap() = Some(at);
            let result = preparation(
                &functions,
                input,
                selected.clone(),
                effects,
                &mut scope,
                factory.observations.as_ref(),
            );
            let Err(CheckedCallPreparationFailure::Binding(
                FunctionBindingCreationFailure::Creator(FirstH(token)),
            )) = result
            else {
                panic!("original first H, no control replacement")
            };
            assert!(Arc::ptr_eq(&token, &factory.token));
            assert_eq!(factory.observations.events(), trace[..=at]);
            drop(scope);
            assert_eq!(factory.observations.held.load(Ordering::SeqCst), 0);
        }
    });
}
fn checked_control(error: CheckedCallPreparationFailure<FirstH>) -> Option<CompileControlError> {
    match error {
        CheckedCallPreparationFailure::Binding(FunctionBindingCreationFailure::Binding(
            FunctionBindingError::Control(cause),
        )) => Some(cause),
        CheckedCallPreparationFailure::Preparation(CheckedOwnerFailure::Original(
            FunctionSpecializationFailure::Control(cause),
        )) => Some(cause),
        CheckedCallPreparationFailure::Preparation(CheckedOwnerFailure::Original(
            FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled),
        )) => Some(CompileControlError::Cancelled),
        CheckedCallPreparationFailure::Preparation(CheckedOwnerFailure::Original(
            FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded),
        )) => Some(CompileControlError::DeadlineExceeded),
        CheckedCallPreparationFailure::Preparation(CheckedOwnerFailure::Original(
            FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted),
        )) => Some(CompileControlError::ResourceExhausted),
        _ => None,
    }
}
#[test]
fn actual_public_frozen_preparation_control_prefixes_have_no_creator_or_footer_tail() {
    let functions = rng_subset();
    let source = package(&functions, 1);
    let program = compile_fragment(
        providers(source.clone()),
        &functions,
        options(1),
        &FixtureControl,
    )
    .unwrap();
    with_actual_call(&source, &program, |input, selected, effects| {
        let good = Factory::new();
        let mut scope = good.begin_scope().unwrap();
        good.observations.start();
        preparation(
            &functions,
            input,
            selected.clone(),
            effects,
            &mut scope,
            good.observations.as_ref(),
        )
        .unwrap();
        let trace = good.observations.events();
        for (at, event) in trace.iter().enumerate() {
            if !matches!(event, Event::Control(..)) {
                continue;
            }
            for cause in causes() {
                let factory = Factory::new();
                let mut scope = factory.begin_scope().unwrap();
                factory.observations.start();
                *factory.observations.control_stop.lock().unwrap() = Some((at, cause.clone()));
                assert_eq!(
                    checked_control(
                        preparation(
                            &functions,
                            input,
                            selected.clone(),
                            effects,
                            &mut scope,
                            factory.observations.as_ref()
                        )
                        .unwrap_err()
                    ),
                    Some(cause)
                );
                assert_eq!(factory.observations.events(), trace[..=at]);
                drop(scope);
                assert_eq!(factory.observations.held.load(Ordering::SeqCst), 0);
            }
        }
    });
}
#[test]
fn actual_public_compiler_functions_keep_primary_h_and_real_bounded_scope_exit() {
    let functions = rng_subset();
    let source = package(&functions, 2);
    let good = Factory::new();
    let program = compile_created(source.clone(), &functions, &good).unwrap();
    let trace = good.observations.events();
    let mut phase = None;
    let mut first = None;
    for (at, event) in trace.iter().enumerate() {
        match event {
            Event::Control(current, _) => phase = Some(*current),
            Event::Exact(..) | Event::Opaque(..)
                if phase == Some(CompilePhase::FunctionSpecialization) =>
            {
                first = Some(at);
                break;
            }
            _ => {}
        }
    }
    let first = first.expect("actual compiler reaches the full function signature constructor");
    assert_eq!(
        program
            .units()
            .iter()
            .map(|unit| unit.calls().len())
            .sum::<usize>(),
        2
    );
    for call in program
        .units()
        .iter()
        .flat_map(|unit| unit.calls().values())
    {
        assert_rand(call);
    }
    drop(program);
    assert_eq!(good.observations.held.load(Ordering::SeqCst), 0);
    assert_eq!(
        good.observations.scope_exits.load(Ordering::SeqCst),
        good.observations.next_scope.load(Ordering::SeqCst)
    );
    let function_control = trace
        .iter()
        .enumerate()
        .find_map(|(at, event)| {
            matches!(event,Event::Control(CompilePhase::FunctionSpecialization,units) if *units>0)
                .then_some(at)
        })
        .expect("actual selected validation has a bounded positive checkpoint");
    for cause in causes() {
        let stopped = Factory::new();
        *stopped.observations.control_stop.lock().unwrap() =
            Some((function_control, cause.clone()));
        assert!(
            matches!(compile_created(source.clone(),&functions,&stopped),
            Err(FragmentCreationError::Control(actual)) if actual==cause)
        );
        assert_eq!(stopped.observations.events(), trace[..=function_control]);
        assert_eq!(stopped.observations.held.load(Ordering::SeqCst), 0);
    }
    let bad = Factory::new();
    *bad.observations.request_stop.lock().unwrap() = Some(first);
    let result = compile_created(source, &functions, &bad);
    let Err(FragmentCreationError::Creator(FirstH(token))) = result else {
        panic!("public compiler preserves function constructor H")
    };
    assert!(Arc::ptr_eq(&token, &bad.token));
    assert_eq!(bad.observations.events(), trace[..=first]);
    assert_eq!(bad.observations.held.load(Ordering::SeqCst), 0);
    assert_eq!(
        bad.observations.scope_exits.load(Ordering::SeqCst),
        bad.observations.next_scope.load(Ordering::SeqCst)
    );
}

// A real original source request uses both shared Value and Lambda channels.
// Its immutable complete types are FE-authored, not inferred from a carrier.
fn nonempty_original_request() -> novarocks_expression_contract::PhysicalCallRequest {
    use novarocks_physical_plan::StaticFunctionArgument as A;
    let json = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        true,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let uuid = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        novarocks_type_contract::ValueLogicalType::Uuid,
    )
    .unwrap();
    novarocks_expression_contract::PhysicalCallRequest {
        arguments: vec![
            A::Value {
                value_type: json.clone(),
                constant: None,
            },
            A::Lambda {
                parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                    vec![uuid].into_boxed_slice(),
                ),
                result_type: json.clone(),
            },
        ]
        .into_boxed_slice(),
        logical_argument_count: 2,
        expected_result_type: Some(json),
    }
}
fn materialize_created_request<'a>(
    source: &'a novarocks_expression_contract::PhysicalCallRequest,
    pools: &novarocks_physical_plan::ConstantPools,
    factory: &Factory,
) -> Result<
    novarocks_physical_plan::MaterializedCallRequest<'a>,
    crate::expressions::ExpressionCreationFailure<FirstH>,
> {
    let mut work = CompileCheckpoints::try_new(
        factory.observations.as_ref(),
        CompilePhase::FunctionSpecialization,
    )?;
    crate::original_requests::materialize_call_request_created(
        source,
        pools,
        &mut work,
        Some((factory, &ConstantValidation::INPUT_DERIVED)),
    )
}
fn assert_materialized_source(
    source: &novarocks_expression_contract::PhysicalCallRequest,
    materialized: &novarocks_physical_plan::MaterializedCallRequest<'_>,
) {
    use novarocks_physical_plan::StaticFunctionArgument as A;
    let actual = materialized.request();
    assert_eq!(actual.logical_argument_count, source.logical_argument_count);
    assert_eq!(actual.arguments.len(), source.arguments.len());
    assert!(std::ptr::eq(
        actual.expected_result_type.unwrap(),
        source.expected_result_type.as_ref().unwrap()
    ));
    let (
        A::Value {
            value_type: expected,
            constant: None,
        },
        novarocks_functions::FunctionArgument::Value {
            value_type: actual,
            constant: None,
        },
    ) = (&source.arguments[0], &actual.arguments[0])
    else {
        panic!("original Value channel");
    };
    assert_eq!(actual, expected);
    assert_eq!(
        actual.logical_type(),
        novarocks_type_contract::ValueLogicalType::Json
    );
    assert!(
        actual
            .typed_binding()
            .value_type()
            .shares_storage(expected.typed_binding().value_type())
    );
    assert!(
        actual
            .typed_binding()
            .layout()
            .shares_storage(expected.typed_binding().layout())
    );
    let (
        A::Lambda {
            parameter_types: expected,
            result_type: expected_result,
        },
        novarocks_functions::FunctionArgument::Lambda {
            parameter_types: actual,
            result_type: actual_result,
        },
    ) = (&source.arguments[1], &materialized.request().arguments[1])
    else {
        panic!("original Lambda channel");
    };
    assert_eq!(actual, expected);
    assert!(std::ptr::eq(
        actual.as_slice().as_ptr(),
        expected.as_slice().as_ptr()
    ));
    assert_eq!(
        actual[0].logical_type(),
        novarocks_type_contract::ValueLogicalType::Uuid
    );
    assert_eq!(actual_result, expected_result);
    assert!(
        actual_result
            .typed_binding()
            .value_type()
            .shares_storage(expected_result.typed_binding().value_type())
    );
}

#[test]
fn actual_nonempty_original_request_quotes_its_backing_and_survives_factory_exit() {
    let source = nonempty_original_request();
    let pools = novarocks_physical_plan::ConstantPools::empty();
    let factory = Factory::new();
    let observations = Arc::clone(&factory.observations);
    let materialized = materialize_created_request(&source, &pools, &factory).unwrap();
    let backing =
        Layout::array::<novarocks_functions::FunctionArgument>(source.arguments.len()).unwrap();
    let backing_quotes = operation_quotes(&observations.events());
    assert!(
        !backing_quotes.is_empty(),
        "actual argument backing is admitted"
    );
    assert!(
        backing_quotes
            .iter()
            .all(|layout| *layout == (backing.size(), backing.align())),
        "only argument backing is new; source types and guarded Lambda parameters are shared"
    );
    let trace = observations.events();
    let header_flush = trace.iter().position(|event| matches!(event, Event::Control(CompilePhase::FunctionSpecialization, units) if *units > 0)).unwrap();
    let first_holder = trace
        .iter()
        .position(|event| matches!(event, Event::Holder(..)))
        .unwrap();
    assert!(header_flush < first_holder);
    assert_eq!(observations.next_scope.load(Ordering::SeqCst), 1);
    assert_eq!(
        observations.scope_exits.load(Ordering::SeqCst),
        0,
        "the local scope has exited but request backing is still independently held"
    );
    assert!(observations.held.load(Ordering::SeqCst) >= backing.size());
    assert_materialized_source(&source, &materialized);
    drop(factory);
    assert_eq!(observations.factory_exits.load(Ordering::SeqCst), 1);
    assert_eq!(observations.scope_exits.load(Ordering::SeqCst), 0);
    assert_materialized_source(&source, &materialized);
    drop(materialized);
    assert_eq!(observations.held.load(Ordering::SeqCst), 0);
    assert_eq!(observations.scope_exits.load(Ordering::SeqCst), 1);

    // The existing caller-owned Scope port keeps its original callback sequence:
    // only the lazy factory route adds the actual holder at the reserve slot.
    let direct_factory = Factory::new();
    let mut direct_scope = direct_factory.begin_scope().unwrap();
    direct_factory.observations.start();
    let mut direct_work = CompileCheckpoints::try_new(
        direct_factory.observations.as_ref(),
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    let direct = novarocks_physical_plan::materialize_call_request_with_creator_in(
        &source,
        &pools,
        &mut direct_scope,
        &mut direct_work,
        novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS,
    )
    .unwrap();
    let without_holder: Vec<_> = trace
        .into_iter()
        .filter(|event| !matches!(event, Event::Holder(..)))
        .collect();
    assert_eq!(direct_factory.observations.events(), without_holder);
    assert_materialized_source(&source, &direct);
    drop(direct_scope);
    assert_eq!(
        direct_factory
            .observations
            .scope_exits
            .load(Ordering::SeqCst),
        0
    );
    drop(direct);
    assert_eq!(direct_factory.observations.held.load(Ordering::SeqCst), 0);
    assert_eq!(
        direct_factory
            .observations
            .scope_exits
            .load(Ordering::SeqCst),
        1
    );

    // The original pure facade stays the same source author and callback trace.
    let pure_control = Factory::new();
    let mut work = CompileCheckpoints::try_new(
        pure_control.observations.as_ref(),
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    let pure =
        crate::original_requests::materialize_call_request_observed(&source, &pools, &mut work)
            .unwrap();
    let expected_trace = pure_control.observations.events();
    let none_control = Factory::new();
    let mut work = CompileCheckpoints::try_new(
        none_control.observations.as_ref(),
        CompilePhase::FunctionSpecialization,
    )
    .unwrap();
    let none = crate::original_requests::materialize_call_request_created::<FirstH, Factory>(
        &source, &pools, &mut work, None,
    )
    .unwrap();
    assert_materialized_source(&source, &pure);
    assert_materialized_source(&source, &none);
    assert_eq!(none_control.observations.events(), expected_trace);
    assert_eq!(
        none_control.observations.next_scope.load(Ordering::SeqCst),
        0
    );
}

#[test]
fn actual_nonempty_original_request_preserves_each_first_h_and_control_prefix_without_tail() {
    use crate::expressions::ExpressionCreationFailure as F;
    let source = nonempty_original_request();
    let pools = novarocks_physical_plan::ConstantPools::empty();
    let good = Factory::new();
    let materialized = materialize_created_request(&source, &pools, &good).unwrap();
    let trace = good.observations.events();
    assert!(trace.iter().any(|event| matches!(event, Event::Exact(..))));
    drop(materialized);
    assert_eq!(good.observations.held.load(Ordering::SeqCst), 0);
    for (at, event) in trace.iter().enumerate() {
        if matches!(
            event,
            Event::Holder(..) | Event::Exact(..) | Event::Opaque(..)
        ) {
            let factory = Factory::new();
            *factory.observations.request_stop.lock().unwrap() = Some(at);
            match materialize_created_request(&source, &pools, &factory) {
                Err(F::Creator(FirstH(actual))) => assert!(Arc::ptr_eq(&actual, &factory.token)),
                other => panic!("originating argument container H changed: {other:?}"),
            }
            assert_eq!(factory.observations.events(), trace[..=at]);
            assert_eq!(factory.observations.held.load(Ordering::SeqCst), 0);
            assert_eq!(
                factory.observations.scope_exits.load(Ordering::SeqCst),
                usize::from(!matches!(event, Event::Holder(..)))
            );
        }
        if matches!(event, Event::Control(..)) {
            for cause in causes() {
                let factory = Factory::new();
                *factory.observations.control_stop.lock().unwrap() = Some((at, cause.clone()));
                assert!(
                    matches!(materialize_created_request(&source, &pools, &factory), Err(F::Control(actual)) if actual == cause)
                );
                assert_eq!(factory.observations.events(), trace[..=at]);
                assert_eq!(factory.observations.held.load(Ordering::SeqCst), 0);
            }
        }
    }
}

#[test]
fn original_request_header_gates_and_pending_control_precede_lazy_scope_creation() {
    use crate::expressions::{ExpressionCreationFailure as F, ExpressionLoweringError as O};
    use novarocks_physical_plan::StaticFunctionArgument as A;
    let pools = novarocks_physical_plan::ConstantPools::empty();
    for axis in 0..3 {
        let mut source = nonempty_original_request();
        match axis {
            0 => source.logical_argument_count = source.arguments.len() + 1,
            1 => {
                source.arguments = vec![
                    source.arguments[0].clone();
                    novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS + 1
                ]
                .into_boxed_slice();
                source.logical_argument_count = source.arguments.len();
            }
            2 => {
                let A::Lambda {
                    parameter_types, ..
                } = &mut source.arguments[1]
                else {
                    unreachable!()
                };
                *parameter_types = novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                    vec![
                        parameter_types[0].clone();
                        novarocks_functions::MAX_CALL_EFFECT_ARGUMENTS + 1
                    ]
                    .into_boxed_slice(),
                );
            }
            _ => unreachable!(),
        }
        let factory = Factory::new();
        // An independently failing holder must not replace the original gate.
        *factory.observations.request_stop.lock().unwrap() = Some(1);
        let error = materialize_created_request(&source, &pools, &factory).unwrap_err();
        if axis == 0 {
            assert!(matches!(
                error,
                F::Owner(O::Invalid(
                    "original request logical argument count exceeds channels"
                ))
            ));
        } else {
            assert!(matches!(
                error,
                F::Control(CompileControlError::ResourceExhausted)
            ));
        }
        assert_eq!(factory.observations.next_scope.load(Ordering::SeqCst), 0);
        assert_eq!(factory.observations.held.load(Ordering::SeqCst), 0);
        assert!(!factory.observations.events().iter().any(|event| matches!(
            event,
            Event::Holder(..) | Event::Exact(..) | Event::Opaque(..)
        )));
    }
    let source = nonempty_original_request();
    for cause in causes() {
        let factory = Factory::new();
        let mut work = CompileCheckpoints::try_new(
            factory.observations.as_ref(),
            CompilePhase::FunctionSpecialization,
        )
        .unwrap();
        work.step().unwrap();
        *factory.observations.control_stop.lock().unwrap() = Some((1, cause.clone()));
        *factory.observations.request_stop.lock().unwrap() = Some(2);
        assert!(
            matches!(crate::original_requests::materialize_call_request_created(
            &source, &pools, &mut work, Some((&factory, &ConstantValidation::INPUT_DERIVED)),
        ), Err(F::Control(actual)) if actual == cause)
        );
        assert_eq!(factory.observations.next_scope.load(Ordering::SeqCst), 0);
        assert_eq!(factory.observations.events().len(), 2);
    }

    let source = novarocks_expression_contract::PhysicalCallRequest {
        arguments: Box::new([]),
        logical_argument_count: 0,
        expected_result_type: None,
    };
    let factory = Factory::new();
    let empty = materialize_created_request(&source, &pools, &factory).unwrap();
    assert!(empty.request().arguments.is_empty());
    assert_eq!(factory.observations.next_scope.load(Ordering::SeqCst), 0);
    drop(empty);
    assert_eq!(factory.observations.held.load(Ordering::SeqCst), 0);
}

#[test]
fn actual_runtime_filter_canonical_keeps_complete_type_creator_and_first_h() {
    use crate::runtime_filter_lowering_tests::{Rf, broadcast};
    use novarocks_local_program::{ProgramNodeKind, StaticFilterContract};

    // Reuse this module's actual finite factory and the original RF source;
    // provider validation remains the original pure provider author.
    fn compile_rf(
        source: Arc<FragmentPackage>,
        functions: &PureEngineFunctionCatalog,
        factory: &Factory,
        stop: Option<usize>,
    ) -> Result<LocalProgram, FragmentCreationError<FirstH>> {
        let validated = validate_fragment_providers(
            source,
            &crate::scan_lowering_tests::providers(),
            &FixtureControl,
        )
        .unwrap();
        let main = RefCell::new(
            factory
                .begin_scope()
                .map_err(FragmentCreationError::Creator)?,
        );
        let mut temporary = factory
            .begin_scope()
            .map_err(FragmentCreationError::Creator)?;
        let mut request = |layout| main.borrow_mut().request_layout(layout);
        let mut opaque = |bytes| main.borrow_mut().observe_opaque_upper_bound(bytes);
        let mut creation =
            ValueSchemaCreation::new(main.borrow().storage_lifetime(), &mut request, &mut opaque);
        factory.observations.start();
        *factory.observations.request_stop.lock().unwrap() = stop;
        compile_fragment_with_creation(
            validated,
            functions,
            crate::scan_lowering_tests::options(None),
            &mut creation,
            &mut temporary,
            factory,
            factory.observations.as_ref(),
        )
    }

    let source = broadcast(Rf::blocking());
    let novarocks_physical_plan::RuntimeFilterDomain::Membership {
        ty: original_type,
        null_semantics: original_null,
    } = &source.cuts().runtime_filters[0].domain
    else {
        panic!("the original source is a membership filter")
    };
    let original_type = original_type.clone();
    assert_eq!(
        *original_null,
        novarocks_physical_plan::RuntimeFilterNullSemantics::NeverMatches,
    );
    let functions = crate::scan_lowering_tests::functions();
    let factory = Factory::new();
    let observations = factory.observations.clone();
    let program = compile_rf(source.clone(), &functions, &factory, None).unwrap();
    let trace = observations.events();
    let canonical_scope = trace
        .iter()
        .find_map(|event| match event {
            // Main and temporary holders were created before trace start;
            // the first new entity is the original membership canonical.
            Event::Holder(scope, _) => Some(*scope),
            _ => None,
        })
        .expect("actual canonical construction opens its independent scope");
    let first_canonical_request = trace
        .iter()
        .position(|event| matches!(event, Event::Exact(scope, _, _) if *scope == canonical_scope))
        .expect("actual canonical construction has a before-allocation quote");
    let canonical = program
        .graph()
        .nodes()
        .iter()
        .find_map(|node| match node.kind() {
            ProgramNodeKind::Join {
                runtime_filters, ..
            } => {
                let [producer] = runtime_filters.as_slice() else {
                    panic!("the original join has one membership producer")
                };
                let StaticFilterContract::Membership {
                    canonical,
                    value_type,
                } = producer.producer.contract()
                else {
                    panic!("the producer retains its original membership contract")
                };
                assert_eq!(value_type, &original_type);
                assert_eq!(
                    canonical.null_semantics(),
                    novarocks_local_program::FilterNullSemantics::NeverMatches,
                );
                Some(canonical.clone())
            }
            _ => None,
        })
        .unwrap();
    let expected = novarocks_local_program::FilterMembershipSchema::try_new(
        original_type.data_type(),
        novarocks_local_program::FilterNullSemantics::NeverMatches,
    )
    .unwrap();
    assert_eq!(
        canonical, expected,
        "the original canonical bytes and digest survive"
    );
    let peak = observations.held.load(Ordering::SeqCst);
    assert!(peak > 0);
    drop(program);
    drop(source);
    drop(functions);
    drop(factory);
    assert_eq!(observations.factory_exits.load(Ordering::SeqCst), 1);
    let remaining = observations.held.load(Ordering::SeqCst);
    assert!(
        remaining > 0 && remaining < peak,
        "only the finite canonical entity survives"
    );
    assert_eq!(canonical, expected);
    drop(canonical);
    assert_eq!(observations.held.load(Ordering::SeqCst), 0);

    let rejected = Factory::new();
    let failed_observations = rejected.observations.clone();
    let token = rejected.token.clone();
    let result = compile_rf(
        broadcast(Rf::blocking()),
        &crate::scan_lowering_tests::functions(),
        &rejected,
        Some(first_canonical_request),
    );
    match result {
        Err(FragmentCreationError::Creator(FirstH(actual))) => {
            assert!(Arc::ptr_eq(&actual, &token));
        }
        Err(other) => panic!("the original canonical request must keep its H: {other:?}"),
        Ok(_) => panic!("the actual canonical request must refuse"),
    }
    assert_eq!(
        failed_observations.events(),
        trace[..=first_canonical_request],
        "neither a later control callback nor a footer follows the primary H",
    );
    drop(rejected);
    assert_eq!(failed_observations.held.load(Ordering::SeqCst), 0);
}

#[test]
fn ordered_topn_compiler_actual_creator_keeps_both_site_collections_after_program_exit() {
    let (source, functions) = crate::runtime_filter_lowering_tests::ordered_scan_package(
        true,
        novarocks_physical_plan::SortDirection::Descending,
        novarocks_physical_plan::NullOrdering::First,
    );
    let validated = validate_fragment_providers(
        source.clone(),
        &crate::scan_lowering_tests::providers(),
        &crate::scan_lowering_tests::FixtureControl,
    )
    .unwrap();
    let factory = Factory::new();
    let observations = factory.observations.clone();
    let program = compile_created_validated(
        validated,
        &functions,
        &factory,
        crate::scan_lowering_tests::options(None),
    )
    .unwrap();
    let mut aggregate = None;
    let mut scan = None;
    for node in program.graph().nodes() {
        match node.kind() {
            novarocks_local_program::ProgramNodeKind::Aggregate { topn_filters, .. }
                if !topn_filters.is_empty() =>
            {
                aggregate = Some(topn_filters.clone())
            }
            novarocks_local_program::ProgramNodeKind::Scan {
                runtime_filters, ..
            } => scan = Some(runtime_filters.clone()),
            _ => {}
        }
    }
    let aggregate = aggregate.unwrap();
    let scan = scan.unwrap();
    assert_eq!(aggregate.len(), 1);
    assert_eq!(scan.len(), 1);
    assert_eq!(
        aggregate[0].producer.contract(),
        scan[0].consumer.contract()
    );
    let peak = observations.held.load(Ordering::SeqCst);
    assert!(peak > 0);
    drop(program);
    drop(source);
    drop(functions);
    drop(factory);
    assert_eq!(observations.factory_exits.load(Ordering::SeqCst), 1);
    let both = observations.held.load(Ordering::SeqCst);
    assert!(both > 0 && both < peak);
    drop(aggregate);
    let scan_only = observations.held.load(Ordering::SeqCst);
    assert!(scan_only > 0 && scan_only < both);
    std::thread::spawn(move || drop(scan)).join().unwrap();
    assert_eq!(observations.held.load(Ordering::SeqCst), 0);
}

#[test]
fn ordered_topn_compiler_actual_creator_preserves_every_refusal_prefix() {
    let (source, functions) = crate::runtime_filter_lowering_tests::ordered_scan_package(
        false,
        novarocks_physical_plan::SortDirection::Ascending,
        novarocks_physical_plan::NullOrdering::Last,
    );
    let run = |factory: &Factory| {
        let validated = validate_fragment_providers(
            source.clone(),
            &crate::scan_lowering_tests::providers(),
            &crate::scan_lowering_tests::FixtureControl,
        )
        .unwrap();
        compile_created_validated(
            validated,
            &functions,
            factory,
            crate::scan_lowering_tests::options(None),
        )
    };
    let baseline = Factory::new();
    drop(run(&baseline).unwrap());
    let trace = baseline.observations.events();
    assert_eq!(baseline.observations.held.load(Ordering::SeqCst), 0);
    assert!(
        trace
            .iter()
            .any(|event| matches!(event, Event::Opaque(1, _)))
    );
    for (at, event) in trace.iter().enumerate() {
        match event {
            Event::Holder(..) | Event::Exact(..) | Event::Opaque(..) => {
                let factory = Factory::new();
                *factory.observations.request_stop.lock().unwrap() = Some(at);
                match run(&factory) {
                    Err(FragmentCreationError::Creator(FirstH(actual))) => {
                        assert!(Arc::ptr_eq(&actual, &factory.token))
                    }
                    Err(other) => panic!("original ordered creation H: {other:?}"),
                    Ok(_) => panic!("ordered request must refuse"),
                }
                assert_eq!(factory.observations.events(), trace[..=at]);
                assert_eq!(factory.observations.held.load(Ordering::SeqCst), 0);
            }
            Event::Control(..) => {
                for cause in [
                    CompileControlError::Cancelled,
                    CompileControlError::DeadlineExceeded,
                    CompileControlError::ResourceExhausted,
                ] {
                    let factory = Factory::new();
                    *factory.observations.control_stop.lock().unwrap() = Some((at, cause.clone()));
                    assert!(
                        matches!(run(&factory), Err(FragmentCreationError::Control(actual)) if actual == cause)
                    );
                    assert_eq!(factory.observations.events(), trace[..=at]);
                    assert_eq!(factory.observations.held.load(Ordering::SeqCst), 0);
                }
            }
        }
    }
}

#[test]
fn synthesized_reads_keep_actual_source_quotes_first_refusal_and_independent_flow_storage() {
    let source = crate::union_lowering_tests::fixture_width(3).package;
    let functions = crate::union_lowering_tests::functions();
    // This fixture retains four units and both original/synthetic source tables.
    // Its explicit test budget accommodates their cumulative actual requests.
    const SCOPE_LIMIT: usize = 256 * 1024;
    let good = Factory::with_scope_limit(SCOPE_LIMIT);
    let program = compile_created(source.clone(), &functions, &good).unwrap_or_else(|_| {
        let events = good.observations.events();
        panic!(
            "actual scoped Union read source refused: events={}, last={:?}, held={}",
            events.len(),
            events.last(),
            good.observations.held.load(Ordering::SeqCst)
        )
    });
    let normalizers = program
        .graph()
        .nodes()
        .iter()
        .filter_map(|node| match node.kind() {
            ProgramNodeKind::Project {
                exprs,
                is_subordinate: true,
                ..
            } => Some(exprs),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(normalizers.len(), 2);
    assert_eq!(program.units().len(), 4);
    let first = normalizers[0][0];
    drop(normalizers);
    let last_unit = program.unit(first.unit).unwrap().clone();
    let flow = last_unit.control_flow().clone();
    assert_eq!(last_unit.roots().len(), 3);
    assert_eq!(flow.use_reference_count(), 3);
    let trace = good.observations.events();
    assert!(trace.iter().any(|event| matches!(event, Event::Exact(..))));
    assert!(trace.iter().any(|event| matches!(event, Event::Opaque(..))));
    drop(program);
    assert!(good.observations.held.load(Ordering::SeqCst) > 0);
    drop(last_unit);
    assert!(
        good.observations.held.load(Ordering::SeqCst) > 0,
        "the independently cloned compiled flow retains its actual allocation owner"
    );
    drop(flow);
    assert_eq!(good.observations.held.load(Ordering::SeqCst), 0);
    assert_eq!(
        good.observations.scope_exits.load(Ordering::SeqCst),
        good.observations.next_scope.load(Ordering::SeqCst)
    );

    // Exercise each actual source, temporary, retained and holder request, not
    // a guessed list of private builder allocations or a successful no-op port.
    for (at, event) in trace.iter().enumerate() {
        if matches!(event, Event::Control(..)) {
            continue;
        }
        let refused = Factory::with_scope_limit(SCOPE_LIMIT);
        *refused.observations.request_stop.lock().unwrap() = Some(at);
        match compile_created(source.clone(), &functions, &refused) {
            Err(FragmentCreationError::Creator(FirstH(token))) => {
                assert!(Arc::ptr_eq(&token, &refused.token));
            }
            _ => panic!("the original synthesized read constructor must return its first H"),
        }
        assert_eq!(refused.observations.events(), trace[..=at]);
        assert_eq!(refused.observations.held.load(Ordering::SeqCst), 0);
    }
}

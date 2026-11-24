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
use arrow_array::{Array, Int32Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileControlError, EvaluationDemand, EvaluationRegionId,
    ExpressionEffectContext, ExpressionUseId, FunctionKind, FunctionValueType, SemanticParameters,
};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

const METADATA_ONLY: &str = "metadata_only_fixture";

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after primary refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
struct CountedOwner {
    original: Arc<dyn InstalledPureOwner>,
    calls: Arc<AtomicUsize>,
}
impl InstalledPureOwner for CountedOwner {
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
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.original
            .prepare(input, selected, frozen, options, control)
    }
}
struct Fixture {
    catalog: EngineFunctionCatalog,
    function: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    arguments: Vec<FunctionArgument>,
    parameters: SemanticParameters,
    uses: [Option<ExpressionUseId>; 1],
    calls: Arc<AtomicUsize>,
}
impl Fixture {
    fn new() -> Self {
        let original = crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut builder = EngineFunctionCatalogBuilder::new();
        for original in original.definitions() {
            let mut definition = original.clone();
            if definition.canonical_name() == "abs" {
                let attachment = definition.binding.as_mut().unwrap().pure.as_mut().unwrap();
                attachment.owner = Arc::new(CountedOwner {
                    original: Arc::clone(&attachment.owner),
                    calls: Arc::clone(&calls),
                });
            }
            builder.register(definition).unwrap();
        }
        // Keep the negative fixture independent of builtin migration progress:
        // its exact existing Utf8 binder stays real, while missing effect and
        // CPU attachments are explicit test inputs rather than claimed installs.
        let mut metadata_only = original
            .definition("title_case", FunctionKind::Scalar)
            .unwrap()
            .clone();
        metadata_only.canonical_name = METADATA_ONLY.into();
        let binding = metadata_only.binding.as_mut().unwrap();
        let overloads = binding
            .declaration
            .overloads()
            .iter()
            .cloned()
            .map(|mut overload| {
                overload.effects = None;
                overload
            });
        binding.declaration = Arc::new(
            crate::FunctionBindingDeclaration::try_new(
                FunctionId::try_new("fixture/scalar/metadata-only/v1").unwrap(),
                FunctionKind::Scalar,
                overloads,
            )
            .unwrap(),
        );
        binding.pure = None;
        builder.register(metadata_only).unwrap();
        // Actual builtins and one explicit metadata-only negative fixture.
        // No complete pure seal or invented installation inventory.
        let catalog = builder.seal_bound().unwrap();
        let arguments = vec![FunctionArgument::Value {
            value_type: FunctionValueType::new(DataType::Int32, false),
            constant: None,
        }];
        let resolved = catalog
            .resolve_bound_user(
                "abs",
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: 1,
                    expected_result_type: None,
                },
                &Control::default(),
            )
            .unwrap();
        Self {
            catalog,
            function: resolved.function_id,
            selected: Arc::new(resolved.selected),
            arguments,
            parameters: SemanticParameters::try_new([]).unwrap(),
            uses: [Some(ExpressionUseId::new(42))],
            calls,
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
            decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::ReportError,
            proof_scope: CallProofScope::Region(self.context().region),
        }
    }
    fn options(&self) -> PureCallPreparation {
        PureCallPreparation::Scalar {
            arguments: ScopedExpressionEffects::pure_value(self.context()),
        }
    }
    fn prepare(
        &self,
        control: &Control,
    ) -> Result<PureCallSpecialization, FunctionSpecializationFailure> {
        self.catalog.prepare_fresh_selected(
            self.input(),
            Arc::clone(&self.selected),
            self.options(),
            control,
        )
    }
}

#[test]
fn ordinary_selected_fresh_uses_the_actual_abs_attachment_once_without_a_catalogue_seal() {
    let fixture = Fixture::new();
    assert!(
        fixture
            .catalog
            .definition(METADATA_ONLY, FunctionKind::Scalar)
            .unwrap()
            .binding
            .as_ref()
            .unwrap()
            .pure
            .is_none()
    );
    let prepared = fixture.prepare(&Control::default()).unwrap();
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 1);
    assert_eq!(prepared.source(), PurePreparationSource::Fresh);
    assert_eq!(prepared.implementation().abi, PureKernelAbi::ScalarV1);
    assert_eq!(
        prepared.implementation().implementation.as_str(),
        "builtin.scalar/abs/selected-v1"
    );
    assert!(Arc::ptr_eq(
        prepared.call_contract().selected_owner(),
        &fixture.selected
    ));
    assert_eq!(prepared.call_contract().context(), fixture.context().into());
    assert_eq!(
        prepared.call_contract().decimal_overflow_policy(),
        fixture.input().decimal_overflow_policy
    );
    assert_eq!(
        prepared.call_contract().effects().value_stability,
        crate::FunctionVolatility::Immutable
    );
    assert_eq!(
        prepared.call_contract().effects().null_behavior,
        FunctionNullBehavior::Strict
    );
    assert_eq!(
        prepared.call_contract().effects().own_row_error,
        novarocks_type_contract::FunctionIntrinsicRowError::MayRaise
    );
    assert!(prepared.call_contract().effects().environment.is_empty());

    // A successful exact attachment preparation never grants the complete
    // catalogue seal: the same metadata-only definition still forbids it.
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder
        .register(
            fixture
                .catalog
                .definition("abs", FunctionKind::Scalar)
                .unwrap()
                .clone(),
        )
        .unwrap();
    let metadata_only = fixture
        .catalog
        .definition(METADATA_ONLY, FunctionKind::Scalar)
        .unwrap();
    let absent = metadata_only.binding_declaration().unwrap().overloads()[0]
        .identity
        .clone();
    builder.register(metadata_only.clone()).unwrap();
    assert!(matches!(builder.seal_pure([]),
        Err(PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(overload))) if overload == absent));
}

#[test]
fn ordinary_selected_fresh_refuses_missing_owner_binding_and_record_as_typed_errors() {
    let mut fixture = Fixture::new();
    let args = [FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Utf8, false),
        constant: None,
    }];
    let resolved = fixture
        .catalog
        .resolve_bound_user(
            METADATA_ONLY,
            FunctionKind::Scalar,
            FunctionBindingRequest {
                arguments: &args,
                logical_argument_count: 1,
                expected_result_type: None,
            },
            &Control::default(),
        )
        .unwrap();
    let selected = Arc::new(resolved.selected);
    let mut input = fixture.input();
    input.function_id = &resolved.function_id;
    input.selected = &selected;
    input.request.arguments = &args;
    assert!(matches!(
        fixture.catalog.prepare_fresh_selected(
            input,
            Arc::clone(&selected),
            fixture.options(),
            &Control::default()
        ),
        Err(FunctionSpecializationFailure::MissingPureImplementation(overload))
            if overload == selected.overload
    ));
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 0);

    // These private mutations exercise defensive dispatcher boundaries; they
    // are not alternate public registration authors or executable catalogues.
    let index = fixture.catalog.identities[&fixture.function];
    let binding = fixture.catalog.definitions[index].binding.as_mut().unwrap();
    binding.pure.as_mut().unwrap().implementations = Arc::from([]);
    assert!(matches!(
        fixture.prepare(&Control::default()),
        Err(FunctionSpecializationFailure::MissingPureImplementation(overload))
            if overload == fixture.selected.overload
    ));
    fixture.catalog.definitions[index].binding = None;
    assert!(matches!(
        fixture.prepare(&Control::default()),
        Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::MissingBindingDeclaration
        ))
    ));
}

#[test]
fn ordinary_selected_fresh_checks_identity_options_complete_source_and_domain_before_publication() {
    let fixture = Fixture::new();
    let other = Arc::new((*fixture.selected).clone());
    assert!(matches!(
        fixture.catalog.prepare_fresh_selected(
            fixture.input(),
            other,
            fixture.options(),
            &Control::default()
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    for fault in 0..6 {
        let mut input = fixture.input();
        let mut selected = (*fixture.selected).clone();
        let mut arguments = fixture.arguments.clone();
        match fault {
            0 => input.kind = FunctionKind::Window,
            1 => {
                selected.overload = FunctionOverloadId::try_new("fixture/unknown/overload").unwrap()
            }
            2 => input.request.logical_argument_count = 0,
            3 => {
                arguments = vec![FunctionArgument::Value {
                    value_type: FunctionValueType::new(DataType::Int64, false),
                    constant: None,
                }]
            }
            4 => input.proof_scope = CallProofScope::Region(EvaluationRegionId::new(8)),
            5 => {
                selected.result_type =
                    FunctionResultType::Scalar(FunctionValueType::new(DataType::Boolean, false))
            }
            _ => unreachable!(),
        }
        let selected = Arc::new(selected);
        input.selected = &selected;
        input.request.arguments = &arguments;
        assert!(
            fixture
                .catalog
                .prepare_fresh_selected(
                    input,
                    selected.clone(),
                    fixture.options(),
                    &Control::default()
                )
                .is_err(),
            "fault {fault}"
        );
    }
    assert!(
        fixture
            .catalog
            .prepare_fresh_selected(
                fixture.input(),
                fixture.selected.clone(),
                PureCallPreparation::Table {
                    arguments: ScopedRelationalEffects::pure_value(
                        novarocks_type_contract::RelationEffectContext::call(
                            novarocks_type_contract::RelationCallId::new(700)
                        )
                    )
                },
                &Control::default()
            )
            .is_err()
    );
    // Domain refinement occurs in the actual owner, while earlier signature
    // and ABI failures never invoke that owner.
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 1);
}

struct ConstructionControl;
impl PureCompileControl for ConstructionControl {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
    }
}
fn constant_policy() -> ConstantPolicy {
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

#[test]
fn selected_fresh_checks_real_constant_pool_source_at_a_nonzero_ordinal() {
    let mut fixture = Fixture::new();
    let ty = FunctionValueType::new(DataType::Int32, false);
    let pool = ConstantPool::try_new(
        Arc::new(ty.try_to_field("source").unwrap()),
        ty.clone(),
        Int32Array::from(vec![77, -9]).to_data(),
        constant_policy(),
        CompilePhase::Validate,
        &ConstructionControl,
    )
    .unwrap();
    let selected_constant = pool.value(1).unwrap();
    assert_eq!(selected_constant.ordinal(), 1);
    fixture.arguments = vec![FunctionArgument::Value {
        value_type: ty.clone(),
        constant: Some(selected_constant),
    }];
    let prepared = fixture.prepare(&Control::default()).unwrap();
    assert!(Arc::ptr_eq(
        prepared.call_contract().selected_owner(),
        &fixture.selected
    ));
    assert_eq!(fixture.calls.load(Ordering::Relaxed), 1);

    let foreign_type = FunctionValueType::new(DataType::Boolean, false);
    let foreign_constant = ConstantValue::from_boolean(
        Arc::new(foreign_type.try_to_field("foreign").unwrap()),
        foreign_type,
        true,
        constant_policy(),
        CompilePhase::Validate,
        &ConstructionControl,
    )
    .unwrap();
    fixture.arguments = vec![FunctionArgument::Value {
        value_type: ty,
        constant: Some(foreign_constant),
    }];
    assert!(matches!(
        fixture.prepare(&Control::default()),
        Err(FunctionSpecializationFailure::Binding(_))
    ));
    assert_eq!(
        fixture.calls.load(Ordering::Relaxed),
        1,
        "foreign constant source reached the installed owner"
    );
    prefixes(|control| fixture.prepare(control), false);
}

fn cause(error: FunctionSpecializationFailure) -> Option<CompileControlError> {
    match error {
        FunctionSpecializationFailure::Control(cause) => Some(cause),
        FunctionSpecializationFailure::Kernel(KernelFailure::Cancelled) => {
            Some(CompileControlError::Cancelled)
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::DeadlineExceeded) => {
            Some(CompileControlError::DeadlineExceeded)
        }
        FunctionSpecializationFailure::Kernel(KernelFailure::ResourceExhausted) => {
            Some(CompileControlError::ResourceExhausted)
        }
        _ => None,
    }
}
fn prefixes(
    run: impl Fn(&Control) -> Result<PureCallSpecialization, FunctionSpecializationFailure>,
    success: bool,
) {
    let good = Control::default();
    assert_eq!(run(&good).is_ok(), success);
    let trace = good.trace.lock().unwrap().clone();
    assert!(!trace.is_empty());
    if success {
        assert!(
            trace.iter().any(|units| *units > 0),
            "successful work was not observed"
        );
    }
    for at in 0..trace.len() {
        for expected in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((at, expected.clone())),
            };
            assert_eq!(cause(run(&control).err().unwrap()), Some(expected));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}
#[test]
fn selected_fresh_original_controls_cover_success_and_pre_failure_prefixes() {
    let fixture = Fixture::new();
    prefixes(|control| fixture.prepare(control), true);
    prefixes(
        |control| {
            fixture.catalog.prepare_fresh_selected(
                fixture.input(),
                fixture.selected.clone(),
                PureCallPreparation::Table {
                    arguments: ScopedRelationalEffects::pure_value(
                        novarocks_type_contract::RelationEffectContext::call(
                            novarocks_type_contract::RelationCallId::new(700),
                        ),
                    ),
                },
                control,
            )
        },
        false,
    );
    let foreign = FunctionId::try_new("fixture/unregistered/function").unwrap();
    prefixes(
        |control| {
            let mut input = fixture.input();
            input.function_id = &foreign;
            fixture.catalog.prepare_fresh_selected(
                input,
                fixture.selected.clone(),
                fixture.options(),
                control,
            )
        },
        false,
    );
}

#[test]
fn sealed_frozen_route_keeps_frozen_facts_mandatory_and_never_retries_fresh() {
    let fixture = Fixture::new();
    let definition = fixture
        .catalog
        .definition("abs", FunctionKind::Scalar)
        .unwrap()
        .clone();
    let binding = definition.binding.as_ref().unwrap();
    // This local inventory comes from the original installed ABS attachment;
    // it makes no claim of an independently closed Server manifest.
    let installed = binding
        .pure
        .as_ref()
        .unwrap()
        .implementations
        .iter()
        .cloned()
        .map(|implementation| InstalledPureKernel {
            function: fixture.function.clone(),
            kind: FunctionKind::Scalar,
            implementation,
            aggregate_state_format: None,
        })
        .collect::<Vec<_>>();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    let sealed = builder.seal_pure(installed).unwrap();
    let fresh = sealed
        .prepare_fresh(
            fixture.input(),
            fixture.selected.clone(),
            fixture.options(),
            &Control::default(),
        )
        .unwrap();
    assert_eq!(fresh.source(), PurePreparationSource::Fresh);
    let frozen = fresh.call_contract().effects().clone();
    let prepared = sealed
        .prepare_frozen(
            fixture.input(),
            fixture.selected.clone(),
            &frozen,
            fixture.options(),
            &Control::default(),
        )
        .unwrap();
    assert_eq!(prepared.source(), PurePreparationSource::Frozen);
    assert!(Arc::ptr_eq(
        prepared.call_contract().selected_owner(),
        &fixture.selected
    ));
    let mut changed = frozen.clone();
    changed.value_stability = crate::FunctionVolatility::Volatile;
    let before = fixture.calls.load(Ordering::Relaxed);
    assert!(
        sealed
            .prepare_frozen(
                fixture.input(),
                fixture.selected.clone(),
                &changed,
                fixture.options(),
                &Control::default()
            )
            .is_err()
    );
    assert_eq!(
        fixture.calls.load(Ordering::Relaxed),
        before + 1,
        "frozen owner must not replay fresh"
    );
    prefixes(
        |control| {
            sealed.prepare_frozen(
                fixture.input(),
                fixture.selected.clone(),
                &frozen,
                fixture.options(),
                control,
            )
        },
        true,
    );
    prefixes(
        |control| {
            sealed.prepare_frozen(
                fixture.input(),
                fixture.selected.clone(),
                &changed,
                fixture.options(),
                control,
            )
        },
        false,
    );
}

#[test]
fn selected_fresh_wrong_abi_and_unknown_identity_keep_real_semantic_failure_without_new_tail() {
    let fixture = Fixture::new();
    let foreign = FunctionId::try_new("fixture/unregistered/first-cause").unwrap();
    for unknown in [false, true] {
        let run = |control: &Control| {
            let mut input = fixture.input();
            if unknown {
                input.function_id = &foreign;
            }
            let options = if unknown {
                fixture.options()
            } else {
                PureCallPreparation::Table {
                    arguments: ScopedRelationalEffects::pure_value(
                        novarocks_type_contract::RelationEffectContext::call(
                            novarocks_type_contract::RelationCallId::new(700),
                        ),
                    ),
                }
            };
            fixture.catalog.prepare_fresh_selected(
                input,
                fixture.selected.clone(),
                options,
                control,
            )
        };
        let baseline = Control::default();
        let error = run(&baseline).err().unwrap();
        if unknown {
            assert!(matches!(
                error,
                FunctionSpecializationFailure::Binding(FunctionBindingError::UnknownFunction)
            ));
        } else {
            assert!(matches!(
                error,
                FunctionSpecializationFailure::InvalidInput(_)
            ));
        }
        let trace = baseline.trace.lock().unwrap().clone();
        let source = CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        );
        let control = Control {
            trace: Mutex::default(),
            refusal: Some((trace.len(), source)),
        };
        let before = fixture.calls.load(Ordering::Relaxed);
        let owners = Arc::strong_count(&fixture.selected);
        let error = run(&control).err().unwrap();
        if unknown {
            assert!(matches!(
                error,
                FunctionSpecializationFailure::Binding(FunctionBindingError::UnknownFunction)
            ));
        } else {
            assert!(matches!(
                error,
                FunctionSpecializationFailure::InvalidInput(_)
            ));
        }
        assert_eq!(*control.trace.lock().unwrap(), trace);
        assert_eq!(fixture.calls.load(Ordering::Relaxed), before);
        assert_eq!(Arc::strong_count(&fixture.selected), owners);
    }
}

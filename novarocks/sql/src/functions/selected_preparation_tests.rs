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
use crate::compiler::SqlFunctionCatalog;
use novarocks_functions::{
    CallEffectInput, FunctionSpecializationFailure, PureCallPreparation, PureKernelAbi,
    PurePreparationSource, ScopedExpressionEffects,
};
use novarocks_type_contract::{
    CallProofScope, CompileControlError, CompilePhase, DecimalOverflowPolicy, EvaluationDemand,
    EvaluationDomainId, ExpressionEffectContext, ExpressionUseId, PureCompileControl,
    SemanticParameters,
};
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Mutex<Option<(usize, CompileControlError)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        let stop = self.stop.lock().unwrap().clone();
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = stop {
            assert!(at <= stop, "callback after original refusal");
        }
        trace.push((phase, units));
        match stop {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
impl Control {
    fn reset(&self, stop: Option<(usize, CompileControlError)>) {
        self.trace.lock().unwrap().clear();
        *self.stop.lock().unwrap() = stop;
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(u32::MAX),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    }
}
fn arguments() -> [FunctionArgument; 1] {
    [FunctionArgument::Value {
        value_type: FunctionValueType::new(
            DataType::List(Arc::new(
                arrow::datatypes::Field::new("authored item", DataType::Int64, false)
                    .with_metadata(std::collections::HashMap::<String, String>::from([(
                        "original".into(),
                        "preserved".into(),
                    )])),
            )),
            true,
        ),
        constant: None,
    }]
}
fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        expected_result_type: None,
        logical_argument_count: arguments.len(),
    }
}
fn input<'a>(
    bound: &'a ResolvedFunctionBinding,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    uses: &'a [Option<ExpressionUseId>],
    parameters: &'a SemanticParameters,
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: context(),
        argument_uses: novarocks_functions::CallArgumentUses::SelectedChannels(uses),
        function_id: &bound.function_id,
        kind: bound.kind,
        selected,
        request: request(arguments),
        environment: &[],
        parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Domain(context().domain),
    }
}
fn options() -> PureCallPreparation {
    PureCallPreparation::Scalar {
        arguments: ScopedExpressionEffects::pure_value(context()),
    }
}
fn assert_original_cause(error: &FunctionSpecializationFailure, cause: CompileControlError) {
    match error {
        FunctionSpecializationFailure::Control(actual) => assert_eq!(*actual, cause),
        FunctionSpecializationFailure::Kernel(actual) => assert!(matches!(
            (actual, cause),
            (
                novarocks_functions::KernelFailure::Cancelled,
                CompileControlError::Cancelled
            ) | (
                novarocks_functions::KernelFailure::DeadlineExceeded,
                CompileControlError::DeadlineExceeded
            ) | (
                novarocks_functions::KernelFailure::ResourceExhausted,
                CompileControlError::ResourceExhausted
            )
        )),
        error => panic!("original control cause was replaced: {error:?}"),
    }
}

#[test]
fn sql_snapshot_selected_port_preserves_actual_installed_owner_and_direct_trace() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let control = Control::default();
    let arguments = arguments();
    let bound =
        SqlFunctionCatalog::resolve_scalar_binding(&catalog, "cardinality", &arguments, &control)
            .unwrap();
    let snapshot = catalog.snapshot();
    let selected = Arc::new(bound.selected.clone());
    let uses = [Some(ExpressionUseId::new(0))];
    let parameters = SemanticParameters::try_new([]).unwrap();
    let call = input(&bound, &selected, &arguments, &uses, &parameters);
    control.reset(None);
    let direct = EngineFunctionCatalog::prepare_fresh_selected(
        &catalog,
        call,
        selected.clone(),
        options(),
        &control,
    )
    .unwrap();
    let original_trace = control.trace();
    control.reset(None);
    let projected = snapshot
        .prepare_fresh_selected(call, selected.clone(), options(), &control)
        .unwrap();
    assert_eq!(control.trace(), original_trace);
    assert_eq!(projected.source(), PurePreparationSource::Fresh);
    assert_eq!(projected.implementation(), direct.implementation());
    assert_eq!(projected.implementation().abi, PureKernelAbi::ScalarV1);
    assert_eq!(projected.call_contract(), direct.call_contract());
    assert!(std::ptr::eq(
        projected.call_contract().selected(),
        selected.as_ref()
    ));
    assert_eq!(
        projected.call_contract().effects().proof_scope,
        call.proof_scope
    );
    assert!(projected.call_contract().effects().environment.is_empty());
}

#[test]
fn sql_selected_port_rejects_substituted_owner_full_type_and_prepares_table_lifecycle() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let control = Control::default();
    let arguments = arguments();
    let bound =
        SqlFunctionCatalog::resolve_scalar_binding(&catalog, "cardinality", &arguments, &control)
            .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let uses = [Some(ExpressionUseId::new(0))];
    let parameters = SemanticParameters::try_new([]).unwrap();
    let call = input(&bound, &selected, &arguments, &uses, &parameters);
    let equal_foreign = Arc::new(selected.as_ref().clone());
    assert!(matches!(
        catalog
            .snapshot()
            .prepare_fresh_selected(call, equal_foreign, options(), &control),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    let mut wrong_type = selected.as_ref().clone();
    let mut wrong_arguments = wrong_type.argument_types.to_vec();
    let FunctionArgumentType::Value(value) = &mut wrong_arguments[0] else {
        panic!("value channel")
    };
    *value = value.with_nullable(false).unwrap();
    wrong_type.argument_types = novarocks_type_contract::TypedFactSlice::from_pure_boxed(
        wrong_arguments.into_boxed_slice(),
    );
    let wrong_type = Arc::new(wrong_type);
    let wrong_call = input(&bound, &wrong_type, &arguments, &uses, &parameters);
    assert!(matches!(
        catalog.snapshot().prepare_fresh_selected(
            wrong_call,
            wrong_type.clone(),
            options(),
            &control
        ),
        Err(FunctionSpecializationFailure::Binding(_))
    ));
    let table = catalog
        .resolve_table_binding("unnest", &arguments, &control)
        .unwrap();
    let table_selected = Arc::new(table.selected.clone());
    let table_input = input(&table, &table_selected, &arguments, &uses, &parameters);
    let prepared = catalog
        .snapshot()
        .prepare_fresh_selected(
            table_input,
            table_selected.clone(),
            PureCallPreparation::Table {
                arguments: ScopedExpressionEffects::pure_value(context()),
            },
            &control,
        )
        .unwrap();
    assert_eq!(prepared.implementation().abi, PureKernelAbi::TableV1);
    assert!(std::ptr::eq(
        prepared.call_contract().selected(),
        table_selected.as_ref()
    ));
}

#[test]
fn sql_selected_port_checks_exact_result_environment_and_lifecycle_without_rebinding() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let control = Control::default();
    let arguments = arguments();
    let bound =
        SqlFunctionCatalog::resolve_scalar_binding(&catalog, "cardinality", &arguments, &control)
            .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let uses = [Some(ExpressionUseId::new(0))];
    let parameters = SemanticParameters::try_new([]).unwrap();
    let snapshot = catalog.snapshot();
    let call = input(&bound, &selected, &arguments, &uses, &parameters);
    let mut variants = Vec::new();
    let mut unknown = selected.as_ref().clone();
    unknown.overload =
        novarocks_functions::FunctionOverloadId::try_new("unknown-selected-overload").unwrap();
    variants.push(unknown);
    let mut wrong_result = selected.as_ref().clone();
    let FunctionResultType::Scalar(actual_result) = &selected.result_type else {
        panic!("cardinality scalar result")
    };
    let different_width = if actual_result.data_type() == &DataType::Int32 {
        DataType::Int64
    } else {
        DataType::Int32
    };
    wrong_result.result_type = FunctionResultType::Scalar(FunctionValueType::new(
        different_width,
        actual_result.nullable(),
    ));
    variants.push(wrong_result);
    let mut wrong_field = selected.as_ref().clone();
    let mut wrong_arguments = wrong_field.argument_types.to_vec();
    let FunctionArgumentType::Value(value) = &mut wrong_arguments[0] else {
        panic!("value channel")
    };
    let actual_child_nullable = value.child_at(0).unwrap().nullable();
    *value = novarocks_type_contract::FunctionValueType::try_with_logical_type(
        DataType::List(Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Int64,
            !actual_child_nullable,
        ))),
        value.nullable(),
        value.logical_type(),
    )
    .unwrap();
    wrong_field.argument_types = novarocks_type_contract::TypedFactSlice::from_pure_boxed(
        wrong_arguments.into_boxed_slice(),
    );
    variants.push(wrong_field);
    for (index, variant) in variants.into_iter().enumerate() {
        let variant = Arc::new(variant);
        let wrong_call = input(&bound, &variant, &arguments, &uses, &parameters);
        let failure = snapshot
            .prepare_fresh_selected(wrong_call, variant.clone(), options(), &control)
            .unwrap_err();
        assert!(
            matches!(failure, FunctionSpecializationFailure::Binding(_)),
            "variant {index}: {failure:?}"
        );
    }
    let parameter_id = novarocks_type_contract::SemanticParameterId::new(0);
    let parameters = SemanticParameters::try_new([(
        parameter_id,
        novarocks_type_contract::SemanticParameterValue::AllowThrowException(true),
    )])
    .unwrap();
    let environment = [novarocks_type_contract::SemanticParameterRef {
        id: parameter_id,
        expected_key: novarocks_type_contract::SemanticParameterKey::AllowThrowException,
    }];
    let mut environment_call = call;
    environment_call.environment = &environment;
    environment_call.parameters = &parameters;
    assert!(matches!(
        snapshot.prepare_fresh_selected(environment_call, selected.clone(), options(), &control),
        Err(FunctionSpecializationFailure::InvalidInput(
            "call effect environment is not an exact frozen declared dependency"
        ))
    ));
    assert!(matches!(
        snapshot.prepare_fresh_selected(
            call,
            selected.clone(),
            PureCallPreparation::Table {
                arguments: ScopedExpressionEffects::pure_value(context())
            },
            &control
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    let mut wrong_kind = call;
    wrong_kind.kind = FunctionKind::Table;
    assert!(matches!(
        snapshot.prepare_fresh_selected(wrong_kind, selected.clone(), options(), &control),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
}

#[test]
fn sql_selected_port_preserves_every_original_compile_failure_prefix() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let control = Control::default();
    let arguments = arguments();
    let bound =
        SqlFunctionCatalog::resolve_scalar_binding(&catalog, "cardinality", &arguments, &control)
            .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let uses = [Some(ExpressionUseId::new(0))];
    let parameters = SemanticParameters::try_new([]).unwrap();
    let call = input(&bound, &selected, &arguments, &uses, &parameters);
    let snapshot = catalog.snapshot();
    control.reset(None);
    snapshot
        .prepare_fresh_selected(call, selected.clone(), options(), &control)
        .unwrap();
    let trace = control.trace();
    assert!(!trace.is_empty());
    for at in 0..trace.len() {
        for cause in CAUSES {
            control.reset(Some((at, cause.clone())));
            let direct = EngineFunctionCatalog::prepare_fresh_selected(
                &catalog,
                call,
                selected.clone(),
                options(),
                &control,
            )
            .unwrap_err();
            assert_original_cause(&direct, cause.clone());
            assert_eq!(control.trace(), trace[..=at]);
            control.reset(Some((at, cause.clone())));
            let projected = snapshot
                .prepare_fresh_selected(call, selected.clone(), options(), &control)
                .unwrap_err();
            assert_original_cause(&projected, cause);
            assert_eq!(
                std::mem::discriminant(&projected),
                std::mem::discriminant(&direct)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

#[derive(Debug, Clone)]
struct DeclarationOnly;

#[test]
fn sql_missing_selected_implementation_keeps_ordinary_tail_and_every_control_prefix() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let control = Control::default();
    let arguments = [];
    let bound =
        SqlFunctionCatalog::resolve_scalar_binding(&catalog, "uuid", &arguments, &control).unwrap();
    let selected = Arc::new(bound.selected.clone());
    let uses = [];
    let parameters = SemanticParameters::try_new([]).unwrap();
    let call = input(&bound, &selected, &arguments, &uses, &parameters);
    let options = || PureCallPreparation::Scalar {
        arguments: ScopedExpressionEffects::pure_value(context()),
    };
    let snapshot = catalog.snapshot();
    control.reset(None);
    let direct = EngineFunctionCatalog::prepare_fresh_selected(
        &catalog,
        call,
        selected.clone(),
        options(),
        &control,
    )
    .unwrap_err();
    let trace = control.trace();
    assert!(matches!(
        direct,
        FunctionSpecializationFailure::MissingPureImplementation(_)
    ));
    // Missing ownership is already an authored error. A later optional
    // checkpoint must not replace that exact selected-overload refusal.
    assert_eq!(
        trace.first(),
        Some(&(CompilePhase::FunctionSpecialization, 0))
    );
    for cause in CAUSES {
        control.reset(Some((trace.len(), cause.clone())));
        let original = EngineFunctionCatalog::prepare_fresh_selected(
            &catalog,
            call,
            selected.clone(),
            options(),
            &control,
        )
        .unwrap_err();
        assert!(matches!(original,
            FunctionSpecializationFailure::MissingPureImplementation(ref id)
                if id == &selected.overload));
        assert_eq!(control.trace(), trace);
        control.reset(Some((trace.len(), cause)));
        let original = snapshot
            .prepare_fresh_selected(call, selected.clone(), options(), &control)
            .unwrap_err();
        assert!(matches!(original,
            FunctionSpecializationFailure::MissingPureImplementation(ref id)
                if id == &selected.overload));
        assert_eq!(control.trace(), trace);
    }
    control.reset(None);
    let projected = snapshot
        .prepare_fresh_selected(call, selected.clone(), options(), &control)
        .unwrap_err();
    assert_eq!(projected.to_string(), direct.to_string());
    assert_eq!(control.trace(), trace);
    for at in 0..trace.len() {
        for cause in CAUSES {
            control.reset(Some((at, cause.clone())));
            let direct = EngineFunctionCatalog::prepare_fresh_selected(
                &catalog,
                call,
                selected.clone(),
                options(),
                &control,
            )
            .unwrap_err();
            assert_original_cause(&direct, cause.clone());
            assert_eq!(control.trace(), trace[..=at]);
            control.reset(Some((at, cause.clone())));
            let projected = snapshot
                .prepare_fresh_selected(call, selected.clone(), options(), &control)
                .unwrap_err();
            assert_original_cause(&projected, cause);
            assert_eq!(
                std::mem::discriminant(&projected),
                std::mem::discriminant(&direct)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}

impl SqlFunctionCatalog for DeclarationOnly {
    fn resolve_aggregate_trusted(
        &self,
        _: &str,
        _: &[DataType],
        _: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        panic!("selected preparation must not resolve a SQL name")
    }
    fn volatility(&self, _: &str) -> novarocks_functions::FunctionVolatility {
        panic!("selected preparation must not infer effects from legacy volatility")
    }
    fn snapshot(&self) -> Arc<dyn SqlFunctionCatalog> {
        Arc::new(self.clone())
    }
    fn resolve_scalar_signature(
        &self,
        _: &str,
        _: &[DataType],
        _: &dyn PureCompileControl,
    ) -> Result<ResolvedScalarFunction, ResolveError> {
        panic!("selected preparation must not resolve a SQL name")
    }
    fn contains_aggregate(&self, _: &str) -> bool {
        panic!("selected preparation must not inspect ambient names")
    }
    fn resolve_aggregate_signature(
        &self,
        _: &str,
        _: &[DataType],
        _: &dyn PureCompileControl,
    ) -> Result<ResolvedAggregateSignature, FunctionResolutionError> {
        panic!("selected preparation must not resolve a SQL name")
    }
}

#[test]
fn sql_declaration_only_snapshot_refuses_without_fabricating_effects() {
    let catalog = build_builtin_engine_function_catalog().unwrap();
    let control = Control::default();
    let arguments = arguments();
    let bound =
        SqlFunctionCatalog::resolve_scalar_binding(&catalog, "cardinality", &arguments, &control)
            .unwrap();
    let selected = Arc::new(bound.selected.clone());
    let uses = [Some(ExpressionUseId::new(0))];
    let parameters = SemanticParameters::try_new([]).unwrap();
    let call = input(&bound, &selected, &arguments, &uses, &parameters);
    control.reset(None);
    assert!(matches!(
        DeclarationOnly.snapshot().prepare_fresh_selected(
            call,
            selected.clone(),
            options(),
            &control
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    assert_eq!(control.trace(), [(CompilePhase::FunctionSpecialization, 0)]);
    for cause in CAUSES {
        control.reset(Some((0, cause.clone())));
        assert!(
            matches!(DeclarationOnly.snapshot().prepare_fresh_selected(call, selected.clone(), options(), &control), Err(FunctionSpecializationFailure::Control(actual)) if actual == cause)
        );
        assert_eq!(control.trace(), [(CompilePhase::FunctionSpecialization, 0)]);
    }
}

#[test]
fn sql_selected_snapshot_prepares_real_window_and_aggregate_owners_without_an_adapter_meter() {
    use novarocks_functions::{
        AggregateKernelPhase, AggregatePreparationOptions, AggregateWindowPreparationOptions,
        WindowCallOptions,
    };

    let catalog = build_builtin_engine_function_catalog().unwrap();
    let snapshot = catalog.snapshot();
    let control = Control::default();
    let arguments = [];
    let uses = [];
    let parameters = SemanticParameters::try_new([]).unwrap();
    for (name, kind, options, abi) in [
        (
            "count",
            FunctionKind::Aggregate,
            PureCallPreparation::Aggregate {
                arguments: ScopedExpressionEffects::pure_value(context()),
                options: AggregatePreparationOptions {
                    state_interpretation: None,
                    phase: AggregateKernelPhase::Single,
                    distinct: false,
                    order_keys: Arc::new([]),
                    state_input_type: None,
                },
            },
            PureKernelAbi::AggregateWindowV1,
        ),
        (
            "count",
            FunctionKind::Aggregate,
            PureCallPreparation::AggregateWindow {
                arguments: ScopedExpressionEffects::pure_value(context()),
                options: AggregateWindowPreparationOptions {
                    aggregate: AggregatePreparationOptions {
                        state_interpretation: None,
                        phase: AggregateKernelPhase::Single,
                        distinct: false,
                        order_keys: Arc::new([]),
                        state_input_type: None,
                    },
                    window: WindowCallOptions::try_new(None, false, &control).unwrap(),
                },
            },
            PureKernelAbi::AggregateWindowV1,
        ),
        (
            "row_number",
            FunctionKind::Window,
            PureCallPreparation::Window {
                arguments: ScopedExpressionEffects::pure_value(context()),
                options: WindowCallOptions::try_new(None, false, &control).unwrap(),
            },
            PureKernelAbi::WindowV1,
        ),
    ] {
        let bound = catalog
            .resolve_bound_user(name, kind, request(&arguments), &control)
            .unwrap();
        let selected = Arc::new(bound.selected.clone());
        let call = input(&bound, &selected, &arguments, &uses, &parameters);
        control.reset(None);
        let direct = EngineFunctionCatalog::prepare_fresh_selected(
            &catalog,
            call,
            selected.clone(),
            options.clone(),
            &control,
        )
        .unwrap();
        let trace = control.trace();
        control.reset(None);
        let projected = snapshot
            .prepare_fresh_selected(call, selected.clone(), options, &control)
            .unwrap();
        assert_eq!(projected.implementation().abi, abi);
        assert_eq!(projected.call_contract(), direct.call_contract());
        assert_eq!(control.trace(), trace);
        assert!(std::ptr::eq(
            projected.call_contract().selected(),
            selected.as_ref()
        ));
        assert_eq!(
            projected.call_contract().effects().own_row_error,
            novarocks_functions::FunctionIntrinsicRowError::NotRowEvaluated
        );
    }
}

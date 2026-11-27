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
use novarocks_type_contract::{
    CompileControlError, FunctionFailureBehavior, FunctionIntrinsicRowError, FunctionVolatility,
};
use std::sync::Mutex;

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct DeclarationControl {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for DeclarationControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(at <= stop, "callback after the first declaration refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn catalog() -> EngineFunctionCatalog {
    crate::builtin::catalogue::build_builtin_engine_function_catalog().unwrap()
}
fn declaration<'a>(
    catalog: &'a EngineFunctionCatalog,
    name: &str,
    kind: FunctionKind,
) -> &'a FunctionBindingDeclaration {
    catalog
        .definition(name, kind)
        .unwrap()
        .binding_declaration()
        .unwrap()
}
fn expected(name: &str) -> (FunctionKind, PureKernelAbi, FunctionEffectDeclaration) {
    let (kind, abi, row_error, null, control, state) = match name {
        "lower" => (
            FunctionKind::Scalar,
            PureKernelAbi::ScalarV1,
            FunctionIntrinsicRowError::NoRowError,
            FunctionNullBehavior::Strict,
            ArgumentControl::OrderedNull,
            FunctionInstanceState::None,
        ),
        "if" => (
            FunctionKind::Scalar,
            PureKernelAbi::ControlIntrinsicV1,
            FunctionIntrinsicRowError::NoRowError,
            FunctionNullBehavior::ControlDefined,
            ArgumentControl::If,
            FunctionInstanceState::None,
        ),
        "coalesce" => (
            FunctionKind::Scalar,
            PureKernelAbi::ControlIntrinsicV1,
            FunctionIntrinsicRowError::NoRowError,
            FunctionNullBehavior::ControlDefined,
            ArgumentControl::Coalesce,
            FunctionInstanceState::None,
        ),
        "count" => (
            FunctionKind::Aggregate,
            PureKernelAbi::AggregateWindowV1,
            FunctionIntrinsicRowError::NotRowEvaluated,
            FunctionNullBehavior::CalledOnNull,
            ArgumentControl::Aggregate,
            FunctionInstanceState::AggregateInstance,
        ),
        "row_number" => (
            FunctionKind::Window,
            PureKernelAbi::WindowV1,
            FunctionIntrinsicRowError::NotRowEvaluated,
            FunctionNullBehavior::CalledOnNull,
            ArgumentControl::Window,
            FunctionInstanceState::WindowPartition,
        ),
        "unnest" => (
            FunctionKind::Table,
            PureKernelAbi::TableV1,
            FunctionIntrinsicRowError::NoRowError,
            FunctionNullBehavior::CalledOnNull,
            ArgumentControl::Table,
            FunctionInstanceState::TableInstance,
        ),
        _ => unreachable!(),
    };
    (
        kind,
        abi,
        FunctionEffectDeclaration {
            value_stability: FunctionVolatility::Immutable,
            own_row_error: row_error,
            failure_behavior: FunctionFailureBehavior::Propagate,
            null_behavior: null,
            argument_control: control,
            instance_state: state,
            observable_effects: ObservableEffects::NONE,
            environment_dependencies: Box::default(),
        },
    )
}
fn prefixes(
    call: impl Fn(&DeclarationControl) -> Result<(), FunctionSpecializationFailure>,
    succeeds: bool,
) {
    let baseline = DeclarationControl::default();
    let original = call(&baseline);
    assert_eq!(original.is_ok(), succeeds);
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&0));
    if succeeds {
        assert!(trace.len() >= 2);
        assert!(trace.iter().any(|units| *units > 0));
    } else {
        // A semantic refusal can precede the first flush. Do not manufacture
        // an outer footer; an armed later callback must not replace the cause.
        for cause in CAUSES {
            let control = DeclarationControl {
                trace: Mutex::new(vec![]),
                refusal: Some((trace.len(), cause)),
            };
            match (call(&control).unwrap_err(), original.as_ref().unwrap_err()) {
                (
                    FunctionSpecializationFailure::Binding(actual),
                    FunctionSpecializationFailure::Binding(expected),
                ) => assert_eq!(&actual, expected),
                (
                    FunctionSpecializationFailure::MissingPureImplementation(actual),
                    FunctionSpecializationFailure::MissingPureImplementation(expected),
                ) => assert_eq!(&actual, expected),
                (
                    FunctionSpecializationFailure::InvalidInput(actual),
                    FunctionSpecializationFailure::InvalidInput(expected),
                ) => assert_eq!(&actual, expected),
                (actual, expected) => panic!(
                    "different ordinary declaration cause: {actual:?}, expected {expected:?}"
                ),
            }
            assert_eq!(*control.trace.lock().unwrap(), trace);
        }
    }
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = DeclarationControl {
                trace: Mutex::new(vec![]),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(call(&control), Err(FunctionSpecializationFailure::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
        }
    }
}

#[test]
fn pure_declaration_actual_installed_kinds_have_independent_abi_and_complete_base_effects() {
    let catalog = catalog();
    for name in ["lower", "if", "coalesce", "count", "row_number", "unnest"] {
        let (kind, abi, effects) = expected(name);
        let declaration = declaration(&catalog, name, kind);
        assert!(!declaration.overloads().is_empty());
        for overload in declaration.overloads() {
            let token = catalog
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    kind,
                    &overload.identity,
                    &DeclarationControl::default(),
                )
                .unwrap();
            assert_eq!(token.implementation().overload, overload.identity);
            assert_eq!(token.implementation().abi, abi);
            assert_eq!(token.effects(), &effects);
            // This query takes no selected argument types, invocation context,
            // frame or policy: these are base declarations, not a prepared call.
        }
    }
}

#[test]
fn pure_declaration_borrows_original_frozen_records_stably_without_new_effect_backing() {
    let catalog = catalog();
    for name in ["lower", "if", "coalesce", "count", "row_number", "unnest"] {
        let (kind, _, _) = expected(name);
        let definition = catalog.definition(name, kind).unwrap();
        let binding = definition.binding.as_ref().unwrap();
        let attachment = binding.pure.as_ref().unwrap();
        for overload in binding.declaration.overloads() {
            let implementation = attachment
                .implementations
                .iter()
                .find(|implementation| implementation.overload == overload.identity)
                .unwrap();
            let control = DeclarationControl::default();
            let first = catalog
                .pure_overload_declaration_observed(
                    binding.declaration.function_id(),
                    kind,
                    &overload.identity,
                    &control,
                )
                .unwrap();
            let second = catalog
                .pure_overload_declaration_observed(
                    binding.declaration.function_id(),
                    kind,
                    &overload.identity,
                    &control,
                )
                .unwrap();
            assert!(std::ptr::eq(first.implementation(), implementation));
            assert!(std::ptr::eq(
                second.implementation(),
                first.implementation()
            ));
            assert!(std::ptr::eq(
                first.effects(),
                overload.effects.as_ref().unwrap()
            ));
            assert!(std::ptr::eq(second.effects(), first.effects()));
        }
    }
}

fn metadata_only(catalog: &EngineFunctionCatalog) -> EngineFunctionCatalog {
    let mut definition = catalog
        .definition("lower", FunctionKind::Scalar)
        .unwrap()
        .clone();
    // Keep the exact original complete binding/resolver but remove CPU attachment.
    // This models the metadata-only registration surface, not a fake pure owner.
    definition.binding.as_mut().unwrap().pure = None;
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    builder.seal().unwrap()
}

#[test]
fn pure_declaration_unknown_identity_overload_wrong_kind_and_metadata_only_refuse_exactly() {
    let catalog = catalog();
    let lower = declaration(&catalog, "lower", FunctionKind::Scalar);
    let overload = &lower.overloads()[0].identity;
    let unknown_id = FunctionId::try_new("fixture/uninstalled/exact-id").unwrap();
    let unknown_overload =
        FunctionOverloadId::try_new("fixture/uninstalled/exact-overload").unwrap();
    assert!(matches!(
        catalog.pure_overload_declaration_observed(
            &unknown_id,
            FunctionKind::Scalar,
            overload,
            &DeclarationControl::default()
        ),
        Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction
        ))
    ));
    assert!(matches!(
        catalog.pure_overload_declaration_observed(
            lower.function_id(),
            FunctionKind::Scalar,
            &unknown_overload,
            &DeclarationControl::default()
        ),
        Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownOverload(actual)
        )) if actual == unknown_overload
    ));
    for wrong_kind in [
        FunctionKind::Aggregate,
        FunctionKind::Window,
        FunctionKind::Table,
    ] {
        assert!(matches!(
            catalog.pure_overload_declaration_observed(
                lower.function_id(),
                wrong_kind,
                overload,
                &DeclarationControl::default()
            ),
            Err(FunctionSpecializationFailure::InvalidInput(
                "pure preparation has a different exact kind or selected owner"
            ))
        ));
    }
    let window_overload =
        &declaration(&catalog, "row_number", FunctionKind::Window).overloads()[0].identity;
    assert!(matches!(
        catalog.pure_overload_declaration_observed(
            lower.function_id(),
            FunctionKind::Scalar,
            window_overload,
            &DeclarationControl::default()
        ),
        Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownOverload(actual)
        )) if &actual == window_overload
    ));
    let metadata = metadata_only(&catalog);
    assert_eq!(declaration(&metadata, "lower", FunctionKind::Scalar), lower);
    assert!(matches!(
        metadata.pure_overload_declaration_observed(
            lower.function_id(),
            FunctionKind::Scalar,
            overload,
            &DeclarationControl::default()
        ),
        Err(FunctionSpecializationFailure::MissingPureImplementation(_))
    ));
    let name_as_id = FunctionId::try_new("lower").unwrap();
    assert!(matches!(
        catalog.pure_overload_declaration_observed(
            &name_as_id,
            FunctionKind::Scalar,
            overload,
            &DeclarationControl::default()
        ),
        Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction
        ))
    ));
}

#[test]
fn pure_declaration_each_actual_small_success_and_ordinary_callback_preserves_all_three_causes() {
    let catalog = catalog();
    for name in ["lower", "if", "coalesce", "count", "row_number", "unnest"] {
        let (kind, _, _) = expected(name);
        let declaration = declaration(&catalog, name, kind);
        for overload in declaration.overloads() {
            prefixes(
                |control| {
                    catalog
                        .pure_overload_declaration_observed(
                            declaration.function_id(),
                            kind,
                            &overload.identity,
                            control,
                        )
                        .map(|_| ())
                },
                true,
            );
        }
    }
    let lower = declaration(&catalog, "lower", FunctionKind::Scalar);
    let overload = &lower.overloads()[0].identity;
    let unknown_id = FunctionId::try_new("fixture/uninstalled/exact-id").unwrap();
    let unknown_overload =
        FunctionOverloadId::try_new("fixture/uninstalled/exact-overload").unwrap();
    prefixes(
        |control| {
            catalog
                .pure_overload_declaration_observed(
                    &unknown_id,
                    FunctionKind::Scalar,
                    overload,
                    control,
                )
                .map(|_| ())
        },
        false,
    );
    prefixes(
        |control| {
            catalog
                .pure_overload_declaration_observed(
                    lower.function_id(),
                    FunctionKind::Scalar,
                    &unknown_overload,
                    control,
                )
                .map(|_| ())
        },
        false,
    );
    prefixes(
        |control| {
            catalog
                .pure_overload_declaration_observed(
                    lower.function_id(),
                    FunctionKind::Aggregate,
                    overload,
                    control,
                )
                .map(|_| ())
        },
        false,
    );
    let metadata = metadata_only(&catalog);
    prefixes(
        |control| {
            metadata
                .pure_overload_declaration_observed(
                    lower.function_id(),
                    FunctionKind::Scalar,
                    overload,
                    control,
                )
                .map(|_| ())
        },
        false,
    );
    let ordinary = DeclarationControl::default();
    assert!(
        catalog
            .pure_overload_declaration_observed(
                lower.function_id(),
                FunctionKind::Scalar,
                &unknown_overload,
                &ordinary
            )
            .is_err()
    );
    // This exact unknown overload is rejected before the bounded lookup flush.
    assert_eq!(*ordinary.trace.lock().unwrap(), [0]);
}

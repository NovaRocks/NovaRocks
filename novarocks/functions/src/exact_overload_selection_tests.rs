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
use arrow_array::StringArray;
use novarocks_type_contract::{
    CompileControlError, CompilePhase, FunctionInstanceState, FunctionNullBehavior,
    PureCompileControl,
};
use std::{collections::HashMap, sync::Mutex};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
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
            assert!(at <= stop, "no callback after first refusal");
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn catalog() -> EngineFunctionCatalog {
    builtin::catalogue::build_builtin_engine_function_catalog().unwrap()
}
fn text_constant(text: &str) -> ConstantValue {
    let ty = FunctionValueType::new(DataType::Utf8, false);
    ConstantValue::from_utf8(
        Arc::new(Field::new("ordered_constant", DataType::Utf8, false)),
        ty,
        text,
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 16,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 16,
            max_type_nodes: 64,
            max_dictionary_depth: 8,
            max_metadata_bytes: 65536,
            max_library_validation_work: 4 << 20,
            max_library_validation_bytes: 4 << 20,
        },
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap()
}
fn value(nullable: bool) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: FunctionValueType::new(DataType::Utf8, nullable),
        constant: None,
    }
}
fn request(arguments: &[FunctionArgument]) -> FunctionBindingRequest<'_> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: arguments.len(),
        expected_result_type: None,
    }
}
fn original(catalog: &EngineFunctionCatalog) -> ResolvedFunctionBinding {
    catalog
        .resolve_bound_user(
            "lower",
            FunctionKind::Scalar,
            request(&[value(false)]),
            &Control::default(),
        )
        .unwrap()
}
fn select(
    catalog: &EngineFunctionCatalog,
    bound: &ResolvedFunctionBinding,
    request: FunctionBindingRequest<'_>,
    control: &Control,
) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError> {
    catalog.select_exact_overload_observed(
        &bound.function_id,
        bound.kind,
        &bound.selected.overload,
        request,
        control,
    )
}
fn prefixes(call: impl Fn(&Control) -> Result<(), FunctionBindingError>, success: bool) {
    let baseline = Control::default();
    let original = call(&baseline);
    assert_eq!(original.is_ok(), success);
    let trace = baseline.trace.lock().unwrap().clone();
    assert_eq!(trace.first(), Some(&0));
    if success {
        assert!(trace.len() >= 2);
        assert!(trace.iter().any(|units| *units > 0));
    } else {
        // A semantic refusal can precede the first flush. Do not manufacture
        // an outer footer; an armed later callback must not replace the cause.
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((trace.len(), cause)),
            };
            assert_eq!(call(&control).unwrap_err(), *original.as_ref().unwrap_err());
            assert_eq!(*control.trace.lock().unwrap(), trace);
        }
    }
    for stop in 0..trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(call(&control), Err(FunctionBindingError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
}

#[test]
fn exact_overload_lower_reselects_late_nullable_types_without_changing_identity() {
    let catalog = catalog();
    let bound = original(&catalog);
    assert_eq!(
        bound.selected.argument_types.as_ref(),
        &[FunctionArgumentType::Value(FunctionValueType::new(
            DataType::Utf8,
            false
        ))]
    );
    assert_eq!(
        bound.selected.result_type,
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, false))
    );
    let late = [value(true)];
    assert!(
        catalog
            .validate_frozen_selection(
                &bound.function_id,
                bound.kind,
                &bound.selected,
                request(&late),
                &Control::default()
            )
            .is_err()
    );
    let selected = select(&catalog, &bound, request(&late), &Control::default()).unwrap();
    assert_eq!(selected.overload, bound.selected.overload);
    assert_eq!(
        selected.argument_types.as_ref(),
        &[FunctionArgumentType::Value(FunctionValueType::new(
            DataType::Utf8,
            true
        ))]
    );
    assert_eq!(
        selected.result_type,
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, true))
    );
    assert!(selected.aggregate.is_none());
    catalog
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &selected,
            request(&late),
            &Control::default(),
        )
        .unwrap();
    let declaration = catalog
        .pure_overload_declaration_observed(
            &bound.function_id,
            bound.kind,
            &selected.overload,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(declaration.implementation().abi, PureKernelAbi::ScalarV1);
    assert_eq!(
        declaration.effects().null_behavior,
        FunctionNullBehavior::Strict
    );
    assert_eq!(
        declaration.effects().instance_state,
        FunctionInstanceState::None
    );
}

#[test]
fn exact_overload_constant_request_preserves_original_selected_ordinal_field_and_backing() {
    let catalog = catalog();
    let bound = original(&catalog);
    let ty = FunctionValueType::new(DataType::Utf8, false);
    let field = Arc::new(
        Field::new("original_source", DataType::Utf8, false).with_metadata(HashMap::from([(
            "provider.unknown".into(),
            "full-source-field".into(),
        )])),
    );
    let pool = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        StringArray::from(vec!["UNUSED_SENTINEL", "ΟΣİß", "unused-tail"]).to_data(),
        ConstantPolicy {
            max_rows: 8,
            max_array_nodes: 16,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 16,
            max_type_nodes: 64,
            max_dictionary_depth: 8,
            max_metadata_bytes: 65536,
            max_library_validation_work: 4 << 20,
            max_library_validation_bytes: 4 << 20,
        },
        CompilePhase::FunctionSpecialization,
        &Control::default(),
    )
    .unwrap();
    let original = pool.value(1).unwrap();
    let backing = original.pool().backing_identity();
    let args = [FunctionArgument::Value {
        value_type: ty.clone(),
        constant: Some(original.clone()),
    }];
    let selected = select(&catalog, &bound, request(&args), &Control::default()).unwrap();
    assert_eq!(
        selected.argument_types.as_ref(),
        &[FunctionArgumentType::Value(ty.clone())]
    );
    catalog
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &selected,
            request(&args),
            &Control::default(),
        )
        .unwrap();
    let FunctionArgument::Value {
        value_type,
        constant: Some(retained),
    } = &args[0]
    else {
        unreachable!()
    };
    assert_eq!(value_type, &ty);
    assert_eq!(retained.ordinal(), 1);
    assert_eq!(retained.try_utf8().unwrap(), Some("ΟΣİß"));
    assert_eq!(retained.pool().backing_identity(), backing);
    assert!(Arc::ptr_eq(retained.pool().field_ref(), &field));
    assert_eq!(
        retained.pool().field_ref().metadata()["provider.unknown"],
        "full-source-field"
    );
    // Selection borrows the request; it is neither a new CV nor provenance.
}

#[test]
fn exact_overload_identity_kind_result_and_domain_failures_remain_explicit() {
    let catalog = catalog();
    let bound = original(&catalog);
    let args = [value(false)];
    assert!(matches!(
        catalog.select_exact_overload_observed(
            &FunctionId::try_new("fixture/unknown/v1").unwrap(),
            bound.kind,
            &bound.selected.overload,
            request(&args),
            &Control::default()
        ),
        Err(FunctionBindingError::UnknownFunction)
    ));
    assert!(matches!(
        catalog.select_exact_overload_observed(
            &bound.function_id,
            FunctionKind::Window,
            &bound.selected.overload,
            request(&args),
            &Control::default()
        ),
        Err(FunctionBindingError::InvalidBinding(_))
    ));
    let unknown = FunctionOverloadId::try_new("fixture/unknown-overload/v1").unwrap();
    assert!(
        matches!(catalog.select_exact_overload_observed(&bound.function_id,bound.kind,&unknown,request(&args),&Control::default()), Err(FunctionBindingError::InvalidBinding(ref message)) if message.as_ref() == "exact selection overload is not declared by this function")
    );
    let expected = FunctionValueType::new(DataType::Int64, false);
    assert!(
        select(
            &catalog,
            &bound,
            FunctionBindingRequest {
                expected_result_type: Some(&expected),
                ..request(&args)
            },
            &Control::default()
        )
        .is_err()
    );
    let nominal = [FunctionArgument::Value {
        value_type: FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            novarocks_type_contract::ValueLogicalType::Json,
        )
        .unwrap(),
        constant: None,
    }];
    assert!(select(&catalog, &bound, request(&nominal), &Control::default()).is_err());
}

#[test]
fn exact_overload_default_resolver_refuses_without_name_resolution_fallback() {
    struct DeclarationOnlyResolver;
    impl FunctionBindingResolver for DeclarationOnlyResolver {
        fn resolve(
            &self,
            _: FunctionBindingRequest<'_>,
            _: &dyn PureCompileControl,
        ) -> Result<FunctionBindingSelection, FunctionBindingError> {
            panic!("exact selection must not resolve by name")
        }
        fn validate_selected(
            &self,
            _: &FunctionBindingSelection,
            _: FunctionBindingRequest<'_>,
            _: &dyn PureCompileControl,
        ) -> Result<(), FunctionBindingError> {
            panic!("no selected implementation is authored")
        }
    }
    let actual = catalog();
    let bound = original(&actual);
    let declaration = actual
        .definition("lower", FunctionKind::Scalar)
        .unwrap()
        .binding_declaration()
        .unwrap()
        .clone();
    let definition = FunctionDefinition::try_new_bound(
        "lower",
        FunctionVisibility::Public,
        declaration,
        Arc::new(DeclarationOnlyResolver),
    )
    .unwrap();
    let mut builder = EngineFunctionCatalogBuilder::new();
    builder.register(definition).unwrap();
    let declaration_only = builder.seal_bound().unwrap();
    assert!(matches!(
        select(
            &declaration_only,
            &bound,
            request(&[value(false)]),
            &Control::default()
        ),
        Err(FunctionBindingError::MissingBindingDeclaration)
    ));
    let args = [value(false)];
    prefixes(
        |control| select(&declaration_only, &bound, request(&args), control).map(|_| ()),
        false,
    );
}

#[test]
fn exact_overload_each_small_callback_keeps_first_control_cause_and_ordinary_footer() {
    let catalog = catalog();
    let bound = original(&catalog);
    let args = [value(true)];
    prefixes(
        |control| select(&catalog, &bound, request(&args), control).map(|_| ()),
        true,
    );
    for (expected, success) in [
        (FunctionValueType::new(DataType::Utf8, true), true),
        (FunctionValueType::new(DataType::Utf8, false), false),
    ] {
        prefixes(
            |control| {
                select(
                    &catalog,
                    &bound,
                    FunctionBindingRequest {
                        expected_result_type: Some(&expected),
                        ..request(&args)
                    },
                    control,
                )
                .map(|_| ())
            },
            success,
        );
    }
    let expected = FunctionValueType::new(DataType::Int64, true);
    prefixes(
        |control| {
            select(
                &catalog,
                &bound,
                FunctionBindingRequest {
                    expected_result_type: Some(&expected),
                    ..request(&args)
                },
                control,
            )
            .map(|_| ())
        },
        false,
    );
    let unknown = FunctionOverloadId::try_new("fixture/missing/v1").unwrap();
    prefixes(
        |control| {
            catalog
                .select_exact_overload_observed(
                    &bound.function_id,
                    bound.kind,
                    &unknown,
                    request(&args),
                    control,
                )
                .map(|_| ())
        },
        false,
    );
}

#[test]
fn exact_overload_wide_invalid_result_observes_real_type_walk_without_new_lower_profile() {
    let catalog = catalog();
    let bound = original(&catalog);
    let fields = (0..320)
        .map(|index| {
            Arc::new(
                Field::new(format!("field_{index}"), DataType::Int64, true).with_metadata(
                    HashMap::from([("provider.opaque".into(), format!("metadata_{index}"))]),
                ),
            )
        })
        .collect::<Vec<_>>();
    let expected =
        FunctionValueType::from_legacy_declared_projection(DataType::Struct(fields.into()), true);
    let args = [value(true)];
    let request = FunctionBindingRequest {
        expected_result_type: Some(&expected),
        ..request(&args)
    };
    let baseline = Control::default();
    assert!(select(&catalog, &bound, request, &baseline).is_err());
    let trace = baseline.trace.lock().unwrap().clone();
    let quantum = trace
        .iter()
        .position(|units| *units == 256)
        .expect("actual shared request type walk quantum");
    for stop in [0, quantum, trace.len() - 1] {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::new(vec![]),
                refusal: Some((stop, cause.clone())),
            };
            assert!(
                matches!(select(&catalog,&bound,request,&control),Err(FunctionBindingError::Control(actual)) if actual==cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..=stop]);
        }
    }
    // The wide type is an invalid result constraint, never a supported LOWER
    // nested argument or installed nested runtime specialization.
}

#[test]
fn exact_overload_installed_coalesce_preserves_control_abi_and_ordered_duplicate_channels() {
    let catalog = catalog();
    let initial = [value(false), value(false), value(false)];
    let bound = catalog
        .resolve_bound_user(
            "coalesce",
            FunctionKind::Scalar,
            request(&initial),
            &Control::default(),
        )
        .unwrap();
    assert_eq!(
        bound.selected.result_type,
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, false))
    );
    let late = [value(true), value(true), value(true)];
    assert!(
        catalog
            .validate_frozen_selection(
                &bound.function_id,
                bound.kind,
                &bound.selected,
                request(&late),
                &Control::default()
            )
            .is_err()
    );
    let expected = FunctionValueType::new(DataType::Utf8, true);
    let late_request = FunctionBindingRequest {
        expected_result_type: Some(&expected),
        ..request(&late)
    };
    let selected = select(&catalog, &bound, late_request, &Control::default()).unwrap();
    assert_eq!(selected.overload, bound.selected.overload);
    assert_eq!(
        selected.result_type,
        FunctionResultType::Scalar(expected.clone())
    );
    assert_eq!(
        selected.argument_types.as_ref(),
        &[
            FunctionArgumentType::Value(FunctionValueType::new(DataType::Utf8, true)),
            FunctionArgumentType::Value(FunctionValueType::new(DataType::Utf8, true)),
            FunctionArgumentType::Value(FunctionValueType::new(DataType::Utf8, true)),
        ]
    );
    catalog
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &selected,
            late_request,
            &Control::default(),
        )
        .unwrap();
    let declaration = catalog
        .pure_overload_declaration_observed(
            &bound.function_id,
            bound.kind,
            &selected.overload,
            &Control::default(),
        )
        .unwrap();
    assert_eq!(
        declaration.implementation().abi,
        PureKernelAbi::ControlIntrinsicV1
    );
    assert_eq!(
        declaration.effects().argument_control,
        novarocks_type_contract::ArgumentControl::Coalesce
    );
    assert_eq!(
        declaration.effects().null_behavior,
        FunctionNullBehavior::ControlDefined
    );
    assert_eq!(
        declaration.effects().instance_state,
        FunctionInstanceState::None
    );
    prefixes(
        |control| select(&catalog, &bound, late_request, control).map(|_| ()),
        true,
    );

    let first = text_constant("first");
    let second = text_constant("second");
    let literal = |constant: &ConstantValue| FunctionArgument::Value {
        value_type: constant.value_type().clone(),
        constant: Some(constant.clone()),
    };
    let ordered = [
        literal(&second),
        value(false),
        literal(&second),
        literal(&first),
    ];
    let selected = select(&catalog, &bound, request(&ordered), &Control::default()).unwrap();
    assert_eq!(selected.overload, bound.selected.overload);
    assert_eq!(selected.argument_types.len(), 4);
    assert_eq!(
        selected.result_type,
        FunctionResultType::Scalar(FunctionValueType::new(DataType::Utf8, false))
    );
    catalog
        .validate_frozen_selection(
            &bound.function_id,
            bound.kind,
            &selected,
            request(&ordered),
            &Control::default(),
        )
        .unwrap();
    for (index, original) in [(0, &second), (2, &second), (3, &first)] {
        let FunctionArgument::Value {
            constant: Some(actual),
            ..
        } = &ordered[index]
        else {
            unreachable!()
        };
        assert_eq!(
            actual.try_utf8().unwrap(),
            Some(if index == 3 { "first" } else { "second" })
        );
        assert_eq!(
            actual.pool().backing_identity(),
            original.pool().backing_identity()
        );
        assert!(Arc::ptr_eq(
            actual.pool().field_ref(),
            original.pool().field_ref()
        ));
    }
    assert!(matches!(
        &ordered[1],
        FunctionArgument::Value { constant: None, .. }
    ));
    prefixes(
        |control| select(&catalog, &bound, request(&ordered), control).map(|_| ()),
        true,
    );
    // Exact selection metadata and the declaration ABI are checked here;
    // no invocation effects, runtime preparation or short-circuit execution.
}

#[test]
fn aggregate_state_argument_contract_is_authored_and_revalidated_by_exact_owner() {
    use novarocks_type_contract::AggregateStateArgumentContract::{
        ExactSignature, ValueRootNullabilityIndependent,
    };
    let catalog = catalog();
    for (name, contract) in [
        ("count", ValueRootNullabilityIndependent),
        ("min", ValueRootNullabilityIndependent),
        ("max", ValueRootNullabilityIndependent),
        ("sum", ExactSignature),
        ("avg", ExactSignature),
        ("array_agg", ExactSignature),
    ] {
        for nullable in [false, true] {
            let arguments = [FunctionArgument::Value {
                value_type: FunctionValueType::new(DataType::Int64, nullable),
                constant: None,
            }];
            let resolved = catalog
                .resolve_bound_user(
                    name,
                    FunctionKind::Aggregate,
                    request(&arguments),
                    &Control::default(),
                )
                .unwrap();
            assert_eq!(
                resolved.selected.argument_types[0],
                arguments[0].argument_type()
            );
            assert_eq!(
                resolved
                    .selected
                    .aggregate
                    .as_ref()
                    .unwrap()
                    .state_argument_contract,
                contract
            );
            let declaration = catalog
                .definition_by_id(&resolved.function_id)
                .unwrap()
                .binding_declaration()
                .unwrap();
            assert_eq!(
                declaration
                    .overloads()
                    .iter()
                    .find(|item| item.identity == resolved.selected.overload)
                    .unwrap()
                    .aggregate
                    .as_ref()
                    .unwrap()
                    .state_argument_contract,
                contract
            );
            let validate = |selected: &FunctionBindingSelection, control: &Control| {
                catalog.validate_frozen_selection(
                    &resolved.function_id,
                    resolved.kind,
                    selected,
                    request(&arguments),
                    control,
                )
            };
            prefixes(|control| validate(&resolved.selected, control), true);
            let mut forged = resolved.selected.clone();
            forged.aggregate.as_mut().unwrap().state_argument_contract = match contract {
                ExactSignature => ValueRootNullabilityIndependent,
                ValueRootNullabilityIndependent => ExactSignature,
            };
            prefixes(|control| validate(&forged, control), false);
        }
    }
}

#[test]
fn exact_trusted_resolution_uses_original_metadata_without_auto_fallback() {
    let catalog = catalog();
    let original = original(&catalog);
    let arguments = [value(false)];
    prefixes(
        |control| {
            let exact = catalog.resolve_bound_trusted_at_overload(
                &original.function_id,
                original.kind,
                &original.selected.overload,
                request(&arguments),
                control,
            )?;
            assert_eq!(exact, original);
            assert_eq!(exact.admitted_execution_abi, None);
            Ok(())
        },
        true,
    );
    let absent =
        FunctionOverloadId::try_new("builtin.scalar/lower/absent-common-coercion/v1").unwrap();
    prefixes(
        |control| {
            catalog
                .resolve_bound_trusted_at_overload(
                    &original.function_id,
                    original.kind,
                    &absent,
                    request(&arguments),
                    control,
                )
                .map(|_| ())
        },
        false,
    );
    prefixes(
        |control| {
            catalog
                .resolve_bound_trusted_at_overload(
                    &original.function_id,
                    FunctionKind::Aggregate,
                    &original.selected.overload,
                    request(&arguments),
                    control,
                )
                .map(|_| ())
        },
        false,
    );
    let foreign = FunctionValueType::new(DataType::Int64, false);
    prefixes(
        |control| {
            catalog
                .resolve_bound_trusted_at_overload(
                    &original.function_id,
                    original.kind,
                    &original.selected.overload,
                    FunctionBindingRequest {
                        expected_result_type: Some(&foreign),
                        ..request(&arguments)
                    },
                    control,
                )
                .map(|_| ())
        },
        false,
    );
}

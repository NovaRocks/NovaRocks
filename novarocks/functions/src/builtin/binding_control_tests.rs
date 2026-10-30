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

use super::{catalogue::builtin_engine_function_catalog, resolver, value_conversion};
use crate::{
    FunctionArgument, FunctionBindingError, FunctionBindingRequest, FunctionKind,
    FunctionResolutionError, FunctionResultType, FunctionValueType,
};
use arrow_schema::{DataType, Field};
use novarocks_type_contract::{
    CompileControlError, CompilePhase, NR_LOGICAL_TYPE_KEY, PureCompileControl, ValueLogicalType,
};
use std::sync::{Arc, Mutex};

struct Control {
    seen: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<usize>,
    error: CompileControlError,
}
impl Control {
    fn new(stop: Option<usize>, error: CompileControlError) -> Self {
        Self {
            seen: Mutex::new(Vec::new()),
            stop,
            error,
        }
    }
    fn seen(&self) -> Vec<(CompilePhase, u32)> {
        self.seen.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        let mut seen = self.seen.lock().unwrap();
        seen.push((phase, work));
        if self.stop == Some(seen.len()) {
            Err(self.error.clone())
        } else {
            Ok(())
        }
    }
}
fn value(ty: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
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
fn wide_values() -> Vec<FunctionArgument> {
    (0..320)
        .map(|_| value(FunctionValueType::new(DataType::Int64, false)))
        .collect()
}
fn conversion_pair() -> (FunctionValueType, FunctionValueType) {
    let source = (0..320)
        .map(|i| {
            Arc::new(
                Field::new(format!("c{i}"), DataType::Utf8, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        NR_LOGICAL_TYPE_KEY.to_string(),
                        "json".to_string(),
                    )]),
                ),
            )
        })
        .collect::<Vec<_>>();
    let target = (0..320)
        .map(|i| Arc::new(Field::new(format!("c{i}"), DataType::Utf8, true)))
        .collect::<Vec<_>>();
    (
        FunctionValueType::from_legacy_declared_projection(DataType::Struct(source.into()), true),
        FunctionValueType::from_legacy_declared_projection(DataType::Struct(target.into()), true),
    )
}

#[test]
fn actual_scalar_dynamic_aggregate_table_families_preserve_original_controls() {
    let catalog = builtin_engine_function_catalog();
    for (name, kind, args, trusted) in [
        ("coalesce", FunctionKind::Scalar, wide_values(), false),
        ("__array_literal", FunctionKind::Scalar, wide_values(), true),
        ("sum", FunctionKind::Aggregate, wide_values(), false),
        (
            "unnest",
            FunctionKind::Table,
            (0..320)
                .map(|_| {
                    value(FunctionValueType::from_legacy_declared_projection(
                        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                        false,
                    ))
                })
                .collect(),
            false,
        ),
    ] {
        let bind = |control: &dyn PureCompileControl| {
            if trusted {
                catalog.resolve_bound_trusted(name, kind, request(&args), control)
            } else {
                catalog.resolve_bound_user(name, kind, request(&args), control)
            }
        };
        let baseline = Control::new(None, CompileControlError::Cancelled);
        let result = bind(&baseline);
        if name == "sum" {
            assert!(matches!(
                result,
                Err(FunctionBindingError::NoMatchingOverload)
            ));
        } else {
            assert!(result.is_ok(), "{name}: {result:?}");
        }
        let seen = baseline.seen();
        let interior = seen
            .iter()
            .position(|(_, work)| *work == 256)
            .expect("actual wide source loop must observe 256 work")
            + 1;
        assert!(
            seen.iter().all(
                |(phase, work)| *phase == CompilePhase::FunctionSpecialization && *work <= 256
            )
        );
        for stop in [1, interior, seen.len()] {
            for error in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control::new(Some(stop), error.clone());
                assert!(
                    matches!(bind(&control),Err(FunctionBindingError::Control(actual)) if actual==error),
                    "{name} stop {stop}"
                );
                assert_eq!(control.seen().len(), stop);
            }
        }
    }
}

#[test]
fn frozen_family_validation_observes_actual_channels_and_completed_tail() {
    let catalog = builtin_engine_function_catalog();
    let args = wide_values();
    for name in ["coalesce", "__array_literal"] {
        let bound = catalog
            .resolve_bound_trusted(
                name,
                FunctionKind::Scalar,
                request(&args),
                crate::binding_test_control(),
            )
            .unwrap();
        let baseline = Control::new(None, CompileControlError::Cancelled);
        catalog
            .validate_bound(&bound, request(&args), &baseline)
            .unwrap();
        let seen = baseline.seen();
        let interior = seen.iter().position(|(_, work)| *work == 256).unwrap() + 1;
        for stop in [1, interior, seen.len()] {
            for error in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control::new(Some(stop), error.clone());
                assert!(
                    matches!(catalog.validate_bound(&bound,request(&args),&control),Err(FunctionBindingError::Control(actual)) if actual==error)
                );
                assert_eq!(control.seen().len(), stop);
            }
        }
    }
}

#[test]
fn exact_scalar_signature_passes_use_the_original_control() {
    let types = (0..320)
        .map(|_| FunctionValueType::new(DataType::Int64, false))
        .collect::<Vec<_>>();
    let baseline = Control::new(None, CompileControlError::Cancelled);
    let (index, selected) =
        resolver::resolve_scalar_value_signature_with_overload("coalesce", &types, &baseline)
            .unwrap();
    assert_eq!(selected.argument_types.len(), 320);
    let seen = baseline.seen();
    let interior = seen.iter().position(|(_, work)| *work == 256).unwrap() + 1;
    for stop in [1, interior, seen.len()] {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::new(Some(stop), error.clone());
            assert!(
                matches!(resolver::resolve_scalar_value_signature_with_overload("coalesce",&types,&control),Err(FunctionResolutionError::Control(actual)) if actual==error)
            );
        }
    }
    let baseline = Control::new(None, CompileControlError::Cancelled);
    assert_eq!(
        resolver::resolve_scalar_value_signature_at_overload("coalesce", index, &types, &baseline)
            .unwrap(),
        selected
    );
    let stop = baseline.seen().len();
    let control = Control::new(Some(stop), CompileControlError::DeadlineExceeded);
    assert!(matches!(
        resolver::resolve_scalar_value_signature_at_overload("coalesce", index, &types, &control),
        Err(FunctionResolutionError::Control(
            CompileControlError::DeadlineExceeded
        ))
    ));
}

#[test]
fn conversion_real_nested_domain_walk_and_selected_validation_are_controlled() {
    let (source, target) = conversion_pair();
    let baseline = Control::new(None, CompileControlError::Cancelled);
    let selection =
        value_conversion::resolve_value_conversion(&source, &target, &baseline).unwrap();
    assert!(matches!(&selection.result_type,FunctionResultType::Scalar(result) if result==&target));
    let seen = baseline.seen();
    let interior = seen.iter().position(|(_, work)| *work == 256).unwrap() + 1;
    for stop in [1, interior, seen.len()] {
        for error in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let control = Control::new(Some(stop), error.clone());
            assert!(
                matches!(value_conversion::resolve_value_conversion(&source,&target,&control),Err(FunctionBindingError::Control(actual)) if actual==error)
            );
        }
    }
    // A genuine registered exact owner validates the same complete selection.
    let catalog = builtin_engine_function_catalog();
    let args = [value(source)];
    let req = FunctionBindingRequest {
        expected_result_type: Some(&target),
        ..request(&args)
    };
    let bound = catalog
        .resolve_bound_trusted(
            value_conversion::VALUE_CONVERSION_NAME,
            FunctionKind::Scalar,
            req,
            crate::binding_test_control(),
        )
        .unwrap();
    assert_eq!(bound.selected, selection);
    let baseline = Control::new(None, CompileControlError::Cancelled);
    catalog.validate_bound(&bound, req, &baseline).unwrap();
    let control = Control::new(
        Some(baseline.seen().len()),
        CompileControlError::ResourceExhausted,
    );
    assert!(matches!(
        catalog.validate_bound(&bound, req, &control),
        Err(FunctionBindingError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
}

#[test]
fn source_type_resource_failure_is_typed_and_domain_is_never_inferred() {
    let catalog = builtin_engine_function_catalog();
    let field = Field::new("item", DataType::Int64, true).with_metadata(
        (0..257)
            .map(|i| (format!("k{i}"), "v".into()))
            .collect::<std::collections::HashMap<String, String>>(),
    );
    let args = [value(FunctionValueType::from_legacy_declared_projection(
        DataType::List(Arc::new(field)),
        false,
    ))];
    assert!(
        catalog
            .resolve_bound_user(
                "coalesce",
                FunctionKind::Scalar,
                request(&args),
                crate::binding_test_control()
            )
            .is_ok(),
        "source annotation resources are checked by source ingress"
    );
    assert!(
        FunctionValueType::try_with_logical_type(DataType::Int64, false, ValueLogicalType::Json,)
            .is_err()
    );
    let args = [value(FunctionValueType::new(
        DataType::FixedSizeBinary(16),
        false,
    ))];
    assert!(matches!(
        catalog.resolve_bound_user(
            "abs",
            FunctionKind::Scalar,
            request(&args),
            crate::binding_test_control()
        ),
        Err(FunctionBindingError::NoMatchingOverload)
    ));
}

#[test]
fn scalar_request_value_lambda_gate_preserves_original_refusal_and_control_prefix() {
    let catalog = builtin_engine_function_catalog();
    // The scalar request gate reads Value and Lambda channels in source order.
    // Every request profile is validated first; a Lambda is never projected
    // into a value carrier or silently skipped by the borrowed view.
    for ordinal in [0, 160, 319] {
        let mut args = wide_values();
        args[ordinal] = FunctionArgument::Lambda {
            parameter_types: novarocks_type_contract::FunctionParameterTypes::from_pure_boxed(
                Box::new([]),
            ),
            result_type: FunctionValueType::new(DataType::Int64, false),
        };
        let bind = |control: &dyn PureCompileControl| {
            catalog.resolve_bound_user("coalesce", FunctionKind::Scalar, request(&args), control)
        };
        let baseline = Control::new(None, CompileControlError::Cancelled);
        assert!(matches!(
            bind(&baseline),
            Err(FunctionBindingError::NoMatchingOverload)
        ));
        let seen = baseline.seen();
        let interior = seen.iter().position(|(_, units)| *units == 256).unwrap() + 1;
        for stop in [1, interior, seen.len()] {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let control = Control::new(Some(stop), cause.clone());
                assert!(
                    matches!(bind(&control), Err(FunctionBindingError::Control(actual)) if actual == cause)
                );
                assert_eq!(control.seen(), seen[..stop]);
            }
        }
    }
}

#[test]
fn real_unknown_binding_and_resolver_errors_precede_armed_completion_control() {
    let catalog = builtin_engine_function_catalog();
    for source in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
        CompileControlError::from_domain(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic,
        ),
    ] {
        let control = Control::new(Some(2), source.clone());
        assert!(matches!(
            catalog.resolve_bound_user(
                "fixture_missing_first_cause",
                FunctionKind::Scalar,
                request(&[]),
                &control
            ),
            Err(FunctionBindingError::UnknownFunction)
        ));
        assert_eq!(control.seen(), [(CompilePhase::FunctionSpecialization, 0)]);
        let control = Control::new(Some(2), source.clone());
        assert!(matches!(
            resolver::resolve_scalar_value_signature_with_overload(
                "fixture_missing_first_cause",
                &[],
                &control
            ),
            Err(FunctionResolutionError::UnknownFunction)
        ));
        assert_eq!(control.seen(), [(CompilePhase::FunctionSpecialization, 0)]);
        let control = Control::new(Some(1), source.clone());
        assert!(
            matches!(catalog.resolve_bound_user("fixture_missing_first_cause",FunctionKind::Scalar,request(&[]),&control),Err(FunctionBindingError::Control(actual)) if actual==source)
        );
    }
}

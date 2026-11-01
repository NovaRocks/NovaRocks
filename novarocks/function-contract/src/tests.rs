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
use arrow_array::{Array, Int64Array};
use arrow_schema::{DataType, Field};
use novarocks_constant_contract::{ConstantError, ConstantPolicy, ConstantPool};
use novarocks_type_contract::{ValueLogicalType, ValueTypeError};
use std::sync::{Arc, Mutex};

const PHASE: CompilePhase = CompilePhase::FunctionSpecialization;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.refusal {
            assert!(trace.len() < at, "callback after original refusal");
        }
        trace.push((phase, units));
        match self.refusal.clone() {
            Some((at, cause)) if trace.len() == at => Err(cause),
            _ => Ok(()),
        }
    }
}
fn every_prefix(
    operation: impl Fn(&dyn PureCompileControl) -> Result<bool, FunctionBindingError>,
    expected: bool,
) -> Vec<(CompilePhase, u32)> {
    let baseline = Control::default();
    assert_eq!(operation(&baseline).unwrap(), expected);
    let trace = baseline.trace.into_inner().unwrap();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    assert!(
        trace.len() >= 2,
        "entry and ordinary/success completion are observed"
    );
    for cause in CAUSES {
        for at in 1..=trace.len() {
            let refused = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert_eq!(
                operation(&refused),
                Err(FunctionBindingError::Control(cause.clone()))
            );
            assert_eq!(*refused.trace.lock().unwrap(), trace[..at]);
        }
    }
    trace
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 32,
        max_logical_elements: 128,
        max_retained_buffer_bytes: 64 * 1024,
        max_type_depth: 16,
        max_type_nodes: 128,
        max_dictionary_depth: 8,
        max_metadata_bytes: 4096,
        max_library_validation_work: 1_000_000,
        max_library_validation_bytes: 1024 * 1024,
    }
}
fn integer_pool(rows: Vec<Option<i64>>) -> ConstantPool {
    let ty = FunctionValueType::new(DataType::Int64, true);
    let field = Arc::new(
        Field::new("original.source", DataType::Int64, true).with_metadata(
            [("producer".to_owned(), "unchanged".to_owned())]
                .into_iter()
                .collect::<std::collections::HashMap<String, String>>(),
        ),
    );
    ConstantPool::try_new(
        field,
        ty,
        Int64Array::from(rows).to_data(),
        policy(),
        PHASE,
        &Control::default(),
    )
    .unwrap()
}
fn value_argument(value: ConstantValue) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: value.value_type().clone(),
        constant: Some(value),
    }
}
fn copied<T: Copy>(value: T) -> (T, T) {
    (value, value)
}
fn cloned<T: Clone>(value: &T) -> T {
    value.clone()
}
fn nested(nullable: bool, child_nullable: bool, metadata: &str) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(
        DataType::Struct(
            vec![Arc::new(
                Field::new("original.child", DataType::Int64, child_nullable).with_metadata(
                    [("original.metadata".to_owned(), metadata.to_owned())]
                        .into_iter()
                        .collect::<std::collections::HashMap<String, String>>(),
                ),
            )]
            .into(),
        ),
        nullable,
    )
}
fn matches(
    argument: &FunctionArgument,
    expected: &FunctionArgumentType,
    control: &dyn PureCompileControl,
) -> Result<bool, FunctionBindingError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = argument.matches_type_observed(expected, &mut work);
    finish_binding_work(result, work)
}

#[test]
fn value_argument_matching_keeps_recursive_nominal_domains_on_identical_layouts() {
    use novarocks_type_contract::{ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime};
    let list = |domain| {
        let child = FunctionValueType::try_with_logical_type(DataType::Utf8, true, domain).unwrap();
        let storage = ValueTypeStorageLifetime::pure();
        let semantic = ValueTypeRef::try_new(
            ValueKind::List(child.value_type().clone()),
            false,
            storage.clone(),
        )
        .unwrap();
        let layout = ValueLayout::try_new(
            &DataType::List(Arc::new(Field::new("", DataType::Utf8, true))),
            storage,
        )
        .unwrap();
        FunctionValueType::try_from_value(semantic, layout).unwrap()
    };
    let nominal = list(ValueLogicalType::Json);
    let physical = list(ValueLogicalType::Physical);
    assert_eq!(nominal.data_type(), physical.data_type());
    let argument: FunctionArgument = FunctionArgument::Value {
        value_type: nominal.clone(),
        constant: None,
    };
    every_prefix(
        |control| {
            matches(
                &argument,
                &FunctionArgumentType::Value(physical.clone()),
                control,
            )
        },
        false,
    );
    every_prefix(
        |control| {
            matches(
                &argument,
                &FunctionArgumentType::Value(nominal.clone()),
                control,
            )
        },
        true,
    );
}

#[test]
fn selected_value_matching_rejects_same_semantics_on_different_physical_offsets() {
    let small =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Physical)
            .unwrap();
    let large = FunctionValueType::try_with_logical_type(
        DataType::LargeUtf8,
        true,
        ValueLogicalType::Physical,
    )
    .unwrap();
    assert!(small.same_semantics(&large));
    let argument: FunctionArgument = FunctionArgument::Value {
        value_type: small.clone(),
        constant: None,
    };
    every_prefix(
        |control| {
            matches(
                &argument,
                &FunctionArgumentType::Value(large.clone()),
                control,
            )
        },
        false,
    );
    every_prefix(
        |control| {
            matches(
                &argument,
                &FunctionArgumentType::Value(small.clone()),
                control,
            )
        },
        true,
    );
}

#[test]
fn borrowed_request_is_copy_without_clone_or_copy_transport_carrier() {
    // Deliberately implements neither Clone nor Copy. The request borrows it.
    struct Transport {
        pool: u32,
        ordinal: u32,
        retained: Box<str>,
    }
    let ty = nested(true, false, "retained");
    let args = [
        FunctionArgument::Value {
            value_type: ty.clone(),
            constant: Some(Transport {
                pool: u32::MAX,
                ordinal: 1,
                retained: "original reference".into(),
            }),
        },
        FunctionArgument::Value {
            value_type: ty.clone(),
            constant: None,
        },
        FunctionArgument::Lambda {
            parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                vec![ty.clone()].into_boxed_slice(),
            ),
            result_type: ty.clone(),
        },
    ];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 3,
        expected_result_type: Some(&ty),
    };
    let (first, second) = copied(request);
    let third = cloned(&request);
    for borrowed in [first, second, third, request] {
        assert!(std::ptr::eq(borrowed.arguments, args.as_slice()));
        assert_eq!(borrowed.logical_argument_count, 3);
        assert!(std::ptr::eq(borrowed.expected_result_type.unwrap(), &ty));
        let FunctionArgument::Value {
            constant: Some(source),
            ..
        } = &borrowed.arguments[0]
        else {
            panic!("selected transport source is absent");
        };
        assert_eq!((source.pool, source.ordinal), (u32::MAX, 1));
        assert_eq!(&*source.retained, "original reference");
        assert!(matches!(
            borrowed.arguments[1],
            FunctionArgument::Value { constant: None, .. }
        ));
        assert_eq!(
            borrowed.arguments[2].argument_type(),
            FunctionArgumentType::Lambda {
                parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
                    vec![ty.clone()].into_boxed_slice()
                ),
                result_type: ty.clone(),
            }
        );
    }
    let unconstrained = FunctionBindingRequest {
        expected_result_type: None,
        ..request
    };
    assert!(copied(unconstrained).0.expected_result_type.is_none());
}

#[test]
fn admitted_nonzero_constant_preserves_selected_source_and_distinguishes_none_from_null() {
    let pool = integer_pool(vec![Some(-99), Some(42), None]);
    let selected = pool.value(1).unwrap();
    let null = pool.value(2).unwrap();
    let args = [value_argument(selected), value_argument(null)];
    let request = FunctionBindingRequest {
        arguments: &args,
        logical_argument_count: 2,
        expected_result_type: None,
    };
    for original in [request, copied(request).0] {
        for (index, ordinal, expected) in [(0, 1, Some(42)), (1, 2, None)] {
            let FunctionArgument::Value {
                value_type,
                constant: Some(value),
            } = &original.arguments[index]
            else {
                panic!("original constant source is absent");
            };
            assert_eq!(value_type, pool.value_type());
            assert_eq!(value.ordinal(), ordinal);
            assert_eq!(value.try_i64().unwrap(), expected);
            assert!(Arc::ptr_eq(value.pool().array(), pool.array()));
            assert!(Arc::ptr_eq(value.pool().field_ref(), pool.field_ref()));
            assert_eq!(value.pool().backing_identity(), pool.backing_identity());
            assert_eq!(value.field().name(), "original.source");
            assert_eq!(
                value.field().metadata().get("producer").map(String::as_str),
                Some("unchanged")
            );
        }
    }
    let other_pool = integer_pool(vec![Some(42), Some(8)]);
    assert_ne!(pool.backing_identity(), other_pool.backing_identity());
    let equal_value = value_argument(other_pool.value(0).unwrap());
    every_prefix(
        |control| args[0].equals_observed(&equal_value, PHASE, control),
        true,
    );
    let nonconstant = FunctionArgument::Value {
        value_type: pool.value_type().clone(),
        constant: None,
    };
    every_prefix(
        |control| args[1].equals_observed(&nonconstant, PHASE, control),
        false,
    );
    every_prefix(
        |control| nonconstant.equals_observed(&nonconstant, PHASE, control),
        true,
    );
}

#[test]
fn lambda_comparison_preserves_complete_nominal_types_and_ignores_source_annotations() {
    let largeint = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let physical = FunctionValueType::new(DataType::FixedSizeBinary(16), true);
    let make_lambda = |nominal, metadata| FunctionArgument::Lambda {
        parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
            vec![nominal, nested(false, true, metadata)].into_boxed_slice(),
        ),
        result_type: nested(true, false, "body"),
    };
    let original = make_lambda(largeint.clone(), "original");
    let equal = make_lambda(largeint.clone(), "original");
    let changed_nominal = make_lambda(physical, "original");
    let changed_metadata = make_lambda(largeint, "foreign");
    let changed_body = FunctionArgument::Lambda {
        parameter_types: match &original {
            FunctionArgument::Lambda {
                parameter_types, ..
            } => parameter_types.clone(),
            _ => unreachable!(),
        },
        result_type: nested(true, true, "different body"),
    };
    every_prefix(
        |control| original.equals_observed(&equal, PHASE, control),
        true,
    );
    every_prefix(
        |control| original.equals_observed(&changed_metadata, PHASE, control),
        true,
    );
    for changed in [changed_nominal, changed_body] {
        every_prefix(
            |control| original.equals_observed(&changed, PHASE, control),
            false,
        );
    }
    let value = FunctionArgument::Value {
        value_type: nested(true, false, "body"),
        constant: None,
    };
    assert_eq!(
        every_prefix(
            |control| original.equals_observed(&value, PHASE, control),
            false
        ),
        vec![(PHASE, 0), (PHASE, 0)]
    );
}

#[test]
fn selected_type_matching_keeps_value_nullability_covariance_but_lambda_exactness() {
    let strict = nested(false, false, "same");
    let nullable = nested(true, true, "same");
    let strict_value = FunctionArgument::Value {
        value_type: strict.clone(),
        constant: None,
    };
    let nullable_value = FunctionArgument::Value {
        value_type: nullable.clone(),
        constant: None,
    };
    every_prefix(
        |control| {
            matches(
                &strict_value,
                &FunctionArgumentType::Value(nullable.clone()),
                control,
            )
        },
        true,
    );
    let root_refusal = every_prefix(
        |control| {
            matches(
                &nullable_value,
                &FunctionArgumentType::Value(strict.clone()),
                control,
            )
        },
        false,
    );
    assert_eq!(root_refusal, vec![(PHASE, 0), (PHASE, 2)]);
    let child_nullable = FunctionArgument::Value {
        value_type: nested(false, true, "same"),
        constant: None,
    };
    every_prefix(
        |control| {
            matches(
                &child_nullable,
                &FunctionArgumentType::Value(strict.clone()),
                control,
            )
        },
        false,
    );
    let lambda = FunctionArgument::Lambda {
        parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
            vec![strict.clone()].into_boxed_slice(),
        ),
        result_type: strict.clone(),
    };
    let exact_lambda = FunctionArgumentType::Lambda {
        parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
            vec![strict.clone()].into_boxed_slice(),
        ),
        result_type: strict.clone(),
    };
    every_prefix(|control| matches(&lambda, &exact_lambda, control), true);
    let widened_lambda = FunctionArgumentType::Lambda {
        parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
            vec![nullable].into_boxed_slice(),
        ),
        result_type: strict,
    };
    every_prefix(|control| matches(&lambda, &widened_lambda, control), false);
    assert_eq!(
        FunctionBindingError::from(ConstantError::Limit("original extent")),
        FunctionBindingError::Constant(ConstantError::Limit("original extent"))
    );
    assert_eq!(
        FunctionBindingError::from(ConstantError::Invalid("original invalid value")),
        FunctionBindingError::Constant(ConstantError::Invalid("original invalid value"))
    );
    for cause in CAUSES {
        assert_eq!(
            FunctionBindingError::from(ConstantError::Control(cause.clone())),
            FunctionBindingError::Control(cause)
        );
    }
    let type_error = ValueTypeError::TooDeep;
    assert_eq!(
        FunctionBindingError::from(type_error),
        FunctionBindingError::ValueType(type_error)
    );
}

#[test]
fn binding_value_type_refusals_keep_the_original_domain_source_and_fact() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    for source in [
        ValueTypeError::InvalidType("memory exhausted"),
        ValueTypeError::UnknownLogicalMetadata,
        ValueTypeError::InvalidLogicalCarrier(ValueLogicalType::LargeInt),
        ValueTypeError::TooDeep,
        ValueTypeError::TooManyNodes,
    ] {
        let expected = source.failure_fact();
        let error = FunctionBindingError::from(source);
        assert_eq!(error.failure_fact(), expected);
        assert_eq!(
            error.source().unwrap().downcast_ref::<ValueTypeError>(),
            Some(&source)
        );
        assert_eq!(
            error.to_string(),
            format!("invalid function binding: {source}")
        );
    }
    assert_eq!(
        FunctionBindingError::from(ValueTypeError::TooDeep)
            .failure_fact()
            .class(),
        FailureClass::Resource
    );
    assert_eq!(
        FunctionBindingError::from(ValueTypeError::InvalidType("ResourceExhausted"))
            .failure_fact()
            .class(),
        FailureClass::Invariant
    );
}

#[test]
fn binding_constant_refusals_preserve_source_and_policy_is_not_control() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    for original in [
        ConstantError::Limit("original constant owner policy"),
        ConstantError::Type(ValueTypeError::TooDeep),
        ConstantError::Invalid("cancelled"),
    ] {
        let expected = original.failure_fact();
        let error = FunctionBindingError::from(original.clone());
        assert_eq!(error.failure_fact(), expected);
        assert_eq!(
            error.source().unwrap().downcast_ref::<ConstantError>(),
            Some(&original)
        );
        assert_eq!(error.control_error(), None);
    }
    assert_eq!(
        FunctionBindingError::from(ConstantError::Limit("cancelled"))
            .failure_fact()
            .class(),
        FailureClass::Resource
    );
    assert_eq!(
        FunctionBindingError::from(ConstantError::Invalid("memory exhausted"))
            .failure_fact()
            .class(),
        FailureClass::Invariant
    );
}

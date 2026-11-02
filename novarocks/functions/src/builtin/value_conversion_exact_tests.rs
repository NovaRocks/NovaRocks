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
use novarocks_type_contract::{CompileControlError, CompilePhase};
use std::sync::Mutex;

struct Control {
    events: Mutex<Vec<(CompilePhase, u32)>>,
    stop: Option<usize>,
    cause: CompileControlError,
}
impl Control {
    fn new(stop: Option<usize>, cause: CompileControlError) -> Self {
        Self {
            events: Mutex::new(Vec::new()),
            stop,
            cause,
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.events.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        let mut events = self.events.lock().unwrap();
        events.push((phase, work));
        if self.stop == Some(events.len()) {
            Err(self.cause.clone())
        } else {
            Ok(())
        }
    }
}
fn control() -> Control {
    Control::new(None, CompileControlError::Cancelled)
}
fn value(ty: FunctionValueType) -> FunctionArgument {
    FunctionArgument::Value {
        value_type: ty,
        constant: None,
    }
}
fn physical(dt: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::from_legacy_declared_projection(dt, nullable)
}
fn logical(dt: DataType, nullable: bool, domain: ValueLogicalType) -> FunctionValueType {
    FunctionValueType::try_with_logical_type(dt, nullable, domain).unwrap()
}
fn request<'a>(
    arguments: &'a [FunctionArgument],
    target: &'a FunctionValueType,
) -> FunctionBindingRequest<'a> {
    FunctionBindingRequest {
        arguments,
        logical_argument_count: 1,
        expected_result_type: Some(target),
    }
}
fn catalog() -> crate::EngineFunctionCatalog {
    let mut builder = crate::EngineFunctionCatalogBuilder::new();
    builder
        .register(value_conversion_definition().unwrap())
        .unwrap();
    builder.seal().unwrap()
}
fn selected(
    catalog: &crate::EngineFunctionCatalog,
    overload: &str,
    arguments: &[FunctionArgument],
    target: &FunctionValueType,
    control: &Control,
) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError> {
    catalog.select_exact_overload_observed(
        &FunctionId::try_new(VALUE_CONVERSION_FUNCTION_ID).unwrap(),
        FunctionKind::Scalar,
        &FunctionOverloadId::try_new(overload).unwrap(),
        request(arguments, target),
        control,
    )
}
fn pairs() -> Vec<(&'static str, FunctionValueType, FunctionValueType)> {
    vec![
        (
            JSON_TEXT,
            logical(DataType::Utf8, false, ValueLogicalType::Json),
            physical(DataType::Utf8, false),
        ),
        (
            SIGNED_LARGEINT,
            physical(DataType::Int16, false),
            logical(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            ),
        ),
        (
            LARGEINT_SIGNED,
            logical(
                DataType::FixedSizeBinary(16),
                true,
                ValueLogicalType::LargeInt,
            ),
            physical(DataType::Int8, true),
        ),
        (
            LARGEINT_FLOAT,
            logical(
                DataType::FixedSizeBinary(16),
                false,
                ValueLogicalType::LargeInt,
            ),
            physical(DataType::Float32, false),
        ),
        (
            NULL_LIFT,
            physical(DataType::Null, true),
            physical(DataType::Int64, true),
        ),
    ]
}

#[test]
fn exact_conversion_installed_owner_authors_all_five_full_signatures() {
    let catalog = catalog();
    for (overload, source, target) in pairs() {
        let arguments = [value(source.clone())];
        let binding = selected(&catalog, overload, &arguments, &target, &control()).unwrap();
        // Independent expected fields, rather than the old resolver's output.
        assert_eq!(binding.overload.as_str(), overload);
        assert_eq!(
            binding.argument_types.as_ref(),
            &[FunctionArgumentType::Value(source)]
        );
        assert_eq!(
            binding.result_type,
            FunctionResultType::Scalar(target.clone())
        );
        assert!(binding.aggregate.is_none());
        catalog
            .validate_frozen_selection(
                &FunctionId::try_new(VALUE_CONVERSION_FUNCTION_ID).unwrap(),
                FunctionKind::Scalar,
                &binding,
                request(&arguments, &target),
                &control(),
            )
            .unwrap();
        assert!(matches!(
            &arguments[0],
            FunctionArgument::Value { constant: None, .. }
        ));
    }
}

#[test]
fn exact_conversion_never_searches_other_pairs_or_widens_explicit_target() {
    let catalog = catalog();
    let arguments = [value(physical(DataType::Int64, false))];
    let target = logical(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    );
    assert!(selected(&catalog, SIGNED_LARGEINT, &arguments, &target, &control()).is_ok());
    for overload in [
        JSON_TEXT,
        LARGEINT_SIGNED,
        LARGEINT_FLOAT,
        NULL_LIFT,
        "fixture/unknown-overload",
    ] {
        assert!(selected(&catalog, overload, &arguments, &target, &control()).is_err());
    }
    let arguments = [value(physical(DataType::Int64, true))];
    let error = selected(&catalog, SIGNED_LARGEINT, &arguments, &target, &control()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("conversion target cannot narrow source nullability")
    );
    let no_target = FunctionBindingRequest {
        arguments: &arguments,
        logical_argument_count: 1,
        expected_result_type: None,
    };
    let error = ValueConversionResolver
        .select_at_overload_observed(
            &FunctionOverloadId::try_new(SIGNED_LARGEINT).unwrap(),
            no_target,
            &control(),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires an explicit complete target")
    );
    // A Physical binary carrier cannot substitute for the nominal LargeInt source.
    assert!(
        selected(
            &catalog,
            LARGEINT_FLOAT,
            &[value(physical(DataType::FixedSizeBinary(16), false))],
            &physical(DataType::Float64, false),
            &control()
        )
        .is_err()
    );
}

#[test]
fn exact_conversion_selected_cv_keeps_original_ordinal_and_engine_refuses_false_type() {
    let source = physical(DataType::Int64, false);
    let field = Arc::new(Field::new("original", DataType::Int64, false));
    let pool = crate::ConstantPool::try_new(
        field.clone(),
        source.clone(),
        Int64Array::from(vec![11, -7, 99]).to_data(),
        crate::ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 32,
            max_logical_elements: 64,
            max_retained_buffer_bytes: 4096,
            max_type_depth: 8,
            max_type_nodes: 32,
            max_dictionary_depth: 4,
            max_metadata_bytes: 4096,
            max_library_validation_work: 65536,
            max_library_validation_bytes: 65536,
        },
        CompilePhase::Validate,
        &control(),
    )
    .unwrap();
    let cv = pool.value(1).unwrap();
    let arguments = [FunctionArgument::Value {
        value_type: source,
        constant: Some(cv.clone()),
    }];
    let target = logical(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    );
    let catalog = catalog();
    selected(&catalog, SIGNED_LARGEINT, &arguments, &target, &control()).unwrap();
    let FunctionArgument::Value {
        constant: Some(retained),
        ..
    } = &arguments[0]
    else {
        panic!("retained CV")
    };
    assert_eq!(retained.ordinal(), 1);
    assert_eq!(retained.try_i64().unwrap(), Some(-7));
    assert!(Arc::ptr_eq(retained.pool().array(), pool.array()));
    assert!(Arc::ptr_eq(retained.pool().field_ref(), &field));
    for false_type in [
        physical(DataType::Int32, false),
        physical(DataType::Int64, true),
    ] {
        let bad = [FunctionArgument::Value {
            value_type: false_type,
            constant: Some(cv.clone()),
        }];
        let error = selected(&catalog, SIGNED_LARGEINT, &bad, &target, &control()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("constant source type differs from its argument type")
        );
    }
}

#[test]
fn exact_conversion_old_validation_still_refuses_signature_drift_before_pair_walk() {
    let arguments = [value(physical(DataType::Int64, false))];
    let target = logical(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    );
    let selected = ValueConversionResolver
        .select_at_overload_observed(
            &FunctionOverloadId::try_new(SIGNED_LARGEINT).unwrap(),
            request(&arguments, &target),
            &control(),
        )
        .unwrap();
    let mut wrong_argument = selected.clone();
    wrong_argument.argument_types = novarocks_type_contract::FunctionArgumentTypes::from_pure_boxed(
        vec![FunctionArgumentType::Value(physical(
            DataType::Int32,
            false,
        ))]
        .into_boxed_slice(),
    );
    let mut wrong_result = selected.clone();
    wrong_result.result_type = FunctionResultType::Scalar(logical(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    ));
    let mut wrong_overload = selected;
    wrong_overload.overload = FunctionOverloadId::try_new(JSON_TEXT).unwrap();
    for drifted in [wrong_argument, wrong_result, wrong_overload] {
        let baseline = control();
        let error = ValueConversionResolver
            .validate_selected(&drifted, request(&arguments, &target), &baseline)
            .unwrap_err();
        assert!(error.to_string().contains(
            "frozen value-domain conversion differs from its exact source, target or overload"
        ));
        let trace = baseline.trace();
        for stop in 1..=trace.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refusing = Control::new(Some(stop), cause.clone());
                let error = ValueConversionResolver
                    .validate_selected(&drifted, request(&arguments, &target), &refusing)
                    .unwrap_err();
                assert_eq!(error.control_error(), Some(cause));
                assert_eq!(refusing.trace(), trace[..stop]);
            }
        }
    }
}

fn json_pair(fields: usize) -> (FunctionValueType, FunctionValueType) {
    let source = (0..fields)
        .map(|index| {
            Arc::new(
                Field::new(format!("c{index}"), DataType::Utf8, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([
                        (NR_LOGICAL_TYPE_KEY.to_owned(), "json".to_owned()),
                        ("provider-id".to_owned(), "retained".to_owned()),
                    ]),
                ),
            )
        })
        .collect::<Vec<_>>();
    let target = (0..fields)
        .map(|index| {
            Arc::new(
                Field::new(format!("c{index}"), DataType::Utf8, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider-id".to_owned(),
                        "retained".to_owned(),
                    )]),
                ),
            )
        })
        .collect::<Vec<_>>();
    (
        physical(DataType::Struct(source.into()), true),
        physical(DataType::Struct(target.into()), true),
    )
}

#[test]
fn exact_conversion_json_preserves_nested_annotations_and_field_identity() {
    let (source, target) = json_pair(2);
    let catalog = catalog();
    let arguments = [value(source)];
    selected(&catalog, JSON_TEXT, &arguments, &target, &control()).unwrap();
    let DataType::Struct(fields) = target.data_type() else {
        unreachable!()
    };
    let decorated =
        physical(
            DataType::Struct(
                vec![
                    Arc::new(fields[0].as_ref().clone().with_metadata(
                        std::collections::HashMap::<String, String>::from([(
                            "provider-id".to_owned(),
                            "changed".to_owned(),
                        )]),
                    )),
                    fields[1].clone(),
                ]
                .into(),
            ),
            true,
        );
    assert!(selected(&catalog, JSON_TEXT, &arguments, &decorated, &control()).is_ok());
    for changed in [
        fields[0].as_ref().clone().with_name("foreign"),
        fields[0].as_ref().clone().with_nullable(false),
    ] {
        let bad = physical(
            DataType::Struct(vec![Arc::new(changed), fields[1].clone()].into()),
            true,
        );
        assert!(selected(&catalog, JSON_TEXT, &arguments, &bad, &control()).is_err());
    }
}

fn assert_prefixes(
    operation: impl Fn(&Control) -> Result<Arc<FunctionBindingSelection>, FunctionBindingError>,
    success: bool,
) {
    let baseline = control();
    assert_eq!(operation(&baseline).is_ok(), success);
    let trace = baseline.trace();
    assert!(trace.len() >= 2);
    assert_eq!(trace.first().unwrap().1, 0);
    // Success and ordinary refusal both reach the original completed footer.
    for stop in 1..=trace.len() {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refusing = Control::new(Some(stop), cause.clone());
            let error = operation(&refusing).unwrap_err();
            assert_eq!(error.control_error(), Some(cause));
            assert_eq!(refusing.trace(), trace[..stop]);
        }
    }
}

#[test]
fn exact_conversion_success_and_ordinary_refusals_keep_every_original_control_prefix() {
    let catalog = catalog();
    let arguments = [value(physical(DataType::Int64, false))];
    let target = logical(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    );
    assert_prefixes(
        |control| selected(&catalog, SIGNED_LARGEINT, &arguments, &target, control),
        true,
    );
    assert_prefixes(
        |control| selected(&catalog, JSON_TEXT, &arguments, &target, control),
        false,
    );
    let narrowed = physical(DataType::Int64, false);
    let nullable = [value(logical(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    ))];
    assert_prefixes(
        |control| selected(&catalog, LARGEINT_SIGNED, &nullable, &narrowed, control),
        false,
    );
}

#[test]
fn exact_conversion_wide_json_has_real_256_work_and_sampled_primary_refusals() {
    let catalog = catalog();
    let (source, target) = json_pair(320);
    let arguments = [value(source)];
    let baseline = control();
    selected(&catalog, JSON_TEXT, &arguments, &target, &baseline).unwrap();
    let trace = baseline.trace();
    let quantum = trace.iter().position(|(_, units)| *units == 256).unwrap();
    let samples = [1, quantum + 1, trace.len()];
    for stop in samples {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refusing = Control::new(Some(stop), cause.clone());
            let error = selected(&catalog, JSON_TEXT, &arguments, &target, &refusing).unwrap_err();
            assert_eq!(error.control_error(), Some(cause));
            assert_eq!(refusing.trace(), trace[..stop]);
        }
    }
}

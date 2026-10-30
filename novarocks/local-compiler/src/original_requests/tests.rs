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
use arrow_array::{Array, Int64Array, new_null_array};
use arrow_schema::{DataType, Field};
use novarocks_functions::ConstantPool;
use novarocks_physical_plan::{ConstantPoolId, ConstantReference, ConstantReferenceError};
use novarocks_type_contract::{CompilePhase, PureCompileControl, ValueLogicalType};
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
        assert!(units <= 256);
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
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1048576,
        max_type_depth: 16,
        max_type_nodes: 128,
        max_dictionary_depth: 8,
        max_metadata_bytes: 4096,
        max_library_validation_work: 1000000,
        max_library_validation_bytes: 1048576,
    }
}
fn integer() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, true)
}
fn reference(pool: u32, ordinal: u32) -> ConstantReference {
    ConstantReference {
        pool: ConstantPoolId::new(pool),
        ordinal,
    }
}
fn request(arguments: Vec<StaticFunctionArgument<ConstantReference>>) -> PhysicalCallRequest {
    PhysicalCallRequest {
        logical_argument_count: arguments.len(),
        arguments: arguments.into_boxed_slice(),
        expected_result_type: None,
        constant_policy: policy(),
    }
}
fn value(
    ty: FunctionValueType,
    constant: Option<ConstantReference>,
) -> StaticFunctionArgument<ConstantReference> {
    StaticFunctionArgument::Value {
        value_type: ty,
        constant,
    }
}
fn original_pool() -> ConstantPool {
    ConstantPool::try_new(
        Arc::new(
            Field::new("request.original", DataType::Int64, true).with_metadata(
                [("source".to_owned(), "original".to_owned())]
                    .into_iter()
                    .collect::<std::collections::HashMap<String, String>>(),
            ),
        ),
        integer(),
        Int64Array::from(vec![Some(-9), Some(42), None]).to_data(),
        policy(),
        PHASE,
        &Control::default(),
    )
    .unwrap()
}
fn pools(pool: &ConstantPool) -> ConstantPools {
    let mut pools = ConstantPools::empty();
    pools
        .insert(ConstantPoolId::new(u32::MAX), pool.clone())
        .unwrap();
    pools
}
fn materialize<'a>(
    source: &'a PhysicalCallRequest,
    pools: &ConstantPools,
    control: &dyn PureCompileControl,
) -> Result<MaterializedCallRequest<'a>, ExpressionLoweringError> {
    let mut work = CompileCheckpoints::try_new(control, PHASE)?;
    let result = materialize_call_request_observed(source, pools, &mut work);
    if matches!(result, Err(ExpressionLoweringError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn prefixes(source: &PhysicalCallRequest, pools: &ConstantPools, ordinary: bool) {
    let baseline = Control::default();
    let result = materialize(source, pools, &baseline);
    assert_eq!(result.is_err(), ordinary);
    let trace = baseline.trace.into_inner().unwrap();
    assert_eq!(trace.first(), Some(&(PHASE, 0)));
    for at in 1..=trace.len() {
        for cause in CAUSES {
            let control = Control {
                trace: Mutex::default(),
                refusal: Some((at, cause.clone())),
            };
            assert!(
                matches!(materialize(source, pools, &control), Err(ExpressionLoweringError::Control(actual)) if actual == cause)
            );
            assert_eq!(*control.trace.lock().unwrap(), trace[..at]);
        }
    }
}

#[test]
fn original_none_some_selected_ordinals_and_result_constraint_remain_distinct() {
    let pool = original_pool();
    let pools = pools(&pool);
    let mut source = request(vec![
        value(integer(), None),
        value(integer(), Some(reference(u32::MAX, 1))),
        value(integer(), Some(reference(u32::MAX, 2))),
    ]);
    // A data-only aggregate channel boundary; no signature or invocation is
    // inferred from it. The third static channel is retained in original order.
    source.logical_argument_count = 2;
    source.constant_policy.max_rows = 0;
    source.constant_policy.max_retained_buffer_bytes = 0;
    let materialized = materialize(&source, &pools, &Control::default()).unwrap();
    assert_eq!(materialized.constant_policy(), source.constant_policy);
    let request = materialized.request();
    assert_eq!(request.logical_argument_count, 2);
    assert!(request.expected_result_type.is_none());
    assert!(matches!(
        request.arguments[0],
        FunctionArgument::Value { constant: None, .. }
    ));
    for (ordinal, index, expected) in [(1, 1, Some(42)), (2, 2, None)] {
        let FunctionArgument::Value {
            value_type,
            constant: Some(value),
        } = &request.arguments[index]
        else {
            panic!("selected original constant");
        };
        assert_eq!(value_type, &integer());
        assert_eq!(value.try_i64().unwrap(), expected);
        assert_eq!(value.ordinal(), ordinal);
        assert_eq!(value.pool().backing_identity(), pool.backing_identity());
        assert!(Arc::ptr_eq(value.pool().array(), pool.array()));
        assert!(Arc::ptr_eq(value.pool().field_ref(), pool.field_ref()));
        assert_eq!(
            value.field().metadata().get("source").map(String::as_str),
            Some("original")
        );
    }
    drop(materialized);
    source.expected_result_type = Some(FunctionValueType::new(DataType::Utf8, false));
    let materialized = materialize(&source, &pools, &Control::default()).unwrap();
    assert!(std::ptr::eq(
        materialized.request().expected_result_type.unwrap(),
        source.expected_result_type.as_ref().unwrap()
    ));
}

#[test]
fn original_full_lambda_and_nested_typed_null_retain_fields_and_nominal_types() {
    let large = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    let field = Arc::new(large.try_to_field("child.original").unwrap().with_metadata(
        // Preserve the original nominal metadata and add a distinct attribute.
        {
            let mut metadata = large
                .try_to_field("child.original")
                .unwrap()
                .metadata()
                .clone();
            metadata.insert("source".to_owned(), "nested".to_owned());
            metadata
        },
    ));
    let nested = FunctionValueType::new(DataType::Struct(vec![field.clone()].into()), true);
    let pool = ConstantPool::try_new(
        Arc::new(nested.try_to_field("request.null").unwrap()),
        nested.clone(),
        new_null_array(nested.data_type(), 2).to_data(),
        policy(),
        PHASE,
        &Control::default(),
    )
    .unwrap();
    let pools = pools(&pool);
    let source = request(vec![
        StaticFunctionArgument::Lambda {
            parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(Box::from([
                nested.clone(),
                large.clone(),
            ])),
            result_type: nested.clone(),
        },
        value(nested.clone(), Some(reference(u32::MAX, 1))),
    ]);
    let materialized = materialize(&source, &pools, &Control::default()).unwrap();
    let request = materialized.request();
    let FunctionArgument::Lambda {
        parameter_types,
        result_type,
    } = &request.arguments[0]
    else {
        panic!("original Lambda shape");
    };
    assert_eq!(&**parameter_types, &[nested.clone(), large]);
    assert_eq!(result_type, &nested);
    let DataType::Struct(fields) = &parameter_types[0].data_type() else {
        panic!("nested parameter");
    };
    let DataType::Struct(original_fields) = nested.data_type() else {
        panic!("original typed Struct");
    };
    assert!(Arc::ptr_eq(&fields[0], &original_fields[0]));
    assert!(fields[0].metadata().is_empty());
    let FunctionArgument::Value {
        constant: Some(value),
        ..
    } = &request.arguments[1]
    else {
        panic!("typed NULL is Some");
    };
    assert!(value.is_null_observed(PHASE, &Control::default()).unwrap());
    assert_eq!(value.ordinal(), 1);
    assert_eq!(value.value_type(), &nested);
    assert!(Arc::ptr_eq(value.pool().field_ref(), pool.field_ref()));
    assert!(Arc::ptr_eq(value.pool().array(), pool.array()));
    prefixes(&source, &pools, false);
}

#[test]
fn original_missing_out_of_bounds_and_full_type_errors_never_become_nonconstant() {
    let pool = original_pool();
    let pools = pools(&pool);
    let missing = request(vec![value(integer(), Some(reference(0, 1)))]);
    assert!(
        matches!(materialize(&missing, &pools, &Control::default()), Err(ExpressionLoweringError::Reference(ConstantReferenceError::MissingPool(id))) if id == ConstantPoolId::new(0))
    );
    let out_of_bounds = request(vec![value(integer(), Some(reference(u32::MAX, 3)))]);
    let error = materialize(&out_of_bounds, &pools, &Control::default()).unwrap_err();
    assert_eq!(error.to_string(), pool.value(3).unwrap_err().to_string());
    let mismatch = request(vec![value(
        FunctionValueType::new(DataType::UInt64, true),
        Some(reference(u32::MAX, 1)),
    )]);
    assert!(
        matches!(materialize(&mismatch, &pools, &Control::default()), Err(ExpressionLoweringError::Reference(ConstantReferenceError::SourceTypeMismatch(actual))) if actual == reference(u32::MAX, 1))
    );
    prefixes(&missing, &pools, true);
    prefixes(&mismatch, &pools, true);
}

#[test]
fn original_small_success_ordinary_tail_and_structural_extent_preserve_primary_control() {
    let pool = original_pool();
    let pools = pools(&pool);
    let source = request(vec![
        value(integer(), None),
        value(integer(), Some(reference(u32::MAX, 1))),
    ]);
    prefixes(&source, &pools, false);
    let mut malformed = request(Vec::new());
    malformed.logical_argument_count = 1;
    prefixes(&malformed, &pools, true);
    let mut source = request(vec![value(integer(), None); MAX_CALL_EFFECT_ARGUMENTS + 1]);
    let control = Control::default();
    assert!(matches!(
        materialize(&source, &pools, &control),
        Err(ExpressionLoweringError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(*control.trace.lock().unwrap(), vec![(PHASE, 0)]);
    source.arguments = Box::from([StaticFunctionArgument::Lambda {
        parameter_types: novarocks_type_contract::TypedFactSlice::from_pure_boxed(
            vec![integer(); MAX_CALL_EFFECT_ARGUMENTS + 1].into_boxed_slice(),
        ),
        result_type: integer(),
    }]);
    source.logical_argument_count = 1;
    let control = Control::default();
    assert!(matches!(
        materialize(&source, &pools, &control),
        Err(ExpressionLoweringError::Control(
            CompileControlError::ResourceExhausted
        ))
    ));
    assert_eq!(*control.trace.lock().unwrap(), vec![(PHASE, 0)]);
}

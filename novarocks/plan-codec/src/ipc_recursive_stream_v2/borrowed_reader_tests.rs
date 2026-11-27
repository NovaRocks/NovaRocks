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
use crate::physical_type_v2::{
    DecodedSourceFieldTable, DecodedTypeTable, TypeProjectionLimits,
    decode_source_field_table_input_derived_with_creator, decode_type_table,
    encode_source_field_table_pure_in, encode_type_table,
};
use novarocks_constant_contract::ConstantError;

fn table(
    field: &Arc<Field>,
    ty: &FunctionValueType,
) -> (DecodedTypeTable, DecodedSourceFieldTable) {
    let limits = TypeProjectionLimits {
        max_definitions: 4096,
        max_expanded_nodes: 16384,
        max_string_bytes: 1 << 20,
    };
    let wire = encode_type_table(&[(u32::MAX, ty.clone())], limits, &Control::default()).unwrap();
    let values = decode_type_table(&wire, limits, &Control::default()).unwrap();
    let encode_control = Control::default();
    let mut encode_work =
        novarocks_type_contract::CompileCheckpoints::try_new(&encode_control, CompilePhase::Encode)
            .unwrap();
    let source =
        encode_source_field_table_pure_in(&[(0, field)], limits, &mut encode_work).unwrap();
    encode_work.finish().unwrap();
    let mut exact = |_: std::alloc::Layout| Ok::<(), ()>(());
    let mut opaque = |_: usize| Ok::<(), ()>(());
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut exact,
        &mut opaque,
    );
    let decode_control = Control::default();
    let mut work =
        novarocks_type_contract::CompileCheckpoints::try_new(&decode_control, CompilePhase::Decode)
            .unwrap();
    let fields = decode_source_field_table_input_derived_with_creator(
        &source,
        crate::physical_type_v2::source_fields_tests::source_bytes(&source),
        &mut creation,
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    (values, fields)
}

fn invoke(
    borrowed: bool,
    stream: &RecursiveConstantStream<'_, '_>,
    field: &Arc<Field>,
    ty: &FunctionValueType,
    invoice: usize,
    control: &Control,
) -> Result<ConstantPool, RecursiveReaderError> {
    if borrowed {
        stream.materialize_pool_borrowed(
            Arc::clone(field),
            ty,
            invoice,
            policy(),
            reader_limits(),
            control,
        )
    } else {
        stream.materialize_pool(
            Arc::clone(field),
            ty.clone(),
            invoice,
            policy(),
            reader_limits(),
            control,
        )
    }
}

fn check_both_traces(
    stream: &RecursiveConstantStream<'_, '_>,
    field: &Arc<Field>,
    ty: &FunctionValueType,
    invoice: usize,
    succeeds: bool,
) {
    let owned = Control::default();
    let borrowed = Control::default();
    let owned_result = invoke(false, stream, field, ty, invoice, &owned);
    let borrowed_result = invoke(true, stream, field, ty, invoice, &borrowed);
    assert_eq!(owned_result.is_ok(), succeeds);
    assert_eq!(borrowed_result.is_ok(), succeeds);
    if !succeeds {
        let owned_error = owned_result.unwrap_err();
        let borrowed_error = borrowed_result.unwrap_err();
        assert!(matches!(owned_error, RecursiveReaderError::Projection(_)));
        assert!(matches!(
            borrowed_error,
            RecursiveReaderError::Projection(_)
        ));
        assert_eq!(owned_error.to_string(), borrowed_error.to_string());
    }
    let trace = owned.trace.lock().unwrap().clone();
    assert_eq!(*borrowed.trace.lock().unwrap(), trace);
    assert!(!trace.is_empty());
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::Decode && *units <= 256)
    );
    for at in 0..trace.len() {
        for cause in CAUSES {
            for borrowed in [false, true] {
                let control = Control {
                    trace: Mutex::new(Vec::new()),
                    stop: Some((at, cause.clone())),
                };
                assert!(matches!(
                    invoke(borrowed, stream, field, ty, invoice, &control),
                    Err(RecursiveReaderError::Projection(FlatPoolResourceError::Control(actual)))
                        if actual == cause
                ));
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn borrowed_recursive_reader_retains_real_type_table_full_field_and_pool_values() {
    let array = nested();
    let field = Arc::new(
        Field::new("original", array.data_type().clone(), true)
            .with_metadata(HashMap::from([("identity".into(), "retained".into())])),
    );
    let ty = FunctionValueType::new(field.data_type().clone(), true);
    let (types, source_fields) = table(&field, &ty);
    let decode_control = Control::default();
    let mut source_work =
        novarocks_type_contract::CompileCheckpoints::try_new(&decode_control, CompilePhase::Decode)
            .unwrap();
    let field = source_fields
        .field_observed(0, &mut source_work)
        .unwrap()
        .unwrap();
    source_work.finish().unwrap();
    let ty = types.value_type(u32::MAX).unwrap();
    let bytes = fixture(Arc::clone(&array), field);
    let checked = stream(&bytes, field);
    let invoice = source(&bytes);
    let pool = invoke(true, &checked, field, ty, invoice, &Control::default()).unwrap();
    assert!(Arc::ptr_eq(pool.field_ref(), field));
    assert_eq!(pool.field().name(), "original");
    assert_eq!(
        pool.field().metadata().get("identity").map(String::as_str),
        Some("retained")
    );
    assert_eq!(pool.array().to_data(), array.to_data());
    assert!(
        pool.value(0)
            .unwrap()
            .is_null_observed(CompilePhase::Decode, &Control::default())
            .unwrap()
    );
    assert_eq!(pool.value(1).unwrap().ordinal(), 1);
    assert_eq!(pool.resource_facts().array_nodes, 4);
    check_both_traces(&checked, field, ty, invoice, true);
}

#[test]
fn borrowed_recursive_reader_preserves_nested_type_failures_and_original_control_prefixes() {
    let array = nested();
    let field = Arc::new(Field::new("original", array.data_type().clone(), true));
    let bytes = fixture(array, &field);
    let checked = stream(&bytes, &field);
    let invoice = source(&bytes);
    let DataType::List(child) = field.data_type() else {
        panic!("actual nested fixture is a List")
    };
    let changed_child = Arc::new(child.as_ref().clone().with_nullable(!child.is_nullable()));
    for wrong in [
        FunctionValueType::new(field.data_type().clone(), false),
        FunctionValueType::new(DataType::List(changed_child), true),
    ] {
        let error =
            invoke(true, &checked, &field, &wrong, invoice, &Control::default()).unwrap_err();
        assert!(matches!(
            error,
            RecursiveReaderError::Projection(FlatPoolResourceError::Constant(
                ConstantError::Invalid(_)
            ))
        ));
        // The frozen core retains Cow::Borrowed until after the resource gate
        // and reader/to_data; this test does not claim allocator instrumentation.
        check_both_traces(&checked, &field, &wrong, invoice, false);
    }
    // Source attributes and dictionary encoding do not change execution identity.
    let source_only_child = Arc::new(child.as_ref().clone().with_metadata(HashMap::from([(
        "provider".into(),
        "changed-source".into(),
    )])));
    for equivalent in [
        FunctionValueType::new(DataType::List(source_only_child), true),
        FunctionValueType::new(
            DataType::Dictionary(
                Box::new(DataType::Int8),
                Box::new(field.data_type().clone()),
            ),
            true,
        ),
    ] {
        check_both_traces(&checked, &field, &equivalent, invoice, true);
    }
    let replacement = Arc::new(field.as_ref().clone());
    check_both_traces(
        &checked,
        &replacement,
        &FunctionValueType::new(field.data_type().clone(), true),
        invoice,
        false,
    );
}

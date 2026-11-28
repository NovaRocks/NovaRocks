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

fn table(
    field: &Arc<Field>,
    ty: &FunctionValueType,
) -> (DecodedTypeTable, DecodedSourceFieldTable) {
    let limits = TypeProjectionLimits {
        max_definitions: 16,
        max_expanded_nodes: 16,
        max_string_bytes: 1 << 20,
    };
    let wire = encode_type_table(&[(0, ty.clone())], limits, &FixtureEncodeControl).unwrap();
    let values = decode_type_table(&wire, limits, &Control::good()).unwrap();
    let encode_control = FixtureEncodeControl;
    let mut encode_work =
        novarocks_type_contract::CompileCheckpoints::try_new(&encode_control, CompilePhase::Encode)
            .unwrap();
    let source =
        encode_source_field_table_pure_in(&[(u32::MAX, field)], limits, &mut encode_work).unwrap();
    encode_work.finish().unwrap();
    let mut exact = |_: std::alloc::Layout| Ok::<(), ()>(());
    let mut opaque = |_: usize| Ok::<(), ()>(());
    let mut creation = novarocks_type_contract::ValueSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut exact,
        &mut opaque,
    );
    let decode_control = Control::good();
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
    stream: &FlatConstantStream<'_, '_>,
    field: &Arc<Field>,
    ty: &FunctionValueType,
    invoice: usize,
    control: &Control,
) -> Result<ConstantPool, FlatReaderError> {
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
        materialize(stream, field, ty, invoice, reader_limits(), control)
    }
}

fn check_both_traces(
    stream: &FlatConstantStream<'_, '_>,
    field: &Arc<Field>,
    ty: &FunctionValueType,
    invoice: usize,
    succeeds: bool,
) {
    let owned = Control::good();
    let borrowed = Control::good();
    let owned_result = invoke(false, stream, field, ty, invoice, &owned);
    let borrowed_result = invoke(true, stream, field, ty, invoice, &borrowed);
    assert_eq!(owned_result.is_ok(), succeeds);
    assert_eq!(borrowed_result.is_ok(), succeeds);
    if !succeeds {
        // Compare actual ordinary diagnostics, never infer a control cause from text.
        let owned_error = owned_result.unwrap_err();
        let borrowed_error = borrowed_result.unwrap_err();
        assert!(matches!(owned_error, FlatReaderError::Projection(_)));
        assert!(matches!(borrowed_error, FlatReaderError::Projection(_)));
        assert_eq!(owned_error.to_string(), borrowed_error.to_string());
    }
    let trace = owned.trace();
    assert_eq!(borrowed.trace(), trace);
    assert!(!trace.is_empty());
    assert!(
        trace
            .iter()
            .all(|(phase, units)| *phase == CompilePhase::Decode && *units <= 256)
    );
    for at in 0..trace.len() {
        for cause in CAUSES {
            for borrowed in [false, true] {
                let control = Control::refusing(at, cause.clone());
                assert!(matches!(
                    invoke(borrowed, stream, field, ty, invoice, &control),
                    Err(FlatReaderError::Projection(FlatPoolResourceError::Control(actual)))
                        if actual == cause
                ));
                assert_eq!(control.trace(), trace[..=at]);
            }
        }
    }
}

#[test]
fn borrowed_flat_reader_retains_real_type_table_field_arc_and_selected_ordinals() {
    let field = Arc::new(
        Field::new("authored-source", DataType::Int64, true)
            .with_metadata(HashMap::from([("provider".into(), "exact-source".into())])),
    );
    let ty = FunctionValueType::new(DataType::Int64, true);
    let (types, source_fields) = table(&field, &ty);
    let decode_control = Control::good();
    let mut source_work =
        novarocks_type_contract::CompileCheckpoints::try_new(&decode_control, CompilePhase::Decode)
            .unwrap();
    let field = source_fields
        .field_observed(u32::MAX, &mut source_work)
        .unwrap()
        .unwrap();
    source_work.finish().unwrap();
    let ty = types.value_type(0).unwrap();
    let input = writer_stream(
        Arc::new(Int64Array::from(vec![Some(111), None, Some(-7)])),
        field,
    );
    let stream = checked(&input, field);
    let invoice = retained(&input, field);
    let pool = invoke(true, &stream, field, ty, invoice, &Control::good()).unwrap();
    assert!(Arc::ptr_eq(pool.field_ref(), field));
    assert_eq!(pool.field().name(), "authored-source");
    assert_eq!(
        pool.field().metadata().get("provider").map(String::as_str),
        Some("exact-source")
    );
    assert_eq!(pool.value_type().logical_type(), ty.logical_type());
    assert_eq!(pool.value_type().nullable(), ty.nullable());
    assert_eq!(pool.value(0).unwrap().try_i64().unwrap(), Some(111));
    assert_eq!(pool.value(1).unwrap().try_i64().unwrap(), None);
    assert_eq!(pool.value(2).unwrap().try_i64().unwrap(), Some(-7));
    check_both_traces(&stream, field, ty, invoice, true);
}

#[test]
fn borrowed_flat_reader_preserves_owned_rejections_and_every_original_control_prefix() {
    let field = Arc::new(Field::new("source", DataType::Int64, true));
    let input = writer_stream(Arc::new(Int64Array::from(vec![Some(71), None])), &field);
    let stream = checked(&input, &field);
    let invoice = retained(&input, &field);
    for wrong in [
        FunctionValueType::new(DataType::Int64, false),
        FunctionValueType::new(DataType::Float64, true),
    ] {
        let error = invoke(true, &stream, &field, &wrong, invoice, &Control::good()).unwrap_err();
        assert!(matches!(
            error,
            FlatReaderError::Projection(FlatPoolResourceError::Constant(ConstantError::Invalid(_)))
        ));
        // The source core's into_owned occurs only after its resource gate and
        // successful reader/to_data. This rejection is not an allocation probe.
        check_both_traces(&stream, &field, &wrong, invoice, false);
    }
    // Dictionary is an input encoding; the declared execution layout is Int64.
    let encoded = FunctionValueType::new(
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Int64)),
        true,
    );
    assert_eq!(encoded.data_type(), &DataType::Int64);
    check_both_traces(&stream, &field, &encoded, invoice, true);
    let replacement = Arc::new(field.as_ref().clone());
    check_both_traces(
        &stream,
        &replacement,
        &FunctionValueType::new(DataType::Int64, true),
        invoice,
        false,
    );
}

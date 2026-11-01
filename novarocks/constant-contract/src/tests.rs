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
use arrow_array::types::Int8Type;
use arrow_array::{DictionaryArray, Float64Array, Int8Array, Int64Array, StringArray};
use arrow_schema::TimeUnit;
use std::sync::Mutex;

struct Control {
    failure: Option<CompileControlError>,
    at_positive: bool,
    units: Mutex<Vec<u32>>,
}
impl Control {
    fn good() -> Self {
        Self {
            failure: None,
            at_positive: false,
            units: Mutex::new(vec![]),
        }
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        self.units.lock().unwrap().push(units);
        if let Some(e) = self.failure.clone()
            && (!self.at_positive || units > 0)
        {
            return Err(e);
        }
        Ok(())
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1_000_000,
        max_array_nodes: 4096,
        max_logical_elements: 100_000_000,
        max_retained_buffer_bytes: 64 * 1024 * 1024,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 16,
        max_metadata_bytes: 1024 * 1024,
        max_library_validation_work: 100_000_000,
        max_library_validation_bytes: 100_000_000,
    }
}
fn pool(array: ArrayRef, nullable: bool) -> ConstantPool {
    let ty = FunctionValueType::new(array.data_type().clone(), nullable);
    let field = Arc::new(Field::new("literal", array.data_type().clone(), nullable));
    ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap()
}
fn eq(l: &ConstantValue, r: &ConstantValue) -> bool {
    l.equals_observed(r, CompilePhase::Validate, &Control::good())
        .unwrap()
}

#[test]
fn shared_decimal_parameter_grammar_matches_arrow_for_every_precision_and_scale() {
    use arrow_array::types::{
        Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType,
        validate_decimal_precision_and_scale,
    };
    fn compare<T: DecimalType>(constructor: fn(u8, i8) -> DataType) {
        for precision in u8::MIN..=u8::MAX {
            for scale in i8::MIN..=i8::MAX {
                let actual = validate_arrow_carrier_parameters_observed::<ConstantError>(
                    &constructor(precision, scale),
                    || Ok(()),
                )
                .map_err(|error| error.to_string());
                let expected = validate_decimal_precision_and_scale::<T>(precision, scale)
                    .map_err(|error| error.to_string());
                assert_eq!(actual, expected, "precision={precision}, scale={scale}");
            }
        }
    }
    compare::<Decimal32Type>(DataType::Decimal32);
    compare::<Decimal64Type>(DataType::Decimal64);
    compare::<Decimal128Type>(DataType::Decimal128);
    compare::<Decimal256Type>(DataType::Decimal256);
}

#[test]
fn pools_share_backing_but_equality_compares_actual_values() {
    let p = pool(Arc::new(Int64Array::from(vec![7, 8, 7])), false);
    assert!(Arc::ptr_eq(&p.0, &p.value(0).unwrap().pool.0));
    assert!(eq(&p.value(0).unwrap(), &p.value(2).unwrap()));
    assert!(!eq(&p.value(0).unwrap(), &p.value(1).unwrap()));
    let other = pool(Arc::new(Int64Array::from(vec![7])), false);
    assert!(eq(&p.value(0).unwrap(), &other.value(0).unwrap()));
    assert_eq!(p.value(0).unwrap().try_i64().unwrap(), Some(7));
    assert!(p.value(u32::MAX).is_err());
}
#[test]
fn exact_float_bits_include_nan_payload_and_negative_zero() {
    let bits = [
        0,
        (-0.0f64).to_bits(),
        0x7ff8000000000001,
        0x7ff8000000000002,
    ];
    let p = pool(
        Arc::new(Float64Array::from(bits.map(f64::from_bits).to_vec())),
        false,
    );
    for i in 0..4 {
        assert_eq!(
            p.value(i).unwrap().try_f64_bits().unwrap(),
            Some(bits[i as usize])
        );
        assert!(eq(&p.value(i).unwrap(), &p.value(i).unwrap()));
    }
    assert!(!eq(&p.value(0).unwrap(), &p.value(1).unwrap()));
    assert!(!eq(&p.value(2).unwrap(), &p.value(3).unwrap()));
}
#[test]
fn typed_null_is_not_wrong_type_or_nonconstant() {
    let p = pool(Arc::new(Int64Array::from(vec![None])), true);
    let q = pool(Arc::new(StringArray::from(vec![None::<&str>])), true);
    assert_eq!(p.value(0).unwrap().try_i64().unwrap(), None);
    assert!(p.value(0).unwrap().try_utf8().is_err());
    assert!(
        p.value(0)
            .unwrap()
            .is_null_observed(CompilePhase::Validate, &Control::good())
            .unwrap()
    );
    assert!(!eq(&p.value(0).unwrap(), &q.value(0).unwrap()));
    let field = Arc::new(Field::new("literal", DataType::Int64, false));
    assert!(matches!(
        ConstantPool::try_new(
            field,
            FunctionValueType::new(DataType::Int64, false),
            Int64Array::from(vec![None]).to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good()
        ),
        Err(ConstantError::Invalid(_))
    ));
}
#[test]
fn dictionary_numbering_and_unused_entries_do_not_change_a_value() {
    let l = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0]),
        Arc::new(StringArray::from(vec!["a", "b"])),
    )
    .unwrap();
    let r = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![1]),
        Arc::new(StringArray::from(vec!["unused", "a", "b"])),
    )
    .unwrap();
    let l = pool(Arc::new(l), false);
    let r = pool(Arc::new(r), false);
    assert!(eq(&l.value(0).unwrap(), &r.value(0).unwrap()));
    #[allow(deprecated)]
    let field = Arc::new(Field::new_dict(
        "literal",
        r.array().data_type().clone(),
        false,
        77,
        true,
    ));
    let different = ConstantPool::try_new(
        field,
        r.value_type().clone(),
        r.array().to_data(),
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    assert!(eq(&l.value(0).unwrap(), &different.value(0).unwrap()));
}
#[test]
fn dictionary_value_null_is_sql_null_even_with_valid_key() {
    let a = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0]),
        Arc::new(StringArray::from(vec![None::<&str>])),
    )
    .unwrap();
    let ty = FunctionValueType::new(a.data_type().clone(), false);
    let field = Arc::new(Field::new("literal", a.data_type().clone(), false));
    assert!(matches!(
        ConstantPool::try_new(
            field,
            ty,
            a.to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good()
        ),
        Err(ConstantError::Invalid(_))
    ));
}
#[test]
fn declared_nullability_must_agree_and_source_attributes_do_not_reauthor_type() {
    let array = Arc::new(Int64Array::from(vec![1])) as ArrayRef;
    let field = Arc::new(Field::new("literal", DataType::Int64, true));
    assert!(matches!(
        ConstantPool::try_new(
            field,
            FunctionValueType::new(DataType::Int64, false),
            array.to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good()
        ),
        Err(ConstantError::Invalid(_))
    ));
    let field = Arc::new(Field::new("literal", DataType::Utf8, false).with_metadata(
        std::collections::HashMap::<String, String>::from([(
            "nr_logical_type".into(),
            "json".into(),
        )]),
    ));
    assert!(
        ConstantPool::try_new(
            field,
            FunctionValueType::new(DataType::Utf8, false),
            StringArray::from(vec!["{}"]).to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good()
        )
        .is_ok()
    );
}
#[test]
fn policy_near_over_and_compact_null_logical_size() {
    let array = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
    let baseline = pool(array.clone(), false).resource_facts();
    let field = Arc::new(Field::new("literal", DataType::Int64, false));
    for (bound, ok) in [
        (baseline.library_validation_work_upper_bound, true),
        (baseline.library_validation_work_upper_bound - 1, false),
    ] {
        let mut p = policy();
        p.max_library_validation_work = bound;
        assert_eq!(
            ConstantPool::try_new(
                field.clone(),
                FunctionValueType::new(DataType::Int64, false),
                array.to_data(),
                p,
                CompilePhase::Validate,
                &Control::good()
            )
            .is_ok(),
            ok
        );
    }
    let array = Arc::new(arrow_array::NullArray::new(1_000_000)) as ArrayRef;
    let field = Arc::new(Field::new("literal", DataType::Null, true));
    let mut p = policy();
    p.max_logical_elements = 999_999;
    assert!(matches!(
        ConstantPool::try_new(
            field.clone(),
            FunctionValueType::new(DataType::Null, true),
            array.to_data(),
            p,
            CompilePhase::Validate,
            &Control::good()
        ),
        Err(ConstantError::Limit(_))
    ));
    p.max_logical_elements = 1_000_000;
    let control = Control::good();
    let accepted = ConstantPool::try_new(
        field,
        FunctionValueType::new(DataType::Null, true),
        array.to_data(),
        p,
        CompilePhase::Validate,
        &control,
    )
    .unwrap();
    assert_eq!(
        accepted.resource_facts().logical_elements_upper_bound,
        1_000_000
    );
    assert!(control.units.lock().unwrap().len() < 10);
}
#[test]
fn controls_are_typed_at_entry_and_every_observed_interval() {
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let control = Control {
            failure: Some(error.clone()),
            at_positive: false,
            units: Mutex::new(vec![]),
        };
        let field = Arc::new(Field::new("literal", DataType::Int64, false));
        assert_eq!(
            ConstantPool::try_new(
                field,
                FunctionValueType::new(DataType::Int64, false),
                Int64Array::from(vec![1]).to_data(),
                policy(),
                CompilePhase::Validate,
                &control
            )
            .unwrap_err(),
            ConstantError::Control(error)
        );
        assert_eq!(*control.units.lock().unwrap(), vec![0]);
    }
    let p = pool(
        Arc::new(StringArray::from(vec!["x".repeat(400_000)])),
        false,
    );
    let q = pool(
        Arc::new(StringArray::from(vec!["x".repeat(400_000)])),
        false,
    );
    let control = Control {
        failure: Some(CompileControlError::Cancelled),
        at_positive: true,
        units: Mutex::new(vec![]),
    };
    assert_eq!(
        p.value(0)
            .unwrap()
            .equals_observed(&q.value(0).unwrap(), CompilePhase::Validate, &control),
        Err(ConstantError::Control(CompileControlError::Cancelled))
    );
    let units = control.units.lock().unwrap();
    assert_eq!(*units, vec![0, 256]);
}

#[test]
fn decimal256_temporal_and_nested_constants_have_exact_owner_values() {
    let ty = FunctionValueType::new(DataType::Decimal256(60, 12), false);
    let field = Arc::new(Field::new("literal", ty.data_type().clone(), false));
    let value = arrow_buffer::i256::from_i128(i128::MAX);
    let a = ConstantValue::from_decimal256(
        field.clone(),
        ty.clone(),
        value,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let b = ConstantValue::from_decimal256(
        field,
        ty,
        value,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    assert!(eq(&a, &b));
    assert_eq!(a.try_decimal256().unwrap(), Some(value));
    let a = pool(
        Arc::new(arrow_array::TimestampNanosecondArray::from(vec![123]).with_timezone("UTC")),
        false,
    );
    let b = pool(
        Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![123]).with_timezone("UTC")),
        false,
    );
    assert!(!eq(&a.value(0).unwrap(), &b.value(0).unwrap()));
    let list =
        arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>(vec![
            Some(vec![Some(1), None, Some(3)]),
        ]);
    let a = pool(Arc::new(list.clone()), false);
    let b = pool(Arc::new(list), false);
    assert!(eq(&a.value(0).unwrap(), &b.value(0).unwrap()));
}
#[test]
fn compact_nested_null_does_not_expand_the_validation_or_equality_walk() {
    let make = || {
        arrow_array::ListArray::new(
            Arc::new(Field::new("item", DataType::Null, true)),
            arrow_buffer::OffsetBuffer::new(vec![0i32, 1_000_000].into()),
            Arc::new(arrow_array::NullArray::new(1_000_000)),
            None,
        )
    };
    let a = pool(Arc::new(make()), false);
    let b = pool(Arc::new(make()), false);
    let control = Control::good();
    assert!(
        a.value(0)
            .unwrap()
            .equals_observed(&b.value(0).unwrap(), CompilePhase::Validate, &control)
            .unwrap()
    );
    assert!(control.units.lock().unwrap().len() < 10);
    assert_eq!(a.resource_facts().logical_elements_upper_bound, 1_000_001);
}
#[test]
fn scalar_factories_do_not_guess_json_or_decimal_precision() {
    let ty = FunctionValueType::try_with_logical_type(
        DataType::Utf8,
        false,
        novarocks_type_contract::ValueLogicalType::Json,
    )
    .unwrap();
    let field = Arc::new(Field::new("literal", DataType::Utf8, false));
    let json = ConstantValue::from_utf8(
        field,
        ty,
        "{}",
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    assert_eq!(
        json.value_type().logical_type(),
        novarocks_type_contract::ValueLogicalType::Json
    );
    assert_eq!(json.try_utf8().unwrap(), Some("{}"));
    let ty = FunctionValueType::new(DataType::Decimal128(2, 0), false);
    let field = Arc::new(Field::new("literal", ty.data_type().clone(), false));
    let error = ConstantValue::from_decimal128(
        field,
        ty,
        123,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap_err();
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert_eq!(error.failure_fact().class(), FailureClass::Data);
    assert_eq!(
        error.failure_fact().reason().domain(),
        novarocks_failure_contract::domain::CONSTANT.get()
    );
    assert_eq!(error.failure_fact().reason().code(), 5);
    let mut source: &(dyn std::error::Error + 'static) = &error;
    loop {
        if let Some(original) = source.downcast_ref::<arrow_schema::ArrowError>() {
            assert!(matches!(
                original,
                arrow_schema::ArrowError::InvalidArgumentError(_)
            ));
            assert!(original.to_string().contains("123"));
            break;
        }
        source = source
            .source()
            .expect("original decimal coefficient SDK refusal");
    }
}

#[test]
fn repeated_views_are_charged_per_validation_reference() {
    let text = "x".repeat(4096);
    let view = arrow_data::ByteView::new(text.len() as u32, &text.as_bytes()[..4]).as_u128();
    let array = arrow_array::StringViewArray::try_new(
        arrow_buffer::ScalarBuffer::from(vec![view; 64]),
        vec![arrow_buffer::Buffer::from(text.as_bytes())],
        None,
    )
    .unwrap();
    let p = pool(Arc::new(array), false);
    assert!(p.resource_facts().library_validation_work_upper_bound >= 64 * 4096);
    let mut bounded = policy();
    bounded.max_library_validation_work = 64 * 4096 - 1;
    assert!(matches!(
        ConstantPool::try_new(
            Arc::new(p.field().clone()),
            p.value_type().clone(),
            p.array().to_data(),
            bounded,
            CompilePhase::Validate,
            &Control::good(),
        ),
        Err(ConstantError::Limit(_))
    ));
}

#[test]
fn library_validation_bytes_fact_is_the_exact_policy_boundary() {
    let array = Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef;
    let baseline = pool(array.clone(), false);
    let facts = baseline.resource_facts();
    assert_eq!(
        facts.library_validation_bytes_upper_bound,
        facts.library_validation_temporary_bytes_upper_bound + 2 * 8,
        "the primitive input inspects its two Int64 values, not buffer capacity"
    );
    let mut bounded = policy();
    bounded.max_library_validation_bytes = facts.library_validation_bytes_upper_bound;
    let accepted = ConstantPool::try_new(
        Arc::new(baseline.field().clone()),
        baseline.value_type().clone(),
        array.to_data(),
        bounded,
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    assert_eq!(accepted.resource_facts(), facts);
    bounded.max_library_validation_bytes -= 1;
    assert_eq!(
        ConstantPool::try_new(
            Arc::new(baseline.field().clone()),
            baseline.value_type().clone(),
            array.to_data(),
            bounded,
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap_err(),
        ConstantError::Limit("opaque Arrow validation byte limit exceeded")
    );
}

#[test]
fn library_validation_bytes_count_shared_children_on_each_actual_visit() {
    let text = "x".repeat(4096);
    let shared = Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef;
    let separate = Arc::new(StringArray::from(vec![text.as_str()])) as ArrayRef;
    let structure = |right: ArrayRef| {
        Arc::new(arrow_array::StructArray::from(vec![
            (
                Arc::new(Field::new("left", DataType::Utf8, false)),
                shared.clone(),
            ),
            (Arc::new(Field::new("right", DataType::Utf8, false)), right),
        ])) as ArrayRef
    };
    let shared_pool = pool(structure(shared.clone()), false);
    let separate_pool = pool(structure(separate), false);
    let shared_facts = shared_pool.resource_facts();
    let separate_facts = separate_pool.resource_facts();
    assert!(
        shared_facts.retained_buffer_capacity_bytes < separate_facts.retained_buffer_capacity_bytes,
        "one shared allocation is retained once"
    );
    assert_eq!(shared_facts.buffer_count, separate_facts.buffer_count);
    assert_eq!(
        shared_facts.library_validation_bytes_upper_bound,
        separate_facts.library_validation_bytes_upper_bound,
        "validation still visits both child references"
    );
    assert_eq!(
        shared_facts.library_validation_work_upper_bound,
        separate_facts.library_validation_work_upper_bound
    );
    let mut bounded = policy();
    bounded.max_retained_buffer_bytes = shared_facts.retained_buffer_capacity_bytes;
    assert!(
        ConstantPool::try_new(
            Arc::new(shared_pool.field().clone()),
            shared_pool.value_type().clone(),
            shared_pool.array().to_data(),
            bounded,
            CompilePhase::Validate,
            &Control::good(),
        )
        .is_ok()
    );
    assert_eq!(
        ConstantPool::try_new(
            Arc::new(separate_pool.field().clone()),
            separate_pool.value_type().clone(),
            separate_pool.array().to_data(),
            bounded,
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap_err(),
        ConstantError::Limit("constant retained buffer limit exceeded")
    );
}

#[test]
fn library_validation_bytes_charge_repeated_view_payloads_with_shared_backing() {
    let text = "x".repeat(4096);
    let view = arrow_data::ByteView::new(text.len() as u32, &text.as_bytes()[..4]).as_u128();
    let array = Arc::new(
        arrow_array::StringViewArray::try_new(
            arrow_buffer::ScalarBuffer::from(vec![view; 64]),
            vec![arrow_buffer::Buffer::from(text.as_bytes())],
            None,
        )
        .unwrap(),
    ) as ArrayRef;
    let all = pool(array.clone(), false);
    let one = pool(array.slice(0, 1), false);
    let facts = all.resource_facts();
    let one_facts = one.resource_facts();
    assert_eq!(
        facts.retained_buffer_capacity_bytes, one_facts.retained_buffer_capacity_bytes,
        "a sliced view keeps the original view table and payload allocations"
    );
    assert!(
        facts.library_validation_bytes_upper_bound > one_facts.library_validation_bytes_upper_bound
    );
    assert!(
        facts.library_validation_bytes_upper_bound
            - facts.library_validation_temporary_bytes_upper_bound
            >= 64 * text.len() as u64,
        "opaque string validation scans each repeated payload reference"
    );
    let mut bounded = policy();
    bounded.max_library_validation_bytes = facts.library_validation_bytes_upper_bound;
    assert!(
        ConstantPool::try_new(
            Arc::new(all.field().clone()),
            all.value_type().clone(),
            all.array().to_data(),
            bounded,
            CompilePhase::Validate,
            &Control::good(),
        )
        .is_ok()
    );
    bounded.max_library_validation_bytes -= 1;
    assert_eq!(
        ConstantPool::try_new(
            Arc::new(all.field().clone()),
            all.value_type().clone(),
            all.array().to_data(),
            bounded,
            CompilePhase::Validate,
            &Control::good(),
        )
        .unwrap_err(),
        ConstantError::Limit("opaque Arrow validation byte limit exceeded")
    );
}

#[test]
fn empty_variadic_buffers_still_have_header_cost() {
    let array = arrow_array::StringViewArray::try_new(
        arrow_buffer::ScalarBuffer::from(Vec::<u128>::new()),
        vec![arrow_buffer::Buffer::from(Vec::<u8>::new()); 512],
        None,
    )
    .unwrap();
    let p = pool(Arc::new(array), false);
    let facts = p.resource_facts();
    assert_eq!(facts.buffer_count, 513);
    assert!(
        facts.library_validation_temporary_bytes_upper_bound
            >= 513 * std::mem::size_of::<arrow_buffer::Buffer>() as u64
    );
}

#[test]
fn selected_encoded_child_keeps_exact_nonnull_field() {
    for nullable in [false, true] {
        let fields: arrow_schema::UnionFields = vec![(
            0,
            Arc::new(Field::new("selected", DataType::Int64, nullable)),
        )]
        .into_iter()
        .collect();
        let dtype = DataType::Union(fields, UnionMode::Dense);
        let data = ArrayData::builder(dtype.clone())
            .len(1)
            .buffers(vec![
                arrow_buffer::Buffer::from_slice_ref([0i8]),
                arrow_buffer::Buffer::from_slice_ref([0i32]),
            ])
            .child_data(vec![Int64Array::from(vec![None]).to_data()])
            .build()
            .unwrap();
        let array = make_array(data);
        let result = ConstantPool::try_new(
            Arc::new(Field::new("literal", dtype.clone(), true)),
            FunctionValueType::new(dtype, true),
            array.to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        );
        assert_eq!(result.is_ok(), nullable);
        if let Ok(p) = result {
            assert!(
                p.value(0)
                    .unwrap()
                    .is_null_observed(CompilePhase::Validate, &Control::good())
                    .unwrap()
            );
        }

        let dtype = DataType::RunEndEncoded(
            Arc::new(Field::new("ends", DataType::Int16, false)),
            Arc::new(Field::new("values", DataType::Int64, nullable)),
        );
        let data = ArrayData::builder(dtype.clone())
            .len(1)
            .child_data(vec![
                arrow_array::Int16Array::from(vec![1]).to_data(),
                Int64Array::from(vec![None]).to_data(),
            ])
            .build()
            .unwrap();
        let result = ConstantPool::try_new(
            Arc::new(Field::new("literal", dtype.clone(), true)),
            FunctionValueType::new(dtype, true),
            make_array(data).to_data(),
            policy(),
            CompilePhase::Validate,
            &Control::good(),
        );
        assert_eq!(result.is_ok(), nullable);
    }
}

#[test]
fn null_parent_preserves_masked_nonnull_child_payload() {
    let dtype =
        DataType::Struct(vec![Arc::new(Field::new("child", DataType::Int64, false))].into());
    let data = ArrayData::builder(dtype.clone())
        .len(1)
        .nulls(Some(arrow_buffer::NullBuffer::new_null(1)))
        .child_data(vec![Int64Array::from(vec![None]).to_data()])
        .build()
        .unwrap();
    let p = pool(make_array(data), true);
    assert!(
        p.value(0)
            .unwrap()
            .is_null_observed(CompilePhase::Validate, &Control::good())
            .unwrap()
    );
}

#[test]
fn raw_parent_offset_extent_checked_before_canonicalization() {
    let dtype =
        DataType::Struct(vec![Arc::new(Field::new("child", DataType::Int64, false))].into());
    let raw = ArrayData::builder(dtype)
        .len(1)
        .offset(1)
        .child_data(vec![Int64Array::from(vec![11, 22]).to_data()])
        .build()
        .unwrap();
    let accepted = ConstantPool::try_new(
        Arc::new(Field::new("literal", raw.data_type().clone(), false)),
        FunctionValueType::new(raw.data_type().clone(), false),
        raw,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let canonical = accepted.array().to_data();
    assert_eq!(canonical.offset(), 0);
    assert_eq!(canonical.child_data()[0].buffer::<i64>(0)[0], 22);
    let expected = pool(
        Arc::new(arrow_array::StructArray::from(vec![(
            Arc::new(Field::new("child", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![22])) as ArrayRef,
        )])),
        false,
    );
    assert!(eq(&accepted.value(0).unwrap(), &expected.value(0).unwrap()));

    let dtype = DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), 1);
    let rejected = ArrayData::builder(dtype.clone())
        .len(1)
        .offset(1)
        .nulls(Some(arrow_buffer::NullBuffer::new_null(1)))
        .child_data(vec![Int64Array::from(vec![None]).to_data()])
        .build()
        .unwrap_err();
    assert!(matches!(
        rejected,
        arrow_schema::ArrowError::InvalidArgumentError(message)
            if message == "Values length 1 is less than the length + offset (2) multiplied by the value size (1) for FixedSizeList(1 x Int64)"
    ));
    // A legal source has enough child extent even under a NULL parent. It
    // still exercises the original pool's offset canonicalization and NULL.
    let raw = ArrayData::builder(dtype)
        .len(1)
        .offset(1)
        .nulls(Some(arrow_buffer::NullBuffer::new_null(1)))
        .child_data(vec![Int64Array::from(vec![Some(11), None]).to_data()])
        .build()
        .unwrap();
    let accepted = ConstantPool::try_new(
        Arc::new(Field::new("literal", raw.data_type().clone(), true)),
        FunctionValueType::new(raw.data_type().clone(), true),
        raw,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let canonical = accepted.array().to_data();
    assert_eq!(canonical.offset(), 0);
    assert_eq!(canonical.len(), 1);
    assert!(
        accepted
            .value(0)
            .unwrap()
            .is_null_observed(CompilePhase::Validate, &Control::good())
            .unwrap()
    );
}

#[test]
fn factories_reject_invalid_grammar_and_large_extents_before_arrow_allocation() {
    let invalid = [
        DataType::Time32(TimeUnit::Nanosecond),
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int64, true)), -1),
    ];
    for dtype in invalid {
        assert!(
            FunctionValueType::try_with_logical_type(dtype, true, ValueLogicalType::Physical)
                .is_err()
        );
    }
    // Arrow 60.1 validates identities before a safe Union DataType can exist.
    for (ids, expected) in [
        (vec![-1], "type ids must be non-negative: -1"),
        (vec![0, 0], "duplicate type id: 0"),
    ] {
        let children = ids
            .iter()
            .map(|_| Arc::new(Field::new("item", DataType::Int64, true)));
        let source = arrow_schema::UnionFields::try_new(ids.clone(), children).unwrap_err();
        assert!(
            matches!(source, arrow_schema::ArrowError::InvalidArgumentError(actual) if actual == expected)
        );
    }
    let dtype = DataType::FixedSizeList(
        Arc::new(Field::new("item", DataType::Int64, true)),
        i32::MAX,
    );
    assert!(matches!(
        ConstantValue::null(
            Arc::new(Field::new("literal", dtype.clone(), true)),
            FunctionValueType::new(dtype, true),
            policy(),
            CompilePhase::Validate,
            &Control::good()
        ),
        Err(ConstantError::Limit(_))
    ));
    let mut bounded = policy();
    bounded.max_retained_buffer_bytes = 4;
    assert!(matches!(
        ConstantValue::from_utf8(
            Arc::new(Field::new("literal", DataType::Utf8, false)),
            FunctionValueType::new(DataType::Utf8, false),
            "payload larger than bound",
            bounded,
            CompilePhase::Validate,
            &Control::good()
        ),
        Err(ConstantError::Limit(_))
    ));
}

#[test]
fn union_tag_lookup_observes_each_candidate_across_repeated_reads() {
    let fields: arrow_schema::UnionFields = (0..128i16)
        .map(|id| {
            (
                id as i8,
                Arc::new(Field::new("value", DataType::Int64, false)),
            )
        })
        .collect();
    let dtype = DataType::Union(fields, UnionMode::Dense);
    let data = ArrayData::builder(dtype.clone())
        .len(1)
        .buffers(vec![
            arrow_buffer::Buffer::from_slice_ref([127i8]),
            arrow_buffer::Buffer::from_slice_ref([0i32]),
        ])
        .child_data(
            (0..128)
                .map(|_| Int64Array::from(vec![7]).to_data())
                .collect(),
        )
        .build()
        .unwrap();
    let p = ConstantPool::try_new(
        Arc::new(Field::new("literal", dtype.clone(), false)),
        FunctionValueType::new(dtype, false),
        data,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let control = Control {
        failure: Some(CompileControlError::Cancelled),
        at_positive: true,
        units: Mutex::new(vec![]),
    };
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
    assert!(union_row(&p.0.data, 0, &mut work).is_ok());
    assert!(matches!(
        union_row(&p.0.data, 0, &mut work),
        Err(ConstantError::Control(CompileControlError::Cancelled))
    ));
    assert_eq!(*control.units.lock().unwrap(), vec![0, 256]);
}

fn payload(value: &ConstantValue) -> u64 {
    value
        .selected_payload_bytes_observed(CompilePhase::Validate, &Control::good())
        .unwrap()
}

#[test]
fn unsigned_accessors_preserve_exact_carriers_and_real_null() {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(arrow_array::UInt8Array::from(vec![Some(u8::MAX), None])),
        Arc::new(arrow_array::UInt16Array::from(vec![Some(u16::MAX), None])),
        Arc::new(arrow_array::UInt32Array::from(vec![Some(u32::MAX), None])),
        Arc::new(arrow_array::UInt64Array::from(vec![Some(u64::MAX), None])),
    ];
    for (array, maximum) in
        arrays
            .into_iter()
            .zip([u8::MAX as u64, u16::MAX as u64, u32::MAX as u64, u64::MAX])
    {
        let p = pool(array, true);
        assert_eq!(p.value(0).unwrap().try_u64().unwrap(), Some(maximum));
        assert_eq!(p.value(1).unwrap().try_u64().unwrap(), None);
        assert!(p.value(0).unwrap().try_i64().is_err());
    }
    for array in [
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        Arc::new(arrow_array::TimestampNanosecondArray::from(vec![1])) as ArrayRef,
        Arc::new(arrow_array::NullArray::new(1)) as ArrayRef,
    ] {
        assert!(pool(array, true).value(0).unwrap().try_u64().is_err());
    }
}

#[test]
fn selected_payload_excludes_pool_rows_padding_and_headers() {
    let huge = "x".repeat(100_000);
    let p = pool(
        Arc::new(StringArray::from(vec![
            Some("é"),
            Some(huge.as_str()),
            None,
        ])),
        true,
    );
    assert_eq!(payload(&p.value(0).unwrap()), 2);
    assert_eq!(payload(&p.value(1).unwrap()), 100_000);
    assert_eq!(payload(&p.value(2).unwrap()), 0);
    assert!(p.resource_facts().retained_buffer_capacity_bytes > 100_002);
    assert!(Arc::ptr_eq(
        &p.value(0).unwrap().pool.0,
        &p.value(1).unwrap().pool.0
    ));
    let boolean = pool(
        Arc::new(arrow_array::BooleanArray::from(vec![Some(true), None])),
        true,
    );
    assert_eq!(payload(&boolean.value(0).unwrap()), 1);
    assert_eq!(payload(&boolean.value(1).unwrap()), 0);
    assert_eq!(
        payload(
            &pool(
                Arc::new(arrow_array::UInt64Array::from(vec![u64::MAX])),
                false
            )
            .value(0)
            .unwrap()
        ),
        8
    );
    let decimal = pool(
        Arc::new(
            arrow_array::Decimal256Array::from(vec![arrow_buffer::i256::from_i128(1)])
                .with_precision_and_scale(60, 0)
                .unwrap(),
        ),
        false,
    );
    assert_eq!(payload(&decimal.value(0).unwrap()), 32);
}

#[test]
fn selected_payload_decodes_dictionary_and_run_end_values() {
    let huge = "z".repeat(50_000);
    let values = Arc::new(StringArray::from(vec![
        Some("q"),
        Some(huge.as_str()),
        None,
    ])) as ArrayRef;
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(0), Some(1), Some(2), None]),
        values.clone(),
    )
    .unwrap();
    let p = pool(Arc::new(dictionary), true);
    assert_eq!(payload(&p.value(0).unwrap()), 1);
    assert_eq!(payload(&p.value(1).unwrap()), 50_000);
    assert_eq!(payload(&p.value(2).unwrap()), 0);
    assert_eq!(payload(&p.value(3).unwrap()), 0);
    assert!(p.value(0).unwrap().try_u64().is_err());

    let dtype = DataType::RunEndEncoded(
        Arc::new(Field::new("ends", DataType::Int16, false)),
        Arc::new(Field::new("values", DataType::Utf8, true)),
    );
    let data = ArrayData::builder(dtype.clone())
        .len(10_000)
        .child_data(vec![
            arrow_array::Int16Array::from(vec![1, 2, 10_000]).to_data(),
            values.to_data(),
        ])
        .build()
        .unwrap();
    let p = ConstantPool::try_new(
        Arc::new(Field::new("literal", dtype.clone(), true)),
        FunctionValueType::new(dtype, true),
        data,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    assert_eq!(payload(&p.value(0).unwrap()), 1);
    assert_eq!(payload(&p.value(1).unwrap()), 50_000);
    assert_eq!(payload(&p.value(2).unwrap()), 0);
    assert_eq!(payload(&p.value(9_999).unwrap()), 0);
}

#[test]
fn selected_payload_counts_nested_selected_children_and_parent_null_masks() {
    let list =
        arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>(vec![
            Some(vec![Some(1), None, Some(3)]),
        ]);
    let columns: Vec<(Arc<Field>, ArrayRef)> = vec![
        (
            Arc::new(Field::new("flag", DataType::Boolean, false)),
            Arc::new(arrow_array::BooleanArray::from(vec![true])),
        ),
        (
            Arc::new(Field::new("text", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["é"])),
        ),
        (
            Arc::new(Field::new("items", list.data_type().clone(), false)),
            Arc::new(list),
        ),
    ];
    let p = pool(Arc::new(arrow_array::StructArray::from(columns)), false);
    assert_eq!(payload(&p.value(0).unwrap()), 19);
    let data = p
        .array()
        .to_data()
        .into_builder()
        .nulls(Some(arrow_buffer::NullBuffer::new_null(1)))
        .build()
        .unwrap();
    let nullable = ConstantPool::try_new(
        Arc::new(Field::new("literal", data.data_type().clone(), true)),
        FunctionValueType::new(data.data_type().clone(), true),
        data,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    assert_eq!(payload(&nullable.value(0).unwrap()), 0);

    let fields: arrow_schema::UnionFields = vec![
        (0, Arc::new(Field::new("selected", DataType::Utf8, true))),
        (1, Arc::new(Field::new("unused", DataType::Utf8, true))),
    ]
    .into_iter()
    .collect();
    let dtype = DataType::Union(fields, UnionMode::Dense);
    let data = ArrayData::builder(dtype.clone())
        .len(2)
        .buffers(vec![
            arrow_buffer::Buffer::from_slice_ref([0i8, 0]),
            arrow_buffer::Buffer::from_slice_ref([0i32, 1]),
        ])
        .child_data(vec![
            StringArray::from(vec![Some("one"), None]).to_data(),
            StringArray::from(vec!["x".repeat(50_000)]).to_data(),
        ])
        .build()
        .unwrap();
    let p = ConstantPool::try_new(
        Arc::new(Field::new("literal", dtype.clone(), true)),
        FunctionValueType::new(dtype, true),
        data,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    assert_eq!(payload(&p.value(0).unwrap()), 3);
    assert_eq!(payload(&p.value(1).unwrap()), 0);
}

#[test]
fn selected_map_payload_counts_entry_values_without_offset_storage() {
    let fields: arrow_schema::Fields = vec![
        Arc::new(Field::new("key", DataType::Utf8, false)),
        Arc::new(Field::new("value", DataType::Utf8, true)),
    ]
    .into();
    let entries_type = DataType::Struct(fields);
    let entries = ArrayData::builder(entries_type.clone())
        .len(2)
        .child_data(vec![
            StringArray::from(vec!["a", "bb"]).to_data(),
            StringArray::from(vec![Some("é"), None]).to_data(),
        ])
        .build()
        .unwrap();
    let dtype = DataType::Map(Arc::new(Field::new("entries", entries_type, false)), false);
    let data = ArrayData::builder(dtype.clone())
        .len(1)
        .buffers(vec![arrow_buffer::Buffer::from_slice_ref([0i32, 2])])
        .child_data(vec![entries])
        .build()
        .unwrap();
    let p = ConstantPool::try_new(
        Arc::new(Field::new("literal", dtype.clone(), false)),
        FunctionValueType::new(dtype, false),
        data,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    assert_eq!(payload(&p.value(0).unwrap()), 5);
}

#[test]
fn selected_payload_controls_are_typed_at_entry_and_actual_256_work() {
    let list =
        arrow_array::ListArray::from_iter_primitive::<arrow_array::types::Int64Type, _, _>(vec![
            Some(vec![Some(1); 512]),
        ]);
    let p = pool(Arc::new(list), false);
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for at_positive in [false, true] {
            let control = Control {
                failure: Some(error.clone()),
                at_positive,
                units: Mutex::new(vec![]),
            };
            assert_eq!(
                p.value(0)
                    .unwrap()
                    .selected_payload_bytes_observed(CompilePhase::Validate, &control),
                Err(ConstantError::Control(error.clone()))
            );
            assert_eq!(
                *control.units.lock().unwrap(),
                if at_positive { vec![0, 256] } else { vec![0] }
            );
        }
    }
    let dtype = DataType::List(Arc::new(Field::new("item", DataType::Null, true)));
    let data = ArrayData::builder(dtype.clone())
        .len(1)
        .buffers(vec![arrow_buffer::Buffer::from_slice_ref([0i32, 500_000])])
        .child_data(vec![arrow_array::NullArray::new(500_000).to_data()])
        .build()
        .unwrap();
    let p = ConstantPool::try_new(
        Arc::new(Field::new("literal", dtype.clone(), false)),
        FunctionValueType::new(dtype, false),
        data,
        policy(),
        CompilePhase::Validate,
        &Control::good(),
    )
    .unwrap();
    let control = Control::good();
    assert_eq!(
        p.value(0)
            .unwrap()
            .selected_payload_bytes_observed(CompilePhase::Validate, &control)
            .unwrap(),
        0
    );
    assert!(control.units.lock().unwrap().len() < 10);
}

#[test]
fn raw_decimal_values_exceeding_exact_precision_are_rejected() {
    let values = vec![
        (
            DataType::Decimal32(2, 0),
            arrow_buffer::Buffer::from_slice_ref([123i32]),
        ),
        (
            DataType::Decimal64(2, 0),
            arrow_buffer::Buffer::from_slice_ref([123i64]),
        ),
        (
            DataType::Decimal128(2, 0),
            arrow_buffer::Buffer::from_slice_ref([123i128]),
        ),
        (
            DataType::Decimal256(2, 0),
            arrow_buffer::Buffer::from_slice_ref([arrow_buffer::i256::from_i128(123)]),
        ),
    ];
    for (dtype, buffer) in values {
        // Arrow structural validation permits the raw fixed-width carrier;
        // the constant owner additionally enforces declared decimal precision.
        let data = ArrayData::builder(dtype.clone())
            .len(1)
            .buffers(vec![buffer])
            .build()
            .unwrap();
        data.validate_full().unwrap();
        assert!(matches!(
            ConstantPool::try_new(
                Arc::new(Field::new("literal", dtype.clone(), false)),
                FunctionValueType::new(dtype, false),
                data,
                policy(),
                CompilePhase::Validate,
                &Control::good()
            ),
            Err(ConstantError::Source(_))
        ));
    }
}

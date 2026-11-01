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
use arrow_array::*;
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_schema::{IntervalUnit, TimeUnit};
use std::{collections::HashMap, sync::Mutex};

const PHASE: CompilePhase = CompilePhase::Decode;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct Control {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    failure: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            failure: None,
        }
    }
    fn refusing(at: usize, cause: CompileControlError) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            failure: Some((at, cause)),
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((refusal, _)) = self.failure {
            assert!(at <= refusal, "callback after primary refusal");
        }
        trace.push((phase, units));
        match self.failure.clone() {
            Some((refusal, cause)) if at == refusal => Err(cause),
            _ => Ok(()),
        }
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
fn resource_input(array: &dyn Array, view_bytes: u64) -> FlatConstantResourceInput {
    let data = array.to_data();
    // Public buffer lengths/capacities form deliberately conservative fixture
    // geometry. No post resource facts or private scanner feed this input.
    let mut visits = 0;
    let mut capacity = 0;
    for buffer in data
        .buffers()
        .iter()
        .chain(data.nulls().map(|nulls| nulls.buffer()))
    {
        visits += buffer.len() as u64;
        capacity += buffer.capacity().max(buffer.len()) as u64;
    }
    // The official UTF8 validator first scans the full values descriptor and
    // can then validate each referenced range; a successful fallback can visit
    // its payload twice, including when an unused suffix is invalid UTF8.
    if matches!(array.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
        visits += data.buffers()[1].len() as u64;
    }
    let count = match array.data_type() {
        DataType::Null => 0,
        DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary => 3,
        DataType::Utf8View | DataType::BinaryView => data.buffers().len() as u64 + 1,
        _ => 2,
    };
    FlatConstantResourceInput {
        rows: array.len() as u64,
        buffer_count_upper_bound: count,
        buffer_visits_bytes_upper_bound: visits,
        retained_buffer_capacity_bytes_upper_bound: capacity,
        view_validation_bytes_upper_bound: view_bytes,
    }
}
fn prefixes(
    field: &Field,
    ty: &FunctionValueType,
    input: FlatConstantResourceInput,
    bound: ConstantPolicy,
    trace: &[(CompilePhase, u32)],
) {
    for at in 0..trace.len() {
        for cause in CAUSES {
            let control = Control::refusing(at, cause.clone());
            assert!(
                matches!(preflight_flat_pool_resources(field,ty,input,bound,PHASE,&control),Err(ConstantError::Control(actual)) if actual==cause)
            );
            assert_eq!(control.trace(), trace[..=at]);
        }
    }
}
fn checked(
    field: &Field,
    ty: &FunctionValueType,
    input: FlatConstantResourceInput,
    bound: ConstantPolicy,
) -> FlatConstantResourceBounds {
    let control = Control::good();
    let result = preflight_flat_pool_resources(field, ty, input, bound, PHASE, &control).unwrap();
    prefixes(field, ty, input, bound, &control.trace());
    result
}
fn ordinary(
    field: &Field,
    ty: &FunctionValueType,
    input: FlatConstantResourceInput,
    bound: ConstantPolicy,
) {
    let control = Control::good();
    let error =
        preflight_flat_pool_resources(field, ty, input, bound, PHASE, &control).unwrap_err();
    assert!(!matches!(error, ConstantError::Control(_)));
    let trace = control.trace();
    assert!(trace.len() >= 2, "ordinary failure omitted its tail");
    prefixes(field, ty, input, bound, &trace);
}
fn dominate(array: ArrayRef, view_bytes: u64) {
    let field = Arc::new(Field::new("source", array.data_type().clone(), true));
    let ty = FunctionValueType::new(array.data_type().clone(), true);
    let input = resource_input(array.as_ref(), view_bytes);
    let bound = checked(&field, &ty, input, policy());
    let pool = ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy(),
        PHASE,
        &Control::good(),
    )
    .unwrap();
    let actual = pool.resource_facts();
    assert_eq!(actual.rows, input.rows);
    assert_eq!(actual.array_nodes, 1);
    assert!(actual.buffer_count <= input.buffer_count_upper_bound);
    assert!(
        actual.retained_buffer_capacity_bytes <= input.retained_buffer_capacity_bytes_upper_bound
    );
    assert_eq!(actual.metadata_bytes, bound.metadata_bytes);
    assert!(
        actual.library_validation_work_upper_bound <= bound.library_validation_work_upper_bound
    );
    assert!(
        actual.library_validation_temporary_bytes_upper_bound
            <= bound.library_validation_temporary_bytes_upper_bound
    );
    assert!(
        actual.library_validation_bytes_upper_bound <= bound.library_validation_bytes_upper_bound
    );
}

#[test]
fn genuine_flat_arrays_and_slices_are_dominated_without_fabricated_arraydata() {
    let mut types = vec![
        DataType::Null,
        DataType::Boolean,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Float16,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal32(9, -1),
        DataType::Decimal64(18, 2),
        DataType::Decimal128(38, -2),
        DataType::Decimal256(76, 38),
        DataType::Date32,
        DataType::Date64,
        DataType::Time32(TimeUnit::Second),
        DataType::Time32(TimeUnit::Millisecond),
        DataType::Time64(TimeUnit::Microsecond),
        DataType::Time64(TimeUnit::Nanosecond),
        DataType::Interval(IntervalUnit::YearMonth),
        DataType::Interval(IntervalUnit::DayTime),
        DataType::Interval(IntervalUnit::MonthDayNano),
        DataType::FixedSizeBinary(0),
        DataType::FixedSizeBinary(16),
        DataType::Utf8,
        DataType::LargeUtf8,
        DataType::Binary,
        DataType::LargeBinary,
        DataType::Utf8View,
        DataType::BinaryView,
    ];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        types.push(DataType::Timestamp(unit, None));
        types.push(DataType::Timestamp(unit, Some(Arc::from(""))));
        types.push(DataType::Duration(unit));
    }
    for ty in types {
        let array = new_null_array(&ty, 3);
        dominate(array.clone(), 0);
        dominate(array.slice(1, 2), 0);
        dominate(new_empty_array(&ty), 0);
    }
    for array in [
        Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])) as ArrayRef,
        Arc::new(Float32Array::from(vec![
            Some(f32::from_bits(0x80000000)),
            None,
            Some(f32::from_bits(0x7fc00071)),
        ])),
        Arc::new(
            Decimal128Array::from(vec![Some(-155), None, Some(155)])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        ),
        Arc::new(StringArray::from(vec![Some("雪\0"), None, Some("🙂")])),
        Arc::new(LargeStringArray::from(vec![Some("雪\0"), None, Some("🙂")])),
        Arc::new(BinaryArray::from(vec![
            Some(b"\0\xff".as_slice()),
            None,
            Some(b"a".as_slice()),
        ])),
        Arc::new(LargeBinaryArray::from(vec![
            Some(b"\0\xff".as_slice()),
            None,
            Some(b"a".as_slice()),
        ])),
    ] {
        dominate(array.clone(), 0);
        dominate(array.slice(1, 2), 0);
    }
}

#[test]
fn repeated_and_null_view_records_preserve_work_distinct_from_unique_backing() {
    let payload = Buffer::from(b"aaaaaaaaaaaaa".to_vec());
    let record = long_view(0, b'a');
    let views = ScalarBuffer::from(vec![record; 3]);
    for array in [
        Arc::new(StringViewArray::new(
            views.clone(),
            vec![payload.clone()],
            Some(arrow_buffer::NullBuffer::from(vec![true, false, true])),
        )) as ArrayRef,
        Arc::new(BinaryViewArray::new(
            views,
            vec![payload],
            Some(arrow_buffer::NullBuffer::from(vec![true, false, true])),
        )),
    ] {
        // Every record has length13, including the actual NULL row. The single
        // 13-byte backing does not reduce the 39-byte repeated inspection.
        dominate(array.clone(), 39);
        dominate(array.slice(1, 2), 26);
        let input = resource_input(array.as_ref(), 39);
        let field = Field::new("source", array.data_type().clone(), true);
        let ty = FunctionValueType::new(array.data_type().clone(), true);
        let actual = checked(&field, &ty, input, policy());
        let unique_only = checked(
            &field,
            &ty,
            FlatConstantResourceInput {
                view_validation_bytes_upper_bound: 13,
                ..input
            },
            policy(),
        );
        assert_eq!(
            actual.library_validation_work_upper_bound
                - unique_only.library_validation_work_upper_bound,
            26
        );
        assert_eq!(
            actual.library_validation_bytes_upper_bound
                - unique_only.library_validation_bytes_upper_bound,
            26
        );
        // unique_only is deliberately NOT an admission proof for this array;
        // only the correctly derived input is compared with the actual owner.
    }
}
fn long_view(index: u32, prefix: u8) -> u128 {
    let mut bytes = [prefix; 16];
    bytes[..4].copy_from_slice(&13u32.to_le_bytes());
    bytes[8..12].copy_from_slice(&index.to_le_bytes());
    bytes[12..].copy_from_slice(&0u32.to_le_bytes());
    u128::from_ne_bytes(bytes)
}

#[test]
fn source_attributes_never_reauthor_complete_type_and_timezone_presence_is_exact() {
    let array: ArrayRef = Arc::new(StringArray::from(vec!["{\"v\":7}"]));
    let input = resource_input(array.as_ref(), 0);
    let plain = Field::new("source", DataType::Utf8, true);
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let labelled = plain.clone().with_metadata(HashMap::from([(
        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_owned(),
        "json".to_owned(),
    )]));
    for field in [&plain, &labelled] {
        checked(field, &json, input, policy());
        let pool = ConstantPool::try_new(
            Arc::new(field.clone()),
            json.clone(),
            array.to_data(),
            policy(),
            PHASE,
            &Control::good(),
        )
        .unwrap();
        assert_eq!(pool.value_type().logical_type(), ValueLogicalType::Json);
        assert_eq!(pool.field().metadata(), field.metadata());
    }
    checked(
        &labelled,
        &FunctionValueType::new(DataType::Utf8, true),
        input,
        policy(),
    );
    ordinary(
        &plain,
        &FunctionValueType::new(DataType::Utf8, false),
        input,
        policy(),
    );
    ordinary(
        &plain,
        &FunctionValueType::new(DataType::Binary, true),
        input,
        policy(),
    );
    let unknown = plain.clone().with_metadata(HashMap::from([(
        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.to_owned(),
        "unknown".to_owned(),
    )]));
    checked(&unknown, &json, input, policy());
    assert!(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::Json
        )
        .is_err()
    );
    let temporal = resource_input(&TimestampNanosecondArray::from(vec![7]), 0);
    let absent = DataType::Timestamp(TimeUnit::Nanosecond, None);
    let empty = DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("")));
    for ty in [&absent, &empty] {
        checked(
            &Field::new("source", ty.clone(), true),
            &FunctionValueType::new(ty.clone(), true),
            temporal,
            policy(),
        );
    }
    ordinary(
        &Field::new("source", absent, true),
        &FunctionValueType::new(empty, true),
        temporal,
        policy(),
    );
}

#[test]
fn every_existing_flat_policy_has_exact_near_and_over_boundary_with_control_tails() {
    let array = Int64Array::from(vec![Some(7), None, Some(-1)]);
    let input = resource_input(&array, 0);
    let field = Field::new("source", DataType::Int64, true);
    let ty = FunctionValueType::new(DataType::Int64, true);
    let measured = checked(&field, &ty, input, policy());
    assert_eq!(measured.metadata_bytes, 6);
    let exact = ConstantPolicy {
        max_rows: 3,
        max_array_nodes: 1,
        max_logical_elements: 3,
        max_retained_buffer_bytes: input.retained_buffer_capacity_bytes_upper_bound,
        max_type_depth: 1,
        max_type_nodes: 1,
        max_dictionary_depth: 0,
        max_metadata_bytes: 6,
        max_library_validation_work: measured.library_validation_work_upper_bound,
        max_library_validation_bytes: measured.library_validation_bytes_upper_bound,
    };
    checked(&field, &ty, input, exact);
    for bound in [
        ConstantPolicy {
            max_rows: 2,
            ..exact
        },
        ConstantPolicy {
            max_array_nodes: 0,
            ..exact
        },
        ConstantPolicy {
            max_logical_elements: 2,
            ..exact
        },
        ConstantPolicy {
            max_retained_buffer_bytes: input.retained_buffer_capacity_bytes_upper_bound - 1,
            ..exact
        },
        ConstantPolicy {
            max_type_depth: 0,
            ..exact
        },
        ConstantPolicy {
            max_type_nodes: 0,
            ..exact
        },
        ConstantPolicy {
            max_metadata_bytes: 5,
            ..exact
        },
        ConstantPolicy {
            max_library_validation_work: measured.library_validation_work_upper_bound - 1,
            ..exact
        },
        ConstantPolicy {
            max_library_validation_bytes: measured.library_validation_bytes_upper_bound - 1,
            ..exact
        },
    ] {
        let error =
            preflight_flat_pool_resources(&field, &ty, input, bound, PHASE, &Control::good())
                .unwrap_err();
        assert!(matches!(error, ConstantError::Limit(_)));
        ordinary(&field, &ty, input, bound);
    }
    // Flat dictionary depth is exactly zero, so policy0 must succeed. There is
    // no negative unsigned policy value to invent as an over-bound fixture.
}

#[test]
fn arithmetic_overflow_and_nonflat_inputs_refuse_before_any_pool_publication() {
    let field = Field::new("source", DataType::Int64, true);
    let ty = FunctionValueType::new(DataType::Int64, true);
    let input = FlatConstantResourceInput {
        rows: 1,
        buffer_count_upper_bound: 2,
        buffer_visits_bytes_upper_bound: 8,
        retained_buffer_capacity_bytes_upper_bound: 64,
        view_validation_bytes_upper_bound: 0,
    };
    let unlimited = ConstantPolicy {
        max_rows: u64::MAX,
        max_array_nodes: u64::MAX,
        max_logical_elements: u64::MAX,
        max_retained_buffer_bytes: u64::MAX,
        max_type_depth: u32::MAX,
        max_type_nodes: u64::MAX,
        max_dictionary_depth: u32::MAX,
        max_metadata_bytes: u64::MAX,
        max_library_validation_work: u64::MAX,
        max_library_validation_bytes: u64::MAX,
    };
    for bad in [
        FlatConstantResourceInput {
            rows: u64::MAX,
            ..input
        },
        FlatConstantResourceInput {
            buffer_count_upper_bound: u64::MAX,
            ..input
        },
        FlatConstantResourceInput {
            buffer_visits_bytes_upper_bound: u64::MAX,
            view_validation_bytes_upper_bound: 1,
            ..input
        },
    ] {
        ordinary(&field, &ty, bad, unlimited);
    }
    for dtype in [
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        DataType::Struct(vec![Field::new("item", DataType::Int64, true)].into()),
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
    ] {
        let source = Field::new("source", dtype.clone(), true);
        let value_type = FunctionValueType::new(dtype, true);
        assert!(matches!(
            preflight_flat_pool_resources(
                &source,
                &value_type,
                input,
                policy(),
                PHASE,
                &Control::good()
            ),
            Err(ConstantError::Invalid(_))
        ));
        ordinary(&source, &value_type, input, policy());
    }
}

#[test]
fn wide_real_flat_field_metadata_reaches_quantum_and_keeps_every_primary_prefix() {
    let array = Int64Array::from(vec![7]);
    let input = resource_input(&array, 0);
    let field = Field::new("source", DataType::Int64, true).with_metadata(
        (0..320)
            .map(|i| (format!("provider_{i}"), format!("fact_{i}")))
            .collect::<std::collections::HashMap<String, String>>(),
    );
    let ty = FunctionValueType::new(DataType::Int64, true);
    let control = Control::good();
    let bounds =
        preflight_flat_pool_resources(&field, &ty, input, policy(), PHASE, &control).unwrap();
    assert!(bounds.metadata_bytes > 320);
    let trace = control.trace();
    assert!(trace.iter().any(|(_, units)| *units == 256));
    prefixes(&field, &ty, input, policy(), &trace);
    ordinary(
        &field,
        &ty,
        input,
        ConstantPolicy {
            max_metadata_bytes: bounds.metadata_bytes - 1,
            ..policy()
        },
    );
}

#[test]
fn allocation_dedup_inline_and_spill_keep_duplicates_and_real_view_owners_exact() {
    let mut set = AllocationSet::default();
    let keys = [(100, 64), (200, 128), (300, 64), (400, 64), (500, 64)];
    for (index, key) in keys.into_iter().enumerate() {
        assert!(set.insert(key));
        for old in &keys[..=index] {
            assert!(!set.insert(*old));
        }
        assert_eq!(set.spill.is_empty(), index < 3);
    }
    assert!(set.insert((100, 128)));
    assert!(!set.insert((100, 128)));
    let payloads: Vec<_> = (*b"abcd")
        .into_iter()
        .map(|byte| Buffer::from(vec![byte; 13]))
        .collect();
    let mut with_duplicate = payloads.clone();
    with_duplicate.push(payloads[0].clone());
    let views = ScalarBuffer::from(vec![
        long_view(0, b'a'),
        long_view(1, b'b'),
        long_view(0, b'a'),
        long_view(2, b'c'),
        long_view(1, b'b'),
        long_view(3, b'd'),
        long_view(4, b'a'),
    ]);
    for array in [
        Arc::new(StringViewArray::new(
            views.clone(),
            with_duplicate.clone(),
            None,
        )) as ArrayRef,
        Arc::new(BinaryViewArray::new(views, with_duplicate, None)),
    ] {
        let data = array.to_data();
        assert_eq!(data.buffers().len(), 6);
        let expected = data.buffers()[..5]
            .iter()
            .map(|b| b.capacity().max(b.len()) as u64)
            .sum::<u64>();
        let field = Arc::new(Field::new("source", array.data_type().clone(), true));
        let ty = FunctionValueType::new(array.data_type().clone(), true);
        let pool =
            ConstantPool::try_new(field, ty, data, policy(), PHASE, &Control::good()).unwrap();
        assert_eq!(
            pool.resource_facts().retained_buffer_capacity_bytes,
            expected
        );
        dominate(array, 7 * 13);
    }
}

#[test]
fn public_pool_backing_request_layout_has_real_payload_and_atomic_header_space() {
    let layout = ConstantPool::backing_allocation_layout();
    assert!(std::alloc::Layout::from_size_align(layout.size(), layout.align()).is_ok());
    assert_eq!(layout, layout.pad_to_align());
    assert!(layout.align() >= std::mem::align_of::<PoolBacking>());
    assert!(
        layout.size()
            >= std::mem::size_of::<PoolBacking>()
                + 2 * std::mem::size_of::<std::sync::atomic::AtomicUsize>()
    );
    // These are actual owner types and a locked std header property, not a
    // PlanCodec private mirror or an allocator/RSS/MEM measurement.
}

#[test]
fn successful_utf8_fallback_with_unused_invalid_suffix_covers_both_payload_visits() {
    let data = ArrayData::builder(DataType::Utf8)
        .len(1)
        .buffers(vec![
            Buffer::from_slice_ref([0i32, 3]),
            Buffer::from(b"abc\xff".to_vec()),
        ])
        .build()
        .unwrap();
    let source: ArrayRef = Arc::new(StringArray::from(data));
    assert_eq!(
        source
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "abc"
    );
    let input = resource_input(source.as_ref(), 0);
    // Eight offset bytes plus the four-byte complete data descriptor twice.
    assert_eq!(input.buffer_visits_bytes_upper_bound, 16);
    dominate(source.clone(), 0);
    let field = Arc::new(Field::new("source", DataType::Utf8, true));
    let ty = FunctionValueType::new(DataType::Utf8, true);
    let fallback = ConstantPool::try_new(
        field.clone(),
        ty.clone(),
        source.to_data(),
        policy(),
        PHASE,
        &Control::good(),
    )
    .unwrap();
    let ordinary = ConstantPool::try_new(
        field,
        ty,
        StringArray::from(vec!["abc"]).to_data(),
        policy(),
        PHASE,
        &Control::good(),
    )
    .unwrap();
    // The extra unused byte changes two payload visits. Neither logical value
    // nor node/buffer/header/metadata geometry changed, so the delta is two.
    assert_eq!(
        fallback
            .resource_facts()
            .library_validation_bytes_upper_bound
            - ordinary
                .resource_facts()
                .library_validation_bytes_upper_bound,
        2
    );
}

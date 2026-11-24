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
//! Actual host-backed core probes; no catalogue, default wallet or fake grant.
use super::*;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{AggregateStateAllocator, KernelDiagnostic, KernelEvaluationControl, KernelFailure};
use arrow_array::{Int64Array, StringArray, UInt32Array};
use std::{alloc::Layout, ptr::NonNull, sync::Mutex, time::Duration};
#[derive(Default)]
struct Ledger {
    attempts: usize,
    bytes: usize,
    live: Vec<(usize, Layout)>,
}
#[derive(Default)]
struct Host {
    ledger: Mutex<Ledger>,
    refusal: Option<(usize, KernelFailure)>,
}
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((stop, cause)) = &self.refusal
            && at == *stop
        {
            return Err(cause.clone());
        }
        let ptr = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes += layout.size();
        ledger.live.push((ptr.as_ptr().addr(), layout));
        Ok(ptr)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, actual)| *address == pointer.as_ptr().addr() && *actual == layout)
            .expect("one original actual allocation release");
        ledger.live.swap_remove(at);
        ledger.bytes -= layout.size();
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "no tail after first cause");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("MAP_AGG never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original")),
        KernelFailure::Internal(KernelDiagnostic::new("original")),
        KernelFailure::Operational(KernelDiagnostic::new("original")),
        KernelFailure::InstanceFailed,
    ]
}
fn input() -> ArrayRef {
    Arc::new(StructArray::new(
        vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("v", DataType::Utf8, true),
        ]
        .into(),
        vec![
            Arc::new(StringArray::from(vec![
                Some("x".repeat(600)),
                Some("other".into()),
                Some("x".repeat(600)),
                None,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                Some("first"),
                None,
                Some("ignored"),
                Some("unread"),
            ])) as ArrayRef,
        ],
        None,
    ))
}
fn target() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}
fn update(
    state: &mut MapAggState<HostAggregateAllocator>,
    input: &MapUpdateInput,
    control: &Control,
) -> Result<(), KernelFailure> {
    let mut checkpoints = EvaluationCheckpoints::new(control);
    let mut work = ScalarWork::new(Some(&mut checkpoints));
    for row in 0..input.keys.len() {
        update_row(state, input, row, &mut work)?;
    }
    work.flush()
}
#[test]
fn map_agg_core_actual_host_first_wins_partial_roundtrip_and_last_drop() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut state = MapAggState::new(allocator.clone());
    let input = update_input(&input()).unwrap();
    update(&mut state, &input, &Control::default()).unwrap();
    let partial = build_array(
        &target(),
        std::iter::once(&state),
        &mut ScalarWork::new(None),
    )
    .unwrap();
    let map = partial.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 2]);
    assert_eq!(map.keys().null_count(), 0);
    assert!(!map.entries().fields()[0].is_nullable());
    let keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(keys.value(0), "x".repeat(600));
    assert_eq!(keys.value(1), "other");
    let values = map.values().as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(values.value(0), "first");
    assert!(values.is_null(1));
    let mut merged = MapAggState::new(allocator.clone());
    let input = merge_input(&partial).unwrap();
    merge_row(&mut merged, &input, 0, &mut ScalarWork::new(None)).unwrap();
    assert_eq!(
        build_array(
            &target(),
            std::iter::once(&merged),
            &mut ScalarWork::new(None)
        )
        .unwrap()
        .to_data(),
        partial.to_data()
    );
    drop((merged, state));
    assert_eq!(
        host.ledger.lock().unwrap().bytes,
        allocator.metadata_bytes()
    );
    drop(allocator);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}

#[test]
fn map_agg_temporal_partial_keeps_the_first_zone_and_distinct_fraction() {
    use crate::builtin::temporal_output::TemporalOutputBuilder;
    use novarocks_type_contract::{
        TemporalMicros, TemporalPrecision, TemporalType, TemporalValue, TimeZoneKey, ValueKind,
        ValueTypeStorageLifetime,
    };
    use novarocks_value_arrays::decode::PreparedTemporalArray;

    let temporal = TemporalType::TimestampWithTimeZone(TemporalPrecision::NANOSECONDS);
    let at = |picos, zone| TemporalValue::TimestampWithTimeZone {
        instant: TemporalMicros::new(0, picos).unwrap(),
        zone,
    };
    let first = at(1000, TimeZoneKey::UTC);
    let duplicate = at(1000, TimeZoneKey::from_offset_minutes(60).unwrap());
    let different = at(2000, TimeZoneKey::from_offset_minutes(60).unwrap());
    let mut keys =
        TemporalOutputBuilder::new(crate::temporal_cast::Type::Temporal(temporal), 3).unwrap();
    for value in [first, duplicate, different] {
        keys.temporal(temporal, value).unwrap();
    }
    let keys = keys.finish();
    let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let key_type = ValueTypeRef::try_new(
        ValueKind::Temporal(temporal),
        false,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let value_type = ValueTypeRef::try_new(
        ValueKind::Scalar(DataType::Int64),
        true,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let declaration = ValueTypeRef::try_new(
        ValueKind::Map {
            key: key_type.clone(),
            value: value_type.clone(),
        },
        true,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    let carrier = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", temporal.carrier(), false),
                    Field::new("value", DataType::Int64, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut state = MapAggState::new(allocator.clone());
    let mut work = ScalarWork::new(None);
    let full_key = FunctionValueType::try_from_value(
        key_type.clone(),
        novarocks_type_contract::ValueLayout::try_new(
            keys.data_type(),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap(),
    )
    .unwrap();
    let identity = KeyIdentity::prepare(&full_key, &mut |_| Ok::<_, KernelFailure>(())).unwrap();
    for row in 0..3 {
        update_from_arrays(
            &mut state,
            &identity,
            (&keys, row, Some(&key_type)),
            (&values, row, Some(&value_type)),
            &mut work,
        )
        .unwrap();
    }
    let partial = build_array_checked_states(
        &carrier,
        Some(&declaration),
        std::iter::once(Ok(&state)),
        &mut work,
    )
    .unwrap();
    let map = partial.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 2]);
    let keys = PreparedTemporalArray::bind(temporal, map.keys().as_ref(), false).unwrap();
    assert_eq!(keys.value(0).unwrap(), Some(first));
    assert_eq!(keys.value(1).unwrap(), Some(different));
    assert_eq!(
        map.values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[1, 3]
    );

    let mut merged = MapAggState::new(allocator.clone());
    let merge = merge_input(&partial).unwrap();
    merge_row_typed(
        &mut merged,
        &identity,
        &merge,
        0,
        Some(&declaration),
        &mut work,
    )
    .unwrap();
    let final_output = build_array_checked_states(
        &carrier,
        Some(&declaration),
        std::iter::once(Ok(&merged)),
        &mut work,
    )
    .unwrap();
    assert_eq!(final_output.to_data(), partial.to_data());
    drop((merged, state, allocator));
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}

#[test]
fn map_agg_core_every_update_callback_preserves_seven_causes_and_no_footer() {
    let input = update_input(&input()).unwrap();
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut state = MapAggState::new(allocator);
    let success = Control::default();
    update(&mut state, &input, &success).unwrap();
    let callbacks = success.trace.lock().unwrap().len();
    assert!(success.trace.lock().unwrap().contains(&256));
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    for stop in 0..callbacks {
        for cause in causes() {
            let host = Arc::new(Host::default());
            let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
            let mut state = MapAggState::new(allocator);
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Default::default()
            };
            let error = update(&mut state, &input, &control).unwrap_err();
            let actual = error;
            assert_eq!(actual, cause);
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            drop(state);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}
#[test]
fn map_agg_core_every_actual_host_frontier_preserves_seven_causes_and_drop() {
    let input = update_input(&input()).unwrap();
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut state = MapAggState::new(allocator);
    update(&mut state, &input, &Control::default()).unwrap();
    let allocations = host.ledger.lock().unwrap().attempts;
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    for stop in 0..allocations {
        for cause in causes() {
            let host = Arc::new(Host {
                refusal: Some((stop, cause.clone())),
                ..Default::default()
            });
            let result = match HostAggregateAllocator::try_new(host.clone()) {
                Err(error) => Err(error),
                Ok(allocator) => {
                    let mut state = MapAggState::new(allocator);
                    update(&mut state, &input, &Control::default())
                }
            };
            let actual = result.unwrap_err();
            assert_eq!(actual, cause);
            assert_eq!(host.ledger.lock().unwrap().attempts, stop + 1);
            assert_eq!(host.ledger.lock().unwrap().bytes, 0);
        }
    }
}
#[test]
fn map_agg_core_duplicate_value_contract_precedes_lookup_and_has_no_control_tail() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut state = MapAggState::new(allocator);
    let keys: ArrayRef = Arc::new(Int64Array::from(vec![None, Some(1), Some(1)]));
    let vals: ArrayRef = Arc::new(UInt32Array::from(vec![Some(7), None, Some(99)]));
    let input = MapUpdateInput { keys, values: vals };
    let control = Control::default();
    let error = update(&mut state, &input, &control).unwrap_err();
    let cause = error;
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    assert_eq!(cause.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(cause.failure_fact().reason().code(), 313);
    assert_eq!(cause.to_string(), "unsupported tracked scalar type: UInt32");
    assert_eq!(state.entries.len(), 1);
    assert!(state.entries[0].value.is_none());
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}

#[test]
fn map_agg_actual_constructor_refusal_keeps_arrow_source_and_releases_schema() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    use std::error::Error;
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let state = MapAggState::new(allocator);
    let entries = Arc::new(Field::new(
        "ResourceExhausted",
        DataType::Struct(
            vec![
                Field::new("key", DataType::Utf8, true),
                Field::new("value", DataType::Utf8, true),
            ]
            .into(),
        ),
        true,
    ));
    let weak = Arc::downgrade(&entries);
    let ty = DataType::Map(entries.clone(), false);
    let error = build_array(&ty, std::iter::once(&state), &mut ScalarWork::new(None)).unwrap_err();
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(error.failure_fact().reason().code(), 344);
    let KernelFailure::Source(source) = error else {
        panic!("actual constructor source required")
    };
    assert!(
        source
            .source()
            .unwrap()
            .source()
            .unwrap()
            .is::<arrow_schema::ArrowError>()
    );
    drop(ty);
    drop(entries);
    assert!(weak.upgrade().is_none());
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}

#[test]
fn map_agg_actual_wrong_carrier_is_an_outer_contract_without_input_ownership() {
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let input: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let weak = Arc::downgrade(&input);
    let error = update_input(&input).err().expect("wrong carrier must fail");
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert_eq!(error.failure_fact().reason().code(), 343);
    assert_eq!(error.to_string(), "map_agg expects struct input");
    drop(input);
    assert!(weak.upgrade().is_none());
}

#[test]
fn map_agg_core_uses_map_identity_in_update_and_partial_merge() {
    use arrow_array::Float64Array;

    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let input = |keys, values| MapUpdateInput {
        keys: Arc::new(Float64Array::from(keys)),
        values: Arc::new(StringArray::from(values)),
    };
    let mut first = MapAggState::new(allocator.clone());
    update(
        &mut first,
        &input(
            vec![-0.0, f64::NAN, 0.0, -f64::NAN],
            vec!["first zero", "first NaN", "duplicate zero", "duplicate NaN"],
        ),
        &Control::default(),
    )
    .unwrap();
    assert_eq!(first.entries.len(), 2);

    let mut second = MapAggState::new(allocator.clone());
    update(
        &mut second,
        &input(
            vec![0.0, -f64::NAN, 2.0],
            vec!["later zero", "later NaN", "two"],
        ),
        &Control::default(),
    )
    .unwrap();
    let target = DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Float64, false),
                    Field::new("value", DataType::Utf8, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    );
    let partial = build_array(&target, [&second].into_iter(), &mut ScalarWork::new(None)).unwrap();
    let input = merge_input(&partial).unwrap();
    merge_row(&mut first, &input, 0, &mut ScalarWork::new(None)).unwrap();
    let result = build_array(&target, [&first].into_iter(), &mut ScalarWork::new(None)).unwrap();
    let map = result.as_any().downcast_ref::<MapArray>().unwrap();
    assert_eq!(map.value_offsets(), &[0, 3]);
    let keys = map.keys().as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(keys.value(0).to_bits(), (-0.0_f64).to_bits());
    assert!(keys.value(1).is_nan());
    assert_eq!(keys.value(2), 2.0);
    let values = map.values().as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        [Some("first zero"), Some("first NaN"), Some("two")],
    );
    drop((first, second, allocator));
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}

#[test]
fn empty_group_emits_a_null_map() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let state = MapAggState::new(allocator);
    let result = build_array(
        &target(),
        std::iter::once(&state),
        &mut ScalarWork::new(None),
    )
    .unwrap();
    let map = result.as_any().downcast_ref::<MapArray>().unwrap();
    assert!(map.is_null(0));
    assert_eq!(map.value_offsets(), &[0, 0]);
    drop(state);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}

#[test]
fn nested_float_key_uses_the_map_identity_and_keeps_the_observed_key() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut work = ScalarWork::new(None);
    {
        let mut state = MapAggState::new(allocator.clone());
        let identity = physical_test_identity(&DataType::List(Arc::new(Field::new_list_field(
            DataType::Float64,
            true,
        ))));
        for signed_zero in [-0.0, 0.0] {
            let mut key = ScalarVec::new_in(allocator.clone());
            key.push(Some(TrackedAggScalarValue::Float64(signed_zero)));
            key.push(None);
            append_entry(
                &mut state,
                &identity,
                TrackedAggScalarValue::List(key),
                None,
                &mut work,
            )
            .unwrap();
        }
        assert_eq!(state.entries.len(), 1);
        let TrackedAggScalarValue::List(key) = &state.entries[0].key else {
            panic!("LIST key required")
        };
        assert!(matches!(
            key.as_slice(),
            [Some(TrackedAggScalarValue::Float64(value)), None] if value.is_sign_negative()
        ));
    }
    drop(allocator);
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}

#[test]
fn map_agg_core_checks_colliding_hashes_against_the_retained_typed_values() {
    let host = Arc::new(Host::default());
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut state = MapAggState::new(allocator.clone());
    let identity = physical_test_identity(&DataType::Int64);
    let mut work = ScalarWork::new(None);
    for (key, value) in [(4, 31), (7, 32), (4, 99)] {
        append_entry_with_hash(
            &mut state,
            &identity,
            TrackedAggScalarValue::Int64(key),
            Some(TrackedAggScalarValue::Int64(value)),
            42,
            &mut work,
        )
        .unwrap();
    }
    assert_eq!(state.key_heads.len(), 1);
    assert_eq!(state.entries.len(), 2);
    assert!(matches!(
        state.entries[0].value,
        Some(TrackedAggScalarValue::Int64(31))
    ));
    assert!(matches!(
        state.entries[1].value,
        Some(TrackedAggScalarValue::Int64(32))
    ));
    drop((state, allocator));
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
}

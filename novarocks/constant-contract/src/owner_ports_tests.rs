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
use arrow_array::{
    DictionaryArray, Float32Array, Int8Array, Int64Array, StructArray, types::Int8Type,
};
use std::{alloc::Layout, collections::HashMap, sync::Mutex};

const PHASE: CompilePhase = CompilePhase::Decode;
const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct Control {
    trace: Mutex<Vec<u32>>,
    refuse: Option<(usize, CompileControlError)>,
}
impl Control {
    fn new(refuse: Option<(usize, CompileControlError)>) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refuse,
        }
    }
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, PHASE);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.refuse {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push(units);
        match self.refuse.clone() {
            Some((stop, cause)) if stop == at => Err(cause),
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
struct Input {
    field: Arc<Field>,
    ty: FunctionValueType,
    data: ArrayData,
}
impl Input {
    fn array(array: ArrayRef, nullable: bool) -> Self {
        let field = Arc::new(
            Field::new("constant", array.data_type().clone(), nullable)
                .with_metadata(HashMap::from([("source".into(), "原始\0field".into())])),
        );
        Self {
            ty: FunctionValueType::new(array.data_type().clone(), nullable),
            field,
            data: array.to_data(),
        }
    }
    fn plain(
        &self,
        control: &Control,
        policy: ConstantPolicy,
    ) -> Result<ConstantPool, ConstantError> {
        ConstantPool::try_new(
            self.field.clone(),
            self.ty.clone(),
            self.data.clone(),
            policy,
            PHASE,
            control,
        )
    }
    fn parent(
        &self,
        control: &Control,
        policy: ConstantPolicy,
        snapshots: &mut Vec<ConstantOwnerResourceFacts>,
    ) -> Result<ConstantPool, ConstantError> {
        let mut work = CompileCheckpoints::try_new(control, PHASE)?;
        let result = ConstantPool::try_new_in(
            self.field.clone(),
            self.ty.clone(),
            self.data.clone(),
            policy,
            &mut |facts| {
                if let Some(previous) = snapshots.last() {
                    assert!(
                        facts.allocation_requests_upper_bound
                            >= previous.allocation_requests_upper_bound
                    );
                    assert!(
                        facts.allocation_request_bytes_upper_bound
                            >= previous.allocation_request_bytes_upper_bound
                    );
                    assert!(
                        facts.cumulative_work_upper_bound >= previous.cumulative_work_upper_bound
                    );
                }
                snapshots.push(*facts);
                Ok(())
            },
            &mut work,
        );
        finish(result, work)
    }
}
fn finish<T>(
    out: Result<T, ConstantError>,
    work: CompileCheckpoints<'_>,
) -> Result<T, ConstantError> {
    if matches!(&out, Err(ConstantError::Control(_))) {
        return out;
    }
    work.finish()?;
    out
}
fn wide(width: usize) -> Input {
    let fields = (0..width)
        .map(|i| Arc::new(Field::new(format!("f{i}"), DataType::Int64, false)))
        .collect::<Vec<_>>();
    let arrays = (0..width)
        .map(|i| Arc::new(Int64Array::from(vec![i as i64, i as i64 + 10])) as ArrayRef)
        .collect::<Vec<_>>();
    Input::array(
        Arc::new(StructArray::new(fields.into(), arrays, None)),
        false,
    )
}
fn prefixes(input: &Input, policy: ConstantPolicy, ordinary: bool) {
    let c = Control::new(None);
    let mut snapshots = Vec::new();
    let baseline = input.parent(&c, policy, &mut snapshots);
    assert_eq!(baseline.is_err(), ordinary);
    let trace = c.trace();
    assert!(trace.len() >= 2);
    for at in 0..trace.len() {
        for cause in CAUSES {
            let c = Control::new(Some((at, cause.clone())));
            let result = input.parent(&c, policy, &mut Vec::new());
            assert!(matches!(result, Err(ConstantError::Control(actual)) if actual==cause));
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}

#[test]
fn parent_pool_keeps_original_field_buffer_ordinal_float_bits_and_typed_null() {
    let input = Input::array(
        Arc::new(Float32Array::from(vec![
            Some(f32::from_bits(0x8000_0000)),
            None,
            Some(f32::from_bits(0x7fc0_1234)),
        ])),
        true,
    );
    let plain = input.plain(&Control::new(None), policy()).unwrap();
    let mut snapshots = Vec::new();
    let parent = input
        .parent(&Control::new(None), policy(), &mut snapshots)
        .unwrap();
    assert!(Arc::ptr_eq(parent.field_ref(), &input.field));
    assert_eq!(parent.value_type(), &input.ty);
    assert_eq!(parent.resource_facts(), plain.resource_facts());
    assert_eq!(
        parent.data().buffers()[0].data_ptr(),
        input.data.buffers()[0].data_ptr()
    );
    let array = parent
        .array()
        .as_any()
        .downcast_ref::<Float32Array>()
        .unwrap();
    assert_eq!(array.value(0).to_bits(), 0x8000_0000);
    assert_eq!(array.value(2).to_bits(), 0x7fc0_1234);
    let selected = parent.value(1).unwrap();
    assert_eq!(selected.ordinal(), 1);
    assert_eq!(
        selected.pool().backing_identity(),
        parent.backing_identity()
    );
    assert!(
        selected
            .is_null_observed(PHASE, &Control::new(None))
            .unwrap()
    );
    let last = snapshots.last().unwrap();
    assert_eq!(last.allocation_requests_upper_bound, 2);
    assert_eq!(
        last.allocation_request_bytes_upper_bound,
        ConstantPool::backing_allocation_layout().size()
            + Layout::new::<RequiredFrame<'_>>().size()
    );
    assert_eq!(
        ConstantPool::type_validation_scratch_layout(),
        Layout::new::<
            [Option<novarocks_type_contract::TypeTraversalFrame<'_>>;
                novarocks_type_contract::MAX_VALUE_TYPE_NODES],
        >()
    );
    assert_eq!(
        ConstantPool::type_validation_scratch_work_upper_bound(),
        ConstantPool::type_validation_scratch_layout().size()
    );
    assert!(
        last.cumulative_work_upper_bound
            >= ConstantPool::type_validation_scratch_work_upper_bound()
    );
}

#[test]
fn actual_dictionary_and_struct_spill_have_independent_owned_request_layouts() {
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0i8, 1]),
        Arc::new(Int64Array::from(vec![7, 9])),
    )
    .unwrap();
    let input = Input::array(Arc::new(dictionary), false);
    let mut snapshots = Vec::new();
    let pool = input
        .parent(&Control::new(None), policy(), &mut snapshots)
        .unwrap();
    assert_eq!(pool.resource_facts().array_nodes, 2);
    assert_eq!(pool.resource_facts().logical_elements_upper_bound, 4);
    let end = snapshots.last().unwrap();
    let expected = ConstantPool::backing_allocation_layout().size()
        + Layout::new::<RequiredFrame<'_>>().size()
        + Layout::array::<ElementMetric>(1).unwrap().size()
        + Layout::array::<RequiredFrame<'_>>(4).unwrap().size();
    assert_eq!(
        (
            end.allocation_requests_upper_bound,
            end.allocation_request_bytes_upper_bound
        ),
        (4, expected)
    );
    let input = wide(4);
    let mut snapshots = Vec::new();
    let parent = input
        .parent(&Control::new(None), policy(), &mut snapshots)
        .unwrap();
    let plain = input.plain(&Control::new(None), policy()).unwrap();
    assert_eq!(parent.resource_facts(), plain.resource_facts());
    let tree =
        novarocks_type_contract::owned_resources::btree::insertion_only::<(usize, usize), ()>(4)
            .unwrap();
    let expected = ConstantPool::backing_allocation_layout().size()
        + Layout::new::<RequiredFrame<'_>>().size()
        + Layout::array::<ElementMetric>(4).unwrap().size()
        + Layout::array::<RequiredFrame<'_>>(5).unwrap().size()
        + tree.request_bytes_upper_bound;
    let end = snapshots.last().unwrap();
    assert_eq!(
        (
            end.allocation_requests_upper_bound,
            end.allocation_request_bytes_upper_bound
        ),
        (8, expected)
    );
}

#[test]
fn original_flat_recursive_resource_authors_keep_plain_laws_and_parent_results() {
    let c = Control::new(None);
    let field = Field::new("constant", DataType::Int64, false);
    let ty = FunctionValueType::new(DataType::Int64, false);
    let input = FlatConstantResourceInput {
        rows: 2,
        buffer_count_upper_bound: 1,
        buffer_visits_bytes_upper_bound: 16,
        retained_buffer_capacity_bytes_upper_bound: 16,
        view_validation_bytes_upper_bound: 0,
    };
    let plain = preflight_flat_pool_resources(&field, &ty, input, policy(), PHASE, &c).unwrap();
    let mut work = CompileCheckpoints::try_new(&c, PHASE).unwrap();
    let mut last = ConstantOwnerResourceFacts::default();
    let parent = preflight_flat_pool_resources_in(
        &field,
        &ty,
        input,
        policy(),
        &mut |f| {
            last = *f;
            Ok(())
        },
        &mut work,
    )
    .unwrap();
    work.finish().unwrap();
    assert_eq!(parent, plain);
    assert_eq!(last.allocation_requests_upper_bound, 0);
    let ty = DataType::Struct(vec![Arc::new(Field::new("child", DataType::Int64, false))].into());
    let field = Field::new("constant", ty.clone(), false);
    let value = FunctionValueType::new(ty, false);
    let input = RecursiveConstantResourceInput {
        buffer_count_upper_bound: 1,
        buffer_visits_bytes_upper_bound: 16,
        retained_buffer_capacity_bytes_upper_bound: 16,
        view_validation_bytes_upper_bound: 0,
        utf8_fallback_validation_bytes_upper_bound: 0,
    };
    for lengths in [vec![2, 2], vec![2], vec![2, 2, 1]] {
        let expected = preflight_recursive_pool_resources(
            &field,
            &value,
            lengths.iter().copied(),
            input,
            policy(),
            PHASE,
            &c,
        );
        let mut work = CompileCheckpoints::try_new(&c, PHASE).unwrap();
        let actual = preflight_recursive_pool_resources_in(
            &field,
            &value,
            lengths,
            input,
            policy(),
            &mut |_| Ok(()),
            &mut work,
        );
        assert_eq!(finish(actual, work), expected);
    }
}

#[test]
fn every_actual_small_success_and_ordinary_caller_tail_preserves_first_cause() {
    let good = Input::array(Arc::new(Int64Array::from(vec![1, 2])), false);
    prefixes(&good, policy(), false);
    let mut bad = Input::array(Arc::new(Int64Array::from(vec![1, 2])), true);
    bad.ty = bad.ty.with_nullable(false).unwrap();
    let plain = bad.plain(&Control::new(None), policy()).unwrap_err();
    assert_eq!(
        plain,
        ConstantError::Invalid("constant field differs from exact value nullability")
    );
    assert_eq!(
        bad.parent(&Control::new(None), policy(), &mut Vec::new())
            .unwrap_err(),
        plain
    );
    prefixes(&bad, policy(), true);
    let mut rows = policy();
    rows.max_rows = 0;
    assert!(matches!(
        good.parent(&Control::new(None), rows, &mut Vec::new()),
        Err(ConstantError::Limit("constant row limit exceeded"))
    ));
    prefixes(&good, rows, true);
}

#[test]
fn known_backing_frame_metric_and_scratch_work_refuse_before_pending_callbacks() {
    let input = wide(4);
    let arc = ConstantPool::backing_allocation_layout().size();
    let frame = Layout::new::<RequiredFrame<'_>>().size();
    let metrics = Layout::array::<ElementMetric>(4).unwrap().size();
    for (axis, cap) in [
        (0, 0),
        (0, 1),
        (0, 2),
        (1, arc - 1),
        (1, arc + frame + metrics - 1),
        (
            2,
            arc + frame + metrics + ConstantPool::type_validation_scratch_work_upper_bound() - 1,
        ),
    ] {
        for pending in [0, 254, 255] {
            for cause in CAUSES {
                let c = Control::new(Some((1, cause)));
                let mut work = CompileCheckpoints::try_new(&c, PHASE).unwrap();
                for _ in 0..pending {
                    work.step().unwrap();
                }
                let out = ConstantPool::try_new_in(
                    input.field.clone(),
                    input.ty.clone(),
                    input.data.clone(),
                    policy(),
                    &mut |f| {
                        let n = match axis {
                            0 => f.allocation_requests_upper_bound,
                            1 => f.allocation_request_bytes_upper_bound,
                            _ => f.cumulative_work_upper_bound,
                        };
                        if n > cap {
                            Err(CompileControlError::ResourceExhausted)
                        } else {
                            Ok(())
                        }
                    },
                    &mut work,
                );
                assert!(matches!(
                    out,
                    Err(ConstantError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(c.trace(), [0]);
            }
        }
    }
}

#[test]
fn wide_actual_struct_preserves_all_children_and_samples_true_work_quantum() {
    let input = wide(320);
    let c = Control::new(None);
    let mut snapshots = Vec::new();
    let pool = input.parent(&c, policy(), &mut snapshots).unwrap();
    assert_eq!(pool.data().child_data().len(), 320);
    assert_eq!(pool.resource_facts().array_nodes, 321);
    assert_eq!(pool.resource_facts().logical_elements_upper_bound, 642);
    assert_eq!(
        pool.array()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap()
            .column(319)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        [319, 329]
    );
    let trace = c.trace();
    let quantum = trace
        .iter()
        .position(|n| *n == 256)
        .expect("actual borrowed type/array loops must expose their quantum");
    for at in [0, quantum, trace.len() / 2, trace.len() - 1] {
        for cause in CAUSES {
            let c = Control::new(Some((at, cause.clone())));
            assert!(
                matches!(input.parent(&c,policy(),&mut Vec::new()),Err(ConstantError::Control(actual)) if actual==cause)
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}

#[test]
fn same_numerical_envelope_maps_only_parent_arithmetic_to_primary_resource() {
    let counts = ValidationCounts {
        nodes: u64::MAX,
        storage_elements: 0,
        buffer_count: 0,
        buffer_visits: 0,
        view_validation_bytes: 0,
        utf8_fallback_validation_bytes: 0,
        masks: 0,
        depth: 2,
    };
    assert!(matches!(
        validation_envelope_core(counts, 0, policy()),
        Err(ConstantError::Limit(
            "constant resource arithmetic overflow"
        ))
    ));
    for pending in [254, 255] {
        for cause in CAUSES {
            let c = Control::new(Some((1, cause)));
            let mut work = CompileCheckpoints::try_new(&c, PHASE).unwrap();
            for _ in 0..pending {
                work.step().unwrap();
            }
            let mut admit = |_: &ConstantOwnerResourceFacts| Ok(());
            let mut observer = PoolObserver::parent(&mut admit);
            assert!(matches!(
                validation_envelope_owned(counts, 0, policy(), &mut observer, &mut work),
                Err(ConstantError::Control(
                    CompileControlError::ResourceExhausted
                ))
            ));
            assert_eq!(c.trace(), [0]);
        }
    }
    let counts = ValidationCounts {
        nodes: 1,
        storage_elements: 2,
        buffer_count: 1,
        buffer_visits: 16,
        view_validation_bytes: 0,
        utf8_fallback_validation_bytes: 0,
        masks: 0,
        depth: 1,
    };
    let mut policy = policy();
    policy.max_library_validation_work = 0;
    assert!(matches!(
        validation_envelope_core_policy(counts, 0, policy, true),
        Err(ConstantError::Limit(
            "opaque Arrow validation work limit exceeded"
        ))
    ));
}

#[test]
fn captured_nested_struct_and_dictionary_metric_requests_precede_late_control() {
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![0i8, 1]),
        Arc::new(Int64Array::from(vec![7, 9])),
    )
    .unwrap();
    // These are the actual child ArrayData headers consumed by the original
    // scanner's capture body, not fabricated pool or package certificates.
    for input in [wide(2), Input::array(Arc::new(dictionary), false)] {
        input.data.validate_full().unwrap();
        let expected_request = Layout::array::<ElementMetric>(input.data.child_data().len())
            .unwrap()
            .size();
        let control = Control::new(None);
        let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
        let plain = scan_child::<ConstantError>(
            &input.data,
            2,
            0,
            policy(),
            &mut ScanFacts::default(),
            &mut PoolObserver::plain(),
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        let control = Control::new(None);
        let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
        let mut last = ConstantOwnerResourceFacts::default();
        let mut admit = |facts: &ConstantOwnerResourceFacts| {
            last = *facts;
            Ok(())
        };
        let actual = scan_child::<ConstantError>(
            &input.data,
            2,
            0,
            policy(),
            &mut ScanFacts::default(),
            &mut PoolObserver::parent(&mut admit),
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        assert_eq!((actual.len, actual.max_value), (plain.len, plain.max_value));
        assert_eq!(last.allocation_requests_upper_bound, 1);
        assert_eq!(last.allocation_request_bytes_upper_bound, expected_request);
        for axis in 0..2 {
            for pending in [254, 255] {
                for cause in CAUSES {
                    let control = Control::new(Some((1, cause)));
                    let mut work = CompileCheckpoints::try_new(&control, PHASE).unwrap();
                    for _ in 0..pending {
                        work.step().unwrap();
                    }
                    let mut admit =
                        |facts: &ConstantOwnerResourceFacts| -> Result<(), ConstantError> {
                            let exceeded = if axis == 0 {
                                facts.allocation_requests_upper_bound > 0
                            } else {
                                facts.allocation_request_bytes_upper_bound > expected_request - 1
                            };
                            if exceeded {
                                Err(CompileControlError::ResourceExhausted.into())
                            } else {
                                Ok(())
                            }
                        };
                    let result = scan_child(
                        &input.data,
                        2,
                        0,
                        policy(),
                        &mut ScanFacts::default(),
                        &mut PoolObserver::parent(&mut admit),
                        &mut work,
                    );
                    assert!(matches!(
                        result,
                        Err(ConstantError::Control(
                            CompileControlError::ResourceExhausted
                        ))
                    ));
                    assert_eq!(control.trace(), [0]);
                }
            }
        }
    }
}

#[test]
fn actual_nested_unknown_source_attributes_are_retained_counted_and_never_semantic_authors() {
    use novarocks_type_contract::{
        ValueField, ValueKind, ValueLayout, ValueTypeRef, ValueTypeStorageLifetime,
    };
    let attributes = HashMap::from([
        (
            novarocks_type_contract::NR_LOGICAL_TYPE_KEY.into(),
            "provider.unknown.future-type".into(),
        ),
        ("provider.large.fact".into(), "x".repeat(2049)),
    ]);
    let child =
        Arc::new(Field::new("payload", DataType::Utf8, false).with_metadata(attributes.clone()));
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![child].into(),
        vec![Arc::new(arrow_array::StringArray::from(vec![
            "{}",
            "{\"a\":1}",
        ]))],
        None,
    ));
    let field =
        Arc::new(Field::new("source", array.data_type().clone(), false).with_metadata(attributes));
    let child_type =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let storage = ValueTypeStorageLifetime::pure();
    let semantic = ValueTypeRef::try_new(
        ValueKind::Struct(
            vec![ValueField::new(
                "payload",
                child_type.value_type().clone(),
                storage.clone(),
            )]
            .into(),
        ),
        false,
        storage.clone(),
    )
    .unwrap();
    let binding = FunctionValueType::try_from_value(
        semantic,
        ValueLayout::try_new(field.data_type(), storage).unwrap(),
    )
    .unwrap();
    let input = Input {
        field,
        ty: binding,
        data: array.to_data(),
    };
    let plain = input.plain(&Control::new(None), policy()).unwrap();
    let mut snapshots = Vec::new();
    let parent = input
        .parent(&Control::new(None), policy(), &mut snapshots)
        .unwrap();
    assert_eq!(plain.resource_facts(), parent.resource_facts());
    assert!(Arc::ptr_eq(parent.field_ref(), &input.field));
    let source_bytes = |field: &Field| {
        field.name().len() as u64
            + field
                .metadata()
                .iter()
                .map(|(key, value)| (key.len() + value.len()) as u64)
                .sum::<u64>()
    };
    let root_bytes = source_bytes(&input.field);
    let DataType::Struct(fields) = input.field.data_type() else {
        panic!()
    };
    let child_bytes = source_bytes(&fields[0]);
    assert_eq!(
        parent.resource_facts().metadata_bytes,
        root_bytes + child_bytes
    );
    assert!(child_bytes > 2048);
    assert!(
        snapshots.last().unwrap().cumulative_work_upper_bound
            >= ConstantPool::type_validation_scratch_work_upper_bound()
    );
    assert_eq!(
        parent.value_type().value_type().child_type_at(0).unwrap(),
        child_type.value_type()
    );
    prefixes(&input, policy(), false);
    let capped = ConstantPolicy {
        max_metadata_bytes: root_bytes + child_bytes - 1,
        ..policy()
    };
    assert!(matches!(
        input.plain(&Control::new(None), capped),
        Err(ConstantError::Limit(_))
    ));
    prefixes(&input, capped, true);
    // The same physical source can be independently declared as plain text;
    // that complete semantic declaration never equals this JSON descendant.
    let physical_type = FunctionValueType::new(input.field.data_type().clone(), false);
    let physical = ConstantPool::try_new(
        input.field.clone(),
        physical_type,
        input.data.clone(),
        policy(),
        PHASE,
        &Control::new(None),
    )
    .unwrap();
    assert!(
        !parent
            .value(0)
            .unwrap()
            .equals_observed(&physical.value(0).unwrap(), PHASE, &Control::new(None))
            .unwrap()
    );
}

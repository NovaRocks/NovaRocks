// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

use super::super::*;
use crate::testkit::CountingAllocator;
use arrow::array::{Array, Int32Array};
use novarocks_type_contract::{
    FunctionValueType, ValueLayout, ValueTypeStorageHold, ValueTypeStorageLifetime,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Creator(Arc<AtomicUsize>);
impl ValueTypeStorageHold for Creator {}
impl Drop for Creator {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Active;
impl KernelEvaluationControl for Active {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
        panic!("creator retention must not wait")
    }
}
fn declared(drops: &Arc<AtomicUsize>) -> FunctionValueType {
    let semantic = FunctionValueType::new(DataType::Int32, true);
    FunctionValueType::try_from_value(
        semantic.value_type().clone(),
        ValueLayout::try_new(
            &DataType::Int32,
            ValueTypeStorageLifetime::held(Arc::new(Creator(drops.clone()))),
        )
        .unwrap(),
    )
    .unwrap()
}

/// This exercises the actual guarded Selected-result branch and then its
/// actual continuation wrapper. The unchanged Server GLOBAL Frame tests
/// separately assert per-physical-allocation last free through the complete
/// installed bitwise/shift kernel chain; this test asserts creator identity.
#[test]
fn fresh_selected_creator_pin_does_not_own_output_array_and_preserves_previous_source_creator() {
    canonical_constant_creator_cases();
    let source_drops = Arc::new(AtomicUsize::new(0));
    let canonical_drops = Arc::new(AtomicUsize::new(0));
    let source_type = declared(&source_drops);
    let canonical = declared(&canonical_drops);
    assert_eq!(source_type, canonical);
    assert!(
        !source_type
            .typed_binding()
            .layout()
            .shares_storage(canonical.typed_binding().layout())
    );
    let tracker = Arc::new(CountingAllocator::new(usize::MAX));
    let host: Arc<dyn novarocks_functions::AggregateStateAllocator> = tracker.clone();
    let source = input_source::retain_result_type(
        Arc::new(Int32Array::from(vec![Some(11), None, Some(7)])),
        &source_type,
        Some(&host),
        &Active,
    )
    .unwrap();
    let source_array = Arc::downgrade(&source);
    let previous = input_source::ArrayContinuation::capture(&source, &host, &Active).unwrap();
    drop(source);
    drop(source_type);
    let value = OwnedValue::Selected(
        Arc::new(Int32Array::from(vec![Some(22), None, Some(14)])),
        Box::default(),
    )
    .with_continuation(Some(previous))
    .retaining_result_type(&canonical, Some(&host), &Active)
    .unwrap();
    drop(canonical);
    let OwnedValue::Continued(value, owner) = value else {
        panic!("actual creator pin");
    };
    let OwnedValue::Selected(array, errors) = *value else {
        panic!("actual compact result");
    };
    assert!(errors.is_empty());
    let output_array = Arc::downgrade(&array);
    let output = owner.retain(array, &Active).unwrap();
    drop(owner);
    assert!(
        output_array.upgrade().is_none(),
        "a fresh creator continuation must not keep the whole output Array"
    );
    assert!(
        source_array.upgrade().is_some(),
        "the previous real source loan is preserved independently"
    );
    let array = output.as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(
        array.iter().collect::<Vec<_>>(),
        vec![Some(22), None, Some(14)]
    );
    let values = array.values().inner().clone();
    let bitmap = array.nulls().unwrap().buffer().clone();
    drop(output);
    assert_eq!(source_drops.load(Ordering::SeqCst), 0);
    assert_eq!(canonical_drops.load(Ordering::SeqCst), 0);
    drop(values);
    assert_eq!(source_drops.load(Ordering::SeqCst), 0);
    assert_eq!(canonical_drops.load(Ordering::SeqCst), 0);
    std::thread::scope(|scope| scope.spawn(move || drop(bitmap)).join().unwrap());
    assert!(source_array.upgrade().is_none());
    assert_eq!(source_drops.load(Ordering::SeqCst), 1);
    assert_eq!(canonical_drops.load(Ordering::SeqCst), 1);
    assert_eq!(tracker.outstanding(), 0);
}
impl novarocks_type_contract::PureCompileControl for Active {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}
fn canonical_constant_creator_cases() {
    canonical_root_source_annotations_need_no_wrapper_host();
    use arrow::array::{ListArray, NullArray, StructArray};
    use arrow::buffer::{OffsetBuffer, ScalarBuffer};
    use arrow::datatypes::Field;
    use novarocks_functions::{ConstantPolicy, ConstantPool};

    for (finite, bufferless) in [(false, false), (true, false), (false, true)] {
        let drops = Arc::new(AtomicUsize::new(0));
        let child = Arc::new(
            Field::new(
                "payload",
                if bufferless {
                    DataType::Null
                } else {
                    DataType::Int32
                },
                true,
            )
            .with_metadata(std::collections::HashMap::<String, String>::from([(
                "provider.fact".into(),
                "kept".into(),
            )])),
        );
        let raw: ArrayRef = if bufferless {
            Arc::new(StructArray::new(
                vec![child].into(),
                vec![Arc::new(NullArray::new(3))],
                None,
            ))
        } else {
            Arc::new(ListArray::new(
                child,
                OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 1, 3, 4])),
                Arc::new(Int32Array::from(vec![111, 22, 33, 999])),
                None,
            ))
        };
        let pure = FunctionValueType::new(raw.data_type().clone(), false);
        let ty = if finite {
            FunctionValueType::try_from_value(
                pure.value_type().clone(),
                ValueLayout::try_new(
                    raw.data_type(),
                    ValueTypeStorageLifetime::held(Arc::new(Creator(drops.clone()))),
                )
                .unwrap(),
            )
            .unwrap()
        } else {
            pure
        };
        let policy = ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 128,
            max_logical_elements: 1024,
            max_retained_buffer_bytes: 1 << 20,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 64,
            max_metadata_bytes: 1 << 20,
            max_library_validation_work: 1 << 20,
            max_library_validation_bytes: 1 << 20,
        };
        let pool = ConstantPool::try_new(
            Arc::new(Field::new("original", raw.data_type().clone(), false)),
            ty.clone(),
            raw.to_data(),
            policy,
            novarocks_type_contract::CompilePhase::Validate,
            &Active,
        )
        .unwrap();
        let original = pool.value(1).unwrap();
        if !finite && !bufferless {
            canonical_projection_original_refusal_prefix(&original, &ty);
        }
        let tracker = Arc::new(CountingAllocator::new(usize::MAX));
        let host: Arc<dyn novarocks_functions::AggregateStateAllocator> = tracker.clone();
        let observed = ObservedControl {
            original: &Active,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        let mut work: Work<'_, '_> = Work {
            control: &observed,
            pending: 0,
            scalar_host: None,
        };
        let value =
            complete_constant_leaf(&original, &ty, Selection::all(1), Some(&host), &mut work)
                .unwrap();
        let created = match value.uncontinued() {
            OwnedValue::ConstantView(view) => {
                assert_eq!(view.original.ordinal(), 1);
                assert_eq!(
                    view.original.pool().backing_identity(),
                    original.pool().backing_identity()
                );
                view.created.clone()
            }
            _ => panic!("raw annotations require the actual metadata creator"),
        };
        let value = value
            .retaining_result_type(&ty, Some(&host), &Active)
            .unwrap();
        let materialized = value
            .into_value(Selection::all(1), &mut work)
            .unwrap()
            .materialize(Selection::all(1), ty.data_type(), &mut work)
            .unwrap();
        let output = materialized.values().clone();
        let alias = if bufferless {
            None
        } else {
            Some(output.to_data().child_data()[0].buffers()[0].clone())
        };
        drop((materialized, original, pool, ty, raw, host));
        assert!(
            created.upgrade().is_some(),
            "new creator must survive actual root gather even for a pure original CV"
        );
        assert!(tracker.outstanding() > 0);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        let last = output.clone();
        drop(output);
        assert!(created.upgrade().is_some());
        drop(last);
        if let Some(alias) = alias {
            assert!(
                created.upgrade().is_some(),
                "last child Buffer retains metadata creator"
            );
            std::thread::scope(|scope| scope.spawn(move || drop(alias)).join().unwrap());
        }
        assert!(created.upgrade().is_none());
        assert_eq!(drops.load(Ordering::SeqCst), usize::from(finite));
        assert_eq!(tracker.outstanding(), 0);
        let (reserved, refunded) = tracker.reservation_totals();
        assert_eq!(reserved, refunded);
    }
}

fn canonical_projection_original_refusal_prefix(
    original: &novarocks_functions::ConstantValue,
    ty: &FunctionValueType,
) {
    use std::sync::Mutex;
    struct Probe {
        checks: Mutex<Vec<u32>>,
        fail: Option<(usize, KernelFailure)>,
    }
    impl KernelEvaluationControl for Probe {
        fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
            let mut checks = self.checks.lock().unwrap();
            let at = checks.len();
            checks.push(work);
            if let Some((stop, cause)) = &self.fail
                && *stop == at
            {
                Err(cause.clone())
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("projection cannot wait")
        }
    }
    fn run(
        original: &novarocks_functions::ConstantValue,
        ty: &FunctionValueType,
        host: &Arc<dyn novarocks_functions::AggregateStateAllocator>,
        probe: &Probe,
    ) -> Result<OwnedValue, KernelFailure> {
        let observed = ObservedControl {
            original: probe,
            refused: AtomicBool::new(false),
            operation_aborted: AtomicBool::new(false),
        };
        let mut work: Work<'_, '_> = Work {
            control: &observed,
            pending: 0,
            scalar_host: None,
        };
        complete_constant_leaf(original, ty, Selection::all(1), Some(host), &mut work)
    }
    let tracker = Arc::new(CountingAllocator::new(usize::MAX));
    let host: Arc<dyn novarocks_functions::AggregateStateAllocator> = tracker.clone();
    let good = Probe {
        checks: Mutex::new(vec![]),
        fail: None,
    };
    drop(run(original, ty, &host, &good).unwrap());
    let checks = good.checks.into_inner().unwrap();
    assert_eq!(tracker.outstanding(), 0);
    let limited = Arc::new(CountingAllocator::with_capacity_reason(
        1,
        novarocks_failure_contract::FailureReason::new(
            novarocks_failure_contract::domain::MEMORY,
            std::num::NonZeroU16::new(8).unwrap(),
        ),
    ));
    let limited_host: Arc<dyn novarocks_functions::AggregateStateAllocator> = limited.clone();
    let limited_probe = Probe {
        checks: Mutex::new(vec![]),
        fail: None,
    };
    let capacity = match run(original, ty, &limited_host, &limited_probe) {
        Err(cause) => cause,
        Ok(_) => panic!("the real finite host must refuse new metadata stock"),
    };
    assert!(matches!(capacity, KernelFailure::Capacity(_)));
    let limited_checks = limited_probe.checks.into_inner().unwrap();
    assert_eq!(limited_checks, checks[..limited_checks.len()]);
    assert_eq!(limited.outstanding(), 0);
    for cause in [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        capacity,
        KernelFailure::Internal(KernelDiagnostic::new("original projection callback source")),
    ] {
        for at in 0..checks.len() {
            let probe = Probe {
                checks: Mutex::new(vec![]),
                fail: Some((at, cause.clone())),
            };
            let actual = match run(original, ty, &host, &probe) {
                Err(cause) => cause,
                Ok(_) => panic!("original callback refusal"),
            };
            assert_eq!(actual, cause);
            assert_eq!(probe.checks.into_inner().unwrap(), checks[..=at]);
            assert_eq!(tracker.outstanding(), 0);
        }
    }
}
fn canonical_root_source_annotations_need_no_wrapper_host() {
    use arrow::array::StructArray;
    use arrow::datatypes::Field;
    struct NoAllocationHost;
    impl novarocks_functions::AggregateStateAllocator for NoAllocationHost {
        fn allocate(&self, _: std::alloc::Layout) -> Result<std::ptr::NonNull<u8>, KernelFailure> {
            panic!("canonical array needs no new wrapper or custody allocation")
        }
        unsafe fn release(&self, _: std::ptr::NonNull<u8>, _: std::alloc::Layout) {
            panic!("the source author made no host allocation")
        }
    }
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("payload", DataType::Int32, true))].into(),
        vec![Arc::new(Int32Array::from(vec![11, 22, 33]))],
        None,
    ));
    let ty = FunctionValueType::new(array.data_type().clone(), false);
    let field = Arc::new(
        Field::new("provider-root", array.data_type().clone(), false).with_metadata(
            std::collections::HashMap::<String, String>::from([(
                "provider.root".into(),
                "must stay original".into(),
            )]),
        ),
    );
    let policy = novarocks_functions::ConstantPolicy {
        max_rows: 16,
        max_array_nodes: 128,
        max_logical_elements: 1024,
        max_retained_buffer_bytes: 1 << 20,
        max_type_depth: 64,
        max_type_nodes: 4096,
        max_dictionary_depth: 64,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 1 << 20,
        max_library_validation_bytes: 1 << 20,
    };
    let pool = novarocks_functions::ConstantPool::try_new(
        field,
        ty.clone(),
        array.to_data(),
        policy,
        novarocks_type_contract::CompilePhase::Validate,
        &Active,
    )
    .unwrap();
    let value = pool.value(1).unwrap();
    let host: Arc<dyn novarocks_functions::AggregateStateAllocator> = Arc::new(NoAllocationHost);
    let observed = ObservedControl {
        original: &Active,
        refused: AtomicBool::new(false),
        operation_aborted: AtomicBool::new(false),
    };
    let mut work: Work<'_, '_> = Work {
        control: &observed,
        pending: 0,
        scalar_host: None,
    };
    let actual =
        complete_constant_leaf(&value, &ty, Selection::all(1), Some(&host), &mut work).unwrap();
    let OwnedValue::Constant(actual) = actual else {
        panic!("root annotations are not array metadata")
    };
    assert_eq!(actual.ordinal(), 1);
    assert!(Arc::ptr_eq(actual.pool().array(), value.pool().array()));
    assert_eq!(
        actual.field().metadata()["provider.root"],
        "must stay original"
    );
}

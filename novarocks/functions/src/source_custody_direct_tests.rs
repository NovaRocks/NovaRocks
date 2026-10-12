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

//! Direct source loans preserve original host failures and real alias lifetime.
use crate::kernel_control::{internal, invalid};
use crate::opaque_memory::{OpaqueAllocationHost, OpaqueRetainedCharge};
use crate::*;
use arrow_array::{Array, ArrayRef, Int64Array, NullArray, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Default)]
pub(crate) struct Ledger {
    pub(crate) events: Vec<(bool, usize)>,
    refusal: Option<(usize, KernelFailure)>,
    refused: bool,
    pub(crate) live: Vec<(usize, Layout)>,
    pub(crate) opaque: usize,
}
#[derive(Default)]
pub(crate) struct Host {
    pub(crate) ledger: Mutex<Ledger>,
}
impl Host {
    fn admit(&self, opaque: bool, bytes: usize) -> Result<(), KernelFailure> {
        let mut ledger = self.ledger.lock().unwrap();
        assert!(!ledger.refused, "no request after a refusal");
        let at = ledger.events.len();
        ledger.events.push((opaque, bytes));
        if let Some((stop, cause)) = &ledger.refusal {
            if *stop == at {
                let cause = cause.clone();
                ledger.refused = true;
                return Err(cause);
            }
        }
        if opaque {
            ledger.opaque += bytes;
        }
        Ok(())
    }
    pub(crate) fn zero(&self) {
        let ledger = self.ledger.lock().unwrap();
        assert_eq!(ledger.opaque, 0);
        assert!(ledger.live.is_empty());
    }
}
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        crate::source_custody_resources_tests::without_recording(|| {
            self.admit(false, layout.size())?;
            let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
                .ok_or(KernelFailure::ResourceExhausted)?;
            self.ledger
                .lock()
                .unwrap()
                .live
                .push((pointer.as_ptr().addr(), layout));
            Ok(pointer)
        })
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        crate::source_custody_resources_tests::without_recording(|| {
            let mut ledger = self.ledger.lock().unwrap();
            let index = ledger
                .live
                .iter()
                .position(|entry| *entry == (pointer.as_ptr().addr(), layout))
                .expect("release original block exactly once");
            ledger.live.swap_remove(index);
            unsafe {
                std::alloc::dealloc(pointer.as_ptr(), layout);
            }
        })
    }
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
}
impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        crate::source_custody_resources_tests::without_recording(|| self.admit(true, bytes))
    }
    fn release_opaque(&self, bytes: usize) {
        crate::source_custody_resources_tests::without_recording(|| {
            let mut ledger = self.ledger.lock().unwrap();
            ledger.opaque = ledger
                .opaque
                .checked_sub(bytes)
                .expect("release live grant once");
        })
    }
}
#[derive(Default)]
pub(crate) struct Control {
    pub(crate) trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, KernelFailure)>,
}
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        crate::source_custody_resources_tests::without_recording(|| {
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            trace.push(units);
            if let Some((stop, cause)) = &self.refusal {
                if *stop == at {
                    return Err(cause.clone());
                }
            }
            Ok(())
        })
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("custody never waits")
    }
}
pub(crate) fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("original host failure"),
        internal("original host failure"),
        KernelFailure::Operational(KernelDiagnostic::new("original host failure")),
        KernelFailure::InstanceFailed,
    ]
}
pub(crate) struct Owner(pub(crate) Arc<AtomicUsize>);
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub(crate) fn direct_run(
    array: &ArrayRef,
    host: Arc<Host>,
    control: &Control,
) -> Result<(), KernelFailure> {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut work = EvaluationCheckpoints::new(control);
    let source =
        SourceBackingOwner::try_from_owned_direct(Owner(drops.clone()), host.clone(), &mut work)?;
    let prepared = PreparedSourceCustody::prepare(array, &mut work)?;
    let charge = OpaqueRetainedCharge::try_new(host)?;
    let mut reservation = charge.reserve_operation(prepared.request_facts().global_peak_upper())?;
    let result = prepared.retain(&source, &mut reservation, &mut work)?;
    drop(reservation);
    drop(result);
    drop(source);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    Ok(())
}
#[test]
fn source_custody_direct_every_host_and_checkpoint_preserves_seven_causes_without_footer() {
    let array: ArrayRef = Arc::new(StringArray::from(vec![
        Some("original"),
        None,
        Some("last"),
    ]));
    let host = Arc::new(Host::default());
    let control = Control::default();
    direct_run(&array, host.clone(), &control).unwrap();
    let requests = host.ledger.lock().unwrap().events.len();
    let checkpoints = control.trace.lock().unwrap().len();
    host.zero();
    for cause in causes() {
        for stop in 0..requests {
            let host = Arc::new(Host::default());
            host.ledger.lock().unwrap().refusal = Some((stop, cause.clone()));
            assert_eq!(
                direct_run(&array, host.clone(), &Control::default()),
                Err(cause.clone())
            );
            assert_eq!(host.ledger.lock().unwrap().events.len(), stop + 1);
            host.zero();
        }
        for stop in 0..checkpoints {
            let host = Arc::new(Host::default());
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert_eq!(
                direct_run(&array, host.clone(), &control),
                Err(cause.clone())
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            host.zero();
        }
    }
}
#[test]
fn source_custody_direct_latched_control_precedes_static_and_host_failures() {
    for cause in causes() {
        let control = Control {
            refusal: Some((0, cause.clone())),
            ..Control::default()
        };
        let mut work = EvaluationCheckpoints::new(&control);
        assert_eq!(work.flush(), Err(cause.clone()));
        let host = Arc::new(Host::default());
        host.ledger.lock().unwrap().refusal = Some((0, internal("secondary host")));
        let drops = Arc::new(AtomicUsize::new(0));
        let result = SourceBackingOwner::try_from_owned_direct(
            Owner(drops.clone()),
            host.clone(),
            &mut work,
        );
        assert!(matches!(result, Err(e) if e == cause));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(host.ledger.lock().unwrap().events.is_empty());
        let array: ArrayRef = Arc::new(NullArray::new(0));
        assert!(
            matches!(PreparedSourceCustody::<Owner>::prepare(&array, &mut work), Err(e) if e == cause)
        );
        assert_eq!(control.trace.lock().unwrap().len(), 1);
        host.zero();
    }
}
#[test]
fn source_custody_direct_same_host_and_complete_peak_are_required_before_publication() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
    let control = Control::default();
    let host = Arc::new(Host::default());
    let other = Arc::new(Host::default());
    let mut work = EvaluationCheckpoints::new(&control);
    let source = SourceBackingOwner::try_from_owned_direct((), host.clone(), &mut work).unwrap();
    for wrong_host in [false, true] {
        let prepared = PreparedSourceCustody::prepare(&array, &mut work).unwrap();
        let bytes = prepared.request_facts().global_peak_upper();
        let charge = OpaqueRetainedCharge::try_new(if wrong_host {
            other.clone()
        } else {
            host.clone()
        })
        .unwrap();
        let mut reservation = charge
            .reserve_operation(if wrong_host { bytes } else { bytes - 1 })
            .unwrap();
        let before = host.ledger.lock().unwrap().events.len();
        let aliases = Arc::strong_count(&array);
        let error = prepared
            .retain(&source, &mut reservation, &mut work)
            .unwrap_err();
        let KernelFailure::InvalidProgram(detail) = error else {
            panic!("named invalid reservation")
        };
        assert_eq!(
            detail.message(),
            if wrong_host {
                "source custody uses a different actual reservation host"
            } else {
                "source custody exceeds its complete operation reservation"
            }
        );
        assert_eq!(host.ledger.lock().unwrap().events.len(), before);
        assert_eq!(Arc::strong_count(&array), aliases);
        drop(reservation);
        drop(charge);
    }
    drop(source);
    host.zero();
    other.zero();
}
#[test]
fn source_custody_direct_buffer_and_child_aliases_keep_original_owner_until_cross_thread_drop() {
    let fields = vec![Arc::new(Field::new("value", DataType::Int64, true))].into();
    let array: ArrayRef = Arc::new(StructArray::new(
        fields,
        vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))],
        None,
    ));
    let host = Arc::new(Host::default());
    let control = Control::default();
    let drops = Arc::new(AtomicUsize::new(0));
    let mut work = EvaluationCheckpoints::new(&control);
    let source =
        SourceBackingOwner::try_from_owned_direct(Owner(drops.clone()), host.clone(), &mut work)
            .unwrap();
    let prepared = PreparedSourceCustody::prepare(&array, &mut work).unwrap();
    let charge = OpaqueRetainedCharge::try_new(host.clone()).unwrap();
    let mut reservation = charge
        .reserve_operation(prepared.request_facts().global_peak_upper())
        .unwrap();
    let result = prepared
        .retain(&source, &mut reservation, &mut work)
        .unwrap();
    drop(reservation);
    drop(charge);
    drop(source);
    drop(array);
    let sliced = result.slice(1, 1);
    let child = sliced
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0)
        .clone();
    let data = child.to_data();
    let last = data.buffers()[0].clone();
    drop(child);
    drop(data);
    drop(sliced);
    drop(result);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(host.ledger.lock().unwrap().opaque > 0);
    std::thread::spawn(move || drop(last)).join().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    host.zero();
}
#[test]
fn source_custody_direct_bufferless_root_slice_pins_but_data_and_childless_buffers_do_not() {
    for array in [
        Arc::new(NullArray::new(3)) as ArrayRef,
        Arc::new(StructArray::new_empty_fields(3, None)) as ArrayRef,
        Arc::new(StructArray::new_empty_fields(
            3,
            Some(arrow_buffer::NullBuffer::new_valid(3)),
        )) as ArrayRef,
    ] {
        let host = Arc::new(Host::default());
        let control = Control::default();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut work = EvaluationCheckpoints::new(&control);
        let source = SourceBackingOwner::try_from_owned_direct(
            Owner(drops.clone()),
            host.clone(),
            &mut work,
        )
        .unwrap();
        let prepared = PreparedSourceCustody::prepare(&array, &mut work).unwrap();
        let charge = OpaqueRetainedCharge::try_new(host.clone()).unwrap();
        let mut reservation = charge
            .reserve_operation(prepared.request_facts().global_peak_upper())
            .unwrap();
        let result = prepared
            .retain(&source, &mut reservation, &mut work)
            .unwrap();
        let slice = result.slice(1, 1);
        let data = result.to_data();
        let into_data = result.clone().into_data();
        drop(reservation);
        drop(charge);
        drop(source);
        drop(array);
        drop(result);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(slice);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        host.zero();
        assert_eq!(data.len(), 3);
        assert_eq!(into_data.len(), 3);
    }
    let array: ArrayRef = Arc::new(StructArray::new(
        vec![Arc::new(Field::new("nulls", DataType::Null, true))].into(),
        vec![Arc::new(NullArray::new(3))],
        None,
    ));
    let host = Arc::new(Host::default());
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let drops = Arc::new(AtomicUsize::new(0));
    let source =
        SourceBackingOwner::try_from_owned_direct(Owner(drops.clone()), host.clone(), &mut work)
            .unwrap();
    let prepared = PreparedSourceCustody::prepare(&array, &mut work).unwrap();
    let charge = OpaqueRetainedCharge::try_new(host.clone()).unwrap();
    let mut reservation = charge
        .reserve_operation(prepared.request_facts().global_peak_upper())
        .unwrap();
    let result = prepared
        .retain(&source, &mut reservation, &mut work)
        .unwrap();
    drop(reservation);
    drop(charge);
    drop(source);
    drop(array);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    host.zero();
    assert_eq!(result.len(), 3);
    assert_eq!(result.to_data().child_data().len(), 1);
}

#[test]
fn source_custody_direct_latched_retain_precedes_a_competing_wrong_host() {
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let host = Arc::new(Host::default());
    let other = Arc::new(Host::default());
    let good = Control::default();
    let mut preparation = EvaluationCheckpoints::new(&good);
    let source =
        SourceBackingOwner::try_from_owned_direct((), host.clone(), &mut preparation).unwrap();
    for cause in causes() {
        let prepared = PreparedSourceCustody::prepare(&array, &mut preparation).unwrap();
        let charge = OpaqueRetainedCharge::try_new(other.clone()).unwrap();
        let mut reservation = charge
            .reserve_operation(prepared.request_facts().global_peak_upper())
            .unwrap();
        let control = Control {
            refusal: Some((0, cause.clone())),
            ..Control::default()
        };
        let mut work = EvaluationCheckpoints::new(&control);
        assert_eq!(work.flush(), Err(cause.clone()));
        assert!(
            matches!(prepared.retain(&source, &mut reservation, &mut work), Err(e) if e == cause)
        );
        assert_eq!(control.trace.lock().unwrap().len(), 1);
    }
    drop(source);
    host.zero();
    other.zero();
}

#[test]
fn source_custody_direct_absent_opaque_capability_refuses_before_any_typed_request() {
    struct NoOpaque;
    impl AggregateStateAllocator for NoOpaque {
        fn allocate(&self, _: Layout) -> Result<NonNull<u8>, KernelFailure> {
            panic!("capability refusal precedes all allocations")
        }
        unsafe fn release(&self, _: NonNull<u8>, _: Layout) {
            panic!("no block was created")
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let result = SourceBackingOwner::try_from_owned_direct(
        Owner(drops.clone()),
        Arc::new(NoOpaque),
        &mut work,
    );
    let cause = match result {
        Err(cause) => cause,
        Ok(_) => panic!("no capability"),
    };
    assert_eq!(
        cause,
        invalid("opaque aggregate requires an actual host reservation capability")
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(*control.trace.lock().unwrap(), vec![0]);
}

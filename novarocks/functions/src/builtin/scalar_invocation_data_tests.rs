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
use crate::opaque_memory::{OpaqueAllocationHost, OpaqueRetainedCharge};
use crate::{
    AggregateStateAllocator, FunctionValueType, KernelDiagnostic, KernelEvaluationControl,
};
use arrow_schema::DataType;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::{
    alloc::Layout,
    ptr::NonNull,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Ledger {
    attempts: usize,
    blocks: Vec<(usize, Layout)>,
    opaque: usize,
    opaque_attempts: usize,
}
struct Host {
    ledger: Mutex<Ledger>,
    actual_limit: usize,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
    opaque_refusal: Mutex<Option<KernelFailure>>,
}
impl Host {
    fn with_limit(actual_limit: usize) -> Arc<Self> {
        Arc::new(Self {
            ledger: Mutex::new(Ledger::default()),
            actual_limit,
            refusal: Mutex::new(None),
            opaque_refusal: Mutex::new(None),
        })
    }
    fn attempts(&self) -> usize {
        self.ledger.lock().unwrap().attempts
    }
    fn released(&self) {
        let l = self.ledger.lock().unwrap();
        assert!(l.blocks.is_empty());
        assert_eq!(l.opaque, 0);
    }
}
impl AggregateStateAllocator for Host {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut l = self.ledger.lock().unwrap();
        let at = l.attempts;
        l.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap() {
            if at == *stop {
                return Err(cause.clone());
            }
        }
        if l.opaque + l.blocks.iter().map(|(_, v)| v.size()).sum::<usize>() + layout.size()
            > self.actual_limit
        {
            return Err(KernelFailure::ResourceExhausted);
        }
        let ptr = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        l.blocks.push((ptr.as_ptr().addr(), layout));
        Ok(ptr)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut l = self.ledger.lock().unwrap();
        let at = l
            .blocks
            .iter()
            .position(|(p, t)| *p == pointer.as_ptr().addr() && *t == layout)
            .expect("exact live host block");
        l.blocks.swap_remove(at);
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
impl OpaqueAllocationHost for Host {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        let mut l = self.ledger.lock().unwrap();
        l.opaque_attempts += 1;
        if let Some(cause) = &*self.opaque_refusal.lock().unwrap() {
            return Err(cause.clone());
        }
        if l.opaque + l.blocks.iter().map(|(_, v)| v.size()).sum::<usize>() + bytes
            > self.actual_limit
        {
            return Err(KernelFailure::ResourceExhausted);
        }
        l.opaque += bytes;
        Ok(())
    }
    fn release_opaque(&self, bytes: usize) {
        let mut l = self.ledger.lock().unwrap();
        assert!(bytes <= l.opaque);
        l.opaque -= bytes;
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
        let mut t = self.trace.lock().unwrap();
        let at = t.len();
        if let Some((stop, _)) = &self.refusal {
            assert!(at <= *stop, "callback after first cause");
        }
        t.push(units);
        match &self.refusal {
            Some((stop, cause)) if at == *stop => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("private diagnostic transport never waits")
    }
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        KernelFailure::InvalidProgram(KernelDiagnostic::new("original host cause")),
        KernelFailure::Internal(KernelDiagnostic::new("original host cause")),
        KernelFailure::Operational(KernelDiagnostic::new("original host cause")),
        KernelFailure::InstanceFailed,
    ]
}
fn contract() -> Arc<ScalarCallContract> {
    // The actual installed MD5 author supplies immutable scalar identity for
    // carrier-only tests. These are not ARRAY producer/ABI acceptance probes.
    let prepared = super::super::string_md5_owner::prepared_for_test_with_policy(
        "md5",
        &[FunctionValueType::new(DataType::Utf8, true)],
        DecimalOverflowPolicy::OutputNull,
    )
    .unwrap();
    Arc::clone(prepared.contract())
}
fn reserve(host: &Arc<Host>, bytes: usize) -> OpaqueReservation {
    OpaqueRetainedCharge::try_new(host.clone())
        .unwrap()
        .reserve_operation(bytes)
        .unwrap()
}
fn prepare(
    host: &Arc<Host>,
    allocator: &HostAggregateAllocator,
    contract: Arc<ScalarCallContract>,
    selection: Selection<'_>,
    control: &Control,
    bytes: usize,
) -> Result<ScalarDataSlot, KernelFailure> {
    let reservation = reserve(host, bytes);
    let mut work = EvaluationCheckpoints::new(control);
    let result = ScalarDataSlot::prepare_for_actual_invocation(
        contract,
        selection,
        allocator,
        reservation,
        &mut work,
    );
    if result.is_err() {
        return result;
    }
    work.finish()?;
    result
}

#[test]
fn scalar_whole_data_private_original_string_move_clone_and_last_drop_keep_actual_charge() {
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let original_contract = contract();
    // This explicit fixture extent is admitted before constructing its String.
    // It is not an ARRAY diagnostic bound or a production default capacity.
    let slot = prepare(
        &host,
        &allocator,
        original_contract.clone(),
        Selection::all(3),
        &Control::default(),
        8192,
    )
    .unwrap();
    let message = "original µ diagnostic: ".repeat(101);
    assert!(message.len() > crate::MAX_ROW_ERROR_MESSAGE_BYTES);
    assert!(message.capacity() <= 8192);
    let pointer = message.as_ptr();
    let cap = message.capacity();
    let attempts = host.attempts();
    let data = slot.publish_original(message);
    assert_eq!(data.message().as_ptr(), pointer);
    assert_eq!(data.0.original.get().unwrap().capacity(), cap);
    assert!(Arc::ptr_eq(data.contract(), &original_contract));
    let copy = data.clone();
    assert!(data.same_backing(&copy));
    assert_eq!(host.attempts(), attempts);
    assert_eq!(data.retained_admission_bytes(), 8192);
    assert!(data.retained_backing_bytes() > cap);
    assert_eq!(
        data.retained_backing_bytes(),
        cap + host
            .ledger
            .lock()
            .unwrap()
            .blocks
            .iter()
            .map(|(_, layout)| layout.size())
            .sum::<usize>()
    );
    drop(slot);
    drop(allocator);
    drop(data);
    assert_eq!(host.ledger.lock().unwrap().opaque, 8192);
    assert_eq!(copy.message().as_ptr(), pointer);
    drop(copy);
    host.released();
}
#[test]
fn scalar_whole_data_private_domain_preserves_explicit_empty_sparse_and_first_publication() {
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let actual = contract();
    for selection in [
        Selection::all(0),
        Selection::try_sparse(11, &[2, 8]).unwrap(),
        Selection::try_sparse(11, &[]).unwrap(),
    ] {
        let slot = prepare(
            &host,
            &allocator,
            actual.clone(),
            selection,
            &Control::default(),
            1024,
        )
        .unwrap();
        let first = slot.publish_original(String::from("original whole-call Data"));
        let repeated = slot.publish_original(String::from("later must not replace first Data"));
        assert!(first.same_backing(&repeated));
        assert_eq!(first.batch_rows(), selection.batch_rows());
        assert_eq!(first.source_len(), selection.len());
        for i in 0..selection.len() {
            assert_eq!(first.source_row(i), selection.row(i));
        }
        assert_eq!(first.source_row(selection.len()), None);
        assert_eq!(first.message(), "original whole-call Data");
    }
    drop(allocator);
    host.released();
}
#[test]
fn scalar_whole_data_private_every_metadata_domain_header_allocation_retains_seven_causes() {
    let actual = contract();
    let rows: Vec<_> = (0..321).map(|x| x * 2).collect();
    let selection = Selection::try_sparse(1000, &rows).unwrap();
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let start = host.attempts();
    let slot = prepare(
        &host,
        &allocator,
        actual.clone(),
        selection,
        &Control::default(),
        1024,
    )
    .unwrap();
    let steps = host.attempts() - start;
    assert!(steps >= 2);
    drop(slot);
    drop(allocator);
    host.released();
    for cause in causes() {
        let host = Host::with_limit(128 * 1024);
        *host.refusal.lock().unwrap() = Some((0, cause.clone()));
        assert!(matches!(HostAggregateAllocator::try_new(host.clone()),Err(error)if error==cause));
        host.released();
        for offset in 0..steps {
            let host = Host::with_limit(128 * 1024);
            let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
            let start = host.attempts();
            *host.refusal.lock().unwrap() = Some((start + offset, cause.clone()));
            assert!(
                matches!(prepare(&host,&allocator,actual.clone(),selection,&Control::default(),1024),Err(error)if error==cause)
            );
            assert_eq!(host.attempts(), start + offset + 1);
            assert_eq!(host.ledger.lock().unwrap().opaque, 0);
            drop(allocator);
            host.released();
        }
    }
}
#[test]
fn scalar_whole_data_private_every_observed_domain_checkpoint_retains_cause_without_tail() {
    let actual = contract();
    let rows: Vec<_> = (0..321).map(|x| x * 2).collect();
    let selection = Selection::try_sparse(1000, &rows).unwrap();
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let control = Control::default();
    let slot = prepare(&host, &allocator, actual.clone(), selection, &control, 1024).unwrap();
    let steps = control.trace.lock().unwrap().len();
    assert!(control.trace.lock().unwrap().contains(&256));
    drop(slot);
    drop(allocator);
    host.released();
    for cause in causes() {
        for stop in 0..steps {
            let host = Host::with_limit(128 * 1024);
            let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
            let control = Control {
                refusal: Some((stop, cause.clone())),
                ..Control::default()
            };
            assert!(
                matches!(prepare(&host,&allocator,actual.clone(),selection,&control,1024),Err(error)if error==cause)
            );
            assert_eq!(control.trace.lock().unwrap().len(), stop + 1);
            assert_eq!(host.ledger.lock().unwrap().opaque, 0);
            drop(allocator);
            host.released();
        }
    }
}
#[test]
fn scalar_whole_data_private_missing_opaque_host_is_named_and_has_no_allocation() {
    struct NoOpaqueHost(Arc<Host>);
    impl AggregateStateAllocator for NoOpaqueHost {
        fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
            self.0.allocate(layout)
        }
        unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
            unsafe { self.0.release(pointer, layout) };
        }
    }
    let host = Host::with_limit(128 * 1024);
    let no_opaque: Arc<dyn AggregateStateAllocator> = Arc::new(NoOpaqueHost(host.clone()));
    let cause = match OpaqueRetainedCharge::try_new(no_opaque) {
        Ok(_) => panic!("missing host cannot mint a diagnostic scope"),
        Err(cause) => cause,
    };
    assert_eq!(
        cause.to_string(),
        "invalid kernel program: opaque aggregate requires an actual host reservation capability"
    );
    assert_eq!(host.attempts(), 0);
    host.released();
}

#[test]
fn scalar_whole_data_private_foreign_scope_and_actual_budget_refusals_do_not_publish() {
    let host = Host::with_limit(128 * 1024);
    let other = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let reservation = reserve(&other, 1024);
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    assert!(matches!(
        ScalarDataSlot::prepare_for_actual_invocation(
            contract(),
            Selection::all(1),
            &allocator,
            reservation,
            &mut work
        ),
        Err(KernelFailure::InvalidProgram(_))
    ));
    assert!(control.trace.lock().unwrap().is_empty());
    other.released();
    drop(allocator);
    host.released();
    for cause in causes() {
        let host = Host::with_limit(128 * 1024);
        *host.opaque_refusal.lock().unwrap() = Some(cause.clone());
        let charge = OpaqueRetainedCharge::try_new(host.clone()).unwrap();
        assert!(matches!(charge.reserve_operation(1024),Err(error)if error==cause));
        drop(charge);
        host.released();
    }
    let host = Host::with_limit(17);
    let charge = OpaqueRetainedCharge::try_new(host.clone()).unwrap();
    assert!(matches!(
        charge.reserve_operation(18),
        Err(KernelFailure::ResourceExhausted)
    ));
    drop(charge);
    host.released();
}
#[test]
fn scalar_whole_data_private_data_and_seven_kernel_causes_end_instance_without_footer_or_replay() {
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let slot = prepare(
        &host,
        &allocator,
        contract(),
        Selection::all(0),
        &Control::default(),
        1024,
    )
    .unwrap();
    let data = slot.publish_original(String::from("original invoked-empty whole-call Data"));
    let mut latch = ScalarInvocationLatch::default();
    let result = latch.execute::<()>(
        || Err(ScalarInvocationFailure::Data(data.clone())),
        || panic!("Data footer must not run"),
    );
    assert!(
        matches!(result,Err(ScalarInvocationFailure::Data(ref got))if got.same_backing(&data)&&got.message()=="original invoked-empty whole-call Data")
    );
    assert!(matches!(
        latch.execute::<()>(
            || panic!("failed invocation replay"),
            || panic!("failed invocation footer")
        ),
        Err(ScalarInvocationFailure::Kernel(
            KernelFailure::InstanceFailed
        ))
    ));
    for cause in causes() {
        let mut latch = ScalarInvocationLatch::default();
        assert_eq!(
            latch.execute::<()>(
                || Err(ScalarInvocationFailure::Kernel(cause.clone())),
                || panic!("Kernel footer must not run")
            ),
            Err(ScalarInvocationFailure::Kernel(cause))
        );
        assert!(matches!(
            latch.execute::<()>(|| panic!("failed replay"), || panic!("failed footer")),
            Err(ScalarInvocationFailure::Kernel(
                KernelFailure::InstanceFailed
            ))
        ));
    }
    let mut latch = ScalarInvocationLatch::default();
    assert_eq!(
        latch.execute(|| Ok(7), || Err(KernelFailure::DeadlineExceeded)),
        Err(ScalarInvocationFailure::Kernel(
            KernelFailure::DeadlineExceeded
        ))
    );
    drop(result);
    drop(data);
    drop(slot);
    drop(allocator);
    host.released();
}

#[path = "scalar_invocation_lifecycle_tests.rs"]
mod scalar_invocation_lifecycle_tests;

#[path = "scalar_invocation_data_direct_tests.rs"]
mod scalar_invocation_data_direct_tests;

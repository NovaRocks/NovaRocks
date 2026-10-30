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

use super::super::array_struct_subfield_core::{project, project_with_port};
use super::super::scalar_invocation_data::{ScalarInvocationData, ScalarInvocationLatch};
use super::*;
use crate::opaque_memory::OpaqueAllocationHost;
use crate::{
    AggregateStateAllocator, FunctionValueType, KernelDiagnostic, KernelEvaluationControl,
};
use arrow_array::{Array, ArrayRef, Int32Array, ListArray, StringArray, StructArray};
use arrow_buffer::OffsetBuffer;
use arrow_schema::DataType;
use arrow_schema::Field;
use novarocks_type_contract::DecimalOverflowPolicy;
use std::collections::HashMap;
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
    opaque_refusal: Mutex<Option<(usize, KernelFailure)>>,
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
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap()
            && at == *stop
        {
            return Err(cause.clone());
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
        let at = l.opaque_attempts;
        l.opaque_attempts += 1;
        if let Some((stop, cause)) = &*self.opaque_refusal.lock().unwrap()
            && at == *stop
        {
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
    // diagnostic-only tests. These are not ARRAY producer/ABI acceptance probes.
    let prepared = super::super::string_md5_owner::prepared_for_test_with_policy(
        "md5",
        &[FunctionValueType::new(DataType::Utf8, true)],
        DecimalOverflowPolicy::OutputNull,
    )
    .unwrap();
    Arc::clone(prepared.contract())
}

fn list(rows: usize) -> ArrayRef {
    let fields = vec![Arc::new(Field::new("ID", DataType::Int32, false))].into();
    let values = Arc::new(StructArray::new(
        fields,
        vec![Arc::new(Int32Array::from_iter_values(0..rows as i32))],
        None,
    ));
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", values.data_type().clone(), true)),
        OffsetBuffer::new((0..=rows as i32).collect::<Vec<_>>().into()),
        values,
        None,
    ))
}
fn invoke(
    host: &Arc<Host>,
    allocator: &HostAggregateAllocator,
    call: &Arc<ScalarCallContract>,
    input: &ArrayRef,
    names: &ArrayRef,
    control: &Control,
) -> Result<ArrayRef, ScalarInvocationFailure> {
    let mut work = EvaluationCheckpoints::new(control);
    let mut port = HostProjectionDiagnostics::try_new(
        allocator,
        host.clone(),
        Arc::clone(call),
        Selection::all(input.len()),
        &mut work,
    )
    .map_err(ScalarInvocationFailure::Kernel)?;
    // This is the actual sole ARRAY core, not a mirrored formatter/decoder.
    // The contract is explicit carrier-only identity; no ARRAY ABI is claimed.
    project_with_port(input, names, None, &mut port)
}
fn data(result: Result<ArrayRef, ScalarInvocationFailure>) -> ScalarInvocationData {
    match result {
        Err(ScalarInvocationFailure::Data(data)) => data,
        other => panic!("expected original whole Data, got {other:?}"),
    }
}
#[test]
fn array_diagnostic_source_missing_field_original_message_and_last_drop() {
    let input = list(1);
    let names: ArrayRef = Arc::new(StringArray::from(vec!["missing"]));
    let original = project(&input, &names, None).unwrap_err().to_string();
    assert_eq!(
        original,
        "__array_struct_subfield field 'missing' does not exist"
    );
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let call = contract();
    let actual = data(invoke(
        &host,
        &allocator,
        &call,
        &input,
        &names,
        &Control::default(),
    ));
    assert_eq!(actual.message(), original);
    assert!(Arc::ptr_eq(actual.contract(), &call));
    assert_eq!(actual.retained_admission_bytes(), original.len());
    let clone = actual.clone();
    assert!(clone.same_backing(&actual));
    drop(actual);
    drop(allocator);
    assert_eq!(host.ledger.lock().unwrap().opaque, original.len());
    drop(clone);
    host.released();
}
#[test]
fn array_diagnostic_source_field_name_values_and_carrier_contracts_are_distinct() {
    let input = list(2);
    let names: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(Vec::<&str>::new())),
        Arc::new(StringArray::from(vec![None::<&str>, Some("ID")])),
        Arc::new(StringArray::from(vec!["ID", "id"])),
    ];
    let call = contract();
    for name in names {
        let original = project(&input, &name, None).unwrap_err().to_string();
        let host = Host::with_limit(128 * 1024);
        let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
        let actual = data(invoke(
            &host,
            &allocator,
            &call,
            &input,
            &name,
            &Control::default(),
        ));
        assert_eq!(actual.message(), original);
        drop(actual);
        drop(allocator);
        host.released();
    }
    use novarocks_failure_contract::{FailureClass, FailureProjection};
    let wrong_names: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let raw = project(&input, &wrong_names, None).unwrap_err();
    let error = invoke(
        &host,
        &allocator,
        &call,
        &input,
        &wrong_names,
        &Control::default(),
    )
    .unwrap_err();
    let ScalarInvocationFailure::Kernel(error) = error else {
        panic!("a physical field-name carrier mismatch must not be invocation Data");
    };
    assert_eq!(error.failure_fact(), raw.failure_fact());
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    drop(allocator);
    host.released();
    let field = Arc::new(
        Field::new("ID", DataType::Int32, false).with_metadata(HashMap::from([(
            "actual-source".to_string(),
            "µ actual metadata ".repeat(111),
        )])),
    );
    let weak = Arc::downgrade(&field);
    let input: ArrayRef = Arc::new(StructArray::new(
        vec![field].into(),
        vec![Arc::new(Int32Array::from(vec![1]))],
        None,
    ));
    let names: ArrayRef = Arc::new(StringArray::from(vec!["ID"]));
    let raw = project(&input, &names, None).unwrap_err();
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let error = invoke(
        &host,
        &allocator,
        &call,
        &input,
        &names,
        &Control::default(),
    )
    .unwrap_err();
    let ScalarInvocationFailure::Kernel(error) = error else {
        panic!("a physical input carrier mismatch must not be invocation Data");
    };
    assert_eq!(error.failure_fact(), raw.failure_fact());
    assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    assert!(error.to_string().len() <= novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES);
    drop(input);
    assert!(weak.upgrade().is_none());
    drop(allocator);
    host.released();
}

#[test]
fn array_diagnostic_source_every_real_allocation_and_opaque_request_keep_seven_causes() {
    let input = list(1);
    let names: ArrayRef = Arc::new(StringArray::from(vec!["missing"]));
    let call = contract();
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let before = host.attempts();
    let actual = data(invoke(
        &host,
        &allocator,
        &call,
        &input,
        &names,
        &Control::default(),
    ));
    let allocations = host.attempts() - before;
    let opaque = host.ledger.lock().unwrap().opaque_attempts;
    assert!(allocations > 0);
    assert_eq!(opaque, 2);
    drop(actual);
    drop(allocator);
    host.released();
    for index in 0..allocations {
        for cause in causes() {
            let host = Host::with_limit(128 * 1024);
            let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
            let start = host.attempts();
            *host.refusal.lock().unwrap() = Some((start + index, cause.clone()));
            assert_eq!(
                invoke(
                    &host,
                    &allocator,
                    &call,
                    &input,
                    &names,
                    &Control::default()
                )
                .unwrap_err(),
                ScalarInvocationFailure::Kernel(cause)
            );
            assert_eq!(host.attempts(), start + index + 1, "no later allocation");
            drop(allocator);
            host.released();
        }
    }
    for index in 0..opaque {
        for cause in causes() {
            let host = Host::with_limit(128 * 1024);
            let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
            *host.opaque_refusal.lock().unwrap() = Some((index, cause.clone()));
            assert_eq!(
                invoke(
                    &host,
                    &allocator,
                    &call,
                    &input,
                    &names,
                    &Control::default()
                )
                .unwrap_err(),
                ScalarInvocationFailure::Kernel(cause)
            );
            assert_eq!(host.ledger.lock().unwrap().opaque_attempts, index + 1);
            drop(allocator);
            host.released();
        }
    }
}
#[test]
fn array_diagnostic_source_every_actual_checkpoint_has_no_tail_and_failed_latch() {
    let input = list(321);
    let names: ArrayRef = Arc::new(StringArray::from(vec!["missing"; 321]));
    let call = contract();
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let control = Control::default();
    let actual = data(invoke(&host, &allocator, &call, &input, &names, &control));
    let trace = control.trace.lock().unwrap().clone();
    assert!(trace.contains(&256));
    drop(actual);
    drop(allocator);
    host.released();
    for index in 0..trace.len() {
        for cause in causes() {
            let host = Host::with_limit(128 * 1024);
            let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
            let control = Control {
                trace: Mutex::new(Vec::new()),
                refusal: Some((index, cause.clone())),
            };
            let mut latch = ScalarInvocationLatch::default();
            let result = latch.execute(
                || invoke(&host, &allocator, &call, &input, &names, &control),
                || panic!("failure must not invoke a success footer"),
            );
            assert_eq!(result.unwrap_err(), ScalarInvocationFailure::Kernel(cause));
            assert_eq!(*control.trace.lock().unwrap(), trace[..=index]);
            let repeat: Result<ArrayRef, _> = latch.execute(
                || panic!("failed instance must not replay core"),
                || panic!("failed instance must not run footer"),
            );
            assert_eq!(
                repeat.unwrap_err(),
                ScalarInvocationFailure::Kernel(KernelFailure::InstanceFailed)
            );
            drop(allocator);
            host.released();
        }
    }
}
#[test]
fn array_diagnostic_source_published_data_does_not_run_footer_or_replay() {
    let input = list(1);
    let names: ArrayRef = Arc::new(StringArray::from(vec!["missing"]));
    let call = contract();
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let mut latch = ScalarInvocationLatch::default();
    let result = latch.execute(
        || {
            invoke(
                &host,
                &allocator,
                &call,
                &input,
                &names,
                &Control::default(),
            )
        },
        || panic!("published Data must not invoke optional post-control"),
    );
    let actual = data(result);
    assert_eq!(
        actual.message(),
        project(&input, &names, None).unwrap_err().to_string()
    );
    let again: Result<ArrayRef, _> = latch.execute(|| panic!("no replay"), || panic!("no footer"));
    assert_eq!(
        again.unwrap_err(),
        ScalarInvocationFailure::Kernel(KernelFailure::InstanceFailed)
    );
    drop(actual);
    drop(allocator);
    host.released();
}
#[test]
fn array_diagnostic_source_successful_v1_projection_preserves_original_values_and_metadata() {
    let input = list(3);
    let names: ArrayRef = Arc::new(StringArray::from(vec!["ID"; 3]));
    let target = Arc::new(
        Field::new("selected", DataType::Int32, true)
            .with_metadata(HashMap::from([("original".into(), "field".into())])),
    );
    let out = project(&input, &names, Some(&DataType::List(target.clone()))).unwrap();
    let list = out.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(list.data_type(), &DataType::List(target));
    assert_eq!(
        list.values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[0, 1, 2]
    );
    assert_eq!(list.value_offsets(), &[0, 1, 2, 3]);
}

#[test]
fn array_diagnostic_source_actual_empty_call_keeps_wrongcase_data_without_inventing_activation() {
    let input = list(0);
    let names: ArrayRef = Arc::new(StringArray::from(vec!["missing"]));
    let original = project(&input, &names, None).unwrap_err().to_string();
    let host = Host::with_limit(128 * 1024);
    let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
    let call = contract();
    // An explicit direct invocation is already known here. This test does not
    // make an empty Project/Filter call or infer activation from Selection.
    let actual = data(invoke(
        &host,
        &allocator,
        &call,
        &input,
        &names,
        &Control::default(),
    ));
    assert_eq!(actual.message(), original);
    assert_eq!(actual.batch_rows(), 0);
    assert_eq!(actual.source_len(), 0);
    assert_eq!(actual.source_row(0), None);
    drop(actual);
    drop(allocator);
    host.released();
}

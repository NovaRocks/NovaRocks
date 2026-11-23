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
use arrow_array::StringArray;
use novarocks_type_contract::{CompileControlError, CompilePhase, PureCompileControl};
use std::{alloc::Layout, ptr::NonNull, sync::Mutex, time::Duration};

struct Compile;
impl PureCompileControl for Compile {
    fn checkpoint(&self, _: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert!(units <= 256);
        Ok(())
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
            assert!(at <= *stop, "callback after primary refusal");
        }
        trace.push(units);
        match &self.refusal {
            Some((stop, cause)) if *stop == at => Err(cause.clone()),
            _ => Ok(()),
        }
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("GROUP_CONCAT never waits")
    }
}
#[derive(Default)]
struct Ledger {
    attempts: usize,
    bytes: usize,
    peak: usize,
    live: Vec<(usize, Layout)>,
    metadata: Option<(usize, Layout)>,
}
#[derive(Default)]
struct Host {
    ledger: Mutex<Ledger>,
    refusal: Mutex<Option<(usize, KernelFailure)>>,
}
impl AggregateStateAllocator for Host {
    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        assert_ne!(layout.size(), 0);
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger.attempts;
        ledger.attempts += 1;
        if let Some((stop, cause)) = &*self.refusal.lock().unwrap()
            && *stop == at
        {
            return Err(cause.clone());
        }
        let pointer = NonNull::new(unsafe { std::alloc::alloc(layout) })
            .ok_or(KernelFailure::ResourceExhausted)?;
        ledger.bytes += layout.size();
        ledger.peak = ledger.peak.max(ledger.bytes);
        let block = (pointer.as_ptr().addr(), layout);
        if ledger.metadata.is_none() {
            ledger.metadata = Some(block);
        }
        ledger.live.push(block);
        Ok(pointer)
    }
    unsafe fn release(&self, pointer: NonNull<u8>, layout: Layout) {
        let mut ledger = self.ledger.lock().unwrap();
        let at = ledger
            .live
            .iter()
            .position(|(address, actual)| *address == pointer.as_ptr().addr() && *actual == layout)
            .expect("exact block released once");
        ledger.live.swap_remove(at);
        ledger.bytes -= layout.size();
        unsafe { std::alloc::dealloc(pointer.as_ptr(), layout) };
    }
}
fn arm_refusal(host: &Host, offset: usize, cause: KernelFailure) {
    let next = host.ledger.lock().unwrap().attempts;
    *host.refusal.lock().unwrap() = Some((next + offset, cause));
}
fn assert_metadata_only(host: &Host) {
    let ledger = host.ledger.lock().unwrap();
    let metadata = ledger
        .metadata
        .expect("successful constructor allocated metadata");
    assert_eq!(ledger.live.as_slice(), &[metadata]);
    assert_eq!(ledger.bytes, metadata.1.size());
}
fn causes() -> [KernelFailure; 7] {
    [
        KernelFailure::Cancelled,
        KernelFailure::DeadlineExceeded,
        KernelFailure::ResourceExhausted,
        invalid("host original"),
        internal("host original"),
        KernelFailure::Operational(KernelDiagnostic::new("host original")),
        KernelFailure::InstanceFailed,
    ]
}
fn listagg_contributors(
    values: &[Option<&str>],
    separators: &[Option<&str>],
    keys: &[i64],
    error: bool,
    filler: &str,
    with_count: bool,
    distinct: bool,
) -> Result<ArrayRef, KernelFailure> {
    let host = Arc::new(Host::default());
    let mut work = ScalarWork::new(None);
    let result = {
        let allocator = HostAggregateAllocator::try_new(host.clone()).unwrap();
        let mut state = ListaggState::new(allocator);
        let columns = [
            Arc::new(StringArray::from(values.to_vec())) as ArrayRef,
            Arc::new(StringArray::from(separators.to_vec())),
            Arc::new(arrow_array::BooleanArray::from(vec![error; values.len()])),
            Arc::new(StringArray::from(vec![filler; values.len()])),
            Arc::new(arrow_array::BooleanArray::from(vec![
                with_count;
                values.len()
            ])),
            Arc::new(arrow_array::Int64Array::from(keys.to_vec())),
        ];
        for index in 0..values.len() {
            let addresses = columns.each_ref().map(|array| (array, index));
            state.update_row(&addresses, &mut work).unwrap();
        }
        core::build_listagg_array(
            std::iter::once(&state),
            distinct,
            &[true],
            &[false],
            &mut work,
        )
    };
    assert_eq!(host.ledger.lock().unwrap().bytes, 0);
    result
}

#[test]
fn listagg_orders_before_freezing_options_and_distinct_compares_all_arguments() {
    let result = listagg_contributors(
        &[Some("a"), Some("b"), Some("b"), Some("a"), Some("skip")],
        &[Some(":"), Some("|"), Some("|"), Some("-"), None],
        &[3, 2, 4, 5, 1],
        true,
        "...",
        false,
        true,
    )
    .unwrap();
    let result = result.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(result.value(0), "b|a|a");
}

#[test]
fn listagg_enforces_whole_value_prefix_and_counts_empty_strings() {
    let value = "é".repeat(524_288);
    let result = listagg_contributors(
        &[Some(&value), Some("b"), Some("c")],
        &[Some("|"); 3],
        &[0, 1, 2],
        false,
        "!",
        true,
        false,
    )
    .unwrap();
    let result = result
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0);
    assert_eq!(result.len(), 1_048_581);
    assert_eq!(&result[..value.len()], value);
    assert!(result.ends_with("|!(2)"));
    let result = listagg_contributors(
        &[Some(""), Some(&value)],
        &[Some("|"); 2],
        &[0, 1],
        false,
        "!",
        true,
        false,
    )
    .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "|!(1)"
    );
}

#[test]
fn listagg_reports_its_own_data_failures_and_returns_null_for_empty() {
    use novarocks_failure_contract::{FailureProjection, kernel_reason};
    let value = "a".repeat(1_048_577);
    let result = listagg_contributors(
        &[Some(&value)],
        &[Some("|")],
        &[0],
        true,
        "...",
        false,
        false,
    );
    assert_eq!(
        result.unwrap_err().failure_fact().reason(),
        kernel_reason::EXCEEDED_FUNCTION_MEMORY_LIMIT
    );
    let filler = "!".repeat(65_537);
    let result = listagg_contributors(
        &[Some("a")],
        &[Some("|")],
        &[0],
        true,
        &filler,
        false,
        false,
    );
    assert_eq!(
        result.unwrap_err().failure_fact().reason(),
        kernel_reason::INVALID_FUNCTION_ARGUMENT
    );
    let result =
        listagg_contributors(&[None], &[Some("|")], &[0], true, &filler, false, false).unwrap();
    assert!(result.is_null(0));
    let result = listagg_contributors(
        &[Some("a")],
        &[Some("|")],
        &[0],
        true,
        &filler[1..],
        false,
        false,
    )
    .unwrap();
    assert_eq!(
        result
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "a"
    );
}

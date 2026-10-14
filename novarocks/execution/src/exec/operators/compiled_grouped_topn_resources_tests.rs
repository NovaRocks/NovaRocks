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

//! Resource admission and real key backing lifetime, not an RSS/performance
//! gate. Existing source-custody tests exercise its author, while these test
//! this caller's production tracker host and stock handoff. The oracle is the
//! immutable tracker limit, original typed cause and final alias destruction.
//! Every test owns an isolated tracker/array; no service or fixture is used.
//! Execution maintains this short affected-change lane beside the consumer.

use super::*;
use crate::exec::operators::compiled_aggregate::expression_allocation_host;
use crate::runtime::mem_tracker::MemTracker;
use arrow::array::{Int64Array, StringArray};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

struct Control(Option<KernelFailure>);
impl KernelEvaluationControl for Control {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        self.0.clone().map_or(Ok(()), Err)
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("key custody never waits")
    }
}

fn shape() -> ArrowWorkShape {
    ArrowWorkShape {
        rows: 3,
        cells: 3,
        payload_bytes: 24,
        fields: 1,
        buffers: 2,
    }
}

#[test]
fn public_work_refuses_overflow_and_real_host_limit_without_leaving_a_charge() {
    let tracker = MemTracker::new_root("grouped-key-work-refusal");
    tracker.install_limit_once(1).unwrap();
    let host = expression_allocation_host(Arc::clone(&tracker));
    assert!(matches!(
        ArrowWorkBudget::admit(host.clone(), shape()),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(tracker.current(), 0);
    assert!(matches!(
        ArrowWorkBudget::admit(
            host,
            ArrowWorkShape {
                rows: usize::MAX,
                ..shape()
            }
        ),
        Err(KernelFailure::ResourceExhausted)
    ));
    assert_eq!(tracker.current(), 0);
}

#[test]
fn returned_key_stock_survives_the_last_buffer_alias_on_another_thread() {
    let tracker = MemTracker::new_root("grouped-key-last-buffer");
    let host = expression_allocation_host(Arc::clone(&tracker));
    let work = ArrowWorkBudget::admit(host, shape()).unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(9)]));
    let output = retain_new_key_array(array, &work, &Control(None)).unwrap();
    drop(work);
    let data = output.to_data();
    let last_buffer = data.buffers()[0].clone();
    drop(data);
    drop(output);
    assert!(
        tracker.current() > 0,
        "the actual source still owns its stock"
    );
    std::thread::spawn(move || drop(last_buffer))
        .join()
        .unwrap();
    assert_eq!(tracker.current(), 0);
}

#[test]
fn variable_key_array_alias_keeps_stock_after_the_processor_work_returns() {
    let tracker = MemTracker::new_root("grouped-key-last-array");
    let work =
        ArrowWorkBudget::admit(expression_allocation_host(tracker.clone()), shape()).unwrap();
    let array: ArrayRef = Arc::new(StringArray::from(vec![Some(""), None, Some("海港")]));
    let output = retain_new_key_array(array, &work, &Control(None)).unwrap();
    let alias = output.slice(2, 1);
    drop(output);
    drop(work);
    assert!(tracker.current() > 0);
    let text = alias.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(text.value(0), "海港");
    drop(alias);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn original_control_refusals_destroy_unpublished_stock_and_preserve_the_cause() {
    // Deadline here is an injected owner control verdict, not a claim that
    // RuntimeKernelControl acquired an independent deadline timer.
    for cause in [KernelFailure::Cancelled, KernelFailure::DeadlineExceeded] {
        let tracker = MemTracker::new_root("grouped-key-control-refusal");
        let work =
            ArrowWorkBudget::admit(expression_allocation_host(tracker.clone()), shape()).unwrap();
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(7), None, Some(9)]));
        let result = retain_new_key_array(array, &work, &Control(Some(cause.clone())));
        match (result, cause) {
            (Err(KernelFailure::Cancelled), KernelFailure::Cancelled)
            | (Err(KernelFailure::DeadlineExceeded), KernelFailure::DeadlineExceeded) => {}
            other => panic!("original control cause changed: {other:?}"),
        }
        drop(work);
        assert_eq!(tracker.current(), 0);
    }
}

#[test]
fn a_late_completion_refusal_drops_the_built_source_and_unpublished_output() {
    struct CompletionControl {
        calls: AtomicUsize,
        refuse_at: Option<usize>,
    }
    impl KernelEvaluationControl for CompletionControl {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            let ordinal = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.refuse_at == Some(ordinal) {
                Err(KernelFailure::Cancelled)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("key custody never waits")
        }
    }
    let tracker = MemTracker::new_root("grouped-key-completion-refusal");
    let work =
        ArrowWorkBudget::admit(expression_allocation_host(tracker.clone()), shape()).unwrap();
    let completed = CompletionControl {
        calls: AtomicUsize::new(0),
        refuse_at: None,
    };
    let array = || Arc::new(Int64Array::from(vec![Some(7), None, Some(9)])) as ArrayRef;
    drop(retain_new_key_array(array(), &work, &completed).unwrap());
    let total = completed.calls.load(Ordering::SeqCst);
    assert!(total > 1);
    // The last observed checkpoint is the original completion after source
    // construction/wrapping, not an early refusal before any stock admission.
    let cancelled = CompletionControl {
        calls: AtomicUsize::new(0),
        refuse_at: Some(total - 1),
    };
    assert!(matches!(
        retain_new_key_array(array(), &work, &cancelled),
        Err(KernelFailure::Cancelled)
    ));
    assert_eq!(cancelled.calls.load(Ordering::SeqCst), total);
    drop(work);
    assert_eq!(tracker.current(), 0);
}

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
//! Repeated progress notification with finite, independent metadata custody.
//!
//! Progress never supplies work-exit proof. A notification fault disables this
//! endpoint; it does not fail the Task or turn a stopped driver into pending work.
//! Opaque unwind payloads remain in this original endpoint. This cannot recover
//! panic=abort or hidden double panics inside a foreign destructor.

use crate::runtime::{
    runtime_state::RuntimeErrorState,
    task::{
        ExecutionFailure, ExecutionReason,
        io::{TaskEvent, TaskEventSink},
    },
};
use novarocks_failure_contract::{FailureClass, FailureFact, FailureProjection};
use std::{
    any::Any,
    mem::ManuallyDrop,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, OnceLock},
};

type Payload = ManuallyDrop<Box<dyn Any + Send>>;
#[derive(Clone, Copy, Debug)]
enum Phase {
    Call,
    CaptureDrop,
}
#[derive(Debug)]
struct ProgressFailure(Phase);
impl std::fmt::Display for ProgressFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "driver progress {:?} panicked", self.0)
    }
}
impl std::error::Error for ProgressFailure {}
impl FailureProjection for ProgressFailure {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Unexpected,
            match self.0 {
                Phase::Call => ExecutionReason::ProgressCallPanicked.reason(),
                Phase::CaptureDrop => ExecutionReason::ProgressCaptureDropPanicked.reason(),
            },
        )
    }
}
struct Custody {
    payloads: Mutex<[[Option<Payload>; 3]; 2]>,
    retained: OnceLock<Arc<Self>>,
}
impl Custody {
    fn save(
        self: &Arc<Self>,
        phase: Phase,
        payload: Box<dyn Any + Send>,
        control: &RuntimeErrorState,
    ) {
        let index = phase as usize;
        {
            let mut slots = self.payloads.lock().unwrap_or_else(|e| e.into_inner());
            assert!(
                slots[index][0].is_none(),
                "each progress fault has one original slot"
            );
            slots[index][0] = Some(ManuallyDrop::new(payload));
        }
        // Install original custody before either foreign diagnostic endpoint.
        self.retained.get_or_init(|| self.clone());
        let cause = ExecutionFailure::from_domain(ProgressFailure(phase));
        for (ordinal, report) in [
            RuntimeErrorState::report_cleanup as fn(&RuntimeErrorState, &ExecutionFailure),
            RuntimeErrorState::report_unresolved_exit,
        ]
        .into_iter()
        .enumerate()
        {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| report(control, &cause))) {
                self.payloads.lock().unwrap_or_else(|e| e.into_inner())[index][ordinal + 1] =
                    Some(ManuallyDrop::new(payload));
            }
        }
    }
}

pub(super) struct ProgressEndpoint {
    disabled: bool,
    custody: Arc<Custody>,
    control: Arc<RuntimeErrorState>,
}
impl ProgressEndpoint {
    pub(super) fn new(control: Arc<RuntimeErrorState>) -> Self {
        Self {
            disabled: false,
            custody: Arc::new(Custody {
                payloads: Mutex::new([[None, None, None], [None, None, None]]),
                retained: OnceLock::new(),
            }),
            control,
        }
    }
    pub(super) fn record(&mut self, sink: &dyn TaskEventSink, event: TaskEvent) {
        if self.disabled {
            return;
        }
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| sink.record(event))) {
            self.disabled = true;
            self.custody.save(Phase::Call, payload, &self.control);
        }
    }
    /// The driver invokes this only for its original progress holder. An
    /// unresolved work capsule instead retains that same holder without Drop.
    pub(super) fn retire(&mut self, sink: Arc<dyn TaskEventSink>) {
        self.disabled = true;
        if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(sink))) {
            self.custody
                .save(Phase::CaptureDrop, payload, &self.control);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::task::io::TaskProgress;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Opaque {
        drops: Arc<AtomicUsize>,
        _data: Arc<Vec<u8>>,
    }
    impl Drop for Opaque {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Sink {
        calls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
        payload_drops: Arc<AtomicUsize>,
        data: Arc<Vec<u8>>,
        call_panic: bool,
        drop_panic: bool,
    }
    impl TaskEventSink for Sink {
        fn record(&self, _: TaskEvent) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.call_panic {
                std::panic::panic_any(Opaque {
                    drops: self.payload_drops.clone(),
                    _data: self.data.clone(),
                });
            }
        }
    }
    impl Drop for Sink {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
            if self.drop_panic {
                std::panic::panic_any(Opaque {
                    drops: self.payload_drops.clone(),
                    _data: self.data.clone(),
                });
            }
        }
    }
    fn event() -> TaskEvent {
        TaskEvent::Progress(TaskProgress::new(
            novarocks_types::UniqueId::new(77, 88),
            1,
            2,
            3,
        ))
    }
    #[test]
    fn healthy_progress_keeps_reporting_and_releases_its_actual_holder() {
        let control = Arc::new(RuntimeErrorState::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let payload_drops = Arc::new(AtomicUsize::new(0));
        let data = Arc::new(vec![7u8; 4096]);
        let weak = Arc::downgrade(&data);
        let sink: Arc<dyn TaskEventSink> = Arc::new(Sink {
            calls: calls.clone(),
            drops: drops.clone(),
            payload_drops,
            data,
            call_panic: false,
            drop_panic: false,
        });
        let mut endpoint = ProgressEndpoint::new(control.clone());
        endpoint.record(sink.as_ref(), event());
        endpoint.record(sink.as_ref(), event());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        endpoint.retire(sink);
        drop(endpoint);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert!(weak.upgrade().is_none());
        assert!(control.error().is_none());
        assert!(control.local_exit_fault().is_none());
    }
    #[test]
    fn progress_fault_disables_only_metadata_and_keeps_original_opaque_payload() {
        for (call_panic, drop_panic) in [(true, false), (false, true), (true, true)] {
            let control = Arc::new(RuntimeErrorState::default());
            let calls = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let payload_drops = Arc::new(AtomicUsize::new(0));
            let data = Arc::new(vec![9u8; 4096]);
            let weak = Arc::downgrade(&data);
            let sink: Arc<dyn TaskEventSink> = Arc::new(Sink {
                calls: calls.clone(),
                drops: drops.clone(),
                payload_drops: payload_drops.clone(),
                data,
                call_panic,
                drop_panic,
            });
            let weak_control = Arc::downgrade(&control);
            let mut endpoint = ProgressEndpoint::new(control.clone());
            endpoint.record(sink.as_ref(), event());
            endpoint.record(sink.as_ref(), event());
            assert_eq!(calls.load(Ordering::SeqCst), if call_panic { 1 } else { 2 });
            endpoint.retire(sink);
            drop(endpoint);
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert_eq!(payload_drops.load(Ordering::SeqCst), 0);
            assert!(
                weak.upgrade().is_some(),
                "the original opaque owner was not discarded"
            );
            assert!(
                control.error().is_none(),
                "progress never accepts a Task failure"
            );
            assert!(control.local_exit_fault().is_some());
            drop(control);
            assert!(
                weak_control.upgrade().is_none(),
                "opaque metadata custody does not retain the Task control"
            );
        }
    }
}

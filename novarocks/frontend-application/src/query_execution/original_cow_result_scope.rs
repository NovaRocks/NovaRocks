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

//! COW-only projection of an existing Internal result admission.

use novarocks_query_application::admitted_query_context::QueryResultCapacityBinding;
use novarocks_spi::connector::{
    ConnectorOperationControl, ConnectorOperationControlView, ConnectorOriginalResultScope,
    ConnectorOriginalResultScopeCheck, ConnectorRequestContext, OriginalResultCheckClass as Class,
    OriginalResultCheckError as Failure,
};
use novarocks_workload_control::ResultWindowClass;
use std::{fmt, sync::Arc, time::Instant};

// No ActivityLease. The executing command/job owns activity until actual exit.
struct OriginalCowResultScope {
    control: ConnectorOperationControlView,
    binding: QueryResultCapacityBinding,
}
#[derive(Debug)]
struct InvalidOriginal(&'static str);
impl fmt::Display for InvalidOriginal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for InvalidOriginal {}
impl novarocks_failure_contract::FailureProjection for InvalidOriginal {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        novarocks_failure_contract::FailureProjection::failure_fact(
            &novarocks_spi::connector::ConnectorErrorKind::InvalidRequest,
        )
    }
}

impl ConnectorOriginalResultScopeCheck for OriginalCowResultScope {
    fn original_deadline(&self) -> Instant {
        self.control.deadline()
    }
    fn check_active(&self) -> Result<(), Failure> {
        self.control
            .check_active()
            .map_err(|e| Failure::new(Class::Control, e))?;
        self.binding
            .scope()
            .check()
            .map_err(|e| Failure::new(Class::Admission, e))?;
        Ok(())
    }
}

/// Retain the SAME original binding and operation control without acquiring
/// an activity lease or treating a metadata invoice as payload authorization.
pub(crate) fn from_original_binding(
    binding: &QueryResultCapacityBinding,
    context: &ConnectorRequestContext,
) -> Result<ConnectorOriginalResultScope, Failure> {
    context
        .check_active()
        .map_err(|e| Failure::new(Class::Control, e))?;
    binding
        .scope()
        .check()
        .map_err(|e| Failure::new(Class::Admission, e))?;
    let window = binding.window_alias();
    if !window.is_for_scope(binding.scope()) || binding.class() != ResultWindowClass::Internal {
        return Err(Failure::new(
            Class::Identity,
            InvalidOriginal("COW requires its original Internal result binding"),
        ));
    }
    let original = ConnectorOriginalResultScope::from_original(Arc::new(OriginalCowResultScope {
        control: context.original_operation_control(),
        binding: binding.clone(),
    }));
    original.check_active()?;
    Ok(original)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_spi::connector::{
        ConnectorCowBeginCause, ConnectorCowBeginFailure, ConnectorStopOwner,
    };
    use std::{
        error::Error,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(10),
            ConnectorStopOwner::new().view(),
            16 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .unwrap()
    }
    struct Canary {
        capacity: novarocks_workload_control::ResultCapacityHandle,
        held_during_cause_exit: Arc<AtomicBool>,
    }
    impl fmt::Debug for Canary {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("original source Debug must never run")
        }
    }
    impl fmt::Display for Canary {
        fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
            panic!("original source Display must never run")
        }
    }
    impl Error for Canary {}
    impl novarocks_failure_contract::FailureProjection for Box<Canary> {
        fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
            novarocks_failure_contract::FailureProjection::failure_fact(
                &novarocks_spi::connector::ConnectorErrorKind::Interrupted,
            )
        }
    }

    impl Drop for Canary {
        fn drop(&mut self) {
            self.held_during_cause_exit.store(
                self.capacity.snapshot().held_positions == [0, 0, 1, 0],
                Ordering::SeqCst,
            );
        }
    }
    #[test]
    fn original_scope_is_lifetime_custody_and_does_not_qualify_metadata_invoices() {
        let (_control, root, binding, capacity) =
            super::super::internal_result_cpu::admitted_internal_fixture();
        let scope = from_original_binding(&binding, &context()).unwrap();
        // This is the same original lifetime view, not a byte-budget author.
        scope.check_active().unwrap();
        let last = scope.retention_guard();
        drop(scope);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert_eq!(capacity.snapshot().held_positions, [0, 0, 1, 0]);
        drop(last);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }

    #[test]
    fn failure_and_last_neutral_alias_keep_actual_window_without_activity() {
        let (_control, root, binding, capacity) =
            super::super::internal_result_cpu::admitted_internal_fixture();
        let scope = from_original_binding(&binding, &context()).unwrap();
        let last = scope.retention_guard();
        let held = Arc::new(AtomicBool::new(false));
        let source = Box::new(Canary {
            capacity: capacity.clone(),
            held_during_cause_exit: held.clone(),
        });
        let raw_pointer = source.as_ref() as *const Canary;
        let failure = ConnectorCowBeginFailure::new(
            ConnectorCowBeginCause::OriginalResult(Failure::new(Class::Control, source)),
            scope,
        );
        // The closed envelope may be printed but never formats the raw source.
        let presentation = format!("{failure} {failure:?}");
        let actual_canary = failure
            .source()
            .and_then(|e| e.source())
            .and_then(|e| e.source())
            .and_then(|e| e.downcast_ref::<Box<Canary>>())
            .is_some_and(|e| std::ptr::eq(e.as_ref(), raw_pointer));
        // A neutral holder cannot self-wait. Rescue scope exits before assertions
        // even if the observer proves an accidental activity lease regression.
        let activity_binding = binding.clone();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let observer = std::thread::spawn(move || {
            activity_binding.wait_result_activities_exited();
            done_tx.send(()).unwrap();
        });
        let no_activity = done_rx.recv_timeout(Duration::from_secs(1)).is_ok();
        drop(binding);
        root.owner.complete();
        root.business.release();
        let before = capacity.snapshot().held_positions;
        drop(failure);
        let after_failure = capacity.snapshot().held_positions;
        drop(last);
        observer.join().unwrap();
        let final_count = capacity.snapshot().held_positions;
        assert!(no_activity);
        assert!(actual_canary);
        assert!(presentation.contains("copy-on-write begin failed"));
        assert!(held.load(Ordering::SeqCst));
        assert_eq!(before, [0, 0, 1, 0]);
        assert_eq!(after_failure, [0, 0, 1, 0]);
        assert_eq!(final_count, [0; 4]);
    }
    #[test]
    fn same_control_projection_preserves_original_deadline_and_stop_error() {
        let (_control, root, binding, capacity) =
            super::super::internal_result_cpu::admitted_internal_fixture();
        let stop = ConnectorStopOwner::new();
        let deadline = Instant::now() + Duration::from_secs(10);
        let context = ConnectorRequestContext::try_new(
            deadline,
            stop.view(),
            16 * 1024 * 1024,
            64 * 1024 * 1024,
        )
        .unwrap();
        let scope = from_original_binding(&binding, &context).unwrap();
        let copy = scope.clone();
        let same = scope.is_same_original(&copy);
        let actual_deadline = scope.original_deadline();
        stop.request_stop();
        let original = context.check_active().err();
        let retained = copy.check_active().err();
        let same_error = retained
            .as_ref()
            .and_then(|e| e.source())
            .and_then(|e| e.downcast_ref::<novarocks_spi::connector::ConnectorError>())
            .zip(original.as_ref())
            .is_some_and(|(a, b)| a == b);
        let class = retained.as_ref().map(|e| e.class());
        drop(copy);
        drop(scope);
        drop(binding);
        root.owner.complete();
        root.business.release();
        assert!(same);
        assert_eq!(actual_deadline, deadline);
        assert!(same_error);
        assert_eq!(class, Some(Class::Control));
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
    }
}

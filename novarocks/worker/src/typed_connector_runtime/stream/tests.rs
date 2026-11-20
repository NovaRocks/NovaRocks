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
use novarocks_execution::exec::chunk::ChunkSchema;
use novarocks_execution::exec::pipeline::operator::input_construction::{
    ConstructionFailure, InputConstructionFactory, try_runtime_input_construction_factory,
};
use novarocks_execution::runtime::task::{ExecutionFailure, ExecutionFailureCause};
use novarocks_execution::runtime::{mem_tracker::MemTracker, runtime_state::RuntimeErrorState};
use novarocks_type_contract::ValueTypeStorageLifetime;

#[test]
fn close_failure_is_reported_while_sibling_exit_is_still_pending() {
    use novarocks_execution::runtime::task::ExecutionFailureReporter;
    use novarocks_spi::connector::{
        ConnectorErrorKind,
        read_stack::{ConnectorOperationExit, ConnectorSourceExitProof},
    };
    use std::sync::Mutex;
    #[derive(Default)]
    struct Reports {
        required: Mutex<Vec<ExecutionFailure>>,
        cleanup: Mutex<Vec<ExecutionFailure>>,
        health: Mutex<Vec<ExecutionFailure>>,
    }
    impl ExecutionFailureReporter for Reports {
        fn report_failure(&self, cause: ExecutionFailure) {
            self.required.lock().unwrap().push(cause);
        }
        fn report_cleanup(&self, cause: &ExecutionFailure) {
            self.cleanup.lock().unwrap().push(cause.clone());
        }
        fn report_unresolved_exit(&self, cause: &ExecutionFailure) {
            self.health.lock().unwrap().push(cause.clone());
        }
    }
    for reader_first in [true, false] {
        let reports = Arc::new(Reports::default());
        let operations = ConnectorSourceOperations::for_test();
        let receiver = operations.claim_failure_receiver().unwrap();
        let mut work = Some(operations.admit(Arc::new(|| {})).unwrap());
        let sibling = operations.admit(Arc::new(|| {})).unwrap();
        let reader_error = ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "actual reader cleanup failed",
        );
        let work_error = ConnectorError::new(
            ConnectorErrorKind::Unavailable,
            "actual source operation failed",
        );
        let expected_reader: ExecutionFailure =
            ConnectorCallFailure::new(ConnectorOperation::CloseReader, reader_error.clone())
                .into();
        let expected_work: ExecutionFailure =
            ConnectorCallFailure::new(ConnectorOperation::SourceExit, work_error.clone())
                .into();
        let (send, wait) = futures::channel::oneshot::channel::<ConnectorError>();
        let reader: BoxFuture<'static, Result<(), ConnectorError>> = if reader_first {
            Box::pin(futures::future::ready(Err(reader_error.clone())))
        } else {
            Box::pin(async move { Err(wait.await.unwrap()) })
        };
        if !reader_first {
            work.take()
                .unwrap()
                .end(ConnectorOperationExit::WorkFailed(work_error.clone()));
        }
        operations.seal();
        let mut observe = Box::pin(observe_source_exit(
            receiver,
            operations.exited(),
            futures::future::ready(
                novarocks_spi::connector::read_stack::ConnectorPreparationExit::Exited,
            ),
            reports.clone(),
        ));
        let mut close = Box::pin(observe_scan_close(
            reader,
            operations.exited(),
            reports.clone(),
        ));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(observe.as_mut().poll(&mut cx).is_pending());
        assert!(close.as_mut().poll(&mut cx).is_pending());
        assert_eq!(
            reports.required.lock().unwrap().len(),
            usize::from(!reader_first)
        );
        assert_eq!(
            reports.cleanup.lock().unwrap().len(),
            usize::from(reader_first)
        );
        if reader_first {
            work.take()
                .unwrap()
                .end(ConnectorOperationExit::WorkFailed(work_error));
        } else {
            send.send(reader_error).unwrap();
        }
        assert!(observe.as_mut().poll(&mut cx).is_pending());
        assert!(close.as_mut().poll(&mut cx).is_pending());
        assert_eq!(*reports.required.lock().unwrap(), vec![expected_work]);
        assert_eq!(*reports.cleanup.lock().unwrap(), vec![expected_reader]);
        assert!(reports.health.lock().unwrap().is_empty());
        assert!(!operations.is_exited());
        drop(sibling);
        assert_eq!(
            futures::executor::block_on(operations.exited()),
            ConnectorSourceExitProof::Exited
        );
        assert_eq!(
            observe.as_mut().poll(&mut cx),
            Poll::Ready(novarocks_execution::exec::node::scan::ScanExitProof::Exited)
        );
        assert_eq!(close.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
        assert_eq!(reports.required.lock().unwrap().len(), 1);
        assert_eq!(reports.cleanup.lock().unwrap().len(), 1);
    }
}

#[test]
fn preparation_unknown_reports_cleanup_and_health_before_later_required_and_real_sibling_exit()
{
    use novarocks_execution::runtime::task::ExecutionFailureReporter;
    use novarocks_spi::connector::{
        ConnectorErrorKind,
        read_stack::{ConnectorOperationExit, ConnectorPreparationExit},
    };
    #[derive(Default)]
    struct Reports {
        required: std::sync::Mutex<Vec<ExecutionFailure>>,
        cleanup: std::sync::Mutex<Vec<ExecutionFailure>>,
        health: std::sync::Mutex<Vec<ExecutionFailure>>,
    }
    impl ExecutionFailureReporter for Reports {
        fn report_failure(&self, cause: ExecutionFailure) {
            self.required.lock().unwrap().push(cause);
        }
        fn report_cleanup(&self, cause: &ExecutionFailure) {
            self.cleanup.lock().unwrap().push(cause.clone());
        }
        fn report_unresolved_exit(&self, cause: &ExecutionFailure) {
            self.health.lock().unwrap().push(cause.clone());
        }
    }
    let reports = Arc::new(Reports::default());
    let operations = ConnectorSourceOperations::for_test();
    let events = operations.claim_failure_receiver().unwrap();
    let unknown = operations.admit(Arc::new(|| {})).unwrap();
    let work = operations.admit(Arc::new(|| {})).unwrap();
    let healthy = operations.admit(Arc::new(|| {})).unwrap();
    let cleanup = ConnectorError::new(
        ConnectorErrorKind::Internal,
        "original preparation owner could not prove physical exit",
    );
    let original = ConnectorError::new(
        ConnectorErrorKind::CorruptData,
        "later accepted source operation failed",
    );
    let expected_cleanup: ExecutionFailure =
        ConnectorCallFailure::new(ConnectorOperation::SourceExit, cleanup.clone()).into();
    let expected_work: ExecutionFailure =
        ConnectorCallFailure::new(ConnectorOperation::SourceExit, original.clone()).into();
    let (release_flow, flow) = futures::channel::oneshot::channel();
    let mut observer = Box::pin(observe_source_exit(
        events,
        operations.exited(),
        async move { flow.await.unwrap() },
        reports.clone(),
    ));
    unknown.end(ConnectorOperationExit::CleanupUnresolved(cleanup.clone()));
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    assert!(observer.as_mut().poll(&mut cx).is_pending());
    assert!(reports.required.lock().unwrap().is_empty());
    assert_eq!(
        *reports.cleanup.lock().unwrap(),
        vec![expected_cleanup.clone()]
    );
    assert_eq!(*reports.health.lock().unwrap(), vec![expected_cleanup]);
    work.report_work_failure(original);
    assert!(observer.as_mut().poll(&mut cx).is_pending());
    assert_eq!(*reports.required.lock().unwrap(), vec![expected_work]);
    work.end(ConnectorOperationExit::Exited);
    operations.seal();
    release_flow
        .send(ConnectorPreparationExit::Unresolved(cleanup))
        .unwrap();
    assert!(
        observer.as_mut().poll(&mut cx).is_pending(),
        "early unknown never drops the remaining work receiver"
    );
    assert_eq!(operations.live_operations(), 1);
    healthy.end(ConnectorOperationExit::Exited);
    assert_eq!(
        observer.as_mut().poll(&mut cx),
        Poll::Ready(novarocks_execution::exec::node::scan::ScanExitProof::Unresolved)
    );
    assert_eq!(reports.required.lock().unwrap().len(), 1);
    assert_eq!(reports.cleanup.lock().unwrap().len(), 1);
    assert_eq!(reports.health.lock().unwrap().len(), 1);
}

#[test]
fn runtime_stream_requires_the_actual_task_source_before_construction() {
    use novarocks_execution_contract::failure::{FailureClass, FailureProjection};
    use novarocks_spi::connector::{ConnectorRequestContext, ConnectorStopOwner};
    let request = ConnectorRequestContext::try_new(
        std::time::Instant::now() + std::time::Duration::from_secs(60),
        ConnectorStopOwner::new().view(),
        1 << 20,
        1 << 22,
    )
    .unwrap();
    let failure = match required_source_operations(&request) {
        Err(failure) => failure,
        Ok(_) => panic!("planning-only context must not authorize a runtime stream"),
    };
    let expected = super::super::TypedScanBindingFailure::MissingExecutionSource;
    assert_eq!(
        failure.failure_fact(),
        novarocks_execution_contract::failure::FailureFact::new(
            FailureClass::Invariant,
            novarocks_execution_contract::failure::FailureReason::new(
                novarocks_execution_contract::failure::domain::WORKER,
                std::num::NonZeroU16::new(14).unwrap(),
            ),
        )
    );
    assert_eq!(failure.failure_fact(), expected.failure_fact());
    assert_eq!(failure.to_string(), expected.to_string());
    assert!(
        matches!(failure.cause(), ExecutionFailureCause::Domain(source)
        if source.downcast_ref::<super::super::TypedScanBindingFailure>().is_some())
    );
    let operations = ConnectorSourceOperations::for_test();
    let request = request.with_execution_source(
        novarocks_spi::connector::ConnectorRangeScope::for_test(1, 2, 1, 3, 4, 7).unwrap(),
        operations.clone(),
    );
    let same = required_source_operations(&request).unwrap();
    let ticket = same.admit(Arc::new(|| {})).unwrap();
    assert_eq!(operations.live_operations(), 1);
    operations.seal();
    assert!(!same.is_exited());
    ticket.end(novarocks_spi::connector::read_stack::ConnectorOperationExit::Exited);
    assert!(same.is_exited());
}

struct StreamStorage {
    _storage: ValueTypeStorageLifetime,
    initialized: [u8; 256],
}
fn declared_schema() -> ChunkSchema {
    ChunkSchema::try_new_typed(vec![], ValueTypeStorageLifetime::pure()).unwrap()
}
fn factory(
    tracker: &Arc<MemTracker>,
    error: Arc<RuntimeErrorState>,
) -> Arc<dyn InputConstructionFactory> {
    try_runtime_input_construction_factory(
        novarocks_execution::exec::pipeline::pipeline::representation_construction_profile()
            .unwrap(),
        tracker.clone(),
        error,
    )
    .unwrap()
}

#[test]
fn claim_box_keeps_its_own_finite_storage_after_factory_and_control_exit() {
    let tracker = MemTracker::new_root("actual claim Box");
    let error = Arc::new(RuntimeErrorState::default());
    let controller = Arc::downgrade(&error);
    let factory = factory(&tracker, error);
    let boxed = create_scan_stream(&declared_schema(), factory.as_ref(), |storage| {
        StreamStorage {
            _storage: storage,
            initialized: [7; 256],
        }
    })
    .unwrap();
    assert_eq!(boxed.initialized, [7; 256]);
    drop(factory);
    assert!(controller.upgrade().is_none());
    assert!(tracker.current() > 0);
    drop(boxed);
    assert_eq!(tracker.current(), 0);
}

#[test]
fn claim_box_memory_and_original_stop_refuse_before_the_constructor_runs() {
    let tracker = MemTracker::new_root("claim constructor admission");
    let error = Arc::new(RuntimeErrorState::default());
    let factory = factory(&tracker, error.clone());
    let initial = tracker.current();
    let schema = declared_schema();
    let sample = create_scan_stream(&schema, factory.as_ref(), |storage| StreamStorage {
        _storage: storage,
        initialized: [0; 256],
    })
    .unwrap();
    let requests = tracker.current() - initial;
    drop(sample);
    assert_eq!(tracker.current(), initial);
    tracker.install_limit_once(initial + requests - 1).unwrap();
    let entered = std::cell::Cell::new(false);
    let refusal = create_scan_stream(&schema, factory.as_ref(), |storage| {
        entered.set(true);
        StreamStorage {
            _storage: storage,
            initialized: [1; 256],
        }
    })
    .err()
    .expect("real Box request refuses");
    assert!(matches!(
        refusal.cause(),
        ExecutionFailureCause::Construction(ConstructionFailure::Memory(_))
    ));
    assert!(
        !entered.get(),
        "no constructor tail after the original tracker refusal"
    );
    assert_eq!(tracker.current(), initial);
    let original = ExecutionFailure::from(novarocks_functions::KernelFailure::Cancelled);
    error.set_error(original.clone());
    let refusal = create_scan_stream(&schema, factory.as_ref(), |storage| {
        entered.set(true);
        StreamStorage {
            _storage: storage,
            initialized: [2; 256],
        }
    })
    .err()
    .unwrap();
    assert_eq!(refusal, original);
    assert!(!entered.get());
    assert_eq!(tracker.current(), initial);
    drop(factory);
    assert_eq!(tracker.current(), 0);
}

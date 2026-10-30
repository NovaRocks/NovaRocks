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
use novarocks_spi::connector::read_stack::adapter::{ProviderReadRuntime, ReadRuntimeAdapter};
use novarocks_spi::connector::read_stack::{ColumnHandle, ConnectorSplit};
use novarocks_spi::connector::{
    CatalogHandle, CatalogVersion, ConnectorInstanceDescriptor, ConnectorInstanceId,
    ConnectorProviderId,
};
use std::thread::ThreadId;

use novarocks_query_application::cpu::{
    QueryBlockingExecutorConfig, QueryBlockingExecutorOwner, QueryCpuExecutorConfig,
    QueryCpuExecutorOwner,
};
use std::{
    num::NonZeroUsize,
    sync::{atomic::AtomicUsize, mpsc},
    time::{Duration, Instant},
};

fn original_owners() -> (
    QueryCpuExecutorOwner,
    QueryBlockingExecutorOwner,
    ConnectorBlockingIoSupervisor,
) {
    let cpu = QueryCpuExecutorOwner::try_new(QueryCpuExecutorConfig::with_idle_keepalive(
        Duration::from_secs(1),
    ))
    .unwrap();
    let blocking = QueryBlockingExecutorOwner::try_new(QueryBlockingExecutorConfig::new(
        NonZeroUsize::new(1).unwrap(),
        NonZeroUsize::new(1).unwrap(),
    ))
    .unwrap();
    let supervisor = ConnectorBlockingIoSupervisor::new(Handle::current());
    supervisor
        .install_original_executors(cpu.executor(), blocking.executor())
        .unwrap();
    (cpu, blocking, supervisor)
}
struct Probe(Arc<AtomicUsize>);
impl Drop for Probe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
struct ApplyInput {
    entered: mpsc::SyncSender<()>,
    release: mpsc::Receiver<()>,
    _probe: Probe,
}
fn gated_apply(input: &mut ApplyInput, _: &ConnectorSourceCloseResult<'_>) -> Option<usize> {
    input.entered.send(()).unwrap();
    input.release.recv_timeout(Duration::from_secs(5)).unwrap();
    Some(17)
}
#[tokio::test]
async fn dropped_borrowed_wait_keeps_the_same_real_input_and_receipt() {
    let (mut cpu, mut blocking, supervisor) = original_owners();
    let (control, root, window) = super::super::tests::admitted();
    let responsibility =
        ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
            .unwrap();
    let pin = responsibility.join_pin();
    drop(responsibility);
    let drops = Arc::new(AtomicUsize::new(0));
    let (entered, seen) = mpsc::sync_channel(1);
    let (release, released) = mpsc::sync_channel(1);
    let mut job = supervisor.prepare_cpu_owned(
        vec![pin],
        ApplyInput {
            entered,
            release: released,
            _probe: Probe(Arc::clone(&drops)),
        },
        gated_apply,
    );
    let pointer = Arc::as_ptr(job.header());
    let mut output = None;
    let mut wait = Box::pin(job.finish_in(&mut output));
    use std::future::Future;
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    seen.recv_timeout(Duration::from_secs(5)).unwrap();
    drop(wait);
    assert_eq!(Arc::as_ptr(job.header()), pointer);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    assert!(output.is_none());
    root.owner
        .cancel(novarocks_workload_control::CancellationReason::Requested);
    root.owner.complete();
    root.business.release();
    drop(window);
    super::super::tests::settle_control(&control);
    let held = control.snapshot();
    assert_eq!(held.root_responsibilities, 1);
    assert_eq!(held.result_windows.held_positions, [0, 0, 1, 0]);
    assert!(held.scopes.iter().any(|child| child.parent.is_some()
        && child.owner == novarocks_workload_control::OwnerState::Active));
    release.send(()).unwrap();
    std::future::poll_fn(|cx| job.poll_progress(cx))
        .await
        .unwrap();
    assert!(job.actual_exit_observed());
    assert!(output.is_none());
    assert_eq!(
        control.snapshot().result_windows.held_positions,
        [0, 0, 1, 0]
    );
    job.finish_in(&mut output).await;
    assert_eq!(output.take().unwrap().unwrap(), 17);
    assert!(job.actual_exited);
    assert_eq!(drops.load(Ordering::Acquire), 1);
    drop(job);
    super::super::tests::until(|| {
        super::super::tests::settle_control(&control);
        control.snapshot().scopes.is_empty()
    });
    control.close_admission();
    assert!(control.shutdown().is_ok());
    cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    blocking
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
struct PanicInput {
    _drops: Probe,
    cause: Option<Probe>,
}
fn panicking_apply(input: &mut PanicInput, _: &ConnectorSourceCloseResult<'_>) -> Option<()> {
    std::panic::panic_any(input.cause.take().unwrap())
}
#[tokio::test]
async fn apply_unwind_restores_the_same_input_box_and_original_opaque_cause() {
    let (mut cpu, mut blocking, supervisor) = original_owners();
    let (control, root, window) = super::super::tests::admitted();
    let responsibility =
        ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
            .unwrap();
    let pin = responsibility.join_pin();
    drop(responsibility);
    let input_drops = Arc::new(AtomicUsize::new(0));
    let cause_drops = Arc::new(AtomicUsize::new(0));
    let mut job = supervisor.prepare_cpu_owned(
        vec![pin],
        PanicInput {
            _drops: Probe(Arc::clone(&input_drops)),
            cause: Some(Probe(Arc::clone(&cause_drops))),
        },
        panicking_apply,
    );
    let call_pointer = {
        let guard = job.header().call.lock().unwrap();
        &**guard.as_ref().unwrap() as *const dyn OriginalCall<()> as *const ()
    };
    let mut output = None;
    job.finish_in(&mut output).await;
    assert!(output.as_ref().unwrap().is_err());
    assert!(job.actual_exited);
    let error = output.as_ref().unwrap().as_ref().unwrap_err();
    let health = error.health_cause();
    assert_eq!(
        Arc::as_ptr(&health) as *const (),
        Arc::as_ptr(&error.original) as *const ()
    );
    root.owner.complete();
    root.business.release();
    drop(window);
    assert_eq!(
        control.snapshot().result_windows.held_positions,
        [0, 0, 1, 0]
    );
    assert_eq!(input_drops.load(Ordering::Acquire), 0);
    assert_eq!(cause_drops.load(Ordering::Acquire), 0);
    let header = job.header.take().unwrap();
    let restored = {
        let guard = header.call.lock().unwrap();
        &**guard.as_ref().unwrap() as *const dyn OriginalCall<()> as *const ()
    };
    assert_eq!(call_pointer, restored);
    assert!(header.poisoned.load(Ordering::Acquire));
    // Explicit fixture disposition after observing the actual CPU exit. There
    // is no Work/Source completion proof inferred from this test-only Drop.
    drop(header.call.lock().unwrap().take());
    drop(header);
    drop(job);
    drop(output);
    assert_eq!(input_drops.load(Ordering::Acquire), 1);
    assert_eq!(cause_drops.load(Ordering::Acquire), 0);
    assert_eq!(
        control.snapshot().result_windows.held_positions,
        [0, 0, 1, 0]
    );
    drop(health);
    assert_eq!(cause_drops.load(Ordering::Acquire), 1);
    super::super::tests::until(|| control.snapshot().scopes.is_empty());
    control.close_admission();
    assert!(control.shutdown().is_ok());
    cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    blocking
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}

#[derive(Debug)]
struct CloseCause(Arc<AtomicUsize>);
impl std::fmt::Display for CloseCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original typed close cause")
    }
}
impl std::error::Error for CloseCause {}
impl novarocks_execution_contract::failure::FailureProjection for CloseCause {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        novarocks_spi::connector::ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::Unavailable,
            "fixture close fact",
        )
        .failure_fact()
    }
}
impl Drop for CloseCause {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}
struct TypedCloseSource {
    cause: Option<CloseCause>,
    dropped: Arc<AtomicUsize>,
    calls: Arc<AtomicUsize>,
}
impl novarocks_spi::connector::read_stack::ConnectorReadSplitSource for TypedCloseSource {
    fn next_batch(
        &mut self,
        _: usize,
        _: &novarocks_spi::connector::read_stack::ConnectorReadDynamicFilterSnapshot,
    ) -> Result<
        novarocks_spi::connector::read_stack::ConnectorSplitBatch<
            novarocks_spi::connector::read_stack::ConnectorReadSplit,
        >,
        novarocks_spi::connector::ConnectorError,
    > {
        Ok(novarocks_spi::connector::read_stack::ConnectorSplitBatch::finished())
    }
    fn is_finished(&self) -> bool {
        false
    }
    fn close(&mut self) -> Result<(), novarocks_spi::connector::ConnectorError> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        Err(novarocks_spi::connector::ConnectorError::from_domain(
            novarocks_spi::connector::ConnectorErrorKind::Unavailable,
            self.cause.take().expect("one original close cause"),
        ))
    }
}
impl Drop for TypedCloseSource {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::AcqRel);
    }
}
fn close_original_source(
    input: &mut crate::query_execution::split_assignment::SupervisedSplitSource,
    result: &ConnectorSourceCloseResult<'_>,
) -> Option<()> {
    if let Err(error) = input.retire_in(result) {
        result.record_error(error);
    }
    Some(())
}
#[tokio::test]
async fn real_source_box_exits_on_original_worker_and_typed_close_header_survives() {
    let (mut cpu, mut blocking, supervisor) = original_owners();
    let cause_drops = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let source = crate::query_execution::split_assignment::SupervisedSplitSource::fixture(
        Box::new(TypedCloseSource {
            cause: Some(CloseCause(Arc::clone(&cause_drops))),
            dropped: Arc::clone(&dropped),
            calls: Arc::clone(&calls),
        }),
    );
    let mut job = supervisor.prepare_cpu_retirement(Vec::new(), source, close_original_source);
    let mut output = None;
    job.finish_in(&mut output).await;
    assert!(output.take().unwrap().is_ok());
    assert!(job.actual_exited);
    assert_eq!(dropped.load(Ordering::Acquire), 1);
    assert_eq!(calls.load(Ordering::Acquire), 1);
    let primary = job
        .source_close_failure()
        .expect("same stable typed close error");
    assert_eq!(
        primary.kind(),
        novarocks_spi::connector::ConnectorErrorKind::Unavailable
    );
    assert_eq!(cause_drops.load(Ordering::Acquire), 0);
    drop(job);
    assert_eq!(cause_drops.load(Ordering::Acquire), 0);
    drop(primary);
    assert_eq!(cause_drops.load(Ordering::Acquire), 1);
    cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    blocking
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
struct CanaryColumn;
impl ColumnHandle for CanaryColumn {}

// A concrete provider Split; the only role-visible value will be ConnectorReadSplit.
struct CanarySplit {
    entered: mpsc::Sender<ThreadId>,
    watchdog_entered: mpsc::Sender<()>,
    release: Mutex<mpsc::Receiver<()>>,
    exited: mpsc::Sender<bool>,
    drop_count: Arc<AtomicUsize>,
}
impl fmt::Debug for CanarySplit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpaqueProviderSplitDropCanary")
    }
}
impl ConnectorSplit for CanarySplit {
    fn retained_size_in_bytes(&self) -> u64 {
        // A provider fact about this fixture, not an allocator/whole-graph bound.
        std::mem::size_of::<Self>() as u64
    }
}
impl Drop for CanarySplit {
    fn drop(&mut self) {
        self.drop_count.fetch_add(1, Ordering::SeqCst);
        let _ = self.entered.send(std::thread::current().id());
        let _ = self.watchdog_entered.send(());
        let released = self
            .release
            .get_mut()
            .map(|receiver| receiver.recv_timeout(Duration::from_secs(2)).is_ok())
            .unwrap_or(false);
        let _ = self.exited.send(released);
    }
}

struct CanaryProvider {
    descriptor: ConnectorInstanceDescriptor,
    catalog: CatalogHandle,
}
impl ProviderReadRuntime for CanaryProvider {
    type Table = ();
    type Column = CanaryColumn;
    type Transaction = ();
    type Split = CanarySplit;

    fn descriptor(&self) -> &ConnectorInstanceDescriptor {
        &self.descriptor
    }
    fn catalog_handle(&self) -> &CatalogHandle {
        &self.catalog
    }
    fn transaction(&self) -> Self::Transaction {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unclaimed_original_opaque_split_drop_is_retired_off_the_waiter_thread() {
    let (mut cpu, mut blocking, supervisor) = original_owners();
    let (control, root, window) = super::super::tests::admitted();
    let responsibility =
        ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias())
            .unwrap();
    let pin = responsibility.join_pin();
    let (entered_tx, entered_rx) = mpsc::channel();
    let (watchdog_tx, watchdog_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let (exited_tx, exited_rx) = mpsc::channel();
    let drop_count = Arc::new(AtomicUsize::new(0));
    let watchdog = std::thread::spawn(move || {
        let observed_enter = watchdog_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        if observed_enter {
            std::thread::sleep(Duration::from_millis(250));
        }
        (observed_enter, release_tx.send(()).is_ok())
    });
    let instance = ConnectorInstanceId::parse("drop.canary").unwrap();
    let adapter = ReadRuntimeAdapter::new(Arc::new(CanaryProvider {
        descriptor: ConnectorInstanceDescriptor {
            provider_id: ConnectorProviderId::parse("drop-canary").unwrap(),
            instance_id: instance.clone(),
        },
        catalog: CatalogHandle::new(instance, CatalogVersion::from_bytes([7; 32])),
    }));
    let split = adapter.wrap_split(CanarySplit {
        entered: entered_tx,
        watchdog_entered: watchdog_tx,
        release: Mutex::new(release_rx),
        exited: exited_tx,
        drop_count: Arc::clone(&drop_count),
    });
    drop(adapter);
    let backing = responsibility.retain_backing();
    drop(responsibility);
    let mut job =
        supervisor.prepare_cpu_retirement(vec![pin], (Some(split), backing), |input, _| {
            drop(input.0.take());
            Some(())
        });
    root.owner.complete();
    root.business.release();
    drop(window);
    let before_wait = control.snapshot();
    let waiter_thread = std::thread::current().id();
    let mut outcome = None;
    let mut wait = Box::pin(job.finish_in(&mut outcome));
    use std::future::Future;
    assert!(
        wait.as_mut()
            .poll(&mut Context::from_waker(std::task::Waker::noop()))
            .is_pending()
    );
    drop(wait);
    let destructor_thread = entered_rx.recv_timeout(Duration::from_secs(2));
    let while_drop_held = control.snapshot();
    let watchdog_result = watchdog.join();
    let destructor_exited = exited_rx.recv_timeout(Duration::from_secs(2));
    job.finish_in(&mut outcome).await;
    assert!(job.actual_exit_observed());
    assert!(matches!(outcome, Some(Ok(()))));
    drop(job);
    super::super::tests::until(|| control.snapshot().scopes.is_empty());
    let after = control.snapshot();
    control.close_admission();
    let workload_exit = control.shutdown();
    cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    blocking
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(before_wait.root_responsibilities, 1);
    assert_eq!(before_wait.result_windows.held_positions, [0, 0, 1, 0]);
    assert_eq!(while_drop_held.root_responsibilities, 1);
    assert_eq!(while_drop_held.result_windows.held_positions, [0, 0, 1, 0]);
    assert!(matches!(watchdog_result, Ok((true, true))));
    assert!(matches!(destructor_exited, Ok(true)));
    assert!(after.scopes.is_empty());
    assert!(workload_exit.is_ok());
    assert_eq!(drop_count.load(Ordering::SeqCst), 1);
    assert_ne!(destructor_thread.unwrap(), waiter_thread);
}

#[test]
fn admitted_provider_work_refuses_foreign_window_and_scope_record_saturation() {
    let (first, first_root, first_window) = super::super::tests::admitted();
    let (second, second_root, second_window) = super::super::tests::admitted();
    assert!(matches!(
        ConnectorBlockingIoResponsibility::admit(
            &first_root.owner.scope(),
            &second_window.retain_alias(),
        ),
        Err(WorkError::Conflict)
    ));
    for (control, root, window) in [
        (first, first_root, first_window),
        (second, second_root, second_window),
    ] {
        root.owner.complete();
        root.business.release();
        drop(window);
        control.close_admission();
        assert!(control.shutdown().is_ok());
    }
    let control = novarocks_workload_control::WorkloadControl::try_new_counted(
        novarocks_workload_control::WorkloadConfig {
            scope_records_limit: 1,
            root_limit: 1,
            business_limit: 1,
            ..Default::default()
        },
    )
    .unwrap()
    .owner;
    control
        .configure_result_capacity(novarocks_workload_control::ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let (root, window) = control
        .root_admission()
        .try_begin_root_with_result(
            novarocks_workload_control::WorkRequest::new(
                novarocks_workload_control::WorkClass::Management,
            ),
            novarocks_workload_control::ResultWindowClass::Local,
        )
        .unwrap();
    assert!(matches!(
        ConnectorBlockingIoResponsibility::admit(&root.owner.scope(), &window.retain_alias(),),
        Err(WorkError::Capacity(_))
    ));
    root.owner.complete();
    root.business.release();
    drop(window);
    control.close_admission();
    assert!(control.shutdown().is_ok());
}

#[tokio::test]
async fn distinct_original_jobs_do_not_overwrite_each_others_raw_failure() {
    fn fail(input: &mut Option<Probe>, _: &ConnectorSourceCloseResult<'_>) -> Option<()> {
        std::panic::panic_any(input.take().unwrap());
    }
    let (mut cpu, mut blocking, supervisor) = original_owners();
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut first =
        supervisor.prepare_cpu_owned(Vec::new(), Some(Probe(Arc::clone(&dropped))), fail);
    let mut second =
        supervisor.prepare_cpu_owned(Vec::new(), Some(Probe(Arc::clone(&dropped))), fail);
    let mut first_output = None;
    let mut second_output = None;
    first.finish_in(&mut first_output).await;
    second.finish_in(&mut second_output).await;
    let first_error = first_output.as_ref().unwrap().as_ref().unwrap_err();
    let second_error = second_output.as_ref().unwrap().as_ref().unwrap_err();
    assert!(!Arc::ptr_eq(&first_error.original, &second_error.original));
    for error in [first_error, second_error] {
        assert!(
            matches!(&error.original.cause, OriginalFailureCause::Preserving(cause)
            if cause.original_payload_is::<Probe>())
        );
    }
    assert_eq!(dropped.load(Ordering::Acquire), 0);
    for job in [&mut first, &mut second] {
        assert!(job.actual_exit_observed());
        let header = job.header.take().unwrap();
        drop(header.call.lock().unwrap().take());
        drop(header);
    }
    drop(first);
    drop(second);
    drop(first_output);
    assert_eq!(dropped.load(Ordering::Acquire), 1);
    drop(second_output);
    assert_eq!(dropped.load(Ordering::Acquire), 2);
    cpu.shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    blocking
        .shutdown_until(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}

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

//! Current physical last-backing and generation-qualified stop boundaries.
//! The retired per-RPC owner facade/index/reaper is not revived.
use super::*;
use crate::task_execution::blocking_io::{
    ConnectorBlockingIoBacking, ConnectorBlockingIoSupervisor, ConnectorOperationJob,
    tests::OriginalExecutorsFixture,
};
use novarocks_workload_control::joint_admission::{JointProgressDriver, ResponsibilityConfig};
use novarocks_workload_control::{
    ControlStorageConfig, PollRoleSourceBacking, ResultCapacityConfig, ResultCapacityHandle,
    ResultWindowAlias, ResultWindowGrant, RootWork, WorkloadConfig, WorkloadControl,
};

struct AliasCase {
    control: Option<WorkloadControl>,
    root: Option<RootWork>,
    driver: Option<JointProgressDriver>,
    capacity: Option<ResultCapacityHandle>,
    window: Option<ResultWindowGrant>,
    alias: Option<ResultWindowAlias>,
    old_cohort: Option<NativeSourceCohort>,
    next_cohort: Option<NativeSourceCohort>,
    admission: Option<ConnectorBlockingIoAdmission>,
    source: Option<ConnectorBlockingIoResponsibility>,
    backing: Option<ConnectorBlockingIoBacking>,
    job: Option<ConnectorOperationJob<()>>,
    physical: Option<OriginalExecutorsFixture>,
    supervisor: Option<ConnectorBlockingIoSupervisor>,
    next_owner: Option<novarocks_workload_control::WorkOwner>,
}
struct RetainedAlias(Option<Box<AliasCase>>);
impl Drop for RetainedAlias {
    fn drop(&mut self) {
        if let Some(original) = self.0.take() {
            static UNKNOWN: std::sync::OnceLock<Mutex<Vec<Box<dyn std::any::Any + Send>>>> =
                std::sync::OnceLock::new();
            UNKNOWN
                .get_or_init(Default::default)
                .lock()
                .unwrap()
                .push(original);
        }
    }
}
struct ActualCaptureDrop(Arc<AtomicUsize>);
impl Drop for ActualCaptureDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn finish_original_capture(
    _: &mut ActualCaptureDrop,
    _: &crate::task_execution::blocking_io::ConnectorSourceCloseResult<'_>,
) -> Option<()> {
    Some(())
}

async fn actual_last_backing_scope_cut(deadline: Instant) {
    // The fixed control inventory and live WorkScope limit are both two:
    // Root + original source. A new positive cohort is not a live child.
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: 1,
        business_limit: 1,
        query_concurrency_limit: 1,
        scope_records_limit: 2,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    let (_, mut driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: std::num::NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control.configure_control_storage(ControlStorageConfig::new(2, &[]).with_process_source_backing(
        PollRoleSourceBacking::from_layouts(&crate::query_execution::native_execution_adapter::source_cohort::process_source_backing_layouts().unwrap(), &[]).unwrap())).unwrap();
    let capacity = control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let parent = root.owner.scope();
    wc::bind(&parent, &mut driver);
    let window = capacity
        .try_acquire(&parent, ResultWindowClass::Client)
        .unwrap();
    let alias = window.retain_alias();
    let cohort = wc::cohort(&parent, 1, &mut driver);
    let mut admission =
        ConnectorBlockingIoResponsibility::request_from_cohort(&parent, &alias, cohort.clone())
            .unwrap();
    let source = admission.ready().await.unwrap();
    let old_scope = source.scope().unwrap();
    let backing = source.retain_backing();
    let supervisor = ConnectorBlockingIoSupervisor::new(Handle::current());
    let physical = OriginalExecutorsFixture::install(&supervisor);
    let destroyed = Arc::new(AtomicUsize::new(0));
    let job = supervisor.prepare_cpu_retirement(
        vec![source.join_pin()],
        ActualCaptureDrop(destroyed.clone()),
        finish_original_capture,
    );
    let mut retained = RetainedAlias(Some(Box::new(AliasCase {
        control: Some(control),
        root: Some(root),
        driver: Some(driver),
        capacity: Some(capacity),
        window: Some(window),
        alias: Some(alias),
        old_cohort: Some(cohort),
        next_cohort: None,
        admission: Some(admission),
        source: Some(source),
        backing: Some(backing),
        job: Some(job),
        physical: Some(physical),
        supervisor: Some(supervisor),
        next_owner: None,
    })));
    let c = retained.0.as_mut().unwrap();
    let mut outcome = None;
    tokio::time::timeout_at(
        deadline.into(),
        c.job.as_mut().unwrap().finish_in(&mut outcome),
    )
    .await
    .unwrap();
    assert!(matches!(outcome, Some(Ok(()))));
    assert!(c.job.as_ref().unwrap().actual_exit_observed());
    assert!(!c.job.as_ref().unwrap().source_drop_unproven());
    assert!(!c.job.as_ref().unwrap().input_or_value_retained());
    assert_eq!(
        destroyed.load(Ordering::SeqCst),
        1,
        "actual original close body/capture exited"
    );
    drop(c.job.take());
    drop(c.source.take());
    assert!(
        old_scope.check().is_ok(),
        "completed work is not last backing destruction"
    );
    // The original global inventory has two positions. A positive cohort
    // already reserves one of them, so the real capacity refusal occurs at
    // cohort admission while the old backing still owns that position.
    assert!(matches!(
        parent.try_request_source_cohort(std::num::NonZeroUsize::MIN),
        Err(WorkError::Capacity("fixed control slots"))
    ));
    assert!(c.next_cohort.is_none() && c.next_owner.is_none());
    drop(c.backing.take());
    wc::drive(c.driver.as_mut().unwrap());
    tokio::time::timeout_at(deadline.into(), old_scope.wait_released())
        .await
        .unwrap();
    assert!(matches!(old_scope.check(), Err(WorkError::Released)));
    // Exactly one new admission AFTER the original authority reports release,
    // matching the original reserve-after-release boundary. The failed
    // admission above did not grant any positive cohort or child position.
    c.next_cohort = Some(wc::cohort(&parent, 1, c.driver.as_mut().unwrap()));
    assert_eq!(c.next_cohort.as_ref().unwrap().remaining(), 1);
    c.next_cohort
        .as_ref()
        .unwrap()
        .take_child_in(&parent, &mut c.next_owner)
        .unwrap()
        .notify();
    let next_scope = c.next_owner.as_ref().unwrap().scope();
    assert!(next_scope.check().is_ok());
    drop(c.old_cohort.take());
    assert!(
        next_scope.check().is_ok(),
        "old exhausted cohort Drop cannot cancel a current original child"
    );
    c.next_owner
        .take()
        .unwrap()
        .complete_after_terminal_cancel_settled();
    wc::drive(c.driver.as_mut().unwrap());
    tokio::time::timeout_at(deadline.into(), next_scope.wait_released())
        .await
        .unwrap();
    c.physical.as_mut().unwrap().shutdown().await;
    drop((
        c.physical.take(),
        c.supervisor.take(),
        c.admission.take(),
        c.next_cohort.take(),
        c.alias.take(),
        c.window.take(),
        c.capacity.take(),
        next_scope,
        old_scope,
        parent,
    ));
    wc::drive(c.driver.as_mut().unwrap());
    wc::finish(
        c.control.take().unwrap(),
        c.root.take().unwrap(),
        c.driver.take().unwrap(),
    );
    drop(retained.0.take());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_lease_drop_cannot_abort_recycled_original_position() {
    let deadline = Instant::now() + Duration::from_secs(2);
    actual_last_backing_scope_cut(deadline).await;
    let mut retained = Case::prepare_retained(1);
    let c = retained.0.as_mut().unwrap();
    let parent = c.root.as_ref().unwrap().owner.scope();
    c.source = Some(
        SubscriptionSource::from_cohort(
            c.f.as_ref().unwrap().key.participation,
            &parent,
            c.cohort.as_ref().unwrap(),
            c.f.as_ref().unwrap().input.clone(),
        )
        .unwrap(),
    );
    let child = c.source.as_ref().unwrap().scope().unwrap();
    let first = Arc::new(Probe::default());
    c.start(0, 2, first.clone(), true);
    let stale = c.join(0, deadline, SubscriptionEnd::RemoteClosed).await;
    assert_eq!(first.polls.load(Ordering::SeqCst), 1);
    assert_eq!(first.drops.load(Ordering::SeqCst), 1);
    assert!(
        child.check().is_ok(),
        "free generation metadata does not retire stable Source child"
    );
    let current = Arc::new(Probe::default());
    c.start(1, 3, current.clone(), false);
    polled(&current, deadline).await;
    assert_eq!(c.source.as_ref().unwrap().scope().unwrap().id(), child.id());
    assert_eq!(
        c.control
            .as_ref()
            .unwrap()
            .snapshot()
            .scopes
            .iter()
            .filter(|row| row.parent == Some(parent.id()))
            .count(),
        1,
        "the new generation reuses the SAME stable Source, not a second child"
    );
    let original = c.jobs[1].as_ref().unwrap().request.clone();
    let phase = c.f.as_ref().unwrap().phase(2);
    assert!(matches!(
        c.source.as_ref().unwrap().prepare(stale.clone(), &phase),
        Err(crate::task_execution::host::subscriptions::SubscriptionSourceError::ReusedGeneration)
    ));
    phase.close_after_exit().unwrap();
    c.source
        .as_ref()
        .unwrap()
        .close_generation(NonZeroU64::new(2).unwrap())
        .unwrap();
    drop(stale); // passive old applied request has no current stop authority.
    assert!(
        !c.source
            .as_ref()
            .unwrap()
            .generation_stop_requested_for_fixture(NonZeroU64::new(3).unwrap())
            .unwrap(),
        "at this original cut, an old generation cannot set the current stop view"
    );
    let before = current.polls.load(Ordering::SeqCst);
    current.wake.wake();
    tokio::time::timeout_at(deadline.into(), async {
        loop {
            let changed = current.entered.notified(); tokio::pin!(changed); changed.as_mut().enable();
            if current.polls.load(Ordering::SeqCst) > before || c.jobs[1].as_ref().unwrap().result.lock().unwrap().is_some() { break; }
            tokio::select! {
                _ = changed.as_mut() => {},
                actual = poll_fn(|cx| c.jobs[1].as_mut().unwrap().role.poll_shared_completion(cx)) => {
                    panic!("old generation unexpectedly completed the current original job: {}", actual.is_ok());
                },
            }
        }
    }).await.unwrap();
    assert!(c.jobs[1].as_ref().unwrap().result.lock().unwrap().is_none());
    assert_eq!(current.drops.load(Ordering::SeqCst), 0);
    assert!(!c.jobs[1].as_ref().unwrap().role.try_retire().unwrap());
    assert!(Arc::ptr_eq(
        &c.source
            .as_ref()
            .unwrap()
            .applied_request(NonZeroU64::new(3).unwrap())
            .unwrap(),
        &original
    ));
    assert_eq!(c.cohort.as_ref().unwrap().remaining(), 0);
    c.source
        .as_ref()
        .unwrap()
        .close_generation(NonZeroU64::new(3).unwrap())
        .unwrap();
    c.join(1, deadline, SubscriptionEnd::CoreClosed).await;
    assert_eq!(current.opens.load(Ordering::SeqCst), 1);
    assert_eq!(current.drops.load(Ordering::SeqCst), 1);
    drop((phase, original, child, parent));
    c.finish().await;
    drop(retained.0.take());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abandoned_owner_closes_projection_and_aborts_without_minting_join_success() {
    // There is no current per-RPC facade/reaper Drop-close contract. The
    // actual Source owner explicitly fences new jobs; only its original
    // generation control closes an already-running RPC. Neither is an exit.
    let mut retained = Case::prepare_retained(1);
    let c = retained.0.as_mut().unwrap();
    let parent = c.root.as_ref().unwrap().owner.scope();
    c.source = Some(
        SubscriptionSource::from_cohort(
            c.f.as_ref().unwrap().key.participation,
            &parent,
            c.cohort.as_ref().unwrap(),
            c.f.as_ref().unwrap().input.clone(),
        )
        .unwrap(),
    );
    let child = c.source.as_ref().unwrap().scope().unwrap();
    let probe = Arc::new(Probe::default());
    let deadline = Instant::now() + Duration::from_secs(2);
    c.start(0, 2, probe.clone(), false);
    polled(&probe, deadline).await;
    c.source.as_ref().unwrap().close_admission();
    let phase = c.f.as_ref().unwrap().phase(3);
    let refused = Arc::new(SubscriptionRequest {
        participation: c.f.as_ref().unwrap().key.participation,
        generation: NonZeroU64::new(3).unwrap(),
        status_cursors: Box::new([]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([]),
    });
    assert!(matches!(
        c.source.as_ref().unwrap().prepare(refused, &phase),
        Err(crate::task_execution::host::subscriptions::SubscriptionSourceError::Closed)
    ));
    phase.close_after_exit().unwrap();
    assert!(c.jobs[0].as_ref().unwrap().result.lock().unwrap().is_none());
    assert!(!c.jobs[0].as_ref().unwrap().role.try_retire().unwrap());
    assert!(
        c.source
            .as_ref()
            .unwrap()
            .reap_generation(NonZeroU64::new(2).unwrap())
            .is_err()
    );
    assert!(c.source.as_ref().unwrap().close_after_jobs().is_err());
    assert!(child.check().is_ok());
    assert_eq!(probe.opens.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.drops.load(Ordering::SeqCst),
        0,
        "admission fence is not original F destruction"
    );
    c.source
        .as_ref()
        .unwrap()
        .close_generation(NonZeroU64::new(2).unwrap())
        .unwrap();
    c.join(0, deadline, SubscriptionEnd::CoreClosed).await;
    assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
    assert_eq!(
        probe.opens.load(Ordering::SeqCst),
        1,
        "no replacement open after original admission closes"
    );
    assert!(
        child.check().is_ok(),
        "actual generation Join does not itself close stable WC Source"
    );
    drop((phase, child, parent));
    c.finish().await;
    drop(retained.0.take());
}

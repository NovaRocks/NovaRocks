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

//! Actual MIO refusal/exit and the same Optimize preparation receiving policy.
use super::*;
use novarocks_failure_contract::FailureProjection;
use novarocks_resource_contract::SharedDependency;
use novarocks_spi::connector::{ConnectorError, ConnectorIoCleanupFuture};
use novarocks_workload_control::{
    CancellationReason, ControlStorageConfig, ResponsibilityConfig, ResultCapacityConfig,
    WorkloadConfig, WorkloadControl,
    joint_admission::{JointProgress, JointProgressDriver},
};
use std::{
    num::NonZeroUsize,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::Waker,
};

fn drive(driver: &mut JointProgressDriver) {
    for _ in 0..100000 {
        if matches!(driver.step(), JointProgress::Idle(_)) {
            return;
        }
    }
    panic!("same responsibility driver must become idle");
}
struct ActualIo {
    released: Arc<AtomicBool>,
    exits: Arc<AtomicUsize>,
}
impl Future for ActualIo {
    type Output = Result<(), ConnectorError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        if self.released.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}
impl Drop for ActualIo {
    fn drop(&mut self) {
        self.exits.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn original_guard_cause_survives_actual_mio_refusal_and_close_without_replacement() {
    // Same actual substrate as MIO's original-owner regressions. LocalHost is
    // this component's explicit route, not a fabricated Native SQL full-D.
    let count = 8;
    let control = WorkloadControl::try_new_counted(WorkloadConfig {
        root_limit: count,
        business_limit: count,
        query_concurrency_limit: count,
        scope_records_limit: count,
        ..WorkloadConfig::default()
    })
    .unwrap()
    .owner;
    control
        .configure_result_capacity(ResultCapacityConfig::V1)
        .unwrap();
    let (_, mut driver) = control
        .configure_responsibility(ResponsibilityConfig {
            work_per_turn: NonZeroUsize::new(7).unwrap(),
        })
        .unwrap();
    control
        .configure_control_storage(ControlStorageConfig::new(count, &[]))
        .unwrap();
    control.mark_ready().unwrap();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let io =
        MetadataIoAdmission::for_original_source(&root.owner, vec![SharedDependency::LocalHost])
            .unwrap();
    let mut cx = Context::from_waker(Waker::noop());
    let mut ready = false;
    for _ in 0..256 {
        ready = io.poll_progress(&root.owner, &mut cx).unwrap();
        if ready {
            break;
        }
        drive(&mut driver);
    }
    assert!(ready);
    let mut request = Box::pin(io.request(SharedDependency::LocalHost).unwrap().admitted());
    let mut permit = None;
    for _ in 0..256 {
        drive(&mut driver);
        io.poll_progress(&root.owner, &mut cx).unwrap();
        if let Poll::Ready(result) = request.as_mut().poll(&mut cx) {
            permit = Some(result.unwrap());
            break;
        }
    }
    let permit = permit.expect("same actual positive AsyncIo request");
    drop(request);
    let released = Arc::new(AtomicBool::new(false));
    let exits = Arc::new(AtomicUsize::new(0));
    let mut original: Option<ConnectorIoCleanupFuture> = Some(Box::pin(ActualIo {
        released: released.clone(),
        exits: exits.clone(),
    }));
    permit
        .begin_stream()
        .unwrap()
        .install_cleanup_in(&mut original)
        .unwrap_or_else(|_| panic!("same original receiving accepts actual I/O F"));
    assert!(original.is_none());

    let mut refused = Box::pin(io.request(SharedDependency::LocalHost).unwrap().admitted());
    root.owner.cancel(CancellationReason::Requested);
    let mut refusal = None;
    for _ in 0..256 {
        io.poll_progress(&root.owner, &mut cx).unwrap();
        drive(&mut driver);
        match refused.as_mut().poll(&mut cx) {
            Poll::Ready(Err(error)) => {
                refusal = Some(error);
                break;
            }
            Poll::Ready(Ok(_)) => panic!("cancelled original cannot admit a second request"),
            Poll::Pending => {}
        }
    }
    let refusal = refusal.expect("actual original WC request refusal");
    drop(refused);
    let primary = super::super::rewrite_job::guard_terminal_error(
        crate::mv::domain::readiness::connector_application_error(refusal),
    );
    let cause = primary
        .failure_cause
        .clone()
        .expect("actual guard receiver preserves cause");
    let fact = primary.selected_failure_fact().unwrap();
    let conclusion = Some(Err(primary));
    let mut ordinary = None;
    let mut health = None;
    assert!(
        poll_preparation_close(&io, &conclusion, &mut ordinary, &mut health, &mut cx).is_pending()
    );
    assert_eq!(exits.load(Ordering::SeqCst), 0);
    released.store(true, Ordering::SeqCst);
    io.poll_continuing(&mut cx).unwrap();
    assert_eq!(
        exits.load(Ordering::SeqCst),
        1,
        "actual same F destructor returned"
    );
    assert!(
        poll_preparation_close(&io, &conclusion, &mut ordinary, &mut health, &mut cx).is_pending(),
        "the real permit alias still holds its cell"
    );
    drop(permit);
    let error = match poll_preparation_close(&io, &conclusion, &mut ordinary, &mut health, &mut cx)
    {
        Poll::Ready(Err(error)) => error,
        _ => panic!("actual quiescence must deliver the already received preparation failure"),
    };
    assert_eq!(error.failure_cause(), Some(&cause));
    assert_eq!(error.execution_failure().unwrap().fact(), fact);
    assert!(matches!(ordinary, Some(WorkError::Cancelled(_))));
    assert!(
        io.failure().is_some(),
        "ordinary close fact remains in the original source"
    );
    assert!(health.is_none() && io.health_failure().is_none());
    assert_eq!(control.snapshot().root_responsibilities, 1);

    // Without a selected business failure, the same ordinary rejection is
    // still visible. Neither an absent conclusion nor Ok may hide it.
    for conclusion in [None, Some(Ok(()))] {
        let error =
            match poll_preparation_close(&io, &conclusion, &mut ordinary, &mut health, &mut cx) {
                Poll::Ready(Err(error)) => error,
                _ => panic!("ordinary original refusal cannot become successful preparation"),
            };
        assert_eq!(
            error.execution_failure().unwrap().fact(),
            ordinary.as_ref().unwrap().failure_fact()
        );
        assert!(
            error
                .failure_cause()
                .unwrap()
                .downcast_ref::<WorkError>()
                .is_some()
        );
        assert!(health.is_none());
    }
    drop((conclusion, error, cause, io));
    root.business.release();
    root.owner.complete_after_terminal_cancel_settled();
    control.close_admission();
    drive(&mut driver);
    assert!(
        control.shutdown().is_ok(),
        "same original Root and WC really drain after actual exit"
    );
}

#[test]
fn raw_metadata_health_still_refuses_close_without_discarding_preparation_cause() {
    // Reuse the real cleanup-poll/secondary-cause-Drop panic receiver; this is
    // not a fabricated actual-quiescence result or a new disposal hook.
    let (io, primary_drops, _) = super::super::io_admission::cleanup_tests::two_failures(true);
    let primary = super::super::rewrite_job::guard_terminal_error(
        crate::mv::domain::readiness::connector_application_error(ConnectorError::new(
            novarocks_spi::connector::ConnectorErrorKind::ResourceExhausted,
            "earlier original preparation refusal",
        )),
    );
    let cause = primary.failure_cause.clone().unwrap();
    let conclusion = Some(Err(primary));
    let mut ordinary = None;
    let mut health = None;
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        poll_preparation_close(&io, &conclusion, &mut ordinary, &mut health, &mut cx),
        Poll::Ready(Err(_))
    ));
    let failure = health
        .as_ref()
        .expect("actual metadata health is independently retained");
    assert!(std::ptr::eq(
        failure
            .cause()
            .downcast_ref::<MetadataIoAdmission>()
            .unwrap(),
        io.as_ref()
    ));
    assert_eq!(
        conclusion
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .failure_cause
            .as_ref(),
        Some(&cause)
    );
    assert_eq!(primary_drops.load(Ordering::SeqCst), 0);
    drop(io);
    assert_eq!(
        primary_drops.load(Ordering::SeqCst),
        0,
        "health receiver holds the original raw cleanup cause"
    );
    drop(health);
    assert_eq!(primary_drops.load(Ordering::SeqCst), 1);
}

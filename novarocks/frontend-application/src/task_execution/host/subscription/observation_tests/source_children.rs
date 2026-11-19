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

//! Original stable WC Source children; no generation, future or RPC is started.
use super::*;
use crate::query_execution::native_execution_adapter::source_cohort::tests as wc;
use crate::task_execution::host::subscriptions::{SubscriptionSource, SubscriptionSourceError};
use novarocks_workload_control::{WorkClass, WorkError, WorkRequest};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_subscription_responsibility_keeps_the_original_workload_class() {
    for class in [
        WorkClass::Query,
        WorkClass::MaterializedView,
        WorkClass::TableMaintenance,
        WorkClass::Management,
    ] {
        let f = Fixture::new(8, 4);
        let (control, mut driver, capacity) = wc::fixture();
        let root = control.try_begin_root(WorkRequest::new(class)).unwrap();
        let parent = root.owner.scope();
        wc::bind(&parent, &mut driver);
        let cohort = wc::cohort(&parent, 1, &mut driver);
        let source =
            SubscriptionSource::from_cohort(f.key.participation, &parent, &cohort, f.input.clone())
                .unwrap();
        let child = source.scope().unwrap();
        let snapshot = control.snapshot();
        let actual = snapshot
            .scopes
            .iter()
            .find(|row| row.id == child.id())
            .unwrap();
        assert_eq!(actual.parent, Some(parent.id()));
        assert_eq!(actual.class, class);
        assert_eq!(
            snapshot
                .scopes
                .iter()
                .filter(|row| row.parent == Some(parent.id()))
                .count(),
            1
        );
        assert_eq!(cohort.remaining(), 0);
        assert!(child.check().is_ok());
        assert!(
            matches!(
                source.take_owner_after_jobs(),
                Err(SubscriptionSourceError::JobsStillLive)
            ),
            "an unstarted generation does not close the stable original Source"
        );
        source.close_new_jobs();
        source.close_after_jobs().unwrap();
        wc::drive(&mut driver);
        tokio::time::timeout(std::time::Duration::from_secs(2), child.wait_released())
            .await
            .unwrap();
        assert!(matches!(child.check(), Err(WorkError::Released)));
        drop((source, child, cohort, parent, capacity));
        wc::finish(control, root, driver);
        f.finish();
    }
}

#[tokio::test]
async fn subscription_refuses_exhausted_cohort_without_individual_admission() {
    let f = Fixture::new(8, 4);
    let (control, mut driver, capacity) = wc::fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let parent = root.owner.scope();
    wc::bind(&parent, &mut driver);
    let cohort = wc::cohort(&parent, 1, &mut driver);
    let mut original = None;
    cohort
        .take_child_in(&parent, &mut original)
        .unwrap()
        .notify();
    original
        .take()
        .unwrap()
        .complete_after_terminal_cancel_settled();
    wc::drive(&mut driver);
    assert_eq!(cohort.remaining(), 0);
    let before = control.snapshot().scopes.len();
    let refused = match SubscriptionSource::from_cohort(
        f.key.participation,
        &parent,
        &cohort,
        f.input.clone(),
    ) {
        Err(error) => error,
        Ok(source) => {
            source.close_new_jobs();
            source.close_after_jobs().unwrap();
            panic!("an exhausted original cohort cannot mint another Source child");
        }
    };
    assert!(matches!(
        refused,
        SubscriptionSourceError::Work(WorkError::Capacity("Native Source cohort exhausted"))
    ));
    assert_eq!(cohort.remaining(), 0);
    assert_eq!(
        control.snapshot().scopes.len(),
        before,
        "refusal cannot switch to individual admission"
    );
    assert_eq!(
        control
            .snapshot()
            .scopes
            .iter()
            .filter(|row| row.parent == Some(parent.id()))
            .count(),
        0
    );
    drop((cohort, parent, capacity));
    wc::finish(control, root, driver);
    f.finish();
}

async fn original_unused_generation(close_before_future: bool) {
    use crate::task_execution::host::subscriptions::{
        CoveredSubscriptionIo, CoveredSubscriptionStream, SubscriptionIoFuture,
    };
    use novarocks_attempt_protocol::observation::SubscriptionRequest;
    use novarocks_task_codec::operation::DecodedCoveredSubscription;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct NeverOpen(Arc<AtomicUsize>);
    impl CoveredSubscriptionIo for NeverOpen {
        fn open(
            &self,
            _: DecodedCoveredSubscription,
        ) -> SubscriptionIoFuture<'_, Result<Box<dyn CoveredSubscriptionStream>, ObservationFailure>>
        {
            self.0.fetch_add(1, Ordering::SeqCst);
            panic!("unused original subscription cannot create an opening RPC future")
        }
    }
    let f = Fixture::new(8, 4);
    let (control, mut driver, capacity) = wc::fixture();
    let root = control
        .try_begin_root(WorkRequest::new(WorkClass::Query))
        .unwrap();
    let parent = root.owner.scope();
    wc::bind(&parent, &mut driver);
    let cohort = wc::cohort(&parent, 1, &mut driver);
    let source =
        SubscriptionSource::from_cohort(f.key.participation, &parent, &cohort, f.input.clone())
            .unwrap();
    let child = source.scope().unwrap();
    let generation = NonZeroU64::new(2).unwrap();
    let phase = f.phase(2);
    let request = Arc::new(SubscriptionRequest {
        participation: f.key.participation,
        generation,
        status_cursors: Box::new([]),
        task_convergence_cursors: Box::new([]),
        participation_cursor: None,
        quiesce_cursor: None,
        required_identities: Box::new([]),
    });
    let job = source.prepare(request.clone(), &phase).unwrap();
    let opens = Arc::new(AtomicUsize::new(0));
    if close_before_future {
        source.close_new_jobs();
        let original = job.run(
            Arc::new(NeverOpen(opens.clone())),
            Arc::new(super::panic_retirement::NoFrames),
        );
        // Dropping this never-polled SAME job destroys the actual local
        // request/stop-view future before its unused generation exits.
        drop(original);
    } else {
        drop(job);
    }
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    f.drain_phase(&phase);
    f.drain_progress();
    assert!(Arc::ptr_eq(
        &source.reap_generation(generation).unwrap(),
        &request
    ));
    assert!(
        child.check().is_ok(),
        "a free generation is not the stable Source's close"
    );
    assert_eq!(
        cohort.remaining(),
        0,
        "the original stable child has no second issuer"
    );
    source.close_new_jobs();
    source.close_after_jobs().unwrap();
    wc::drive(&mut driver);
    tokio::time::timeout(std::time::Duration::from_secs(2), child.wait_released())
        .await
        .unwrap();
    assert!(matches!(child.check(), Err(WorkError::Released)));
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    drop((request, phase, source, child, cohort, parent, capacity));
    wc::finish(control, root, driver);
    f.finish();
}
#[tokio::test]
async fn empty_reservation_drop_releases_only_unstarted_original_child() {
    // The per-RPC child/reaper was retired. Unused generation exit and explicit
    // stable Source/WC-child close retain their separate original boundaries.
    original_unused_generation(false).await;
}
#[tokio::test]
async fn close_after_reservation_retires_unpolled_future_without_spawning_it() {
    original_unused_generation(true).await;
}

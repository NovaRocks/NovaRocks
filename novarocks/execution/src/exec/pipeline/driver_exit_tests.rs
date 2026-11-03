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
//! Exit ordering, error collection and one-shot hook oracles.
use super::super::operator::{ExitFailure, ExitObservation};
use super::*;
use crate::exec::pipeline::operator::OperatorLifecycle;
use std::sync::atomic::AtomicUsize;

struct OwnerResource {
    values: Vec<u64>,
    drops: Arc<AtomicUsize>,
}
impl Drop for OwnerResource {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
struct Owner {
    index: usize,
    begins: Arc<Vec<AtomicUsize>>,
    closes: Arc<Vec<AtomicUsize>>,
    pending: Arc<AtomicBool>,
    event: Arc<Observable>,
    exit_failure: Option<ExecutionFailure>,
    resource: Option<Arc<OwnerResource>>,
    close_error: bool,
    close_panic: bool,
}
impl OperatorLifecycle for Owner {
    fn name(&self) -> &str {
        "EXIT_OWNER"
    }

    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.begins[self.index].fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn observe_exit(&mut self) -> ExitObservation {
        if self.pending.load(Ordering::SeqCst) {
            ExitObservation::pending(FinishWatch::Notify(Arc::clone(&self.event)))
        } else if let Some(failure) = self.exit_failure.take() {
            ExitObservation::exited().with_failure(ExitFailure::Required(failure))
        } else {
            ExitObservation::exited()
        }
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.closes[self.index].fetch_add(1, Ordering::SeqCst);
        assert!(!self.close_panic, "owned close panic");
        if self.close_error {
            Err(novarocks_functions::KernelFailure::ResourceExhausted.into())
        } else {
            Ok(())
        }
    }
}
impl crate::exec::pipeline::shape::SourceKernel for Owner {
    fn start(&self) -> crate::exec::pipeline::shape::SourceStart {
        crate::exec::pipeline::shape::SourceStart::Finished
    }
    fn poll_output(
        &mut self,
        _: &mut DriverStep<'_>,
    ) -> ExecutionResult<super::super::step::StepResult> {
        panic!("exit-only owner has no data work")
    }
}
fn owners(count: usize) -> (Vec<Owner>, Arc<Vec<AtomicUsize>>, Arc<Vec<AtomicUsize>>) {
    let begins = Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let closes = Arc::new((0..count).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let owners = (0..count)
        .map(|index| Owner {
            index,
            begins: begins.clone(),
            closes: closes.clone(),
            pending: Arc::new(AtomicBool::new(false)),
            event: Arc::new(Observable::new()),
            exit_failure: None,
            resource: None,
            close_error: false,
            close_panic: false,
        })
        .collect();
    (owners, begins, closes)
}
fn driver(owners: Vec<Owner>) -> PipelineDriver {
    PipelineDriver::new(
        1,
        owners
            .into_iter()
            .map(|owner| {
                Box::new(crate::exec::pipeline::shape::Source::new(owner)) as Box<dyn Operator>
            })
            .collect(),
        None,
        vec![],
        Arc::new(RuntimeState::default()),
        None,
    )
}
fn assert_once(counters: &Arc<Vec<AtomicUsize>>) {
    for counter in counters.iter() {
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }
}
#[test]
fn success_starts_all_exits_and_closes_each_actual_exited_owner_once() {
    let (owners, begins, closes) = owners(3);
    let pending = owners[1].pending.clone();
    pending.store(true, Ordering::SeqCst);
    let mut driver = driver(owners);
    assert_eq!(
        driver.finish_with_state(DriverState::Finished),
        DriverState::PendingFinish
    );
    assert_once(&begins);
    assert_eq!(closes[0].load(Ordering::SeqCst), 1);
    assert_eq!(
        closes[1].load(Ordering::SeqCst),
        0,
        "the actual pending owner cannot close"
    );
    assert_eq!(closes[2].load(Ordering::SeqCst), 1);
    pending.store(false, Ordering::SeqCst);
    assert_eq!(driver.process(Duration::ZERO), DriverState::Finished);
    drop(driver);
    assert_once(&begins);
    assert_once(&closes);
}
#[test]
fn exit_error_discovered_by_the_last_poll_prevents_success() {
    let (mut owners, begins, closes) = owners(2);
    let original: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
    owners[1].exit_failure = Some(original.clone());
    let pending = owners[1].pending.clone();
    pending.store(true, Ordering::SeqCst);
    let mut driver = driver(owners);
    assert_eq!(
        driver.finish_with_state(DriverState::Finished),
        DriverState::PendingFinish
    );
    pending.store(false, Ordering::SeqCst);
    let DriverState::Failed(actual) = driver.process(Duration::ZERO) else {
        panic!("actual Required origin must prevent success");
    };
    assert!(actual.same_occurrence(&original));
    assert!(
        driver
            .runtime_state
            .error()
            .unwrap()
            .same_occurrence(&original)
    );
    drop(driver);
    assert_once(&begins);
    assert_once(&closes);
}
#[test]
fn one_failed_exit_is_reported_while_a_sibling_still_owns_work() {
    let (mut owners, _, closes) = owners(2);
    let original: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
    owners[0].exit_failure = Some(original.clone());
    let pending = owners[1].pending.clone();
    pending.store(true, Ordering::SeqCst);
    let mut driver = driver(owners);
    assert_eq!(
        driver.finish_with_state(DriverState::Finished),
        DriverState::PendingFinish
    );
    assert!(
        driver
            .runtime_state
            .error()
            .unwrap()
            .same_occurrence(&original)
    );
    assert_eq!(
        closes[0].load(Ordering::SeqCst),
        1,
        "this author's actual exit permits its independent close"
    );
    assert_eq!(
        closes[1].load(Ordering::SeqCst),
        0,
        "the actual pending sibling cannot close"
    );
    pending.store(false, Ordering::SeqCst);
    let DriverState::Failed(actual) = driver.process(Duration::ZERO) else {
        panic!("actual Required origin must prevent success");
    };
    assert!(actual.same_occurrence(&original));
    assert!(
        driver
            .runtime_state
            .error()
            .unwrap()
            .same_occurrence(&original)
    );
    drop(driver);
    assert_once(&closes);
}
#[test]
fn originating_step_failure_is_visible_before_pending_owner_exit() {
    let (mut owners, begins, closes) = owners(2);
    let original: ExecutionFailure = novarocks_functions::KernelFailure::ResourceExhausted.into();
    owners[0].exit_failure = Some(original.clone());
    let pending = owners[1].pending.clone();
    pending.store(true, Ordering::SeqCst);
    let mut driver = driver(owners);
    let primary: ExecutionFailure = novarocks_functions::KernelFailure::InvalidProgram(
        novarocks_functions::KernelDiagnostic::new("originating synchronous step failure"),
    )
    .into();
    assert_eq!(
        driver.finish_with_state(DriverState::Failed(primary.clone())),
        DriverState::PendingFinish
    );
    assert_eq!(driver.runtime_state.error(), Some(primary.clone()));
    assert_once(&begins);
    assert_eq!(
        closes[0].load(Ordering::SeqCst),
        1,
        "this author's actual exit permits its independent close"
    );
    assert_eq!(
        closes[1].load(Ordering::SeqCst),
        0,
        "the actual pending sibling cannot close"
    );
    pending.store(false, Ordering::SeqCst);
    assert_eq!(
        driver.process(Duration::ZERO),
        DriverState::Failed(primary.clone())
    );
    assert_eq!(driver.runtime_state.error(), Some(primary));
    drop(driver);
    assert_once(&closes);
}
#[test]
fn final_close_error_is_bounded_cleanup_and_preserves_success_or_cancellation() {
    for cancel in [false, true] {
        let (mut owners, begins, closes) = owners(7);
        for owner in &mut owners {
            owner.close_error = true;
        }
        let mut driver = driver(owners);
        let proposed = if cancel {
            DriverState::Canceled
        } else {
            DriverState::Finished
        };
        let actual = driver.finish_with_state(proposed.clone());
        assert_eq!(actual, proposed);
        assert!(
            driver.runtime_state.error().is_none(),
            "returned close error never invents an originating failure"
        );
        assert_eq!(driver.cleanup_diagnostics.len(), 4);
        assert_eq!(driver.cleanup_diagnostics_dropped, 3);
        drop(driver);
        assert_once(&begins);
        assert_once(&closes);
    }
}
#[test]
fn a_catchable_close_panic_keeps_original_owner_and_does_not_skip_siblings_or_repeat_hooks() {
    let (mut owners, begins, closes) = owners(3);
    owners[1].close_panic = true;
    let drops = Arc::new(AtomicUsize::new(0));
    let resource = Arc::new(OwnerResource {
        values: vec![7, 11, 19],
        drops: drops.clone(),
    });
    let weak = Arc::downgrade(&resource);
    owners[1].resource = Some(resource);
    let mut driver = driver(owners);
    let control = driver.runtime_state.error_state();
    assert_eq!(
        driver.finish_with_state(DriverState::Finished),
        DriverState::PendingFinish
    );
    assert!(
        control.error().is_none(),
        "cleanup panic cannot become Required"
    );
    let health = control.local_exit_fault();
    assert!(health.is_some());
    assert_once(&begins);
    assert_once(&closes);
    assert_eq!(weak.upgrade().unwrap().values, [7, 11, 19]);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    for _ in 0..3 {
        assert_eq!(driver.process(Duration::ZERO), DriverState::PendingFinish);
    }
    assert_once(&begins);
    assert_once(&closes);
    assert_eq!(control.local_exit_fault(), health);
    assert!(control.error().is_none());
    // Required production dependency: the original Driver's unknown-owner
    // custody must survive facade Drop. A test leak cannot supply that proof.
    drop(driver);
    assert_eq!(
        weak.upgrade()
            .expect("original unknown operator stays in original Driver custody")
            .values,
        [7, 11, 19]
    );
    assert_eq!(drops.load(Ordering::SeqCst), 0);
}

#[test]
fn healthy_driver_releases_the_same_actual_owner_resource_after_true_exit() {
    let (mut owners, begins, closes) = owners(1);
    let drops = Arc::new(AtomicUsize::new(0));
    let resource = Arc::new(OwnerResource {
        values: vec![23, 29],
        drops: drops.clone(),
    });
    let weak = Arc::downgrade(&resource);
    owners[0].resource = Some(resource);
    let mut driver = driver(owners);
    assert_eq!(
        driver.finish_with_state(DriverState::Finished),
        DriverState::Finished
    );
    assert_once(&begins);
    assert_once(&closes);
    drop(driver);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

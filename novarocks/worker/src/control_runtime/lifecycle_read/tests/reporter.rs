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

//! Actual original Host/Work/read authority with pure Context lifecycle facts.
//! The fixture's physical protocol inputs are symbolic; these tests prove
//! neither Native runtime exit nor failure-owner S8 cleanup.

use super::*;

#[derive(Debug)]
struct OriginalSource(Arc<AtomicUsize>);
impl std::fmt::Display for OriginalSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original reporter proposal source")
    }
}
impl std::error::Error for OriginalSource {}
impl FailureProjection for OriginalSource {
    fn failure_fact(&self) -> FailureFact {
        FailureFact::new(
            FailureClass::Data,
            crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
        )
    }
}
impl Drop for OriginalSource {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[test]
fn original_host_reporters_only_propose_and_keep_the_same_sealed_neutral_read_after_owner_exit() {
    let mut f = Fixture::new();
    let strong = Arc::strong_count(f.port());
    let original_work = f.scope.snapshot().unwrap();
    let original_inputs = f.runtime.role_inventory(actor(), None, 64).unwrap();
    let reporter = f.host.as_ref().unwrap().reporter();
    let clones: Vec<_> = (0..8).map(|_| reporter.clone()).collect();
    let another = f.host.as_ref().unwrap().reporter();
    assert_eq!(Arc::strong_count(f.port()), strong);
    assert_eq!(f.scope.snapshot().unwrap(), original_work);
    assert_eq!(
        f.runtime.role_inventory(actor(), None, 64).unwrap(),
        original_inputs
    );
    assert_eq!(reporter.identity(), f.port().identity());
    assert_eq!(another.identity(), reporter.identity());
    let (mut lifetime, mut capacity) = f.lifetime();
    let weak_port = Arc::downgrade(f.port());
    let initial = f.port().observe_neutral();
    assert_eq!(reporter.current().unwrap(), initial.status);
    assert_eq!(reporter.convergence().unwrap(), initial.convergence);
    assert_eq!(
        reporter.secondary_diagnostics().unwrap(),
        initial.diagnostics
    );
    let drops = Arc::new(AtomicUsize::new(0));
    clones[0].failing(
        novarocks_execution::runtime::fragment::ExecutionFailure::from_domain(OriginalSource(
            drops.clone(),
        )),
        TaskFailurePhase::Execution,
    );
    // A real source is retained by the original ingress, but submitting it
    // cannot adjudicate or publish a new status through this detached view.
    assert_eq!(reporter.current().unwrap(), initial.status);
    assert_eq!(reporter.convergence().unwrap(), initial.convergence);
    assert_eq!(drops.load(Ordering::Acquire), 0);
    f.publish_ingress();
    assert_eq!(
        reporter.current().unwrap().state(),
        novarocks_execution_contract::TaskState::Failing
    );
    assert!(f.port().publication().borrow().has_rich_source());
    assert_eq!(Arc::strong_count(f.port()), strong);
    let mut summary = ProposalSummary::default();
    summary.cleanup(Diagnostic {
        phase: TaskFailurePhase::Execution,
        fact: FailureFact::new(
            FailureClass::Unexpected,
            crate::failure_reason::WorkerFailureReason::CompletionWorkUnresolved.reason(),
        ),
        detail: SafeDetail::truncating("original neutral reporter diagnostic"),
        cleanup: true,
    });
    f.command(ContextInput::Summary {
        task: reporter.identity(),
        generation: generation(),
        cut: None,
        summary,
    });
    f.retire_task();
    f.port().retire_execution_port();
    f.detach_source();
    assert_eq!(drops.load(Ordering::Acquire), 1);
    exit_lifetime(&mut f, &mut lifetime, &mut capacity);
    let sealed = lifetime.seal_after_exit().unwrap();
    assert_eq!(
        sealed.status.state(),
        novarocks_execution_contract::TaskState::Failed
    );
    assert!(sealed.convergence.retired());
    assert_eq!(sealed.diagnostics.entries().len(), 1);
    f.finish();
    assert!(weak_port.upgrade().is_none());
    // All readers use the one original seal after the actual port and ledger
    // inventory are gone. Their metadata cannot keep the rich source alive.
    for view in std::iter::once(&reporter)
        .chain(std::iter::once(&another))
        .chain(clones.iter())
    {
        assert_eq!(view.current().unwrap(), sealed.status);
        assert_eq!(view.convergence().unwrap(), sealed.convergence);
        assert_eq!(view.secondary_diagnostics().unwrap(), sealed.diagnostics);
    }
    assert_eq!(drops.load(Ordering::Acquire), 1);
    assert!(weak_port.upgrade().is_none());
}

#[test]
fn an_unsealed_reporter_returns_owner_missing_for_every_read_after_real_inventory_exit() {
    let mut f = Fixture::new();
    let reporter = f.host.as_ref().unwrap().reporter();
    let another = reporter.clone();
    let weak_port = Arc::downgrade(f.port());
    let identity = reporter.identity();
    assert_eq!(reporter.current().unwrap(), f.port().current());
    let (mut lifetime, mut capacity) = f.lifetime();
    // Even genuine pure-protocol retirement does not authorize a reader to
    // manufacture its own seal when the original host deliberately omitted it.
    f.retire_task();
    exit_lifetime(&mut f, &mut lifetime, &mut capacity);
    f.finish();
    assert!(weak_port.upgrade().is_none());
    for view in [&reporter, &another] {
        assert_eq!(view.identity(), identity);
        assert!(matches!(view.current(), Err(TaskReadError::OwnerMissing)));
        assert!(matches!(
            view.convergence(),
            Err(TaskReadError::OwnerMissing)
        ));
        assert!(matches!(
            view.secondary_diagnostics(),
            Err(TaskReadError::OwnerMissing)
        ));
    }
    assert!(weak_port.upgrade().is_none());
}

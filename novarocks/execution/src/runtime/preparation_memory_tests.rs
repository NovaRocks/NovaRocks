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
use novarocks_memory::{AccountKind, AuthorityConfig, ExternalRef, MemoryAuthority};
use novarocks_types::{
    QueryId,
    identity::{AttemptId, QueryExecutionId},
};
use std::{cell::Cell, sync::Arc};

#[derive(Default)]
struct Journal {
    workset: Option<usize>,
    pending: Option<CoverageReceipt>,
    shortage: Option<ShortageReceipt>,
    capacity: Option<CapacityError>,
    body: KernelMemoryJournal,
}
impl Journal {
    fn loan(&mut self) -> PreparationMemoryJournalLoan<'_> {
        PreparationMemoryJournalLoan {
            workset_bytes: &mut self.workset,
            pending: &mut self.pending,
            shortage: &mut self.shortage,
            capacity: &mut self.capacity,
            body: &mut self.body,
        }
    }
}
struct Control {
    checks: Cell<usize>,
    stop_at: usize,
}
impl Control {
    fn running() -> Self {
        Self {
            checks: Cell::new(0),
            stop_at: usize::MAX,
        }
    }
}
impl SynchronousPreparationControl for Control {
    fn checkpoint(&self) -> crate::runtime::task::ExecutionResult<()> {
        self.checks.set(self.checks.get() + 1);
        if self.checks.get() == self.stop_at {
            Err(crate::runtime::task::ExecutionFailure::from_domain(
                PreparationMemoryStop::Abort(AbortCause::LeaseExpired),
            ))
        } else {
            Ok(())
        }
    }
    fn wait(&self) -> crate::runtime::task::ExecutionResult<()> {
        panic!("a granted contract test must not enter Pending");
    }
}
fn bound() -> QueryMemoryBinding {
    let mut cfg = AuthorityConfig::new(131072, 65536, 65536);
    cfg.max_accounts = 32;
    cfg.max_active_owners = 4;
    cfg.metadata_budget_bytes = 16 * 1024;
    cfg.top_up = novarocks_memory::TopUpPolicy::uniform(1024);
    let authority = Arc::new(MemoryAuthority::new(cfg).unwrap());
    let account = authority
        .create_account(AccountKind::Work, ExternalRef::NONE)
        .unwrap();
    let execution =
        QueryExecutionId::new(QueryId::new(37, 201), AttemptId::new(1).unwrap()).unwrap();
    QueryMemoryBinding::try_new(execution, authority, account).unwrap()
}
fn facts() -> CompleteMetadataRequestFacts {
    CompleteMetadataRequestFacts {
        allocation_requests_upper_bound: 1,
        allocation_request_bytes_upper_bound: 600,
    }
}
#[test]
fn preparation_memory_real_grant_runs_original_body_once_and_settles() {
    let binding = bound();
    let control = Control::running();
    let mut journal = Journal::default();
    let calls = Cell::new(0);
    let output = SynchronousPreparationMemory::new(Some(&binding), &control, journal.loan())
        .materialize(facts(), "explicit test overflow", || {
            calls.set(calls.get() + 1);
            Ok::<_, ()>(73)
        })
        .unwrap();
    assert_eq!(output, 73);
    assert_eq!(calls.get(), 1);
    assert_eq!(journal.workset, Some(608));
    assert_eq!(journal.body.settlement.as_ref().unwrap().next_step, Ok(()));
    assert_eq!(journal.body.stopped, Some(Ok(())));
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
}
#[test]
fn preparation_memory_stop_before_or_after_real_grant_never_runs_body() {
    for stop_at in [1, 2] {
        let binding = bound();
        let control = Control {
            checks: Cell::new(0),
            stop_at,
        };
        let mut journal = Journal::default();
        let calls = Cell::new(0);
        let result = SynchronousPreparationMemory::new(Some(&binding), &control, journal.loan())
            .materialize(facts(), "explicit test overflow", || {
                calls.set(calls.get() + 1);
                Ok::<_, ()>(())
            });
        assert!(matches!(
            result,
            Err(SynchronousPreparationFailure::Host(
                PreparationMemoryRefusal::Interrupted(ref cause)
            )) if preparation_stop_source(cause) == Some(&PreparationMemoryStop::Abort(AbortCause::LeaseExpired))
        ));
        assert_eq!(calls.get(), 0);
        assert!(journal.body.settlement.is_none());
        assert_eq!(binding.account().snapshot().granted_bytes, 0);
    }
}
#[test]
fn preparation_memory_missing_binding_remains_nominal_after_original_stop_check() {
    let control = Control::running();
    let mut journal = Journal::default();
    let result = SynchronousPreparationMemory::new(None, &control, journal.loan()).materialize(
        facts(),
        "explicit test overflow",
        || -> Result<(), ()> {
            panic!("missing binding cannot execute the body");
        },
    );
    assert!(matches!(
        result,
        Err(SynchronousPreparationFailure::Host(
            PreparationMemoryRefusal::MissingQueryMemory
        ))
    ));
    assert_eq!(control.checks.get(), 1);
    assert!(journal.body.settlement.is_none());
}
#[test]
fn preparation_memory_overflow_preserves_caller_detail_without_request_or_body() {
    let control = Control::running();
    let mut journal = Journal::default();
    let facts = CompleteMetadataRequestFacts {
        allocation_requests_upper_bound: usize::MAX,
        allocation_request_bytes_upper_bound: 0,
    };
    let result = SynchronousPreparationMemory::new(None, &control, journal.loan()).materialize(
        facts,
        "original caller overflow text",
        || -> Result<(), ()> {
            panic!("failed request arithmetic cannot execute the body");
        },
    );
    assert!(matches!(
        result,
        Err(SynchronousPreparationFailure::Host(
            PreparationMemoryRefusal::Capacity(CapacityError::Invalid {
                detail: "original caller overflow text"
            })
        ))
    ));
    assert_eq!(control.checks.get(), 0);
    assert_eq!(
        journal.capacity,
        Some(CapacityError::Invalid {
            detail: "original caller overflow text"
        })
    );
    assert!(journal.workset.is_none() && journal.body.settlement.is_none());
}
#[test]
fn preparation_memory_body_error_wins_over_real_debt_and_policy_settlement() {
    use novarocks_memory::attribution::AttributingAllocator;
    use std::{
        alloc::{GlobalAlloc, Layout, System},
        ptr::NonNull,
    };
    let binding = bound();
    let control = Control::running();
    let mut journal = Journal::default();
    let allocator = AttributingAllocator::new(System);
    let actual = Layout::from_size_align(1200, 8).unwrap();
    let original = "original body error before settlement";
    let result = SynchronousPreparationMemory::new(Some(&binding), &control, journal.loan())
        .materialize(facts(), "explicit test overflow", || {
            // Fault injection: a real block exceeds this test's stated envelope.
            // This proves first-cause reporting, not a legal metadata recipe.
            // SAFETY: the allocator receives a valid nonzero Layout; its exact
            // successful block is released once below with this same Layout.
            let block = NonNull::new(unsafe { allocator.alloc(actual) }).unwrap();
            binding
                .account()
                .install_policy(0, novarocks_memory::LimitDimension::Work);
            Err::<(), _>((original, block))
        });
    let Err(SynchronousPreparationFailure::Body((message, block))) = result else {
        panic!("the original body error must stay primary");
    };
    assert!(std::ptr::eq(message.as_ptr(), original.as_ptr()));
    let receipt = journal.body.settlement.as_ref().unwrap();
    assert_eq!(receipt.accepted_live, 1208);
    assert_eq!(receipt.debt, 600);
    assert!(matches!(
        receipt.next_step,
        Err(CapacityError::QueryLimit(_))
    ));
    assert!(journal.capacity.is_none());
    assert_eq!(journal.body.stopped, Some(Ok(())));
    // SAFETY: release the one successful block with its original allocator/Layout.
    unsafe { allocator.dealloc(block.as_ptr(), actual) };
}

#[test]
fn preparation_stop_is_an_observation_and_cannot_refine_another_tasks_cause() {
    use novarocks_execution_contract::{AbortCause, CancelReason};
    use novarocks_failure_contract::{FailureClass, FailureEvidence, FailureProjection};
    for stop in [
        PreparationMemoryStop::Cancel(CancelReason::UpstreamNoLongerNeeded),
        PreparationMemoryStop::Abort(AbortCause::QueryFailed),
        PreparationMemoryStop::Abort(AbortCause::PeerTaskFailed),
        PreparationMemoryStop::Abort(AbortCause::LeaseExpired),
    ] {
        let fact = PreparationMemoryRefusal::Interrupted(
            crate::runtime::task::ExecutionFailure::from_domain(stop),
        )
        .failure_fact();
        assert_eq!(fact.class(), FailureClass::Control);
        assert_eq!(fact.evidence(), FailureEvidence::StopObservation);
        assert_eq!(fact.reason().code(), 3);
        assert_eq!(fact.resource(), None);
    }
    let setup = PreparationMemoryRefusal::MissingQueryMemory.failure_fact();
    assert_eq!(setup.class(), FailureClass::Invariant);
    assert_eq!(setup.evidence(), FailureEvidence::Originating);
    let original = CapacityError::Invalid {
        detail: "actual preparation source model",
    };
    let refusal = PreparationMemoryRefusal::Capacity(original);
    assert!(
        std::error::Error::source(&refusal)
            .unwrap()
            .is::<CapacityError>()
    );
}

fn preparation_stop_source<'a>(
    cause: &'a (dyn std::error::Error + 'static),
) -> Option<&'a PreparationMemoryStop> {
    let mut current = Some(cause);
    while let Some(cause) = current {
        if let Some(stop) = cause.downcast_ref::<PreparationMemoryStop>() {
            return Some(stop);
        }
        current = cause.source();
    }
    None
}

#[test]
fn preparation_control_keeps_an_actual_originating_failure_without_inventing_stop_intent() {
    use crate::runtime::task::{ExecutionFailure, ExecutionResult};
    use novarocks_failure_contract::{FailureEvidence, FailureProjection};
    struct OriginalControl(ExecutionFailure);
    impl SynchronousPreparationControl for OriginalControl {
        fn checkpoint(&self) -> ExecutionResult<()> {
            Err(self.0.clone())
        }
        fn wait(&self) -> ExecutionResult<()> {
            panic!("a refused control never enters Pending")
        }
    }
    let raw = novarocks_functions::KernelFailure::ResourceExhausted;
    let expected = raw.failure_fact();
    let original: ExecutionFailure = raw.into();
    let control = OriginalControl(original.clone());
    let binding = bound();
    let calls = Cell::new(0);
    let mut journal = Journal::default();
    let Err(SynchronousPreparationFailure::Host(
        refusal @ PreparationMemoryRefusal::Interrupted(_),
    )) = SynchronousPreparationMemory::new(Some(&binding), &control, journal.loan()).materialize(
        facts(),
        "explicit test overflow",
        || {
            calls.set(calls.get() + 1);
            Ok::<_, ()>(())
        },
    )
    else {
        panic!("the control's actual refusal must win before admission or body");
    };
    assert_eq!(refusal.failure_fact(), expected);
    assert_eq!(
        refusal.failure_fact().evidence(),
        FailureEvidence::Originating
    );
    let PreparationMemoryRefusal::Interrupted(source) = &refusal else {
        unreachable!()
    };
    assert!(std::ptr::eq(source.cause(), original.cause()));
    assert!(std::ptr::eq(
        std::error::Error::source(&refusal)
            .unwrap()
            .downcast_ref::<ExecutionFailure>()
            .unwrap()
            .cause(),
        original.cause()
    ));
    assert_eq!(calls.get(), 0);
    assert!(journal.body.settlement.is_none());
    assert_eq!(binding.account().snapshot().granted_bytes, 0);
}

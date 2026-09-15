//! FE process-runtime responsibility for provider credential jobs that outlive
//! an attempt.
//!
//! A terminal attempt must not keep a client, Worker context, or task drain
//! waiting for an already-entered provider call.  It also must not discard the
//! call: the Connector admission permit remains owned until that call really
//! returns.  This owner retains only a secret-free lifecycle record.  The
//! submitted task consumes and drops any provider response before publishing
//! its terminal status.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use novarocks_types::QueryExecutionId;
use tokio::runtime::Handle;
use tokio::sync::Notify;

use super::blocking_io::{ConnectorBlockingIoError, ConnectorBlockingIoJob};

const FENCE_OPEN: u8 = 0;
const FENCE_ENTERED: u8 = 1;
const FENCE_CLOSED: u8 = 2;

/// One attempt-local capability used exactly at the external provider edge.
///
/// `enter_provider_call` is the linearization point between an attempt's
/// terminal fence and the real external request.  Once it reports `Entered`,
/// shutdown cannot claim that the request was prevented; it must transfer the
/// job to the process-runtime owner instead.
#[derive(Clone, Debug)]
pub(crate) struct AttemptProviderGenerationFence(Arc<AtomicU8>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ProviderCallEntry {
    Entered,
    Closed,
}

impl AttemptProviderGenerationFence {
    pub(crate) fn new() -> Self {
        Self(Arc::new(AtomicU8::new(FENCE_OPEN)))
    }

    pub(crate) fn enter_provider_call(&self) -> ProviderCallEntry {
        match self.0.compare_exchange(
            FENCE_OPEN,
            FENCE_ENTERED,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => ProviderCallEntry::Entered,
            Err(FENCE_CLOSED) => ProviderCallEntry::Closed,
            Err(FENCE_ENTERED) => ProviderCallEntry::Entered,
            Err(state) => unreachable!("unknown provider generation fence state {state}"),
        }
    }

    pub(crate) fn close(&self) {
        let _ = self.0.compare_exchange(
            FENCE_OPEN,
            FENCE_CLOSED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }
}

/// A secret-free terminal observation for a residual provider job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CredentialResidualJobOutcome {
    Completed,
    Failed,
    DeadlineExhausted,
    FencedBeforeProviderCall,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CredentialResidualJobRecord {
    pub(crate) execution_id: QueryExecutionId,
    pub(crate) outcome: CredentialResidualJobOutcome,
}

#[derive(Default)]
struct CredentialResidualJobState {
    next_id: u64,
    active: BTreeMap<u64, QueryExecutionId>,
    terminal: Vec<CredentialResidualJobRecord>,
}

struct CredentialResidualJobInner {
    runtime: Handle,
    state: Mutex<CredentialResidualJobState>,
    changed: Notify,
}

/// Host-owned process runtime.  Cloneable handles cannot inspect or mutate
/// credential material; they can only transfer one outstanding job.
pub(crate) struct CredentialResidualJobOwner {
    inner: Arc<CredentialResidualJobInner>,
}

#[derive(Clone)]
pub(crate) struct CredentialResidualJobHandle {
    inner: Arc<CredentialResidualJobInner>,
}

impl CredentialResidualJobOwner {
    pub(crate) fn new(runtime: Handle) -> Self {
        Self {
            inner: Arc::new(CredentialResidualJobInner {
                runtime,
                state: Mutex::new(CredentialResidualJobState::default()),
                changed: Notify::new(),
            }),
        }
    }

    pub(crate) fn handle(&self) -> CredentialResidualJobHandle {
        CredentialResidualJobHandle {
            inner: Arc::clone(&self.inner),
        }
    }

    pub(crate) async fn shutdown_until(&self, deadline: Instant) -> Result<(), String> {
        loop {
            let notified = self.inner.changed.notified();
            if self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .active
                .is_empty()
            {
                return Ok(());
            }
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), notified)
                .await
                .map_err(|_| {
                    "frontend credential residual provider-job shutdown deadline exceeded"
                        .to_string()
                })?;
        }
    }

    #[cfg(test)]
    pub(crate) fn active_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .active
            .len()
    }

    #[cfg(test)]
    pub(crate) fn terminal_records(&self) -> Vec<CredentialResidualJobRecord> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .terminal
            .clone()
    }
}

impl CredentialResidualJobHandle {
    /// Transfers one job without retaining its typed output in process state.
    /// `classify` runs in the draining task and must not expose secret values.
    pub(crate) fn retain<T, F>(
        &self,
        execution_id: QueryExecutionId,
        job: ConnectorBlockingIoJob<T>,
        classify: F,
    ) where
        T: Send + 'static,
        F: FnOnce(Result<T, ConnectorBlockingIoError>) -> CredentialResidualJobOutcome
            + Send
            + 'static,
    {
        let id = {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let id = state.next_id;
            state.next_id = state
                .next_id
                .checked_add(1)
                .expect("residual job id overflow");
            state.active.insert(id, execution_id);
            id
        };
        let inner = Arc::clone(&self.inner);
        self.inner.runtime.spawn(async move {
            let outcome = classify(job.finish().await);
            let mut state = inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let execution_id = state
                .active
                .remove(&id)
                .expect("residual credential job must remain registered until its task exits");
            state.terminal.push(CredentialResidualJobRecord {
                execution_id,
                outcome,
            });
            drop(state);
            inner.changed.notify_waiters();
        });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;
    use crate::task_execution::blocking_io::ConnectorBlockingIoSupervisor;
    use novarocks_native_adapter::connector_blocking_io::ConnectorBlockingIoBudget;
    use novarocks_types::{AttemptId, QueryId};
    use tokio::sync::oneshot;

    #[test]
    fn fence_prevents_a_call_after_close() {
        let fence = AttemptProviderGenerationFence::new();
        fence.close();
        assert_eq!(fence.enter_provider_call(), ProviderCallEntry::Closed);
    }

    #[test]
    fn queued_job_observes_the_attempt_fence_before_its_provider_call() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .max_blocking_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let supervisor = ConnectorBlockingIoSupervisor::new(
            runtime.handle().clone(),
            ConnectorBlockingIoBudget::try_new(2, 1).expect("budget"),
        );
        let (blocker_entered, blocker_entered_rx) = mpsc::channel();
        let (release_first, release_first_rx) = mpsc::channel();
        let blocker = supervisor.spawn_protected(move || {
            blocker_entered
                .send(())
                .expect("test observes blocking admission");
            release_first_rx
                .recv()
                .expect("test releases blocking admission");
        });
        let (second_entered, second_entered_rx) = mpsc::channel();
        let (release_second, release_second_rx) = mpsc::channel();
        let second_blocker = supervisor.spawn_protected(move || {
            second_entered
                .send(())
                .expect("test observes second blocking admission");
            release_second_rx
                .recv()
                .expect("test releases second blocking admission");
        });
        blocker_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("protected lane starts the blocker");
        second_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("protected lane starts the second blocker");

        let fence = AttemptProviderGenerationFence::new();
        let provider_fence = fence.clone();
        let provider_called = Arc::new(AtomicBool::new(false));
        let called = Arc::clone(&provider_called);
        let queued =
            supervisor.spawn_protected(move || match provider_fence.enter_provider_call() {
                ProviderCallEntry::Entered => {
                    called.store(true, Ordering::SeqCst);
                    ProviderCallEntry::Entered
                }
                ProviderCallEntry::Closed => ProviderCallEntry::Closed,
            });

        fence.close();
        release_first.send(()).expect("release first blocker");
        release_second.send(()).expect("release second blocker");
        let outcome = runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(1), queued.finish())
                .await
                .expect("queued job converges")
                .expect("queued job completes")
        });
        assert_eq!(outcome, ProviderCallEntry::Closed);
        assert!(
            !provider_called.load(Ordering::SeqCst),
            "a job queued behind the permit must not call a provider after its attempt ended"
        );
        runtime.block_on(async {
            blocker.finish().await.expect("blocker completes");
            second_blocker
                .finish()
                .await
                .expect("second blocker completes");
        });
    }

    #[tokio::test]
    async fn owner_retains_admission_until_the_job_really_exits() {
        let runtime = Handle::current();
        let supervisor = ConnectorBlockingIoSupervisor::new(
            runtime.clone(),
            ConnectorBlockingIoBudget::try_new(2, 1).expect("budget"),
        );
        let owner = CredentialResidualJobOwner::new(runtime);
        let (entered, entered_rx) = oneshot::channel();
        let (release, release_rx) = oneshot::channel();
        let job = supervisor.spawn_protected(move || {
            let _ = entered.send(());
            release_rx
                .blocking_recv()
                .expect("test releases provider call");
        });
        entered_rx.await.expect("provider call entered");
        let execution_id =
            QueryExecutionId::new(QueryId::new(1, 2), AttemptId::new(1).expect("attempt"))
                .expect("execution identity");
        owner.handle().retain(execution_id, job, |outcome| {
            assert!(outcome.is_ok());
            CredentialResidualJobOutcome::Completed
        });
        assert_eq!(owner.active_count(), 1);
        release.send(()).expect("release provider call");
        owner
            .shutdown_until(Instant::now() + Duration::from_secs(1))
            .await
            .expect("job converges");
        assert_eq!(owner.active_count(), 0);
        assert_eq!(
            owner.terminal_records(),
            vec![CredentialResidualJobRecord {
                execution_id,
                outcome: CredentialResidualJobOutcome::Completed,
            }]
        );
    }
}

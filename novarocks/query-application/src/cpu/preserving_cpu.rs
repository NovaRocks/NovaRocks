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

//! Stable receipt for CPU preparation on the existing elastic worker owner.
//! Reading this API supplies no physical limit or finite-source qualification.

use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};

/// Normal refusal preserves the same unpublished call in its original slot.
#[derive(Debug)]
pub enum QueryCpuPublishRefusal {
    Empty,
    ForeignPool,
    Closed,
    WorkerExited,
    Spawn(std::io::Error),
    Invariant(&'static str),
}
pub struct QueryCpuPrepareRefusal<F> {
    work: F,
}
impl<F> QueryCpuPrepareRefusal<F> {
    pub fn into_work(self) -> F {
        self.work
    }
}
impl<F> std::fmt::Debug for QueryCpuPrepareRefusal<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QueryCpuPrepareRefusal(nonempty receipt)")
    }
}
#[must_use = "Keep the original unpublished call or publish it once"]
pub struct QueryCpuPreparedJob {
    work: Arc<CpuWork>,
}
#[must_use = "Result delivery is distinct from CPU worker actual exit"]
pub struct QueryCpuReceipt<T> {
    result: Option<oneshot::Receiver<Result<CpuResult, QueryBlockingFailure>>>,
    exit: Arc<bounded_worker::BoundedWorkerActualExit>,
    _type: std::marker::PhantomData<fn() -> T>,
}
impl<T: Send + 'static> QueryCpuReceipt<T> {
    pub fn poll_result(&mut self, cx: &mut Context<'_>) -> Poll<Result<T, QueryBlockingFailure>> {
        let Some(receiver) = self.result.as_mut() else {
            return Poll::Ready(Err(QueryBlockingFailure::ExecutorClosed));
        };
        let value = std::task::ready!(Pin::new(receiver).poll(cx));
        self.result.take();
        Poll::Ready(
            value
                .unwrap_or(Err(QueryBlockingFailure::ExecutorClosed))
                .and_then(|value| {
                    value
                        .downcast::<T>()
                        .map(|value| *value)
                        .map_err(|original| {
                            QueryBlockingFailure::InvalidResultType(
                                QueryBlockingFailurePayload::new(original),
                            )
                        })
                }),
        )
    }
    pub fn poll_actual_exit(
        &self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), QueryBlockingActualExitFailure>> {
        self.exit.poll(cx)
    }
    /// Check even after actual-exit Ok: the foreign exit wake may panic after
    /// the real phase was published. This aliases the original carrier.
    pub fn actual_exit_failure(&self) -> Option<QueryBlockingFailure> {
        self.exit.failure()
    }
}
struct CpuInput {
    call: Option<Box<dyn FnOnce() -> CpuResult + Send>>,
    result: Option<oneshot::Sender<Result<CpuResult, QueryBlockingFailure>>>,
    // This is an alias of the original stable input/source owner, not a newly
    // minted budget. Its final Drop is part of this worker's actual exit.
    custody: Option<Arc<dyn Send + Sync>>,
}
pub(super) struct CpuWork {
    pool: std::sync::Weak<ElasticPlanningPool>,
    input: Mutex<CpuInput>,
    exit: Arc<bounded_worker::BoundedWorkerActualExit>,
    notification: Mutex<[Option<QueryBlockingFailure>; 2]>,
    exited: AtomicBool,
}
/// Known requested layouts of this original author, not a complete payment.
/// Platform Mutex/channel backing and the actual retained input/result graph
/// are deliberately excluded rather than guessed from dependency internals.
#[derive(Clone, Copy, Debug)]
pub struct QueryCpuPreservingGeometry {
    pub header: std::alloc::Layout,
    pub actual_exit_header: std::alloc::Layout,
    pub call: std::alloc::Layout,
    pub prepared_inline: std::alloc::Layout,
    pub receipt_inline: std::alloc::Layout,
    pub worker_record_inline: std::alloc::Layout,
}
fn make_cpu_call<T, F>(work: F) -> impl FnOnce() -> CpuResult + Send + 'static
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    move || Box::new(work()) as CpuResult
}
impl QueryCpuExecutor {
    pub fn preserving_geometry_for_composition<T, F>() -> Result<
        QueryCpuPreservingGeometry,
        novarocks_type_contract::owned_resources::layout::LayoutResourceError,
    >
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        fn layout<I, O>(_: fn(I) -> O) -> std::alloc::Layout {
            std::alloc::Layout::new::<O>()
        }
        Ok(QueryCpuPreservingGeometry {
            header: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<CpuWork>(),
            )?,
            actual_exit_header: novarocks_type_contract::owned_resources::layout::arc_layout(
                std::alloc::Layout::new::<bounded_worker::BoundedWorkerActualExit>(),
            )?,
            call: layout(make_cpu_call::<T, F>),
            prepared_inline: std::alloc::Layout::new::<QueryCpuPreparedJob>(),
            receipt_inline: std::alloc::Layout::new::<QueryCpuReceipt<T>>(),
            worker_record_inline: std::alloc::Layout::new::<ElasticWorkerState>(),
        })
    }
    /// Original callers must pay/retain the actual input and this header before
    /// preparation. The custody alias points to that same stable packet.
    /// The unique input is not captured by a cancellable publisher future.
    pub fn prepare_cpu_preserving_in<T, F, G>(
        &self,
        work: F,
        custody: &Arc<G>,
        receipt: &mut Option<QueryCpuReceipt<T>>,
    ) -> Result<QueryCpuPreparedJob, QueryCpuPrepareRefusal<F>>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
        G: Send + Sync + 'static,
    {
        if receipt.is_some() {
            return Err(QueryCpuPrepareRefusal { work });
        }
        let (result, receiver) = oneshot::channel();
        let exit = Arc::new(bounded_worker::BoundedWorkerActualExit::new());
        let retained: Arc<dyn Send + Sync> = custody.clone();
        let record = Arc::new(CpuWork {
            pool: Arc::downgrade(&self.pool),
            input: Mutex::new(CpuInput {
                call: Some(Box::new(make_cpu_call(work))),
                result: Some(result),
                custody: Some(retained),
            }),
            exit: Arc::clone(&exit),
            notification: Mutex::new([None, None]),
            exited: AtomicBool::new(false),
        });
        *receipt = Some(QueryCpuReceipt {
            result: Some(receiver),
            exit,
            _type: std::marker::PhantomData,
        });
        Ok(QueryCpuPreparedJob { work: record })
    }
    /// Synchronous original worker publication. No job F, callback or wake runs
    /// under pool State; a normal refusal leaves the exact prepared slot full.
    pub fn publish_cpu_preserving_in(
        &self,
        prepared: &mut Option<QueryCpuPreparedJob>,
    ) -> Result<(), QueryCpuPublishRefusal> {
        let original = prepared.as_ref().ok_or(QueryCpuPublishRefusal::Empty)?;
        if !original.work.pool.ptr_eq(&Arc::downgrade(&self.pool)) {
            return Err(QueryCpuPublishRefusal::ForeignPool);
        }
        self.pool.reap_finished();
        let assignment = self.pool.assign_worker().map_err(|error| match error {
            ElasticAssignmentError::Closed => QueryCpuPublishRefusal::Closed,
            ElasticAssignmentError::WorkerIdOverflow => {
                QueryCpuPublishRefusal::Invariant("query CPU worker id overflow")
            }
            ElasticAssignmentError::JoinCountOverflow => {
                QueryCpuPublishRefusal::Invariant("query CPU worker join obligation count overflow")
            }
        })?;
        match assignment {
            ElasticAssignment::Idle { id, sender } => {
                let job = ElasticPlanningJob::Preserving(Arc::clone(&original.work));
                if let Err(error) = sender.send(WorkerCommand::Run(job)) {
                    self.pool.state.lock().unwrap().workers.remove(&id);
                    drop(error); // Only an alias; prepared retains the real call.
                    return Err(QueryCpuPublishRefusal::WorkerExited);
                }
            }
            ElasticAssignment::Start { id } => {
                let (sender, receiver) = mpsc::channel();
                let pool = Arc::clone(&self.pool);
                let work = Arc::clone(&original.work);
                match std::thread::Builder::new()
                    .name(format!("novarocks-query-planning-{id}"))
                    .stack_size(novarocks_types::WORKER_STACK_SIZE_BYTES)
                    .spawn(move || {
                        worker_loop(
                            pool,
                            id,
                            sender,
                            receiver,
                            ElasticPlanningJob::Preserving(work),
                        )
                    }) {
                    Ok(join) => {
                        self.pool.state.lock().unwrap().joins.insert(id, join);
                    }
                    Err(error) => {
                        let mut state = self.pool.state.lock().unwrap();
                        state.workers.remove(&id);
                        state.unjoined_starts -= 1;
                        drop(state);
                        return Err(QueryCpuPublishRefusal::Spawn(error));
                    }
                }
            }
        }
        // The accepted original command/thread already owns another alias.
        drop(prepared.take());
        Ok(())
    }
}
impl CpuWork {
    pub(super) fn run(self: &Arc<Self>, pool: &Arc<ElasticPlanningPool>, id: WorkerId) {
        let (call, result) = {
            let mut input = self.input.lock().unwrap_or_else(|e| e.into_inner());
            (
                input.call.take().expect("original CPU call runs once"),
                input
                    .result
                    .take()
                    .expect("original CPU result sender exists"),
            )
        };
        let outcome = catch_unwind(AssertUnwindSafe(call)).map_err(QueryBlockingFailure::panicked);
        // A normal caught F panic retains the opaque Box in the original
        // result channel. It does not keep the CPU worker position forever.
        if let Err(original) = catch_unwind(AssertUnwindSafe(|| {
            if let Err(undelivered) = result.send(outcome) {
                drop(undelivered);
            }
        })) {
            self.fail(pool, id, QueryBlockingFailure::panicked(original));
        }
    }
    pub(super) fn finish_custody(self: &Arc<Self>, pool: &Arc<ElasticPlanningPool>, id: WorkerId) {
        let custody = self
            .input
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .custody
            .take();
        if let Err(original) = catch_unwind(AssertUnwindSafe(|| drop(custody))) {
            self.fail(pool, id, QueryBlockingFailure::panicked(original));
        }
    }
    pub(super) fn publish_exit(self: &Arc<Self>, pool: &Arc<ElasticPlanningPool>, id: WorkerId) {
        self.exited.store(true, Ordering::Release);
        if let Some(original) = self.exit.publish(1, None) {
            self.fail(pool, id, QueryBlockingFailure::panicked(original));
        }
    }
    fn fail(
        self: &Arc<Self>,
        pool: &Arc<ElasticPlanningPool>,
        id: WorkerId,
        cause: QueryBlockingFailure,
    ) -> ! {
        // The same original worker map retains the record; this is not a
        // second failure registry or reaper. It cannot report a clean drain.
        {
            let mut state = pool.state.lock().unwrap_or_else(|e| e.into_inner());
            state.accepting = false;
            state.workers.insert(
                id,
                ElasticWorkerState::Failed {
                    _original: Arc::clone(self),
                },
            );
        }
        self.notification.lock().unwrap_or_else(|e| e.into_inner())[0] = Some(cause.clone());
        if let Some(original) = self.exit.publish(2, Some(&cause)) {
            self.notification.lock().unwrap_or_else(|e| e.into_inner())[1] =
                Some(QueryBlockingFailure::panicked(original));
        }
        // Thread termination alone does not clear this original failed row.
        // Owner explicit failure disposition is a separate operation.
        std::panic::resume_unwind(Box::new(CpuWorkerUnproven));
    }
}
impl Drop for CpuWork {
    fn drop(&mut self) {
        if !self.exited.load(Ordering::Acquire) {
            // An unpublished refusal or an accepted command lost when its
            // original worker failed has no actual-exit proof. Wake the same
            // observer with a typed unproven phase, never a successful exit.
            if let Some(original) = self
                .exit
                .publish(2, Some(&QueryBlockingFailure::ExecutorClosed))
            {
                std::panic::resume_unwind(original);
            }
        }
    }
}
struct CpuWorkerUnproven;

#[cfg(test)]
mod tests;

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

//! Original serial owner's stable call/result and borrowed actual-exit poll.
//! No publisher task, implicit Drop submission, queue or capacity author.

use super::*;
use novarocks_query_application::cpu::{
    QueryBlockingExecutor, QueryBlockingPreparedJob, QueryBlockingReceipt,
    QueryBlockingReservation, QueryCpuPreparedJob, QueryCpuReceipt,
};
use novarocks_workload_control::joint_admission::{
    BlockingActionAdmission, BlockingActionSource, JointLocalGrant,
};
use std::{
    num::NonZeroU64,
    task::{Context, Poll},
};

/// Known source-owned metadata and original physical call/row layout. Reading
/// it is not payment; fixed-role header/final-alias coverage remains necessary.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ConnectorBlockingIoActionQuote {
    metadata: NonZeroU64,
    physical_queue: NonZeroU64,
}
impl ConnectorBlockingIoActionQuote {
    pub(crate) const fn metadata_bytes(self) -> NonZeroU64 {
        self.metadata
    }
    pub(crate) const fn physical_queue_bytes(self) -> NonZeroU64 {
        self.physical_queue
    }
}
trait OriginalCall<T>: Send {
    fn run(&mut self, result: &ConnectorSourceCloseResult<'_>) -> Option<T>;
}
struct OriginalInput<I, T> {
    input: I,
    apply: fn(&mut I, &ConnectorSourceCloseResult<'_>) -> Option<T>,
}
impl<I: Send, T> OriginalCall<T> for OriginalInput<I, T> {
    fn run(&mut self, result: &ConnectorSourceCloseResult<'_>) -> Option<T> {
        (self.apply)(&mut self.input, result)
    }
}
type Call<T> = Box<dyn OriginalCall<T>>;
struct CallLease<'a, T: Send + 'static> {
    header: &'a Header<T>,
    call: Option<Call<T>>,
}
impl<T: Send + 'static> Drop for CallLease<'_, T> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.header.poisoned.store(true, Ordering::Release);
        }
        if self.call.is_some() {
            let mut original = self.header.call.lock().unwrap_or_else(|e| e.into_inner());
            assert!(original.is_none(), "original call lease receiver is empty");
            *original = self.call.take();
        }
    }
}
struct Header<T: Send + 'static> {
    call: Mutex<Option<Call<T>>>,
    value: Mutex<Option<T>>,
    source_close_failure: Mutex<Option<Arc<novarocks_spi::connector::ConnectorError>>>,
    callback_failure: Mutex<Option<novarocks_query_application::cpu::QueryBlockingFailure>>,
    source_drop_unproven: AtomicBool,
    pins: Box<[ConnectorBlockingIoJoinPin]>,
    poisoned: AtomicBool,
}
fn make_work<T: Send + 'static>(header: Arc<Header<T>>) -> impl FnOnce() + Send + 'static {
    move || {
        assert!(
            !header.poisoned.load(Ordering::Acquire),
            "unwound original call is never retried"
        );
        let call = header
            .call
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
            .expect("original Connector call runs once");
        let mut lease = CallLease {
            header: &header,
            call: Some(call),
        };
        // An already prepared original call retains its exact source. Stop
        // prevents the serial owner from making a successor; it never drops
        // this input or manufactures a result/actual-exit acknowledgement.
        let value = lease
            .call
            .as_mut()
            .unwrap()
            .run(&ConnectorSourceCloseResult {
                failure: &header.source_close_failure,
                worker_failure: Some(&header.callback_failure),
                source_drop_unproven: Some(&header.source_drop_unproven),
            });
        if let Some(value) = value {
            let previous = header
                .value
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .replace(value);
            assert!(previous.is_none(), "one original output receiver");
            drop(previous);
        }
        // Successful output/typed close cause precedes every input destructor.
        // An apply panic instead restores the same Box/input to the same header.
        drop(lease.call.take());
    }
}
#[expect(
    clippy::large_enum_variant,
    reason = "Dispatch stays inline in the original ConnectorOperationJob slot; its blocking quote includes that complete row, avoiding a second allocation for the call phase."
)]
enum Dispatch {
    Cpu {
        prepared: Option<QueryCpuPreparedJob>,
        receipt: Option<QueryCpuReceipt<()>>,
    },
    Blocking {
        source: Option<BlockingActionSource>,
        admission: Option<BlockingActionAdmission>,
        grant: Option<JointLocalGrant<QueryBlockingReservation>>,
        prepared: Option<QueryBlockingPreparedJob>,
        receipt: Option<QueryBlockingReceipt<()>>,
        quote: ConnectorBlockingIoActionQuote,
    },
}
/// Kept directly in the original initializer/pump. F wait cancellation borrows
/// it; the only original input is in Header.call until actual worker entry.
pub(crate) struct ConnectorOperationJob<T: Send + 'static> {
    header: Option<Arc<Header<T>>>,
    supervisor: ConnectorBlockingIoSupervisor,
    dispatch: Dispatch,
    published: bool,
    result_observed: bool,
    actual_exited: bool,
    failure: Option<ConnectorBlockingIoError>,
    secondary: [Option<ConnectorBlockingIoError>; 2],
}
impl ConnectorBlockingIoSupervisor {
    /// Production uses a closed function item borrowing its same original
    /// input Box. The sole assignment/source never moves into a FnOnce F.
    pub(crate) fn prepare_cpu_owned<I, T>(
        &self,
        pins: Vec<ConnectorBlockingIoJoinPin>,
        input: I,
        apply: fn(&mut I, &ConnectorSourceCloseResult<'_>) -> Option<T>,
    ) -> ConnectorOperationJob<T>
    where
        I: Send + 'static,
        T: Send + 'static,
    {
        self.prepare_operation(pins, Box::new(OriginalInput { input, apply }), None)
    }
    pub(crate) fn prepare_cpu_retirement<I, T>(
        &self,
        pins: Vec<ConnectorBlockingIoJoinPin>,
        input: I,
        apply: fn(&mut I, &ConnectorSourceCloseResult<'_>) -> Option<T>,
    ) -> ConnectorOperationJob<T>
    where
        I: Send + 'static,
        T: Send + 'static,
    {
        self.prepare_operation(pins, Box::new(OriginalInput { input, apply }), None)
    }
    /// Same original responsibility is both the full-D source and the pin;
    /// an unrelated source/pin vector cannot be paired with this job.
    pub(crate) fn prepare_blocking_owned_in<I, T>(
        &self,
        responsibility: &ConnectorBlockingIoResponsibility,
        input: &mut Option<I>,
        apply: fn(&mut I, &ConnectorSourceCloseResult<'_>) -> Option<T>,
        destination: &mut Option<ConnectorOperationJob<T>>,
    ) -> Result<(), WorkError>
    where
        I: Send + 'static,
        T: Send + 'static,
    {
        self.prepare_genuine_in(
            responsibility,
            input,
            apply,
            destination,
            novarocks_workload_control::joint_admission::JointLane::Foreground,
        )
    }
    fn prepare_genuine_in<I, T>(
        &self,
        responsibility: &ConnectorBlockingIoResponsibility,
        input: &mut Option<I>,
        apply: fn(&mut I, &ConnectorSourceCloseResult<'_>) -> Option<T>,
        destination: &mut Option<ConnectorOperationJob<T>>,
        lane: novarocks_workload_control::joint_admission::JointLane,
    ) -> Result<(), WorkError>
    where
        I: Send + 'static,
        T: Send + 'static,
    {
        if input.is_none() || destination.is_some() {
            return Err(WorkError::Conflict);
        }
        if self.executors.get().is_none() {
            return Err(WorkError::NotReady);
        }
        let source = responsibility
            .0
            .owner
            .as_ref()
            .ok_or(WorkError::Released)?
            .blocking_action_source(lane)?;
        let quote = Self::blocking_action_quote_for_input::<I, T>()?;
        let pins = vec![responsibility.join_pin()];
        let original = input.take().unwrap();
        *destination = Some(self.prepare_operation(
            pins,
            Box::new(OriginalInput {
                input: original,
                apply,
            }),
            Some((source, quote)),
        ));
        Ok(())
    }
    fn prepare_operation<T: Send + 'static>(
        &self,
        pins: Vec<ConnectorBlockingIoJoinPin>,
        call: Call<T>,
        blocking: Option<(BlockingActionSource, ConnectorBlockingIoActionQuote)>,
    ) -> ConnectorOperationJob<T> {
        ConnectorOperationJob {
            header: Some(Arc::new(Header {
                call: Mutex::new(Some(call)),
                value: Mutex::new(None),
                source_close_failure: Mutex::new(None),
                callback_failure: Mutex::new(None),
                source_drop_unproven: AtomicBool::new(false),
                pins: pins.into_boxed_slice(),
                poisoned: AtomicBool::new(false),
            })),
            supervisor: self.clone(),
            dispatch: match blocking {
                Some((source, quote)) => Dispatch::Blocking {
                    source: Some(source),
                    admission: None,
                    grant: None,
                    prepared: None,
                    receipt: None,
                    quote,
                },
                None => Dispatch::Cpu {
                    prepared: None,
                    receipt: None,
                },
            },
            published: false,
            result_observed: false,
            actual_exited: false,
            failure: None,
            secondary: [None, None],
        }
    }
    /// Same original call factory. Arbitrary input/result pointees, platform
    /// channel/Mutex backing and Wakers remain their actual creators' obligation.
    pub(crate) fn blocking_action_quote_for_input<I, T>()
    -> Result<ConnectorBlockingIoActionQuote, WorkError>
    where
        I: Send + 'static,
        T: Send + 'static,
    {
        fn call_layout<I, O>(_: fn(I) -> O) -> std::alloc::Layout {
            std::alloc::Layout::new::<O>()
        }
        fn quote_for_header<T: Send + 'static, F: FnOnce() + Send + 'static>(
            _: fn(Arc<Header<T>>) -> F,
        ) -> std::alloc::Layout {
            QueryBlockingExecutor::preserving_call_layout::<(), F>()
        }
        let queue = QueryBlockingExecutor::queue_geometry_for_composition()
            .map_err(|_| WorkError::ArithmeticOverflow)?;
        let worker = quote_for_header(make_work::<T>);
        let original_call = std::alloc::Layout::new::<OriginalInput<I, T>>();
        let header = novarocks_type_contract::owned_resources::layout::arc_layout(
            std::alloc::Layout::new::<Header<T>>(),
        )
        .map_err(|_| WorkError::ArithmeticOverflow)?;
        let metadata = [
            header.size(),
            std::mem::size_of::<ConnectorOperationJob<T>>(),
            original_call.size(),
            queue.actual_exit_header.size(),
        ]
        .into_iter()
        .try_fold(0usize, |sum, n| sum.checked_add(n))
        .ok_or(WorkError::ArithmeticOverflow)?;
        let physical = queue
            .queue_record_inline
            .size()
            .checked_add(worker.size())
            .ok_or(WorkError::ArithmeticOverflow)?;
        let nz = |n: usize| {
            u64::try_from(n)
                .ok()
                .and_then(NonZeroU64::new)
                .ok_or(WorkError::ArithmeticOverflow)
        };
        // Keep the same function item instantiated above; no fake unit call is constructed.
        let _ = call_layout(make_work::<T>);
        Ok(ConnectorBlockingIoActionQuote {
            metadata: nz(metadata)?,
            physical_queue: nz(physical)?,
        })
    }
}
impl<T: Send + 'static> ConnectorOperationJob<T> {
    fn header(&self) -> &Arc<Header<T>> {
        self.header.as_ref().expect("original job header retained")
    }
    fn error(&self, cause: OriginalFailureCause, detail: &'static str) -> ConnectorBlockingIoError {
        ConnectorBlockingIoError {
            detail: detail.to_owned(),
            original: Arc::new(OriginalFailure {
                cause,
                _backing: self
                    .header()
                    .pins
                    .iter()
                    .map(|pin| ConnectorBlockingIoBacking(Arc::clone(&pin.cell)))
                    .collect(),
            }),
        }
    }
    fn remember_worker_failure(
        &mut self,
        cause: novarocks_query_application::cpu::QueryBlockingFailure,
    ) {
        let same = |error: &ConnectorBlockingIoError| match &error.original.cause {
            OriginalFailureCause::Preserving(old) => old.shares_original_with(&cause),
            _ => false,
        };
        if self.failure.as_ref().is_some_and(same) || self.secondary.iter().flatten().any(same) {
            return;
        }
        let error = self.error(
            OriginalFailureCause::Preserving(cause),
            "original Connector worker failed",
        );
        if self.failure.is_none() {
            self.failure = Some(error);
        } else if let Some(slot) = self.secondary.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(error);
        } else {
            std::mem::forget(error);
            panic!("three original Connector failure cuts exceeded");
        }
    }
    pub(crate) fn source_close_failure(
        &self,
    ) -> Option<Arc<novarocks_spi::connector::ConnectorError>> {
        self.header()
            .source_close_failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    /// No State/header lock crosses either QA poll, callback or notification.
    pub(crate) fn poll_progress(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), ConnectorBlockingIoError>> {
        if !self.published {
            if let Some(error) = &self.failure {
                return Poll::Ready(Err(error.clone()));
            }
            let Some(executors) = self.supervisor.executors.get().cloned() else {
                let error = self.error(
                    OriginalFailureCause::Admission(WorkError::NotReady),
                    "original Connector executors are not installed",
                );
                self.failure = Some(error.clone());
                return Poll::Ready(Err(error));
            };
            let header = Arc::clone(self.header());
            let result: Result<bool, OriginalFailureCause> = (|| match &mut self.dispatch {
                Dispatch::Cpu { prepared, receipt } => {
                    if prepared.is_none() {
                        *prepared = Some(
                            executors
                                .cpu
                                .prepare_cpu_preserving_in(
                                    make_work(Arc::clone(&header)),
                                    &header,
                                    receipt,
                                )
                                .map_err(|_| {
                                    OriginalFailureCause::Admission(WorkError::Conflict)
                                })?,
                        );
                    }
                    executors
                        .cpu
                        .publish_cpu_preserving_in(prepared)
                        .map_err(|refusal| OriginalFailureCause::CpuPublication {
                            _refusal: refusal,
                        })?;
                    self.published = true;
                    Ok(true)
                }
                Dispatch::Blocking {
                    source,
                    admission,
                    grant,
                    prepared,
                    receipt,
                    quote,
                } => {
                    if admission.is_none() {
                        *admission = Some(
                            source
                                .take()
                                .expect("same blocking source")
                                .begin(quote.metadata_bytes(), quote.physical_queue_bytes())
                                .map_err(|e| OriginalFailureCause::Admission(e.error))?,
                        );
                    }
                    if grant.is_none() {
                        let mut offer = match admission.as_mut().unwrap().poll_local_offer(cx) {
                            Poll::Pending => return Ok(false),
                            Poll::Ready(Err(e)) => {
                                return Err(OriginalFailureCause::Admission(e.error));
                            }
                            Poll::Ready(Ok(offer)) => offer,
                        };
                        let mut raw=match executors.blocking.try_reserve(){
                            Ok(raw)=>Some(raw),Err(novarocks_query_application::cpu::QueryBlockingReserveRefusal::Full)=> {
                                let mut absent:Option<QueryBlockingReservation>=None;
                                offer.reobserve_after_drop(&mut absent).map_err(OriginalFailureCause::Admission)?.notify();
                                return Ok(false);
                            }
                            Err(novarocks_query_application::cpu::QueryBlockingReserveRefusal::Closed)=>return Err(OriginalFailureCause::Admission(WorkError::Closed)),
                            Err(other)=>return Err(OriginalFailureCause::BlockingReserve { _refusal: other }),
                        };
                        let notification = match offer.accept_in(&mut raw, grant) {
                            Ok(notification) => notification,
                            Err(error) => {
                                offer
                                    .reobserve_after_drop(&mut raw)
                                    .map_err(OriginalFailureCause::Admission)?
                                    .notify();
                                return Err(OriginalFailureCause::Admission(error));
                            }
                        };
                        // The original accepted guard is already stable before wake.
                        notification.notify();
                    }
                    if prepared.is_none() {
                        *prepared = Some(
                            executors
                                .blocking
                                .prepare_preserving_in(
                                    grant.as_mut().unwrap(),
                                    make_work(Arc::clone(&header)),
                                    receipt,
                                )
                                .map_err(|refusal| OriginalFailureCause::BlockingPublication {
                                    _refusal: refusal.reason(),
                                })?,
                        );
                    }
                    let notification = executors
                        .blocking
                        .publish_joint_in(grant, prepared)
                        .map_err(|refusal| OriginalFailureCause::BlockingPublication {
                            _refusal: refusal,
                        })?;
                    self.published = true;
                    notification.notify();
                    Ok(true)
                }
            })();
            match result {
                Ok(false) => return Poll::Pending,
                Ok(true) => {}
                Err(cause) => {
                    if self.failure.is_none() {
                        self.failure = Some(
                            self.error(cause, "original Connector admission/publication failed"),
                        );
                    }
                    return Poll::Ready(Err(self.failure.as_ref().unwrap().clone()));
                }
            }
        }
        let callback_failure = self
            .header()
            .callback_failure
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(error) = callback_failure {
            self.remember_worker_failure(error);
        }
        if !self.result_observed {
            let result = match &mut self.dispatch {
                Dispatch::Cpu { receipt, .. } => receipt.as_mut().unwrap().poll_result(cx),
                Dispatch::Blocking { receipt, .. } => receipt.as_mut().unwrap().poll_result(cx),
            };
            if let Poll::Ready(result) = result {
                self.result_observed = true;
                if let Err(error) = result {
                    self.remember_worker_failure(error);
                }
            }
        }
        let (exit, failure) = match &self.dispatch {
            Dispatch::Cpu { receipt, .. } => {
                let receipt = receipt.as_ref().unwrap();
                (receipt.poll_actual_exit(cx), receipt.actual_exit_failure())
            }
            Dispatch::Blocking { receipt, .. } => {
                let receipt = receipt.as_ref().unwrap();
                (receipt.poll_actual_exit(cx), receipt.actual_exit_failure())
            }
        };
        if let Some(error) = failure {
            self.remember_worker_failure(error);
        }
        match exit {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(_)) => {
                if self.failure.is_none() {
                    self.remember_worker_failure(
                        novarocks_query_application::cpu::QueryBlockingFailure::ExecutorClosed,
                    );
                }
                Poll::Ready(Err(self.failure.as_ref().unwrap().clone()))
            }
            Poll::Ready(Ok(())) => {
                self.actual_exited = true;
                for pin in &self.header().pins {
                    pin.joined.store(true, Ordering::Release);
                }
                match &self.failure {
                    Some(error) => Poll::Ready(Err(error.clone())),
                    None => Poll::Ready(Ok(())),
                }
            }
        }
    }
    /// A result and a later notification/cleanup failure may coexist. The
    /// original typed consumer moves the value to its own stable late slot;
    /// this never converts the failure into success or releases an input.
    pub(crate) fn take_completed_value_in(&mut self, destination: &mut Option<T>) -> bool {
        if !self.actual_exited || destination.is_some() {
            return false;
        }
        let value = self
            .header()
            .value
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(value) = value {
            *destination = Some(value);
            true
        } else {
            false
        }
    }
    pub(crate) fn try_take(&mut self) -> Option<Result<T, ConnectorBlockingIoError>> {
        if !self.actual_exited {
            return None;
        }
        if let Some(error) = &self.failure {
            return Some(Err(error.clone()));
        }
        let value = self
            .header()
            .value
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()?;
        Some(Ok(value))
    }
    pub(crate) async fn finish_in(
        &mut self,
        destination: &mut Option<Result<T, ConnectorBlockingIoError>>,
    ) {
        assert!(
            destination.is_none(),
            "one original stable Connector output"
        );
        let result = std::future::poll_fn(|cx| self.poll_progress(cx)).await;
        if let Err(error) = result {
            *destination = Some(Err(error));
            return;
        }
        *destination = self.try_take();
        assert!(
            destination.is_some(),
            "business call returned its original output"
        );
    }
    pub(crate) fn source_drop_unproven(&self) -> bool {
        self.header().source_drop_unproven.load(Ordering::Acquire)
    }
    pub(crate) fn input_or_value_retained(&self) -> bool {
        self.header()
            .call
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some()
            || self
                .header()
                .value
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_some()
    }
    pub(crate) fn actual_exit_observed(&self) -> bool {
        self.actual_exited
    }
}
impl<T: Send + 'static> Drop for ConnectorOperationJob<T> {
    fn drop(&mut self) {
        let Some(header) = self.header.take() else {
            return;
        };
        let empty = header
            .value
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_none();
        let call_retained = header
            .call
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some();
        if !self.actual_exited || !empty || call_retained {
            // Invalid whole-owner Drop is not shutdown. Keep this same charged
            // original source/pins rather than schedule detached retirement or
            // let ordinary field cleanup forge physical/source exit.
            std::mem::forget(header);
        }
    }
}

#[cfg(test)]
mod tests;

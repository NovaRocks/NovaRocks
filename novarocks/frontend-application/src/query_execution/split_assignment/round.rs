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

//! The split-assignment resource one execution round owns.
//!
//! It exists for exactly one round: aborting the round closes every split
//! source and sender, and a replacement round builds a new one under a new
//! attempt id rather than resuming this. Nothing here survives a round, which
//! is what keeps a replaced attempt from inheriting a sequence space.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use novarocks_proto_codec::lifecycle::QueryExecutionId;
use novarocks_spi::connector::ConnectorReadWireEncoder;
use novarocks_spi::connector::read_stack::{ConnectorReadColumnHandle, ConnectorReadSplitSource};
use novarocks_spi::connector::read_stack::{ConnectorSplitBatch, SplitSourceProfile};

use super::driver::{
    AssignmentTarget, SplitAssignmentDriver, SplitAssignmentDriverError, SplitAssignmentStop,
};
use super::transport::TaskUpdateTransport;
use crate::runtime_filter::feedback::RuntimeFilterFeedbackState;
use crate::task_protocol_config::TaskUpdateRetryPolicy;

/// How many splits one batch pulls from a source.
///
/// Bounded well below the wire limit so a slow task's queue drains between
/// batches instead of filling in one shot.
pub(crate) const DEFAULT_PUMP_BATCH_SIZE: usize = 256;

/// How long the pump waits when no source could make progress.
const IDLE_PUMP_BACKOFF: std::time::Duration = std::time::Duration::from_millis(2);
#[cfg(test)]
pub(crate) const DEFAULT_INITIAL_DYNAMIC_FILTER_WAIT_CAP: Duration = Duration::from_secs(1);

/// One typed scan's split source, with the scan it feeds.
pub(crate) struct RoundSplitSource {
    pub(crate) scan: super::ScanNodeKey,
    pub(crate) source: super::SupervisedSplitSource,
    pub(crate) encoder: Arc<dyn ConnectorReadWireEncoder>,
    /// FE admission state is query-attempt local and shared only with this
    /// attempt's control readers.  The source observes it afresh for every
    /// batch; already emitted splits are never revisited.
    pub(crate) feedback: Arc<RuntimeFilterFeedbackState>,
    /// Exact carrier binding -> opaque connector column mapping frozen by the
    /// prepared scan.  A feedback channel can constrain no other column.
    pub(crate) feedback_bindings: Vec<(u32, ConnectorReadColumnHandle)>,
    pub(crate) initial_wait_initialized: bool,
    pub(crate) initial_wait_deadline: Option<Instant>,
    // All enclosing source backings retire before this fixed original alias.
    pub(crate) _backing: Option<crate::task_execution::blocking_io::ConnectorBlockingIoBacking>,
}

/// The per-round owner of every split source and the driver that drains them.
pub(crate) struct RoundSplitAssignment {
    driver: SplitAssignmentDriver,
    sources: Vec<Option<RoundSplitSource>>,
    stop: SplitAssignmentStop,
    /// Closing owns source cleanup. It is deliberately independent from
    /// `stop`: the coordinator can signal stop before the serial pump reaches
    /// cleanup, and that pump must still release every source afterwards.
    closed: bool,
    /// Next source considered for one bounded enumeration operation.
    next_source: usize,
    started_at: Instant,
    initial_dynamic_filter_wait_cap: Duration,
}

/// What one admitted synchronous split-source operation produced.
pub(crate) enum RoundSplitEnumeration {
    Ready(Box<RoundSplitEnumerationRequest>),
    /// Every remaining source is temporarily blocked by feedback or task
    /// backpressure. The owner parks outside Connector admission.
    Idle(Duration),
    Finished,
}

/// One source moved into an ordinary blocking job.
pub(crate) struct RoundSplitEnumerationRequest {
    slot: usize,
    dynamic_filter:
        Option<novarocks_spi::connector::read_stack::ConnectorReadDynamicFilterSnapshot>,
    started_at: Instant,
    initial_dynamic_filter_wait_cap: Duration,
    source: RoundSplitSource,
}

impl RoundSplitEnumerationRequest {
    pub(crate) fn join_pin(
        &self,
    ) -> Option<crate::task_execution::blocking_io::ConnectorBlockingIoJoinPin> {
        self.source.source.join_pin()
    }
}

/// One completed Connector call, including the source the serial owner must
/// either adopt or close through supervised work.
pub(crate) struct RoundSplitEnumerationResult {
    pub(crate) slot: usize,
    pub(crate) batch: Result<
        Option<ConnectorSplitBatch<novarocks_spi::connector::read_stack::ConnectorReadSplit>>,
        SplitAssignmentDriverError,
    >,
    pub(crate) source: RoundSplitSource,
}

impl RoundSplitAssignment {
    pub(crate) fn join_pins(
        &self,
    ) -> Vec<crate::task_execution::blocking_io::ConnectorBlockingIoJoinPin> {
        self.sources
            .iter()
            .flatten()
            .filter_map(|source| source.source.join_pin())
            .collect()
    }
    pub(crate) fn new(
        execution_id: QueryExecutionId,
        transport: Arc<dyn TaskUpdateTransport>,
        tasks: BTreeMap<super::ScanNodeKey, Vec<AssignmentTarget>>,
        max_queued_splits_per_task: u64,
        sources: Vec<RoundSplitSource>,
        retry_policy: TaskUpdateRetryPolicy,
        initial_dynamic_filter_wait_cap: Duration,
    ) -> Self {
        let stop = SplitAssignmentStop::default();
        Self {
            driver: SplitAssignmentDriver::new(
                execution_id,
                transport,
                tasks,
                max_queued_splits_per_task,
                sources
                    .iter()
                    .map(|source| (source.scan, Arc::clone(&source.encoder)))
                    .collect(),
                retry_policy,
                stop.clone(),
            ),
            sources: sources.into_iter().map(Some).collect(),
            stop,
            closed: false,
            next_source: 0,
            started_at: Instant::now(),
            initial_dynamic_filter_wait_cap,
        }
    }

    /// A handle that closes this round's assignment from another thread.
    ///
    /// Cancellation reaches the coordinator on the statement thread while the
    /// pump may be blocked in a batch request, so the stop signal has to be
    /// observable without holding the round.
    pub(crate) fn stop_handle(&self) -> SplitAssignmentStop {
        self.stop.clone()
    }

    /// Runs at most one synchronous Connector enumeration operation.
    ///
    /// The process supervisor calls this under one ordinary permit. It never
    /// sends a TaskUpdate or waits for an acknowledgement; the serial owner
    /// adopts the returned batch first and starts transport work separately.
    pub(crate) fn enumerate_once(
        &mut self,
    ) -> Result<RoundSplitEnumeration, SplitAssignmentDriverError> {
        if self.closed || self.stop.is_stopped() {
            return Err(SplitAssignmentDriverError::Closed);
        }
        let mut pending = false;
        let mut feedback_wait_deadline = None;
        let source_count = self.sources.len();
        let mut selected = None;
        for offset in 0..source_count {
            let index = (self.next_source + offset) % source_count;
            let Some(source) = self.sources[index].as_ref() else {
                continue;
            };
            let scan = source.scan;
            if self.driver.is_terminal_for(scan) {
                continue;
            }
            pending = true;
            if self.driver.is_backpressured(scan) {
                continue;
            }
            let source = self.sources[index]
                .as_mut()
                .expect("the selected source remains owned by the round");
            if !source.initial_wait_initialized {
                selected = Some((index, None));
                break;
            }
            if let Some(deadline) = source.initial_wait_deadline {
                if Instant::now() < deadline
                    && source.feedback.is_initial_wait_blocked(
                        scan.plan_node_id(),
                        source
                            .feedback_bindings
                            .iter()
                            .map(|(binding_id, _)| *binding_id),
                    )
                {
                    feedback_wait_deadline = Some(
                        feedback_wait_deadline
                            .map_or(deadline, |current: Instant| current.min(deadline)),
                    );
                    continue;
                }
                source.initial_wait_deadline = None;
            }
            let dynamic_filter = source
                .feedback
                .snapshot_for_scan(scan.plan_node_id(), &source.feedback_bindings);
            selected = Some((index, Some(dynamic_filter)));
            break;
        }

        let Some((index, dynamic_filter)) = selected else {
            if !pending {
                return Ok(RoundSplitEnumeration::Finished);
            }
            let wait = feedback_wait_deadline.map_or(IDLE_PUMP_BACKOFF, |deadline| {
                deadline
                    .saturating_duration_since(Instant::now())
                    .min(IDLE_PUMP_BACKOFF)
            });
            return Ok(RoundSplitEnumeration::Idle(wait));
        };
        self.next_source = (index + 1) % source_count.max(1);
        let source = self.sources[index]
            .take()
            .expect("the selected source remains owned by the round");
        Ok(RoundSplitEnumeration::Ready(Box::new(
            RoundSplitEnumerationRequest {
                slot: index,
                source,
                dynamic_filter,
                started_at: self.started_at,
                initial_dynamic_filter_wait_cap: self.initial_dynamic_filter_wait_cap,
            },
        )))
    }

    /// Performs the only Connector call in one ordinary blocking job.
    #[cfg(test)]
    pub(crate) fn enumerate_source(
        request: RoundSplitEnumerationRequest,
    ) -> RoundSplitEnumerationResult {
        Self::enumerate_source_inner(request, None)
    }
    pub(crate) fn enumerate_source_retained(
        request: RoundSplitEnumerationRequest,
        retained: &crate::task_execution::blocking_io::ConnectorSourceCloseResult<'_>,
    ) -> RoundSplitEnumerationResult {
        Self::enumerate_source_inner(request, Some(retained))
    }
    fn enumerate_source_inner(
        mut request: RoundSplitEnumerationRequest,
        retained: Option<&crate::task_execution::blocking_io::ConnectorSourceCloseResult<'_>>,
    ) -> RoundSplitEnumerationResult {
        use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
        use novarocks_query_application::execution_error::QueryFailureReason;
        let scan = request.source.scan;
        let batch = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let Some(dynamic_filter) = request.dynamic_filter.as_ref() else {
                let requested = request
                    .source
                    .source
                    .initial_dynamic_filter_wait_request()
                    .min(request.initial_dynamic_filter_wait_cap);
                request.source.initial_wait_initialized = true;
                request.source.initial_wait_deadline =
                    (!requested.is_zero()).then(|| request.started_at + requested);
                return Ok(None);
            };
            request
                .source
                .source
                .next_batch(DEFAULT_PUMP_BATCH_SIZE, dynamic_filter)
                .map(Some)
        }))
        .map_err(|original| {
            if let Some(retained) = retained {
                retained.record_panic(original);
            } else {
                drop(original);
            }
            SplitAssignmentDriverError::SourceEnumeration {
                scan,
                fact: QueryFailureReason::SplitAssignmentFailed.failure_fact(),
                detail: SafeDetail::truncating(
                    "the connector split source panicked while enumerating",
                ),
            }
        })
        .and_then(|batch| {
            batch.map_err(|error| SplitAssignmentDriverError::SourceEnumeration {
                scan,
                fact: error.failure_fact(),
                detail: SafeDetail::truncating("enumerating the connector split source failed"),
            })
        });
        RoundSplitEnumerationResult {
            slot: request.slot,
            source: request.source,
            batch,
        }
    }

    /// Restores the source before the batch can change driver state.
    pub(crate) fn adopt_enumeration(
        &mut self,
        result: RoundSplitEnumerationResult,
    ) -> Result<
        Option<(
            super::ScanNodeKey,
            ConnectorSplitBatch<novarocks_spi::connector::read_stack::ConnectorReadSplit>,
        )>,
        SplitAssignmentDriverError,
    > {
        let scan = result.source.scan;
        let slot = self
            .sources
            .get_mut(result.slot)
            .expect("an enumerated source retains its exact round slot");
        assert!(
            slot.is_none(),
            "an enumerated source can be adopted only once"
        );
        *slot = Some(result.source);
        result.batch.map(|batch| batch.map(|batch| (scan, batch)))
    }

    /// Applies one already-enumerated batch and performs its TaskUpdate waits.
    ///
    /// This method must run outside Connector ordinary admission.
    pub(crate) fn deliver(
        &mut self,
        scan: super::ScanNodeKey,
        batch: ConnectorSplitBatch<novarocks_spi::connector::read_stack::ConnectorReadSplit>,
    ) -> Result<bool, SplitAssignmentDriverError> {
        let no_more_splits = batch.no_more_splits();
        let splits = batch
            .into_splits()
            .into_iter()
            .map(super::super::connector_domain::Split::new)
            .collect::<Vec<_>>();
        let has_work = !splits.is_empty();
        if !has_work && !no_more_splits {
            return Ok(false);
        }
        let placement = self.driver.distribute(scan, splits)?;
        self.driver
            .start_delivery(scan, placement, no_more_splits)?;
        Ok(true)
    }

    pub(crate) fn drive_delivery(&mut self) -> Result<bool, SplitAssignmentDriverError> {
        self.driver.drive_delivery()
    }

    pub(crate) fn next_delivery_wake_at(&self) -> Option<Instant> {
        self.driver.next_delivery_wake_at()
    }

    pub(crate) fn delivery_in_progress(&self) -> bool {
        self.driver.delivery_in_progress()
    }

    /// Close every source once, isolating each provider hook. Normal domain
    /// errors are facts; only the outer original job proves physical exit.
    fn close_sources(
        &mut self,
        collect_profile: bool,
        retained: Option<&crate::task_execution::blocking_io::ConnectorSourceCloseResult<'_>>,
    ) -> Result<SplitSourceProfile, SplitAssignmentDriverError> {
        use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
        if self.closed {
            return Ok(SplitSourceProfile::default());
        }
        self.closed = true;
        self.stop.stop();
        self.driver.close();
        let mut total = SplitSourceProfile::default();
        let mut first_error = None;
        for entry in self.sources.iter_mut().flatten() {
            if collect_profile {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    entry.source.profile_snapshot()
                })) {
                    Ok(profile) => {
                        total.files_considered = total
                            .files_considered
                            .saturating_add(profile.files_considered);
                        total.files_pruned =
                            total.files_pruned.saturating_add(profile.files_pruned);
                        total.files_expanded =
                            total.files_expanded.saturating_add(profile.files_expanded);
                        total.splits_emitted =
                            total.splits_emitted.saturating_add(profile.splits_emitted);
                    }
                    Err(payload) => {
                        first_error.get_or_insert_with(|| {
                            source_close_panic(entry.scan, SourceClosePanic::Profile)
                        });
                        // This runs on the same original blocking job. A real
                        // payload destructor panic escapes to its existing
                        // supervisor and cannot mint the normal join proof.
                        if let Some(retained) = retained {
                            retained.record_panic(payload);
                        } else {
                            drop(payload);
                        }
                    }
                }
            }
            // SupervisedSplitSource sets its once-close flag before entering
            // the provider. Its Drop will never rerun this interrupted hook.
            match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| entry.source.close())) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    first_error.get_or_insert_with(|| SplitAssignmentDriverError::SourceClose {
                        scan: entry.scan,
                        fact: error.failure_fact(),
                        detail: SafeDetail::truncating("closing the connector split source failed"),
                    });
                    if let Some(retained) = retained {
                        retained.record_error(error);
                    } else {
                        drop(error);
                    }
                }
                Err(payload) => {
                    first_error.get_or_insert_with(|| {
                        source_close_panic(entry.scan, SourceClosePanic::Close)
                    });
                    if let Some(retained) = retained {
                        retained.record_panic(payload);
                    } else {
                        drop(payload);
                    }
                }
            }
        }
        if let Some(retained) = retained {
            for entry in self.sources.iter_mut().flatten() {
                if let Err(error) = entry.source.retire_in(retained) {
                    retained.record_error(error);
                }
            }
        }
        first_error.map_or(Ok(total), Err)
    }

    /// Consumes the original assignment so its actual sources, transport and
    /// captures have all retired before this original blocking job returns.
    #[cfg(test)]
    pub(crate) fn close_with_profile(
        mut self,
    ) -> Result<SplitSourceProfile, SplitAssignmentDriverError> {
        let failure = std::sync::Mutex::new(None);
        let panic = std::sync::Mutex::new(None);
        let unproven = std::sync::atomic::AtomicBool::new(false);
        let retained = crate::task_execution::blocking_io::ConnectorSourceCloseResult::for_fixture(
            &failure, &panic, &unproven,
        );
        let result = self.close_sources(true, Some(&retained));
        drop(self);
        result
    }

    pub(crate) fn close_with_profile_in(
        &mut self,
        retained: &crate::task_execution::blocking_io::ConnectorSourceCloseResult<'_>,
    ) -> Result<SplitSourceProfile, SplitAssignmentDriverError> {
        self.close_sources(true, Some(retained))
    }

    /// Idempotent fallback for owners that have not reached serial close.
    #[cfg(test)]
    pub(crate) fn close(&mut self) {
        let failure = std::sync::Mutex::new(None);
        let panic = std::sync::Mutex::new(None);
        let unproven = std::sync::atomic::AtomicBool::new(false);
        let retained = crate::task_execution::blocking_io::ConnectorSourceCloseResult::for_fixture(
            &failure, &panic, &unproven,
        );
        if let Err(error) = self.close_sources(false, Some(&retained)) {
            tracing::warn!(error = %error, "closing the original split assignment failed");
        }
    }
}

impl Drop for RoundSplitAssignment {
    fn drop(&mut self) {
        // Actual close is borrowed by the original pump/initializer. View Drop
        // only stops production; it never invokes Provider callbacks or jobs.
        self.stop.stop();
    }
}

pub(crate) type RoundSplitAssignmentStop = SplitAssignmentStop;

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use novarocks_spi::connector::ConnectorError;
    use novarocks_spi::connector::read_stack::{
        ConnectorReadDynamicFilterSnapshot, ConnectorReadSplit, ConnectorReadSplitSource,
        ConnectorSplitBatch,
    };
    use novarocks_types::{AttemptId, QueryId};

    use crate::query_execution::connector_domain::TaskUpdateRequest;
    use crate::query_execution::split_assignment::{
        TaskUpdateOutcome, TaskUpdateTicket, TaskUpdateTransportError,
    };

    use super::*;

    struct NeverSend;

    impl TaskUpdateTransport for NeverSend {
        fn begin(
            &self,
            _execution_id: QueryExecutionId,
            _target: &AssignmentTarget,
            _request: &TaskUpdateRequest,
        ) -> Result<TaskUpdateTicket, TaskUpdateTransportError> {
            panic!("closing a round must not send a task update")
        }

        fn poll(
            &self,
            _ticket: TaskUpdateTicket,
            _stop: &SplitAssignmentStop,
        ) -> Option<Result<TaskUpdateOutcome, TaskUpdateTransportError>> {
            panic!("closing a round must not poll a task update")
        }
    }

    struct CloseCountingSource {
        close_calls: Arc<AtomicUsize>,
    }

    impl ConnectorReadSplitSource for CloseCountingSource {
        fn next_batch(
            &mut self,
            _max_size: usize,
            _dynamic_filter: &ConnectorReadDynamicFilterSnapshot,
        ) -> Result<ConnectorSplitBatch<ConnectorReadSplit>, ConnectorError> {
            panic!("close lifecycle tests must not enumerate splits")
        }

        fn is_finished(&self) -> bool {
            false
        }

        fn close(&mut self) -> Result<(), ConnectorError> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    struct InertCodec;

    impl ConnectorReadWireEncoder for InertCodec {
        fn owner(&self) -> &str {
            "round-close-test"
        }

        fn encode_relation_payload(
            &self,
            _relation: &novarocks_spi::connector::read_stack::ConnectorReadRelation,
        ) -> Result<
            novarocks_spi::connector::ConnectorReadRelationPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("close lifecycle tests must not encode relations")
        }

        fn encode_column_payload(
            &self,
            _column: &novarocks_spi::connector::read_stack::ConnectorReadColumnHandle,
        ) -> Result<
            novarocks_spi::connector::ConnectorEncodedPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("close lifecycle tests must not encode columns")
        }

        fn encode_transaction_payload(
            &self,
            _transaction: &novarocks_spi::connector::read_stack::ConnectorReadTransactionHandle,
        ) -> Result<
            novarocks_spi::connector::ConnectorEncodedPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("close lifecycle tests must not encode transactions")
        }

        fn encode_split_payload(
            &self,
            _split: &ConnectorReadSplit,
        ) -> Result<
            novarocks_spi::connector::ConnectorReadSplitPayload,
            novarocks_spi::connector::ConnectorCodecError,
        > {
            unreachable!("close lifecycle tests must not encode splits")
        }
    }

    fn assignment(close_calls: Arc<AtomicUsize>) -> RoundSplitAssignment {
        let execution_id = QueryExecutionId::new(
            QueryId::new(9, 9),
            AttemptId::new(1).expect("attempt id must be valid"),
        )
        .expect("execution id must be valid");
        RoundSplitAssignment::new(
            execution_id,
            Arc::new(NeverSend),
            BTreeMap::new(),
            1,
            vec![RoundSplitSource {
                scan: crate::query_execution::split_assignment::ScanNodeKey::new(1u32, 7),
                source: super::super::SupervisedSplitSource::fixture(Box::new(
                    CloseCountingSource { close_calls },
                )),
                encoder: Arc::new(InertCodec),
                feedback: Arc::new(
                    RuntimeFilterFeedbackState::new(execution_id, Default::default())
                        .expect("empty feedback declaration"),
                ),
                feedback_bindings: Vec::new(),
                initial_wait_initialized: false,
                initial_wait_deadline: None,
                _backing: None,
            }],
            TaskUpdateRetryPolicy::default(),
            DEFAULT_INITIAL_DYNAMIC_FILTER_WAIT_CAP,
        )
    }

    #[test]
    fn stop_handle_is_visible_to_the_pump() {
        let stop = RoundSplitAssignmentStop::default();
        assert!(!stop.is_stopped());
        stop.stop();
        assert!(stop.is_stopped());
    }

    #[test]
    fn close_releases_sources_after_an_external_stop() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let mut assignment = assignment(Arc::clone(&close_calls));

        assignment.stop_handle().stop();
        assignment.close();

        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn close_releases_each_source_only_once() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let mut assignment = assignment(Arc::clone(&close_calls));

        assignment.close();
        assignment.close();

        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }
}

#[derive(Clone, Copy)]
enum SourceClosePanic {
    Profile,
    Close,
}
impl novarocks_execution_contract::failure::FailureProjection for SourceClosePanic {
    fn failure_fact(&self) -> novarocks_execution_contract::failure::FailureFact {
        use novarocks_execution_contract::failure::{
            FailureClass, FailureFact, FailureReason, domain,
        };
        FailureFact::new(
            FailureClass::Unexpected,
            FailureReason::new(
                domain::QUERY,
                std::num::NonZeroU16::new(match self {
                    Self::Profile => 180,
                    Self::Close => 181,
                })
                .unwrap(),
            ),
        )
    }
}
fn source_close_panic(
    scan: super::ScanNodeKey,
    cause: SourceClosePanic,
) -> SplitAssignmentDriverError {
    use novarocks_execution_contract::{SafeDetail, failure::FailureProjection};
    SplitAssignmentDriverError::SourceClose {
        scan,
        fact: cause.failure_fact(),
        detail: SafeDetail::truncating(match cause {
            SourceClosePanic::Profile => {
                "the connector split source profile hook panicked during close"
            }
            SourceClosePanic::Close => "the connector split source close hook panicked",
        }),
    }
}

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

//! Native projection of the one Node-owned covered observation R. The outer
//! Observation lane body owns its real stream permit through physical body
//! exit. Ordinary running ownership is setup-only and never retained here.

use crate::{
    native_ingress::NativeIngressOwnership, task_protocol::TaskStatusEventStream,
    worker_node_projection::encode_frame,
};
use novarocks_proto_codec::FieldPath;
use novarocks_proto_models::novarocks as proto;
use novarocks_task_codec::operation::{
    CoveredObservationBookmark, CoveredStatusStreamEvent, CoveredStatusStreamFact,
    NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES, decode_covered_subscribe_task_status,
    encode_covered_status_event,
};
use novarocks_worker::{
    NodeCoveredObservationBookmark, NodeCoveredObservationFrame, NodeCoveredSubscriptionError,
    NodeCoveredSubscriptionRequest, WorkerObservationError, WorkerObservationErrorKind,
    WorkerObservationStream, WorkerTaskControl,
};
use prost::Message;
use std::{
    future::Future,
    hash::{Hash, Hasher},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio_stream::Stream;
use tonic::Status;

const BOOKMARK_INTERVAL: Duration = Duration::from_secs(1);

pub(crate) fn subscribe_task_status(
    control: &WorkerTaskControl,
    request: proto::SubscribeTaskStatusRequest,
    deadline: Instant,
    ownership: Option<Arc<NativeIngressOwnership>>,
    #[cfg(novarocks_dev_controls)] test_controls: Option<
        Arc<crate::test_controls::NativeTestControls>,
    >,
) -> Result<TaskStatusEventStream, Status> {
    let decoded =
        decode_covered_subscribe_task_status(&request, FieldPath::root("subscribe_task_status"))
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
    let deadline = ownership
        .as_ref()
        .map_or(deadline, |owner| deadline.min(owner.deadline()));
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(Status::deadline_exceeded(
            "the original status subscription admission deadline elapsed",
        ));
    }
    let generation = decoded.generation.get();
    let participation = decoded.participation;
    let position = NodeCoveredSubscriptionRequest {
        status: decoded.status_cursors,
        convergence: decoded.task_convergence_cursors,
        participation: decoded.participation_cursor,
        quiesce: decoded.quiesce_cursor.map(|cursor| {
            novarocks_worker::NodeQuiesceObservationCursor {
                participation: cursor.participation,
                fence_version: cursor.fence_version,
            }
        }),
        required: decoded.required_identities,
    };
    // Observation RPCs carry no wire operation id. This local correlation id
    // is not a Work, source, capacity or replay authority.
    let stream = control
        .begin_status_subscription(
            participation,
            novarocks_execution_contract::TaskOperationId::new_v7(),
            control.now().saturating_add(remaining),
            generation,
            position,
        )
        .map_err(crate::worker_node_ingress::request_status)?;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    participation.hash(&mut hasher);
    let stagger = Duration::from_millis(hasher.finish() % 750);
    let first = tokio::time::Instant::now() + Duration::from_millis(250) + stagger;
    // The real outer Observation lane stream guard remains with the HTTP
    // response body. Holding the ordinary setup gate here would starve Task
    // operations behind long-lived status streams.
    drop(ownership);
    Ok(Box::pin(NodeObservationStream {
        stream,
        generation,
        bookmark_timer: Box::pin(tokio::time::sleep_until(first)),
        catch_up_complete: false,
        ended: false,
        participation,
        diagnostic: ObservationDiagnostic::default(),
        #[cfg(novarocks_dev_controls)]
        test_controls,
        #[cfg(novarocks_dev_controls)]
        initial_fault_checked: false,
    }))
}

struct NodeObservationStream {
    // The same original R is bounded by the outer Observation stream lane.
    stream: WorkerObservationStream,
    generation: u64,
    bookmark_timer: Pin<Box<tokio::time::Sleep>>,
    catch_up_complete: bool,
    ended: bool,
    diagnostic: ObservationDiagnostic,
    participation: novarocks_execution_contract::ParticipationRef,
    #[cfg(novarocks_dev_controls)]
    test_controls: Option<Arc<crate::test_controls::NativeTestControls>>,
    #[cfg(novarocks_dev_controls)]
    initial_fault_checked: bool,
}
// The diagnostic prefix follows actual finite initial source frames, then at
// most one replacement pass of the same size. It never gates delivery or ACK.
#[derive(Default)]
struct ObservationDiagnostic {
    initial_frames: usize,
    live_remaining: usize,
    pending_logged: bool,
    bookmark_logged: bool,
    last_bookmark: Option<NodeCoveredObservationBookmark>,
}
struct FrameDiagnostic {
    phase: &'static str,
    fact: &'static str,
    revision: Option<u64>,
    task: Option<novarocks_execution_contract::TaskIdentity>,
    status: Option<(u64, novarocks_execution_contract::TaskState, bool)>,
    convergence: Option<u64>,
    quiesce: Option<(u64, usize)>,
    initial_cut: Option<u64>,
}
impl FrameDiagnostic {
    fn from_original(frame: &NodeCoveredObservationFrame) -> Self {
        use novarocks_worker::NodeCoveredObservationFact as F;
        let mut diagnostic = Self {
            phase: "catch_up",
            fact: "catch_up_complete",
            revision: None,
            task: None,
            status: None,
            convergence: None,
            quiesce: None,
            initial_cut: None,
        };
        let fact = match frame {
            NodeCoveredObservationFrame::CatchUp(fact) => fact,
            NodeCoveredObservationFrame::Live { revision, fact } => {
                diagnostic.phase = "live";
                diagnostic.revision = Some(*revision);
                fact
            }
            NodeCoveredObservationFrame::CatchUpComplete { initial_cut } => {
                diagnostic.initial_cut = Some(*initial_cut);
                return diagnostic;
            }
        };
        match fact {
            F::Status(status) => {
                diagnostic.fact = "status";
                diagnostic.task = Some(status.identity());
                diagnostic.status =
                    Some((status.version().get(), status.state(), status.installed()));
            }
            F::TaskConvergence(receipt) => {
                diagnostic.fact = "actual_stopped";
                diagnostic.task = Some(receipt.identity());
                diagnostic.convergence = Some(receipt.version().get());
            }
            F::ParticipationConvergence(receipt) => {
                diagnostic.fact = "participation_convergence";
                diagnostic.convergence = Some(receipt.version().get());
            }
            F::Quiesce(receipt) => {
                diagnostic.fact = "quiesce";
                diagnostic.quiesce =
                    Some((receipt.fence_version(), receipt.accepted_tasks().len()));
            }
            F::StatusUnchanged(task)
            | F::TaskConvergenceUnchanged(task)
            | F::Unknown(task)
            | F::Gone(task) => {
                diagnostic.fact = match fact {
                    F::StatusUnchanged(_) => "status_unchanged",
                    F::TaskConvergenceUnchanged(_) => "convergence_unchanged",
                    F::Unknown(_) => "unknown",
                    _ => "gone",
                };
                diagnostic.task = Some(*task);
            }
        }
        diagnostic
    }
}
impl NodeObservationStream {
    fn trace_delivery(
        &mut self,
        delivery: u64,
        fact: FrameDiagnostic,
        bookmark: NodeCoveredObservationBookmark,
    ) {
        let log = if fact.initial_cut.is_some() {
            self.diagnostic.live_remaining = self.diagnostic.initial_frames;
            true
        } else if fact.phase == "catch_up" {
            self.diagnostic.initial_frames = self.diagnostic.initial_frames.saturating_add(1);
            true
        } else if self.diagnostic.live_remaining > 0 {
            self.diagnostic.live_remaining -= 1;
            true
        } else {
            false
        };
        self.diagnostic.last_bookmark = Some(bookmark);
        if log {
            tracing::debug!(pid = std::process::id(), participation = ?self.participation, generation = self.generation,
                delivery, phase = fact.phase, fact = fact.fact, revision = ?fact.revision,
                task = ?fact.task, status = ?fact.status, convergence = ?fact.convergence,
                quiesce = ?fact.quiesce, initial_cut = ?fact.initial_cut,
                sequence = bookmark.sequence, covered_prefix = bookmark.covered_prefix, source_cut = bookmark.source_cut,
                "Original covered frame reached ordered Native delivery");
        }
    }
    fn bookmark(&mut self) -> Result<proto::TaskStatusStreamEvent, Status> {
        let bookmark = self.stream.bookmark().map_err(observation_status)?;
        let changed = self.diagnostic.last_bookmark.is_none_or(|old| {
            (old.covered_prefix, old.source_cut) != (bookmark.covered_prefix, bookmark.source_cut)
        });
        let log =
            !self.diagnostic.bookmark_logged || (changed && self.diagnostic.live_remaining > 0);
        if log {
            if self.diagnostic.bookmark_logged {
                self.diagnostic.live_remaining -= 1;
            }
            self.diagnostic.bookmark_logged = true;
            tracing::debug!(pid = std::process::id(), participation = ?self.participation, generation = bookmark.generation,
                sequence = bookmark.sequence, covered_prefix = bookmark.covered_prefix, source_cut = bookmark.source_cut,
                "Original covered source emitted a Native bookmark");
        }
        self.diagnostic.last_bookmark = Some(bookmark);
        encode_covered_status_event(&CoveredStatusStreamEvent {
            fact: CoveredStatusStreamFact::Bookmark(CoveredObservationBookmark {
                generation: bookmark.generation,
                sequence: bookmark.sequence,
                covered_prefix: bookmark.covered_prefix,
                source_cut: bookmark.source_cut,
            }),
            source_revision: None,
        })
        .map_err(|error| Status::internal(error.to_string()))
    }
    fn end(&mut self, error: Status) -> Poll<Option<Result<proto::TaskStatusStreamEvent, Status>>> {
        self.ended = true;
        Poll::Ready(Some(Err(error)))
    }
}
impl Stream for NodeObservationStream {
    type Item = Result<proto::TaskStatusStreamEvent, Status>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.ended {
            return Poll::Ready(None);
        }
        if this.bookmark_timer.as_mut().poll(cx).is_ready() {
            this.bookmark_timer
                .as_mut()
                .reset(tokio::time::Instant::now() + BOOKMARK_INTERVAL);
            // CatchUpComplete is the real source boundary; an intake Accepted
            // is never projected as completed initial coverage.
            if this.catch_up_complete {
                let bookmark = this.bookmark();
                return match bookmark {
                    Ok(wire) => Poll::Ready(Some(Ok(wire))),
                    Err(error) => this.end(error),
                };
            }
        }
        let delivery = match this.stream.poll_next(cx) {
            Poll::Pending => {
                if !this.diagnostic.pending_logged {
                    this.diagnostic.pending_logged = true;
                    tracing::debug!(pid = std::process::id(), participation = ?this.participation, generation = this.generation,
                        catch_up_complete = this.catch_up_complete,
                        "Original covered stream is awaiting its first unavailable frame");
                }
                return Poll::Pending;
            }
            Poll::Ready(Err(error)) => return this.end(observation_status(error)),
            Poll::Ready(Ok(delivery)) => delivery,
        };
        #[cfg(novarocks_dev_controls)]
        if !this.initial_fault_checked {
            this.initial_fault_checked = true;
            if let Some(controls) = &this.test_controls {
                match crate::task_protocol_fault::task_status_subscription_dropped(controls, this.participation) {
                    Ok(true) => return this.end(Status::unavailable("runner-owned task status stream dropped after the subscription was established")),
                    Err(error) => return this.end(error),
                    Ok(false) => {}
                }
            }
        }
        let catch_up_complete = matches!(
            delivery.selected().frame,
            NodeCoveredObservationFrame::CatchUpComplete { .. }
        );
        let wire = match encode_frame(&delivery.selected().frame, this.generation) {
            Ok(wire) => wire,
            Err(error) => return this.end(error),
        };
        if wire.encoded_len() > NATIVE_GRPC_DECODED_MESSAGE_MAX_BYTES {
            return this.end(Status::resource_exhausted(
                "covered observation frame exceeds the native message bound",
            ));
        }
        #[cfg(novarocks_dev_controls)]
        let wire = {
            let mut wire = wire;
            if let (Some(identity), Some(controls)) = (
                frame_task(&delivery.selected().frame),
                this.test_controls.as_ref(),
            ) {
                crate::task_protocol_fault::task_status_foreign_process(
                    controls, identity, &mut wire,
                );
            }
            wire
        };
        let diagnostic = FrameDiagnostic::from_original(&delivery.selected().frame);
        let delivery_id = delivery.selected().delivery_id;
        // Returning this ordered item is the existing Native transport
        // acceptance boundary. Encoding, selection and Drop never ACK it.
        match delivery.delivered() {
            Ok(bookmark) => {
                this.trace_delivery(delivery_id, diagnostic, bookmark);
                this.catch_up_complete |= catch_up_complete;
                Poll::Ready(Some(Ok(wire)))
            }
            Err((error, original)) => {
                drop(original);
                this.end(observation_status(error))
            }
        }
    }
}

impl Drop for NodeObservationStream {
    fn drop(&mut self) {
        // Read only the last actual receipt; requesting a fresh bookmark here
        // would change source sequence during diagnostic teardown.
        tracing::debug!(pid = std::process::id(), participation = ?self.participation, generation = self.generation,
            catch_up_complete = self.catch_up_complete, ended = self.ended,
            initial_frames = self.diagnostic.initial_frames,
            last_bookmark = ?self.diagnostic.last_bookmark,
            "Original covered Native stream owner is dropping");
    }
}

fn observation_status(error: WorkerObservationError) -> Status {
    use NodeCoveredSubscriptionError as E;
    if let Some(refusal) = error.source_refusal() {
        return match refusal {
            E::InitialSnapshotTooLarge { .. }
            | E::FrameTooLarge { .. }
            | E::TargetLimitExceeded => Status::resource_exhausted(refusal.to_string()),
            E::InvalidGeneration
            | E::MismatchedParticipation
            | E::FutureTaskCursor(_)
            | E::FutureTaskConvergenceCursor(_)
            | E::FutureParticipationCursor
            | E::FutureQuiesceCursor => Status::invalid_argument(refusal.to_string()),
            _ => Status::internal(refusal.to_string()),
        };
    }
    match error.kind() {
        WorkerObservationErrorKind::ParticipationRemoved => Status::failed_precondition(
            "the original status subscription Participation was removed",
        ),
        WorkerObservationErrorKind::OwnerFailed => {
            Status::internal("the original status subscription owner failed")
        }
        WorkerObservationErrorKind::FrameOutstanding => {
            Status::internal("the original status frame is still in delivery")
        }
        WorkerObservationErrorKind::IntakeRejected => {
            Status::failed_precondition("the original Worker refused status subscription intake")
        }
        WorkerObservationErrorKind::SourceRejected => {
            Status::internal("the original status subscription source failed")
        }
    }
}

#[cfg(novarocks_dev_controls)]
fn frame_task(
    frame: &NodeCoveredObservationFrame,
) -> Option<novarocks_execution_contract::TaskIdentity> {
    use novarocks_worker::NodeCoveredObservationFact as Fact;
    let fact = match frame {
        NodeCoveredObservationFrame::CatchUp(fact)
        | NodeCoveredObservationFrame::Live { fact, .. } => fact,
        NodeCoveredObservationFrame::CatchUpComplete { .. } => return None,
    };
    match fact {
        Fact::Status(status) => Some(status.identity()),
        Fact::StatusUnchanged(identity)
        | Fact::Unknown(identity)
        | Fact::Gone(identity)
        | Fact::TaskConvergenceUnchanged(identity) => Some(*identity),
        Fact::TaskConvergence(receipt) => Some(receipt.identity()),
        Fact::ParticipationConvergence(_) | Fact::Quiesce(_) => None,
    }
}

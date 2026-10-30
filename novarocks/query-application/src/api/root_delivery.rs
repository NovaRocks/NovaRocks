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

//! Carrier-free root read boundary. Every decoded reply retains the complete
//! pre-admitted window; logical ACK, read seal and timeouts do not free it.

use super::QueryExecutionError;
use novarocks_execution_contract::{
    TaskIdentity,
    root_lifetime::RootReadSealed,
    root_result::{
        RootReadDeferral, RootReadOutcome, RootReadRefusal, RootReadResponse, RootResultEnd,
        RootResultRead, RootResultReply,
    },
};
use novarocks_workload_control::{ResultWindowAlias, ResultWindowClass, WorkError};
use std::{future::Future, num::NonZeroU64, pin::Pin};

/// Native adapters implement the wire call, while the application retains
/// one exact read owner and serializes both fetch and optional ACK-only calls.
/// The physical guard follows the body/codec/connection until actual exit.
pub trait BoundedRootReadPort: Send + Sync + 'static {
    /// A failure carries its recovery class and topology requirement exactly
    /// as the transport classified it.
    fn read(
        &self,
        request: RootResultRead,
        physical_guard: ResultWindowAlias,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = Result<RetainedRootReply, crate::coordination::RootResultFetchFailure>,
                > + Send
                + 'static,
        >,
    >;
    /// Stop future fetch/replay dispatch before lifecycle joins and Release.
    /// This is local read closure, not a Worker compound close operation.
    fn seal(&self, sealed: RootReadSealed) -> Result<(), QueryExecutionError>;
}

/// A bounded reply cannot expose an independently clonable Bytes backing.
/// Native decoding transfers the response and its actual physical guard here;
/// protocol adapters receive only borrowed payload slices with exact sequence.
pub struct RetainedRootReply {
    response: RootReadResponse,
    physical_guard: ResultWindowAlias,
}
impl RetainedRootReply {
    pub fn try_new(
        response: RootReadResponse,
        physical_guard: ResultWindowAlias,
        simultaneously_live_backing_bytes: u64,
    ) -> Result<Self, WorkError> {
        let valid_class = match response.kind() {
            novarocks_result_contract::RootOutputKind::ClientRows => matches!(
                physical_guard.class(),
                ResultWindowClass::Client | ResultWindowClass::Local
            ),
            novarocks_result_contract::RootOutputKind::InternalFacts(_) => {
                physical_guard.class() == ResultWindowClass::Internal
            }
            novarocks_result_contract::RootOutputKind::CountOnly => {
                physical_guard.class() != ResultWindowClass::Closing
            }
        };
        let covers_visible_body = match &response {
            RootReadResponse::Reply(RootResultReply {
                outcome: RootReadOutcome::Data(data),
                ..
            }) => simultaneously_live_backing_bytes >= data.body().len() as u64,
            _ => true,
        };
        let checked = if !valid_class || response.validate().is_err() || !covers_visible_body {
            Err(WorkError::Conflict)
        } else {
            physical_guard.check_backing_total(simultaneously_live_backing_bytes)
        };
        if let Err(error) = checked {
            drop(response);
            drop(physical_guard);
            return Err(error);
        }
        Ok(Self {
            response,
            physical_guard,
        })
    }
    pub fn root_task(&self) -> TaskIdentity {
        self.response.root_task()
    }
    /// Every response retains its original admitted physical window.
    pub fn response(&self) -> &RootReadResponse {
        &self.response
    }
    pub fn profile(&self) -> novarocks_result_contract::RootProfileId {
        self.response.profile()
    }
    pub fn kind(&self) -> novarocks_result_contract::RootOutputKind {
        self.response.kind()
    }
    pub fn outcome(&self) -> RootReplyView<'_> {
        match &self.response {
            RootReadResponse::Deferred(deferred) => RootReplyView::Deferred(deferred.reason()),
            RootReadResponse::Refused(refused) => RootReplyView::Refused(refused.as_ref()),
            RootReadResponse::Reply(reply) => match &reply.outcome {
                RootReadOutcome::AckOnly => RootReplyView::AckOnly,
                RootReadOutcome::NotReady => RootReplyView::NotReady,
                RootReadOutcome::Retired => RootReplyView::Retired,
                RootReadOutcome::AwaitTerminalControl => RootReplyView::AwaitTerminalControl,
                RootReadOutcome::End(end) => RootReplyView::End(*end),
                RootReadOutcome::Data(data) => RootReplyView::Data {
                    sequence: data.sequence(),
                    body: data.body().as_ref(),
                    end_after_data: data.end_after_data(),
                },
            },
        }
    }

    pub fn retain_physical_guard(&self) -> ResultWindowAlias {
        self.physical_guard.clone()
    }
}
/// Feature-only scalars copied from the original retained native reply.
/// Visible bytes do not claim allocation backing bytes or physical exit.
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
#[derive(Clone, Copy, Debug)]
pub struct RootDataScalars {
    pub root_task: TaskIdentity,
    pub profile: novarocks_result_contract::RootProfileId,
    pub kind: novarocks_result_contract::RootOutputKind,
    pub accepted_consumed: u64,
    pub native_sequence: NonZeroU64,
    pub body_bytes: u64,
    pub end_after_data: Option<RootResultEnd>,
}
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
impl RetainedRootReply {
    pub fn data_scalars(&self) -> Option<RootDataScalars> {
        let RootReplyView::Data {
            sequence,
            body,
            end_after_data,
        } = self.outcome()
        else {
            return None;
        };
        Some(RootDataScalars {
            root_task: self.root_task(),
            profile: self.profile(),
            kind: self.kind(),
            accepted_consumed: match &self.response {
                RootReadResponse::Reply(reply) => reply.accepted_consumed,
                _ => unreachable!("Data is always an actual receipt"),
            },
            native_sequence: sequence,
            body_bytes: body.len() as u64,
            end_after_data,
        })
    }
}
#[cfg(feature = "mem-1-m07-exact-mysql-write")]
#[derive(Clone, Copy, Debug)]
pub struct ResidentSegmentScalars {
    pub data: RootDataScalars,
    pub window_sequence: NonZeroU64,
    pub completed_rows_by_item: u64,
    pub has_validated_client_rows: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum RootReplyView<'a> {
    Deferred(RootReadDeferral),
    Refused(&'a RootReadRefusal),
    AckOnly,
    NotReady,
    Retired,
    AwaitTerminalControl,
    Data {
        sequence: NonZeroU64,
        body: &'a [u8],
        end_after_data: Option<RootResultEnd>,
    },
    End(RootResultEnd),
}

/// A validated prefetch owned by the same two-item relay window. It has no
/// delivery receipt: a closing transfer always abandons it, never ACKs it.
pub struct ResidentRootSegment {
    pub(crate) sequence: NonZeroU64,
    pub(crate) reply: std::sync::Arc<RetainedRootReply>,
    pub(crate) client_rows: Option<(
        novarocks_result_contract::ClientRowProfile,
        novarocks_result_contract::ClientRowStreamCursor,
    )>,
    pub(crate) rows: u64,
}
impl ResidentRootSegment {
    #[cfg(feature = "mem-1-m07-exact-mysql-write")]
    pub fn fixed_scalars(&self) -> Option<ResidentSegmentScalars> {
        Some(ResidentSegmentScalars {
            data: self.reply.data_scalars()?,
            window_sequence: self.sequence,
            completed_rows_by_item: self.rows,
            has_validated_client_rows: self.client_rows.is_some(),
        })
    }

    fn share(&self) -> Self {
        Self {
            sequence: self.sequence,
            reply: self.reply.clone(),
            client_rows: self.client_rows,
            rows: self.rows,
        }
    }

    pub fn client_rows(&self) -> Option<novarocks_result_contract::ValidatedClientBody<'_>> {
        let (profile, before) = self.client_rows?;
        let RootReplyView::Data { body, .. } = self.reply.outcome() else {
            return None;
        };
        Some(
            before
                .validate_body(profile, body)
                .expect("resident root body already validated"),
        )
    }
}

/// Shared ownership of the two already validated window items. The
/// cancellation cut and publisher serialize on this lock; no read capability
/// or independently clonable payload escapes it.
#[derive(Clone, Default)]
pub struct RootRelayResidentWindow(std::sync::Arc<std::sync::Mutex<ResidentState>>);
#[derive(Default)]
struct ResidentState {
    frozen: bool,
    ready: Option<ResidentRootSegment>,
    delivering: Option<ResidentRootSegment>,
}
impl RootRelayResidentWindow {
    pub(crate) fn publish(&self, segment: ResidentRootSegment) -> bool {
        let mut state = self.0.lock().expect("resident window poisoned");
        if state.frozen || state.ready.is_some() {
            return false;
        }
        state.ready = Some(segment);
        true
    }
    pub(crate) fn take(&self) -> Option<ResidentRootSegment> {
        let mut state = self.0.lock().expect("resident window poisoned");
        if state.frozen {
            None
        } else {
            let segment = state.ready.take()?;
            assert!(
                state.delivering.is_none(),
                "only one root delivery is in flight"
            );
            state.delivering = Some(segment.share());
            Some(segment)
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.0
            .lock()
            .expect("resident window poisoned")
            .ready
            .is_none()
    }
    pub(crate) fn retire(&self, sequence: NonZeroU64) {
        let mut state = self.0.lock().expect("resident window poisoned");
        if state
            .delivering
            .as_ref()
            .is_some_and(|item| item.sequence == sequence)
        {
            state.delivering = None;
        }
    }
    /// Freeze the two already validated items, including actor handoff phases.
    /// No fetch, delivery receipt or ACK is generated by this transfer.
    pub fn freeze(&self) -> [Option<ResidentRootSegment>; 2] {
        let mut state = self.0.lock().expect("resident window poisoned");
        state.frozen = true;
        [state.delivering.take(), state.ready.take()]
    }
}

#[cfg(all(test, feature = "mem-1-m07-exact-mysql-write"))]
mod scalar_projection_tests {
    use super::*;
    use novarocks_execution_contract::root_result::{RootResultData, RootResultReply};
    use novarocks_result_contract::{
        ClientRowProfile, ClientRowStreamCursor, RootOutputKind, RootProfileId, RootProfileV1,
    };
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    use novarocks_workload_control::{
        ResourceConfig, ResultCapacityConfig, WorkClass, WorkRequest, WorkloadConfig,
        WorkloadControl,
    };
    use std::sync::Arc;

    #[test]
    fn original_window_freeze_scalar_copy_does_not_retain_reply_or_capacity_aliases() {
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024 * 1024,
                control_bytes: 1024,
                per_scope_bytes: 1024 * 1024 - 1024,
            },
        )
        .unwrap();
        let capacity = workload
            .configure_result_capacity(ResultCapacityConfig::V1)
            .unwrap();
        workload.mark_ready().unwrap();
        let root = workload
            .try_begin_root(WorkRequest::new(WorkClass::Query))
            .unwrap();
        let grant = capacity
            .try_acquire(&root.owner.scope(), ResultWindowClass::Client)
            .unwrap();
        let task = TaskIdentity::new(
            QueryExecutionId::new(QueryId::new(901, 71), AttemptId::new(9).unwrap()).unwrap(),
            StageId::new(7).unwrap(),
            TaskId::new(13).unwrap(),
            BackendProcessId::new_v7(),
        );
        let profile = ClientRowProfile::try_new(
            RootProfileV1::SEGMENT_BYTES,
            RootProfileV1::ROW_PAYLOAD_BYTES,
        )
        .unwrap();
        let make = |sequence: u64, bytes: &'static [u8], before, rows| ResidentRootSegment {
            sequence: NonZeroU64::new(sequence).unwrap(),
            rows,
            client_rows: Some((profile, before)),
            reply: Arc::new(
                RetainedRootReply::try_new(
                    novarocks_execution_contract::root_result::RootReadResponse::Reply(
                        RootResultReply {
                            root_task: task,
                            profile: RootProfileId::V1,
                            kind: RootOutputKind::ClientRows,
                            accepted_consumed: 0,
                            outcome: RootReadOutcome::Data(
                                RootResultData::try_new(
                                    RootOutputKind::ClientRows,
                                    NonZeroU64::new(sequence).unwrap(),
                                    bytes::Bytes::from_static(bytes),
                                    None,
                                )
                                .unwrap(),
                            ),
                        },
                    ),
                    grant.retain_alias(),
                    4096 + bytes.len() as u64,
                )
                .unwrap(),
            ),
        };
        let window = RootRelayResidentWindow::default();
        assert!(window.publish(make(
            1,
            &[4, 0, 0, 0, 3, b'a'],
            ClientRowStreamCursor::new(),
            0
        )));
        let delivering = window.take().unwrap();
        let weak = Arc::downgrade(&delivering.reply);
        let count = Arc::strong_count(&delivering.reply);
        let first = delivering.fixed_scalars().unwrap();
        assert_eq!(Arc::strong_count(&delivering.reply), count);
        let after = delivering.client_rows().unwrap().after();
        assert!(window.publish(make(2, b"bc", after, 1)));
        let original = window.freeze(); // Exactly one original move; no observer freeze.
        assert!(window.0.lock().unwrap().frozen);
        assert!(window.take().is_none());
        assert!(!window.publish(make(
            3,
            &[1, 0, 0, 0, b'x'],
            ClientRowStreamCursor::new(),
            1
        )));
        let before_count = weak.strong_count();
        let copied = [
            original[0].as_ref().unwrap().fixed_scalars().unwrap(),
            original[1].as_ref().unwrap().fixed_scalars().unwrap(),
        ];
        assert_eq!(weak.strong_count(), before_count);
        assert_eq!(first.data.root_task, task);
        assert_eq!(copied[0].data.root_task, copied[1].data.root_task);
        assert_eq!(copied[0].window_sequence.get(), 1);
        assert_eq!(copied[1].window_sequence.get(), 2);
        assert_eq!(copied[0].data.native_sequence.get(), 1);
        assert_eq!(copied[1].data.native_sequence.get(), 2);
        assert_eq!(copied[0].data.body_bytes, 6);
        assert_eq!(copied[1].data.body_bytes, 2);
        assert_eq!(copied[0].data.accepted_consumed, 0);
        assert_eq!(copied[1].data.accepted_consumed, 0);
        assert_eq!(copied[1].completed_rows_by_item, 1);
        drop(grant);
        drop(original);
        assert_eq!(
            capacity.snapshot().held_positions[0],
            1,
            "original delivery still owns backing"
        );
        drop(delivering);
        assert_eq!(weak.strong_count(), 0);
        assert_eq!(capacity.snapshot().held_positions, [0; 4]);
        assert_eq!(
            copied[0].data.root_task, task,
            "copy still readable after actual last alias exit"
        );
        root.owner.complete();
    }
}

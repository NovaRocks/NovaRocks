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

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

//! One move-only settlement follows the original application owner.
use novarocks_query_application::{
    cancellation::{QueryCancellationReason, QueryCancellationView},
    protocol_delivery::{GovernedProtocolOwner, StreamingStatementResult},
    session::SessionProtocolTerminal,
    session_control::{GovernedStatementFinishOutcome, GovernedStatementVisibilitySealOutcome},
};
pub(crate) enum SettlementOwner {
    Streaming(Box<StreamingStatementResult>),
    Governed(Box<GovernedProtocolOwner>),
    Ungoverned,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Disposition {
    Complete,
    Fail,
    Cancelled,
    ClientCancel,
    Abandon,
}
pub(crate) struct Settlement {
    pub(crate) owner: SettlementOwner,
    pub(crate) terminal: Box<dyn SessionProtocolTerminal>,
    pub(crate) session:
        Option<std::sync::Arc<dyn novarocks_query_application::session::QuerySession>>,
}
impl SettlementOwner {
    fn cancellation_view(&self) -> Option<QueryCancellationView> {
        match self {
            Self::Streaming(owner) => Some(owner.cancellation()),
            Self::Governed(owner) => Some(owner.cancellation()),
            Self::Ungoverned => None,
        }
    }
    pub(crate) fn cancellation_reason(
        &self,
    ) -> Option<novarocks_query_application::cancellation::QueryCancellationReason> {
        match self {
            Self::Streaming(owner) => owner.cancellation().reason(),
            Self::Governed(owner) => owner.cancellation().reason(),
            Self::Ungoverned => None,
        }
    }
    pub(crate) fn seal(&mut self) -> GovernedStatementVisibilitySealOutcome {
        match self {
            Self::Streaming(owner) => owner.seal_success_visibility(),
            Self::Governed(owner) => owner.seal_success_visibility(),
            Self::Ungoverned => GovernedStatementVisibilitySealOutcome::Sealed,
        }
    }
}
impl Settlement {
    pub(crate) fn finish(
        self,
        disposition: Disposition,
    ) -> (
        GovernedStatementFinishOutcome,
        Option<QueryCancellationReason>,
    ) {
        let cancellation = self.owner.cancellation_view();
        let outcome = match self.owner {
            SettlementOwner::Streaming(owner) => match disposition {
                Disposition::Complete => owner.complete(),
                Disposition::Fail => owner.fail(),
                Disposition::Cancelled => owner.settle_cancellation(),
                Disposition::ClientCancel => owner.client_cancel_requested(),
                Disposition::Abandon => owner.client_disconnected(),
            },
            SettlementOwner::Governed(mut owner) => match disposition {
                Disposition::Complete => owner.complete(),
                Disposition::Fail => owner.fail(),
                Disposition::Cancelled => owner.settle_cancellation(),
                Disposition::ClientCancel => owner.client_cancel_requested(),
                Disposition::Abandon => owner.client_disconnected(),
            },
            SettlementOwner::Ungoverned => {
                if disposition == Disposition::Complete {
                    GovernedStatementFinishOutcome::Completed
                } else {
                    GovernedStatementFinishOutcome::ProtocolFailed
                }
            }
        };
        let reason = cancellation.and_then(|view| view.reason());
        self.terminal.complete();
        // Closing the original session before settlement would publish ClientDisconnected.
        if let Some(session) = self.session {
            session.close();
        }
        (outcome, reason)
    }
}

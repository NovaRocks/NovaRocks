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

//! Frontend-owned session and statement cancellation state.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use novarocks::query_execution::cancellation::{QueryCancellationReason, QueryCancellationSource};
use novarocks::query_execution::control::{
    QueryCancelOutcome, QueryControlError, QueryControlPort, QueryControlService, SessionIdentity,
    SessionToken, StatementFinishOutcome, StatementRegistration, StatementToken,
};

#[derive(Default)]
pub struct FrontendQueryControl {
    state: Mutex<QueryControlState>,
}

#[derive(Default)]
struct QueryControlState {
    next_session_epoch: u64,
    sessions: BTreeMap<u32, SessionEntry>,
}

struct SessionEntry {
    session_epoch: u64,
    principal: Arc<str>,
    next_statement_generation: u64,
    active: Option<ActiveStatement>,
}

struct ActiveStatement {
    generation: u64,
    cancellation: QueryCancellationSource,
}

impl FrontendQueryControl {
    pub fn service() -> QueryControlService {
        QueryControlService::new(Arc::new(Self::default()))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, QueryControlState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

impl QueryControlPort for FrontendQueryControl {
    fn register_session(
        &self,
        identity: SessionIdentity,
    ) -> Result<SessionToken, QueryControlError> {
        let mut state = self.lock();
        if state.sessions.contains_key(&identity.connection_id()) {
            return Err(QueryControlError::ConnectionIdInUse);
        }
        state.next_session_epoch = state.next_session_epoch.wrapping_add(1);
        if state.next_session_epoch == 0 {
            state.next_session_epoch = 1;
        }
        let token = SessionToken::new(identity.connection_id(), state.next_session_epoch);
        state.sessions.insert(
            token.connection_id(),
            SessionEntry {
                session_epoch: token.session_epoch(),
                principal: Arc::from(identity.principal()),
                next_statement_generation: 0,
                active: None,
            },
        );
        Ok(token)
    }

    fn unregister_session(&self, token: SessionToken) {
        let mut state = self.lock();
        if state
            .sessions
            .get(&token.connection_id())
            .is_some_and(|entry| entry.session_epoch == token.session_epoch())
        {
            state.sessions.remove(&token.connection_id());
        }
    }

    fn begin_statement(
        &self,
        session: SessionToken,
    ) -> Result<StatementRegistration, QueryControlError> {
        let mut state = self.lock();
        let entry = state
            .sessions
            .get_mut(&session.connection_id())
            .ok_or(QueryControlError::UnknownSession)?;
        if entry.session_epoch != session.session_epoch() {
            return Err(QueryControlError::StaleSession);
        }
        if entry.active.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        entry.next_statement_generation = entry.next_statement_generation.wrapping_add(1);
        if entry.next_statement_generation == 0 {
            entry.next_statement_generation = 1;
        }
        let cancellation = QueryCancellationSource::new();
        let registration = StatementRegistration::new(
            StatementToken::new(session, entry.next_statement_generation),
            cancellation.view(),
        );
        entry.active = Some(ActiveStatement {
            generation: entry.next_statement_generation,
            cancellation,
        });
        Ok(registration)
    }

    fn finish_statement(&self, statement: StatementToken) -> StatementFinishOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().connection_id()) else {
            return StatementFinishOutcome::Stale;
        };
        if entry.session_epoch != statement.session().session_epoch() {
            return StatementFinishOutcome::Stale;
        }
        let Some(active) = entry.active.as_ref() else {
            return StatementFinishOutcome::Stale;
        };
        if active.generation != statement.generation() {
            return StatementFinishOutcome::Stale;
        }
        let reason = active.cancellation.view().reason();
        entry.active = None;
        match reason {
            Some(reason) => StatementFinishOutcome::Cancelled(reason),
            None => StatementFinishOutcome::Completed,
        }
    }

    fn cancel_session_statement(
        &self,
        session: SessionToken,
        reason: QueryCancellationReason,
    ) -> QueryCancelOutcome {
        let state = self.lock();
        let Some(entry) = state.sessions.get(&session.connection_id()) else {
            return QueryCancelOutcome::UnknownSession;
        };
        if entry.session_epoch != session.session_epoch() {
            return QueryCancelOutcome::UnknownSession;
        }
        let Some(active) = entry.active.as_ref() else {
            return QueryCancelOutcome::NoActiveStatement;
        };
        match active.cancellation.request(reason) {
            novarocks::query_execution::cancellation::QueryCancellationRequestResult::Requested => {
                QueryCancelOutcome::Requested
            }
            novarocks::query_execution::cancellation::QueryCancellationRequestResult::AlreadyRequested(reason) => {
                QueryCancelOutcome::AlreadyRequested(reason)
            }
        }
    }

    fn kill_query(&self, requester: SessionToken, target_connection_id: u32) -> QueryCancelOutcome {
        let state = self.lock();
        let Some(requester_entry) = state.sessions.get(&requester.connection_id()) else {
            return QueryCancelOutcome::UnknownSession;
        };
        if requester_entry.session_epoch != requester.session_epoch() {
            return QueryCancelOutcome::UnknownSession;
        }
        let requester_principal = Arc::clone(&requester_entry.principal);
        let Some(target_entry) = state.sessions.get(&target_connection_id) else {
            return QueryCancelOutcome::UnknownSession;
        };
        if target_entry.principal != requester_principal {
            return QueryCancelOutcome::PermissionDenied;
        }
        let Some(active) = target_entry.active.as_ref() else {
            return QueryCancelOutcome::NoActiveStatement;
        };
        match active
            .cancellation
            .request(QueryCancellationReason::ExplicitKill {
                requester_connection_id: requester.connection_id(),
            })
        {
            novarocks::query_execution::cancellation::QueryCancellationRequestResult::Requested => {
                QueryCancelOutcome::Requested
            }
            novarocks::query_execution::cancellation::QueryCancellationRequestResult::AlreadyRequested(reason) => {
                QueryCancelOutcome::AlreadyRequested(reason)
            }
        }
    }

    fn cancel_all(&self, reason: QueryCancellationReason) {
        let state = self.lock();
        for entry in state.sessions.values() {
            if let Some(active) = entry.active.as_ref() {
                let _ = active.cancellation.request(reason.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks::query_execution::control::QueryControlPort;

    fn register(control: &FrontendQueryControl, id: u32, principal: &str) -> SessionToken {
        control
            .register_session(SessionIdentity::new(id, principal))
            .expect("register session")
    }

    #[test]
    fn stale_session_and_statement_cannot_remove_successor() {
        let control = FrontendQueryControl::default();
        let first = register(&control, 7, "root");
        let old = control.begin_statement(first).expect("begin old");
        control.unregister_session(first);
        let second = register(&control, 7, "root");
        let current = control.begin_statement(second).expect("begin current");
        assert_eq!(
            control.finish_statement(old.token()),
            StatementFinishOutcome::Stale
        );
        assert_eq!(
            control.cancel_session_statement(second, QueryCancellationReason::ClientDisconnected),
            QueryCancelOutcome::Requested
        );
        assert!(current.cancellation().is_cancelled());
    }

    #[test]
    fn repeated_kill_preserves_first_reason() {
        let control = FrontendQueryControl::default();
        let target = register(&control, 7, "root");
        let requester = register(&control, 8, "root");
        let active = control.begin_statement(target).expect("begin target");
        assert_eq!(
            control.kill_query(requester, 7),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control.kill_query(requester, 7),
            QueryCancelOutcome::AlreadyRequested(QueryCancellationReason::ExplicitKill {
                requester_connection_id: 8,
            })
        );
        assert_eq!(
            control.finish_statement(active.token()),
            StatementFinishOutcome::Cancelled(QueryCancellationReason::ExplicitKill {
                requester_connection_id: 8,
            })
        );
    }

    #[test]
    fn permission_and_idle_do_not_change_target() {
        let control = FrontendQueryControl::default();
        let target = register(&control, 7, "root");
        let foreign = register(&control, 8, "other");
        let own = register(&control, 9, "root");
        assert_eq!(
            control.kill_query(own, 99),
            QueryCancelOutcome::UnknownSession
        );
        assert_eq!(
            control.kill_query(own, 7),
            QueryCancelOutcome::NoActiveStatement
        );
        let active = control.begin_statement(target).expect("begin target");
        assert_eq!(
            control.kill_query(foreign, 7),
            QueryCancelOutcome::PermissionDenied
        );
        assert!(!active.cancellation().is_cancelled());
    }

    #[test]
    fn finish_and_cancel_are_linearized_and_busy_session_rejects_successor() {
        let control = FrontendQueryControl::default();
        let target = register(&control, 7, "root");
        let requester = register(&control, 8, "root");
        let active = control.begin_statement(target).expect("begin target");

        assert!(matches!(
            control.begin_statement(target),
            Err(QueryControlError::StatementBusy)
        ));
        assert_eq!(
            control.kill_query(requester, 7),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control.finish_statement(active.token()),
            StatementFinishOutcome::Cancelled(QueryCancellationReason::ExplicitKill {
                requester_connection_id: 8,
            })
        );
        assert_eq!(
            control.kill_query(requester, 7),
            QueryCancelOutcome::NoActiveStatement
        );
        assert!(
            control.begin_statement(target).is_ok(),
            "only the matching active generation is released"
        );
    }
}

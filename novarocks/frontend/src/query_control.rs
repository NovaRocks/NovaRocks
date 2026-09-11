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

use crate::ClientConnectionToken;
use crate::common::query_cancellation::{QueryCancellationReason, QueryCancellationSource};
use crate::query_execution::control::{
    ConnectionKillAuthorization, GovernedStatementCancellation, GovernedStatementFinishOutcome,
    GovernedStatementRegistration, GovernedStatementVisibilitySealOutcome, QueryCancelOutcome,
    QueryControlError, QueryControlPort, QueryControlService, SessionIdentity, SessionToken,
    StatementFinishOutcome, StatementRegistration, StatementToken,
};
use novarocks_workload_control::{
    CancellationReason, WorkCancellationRequestOutcome, WorkError, WorkSuccessSealOutcome,
};

// Design: ADR-0102 (docs/adr/ADR-0102-mysql-kill-connection-lifecycle-ownership.md)
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
    connection: ClientConnectionToken,
    principal: Arc<str>,
    next_statement_generation: u64,
    active: Option<ActiveStatement>,
    unregister_pending: bool,
}

struct ActiveStatement {
    generation: u64,
    cancellation: ActiveStatementCancellation,
    success_visibility_sealed: bool,
}

enum ActiveStatementCancellation {
    Legacy(QueryCancellationSource),
    Governed(GovernedStatementCancellation),
}

impl ActiveStatementCancellation {
    fn request(&self, reason: QueryCancellationReason) -> QueryCancelOutcome {
        match self {
            Self::Legacy(cancellation) => match cancellation.request(reason) {
                crate::common::query_cancellation::QueryCancellationRequestResult::Requested => {
                    QueryCancelOutcome::Requested
                }
                crate::common::query_cancellation::QueryCancellationRequestResult::AlreadyRequested(
                    reason,
                ) => QueryCancelOutcome::AlreadyRequested(reason),
            },
            Self::Governed(cancellation) => {
                let workload_reason = workload_cancellation_reason(&reason);
                match cancellation.requester().request_with_outcome(workload_reason) {
                    Ok(WorkCancellationRequestOutcome::Requested) => {
                        cancellation.remember_query_reason(reason);
                        QueryCancelOutcome::Requested
                    }
                    Ok(WorkCancellationRequestOutcome::AlreadyRequested(existing)) => {
                        let first = cancellation.first_query_reason();
                        QueryCancelOutcome::AlreadyRequested(query_cancellation_reason(
                            existing,
                            first.as_ref().unwrap_or(&reason),
                        ))
                    }
                    Ok(WorkCancellationRequestOutcome::SuccessSealed) => {
                        QueryCancelOutcome::NoActiveStatement
                    }
                    Err(error) => QueryCancelOutcome::Failed(error),
                }
            }
        }
    }
}

fn workload_cancellation_reason(reason: &QueryCancellationReason) -> CancellationReason {
    match reason {
        QueryCancellationReason::ExecutionCancellationRequested => CancellationReason::Requested,
        QueryCancellationReason::ExecutionOwnerDropped => CancellationReason::OwnerDropped,
        QueryCancellationReason::ExplicitKill {
            requester_connection_id,
        } => CancellationReason::ExplicitKill {
            requester_connection_id: u64::from(*requester_connection_id),
        },
        QueryCancellationReason::ExplicitKillConnection {
            requester_connection_id,
        } => CancellationReason::ExplicitKillConnection {
            requester_connection_id: u64::from(*requester_connection_id),
        },
        QueryCancellationReason::ClientDisconnected => CancellationReason::ClientDisconnected,
        QueryCancellationReason::DeadlineExceeded { .. } => CancellationReason::DeadlineExceeded,
        QueryCancellationReason::FrontendDrainDeadlineExceeded { .. } => {
            CancellationReason::FrontendDrainDeadlineExceeded
        }
        QueryCancellationReason::ServerShutdown => CancellationReason::ServerShutdown,
    }
}

fn query_cancellation_reason(
    reason: CancellationReason,
    requested: &QueryCancellationReason,
) -> QueryCancellationReason {
    match reason {
        CancellationReason::ExplicitKill {
            requester_connection_id,
        } => QueryCancellationReason::ExplicitKill {
            requester_connection_id: u32::try_from(requester_connection_id).unwrap_or(u32::MAX),
        },
        CancellationReason::ExplicitKillConnection {
            requester_connection_id,
        } => QueryCancellationReason::ExplicitKillConnection {
            requester_connection_id: u32::try_from(requester_connection_id).unwrap_or(u32::MAX),
        },
        CancellationReason::ClientDisconnected => QueryCancellationReason::ClientDisconnected,
        CancellationReason::DeadlineExceeded => match requested {
            QueryCancellationReason::DeadlineExceeded { timeout_ms } => {
                QueryCancellationReason::DeadlineExceeded {
                    timeout_ms: *timeout_ms,
                }
            }
            _ => QueryCancellationReason::DeadlineExceeded { timeout_ms: 0 },
        },
        CancellationReason::FrontendDrainDeadlineExceeded => match requested {
            QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms } => {
                QueryCancellationReason::FrontendDrainDeadlineExceeded {
                    timeout_ms: *timeout_ms,
                }
            }
            _ => QueryCancellationReason::FrontendDrainDeadlineExceeded { timeout_ms: 0 },
        },
        CancellationReason::ServerShutdown => QueryCancellationReason::ServerShutdown,
        CancellationReason::Requested => QueryCancellationReason::ExecutionCancellationRequested,
        CancellationReason::OwnerDropped => QueryCancellationReason::ExecutionOwnerDropped,
    }
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
                connection: identity.connection_token(),
                principal: Arc::from(identity.principal()),
                next_statement_generation: 0,
                active: None,
                unregister_pending: false,
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
            let entry = state.sessions.get_mut(&token.connection_id()).unwrap();
            if let Some(active) = entry.active.as_ref() {
                entry.unregister_pending = true;
                if !active.success_visibility_sealed
                    && let QueryCancelOutcome::Failed(error) = active
                        .cancellation
                        .request(QueryCancellationReason::ClientDisconnected)
                {
                    tracing::error!(
                        connection_id = token.connection_id(),
                        session_epoch = token.session_epoch(),
                        error = %error,
                        "retain query-control session after workload cancellation failed"
                    );
                }
                return;
            }
            state.sessions.remove(&token.connection_id());
        }
    }

    fn begin_statement(
        &self,
        session: SessionToken,
    ) -> Result<StatementRegistration, QueryControlError> {
        self.begin_statement_with_cancellation(session, QueryCancellationSource::new())
    }

    fn begin_statement_with_cancellation(
        &self,
        session: SessionToken,
        cancellation: QueryCancellationSource,
    ) -> Result<StatementRegistration, QueryControlError> {
        let mut state = self.lock();
        let entry = state
            .sessions
            .get_mut(&session.connection_id())
            .ok_or(QueryControlError::UnknownSession)?;
        if entry.session_epoch != session.session_epoch() {
            return Err(QueryControlError::StaleSession);
        }
        if entry.unregister_pending {
            return Err(QueryControlError::UnknownSession);
        }
        if entry.active.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        entry.next_statement_generation = entry.next_statement_generation.wrapping_add(1);
        if entry.next_statement_generation == 0 {
            entry.next_statement_generation = 1;
        }
        let registration = StatementRegistration::new(
            StatementToken::new(session, entry.next_statement_generation),
            cancellation.view(),
        );
        entry.active = Some(ActiveStatement {
            generation: entry.next_statement_generation,
            cancellation: ActiveStatementCancellation::Legacy(cancellation),
            success_visibility_sealed: false,
        });
        Ok(registration)
    }

    fn begin_statement_with_governed_cancellation(
        &self,
        session: SessionToken,
        cancellation: GovernedStatementCancellation,
    ) -> Result<GovernedStatementRegistration, QueryControlError> {
        let mut state = self.lock();
        let entry = state
            .sessions
            .get_mut(&session.connection_id())
            .ok_or(QueryControlError::UnknownSession)?;
        if entry.session_epoch != session.session_epoch() {
            return Err(QueryControlError::StaleSession);
        }
        if entry.unregister_pending {
            return Err(QueryControlError::UnknownSession);
        }
        if entry.active.is_some() {
            return Err(QueryControlError::StatementBusy);
        }
        entry.next_statement_generation = entry.next_statement_generation.wrapping_add(1);
        if entry.next_statement_generation == 0 {
            entry.next_statement_generation = 1;
        }
        let registration = GovernedStatementRegistration::new(
            StatementToken::new(session, entry.next_statement_generation),
            cancellation.view().clone(),
        );
        entry.active = Some(ActiveStatement {
            generation: entry.next_statement_generation,
            cancellation: ActiveStatementCancellation::Governed(cancellation),
            success_visibility_sealed: false,
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
        let ActiveStatementCancellation::Legacy(cancellation) = &active.cancellation else {
            return StatementFinishOutcome::Stale;
        };
        let reason = if active.success_visibility_sealed {
            None
        } else {
            cancellation.view().reason()
        };
        let unregister_pending = entry.unregister_pending;
        entry.active = None;
        let outcome = match reason {
            Some(reason) => StatementFinishOutcome::Cancelled(reason),
            None => StatementFinishOutcome::Completed,
        };
        if unregister_pending {
            state.sessions.remove(&statement.session().connection_id());
        }
        outcome
    }

    fn seal_governed_statement_visibility(
        &self,
        statement: StatementToken,
    ) -> GovernedStatementVisibilitySealOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().connection_id()) else {
            return GovernedStatementVisibilitySealOutcome::Stale;
        };
        if entry.session_epoch != statement.session().session_epoch() {
            return GovernedStatementVisibilitySealOutcome::Stale;
        }
        let Some(active) = entry.active.as_mut() else {
            return GovernedStatementVisibilitySealOutcome::Stale;
        };
        if active.generation != statement.generation() {
            return GovernedStatementVisibilitySealOutcome::Stale;
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return GovernedStatementVisibilitySealOutcome::Stale;
        };
        if active.success_visibility_sealed {
            return GovernedStatementVisibilitySealOutcome::Sealed;
        }
        match cancellation.success_sealer().seal() {
            Ok(WorkSuccessSealOutcome::Sealed | WorkSuccessSealOutcome::AlreadySealed) => {
                active.success_visibility_sealed = true;
                GovernedStatementVisibilitySealOutcome::Sealed
            }
            Ok(WorkSuccessSealOutcome::Cancelled(reason)) => {
                GovernedStatementVisibilitySealOutcome::Cancelled(reason)
            }
            Err(_) => GovernedStatementVisibilitySealOutcome::Stale,
        }
    }

    fn finish_governed_statement(
        &self,
        statement: StatementToken,
    ) -> GovernedStatementFinishOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().connection_id()) else {
            return GovernedStatementFinishOutcome::Stale;
        };
        if entry.session_epoch != statement.session().session_epoch() {
            return GovernedStatementFinishOutcome::Stale;
        }
        let Some(active) = entry.active.as_ref() else {
            return GovernedStatementFinishOutcome::Stale;
        };
        if active.generation != statement.generation() {
            return GovernedStatementFinishOutcome::Stale;
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return GovernedStatementFinishOutcome::Stale;
        };
        let reason = if active.success_visibility_sealed {
            None
        } else {
            cancellation.view().reason()
        };
        let unregister_pending = entry.unregister_pending;
        entry.active = None;
        let outcome = match reason {
            Some(reason) => GovernedStatementFinishOutcome::Cancelled(reason),
            None => GovernedStatementFinishOutcome::Completed,
        };
        if unregister_pending {
            state.sessions.remove(&statement.session().connection_id());
        }
        outcome
    }

    fn fail_governed_statement(&self, statement: StatementToken) -> GovernedStatementFinishOutcome {
        let mut state = self.lock();
        let Some(entry) = state.sessions.get_mut(&statement.session().connection_id()) else {
            return GovernedStatementFinishOutcome::Stale;
        };
        if entry.session_epoch != statement.session().session_epoch() {
            return GovernedStatementFinishOutcome::Stale;
        }
        let Some(active) = entry.active.as_ref() else {
            return GovernedStatementFinishOutcome::Stale;
        };
        if active.generation != statement.generation()
            || !matches!(
                active.cancellation,
                ActiveStatementCancellation::Governed(_)
            )
        {
            return GovernedStatementFinishOutcome::Stale;
        }
        let unregister_pending = entry.unregister_pending;
        entry.active = None;
        if unregister_pending {
            state.sessions.remove(&statement.session().connection_id());
        }
        GovernedStatementFinishOutcome::ProtocolFailed
    }

    fn cancel_governed_statement(
        &self,
        statement: StatementToken,
        reason: CancellationReason,
    ) -> Result<Option<WorkCancellationRequestOutcome>, WorkError> {
        let state = self.lock();
        let Some(entry) = state.sessions.get(&statement.session().connection_id()) else {
            return Ok(None);
        };
        if entry.session_epoch != statement.session().session_epoch() {
            return Ok(None);
        }
        let Some(active) = entry.active.as_ref() else {
            return Ok(None);
        };
        if active.generation != statement.generation() {
            return Ok(None);
        }
        if active.success_visibility_sealed {
            return Ok(None);
        }
        let ActiveStatementCancellation::Governed(cancellation) = &active.cancellation else {
            return Ok(None);
        };
        cancellation
            .requester()
            .request_with_outcome(reason)
            .map(Some)
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
        if active.success_visibility_sealed {
            return QueryCancelOutcome::NoActiveStatement;
        }
        active.cancellation.request(reason)
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
        if active.success_visibility_sealed {
            return QueryCancelOutcome::NoActiveStatement;
        }
        active
            .cancellation
            .request(QueryCancellationReason::ExplicitKill {
                requester_connection_id: requester.connection_id(),
            })
    }

    fn authorize_connection_kill(
        &self,
        requester: SessionToken,
        target_connection_id: u32,
    ) -> ConnectionKillAuthorization {
        let state = self.lock();
        let Some(requester_entry) = state.sessions.get(&requester.connection_id()) else {
            return ConnectionKillAuthorization::UnknownSession;
        };
        if requester_entry.session_epoch != requester.session_epoch() {
            return ConnectionKillAuthorization::UnknownSession;
        }
        let Some(target_entry) = state.sessions.get(&target_connection_id) else {
            return ConnectionKillAuthorization::UnknownSession;
        };
        if target_entry.principal != requester_entry.principal {
            return ConnectionKillAuthorization::PermissionDenied;
        }
        ConnectionKillAuthorization::Authorized(target_entry.connection)
    }

    fn cancel_all(&self, reason: QueryCancellationReason) {
        let state = self.lock();
        for entry in state.sessions.values() {
            if let Some(active) = entry.active.as_ref() {
                if active.success_visibility_sealed {
                    continue;
                }
                if let QueryCancelOutcome::Failed(error) =
                    active.cancellation.request(reason.clone())
                {
                    tracing::error!(
                        error = %error,
                        "failed to request governed statement cancellation during cancel-all"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_execution::control::{GovernedQueryStatementBeginError, QueryControlPort};
    use novarocks_workload_control::{ResourceConfig, WorkloadConfig, WorkloadControl};

    fn register(
        control: &FrontendQueryControl,
        id: u32,
        generation: u64,
        principal: &str,
    ) -> SessionToken {
        control
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(id, generation).expect("valid connection token"),
                principal,
            ))
            .expect("register session")
    }

    #[test]
    fn stale_session_and_statement_cannot_remove_successor() {
        let control = FrontendQueryControl::default();
        let first = register(&control, 7, 1, "root");
        let old = control.begin_statement(first).expect("begin old");
        control.unregister_session(first);
        assert!(matches!(
            control.register_session(SessionIdentity::new(
                ClientConnectionToken::new(7, 2).expect("valid connection token"),
                "root",
            )),
            Err(QueryControlError::ConnectionIdInUse)
        ));
        assert_eq!(
            control.finish_statement(old.token()),
            StatementFinishOutcome::Cancelled(QueryCancellationReason::ClientDisconnected)
        );
        let second = register(&control, 7, 2, "root");
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
        let target = register(&control, 7, 1, "root");
        let requester = register(&control, 8, 1, "root");
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
        let target = register(&control, 7, 1, "root");
        let foreign = register(&control, 8, 1, "other");
        let own = register(&control, 9, 1, "root");
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
        let target = register(&control, 7, 1, "root");
        let requester = register(&control, 8, 1, "root");
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

    #[test]
    fn caller_owned_cancellation_source_controls_the_registered_statement() {
        let control = FrontendQueryControl::default();
        let session = register(&control, 7, 1, "root");
        let source = QueryCancellationSource::new();
        let active = control
            .begin_statement_with_cancellation(session, source.clone())
            .expect("begin statement with lifecycle source");

        assert_eq!(
            source.request(QueryCancellationReason::FrontendDrainDeadlineExceeded {
                timeout_ms: 300_000,
            }),
            crate::common::query_cancellation::QueryCancellationRequestResult::Requested
        );
        assert!(active.cancellation().is_cancelled());
        assert_eq!(
            control.finish_statement(active.token()),
            StatementFinishOutcome::Cancelled(
                QueryCancellationReason::FrontendDrainDeadlineExceeded {
                    timeout_ms: 300_000,
                }
            )
        );
    }

    #[test]
    fn connection_kill_authorization_returns_only_the_exact_target_token() {
        let control = FrontendQueryControl::default();
        let target = register(&control, 7, 3, "root");
        let requester = register(&control, 8, 4, "root");
        let foreign = register(&control, 9, 5, "other");

        assert_eq!(
            control.authorize_connection_kill(requester, 7),
            ConnectionKillAuthorization::Authorized(
                ClientConnectionToken::new(7, 3).expect("valid token")
            )
        );
        assert_eq!(
            control.authorize_connection_kill(requester, 99),
            ConnectionKillAuthorization::UnknownSession
        );
        assert_eq!(
            control.authorize_connection_kill(foreign, 7),
            ConnectionKillAuthorization::PermissionDenied
        );

        control.unregister_session(target);
        let successor = register(&control, 7, 6, "root");
        assert_eq!(
            control.authorize_connection_kill(requester, 7),
            ConnectionKillAuthorization::Authorized(
                ClientConnectionToken::new(7, 6).expect("valid token")
            )
        );
        control.unregister_session(successor);
    }

    fn governed_control() -> (
        Arc<FrontendQueryControl>,
        QueryControlService,
        WorkloadControl,
    ) {
        let control = Arc::new(FrontendQueryControl::default());
        let service = QueryControlService::new(control.clone());
        let workload = WorkloadControl::try_new(
            WorkloadConfig::default(),
            ResourceConfig {
                total_bytes: 1024,
                control_bytes: 128,
                per_scope_bytes: 896,
            },
        )
        .expect("workload authority");
        workload.mark_ready().expect("workload authority ready");
        (control, service, workload)
    }

    #[test]
    fn governed_kill_requests_the_registered_work_scope() {
        let (control, service, workload) = governed_control();
        let target = register(&control, 7, 1, "root");
        let requester = register(&control, 8, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(target, &workload.root_admission(), None, None)
            .expect("governed query statement");
        let owner = statement
            .take_execution_owner()
            .expect("root owner transfers once");

        assert_eq!(
            control.kill_query(requester, target.connection_id()),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            statement.cancellation().reason(),
            Some(CancellationReason::ExplicitKill {
                requester_connection_id: u64::from(requester.connection_id()),
            })
        );
        owner.complete();
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::ExplicitKill {
                requester_connection_id: u64::from(requester.connection_id()),
            })
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_late_kill_reports_the_existing_owner_drop_reason() {
        let (control, service, workload) = governed_control();
        let target = register(&control, 7, 1, "root");
        let requester = register(&control, 8, 1, "root");
        let statement = service
            .begin_governed_query_statement(target, &workload.root_admission(), None, None)
            .expect("governed query statement");

        assert_eq!(
            control
                .cancel_session_statement(target, QueryCancellationReason::ExecutionOwnerDropped,),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control.kill_query(requester, target.connection_id()),
            QueryCancelOutcome::AlreadyRequested(QueryCancellationReason::ExecutionOwnerDropped,)
        );
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::OwnerDropped)
        );
    }

    #[test]
    fn governed_disconnect_and_timeout_use_the_same_workload_authority() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None)
            .expect("governed query statement");
        let owner = statement
            .take_execution_owner()
            .expect("root owner transfers once");

        assert_eq!(
            control.cancel_session_statement(
                session,
                QueryCancellationReason::DeadlineExceeded { timeout_ms: 50 },
            ),
            QueryCancelOutcome::Requested
        );
        assert_eq!(
            control.cancel_session_statement(session, QueryCancellationReason::ClientDisconnected,),
            QueryCancelOutcome::AlreadyRequested(QueryCancellationReason::DeadlineExceeded {
                timeout_ms: 50,
            })
        );
        assert_eq!(
            statement.cancellation().reason(),
            Some(CancellationReason::DeadlineExceeded)
        );
        owner.complete();
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Cancelled(CancellationReason::DeadlineExceeded)
        );

        let session_lease = service
            .register_session(SessionIdentity::new(
                ClientConnectionToken::new(9, 1).expect("valid connection token"),
                "root",
            ))
            .expect("register second session");
        let session = session_lease.token();
        let statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None)
            .expect("second governed query statement");
        drop(session_lease);
        assert_eq!(
            statement.cancellation().reason(),
            Some(CancellationReason::ClientDisconnected)
        );
    }

    #[test]
    fn governed_generation_and_business_permit_span_execution_start_to_protocol_finish() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut first = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None)
            .expect("first governed query statement");
        let first_generation = first.token().generation();
        let owner = first
            .take_execution_owner()
            .expect("execution start consumes the root owner");
        owner.complete();

        assert_eq!(workload.snapshot().businesses, 1);
        assert!(matches!(
            service.begin_governed_query_statement(session, &workload.root_admission(), None, None),
            Err(GovernedQueryStatementBeginError::QueryControl(
                QueryControlError::StatementBusy
            ))
        ));
        assert_eq!(
            workload.snapshot().businesses,
            1,
            "rejected successor admission rolls back its root and business permit"
        );

        assert_eq!(first.finish(), GovernedStatementFinishOutcome::Completed);
        assert_eq!(workload.snapshot().businesses, 0);
        let second = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None)
            .expect("protocol completion releases the next generation");
        assert!(second.token().generation() > first_generation);
    }

    #[test]
    fn governed_success_seal_wins_against_late_kill_until_protocol_finish() {
        let (control, service, workload) = governed_control();
        let target = register(&control, 7, 1, "root");
        let requester = register(&control, 8, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(target, &workload.root_admission(), None, None)
            .expect("governed query statement");
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();

        assert_eq!(
            statement.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(workload.snapshot().businesses, 1);
        assert_eq!(
            control.kill_query(requester, target.connection_id()),
            QueryCancelOutcome::NoActiveStatement
        );
        assert_eq!(statement.cancellation().reason(), None);
        assert_eq!(
            statement.finish(),
            GovernedStatementFinishOutcome::Completed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_eof_failure_after_success_seal_is_protocol_failed() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None)
            .expect("governed query statement");
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();

        assert_eq!(
            statement.seal_success_visibility(),
            GovernedStatementVisibilitySealOutcome::Sealed
        );
        assert_eq!(
            statement.protocol_fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn governed_internal_protocol_failure_does_not_fabricate_cancellation() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None)
            .expect("governed query statement");
        let cancellation = statement.cancellation().clone();
        statement
            .take_execution_owner()
            .expect("execution owner")
            .complete();

        assert_eq!(
            statement.protocol_fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        assert_eq!(cancellation.reason(), None);
        assert_eq!(workload.snapshot().businesses, 0);
    }

    #[test]
    fn unregister_is_deferred_until_the_active_generation_settles() {
        let (control, service, workload) = governed_control();
        let connection = ClientConnectionToken::new(17, 1).expect("connection token");
        let session = service
            .register_session(SessionIdentity::new(connection, "root"))
            .expect("register session");
        let token = session.token();
        let statement = service
            .begin_governed_query_statement(token, &workload.root_admission(), None, None)
            .expect("begin governed statement");

        drop(session);
        assert!(
            matches!(
                service.register_session(SessionIdentity::new(connection, "root")),
                Err(QueryControlError::ConnectionIdInUse)
            ),
            "the registry retains the deferred unregister owner"
        );
        assert_eq!(
            statement.protocol_fail(),
            GovernedStatementFinishOutcome::ProtocolFailed
        );
        let successor = service
            .register_session(SessionIdentity::new(connection, "root"))
            .expect("settlement completes deferred unregister");
        assert_ne!(successor.token().session_epoch(), token.session_epoch());
        drop(successor);
        assert!(
            !control
                .lock()
                .sessions
                .contains_key(&connection.connection_id())
        );
    }

    #[tokio::test]
    async fn governed_root_deadline_is_observed_without_a_legacy_cancellation_source() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(10);
        let statement = service
            .begin_governed_query_statement(
                session,
                &workload.root_admission(),
                Some(deadline),
                Some(1),
            )
            .expect("deadline-bound governed query statement");

        assert_eq!(
            tokio::time::timeout(
                std::time::Duration::from_secs(1),
                statement.cancellation().cancelled(),
            )
            .await
            .expect("deadline cancellation arrives"),
            CancellationReason::DeadlineExceeded
        );
    }

    #[test]
    fn dropping_protocol_owner_after_execution_handoff_cancels_the_same_root() {
        let (control, service, workload) = governed_control();
        let session = register(&control, 7, 1, "root");
        let mut statement = service
            .begin_governed_query_statement(session, &workload.root_admission(), None, None)
            .expect("governed query statement");
        let scope = statement.scope().clone();
        let owner = statement
            .take_execution_owner()
            .expect("execution owner transfers once");

        drop(statement);

        assert_eq!(
            scope.cancellation().expect("root remains owned").reason(),
            Some(CancellationReason::OwnerDropped)
        );
        assert_eq!(workload.snapshot().businesses, 0);
        owner.complete();
    }
}

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

//! Frontend-owned session cancellation control contract.

use std::sync::Arc;

use super::cancellation::{QueryCancellationReason, QueryCancellationView};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionIdentity {
    connection_id: u32,
    principal: Arc<str>,
}

impl SessionIdentity {
    pub fn new(connection_id: u32, principal: impl Into<Arc<str>>) -> Self {
        Self {
            connection_id,
            principal: principal.into(),
        }
    }

    pub const fn connection_id(&self) -> u32 {
        self.connection_id
    }

    pub fn principal(&self) -> &str {
        &self.principal
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionToken {
    connection_id: u32,
    session_epoch: u64,
}

impl SessionToken {
    pub const fn new(connection_id: u32, session_epoch: u64) -> Self {
        Self {
            connection_id,
            session_epoch,
        }
    }

    pub const fn connection_id(self) -> u32 {
        self.connection_id
    }

    pub const fn session_epoch(self) -> u64 {
        self.session_epoch
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct StatementToken {
    session: SessionToken,
    generation: u64,
}

impl StatementToken {
    pub const fn new(session: SessionToken, generation: u64) -> Self {
        Self {
            session,
            generation,
        }
    }

    pub const fn session(self) -> SessionToken {
        self.session
    }

    pub const fn generation(self) -> u64 {
        self.generation
    }
}

#[derive(Clone)]
pub struct StatementRegistration {
    token: StatementToken,
    cancellation: QueryCancellationView,
}

impl StatementRegistration {
    pub fn new(token: StatementToken, cancellation: QueryCancellationView) -> Self {
        Self {
            token,
            cancellation,
        }
    }

    pub const fn token(&self) -> StatementToken {
        self.token
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        &self.cancellation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryControlError {
    ConnectionIdInUse,
    UnknownSession,
    StaleSession,
    StatementBusy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryCancelOutcome {
    Requested,
    AlreadyRequested(QueryCancellationReason),
    NoActiveStatement,
    UnknownSession,
    PermissionDenied,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatementFinishOutcome {
    Completed,
    Cancelled(QueryCancellationReason),
    Stale,
}

// Design: ADR-0010 (docs/adr/ADR-0010-explicit-query-cancellation-surface.md)
pub trait QueryControlPort: Send + Sync + 'static {
    fn register_session(
        &self,
        identity: SessionIdentity,
    ) -> Result<SessionToken, QueryControlError>;
    fn unregister_session(&self, token: SessionToken);
    fn begin_statement(
        &self,
        session: SessionToken,
    ) -> Result<StatementRegistration, QueryControlError>;
    fn finish_statement(&self, statement: StatementToken) -> StatementFinishOutcome;
    fn cancel_session_statement(
        &self,
        session: SessionToken,
        reason: QueryCancellationReason,
    ) -> QueryCancelOutcome;
    fn kill_query(&self, requester: SessionToken, target_connection_id: u32) -> QueryCancelOutcome;
    fn cancel_all(&self, reason: QueryCancellationReason);
}

#[derive(Clone)]
pub struct QueryControlService {
    port: Arc<dyn QueryControlPort>,
}

impl QueryControlService {
    pub fn new(port: Arc<dyn QueryControlPort>) -> Self {
        Self { port }
    }

    pub fn register_session(
        &self,
        identity: SessionIdentity,
    ) -> Result<QuerySessionLease, QueryControlError> {
        let token = self.port.register_session(identity)?;
        Ok(QuerySessionLease {
            service: self.clone(),
            token,
            released: false,
        })
    }

    pub fn begin_statement(
        &self,
        session: SessionToken,
    ) -> Result<ActiveStatementLease, QueryControlError> {
        let registration = self.port.begin_statement(session)?;
        Ok(ActiveStatementLease {
            service: self.clone(),
            registration,
            finished: false,
        })
    }

    pub fn cancel_session_statement(
        &self,
        session: SessionToken,
        reason: QueryCancellationReason,
    ) -> QueryCancelOutcome {
        self.port.cancel_session_statement(session, reason)
    }

    pub fn kill_query(
        &self,
        requester: SessionToken,
        target_connection_id: u32,
    ) -> QueryCancelOutcome {
        self.port.kill_query(requester, target_connection_id)
    }

    pub fn cancel_all(&self, reason: QueryCancellationReason) {
        self.port.cancel_all(reason);
    }

    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        Self::new(Arc::new(TestQueryControlPort))
    }
}

pub struct QuerySessionLease {
    service: QueryControlService,
    token: SessionToken,
    released: bool,
}

impl QuerySessionLease {
    pub const fn token(&self) -> SessionToken {
        self.token
    }

    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if !self.released {
            self.service.port.unregister_session(self.token);
            self.released = true;
        }
    }
}

impl Drop for QuerySessionLease {
    fn drop(&mut self) {
        self.release_inner();
    }
}

pub struct ActiveStatementLease {
    service: QueryControlService,
    registration: StatementRegistration,
    finished: bool,
}

impl ActiveStatementLease {
    pub const fn token(&self) -> StatementToken {
        self.registration.token()
    }

    pub fn cancellation(&self) -> &QueryCancellationView {
        self.registration.cancellation()
    }

    pub fn finish(&mut self) -> StatementFinishOutcome {
        if self.finished {
            return StatementFinishOutcome::Stale;
        }
        self.finished = true;
        self.service
            .port
            .finish_statement(self.registration.token())
    }
}

impl Drop for ActiveStatementLease {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

#[cfg(test)]
struct TestQueryControlPort;

#[cfg(test)]
impl QueryControlPort for TestQueryControlPort {
    fn register_session(
        &self,
        _identity: SessionIdentity,
    ) -> Result<SessionToken, QueryControlError> {
        Err(QueryControlError::UnknownSession)
    }

    fn unregister_session(&self, _token: SessionToken) {}

    fn begin_statement(
        &self,
        _session: SessionToken,
    ) -> Result<StatementRegistration, QueryControlError> {
        Err(QueryControlError::UnknownSession)
    }

    fn finish_statement(&self, _statement: StatementToken) -> StatementFinishOutcome {
        StatementFinishOutcome::Stale
    }

    fn cancel_session_statement(
        &self,
        _session: SessionToken,
        _reason: QueryCancellationReason,
    ) -> QueryCancelOutcome {
        QueryCancelOutcome::UnknownSession
    }

    fn kill_query(
        &self,
        _requester: SessionToken,
        _target_connection_id: u32,
    ) -> QueryCancelOutcome {
        QueryCancelOutcome::UnknownSession
    }

    fn cancel_all(&self, _reason: QueryCancellationReason) {}
}

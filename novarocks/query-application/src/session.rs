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

use std::sync::Arc;

use async_trait::async_trait;

use crate::cancellation::QueryCancellationReason;
use crate::protocol_delivery::QuerySessionOutput;
use crate::query_control::ClientQueryClaim;
use crate::session_control::ClientQueryId;
use crate::session_effects::SessionEffects;
use crate::session_error::QueryServiceError;
use crate::session_value::{SessionValue, StatementSessionError};
use crate::statement_kind::StatementKind;

/// Role-local lifetime that remains live until a protocol adapter has reached
/// its terminal wire outcome.
pub trait SessionProtocolTerminal: Send {
    fn complete(self: Box<Self>);
}

struct OutputOwnsProtocolTerminal;

impl SessionProtocolTerminal for OutputOwnsProtocolTerminal {
    fn complete(self: Box<Self>) {}
}

/// Move-only session output together with the exact terminal lifecycle the
/// protocol adapter must consume after its final wire outcome.
#[must_use = "the session output must be consumed by a protocol terminal"]
pub struct QuerySessionStatement {
    output: QuerySessionOutput,
    terminal: Box<dyn SessionProtocolTerminal>,
    effects: SessionEffects,
    kind: StatementKind,
}

impl QuerySessionStatement {
    pub fn new(
        mut output: QuerySessionOutput,
        terminal: Box<dyn SessionProtocolTerminal>,
        kind: StatementKind,
    ) -> Self {
        let effects = match &mut output {
            QuerySessionOutput::GovernedCompletion(completion) => completion.take_session_effects(),
            _ => SessionEffects::default(),
        };
        Self {
            output,
            terminal,
            effects,
            kind,
        }
    }
    /// The result owns the original terminal lifecycle; syntax supplies its explicit kind.
    pub fn output_owned(output: QuerySessionOutput, kind: StatementKind) -> Self {
        Self::new(output, Box::new(OutputOwnsProtocolTerminal), kind)
    }
    pub fn with_trino_statement_source(
        mut self,
        source: crate::sql::trino_admission::TrinoStatementSource,
    ) -> Self {
        if let QuerySessionOutput::GovernedError(error) = &mut self.output {
            error.attach_trino_statement_source(source);
        }
        self
    }

    pub fn session_effects(&self) -> &SessionEffects {
        &self.effects
    }
    pub const fn statement_kind(&self) -> StatementKind {
        self.kind
    }
    pub fn into_parts(self) -> (QuerySessionOutput, Box<dyn SessionProtocolTerminal>) {
        (self.output, self.terminal)
    }
    pub fn into_parts_with_session(
        self,
    ) -> (
        QuerySessionOutput,
        Box<dyn SessionProtocolTerminal>,
        SessionEffects,
        StatementKind,
    ) {
        (self.output, self.terminal, self.effects, self.kind)
    }
}

/// Static output admitted by the parser owner. CALL remains unresolved until
/// its application route selects the actual row or command carrier.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementOutputShape {
    ResultSet,
    Empty,
    Rows,
    UndeterminedCall,
}

impl StatementOutputShape {
    pub fn command_row_field(self) -> Option<crate::api::ResultField> {
        (self == Self::Rows).then(|| {
            crate::api::ResultField::new(
                "rows",
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    true,
                ),
            )
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatementAdmission {
    pub kind: StatementKind,
    pub shape: StatementOutputShape,
}
/// One original statement retains its immutable admission fact. Observation
/// grants neither execution nor access to the statement owner.
#[derive(Clone)]
pub struct StatementAdmissionPublisher(tokio::sync::watch::Sender<Option<StatementAdmission>>);
impl Default for StatementAdmissionPublisher {
    fn default() -> Self {
        Self(tokio::sync::watch::channel(None).0)
    }
}
impl StatementAdmissionPublisher {
    pub fn publish(&self, admission: StatementAdmission) {
        self.0.send_if_modified(|slot| {
            if let Some(original) = slot {
                assert_eq!(*original, admission, "one original statement admission");
                false
            } else {
                *slot = Some(admission);
                true
            }
        });
    }
    pub fn current(&self) -> Option<StatementAdmission> {
        *self.0.borrow()
    }
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<Option<StatementAdmission>> {
        self.0.subscribe()
    }
}

/// One client statement supplies its whole session, independent of a connection.
pub struct StatementSessionRequest {
    client_query_id: ClientQueryId,
    principal: Arc<str>,
    session: SessionValue,
    admission: StatementAdmissionPublisher,
    client_query_claim: Option<ClientQueryClaim>,
}
impl StatementSessionRequest {
    pub fn new(
        client_query_id: ClientQueryId,
        principal: impl Into<Arc<str>>,
        session: SessionValue,
    ) -> Self {
        Self {
            client_query_id,
            principal: principal.into(),
            session,
            admission: StatementAdmissionPublisher::default(),
            client_query_claim: None,
        }
    }
    pub fn with_client_query_claim(mut self, claim: ClientQueryClaim) -> Self {
        self.client_query_claim = Some(claim);
        self
    }
    pub fn admission(&self) -> StatementAdmissionPublisher {
        self.admission.clone()
    }
    pub fn into_parts(self) -> (ClientQueryId, Arc<str>, SessionValue) {
        (self.client_query_id, self.principal, self.session)
    }
    pub fn into_parts_with_client_query_claim(
        self,
    ) -> (
        ClientQueryId,
        Arc<str>,
        SessionValue,
        Option<ClientQueryClaim>,
    ) {
        (
            self.client_query_id,
            self.principal,
            self.session,
            self.client_query_claim,
        )
    }
}

/// Application-owned client session consumed by a protocol adapter.
#[async_trait]
pub trait QuerySession: Send + Sync + 'static {
    /// Execute the one statement admitted with this original client session.
    /// Query Application owns framing, admission and the terminal lifecycle.
    async fn execute_statement(
        &self,
        sql: &str,
    ) -> Result<QuerySessionStatement, QueryServiceError>;

    fn cancel_current(&self, reason: QueryCancellationReason);

    fn close(&self);
}

pub trait QuerySessionFactory: Send + Sync + 'static {
    fn open_statement_session(
        &self,
        request: StatementSessionRequest,
    ) -> Result<Arc<dyn QuerySession>, StatementSessionError>;

    fn cancel_all(&self, reason: QueryCancellationReason);
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct ProbeTerminal(Arc<AtomicBool>);

    impl SessionProtocolTerminal for ProbeTerminal {
        fn complete(self: Box<Self>) {
            self.0.store(true, Ordering::Release);
        }
    }

    #[test]
    fn statement_carries_terminal_until_the_adapter_consumes_it() {
        let completed = Arc::new(AtomicBool::new(false));
        let statement = QuerySessionStatement::new(
            QuerySessionOutput::Ok,
            Box::new(ProbeTerminal(Arc::clone(&completed))),
            StatementKind::Empty,
        );

        let (output, terminal) = statement.into_parts();
        assert!(matches!(output, QuerySessionOutput::Ok));
        assert!(!completed.load(Ordering::Acquire));
        terminal.complete();
        assert!(completed.load(Ordering::Acquire));
    }
    #[tokio::test]
    async fn original_admission_observation_precedes_statement_completion() {
        let publisher = StatementAdmissionPublisher::default();
        let mut observer = publisher.subscribe();
        let fact = StatementAdmission {
            kind: StatementKind::Analyze,
            shape: StatementOutputShape::Rows,
        };
        publisher.publish(fact);
        observer.changed().await.unwrap();
        assert_eq!(*observer.borrow_and_update(), Some(fact));
        publisher.publish(fact);
        assert!(!observer.has_changed().unwrap());
    }
}

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
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StatementAdmission {
    pub kind: StatementKind,
    pub shape: StatementOutputShape,
}
// Design: ADR-0217 (docs/adr/ADR-0217-sole-trino-entry-and-application-owned-completion.md)
impl StatementAdmission {
    pub fn from_statement(statement: &novarocks_parser::ast::Statement) -> Self {
        use novarocks_parser::ast::{DmlStatement, Statement};
        let kind = StatementKind::from_statement(statement);
        let shape = if matches!(
            statement,
            Statement::Dml(DmlStatement::CreateTableAsSelect(_))
        ) || matches!(
            kind,
            StatementKind::Insert
                | StatementKind::Delete
                | StatementKind::Update
                | StatementKind::Merge
                | StatementKind::RefreshMaterializedView
                | StatementKind::Analyze
        ) {
            StatementOutputShape::Rows
        } else if kind.is_row_kind()
            || kind == StatementKind::Optimize
            || matches!(
                statement,
                Statement::Iceberg(novarocks_parser::ast::IcebergStatement::AlterTable(alter))
                    if matches!(&alter.action, novarocks_parser::ast::IcebergTableAction::AddFiles(_))
            )
        {
            // ADD FILES already returns the original domain status result set.
            // It is not a counted command and must not discard those rows.
            StatementOutputShape::ResultSet
        } else if kind == StatementKind::Call {
            StatementOutputShape::UndeterminedCall
        } else {
            StatementOutputShape::Empty
        };
        Self { kind, shape }
    }
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
        }
    }
    pub fn admission(&self) -> StatementAdmissionPublisher {
        self.admission.clone()
    }
    pub fn into_parts(self) -> (ClientQueryId, Arc<str>, SessionValue) {
        (self.client_query_id, self.principal, self.session)
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
    #[test]
    fn admitted_shape_distinguishes_ctas_and_preserves_call_uncertainty() {
        fn admission(sql: &str) -> StatementAdmission {
            let statements = novarocks_parser::parse(sql).unwrap();
            assert_eq!(statements.len(), 1);
            StatementAdmission::from_statement(&statements[0])
        }
        let create = admission("CREATE TABLE t (v BIGINT)");
        let ctas = admission("CREATE TABLE IF NOT EXISTS t AS SELECT 1");
        assert_eq!(create.kind, ctas.kind);
        assert_eq!(create.shape, StatementOutputShape::Empty);
        assert_eq!(ctas.shape, StatementOutputShape::Rows);
        for sql in [
            "INSERT INTO t SELECT 1",
            "INSERT OVERWRITE t SELECT 1",
            "DELETE FROM t",
            "UPDATE t SET v = 1",
            "MERGE INTO t USING src s ON t.id = s.id WHEN MATCHED THEN DELETE",
            "ANALYZE TABLE t",
            "REFRESH MATERIALIZED VIEW mv",
        ] {
            assert_eq!(admission(sql).shape, StatementOutputShape::Rows, "{sql}");
        }
        assert_eq!(admission("SELECT 1").shape, StatementOutputShape::ResultSet);
        let add_files = admission("ALTER TABLE t ADD FILES FROM 's3://bucket/input'");
        assert_eq!(add_files.kind, StatementKind::AddFiles);
        assert_eq!(add_files.shape, StatementOutputShape::ResultSet);
        assert!(
            !add_files.kind.is_row_kind(),
            "business classification is unchanged"
        );
        for sql in [
            "ALTER TABLE t ADD EQUALITY DELETE (id) VALUES (2)",
            "TRUNCATE TABLE t",
        ] {
            assert_eq!(admission(sql).shape, StatementOutputShape::Empty, "{sql}");
        }
        assert_eq!(
            admission("CALL system.expire_snapshots('t')").shape,
            StatementOutputShape::UndeterminedCall
        );
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

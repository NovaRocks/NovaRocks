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
use crate::client_connection::ClientConnectionToken;
use crate::protocol_delivery::QuerySessionOutput;
use crate::session_error::QueryServiceError;

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
}

impl QuerySessionStatement {
    pub fn new(output: QuerySessionOutput, terminal: Box<dyn SessionProtocolTerminal>) -> Self {
        Self { output, terminal }
    }

    /// Use when the output itself owns every statement lifetime, such as a
    /// governed query result whose protocol owner is embedded in the output.
    pub fn output_owned(output: QuerySessionOutput) -> Self {
        Self::new(output, Box::new(OutputOwnsProtocolTerminal))
    }

    pub fn into_parts(self) -> (QuerySessionOutput, Box<dyn SessionProtocolTerminal>) {
        (self.output, self.terminal)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerySessionOpenRequest {
    connection: ClientConnectionToken,
    principal: Arc<str>,
}

impl QuerySessionOpenRequest {
    pub fn new(connection: ClientConnectionToken, principal: impl Into<Arc<str>>) -> Self {
        Self {
            connection,
            principal: principal.into(),
        }
    }

    pub const fn connection_id(&self) -> u32 {
        self.connection.connection_id()
    }
    pub const fn connection_token(&self) -> ClientConnectionToken {
        self.connection
    }
    pub fn principal(&self) -> &str {
        &self.principal
    }
}

/// Application-owned client session consumed by a protocol adapter.
#[async_trait]
pub trait QuerySession: Send + Sync + 'static {
    /// Handles a COM_INIT_DB request. Like COM_QUERY, the result retains its
    /// governed statement owner until the protocol adapter has written the
    /// terminal packet.
    async fn init_database(&self, schema: &str)
    -> Result<QuerySessionStatement, QueryServiceError>;

    /// Executes one protocol-framed SQL fragment. Query Application validates
    /// it as exactly one statement; protocol adapters use this for negotiated
    /// multi-statement requests so statement ordering remains application
    /// owned.
    async fn execute_statement(
        &self,
        sql: &str,
    ) -> Result<QuerySessionStatement, QueryServiceError>;

    /// Executes an unnegotiated COM_QUERY request. Implementations must reject
    /// more than one executable SQL statement.
    async fn execute_batch(&self, sql: &str) -> Result<QuerySessionStatement, QueryServiceError>;

    fn cancel_current(&self, reason: QueryCancellationReason);

    fn close(&self);
}

pub trait QuerySessionFactory: Send + Sync + 'static {
    fn open_session(
        &self,
        request: QuerySessionOpenRequest,
    ) -> Result<Arc<dyn QuerySession>, QueryServiceError>;

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
    fn admission_request_retains_exact_connection_identity_and_principal() {
        let request = QuerySessionOpenRequest::new(
            ClientConnectionToken::new(42, 7).expect("connection token"),
            "alice",
        );

        assert_eq!(request.connection_id(), 42);
        assert_eq!(request.connection_token().generation(), 7);
        assert_eq!(request.principal(), "alice");
    }

    #[test]
    fn statement_carries_terminal_until_the_adapter_consumes_it() {
        let completed = Arc::new(AtomicBool::new(false));
        let statement = QuerySessionStatement::new(
            QuerySessionOutput::Ok,
            Box::new(ProbeTerminal(Arc::clone(&completed))),
        );

        let (output, terminal) = statement.into_parts();
        assert!(matches!(output, QuerySessionOutput::Ok));
        assert!(!completed.load(Ordering::Acquire));
        terminal.complete();
        assert!(completed.load(Ordering::Acquire));
    }
}

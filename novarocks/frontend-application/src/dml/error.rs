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

use std::fmt;

use novarocks_query_application::engine_error::EngineErrorCode;
use novarocks_spi::connector::{LakePublicationDisposition, LakePublicationTerminal};
use novarocks_sql::analyze_error::AnalyzeError;
use novarocks_user_error::UserError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DmlErrorKind {
    Executor,
    Commit,
    CommittedButUnfinalized,
    Admission,
}

#[derive(Debug)]
pub struct DmlError {
    kind: DmlErrorKind,
    message: String,
    publication_terminal: Option<LakePublicationTerminal>,
    user_error: Option<UserError>,
    engine_error_code: Option<EngineErrorCode>,
}

/// DML-local carrier for a SQL analysis error before the frontend client
/// boundary renders it as a [`UserError`].
///
/// The carrier intentionally keeps non-analysis failures as opaque engine
/// text. It must not infer a user-facing code from that text.
#[derive(Debug)]
pub enum DmlExecutionError {
    Engine(String),
    Analyze(AnalyzeError),
}

impl DmlExecutionError {
    pub(crate) fn from_compile(error: novarocks_sql::compiler::SqlCompileError) -> Self {
        match error {
            novarocks_sql::compiler::SqlCompileError::Analyze(error) => Self::Analyze(error),
            error => Self::Engine(error.to_string()),
        }
    }

    pub(crate) fn into_dml_error(self, source: Option<&str>) -> DmlError {
        match self {
            Self::Engine(error) => DmlError::executor(error),
            Self::Analyze(error) => DmlError::admit(error.to_user_error(source)),
        }
    }
}

impl From<String> for DmlExecutionError {
    fn from(error: String) -> Self {
        Self::Engine(error)
    }
}

impl From<&str> for DmlExecutionError {
    fn from(error: &str) -> Self {
        Self::Engine(error.to_string())
    }
}

impl From<crate::query_execution::planning::time_travel::TimeTravelRewriteError>
    for DmlExecutionError
{
    fn from(error: crate::query_execution::planning::time_travel::TimeTravelRewriteError) -> Self {
        match error {
            crate::query_execution::planning::time_travel::TimeTravelRewriteError::Engine(
                error,
            ) => Self::Engine(error),
            crate::query_execution::planning::time_travel::TimeTravelRewriteError::Analyze(
                error,
            ) => Self::Analyze(error),
        }
    }
}

impl std::fmt::Display for DmlExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Engine(error) => formatter.write_str(error),
            Self::Analyze(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DmlExecutionError {}

impl DmlError {
    pub(crate) fn new(kind: DmlErrorKind, error: impl fmt::Display) -> Self {
        Self {
            kind,
            message: error.to_string(),
            publication_terminal: None,
            user_error: None,
            engine_error_code: None,
        }
    }

    pub(crate) fn executor(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Executor, error)
    }

    pub(crate) fn commit(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Commit, error)
    }

    /// Attaches a terminal produced by the statement-local publication state
    /// machine. This additive API deliberately does not infer a disposition
    /// from the broad DML error kind.
    pub(crate) fn with_publication_terminal(mut self, terminal: LakePublicationTerminal) -> Self {
        self.publication_terminal = Some(terminal);
        self
    }

    /// Reports a local finalization failure after the Catalog publication is
    /// already known committed. The explicit terminal remains authoritative
    /// and keeps retry disabled.
    pub(crate) fn known_committed_finalization_failed(
        terminal: LakePublicationTerminal,
        error: impl fmt::Display,
    ) -> Self {
        debug_assert_eq!(
            terminal.disposition(),
            LakePublicationDisposition::KnownCommitted,
            "known-committed finalization errors require a committed terminal"
        );
        Self {
            kind: DmlErrorKind::CommittedButUnfinalized,
            message: format!("{error}; publication is known committed; do not retry statement"),
            publication_terminal: Some(terminal),
            user_error: None,
            engine_error_code: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn admission(error: impl fmt::Display) -> Self {
        Self::new(DmlErrorKind::Admission, error)
    }

    /// Carries an admission error across DML boundaries without losing its code or location.
    pub(crate) fn admit(error: UserError) -> Self {
        Self {
            kind: DmlErrorKind::Admission,
            message: error.to_string(),
            publication_terminal: None,
            user_error: Some(error),
            engine_error_code: None,
        }
    }

    /// Carries an explicit engine-level code to the SQL boundary. DML must
    /// never reconstruct these codes by parsing its own display text.
    pub(crate) fn with_engine_error_code(mut self, code: EngineErrorCode) -> Self {
        self.engine_error_code = Some(code);
        self
    }

    pub const fn kind(&self) -> DmlErrorKind {
        self.kind
    }

    pub fn user_error(&self) -> Option<&UserError> {
        self.user_error.as_ref()
    }

    pub(crate) const fn engine_error_code(&self) -> Option<EngineErrorCode> {
        self.engine_error_code
    }

    pub fn publication_terminal(&self) -> Option<&LakePublicationTerminal> {
        self.publication_terminal.as_ref()
    }
}

impl fmt::Display for DmlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}: {}", self.kind, self.message)?;
        if let Some(terminal) = &self.publication_terminal {
            let target = terminal.target();
            write!(
                formatter,
                " (lake publication id={} family={} target={}.{}{}{} disposition={:?} next_action={:?} do_not_retry={})",
                terminal.header().publication_id(),
                terminal.header().family(),
                target.catalog(),
                target.namespace(),
                target
                    .table()
                    .map(|table| format!(".{table}"))
                    .unwrap_or_default(),
                target
                    .reference()
                    .map(|reference| format!("@{reference}"))
                    .unwrap_or_default(),
                terminal.disposition(),
                terminal.next_action(),
                terminal.do_not_retry(),
            )?;
        }
        Ok(())
    }
}

impl std::error::Error for DmlError {}

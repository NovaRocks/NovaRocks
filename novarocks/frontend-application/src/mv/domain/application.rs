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

//! Materialized-view application and engine ports.

use std::{fmt, sync::Arc};

use novarocks_mv_application::engine_properties::MvRefreshPropertyChanges;
use novarocks_mv_application::persistence::definition::CreateMvDefinitionRequest;
use novarocks_mv_application::persistence::dependency::CreateMvDependencyRequest;
use novarocks_mv_application::repository::InitialMvRefreshConfiguration;
use novarocks_parser::ast::Query;
use novarocks_spi::connector::{ConnectorProviderBindingKey, DdlPropertyChange, DdlPropertyValue};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvDropStatement {
    pub name_parts: Vec<String>,
    pub if_exists: bool,
}

/// Frontend-owned admitted materialized-view alteration.
#[derive(Clone, Debug, PartialEq)]
pub enum MvAlterAction {
    RefreshProperties(MvRefreshPropertyChanges),
    RefreshAndStorageProperties {
        changes: MvRefreshPropertyChanges,
        owner: ConnectorProviderBindingKey,
        properties: Arc<[(Arc<str>, DdlPropertyChange)]>,
    },
    StorageProperties {
        owner: ConnectorProviderBindingKey,
        properties: Arc<[(Arc<str>, DdlPropertyChange)]>,
    },
}

/// Inputs to the managed replacement of the default spec and its data.
#[derive(Clone, Debug)]
pub(crate) struct MvRepartitionFields {
    pub(crate) owner: ConnectorProviderBindingKey,
    pub(crate) properties: Arc<[(Arc<str>, DdlPropertyChange)]>,
    pub(crate) partitioning_index: usize,
    pub(crate) configuration: Option<MvRefreshPropertyChanges>,
}

/// Frontend-owned request for altering one admitted materialized view.
#[derive(Clone, Debug, PartialEq)]
pub struct MvAlterStatement {
    pub name_parts: Vec<String>,
    pub action: MvAlterAction,
}

// Design: ADR-0101 (docs/adr/ADR-0101-native-sql-language-authority-and-owner-boundaries.md)
/// Frontend-owned refresh request. The request keeps the admitted target name
/// distinct from SQL planning's immutable refresh semantic value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvRefreshRequest {
    pub name_parts: Vec<String>,
    pub full: bool,
}

impl MvRefreshRequest {
    pub fn sql_refresh_statement(&self) -> novarocks_sql::planning::mv::MvRefreshStatement {
        novarocks_sql::planning::mv::MvRefreshStatement {
            name_parts: self.name_parts.clone(),
            full: self.full,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MvCreateStorage {
    pub target: crate::mv::domain::refresh::target::IcebergMvTarget,
    pub binding: ConnectorProviderBindingKey,
    pub properties: Arc<[(Arc<str>, DdlPropertyValue)]>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MvCreateStatement {
    pub if_not_exists: bool,
    pub refresh: InitialMvRefreshConfiguration,
    pub select_sql: String,
    pub select_query: Query,
    pub storage: MvCreateStorage,
    pub primary_key: Option<Vec<String>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MvApplicationErrorKind {
    InvalidRequest,
    Engine,
    Unavailable,
    /// An exact target binding or source snapshot captured by preparation
    /// changed before provider dispatch. A fresh preparation may be attempted.
    BindingInvalidated,
    AlreadyActive,
    ShutdownCancelled,
    CommitUnknown,
    KnownCommittedFinalizeFailed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MvApplicationError {
    kind: MvApplicationErrorKind,
    message: String,
    original_failure: Option<novarocks_failure_contract::SharedFailureCause>,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
    analyze_error: Option<novarocks_sql::analyze_error::AnalyzeError>,
}

impl MvApplicationError {
    pub fn new(kind: MvApplicationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            original_failure: None,
            compile_control: None,
            analyze_error: None,
        }
    }

    /// Preserve the refresh product's original scheduler classification.
    pub(crate) fn from_refresh_product(
        error: &novarocks_mv_application::product::MvProductError,
    ) -> Self {
        use novarocks_mv_application::product::MvProductErrorKind as Kind;
        let kind = match error.kind() {
            Kind::KnownCommittedFinalizeFailed => {
                MvApplicationErrorKind::KnownCommittedFinalizeFailed
            }
            Kind::Conflict => MvApplicationErrorKind::AlreadyActive,
            Kind::Unavailable => MvApplicationErrorKind::Unavailable,
            Kind::InvalidRequest => MvApplicationErrorKind::InvalidRequest,
            Kind::CommitUnknown => MvApplicationErrorKind::CommitUnknown,
            Kind::TargetReplaced => MvApplicationErrorKind::BindingInvalidated,
            Kind::ProviderKnownUncommitted | Kind::Corruption | Kind::ShutdownCancelled => {
                MvApplicationErrorKind::Engine
            }
        };
        let mut failure = Self::new(kind, error.to_string())
            .with_compile_control(error.compile_control_error())
            .with_analyze_error(error.analyze_error().cloned());
        if let Some(cause) = error.original_failure() {
            failure = failure.with_original_failure(cause.clone());
        }
        failure
    }

    /// Preserve the Connector author's original cause without deriving business
    /// disposition or compilation authority from it.
    pub fn from_connector(
        kind: MvApplicationErrorKind,
        error: novarocks_spi::connector::ConnectorError,
    ) -> Self {
        let message = error.to_string();
        let original = std::error::Error::source(&error)
            .and_then(|source| {
                source.downcast_ref::<novarocks_failure_contract::SharedFailureCause>()
            })
            .cloned()
            .unwrap_or_else(|| novarocks_failure_contract::SharedFailureCause::new(error));
        Self::new(kind, message).with_original_failure(original)
    }

    pub(crate) fn from_compile(error: novarocks_sql::compiler::SqlCompileError) -> Self {
        let control = match &error {
            novarocks_sql::compiler::SqlCompileError::Cancelled => {
                Some(novarocks_type_contract::CompileControlError::Cancelled)
            }
            novarocks_sql::compiler::SqlCompileError::DeadlineExceeded => {
                Some(novarocks_type_contract::CompileControlError::DeadlineExceeded)
            }
            novarocks_sql::compiler::SqlCompileError::ResourceExhausted => {
                Some(novarocks_type_contract::CompileControlError::ResourceExhausted)
            }
            novarocks_sql::compiler::SqlCompileError::Analyze(error) => error.control_error(),
            novarocks_sql::compiler::SqlCompileError::CompilationSource(source) => Some(
                novarocks_type_contract::CompileControlError::Source(source.clone()),
            ),
            _ => None,
        };
        let analyze_error = match &error {
            novarocks_sql::compiler::SqlCompileError::Analyze(error) => Some(error.clone()),
            _ => None,
        };
        // Public SQL codes are rendered only at the final typed command boundary.
        let message = error.to_string();
        let mut failure = Self::new(MvApplicationErrorKind::InvalidRequest, message);
        if let novarocks_sql::compiler::SqlCompileError::CompilationSource(source) = &error {
            failure.original_failure = Some(source.clone());
        }
        failure.compile_control = control;
        failure.analyze_error = analyze_error;
        failure
    }
    pub(crate) fn with_compile_control(
        mut self,
        error: Option<novarocks_type_contract::CompileControlError>,
    ) -> Self {
        self.compile_control = error;
        self
    }
    pub fn with_original_failure(
        mut self,
        cause: novarocks_failure_contract::SharedFailureCause,
    ) -> Self {
        self.original_failure = Some(cause);
        self
    }

    pub fn original_failure(&self) -> Option<&novarocks_failure_contract::SharedFailureCause> {
        self.original_failure.as_ref()
    }

    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control.clone()
    }

    /// Retain the original typed analysis cause, never classify diagnostic text.
    pub(crate) fn with_analyze_error(
        mut self,
        error: Option<novarocks_sql::analyze_error::AnalyzeError>,
    ) -> Self {
        if matches!(
            self.kind,
            MvApplicationErrorKind::CommitUnknown
                | MvApplicationErrorKind::KnownCommittedFinalizeFailed
        ) {
            self.analyze_error = None;
            return self;
        }
        self.analyze_error = error;
        self
    }

    pub fn analyze_error(&self) -> Option<&novarocks_sql::analyze_error::AnalyzeError> {
        self.analyze_error.as_ref()
    }

    pub fn kind(&self) -> MvApplicationErrorKind {
        self.kind
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for MvApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for MvApplicationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.original_failure.as_ref().map(|cause| cause as _)
    }
}

/// CREATE-only facts used to assemble canonical lake documents. This is not a
/// StateStore command: durable projection is constructed only from a later
/// exact lake observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvCreateProjectionSeed {
    pub definition: CreateMvDefinitionRequest,
    pub refresh: InitialMvRefreshConfiguration,
    pub dependencies: Vec<CreateMvDependencyRequest>,
}

#[cfg(test)]
mod compile_error_tests {
    use super::*;
    use novarocks_functions::{FunctionBindingError, FunctionOverloadId};
    use novarocks_sql::{analyze_error::AnalyzeError, compiler::SqlCompileError};
    use novarocks_type_contract::CompileControlError;

    #[test]
    fn mv_application_and_create_preserve_the_original_analysis_code() {
        let compile = SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
            FunctionOverloadId::try_new("fixture/mv/missing-pure-v1").unwrap(),
        ));
        let SqlCompileError::Analyze(ref analyze) = compile else {
            panic!("missing implementation must retain typed analysis identity");
        };
        let expected = analyze.to_string();
        let expected_analyze = analyze.clone();
        let application = MvApplicationError::from_compile(compile.clone());
        assert_eq!(application.kind(), MvApplicationErrorKind::InvalidRequest);
        assert_eq!(application.analyze_error(), Some(&expected_analyze));
        assert_eq!(application.message(), expected);
        assert!(
            !application
                .message()
                .starts_with("[sql.analyze.unavailable_implementation] ")
        );
        assert_eq!(application.compile_control_error(), None);
    }

    #[test]
    fn mv_application_and_create_keep_the_original_compile_source() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        for original in [
            novarocks_plan_codec::PhysicalEncodeError::UnsupportedCapability("router".into()),
            novarocks_plan_codec::PhysicalEncodeError::Invalid("unsupported diagnostic".into()),
        ] {
            let source = SharedFailureCause::new(original);
            let fact = source.failure_fact();
            let compile = SqlCompileError::CompilationSource(source.clone());
            let application = MvApplicationError::from_compile(compile.clone());
            assert_eq!(application.original_failure(), Some(&source));
            assert_eq!(
                std::error::Error::source(&application)
                    .and_then(|cause| cause.downcast_ref::<SharedFailureCause>()),
                Some(&source),
            );
            let Some(CompileControlError::Source(carried)) = application.compile_control_error()
            else {
                panic!("keep the original compile source");
            };
            assert_eq!(
                carried, source,
                "SharedFailureCause equality is Arc identity"
            );
            assert_eq!(carried.failure_fact(), fact);
            assert!(
                carried
                    .downcast_ref::<novarocks_plan_codec::PhysicalEncodeError>()
                    .is_some()
            );
            assert_eq!(application.analyze_error(), None);
        }
    }

    #[test]
    fn mv_compile_projection_does_not_classify_unsupported_diagnostic_text() {
        let message = "unsupported diagnostic: UnsupportedCompiledShape";
        for compile in [
            SqlCompileError::Analyze(AnalyzeError::internal(message)),
            SqlCompileError::Compilation(message.into()),
        ] {
            let expected_analyze = match &compile {
                SqlCompileError::Analyze(error) => Some(error.clone()),
                _ => None,
            };
            let expected = compile.to_string();
            let application = MvApplicationError::from_compile(compile.clone());
            assert_eq!(application.message(), expected);
            assert_eq!(application.original_failure(), None);
            assert_eq!(application.compile_control_error(), None);
            assert_eq!(application.analyze_error(), expected_analyze.as_ref());
        }
    }

    #[test]
    fn mv_compile_projection_preserves_all_three_control_causes_and_original_messages() {
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for compile in [
                SqlCompileError::from(control.clone()),
                SqlCompileError::Analyze(AnalyzeError::control(control.clone())),
            ] {
                let expected_analyze = match &compile {
                    SqlCompileError::Analyze(error) => Some(error.clone()),
                    _ => None,
                };
                let expected = compile.to_string();
                let application = MvApplicationError::from_compile(compile.clone());
                assert_eq!(application.message(), expected);
                assert_eq!(application.original_failure(), None);
                assert_eq!(application.compile_control_error(), Some(control.clone()));
                assert_eq!(application.analyze_error(), expected_analyze.as_ref());
                assert_eq!(application.kind(), MvApplicationErrorKind::InvalidRequest);
            }
        }
    }

    #[test]
    fn connector_projection_keeps_original_shared_cause_and_caller_kind() {
        use novarocks_failure_contract::{FailureProjection, SharedFailureCause};
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let error = ConnectorError::from_domain(
            ConnectorErrorKind::ResourceExhausted,
            novarocks_workload_control::WorkError::Capacity("refresh pin source refusal"),
        );
        let original = std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<SharedFailureCause>())
            .expect("actual Connector domain source")
            .clone();
        let fact = original.failure_fact();
        let message = error.to_string();
        let projected = MvApplicationError::from_connector(MvApplicationErrorKind::Engine, error);
        assert_eq!(projected.kind(), MvApplicationErrorKind::Engine);
        assert_eq!(projected.message(), message);
        assert_eq!(projected.original_failure(), Some(&original));
        assert_eq!(
            std::error::Error::source(&projected)
                .and_then(|source| source.downcast_ref::<SharedFailureCause>()),
            Some(&original),
        );
        assert_eq!(projected.original_failure().unwrap().failure_fact(), fact);
        assert!(
            projected
                .original_failure()
                .unwrap()
                .downcast_ref::<novarocks_workload_control::WorkError>()
                .is_some()
        );
        assert_eq!(projected.compile_control_error(), None);
        assert_eq!(projected.analyze_error(), None);
    }

    #[test]
    fn plain_connector_is_captured_once_as_the_actual_error() {
        use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
        let error =
            ConnectorError::new(ConnectorErrorKind::Unavailable, "refresh provider offline");
        assert!(std::error::Error::source(&error).is_none());
        let message = error.to_string();
        let projected =
            MvApplicationError::from_connector(MvApplicationErrorKind::Unavailable, error);
        let original = projected.original_failure().expect("captured actual error");
        let actual = original
            .downcast_ref::<ConnectorError>()
            .expect("no synthetic wrapper");
        assert_eq!(actual.kind(), ConnectorErrorKind::Unavailable);
        assert_eq!(actual.to_string(), message);
        assert_eq!(projected.clone().original_failure(), Some(original));
        assert_eq!(projected.kind(), MvApplicationErrorKind::Unavailable);
        assert_eq!(projected.compile_control_error(), None);
    }
}

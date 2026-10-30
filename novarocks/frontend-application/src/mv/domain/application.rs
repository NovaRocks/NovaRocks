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

use std::fmt;

use novarocks_mv_application::persistence::definition::CreateMvDefinitionRequest;
use novarocks_mv_application::persistence::dependency::CreateMvDependencyRequest;
use novarocks_mv_application::repository::InitialMvRefreshConfiguration;
use novarocks_parser::ast::{
    LiteralKind, MaterializedViewPartitionArgument, MaterializedViewPartitionField, Query,
};
use novarocks_sql::semantic::IcebergPartitionFieldExpr;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvCreatePartitionField {
    Identity { column: String },
    Year { column: String },
    Month { column: String },
    Day { column: String },
    Hour { column: String },
    Bucket { column: String, num_buckets: u32 },
    Truncate { column: String, width: u32 },
    Void { column: String },
}

impl From<&IcebergPartitionFieldExpr> for MvCreatePartitionField {
    fn from(value: &IcebergPartitionFieldExpr) -> Self {
        match value {
            IcebergPartitionFieldExpr::Identity { column } => Self::Identity {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Year { column } => Self::Year {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Month { column } => Self::Month {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Day { column } => Self::Day {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Hour { column } => Self::Hour {
                column: column.clone(),
            },
            IcebergPartitionFieldExpr::Bucket {
                column,
                num_buckets,
            } => Self::Bucket {
                column: column.clone(),
                num_buckets: *num_buckets,
            },
            IcebergPartitionFieldExpr::Truncate { column, width } => Self::Truncate {
                column: column.clone(),
                width: *width,
            },
            IcebergPartitionFieldExpr::Void { column } => Self::Void {
                column: column.clone(),
            },
        }
    }
}

impl From<&MaterializedViewPartitionField> for MvCreatePartitionField {
    fn from(value: &MaterializedViewPartitionField) -> Self {
        let (transform, arguments) = match value {
            MaterializedViewPartitionField::Identity(column) => {
                return Self::Identity {
                    column: normalized_partition_identifier(&column.value),
                };
            }
            MaterializedViewPartitionField::Transform {
                name, arguments, ..
            } => (name.value.to_ascii_lowercase(), arguments.as_slice()),
        };
        let column = partition_column_argument(arguments, &transform);
        match transform.as_str() {
            "identity" => Self::Identity { column },
            "year" => Self::Year { column },
            "month" => Self::Month { column },
            "day" => Self::Day { column },
            "hour" => Self::Hour { column },
            "void" => Self::Void { column },
            "bucket" => Self::Bucket {
                column,
                num_buckets: partition_u32_argument(arguments, "bucket"),
            },
            "truncate" => Self::Truncate {
                column,
                width: partition_u32_argument(arguments, "truncate"),
            },
            _ => unreachable!("MV partition transform was validated during SQL admission"),
        }
    }
}

impl From<&MvCreatePartitionField> for IcebergPartitionFieldExpr {
    fn from(value: &MvCreatePartitionField) -> Self {
        match value {
            MvCreatePartitionField::Identity { column } => Self::Identity {
                column: column.clone(),
            },
            MvCreatePartitionField::Year { column } => Self::Year {
                column: column.clone(),
            },
            MvCreatePartitionField::Month { column } => Self::Month {
                column: column.clone(),
            },
            MvCreatePartitionField::Day { column } => Self::Day {
                column: column.clone(),
            },
            MvCreatePartitionField::Hour { column } => Self::Hour {
                column: column.clone(),
            },
            MvCreatePartitionField::Bucket {
                column,
                num_buckets,
            } => Self::Bucket {
                column: column.clone(),
                num_buckets: *num_buckets,
            },
            MvCreatePartitionField::Truncate { column, width } => Self::Truncate {
                column: column.clone(),
                width: *width,
            },
            MvCreatePartitionField::Void { column } => Self::Void {
                column: column.clone(),
            },
        }
    }
}

fn normalized_partition_identifier(value: &str) -> String {
    novarocks_types::naming::normalize_identifier(value)
        .expect("MV partition identifier was validated during SQL admission")
}

fn partition_column_argument(
    arguments: &[MaterializedViewPartitionArgument],
    transform: &str,
) -> String {
    let Some(MaterializedViewPartitionArgument::Ident(column)) = arguments.first() else {
        unreachable!("MV {transform} partition transform was validated during SQL admission");
    };
    normalized_partition_identifier(&column.value)
}

fn partition_u32_argument(arguments: &[MaterializedViewPartitionArgument], transform: &str) -> u32 {
    let Some(MaterializedViewPartitionArgument::Literal(value)) = arguments.get(1) else {
        unreachable!("MV {transform} partition transform was validated during SQL admission");
    };
    let LiteralKind::Number(value) = &value.kind else {
        unreachable!("MV {transform} partition transform was validated during SQL admission");
    };
    let value = value
        .parse::<u32>()
        .expect("MV partition numeric argument was validated during SQL admission");
    assert!(
        value > 0,
        "MV partition numeric argument was validated during SQL admission"
    );
    value
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvCreateDistribution {
    pub hash_columns: Vec<String>,
    pub bucket_count: Option<u32>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum MvCreateRefreshPolicy {
    #[default]
    Manual,
    AsyncOnChange,
    AsyncInterval {
        interval_ms: i64,
    },
}

/// Frontend-owned request for dropping one admitted materialized view.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvDropStatement {
    pub name_parts: Vec<String>,
    pub if_exists: bool,
}

/// Frontend-owned admitted materialized-view alteration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MvAlterAction {
    SetRefresh(MvCreateRefreshPolicy),
    SetProperties(Vec<(String, String)>),
    PauseRefresh,
    ResumeRefresh,
    Repartition(Vec<MvCreatePartitionField>),
}

/// Frontend-owned request for altering one admitted materialized view.
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// Frontend-owned request for listing materialized views.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvShowStatement {
    pub database: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MvCreateStatement {
    pub name_parts: Vec<String>,
    pub if_not_exists: bool,
    pub partition_by: Option<Vec<MvCreatePartitionField>>,
    pub distribution: Option<MvCreateDistribution>,
    pub refresh_policy: MvCreateRefreshPolicy,
    pub select_sql: String,
    pub select_query: Query,
    pub properties: Vec<(String, String)>,
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
    unavailable_implementation: Option<novarocks_sql::analyze_error::AnalyzeError>,
}

impl MvApplicationError {
    pub fn new(kind: MvApplicationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            original_failure: None,
            compile_control: None,
            unavailable_implementation: None,
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
            .with_unavailable_implementation(error.unavailable_implementation().cloned());
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
        let unavailable_implementation = match &error {
            novarocks_sql::compiler::SqlCompileError::Analyze(error)
                if error.kind()
                    == novarocks_sql::analyze_error::AnalyzeErrorKind::UnavailableImplementation =>
            {
                Some(error.clone())
            }
            _ => None,
        };
        // Public SQL codes are rendered only at the final typed command boundary.
        let message = error.to_string();
        let mut failure = Self::new(MvApplicationErrorKind::InvalidRequest, message);
        if let novarocks_sql::compiler::SqlCompileError::CompilationSource(source) = &error {
            failure.original_failure = Some(source.clone());
        }
        failure.compile_control = control;
        failure.unavailable_implementation = unavailable_implementation;
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

    /// Retain only the typed missing-implementation cause, never diagnostic text.
    pub(crate) fn with_unavailable_implementation(
        mut self,
        error: Option<novarocks_sql::analyze_error::AnalyzeError>,
    ) -> Self {
        if matches!(
            self.kind,
            MvApplicationErrorKind::CommitUnknown
                | MvApplicationErrorKind::KnownCommittedFinalizeFailed
        ) {
            self.unavailable_implementation = None;
            return self;
        }
        self.unavailable_implementation = error.filter(|error| {
            error.kind()
                == novarocks_sql::analyze_error::AnalyzeErrorKind::UnavailableImplementation
        });
        self
    }

    pub fn unavailable_implementation(
        &self,
    ) -> Option<&novarocks_sql::analyze_error::AnalyzeError> {
        self.unavailable_implementation.as_ref()
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
    fn mv_application_and_create_preserve_only_the_missing_implementation_sql_code() {
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
        assert_eq!(
            application.unavailable_implementation(),
            Some(&expected_analyze)
        );
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
            assert_eq!(application.unavailable_implementation(), None);
        }
    }

    #[test]
    fn mv_compile_projection_does_not_classify_unsupported_diagnostic_text() {
        let message = "unsupported diagnostic: UnsupportedCompiledShape";
        for compile in [
            SqlCompileError::Analyze(AnalyzeError::internal(message)),
            SqlCompileError::Compilation(message.into()),
        ] {
            let expected = compile.to_string();
            let application = MvApplicationError::from_compile(compile.clone());
            assert_eq!(application.message(), expected);
            assert_eq!(application.original_failure(), None);
            assert_eq!(application.compile_control_error(), None);
            assert_eq!(application.unavailable_implementation(), None);
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
                let expected = compile.to_string();
                let application = MvApplicationError::from_compile(compile.clone());
                assert_eq!(application.message(), expected);
                assert_eq!(application.original_failure(), None);
                assert_eq!(application.compile_control_error(), Some(control.clone()));
                assert_eq!(application.unavailable_implementation(), None);
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
        assert_eq!(projected.unavailable_implementation(), None);
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

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

//! Typed materialized-view refresh lifecycle contracts.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;

use crate::mv::domain::application::MvRefreshRequest;
use crate::mv::domain::refresh::planning::RefreshPlanContract;
use novarocks_sql::planning::mv::SqlMvTarget as MvTarget;

#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub struct RefreshRequest {
    pub target: MvTarget,
    pub current_catalog: Option<String>,
    pub current_database: String,
    pub statement: MvRefreshRequest,
}

#[derive(Clone, Debug)]
pub struct RefreshPlan {
    pub contract: RefreshPlanContract,
}

#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub struct RefreshOutcome {
    pub mv_id: Option<i64>,
    pub target: MvTarget,
    pub rows: Option<i64>,
    pub base_snapshots: BTreeMap<String, i64>,
    pub base_table_object_ids: BTreeMap<String, novarocks_spi::connector::ConnectorTableObjectId>,
    pub target_snapshot_id: Option<i64>,
    pub backend_outcome: BackendRefreshOutcome,
}

#[derive(Clone, Debug)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub enum BackendRefreshOutcome {
    StarRocks(StarRocksTableRefreshOutcome),
    Iceberg(IcebergRefreshOutcome),
}

#[derive(Clone, Debug, Default)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub struct StarRocksTableRefreshOutcome {
    pub completed_inside_execute: bool,
}

#[derive(Clone, Debug, Default)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub struct IcebergRefreshOutcome {
    pub completed_inside_execute: bool,
}

#[derive(Clone)]
#[allow(
    dead_code,
    reason = "Retained for staged materialized-view integration and recovery wiring."
)]
pub struct RefreshCtx {
    pub refresh_id: Option<i64>,
    pub expected_target_snapshot_id: Option<i64>,
    pub recovery_required: bool,
    pub connector_context: novarocks_spi::connector::ConnectorRequestContext,
}

impl RefreshCtx {
    #[allow(
        dead_code,
        reason = "Retained for staged materialized-view integration and recovery wiring."
    )]
    pub fn new(connector_context: novarocks_spi::connector::ConnectorRequestContext) -> Self {
        Self {
            refresh_id: None,
            expected_target_snapshot_id: None,
            recovery_required: false,
            connector_context,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MvListRow {
    pub name: String,
    pub database: String,
    pub storage_engine: String,
    pub refresh_mode: String,
    pub last_refresh_time: Option<String>,
    pub last_refresh_rows: Option<String>,
    pub base_tables: String,
    pub select_text: String,
    pub dependencies: String,
    pub refresh_paused: String,
    pub next_refresh_time: Option<String>,
    pub last_scheduler_error: Option<String>,
    pub max_staleness_ms: Option<String>,
    pub refresh_state: String,
    pub retry_after_time: Option<String>,
    /// Whether this process may manage the target, and why not when it may
    /// not. A target it can only read is still listed: it is being queried,
    /// and leaving it out would report it as gone.
    pub manageability: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefreshErrorKind {
    UserError,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefreshError {
    pub kind: RefreshErrorKind,
    pub message: String,
    compile_control: Option<novarocks_type_contract::CompileControlError>,
    unavailable_implementation: Option<novarocks_sql::analyze_error::AnalyzeError>,
}

impl RefreshError {
    pub fn new(kind: RefreshErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            compile_control: None,
            unavailable_implementation: None,
        }
    }

    pub fn compile_control_error(&self) -> Option<novarocks_type_contract::CompileControlError> {
        self.compile_control.clone()
    }

    pub fn unavailable_implementation(
        &self,
    ) -> Option<&novarocks_sql::analyze_error::AnalyzeError> {
        self.unavailable_implementation.as_ref()
    }

    pub fn user(message: impl Into<String>) -> Self {
        Self::new(RefreshErrorKind::UserError, message)
    }
}

impl From<novarocks_sql::compiler::SqlCompileError> for RefreshError {
    fn from(error: novarocks_sql::compiler::SqlCompileError) -> Self {
        use novarocks_sql::compiler::SqlCompileError as E;
        use novarocks_type_contract::CompileControlError as C;
        let control = match &error {
            E::Cancelled => Some(C::Cancelled),
            E::DeadlineExceeded => Some(C::DeadlineExceeded),
            E::ResourceExhausted => Some(C::ResourceExhausted),
            E::Analyze(error) => error.control_error(),
            E::CompilationSource(source) => Some(C::Source(source.clone())),
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
        let mut failure = Self::user(message);
        failure.compile_control = control;
        failure.unavailable_implementation = unavailable_implementation;
        failure
    }
}

impl From<String> for RefreshError {
    fn from(message: String) -> Self {
        Self::user(message)
    }
}

impl fmt::Display for RefreshError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Error for RefreshError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv::domain::model::MvStorageEngine;

    #[test]
    fn refresh_error_formats_message_with_kind() {
        let error = RefreshError::user("refresh planning failed");

        assert_eq!(error.kind, RefreshErrorKind::UserError);
        assert_eq!(error.to_string(), "refresh planning failed");
    }

    #[test]
    fn storage_engine_maps_to_backend_name() {
        assert_eq!(MvStorageEngine::StarRocks.as_sql_str(), "starrocks");
        assert_eq!(MvStorageEngine::StarRocks.backend_name(), "starrocks");
        assert_eq!(MvStorageEngine::Iceberg.as_sql_str(), "iceberg");
        assert_eq!(MvStorageEngine::Iceberg.backend_name(), "iceberg");
        let err = MvStorageEngine::from_sql_str("starrocks").unwrap_err();
        assert!(err.contains("storage_engine='starrocks'"), "err={err}");
        assert_eq!(
            MvStorageEngine::from_sql_str("iceberg").unwrap(),
            MvStorageEngine::Iceberg
        );
        // Regression assertion: the legacy `"managed"` alias is no longer
        // accepted after the catalog rename. Do not change to "starrocks"
        // here — that's a valid value and would make the unwrap_err() panic.
        assert!(
            MvStorageEngine::from_sql_str("managed")
                .unwrap_err()
                .contains("unknown materialized view storage_engine"),
        );
        assert_eq!(
            MvStorageEngine::from_sql_str("duckdb").unwrap_err(),
            "unknown materialized view storage_engine `duckdb`"
        );
    }
    #[test]
    fn refresh_compile_projection_preserves_only_the_missing_implementation_sql_code() {
        use novarocks_functions::{FunctionBindingError, FunctionOverloadId};
        use novarocks_sql::{analyze_error::AnalyzeError, compiler::SqlCompileError};

        let compile = SqlCompileError::from(FunctionBindingError::UnavailableImplementation(
            FunctionOverloadId::try_new("fixture/mv/refresh/missing-pure-v1").unwrap(),
        ));
        let SqlCompileError::Analyze(ref analyze) = compile else {
            panic!("missing implementation must retain typed analysis identity");
        };
        let expected = analyze.to_string();
        let expected_analyze = analyze.clone();
        let failure = RefreshError::from(compile);
        assert_eq!(failure.message, expected);
        assert_eq!(
            failure.unavailable_implementation(),
            Some(&expected_analyze)
        );
        assert!(
            !failure
                .message
                .starts_with("[sql.analyze.unavailable_implementation] ")
        );
        assert_eq!(failure.kind, RefreshErrorKind::UserError);
        assert_eq!(failure.compile_control_error(), None);

        let ordinary = SqlCompileError::Analyze(AnalyzeError::internal(
            "unsupported diagnostic: UnsupportedCompiledShape",
        ));
        let expected = ordinary.to_string();
        let ordinary = RefreshError::from(ordinary);
        assert_eq!(ordinary.message, expected);
        assert_eq!(ordinary.kind, RefreshErrorKind::UserError);
        assert_eq!(ordinary.unavailable_implementation(), None);
        assert_eq!(ordinary.compile_control_error(), None);
    }

    #[test]
    fn compile_control_cause_keeps_existing_refresh_kind_and_rollback_rule() {
        use novarocks_type_contract::CompileControlError;
        for control in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let compile = novarocks_sql::compiler::SqlCompileError::from(control.clone());
            let expected = compile.to_string();
            let error = RefreshError::from(compile);
            assert_eq!(error.message, expected);
            assert_eq!(error.compile_control_error(), Some(control));
            assert_eq!(error.unavailable_implementation(), None);
            assert_eq!(error.kind, RefreshErrorKind::UserError);
            assert_eq!(
                RefreshError::user(error.message.clone()).compile_control_error(),
                None
            );
        }
    }
}

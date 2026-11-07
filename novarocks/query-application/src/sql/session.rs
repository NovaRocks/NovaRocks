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

//! Query-application session state and admitted property projections.
use super::session_admit::SessionAdmitError;
use crate::session_effects::SessionEffects;
use crate::session_error::QueryServiceError;
use crate::session_value::{SessionTimeZone, SessionValue, StatementSessionError};
use novarocks_parser::ast;
use novarocks_result_contract::ClientPresentationFamily;
use novarocks_sql::compiler::SessionOptimizerSettings;
use novarocks_types::naming::DEFAULT_DATABASE;
use std::fmt;
mod property_error;
pub mod registry;
pub use property_error::session_property_error;
use registry::{SessionCatalogPropertyPort, SessionPropertyInputs, SessionPropertyRegistry};

/// The connection owns input values and the derived execution projection.
/// Runtime settings never author an explicit property override.
#[derive(Clone)]
pub struct SessionSqlState {
    current_catalog: Option<String>,
    current_database: String,
    execution_settings: SessionExecutionSettings,
    optimizer_settings: SessionOptimizerSettings,
    sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings,
    property_inputs: SessionPropertyInputs,
    presentation: Option<ClientPresentationFamily>,
    time_zone: Option<SessionTimeZone>,
}
impl Default for SessionSqlState {
    fn default() -> Self {
        Self {
            current_catalog: None,
            current_database: DEFAULT_DATABASE.into(),
            execution_settings: SessionExecutionSettings::default(),
            optimizer_settings: SessionOptimizerSettings::default(),
            sql_semantics: novarocks_sql::sql_mode::SqlSemanticSettings::default(),
            property_inputs: SessionPropertyInputs::default(),
            presentation: None,
            time_zone: None,
        }
    }
}
impl SessionSqlState {
    pub fn current_catalog(&self) -> Option<&str> {
        self.current_catalog.as_deref()
    }
    pub fn current_database(&self) -> &str {
        &self.current_database
    }
    pub fn sql_semantics(&self) -> &novarocks_sql::sql_mode::SqlSemanticSettings {
        &self.sql_semantics
    }
    pub fn execution_settings(&self) -> &SessionExecutionSettings {
        &self.execution_settings
    }
    pub fn presentation(&self) -> Option<&ClientPresentationFamily> {
        self.presentation.as_ref()
    }
    pub fn time_zone(&self) -> Option<&SessionTimeZone> {
        self.time_zone.as_ref()
    }
    pub fn property_inputs(&self) -> &SessionPropertyInputs {
        &self.property_inputs
    }
    pub fn from_session_value(
        value: SessionValue,
        registry: &SessionPropertyRegistry,
        catalogs: &dyn SessionCatalogPropertyPort,
    ) -> Result<Self, StatementSessionError> {
        let inputs = registry
            .prepare_header_inputs(&value.properties, catalogs)
            .map_err(session_property_error)?;
        let mut state = Self {
            presentation: Some(value.presentation),
            time_zone: value.time_zone,
            ..Self::default()
        };
        state.assign_session_names(
            value.catalog,
            value.schema.unwrap_or_else(|| DEFAULT_DATABASE.into()),
        );
        inputs
            .project_into(&mut state)
            .map_err(session_property_error)?;
        state.property_inputs = inputs;
        Ok(state)
    }
    /// Names are client input until the role's original catalog owner admits them.
    pub fn assign_session_names(&mut self, catalog: Option<String>, schema: String) {
        self.current_catalog = catalog;
        self.current_database = schema;
    }
    pub fn set_resolved_database_context(&mut self, catalog: Option<String>, database: String) {
        self.assign_session_names(catalog, database);
    }
    /// System input helper: catalog facts must use whole-header admission.
    pub fn apply_session_property_text(
        &mut self,
        registry: &SessionPropertyRegistry,
        name: &str,
        text: &str,
    ) -> Result<(), StatementSessionError> {
        let property = registry
            .prepare_system_header(name, text)
            .map_err(session_property_error)?;
        self.apply_effects(&SessionEffects::property(property))?;
        Ok(())
    }
    /// Explicit input text. Declaration defaults are exposed by registry metadata.
    pub fn render_session_property(&self, name: &str) -> Option<String> {
        self.property_inputs
            .raw_system_value(name)
            .map(str::to_owned)
    }
    /// Prepare on the caller's CPU owner, then publish this whole state once.
    pub fn apply_effects(&mut self, effects: &SessionEffects) -> Result<(), QueryServiceError> {
        let mut staged = self.clone();
        if let Some(names) = &effects.catalog_schema {
            staged.assign_session_names(names.catalog.clone(), names.schema.clone());
        }
        for property in &effects.properties {
            property
                .project_into(&mut staged)
                .map_err(session_property_error)?;
            staged.property_inputs.insert(property.clone());
        }
        *self = staged;
        Ok(())
    }
    pub fn into_query_attempt_inputs(
        self,
    ) -> (
        Option<String>,
        String,
        SessionExecutionSettings,
        SessionOptimizerSettings,
        novarocks_sql::sql_mode::SqlSemanticSettings,
    ) {
        let group_concat_max_len = self.execution_settings.group_concat_max_len();
        (
            self.current_catalog,
            self.current_database,
            self.execution_settings,
            self.optimizer_settings,
            self.sql_semantics
                .with_group_concat_max_len(group_concat_max_len),
        )
    }
}

/// GLOBAL is refused before lookup, including unknown property names.
pub fn admit_session_set_assignment(
    source: &str,
    assignment: &ast::SetAssignment,
) -> Result<(), QueryServiceError> {
    if matches!(assignment.scope, ast::SetScope::Global) {
        return Err(QueryServiceError::from_user_error(
            SessionAdmitError::GlobalScopeUnsupported.to_user_error(
                source,
                assignment.span,
                format!("SET GLOBAL {} is not supported", assignment.name.value),
            ),
        ));
    }
    Ok(())
}

/// Connection-local settings that SQL admission has validated.
///
/// This type deliberately contains no protocol DTO. MySQL and native role
/// adapters project this state into their own validated wire contracts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionExecutionSettings {
    query_timeout_secs: Option<u64>,
    query_mem_limit_bytes: Option<i64>,
    group_concat_max_len: i64,
    pipeline_dop: Option<i32>,
    enable_parquet_reader_page_index: bool,
    enable_scan_datacache: bool,
    enable_populate_datacache: bool,
    runtime_filter_scan_wait_time_ms: Option<i64>,
    runtime_filter_wait_timeout_ms: Option<i32>,
}

impl Default for SessionExecutionSettings {
    fn default() -> Self {
        Self {
            query_timeout_secs: None,
            query_mem_limit_bytes: None,
            group_concat_max_len: 1024,
            pipeline_dop: None,
            enable_parquet_reader_page_index: false,
            enable_scan_datacache: false,
            enable_populate_datacache: false,
            runtime_filter_scan_wait_time_ms: None,
            runtime_filter_wait_timeout_ms: None,
        }
    }
}

impl SessionExecutionSettings {
    pub const fn query_mem_limit_bytes(&self) -> Option<i64> {
        self.query_mem_limit_bytes
    }

    pub fn set_query_mem_limit_bytes(&mut self, bytes: i64) -> Result<(), SessionSettingError> {
        if bytes <= 0 {
            return Err(SessionSettingError::NonPositiveQueryMemoryLimit);
        }
        self.query_mem_limit_bytes = Some(bytes);
        Ok(())
    }
    pub const fn query_timeout_secs(&self) -> Option<u64> {
        self.query_timeout_secs
    }

    pub fn set_query_timeout_secs(&mut self, seconds: u64) {
        self.query_timeout_secs = (seconds > 0).then_some(seconds);
    }

    pub const fn group_concat_max_len(&self) -> i64 {
        self.group_concat_max_len
    }

    /// Keep the session value verbatim; aggregate lowering clamps it to the
    /// supported minimum before execution.
    pub fn set_group_concat_max_len(&mut self, value: i64) {
        self.group_concat_max_len = value;
    }

    pub const fn pipeline_dop(&self) -> Option<i32> {
        self.pipeline_dop
    }

    pub fn set_pipeline_dop(&mut self, value: i32) {
        self.pipeline_dop = (value > 0).then_some(value);
    }

    pub const fn enable_parquet_reader_page_index(&self) -> bool {
        self.enable_parquet_reader_page_index
    }

    pub fn set_enable_parquet_reader_page_index(&mut self, enabled: bool) {
        self.enable_parquet_reader_page_index = enabled;
    }

    pub const fn enable_scan_datacache(&self) -> bool {
        self.enable_scan_datacache
    }

    pub fn set_enable_scan_datacache(&mut self, enabled: bool) {
        self.enable_scan_datacache = enabled;
    }

    pub const fn enable_populate_datacache(&self) -> bool {
        self.enable_populate_datacache
    }

    pub fn set_enable_populate_datacache(&mut self, enabled: bool) {
        self.enable_populate_datacache = enabled;
    }

    pub const fn runtime_filter_scan_wait_time_ms(&self) -> Option<i64> {
        self.runtime_filter_scan_wait_time_ms
    }

    pub fn set_runtime_filter_scan_wait_time_ms(
        &mut self,
        value: i64,
    ) -> Result<(), SessionSettingError> {
        if value < 0 {
            return Err(SessionSettingError::NegativeRuntimeFilterScanWaitTime);
        }
        self.runtime_filter_scan_wait_time_ms = Some(value);
        Ok(())
    }

    pub const fn runtime_filter_wait_timeout_ms(&self) -> Option<i32> {
        self.runtime_filter_wait_timeout_ms
    }

    pub fn set_runtime_filter_wait_timeout_ms(
        &mut self,
        value: i32,
    ) -> Result<(), SessionSettingError> {
        if value < 0 {
            return Err(SessionSettingError::NegativeRuntimeFilterWaitTimeout);
        }
        self.runtime_filter_wait_timeout_ms = Some(value);
        Ok(())
    }
}

/// A closed validation failure for one SQL-session setting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SessionSettingError {
    NonPositiveQueryMemoryLimit,
    NegativeRuntimeFilterScanWaitTime,
    NegativeRuntimeFilterWaitTimeout,
}

impl fmt::Display for SessionSettingError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonPositiveQueryMemoryLimit => {
                formatter.write_str("query_mem_limit must be positive")
            }
            Self::NegativeRuntimeFilterScanWaitTime => {
                formatter.write_str("runtime_filter_scan_wait_time must be non-negative")
            }
            Self::NegativeRuntimeFilterWaitTimeout => {
                formatter.write_str("global_runtime_filter_wait_timeout must be non-negative")
            }
        }
    }
}

impl std::error::Error for SessionSettingError {}

#[cfg(test)]
mod tests {
    use super::{SessionExecutionSettings, SessionSettingError, SessionSqlState};
    use novarocks_types::naming::DEFAULT_DATABASE;
    #[test]
    fn default_sql_session_state_is_neutral_and_empty() {
        let state = SessionSqlState::default();
        assert_eq!(state.current_catalog(), None);
        assert_eq!(state.current_database(), DEFAULT_DATABASE);
        assert_eq!(state.presentation(), None);
        assert_eq!(
            state.execution_settings(),
            &SessionExecutionSettings::default()
        );
    }

    #[test]
    fn preserves_session_values_before_wire_projection() {
        let mut settings = SessionExecutionSettings::default();
        settings.set_query_timeout_secs(17);
        settings.set_pipeline_dop(4);
        settings
            .set_runtime_filter_scan_wait_time_ms(0)
            .expect("zero is valid");
        settings.set_group_concat_max_len(-1);

        assert_eq!(settings.query_timeout_secs(), Some(17));
        assert_eq!(settings.pipeline_dop(), Some(4));
        assert_eq!(settings.runtime_filter_scan_wait_time_ms(), Some(0));
        assert_eq!(settings.group_concat_max_len(), -1);
    }

    #[test]
    fn preserves_boolean_switches() {
        let mut settings = SessionExecutionSettings::default();
        settings.set_enable_parquet_reader_page_index(true);
        settings.set_enable_scan_datacache(true);
        settings.set_enable_populate_datacache(true);

        assert!(settings.enable_parquet_reader_page_index());
        assert!(settings.enable_scan_datacache());
        assert!(settings.enable_populate_datacache());
    }

    #[test]
    fn rejects_negative_runtime_filter_settings() {
        let mut settings = SessionExecutionSettings::default();
        assert_eq!(
            settings.set_runtime_filter_scan_wait_time_ms(-1),
            Err(SessionSettingError::NegativeRuntimeFilterScanWaitTime)
        );
        assert_eq!(
            settings.set_runtime_filter_wait_timeout_ms(-1),
            Err(SessionSettingError::NegativeRuntimeFilterWaitTimeout)
        );
    }
}

#[cfg(test)]
#[path = "group_concat_admission_tests.rs"]
mod group_concat_admission_tests;

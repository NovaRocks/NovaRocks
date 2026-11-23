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

//! Immutable per-statement session input, without catalog existence checks.
use std::{fmt, sync::Arc};

use crate::session_error::QueryServiceError;
use novarocks_result_contract::ClientPresentationFamily;
use novarocks_types::naming::{DEFAULT_CATALOG, normalize_identifier};

/// One validated definition, shared with the statement that uses its text.
#[derive(Clone, PartialEq, Eq)]
pub struct PreparedStatementDefinition {
    sql: Arc<str>,
}

impl PreparedStatementDefinition {
    pub fn new(sql: Arc<str>) -> Self {
        Self { sql }
    }

    pub fn sql(&self) -> &Arc<str> {
        &self.sql
    }
}

impl fmt::Debug for PreparedStatementDefinition {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedStatementDefinition")
            .field("sql_bytes", &self.sql.len())
            .finish_non_exhaustive()
    }
}

/// Names are compared exactly as received. A statement can retain its chosen
/// definition without keeping the request's other definitions alive.
#[derive(Clone, Debug, Default)]
pub struct PreparedStatementSnapshot(Option<Arc<PreparedStatementEntries>>);

struct PreparedStatementEntries {
    definitions: Vec<(String, PreparedStatementDefinition)>,
}

impl fmt::Debug for PreparedStatementEntries {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PreparedStatementEntries")
            .field("count", &self.definitions.len())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DuplicatePreparedStatementName;

impl fmt::Display for DuplicatePreparedStatementName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Prepared statement names must be unique after decoding")
    }
}

impl std::error::Error for DuplicatePreparedStatementName {}

impl PreparedStatementSnapshot {
    pub fn new(
        mut definitions: Vec<(String, PreparedStatementDefinition)>,
    ) -> Result<Self, DuplicatePreparedStatementName> {
        definitions.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        if definitions.windows(2).any(|pair| pair[0].0 == pair[1].0) {
            return Err(DuplicatePreparedStatementName);
        }
        Ok(Self(Some(Arc::new(PreparedStatementEntries {
            definitions,
        }))))
    }

    pub fn get(&self, name: &str) -> Option<&PreparedStatementDefinition> {
        let definitions = &self.0.as_ref()?.definitions;
        let index = definitions
            .binary_search_by(|candidate| candidate.0.as_str().cmp(name))
            .ok()?;
        Some(&definitions[index].1)
    }
}

pub type SessionTimeZone = novarocks_type_contract::TimeZoneKey;

fn parse_session_header_zone(zone: &str) -> Result<SessionTimeZone, StatementSessionError> {
    SessionTimeZone::parse(zone)
        .ok_or_else(|| StatementSessionError::TimeZoneNotSupported { zone: zone.into() })
}

#[derive(Clone, Debug)]
pub struct SessionValue {
    pub(crate) catalog: Option<String>,
    pub(crate) schema: Option<String>,
    pub(crate) properties: Vec<(String, String)>,
    pub(crate) time_zone: Option<SessionTimeZone>,
    pub(crate) presentation: ClientPresentationFamily,
    prepared_statements: PreparedStatementSnapshot,
}
impl SessionValue {
    pub fn try_new(
        catalog: Option<&str>,
        schema: Option<&str>,
        properties: Vec<(String, String)>,
        time_zone: Option<&str>,
        presentation: ClientPresentationFamily,
    ) -> Result<Self, StatementSessionError> {
        let normalize = |name: &str| {
            normalize_identifier(name).map_err(|detail| {
                StatementSessionError::Service(QueryServiceError::new(
                    crate::session_error::QueryServiceErrorKind::InvalidValue,
                    detail,
                ))
            })
        };
        let catalog = catalog
            .map(normalize)
            .transpose()?
            .filter(|name| name != DEFAULT_CATALOG);
        let schema = schema.map(normalize).transpose()?;
        let time_zone = time_zone.map(parse_session_header_zone).transpose()?;
        Ok(Self {
            catalog,
            schema,
            properties,
            time_zone,
            presentation,
            prepared_statements: PreparedStatementSnapshot::default(),
        })
    }
    pub fn with_prepared_statements(
        mut self,
        prepared_statements: PreparedStatementSnapshot,
    ) -> Self {
        self.prepared_statements = prepared_statements;
        self
    }

    pub fn prepared_statements(&self) -> &PreparedStatementSnapshot {
        &self.prepared_statements
    }

    pub fn presentation(&self) -> &ClientPresentationFamily {
        &self.presentation
    }
    pub fn time_zone(&self) -> Option<&SessionTimeZone> {
        self.time_zone.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StatementSessionError {
    InvalidSessionProperty { name: String, detail: String },
    TimeZoneNotSupported { zone: String },
    Service(QueryServiceError),
}
impl std::fmt::Display for StatementSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidSessionProperty { name, detail } => {
                write!(f, "Invalid session property {name}: {detail}")
            }
            Self::TimeZoneNotSupported { zone } => write!(f, "Time zone not supported: {zone}"),
            Self::Service(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for StatementSessionError {}
impl From<QueryServiceError> for StatementSessionError {
    fn from(error: QueryServiceError) -> Self {
        Self::Service(error)
    }
}

#[cfg(test)]
mod tests {
    struct NoCatalogAccess;
    impl crate::sql::session::registry::SessionCatalogPropertyPort for NoCatalogAccess {
        fn session_property_catalog(
            &self,
            _: &str,
        ) -> Result<
            crate::sql::session::registry::SessionCatalogPropertyAdmission,
            crate::session_error::QueryServiceError,
        > {
            panic!("system-only session input must not acquire catalog facts")
        }
    }
    use super::*;
    #[test]
    fn time_zone_headers_use_the_frozen_zone_index() {
        assert_eq!(
            parse_session_header_zone("+14:00")
                .unwrap()
                .fixed_offset_minutes(),
            Some(840)
        );
        assert_eq!(
            parse_session_header_zone("-05:30")
                .unwrap()
                .fixed_offset_minutes(),
            Some(-330)
        );
        assert_eq!(
            parse_session_header_zone("Asia/Shanghai").unwrap().name(),
            "Asia/Shanghai"
        );
        for zone in ["+14:01", "+18:00", "+01:60", "invalid-zone"] {
            assert!(matches!(
                parse_session_header_zone(zone),
                Err(StatementSessionError::TimeZoneNotSupported { .. })
            ));
        }
    }
    #[test]
    fn effective_zone_prefers_the_typed_override_then_the_header_then_the_config() {
        use crate::session_effects::SessionEffects;
        use crate::sql::session::{SessionSqlState, registry::SessionPropertyRegistry};
        use novarocks_type_contract::{StatementStartUtc, StatementTimeEnvironment};

        let registry = SessionPropertyRegistry::new().unwrap();
        let admitted = StatementTimeEnvironment {
            start: StatementStartUtc::try_new(-1, 987_654_321).unwrap(),
            zone: SessionTimeZone::parse("America/New_York").unwrap(),
        };
        assert_eq!(
            SessionSqlState::default().statement_time(admitted),
            admitted
        );

        let header = SessionTimeZone::parse("Europe/Warsaw").unwrap();
        let value = SessionValue::try_new(
            None,
            None,
            vec![("time_zone_id".into(), "Asia/Shanghai".into())],
            Some(header.name()),
            ClientPresentationFamily::TrinoJson(novarocks_result_contract::TrinoJsonFacts {
                parametric_datetime: true,
                parametric_interval: false,
                variant: true,
                variant_binary: true,
            }),
        )
        .unwrap();
        let mut state =
            SessionSqlState::from_session_value(value, &registry, &NoCatalogAccess).unwrap();
        let projected = state.statement_time(admitted);
        assert_eq!(projected.start, admitted.start);
        assert_eq!(projected.zone.name(), "Asia/Shanghai");
        assert_eq!(state.time_zone(), Some(&header));

        let query =
            novarocks_parser::parser::parse_query("WITH SESSION time_zone_id = 'UTC' SELECT 1")
                .unwrap();
        let property = registry
            .resolve_sql_property(&query.session_properties[0].name, &NoCatalogAccess)
            .unwrap();
        let clear = SessionEffects::reset_property(property);
        assert_eq!(
            clear.reset_properties().collect::<Vec<_>>(),
            ["time_zone_id"]
        );
        state.apply_effects(&clear).unwrap();
        let projected = state.statement_time(admitted);
        assert_eq!(projected.start, admitted.start);
        assert_eq!(projected.zone, header);
        assert_eq!(state.render_session_property("time_zone_id"), None);
    }

    #[test]
    fn names_are_normalized_without_requiring_remote_existence() {
        let value = SessionValue::try_new(
            Some("Unknown_Lake"),
            Some("Removed_Schema"),
            vec![],
            None,
            ClientPresentationFamily::TrinoJson(novarocks_result_contract::TrinoJsonFacts {
                parametric_datetime: true,
                parametric_interval: false,
                variant: true,
                variant_binary: true,
            }),
        )
        .unwrap();
        assert_eq!(value.catalog.as_deref(), Some("unknown_lake"));
        assert_eq!(value.schema.as_deref(), Some("removed_schema"));
        assert!(
            SessionValue::try_new(
                Some(DEFAULT_CATALOG),
                None,
                vec![],
                None,
                ClientPresentationFamily::TrinoJson(novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    parametric_interval: false,
                    variant: true,
                    variant_binary: true
                })
            )
            .unwrap()
            .catalog
            .is_none()
        );
    }

    #[test]
    fn statement_input_transfers_original_family_zone_names_and_properties() {
        use crate::sql::session::SessionSqlState;
        use novarocks_result_contract::TrinoJsonFacts;

        let family = ClientPresentationFamily::TrinoJson(TrinoJsonFacts {
            parametric_datetime: true,
            parametric_interval: false,
            variant: true,
            variant_binary: false,
        });
        let value = SessionValue::try_new(
            Some("`Unknown_Lake`"),
            Some(" Deleted_Schema "),
            vec![
                ("query_timeout".into(), "41".into()),
                ("sql_mode".into(), "GROUP_CONCAT_LEGACY,32".into()),
            ],
            Some("Europe/Warsaw"),
            family,
        )
        .unwrap();
        let state = SessionSqlState::from_session_value(
            value,
            &crate::sql::session::registry::SessionPropertyRegistry::new().unwrap(),
            &NoCatalogAccess,
        )
        .unwrap();
        assert_eq!(state.current_catalog(), Some("unknown_lake"));
        assert_eq!(state.current_database(), "deleted_schema");
        assert_eq!(state.presentation(), Some(&family));
        assert_eq!(
            state.time_zone(),
            SessionTimeZone::parse("Europe/Warsaw").as_ref()
        );
        assert_eq!(state.execution_settings().query_timeout_secs(), Some(41));
        assert_eq!(
            state.sql_semantics().sql_mode().assignment(),
            "GROUP_CONCAT_LEGACY,32"
        );
        assert!(state.sql_semantics().sql_mode().group_concat_legacy());
    }

    #[test]
    fn session_input_names_and_time_zones_fail_with_original_typed_errors() {
        use crate::session_error::QueryServiceErrorKind;

        for (catalog, schema) in [
            (Some("lake.with.dot"), None),
            (Some("lake"), Some("bad-schema")),
            (Some(""), None),
        ] {
            let error = SessionValue::try_new(
                catalog,
                schema,
                vec![],
                None,
                ClientPresentationFamily::TrinoJson(novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    parametric_interval: false,
                    variant: true,
                    variant_binary: true,
                }),
            )
            .unwrap_err();
            assert!(
                matches!(error, StatementSessionError::Service(error) if error.kind() == QueryServiceErrorKind::InvalidValue)
            );
        }
        for zone in ["+18:59", "-18:01", "+00:99", "+02:00:01", ""] {
            let error = SessionValue::try_new(
                None,
                None,
                vec![],
                Some(zone),
                ClientPresentationFamily::TrinoJson(novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    parametric_interval: false,
                    variant: true,
                    variant_binary: true,
                }),
            )
            .unwrap_err();
            assert!(
                matches!(error, StatementSessionError::TimeZoneNotSupported { zone: actual } if actual == zone)
            );
        }
    }

    #[test]
    fn absent_names_and_zone_preserve_the_explicit_client_presentation() {
        use crate::sql::session::SessionSqlState;
        use novarocks_types::naming::DEFAULT_DATABASE;

        let value = SessionValue::try_new(
            None,
            None,
            vec![],
            None,
            ClientPresentationFamily::TrinoJson(novarocks_result_contract::TrinoJsonFacts {
                parametric_datetime: true,
                parametric_interval: false,
                variant: true,
                variant_binary: true,
            }),
        )
        .unwrap();
        let state = SessionSqlState::from_session_value(
            value,
            &crate::sql::session::registry::SessionPropertyRegistry::new().unwrap(),
            &NoCatalogAccess,
        )
        .unwrap();
        assert_eq!(state.current_catalog(), None);
        assert_eq!(state.current_database(), DEFAULT_DATABASE);
        assert_eq!(
            state.presentation(),
            Some(&ClientPresentationFamily::TrinoJson(
                novarocks_result_contract::TrinoJsonFacts {
                    parametric_datetime: true,
                    parametric_interval: false,
                    variant: true,
                    variant_binary: true
                }
            ))
        );
        assert_eq!(state.time_zone(), None);
        for (zone, minutes) in [("+00:00", 0), ("-00:00", 0), ("-14:00", -840)] {
            assert_eq!(
                parse_session_header_zone(zone)
                    .unwrap()
                    .fixed_offset_minutes(),
                Some(minutes)
            );
        }
    }
}

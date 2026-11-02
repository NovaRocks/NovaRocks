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
use crate::session_error::QueryServiceError;
use novarocks_result_contract::ClientPresentationFamily;
use novarocks_types::naming::{DEFAULT_CATALOG, normalize_identifier};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionTimeZone {
    Region(Box<str>),
    FixedOffsetMinutes(i16),
}
impl SessionTimeZone {
    pub fn parse(zone: &str) -> Result<Self, StatementSessionError> {
        let unsupported = || StatementSessionError::TimeZoneNotSupported { zone: zone.into() };
        if zone.starts_with(['+', '-']) {
            let bytes = zone.as_bytes();
            if bytes.len() != 6
                || bytes[3] != b':'
                || ![bytes[1], bytes[2], bytes[4], bytes[5]]
                    .iter()
                    .all(u8::is_ascii_digit)
            {
                return Err(unsupported());
            }
            let hours = i16::from(bytes[1] - b'0') * 10 + i16::from(bytes[2] - b'0');
            let minutes = i16::from(bytes[4] - b'0') * 10 + i16::from(bytes[5] - b'0');
            if hours > 18 || minutes >= 60 || (hours == 18 && minutes != 0) {
                return Err(unsupported());
            }
            let offset = hours * 60 + minutes;
            return Ok(Self::FixedOffsetMinutes(if bytes[0] == b'-' {
                -offset
            } else {
                offset
            }));
        }
        zone.parse::<chrono_tz::Tz>().map_err(|_| unsupported())?;
        Ok(Self::Region(zone.into()))
    }
}

#[derive(Clone, Debug)]
pub struct SessionValue {
    pub(crate) catalog: Option<String>,
    pub(crate) schema: Option<String>,
    pub(crate) properties: Vec<(String, String)>,
    pub(crate) time_zone: Option<SessionTimeZone>,
    pub(crate) presentation: ClientPresentationFamily,
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
        let time_zone = time_zone.map(SessionTimeZone::parse).transpose()?;
        Ok(Self {
            catalog,
            schema,
            properties,
            time_zone,
            presentation,
        })
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
    use super::*;
    #[test]
    fn time_zone_inputs_are_validated_without_assigning_semantics() {
        assert_eq!(
            SessionTimeZone::parse("+18:00").unwrap(),
            SessionTimeZone::FixedOffsetMinutes(1080)
        );
        assert_eq!(
            SessionTimeZone::parse("-05:30").unwrap(),
            SessionTimeZone::FixedOffsetMinutes(-330)
        );
        assert_eq!(
            SessionTimeZone::parse("Asia/Shanghai").unwrap(),
            SessionTimeZone::Region("Asia/Shanghai".into())
        );
        for zone in ["+18:01", "+19:00", "+01:60", "+8:00", "invalid-zone"] {
            assert!(matches!(
                SessionTimeZone::parse(zone),
                Err(StatementSessionError::TimeZoneNotSupported { .. })
            ));
        }
    }
    #[test]
    fn names_are_normalized_without_requiring_remote_existence() {
        let value = SessionValue::try_new(
            Some("Unknown_Lake"),
            Some("Removed_Schema"),
            vec![],
            None,
            ClientPresentationFamily::MysqlText,
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
                ClientPresentationFamily::MysqlText
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
        let state = SessionSqlState::from_session_value(value).unwrap();
        assert_eq!(state.current_catalog(), Some("unknown_lake"));
        assert_eq!(state.current_database(), "deleted_schema");
        assert_eq!(state.presentation(), &family);
        assert_eq!(
            state.time_zone(),
            Some(&SessionTimeZone::Region("Europe/Warsaw".into()))
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
                ClientPresentationFamily::MysqlText,
            )
            .unwrap_err();
            assert!(
                matches!(error, StatementSessionError::Service(error) if error.kind() == QueryServiceErrorKind::InvalidValue)
            );
        }
        for zone in ["+18:59", "-18:01", "+00:99", "UTC+08:00", ""] {
            let error = SessionValue::try_new(
                None,
                None,
                vec![],
                Some(zone),
                ClientPresentationFamily::MysqlText,
            )
            .unwrap_err();
            assert!(
                matches!(error, StatementSessionError::TimeZoneNotSupported { zone: actual } if actual == zone)
            );
        }
    }

    #[test]
    fn absent_inputs_keep_existing_defaults_and_do_not_invent_time_zone() {
        use crate::sql::session::SessionSqlState;
        use novarocks_types::naming::DEFAULT_DATABASE;

        let value = SessionValue::try_new(
            None,
            None,
            vec![],
            None,
            ClientPresentationFamily::MysqlText,
        )
        .unwrap();
        let state = SessionSqlState::from_session_value(value).unwrap();
        assert_eq!(state.current_catalog(), None);
        assert_eq!(state.current_database(), DEFAULT_DATABASE);
        assert_eq!(state.presentation(), &ClientPresentationFamily::MysqlText);
        assert_eq!(state.time_zone(), None);
        for (zone, minutes) in [("+00:00", 0), ("-00:00", 0), ("-18:00", -1080)] {
            assert_eq!(
                SessionTimeZone::parse(zone).unwrap(),
                SessionTimeZone::FixedOffsetMinutes(minutes)
            );
        }
    }
}

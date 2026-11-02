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

//! HTTP syntax is checked at submission; session semantics are checked at dispatch.
use axum::http::{HeaderMap, HeaderValue, StatusCode, header::InvalidHeaderValue};
use novarocks_query_application::{
    session_effects::SessionEffects,
    session_value::{SessionValue, StatementSessionError},
};
use novarocks_result_contract::{ClientPresentationFamily, TrinoJsonFacts};
use serde::Serialize;

#[derive(Clone, Debug)]
pub(crate) struct ParsedStatementRequest {
    sql: String,
    principal: String,
    catalog: Option<String>,
    schema: Option<String>,
    properties: Vec<(String, String)>,
    time_zone: Option<String>,
    facts: TrinoJsonFacts,
    unsupported: Option<String>,
    observation: RequestObservation,
}

#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct RequestObservation {
    pub source: Option<String>,
    pub client_tags: Vec<String>,
    pub client_info: Option<String>,
    pub trace_token: Option<String>,
    pub language: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RequestRejection {
    pub status: StatusCode,
    pub message: String,
}
impl RequestRejection {
    fn malformed(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
}
impl std::fmt::Display for RequestRejection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for RequestRejection {}

#[derive(Clone, Debug)]
pub(crate) enum RequestSessionError {
    Unsupported(String),
    Session(StatementSessionError),
}
impl std::fmt::Display for RequestSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unsupported(message) => f.write_str(message),
            Self::Session(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for RequestSessionError {}

impl ParsedStatementRequest {
    pub fn parse(
        headers: &HeaderMap,
        sql: String,
        configured_user: &str,
    ) -> Result<Self, RequestRejection> {
        let user = singleton(headers, "x-trino-user")?.map(str::trim);
        if user != Some(configured_user) {
            return Err(RequestRejection {
                status: StatusCode::UNAUTHORIZED,
                message: "Invalid Trino user".into(),
            });
        }
        if sql.trim().is_empty() {
            return Err(RequestRejection::malformed("SQL statement is empty"));
        }
        let catalog = trim_empty(singleton(headers, "x-trino-catalog")?);
        let schema = trim_empty(singleton(headers, "x-trino-schema")?);
        if schema.is_some() && catalog.is_none() {
            return Err(RequestRejection::malformed(
                "Schema is set but catalog is not",
            ));
        }
        let mut properties: Vec<(String, String)> = Vec::new();
        // Trino 483 parseProperty applies the last value for repeated names.
        // Decoding happens after comma and '=' framing, so encoded separators
        // remain part of the property value.
        for value in headers.get_all("x-trino-session") {
            let value = value
                .to_str()
                .map_err(|_| RequestRejection::malformed("Invalid X-Trino-Session header"))?;
            for assignment in value
                .split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
            {
                let mut parts = assignment.split('=');
                let name = parts.next().unwrap_or_default().trim();
                let value = parts
                    .next()
                    .ok_or_else(|| RequestRejection::malformed("Invalid X-Trino-Session header"))?
                    .trim();
                if parts.next().is_some()
                    || name.is_empty()
                    || name.rsplit_once('.').is_some_and(|(catalog, property)| {
                        catalog.is_empty() || property.is_empty()
                    })
                {
                    return Err(RequestRejection::malformed(
                        "Invalid X-Trino-Session header",
                    ));
                }
                let value = decode_form_value(value).ok_or_else(|| {
                    RequestRejection::malformed("Invalid X-Trino-Session URL encoding")
                })?;
                if let Some((_, previous)) = properties.iter_mut().find(|(key, _)| key == name) {
                    *previous = value;
                } else {
                    properties.push((name.into(), value));
                }
            }
        }
        let capabilities = singleton(headers, "x-trino-client-capabilities")?.unwrap_or_default();
        let mut facts = TrinoJsonFacts::default();
        for capability in capabilities
            .split(',')
            .map(str::trim)
            .filter(|part| !part.is_empty())
        {
            match capability {
                "PARAMETRIC_DATETIME" => facts.parametric_datetime = true,
                "VARIANT" => facts.variant = true,
                "VARIANT_BINARY" => facts.variant_binary = true,
                _ => {} // Other advertised capabilities do not change this presentation contract.
            }
        }
        let time_zone = singleton(headers, "x-trino-time-zone")?.map(str::to_owned);
        let transaction = trim_empty(singleton(headers, "x-trino-transaction-id")?);
        let mut unsupported = transaction
            .filter(|value| !value.eq_ignore_ascii_case("NONE"))
            .map(|_| "Multi-statement transactions are not supported".into());
        for name in [
            "x-trino-prepared-statement",
            "x-trino-role",
            "x-trino-extra-credential",
            "x-trino-path",
        ] {
            // These features have no semantic owner in TRC-1. They are retained
            // as a dispatch error without parsing or retaining credential data.
            for value in headers.get_all(name) {
                let value = value
                    .to_str()
                    .map_err(|_| RequestRejection::malformed(format!("Invalid {name} header")))?;
                if !value.trim().is_empty() && unsupported.is_none() {
                    unsupported = Some(format!("Header {name} is not supported"));
                }
            }
        }
        let source = singleton(headers, "x-trino-source")?.map(str::to_owned);
        let client_tags = singleton(headers, "x-trino-client-tags")?
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|tag| !tag.is_empty())
            .fold(Vec::new(), |mut tags, tag| {
                if !tags.iter().any(|existing| existing == tag) {
                    tags.push(tag.to_owned());
                }
                tags
            });
        let client_info = singleton(headers, "x-trino-client-info")?.map(str::to_owned);
        let trace_token = trim_empty(singleton(headers, "x-trino-trace-token")?);
        let language = singleton(headers, "x-trino-language")?.map(str::to_owned);
        Ok(Self {
            sql,
            principal: configured_user.into(),
            catalog,
            schema,
            properties,
            time_zone,
            facts,
            unsupported,
            observation: RequestObservation {
                source,
                client_tags,
                client_info,
                trace_token,
                language,
            },
        })
    }
    pub fn sql(&self) -> &str {
        &self.sql
    }
    pub fn principal(&self) -> &str {
        &self.principal
    }
    pub fn observation(&self) -> &RequestObservation {
        &self.observation
    }
    pub fn session_value(&self) -> Result<SessionValue, RequestSessionError> {
        if let Some(message) = &self.unsupported {
            return Err(RequestSessionError::Unsupported(message.clone()));
        }
        SessionValue::try_new(
            self.catalog.as_deref(),
            self.schema.as_deref(),
            self.properties.clone(),
            self.time_zone.as_deref(),
            ClientPresentationFamily::TrinoJson(self.facts),
        )
        .map_err(RequestSessionError::Session)
    }
}

fn singleton<'a>(
    headers: &'a HeaderMap,
    name: &'static str,
) -> Result<Option<&'a str>, RequestRejection> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(RequestRejection::malformed(format!(
            "Multiple {name} headers"
        )));
    }
    value
        .to_str()
        .map(Some)
        .map_err(|_| RequestRejection::malformed(format!("Invalid {name} header")))
}
fn trim_empty(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}
fn decode_form_value(value: &str) -> Option<String> {
    fn hex(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => decoded.push(b' '),
            b'%' => {
                decoded.push(hex(*bytes.get(index + 1)?)? * 16 + hex(*bytes.get(index + 2)?)?);
                index += 2;
            }
            byte => decoded.push(byte),
        }
        index += 1;
    }
    String::from_utf8(decoded).ok()
}
fn encode_form_value(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut encoded = String::new();
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'*' => {
                encoded.push(char::from(byte))
            }
            b' ' => encoded.push('+'),
            byte => {
                encoded.push('%');
                encoded.push(char::from(HEX[(byte >> 4) as usize]));
                encoded.push(char::from(HEX[(byte & 15) as usize]));
            }
        }
    }
    encoded
}

/// Only the successful statement's explicit effects create response headers.
pub(crate) fn session_effect_headers(
    effects: &SessionEffects,
) -> Result<HeaderMap, InvalidHeaderValue> {
    let mut headers = HeaderMap::new();
    if let Some(names) = effects.catalog_schema() {
        headers.insert(
            "x-trino-set-catalog",
            HeaderValue::from_str(
                names
                    .catalog
                    .as_deref()
                    .unwrap_or(novarocks_types::naming::DEFAULT_CATALOG),
            )?,
        );
        headers.insert("x-trino-set-schema", HeaderValue::from_str(&names.schema)?);
    }
    for (name, value) in effects.set_properties() {
        headers.append(
            "x-trino-set-session",
            HeaderValue::from_str(&format!("{name}={}", encode_form_value(&value)))?,
        );
    }
    for name in effects.reset_properties() {
        headers.append("x-trino-clear-session", HeaderValue::from_str(name)?);
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_query_application::sql::session::SessionSqlState;
    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-user", HeaderValue::from_static("root"));
        headers
    }
    fn parse(headers: &HeaderMap) -> Result<ParsedStatementRequest, RequestRejection> {
        ParsedStatementRequest::parse(headers, "SELECT 1".into(), "root")
    }
    #[test]
    fn syntax_rejection_precedes_query_registration() {
        assert_eq!(
            parse(&HeaderMap::new()).unwrap_err().status,
            StatusCode::UNAUTHORIZED
        );
        let mut input = headers();
        input.insert("x-trino-user", HeaderValue::from_static("other"));
        assert_eq!(parse(&input).unwrap_err().status, StatusCode::UNAUTHORIZED);
        input = headers();
        assert_eq!(
            ParsedStatementRequest::parse(&input, " \n\t".into(), "root")
                .unwrap_err()
                .status,
            StatusCode::BAD_REQUEST
        );
        input.insert("x-trino-schema", HeaderValue::from_static("sales"));
        assert_eq!(
            parse(&input).unwrap_err().message,
            "Schema is set but catalog is not"
        );
        input.insert("x-trino-catalog", HeaderValue::from_static(""));
        assert_eq!(parse(&input).unwrap_err().status, StatusCode::BAD_REQUEST);
        input.insert("x-trino-catalog", HeaderValue::from_static("lake"));
        assert!(parse(&input).is_ok());
        input.append("x-trino-catalog", HeaderValue::from_static("lake2"));
        assert_eq!(parse(&input).unwrap_err().status, StatusCode::BAD_REQUEST);
    }
    #[test]
    fn session_headers_are_framed_then_decoded_and_last_assignment_wins() {
        let mut input = headers();
        input.append(
            "x-trino-session",
            HeaderValue::from_static("query_timeout=1, sql_mode=A%2CB%3DC+%2B%E4%B8%AD"),
        );
        input.append(
            "x-trino-session",
            HeaderValue::from_static("query_timeout=2"),
        );
        let request = parse(&input).unwrap();
        assert_eq!(
            request.properties,
            [
                ("query_timeout".into(), "2".into()),
                ("sql_mode".into(), "A,B=C +中".into())
            ]
        );
        for value in [
            "query_timeout",
            "=1",
            ".foo=1",
            "foo.=1",
            "query_timeout=1=2",
            "sql_mode=%",
            "sql_mode=%2G",
            "sql_mode=%FF",
        ] {
            let mut input = headers();
            input.insert("x-trino-session", HeaderValue::from_str(value).unwrap());
            assert_eq!(
                parse(&input).unwrap_err().status,
                StatusCode::BAD_REQUEST,
                "{value}"
            );
        }
    }
    #[test]
    fn unsupported_features_and_invalid_zones_are_deferred_to_first_get() {
        for (name, value) in [
            ("x-trino-transaction-id", "123"),
            ("x-trino-prepared-statement", "stmt=SELECT+1"),
            ("x-trino-role", "NONE"),
            ("x-trino-extra-credential", "secret=value"),
            ("x-trino-path", "lake.sales"),
        ] {
            let mut input = headers();
            input.insert(name, HeaderValue::from_static(value));
            let request = parse(&input).unwrap();
            assert!(
                matches!(
                    request.session_value(),
                    Err(RequestSessionError::Unsupported(_))
                ),
                "{name}"
            );
            if name == "x-trino-extra-credential" {
                assert!(!format!("{request:?}").contains("secret=value"));
            }
        }
        let mut input = headers();
        input.insert(
            "x-trino-time-zone",
            HeaderValue::from_static("invalid-zone"),
        );
        assert!(matches!(
            parse(&input).unwrap().session_value(),
            Err(RequestSessionError::Session(
                StatementSessionError::TimeZoneNotSupported { .. }
            ))
        ));
        input.remove("x-trino-time-zone");
        input.insert("x-trino-transaction-id", HeaderValue::from_static("none"));
        assert!(parse(&input).unwrap().session_value().is_ok());
    }
    #[test]
    fn session_properties_reach_original_semantic_owner_without_remote_lookup() {
        let mut input = headers();
        input.insert("x-trino-catalog", HeaderValue::from_static("unknown_lake"));
        input.insert("x-trino-schema", HeaderValue::from_static("removed_schema"));
        assert!(
            SessionSqlState::from_session_value(parse(&input).unwrap().session_value().unwrap())
                .is_ok()
        );
        input.insert(
            "x-trino-session",
            HeaderValue::from_static("unknown_setting=1"),
        );
        assert!(matches!(
            SessionSqlState::from_session_value(parse(&input).unwrap().session_value().unwrap()),
            Err(StatementSessionError::InvalidSessionProperty { .. })
        ));
    }
    #[test]
    fn capabilities_are_exact_and_observation_does_not_change_the_session() {
        let mut input = headers();
        input.insert(
            "x-trino-client-capabilities",
            HeaderValue::from_static(
                "PARAMETRIC_DATETIME,VARIANT_BINARY,OTHER,VARIANT, parametric_datetime",
            ),
        );
        input.insert("x-trino-source", HeaderValue::from_static("cli"));
        input.insert("x-trino-client-tags", HeaderValue::from_static("a,b,a"));
        input.insert("x-trino-client-info", HeaderValue::from_static("tool info"));
        input.insert("x-trino-trace-token", HeaderValue::from_static(" trace-1 "));
        input.insert("x-trino-language", HeaderValue::from_static("en-US"));
        let request = parse(&input).unwrap();
        assert_eq!(request.sql(), "SELECT 1");
        assert_eq!(request.principal(), "root");
        assert_eq!(
            request.session_value().unwrap().presentation(),
            &ClientPresentationFamily::TrinoJson(TrinoJsonFacts {
                parametric_datetime: true,
                variant: true,
                variant_binary: true
            })
        );
        assert_eq!(request.observation().source.as_deref(), Some("cli"));
        assert_eq!(request.observation().client_tags, ["a", "b"]);
        assert_eq!(
            request.observation().trace_token.as_deref(),
            Some("trace-1")
        );
    }
    #[test]
    fn response_headers_are_explicit_effects_with_java_form_encoding() {
        assert!(
            session_effect_headers(&SessionEffects::default())
                .unwrap()
                .is_empty()
        );
        let headers =
            session_effect_headers(&SessionEffects::set_catalog_schema(None, "db".into())).unwrap();
        assert_eq!(headers["x-trino-set-catalog"], "default_catalog");
        assert_eq!(headers["x-trino-set-schema"], "db");
        assert!(!headers.contains_key("x-trino-set-session"));
        assert_eq!(headers["x-trino-clear-session"], "catalog");
        let headers = session_effect_headers(&SessionEffects::set_catalog_assignment(
            Some("lake".into()),
            "sales".into(),
        ))
        .unwrap();
        assert_eq!(headers["x-trino-set-session"], "catalog=lake");
        assert!(!headers.contains_key("x-trino-clear-session"));
        let source = "SET sql_mode = 'A,B=C +中', pipeline_dop = 2";
        let statements = novarocks_parser::parse(source).unwrap();
        let [
            novarocks_parser::ast::Statement::Session(
                novarocks_parser::ast::SessionStatement::Set(statement),
            ),
        ] = statements.as_slice()
        else {
            panic!("expected one SET statement");
        };
        let mut effects = SessionEffects::default();
        for assignment in &statement.assignments {
            let novarocks_query_application::sql::session::SessionAssignment::Effects(change) =
                novarocks_query_application::sql::session::session_set_assignment_effects(
                    source, assignment,
                )
                .unwrap()
            else {
                panic!("expected typed session property effect");
            };
            effects.append(change);
        }
        let headers = session_effect_headers(&effects).unwrap();
        assert_eq!(
            headers
                .get_all("x-trino-set-session")
                .iter()
                .map(|value| value.to_str().unwrap())
                .collect::<Vec<_>>(),
            ["sql_mode=A%2CB%3DC+%2B%E4%B8%AD", "pipeline_dop=2"]
        );
        assert!(!headers.contains_key("x-trino-set-catalog"));
        for value in ["A,B=C +中", "a*b._-", "a/b", ""] {
            assert_eq!(
                decode_form_value(&encode_form_value(value)).as_deref(),
                Some(value)
            );
        }
        assert_eq!(encode_form_value("A,B=C +中"), "A%2CB%3DC+%2B%E4%B8%AD");
    }
    #[test]
    fn canonical_property_headers_clear_obsolete_client_aliases() {
        let sql = "SET CBO_DISABLED_RULES = 'new_rule'";
        let statements = novarocks_parser::parse(sql).unwrap();
        let [
            novarocks_parser::ast::Statement::Session(
                novarocks_parser::ast::SessionStatement::Set(set),
            ),
        ] = statements.as_slice()
        else {
            panic!("expected one SET");
        };
        let novarocks_query_application::sql::session::SessionAssignment::Effects(effects) =
            novarocks_query_application::sql::session::session_set_assignment_effects(
                sql,
                &set.assignments[0],
            )
            .unwrap()
        else {
            panic!("expected typed effects");
        };
        let headers = session_effect_headers(&effects).unwrap();
        assert_eq!(
            headers["x-trino-set-session"],
            "disable_optimizer_rules=new_rule"
        );
        assert_eq!(headers["x-trino-clear-session"], "cbo_disabled_rules");
    }
}

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

/// Trino 483 query.max-length, measured in Java UTF-16 code units.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryTextLimit {
    units: u32,
    byte_limit: usize,
}
impl QueryTextLimit {
    pub const DEFAULT_UNITS: u32 = 1_000_000;
    pub const MAX_UNITS: u32 = 1_000_000_000;

    pub fn new(units: u32) -> Result<Self, &'static str> {
        if units > Self::MAX_UNITS {
            return Err(
                "Trino query maximum length must be between 0 and 1000000000 UTF-16 code units",
            );
        }
        let byte_limit = usize::try_from(units)
            .ok()
            .and_then(|units| units.checked_mul(3))
            .ok_or("Trino query maximum length byte bound overflows usize")?;
        Ok(Self { units, byte_limit })
    }
    pub fn units(self) -> u32 {
        self.units
    }
    pub fn byte_limit(self) -> usize {
        self.byte_limit
    }
}
impl Default for QueryTextLimit {
    fn default() -> Self {
        Self {
            units: Self::DEFAULT_UNITS,
            byte_limit: 3 * Self::DEFAULT_UNITS as usize,
        }
    }
}
impl Serialize for QueryTextLimit {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.units)
    }
}
impl<'de> serde::Deserialize<'de> for QueryTextLimit {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let units = <u32 as serde::Deserialize>::deserialize(deserializer)?;
        Self::new(units).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug)]
pub(crate) enum StatementBody {
    Complete(String),
    ByteLimitExceeded,
}

#[derive(Clone, Debug)]
pub(crate) struct ParsedStatementRequest {
    sql: String,
    principal: String,
    catalog: Option<String>,
    schema: Option<String>,
    properties: Vec<(String, String)>,
    time_zone: Option<String>,
    facts: TrinoJsonFacts,
    query_text_too_large: Option<String>,
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
    QueryTextTooLarge(String),
    Unsupported(String),
    Session(StatementSessionError),
}
impl std::fmt::Display for RequestSessionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueryTextTooLarge(message) => f.write_str(message),
            Self::Unsupported(message) => f.write_str(message),
            Self::Session(error) => std::fmt::Display::fmt(error, f),
        }
    }
}
impl std::error::Error for RequestSessionError {}

impl ParsedStatementRequest {
    #[cfg(test)]
    pub fn parse(
        headers: &HeaderMap,
        sql: String,
        configured_user: &str,
    ) -> Result<Self, RequestRejection> {
        Self::parse_body(
            headers,
            StatementBody::Complete(sql),
            configured_user,
            QueryTextLimit::default(),
        )
    }
    pub(crate) fn parse_body(
        headers: &HeaderMap,
        body: StatementBody,
        configured_user: &str,
        limit: QueryTextLimit,
    ) -> Result<Self, RequestRejection> {
        let user = singleton(headers, "x-trino-user")?.map(str::trim);
        if user != Some(configured_user) {
            return Err(RequestRejection {
                status: StatusCode::UNAUTHORIZED,
                message: "Invalid Trino user".into(),
            });
        }
        let (sql, query_text_too_large) = match body {
            StatementBody::Complete(sql) => {
                let units = sql.encode_utf16().count();
                let error = (units > limit.units() as usize).then(|| {
                    format!(
                        "Query text length ({units}) exceeds the maximum length ({})",
                        limit.units()
                    )
                });
                (sql, error)
            }
            // No complete text or character count exists after early body stop.
            // Keep an empty diagnostic view; typed refusal precedes Session creation.
            StatementBody::ByteLimitExceeded => (
                String::new(),
                Some(format!(
                    "Query text exceeds the maximum length ({})",
                    limit.units()
                )),
            ),
        };
        if query_text_too_large.is_none() && sql.trim().is_empty() {
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
            query_text_too_large,
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
        if let Some(message) = &self.query_text_too_large {
            return Err(RequestSessionError::QueryTextTooLarge(message.clone()));
        }
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
    use novarocks_query_application::sql::session::{
        SessionSqlState,
        registry::{
            SessionCatalogPropertyAdmission, SessionCatalogPropertyPort, SessionPropertyRegistry,
            SessionPropertyValue,
        },
    };
    struct NoCatalogAccess;
    impl SessionCatalogPropertyPort for NoCatalogAccess {
        fn session_property_catalog(
            &self,
            _: &str,
        ) -> Result<
            SessionCatalogPropertyAdmission,
            novarocks_query_application::session_error::QueryServiceError,
        > {
            panic!("system-only session properties must not inspect catalog facts")
        }
    }
    fn headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("x-trino-user", HeaderValue::from_static("root"));
        headers
    }
    fn parse(headers: &HeaderMap) -> Result<ParsedStatementRequest, RequestRejection> {
        ParsedStatementRequest::parse(headers, "SELECT 1".into(), "root")
    }
    #[test]
    fn query_length_accepts_the_exact_utf16_limit_and_rejects_one_more() {
        let limit = QueryTextLimit::new(8).unwrap();
        let exact = ParsedStatementRequest::parse_body(
            &headers(),
            StatementBody::Complete("SELECT 1".into()),
            "root",
            limit,
        )
        .unwrap();
        assert!(exact.session_value().is_ok());
        assert_eq!(exact.sql(), "SELECT 1");
        let over = ParsedStatementRequest::parse_body(
            &headers(),
            StatementBody::Complete("SELECT 1 ".into()),
            "root",
            limit,
        )
        .unwrap();
        let Err(RequestSessionError::QueryTextTooLarge(message)) = over.session_value() else {
            panic!("one extra original code unit must be rejected before opening a Session");
        };
        assert_eq!(
            message,
            "Query text length (9) exceeds the maximum length (8)"
        );
    }
    #[test]
    fn query_length_counts_supplementary_characters_as_two_units() {
        for (text, limit, accepted) in [("😀", 2, true), ("😀x", 2, false), ("\u{0800}", 1, true)]
        {
            let request = ParsedStatementRequest::parse_body(
                &headers(),
                StatementBody::Complete(text.into()),
                "root",
                QueryTextLimit::new(limit).unwrap(),
            )
            .unwrap();
            assert_eq!(request.session_value().is_ok(), accepted, "{text:?}");
        }
    }
    #[test]
    fn zero_query_limit_is_not_unlimited_and_early_stop_has_no_invented_length() {
        let limit = QueryTextLimit::new(0).unwrap();
        for body in [
            StatementBody::Complete("x".into()),
            StatementBody::ByteLimitExceeded,
        ] {
            let request =
                ParsedStatementRequest::parse_body(&headers(), body, "root", limit).unwrap();
            assert!(matches!(
                request.session_value(),
                Err(RequestSessionError::QueryTextTooLarge(_))
            ));
        }
        let stopped = ParsedStatementRequest::parse_body(
            &headers(),
            StatementBody::ByteLimitExceeded,
            "root",
            limit,
        )
        .unwrap();
        assert_eq!(
            stopped.sql(),
            "",
            "early stop retains only an empty diagnostic view"
        );
        assert_eq!(
            stopped.session_value().unwrap_err().to_string(),
            "Query text exceeds the maximum length (0)"
        );
        assert_eq!(
            ParsedStatementRequest::parse_body(
                &headers(),
                StatementBody::Complete(String::new()),
                "root",
                limit,
            )
            .unwrap_err()
            .message,
            "SQL statement is empty"
        );
    }
    #[test]
    fn oversize_query_keeps_original_header_rejections() {
        let limit = QueryTextLimit::new(0).unwrap();
        assert_eq!(
            ParsedStatementRequest::parse_body(
                &HeaderMap::new(),
                StatementBody::ByteLimitExceeded,
                "root",
                limit,
            )
            .unwrap_err()
            .status,
            StatusCode::UNAUTHORIZED
        );
        let mut input = headers();
        input.insert("x-trino-schema", HeaderValue::from_static("sales"));
        assert_eq!(
            ParsedStatementRequest::parse_body(
                &input,
                StatementBody::ByteLimitExceeded,
                "root",
                limit,
            )
            .unwrap_err()
            .message,
            "Schema is set but catalog is not"
        );
    }
    #[test]
    fn query_text_limit_has_validated_serde_and_exact_default_and_bounds() {
        let default = QueryTextLimit::default();
        assert_eq!(
            (default.units(), default.byte_limit()),
            (1_000_000, 3_000_000)
        );
        for (units, bytes) in [(0, 0), (1_000_000_000, 3_000_000_000)] {
            let limit: QueryTextLimit = serde_json::from_str(&units.to_string()).unwrap();
            assert_eq!((limit.units(), limit.byte_limit()), (units, bytes));
            assert_eq!(serde_json::to_string(&limit).unwrap(), units.to_string());
        }
        for text in ["-1", "1000000001", "4294967296", "1.5", "\"1000000\""] {
            assert!(
                serde_json::from_str::<QueryTextLimit>(text).is_err(),
                "{text}"
            );
        }
        assert!(QueryTextLimit::new(1_000_000_001).is_err());
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
        let registry = SessionPropertyRegistry::new().unwrap();
        assert!(
            SessionSqlState::from_session_value(
                parse(&input).unwrap().session_value().unwrap(),
                &registry,
                &NoCatalogAccess,
            )
            .is_ok()
        );
        input.insert(
            "x-trino-session",
            HeaderValue::from_static("unknown_setting=1"),
        );
        assert!(matches!(
            SessionSqlState::from_session_value(
                parse(&input).unwrap().session_value().unwrap(), &registry, &NoCatalogAccess,
            ),
            Err(StatementSessionError::Service(error)) if error.user_error().is_some_and(
                |user| user.code().as_str() == "sql.admit.invalid_session_property"
            )
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
        assert!(!headers.contains_key("x-trino-clear-session"));
        let headers = session_effect_headers(&SessionEffects::set_catalog_schema(
            Some("lake".into()),
            "sales".into(),
        ))
        .unwrap();
        assert_eq!(headers["x-trino-set-catalog"], "lake");
        assert_eq!(headers["x-trino-set-schema"], "sales");
        assert!(!headers.contains_key("x-trino-set-session"));
        assert!(!headers.contains_key("x-trino-clear-session"));
        // This adapter consumes already admitted typed effects. SQL evaluation
        // belongs to the original required-constant service, not this test.
        let registry = SessionPropertyRegistry::new().unwrap();
        let mut effects = SessionEffects::property(
            registry
                .prepare_sql(
                    "sql_mode",
                    SessionPropertyValue::Varchar("A,B=C +中".into()),
                )
                .unwrap(),
        );
        effects.append(SessionEffects::property(
            registry
                .prepare_sql("pipeline_dop", SessionPropertyValue::Integer(2))
                .unwrap(),
        ));
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
    fn retired_alias_is_rejected_and_canonical_effects_do_not_clear_properties() {
        use novarocks_query_application::sql::session::registry::SessionPropertyError;
        let registry = SessionPropertyRegistry::new().unwrap();
        assert!(matches!(
            registry.prepare_sql("CBO_DISABLED_RULES", SessionPropertyValue::Varchar("new_rule".into())),
            Err(SessionPropertyError::UnknownProperty { name }) if name == "cbo_disabled_rules"
        ));
        let effects = SessionEffects::property(
            registry
                .prepare_sql(
                    "DISABLE_OPTIMIZER_RULES",
                    SessionPropertyValue::Varchar("new_rule".into()),
                )
                .unwrap(),
        );
        let headers = session_effect_headers(&effects).unwrap();
        assert_eq!(
            headers["x-trino-set-session"],
            "disable_optimizer_rules=new_rule"
        );
        assert!(!headers.contains_key("x-trino-clear-session"));
    }
}

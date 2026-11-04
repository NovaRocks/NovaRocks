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

//! Version-local source header. The version's schema and formal nominal map
//! remain the sole public type declaration; this document cannot author types.

use novarocks_spi::connector::{ConnectorError, ConnectorErrorKind};
use serde::{Deserialize, Serialize};

pub(super) const KEY: &str = "novarocks.view.output-contract.v1";
const FORMAT_VERSION: u16 = 1;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    format_version: u16,
    source_field_names: Vec<Option<String>>,
}

pub(super) fn encode<'a>(
    source_field_names: impl IntoIterator<Item = Option<&'a str>>,
) -> Result<String, ConnectorError> {
    let header = Header {
        format_version: FORMAT_VERSION,
        source_field_names: source_field_names
            .into_iter()
            .map(|name| name.map(str::to_owned))
            .collect(),
    };
    if header.source_field_names.is_empty()
        || header
            .source_field_names
            .iter()
            .flatten()
            .any(String::is_empty)
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            "view output contract contains an empty header or source field name",
        ));
    }
    serde_json::to_string(&header).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::InvalidRequest,
            format!("encode view output contract: {error}"),
        )
    })
}

pub(super) fn decode(
    value: Option<&str>,
    schema_width: usize,
) -> Result<Vec<Option<String>>, ConnectorError> {
    let value = value.ok_or_else(|| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "NovaRocks view version is missing its output contract",
        )
    })?;
    let header: Header = serde_json::from_str(value).map_err(|error| {
        ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            format!("invalid NovaRocks view output contract: {error}"),
        )
    })?;
    if header.format_version != FORMAT_VERSION {
        return Err(ConnectorError::new(
            ConnectorErrorKind::Unsupported,
            format!(
                "unsupported NovaRocks view output contract version {}",
                header.format_version
            ),
        ));
    }
    if schema_width == 0
        || header.source_field_names.len() != schema_width
        || header
            .source_field_names
            .iter()
            .flatten()
            .any(String::is_empty)
    {
        return Err(ConnectorError::new(
            ConnectorErrorKind::CorruptData,
            "NovaRocks view output contract does not match its version schema",
        ));
    }
    Ok(header.source_field_names)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_header_round_trip_preserves_anonymous_occurrences_and_spelling() {
        let encoded = encode([Some("MiXeD"), None, Some("field.with.dots")]).unwrap();
        assert_eq!(
            decode(Some(&encoded), 3).unwrap(),
            vec![Some("MiXeD".into()), None, Some("field.with.dots".into())]
        );
        assert!(!encoded.contains("data_type"));
    }

    #[test]
    fn missing_or_malformed_nova_contract_cannot_be_rebuilt_from_current_body() {
        for value in [
            None,
            Some("{}"),
            Some("null"),
            Some(r#"{"format_version":1,"source_field_names":[""]}"#),
            Some(r#"{"format_version":1,"source_field_names":[null],"other":1}"#),
        ] {
            assert_eq!(
                decode(value, 1).unwrap_err().kind(),
                ConnectorErrorKind::CorruptData
            );
        }
        let encoded = encode([Some("x")]).unwrap();
        assert_eq!(
            decode(Some(&encoded), 2).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
        assert_eq!(
            decode(Some(&encoded), 0).unwrap_err().kind(),
            ConnectorErrorKind::CorruptData
        );
    }

    #[test]
    fn unknown_header_version_is_a_capability_refusal() {
        assert_eq!(
            decode(
                Some(r#"{"format_version":2,"source_field_names":[null]}"#),
                1
            )
            .unwrap_err()
            .kind(),
            ConnectorErrorKind::Unsupported
        );
    }
}

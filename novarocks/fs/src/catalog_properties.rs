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

//! Connector-neutral decoding of catalog supplied object-store properties.
//!
//! Catalog implementations retain ownership of which properties they accept.
//! This module only recognizes the shared AWS S3 property spelling and turns a
//! complete credential set into the process-local [`ObjectStoreConfig`].

use std::collections::BTreeMap;

use crate::{ObjectStoreConfig, ObjectStoreEndpointConfig, SecretValue};

/// AWS S3 properties that affect an [`ObjectStoreConfig`].
///
/// The first spelling for a setting is its canonical catalog spelling.  The
/// remaining spellings are compatibility aliases accepted from existing
/// catalogs.
pub const AWS_S3_CATALOG_PROPERTY_KEYS: &[&str] = &[
    "aws.s3.endpoint",
    "aws.s3.endpoint_url",
    "aws.s3.accessKeyId",
    "aws.s3.access_key",
    "aws.s3.accessKeySecret",
    "aws.s3.secret_key",
    "aws.s3.sessionToken",
    "aws.s3.session_token",
    "aws.s3.region",
    "aws.s3.enable_path_style_access",
    "aws.s3.max_retries",
    "aws.s3.retry_max_times",
    "aws.s3.retry_min_delay_ms",
    "aws.s3.retry_max_delay_ms",
    "aws.s3.request_timeout_ms",
    "aws.s3.timeout_ms",
    "aws.s3.io_timeout_ms",
];

const ENDPOINT_KEYS: &[&str] = &["aws.s3.endpoint", "aws.s3.endpoint_url"];
const ACCESS_KEY_ID_KEYS: &[&str] = &["aws.s3.accessKeyId", "aws.s3.access_key"];
const ACCESS_KEY_SECRET_KEYS: &[&str] = &["aws.s3.accessKeySecret", "aws.s3.secret_key"];
const SESSION_TOKEN_KEYS: &[&str] = &["aws.s3.sessionToken", "aws.s3.session_token"];
const REGION_KEYS: &[&str] = &["aws.s3.region"];
const PATH_STYLE_KEYS: &[&str] = &["aws.s3.enable_path_style_access"];
const RETRY_MAX_TIMES_KEYS: &[&str] = &["aws.s3.max_retries", "aws.s3.retry_max_times"];
const RETRY_MIN_DELAY_MS_KEYS: &[&str] = &["aws.s3.retry_min_delay_ms"];
const RETRY_MAX_DELAY_MS_KEYS: &[&str] = &["aws.s3.retry_max_delay_ms"];
const REQUEST_TIMEOUT_MS_KEYS: &[&str] = &["aws.s3.request_timeout_ms", "aws.s3.timeout_ms"];
const IO_TIMEOUT_MS_KEYS: &[&str] = &["aws.s3.io_timeout_ms"];

/// Normalizes recognized AWS S3 catalog property keys without changing values.
///
/// The normalization is intentionally limited to the connector-neutral S3
/// keys. Unknown properties are retained under their lowercase spelling, the
/// same form catalog property routing historically used.
pub fn normalize_aws_s3_catalog_properties(
    properties: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    properties
        .iter()
        .map(|(key, value)| {
            let normalized = AWS_S3_CATALOG_PROPERTY_KEYS
                .iter()
                .find(|candidate| candidate.eq_ignore_ascii_case(key))
                .map(|candidate| (*candidate).to_string())
                .unwrap_or_else(|| key.to_ascii_lowercase());
            (normalized, value.clone())
        })
        .collect()
}

/// Builds a process-local object-store configuration from AWS S3 catalog
/// properties.
///
/// A catalog can be valid without directly supplied S3 credentials (for
/// example when it reads local files or vends credentials later). Therefore an
/// absent or incomplete set of endpoint, access key, and secret returns
/// `Ok(None)`. An invalid present `aws.s3.enable_path_style_access` value is a
/// configuration error even when the credential set is incomplete. Invalid
/// numeric retry or timeout values are ignored and remain unset, so runtime
/// defaults may apply.
pub fn object_store_config_from_aws_s3_catalog_properties(
    properties: &BTreeMap<String, String>,
) -> Result<Option<ObjectStoreConfig>, String> {
    let properties = normalize_aws_s3_catalog_properties(properties);
    let enable_path_style_access = optional_bool_property(
        &properties,
        PATH_STYLE_KEYS,
        "aws.s3.enable_path_style_access",
    )?;

    let (Some(endpoint), Some(access_key_id), Some(access_key_secret)) = (
        first_nonempty_property(&properties, ENDPOINT_KEYS),
        first_nonempty_property(&properties, ACCESS_KEY_ID_KEYS),
        first_nonempty_property(&properties, ACCESS_KEY_SECRET_KEYS),
    ) else {
        return Ok(None);
    };

    Ok(Some(ObjectStoreConfig {
        endpoint: endpoint.to_string(),
        access_key_id: SecretValue::new(access_key_id),
        access_key_secret: SecretValue::new(access_key_secret),
        session_token: first_nonempty_property(&properties, SESSION_TOKEN_KEYS)
            .map(SecretValue::new),
        enable_path_style_access,
        region: first_nonempty_property(&properties, REGION_KEYS).map(str::to_string),
        retry_max_times: first_nonempty_property(&properties, RETRY_MAX_TIMES_KEYS)
            .and_then(|value| value.parse::<usize>().ok()),
        retry_min_delay_ms: first_nonempty_property(&properties, RETRY_MIN_DELAY_MS_KEYS)
            .and_then(|value| value.parse::<u64>().ok()),
        retry_max_delay_ms: first_nonempty_property(&properties, RETRY_MAX_DELAY_MS_KEYS)
            .and_then(|value| value.parse::<u64>().ok()),
        timeout_ms: first_nonempty_property(&properties, REQUEST_TIMEOUT_MS_KEYS)
            .and_then(|value| value.parse::<u64>().ok()),
        io_timeout_ms: first_nonempty_property(&properties, IO_TIMEOUT_MS_KEYS)
            .and_then(|value| value.parse::<u64>().ok()),
    }))
}

/// Decodes catalog property pairs without requiring application code to
/// duplicate the transient map conversion. The resulting credentials remain
/// process-local and cannot be persisted as catalog state.
pub fn object_store_config_from_aws_s3_catalog_property_pairs(
    properties: &[(String, String)],
) -> Result<Option<ObjectStoreConfig>, String> {
    object_store_config_from_aws_s3_catalog_properties(
        &properties
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    )
}

/// Builds non-secret object-store endpoint settings from catalog properties.
///
/// Credential material is deliberately absent: callers must resolve the exact
/// role-local credential binding before constructing an object-store provider.
pub fn object_store_endpoint_config_from_aws_s3_catalog_property_pairs(
    properties: &[(String, String)],
) -> Result<Option<ObjectStoreEndpointConfig>, String> {
    let properties = normalize_aws_s3_catalog_properties(
        &properties
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
    );
    let endpoint = first_nonempty_property(&properties, ENDPOINT_KEYS);
    let Some(endpoint) = endpoint else {
        return Ok(None);
    };
    let enable_path_style_access = optional_bool_property(
        &properties,
        PATH_STYLE_KEYS,
        "aws.s3.enable_path_style_access",
    )?;
    let parse_usize = |keys: &[&str], label: &str| {
        first_nonempty_property(&properties, keys)
            .map(|value| {
                value
                    .parse::<usize>()
                    .map_err(|_| format!("invalid {label} value"))
            })
            .transpose()
    };
    let parse_u64 = |keys: &[&str], label: &str| {
        first_nonempty_property(&properties, keys)
            .map(|value| {
                value
                    .parse::<u64>()
                    .map_err(|_| format!("invalid {label} value"))
            })
            .transpose()
    };

    Ok(Some(ObjectStoreEndpointConfig {
        endpoint: endpoint.to_string(),
        enable_path_style_access,
        region: first_nonempty_property(&properties, REGION_KEYS).map(str::to_string),
        retry_max_times: parse_usize(RETRY_MAX_TIMES_KEYS, "aws.s3.max_retries")?,
        retry_min_delay_ms: parse_u64(RETRY_MIN_DELAY_MS_KEYS, "aws.s3.retry_min_delay_ms")?,
        retry_max_delay_ms: parse_u64(RETRY_MAX_DELAY_MS_KEYS, "aws.s3.retry_max_delay_ms")?,
        timeout_ms: parse_u64(REQUEST_TIMEOUT_MS_KEYS, "aws.s3.request_timeout_ms")?,
        io_timeout_ms: parse_u64(IO_TIMEOUT_MS_KEYS, "aws.s3.io_timeout_ms")?,
    }))
}

fn first_nonempty_property<'a>(
    properties: &'a BTreeMap<String, String>,
    keys: &[&str],
) -> Option<&'a str> {
    keys.iter().find_map(|key| {
        properties
            .get(*key)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    })
}

fn optional_bool_property(
    properties: &BTreeMap<String, String>,
    keys: &[&str],
    error_key: &str,
) -> Result<Option<bool>, String> {
    let Some(value) = first_nonempty_property(properties, keys) else {
        return Ok(None);
    };
    parse_bool(value).map(Some).ok_or_else(|| {
        format!(
            "aws_s3_properties object-store property {error_key} has invalid boolean value: {value}"
        )
    })
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_aws_s3_catalog_properties, object_store_config_from_aws_s3_catalog_properties,
        object_store_config_from_aws_s3_catalog_property_pairs,
        object_store_endpoint_config_from_aws_s3_catalog_property_pairs,
    };
    use crate::SecretValue;
    use std::collections::BTreeMap;

    fn properties(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
            .collect()
    }

    #[test]
    fn normalizes_known_aws_keys_case_insensitively() {
        let normalized = normalize_aws_s3_catalog_properties(&properties(&[
            ("AWS.S3.ENDPOINT_URL", "http://localhost:9000"),
            ("custom.Flag", "value"),
        ]));

        assert_eq!(
            normalized.get("aws.s3.endpoint_url").map(String::as_str),
            Some("http://localhost:9000")
        );
        assert_eq!(
            normalized.get("custom.flag").map(String::as_str),
            Some("value")
        );
    }

    #[test]
    fn parses_complete_aws_config_with_all_optional_fields() {
        let config = object_store_config_from_aws_s3_catalog_properties(&properties(&[
            ("AWS.S3.ENDPOINT_URL", " http://localhost:9000/ "),
            ("aws.s3.accessKeyId", " ak "),
            ("aws.s3.accessKeySecret", " sk "),
            ("aws.s3.sessionToken", " token "),
            ("aws.s3.region", " us-east-1 "),
            ("aws.s3.enable_path_style_access", "YES"),
            ("aws.s3.max_retries", "9"),
            ("aws.s3.retry_min_delay_ms", "12"),
            ("aws.s3.retry_max_delay_ms", "34"),
            ("aws.s3.request_timeout_ms", "56"),
            ("aws.s3.io_timeout_ms", "78"),
        ]))
        .expect("parse catalog properties")
        .expect("complete credentials");

        assert_eq!(config.endpoint, "http://localhost:9000/");
        assert_eq!(config.access_key_id.expose_secret(), "ak");
        assert_eq!(config.access_key_secret.expose_secret(), "sk");
        assert_eq!(
            config
                .session_token
                .as_ref()
                .map(SecretValue::expose_secret),
            Some("token")
        );
        assert_eq!(config.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.enable_path_style_access, Some(true));
        assert_eq!(config.retry_max_times, Some(9));
        assert_eq!(config.retry_min_delay_ms, Some(12));
        assert_eq!(config.retry_max_delay_ms, Some(34));
        assert_eq!(config.timeout_ms, Some(56));
        assert_eq!(config.io_timeout_ms, Some(78));
    }

    #[test]
    fn parses_non_secret_endpoint_without_catalog_credentials() {
        let config = object_store_endpoint_config_from_aws_s3_catalog_property_pairs(&[
            (
                "aws.s3.endpoint".to_string(),
                "http://127.0.0.1:9000".to_string(),
            ),
            ("aws.s3.region".to_string(), "us-east-1".to_string()),
            (
                "aws.s3.enable_path_style_access".to_string(),
                "true".to_string(),
            ),
        ])
        .expect("parse endpoint")
        .expect("endpoint config");

        assert_eq!(config.endpoint, "http://127.0.0.1:9000");
        assert_eq!(config.region.as_deref(), Some("us-east-1"));
        assert_eq!(config.enable_path_style_access, Some(true));
    }

    #[test]
    fn incomplete_credentials_are_optional_but_invalid_path_style_fails() {
        let incomplete = object_store_config_from_aws_s3_catalog_properties(&properties(&[(
            "aws.s3.endpoint",
            "http://localhost:9000",
        )]))
        .expect("incomplete credentials are optional");
        assert!(incomplete.is_none());

        let error = object_store_config_from_aws_s3_catalog_properties(&properties(&[(
            "aws.s3.enable_path_style_access",
            "maybe",
        )]))
        .expect_err("invalid present bool must fail");
        assert!(error.contains("invalid boolean value: maybe"), "{error}");
    }

    #[test]
    fn malformed_retry_and_timeout_values_remain_unset() {
        let config = object_store_config_from_aws_s3_catalog_properties(&properties(&[
            ("aws.s3.endpoint", "http://localhost:9000"),
            ("aws.s3.access_key", "ak"),
            ("aws.s3.secret_key", "sk"),
            ("aws.s3.retry_max_times", "nope"),
            ("aws.s3.retry_min_delay_ms", "-1"),
            ("aws.s3.request_timeout_ms", "bad"),
            ("aws.s3.io_timeout_ms", "10"),
        ]))
        .expect("invalid numeric properties are ignored")
        .expect("complete credentials");

        assert_eq!(config.retry_max_times, None);
        assert_eq!(config.retry_min_delay_ms, None);
        assert_eq!(config.timeout_ms, None);
        assert_eq!(config.io_timeout_ms, Some(10));
    }

    #[test]
    fn parses_property_pairs_without_an_application_wrapper() {
        let config = object_store_config_from_aws_s3_catalog_property_pairs(&[
            (
                "aws.s3.endpoint".to_string(),
                "http://localhost:9000".to_string(),
            ),
            ("aws.s3.access_key".to_string(), "ak".to_string()),
            ("aws.s3.secret_key".to_string(), "sk".to_string()),
        ])
        .expect("parse property pairs")
        .expect("complete credentials");
        assert_eq!(config.endpoint, "http://localhost:9000");
    }
}

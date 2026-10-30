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

//! The one configuration handshake and immutable capability binding.
//!
//! Optional idempotency capability errors are isolated from configuration,
//! authentication and routing failures. No client property can grant support.

use std::collections::HashMap;
use std::time::Duration;

use reqwest::Method;
use serde::Deserialize;

use super::attempt::BindingStepError;
use super::auth::RestAuthOwner;
use super::binding::{SessionBinding, protocol_headers};
use super::evidence::{BindingFailure, DispatchStage};
use super::http::{RequestContext, RestRequest};

#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub(crate) enum IdempotencyCapability {
    Supported {
        lifetime: Duration,
    },
    #[default]
    Absent,
    Invalid {
        reason: &'static str,
    },
}

#[derive(Deserialize)]
struct Configuration {
    defaults: HashMap<String, String>,
    overrides: HashMap<String, String>,
    endpoints: Option<Vec<String>>,
    // A raw value deliberately distinguishes absence from JSON null and never
    // makes an otherwise valid configuration fail to decode.
    #[serde(
        default,
        rename = "idempotency-key-lifetime",
        deserialize_with = "capture_lifetime"
    )]
    lifetime: Option<Box<serde_json::value::RawValue>>,
}

fn capture_lifetime<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Box<serde_json::value::RawValue>>, D::Error> {
    Box::<serde_json::value::RawValue>::deserialize(deserializer).map(Some)
}

fn capability(raw: Option<&serde_json::value::RawValue>) -> IdempotencyCapability {
    let Some(raw) = raw else {
        return IdempotencyCapability::Absent;
    };
    let Ok(value) = serde_json::from_str::<String>(raw.get()) else {
        return IdempotencyCapability::Invalid {
            reason: "lifetime must be an ISO-8601 duration string",
        };
    };
    match parse_lifetime(&value) {
        Some(lifetime) => IdempotencyCapability::Supported { lifetime },
        None => IdempotencyCapability::Invalid {
            reason: "lifetime must be positive, fixed-length and representable",
        },
    }
}

/// Fixed-length ISO-8601 days/time durations. Calendar years and months have
/// no exact elapsed lifetime and therefore cannot authorize key recovery.
fn parse_lifetime(value: &str) -> Option<Duration> {
    let mut rest = value.strip_prefix('P')?;
    let mut time = false;
    let mut order = 0;
    let mut count = 0;
    let mut nanos: u128 = 0;
    while !rest.is_empty() {
        if let Some(next) = rest.strip_prefix('T') {
            if time {
                return None;
            }
            time = true;
            rest = next;
            if rest.is_empty() {
                return None;
            }
            continue;
        }
        let end = rest.find(|ch: char| !ch.is_ascii_digit() && ch != '.')?;
        let number = &rest[..end];
        let unit = rest.as_bytes()[end];
        rest = &rest[end + 1..];
        let (next_order, seconds): (u8, u128) = match (time, unit) {
            (false, b'D') => (1, 86400),
            (true, b'H') => (2, 3600),
            (true, b'M') => (3, 60),
            (true, b'S') => (4, 1),
            _ => return None,
        };
        if next_order <= order || number.is_empty() {
            return None;
        }
        order = next_order;
        count += 1;
        let (whole, fraction) = number.split_once('.').unwrap_or((number, ""));
        if whole.is_empty()
            || fraction.len() > 9
            || (number.contains('.') && (fraction.is_empty() || unit != b'S'))
        {
            return None;
        }
        let whole: u128 = whole.parse().ok()?;
        nanos = nanos.checked_add(whole.checked_mul(seconds)?.checked_mul(1_000_000_000)?)?;
        if !fraction.is_empty() {
            let fraction_value: u128 = fraction.parse().ok()?;
            nanos = nanos
                .checked_add(fraction_value.checked_mul(10_u128.pow(9 - fraction.len() as u32))?)?;
        }
    }
    if count == 0 || nanos == 0 {
        return None;
    }
    let seconds = u64::try_from(nanos / 1_000_000_000).ok()?;
    let duration = Duration::new(seconds, (nanos % 1_000_000_000) as u32);
    // The request owner must be able to compute a monotonic reuse deadline.
    std::time::Instant::now().checked_add(duration)?;
    Some(duration)
}

pub(crate) async fn establish(
    owner: &RestAuthOwner,
    properties: &HashMap<String, String>,
) -> Result<SessionBinding, BindingStepError> {
    establish_in(owner, properties, &RequestContext::for_read()).await
}
pub(crate) async fn establish_in(
    owner: &RestAuthOwner,
    properties: &HashMap<String, String>,
    context: &RequestContext,
) -> Result<SessionBinding, BindingStepError> {
    let mut url = reqwest::Url::parse(&format!("{}/v1/config", owner.persistent().uri()))
        .map_err(|_| BindingFailure::InvalidServerConfig)?;
    if let Some(warehouse) = properties.get("warehouse") {
        url.query_pairs_mut().append_pair("warehouse", warehouse);
    }
    let mut headers = protocol_headers();
    for (name, value) in owner.persistent().headers() {
        headers.insert(name.clone(), value.clone());
    }
    owner
        .authorize_in(&mut headers, context)
        .await
        .map_err(BindingStepError::from)?;
    let (stage, body) = owner
        .http()
        .send_once(
            Ok(RestRequest::new(Method::GET, url.to_string(), headers)),
            &[200],
            context,
        )
        .await
        .map_err(|original| {
            BindingStepError::from_original(BindingFailure::ConfigFetch, original)
        })?;
    let body = match (stage, body) {
        (DispatchStage::Status { code: 200, .. }, Some(body)) => body,
        _ => return Err(BindingFailure::ConfigFetch.into()),
    };
    let bytes = body
        .read_to_end()
        .await
        .map_err(|_| BindingFailure::ConfigFetch)?;
    let mut config: Configuration =
        serde_json::from_slice(&bytes).map_err(|_| BindingFailure::InvalidServerConfig)?;
    // URI is special in the upstream merge: only overrides can change it.
    let uri = config
        .overrides
        .remove("uri")
        .unwrap_or_else(|| owner.persistent().uri().to_string());
    let mut merged = config.defaults;
    merged.extend(properties.clone());
    merged.extend(config.overrides);
    merged.insert("uri".to_string(), uri);
    let mut binding = SessionBinding::from_merged_properties(&merged, owner.persistent())?;
    binding.set_capabilities(config.endpoints, capability(config.lifetime.as_deref()))?;
    Ok(binding)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifetime_uses_only_the_optional_top_level_field() {
        for (json, expected) in [
            (
                r#"{"defaults":{"idempotency-key-lifetime":"PT1H"},"overrides":{}}"#,
                IdempotencyCapability::Absent,
            ),
            (
                r#"{"defaults":{},"overrides":{},"idempotency-key-lifetime":"PT30M"}"#,
                IdempotencyCapability::Supported {
                    lifetime: Duration::from_secs(1800),
                },
            ),
        ] {
            let config: Configuration = serde_json::from_str(json).unwrap();
            assert_eq!(capability(config.lifetime.as_deref()), expected);
        }
        for value in [
            "null",
            "1",
            "true",
            "[]",
            "{}",
            "\"PT0S\"",
            "\"PT-1S\"",
            "\"P1Y\"",
            "\"PT999999999999999999999999999H\"",
        ] {
            let json = format!(
                r#"{{"defaults":{{}},"overrides":{{}},"idempotency-key-lifetime":{value}}}"#
            );
            let config: Configuration = serde_json::from_str(&json).unwrap();
            assert!(
                matches!(
                    capability(config.lifetime.as_deref()),
                    IdempotencyCapability::Invalid { .. }
                ),
                "{value}"
            );
        }
    }

    #[test]
    fn fixed_iso_duration_is_checked_without_rounding() {
        assert_eq!(
            parse_lifetime("P1DT2H3M4.005S"),
            Some(Duration::new(93784, 5_000_000))
        );
        for input in ["P", "PT", "PT1S1H", "PT1.1H", "PT1..2S", "PT1Sfoo", "PT0S"] {
            assert!(parse_lifetime(input).is_none(), "{input}");
        }
    }
    #[tokio::test]
    async fn the_config_request_and_effective_route_follow_the_upstream_special_merge() {
        use super::super::http::RestHttpClient;
        use super::super::test_server::{FrontendFixture, Reply, Script, TestServer};
        // Defaults may not replace URI; URI override is removed before the
        // ordinary defaults < client < overrides merge. Warehouse remains in it.
        for override_uri in [false, true] {
            let server = TestServer::start(Script::new().on_fn("GET", "/v1/config", move |_, _| {
                let mut overrides = serde_json::json!({"warehouse":"server-wh", "prefix":"effective"});
                if override_uri { overrides["uri"] = serde_json::json!("http://alternate.example/catalog"); }
                Reply::json(200, &serde_json::json!({
                    "defaults":{"uri":"http://ignored.example", "warehouse":"default-wh", "prefix":"default-prefix"},
                    "overrides":overrides, "idempotency-key-lifetime":"PT24H"
                }))
            }));
            let fixture = FrontendFixture::new(
                &[
                    ("uri", &server.url()),
                    ("warehouse", "client warehouse/&"),
                    ("prefix", "client-prefix"),
                ],
                None,
                RestHttpClient::new().unwrap(),
            )
            .await;
            let binding = fixture.ensure_session().await.unwrap();
            let expected_uri = if override_uri {
                "http://alternate.example/catalog".to_string()
            } else {
                server.url()
            };
            assert_eq!(binding.uri(), expected_uri);
            assert_eq!(binding.warehouse(), Some("server-wh"));
            assert_eq!(binding.prefix(), Some("effective"));
            assert_eq!(
                binding.idempotency_capability(),
                &IdempotencyCapability::Supported {
                    lifetime: Duration::from_secs(86400)
                }
            );
            let requests = server.requests();
            assert_eq!(requests.len(), 1);
            assert_eq!(requests[0].path(), "/v1/config");
            assert_eq!(
                requests[0].query(),
                Some("warehouse=client+warehouse%2F%26")
            );
            assert!(requests[0].header("idempotency-key").is_none());
        }
    }

    #[tokio::test]
    async fn advertised_paths_are_opaque_capabilities_and_do_not_select_request_destinations() {
        use super::super::http::RestHttpClient;
        use super::super::test_server::{FrontendFixture, Reply, Script, TestServer};
        let advertised = [
            "POST v1/oauth/tokens",
            "POST https://auth-server.com/token",
            "GET v1/config",
            "HEAD /v1/{prefix}/namespaces/{namespace}",
        ];
        let server = TestServer::start(
            Script::new()
                .on(
                    "GET",
                    "/v1/config",
                    Reply::config(&[], &[], Some(&advertised)),
                )
                .on("HEAD", "/v1/namespaces/db", Reply::bytes(200, Vec::new())),
        );
        let fixture = FrontendFixture::new(
            &[("uri", &server.url())],
            None,
            RestHttpClient::new().unwrap(),
        )
        .await;
        let binding = fixture.ensure_session().await.unwrap();
        assert_eq!(binding.uri(), server.url());
        assert_eq!(binding.advertised_endpoints().unwrap(), advertised);
        assert!(
            fixture
                .endpoints
                .namespace_exists(&crate::iceberg::NamespaceIdent::new("db".into()))
                .await
                .unwrap()
        );
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(server.count("GET", "/v1/config"), 1);
        assert_eq!(server.count("HEAD", "/v1/namespaces/db"), 1);
    }

    #[tokio::test]
    async fn malformed_advertised_endpoints_refuse_the_session_before_catalog_requests() {
        use super::super::http::RestHttpClient;
        use super::super::test_server::{FrontendFixture, Reply, Script, TestServer};
        for malformed in ["GET", "GET ", " /v1", "GET  /v1", "GET /v1 extra", ""] {
            let server = TestServer::start(Script::new().on(
                "GET",
                "/v1/config",
                Reply::config(&[], &[], Some(&[malformed])),
            ));
            let fixture = FrontendFixture::new(
                &[("uri", &server.url())],
                None,
                RestHttpClient::new().unwrap(),
            )
            .await;
            assert_eq!(
                fixture.ensure_session().await.unwrap_err().step,
                BindingFailure::InvalidServerConfig
            );
            assert_eq!(server.requests().len(), 1, "{malformed:?}");
        }
    }

    #[tokio::test]
    async fn optional_capability_and_headers_do_not_change_configuration_authority() {
        use super::super::http::RestHttpClient;
        use super::super::test_server::{FrontendFixture, Reply, Script, TestServer};
        let server = TestServer::start(Script::new().on(
            "GET",
            "/v1/config",
            Reply::config(
                &[("idempotency-key-lifetime", "PT1H")],
                &[
                    ("header.If-None-Match", "injected"),
                    ("header.Idempotency-Key", "injected"),
                ],
                None,
            ),
        ));
        let fixture = FrontendFixture::new(
            &[("uri", &server.url())],
            None,
            RestHttpClient::new().unwrap(),
        )
        .await;
        let binding = fixture.ensure_session().await.unwrap();
        assert_eq!(
            binding.idempotency_capability(),
            &IdempotencyCapability::Absent
        );
        let headers = binding.request_headers(None);
        assert!(!headers.contains_key("if-none-match"));
        assert!(!headers.contains_key("idempotency-key"));
        assert_eq!(binding.excluded_protocol_headers().len(), 2);
        assert_eq!(server.count("GET", "/v1/config"), 1);
        for name in ["header.If-None-Match", "header.Idempotency-Key"] {
            let properties = HashMap::from([
                ("uri".to_string(), server.url()),
                (name.to_string(), "injected".to_string()),
            ]);
            assert!(
                super::super::auth::PersistentRestConfig::from_properties(&properties).is_err()
            );
        }
    }
}

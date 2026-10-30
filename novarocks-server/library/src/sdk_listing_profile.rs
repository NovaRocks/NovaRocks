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

//! Startup agreement for the frozen external-listing profile.
//!
//! This checks owner parameters, never grants provider response memory. Trusted
//! SDK buffers remain the r7 exception; request deadline and stop stay external.

use anyhow::{Result, anyhow, ensure};
use novarocks_spi::connector::ConnectorListingBound;
use std::time::Duration;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ListingStartupParameters {
    iceberg_positions: usize,
    rest_connect: Duration,
    rest_read: Duration,
    list_body_bytes: usize,
    owned: ConnectorListingBound,
}

// Independent literals from accepted profile-v1. Runtime does not load a
// documentation file or accept alternate tuning through these parameters.
const FROZEN_V1: ListingStartupParameters = ListingStartupParameters {
    iceberg_positions: 8,
    rest_connect: Duration::from_millis(5000),
    rest_read: Duration::from_millis(30000),
    list_body_bytes: 16 * 1024 * 1024,
    owned: ConnectorListingBound {
        entries: 65536,
        page_entries: 256,
        pages: 1024,
        name_bytes: 65536,
        total_name_bytes: 16 * 1024 * 1024,
        continuation_token_bytes: 4096,
    },
};

fn current_parameters() -> ListingStartupParameters {
    let (positions, rest_connect, rest_read) =
        novarocks_connector_iceberg::catalog_runtime::frozen_sdk_listing_parameters();
    ListingStartupParameters {
        iceberg_positions: positions,
        rest_connect,
        rest_read,
        list_body_bytes: novarocks_fs::frozen_object_store_list_body_limit_bytes(),
        owned: ConnectorListingBound::V1,
    }
}

pub(crate) fn validate_current() -> Result<()> {
    validate_parameters(current_parameters())
}

fn exact_milliseconds(duration: Duration, name: &'static str) -> Result<u64> {
    ensure!(
        duration.subsec_nanos().is_multiple_of(1_000_000),
        "external listing {name} is not an exact millisecond duration"
    );
    let milliseconds = u64::try_from(duration.as_millis())
        .map_err(|_| anyhow!("external listing {name} milliseconds overflow"))?;
    ensure!(milliseconds != 0, "external listing {name} must be nonzero");
    Ok(milliseconds)
}

fn validate_parameters(actual: ListingStartupParameters) -> Result<()> {
    let connect_ms = exact_milliseconds(actual.rest_connect, "REST connect timeout")?;
    let read_ms = exact_milliseconds(actual.rest_read, "REST read timeout")?;
    // Arithmetic validation only. This is not an allowance for every page to
    // coexist, and no product of SDK positions and unrelated FS bytes is made.
    actual
        .owned
        .page_entries
        .checked_mul(actual.owned.pages)
        .ok_or_else(|| anyhow!("external listing page geometry overflows"))?;
    actual.owned.validate().map_err(anyhow::Error::new)?;
    for (name, value, expected) in [
        (
            "Iceberg catalog positions",
            actual.iceberg_positions,
            FROZEN_V1.iceberg_positions,
        ),
        (
            "OpenDAL List response bytes",
            actual.list_body_bytes,
            FROZEN_V1.list_body_bytes,
        ),
        (
            "owned listing entries",
            actual.owned.entries,
            FROZEN_V1.owned.entries,
        ),
        (
            "owned listing page entries",
            actual.owned.page_entries,
            FROZEN_V1.owned.page_entries,
        ),
        (
            "owned listing pages",
            actual.owned.pages,
            FROZEN_V1.owned.pages,
        ),
        (
            "owned listing name bytes",
            actual.owned.name_bytes,
            FROZEN_V1.owned.name_bytes,
        ),
        (
            "owned listing retained names bytes",
            actual.owned.total_name_bytes,
            FROZEN_V1.owned.total_name_bytes,
        ),
        (
            "owned listing token bytes",
            actual.owned.continuation_token_bytes,
            FROZEN_V1.owned.continuation_token_bytes,
        ),
    ] {
        ensure!(
            value == expected,
            "external listing {name} does not match frozen profile v1"
        );
    }
    ensure!(
        connect_ms == 5000,
        "external listing REST connect timeout does not match frozen profile v1"
    );
    ensure!(
        read_ms == 30000,
        "external listing REST read timeout does not match frozen profile v1"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn original_owner_parameters_match_the_independent_profile_literals() {
        validate_current().expect("original owner parameters agree before role composition");
        let actual = current_parameters();
        let profile: serde_json::Value = serde_json::from_str(include_str!(
            "../../../docs/testing/mem-1-m07/profile-v1.json"
        ))
        .expect("frozen profile is valid JSON");
        let sdk = &profile["external_sdk_listing"];
        assert_eq!(sdk["accounting_revision"].as_u64(), Some(7));
        for (key, actual) in [
            (
                "per_catalog_listing_concurrency",
                actual.iceberg_positions as u64,
            ),
            (
                "rest_connect_timeout_ms",
                exact_milliseconds(actual.rest_connect, "test connect").unwrap(),
            ),
            (
                "rest_read_timeout_ms",
                exact_milliseconds(actual.rest_read, "test read").unwrap(),
            ),
            (
                "object_store_list_response_body_bytes",
                actual.list_body_bytes as u64,
            ),
            ("rest_page_entries", actual.owned.page_entries as u64),
            ("maximum_listing_entries", actual.owned.entries as u64),
            ("maximum_listing_pages", actual.owned.pages as u64),
            ("single_name_bytes", actual.owned.name_bytes as u64),
            ("retained_names_bytes", actual.owned.total_name_bytes as u64),
            ("token_bytes", actual.owned.continuation_token_bytes as u64),
        ] {
            assert_eq!(sdk[key].as_u64(), Some(actual), "{key}");
        }
        let local = &profile["local_source"];
        for (key, actual) in [
            ("entries", actual.owned.entries as u64),
            ("page_entries", actual.owned.page_entries as u64),
            ("pages", actual.owned.pages as u64),
            ("single_name_path_bytes", actual.owned.name_bytes as u64),
            ("snapshot_bytes", actual.owned.total_name_bytes as u64),
            (
                "continuation_token_bytes",
                actual.owned.continuation_token_bytes as u64,
            ),
        ] {
            assert_eq!(local[key].as_u64(), Some(actual), "{key}");
        }
        // These are frozen behavioral requirements, not getter-based proof
        // of reqwest/OpenDAL I/O. Existing C6s tests exercise those behaviors.
        assert!(
            sdk.as_object()
                .unwrap()
                .contains_key("rest_overall_client_timeout_ms")
        );
        assert!(sdk["rest_overall_client_timeout_ms"].is_null());
        assert_eq!(
            sdk["object_store_body_limit_operation"].as_str(),
            Some("List")
        );
        assert_eq!(
            sdk["object_store_body_limit_retryable"].as_bool(),
            Some(false)
        );
    }

    #[test]
    fn every_sdk_parameter_drift_is_refused_including_tighter_values() {
        for mutate in [
            (|p: &mut ListingStartupParameters| p.iceberg_positions = 0)
                as fn(&mut ListingStartupParameters),
            |p| p.iceberg_positions = 7,
            |p| p.iceberg_positions = 9,
            |p| p.rest_connect = Duration::from_millis(4999),
            |p| p.rest_connect = Duration::from_millis(5001),
            |p| p.rest_read = Duration::from_millis(29999),
            |p| p.rest_read = Duration::from_millis(30001),
            |p| p.list_body_bytes = 0,
            |p| p.list_body_bytes = 16 * 1024 * 1024 - 1,
            |p| p.list_body_bytes = 16 * 1024 * 1024 + 1,
        ] {
            let mut changed = FROZEN_V1;
            mutate(&mut changed);
            assert!(validate_parameters(changed).is_err());
        }
    }

    #[test]
    fn owned_listing_profile_drift_does_not_become_a_startup_default() {
        for mutate in [
            (|p: &mut ConnectorListingBound| p.entries = 65535) as fn(&mut ConnectorListingBound),
            |p| p.page_entries = 255,
            |p| p.pages = 1023,
            |p| p.name_bytes = 65535,
            |p| p.total_name_bytes = 16 * 1024 * 1024 - 1,
            |p| p.continuation_token_bytes = 4095,
        ] {
            let mut changed = FROZEN_V1;
            mutate(&mut changed.owned);
            assert!(validate_parameters(changed).is_err());
        }
    }

    #[test]
    fn original_connector_validation_error_retains_its_type_and_classification() {
        let mut changed = FROZEN_V1;
        changed.owned.pages = 0;
        let error = validate_parameters(changed).expect_err("zero pages cannot make progress");
        let original = error
            .downcast_ref::<novarocks_spi::connector::ConnectorError>()
            .expect("startup validation must retain the original connector error");
        assert_eq!(
            original.kind(),
            novarocks_spi::connector::ConnectorErrorKind::InvalidRequest
        );
    }

    #[test]
    fn zero_fractional_and_overflowing_durations_and_page_products_fail_checked() {
        for timeout in [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::MAX,
            Duration::from_secs(u64::MAX),
        ] {
            let mut changed = FROZEN_V1;
            changed.rest_connect = timeout;
            assert!(validate_parameters(changed).is_err());
            changed = FROZEN_V1;
            changed.rest_read = timeout;
            assert!(validate_parameters(changed).is_err());
        }
        let error = exact_milliseconds(Duration::from_secs(u64::MAX), "test duration")
            .expect_err("a whole-millisecond duration can overflow the published integer");
        assert!(error.to_string().contains("milliseconds overflow"));
        let mut changed = FROZEN_V1;
        changed.owned.page_entries = usize::MAX;
        changed.owned.pages = 2;
        let error = validate_parameters(changed).expect_err("overflow before any new owner work");
        assert!(error.to_string().contains("page geometry overflows"));
    }
}

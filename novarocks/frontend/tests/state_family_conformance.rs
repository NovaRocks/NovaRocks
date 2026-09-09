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

//! Store-content conformance: every key a frontend leaves behind must belong
//! to a registered family whose classification permits durability.
//!
//! This is the behavioural form of the closed manifest. A source-shape check
//! ("no module mentions this prefix any more") would pass the moment a literal
//! moved; scanning the real keyspace passes only when the running frontend
//! actually stopped writing the retired families.
//!
//! The scan deliberately starts from an empty key and runs to an all-`0xff`
//! bound. Retired coordination keys began with a NUL byte, so a scan anchored
//! at any printable prefix would have reported success without ever looking
//! where the evidence was.
//!
//! Coverage this focused layer does **not** reach, which the product-topology
//! acceptance owns instead (`state-family/wipe-start-rebuild` and the
//! `lnp-3e-state-family` SQL suite):
//!
//! * DML, `ANALYZE` and `OPTIMIZE` statement paths, which need live backends;
//! * MV creation and refresh, which need a provider and a lake;
//! * accelerator cold rebuild after a whole-store wipe, which needs a real
//!   catalog to rebuild from.
//!
//! Naming the gaps matters: a scan that silently covered less than the
//! statement surface would look exactly like one that covered all of it.

use bytes::Bytes;
use novarocks_frontend::state_family::{DurabilityAdmission, StateFamily};
use novarocks_frontend::table_maintenance::gc_observation::{
    GcOwnedRefObservation, GcOwnedRefObservationAccelerator,
};
use novarocks_frontend::{
    ClusterBackendOpenConfig, FrontendApplicationHost, FrontendExecutionConfig,
    FrontendNativeTransport,
};
use novarocks_native_trust::{
    DeploymentId, NativeCallerSubject, NativeTransportMode, NativeTrust, ValidatedSharedSecret,
};
use novarocks_secret::SecretValue;
use novarocks_state_store_api::{Direction, Key, KeyRange, RangeRequest, StateStore};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

mod common;
use common::state_store_fixture;

/// Prefixes this series retired. They are asserted absent by value rather than
/// by manifest lookup: the manifest no longer knows these families exist, so
/// only a literal can prove the running frontend stopped writing them.
const RETIRED_PREFIXES: &[&[u8]] = &[
    b"\0novarocks/cp/v1/control",
    b"\0novarocks/cp/v1/lease/",
    b"novarocks/frontend/views/v1/",
    b"novarocks/frontend/views/v2/",
];

/// Every key in the store, paged over the whole keyspace.
async fn scan_all_keys(store: &Arc<dyn StateStore>) -> Vec<Vec<u8>> {
    // `Key` validates only length, so an empty key is legal and sorts below
    // every real key, including the NUL-prefixed retired coordination keys.
    let range = KeyRange::new(
        Key::try_from(Bytes::new()).expect("empty lower bound is a valid key"),
        Key::try_from(Bytes::from_static(&[0xff; 64])).expect("upper bound is a valid key"),
    )
    .expect("full keyspace range");
    let mut read = store.begin_read().await.expect("begin read transaction");
    let mut keys = Vec::new();
    let mut continuation = None;
    loop {
        let page = read
            .range(&RangeRequest {
                range: range.clone(),
                direction: Direction::Forward,
                page_size: store.limits().max_page_size.min(256),
                continuation,
            })
            .await
            .expect("range scan over the whole keyspace");
        keys.extend(
            page.records
                .iter()
                .map(|record| record.key.as_bytes().to_vec()),
        );
        continuation = page.continuation;
        if continuation.is_none() {
            break;
        }
    }
    keys
}

/// Assert every observed key is attributable to a durability-permitted family.
///
/// Callers pass the workload they ran so a failure names it: the useful part of
/// this assertion is which statement produced an unregistered key.
fn assert_keys_conform(keys: &[Vec<u8>], workload: &str) {
    let mut unattributed = Vec::new();
    let mut forbidden = Vec::new();
    for key in keys {
        match StateFamily::for_key(key) {
            None => unattributed.push(String::from_utf8_lossy(key).into_owned()),
            Some(family) => {
                if family.durability_admission() == DurabilityAdmission::Forbidden {
                    forbidden.push((
                        family.family_id(),
                        String::from_utf8_lossy(key).into_owned(),
                    ));
                }
            }
        }
    }
    assert!(
        unattributed.is_empty(),
        "after {workload}, these keys belong to no registered state family; \
         a durable family was added without a manifest entry: {unattributed:?}"
    );
    assert!(
        forbidden.is_empty(),
        "after {workload}, these keys belong to families whose classification \
         forbids durability; process runtime state was persisted: {forbidden:?}"
    );
    for key in keys {
        for retired in RETIRED_PREFIXES {
            assert!(
                !key.starts_with(retired),
                "after {workload}, the frontend wrote a retired family key {}",
                String::from_utf8_lossy(key)
            );
        }
    }
}

#[test]
fn retired_family_keys_are_attributable_to_no_registered_family() {
    // The retirement is only real if the manifest cannot name these families.
    // If a later change re-registers one, this fails and the absence assertion
    // in `assert_keys_conform` stops being meaningful.
    for retired in RETIRED_PREFIXES {
        let mut key = retired.to_vec();
        key.extend_from_slice(b"probe-suffix");
        assert_eq!(
            StateFamily::for_key(&key),
            None,
            "retired prefix {} is registered in the manifest again",
            String::from_utf8_lossy(retired)
        );
    }
}

#[test]
fn every_durable_family_is_reachable_by_key_attribution() {
    // `assert_keys_conform` can only catch a stray key if attribution actually
    // resolves the families that are supposed to be durable. A family whose
    // prefix never attributes back to itself would make the scan vacuous.
    for family in StateFamily::ALL {
        let Some(prefix) = family.persistent_prefix() else {
            assert_eq!(
                family.durability_admission(),
                DurabilityAdmission::Forbidden,
                "{} has no persistent prefix but permits durability",
                family.family_id()
            );
            continue;
        };
        assert_eq!(
            family.durability_admission(),
            DurabilityAdmission::Permitted,
            "{} carries a persistent prefix but forbids durability",
            family.family_id()
        );
        let mut key = prefix.as_bytes().to_vec();
        key.extend_from_slice(b"/probe-suffix");
        assert_eq!(
            StateFamily::for_key(&key),
            Some(family),
            "a key under {}'s own prefix did not attribute back to it",
            family.family_id()
        );
    }
}

fn test_native_trust() -> Arc<NativeTrust> {
    Arc::new(NativeTrust::new(
        DeploymentId::parse("state-family-conformance").expect("deployment"),
        ValidatedSharedSecret::new(SecretValue::new("0123456789abcdef0123456789abcdef"))
            .expect("secret"),
        NativeCallerSubject::parse("fe@127.0.0.1:19040").expect("subject"),
        NativeTransportMode::Disabled,
    ))
}

fn backend_config() -> ClusterBackendOpenConfig {
    ClusterBackendOpenConfig::new(
        novarocks_types::ClusterRole::Fe,
        novarocks_types::NativeCompatibilityId::new([0x71; 32]),
        Duration::from_secs(1),
        1,
        Duration::from_secs(1),
    )
    .expect("valid frontend backend config")
}

/// Open the whole frontend application, not just its StateStore host.
///
/// This is the part of the gate that exercises product code: opening the
/// application opens every owner in the series - catalog desired state,
/// backend membership, the MV accelerator, GC observations and the view
/// registry - and each one gets its chance to write. The retired coordination
/// family wrote its control record on exactly this path.
async fn open_application(
    input: novarocks_frontend::StateStoreHostInput,
) -> FrontendApplicationHost {
    let registry = state_store_fixture::registry();
    FrontendApplicationHost::open_with_role_factories_and_state_store_registry(
        Some(input),
        &registry,
        FrontendExecutionConfig::new(
            "127.0.0.1",
            19310,
            std::num::NonZeroUsize::new(1).unwrap(),
            novarocks_types::NativeCompatibilityId::new([0x71; 32]),
            std::sync::Arc::new(
                novarocks_sql::compiler::build_builtin_engine_function_catalog()
                    .expect("builtin function catalog"),
            ),
        ),
        backend_config(),
        Vec::new(),
        tokio::runtime::Handle::current(),
        test_native_trust(),
        FrontendNativeTransport::plaintext(),
    )
    .await
    .expect("open frontend application host")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_the_whole_frontend_writes_no_durable_record_at_all() {
    // Stronger, and less fragile, than scanning for unregistered keys: opening
    // a frontend now writes *nothing*. Before this series the same open
    // bootstrapped a control-plane incarnation record under
    // `\0novarocks/cp/v1/control`, so this assertion is what the coordination
    // retirement bought. A scan-for-strays assertion here would have passed
    // trivially against an empty store and proven nothing.
    let input = state_store_fixture::input("state-family-conformance-application");
    let host = open_application(input.clone()).await;
    let store = host.state_store().expect("configured StateStore");
    let keys = scan_all_keys(&store).await;
    assert!(
        keys.is_empty(),
        "opening a frontend wrote durable records before any statement ran: {:?}",
        keys.iter()
            .map(|key| String::from_utf8_lossy(key).into_owned())
            .collect::<Vec<_>>()
    );
    drop(store);
    host.shutdown()
        .await
        .expect("shutdown frontend application");

    // Reopening is the other place a retired family would resurface, through a
    // startup decoder rather than a bootstrap write.
    let reopened = open_application(input).await;
    let store = reopened.state_store().expect("configured StateStore");
    assert!(
        scan_all_keys(&store).await.is_empty(),
        "reopening a frontend wrote durable records"
    );
    drop(store);
    reopened
        .shutdown()
        .await
        .expect("shutdown reopened frontend application");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_records_written_by_real_owners_all_attribute_to_the_manifest() {
    // The scan is only evidence if something actually wrote. Drive a real
    // owner's write path, then attribute every resulting key.
    let mut host = state_store_fixture::open("state-family-conformance-writes").await;
    let store = host.state_store().expect("configured StateStore");

    let accelerator = GcOwnedRefObservationAccelerator::open(Arc::clone(&store))
        .await
        .expect("open GC owned-ref observation accelerator");
    let observation = GcOwnedRefObservation::try_new(
        Uuid::from_u128(0x3c3),
        "refs/tags/conformance".to_string(),
        7,
        1,
        [0x5a; 32],
        1,
    )
    .expect("valid owned-ref observation");
    accelerator
        .observe(observation, 1_700_000_000_000, 60_000)
        .await
        .expect("record an owned-ref observation");

    let keys = scan_all_keys(&store).await;
    assert!(
        !keys.is_empty(),
        "the GC accelerator wrote nothing, so this scan would prove nothing"
    );
    assert_keys_conform(&keys, "recording a GC owned-ref observation");
    assert_eq!(
        keys.iter()
            .filter(|key| StateFamily::for_key(key) == Some(StateFamily::GcOwnedRefObservation))
            .count(),
        keys.len(),
        "a GC observation write touched a family other than its own"
    );

    // Per-family wipe isolation: wiping this family clears exactly its own
    // prefix, and the store returns to empty because nothing else wrote.
    accelerator
        .wipe_family()
        .await
        .expect("wipe the GC observation family");
    assert!(
        scan_all_keys(&store).await.is_empty(),
        "wiping one accelerator family left records behind"
    );

    drop(store);
    drop(accelerator);
    host.shutdown(std::time::Instant::now() + Duration::from_secs(5))
        .await
        .expect("shutdown frontend StateStore host");
}

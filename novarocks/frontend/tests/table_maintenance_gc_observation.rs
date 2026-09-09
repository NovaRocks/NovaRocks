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

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::state_store_fixture;
use novarocks_frontend::table_maintenance::gc_observation::{
    GcOwnedRefObservation, GcOwnedRefObservationAccelerator, GcOwnedRefObservationDecision,
};
use novarocks_state_store_api::{
    CommitOutcome, Key, Precondition, StateStore, TransactionId, Value,
};
use uuid::Uuid;

const GC_OBSERVATION_PREFIX: &str =
    "novarocks/frontend/table-maintenance/v7/gc-owned-ref-observations/";

async fn store() -> Arc<dyn StateStore> {
    let host = state_store_fixture::open(format!("gc-observation-{}", Uuid::now_v7())).await;
    host.state_store().expect("test StateStore exposure")
}

fn observation(
    table_uuid: Uuid,
    ref_name: &str,
    head_snapshot_id: i64,
    provenance_version: u16,
    digest: u8,
) -> GcOwnedRefObservation {
    GcOwnedRefObservation::try_new(
        table_uuid,
        ref_name.to_string(),
        head_snapshot_id,
        provenance_version,
        [digest; 32],
        1,
    )
    .expect("valid test owned-ref observation")
}

fn key(table_uuid: Uuid, ref_name: &str) -> Key {
    Key::try_from(Bytes::from(format!(
        "{GC_OBSERVATION_PREFIX}{table_uuid}/{}",
        hex::encode(ref_name.as_bytes())
    )))
    .expect("valid GC observation key")
}

async fn put_raw(store: &dyn StateStore, key: Key, value: Value) {
    let mut transaction = store
        .begin_write(
            TransactionId::from(Uuid::now_v7()),
            "write GC observation corrupt test record",
        )
        .await
        .expect("begin test write");
    transaction
        .put(key, value, Precondition::Absent)
        .await
        .expect("write test record");
    assert!(matches!(
        transaction.commit().await,
        CommitOutcome::Committed(_)
    ));
}

#[tokio::test]
async fn observations_survive_process_reopen_but_changed_facts_restart_maturity() {
    let store = store().await;
    let first = GcOwnedRefObservationAccelerator::open(Arc::clone(&store))
        .await
        .expect("open accelerator");
    let table_uuid = Uuid::from_u128(0x3c1);
    let ref_name = "__novarocks_gc_candidate";
    let original = observation(table_uuid, ref_name, 101, 1, 9);

    assert_eq!(
        first.observe(original.clone(), 1_000, 500).await.unwrap(),
        GcOwnedRefObservationDecision::NotMature {
            first_observed_at_ms: 1_000
        }
    );
    drop(first);

    let reopened = GcOwnedRefObservationAccelerator::open(Arc::clone(&store))
        .await
        .expect("reopen accelerator");
    assert_eq!(
        reopened.observe(original, 1_501, 500).await.unwrap(),
        GcOwnedRefObservationDecision::Mature {
            first_observed_at_ms: 1_000
        }
    );
    let changed = observation(table_uuid, ref_name, 102, 1, 9);
    assert_eq!(
        reopened.observe(changed, 2_000, 500).await.unwrap(),
        GcOwnedRefObservationDecision::NotMature {
            first_observed_at_ms: 2_000
        }
    );
}

#[tokio::test]
async fn clone_wipe_is_idempotent_and_restarts_the_safety_clock() {
    let store = store().await;
    let accelerator = GcOwnedRefObservationAccelerator::open(Arc::clone(&store))
        .await
        .expect("open accelerator");
    let fact = observation(Uuid::from_u128(0x3c2), "__novarocks_clone", 101, 1, 9);

    accelerator.observe(fact.clone(), 1_000, 500).await.unwrap();
    assert_eq!(
        accelerator.observe(fact.clone(), 1_501, 500).await.unwrap(),
        GcOwnedRefObservationDecision::Mature {
            first_observed_at_ms: 1_000
        }
    );
    assert_eq!(
        accelerator
            .policy()
            .wipe_for_clone(&accelerator)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        accelerator
            .policy()
            .wipe_for_clone(&accelerator)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        accelerator.observe(fact, 2_000, 500).await.unwrap(),
        GcOwnedRefObservationDecision::NotMature {
            first_observed_at_ms: 2_000
        }
    );
}

#[tokio::test]
async fn corrupt_record_is_replaced_and_never_preserves_old_maturity() {
    let store = store().await;
    let accelerator = GcOwnedRefObservationAccelerator::open(Arc::clone(&store))
        .await
        .expect("open accelerator");
    let table_uuid = Uuid::from_u128(0x3c3);
    let ref_name = "__novarocks_corrupt";
    put_raw(
        store.as_ref(),
        key(table_uuid, ref_name),
        Value::try_from(Bytes::from_static(b"not-canonical-json")).expect("valid test value"),
    )
    .await;
    let fact = observation(table_uuid, ref_name, 101, 1, 9);

    assert_eq!(
        accelerator.observe(fact.clone(), 1_000, 500).await.unwrap(),
        GcOwnedRefObservationDecision::NotMature {
            first_observed_at_ms: 1_000
        }
    );
    assert_eq!(
        accelerator.observe(fact, 1_501, 500).await.unwrap(),
        GcOwnedRefObservationDecision::Mature {
            first_observed_at_ms: 1_000
        }
    );
}

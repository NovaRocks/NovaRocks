// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

// Pure predicates and typed submitted-request checks, not Native acceptance.
use super::super::result_delivery_inputs::RetentionShape;
use super::*;
use sha2::{Digest, Sha256};
#[test]
fn original_harness_role_tuple_serializes_four_flat_exact_identities() {
    let roles: [_; 4] = std::array::from_fn(|index| ProcessLaunchIdentity {
        role: ["fe", "be-0", "be-1", "be-2"][index].into(),
        pid: 100 + index as u32,
        process_start_token: format!("macos:1791559530:{}", 100 + index),
    });
    let actual = original_role_inventory((&roles[0], &roles[1..])).unwrap();
    let flat = actual.as_array().unwrap();
    assert_eq!(flat.len(), 4);
    for (row, original) in flat.iter().zip(&roles) {
        assert_eq!(row, &serde_json::to_value(original).unwrap());
    }
    // The historical tuple wire shape has two positions and must never be
    // emitted as a substitute for the independently observed four roles.
    assert_ne!(
        actual,
        serde_json::to_value((&roles[0], &roles[1..])).unwrap()
    );
    assert!(original_role_inventory((&roles[0], &roles[1..3])).is_err());
    assert!(original_role_inventory((&roles[1], &roles[1..])).is_err());
    let mut reordered = roles[1..].to_vec();
    reordered.swap(0, 1);
    assert!(original_role_inventory((&roles[0], &reordered)).is_err());
}
fn roots(sealed: bool, holder: bool) -> [BTreeMap<String, u64>; 3] {
    let mut roots: [_; 3] = std::array::from_fn(|_| {
        ROOT_FIELDS
            .iter()
            .map(|name| (name.to_string(), 0))
            .collect::<BTreeMap<_, _>>()
    });
    let row = &mut roots[1];
    for name in [
        "channels",
        "terminal_task_records",
        "producers_exited",
        "ends_published",
    ] {
        row.insert(name.into(), 1);
    }
    row.insert("sealed".into(), u64::from(sealed));
    row.insert("data_positions".into(), if sealed { 0 } else { 2 });
    row.insert("payload_bytes".into(), if sealed { 0 } else { S + 8 });
    row.insert("segments".into(), if sealed { 0 } else { 2 });
    if holder {
        for name in [
            "deliveries",
            "retained_reservations",
            "metadata_holders",
            "metadata_bytes",
        ] {
            row.insert(name.into(), 1);
        }
    }
    roots
}
#[test]
fn genuine_w2_quiet_and_sealed_positive_holder_have_distinct_facts() {
    assert!(roots_match(&roots(false, false), 1, false, false).unwrap());
    assert!(!roots_match(&roots(false, false), 1, false, true).unwrap());
    assert!(roots_match(&roots(false, true), 1, false, true).unwrap());
    assert!(roots_match(&roots(true, true), 1, true, true).unwrap());
    assert!(!roots_match(&roots(true, true), 1, false, true).unwrap());
}
#[test]
fn zero_census_or_released_participation_never_substitutes_for_sealed_holder() {
    let mut roots = roots(true, true);
    roots[1].values_mut().for_each(|value| *value = 0);
    assert!(!roots_match(&roots, 1, true, true).unwrap());
    roots[1].insert("deliveries".into(), 1);
    roots[1].insert("retained_reservations".into(), 1);
    assert!(!roots_match(&roots, 1, true, true).unwrap());
}
#[test]
fn every_required_physical_holder_dimension_and_end_frontier_must_be_positive() {
    for name in [
        "deliveries",
        "retained_reservations",
        "metadata_holders",
        "metadata_bytes",
    ] {
        let mut roots = roots(true, true);
        roots[1].insert(name.into(), 0);
        assert!(!roots_match(&roots, 1, true, true).unwrap(), "{name}");
    }
    let mut roots = roots(true, true);
    roots[1].insert("ends_acknowledged".into(), 1);
    assert!(!roots_match(&roots, 1, true, true).unwrap());
}
#[test]
fn native_full_copy_holder_is_independent_of_original_segment_release() {
    // The encoded unary DATA has an independent full-copy grant. Original
    // Worker segments remain queued before seal and exit after seal; neither
    // their presence nor their absence substitutes for the Native send owner.
    for sealed in [false, true] {
        let expected = if sealed { 0 } else { 2 };
        assert!(roots_match(&roots(sealed, true), 1, sealed, true).unwrap());
        for segments in [0, 1, 2, 3] {
            let mut rows = roots(sealed, true);
            rows[1].insert("segments".into(), segments);
            assert_eq!(
                roots_match(&rows, 1, sealed, true).unwrap(),
                segments == expected
            );
        }
        let mut rows = roots(sealed, true);
        rows[1].insert("deliveries".into(), 0);
        assert!(!roots_match(&rows, 1, sealed, true).unwrap());
    }
}
#[test]
fn foreign_root_wrong_backend_missing_or_extra_projection_field_refuse() {
    assert!(!roots_match(&roots(true, true), 0, true, true).unwrap());
    assert!(roots_match(&roots(true, true), 3, true, true).is_err());
    let mut rows = roots(true, true);
    rows[0].insert("channels".into(), 1);
    assert!(!roots_match(&rows, 1, true, true).unwrap());
    let mut rows = roots(true, true);
    rows[1].remove("metadata_holders");
    assert!(roots_match(&rows, 1, true, true).is_err());
    let mut rows = roots(true, true);
    rows[1].insert("unfrozen".into(), 1);
    assert!(roots_match(&rows, 1, true, true).is_err());
}
fn root() -> TaskIdentity {
    use novarocks_types::{
        AttemptId, BackendProcessId, QueryExecutionId, QueryId, StageId, TaskId,
    };
    TaskIdentity::new(
        QueryExecutionId::new(QueryId::new(-7, 23), AttemptId::new(2).unwrap()).unwrap(),
        StageId::new(9).unwrap(),
        TaskId::new(17).unwrap(),
        "019a0203-0405-7000-8000-000000000001"
            .parse::<BackendProcessId>()
            .unwrap(),
    )
}
#[test]
fn actual_request_encodes_full_identity_nonzero_wait_and_proven_ack_only_frontier() {
    let original = root();
    for ack in [false, true] {
        let read = request(original, ack).unwrap();
        let mut wire = encode_read(&read);
        assert_eq!(
            decode_read(&wire, FieldPath::root("component_actual_request")).unwrap(),
            read
        );
        assert_eq!(read.root_task(), original);
        assert_eq!(
            read.wanted().map(NonZeroU64::get),
            if ack { None } else { Some(1) }
        );
        assert_eq!(read.consumed(), u64::from(ack));
        wire.max_wait_millis = 0;
        assert!(decode_read(&wire, FieldPath::root("component_zero_wait")).is_err());
    }
}
#[test]
fn independent_trino_data1_literal_and_original_clocks_remain_fixed() {
    let [first, tail] = RetentionShape::Normal.independent_segments().unwrap();
    assert_eq!(first.len(), S as usize);
    assert_eq!(tail, [b'x', b'x', b'x', b'x', b'x', b'x', b'"', b']']);
    assert_eq!(
        format!("{:x}", Sha256::digest(first)),
        "708e2b855b167d324638a01359fd1be6106151a152ce41ad56dcb15811e93b0c"
    );
    assert_eq!(
        (WHOLE.as_millis(), PROTOCOL.as_millis(), SAMPLES),
        (20000, 5000, 51)
    );
}
#[test]
fn cancellation_requires_the_actual_typed_trino_error() {
    let error = |code, name, kind| {
        serde_json::from_str::<TrinoError>(&json!({"message":"actual test error","errorCode":code,"errorName":name,"errorType":kind}).to_string()).unwrap()
    };
    validate_public_terminal(&error(3, "USER_CANCELED", "USER_ERROR")).unwrap();
    for (code, name, kind) in [
        (0, "GENERIC_USER_ERROR", "USER_ERROR"),
        (65536, "GENERIC_INTERNAL_ERROR", "INTERNAL_ERROR"),
        (3, "USER_CANCELED", "INTERNAL_ERROR"),
    ] {
        assert!(validate_public_terminal(&error(code, name, kind)).is_err());
    }
}

#[test]
fn failure_evidence_reads_owned_context_without_invoking_an_unknown_error_formatter() {
    #[derive(Debug)]
    struct NeverFormat;
    impl std::fmt::Display for NeverFormat {
        fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            panic!("raw source Display must not be invoked");
        }
    }
    impl std::error::Error for NeverFormat {}
    let original = anyhow::Error::new(NeverFormat);
    assert_eq!(error_fact(&original)["class"], "OtherActualSourceRetained");
    let missing = None::<()>
        .context("required original observer field missing")
        .unwrap_err();
    let fact = error_fact(&missing);
    assert_eq!(fact["class"], "StaticContext");
    assert_eq!(fact["context"], "required original observer field missing");
}

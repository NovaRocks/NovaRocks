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

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use novarocks_spi::connector::read_stack::ConnectorReadSourceProjection as Projection;
use novarocks_spi::connector::{
    ConnectorChangePartition, ConnectorChangePartitionField, ConnectorChangePartitionTransform,
    ConnectorChangePartitionValue, ConnectorChangeWindowFacts as Facts,
    ConnectorChangeWindowNegativeWeight as Negative, ConnectorChangeWindowNetZeroBasis as Zero,
    ConnectorChangeWindowNotIncrementalReason as Reason,
    ConnectorChangeWindowPartitionImpact as Impact,
    ConnectorChangeWindowPartitionImpactBuilder as ImpactBuilder,
    ConnectorChangeWindowVolume as Volume, ConnectorErrorKind, ConnectorRequestContext,
    ConnectorStopOwner,
};

fn context(max_bytes: usize) -> ConnectorRequestContext {
    ConnectorRequestContext::try_new(
        Instant::now() + Duration::from_secs(30),
        ConnectorStopOwner::new().view(),
        1,
        max_bytes,
    )
    .expect("valid request context")
}

fn field(identity: Bytes, column: &str, value: &str) -> ConnectorChangePartitionField {
    ConnectorChangePartitionField::try_new(
        identity,
        column,
        ConnectorChangePartitionTransform::Identity,
        ConnectorChangePartitionValue::String(Arc::from(value)),
    )
    .expect("valid partition field")
}

fn partition(spec: Bytes, identity: Bytes, name: &str, value: &str) -> ConnectorChangePartition {
    ConnectorChangePartition::try_new(spec, vec![field(identity, name, value)])
        .expect("valid partition")
}

#[test]
fn closed_window_facts_permit_only_the_declared_relations() {
    let volume = Volume {
        source_event_rows_upper_bound: u128::MAX,
        source_data_file_bytes: u128::MAX,
    };
    let tiny_request = context(1);
    let partition_impact = ImpactBuilder::new(false, &tiny_request).finish();
    assert_eq!(partition_impact, Impact::Unavailable);

    let reasons = [
        Reason::SnapshotAncestryBroken {
            from_snapshot_id: 5,
        },
        Reason::SchemaChanged { snapshot_id: 9 },
    ];
    let net_zero = [Zero::SameSnapshot, Zero::ReplaceOnly, Zero::EqualEndpoints];
    let negatives = [Negative::Absent, Negative::Possible];
    let cases = reasons
        .into_iter()
        .map(|reason| (Facts::NotIncremental(reason), false, false))
        .chain(
            net_zero
                .into_iter()
                .map(|basis| (Facts::NetZero(basis), false, true)),
        )
        .chain(negatives.into_iter().map(|negative_weight| {
            (
                Facts::Difference {
                    negative_weight,
                    partition_impact: partition_impact.clone(),
                    volume,
                },
                true,
                true,
            )
        }));
    for (facts, permits_from, permits_delta) in cases {
        assert!(facts.permits(Projection::To));
        assert_eq!(facts.permits(Projection::From), permits_from);
        assert_eq!(facts.permits(Projection::Delta), permits_delta);
        if let Facts::Difference { volume: actual, .. } = facts {
            assert_eq!(actual, volume);
        }
    }
}

#[test]
fn exact_hint_charges_unique_entries_per_side_and_degrades_at_the_boundary() {
    // Each partition retains 16+24 structural bytes and four one-byte strings.
    let budget = context(16 + 2 * 44);
    let partition = |value| {
        partition(
            Bytes::from_static(b"s"),
            Bytes::from_static(b"i"),
            "c",
            value,
        )
    };
    let mut builder = ImpactBuilder::new(true, &budget);
    let first = partition("a");
    builder.add(first.clone());
    builder.add(first.clone());
    builder.remove(first.clone());
    let Impact::Exact(exact) = builder.finish() else {
        panic!("both sides fit exactly");
    };
    assert!(exact.has_row_deletes());
    assert_eq!(exact.added(), std::slice::from_ref(&first));
    assert_eq!(exact.removed(), std::slice::from_ref(&first));
    let impact = Impact::Exact(exact.clone());
    let Impact::Exact(clone) = impact.clone() else {
        panic!("clone has the sealed exact payload");
    };
    assert!(Arc::ptr_eq(&exact, &clone));

    let mut builder = ImpactBuilder::new(true, &budget);
    builder.add(first.clone());
    builder.remove(first.clone());
    builder.add(partition("b"));
    builder.add(first);
    assert!(!builder.is_available());
    assert_eq!(builder.finish(), Impact::Unavailable);
}

#[test]
fn exact_hint_keeps_semantic_identities_and_chooses_a_canonical_display_name() {
    let make = |spec, identity, name| {
        partition(
            Bytes::from_static(spec),
            Bytes::from_static(identity),
            name,
            "v",
        )
    };
    let mut builder = ImpactBuilder::new(false, &context(16 + 3 * 44));
    builder.add(make(b"s", b"i", "z"));
    builder.add(make(b"s", b"i", "a"));
    builder.add(make(b"t", b"i", "b"));
    builder.add(make(b"s", b"j", "c"));
    let Impact::Exact(exact) = builder.finish() else {
        panic!("one canonical name per identity fits");
    };
    assert_eq!(
        exact
            .added()
            .iter()
            .map(|partition| partition.fields()[0].source_column())
            .collect::<Vec<_>>(),
        ["a", "c", "b"],
    );

    let mut builder = ImpactBuilder::new(false, &context(16 + 2 * 44));
    builder.add(make(b"s", b"i", "z"));
    builder.add(make(b"s", b"i", "aaa"));
    builder.add(make(b"t", b"i", "b"));
    assert_eq!(builder.finish(), Impact::Unavailable);

    let mut builder = ImpactBuilder::new(false, &context(16 + 2 * 44));
    builder.add(make(b"s", b"i", "zzz"));
    builder.add(make(b"s", b"i", "a"));
    builder.add(make(b"t", b"i", "b"));
    assert!(matches!(builder.finish(), Impact::Exact(_)));
}

#[test]
fn old_partition_caps_do_not_limit_structurally_valid_hints() {
    let mut builder = ImpactBuilder::new(false, &context(8 * 1024 * 1024));
    let mut fields = (1..=257)
        .map(|buckets| {
            ConnectorChangePartitionField::try_new(
                Bytes::from_static(b"field"),
                "account",
                ConnectorChangePartitionTransform::Bucket {
                    buckets: NonZeroU32::new(buckets).expect("nonzero bucket count"),
                },
                ConnectorChangePartitionValue::Null,
            )
            .expect("valid bucket partition field")
        })
        .collect::<Vec<_>>();
    fields.reverse();
    let wide_partition = ConnectorChangePartition::try_new(Bytes::from(vec![1; 1025]), fields)
        .expect("valid wide partition");
    assert_eq!(
        wide_partition.fields()[0].transform(),
        ConnectorChangePartitionTransform::Bucket {
            buckets: NonZeroU32::MIN
        },
    );
    for idx in 0..=256_u32 {
        let partition = ConnectorChangePartition::try_new(
            Bytes::copy_from_slice(&idx.to_be_bytes()),
            wide_partition.fields().to_vec(),
        )
        .expect("valid wide partition");
        builder.add(partition);
    }
    builder.add(wide_partition);
    builder.add(partition(
        Bytes::from_static(b"s"),
        Bytes::from(vec![2; 1025]),
        "i",
        "v",
    ));
    for idx in 0..16_384_u32 {
        builder.add(partition(
            Bytes::from_static(b"m"),
            Bytes::copy_from_slice(&idx.to_be_bytes()),
            "i",
            "v",
        ));
    }
    let Impact::Exact(exact) = builder.finish() else {
        panic!("all legal entries fit the request budget");
    };
    assert_eq!(exact.added().len(), 257 + 2 + 16_384);
}

#[test]
fn invalid_partition_shape_still_fails_construction() {
    let first = field(Bytes::from_static(b"i"), "c", "v");
    assert_eq!(
        ConnectorChangePartition::try_new(Bytes::new(), vec![first.clone()])
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::InvalidRequest,
    );
    assert_eq!(
        ConnectorChangePartition::try_new(Bytes::from_static(b"s"), Vec::new())
            .unwrap_err()
            .kind(),
        ConnectorErrorKind::InvalidRequest,
    );
    assert_eq!(
        ConnectorChangePartition::try_new(
            Bytes::from_static(b"s"),
            vec![first, field(Bytes::from_static(b"i"), "renamed", "w")],
        )
        .unwrap_err()
        .kind(),
        ConnectorErrorKind::InvalidRequest,
    );
    assert_eq!(
        ConnectorChangePartitionField::try_new(
            Bytes::new(),
            "c",
            ConnectorChangePartitionTransform::Identity,
            ConnectorChangePartitionValue::Null,
        )
        .unwrap_err()
        .kind(),
        ConnectorErrorKind::InvalidRequest,
    );
}

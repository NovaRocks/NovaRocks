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

use std::collections::{BTreeMap, HashSet};
use std::hash::{BuildHasherDefault, Hasher};
use std::sync::Arc;

use crate::iceberg::spec::{
    Datum, FormatVersion, Literal, ManifestStatus, NestedField, PartitionSpec, PrimitiveLiteral,
    PrimitiveType, Schema, Struct, Type,
};

use super::*;

fn schema() -> Schema {
    Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "p", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(3, "f", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::optional(4, "s", Type::Primitive(PrimitiveType::String)).into(),
        ])
        .build()
        .unwrap()
}

fn spec(id: i32) -> PartitionSpec {
    match id {
        0 => PartitionSpec::unpartition_spec(),
        _ => serde_json::from_value(serde_json::json!({"spec-id": id, "fields": [{"source-id":2,"field-id":1000,"name":"p","transform": if id == 3 { "void" } else { "identity" }}]})).unwrap(),
    }
}

fn partition(id: i32, value: Option<i32>) -> TypedPartition {
    let values: Struct = if id == 0 {
        Struct::empty()
    } else {
        [value.map(Literal::int)].into_iter().collect()
    };
    TypedPartition::bind(&spec(id), &schema(), &values).unwrap()
}

fn domain(snapshot: i64) -> Arc<ReadDomain> {
    Arc::new(ReadDomain::new(
        ReadObservationId::try_new([1; 16]).unwrap(),
        PinnedEndpointFacts::try_new(
            uuid::Uuid::from_bytes([2; 16]),
            "s3://table/metadata.json",
            snapshot,
            &schema(),
            &[spec(0), spec(1), spec(2), spec(3)],
        )
        .unwrap(),
    ))
}

fn seq(value: i64) -> DataSequenceNumber {
    DataSequenceNumber::try_new(value).unwrap()
}

fn data(path: &str, sequence: i64, spec_id: i32, value: Option<i32>) -> DataFileFact {
    DataFileFact::try_new(
        path,
        seq(sequence),
        partition(spec_id, value),
        10,
        FileMetrics::default(),
    )
    .unwrap()
}

fn group(ids: &[i32]) -> EqualityFieldGroup {
    EqualityFieldGroup::bind(ids, &schema()).unwrap()
}

fn fact(path: &str, sequence: i64, partition: TypedPartition, kind: DeleteKind) -> Arc<DeleteFact> {
    let dv = matches!(kind, DeleteKind::DeletionVector { .. });
    Arc::new(
        DeleteFact::try_new(DeleteFactParams {
            address: if dv {
                DeleteContentAddress::puffin(path, 4, 42, 100).unwrap()
            } else {
                DeleteContentAddress::file(path).unwrap()
            },
            kind,
            sequence: seq(sequence),
            partition,
            read: DeleteReadFacts {
                format: if dv {
                    DeleteFormat::Puffin
                } else {
                    DeleteFormat::Parquet
                },
                file_size: 100,
                record_count: 1,
                key_metadata: Arc::from([]),
            },
            metrics: FileMetrics::default(),
        })
        .unwrap(),
    )
}

fn equality(path: &str, sequence: i64, spec_id: i32, value: Option<i32>) -> Arc<DeleteFact> {
    fact(
        path,
        sequence,
        partition(spec_id, value),
        DeleteKind::Equality(group(&[1])),
    )
}

fn dv(path: &str, target: &str, sequence: i64) -> Arc<DeleteFact> {
    fact(
        path,
        sequence,
        partition(1, Some(99)),
        DeleteKind::DeletionVector {
            exact_target: Arc::from(target),
        },
    )
}

fn with_metrics(fact: &DeleteFact, metrics: FileMetrics) -> Arc<DeleteFact> {
    Arc::new(
        DeleteFact::try_new(DeleteFactParams {
            address: fact.address().clone(),
            kind: fact.kind().clone(),
            sequence: fact.sequence(),
            partition: fact.partition().clone(),
            read: fact.read().clone(),
            metrics,
        })
        .unwrap(),
    )
}

fn raw(fact: &DeleteFact, status: ManifestStatus, sequence: Option<i64>) -> RawDeleteEntry {
    RawDeleteEntry {
        sequence: EntrySequence {
            format_version: FormatVersion::V2,
            status,
            data_sequence: sequence,
            manifest_sequence: 10,
        },
        file: RawDeleteFile {
            address: fact.address().clone(),
            kind: fact.kind().clone(),
            partition: fact.partition().clone(),
            read: fact.read().clone(),
            metrics: fact.metrics().clone(),
        },
    }
}

fn observation(entries: Vec<RawDeleteEntry>) -> Result<DeleteObservation> {
    DeleteObservation::from_manifests([ManifestDeleteObservation {
        manifest_path: Arc::from("manifest-1"),
        entries,
    }])
}

fn index(facts: Vec<Arc<DeleteFact>>) -> DeleteCandidateIndex {
    DeleteCandidateIndex::try_new(
        domain(20),
        DeleteObservation::from_normalized_recall(facts).unwrap(),
    )
    .unwrap()
}

fn applications(set: &DeleteSet) -> HashSet<DeleteApplication> {
    set.members()
        .map(|fact| fact.application().clone())
        .collect()
}

#[test]
fn live_sequence_normalization_uses_only_legal_inheritance() {
    let mut entry = EntrySequence {
        format_version: FormatVersion::V2,
        status: ManifestStatus::Added,
        data_sequence: None,
        manifest_sequence: 7,
    };
    assert_eq!(entry.live_sequence().unwrap(), Some(seq(7)));
    entry.data_sequence = Some(3);
    assert_eq!(entry.live_sequence().unwrap(), Some(seq(3)));
    entry.status = ManifestStatus::Existing;
    entry.data_sequence = None;
    assert_eq!(
        entry.live_sequence().unwrap_err().kind,
        DeleteSemanticsErrorKind::MissingSequence
    );
    entry.format_version = FormatVersion::V1;
    assert_eq!(entry.live_sequence().unwrap(), Some(seq(0)));
}

#[test]
fn deleted_entries_are_skipped_but_change_consumers_cannot_inherit_them() {
    let entry = EntrySequence {
        format_version: FormatVersion::V3,
        status: ManifestStatus::Deleted,
        data_sequence: None,
        manifest_sequence: 7,
    };
    assert_eq!(entry.live_sequence().unwrap(), None);
    assert_eq!(
        entry.required_sequence().unwrap_err().kind,
        DeleteSemanticsErrorKind::MissingSequence
    );
    let deleted = raw(&dv("dv", "data", 3), ManifestStatus::Deleted, None);
    assert!(observation(vec![deleted]).unwrap().facts().is_empty());
}

#[test]
fn negative_explicit_and_inherited_sequences_are_errors() {
    for explicit in [Some(-1), None] {
        let entry = EntrySequence {
            format_version: FormatVersion::V2,
            status: ManifestStatus::Added,
            data_sequence: explicit,
            manifest_sequence: -1,
        };
        assert_eq!(
            entry.live_sequence().unwrap_err().kind,
            DeleteSemanticsErrorKind::InvalidSequence
        );
    }
    assert_eq!(seq(0).get(), 0);
}

#[test]
fn typed_partition_includes_spec_null_and_type_and_recognizes_all_void() {
    assert_ne!(partition(1, Some(1)), partition(2, Some(1)));
    assert_ne!(partition(1, None), partition(1, Some(0)));
    assert!(partition(0, None).is_unpartitioned());
    assert!(partition(3, None).is_unpartitioned());
    assert!(!partition(1, None).is_unpartitioned());
    assert!(TypedPartition::bind(&spec(1), &schema(), &Struct::empty()).is_err());
}

#[test]
fn stored_unknown_transform_values_are_not_discarded() {
    let unknown: PartitionSpec = serde_json::from_value(serde_json::json!({"spec-id":8,"fields":[{"source-id":2,"field-id":1000,"name":"p","transform":"unknown"}]})).unwrap();
    let a = TypedPartition::bind(
        &unknown,
        &schema(),
        &[Some(Literal::string("a"))].into_iter().collect(),
    )
    .unwrap();
    let b = TypedPartition::bind(
        &unknown,
        &schema(),
        &[Some(Literal::string("b"))].into_iter().collect(),
    )
    .unwrap();
    assert_ne!(a, b);
    assert!(!a.is_unpartitioned());
}

#[test]
fn float_keys_canonicalize_nan_and_preserve_signed_zero() {
    let key = |value: f64| {
        CanonicalScalar::from_literal(
            &PrimitiveLiteral::Double(value.into()),
            &PrimitiveType::Double,
        )
        .unwrap()
    };
    assert_eq!(key(f64::NAN), key(f64::from_bits(0xfff8_0000_0000_0042)));
    assert_ne!(key(-0.0), key(0.0));
    let mut keys = HashSet::new();
    keys.insert(key(f64::NAN));
    keys.insert(key(f64::from_bits(0x7ff8_0000_0000_0042)));
    assert_eq!(keys.len(), 1);
}

#[test]
fn historical_values_and_partition_constants_share_resolved_promoted_keys() {
    let from_int =
        CanonicalScalar::from_literal(&PrimitiveLiteral::Int(7), &PrimitiveType::Long).unwrap();
    let from_long =
        CanonicalScalar::from_literal(&PrimitiveLiteral::Long(7), &PrimitiveType::Long).unwrap();
    assert_eq!(from_int, from_long);
    let promoted_schema = Schema::builder()
        .with_fields(vec![
            NestedField::optional(2, "p", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .unwrap();
    let old = TypedPartition::bind(
        &spec(1),
        &promoted_schema,
        &[Some(Literal::int(7))].into_iter().collect(),
    )
    .unwrap();
    let new = TypedPartition::bind(
        &spec(1),
        &promoted_schema,
        &[Some(Literal::long(7))].into_iter().collect(),
    )
    .unwrap();
    assert_eq!(old, new);
    assert_eq!(old.values(), &[Some(from_long)]);
    assert_eq!(
        CanonicalScalar::from_literal(
            &PrimitiveLiteral::Float(1.5f32.into()),
            &PrimitiveType::Double
        )
        .unwrap(),
        CanonicalScalar::from_literal(
            &PrimitiveLiteral::Double(1.5f64.into()),
            &PrimitiveType::Double
        )
        .unwrap()
    );
}

#[test]
fn equality_fields_bind_ids_and_types_independently_of_manifest_order() {
    assert_eq!(group(&[2, 1]), group(&[1, 2]));
    assert_eq!(group(&[1]).fields(), &[(1, PrimitiveType::Long)]);
    assert!(EqualityFieldGroup::bind(&[], &schema()).is_err());
    assert!(EqualityFieldGroup::bind(&[1, 1], &schema()).is_err());
    assert!(EqualityFieldGroup::bind(&[99], &schema()).is_err());
}

#[test]
fn content_addresses_distinguish_blobs_and_validate_complete_ranges() {
    assert_ne!(
        DeleteContentAddress::puffin("p", 4, 42, 100).unwrap(),
        DeleteContentAddress::puffin("p", 46, 42, 100).unwrap()
    );
    for (offset, length) in [(-1, 42), (4, 0), (80, 42), (i64::MAX, 42)] {
        assert!(DeleteContentAddress::puffin("p", offset, length, 100).is_err());
    }
    assert!(DeleteContentAddress::file("").is_err());
    assert_ne!(
        DeleteContentAddress::file("s3://a/f").unwrap(),
        DeleteContentAddress::file("s3a://a/f").unwrap()
    );
}

#[test]
fn raw_duplicate_dv_entries_fail_but_duplicate_manifest_references_are_one_observation() {
    let entry = raw(&dv("p", "data", 10), ManifestStatus::Added, Some(10));
    assert_eq!(
        observation(vec![entry.clone(), entry.clone()])
            .unwrap_err()
            .kind,
        DeleteSemanticsErrorKind::MultipleDeletionVectors
    );
    let manifest = ManifestDeleteObservation {
        manifest_path: Arc::from("one"),
        entries: vec![entry.clone()],
    };
    let accepted = DeleteObservation::from_manifests([manifest.clone(), manifest]).unwrap();
    assert_eq!(accepted.facts().len(), 1);
    assert_eq!(accepted.observed_manifest_count(), 1);
    let other = ManifestDeleteObservation {
        manifest_path: Arc::from("two"),
        entries: vec![entry],
    };
    let first = ManifestDeleteObservation {
        manifest_path: Arc::from("one"),
        entries: accepted
            .facts()
            .iter()
            .map(|f| raw(f, ManifestStatus::Added, Some(10)))
            .collect(),
    };
    assert_eq!(
        DeleteObservation::from_manifests([first, other])
            .unwrap_err()
            .kind,
        DeleteSemanticsErrorKind::MultipleDeletionVectors
    );
}

#[test]
fn raw_position_and_equality_multiplicity_and_provenance_survive() {
    for fact in [
        equality("e", 4, 1, Some(1)),
        fact(
            "p",
            4,
            partition(1, Some(1)),
            DeleteKind::Position { exact_target: None },
        ),
    ] {
        let raw = raw(&fact, ManifestStatus::Existing, Some(4));
        let observed = observation(vec![raw.clone(), raw]).unwrap();
        assert_eq!(observed.facts().len(), 2);
        assert_eq!(observed.facts()[0].provenance().unwrap().entry_ordinal, 0);
        assert_eq!(observed.facts()[1].provenance().unwrap().entry_ordinal, 1);
    }
}

#[test]
fn normalized_recall_is_idempotent_without_weakening_raw_uniqueness() {
    let one = dv("p", "data", 10);
    assert_eq!(
        DeleteObservation::from_normalized_recall([one.clone(), one])
            .unwrap()
            .facts()
            .len(),
        1
    );
    assert_eq!(
        DeleteObservation::from_normalized_recall([dv("p", "data", 10), dv("p", "data", 11)])
            .unwrap_err()
            .kind,
        DeleteSemanticsErrorKind::MultipleDeletionVectors
    );
}

#[test]
fn one_dv_blob_can_have_two_distinct_target_applications() {
    let a = dv("p", "a", 10);
    let b = dv("p", "b", 10);
    assert_eq!(a.address(), b.address());
    assert_ne!(a.application(), b.application());
    let observed = observation(vec![
        raw(&a, ManifestStatus::Added, Some(10)),
        raw(&b, ManifestStatus::Added, Some(10)),
    ])
    .unwrap();
    let index = DeleteCandidateIndex::try_new(domain(20), observed).unwrap();
    assert!(matches!(
        index
            .for_data(&data("a", 9, 1, Some(1)))
            .unwrap()
            .position(),
        PositionSource::OneDv(_)
    ));
    assert!(matches!(
        index
            .for_data(&data("b", 9, 2, Some(2)))
            .unwrap()
            .position(),
        PositionSource::OneDv(_)
    ));
}

#[test]
fn same_address_different_sequence_count_scope_and_fields_are_retained() {
    let a = equality("e", 3, 1, Some(1));
    let mut count = a.read().clone();
    count.record_count = 9;
    let count = Arc::new(
        DeleteFact::try_new(DeleteFactParams {
            address: a.address().clone(),
            kind: a.kind().clone(),
            sequence: a.sequence(),
            partition: a.partition().clone(),
            read: count,
            metrics: FileMetrics::default(),
        })
        .unwrap(),
    );
    let observed = DeleteObservation::from_normalized_recall([
        a,
        count,
        equality("e", 4, 1, Some(1)),
        equality("e", 3, 1, Some(2)),
        fact(
            "e",
            3,
            partition(1, Some(1)),
            DeleteKind::Equality(group(&[2])),
        ),
    ])
    .unwrap();
    assert_eq!(observed.facts().len(), 5);
    let index = DeleteCandidateIndex::try_new(domain(20), observed).unwrap();
    assert_eq!(
        index
            .for_data(&data("d", 2, 1, Some(1)))
            .unwrap()
            .member_count(),
        4
    );
    assert_eq!(
        index
            .for_data(&data("d", 3, 1, Some(1)))
            .unwrap()
            .member_count(),
        1
    );
}

#[test]
fn kind_specific_sequences_include_same_commit_positions_but_not_equality() {
    for delete_sequence in 1..=3 {
        let index = index(vec![
            equality("e", delete_sequence, 1, Some(1)),
            fact(
                "p",
                delete_sequence,
                partition(1, Some(1)),
                DeleteKind::Position { exact_target: None },
            ),
        ]);
        let selected = index.for_data(&data("d", 2, 1, Some(1))).unwrap();
        assert_eq!(
            selected
                .equality()
                .iter()
                .map(|v| v.members().len())
                .sum::<usize>(),
            usize::from(delete_sequence > 2)
        );
        assert_eq!(
            selected.member_count(),
            usize::from(delete_sequence > 2) + usize::from(delete_sequence >= 2)
        );
    }
}

#[test]
fn exact_path_position_and_dv_ignore_partition_but_untargeted_position_does_not() {
    let data = data("d", 2, 1, Some(1));
    let exact = fact(
        "p",
        2,
        partition(2, Some(99)),
        DeleteKind::Position {
            exact_target: Some(Arc::from("d")),
        },
    );
    let partitioned = fact(
        "q",
        2,
        partition(2, Some(99)),
        DeleteKind::Position { exact_target: None },
    );
    assert_eq!(
        index(vec![exact, partitioned])
            .for_data(&data)
            .unwrap()
            .member_count(),
        1
    );
    assert_eq!(
        index(vec![dv("v", "d", 2)])
            .for_data(&data)
            .unwrap()
            .member_count(),
        1
    );
    assert_eq!(
        index(vec![dv("v", "other", 1)])
            .for_data(&data)
            .unwrap()
            .member_count(),
        0
    );
}

#[test]
fn equality_global_is_proven_by_spec_and_never_inferred_from_data_partition() {
    let index = index(vec![
        equality("g", 3, 0, None),
        equality("void", 3, 3, None),
        equality("a", 3, 1, Some(1)),
        equality("b", 3, 1, Some(2)),
    ]);
    assert_eq!(
        index
            .for_data(&data("d", 2, 1, Some(1)))
            .unwrap()
            .member_count(),
        3
    );
    assert_eq!(
        index
            .for_data(&data("d", 2, 2, Some(1)))
            .unwrap()
            .member_count(),
        2
    );
}

#[test]
fn dv_validation_precedes_suffix_and_supersedes_position_files() {
    let position = fact(
        "p",
        8,
        partition(1, Some(1)),
        DeleteKind::Position { exact_target: None },
    );
    let selected = index(vec![
        position.clone(),
        dv("v", "d", 2),
        equality("e", 3, 1, Some(1)),
    ])
    .for_data(&data("d", 2, 1, Some(1)))
    .unwrap();
    assert!(matches!(selected.position(), PositionSource::OneDv(_)));
    assert_eq!(selected.member_count(), 2);
    assert_eq!(
        index(vec![position, dv("v", "d", 1)])
            .for_data(&data("d", 2, 1, Some(1)))
            .unwrap_err()
            .kind,
        DeleteSemanticsErrorKind::DeletionVectorOlderThanData
    );
}

#[test]
fn reliable_path_bounds_infer_exact_target_but_broad_or_missing_bounds_do_not() {
    let metrics = |lower: &str, upper: &str| {
        FileMetrics::new(BTreeMap::from([(
            POSITION_FILE_PATH_FIELD_ID,
            FieldMetrics {
                lower_bound: Some(Datum::string(lower)),
                upper_bound: Some(Datum::string(upper)),
                ..FieldMetrics::unknown(PrimitiveType::String)
            },
        )]))
    };
    let base = fact(
        "p",
        3,
        partition(2, Some(2)),
        DeleteKind::Position { exact_target: None },
    );
    let narrow = with_metrics(&base, metrics("d", "d"));
    assert!(
        matches!(narrow.kind(), DeleteKind::Position { exact_target: Some(path) } if path.as_ref() == "d")
    );
    assert_eq!(
        index(vec![narrow])
            .for_data(&data("d", 2, 1, Some(1)))
            .unwrap()
            .member_count(),
        1
    );
    assert!(matches!(
        with_metrics(&base, metrics("a", "z")).kind(),
        DeleteKind::Position { exact_target: None }
    ));
    assert!(FileMetrics::default().exact_position_target().is_none());
}

fn long_metrics(lower: i64, upper: i64, null_count: Option<u64>) -> FieldMetrics {
    FieldMetrics {
        resolved_type: PrimitiveType::Long,
        value_count: Some(10),
        null_count,
        nan_count: None,
        lower_bound: Some(Datum::long(lower)),
        upper_bound: Some(Datum::long(upper)),
    }
}

#[test]
fn missing_null_statistics_do_not_prove_disjoint_keys() {
    assert!(long_metrics(1, 2, None).may_overlap(&long_metrics(8, 9, None)));
    assert!(!long_metrics(1, 2, Some(0)).may_overlap(&long_metrics(8, 9, None)));
    assert!(long_metrics(1, 2, Some(1)).may_overlap(&long_metrics(8, 9, Some(1))));
}

#[test]
fn all_null_empty_and_nan_cases_are_independently_proved() {
    let all_null = FieldMetrics {
        value_count: Some(10),
        null_count: Some(10),
        ..FieldMetrics::unknown(PrimitiveType::Long)
    };
    assert!(!all_null.may_overlap(&long_metrics(1, 2, Some(0))));
    assert!(all_null.may_overlap(&long_metrics(1, 2, None)));
    let empty = FieldMetrics {
        value_count: Some(0),
        ..FieldMetrics::unknown(PrimitiveType::Long)
    };
    assert!(!empty.may_overlap(&FieldMetrics::unknown(PrimitiveType::Long)));
    let float = |low, high, nan| FieldMetrics {
        resolved_type: PrimitiveType::Double,
        value_count: Some(10),
        null_count: Some(0),
        nan_count: nan,
        lower_bound: Some(Datum::double(low)),
        upper_bound: Some(Datum::double(high)),
    };
    assert!(float(1.0, 2.0, None).may_overlap(&float(8.0, 9.0, None)));
    assert!(!float(1.0, 2.0, Some(0)).may_overlap(&float(8.0, 9.0, None)));
    assert!(float(f64::NAN, 2.0, Some(0)).may_overlap(&float(8.0, 9.0, Some(0))));
}

#[test]
fn illegal_counts_and_bounds_remain_conservative() {
    let malformed = FieldMetrics {
        null_count: Some(11),
        ..long_metrics(1, 2, Some(0))
    };
    assert!(malformed.may_overlap(&long_metrics(8, 9, Some(0))));
    assert!(long_metrics(2, 1, Some(0)).may_overlap(&long_metrics(8, 9, Some(0))));
    let incompatible = FieldMetrics {
        lower_bound: Some(Datum::double(1.0)),
        upper_bound: Some(Datum::double(2.0)),
        ..long_metrics(1, 2, Some(0))
    };
    assert!(incompatible.may_overlap(&long_metrics(8, 9, Some(0))));
}

#[test]
fn bounds_promote_only_legally_and_outer_truncation_is_safe() {
    let promoted = FieldMetrics {
        lower_bound: Some(Datum::int(1)),
        upper_bound: Some(Datum::int(2)),
        ..long_metrics(1, 2, Some(0))
    };
    assert!(!promoted.may_overlap(&long_metrics(8, 9, Some(0))));
    let range = |lo: &str, hi: &str| FieldMetrics {
        resolved_type: PrimitiveType::String,
        null_count: Some(0),
        lower_bound: Some(Datum::string(lo)),
        upper_bound: Some(Datum::string(hi)),
        ..FieldMetrics::unknown(PrimitiveType::String)
    };
    // An outward upper truncation can overlap while the actual strings do not.
    assert!(range("a", "b").may_overlap(&range("az", "azzz")));
    assert!(!range("a", "b").may_overlap(&range("c", "d")));
}

#[test]
fn signed_fractional_and_decimal_bounds_promote_by_value_not_bit_reinterpretation() {
    let promoted = FieldMetrics {
        lower_bound: Some(Datum::int(-17)),
        upper_bound: Some(Datum::int(29)),
        ..long_metrics(0, 0, Some(0))
    }
    .bind_bounds();
    assert_eq!(promoted.lower_bound, Some(Datum::long(-17)));
    assert_eq!(promoted.upper_bound, Some(Datum::long(29)));
    let promoted = FieldMetrics {
        resolved_type: PrimitiveType::Double,
        lower_bound: Some(Datum::float(-1.75f32)),
        upper_bound: Some(Datum::float(2.125f32)),
        ..FieldMetrics::unknown(PrimitiveType::Double)
    }
    .bind_bounds();
    assert_eq!(promoted.lower_bound, Some(Datum::double(-1.75)));
    assert_eq!(promoted.upper_bound, Some(Datum::double(2.125)));
    let decimal = Datum::try_from_bytes(
        &(-1234i128).to_be_bytes(),
        PrimitiveType::Decimal {
            precision: 4,
            scale: 2,
        },
    )
    .unwrap();
    let widened = PrimitiveType::Decimal {
        precision: 9,
        scale: 2,
    };
    let promoted = FieldMetrics {
        lower_bound: Some(decimal.clone()),
        upper_bound: Some(decimal.clone()),
        ..FieldMetrics::unknown(widened.clone())
    }
    .bind_bounds();
    assert_eq!(promoted.lower_bound.as_ref().unwrap().data_type(), &widened);
    assert_eq!(
        promoted.lower_bound.as_ref().unwrap().literal(),
        &PrimitiveLiteral::Int128(-1234)
    );
    let illegal_scale = FieldMetrics {
        lower_bound: Some(decimal),
        ..FieldMetrics::unknown(PrimitiveType::Decimal {
            precision: 9,
            scale: 3,
        })
    }
    .bind_bounds();
    assert!(illegal_scale.lower_bound.is_none());
}

#[test]
fn statistics_change_required_members_not_logical_event_identity() {
    let delete = with_metrics(
        &equality("e", 3, 1, Some(1)),
        FileMetrics::new(BTreeMap::from([(1, long_metrics(1, 2, Some(0)))])),
    );
    let mut data = data("d", 2, 1, Some(1));
    data.metrics = FileMetrics::new(BTreeMap::from([(1, long_metrics(8, 9, Some(0)))]));
    let logical = index(vec![delete]).for_data(&data).unwrap();
    let all = logical.load_view(StatisticsPolicy::Disabled);
    let pruned = logical.load_view(StatisticsPolicy::MetadataBudget {
        max_candidate_members: 10,
        max_field_comparisons: 10,
    });
    assert_eq!(all.member_count(), 1);
    assert_eq!(pruned.member_count(), 0);
    assert!(all.logical().same_applications(pruned.logical()));
    assert_eq!(all.cost().visited_members, 0);
    assert_eq!(pruned.cost().field_comparisons, 1);
}

#[test]
fn statistics_policy_decides_from_metadata_before_member_iteration() {
    let facts = (0..100)
        .map(|i| equality(&format!("e{i}"), i + 1, 0, None))
        .collect();
    let logical = index(facts).for_data(&data("d", 0, 1, Some(1))).unwrap();
    let load = logical.load_view(StatisticsPolicy::MetadataBudget {
        max_candidate_members: 99,
        max_field_comparisons: 100,
    });
    assert_eq!(load.decision(), StatisticsDecision::MetadataBudgetExceeded);
    assert_eq!(load.cost().visited_members, 0);
    assert_eq!(load.member_count(), 100);
}

#[test]
fn dense_statistics_exclusions_use_bitmap_instead_of_expanded_member_vectors() {
    let facts = (0..1024)
        .map(|i| {
            with_metrics(
                &equality(&format!("e{i}"), i + 1, 0, None),
                FileMetrics::new(BTreeMap::from([(1, long_metrics(1, 2, Some(0)))])),
            )
        })
        .collect();
    let mut data = data("d", 0, 1, Some(1));
    data.metrics = FileMetrics::new(BTreeMap::from([(1, long_metrics(8, 9, Some(0)))]));
    let logical = index(facts).for_data(&data).unwrap();
    let load = logical.load_view(StatisticsPolicy::MetadataBudget {
        max_candidate_members: 1024,
        max_field_comparisons: 1024,
    });
    assert_eq!(load.member_count(), 0);
    assert_eq!(load.cost().visited_members, 1024);
    assert_eq!(load.cost().retained_exclusion_bytes, 128);
    assert_eq!(load.cost().temporary_exclusion_capacity_bytes, 8192);
    assert_eq!(load.logical().member_count(), 1024);
}

#[test]
fn sparse_statistics_exclusions_retain_only_the_exceptions() {
    let facts = (0..1024)
        .map(|i| {
            let bounds = if i == 7 {
                long_metrics(1, 2, Some(0))
            } else {
                long_metrics(8, 9, Some(0))
            };
            with_metrics(
                &equality(&format!("e{i}"), i + 1, 0, None),
                FileMetrics::new(BTreeMap::from([(1, bounds)])),
            )
        })
        .collect();
    let mut data = data("d", 0, 1, Some(1));
    data.metrics = FileMetrics::new(BTreeMap::from([(1, long_metrics(8, 9, Some(0)))]));
    let view = index(facts)
        .for_data(&data)
        .unwrap()
        .load_view(StatisticsPolicy::MetadataBudget {
            max_candidate_members: 1024,
            max_field_comparisons: 1024,
        });
    assert_eq!(view.member_count(), 1023);
    assert_eq!(
        view.cost().retained_exclusion_bytes,
        std::mem::size_of::<usize>()
    );
    assert_eq!(view.cost().field_comparisons, 1024);
}

#[test]
fn conservative_integer_metrics_never_prune_an_actual_common_key() {
    for left_start in -4..4 {
        for right_start in -4..4 {
            for left_nulls in [None, Some(0), Some(1)] {
                for right_nulls in [None, Some(0), Some(1)] {
                    let left = long_metrics(left_start, left_start + 2, left_nulls);
                    let right = long_metrics(right_start, right_start + 2, right_nulls);
                    // Unknown null counts may include null. The small oracle
                    // enumerates actual ordinary values independently of bounds.
                    let common = (left_start..=left_start + 2)
                        .any(|x| (right_start..=right_start + 2).any(|y| x == y))
                        || (left_nulls != Some(0) && right_nulls != Some(0));
                    if common {
                        assert!(left.may_overlap(&right));
                    }
                }
            }
        }
    }
}

#[derive(Default)]
struct CollisionHasher;
impl Hasher for CollisionHasher {
    fn finish(&self) -> u64 {
        0
    }
    fn write(&mut self, _: &[u8]) {}
}

#[test]
fn exact_set_identity_survives_hash_collisions_and_separates_applications() {
    let data = data("d", 0, 1, Some(1));
    let a = index(vec![equality("a", 1, 0, None)])
        .for_data(&data)
        .unwrap();
    let same_address = index(vec![equality("a", 2, 0, None)])
        .for_data(&data)
        .unwrap();
    let b = index(vec![equality("b", 1, 0, None)])
        .for_data(&data)
        .unwrap();
    assert!(a.same_addresses_with_hasher(
        &same_address,
        BuildHasherDefault::<CollisionHasher>::default()
    ));
    assert!(!a.same_addresses_with_hasher(&b, BuildHasherDefault::<CollisionHasher>::default()));
    assert!(!a.same_applications(&same_address));
}

#[test]
fn normalized_admission_rejects_wrong_domain_scope_sequence_and_mixed_position_shape() {
    let data = data("d", 2, 1, Some(1));
    let good = equality("e", 3, 1, Some(1));
    assert_eq!(
        validate_normalized_closure(&domain(20), domain(21), &data, [good.clone()])
            .unwrap_err()
            .kind,
        DeleteSemanticsErrorKind::DomainMismatch
    );
    for invalid in [equality("e", 2, 1, Some(1)), equality("e", 3, 1, Some(2))] {
        assert_eq!(
            validate_normalized_closure(&domain(20), domain(20), &data, [invalid])
                .unwrap_err()
                .kind,
            DeleteSemanticsErrorKind::InvalidClosure
        );
    }
    let position = fact(
        "p",
        2,
        partition(1, Some(1)),
        DeleteKind::Position { exact_target: None },
    );
    assert_eq!(
        validate_normalized_closure(&domain(20), domain(20), &data, [dv("v", "d", 2), position])
            .unwrap_err()
            .kind,
        DeleteSemanticsErrorKind::InvalidClosure
    );
    let dv = dv("v", "d", 2);
    assert!(matches!(
        validate_normalized_closure(&domain(20), domain(20), &data, [dv.clone(), dv, good])
            .unwrap()
            .position,
        ValidatedPositionSource::OneDv(_)
    ));
}

#[test]
fn pinned_domain_rejects_a_made_up_global_scope_spec() {
    let fake = PartitionSpec::unpartition_spec().with_spec_id(1);
    let fake_partition = TypedPartition::bind(&fake, &schema(), &Struct::empty()).unwrap();
    let delete = fact("e", 3, fake_partition, DeleteKind::Equality(group(&[1])));
    assert_eq!(
        DeleteCandidateIndex::try_new(
            domain(20),
            DeleteObservation::from_normalized_recall([delete.clone()]).unwrap()
        )
        .unwrap_err()
        .kind,
        DeleteSemanticsErrorKind::InvalidPartition
    );
    assert_eq!(
        validate_normalized_closure(&domain(20), domain(20), &data("d", 2, 1, Some(1)), [delete])
            .unwrap_err()
            .kind,
        DeleteSemanticsErrorKind::InvalidPartition
    );
}

// Independent small reference model: no production membership predicate or
// candidate index is used. It walks every observed descriptor for each data.
fn reference(data: &DataFileFact, facts: &[Arc<DeleteFact>]) -> HashSet<DeleteApplication> {
    let dv = facts.iter().find(|fact| matches!(fact.kind(), DeleteKind::DeletionVector { exact_target } if exact_target.as_ref() == data.path()));
    let mut result = HashSet::new();
    if let Some(dv) = dv {
        assert!(dv.sequence().get() >= data.sequence().get());
        result.insert(dv.application().clone());
    }
    for fact in facts {
        let selected = match fact.kind() {
            DeleteKind::Equality(_) => {
                fact.sequence().get() > data.sequence().get()
                    && (fact.partition().is_unpartitioned() || fact.partition() == data.partition())
            }
            DeleteKind::Position { exact_target } => {
                dv.is_none()
                    && fact.sequence().get() >= data.sequence().get()
                    && match exact_target {
                        Some(path) => path.as_ref() == data.path(),
                        None => fact.partition() == data.partition(),
                    }
            }
            DeleteKind::DeletionVector { .. } => false,
        };
        if selected {
            result.insert(fact.application().clone());
        }
    }
    result
}

#[test]
fn candidate_index_matches_independent_reference_across_permutations() {
    let mut facts = Vec::new();
    for x in 0..8 {
        for p in 0..3 {
            facts.push(equality(
                &format!("e{x}-{p}"),
                x,
                if p == 0 { 0 } else { 1 },
                Some(p),
            ));
            facts.push(fact(
                &format!("p{x}-{p}"),
                x,
                partition(1, Some(p)),
                DeleteKind::Position {
                    exact_target: if p == 0 { Some(Arc::from("d")) } else { None },
                },
            ));
        }
    }
    facts.push(dv("dv", "with-dv", 10));
    for rotation in 0..11 {
        let mut permutation = facts.clone();
        permutation.rotate_left(rotation);
        if rotation % 2 == 0 {
            permutation.reverse();
        }
        let index = index(permutation);
        for d in 0..10 {
            for p in 0..3 {
                for path in ["d", "other", "with-dv"] {
                    let data = data(path, d, 1, Some(p));
                    assert_eq!(
                        applications(&index.for_data(&data).unwrap()),
                        reference(&data, &facts)
                    );
                }
            }
        }
    }
}

#[test]
fn suffix_storage_does_not_multiply_by_file_or_checkpoint_count() {
    let m = 8192;
    let facts = (0..m)
        .map(|i| equality(&format!("e{i}"), i + 1, 0, None))
        .collect();
    let index = index(facts);
    assert_eq!(index.size().member_references, m as usize);
    assert_eq!(index.size().buckets, 1);
    let views = (0..128)
        .map(|i| index.for_data(&data("d", i * 32, 1, Some(1))).unwrap())
        .collect::<Vec<_>>();
    for view in &views {
        assert_eq!(view.lookup_cost().view_count, 1);
        assert!(view.lookup_cost().sequence_comparisons <= 15);
        assert!(Arc::ptr_eq(
            views[0].equality()[0].bucket(),
            view.equality()[0].bucket()
        ));
        assert_eq!(
            view.load_view(StatisticsPolicy::Disabled)
                .cost()
                .visited_members,
            0
        );
    }
}

#[test]
fn unrelated_partition_buckets_do_not_increase_lookup_work() {
    let facts = (0..2048)
        .map(|p| equality(&format!("e{p}"), 10, 1, Some(p)))
        .collect();
    let index = index(facts);
    let view = index.for_data(&data("d", 1, 1, Some(9))).unwrap();
    assert_eq!(view.member_count(), 1);
    assert_eq!(view.lookup_cost().bucket_lookups, 1);
    assert_eq!(view.lookup_cost().sequence_comparisons, 1);
}

#[test]
fn bucket_lifetime_follows_live_views_without_historical_interner_roots() {
    let index = index(vec![equality("e", 3, 0, None)]);
    let view = index.for_data(&data("d", 1, 1, Some(1))).unwrap();
    let weak = Arc::downgrade(view.equality()[0].bucket());
    drop(index);
    assert!(weak.upgrade().is_some());
    drop(view);
    assert!(weak.upgrade().is_none());
}

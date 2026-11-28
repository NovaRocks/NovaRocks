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

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use novarocks_spi::connector::ConnectorPayloadByteBudget;
use novarocks_spi::connector::read_stack::{ConnectorValueType, Range, ValueSet};

use super::*;
use crate::iceberg::spec::{
    DataContentType, DataFileBuilder, FormatVersion, NestedField, Schema, SortOrder, Struct,
    TableMetadataBuilder, UnboundPartitionSpecBuilder,
};

pub(crate) fn metadata(value_type: Type, transform: Option<Transform>) -> TableMetadata {
    let schema = Schema::builder()
        .with_fields([
            Arc::new(NestedField::optional(
                1,
                "region",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(2, "value", value_type)),
        ])
        .build()
        .unwrap();
    let mut spec = UnboundPartitionSpecBuilder::new();
    if let Some(transform) = transform {
        spec = spec
            .add_partition_field(
                if transform == Transform::Identity {
                    1
                } else {
                    2
                },
                "partition",
                transform,
            )
            .unwrap();
    }
    TableMetadataBuilder::new(
        schema,
        spec.build(),
        SortOrder::unsorted_order(),
        "memory://unit/db/orders".to_owned(),
        FormatVersion::V3,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}

fn domain(
    metadata: &TableMetadata,
    predicates: &[(i32, Domain)],
) -> TupleDomain<IcebergColumnHandle> {
    TupleDomain::with_column_domains(
        predicates
            .iter()
            .map(|(id, domain)| {
                let field = metadata.current_schema().field_by_id(*id).unwrap();
                (
                    IcebergColumnHandle::base_column(field).unwrap(),
                    domain.clone(),
                )
            })
            .collect(),
    )
    .unwrap()
}

fn one(value: ConnectorValue) -> Domain {
    Domain::single_value(value).unwrap()
}

fn text(value: &str) -> ConnectorValue {
    ConnectorValue::Varchar(value.into())
}

pub(crate) fn preparation() -> novarocks_spi::connector::ConnectorRowMutationPreparation {
    use arrow::datatypes::{DataType, Field, Schema};
    use novarocks_spi::connector::{ConnectorInstanceId, ConnectorTableHandle};
    crate::commit::preparation_for_test_source(
        ConnectorTableHandle::try_new(
            ConnectorInstanceId::parse("test").unwrap(),
            bytes::Bytes::from_static(b"table"),
        )
        .unwrap(),
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
    )
}

pub(crate) fn binding(metadata: &TableMetadata) -> RewrittenTargetBinding {
    RewrittenTargetBinding {
        table_uuid: metadata.uuid(),
        snapshot_id: metadata.current_snapshot_id(),
        preparation_identity: preparation().preparation_identity().clone(),
    }
}

fn fact(
    metadata: &TableMetadata,
    enforced: &TupleDomain<IcebergColumnHandle>,
    unenforced: &TupleDomain<IcebergColumnHandle>,
) -> IcebergRewrittenTargetFact {
    IcebergRewrittenTargetFact::new(
        binding(metadata),
        Arc::clone(metadata.current_schema()),
        enforced,
        unenforced,
    )
    .unwrap()
}

fn compose(
    metadata: &TableMetadata,
    predicates: &[(i32, Domain)],
    written: WrittenPartitionDomain,
) -> ConflictScope {
    let fact = fact(metadata, &TupleDomain::all(), &domain(metadata, predicates));
    ConflictScope::compose(&fact, written, metadata).unwrap()
}

pub(crate) fn partition(
    metadata: &TableMetadata,
    spec_id: i32,
    value: Option<Datum>,
) -> IcebergArtifactPartition {
    let values = Struct::from_iter([value.map(Into::into)]);
    let descriptor =
        crate::write_descriptor::encode_partition_descriptor(&values, spec_id, metadata).unwrap();
    IcebergArtifactPartition::try_new(String::new(), String::new(), spec_id, descriptor).unwrap()
}

#[derive(Default)]
pub(crate) struct Bytes(Arc<AtomicUsize>);

impl Bytes {
    pub(crate) fn author(self: &Arc<Self>) -> ConnectorPayloadRetentionGuard {
        ConnectorPayloadRetentionGuard::from_byte_budget(self.clone())
    }

    pub(crate) fn held(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl ConnectorPayloadByteBudget for Bytes {
    fn reserve_bytes(
        &self,
        bytes: usize,
    ) -> Result<ConnectorPayloadRetentionGuard, ConnectorError> {
        struct Receipt(Arc<AtomicUsize>, usize);
        impl Drop for Receipt {
            fn drop(&mut self) {
                self.0.fetch_sub(self.1, Ordering::SeqCst);
            }
        }
        self.0.fetch_add(bytes, Ordering::SeqCst);
        Ok(ConnectorPayloadRetentionGuard::new(Receipt(
            self.0.clone(),
            bytes,
        )))
    }
}

fn file(partition: Option<Datum>, bounds: Option<(Datum, Datum)>, is_dv: bool) -> DataFile {
    let (lower_bounds, upper_bounds) = match bounds {
        Some((lower, upper)) => (HashMap::from([(2, lower)]), HashMap::from([(2, upper)])),
        None => (HashMap::new(), HashMap::new()),
    };
    DataFileBuilder::default()
        .content(if is_dv {
            DataContentType::PositionDeletes
        } else {
            DataContentType::Data
        })
        .file_path("memory://unit/file".to_owned())
        .file_format(if is_dv {
            DataFileFormat::Puffin
        } else {
            DataFileFormat::Parquet
        })
        .partition(Struct::from_iter([partition.map(Into::into)]))
        .record_count(4)
        .file_size_in_bytes(100)
        .lower_bounds(lower_bounds)
        .upper_bounds(upper_bounds)
        .build()
        .unwrap()
}

fn might(scope: &ConflictScope, metadata: &TableMetadata, file: &DataFile) -> bool {
    scope
        .may_match(&CandidateFile {
            spec_id: metadata.default_partition_spec_id(),
            metadata,
            file,
        })
        .unwrap()
}

#[test]
fn composition_intersects_both_scan_domains_and_written_values_before_type_filtering() {
    let metadata = metadata(
        Type::Primitive(PrimitiveType::Long),
        Some(Transform::Identity),
    );
    let enforced = domain(&metadata, &[(1, one(text("east")))]);
    let unenforced = domain(&metadata, &[(1, one(text("west")))]);
    let fact = fact(&metadata, &enforced, &unenforced);
    assert!(matches!(
        ConflictScope::compose(&fact, WrittenPartitionDomain::default(), &metadata).unwrap(),
        ConflictScope::Never
    ));

    let collector = WrittenPartitionCollector::new(&metadata);
    let existing = WrittenPartitionDomain::default();
    let mut written = WrittenPartitionDomain::default();
    let budget = Arc::new(Bytes::default());
    let author = budget.author();
    let west = partition(
        &metadata,
        metadata.default_partition_spec_id(),
        Some(Datum::string("west")),
    );
    collector
        .observe(&existing, &mut written, Some(&west), &author)
        .unwrap();
    assert!(budget.held() > 0);
    assert!(matches!(
        compose(&metadata, &[(1, one(text("east")))], written),
        ConflictScope::Never
    ));
    assert_eq!(budget.held(), 0);

    let metadata_handle = IcebergColumnHandle::base_column(&NestedField::required(
        IcebergMetadataColumn::from_field_id(crate::typed_read::ICEBERG_METADATA_FIELD_ID_PATH)
            .unwrap()
            .field_id(),
        "_file",
        Type::Primitive(PrimitiveType::String),
    ))
    .unwrap();
    let enforced =
        TupleDomain::with_column_domains(BTreeMap::from([(metadata_handle, one(text("ignored")))]))
            .unwrap();
    let empty = IcebergRewrittenTargetFact::new(
        binding(&metadata),
        metadata.current_schema().clone(),
        &enforced,
        &TupleDomain::all(),
    )
    .unwrap();
    assert!(matches!(
        ConflictScope::compose(&empty, WrittenPartitionDomain::default(), &metadata).unwrap(),
        ConflictScope::All
    ));
}

#[test]
fn written_values_widen_for_unpartitioned_missing_and_nondefault_tasks() {
    let metadata = metadata(
        Type::Primitive(PrimitiveType::Long),
        Some(Transform::Identity),
    );
    let collector = WrittenPartitionCollector::new(&metadata);
    assert_eq!(collector.identities, vec![(0, 1)]);
    let budget = Arc::new(Bytes::default());
    let author = budget.author();
    let mut existing = WrittenPartitionDomain::default();
    let mut record = WrittenPartitionDomain::default();
    let east = partition(
        &metadata,
        metadata.default_partition_spec_id(),
        Some(Datum::string("east")),
    );
    collector
        .observe(&existing, &mut record, Some(&east), &author)
        .unwrap();
    existing.apply(record);
    let one_value = budget.held();
    assert!(one_value > 0);
    let mut duplicate = WrittenPartitionDomain::default();
    collector
        .observe(&existing, &mut duplicate, Some(&east), &author)
        .unwrap();
    assert_eq!(budget.held(), one_value);
    let wrong_spec = IcebergArtifactPartition::try_new(
        String::new(),
        String::new(),
        east.partition_spec_id() + 1,
        east.descriptor().clone(),
    )
    .unwrap();
    let mut record = WrittenPartitionDomain::default();
    collector
        .observe(&existing, &mut record, Some(&wrong_spec), &author)
        .unwrap();
    assert!(record.all);
    existing.apply(record);
    assert_eq!(budget.held(), 0);
    assert!(matches!(
        compose(&metadata, &[], existing),
        ConflictScope::All
    ));

    for missing_or_unpartitioned in [Some(&east), None] {
        let mut record = WrittenPartitionDomain::default();
        let unpartitioned = metadata
            .clone()
            .into_builder(None)
            .add_partition_spec(UnboundPartitionSpecBuilder::new().build())
            .unwrap()
            .set_default_partition_spec(east.partition_spec_id() + 1)
            .unwrap()
            .build()
            .unwrap()
            .metadata;
        WrittenPartitionCollector::new(&unpartitioned)
            .observe(
                &WrittenPartitionDomain::default(),
                &mut record,
                missing_or_unpartitioned,
                &author,
            )
            .unwrap();
        assert!(record.all);
    }

    let changed = metadata
        .clone()
        .into_builder(None)
        .add_partition_spec(
            UnboundPartitionSpecBuilder::new()
                .add_partition_field(1, "new_name", Transform::Identity)
                .unwrap()
                .build(),
        )
        .unwrap()
        .set_default_partition_spec(east.partition_spec_id() + 1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    assert!(
        WrittenPartitionCollector::new(&changed)
            .identities
            .is_empty()
    );
    let bucket = super::tests::metadata(
        Type::Primitive(PrimitiveType::Long),
        Some(Transform::Bucket(8)),
    );
    assert!(
        WrittenPartitionCollector::new(&bucket)
            .identities
            .is_empty()
    );
}

#[test]
fn type_restrictions_preserve_only_convertible_domains() {
    let uuid = metadata(Type::Primitive(PrimitiveType::Uuid), None);
    assert!(matches!(
        compose(
            &uuid,
            &[(2, one(ConnectorValue::Uuid([1; 16])))],
            WrittenPartitionDomain::default()
        ),
        ConflictScope::All
    ));
    for domain in [
        Domain::only_null(ConnectorValueType::Uuid),
        Domain::not_null(ConnectorValueType::Uuid),
    ] {
        assert!(matches!(
            compose(&uuid, &[(2, domain)], WrittenPartitionDomain::default()),
            ConflictScope::Domain(_)
        ));
    }
    let structure = metadata(
        Type::Struct(crate::iceberg::spec::StructType::new(vec![Arc::new(
            NestedField::optional(3, "inner", Type::Primitive(PrimitiveType::String)),
        )])),
        None,
    );
    assert!(matches!(
        compose(
            &structure,
            &[(2, Domain::only_null(ConnectorValueType::NonComparable))],
            WrittenPartitionDomain::default()
        ),
        ConflictScope::All
    ));
    let variant = metadata(Type::Variant(crate::iceberg::spec::VariantType), None);
    for domain in [
        Domain::only_null(ConnectorValueType::NonComparable),
        Domain::not_null(ConnectorValueType::NonComparable),
    ] {
        assert!(matches!(
            compose(&variant, &[(2, domain)], WrittenPartitionDomain::default()),
            ConflictScope::Domain(_)
        ));
    }
}

#[test]
fn partition_projection_matches_public_transform_across_signed_edges() {
    const DAY: i64 = 86_400_000_000;
    for transform in [
        Transform::Year,
        Transform::Month,
        Transform::Day,
        Transform::Hour,
        Transform::Truncate(10),
        Transform::Bucket(16),
    ] {
        let temporal = matches!(
            transform,
            Transform::Year | Transform::Month | Transform::Day | Transform::Hour
        );
        let primitive = if temporal {
            PrimitiveType::Timestamp
        } else {
            PrimitiveType::Long
        };
        let datum = |value| {
            if temporal {
                Datum::timestamp_micros(value)
            } else {
                Datum::long(value)
            }
        };
        let value = |value| {
            if temporal {
                ConnectorValue::TimestampMicros(value)
            } else {
                ConnectorValue::BigInt(value)
            }
        };
        let metadata = metadata(Type::Primitive(primitive), Some(transform));
        let field = &metadata.default_partition_spec().fields()[0];
        let term = bind_field(metadata.current_schema(), 2).unwrap();
        let samples = if temporal {
            [-DAY - 1, -DAY, -1, 0, 1, DAY]
        } else {
            [-20, -10, -1, 0, 1, 20]
        };
        let spec_id = metadata.default_partition_spec_id();
        for raw in [samples[0], -1, 0] {
            for (leaf, domain) in [
                (
                    binary(&term, PredicateOperator::Eq, datum(raw)),
                    one(value(raw)),
                ),
                (
                    binary(&term, PredicateOperator::LessThan, datum(raw)),
                    Domain::new(
                        ValueSet::of_ranges(
                            value(raw).value_type(),
                            vec![
                                Range::try_new(
                                    value(raw).value_type(),
                                    Bound::Unbounded,
                                    Bound::Exclusive(value(raw)),
                                )
                                .unwrap(),
                            ],
                        )
                        .unwrap(),
                        false,
                    ),
                ),
            ] {
                let scope = compose(&metadata, &[(2, domain)], WrittenPartitionDomain::default());
                let projection = transform.project(&field.name, &leaf).unwrap();
                let function =
                    crate::iceberg::transform::create_transform_function(&transform).unwrap();
                for sample in samples.into_iter().map(Some).chain([None]) {
                    let part =
                        sample.and_then(|raw| function.transform_literal(&datum(raw)).unwrap());
                    let direct = |part: Option<&Datum>| match (&projection, part) {
                        (None, _) => true,
                        (_, None) => false,
                        (Some(Predicate::Set(allowed)), Some(part)) => {
                            allowed.literals().contains(part)
                        }
                        (Some(Predicate::Binary(comparison)), Some(part)) => {
                            match comparison.op() {
                                PredicateOperator::Eq => part == comparison.literal(),
                                PredicateOperator::LessThanOrEq => part <= comparison.literal(),
                                _ => panic!("unexpected projection {comparison}"),
                            }
                        }
                        _ => panic!("unexpected projected predicate"),
                    };
                    let widened_hour = match (transform, part.as_ref().map(Datum::literal)) {
                        (Transform::Hour, Some(PrimitiveLiteral::Int(part))) if *part <= 0 => {
                            direct(Some(&Datum::int(part - 1)))
                        }
                        _ => false,
                    };
                    let expected = direct(part.as_ref()) || widened_hour;
                    let file = file(part, None, true);
                    let candidate = CandidateFile {
                        spec_id,
                        file: &file,
                        metadata: &metadata,
                    };
                    assert_eq!(
                        scope.may_match(&candidate).unwrap(),
                        expected,
                        "transform={transform:?} raw={raw} sample={sample:?}"
                    );
                }
            }
        }
        let scope = compose(
            &metadata,
            &[(2, Domain::only_null(value(0).value_type()))],
            WrittenPartitionDomain::default(),
        );
        assert!(might(&scope, &metadata, &file(None, None, true)));
    }
}

#[test]
fn legal_extreme_bounds_do_not_enter_unchecked_vendor_arithmetic() {
    let metadata = metadata(
        Type::Primitive(PrimitiveType::Long),
        Some(Transform::Truncate(10)),
    );
    let exclusive_min = Domain::new(
        ValueSet::of_ranges(
            ConnectorValueType::BigInt,
            vec![
                Range::try_new(
                    ConnectorValueType::BigInt,
                    Bound::Unbounded,
                    Bound::Exclusive(ConnectorValue::BigInt(i64::MIN)),
                )
                .unwrap(),
            ],
        )
        .unwrap(),
        false,
    );
    assert!(matches!(
        compose(
            &metadata,
            &[(2, exclusive_min)],
            WrittenPartitionDomain::default()
        ),
        ConflictScope::Never
    ));
    let dates = super::tests::metadata(Type::Primitive(PrimitiveType::Date), Some(Transform::Year));
    let extreme_date = compose(
        &dates,
        &[(2, one(ConnectorValue::Date(i32::MAX)))],
        WrittenPartitionDomain::default(),
    );
    assert!(extreme_date.to_string().contains(&i32::MAX.to_string()));
    let minimum = compose(
        &metadata,
        &[(2, one(ConnectorValue::BigInt(i64::MIN)))],
        WrittenPartitionDomain::default(),
    );
    assert!(might(
        &minimum,
        &metadata,
        &file(Some(Datum::long(0)), None, true)
    ));
    let signed = compose(
        &metadata,
        &[(2, one(ConnectorValue::BigInt(-1)))],
        WrittenPartitionDomain::default(),
    );
    assert!(might(
        &signed,
        &metadata,
        &file(Some(Datum::long(-10)), None, true)
    ));
    assert!(!might(
        &signed,
        &metadata,
        &file(Some(Datum::long(0)), None, true)
    ));
}

#[test]
fn inclusive_filter_covers_orders_example_file_facts_and_dv_partitions() {
    let metadata = metadata(
        Type::Primitive(PrimitiveType::Long),
        Some(Transform::Identity),
    );
    let collector = WrittenPartitionCollector::new(&metadata);
    let author = Arc::new(Bytes::default()).author();
    let mut written = WrittenPartitionDomain::default();
    collector
        .observe(
            &WrittenPartitionDomain::default(),
            &mut written,
            Some(&partition(
                &metadata,
                metadata.default_partition_spec_id(),
                Some(Datum::string("east")),
            )),
            &author,
        )
        .unwrap();
    let scope = compose(
        &metadata,
        &[(1, one(text("east"))), (2, one(ConnectorValue::BigInt(7)))],
        written,
    );
    for (region, id, is_dv, expected) in [
        ("west", 100, false, false), // 1: append outside the partition
        ("east", 7, false, true),    // 2: append to conflict scope
        ("east", 3, false, false),   // 3: metrics exclude the ID
        ("east", 3, true, true),     // 4: DV uses its partition only
        ("east", 7, false, true),    // 5: removed referenced data file
        ("east", 8, true, true),     // 6: excluded later by the path/effect policy
        ("west", 7, true, false),    // a DV in another partition is disjoint
    ] {
        let file = file(
            Some(Datum::string(region)),
            Some((Datum::long(id), Datum::long(id))),
            is_dv,
        );
        assert_eq!(might(&scope, &metadata, &file), expected);
    }
    assert!(might(
        &scope,
        &metadata,
        &file(Some(Datum::string("east")), None, false)
    ));
}

#[test]
fn metrics_use_null_nan_and_conservatively_truncated_string_bounds() {
    let metadata = metadata(Type::Primitive(PrimitiveType::Double), None);
    let floating = file(None, None, false);
    let counts = |nulls, nans| {
        DataFileBuilder::default()
            .content(floating.content_type())
            .file_path(floating.file_path().to_owned())
            .file_format(floating.file_format())
            .record_count(4)
            .file_size_in_bytes(100)
            .value_counts(HashMap::from([(2, 4)]))
            .null_value_counts(HashMap::from([(2, nulls)]))
            .nan_value_counts(HashMap::from([(2, nans)]))
            .build()
            .unwrap()
    };
    let numeric = compose(
        &metadata,
        &[(2, one(ConnectorValue::Double(1.0)))],
        WrittenPartitionDomain::default(),
    );
    assert!(!might(&numeric, &metadata, &counts(4, 0)));
    assert!(!might(&numeric, &metadata, &counts(0, 4)));
    let only_null = compose(
        &metadata,
        &[(2, Domain::only_null(ConnectorValueType::Double))],
        WrittenPartitionDomain::default(),
    );
    assert!(might(&only_null, &metadata, &counts(4, 0)));
    assert!(!might(&only_null, &metadata, &counts(0, 4)));
    assert!(might(&only_null, &metadata, &floating));

    let strings = super::tests::metadata(Type::Primitive(PrimitiveType::String), None);
    let file = file(None, Some((Datum::string("a"), Datum::string("b"))), false);
    let inside = compose(
        &strings,
        &[(2, one(text("a long truncated value")))],
        WrittenPartitionDomain::default(),
    );
    let outside = compose(
        &strings,
        &[(2, one(text("beyond the upper bound")))],
        WrittenPartitionDomain::default(),
    );
    assert!(might(&inside, &strings, &file));
    assert!(!might(&outside, &strings, &file));
}

#[test]
fn candidate_partition_spec_is_taken_from_the_new_head() {
    let source = metadata(Type::Primitive(PrimitiveType::Long), None);
    let latest = source
        .clone()
        .into_builder(None)
        .add_partition_spec(
            UnboundPartitionSpecBuilder::new()
                .add_partition_field(2, "values", Transform::Truncate(10))
                .unwrap()
                .build(),
        )
        .unwrap()
        .set_default_partition_spec(1)
        .unwrap()
        .build()
        .unwrap()
        .metadata;
    assert!(
        source
            .partition_spec_by_id(latest.default_partition_spec_id())
            .is_none()
    );
    let scope = compose(
        &source,
        &[(2, one(ConnectorValue::BigInt(12)))],
        WrittenPartitionDomain::default(),
    );
    assert!(might(
        &scope,
        &latest,
        &file(Some(Datum::long(10)), None, true)
    ));
    assert!(!might(
        &scope,
        &latest,
        &file(Some(Datum::long(30)), None, true)
    ));
}

#[test]
fn partition_domain_keeps_more_values_than_the_spi_limit() {
    let metadata = metadata(
        Type::Primitive(PrimitiveType::Long),
        Some(Transform::Identity),
    );
    let collector = WrittenPartitionCollector::new(&metadata);
    let budget = Arc::new(Bytes::default());
    let author = budget.author();
    let mut existing = WrittenPartitionDomain::default();
    for value in 0..4100 {
        let part = partition(
            &metadata,
            metadata.default_partition_spec_id(),
            Some(Datum::string(value.to_string())),
        );
        let mut record = WrittenPartitionDomain::default();
        collector
            .observe(&existing, &mut record, Some(&part), &author)
            .unwrap();
        existing.apply(record);
    }
    let scope = compose(&metadata, &[], existing);
    assert!(might(
        &scope,
        &metadata,
        &file(Some(Datum::string("4099")), None, true)
    ));
    assert!(!might(
        &scope,
        &metadata,
        &file(Some(Datum::string("4100")), None, true)
    ));
    assert!(budget.held() > 0);
    drop(scope);
    assert_eq!(budget.held(), 0);
}

#[test]
fn metrics_keep_java_in_short_circuits_without_weakening_partition_projection() {
    fn upper_only(value: i64) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path("memory://unit/file".to_owned())
            .file_format(DataFileFormat::Parquet)
            .record_count(4)
            .file_size_in_bytes(100)
            .upper_bounds(HashMap::from([(2, Datum::long(value))]))
            .build()
            .unwrap()
    }
    let metadata = metadata(Type::Primitive(PrimitiveType::Long), None);
    let scope = |values: Vec<ConnectorValue>| {
        compose(
            &metadata,
            &[(
                2,
                Domain::new(
                    ValueSet::of_values(ConnectorValueType::BigInt, values).unwrap(),
                    false,
                ),
            )],
            WrittenPartitionDomain::default(),
        )
    };
    assert!(!might(
        &scope(vec![ConnectorValue::BigInt(2)]),
        &metadata,
        &upper_only(1)
    ));
    assert!(might(
        &scope(vec![ConnectorValue::BigInt(2), ConnectorValue::BigInt(4)]),
        &metadata,
        &upper_only(1)
    ));
    let candidate = file(None, Some((Datum::long(-2), Datum::long(-1))), false);
    assert!(!might(
        &scope((0..200).map(|v| ConnectorValue::BigInt(v * 2)).collect()),
        &metadata,
        &candidate
    ));
    assert!(might(
        &scope((0..201).map(|v| ConnectorValue::BigInt(v * 2)).collect()),
        &metadata,
        &candidate
    ));
}

#[test]
fn hour_and_timestamptz_day_keep_the_legacy_preepoch_partition() {
    for transform in [Transform::Hour, Transform::Day] {
        let metadata = metadata(Type::Primitive(PrimitiveType::Timestamptz), Some(transform));
        let term = bind_field(metadata.current_schema(), 2).unwrap();
        let leaf = binary(&term, PredicateOperator::Eq, Datum::timestamptz_micros(-1));
        let field = &metadata.default_partition_spec().fields()[0];
        let projection = transform.project(&field.name, &leaf).unwrap().unwrap();
        let partition = |value| {
            if transform == Transform::Hour {
                Datum::int(value)
            } else {
                Datum::date(value)
            }
        };
        let as_literal = |value| Literal::from(partition(value));
        assert!(projected_may_match(&projection, Some(&as_literal(-1))));
        assert!(!projected_may_match(&projection, Some(&as_literal(0))));
        let scope = compose(
            &metadata,
            &[(2, one(ConnectorValue::TimestampTzMicros(-1)))],
            WrittenPartitionDomain::default(),
        );
        for (partition_number, expected) in [(-1, true), (0, true), (1, false), (-2, false)] {
            let file = file(Some(partition(partition_number)), None, true);
            assert_eq!(might(&scope, &metadata, &file), expected);
        }
        let known_disjoint = file(
            Some(partition(0)),
            Some((Datum::timestamptz_micros(3), Datum::timestamptz_micros(3))),
            false,
        );
        assert!(!might(&scope, &metadata, &known_disjoint));
    }
}

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

//! Optional metrics and identity partitions follow the supported Iceberg facts.
//! These local manifest projections protect the summary reader independently of
//! optimizer evidence and need no network or external fixture.
use super::*;
use crate::iceberg::spec::{
    FormatVersion, NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, TableMetadata,
    TableMetadataBuilder, Transform, Type,
};

fn table(primitive: PrimitiveType, identity: bool) -> TableMetadata {
    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "k",
                Type::Primitive(primitive),
            ))])
            .build()
            .unwrap(),
    );
    let spec = if identity {
        PartitionSpec::builder(schema.clone())
            .add_partition_field("k", "k_partition", Transform::Identity)
            .unwrap()
            .build()
            .unwrap()
    } else {
        PartitionSpec::unpartition_spec()
    };
    TableMetadataBuilder::new(
        schema.as_ref().clone(),
        spec,
        SortOrder::unsorted_order(),
        "/tmp/show-stats-facts".into(),
        FormatVersion::V2,
        HashMap::new(),
    )
    .unwrap()
    .build()
    .unwrap()
    .metadata
}
fn file(
    rows: i64,
    nulls: Option<i64>,
    size: Option<i64>,
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
) -> DataFileWithStats {
    DataFileWithStats {
        path: "data.parquet".into(),
        size: 100,
        record_count: Some(rows),
        column_stats: Some(HashMap::from([(
            "k".into(),
            crate::scan_model::IcebergColumnStats {
                field_id: Some(1),
                null_count: nulls,
                value_count: Some(rows),
                column_size: size,
                lower_bound: lower,
                upper_bound: upper,
            },
        )])),
        partition_spec_id: None,
        partition_key: None,
        partition_values: None,
        manifest_path: None,
        partition_field_values: Vec::new(),
        first_row_id: None,
        data_sequence_number: None,
        delete_files: Vec::new(),
    }
}
fn facts(metadata: &TableMetadata, files: &[DataFileWithStats]) -> summary::ColumnFacts {
    summary::column_facts(
        metadata,
        &metadata.current_schema().as_struct().fields()[0],
        files,
        || Ok(()),
    )
    .unwrap()
}
#[test]
fn optional_counts_accumulate_only_the_files_that_report_them() {
    let metadata = table(PrimitiveType::Long, false);
    let complete = file(3, Some(1), Some(45), None, None);
    let absent = file(2, None, None, None, None);
    assert_eq!(
        facts(&metadata, &[complete, absent.clone()]),
        (Some(1), Some(45), None, None)
    );
    assert_eq!(facts(&metadata, &[absent]), (None, None, None, None));
}
#[test]
fn all_null_files_do_not_participate_in_bounds_but_unknown_null_counts_do() {
    let metadata = table(PrimitiveType::Long, false);
    let known = file(
        3,
        Some(0),
        None,
        Some(1i64.to_le_bytes().to_vec()),
        Some(3i64.to_le_bytes().to_vec()),
    );
    let all_null = file(2, Some(2), None, None, None);
    assert_eq!(
        facts(&metadata, &[known.clone(), all_null]),
        (Some(2), None, Some(1.0), Some(3.0))
    );
    let missing_lower = file(2, None, None, None, Some(9i64.to_le_bytes().to_vec()));
    assert_eq!(
        facts(&metadata, &[known.clone(), missing_lower]),
        (Some(0), None, None, Some(9.0))
    );
    let missing_upper = file(2, None, None, Some((-1i64).to_le_bytes().to_vec()), None);
    assert_eq!(
        facts(&metadata, &[known, missing_upper]),
        (Some(0), None, Some(-1.0), None)
    );
}
#[test]
fn identity_null_partitions_supply_counts_and_values_supply_bounds() {
    let metadata = table(PrimitiveType::Long, true);
    let mut null = file(3, None, None, None, None);
    null.partition_spec_id = Some(metadata.default_partition_spec_id());
    null.partition_values = Some([None].into_iter().collect());
    assert_eq!(facts(&metadata, &[null]), (Some(3), None, None, None));
    let mut value = file(2, Some(0), None, None, None);
    value.partition_spec_id = Some(metadata.default_partition_spec_id());
    value.partition_values = Some(
        [Some(crate::iceberg::spec::Literal::long(7))]
            .into_iter()
            .collect(),
    );
    assert_eq!(
        facts(&metadata, &[value]),
        (Some(0), None, Some(7.0), Some(7.0))
    );
}
#[test]
fn decimal_and_timestamp_bounds_preserve_their_source_domains() {
    let decimal = PrimitiveType::Decimal {
        precision: 38,
        scale: 4,
    };
    let datum = crate::iceberg::spec::Datum::decimal_from_str("12345678.9012").unwrap();
    assert_eq!(
        summary::bound(Some(datum.to_bytes().unwrap().as_ref()), &decimal).unwrap(),
        Some(12345678.9012)
    );
    let negative = crate::iceberg::spec::Datum::decimal_from_str("-0.0001").unwrap();
    assert_eq!(
        summary::bound(Some(negative.to_bytes().unwrap().as_ref()), &decimal).unwrap(),
        Some(-0.0001)
    );
    assert_eq!(
        summary::primitive_bound(
            &crate::iceberg::spec::PrimitiveLiteral::Long(-1),
            &PrimitiveType::Timestamptz
        ),
        Some(-1.0)
    );
    assert_eq!(
        summary::column_type(&Type::Primitive(PrimitiveType::Timestamptz)),
        novarocks_spi::connector::StatisticsSummaryColumnType::Timestamp {
            precision: 6,
            with_time_zone: true
        }
    );
    assert_eq!(
        summary::column_type(&Type::Primitive(PrimitiveType::Fixed(7))),
        novarocks_spi::connector::StatisticsSummaryColumnType::FixedBinary { width: 7 }
    );
}
#[test]
fn no_data_files_have_no_column_facts() {
    assert_eq!(
        facts(&table(PrimitiveType::Long, false), &[]),
        (None, None, None, None)
    );
    assert_eq!(
        summary::physical_row_count::<DataFileWithStats>(&[], || Ok(())).unwrap(),
        0
    );
    assert_eq!(
        summary::physical_row_count(
            &[
                file(3, None, None, None, None),
                file(2, None, None, None, None)
            ],
            || Ok(())
        )
        .unwrap(),
        5
    );
}

#[test]
fn summary_metadata_aliases_are_classified_by_the_provider_vocabulary() {
    for name in ["orders$files", "orders$history", "orders$snapshots"] {
        assert!(crate::metadata::is_metadata_table_alias(name));
    }
    for name in ["orders", "orders$ordinary_name"] {
        assert!(!crate::metadata::is_metadata_table_alias(name));
    }
}

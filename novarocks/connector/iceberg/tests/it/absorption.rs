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

use std::{collections::BTreeMap, sync::Arc};

use novarocks_connector_iceberg::iceberg::spec::{
    NestedField, PartitionSpec, PrimitiveType, Schema, Transform,
};
use novarocks_connector_iceberg::typed_read::{
    IcebergColumnHandle, IcebergTableHandle, IcebergTableHandleParams,
};
use novarocks_spi::connector::read_stack::{
    Bound, ConnectorValue, ConnectorValueType, Constraint, Domain, Range, SchemaTableName,
    TupleDomain, ValueSet,
};

fn absorb(primitive: PrimitiveType, transform: Transform, domain: Domain) -> bool {
    let schema = Arc::new(
        Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "key",
                primitive.into(),
            ))])
            .build()
            .unwrap(),
    );
    let spec = PartitionSpec::builder(schema.clone())
        .with_spec_id(0)
        .add_partition_field("key", "partition_key", transform)
        .unwrap()
        .build()
        .unwrap();
    let table = IcebergTableHandle::try_new(IcebergTableHandleParams {
        schema_table_name: SchemaTableName::try_new("test", "t").unwrap(),
        snapshot_id: None,
        read_domain: None,
        table_schema_json: serde_json::to_string(&schema).unwrap(),
        spec_id: Some(0),
        partition_spec_jsons: BTreeMap::from([(0, serde_json::to_string(&spec).unwrap())]),
        format_version: 3,
        unenforced_predicate: TupleDomain::all(),
        enforced_predicate: TupleDomain::all(),
        limit: None,
        projected_columns: Default::default(),
        name_mapping_json: None,
        table_location: "memory:///t".into(),
        storage_properties: Default::default(),
        pinned_data_files: None,
    })
    .unwrap();
    let column = IcebergColumnHandle::base_column_of(&schema, 1).unwrap();
    let offered = TupleDomain::with_column_domains(BTreeMap::from([(column, domain)])).unwrap();
    let result = table
        .apply_filter(&Constraint::of_summary(offered))
        .unwrap();
    let exact = result.remaining_filter().is_all();
    assert_eq!(
        result.handle().enforced_predicate().columns().count(),
        usize::from(exact)
    );
    exact
}

fn ge(value: ConnectorValue) -> Domain {
    range(
        value.value_type(),
        Bound::Inclusive(value),
        Bound::Unbounded,
    )
}

fn range(kind: ConnectorValueType, low: Bound, high: Bound) -> Domain {
    let range = Range::try_new(kind, low, high).unwrap();
    Domain::new(ValueSet::of_ranges(kind, vec![range]).unwrap(), false)
}

#[test]
fn absorption_bucket_void_and_signed_truncate_preserve_unproved_ranges() {
    for (transform, domain, expected) in [
        (
            Transform::Void,
            Domain::only_null(ConnectorValueType::Integer),
            false,
        ),
        (
            Transform::Bucket(8),
            Domain::only_null(ConnectorValueType::Integer),
            true,
        ),
        (
            Transform::Bucket(8),
            Domain::not_null(ConnectorValueType::Integer),
            false,
        ),
        (
            Transform::Truncate(10),
            ge(ConnectorValue::Integer(10)),
            true,
        ),
        (
            Transform::Truncate(10),
            ge(ConnectorValue::Integer(-20)),
            true,
        ),
        (
            Transform::Truncate(10),
            ge(ConnectorValue::Integer(-19)),
            false,
        ),
        (
            Transform::Truncate(10),
            ge(ConnectorValue::Integer(i32::MIN)),
            false,
        ),
        (
            Transform::Truncate(10),
            Domain::single_value(ConnectorValue::Integer(10)).unwrap(),
            false,
        ),
    ] {
        assert_eq!(absorb(PrimitiveType::Int, transform, domain), expected);
    }
}

#[test]
fn absorption_refuses_unrepresentable_partition_buckets() {
    let wide = u32::try_from(i32::MAX).unwrap() + 1;
    assert!(!absorb(
        PrimitiveType::Int,
        Transform::Truncate(wide),
        ge(ConnectorValue::Integer(0)),
    ));
    assert!(!absorb(
        PrimitiveType::Int,
        Transform::Truncate(10),
        ge(ConnectorValue::Integer(-2_147_483_640)),
    ));
    assert!(absorb(
        PrimitiveType::Int,
        Transform::Truncate(10),
        ge(ConnectorValue::Integer(-2_147_483_630)),
    ));
    assert!(!absorb(
        PrimitiveType::Long,
        Transform::Truncate(10),
        ge(ConnectorValue::BigInt(-9_223_372_036_854_775_800)),
    ));

    const MICROS_PER_HOUR: i64 = 3_600_000_000;
    let at = |hour: i64| ge(ConnectorValue::TimestampMicros(hour * MICROS_PER_HOUR));
    assert!(absorb(
        PrimitiveType::Timestamp,
        Transform::Hour,
        at(i64::from(i32::MAX))
    ));
    assert!(!absorb(
        PrimitiveType::Timestamp,
        Transform::Hour,
        at(i64::from(i32::MAX) + 1)
    ));
    assert!(!absorb(
        PrimitiveType::Timestamp,
        Transform::Hour,
        at(i64::from(i32::MIN))
    ));
}

#[test]
fn absorption_handles_calendar_precision_and_unicode_bound_direction() {
    let at = ConnectorValue::Date(18_414); // 2020-06-01
    assert!(absorb(
        PrimitiveType::Date,
        Transform::Month,
        ge(at.clone())
    ));
    assert!(absorb(
        PrimitiveType::Date,
        Transform::Day,
        Domain::single_value(at).unwrap()
    ));
    assert!(!absorb(
        PrimitiveType::Date,
        Transform::Month,
        ge(ConnectorValue::Date(18_415))
    ));
    assert!(absorb(
        PrimitiveType::TimestamptzNs,
        Transform::Hour,
        ge(ConnectorValue::TimestampTzNanos(-3_600_000_000_000))
    ));
    assert!(!absorb(
        PrimitiveType::Timestamp,
        Transform::Day,
        ge(ConnectorValue::TimestampMicros(-1))
    ));

    use novarocks_type_contract::{TemporalMicros, TemporalPrecision, TemporalType, TemporalValue};
    for (storage, precision) in [
        (PrimitiveType::Timestamp, TemporalPrecision::MICROSECONDS),
        (PrimitiveType::TimestampNs, TemporalPrecision::NANOSECONDS),
    ] {
        // The provider receives the wider transport declaration, then proves
        // alignment against the actual storage lattice.
        let data_type = TemporalType::Timestamp(TemporalPrecision::PICOSECONDS);
        let at = |picos| ConnectorValue::Temporal {
            data_type,
            value: TemporalValue::Timestamp(TemporalMicros::from_picos(picos).unwrap()),
        };
        assert!(absorb(storage.clone(), Transform::Day, ge(at(0))));
        assert!(!absorb(
            storage,
            Transform::Day,
            ge(at(i128::from(precision.picosecond_step()))),
        ));
    }

    let short = ConnectorValue::Varchar(Arc::from("é"));
    assert!(!absorb(
        PrimitiveType::String,
        Transform::Truncate(2),
        ge(short.clone())
    ));
    assert!(absorb(
        PrimitiveType::String,
        Transform::Truncate(2),
        range(
            ConnectorValueType::Varchar,
            Bound::Exclusive(short),
            Bound::Unbounded
        )
    ));
    assert!(absorb(
        PrimitiveType::Binary,
        Transform::Truncate(2),
        Domain::not_null(ConnectorValueType::Varbinary)
    ));
}

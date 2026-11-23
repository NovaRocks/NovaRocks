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

use arrow_schema::{DataType, TimeUnit};
use novarocks_type_contract::*;

fn temporal_type(ty: TemporalType, nullable: bool) -> ValueTypeRef {
    ValueTypeRef::try_new(
        ValueKind::Temporal(ty),
        nullable,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap()
}

#[test]
fn temporal_binding_preserves_identity_for_the_same_physical_layout() {
    let millis = temporal_type(TemporalType::Timestamp(TemporalPrecision::DEFAULT), true);
    let micros = temporal_type(
        TemporalType::Timestamp(TemporalPrecision::MICROSECONDS),
        true,
    );
    let layout = DataType::Timestamp(TimeUnit::Microsecond, None);
    let physical = ValueTypeRef::try_from_declared_arrow(
        &layout,
        true,
        ValueLogicalType::Physical,
        ValueTypeStorageLifetime::pure(),
    )
    .unwrap();
    for declaration in [&millis, &micros, &physical] {
        validate_value_layout_observed::<ExecutionValueTypeError>(declaration, &layout, |_| Ok(()))
            .unwrap();
    }
    let identity = |value: &ValueTypeRef| {
        let mut bytes = Vec::new();
        write_value_type_identity_observed::<std::convert::Infallible>(value, |part| {
            bytes.extend_from_slice(part);
            Ok(())
        })
        .unwrap();
        bytes
    };
    assert_ne!(identity(&millis), identity(&micros));
    assert_ne!(identity(&micros), identity(&physical));
    let valid = |ty| {
        FunctionValueType::try_from_value(
            ty,
            ValueLayout::try_new(&layout, ValueTypeStorageLifetime::pure()).unwrap(),
        )
        .unwrap()
    };
    let short = valid(millis);
    let long = valid(micros);
    assert_eq!(
        comparison_common_value_type(&short, &long).unwrap(),
        Some(long)
    );
}

#[test]
fn temporal_values_check_precision_and_zoned_range() {
    let p3 = TemporalType::Timestamp(TemporalPrecision::DEFAULT);
    assert!(TemporalValue::from_i64_carrier(p3, -1).is_err());
    assert!(TemporalValue::from_i64_carrier(p3, -1_000).is_ok());
    let zoned = TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS);
    let value = |micros| TemporalValue::TimestampWithTimeZone {
        instant: TemporalMicros::whole_micros(micros),
        zone: TimeZoneKey::UTC,
    };
    let first_micros = MIN_ZONED_TIMESTAMP_MILLIS * 1000;
    let last_micros = (MAX_ZONED_TIMESTAMP_MILLIS + 1) * 1000 - 1;
    assert!(value(first_micros).validate(zoned).is_ok());
    assert!(value(last_micros).validate(zoned).is_ok());
    assert!(value(first_micros - 1).validate(zoned).is_err());
    assert!(value(last_micros + 1).validate(zoned).is_err());
    let TemporalCarrier::ZonedTimestamp(mut bytes) = value(first_micros).to_carrier(zoned).unwrap()
    else {
        panic!("unexpected carrier");
    };
    bytes[12..].copy_from_slice(&u16::MAX.to_le_bytes());
    assert_eq!(
        TemporalValue::from_fixed_carrier(zoned, &bytes),
        Err(TemporalValueError::TimeZoneKey)
    );
}

#[test]
fn temporal_relation_key_preserves_observable_zones_and_negative_fractions() {
    let ty = TemporalType::TimestampWithTimeZone(TemporalPrecision::PICOSECONDS);
    let instant = TemporalMicros::from_picos(-1).unwrap();
    let utc = TemporalValue::TimestampWithTimeZone {
        instant,
        zone: TimeZoneKey::UTC,
    };
    let shanghai = TemporalValue::TimestampWithTimeZone {
        instant,
        zone: TimeZoneKey::parse("Asia/Shanghai").unwrap(),
    };
    assert_eq!(instant.micros(), -1);
    assert_eq!(instant.picoseconds_of_microsecond(), 999_999);
    assert_ne!(utc.to_carrier(ty), shanghai.to_carrier(ty));
    assert_eq!(utc.relation_key(ty), shanghai.relation_key(ty));
    let epoch = TemporalValue::TimestampWithTimeZone {
        instant: TemporalMicros::whole_micros(0),
        zone: TimeZoneKey::UTC,
    };
    assert!(
        utc.relation_key(ty).unwrap().order_bytes() < epoch.relation_key(ty).unwrap().order_bytes()
    );
}

#[test]
fn temporal_time_keys_wrap_at_midnight() {
    let ty = TemporalType::TimeWithTimeZone(TemporalPrecision::WHOLE_SECONDS);
    let utc = TemporalValue::TimeWithTimeZone {
        picos: PICOS_PER_MINUTE,
        offset_minutes: 0,
    };
    let west = TemporalValue::TimeWithTimeZone {
        picos: 1381 * PICOS_PER_MINUTE,
        offset_minutes: -60,
    };
    assert_eq!(utc.relation_key(ty), west.relation_key(ty));
    assert_ne!(utc.to_carrier(ty), west.to_carrier(ty));
}

#[test]
fn temporal_interval_distinguishes_structural_and_declared_leading_precision() {
    let transient = IntervalQualifier::new(
        IntervalField::Day,
        IntervalField::Second,
        13,
        TemporalPrecision::PICOSECONDS,
    )
    .unwrap();
    assert!(transient.validate_declared_precision().is_err());
    assert!(
        IntervalQualifier::new(
            IntervalField::Day,
            IntervalField::Month,
            1,
            TemporalPrecision::WHOLE_SECONDS,
        )
        .is_err()
    );
    assert_eq!(
        TemporalType::Interval(transient).carrier(),
        DataType::FixedSizeBinary(12)
    );
}

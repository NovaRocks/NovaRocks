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

use super::*;
use arrow_schema::TimeUnit;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Validate);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        trace.push(units);
        match self.refusal.clone() {
            Some((at, cause)) if trace.len() == at + 1 => Err(cause),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1,
        max_array_nodes: 16,
        max_logical_elements: 16,
        max_retained_buffer_bytes: 2_000_000,
        max_type_depth: 8,
        max_type_nodes: 16,
        max_dictionary_depth: 2,
        max_metadata_bytes: 500_000,
        max_library_validation_work: 5_000_000,
        max_library_validation_bytes: 5_000_000,
    }
}
fn physical(data_type: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(data_type, nullable)
}
fn field(ty: &FunctionValueType) -> Arc<Field> {
    Arc::new(
        ty.try_to_field("literal").unwrap().with_metadata(
            ty.try_to_field("literal")
                .unwrap()
                .metadata()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .chain([("provider.annotation".to_owned(), "authored".to_owned())])
                .collect::<std::collections::HashMap<String, String>>(),
        ),
    )
}

#[test]
fn unsigned_and_largeint_factories_keep_independent_domains_and_big_endian_payloads() {
    for value in [0, 1, 1 << 63, u64::MAX] {
        let ty = physical(DataType::UInt64, false);
        let cv = ConstantValue::from_u64(
            field(&ty),
            ty.clone(),
            value,
            policy(),
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(cv.try_u64().unwrap(), Some(value));
        assert_eq!(cv.value_type(), &ty);
        assert!(cv.try_i64().is_err());
        assert!(cv.try_date32().is_err());
    }
    let ty = FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        true,
        ValueLogicalType::LargeInt,
    )
    .unwrap();
    for value in [i128::MIN, -1, 0, 1, i128::MAX] {
        let cv = ConstantValue::from_largeint(
            field(&ty),
            ty.clone(),
            value,
            policy(),
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(cv.try_largeint().unwrap(), Some(value));
        let array = cv
            .pool()
            .array()
            .as_any()
            .downcast_ref::<arrow_array::FixedSizeBinaryArray>()
            .unwrap();
        assert_eq!(array.value(0), &value.to_be_bytes());
        assert_eq!(cv.value_type(), &ty);
        assert_eq!(
            cv.field()
                .metadata()
                .get("provider.annotation")
                .map(String::as_str),
            Some("authored")
        );
        assert!(cv.try_binary().is_err());
    }
    for logical in [ValueLogicalType::Physical, ValueLogicalType::Uuid] {
        let ty =
            FunctionValueType::try_with_logical_type(DataType::FixedSizeBinary(16), true, logical)
                .unwrap();
        assert!(
            ConstantValue::from_largeint(
                field(&ty),
                ty.clone(),
                -1,
                policy(),
                CompilePhase::Validate,
                &Control::default()
            )
            .is_err()
        );
        let cv = ConstantValue::null(
            field(&ty),
            ty.clone(),
            policy(),
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        assert!(cv.try_largeint().is_err());
        assert_eq!(cv.value_type(), &ty);
    }
}

#[test]
fn exact_temporal_factories_keep_units_zones_and_raw_extreme_values() {
    for value in [i32::MIN, -1, 0, 1, i32::MAX] {
        let ty = physical(DataType::Date32, false);
        let cv = ConstantValue::from_date32(
            field(&ty),
            ty.clone(),
            value,
            policy(),
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(cv.try_date32().unwrap(), Some(value));
        assert_eq!(cv.value_type(), &ty);
        assert!(cv.try_i64().is_err());
        assert!(cv.try_timestamp().is_err());
    }
    for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
        for value in [-1, 0, 86_399_000_000] {
            let ty = physical(DataType::Time64(unit), false);
            let cv = ConstantValue::from_time64(
                field(&ty),
                ty.clone(),
                value,
                policy(),
                CompilePhase::Validate,
                &Control::default(),
            )
            .unwrap();
            assert_eq!(cv.try_time64().unwrap(), Some(value));
            assert_eq!(cv.pool().array().data_type(), ty.data_type());
            assert!(cv.try_i64().is_err());
        }
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [
            None,
            Some(Arc::<str>::from("")),
            Some(Arc::from("UTC")),
            Some(Arc::from("Asia/Shanghai")),
        ] {
            for value in [i64::MIN, -1, 0, 1, i64::MAX] {
                let ty = physical(DataType::Timestamp(unit, zone.clone()), true);
                let cv = ConstantValue::from_timestamp(
                    field(&ty),
                    ty.clone(),
                    value,
                    policy(),
                    CompilePhase::Validate,
                    &Control::default(),
                )
                .unwrap();
                assert_eq!(cv.try_timestamp().unwrap(), Some(value));
                assert_eq!(cv.pool().array().data_type(), ty.data_type());
                assert_eq!(cv.value_type(), &ty);
                assert!(cv.try_time64().is_err());
            }
        }
    }
    let int = physical(DataType::Int64, false);
    let cv = ConstantValue::from_i64(
        field(&int),
        int,
        -1,
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    assert!(cv.try_time64().is_err());
    assert!(cv.try_timestamp().is_err());
    assert!(
        FunctionValueType::try_with_logical_type(
            DataType::Time64(TimeUnit::Second),
            false,
            ValueLogicalType::Physical
        )
        .is_err()
    );
}

#[test]
fn decimal256_byte_factory_preserves_signed_coefficients_precision_and_negative_scale() {
    let mut minus_one = [0xff; 32];
    let mut wide = [0; 32];
    wide[15] = 1; // 2^128, above the i128 coefficient domain.
    for raw in [[0; 32], minus_one, wide] {
        let ty = physical(DataType::Decimal256(76, -2), false);
        let cv = ConstantValue::from_decimal256_be(
            field(&ty),
            ty.clone(),
            raw,
            policy(),
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(cv.try_decimal256_be().unwrap(), Some(raw));
        assert_eq!(
            cv.try_decimal256().unwrap(),
            Some(arrow_buffer::i256::from_be_bytes(raw))
        );
        assert_eq!(cv.value_type(), &ty);
        assert!(cv.try_largeint().is_err());
    }
    minus_one = [0; 32];
    minus_one[31] = 10;
    let ty = physical(DataType::Decimal256(1, 0), false);
    assert!(
        ConstantValue::from_decimal256_be(
            field(&ty),
            ty,
            minus_one,
            policy(),
            CompilePhase::Validate,
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn byte_factories_build_each_exact_offset_or_view_carrier_without_domain_downgrade() {
    for dtype in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
        for payload in [
            "",
            "é漢字",
            "an external view payload longer than twelve bytes",
        ] {
            let ty = physical(dtype.clone(), true);
            let cv = ConstantValue::from_utf8(
                field(&ty),
                ty.clone(),
                payload,
                policy(),
                CompilePhase::Validate,
                &Control::default(),
            )
            .unwrap();
            assert_eq!(cv.try_utf8().unwrap(), Some(payload));
            assert_eq!(cv.pool().array().data_type(), &dtype);
            assert_eq!(cv.value_type(), &ty);
            assert!(cv.try_binary().is_err());
        }
    }
    for dtype in [
        DataType::Binary,
        DataType::LargeBinary,
        DataType::BinaryView,
    ] {
        for payload in [
            &b""[..],
            &[0xff, 0, 0x80][..],
            &b"an external view payload longer than twelve bytes"[..],
        ] {
            let ty = physical(dtype.clone(), true);
            let cv = ConstantValue::from_binary(
                field(&ty),
                ty.clone(),
                payload,
                policy(),
                CompilePhase::Validate,
                &Control::default(),
            )
            .unwrap();
            assert_eq!(cv.try_binary().unwrap(), Some(payload));
            assert_eq!(cv.pool().array().data_type(), &dtype);
            assert_eq!(cv.value_type(), &ty);
            assert!(cv.try_utf8().is_err());
        }
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let cv = ConstantValue::from_utf8(
        field(&json),
        json.clone(),
        "{\"x\":1}",
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(cv.value_type(), &json);
    assert_eq!(cv.try_utf8().unwrap(), Some("{\"x\":1}"));
    let variant = FunctionValueType::try_with_logical_type(
        DataType::LargeBinary,
        false,
        ValueLogicalType::Variant,
    )
    .unwrap();
    let cv = ConstantValue::from_binary(
        field(&variant),
        variant.clone(),
        &[1, 0, 255],
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(cv.value_type(), &variant);
    assert_eq!(cv.try_binary().unwrap(), Some(&[1, 0, 255][..]));
    assert!(
        ConstantValue::from_utf8(
            field(&variant),
            variant,
            "raw",
            policy(),
            CompilePhase::Validate,
            &Control::default()
        )
        .is_err()
    );
}

#[test]
fn typed_nulls_keep_temporal_nominal_and_decimal_type_while_getters_return_none() {
    let types = [
        physical(DataType::UInt64, true),
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            true,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
        physical(DataType::Date32, true),
        physical(DataType::Time64(TimeUnit::Microsecond), true),
        physical(
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            true,
        ),
        physical(DataType::Decimal256(76, 2), true),
    ];
    for (index, ty) in types.into_iter().enumerate() {
        let cv = ConstantValue::null(
            field(&ty),
            ty.clone(),
            policy(),
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(cv.value_type(), &ty);
        match index {
            0 => assert_eq!(cv.try_u64().unwrap(), None),
            1 => assert_eq!(cv.try_largeint().unwrap(), None),
            2 => assert_eq!(cv.try_date32().unwrap(), None),
            3 => assert_eq!(cv.try_time64().unwrap(), None),
            4 => assert_eq!(cv.try_timestamp().unwrap(), None),
            5 => assert_eq!(cv.try_decimal256_be().unwrap(), None),
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Copy)]
enum Factory {
    Unsigned,
    LargeInt,
    Date,
    Time,
    Timestamp,
    Interval,
    Decimal,
    Utf8,
    Binary,
}
fn construct(
    factory: Factory,
    policy: ConstantPolicy,
    control: &Control,
) -> Result<ConstantValue, ConstantError> {
    let ty = match factory {
        Factory::Unsigned => physical(DataType::UInt64, false),
        Factory::LargeInt => FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::LargeInt,
        )
        .unwrap(),
        Factory::Date => physical(DataType::Date32, false),
        Factory::Time => physical(DataType::Time64(TimeUnit::Nanosecond), false),
        Factory::Timestamp => physical(
            DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
            false,
        ),
        Factory::Interval => physical(
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
            false,
        ),
        Factory::Decimal => physical(DataType::Decimal256(76, -1), false),
        Factory::Utf8 => physical(DataType::Utf8View, false),
        Factory::Binary => physical(DataType::BinaryView, false),
    };
    let f = field(&ty);
    match factory {
        Factory::Unsigned => {
            ConstantValue::from_u64(f, ty, u64::MAX, policy, CompilePhase::Validate, control)
        }
        Factory::LargeInt => {
            ConstantValue::from_largeint(f, ty, i128::MIN, policy, CompilePhase::Validate, control)
        }
        Factory::Date => {
            ConstantValue::from_date32(f, ty, -1, policy, CompilePhase::Validate, control)
        }
        Factory::Time => {
            ConstantValue::from_time64(f, ty, 1, policy, CompilePhase::Validate, control)
        }
        Factory::Timestamp => {
            ConstantValue::from_timestamp(f, ty, -1, policy, CompilePhase::Validate, control)
        }
        Factory::Interval => ConstantValue::from_interval_month_day_nano(
            f,
            ty,
            (i32::MIN, i32::MAX, i64::MIN),
            policy,
            CompilePhase::Validate,
            control,
        ),
        Factory::Decimal => ConstantValue::from_decimal256_be(
            f,
            ty,
            [0; 32],
            policy,
            CompilePhase::Validate,
            control,
        ),
        Factory::Utf8 => ConstantValue::from_utf8(
            f,
            ty,
            "actual external view backing",
            policy,
            CompilePhase::Validate,
            control,
        ),
        Factory::Binary => ConstantValue::from_binary(
            f,
            ty,
            b"actual external view backing",
            policy,
            CompilePhase::Validate,
            control,
        ),
    }
}
const FACTORIES: [Factory; 9] = [
    Factory::Unsigned,
    Factory::LargeInt,
    Factory::Date,
    Factory::Time,
    Factory::Timestamp,
    Factory::Interval,
    Factory::Decimal,
    Factory::Utf8,
    Factory::Binary,
];

#[test]
fn every_factory_preserves_original_control_at_entry_preflight_constructor_and_validation_tail() {
    for factory in FACTORIES {
        let success = Control::default();
        construct(factory, policy(), &success).unwrap();
        let trace = success.trace.into_inner().unwrap();
        assert_eq!(trace[0], 0);
        assert!(trace.len() >= 4);
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            for at in 0..trace.len() {
                let control = Control {
                    refusal: Some((at, cause.clone())),
                    ..Control::default()
                };
                assert_eq!(
                    construct(factory, policy(), &control).unwrap_err(),
                    ConstantError::Control(cause.clone())
                );
                assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
            }
        }
    }
}

#[test]
fn factories_keep_real_metadata_quantum_and_finite_admission_refusal_before_backing() {
    let ty = physical(DataType::LargeUtf8, false);
    let metadata: HashMap<_, _> = (0..320)
        .map(|i| (format!("key{i:03}"), "v".repeat(1024)))
        .collect();
    let f = Arc::new(Field::new("literal", DataType::LargeUtf8, false).with_metadata(metadata));
    // The actual owner reserves error-formatting scratch as
    // (metadata bytes + 256 bytes per array node) * depth * 16.
    // This single-node field has 329607 metadata bytes, so diagnostics alone
    // require 5277808 bytes. Admit that finite fixture instead of lowering it.
    let mut wide_policy = policy();
    wide_policy.max_library_validation_bytes = 6_000_000;
    let success = Control::default();
    let value = ConstantValue::from_utf8(
        f.clone(),
        ty.clone(),
        "value",
        wide_policy,
        CompilePhase::Validate,
        &success,
    )
    .unwrap();
    let facts = value.pool().resource_facts();
    assert_eq!(facts.metadata_bytes, 329607);
    assert!(facts.library_validation_bytes_upper_bound > policy().max_library_validation_bytes);
    assert!(facts.library_validation_bytes_upper_bound <= wide_policy.max_library_validation_bytes);
    let trace = success.trace.into_inner().unwrap();
    assert!(trace.contains(&256));
    for cause in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let at = trace.iter().position(|units| *units == 256).unwrap();
        let control = Control {
            refusal: Some((at, cause.clone())),
            ..Control::default()
        };
        assert_eq!(
            ConstantValue::from_utf8(
                f.clone(),
                ty.clone(),
                "value",
                wide_policy,
                CompilePhase::Validate,
                &control
            )
            .unwrap_err(),
            ConstantError::Control(cause)
        );
        assert_eq!(*control.trace.lock().unwrap(), trace[..=at]);
    }
    for factory in FACTORIES {
        for deny in 0..3 {
            let mut p = policy();
            match deny {
                0 => p.max_rows = 0,
                1 => p.max_retained_buffer_bytes = 0,
                _ => p.max_library_validation_bytes = 0,
            }
            assert!(matches!(
                construct(factory, p, &Control::default()),
                Err(ConstantError::Limit(_))
            ));
        }
    }
}

#[test]
fn interval_factory_preserves_three_independent_signed_units_nulls_and_exact_carrier() {
    let ty = physical(
        DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
        true,
    );
    for value in [
        (0, 0, 0),
        (1, 2, 3),
        (-1, -2, -3),
        (i32::MIN, i32::MIN, i64::MIN),
        (i32::MAX, i32::MAX, i64::MAX),
        (i32::MIN, i32::MAX, i64::MAX),
        (i32::MAX, i32::MIN, i64::MIN),
    ] {
        let cv = ConstantValue::from_interval_month_day_nano(
            field(&ty),
            ty.clone(),
            value,
            policy(),
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        assert_eq!(cv.try_interval_month_day_nano().unwrap(), Some(value));
        assert_eq!(cv.value_type(), &ty);
        let native = cv
            .pool()
            .array()
            .as_any()
            .downcast_ref::<arrow_array::IntervalMonthDayNanoArray>()
            .unwrap()
            .value(0);
        assert_eq!(native.months, value.0);
        assert_eq!(native.days, value.1);
        assert_eq!(native.nanoseconds, value.2);
        assert!(cv.try_i64().is_err());
        assert!(cv.try_timestamp().is_err());
    }
    let null = ConstantValue::null(
        field(&ty),
        ty.clone(),
        policy(),
        CompilePhase::Validate,
        &Control::default(),
    )
    .unwrap();
    assert_eq!(null.try_interval_month_day_nano().unwrap(), None);
    assert_eq!(null.value_type(), &ty);
    for dtype in [
        DataType::Int64,
        DataType::FixedSizeBinary(16),
        DataType::Interval(arrow_schema::IntervalUnit::DayTime),
        DataType::Interval(arrow_schema::IntervalUnit::YearMonth),
    ] {
        let wrong = physical(dtype, true);
        assert!(
            ConstantValue::from_interval_month_day_nano(
                field(&wrong),
                wrong.clone(),
                (1, 2, 3),
                policy(),
                CompilePhase::Validate,
                &Control::default()
            )
            .is_err()
        );
        let null = ConstantValue::null(
            field(&wrong),
            wrong,
            policy(),
            CompilePhase::Validate,
            &Control::default(),
        )
        .unwrap();
        assert!(null.try_interval_month_day_nano().is_err());
    }
}

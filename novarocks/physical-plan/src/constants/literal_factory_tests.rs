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
use arrow_array::{Array, BooleanArray, Date32Array, Float64Array, Int64Array, UInt64Array};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use novarocks_constant_contract::ConstantPolicy;
use novarocks_type_contract::{CompilePhase, PureCompileControl, ValueLogicalType, ValueTypeError};
use std::sync::Mutex;

#[derive(Clone, Debug, Eq, PartialEq)]
enum Error {
    Type(ValueTypeError),
    Constant(ConstantError),
    Control(CompileControlError),
}
impl From<ValueTypeError> for Error {
    fn from(value: ValueTypeError) -> Self {
        Self::Type(value)
    }
}
impl From<ConstantError> for Error {
    fn from(value: ConstantError) -> Self {
        Self::Constant(value)
    }
}
impl From<CompileControlError> for Error {
    fn from(value: CompileControlError) -> Self {
        Self::Control(value)
    }
}
impl Error {
    fn control(&self) -> Option<CompileControlError> {
        match self {
            Self::Control(error) | Self::Constant(ConstantError::Control(error)) => {
                Some(error.clone())
            }
            _ => None,
        }
    }
    fn primary(&self) -> bool {
        self.control().is_some() || matches!(self, Self::Constant(ConstantError::Limit(_)))
    }
}
struct Control {
    phase: CompilePhase,
    trace: Mutex<Vec<u32>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl Control {
    fn good() -> Self {
        Self {
            phase: CompilePhase::Validate,
            trace: Default::default(),
            refusal: None,
        }
    }
    fn trace(&self) -> Vec<u32> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, self.phase);
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let position = trace.len();
        if let Some((stop, _)) = self.refusal {
            assert!(
                position <= stop,
                "callback after the first original refusal"
            );
        }
        trace.push(units);
        match self.refusal.clone() {
            Some((stop, error)) if stop == position => Err(error),
            _ => Ok(()),
        }
    }
}
fn policy() -> ConstantPolicy {
    ConstantPolicy {
        max_rows: 1,
        max_array_nodes: 16,
        max_logical_elements: 65536,
        max_retained_buffer_bytes: 4 << 20,
        max_type_depth: 16,
        max_type_nodes: 64,
        max_dictionary_depth: 4,
        max_metadata_bytes: 1 << 20,
        max_library_validation_work: 64 << 20,
        max_library_validation_bytes: 64 << 20,
    }
}
fn admit(
    literal: &crate::LiteralValue,
    ty: &FunctionValueType,
    policy: ConstantPolicy,
    control: &Control,
) -> Result<ConstantValue, Error> {
    let mut work = CompileCheckpoints::try_new(control, control.phase)?;
    let result = literal_constant_observed::<Error>(literal, ty, policy, control.phase, &mut work);
    if result.as_ref().is_err_and(Error::primary) {
        return result;
    }
    work.finish()?;
    result
}
fn plain(dt: DataType, nullable: bool) -> FunctionValueType {
    FunctionValueType::new(dt, nullable)
}
fn largeint() -> FunctionValueType {
    FunctionValueType::try_with_logical_type(
        DataType::FixedSizeBinary(16),
        false,
        ValueLogicalType::LargeInt,
    )
    .unwrap()
}
fn same_source(value: &ConstantValue, ty: &FunctionValueType) {
    assert_eq!(value.value_type(), ty);
    assert_eq!(value.field().name(), "constant");
    assert_eq!(value.field().data_type(), ty.data_type());
    assert_eq!(value.field().is_nullable(), ty.nullable());
    assert!(value.field().metadata().as_arc().is_none());
    assert_eq!(value.ordinal(), 0);
    assert_eq!(value.pool().array().len(), 1);
    assert_eq!(value.pool().resource_facts().rows, 1);
}

#[test]
fn literal_factory_all_fourteen_variants_preserve_independent_values_and_complete_types() {
    use crate::LiteralValue as L;
    let mut coefficient = [0xff; 32];
    coefficient[31] = 0xd6; // Independent two's-complement -42.
    let entries = vec![
        (L::Null, plain(DataType::Int64, true)),
        (L::Boolean(false), plain(DataType::Boolean, false)),
        (L::Int64(i64::MIN), plain(DataType::Int64, false)),
        (L::UInt64(u64::MAX), plain(DataType::UInt64, false)),
        (
            L::Float64Bits(0x7ff0_0000_0000_0043),
            plain(DataType::Float64, false),
        ),
        (L::LargeInt(i128::MIN + 43), largeint()),
        (
            L::Decimal128(-12345),
            plain(DataType::Decimal128(10, -2), false),
        ),
        (
            L::Decimal256(coefficient),
            plain(DataType::Decimal256(76, 17), false),
        ),
        (L::Utf8("hé🦀\0".into()), plain(DataType::Utf8, false)),
        (
            L::Binary(vec![0xff, 0, 0x80, 7].into_boxed_slice()),
            plain(DataType::Binary, false),
        ),
        (L::Date32(-1), plain(DataType::Date32, false)),
        (
            L::Time64(987654321),
            plain(DataType::Time64(TimeUnit::Microsecond), false),
        ),
        (
            L::Timestamp(-123456789),
            plain(
                DataType::Timestamp(TimeUnit::Nanosecond, Some("".into())),
                false,
            ),
        ),
        (
            L::IntervalMonthDayNano {
                months: i32::MIN,
                days: i32::MAX,
                nanoseconds: i64::MIN,
            },
            plain(DataType::Interval(IntervalUnit::MonthDayNano), false),
        ),
    ];
    assert_eq!(entries.len(), 14);
    for (literal, ty) in entries {
        let value = admit(&literal, &ty, policy(), &Control::good()).unwrap();
        same_source(&value, &ty);
        let array = value.pool().array();
        match literal {
            L::Null => assert!(array.is_null(0)),
            L::Boolean(_) => {
                assert_eq!(value.try_boolean().unwrap(), Some(false));
                assert!(
                    !array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .value(0)
                );
            }
            L::Int64(_) => {
                assert_eq!(value.try_i64().unwrap(), Some(i64::MIN));
                assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    i64::MIN
                );
            }
            L::UInt64(_) => {
                assert_eq!(value.try_u64().unwrap(), Some(u64::MAX));
                assert_eq!(
                    array
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(0),
                    u64::MAX
                );
            }
            L::Float64Bits(_) => assert_eq!(
                array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(0)
                    .to_bits(),
                0x7ff0_0000_0000_0043
            ),
            L::LargeInt(_) => assert_eq!(value.try_largeint().unwrap(), Some(i128::MIN + 43)),
            L::Decimal128(_) => assert_eq!(value.try_decimal128().unwrap(), Some(-12345)),
            L::Decimal256(_) => assert_eq!(value.try_decimal256_be().unwrap(), Some(coefficient)),
            L::Utf8(_) => assert_eq!(value.try_utf8().unwrap(), Some("hé🦀\0")),
            L::Binary(_) => assert_eq!(
                value.try_binary().unwrap(),
                Some([0xff, 0, 0x80, 7].as_slice())
            ),
            L::Date32(_) => assert_eq!(
                array
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap()
                    .value(0),
                -1
            ),
            L::Time64(_) => assert_eq!(value.try_time64().unwrap(), Some(987654321)),
            L::Timestamp(_) => assert_eq!(value.try_timestamp().unwrap(), Some(-123456789)),
            L::IntervalMonthDayNano { .. } => assert_eq!(
                value.try_interval_month_day_nano().unwrap(),
                Some((i32::MIN, i32::MAX, i64::MIN))
            ),
        }
    }
}

#[test]
fn literal_factory_keeps_float_bits_nominal_identity_timezone_presence_and_typed_null() {
    use crate::LiteralValue as L;
    for bits in [0, 1u64 << 63, 0x7ff0_0000_0000_0001, 0xfff8_0123_4567_89ab] {
        let value = admit(
            &L::Float64Bits(bits),
            &plain(DataType::Float64, false),
            policy(),
            &Control::good(),
        )
        .unwrap();
        assert_eq!(value.try_f64_bits().unwrap(), Some(bits));
    }
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [None, Some("".into()), Some("UTC".into())] {
            let ty = plain(DataType::Timestamp(unit, zone), false);
            let value = admit(&L::Timestamp(17), &ty, policy(), &Control::good()).unwrap();
            same_source(&value, &ty);
            assert_eq!(value.try_timestamp().unwrap(), Some(17));
        }
    }
    let json =
        FunctionValueType::try_with_logical_type(DataType::Utf8, true, ValueLogicalType::Json)
            .unwrap();
    let value = admit(
        &L::Utf8("{\"x\":1}".into()),
        &json,
        policy(),
        &Control::good(),
    )
    .unwrap();
    same_source(&value, &json);
    assert_eq!(value.value_type().logical_type(), ValueLogicalType::Json);
    assert_eq!(value.try_utf8().unwrap(), Some("{\"x\":1}"));
    for ty in [
        json,
        plain(DataType::Decimal128(8, 3), true),
        plain(
            DataType::Timestamp(TimeUnit::Microsecond, Some("".into())),
            true,
        ),
    ] {
        let value = admit(&L::Null, &ty, policy(), &Control::good()).unwrap();
        same_source(&value, &ty);
        assert!(value.pool().array().is_null(0));
    }
}

#[test]
fn literal_factory_refuses_wrong_carriers_nonnullable_null_and_invalid_field_domains() {
    use crate::LiteralValue as L;
    for (literal, ty, message) in [
        (
            L::LargeInt(7),
            plain(DataType::FixedSizeBinary(16), false),
            "LARGEINT factory requires exact logical LARGEINT type",
        ),
        (
            L::Date32(7),
            plain(DataType::Date64, false),
            "date factory requires exact Physical Date32 type",
        ),
        (
            L::Timestamp(7),
            plain(DataType::Utf8, false),
            "timestamp factory requires exact Timestamp type",
        ),
        (
            L::Null,
            plain(DataType::Int64, false),
            "NULL factory requires nullable exact type",
        ),
    ] {
        assert_eq!(
            admit(&literal, &ty, policy(), &Control::good()).err(),
            Some(Error::Constant(ConstantError::Invalid(message)))
        );
    }
    assert!(matches!(
        admit(
            &L::Int64(7),
            &plain(DataType::Binary, false),
            policy(),
            &Control::good()
        ),
        Err(Error::Constant(ConstantError::Invalid(_)))
    ));
    assert_eq!(
        FunctionValueType::try_with_logical_type(
            DataType::FixedSizeBinary(16),
            false,
            ValueLogicalType::Json
        )
        .unwrap_err(),
        ValueTypeError::InvalidLogicalCarrier(ValueLogicalType::Json)
    );
    assert!(matches!(
        admit(
            &L::Decimal128(123),
            &plain(DataType::Decimal128(2, 0), false),
            policy(),
            &Control::good()
        ),
        Err(Error::Constant(ConstantError::Source(_)))
    ));
}

#[test]
fn literal_factory_actual_owner_policy_bounds_accept_exact_and_refuse_one_below_without_tail() {
    let literal = crate::LiteralValue::Int64(7);
    let ty = plain(DataType::Int64, false);
    let facts = admit(&literal, &ty, policy(), &Control::good())
        .unwrap()
        .pool()
        .resource_facts();
    for (axis, exact) in [
        (0, facts.rows),
        (1, facts.array_nodes),
        (2, facts.logical_elements_upper_bound),
    ] {
        assert!(exact > 0);
        let set = |mut p: ConstantPolicy, limit| {
            match axis {
                0 => p.max_rows = limit,
                1 => p.max_array_nodes = limit,
                _ => p.max_logical_elements = limit,
            };
            p
        };
        assert!(admit(&literal, &ty, set(policy(), exact), &Control::good()).is_ok());
        let low = set(policy(), exact - 1);
        let control = Control::good();
        assert!(matches!(
            admit(&literal, &ty, low, &control),
            Err(Error::Constant(ConstantError::Limit(_)))
        ));
        let raw = Control::good();
        let mut work = CompileCheckpoints::try_new(&raw, raw.phase).unwrap();
        assert!(matches!(
            literal_constant_observed::<Error>(&literal, &ty, low, raw.phase, &mut work),
            Err(Error::Constant(ConstantError::Limit(_)))
        ));
        // The concrete caller must not add an ordinary finish after the owner
        // limit. Compare the exact same entry/delegate prefix without finishing.
        assert_eq!(control.trace(), raw.trace());
    }
    // Derive a tiny constructor acceptance boundary independently of the post
    // retained facts: the factory includes conservative header/alignment costs.
    let mut low = 0;
    let mut high = 4096;
    let set = |limit| ConstantPolicy {
        max_retained_buffer_bytes: limit,
        ..policy()
    };
    assert!(admit(&literal, &ty, set(high), &Control::good()).is_ok());
    while low < high {
        let middle = low + (high - low) / 2;
        match admit(&literal, &ty, set(middle), &Control::good()) {
            Ok(_) => high = middle,
            Err(Error::Constant(ConstantError::Limit(_))) => low = middle + 1,
            Err(error) => panic!("unexpected resource-boundary error: {error:?}"),
        }
    }
    assert!(low >= facts.retained_buffer_capacity_bytes);
    assert!(admit(&literal, &ty, set(low), &Control::good()).is_ok());
    assert!(matches!(
        admit(&literal, &ty, set(low - 1), &Control::good()),
        Err(Error::Constant(ConstantError::Limit(_)))
    ));
}

#[test]
fn literal_factory_all_small_actual_callback_prefixes_keep_three_original_control_causes() {
    for (literal, ty, succeeds) in [
        (
            crate::LiteralValue::Utf8("hé".into()),
            plain(DataType::Utf8, false),
            true,
        ),
        (
            crate::LiteralValue::Null,
            plain(DataType::Int64, false),
            false,
        ),
        (
            crate::LiteralValue::Utf8("{}".into()),
            plain(DataType::Int64, false),
            false,
        ),
    ] {
        let baseline_control = Control::good();
        let result = admit(&literal, &ty, policy(), &baseline_control);
        assert_eq!(result.is_ok(), succeeds);
        let baseline = baseline_control.trace();
        assert_eq!(baseline[0], 0);
        assert_eq!(baseline.last(), Some(&0)); // Caller-owned ordinary/success finish.
        for position in 0..baseline.len() {
            for cause in [
                CompileControlError::Cancelled,
                CompileControlError::DeadlineExceeded,
                CompileControlError::ResourceExhausted,
            ] {
                let refused = Control {
                    refusal: Some((position, cause.clone())),
                    ..Control::good()
                };
                let error = admit(&literal, &ty, policy(), &refused).err().unwrap();
                assert_eq!(error.control(), Some(cause));
                assert_eq!(refused.trace(), baseline[..=position]);
            }
        }
    }
}

#[test]
fn literal_factory_borrows_original_scope_and_leaves_final_observation_to_caller() {
    let ty = plain(DataType::Int64, false);
    for literal in [crate::LiteralValue::Int64(-8), crate::LiteralValue::Null] {
        let control = Control {
            phase: CompilePhase::LowerProgram,
            ..Control::good()
        };
        let mut work = CompileCheckpoints::try_new(&control, control.phase).unwrap();
        let result =
            literal_constant_observed::<Error>(&literal, &ty, policy(), control.phase, &mut work);
        let before = control.trace();
        work.finish().unwrap();
        let after = control.trace();
        assert_eq!(&after[..before.len()], before);
        assert_eq!(after.len(), before.len() + 1);
        assert_eq!(after.last(), Some(&0));
        match literal {
            crate::LiteralValue::Int64(_) => {
                assert_eq!(result.unwrap().try_i64().unwrap(), Some(-8))
            }
            crate::LiteralValue::Null => assert_eq!(
                result.err(),
                Some(Error::Constant(ConstantError::Invalid(
                    "NULL factory requires nullable exact type"
                )))
            ),
            _ => unreachable!(),
        }
    }
}

#[test]
fn literal_factory_wide_typed_null_source_observes_actual_quantum_and_original_prefix() {
    let text = "hé🦀".repeat(4096);
    let literal = crate::LiteralValue::Utf8(text.clone().into_boxed_str());
    let ty = plain(DataType::Utf8, false);
    let control = Control::good();
    let value = admit(&literal, &ty, policy(), &control).unwrap();
    assert_eq!(value.try_utf8().unwrap(), Some(text.as_str()));
    // A trusted &str factory preserves Unicode values but does not promise
    // internal byte callbacks. A real wide NULL type supplies the observed
    // type/array traversal instead of charging fictitious payload chunks.
    let fields: Vec<_> = (0..320)
        .map(|index| arrow_schema::Field::new(format!("source_{index}"), DataType::Int64, true))
        .collect();
    let ty = plain(DataType::Struct(fields.into()), true);
    let literal = crate::LiteralValue::Null;
    let wide_policy = ConstantPolicy {
        max_array_nodes: 1024,
        max_type_nodes: 1024,
        ..policy()
    };
    let control = Control::good();
    let value = admit(&literal, &ty, wide_policy, &control).unwrap();
    same_source(&value, &ty);
    assert!(value.pool().array().is_null(0));
    assert_eq!(value.pool().resource_facts().array_nodes, 321);
    let array = value
        .pool()
        .array()
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();
    assert_eq!(array.num_columns(), 320);
    assert!(
        array
            .columns()
            .iter()
            .all(|column| column.len() == 1 && column.is_null(0))
    );
    let baseline = control.trace();
    assert!(baseline.contains(&256));
    let positions: Vec<_> = baseline
        .iter()
        .enumerate()
        .filter_map(|(index, units)| (*units == 256).then_some(index))
        .take(2)
        .chain([0, baseline.len() - 1])
        .collect();
    for position in positions {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let refused = Control {
                refusal: Some((position, cause.clone())),
                ..Control::good()
            };
            assert_eq!(
                admit(&literal, &ty, wide_policy, &refused)
                    .err()
                    .unwrap()
                    .control(),
                Some(cause)
            );
            assert_eq!(refused.trace(), baseline[..=position]);
        }
    }
}

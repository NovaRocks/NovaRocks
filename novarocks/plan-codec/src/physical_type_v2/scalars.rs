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

//! Single-node primitive carrier projection for the flat v2 type table.
//! Nested references and complete value-type validation belong to the parent.
//! The parent observes this bounded converter with the original control.

use super::TypeCodecError;
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use novarocks_proto_models::{physical_type_v2 as wire, plan};
use novarocks_type_contract::{CarrierParameterError, validate_arrow_carrier_parameters_observed};
use wire::carrier_type_definition::Kind;

type E = TypeCodecError;

fn parameters(data_type: &DataType) -> Result<(), E> {
    // Only a primitive node enters this helper: no nested fields, Union IDs or
    // unbounded traversal. The parent owns observation and full tree checks.
    validate_arrow_carrier_parameters_observed::<CarrierParameterError>(data_type, || Ok(()))
        .map_err(|_| E::InvalidShape("invalid primitive carrier parameters"))
}
fn encode_unit(unit: &TimeUnit) -> i32 {
    match unit {
        TimeUnit::Second => plan::ArrowTimeUnit::Second as i32,
        TimeUnit::Millisecond => plan::ArrowTimeUnit::Millisecond as i32,
        TimeUnit::Microsecond => plan::ArrowTimeUnit::Microsecond as i32,
        TimeUnit::Nanosecond => plan::ArrowTimeUnit::Nanosecond as i32,
    }
}
fn decode_unit(unit: i32) -> Result<TimeUnit, E> {
    match plan::ArrowTimeUnit::try_from(unit) {
        Ok(plan::ArrowTimeUnit::Second) => Ok(TimeUnit::Second),
        Ok(plan::ArrowTimeUnit::Millisecond) => Ok(TimeUnit::Millisecond),
        Ok(plan::ArrowTimeUnit::Microsecond) => Ok(TimeUnit::Microsecond),
        Ok(plan::ArrowTimeUnit::Nanosecond) => Ok(TimeUnit::Nanosecond),
        _ => Err(E::InvalidShape("unknown or unspecified time unit")),
    }
}
fn decimal(precision: u32, scale: i32, constructor: fn(u8, i8) -> DataType) -> Result<DataType, E> {
    let precision = u8::try_from(precision)
        .map_err(|_| E::InvalidShape("decimal precision is not representable"))?;
    let scale =
        i8::try_from(scale).map_err(|_| E::InvalidShape("decimal scale is not representable"))?;
    let data_type = constructor(precision, scale);
    parameters(&data_type)?;
    Ok(data_type)
}

pub(super) fn encode_scalar(data_type: &DataType) -> Result<Option<Kind>, E> {
    encode_scalar_core(data_type, true, None)
}

/// The selector is private to the original source encoder and contains the
/// actual immutable recipe loan. Callers cannot supply a skip-validation flag.
pub(super) fn encode_scalar_from_source(
    data_type: &DataType,
    source: super::encode::SourceLaw<'_>,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<Option<Kind>, E> {
    encode_scalar_core(data_type, source.is_strict(), Some(work))
}

fn encode_scalar_core(
    data_type: &DataType,
    _strict: bool,
    work: Option<&mut novarocks_type_contract::CompileCheckpoints<'_>>,
) -> Result<Option<Kind>, E> {
    use plan::ArrowPrimitiveType as P;
    let kind = match data_type {
        DataType::Null => Kind::Primitive(P::Null as i32),
        DataType::Boolean => Kind::Primitive(P::Boolean as i32),
        DataType::Int8 => Kind::Primitive(P::Int8 as i32),
        DataType::Int16 => Kind::Primitive(P::Int16 as i32),
        DataType::Int32 => Kind::Primitive(P::Int32 as i32),
        DataType::Int64 => Kind::Primitive(P::Int64 as i32),
        DataType::UInt8 => Kind::Primitive(P::Uint8 as i32),
        DataType::UInt16 => Kind::Primitive(P::Uint16 as i32),
        DataType::UInt32 => Kind::Primitive(P::Uint32 as i32),
        DataType::UInt64 => Kind::Primitive(P::Uint64 as i32),
        DataType::Float16 => Kind::Primitive(P::Float16 as i32),
        DataType::Float32 => Kind::Primitive(P::Float32 as i32),
        DataType::Float64 => Kind::Primitive(P::Float64 as i32),
        DataType::Date32 => Kind::Primitive(P::Date32 as i32),
        DataType::Date64 => Kind::Primitive(P::Date64 as i32),
        DataType::Binary => Kind::Primitive(P::Binary as i32),
        DataType::BinaryView => Kind::Primitive(P::BinaryView as i32),
        DataType::LargeBinary => Kind::Primitive(P::LargeBinary as i32),
        DataType::Utf8 => Kind::Primitive(P::Utf8 as i32),
        DataType::Utf8View => Kind::Primitive(P::Utf8View as i32),
        DataType::LargeUtf8 => Kind::Primitive(P::LargeUtf8 as i32),
        DataType::Timestamp(unit, zone) => Kind::Timestamp(plan::ArrowTimestampType {
            unit: encode_unit(unit),
            timezone: match (zone, work) {
                (Some(zone), Some(work)) => {
                    Some(crate::arrow_metadata_v2::copy_string(zone, work)?)
                }
                (Some(zone), None) => Some(zone.to_string()),
                (None, _) => None,
            },
        }),
        DataType::Time32(unit) => Kind::Time32(plan::ArrowTimeType {
            unit: encode_unit(unit),
        }),
        DataType::Time64(unit) => Kind::Time64(plan::ArrowTimeType {
            unit: encode_unit(unit),
        }),
        DataType::Duration(unit) => Kind::Duration(plan::ArrowTimeType {
            unit: encode_unit(unit),
        }),
        DataType::Interval(unit) => Kind::Interval(match unit {
            IntervalUnit::YearMonth => plan::ArrowIntervalUnit::YearMonth as i32,
            IntervalUnit::DayTime => plan::ArrowIntervalUnit::DayTime as i32,
            IntervalUnit::MonthDayNano => plan::ArrowIntervalUnit::MonthDayNano as i32,
        }),
        DataType::FixedSizeBinary(width) => Kind::FixedSizeBinary(*width),
        DataType::Decimal32(precision, scale) => Kind::Decimal32(plan::ArrowDecimalType {
            precision: u32::from(*precision),
            scale: i32::from(*scale),
        }),
        DataType::Decimal64(precision, scale) => Kind::Decimal64(plan::ArrowDecimalType {
            precision: u32::from(*precision),
            scale: i32::from(*scale),
        }),
        DataType::Decimal128(precision, scale) => Kind::Decimal128(plan::ArrowDecimalType {
            precision: u32::from(*precision),
            scale: i32::from(*scale),
        }),
        DataType::Decimal256(precision, scale) => Kind::Decimal256(plan::ArrowDecimalType {
            precision: u32::from(*precision),
            scale: i32::from(*scale),
        }),
        DataType::List(_)
        | DataType::ListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::LargeList(_)
        | DataType::LargeListView(_)
        | DataType::Struct(_)
        | DataType::Union(_, _)
        | DataType::Dictionary(_, _)
        | DataType::Map(_, _)
        | DataType::RunEndEncoded(_, _) => return Ok(None),
    };
    parameters(data_type)?;
    Ok(Some(kind))
}

pub(super) fn decode_scalar(kind: &Kind) -> Result<Option<DataType>, E> {
    decode_scalar_core(kind, true)
}

/// This policy is derived only from the sealed original package graph. The
/// primitive/decimal/unit grammar stays with the same converter below.
pub(super) fn decode_scalar_in_package(
    kind: &Kind,
    graph: &super::PreparedPackageTypeGraph<'_>,
    id: u32,
    work: &mut novarocks_type_contract::CompileCheckpoints<'_>,
) -> Result<Option<DataType>, E> {
    let strict = graph.domain(super::graph::Node::Carrier(id), work)?
        != super::PackageTypeRootDomain::Writer;
    decode_scalar_core(kind, strict)
}

fn decode_scalar_core(kind: &Kind, _strict: bool) -> Result<Option<DataType>, E> {
    use plan::ArrowPrimitiveType as P;
    let data_type = match kind {
        Kind::Primitive(value) => match P::try_from(*value) {
            Ok(P::Null) => DataType::Null,
            Ok(P::Boolean) => DataType::Boolean,
            Ok(P::Int8) => DataType::Int8,
            Ok(P::Int16) => DataType::Int16,
            Ok(P::Int32) => DataType::Int32,
            Ok(P::Int64) => DataType::Int64,
            Ok(P::Uint8) => DataType::UInt8,
            Ok(P::Uint16) => DataType::UInt16,
            Ok(P::Uint32) => DataType::UInt32,
            Ok(P::Uint64) => DataType::UInt64,
            Ok(P::Float16) => DataType::Float16,
            Ok(P::Float32) => DataType::Float32,
            Ok(P::Float64) => DataType::Float64,
            Ok(P::Date32) => DataType::Date32,
            Ok(P::Date64) => DataType::Date64,
            Ok(P::Binary) => DataType::Binary,
            Ok(P::BinaryView) => DataType::BinaryView,
            Ok(P::LargeBinary) => DataType::LargeBinary,
            Ok(P::Utf8) => DataType::Utf8,
            Ok(P::Utf8View) => DataType::Utf8View,
            Ok(P::LargeUtf8) => DataType::LargeUtf8,
            _ => return Err(E::InvalidShape("unknown or unspecified primitive type")),
        },
        Kind::Timestamp(value) => {
            let unit = decode_unit(value.unit)?;
            DataType::Timestamp(unit, value.timezone.as_deref().map(Into::into))
        }
        Kind::Time32(value) => DataType::Time32(decode_unit(value.unit)?),
        Kind::Time64(value) => DataType::Time64(decode_unit(value.unit)?),
        Kind::Duration(value) => DataType::Duration(decode_unit(value.unit)?),
        Kind::Interval(value) => {
            DataType::Interval(match plan::ArrowIntervalUnit::try_from(*value) {
                Ok(plan::ArrowIntervalUnit::YearMonth) => IntervalUnit::YearMonth,
                Ok(plan::ArrowIntervalUnit::DayTime) => IntervalUnit::DayTime,
                Ok(plan::ArrowIntervalUnit::MonthDayNano) => IntervalUnit::MonthDayNano,
                _ => return Err(E::InvalidShape("unknown or unspecified interval unit")),
            })
        }
        Kind::FixedSizeBinary(width) => DataType::FixedSizeBinary(*width),
        Kind::Decimal32(value) => decimal(value.precision, value.scale, DataType::Decimal32)?,
        Kind::Decimal64(value) => decimal(value.precision, value.scale, DataType::Decimal64)?,
        Kind::Decimal128(value) => decimal(value.precision, value.scale, DataType::Decimal128)?,
        Kind::Decimal256(value) => decimal(value.precision, value.scale, DataType::Decimal256)?,
        Kind::ListFieldId(_)
        | Kind::ListViewFieldId(_)
        | Kind::FixedSizeList(_)
        | Kind::LargeListFieldId(_)
        | Kind::LargeListViewFieldId(_)
        | Kind::StructType(_)
        | Kind::UnionType(_)
        | Kind::Dictionary(_)
        | Kind::Map(_)
        | Kind::RunEndEncoded(_) => return Ok(None),
    };
    parameters(&data_type)?;
    Ok(Some(data_type))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, UnionFields, UnionMode};
    use std::sync::Arc;

    type DecimalConstructor = fn(u8, i8) -> DataType;
    type WireDecimalConstructor = fn(plan::ArrowDecimalType) -> Kind;

    #[test]
    fn all_twenty_one_primitive_variants_have_independent_stable_wire_numbers() {
        let expected = [
            (DataType::Null, 1),
            (DataType::Boolean, 2),
            (DataType::Int8, 3),
            (DataType::Int16, 4),
            (DataType::Int32, 5),
            (DataType::Int64, 6),
            (DataType::UInt8, 7),
            (DataType::UInt16, 8),
            (DataType::UInt32, 9),
            (DataType::UInt64, 10),
            (DataType::Float16, 11),
            (DataType::Float32, 12),
            (DataType::Float64, 13),
            (DataType::Date32, 14),
            (DataType::Date64, 15),
            (DataType::Binary, 16),
            (DataType::BinaryView, 17),
            (DataType::LargeBinary, 18),
            (DataType::Utf8, 19),
            (DataType::Utf8View, 20),
            (DataType::LargeUtf8, 21),
        ];
        for (data_type, number) in expected {
            assert_eq!(
                encode_scalar(&data_type).unwrap(),
                Some(Kind::Primitive(number))
            );
            assert_eq!(
                decode_scalar(&Kind::Primitive(number)).unwrap(),
                Some(data_type)
            );
        }
    }

    #[test]
    fn temporal_units_preserve_timezone_absence_empty_and_exact_text() {
        for (unit, number) in [
            (TimeUnit::Second, 1),
            (TimeUnit::Millisecond, 2),
            (TimeUnit::Microsecond, 3),
            (TimeUnit::Nanosecond, 4),
        ] {
            for zone in [
                None,
                Some(""),
                Some("UTC"),
                Some("Asia/Shanghai"),
                Some("+05:30"),
            ] {
                let data_type = DataType::Timestamp(unit, zone.map(Into::into));
                let expected = Kind::Timestamp(plan::ArrowTimestampType {
                    unit: number,
                    timezone: zone.map(str::to_owned),
                });
                assert_eq!(encode_scalar(&data_type).unwrap(), Some(expected.clone()));
                assert_eq!(decode_scalar(&expected).unwrap(), Some(data_type));
            }
            let duration = Kind::Duration(plan::ArrowTimeType { unit: number });
            assert_eq!(
                encode_scalar(&DataType::Duration(unit)).unwrap(),
                Some(duration.clone())
            );
            assert_eq!(
                decode_scalar(&duration).unwrap(),
                Some(DataType::Duration(unit))
            );
            let time = if number <= 2 {
                Kind::Time32(plan::ArrowTimeType { unit: number })
            } else {
                Kind::Time64(plan::ArrowTimeType { unit: number })
            };
            let data_type = if number <= 2 {
                DataType::Time32(unit)
            } else {
                DataType::Time64(unit)
            };
            assert_eq!(encode_scalar(&data_type).unwrap(), Some(time.clone()));
            assert_eq!(decode_scalar(&time).unwrap(), Some(data_type));
        }
        for (unit, number) in [
            (IntervalUnit::YearMonth, 1),
            (IntervalUnit::DayTime, 2),
            (IntervalUnit::MonthDayNano, 3),
        ] {
            assert_eq!(
                encode_scalar(&DataType::Interval(unit)).unwrap(),
                Some(Kind::Interval(number))
            );
            assert_eq!(
                decode_scalar(&Kind::Interval(number)).unwrap(),
                Some(DataType::Interval(unit))
            );
        }
    }

    #[test]
    fn all_decimal_widths_preserve_precision_and_every_negative_scale() {
        let constructors: [(DecimalConstructor, WireDecimalConstructor, u8); 4] = [
            (DataType::Decimal32, Kind::Decimal32, 9),
            (DataType::Decimal64, Kind::Decimal64, 18),
            (DataType::Decimal128, Kind::Decimal128, 38),
            (DataType::Decimal256, Kind::Decimal256, 76),
        ];
        for (data_type, wire_type, max_precision) in constructors {
            for scale in i8::MIN..=0 {
                let source = data_type(max_precision, scale);
                let expected = wire_type(plan::ArrowDecimalType {
                    precision: u32::from(max_precision),
                    scale: i32::from(scale),
                });
                assert_eq!(encode_scalar(&source).unwrap(), Some(expected.clone()));
                assert_eq!(decode_scalar(&expected).unwrap(), Some(source));
            }
            let source = data_type(max_precision, max_precision as i8);
            assert_eq!(
                decode_scalar(&encode_scalar(&source).unwrap().unwrap()).unwrap(),
                Some(source)
            );
        }
    }

    #[test]
    fn fixed_binary_width_is_exact_and_never_authors_largeint_or_uuid() {
        for width in [0, 1, 16, i32::MAX] {
            assert_eq!(
                encode_scalar(&DataType::FixedSizeBinary(width)).unwrap(),
                Some(Kind::FixedSizeBinary(width))
            );
            assert_eq!(
                decode_scalar(&Kind::FixedSizeBinary(width)).unwrap(),
                Some(DataType::FixedSizeBinary(width))
            );
        }
        assert!(encode_scalar(&DataType::FixedSizeBinary(-1)).is_err());
        assert!(decode_scalar(&Kind::FixedSizeBinary(-1)).is_err());
    }

    #[test]
    fn unspecified_and_unknown_closed_enums_are_errors_not_nearby_types() {
        for value in [0, -1, i32::MAX] {
            for kind in [
                Kind::Primitive(value),
                Kind::Timestamp(plan::ArrowTimestampType {
                    unit: value,
                    timezone: None,
                }),
                Kind::Time32(plan::ArrowTimeType { unit: value }),
                Kind::Time64(plan::ArrowTimeType { unit: value }),
                Kind::Duration(plan::ArrowTimeType { unit: value }),
                Kind::Interval(value),
            ] {
                assert!(matches!(decode_scalar(&kind), Err(E::InvalidShape(_))));
            }
        }
        for unit in [TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            assert!(encode_scalar(&DataType::Time32(unit)).is_err());
            assert!(
                decode_scalar(&Kind::Time32(plan::ArrowTimeType {
                    unit: encode_unit(&unit)
                }))
                .is_err()
            );
        }
        for unit in [TimeUnit::Second, TimeUnit::Millisecond] {
            assert!(encode_scalar(&DataType::Time64(unit)).is_err());
            assert!(
                decode_scalar(&Kind::Time64(plan::ArrowTimeType {
                    unit: encode_unit(&unit)
                }))
                .is_err()
            );
        }
    }

    #[test]
    fn decimal_representation_and_actual_parameter_grammar_reject_overflow_without_clamping() {
        let constructors: [(DecimalConstructor, WireDecimalConstructor, u8); 4] = [
            (DataType::Decimal32, Kind::Decimal32, 9),
            (DataType::Decimal64, Kind::Decimal64, 18),
            (DataType::Decimal128, Kind::Decimal128, 38),
            (DataType::Decimal256, Kind::Decimal256, 76),
        ];
        for (data_type, wire_type, max_precision) in constructors {
            for (precision, scale) in [
                (256, 0),
                (u32::MAX, 0),
                (1, 128),
                (1, -129),
                (1, i32::MIN),
                (1, i32::MAX),
                (0, 0),
                (u32::from(max_precision) + 1, 0),
                (1, 2),
            ] {
                assert!(matches!(
                    decode_scalar(&wire_type(plan::ArrowDecimalType { precision, scale })),
                    Err(E::InvalidShape(_))
                ));
            }
            for source in [
                data_type(0, 0),
                data_type(max_precision + 1, 0),
                data_type(1, 2),
            ] {
                assert!(encode_scalar(&source).is_err());
            }
        }
    }

    #[test]
    fn timezone_spelling_round_trips_beyond_the_old_byte_limit() {
        for zone in [String::new(), "a".repeat(1024), "é".repeat(1024 / 2 + 1)] {
            let source = DataType::Timestamp(TimeUnit::Nanosecond, Some(zone.as_str().into()));
            let expected = Kind::Timestamp(plan::ArrowTimestampType {
                unit: 4,
                timezone: Some(zone),
            });
            assert_eq!(encode_scalar(&source).unwrap(), Some(expected.clone()));
            assert_eq!(decode_scalar(&expected).unwrap(), Some(source));
        }
    }

    #[test]
    fn every_nested_variant_is_delegated_without_reconstructing_or_validating_its_references() {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        for data_type in [
            DataType::List(field.clone()),
            DataType::ListView(field.clone()),
            DataType::FixedSizeList(field.clone(), 1),
            DataType::LargeList(field.clone()),
            DataType::LargeListView(field.clone()),
            DataType::Struct(vec![field.clone()].into()),
            DataType::Union(UnionFields::empty(), UnionMode::Dense),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::Map(field.clone(), false),
            DataType::RunEndEncoded(field.clone(), field),
        ] {
            assert_eq!(encode_scalar(&data_type).unwrap(), None);
        }
        for kind in [
            Kind::ListFieldId(0),
            Kind::ListViewFieldId(u32::MAX),
            Kind::FixedSizeList(wire::FixedSizeList {
                item_field_id: Some(0),
                length: 1,
            }),
            Kind::LargeListFieldId(0),
            Kind::LargeListViewFieldId(u32::MAX),
            Kind::StructType(wire::StructFields {
                field_ids: vec![0, u32::MAX],
            }),
            Kind::UnionType(wire::UnionFields {
                mode: 2,
                fields: vec![],
            }),
            Kind::Dictionary(wire::DictionaryTypes {
                key_type_id: Some(0),
                value_type_id: Some(u32::MAX),
            }),
            Kind::Map(wire::MapField {
                entries_field_id: Some(0),
                ordered: true,
            }),
            Kind::RunEndEncoded(wire::RunEndEncodedFields {
                run_ends_field_id: Some(0),
                values_field_id: Some(u32::MAX),
            }),
        ] {
            assert_eq!(decode_scalar(&kind).unwrap(), None);
        }
    }
}

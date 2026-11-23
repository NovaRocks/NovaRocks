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

//! Trino 484 catalog type names and JDBC column facts.
use arrow::datatypes::{DataType, TimeUnit};
use novarocks_type_contract::{CharacterKind, TemporalType, ValueKind, ValueTypeRef};
use std::fmt::{self, Display, Write};

/// Uses the full recursive declaration, including names and semantic children.
pub(super) struct MetadataTypeName<'a>(pub(super) &'a ValueTypeRef);

impl Display for MetadataTypeName<'_> {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        use ValueKind as K;
        match self.0.kind() {
            K::String => output.write_str("varchar"),
            K::Binary => output.write_str("varbinary"),
            K::BoundedCharacter(character) => {
                let name = match character.kind() {
                    CharacterKind::Char => "char",
                    CharacterKind::Varchar => "varchar",
                };
                write!(output, "{name}({})", character.length())
            }
            K::Nominal(domain) => output.write_str(domain.metadata_value().ok_or(fmt::Error)?),
            K::Temporal(ty) => match ty {
                TemporalType::Time(precision) => {
                    write!(output, "time({})", precision.digits())
                }
                TemporalType::TimeWithTimeZone(precision) => {
                    write!(output, "time({}) with time zone", precision.digits())
                }
                TemporalType::Timestamp(precision) => {
                    write!(output, "timestamp({})", precision.digits())
                }
                TemporalType::TimestampWithTimeZone(precision) => {
                    write!(output, "timestamp({}) with time zone", precision.digits())
                }
                TemporalType::Interval(qualifier) if qualifier.is_year_month() => {
                    output.write_str("interval year to month")
                }
                TemporalType::Interval(_) => output.write_str("interval day to second"),
            },
            K::List(item) | K::FixedSizeList { item, .. } => {
                write!(output, "array({})", Self(item))
            }
            K::Map { key, value } => write!(output, "map({}, {})", Self(key), Self(value)),
            K::Struct(fields) => {
                output.write_str("row(")?;
                for (index, field) in fields.iter().enumerate() {
                    if index != 0 {
                        output.write_str(", ")?;
                    }
                    output.write_char('"')?;
                    for ch in field.name().chars() {
                        if ch == '"' {
                            output.write_char('"')?;
                        }
                        output.write_char(ch)?;
                    }
                    write!(output, "\" {}", Self(field.value_type()))?;
                }
                output.write_char(')')
            }
            K::Scalar(ty) => match ty {
                DataType::Null => output.write_str("unknown"),
                DataType::Boolean => output.write_str("boolean"),
                DataType::Int8 => output.write_str("tinyint"),
                DataType::Int16 => output.write_str("smallint"),
                DataType::Int32 => output.write_str("integer"),
                DataType::Int64 => output.write_str("bigint"),
                DataType::Float32 => output.write_str("real"),
                DataType::Float64 => output.write_str("double"),
                DataType::Decimal32(precision, scale)
                | DataType::Decimal64(precision, scale)
                | DataType::Decimal128(precision, scale)
                | DataType::Decimal256(precision, scale) => {
                    write!(output, "decimal({precision},{scale})")
                }
                DataType::Date32 | DataType::Date64 => output.write_str("date"),
                DataType::Time32(unit) | DataType::Time64(unit) => {
                    write!(output, "time({})", unit_precision(*unit))
                }
                DataType::Timestamp(unit, zone) => {
                    write!(output, "timestamp({})", unit_precision(*unit))?;
                    if zone.is_some() {
                        output.write_str(" with time zone")?;
                    }
                    Ok(())
                }
                DataType::FixedSizeBinary(_) => output.write_str("varbinary"),
                _ => Err(fmt::Error),
            },
            K::Union(_) => Err(fmt::Error),
        }
    }
}

const fn unit_precision(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 0,
        TimeUnit::Millisecond => 3,
        TimeUnit::Microsecond => 6,
        TimeUnit::Nanosecond => 9,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct JdbcColumnType {
    pub(super) code: i64,
    pub(super) column_size: Option<i64>,
    pub(super) decimal_digits: Option<i64>,
    pub(super) radix: Option<i64>,
    pub(super) char_octet_length: Option<i64>,
}

impl JdbcColumnType {
    const fn plain(code: i64) -> Self {
        Self {
            code,
            column_size: None,
            decimal_digits: None,
            radix: None,
            char_octet_length: None,
        }
    }

    const fn numeric(code: i64, size: i64, radix: i64, digits: Option<i64>) -> Self {
        Self {
            column_size: Some(size),
            radix: Some(radix),
            decimal_digits: digits,
            ..Self::plain(code)
        }
    }

    const fn character(code: i64, length: i64) -> Self {
        Self {
            column_size: Some(length),
            char_octet_length: Some(length),
            ..Self::plain(code)
        }
    }

    fn temporal(code: i64, whole: i64, precision: i64) -> Self {
        Self {
            column_size: Some(whole + precision.min(1) + precision),
            decimal_digits: Some(precision),
            ..Self::plain(code)
        }
    }

    pub(super) fn from_value(value: &ValueTypeRef) -> Self {
        use ValueKind as K;
        // ColumnJdbcTable in 484 uses the longest available Java time-zone
        // ID (32 characters on the declared 484 reference image).
        const TIMESTAMP_WITH_ZONE_WHOLE: i64 = 1 + 6 + 15 + 1 + 32;
        match value.kind() {
            K::String => Self::character(12, i64::from(i32::MAX)),
            K::BoundedCharacter(ty) => Self::character(
                match ty.kind() {
                    CharacterKind::Char => 1,
                    CharacterKind::Varchar => 12,
                },
                i64::from(ty.length()),
            ),
            K::Binary | K::Scalar(DataType::FixedSizeBinary(_)) => {
                Self::character(-3, i64::from(i32::MAX))
            }
            K::List(_) | K::FixedSizeList { .. } => Self::plain(2003),
            K::Temporal(ty) => {
                let precision = i64::from(ty.precision().digits());
                match ty {
                    TemporalType::Time(_) => Self::temporal(92, 8, precision),
                    TemporalType::TimeWithTimeZone(_) => Self::temporal(2013, 14, precision),
                    TemporalType::Timestamp(_) => Self::temporal(93, 21, precision),
                    TemporalType::TimestampWithTimeZone(_) => {
                        Self::temporal(2014, TIMESTAMP_WITH_ZONE_WHOLE, precision)
                    }
                    TemporalType::Interval(_) => Self::plain(2000),
                }
            }
            K::Scalar(ty) => match ty {
                DataType::Boolean => Self::plain(16),
                DataType::Int8 => Self::numeric(-6, 3, 10, None),
                DataType::Int16 => Self::numeric(5, 5, 10, None),
                DataType::Int32 => Self::numeric(4, 10, 10, None),
                DataType::Int64 => Self::numeric(-5, 19, 10, None),
                DataType::Float32 => Self::numeric(7, 24, 2, None),
                DataType::Float64 => Self::numeric(8, 53, 2, None),
                DataType::Decimal32(p, s)
                | DataType::Decimal64(p, s)
                | DataType::Decimal128(p, s)
                | DataType::Decimal256(p, s) => {
                    Self::numeric(3, i64::from(*p), 10, Some(i64::from(*s)))
                }
                DataType::Date32 | DataType::Date64 => Self {
                    column_size: Some(14),
                    ..Self::plain(91)
                },
                DataType::Time32(unit) | DataType::Time64(unit) => {
                    Self::temporal(92, 8, unit_precision(*unit))
                }
                DataType::Timestamp(unit, None) => Self::temporal(93, 21, unit_precision(*unit)),
                DataType::Timestamp(unit, Some(_)) => {
                    Self::temporal(2014, TIMESTAMP_WITH_ZONE_WHOLE, unit_precision(*unit))
                }
                _ => Self::plain(2000),
            },
            _ => Self::plain(2000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;
    use novarocks_type_contract::{CharacterType, FunctionValueType, TemporalPrecision};
    use novarocks_types::schema::SqlType;
    use std::sync::Arc;

    #[test]
    fn system_tables_metadata_uses_typed_recursive_spelling_and_jdbc_geometry() {
        let carrier = DataType::Struct(
            [
                Arc::new(Field::new("mixed Case", DataType::Int64, false)),
                Arc::new(Field::new(
                    "decimal_list",
                    DataType::List(Arc::new(Field::new(
                        "item",
                        DataType::Decimal128(12, 2),
                        true,
                    ))),
                    true,
                )),
            ]
            .into(),
        );
        let declaration = FunctionValueType::new(carrier, true);
        assert_eq!(
            MetadataTypeName(declaration.value_type()).to_string(),
            "row(\"mixed Case\" bigint, \"decimal_list\" array(decimal(12,2)))"
        );
        assert_eq!(
            JdbcColumnType::from_value(declaration.value_type()).code,
            2000
        );
        let char_type = CharacterType::new(CharacterKind::Char, 5).unwrap();
        let char_typed = novarocks_types::execution_value_type_from_declaration(
            &SqlType::BoundedCharacter(char_type),
            &DataType::Utf8,
            true,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        assert_eq!(
            MetadataTypeName(char_typed.value_type()).to_string(),
            "char(5)"
        );
        assert_eq!(
            JdbcColumnType::from_value(char_typed.value_type()),
            JdbcColumnType {
                code: 1,
                column_size: Some(5),
                decimal_digits: None,
                radix: None,
                char_octet_length: Some(5),
            }
        );
        let zone = TemporalType::TimestampWithTimeZone(TemporalPrecision::MICROSECONDS);
        let zoned = novarocks_types::execution_value_type_from_declaration(
            &SqlType::Temporal(zone),
            &zone.carrier(),
            true,
            novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        assert_eq!(
            MetadataTypeName(zoned.value_type()).to_string(),
            "timestamp(6) with time zone"
        );
        let jdbc = JdbcColumnType::from_value(zoned.value_type());
        assert_eq!(jdbc.code, 2014);
        assert_eq!(jdbc.column_size, Some(62));
        assert_eq!(jdbc.decimal_digits, Some(6));
    }
}

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

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: arrow::datatypes::DataType,
    pub nullable: bool,
    pub write_default: Option<ColumnDefault>,
    /// Logical (StarRocks) type when the Arrow `data_type` collapses several
    /// distinct logical kinds onto the same storage representation. Today the
    /// consumers are logical types such as JSON, BITMAP, and HLL when they
    /// materialise as generic Arrow storage. The analyzer uses this side table
    /// to preserve StarRocks semantics that are not encoded in Arrow alone.
    /// `None` means "the Arrow type is the authoritative type".
    pub logical_type: Option<SqlType>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnDefault {
    Null,
    Boolean(bool),
    Int32(i32),
    Int64(i64),
    Float32 {
        bits: u32,
    },
    Float64 {
        bits: u64,
    },
    Decimal {
        unscaled: i128,
        precision: u8,
        scale: i8,
    },
    String(String),
    Binary(Vec<u8>),
    Date {
        days_since_epoch: i32,
    },
    TimeMicros {
        micros_since_midnight: i64,
    },
    TimestampMicros {
        micros_since_epoch: i64,
    },
    TimestamptzMicros {
        micros_since_epoch: i64,
    },
    TimestampNanos {
        nanos_since_epoch: i64,
    },
    TimestamptzNanos {
        nanos_since_epoch: i64,
    },
    Uuid([u8; 16]),
    Fixed {
        size: u64,
        bytes: Vec<u8>,
    },
    Struct(Vec<(String, ColumnDefault)>),
    Array(Vec<ColumnDefault>),
    Map(Vec<(ColumnDefault, ColumnDefault)>),
}

pub fn validate_column_default(value: &ColumnDefault) -> Result<(), String> {
    if matches!(value, ColumnDefault::Null) {
        return Err("top-level column default cannot be NULL".to_string());
    }
    validate_nested_column_default(value)
}

fn validate_nested_column_default(value: &ColumnDefault) -> Result<(), String> {
    match value {
        ColumnDefault::Fixed {
            size,
            bytes: fixed_bytes,
        } => {
            let byte_len = u64::try_from(fixed_bytes.len())
                .map_err(|_| "FIXED default byte length does not fit u64".to_string())?;
            if *size != byte_len {
                return Err(format!(
                    "FIXED default size {size} does not match byte length {byte_len}"
                ));
            }
        }
        ColumnDefault::Struct(fields) => {
            for (_, field_value) in fields {
                validate_nested_column_default(field_value)?;
            }
        }
        ColumnDefault::Array(elements) => {
            for element in elements {
                validate_nested_column_default(element)?;
            }
        }
        ColumnDefault::Map(entries) => {
            let mut keys = Vec::with_capacity(entries.len());
            for (index, (key, map_value)) in entries.iter().enumerate() {
                if matches!(key, ColumnDefault::Null) {
                    return Err("map key cannot be NULL".to_string());
                }
                if keys.contains(&key) {
                    return Err(format!("duplicate map key at index {index}"));
                }
                validate_nested_column_default(key)?;
                validate_nested_column_default(map_value)?;
                keys.push(key);
            }
        }
        ColumnDefault::Null
        | ColumnDefault::Boolean(_)
        | ColumnDefault::Int32(_)
        | ColumnDefault::Int64(_)
        | ColumnDefault::Float32 { .. }
        | ColumnDefault::Float64 { .. }
        | ColumnDefault::Decimal { .. }
        | ColumnDefault::String(_)
        | ColumnDefault::Binary(_)
        | ColumnDefault::Date { .. }
        | ColumnDefault::TimeMicros { .. }
        | ColumnDefault::TimestampMicros { .. }
        | ColumnDefault::TimestamptzMicros { .. }
        | ColumnDefault::TimestampNanos { .. }
        | ColumnDefault::TimestamptzNanos { .. }
        | ColumnDefault::Uuid(_) => {}
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SqlType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    LargeInt,
    Float,
    Double,
    Decimal {
        precision: u8,
        scale: i8,
    },
    String,
    Json,
    Binary,
    Bitmap,
    Hll,
    Boolean,
    Date,
    DateTime,
    /// Iceberg v3 nanosecond timestamp (`timestamp_ns`). Default DATETIME stays
    /// microsecond; this is a distinct variant so existing DATETIME behavior is
    /// untouched. Time zone (`timestamptz_ns`) is carried at the Arrow level on
    /// read/insert; native CREATE of the tz variant is out of scope.
    DateTimeNs,
    Time,
    Array(Box<SqlType>),
    Map(Box<SqlType>, Box<SqlType>),
    Struct(Vec<(String, SqlType)>),
    /// Iceberg v3 unshredded variant. Carried as Arrow `LargeBinary`
    /// in execution; persisted as a parquet group with `LogicalType::Variant`.
    Variant,
}

#[cfg(test)]
mod tests {
    use super::ColumnDefault;
    use super::SqlType;
    use super::validate_column_default;

    #[test]
    fn column_default_preserves_exact_variant_vocabulary() {
        let variants = vec![
            ColumnDefault::Null,
            ColumnDefault::Boolean(true),
            ColumnDefault::Int32(i32::MIN),
            ColumnDefault::Int64(i64::MAX),
            ColumnDefault::Float32 {
                bits: (-0.0_f32).to_bits(),
            },
            ColumnDefault::Float64 {
                bits: (-0.0_f64).to_bits(),
            },
            ColumnDefault::Decimal {
                unscaled: -123_456_789,
                precision: 38,
                scale: -2,
            },
            ColumnDefault::String("default".to_string()),
            ColumnDefault::Binary((0_u16..=255).map(|byte| byte as u8).collect()),
            ColumnDefault::Date {
                days_since_epoch: -1,
            },
            ColumnDefault::TimeMicros {
                micros_since_midnight: 86_399_999_999,
            },
            ColumnDefault::TimestampMicros {
                micros_since_epoch: -1,
            },
            ColumnDefault::TimestamptzMicros {
                micros_since_epoch: 1,
            },
            ColumnDefault::TimestampNanos {
                nanos_since_epoch: -1,
            },
            ColumnDefault::TimestamptzNanos {
                nanos_since_epoch: 1,
            },
            ColumnDefault::Uuid([
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ]),
            ColumnDefault::Fixed {
                size: 4,
                bytes: vec![0x00, 0x7f, 0x80, 0xff],
            },
            ColumnDefault::Struct(vec![
                ("first".to_string(), ColumnDefault::Null),
                ("second".to_string(), ColumnDefault::Int32(2)),
            ]),
            ColumnDefault::Array(vec![ColumnDefault::Null, ColumnDefault::Boolean(false)]),
            ColumnDefault::Map(vec![
                (
                    ColumnDefault::String("first".to_string()),
                    ColumnDefault::Int64(1),
                ),
                (
                    ColumnDefault::String("second".to_string()),
                    ColumnDefault::Null,
                ),
            ]),
        ];

        assert_eq!(variants.len(), 20);
        assert_eq!(variants.clone(), variants);
    }

    #[test]
    fn column_default_preserves_float_bits_binary_and_nested_order() {
        let float_patterns = [
            ColumnDefault::Float32 {
                bits: (-0.0_f32).to_bits(),
            },
            ColumnDefault::Float32 { bits: 0x7fc0_1234 },
            ColumnDefault::Float64 {
                bits: (-0.0_f64).to_bits(),
            },
            ColumnDefault::Float64 {
                bits: 0x7ff8_0000_0000_1234,
            },
        ];
        assert_ne!(float_patterns[0], ColumnDefault::Float32 { bits: 0 });
        assert_ne!(float_patterns[2], ColumnDefault::Float64 { bits: 0 });

        let binary = ColumnDefault::Binary((0_u16..=255).map(|byte| byte as u8).collect());
        assert_eq!(
            binary,
            ColumnDefault::Binary((0_u16..=255).map(|byte| byte as u8).collect())
        );

        let nested = ColumnDefault::Struct(vec![
            (
                "outer".to_string(),
                ColumnDefault::Array(vec![ColumnDefault::Map(vec![
                    (
                        ColumnDefault::String("first".to_string()),
                        ColumnDefault::Null,
                    ),
                    (
                        ColumnDefault::String("second".to_string()),
                        ColumnDefault::Struct(vec![
                            ("x".to_string(), ColumnDefault::Int32(1)),
                            ("y".to_string(), ColumnDefault::Boolean(true)),
                        ]),
                    ),
                ])]),
            ),
            (
                "decimal".to_string(),
                ColumnDefault::Decimal {
                    unscaled: 12_345,
                    precision: 10,
                    scale: 2,
                },
            ),
        ]);
        assert_eq!(nested.clone(), nested);
    }

    #[test]
    fn column_default_validation_rejects_only_structural_invalidity() {
        assert_eq!(
            validate_column_default(&ColumnDefault::Null).unwrap_err(),
            "top-level column default cannot be NULL"
        );
        assert!(
            validate_column_default(&ColumnDefault::Fixed {
                size: 2,
                bytes: vec![1],
            })
            .unwrap_err()
            .contains("does not match")
        );
        assert_eq!(
            validate_column_default(&ColumnDefault::Map(vec![(
                ColumnDefault::Null,
                ColumnDefault::Int32(1),
            )]))
            .unwrap_err(),
            "map key cannot be NULL"
        );
        assert!(
            validate_column_default(&ColumnDefault::Map(vec![
                (
                    ColumnDefault::String("key".to_string()),
                    ColumnDefault::Null
                ),
                (
                    ColumnDefault::String("key".to_string()),
                    ColumnDefault::Int32(1),
                ),
            ]))
            .unwrap_err()
            .contains("duplicate map key")
        );

        validate_column_default(&ColumnDefault::Struct(vec![
            ("nullable".to_string(), ColumnDefault::Null),
            (
                "negative_scale".to_string(),
                ColumnDefault::Decimal {
                    unscaled: 1,
                    precision: 10,
                    scale: -1,
                },
            ),
            (
                "non_finite".to_string(),
                ColumnDefault::Float64 {
                    bits: f64::NAN.to_bits(),
                },
            ),
        ]))
        .unwrap();
    }

    #[test]
    fn sql_type_preserves_exact_variant_vocabulary() {
        let variants = vec![
            SqlType::TinyInt,
            SqlType::SmallInt,
            SqlType::Int,
            SqlType::BigInt,
            SqlType::LargeInt,
            SqlType::Float,
            SqlType::Double,
            SqlType::Decimal {
                precision: 38,
                scale: -2,
            },
            SqlType::String,
            SqlType::Json,
            SqlType::Binary,
            SqlType::Bitmap,
            SqlType::Hll,
            SqlType::Boolean,
            SqlType::Date,
            SqlType::DateTime,
            SqlType::DateTimeNs,
            SqlType::Time,
            SqlType::Array(Box::new(SqlType::Int)),
            SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::BigInt)),
            SqlType::Struct(vec![("value".to_string(), SqlType::Boolean)]),
            SqlType::Variant,
        ];

        assert_eq!(variants.len(), 22);
        assert_eq!(variants.clone(), variants);

        let nested = SqlType::Array(Box::new(SqlType::Map(
            Box::new(SqlType::String),
            Box::new(SqlType::Struct(vec![
                ("x".to_string(), SqlType::DateTimeNs),
                ("v".to_string(), SqlType::Variant),
            ])),
        )));
        assert_eq!(
            nested,
            SqlType::Array(Box::new(SqlType::Map(
                Box::new(SqlType::String),
                Box::new(SqlType::Struct(vec![
                    ("x".to_string(), SqlType::DateTimeNs),
                    ("v".to_string(), SqlType::Variant),
                ])),
            )))
        );
        assert_eq!(
            format!("{nested:?}"),
            "Array(Map(String, Struct([(\"x\", DateTimeNs), (\"v\", Variant)])))"
        );
    }
}

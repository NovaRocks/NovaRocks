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

use arrow::datatypes::DataType;

use novarocks_catalog::schema::SqlType;

pub(crate) fn arrow_data_type_to_sql_type(data_type: &DataType) -> Result<SqlType, String> {
    match data_type {
        DataType::Boolean => Ok(SqlType::Boolean),
        DataType::Int8 => Ok(SqlType::TinyInt),
        DataType::Int16 => Ok(SqlType::SmallInt),
        DataType::Int32 => Ok(SqlType::Int),
        DataType::Int64 => Ok(SqlType::BigInt),
        DataType::Float32 => Ok(SqlType::Float),
        DataType::Float64 => Ok(SqlType::Double),
        DataType::Utf8 => Ok(SqlType::String),
        DataType::Binary => Ok(SqlType::Binary),
        DataType::Date32 => Ok(SqlType::Date),
        DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => Ok(SqlType::DateTimeNs),
        DataType::Timestamp(_, _) => Ok(SqlType::DateTime),
        DataType::Time64(_) => Ok(SqlType::Time),
        DataType::FixedSizeBinary(width)
            if *width == novarocks_types::largeint::LARGEINT_BYTE_WIDTH =>
        {
            Ok(SqlType::LargeInt)
        }
        DataType::Decimal128(precision, scale) => Ok(SqlType::Decimal {
            precision: *precision,
            scale: *scale,
        }),
        DataType::List(field) => Ok(SqlType::Array(Box::new(arrow_data_type_to_sql_type(
            field.data_type(),
        )?))),
        DataType::Struct(fields) => Ok(SqlType::Struct(
            fields
                .iter()
                .map(|field| {
                    Ok((
                        field.name().clone(),
                        arrow_data_type_to_sql_type(field.data_type())?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?,
        )),
        DataType::Map(entries, _) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err("MAP output type must use struct entries".to_string());
            };
            let (_, key) = fields
                .find("key")
                .ok_or_else(|| "MAP output type is missing key field".to_string())?;
            let (_, value) = fields
                .find("value")
                .ok_or_else(|| "MAP output type is missing value field".to_string())?;
            Ok(SqlType::Map(
                Box::new(arrow_data_type_to_sql_type(key.data_type())?),
                Box::new(arrow_data_type_to_sql_type(value.data_type())?),
            ))
        }
        other => Err(format!("unsupported MV output type: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Fields, TimeUnit};

    use super::arrow_data_type_to_sql_type;
    use novarocks_catalog::schema::SqlType;

    #[test]
    fn arrow_data_type_to_sql_type_preserves_scalar_contract() {
        let cases = [
            (DataType::Boolean, SqlType::Boolean),
            (DataType::Int8, SqlType::TinyInt),
            (DataType::Int16, SqlType::SmallInt),
            (DataType::Int32, SqlType::Int),
            (DataType::Int64, SqlType::BigInt),
            (
                DataType::FixedSizeBinary(novarocks_types::largeint::LARGEINT_BYTE_WIDTH),
                SqlType::LargeInt,
            ),
            (
                DataType::Decimal128(38, -2),
                SqlType::Decimal {
                    precision: 38,
                    scale: -2,
                },
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                SqlType::DateTimeNs,
            ),
        ];

        for (arrow_type, expected) in cases {
            assert_eq!(
                arrow_data_type_to_sql_type(&arrow_type).expect("supported scalar type"),
                expected,
                "Arrow type {arrow_type:?}"
            );
        }
    }

    #[test]
    fn arrow_data_type_to_sql_type_preserves_nested_shape_and_order() {
        let map_entries = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new("key", DataType::Utf8, false)),
            Arc::new(Field::new("value", DataType::Decimal128(20, -3), true)),
        ]));
        let nested = DataType::Struct(Fields::from(vec![
            Arc::new(Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            )),
            Arc::new(Field::new(
                "attrs",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Map(Arc::new(Field::new("entries", map_entries, false)), false),
                    true,
                ))),
                true,
            )),
        ]));

        assert_eq!(
            arrow_data_type_to_sql_type(&nested).expect("supported nested type"),
            SqlType::Struct(vec![
                ("ts".to_string(), SqlType::DateTimeNs),
                (
                    "attrs".to_string(),
                    SqlType::Array(Box::new(SqlType::Map(
                        Box::new(SqlType::String),
                        Box::new(SqlType::Decimal {
                            precision: 20,
                            scale: -3,
                        }),
                    ))),
                ),
            ])
        );
    }

    #[test]
    fn arrow_data_type_to_sql_type_rejects_null_exactly() {
        assert_eq!(
            arrow_data_type_to_sql_type(&DataType::Null),
            Err("unsupported MV output type: Null".to_string())
        );
    }
}

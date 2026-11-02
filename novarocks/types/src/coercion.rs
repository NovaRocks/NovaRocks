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

use std::sync::Arc;

use arrow_schema::{DataType, Field, Fields};

pub use novarocks_type_contract::{comparison_common_type, decimal_compare_type, wider_type};

/// The same type with a provider's own decoration taken off its nested fields.
///
/// A plan's value type is a logical SQL type: a list of a type, a map of two,
/// a struct of named types. Which Parquet field a nested column came from is
/// the provider's fact about its read -- Iceberg hands it over on its column
/// handle, and Trino likewise keeps field ids out of `io.trino.spi.type.Type`
/// -- and a plan that repeated it would make the same value two different
/// types depending on where it was read, disagreeing with every function
/// signature it is fed to.
///
/// So a nested field keeps only what the type says: a list's element is
/// `item`, a map's entries are `entries` of `key` and `value`, struct fields
/// keep the names the statement gave them, and only logical identity metadata
/// survives.
/// What each field admits is left exactly as it was: nullability is a fact
/// about the values, not decoration.
pub fn undecorated_nested_type(data_type: &DataType) -> DataType {
    fn field(name: &str, source: &Field) -> Arc<Field> {
        let field = Field::new(
            name,
            undecorated_nested_type(source.data_type()),
            source.is_nullable(),
        );
        // A field's logical type says what it is -- a JSON string is not a
        // string -- so it survives; everything else in the metadata is the
        // provider's bookkeeping about where the column came from.
        match source.metadata().get(crate::logical::NR_LOGICAL_TYPE_KEY) {
            Some(logical) => Arc::new(field.with_metadata(std::collections::HashMap::from([(
                crate::logical::NR_LOGICAL_TYPE_KEY.to_string(),
                logical.clone(),
            )]))),
            None => Arc::new(field),
        }
    }
    match data_type {
        DataType::List(element) => DataType::List(field("item", element)),
        DataType::LargeList(element) => DataType::LargeList(field("item", element)),
        DataType::FixedSizeList(element, len) => {
            DataType::FixedSizeList(field("item", element), *len)
        }
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|nested| field(nested.name(), nested))
                .collect::<Vec<_>>(),
        )),
        DataType::Map(entries, sorted) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return data_type.clone();
            };
            if fields.len() != 2 {
                return data_type.clone();
            }
            DataType::Map(
                Arc::new(with_invalid_container_marker(
                    Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            field("key", &fields[0]),
                            field("value", &fields[1]),
                        ])),
                        entries.is_nullable(),
                    ),
                    entries
                        .metadata()
                        .contains_key(crate::logical::NR_LOGICAL_TYPE_KEY),
                )),
                *sorted,
            )
        }
        other => other.clone(),
    }
}

fn with_invalid_container_marker(field: Field, has_marker: bool) -> Field {
    // The Map entries container cannot own any scalar logical domain. Keep a
    // bounded rejection witness through normalization instead of erasing it.
    if has_marker {
        field.with_metadata(std::collections::HashMap::from([(
            crate::logical::NR_LOGICAL_TYPE_KEY.to_owned(),
            "invalid".to_owned(),
        )]))
    } else {
        field
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, Field, Fields};
    use std::sync::Arc;

    #[test]
    fn comparison_common_type_numeric_and_decimal() {
        // equal -> Ok(None)
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Int32).unwrap(),
            None
        );
        // int width mismatch -> both Int64
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Int64).unwrap(),
            Some(DataType::Int64)
        );
        assert_eq!(
            comparison_common_type(&DataType::Int16, &DataType::Int8).unwrap(),
            Some(DataType::Int64)
        );
        // int <-> float / float x float -> both Float64
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Float64).unwrap(),
            Some(DataType::Float64)
        );
        assert_eq!(
            comparison_common_type(&DataType::Float32, &DataType::Float64).unwrap(),
            Some(DataType::Float64)
        );
        // decimal x decimal <=38 -> common Decimal128
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Decimal128(18, 4))
                .unwrap(),
            Some(DataType::Decimal128(18, 4))
        );
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Decimal128(10, 2))
                .unwrap(),
            None
        );
        // int x decimal: int modeled as zero-scale decimal -> common decimal
        // Int32 -> Decimal128(10,0); scale=2, int_digits=max(10,8)=10, prec=12
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Decimal128(10, 2)).unwrap(),
            Some(DataType::Decimal128(12, 2))
        );
        // string x numeric -> numeric operand's type
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Int32).unwrap(),
            Some(DataType::Int32)
        );
    }

    #[test]
    fn comparison_common_type_new_arms_s3() {
        assert_eq!(
            comparison_common_type(&DataType::Int64, &DataType::Decimal128(10, 2)).unwrap(),
            Some(DataType::Decimal128(21, 2))
        );
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Int32).unwrap(),
            Some(DataType::Decimal128(12, 2))
        );
        assert_eq!(
            comparison_common_type(&DataType::Float64, &DataType::Decimal128(10, 2)).unwrap(),
            Some(DataType::Float64)
        );
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Float32).unwrap(),
            Some(DataType::Float64)
        );
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Int32).unwrap(),
            Some(DataType::Int32)
        );
        assert_eq!(
            comparison_common_type(&DataType::LargeUtf8, &DataType::Int32).unwrap(),
            Some(DataType::Int32)
        );
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(10, 2), &DataType::Utf8).unwrap(),
            Some(DataType::Decimal128(10, 2))
        );
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Float64).unwrap(),
            Some(DataType::Float64)
        );
        let largeint = DataType::FixedSizeBinary(crate::largeint::LARGEINT_BYTE_WIDTH);
        assert_eq!(
            comparison_common_type(&largeint, &DataType::Decimal128(10, 2)).unwrap(),
            None
        );
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Utf8).unwrap(),
            None
        );
        assert_eq!(
            comparison_common_type(&DataType::Utf8, &DataType::Date32).unwrap(),
            None
        );
    }

    #[test]
    fn comparison_common_type_recurses_into_complex_shapes() {
        assert_eq!(
            comparison_common_type(
                &DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                &DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Decimal128(10, 2),
                    true
                ))),
            )
            .unwrap(),
            Some(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Decimal128(12, 2),
                true
            ))))
        );

        assert_eq!(
            comparison_common_type(
                &DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                &DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Decimal128(10, 2),
                    true
                ))),
            )
            .unwrap(),
            Some(DataType::List(Arc::new(Field::new(
                "item",
                DataType::Utf8,
                true
            ))))
        );

        assert_eq!(
            comparison_common_type(
                &DataType::Struct(Fields::from(vec![
                    Field::new("c0", DataType::Decimal128(16, 3), true),
                    Field::new("c1", DataType::Utf8, true),
                ])),
                &DataType::Struct(Fields::from(vec![
                    Field::new("c0", DataType::Int32, true),
                    Field::new("c1", DataType::Utf8, true),
                ])),
            )
            .unwrap(),
            Some(DataType::Struct(Fields::from(vec![
                Field::new("c0", DataType::Decimal128(16, 3), true),
                Field::new("c1", DataType::Utf8, true),
            ])))
        );

        let decimal_map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Decimal128(16, 3), false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        );
        let int_map = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new("value", DataType::Utf8, true),
                ])),
                false,
            )),
            false,
        );
        assert_eq!(
            comparison_common_type(&decimal_map, &int_map).unwrap(),
            Some(decimal_map)
        );
    }

    #[test]
    fn comparison_common_type_boolean_null_and_largeint_edges() {
        let largeint = DataType::FixedSizeBinary(crate::largeint::LARGEINT_BYTE_WIDTH);

        assert_eq!(
            comparison_common_type(&DataType::Boolean, &DataType::Int64).unwrap(),
            Some(DataType::Int64)
        );
        assert_eq!(
            comparison_common_type(&DataType::Int32, &DataType::Boolean).unwrap(),
            Some(DataType::Int64)
        );
        assert_eq!(
            comparison_common_type(&DataType::Boolean, &DataType::Float64).unwrap(),
            Some(DataType::Float64)
        );
        assert_eq!(
            comparison_common_type(&DataType::Boolean, &DataType::Decimal128(10, 2)).unwrap(),
            Some(DataType::Decimal128(10, 2))
        );
        assert_eq!(
            comparison_common_type(&DataType::Int64, &DataType::Null).unwrap(),
            Some(DataType::Int64)
        );
        assert_eq!(
            comparison_common_type(&DataType::Null, &DataType::Boolean).unwrap(),
            Some(DataType::Boolean)
        );
        assert_eq!(
            comparison_common_type(&largeint, &DataType::Int64).unwrap(),
            Some(largeint.clone())
        );
        assert_eq!(
            comparison_common_type(&DataType::Int32, &largeint).unwrap(),
            Some(largeint)
        );
    }

    #[test]
    fn comparison_common_type_decimal_overflow_promotes_to_256() {
        // scale=max(0,10)=10, int_digits=max(30-0,30-10)=30, precision=40 > 38 -> Decimal256
        assert_eq!(
            comparison_common_type(&DataType::Decimal128(30, 0), &DataType::Decimal128(30, 10))
                .unwrap(),
            Some(DataType::Decimal256(40, 10))
        );
        // either side already Decimal256 -> Decimal256
        assert_eq!(
            comparison_common_type(&DataType::Decimal256(40, 2), &DataType::Decimal128(10, 2))
                .unwrap(),
            Some(DataType::Decimal256(40, 2))
        );
    }

    #[test]
    fn comparison_common_type_decimal_overflow_beyond_256_errs() {
        // precision > 76 -> Err
        let err =
            comparison_common_type(&DataType::Decimal256(76, 0), &DataType::Decimal256(76, 38));
        let err = err.expect_err("expected overflow Err");
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        assert_eq!(err.failure_fact().class(), FailureClass::Representation);
        assert_eq!(
            err.failure_fact().representation().unwrap().required,
            Some(114)
        );
        assert!(
            err.to_string().contains("precision overflow"),
            "expected precision overflow Err, got {err}"
        );
    }

    #[test]
    fn wider_type_decimal_vs_float64_returns_float64() {
        let result = wider_type(&DataType::Decimal128(7, 2), &DataType::Float64);
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn public_carrier_api_uses_the_neutral_owner() {
        let cases = [
            (
                DataType::Decimal128(38, 0),
                DataType::Decimal128(38, 20),
                DataType::Decimal256(58, 20),
            ),
            (
                DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            ),
        ];
        for (left, right, expected) in cases {
            assert_eq!(crate::wider_type(&left, &right), expected);
            assert_eq!(novarocks_type_contract::wider_type(&left, &right), expected);
        }
    }

    #[test]
    fn wider_type_float32_vs_decimal_returns_float64() {
        let result = wider_type(&DataType::Float32, &DataType::Decimal128(18, 6));
        assert_eq!(result, DataType::Float64);
    }

    #[test]
    fn mixed_decimal_integer_common_type_covers_the_integer_range() {
        for (integer, precision) in [
            (DataType::Int8, 4),
            (DataType::Int16, 6),
            (DataType::Int32, 11),
            (DataType::Int64, 20),
        ] {
            let decimal = DataType::Decimal128(2, 1);
            let expected = DataType::Decimal128(precision, 1);
            assert_eq!(wider_type(&decimal, &integer), expected);
            assert_eq!(wider_type(&integer, &decimal), expected);
        }
        assert_eq!(
            wider_type(&DataType::Decimal128(38, 37), &DataType::Int64),
            DataType::Decimal256(56, 37)
        );
    }

    #[test]
    fn wider_type_preserves_decimal256_with_integer_fallback() {
        let wide = DataType::Decimal256(39, 9);

        assert_eq!(wider_type(&wide, &DataType::Int64), wide);
        assert_eq!(wider_type(&DataType::Int64, &wide), wide);
        assert_eq!(
            wider_type(&DataType::Decimal128(38, 9), &DataType::Decimal256(39, 9)),
            DataType::Decimal256(39, 9)
        );
        assert_eq!(wider_type(&wide, &DataType::Float64), DataType::Float64);
    }

    #[test]
    fn wider_type_string_vs_decimal_returns_string() {
        let result = wider_type(&DataType::Utf8, &DataType::Decimal128(26, 2));
        assert_eq!(result, DataType::Utf8);
    }

    #[test]
    fn wider_type_array_string_vs_decimal_returns_array_string() {
        let left = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        let right = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Decimal128(26, 2),
            true,
        )));

        let result = wider_type(&left, &right);
        let DataType::List(item) = result else {
            panic!("expected array type");
        };
        assert_eq!(item.data_type(), &DataType::Utf8);
    }

    #[test]
    fn wider_type_promotes_map_key_and_value_types() {
        let left = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Null, true)),
                        Arc::new(Field::new("value", DataType::Null, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let right = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Arc::new(Field::new("key", DataType::Int64, true)),
                        Arc::new(Field::new("value", DataType::Int64, true)),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );

        let widened = wider_type(&left, &right);
        let DataType::Map(entries, _) = widened else {
            panic!("expected map type");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected entries struct");
        };
        assert_eq!(fields[0].data_type(), &DataType::Int64);
        assert_eq!(fields[1].data_type(), &DataType::Int64);
    }
}

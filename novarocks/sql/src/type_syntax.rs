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

//! SQL type syntax for the existing DML declaration projection.
//!
//! Value domains and layout-to-declaration choices remain owned by `types`.
//! This author only expresses that projection in the parser's syntax tree.

use std::{error::Error, fmt};

use novarocks_parser::{SYNTHETIC, ast, build};
use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
use novarocks_types::{ColumnValueTypeError, schema::SqlType, sql_type_from_value_type};

/// A declaration rejection or construction defect, without a parser re-entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TypeSyntaxError {
    Declaration(ColumnValueTypeError),
    Construction(build::BuildError),
    Uuid,
    OpaqueDomain(ValueLogicalType),
    NegativeDecimalScale { precision: u8, scale: i8 },
    EmptyStruct,
}

impl fmt::Display for TypeSyntaxError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Declaration(error) => error.fmt(formatter),
            Self::Construction(error) => error.fmt(formatter),
            Self::Uuid => formatter.write_str("UUID has no SQL DDL type spelling"),
            Self::OpaqueDomain(_) => formatter
                .write_str("internal opaque value domains have no public Iceberg SQL type name"),
            Self::NegativeDecimalScale { precision, scale } => write!(
                formatter,
                "negative decimal scale has no SQL type spelling: DECIMAL({precision}, {scale})"
            ),
            Self::EmptyStruct => formatter.write_str("empty STRUCT has no SQL type spelling"),
        }
    }
}

impl Error for TypeSyntaxError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Declaration(error) => Some(error),
            Self::Construction(error) => Some(error),
            _ => None,
        }
    }
}

impl From<ColumnValueTypeError> for TypeSyntaxError {
    fn from(error: ColumnValueTypeError) -> Self {
        Self::Declaration(error)
    }
}

/// Construct the existing DML SQL type spelling from the complete value type.
///
/// The original `types` projection deliberately loses layout details such as
/// fixed binary width, list offset width and timestamp unit distinctions.
/// This function preserves that contract rather than inventing a new mapping.
pub fn sql_type_syntax(value: &FunctionValueType) -> Result<ast::TypeName, TypeSyntaxError> {
    declaration_syntax(&sql_type_from_value_type(value)?)
}

fn declaration_syntax(declaration: &SqlType) -> Result<ast::TypeName, TypeSyntaxError> {
    let scalar = match declaration {
        SqlType::TinyInt => "TINYINT",
        SqlType::SmallInt => "SMALLINT",
        SqlType::Int => "INT",
        SqlType::BigInt => "BIGINT",
        SqlType::LargeInt => "LARGEINT",
        SqlType::Float => "FLOAT",
        SqlType::Double => "DOUBLE",
        SqlType::String => "STRING",
        SqlType::Json => "JSON",
        SqlType::Binary => "VARBINARY",
        SqlType::Bitmap => "BITMAP",
        SqlType::Hll => "HLL",
        SqlType::Boolean => "BOOLEAN",
        SqlType::Date => "DATE",
        SqlType::DateTime => "DATETIME",
        SqlType::DateTimeNs => "DATETIME_NS",
        SqlType::Time => "TIME",
        SqlType::Variant => "VARIANT",
        SqlType::Uuid => return Err(TypeSyntaxError::Uuid),
        SqlType::Object => return Err(TypeSyntaxError::OpaqueDomain(ValueLogicalType::Object)),
        SqlType::Percentile => {
            return Err(TypeSyntaxError::OpaqueDomain(ValueLogicalType::Percentile));
        }
        SqlType::Decimal { precision, scale } => {
            if *scale < 0 {
                return Err(TypeSyntaxError::NegativeDecimalScale {
                    precision: *precision,
                    scale: *scale,
                });
            }
            return parameterized_type(
                "DECIMAL",
                vec![number_argument(*precision), number_argument(*scale as u8)],
            );
        }
        SqlType::Array(item) => {
            return parameterized_type(
                "ARRAY",
                vec![ast::TypeNameArgument::Type(declaration_syntax(item)?)],
            );
        }
        SqlType::Map(key, value) => {
            return parameterized_type(
                "MAP",
                vec![
                    ast::TypeNameArgument::Type(declaration_syntax(key)?),
                    ast::TypeNameArgument::Type(declaration_syntax(value)?),
                ],
            );
        }
        SqlType::Struct(fields) => {
            if fields.is_empty() {
                return Err(TypeSyntaxError::EmptyStruct);
            }
            let arguments = fields
                .iter()
                .map(|(name, declaration)| {
                    Ok(ast::TypeNameArgument::Field(ast::StructField {
                        name: build::ident(name, Some('`')),
                        data_type: declaration_syntax(declaration)?,
                        span: SYNTHETIC,
                    }))
                })
                .collect::<Result<Vec<_>, TypeSyntaxError>>()?;
            return parameterized_type("STRUCT", arguments);
        }
    };
    Ok(build::simple_type(scalar))
}

fn number_argument(value: u8) -> ast::TypeNameArgument {
    ast::TypeNameArgument::Literal(ast::Literal {
        kind: ast::LiteralKind::Number(value.to_string()),
        span: SYNTHETIC,
    })
}

fn parameterized_type(
    name: &str,
    arguments: Vec<ast::TypeNameArgument>,
) -> Result<ast::TypeName, TypeSyntaxError> {
    let separators = vec![true; arguments.len().saturating_sub(1)];
    build::type_name(build::simple_type(name).name, arguments, separators)
        .map_err(TypeSyntaxError::Construction)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, TimeUnit};
    use novarocks_parser::ast::SyntaxEq;
    use novarocks_type_contract::ValueTypeStorageLifetime;

    use super::*;

    fn assert_spelling(value: FunctionValueType, spelling: &str) {
        let constructed = sql_type_syntax(&value).unwrap();
        // The expected spelling is fixed independently of the syntax author.
        // Parser entry exists only in this unit-test oracle.
        let statements =
            novarocks_parser::parse(&format!("SELECT CAST(NULL AS {spelling})")).unwrap();
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query");
        };
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("SELECT");
        };
        let ast::SelectItem::UnnamedExpr(ast::Expr::Cast(cast)) = &select.projection[0] else {
            panic!("CAST");
        };
        assert!(constructed.syntax_eq(&cast.data_type), "{spelling}");
    }

    fn declared(declaration: SqlType, carrier: DataType) -> FunctionValueType {
        FunctionValueType::try_from_typed_binding(
            novarocks_types::execution_value_type_from_declaration(
                &declaration,
                &carrier,
                true,
                ValueTypeStorageLifetime::pure(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn scalar_syntax_preserves_the_existing_declaration_projection() {
        for (carrier, spelling) in [
            (DataType::Boolean, "BOOLEAN"),
            (DataType::Int8, "TINYINT"),
            (DataType::Int16, "SMALLINT"),
            (DataType::Int32, "INT"),
            (DataType::Int64, "BIGINT"),
            (DataType::Float32, "FLOAT"),
            (DataType::Float64, "DOUBLE"),
            (DataType::Decimal128(18, 2), "DECIMAL(18, 2)"),
            (DataType::Decimal256(76, 20), "DECIMAL(76, 20)"),
            (DataType::Utf8, "STRING"),
            (DataType::LargeUtf8, "STRING"),
            (DataType::Utf8View, "STRING"),
            (DataType::Binary, "VARBINARY"),
            (DataType::LargeBinary, "VARBINARY"),
            (DataType::BinaryView, "VARBINARY"),
            (DataType::FixedSizeBinary(7), "VARBINARY"),
            (DataType::Date32, "DATE"),
            (DataType::Timestamp(TimeUnit::Second, None), "DATETIME"),
            (DataType::Timestamp(TimeUnit::Millisecond, None), "DATETIME"),
            (DataType::Timestamp(TimeUnit::Microsecond, None), "DATETIME"),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                "DATETIME_NS",
            ),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                "DATETIME_NS",
            ),
            (DataType::Time64(TimeUnit::Microsecond), "TIME"),
            (DataType::Time64(TimeUnit::Nanosecond), "TIME"),
        ] {
            assert_spelling(FunctionValueType::new(carrier, true), spelling);
        }
    }

    #[test]
    fn nominal_syntax_uses_authored_domains() {
        for (domain, carrier, spelling) in [
            (ValueLogicalType::Json, DataType::Utf8, "JSON"),
            (ValueLogicalType::Variant, DataType::LargeBinary, "VARIANT"),
            (ValueLogicalType::Hll, DataType::Binary, "HLL"),
            (ValueLogicalType::Bitmap, DataType::Binary, "BITMAP"),
            (
                ValueLogicalType::LargeInt,
                DataType::FixedSizeBinary(16),
                "LARGEINT",
            ),
        ] {
            assert_spelling(
                FunctionValueType::try_with_logical_type(carrier, true, domain).unwrap(),
                spelling,
            );
        }
    }

    fn map_type(key: DataType, value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", key, false),
                        Field::new("value", value, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        )
    }

    #[test]
    fn collection_syntax_preserves_nesting_field_quotes_and_separator_spaces() {
        for carrier in [
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true))),
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, true)), 3),
        ] {
            assert_spelling(FunctionValueType::new(carrier, true), "ARRAY<INT>");
        }
        assert_spelling(
            FunctionValueType::new(map_type(DataType::Int64, DataType::Utf8), true),
            "MAP<BIGINT, STRING>",
        );
        let list = DataType::List(Arc::new(Field::new("item", DataType::Boolean, true)));
        let structure = DataType::Struct(
            vec![
                Field::new("a`b", DataType::Int32, true),
                Field::new("space name", map_type(DataType::Utf8, list), true),
            ]
            .into(),
        );
        assert_spelling(
            FunctionValueType::new(structure, true),
            "STRUCT<`a``b` INT, `space name` MAP<STRING, ARRAY<BOOLEAN>>>",
        );
    }

    #[test]
    fn nested_nominal_syntax_retains_complete_child_declarations() {
        let declaration = SqlType::Struct(vec![
            ("documents".into(), SqlType::Array(Box::new(SqlType::Json))),
            (
                "lookup".into(),
                SqlType::Map(
                    Box::new(SqlType::String),
                    Box::new(SqlType::Struct(vec![("value".into(), SqlType::Variant)])),
                ),
            ),
        ]);
        let carrier = DataType::Struct(
            vec![
                Field::new(
                    "documents",
                    DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ),
                Field::new(
                    "lookup",
                    map_type(
                        DataType::Utf8,
                        DataType::Struct(
                            vec![Field::new("value", DataType::LargeBinary, true)].into(),
                        ),
                    ),
                    true,
                ),
            ]
            .into(),
        );
        assert_spelling(
            declared(declaration, carrier),
            "STRUCT<`documents` ARRAY<JSON>, `lookup` MAP<STRING, STRUCT<`value` VARIANT>>>",
        );
    }

    #[test]
    fn domains_without_public_spellings_keep_their_original_rejections() {
        for (domain, carrier, expected) in [
            (
                ValueLogicalType::Uuid,
                DataType::FixedSizeBinary(16),
                TypeSyntaxError::Uuid,
            ),
            (
                ValueLogicalType::Object,
                DataType::Binary,
                TypeSyntaxError::OpaqueDomain(ValueLogicalType::Object),
            ),
            (
                ValueLogicalType::Percentile,
                DataType::Binary,
                TypeSyntaxError::OpaqueDomain(ValueLogicalType::Percentile),
            ),
        ] {
            let value = FunctionValueType::try_with_logical_type(carrier, true, domain).unwrap();
            let error = sql_type_syntax(&value).unwrap_err();
            assert_eq!(error, expected);
            assert_eq!(
                error.to_string(),
                if domain == ValueLogicalType::Uuid {
                    "UUID has no SQL DDL type spelling"
                } else {
                    "internal opaque value domains have no public Iceberg SQL type name"
                }
            );
        }
    }

    #[test]
    fn previously_unparseable_declarations_are_rejected_by_type() {
        for carrier in [DataType::Decimal128(18, -2), DataType::Decimal256(76, -2)] {
            let error =
                sql_type_syntax(&FunctionValueType::new(carrier.clone(), true)).unwrap_err();
            let precision = match carrier {
                DataType::Decimal128(precision, _) | DataType::Decimal256(precision, _) => {
                    precision
                }
                _ => unreachable!(),
            };
            assert_eq!(
                error,
                TypeSyntaxError::NegativeDecimalScale {
                    precision,
                    scale: -2
                }
            );
            assert!(
                novarocks_parser::parse(&format!("SELECT CAST(NULL AS DECIMAL({precision}, -2))"))
                    .is_err()
            );
        }
        assert_eq!(
            sql_type_syntax(&FunctionValueType::new(
                DataType::Struct(Vec::<Field>::new().into()),
                true
            )),
            Err(TypeSyntaxError::EmptyStruct)
        );
        assert!(novarocks_parser::parse("SELECT CAST(NULL AS STRUCT<>)").is_err());
    }

    #[test]
    fn declaration_projection_failure_keeps_its_original_typed_cause() {
        let error = sql_type_syntax(&FunctionValueType::new(DataType::UInt64, true)).unwrap_err();
        let TypeSyntaxError::Declaration(original) = &error else {
            panic!("declaration failure");
        };
        assert_eq!(
            original,
            &ColumnValueTypeError::UnrepresentableCarrier(DataType::UInt64)
        );
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<ColumnValueTypeError>(),
            Some(original)
        );
        assert_eq!(error.to_string(), original.to_string());
    }
}

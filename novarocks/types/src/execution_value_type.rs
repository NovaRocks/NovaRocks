// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Explicit SQL declaration author for recursive execution value semantics.
//!
//! The declaration authors logical domains and named Struct children. Arrow
//! contributes only exact layout parameters and child NULL contracts. Provider
//! annotations are neither a semantic source nor a fallback in this author.

use std::fmt;

use arrow_schema::{DataType, TimeUnit};
use novarocks_type_contract::{
    ExecutionValueTypeError, MAX_VALUE_TYPE_DEPTH, MAX_VALUE_TYPE_NODES, TypedValueBinding,
    ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef, ValueTypeStorageLifetime,
};

use crate::schema::SqlType;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionColumnTypeError {
    DeclarationMismatch,
    UnrepresentableValue(&'static str),
    UnrepresentableCarrier(DataType),
    Type(ExecutionValueTypeError),
}

impl From<ExecutionValueTypeError> for ExecutionColumnTypeError {
    fn from(error: ExecutionValueTypeError) -> Self {
        Self::Type(error)
    }
}

impl fmt::Display for ExecutionColumnTypeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeclarationMismatch => {
                formatter.write_str("SQL declaration conflicts with its execution value layout")
            }
            Self::UnrepresentableValue(reason) => {
                write!(
                    formatter,
                    "SQL declaration cannot represent execution value: {reason}"
                )
            }
            Self::UnrepresentableCarrier(carrier) => {
                write!(
                    formatter,
                    "SQL declaration cannot represent the {carrier:?} carrier"
                )
            }
            Self::Type(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for ExecutionColumnTypeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DeclarationMismatch
            | Self::UnrepresentableValue(_)
            | Self::UnrepresentableCarrier(_) => None,
            Self::Type(error) => Some(error),
        }
    }
}

/// Bind an explicit declaration to a borrowed exact carrier structure.
///
/// This is an authoring entry point, not a repair path for untyped execution
/// data. Every child receives its NULL contract from the corresponding Arrow
/// field. List/Map role labels are canonicalized by the layout contract; SQL
/// Struct names and order must already agree. The supplied lifetime covers
/// all newly constructed semantic and layout storage, including retained
/// child references. Construction grants no production carrier capability.
pub fn execution_value_type_from_declaration(
    declaration: &SqlType,
    carrier: &DataType,
    nullable: bool,
    storage: ValueTypeStorageLifetime,
) -> Result<TypedValueBinding, ExecutionColumnTypeError> {
    validate_execution_declaration_observed::<ExecutionColumnTypeError>(declaration, || Ok(()))?;
    let layout = ValueLayout::try_new(carrier, storage.clone())?;
    let value_type = author(declaration, layout.data_type(), nullable, &storage)?;
    Ok(TypedValueBinding::try_new(value_type, layout)?)
}

/// Validate a borrowed declaration before any semantic or layout construction.
///
/// A declaration may be deeper or wider than an unrelated carrier. Its own
/// bounded traversal therefore precedes carrier preflight. The caller observes
/// each actual declaration node and can stop the walk without a new receipt or
/// an inferred resource grant. This walk does not copy declarations or names.
pub fn validate_execution_declaration_observed<E>(
    declaration: &SqlType,
    mut observe: impl FnMut() -> Result<(), E>,
) -> Result<(), E>
where
    E: From<ExecutionColumnTypeError>,
{
    fn visit<E: From<ExecutionColumnTypeError>>(
        declaration: &SqlType,
        depth: usize,
        nodes: &mut usize,
        observe: &mut impl FnMut() -> Result<(), E>,
    ) -> Result<(), E> {
        observe()?;
        if depth > MAX_VALUE_TYPE_DEPTH {
            return Err(E::from(ExecutionColumnTypeError::Type(
                ExecutionValueTypeError::Depth,
            )));
        }
        *nodes = nodes.checked_add(1).ok_or_else(|| {
            E::from(ExecutionColumnTypeError::Type(
                ExecutionValueTypeError::Nodes,
            ))
        })?;
        if *nodes > MAX_VALUE_TYPE_NODES {
            return Err(E::from(ExecutionColumnTypeError::Type(
                ExecutionValueTypeError::Nodes,
            )));
        }
        match declaration {
            SqlType::Array(item) => visit(item, depth + 1, nodes, observe),
            SqlType::Map(key, value) => {
                visit(key, depth + 1, nodes, observe)?;
                visit(value, depth + 1, nodes, observe)
            }
            SqlType::Struct(fields) => {
                for (_, child) in fields {
                    visit(child, depth + 1, nodes, observe)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
    visit(declaration, 1, &mut 0, &mut observe)
}

/// Project a bound semantic tree back to its explicit SQL declaration.
///
/// NULL contracts and exact physical layout remain on the binding: SQL's type
/// declaration has no separate field for them. The returned recursive domains
/// can be stored with that layout without using Arrow annotations. Unsupported
/// pure value types are refused, never converted to a nearby SQL scalar.
pub fn sql_type_from_execution_value(
    binding: &TypedValueBinding,
) -> Result<SqlType, ExecutionColumnTypeError> {
    sql_type_from_execution_value_observed(binding, |_| Ok::<(), ExecutionColumnTypeError>(()))
}

/// Observe the existing projection's actual node work and each construction
/// before copying names or allocating the presentation's containers. These
/// observations supply no capacity policy and cannot alter the bound type.
pub fn sql_type_from_execution_value_observed<E: From<ExecutionColumnTypeError>>(
    binding: &TypedValueBinding,
    mut observe: impl FnMut(usize) -> Result<(), E>,
) -> Result<SqlType, E> {
    fn declaration<E: From<ExecutionColumnTypeError>>(
        value: &ValueTypeRef,
        observe: &mut impl FnMut(usize) -> Result<(), E>,
    ) -> Result<SqlType, E> {
        observe(1)?;
        Ok(match value.kind() {
            ValueKind::String => SqlType::String,
            ValueKind::Binary => SqlType::Binary,
            ValueKind::Nominal(domain) => match domain {
                ValueLogicalType::Json => SqlType::Json,
                ValueLogicalType::Variant => SqlType::Variant,
                ValueLogicalType::Hll => SqlType::Hll,
                ValueLogicalType::Bitmap => SqlType::Bitmap,
                ValueLogicalType::Object => SqlType::Object,
                ValueLogicalType::Percentile => SqlType::Percentile,
                ValueLogicalType::LargeInt => SqlType::LargeInt,
                ValueLogicalType::Uuid => SqlType::Uuid,
                ValueLogicalType::Physical => {
                    return Err(ExecutionColumnTypeError::UnrepresentableValue(
                        "Physical is not an authored semantic domain",
                    )
                    .into());
                }
            },
            ValueKind::List(item) | ValueKind::FixedSizeList { item, .. } => {
                let item = declaration(item, observe)?;
                observe(1)?;
                SqlType::Array(Box::new(item))
            }
            ValueKind::Struct(fields) => {
                observe(1)?;
                let mut projected = Vec::with_capacity(fields.len());
                for field in fields.iter() {
                    // Names are already valid UTF-8 semantic names. Observe
                    // every byte before copying the original name backing.
                    observe(field.name().len())?;
                    let name = field.name().to_owned();
                    let child = declaration(field.value_type(), observe)?;
                    projected.push((name, child));
                }
                SqlType::Struct(projected)
            }
            ValueKind::Map { key, value } => {
                let key = declaration(key, observe)?;
                observe(1)?;
                let key = Box::new(key);
                let value = declaration(value, observe)?;
                observe(1)?;
                SqlType::Map(key, Box::new(value))
            }
            ValueKind::Union(_) => {
                return Err(ExecutionColumnTypeError::UnrepresentableValue(
                    "SQL has no declaration for a Union value",
                )
                .into());
            }
            ValueKind::Scalar(carrier) => match carrier {
                DataType::Boolean => SqlType::Boolean,
                DataType::Int8 => SqlType::TinyInt,
                DataType::Int16 => SqlType::SmallInt,
                DataType::Int32 => SqlType::Int,
                DataType::Int64 => SqlType::BigInt,
                DataType::Float32 => SqlType::Float,
                DataType::Float64 => SqlType::Double,
                DataType::FixedSizeBinary(_) => SqlType::Binary,
                DataType::Date32 => SqlType::Date,
                DataType::Timestamp(TimeUnit::Nanosecond, _) => SqlType::DateTimeNs,
                DataType::Timestamp(_, _) => SqlType::DateTime,
                DataType::Time64(_) => SqlType::Time,
                DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
                    SqlType::Decimal {
                        precision: *precision,
                        scale: *scale,
                    }
                }
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    return Err(
                        ExecutionColumnTypeError::UnrepresentableCarrier(carrier.clone()).into(),
                    );
                }
                _ => {
                    return Err(
                        ExecutionColumnTypeError::UnrepresentableCarrier(carrier.clone()).into(),
                    );
                }
            },
        })
    }
    declaration(binding.value_type(), &mut observe)
}

fn nominal_domain(declaration: &SqlType) -> Option<ValueLogicalType> {
    match declaration {
        SqlType::Json => Some(ValueLogicalType::Json),
        SqlType::Variant => Some(ValueLogicalType::Variant),
        SqlType::Hll => Some(ValueLogicalType::Hll),
        SqlType::Bitmap => Some(ValueLogicalType::Bitmap),
        SqlType::Object => Some(ValueLogicalType::Object),
        SqlType::Percentile => Some(ValueLogicalType::Percentile),
        SqlType::LargeInt => Some(ValueLogicalType::LargeInt),
        SqlType::Uuid => Some(ValueLogicalType::Uuid),
        _ => None,
    }
}

fn author(
    declaration: &SqlType,
    layout: &DataType,
    nullable: bool,
    storage: &ValueTypeStorageLifetime,
) -> Result<ValueTypeRef, ExecutionColumnTypeError> {
    if let Some(domain) = nominal_domain(declaration) {
        // The existing nominal carrier contract remains strict. Equal-width
        // bytes cannot independently author UUID, LARGEINT or opaque domains.
        domain
            .validate_carrier(layout)
            .map_err(ExecutionValueTypeError::from)?;
        return Ok(ValueTypeRef::try_new(
            ValueKind::Nominal(domain),
            nullable,
            storage.clone(),
        )?);
    }

    let mismatch = || ExecutionColumnTypeError::DeclarationMismatch;
    let child = |declaration, field: &arrow_schema::Field| {
        author(declaration, field.data_type(), field.is_nullable(), storage)
    };
    let kind = match (declaration, layout) {
        (
            SqlType::Array(declaration),
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item),
        ) => ValueKind::List(child(declaration, item)?),
        (SqlType::Array(declaration), DataType::FixedSizeList(item, length)) => {
            ValueKind::FixedSizeList {
                item: child(declaration, item)?,
                length: *length,
            }
        }
        (SqlType::Struct(declarations), DataType::Struct(fields)) => {
            if declarations.len() != fields.len() {
                return Err(mismatch());
            }
            let fields = declarations
                .iter()
                .zip(fields)
                .map(|((name, declaration), field)| {
                    if name != field.name() {
                        return Err(mismatch());
                    }
                    Ok(ValueField::new(
                        name.as_str(),
                        child(declaration, field)?,
                        storage.clone(),
                    ))
                })
                .collect::<Result<Box<[_]>, ExecutionColumnTypeError>>()?;
            ValueKind::Struct(fields)
        }
        (SqlType::Map(key, value), DataType::Map(entries, _)) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(mismatch());
            };
            if fields.len() != 2 {
                return Err(mismatch());
            }
            ValueKind::Map {
                key: child(key, &fields[0])?,
                value: child(value, &fields[1])?,
            }
        }
        (SqlType::String, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => {
            ValueKind::String
        }
        (SqlType::Binary, DataType::Binary | DataType::LargeBinary | DataType::BinaryView) => {
            ValueKind::Binary
        }
        // FIXED retains its physical width, without becoming a nominal type.
        (SqlType::Binary, DataType::FixedSizeBinary(_)) => ValueKind::Scalar(layout.clone()),
        (SqlType::Boolean, DataType::Boolean)
        | (SqlType::TinyInt, DataType::Int8)
        | (SqlType::SmallInt, DataType::Int16)
        | (SqlType::Int, DataType::Int32)
        | (SqlType::BigInt, DataType::Int64)
        | (SqlType::Float, DataType::Float32)
        | (SqlType::Double, DataType::Float64)
        | (SqlType::Date, DataType::Date32)
        | (
            SqlType::DateTime,
            DataType::Timestamp(
                TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond,
                _,
            ),
        )
        | (SqlType::DateTimeNs, DataType::Timestamp(TimeUnit::Nanosecond, _))
        | (SqlType::Time, DataType::Time64(_)) => ValueKind::Scalar(layout.clone()),
        (
            SqlType::Decimal { precision, scale },
            DataType::Decimal128(actual_precision, actual_scale)
            | DataType::Decimal256(actual_precision, actual_scale),
        ) if precision == actual_precision && scale == actual_scale => {
            ValueKind::Scalar(layout.clone())
        }
        _ => return Err(mismatch()),
    };
    Ok(ValueTypeRef::try_new(kind, nullable, storage.clone())?)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use arrow_schema::Field;
    use novarocks_type_contract::ValueTypeStorageHold;

    use super::*;

    fn bind(declaration: &SqlType, carrier: &DataType, nullable: bool) -> TypedValueBinding {
        execution_value_type_from_declaration(
            declaration,
            carrier,
            nullable,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap()
    }

    #[test]
    fn declarations_author_root_domains_without_guessing_equal_carriers() {
        for (declaration, carrier, domain) in [
            (SqlType::Json, DataType::Utf8, ValueLogicalType::Json),
            (
                SqlType::Uuid,
                DataType::FixedSizeBinary(16),
                ValueLogicalType::Uuid,
            ),
            (
                SqlType::LargeInt,
                DataType::FixedSizeBinary(16),
                ValueLogicalType::LargeInt,
            ),
            (
                SqlType::Variant,
                DataType::LargeBinary,
                ValueLogicalType::Variant,
            ),
            (SqlType::Bitmap, DataType::Binary, ValueLogicalType::Bitmap),
            (SqlType::Hll, DataType::Binary, ValueLogicalType::Hll),
            (
                SqlType::Object,
                DataType::LargeBinary,
                ValueLogicalType::Object,
            ),
            (
                SqlType::Percentile,
                DataType::Binary,
                ValueLogicalType::Percentile,
            ),
        ] {
            let value = bind(&declaration, &carrier, false);
            assert_eq!(value.value_type().kind(), &ValueKind::Nominal(domain));
            let physical_declaration = match carrier {
                DataType::Utf8 => SqlType::String,
                _ => SqlType::Binary,
            };
            let physical = bind(&physical_declaration, &carrier, false);
            assert!(!value.same_semantics(&physical));
        }
    }

    #[test]
    fn recursive_declaration_retains_json_uuid_variant_and_child_nullability() {
        let declaration = SqlType::Struct(vec![
            ("documents".into(), SqlType::Array(Box::new(SqlType::Json))),
            ("uuid".into(), SqlType::Uuid),
            ("variant".into(), SqlType::Variant),
        ]);
        let carrier = DataType::Struct(
            vec![
                Field::new(
                    "documents",
                    DataType::LargeList(Arc::new(Field::new("element", DataType::Utf8, true))),
                    false,
                ),
                Field::new("uuid", DataType::FixedSizeBinary(16), false),
                Field::new("variant", DataType::LargeBinary, true),
            ]
            .into(),
        );
        let binding = bind(&declaration, &carrier, true);
        assert!(binding.value_type().nullable());
        let ValueKind::Struct(fields) = binding.value_type().kind() else {
            panic!("Struct expected");
        };
        assert_eq!(
            fields.iter().map(ValueField::name).collect::<Vec<_>>(),
            ["documents", "uuid", "variant"]
        );
        assert!(!fields[0].value_type().nullable());
        let ValueKind::List(item) = fields[0].value_type().kind() else {
            panic!("List expected");
        };
        assert!(item.nullable());
        assert_eq!(item.kind(), &ValueKind::Nominal(ValueLogicalType::Json));
        assert_eq!(
            fields[1].value_type().kind(),
            &ValueKind::Nominal(ValueLogicalType::Uuid)
        );
        assert!(!fields[1].value_type().nullable());
        assert_eq!(
            fields[2].value_type().kind(),
            &ValueKind::Nominal(ValueLogicalType::Variant)
        );
        assert!(fields[2].value_type().nullable());
    }

    #[test]
    fn format_role_labels_and_annotations_do_not_author_value_semantics() {
        let declaration = SqlType::Map(
            Box::new(SqlType::String),
            Box::new(SqlType::Array(Box::new(SqlType::Json))),
        );
        let carrier = |entries, key, value, item| {
            DataType::Map(
                Arc::new(Field::new(
                    entries,
                    DataType::Struct(
                        vec![
                            Field::new(key, DataType::Utf8, false),
                            Field::new(
                                value,
                                DataType::List(Arc::new(
                                    Field::new(item, DataType::Utf8, true).with_metadata(
                                        std::collections::HashMap::<String, String>::from([
                                            (
                                                novarocks_type_contract::NR_LOGICAL_TYPE_KEY.into(),
                                                "uninterpreted".into(),
                                            ),
                                            ("PARQUET:field_id".into(), "19".into()),
                                        ]),
                                    ),
                                )),
                                true,
                            ),
                        ]
                        .into(),
                    ),
                    false,
                )),
                true,
            )
        };
        let left = bind(&declaration, &carrier("pairs", "k", "v", "element"), false);
        let right = bind(
            &declaration,
            &carrier("entries", "key", "value", "item"),
            false,
        );
        assert_eq!(left, right);
        let ValueKind::Map { key, value } = left.value_type().kind() else {
            panic!("Map expected");
        };
        assert!(!key.nullable());
        assert_eq!(key.kind(), &ValueKind::String);
        let ValueKind::List(item) = value.kind() else {
            panic!("List expected");
        };
        assert_eq!(item.kind(), &ValueKind::Nominal(ValueLogicalType::Json));
        let DataType::Map(entries, true) = left.layout().data_type() else {
            panic!("ordered Map expected");
        };
        assert_eq!(entries.name(), "entries");
        assert!(entries.metadata().is_empty());
    }

    #[test]
    fn mismatched_declarations_and_exact_scalar_parameters_are_refused() {
        for (declaration, carrier) in [
            (SqlType::BigInt, DataType::Int32),
            (SqlType::String, DataType::Binary),
            (SqlType::Json, DataType::LargeUtf8),
            (SqlType::Uuid, DataType::FixedSizeBinary(15)),
            (SqlType::Variant, DataType::Binary),
            (
                SqlType::Decimal {
                    precision: 10,
                    scale: 2,
                },
                DataType::Decimal128(12, 2),
            ),
            (SqlType::Array(Box::new(SqlType::Int)), DataType::Int32),
            (
                SqlType::Struct(vec![("declared".into(), SqlType::BigInt)]),
                DataType::Struct(vec![Field::new("actual", DataType::Int64, true)].into()),
            ),
            (
                SqlType::Struct(vec![
                    ("a".into(), SqlType::BigInt),
                    ("b".into(), SqlType::BigInt),
                ]),
                DataType::Struct(
                    vec![
                        Field::new("b", DataType::Int64, false),
                        Field::new("a", DataType::Int64, true),
                    ]
                    .into(),
                ),
            ),
        ] {
            assert!(
                execution_value_type_from_declaration(
                    &declaration,
                    &carrier,
                    false,
                    ValueTypeStorageLifetime::pure()
                )
                .is_err()
            );
        }
        let carrier = DataType::Decimal256(50, -2);
        assert_eq!(
            bind(
                &SqlType::Decimal {
                    precision: 50,
                    scale: -2
                },
                &carrier,
                true
            )
            .layout()
            .data_type(),
            &carrier
        );
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
        ] {
            let carrier = DataType::Timestamp(unit, Some("UTC".into()));
            assert_eq!(
                bind(&SqlType::DateTime, &carrier, false)
                    .layout()
                    .data_type(),
                &carrier
            );
        }
    }

    #[test]
    fn physical_byte_layouts_and_fixed_list_lengths_remain_exact() {
        let short = bind(&SqlType::String, &DataType::Utf8, false);
        let wide = bind(&SqlType::String, &DataType::LargeUtf8, false);
        assert!(short.same_semantics(&wide));
        assert_ne!(short, wide);
        let declaration = SqlType::Array(Box::new(SqlType::Int));
        let carrier =
            DataType::FixedSizeList(Arc::new(Field::new("element", DataType::Int32, false)), 7);
        let binding = bind(&declaration, &carrier, true);
        let ValueKind::FixedSizeList { item, length } = binding.value_type().kind() else {
            panic!("FixedSizeList expected");
        };
        assert_eq!(*length, 7);
        assert!(!item.nullable());
        assert_eq!(
            bind(&SqlType::Binary, &DataType::FixedSizeBinary(16), false)
                .value_type()
                .kind(),
            &ValueKind::Scalar(DataType::FixedSizeBinary(16))
        );
    }

    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn independently_retained_nested_semantics_keep_the_author_storage_alive() {
        let exits = Arc::new(AtomicUsize::new(0));
        let declaration = SqlType::Struct(vec![("json".into(), SqlType::Json)]);
        let carrier = DataType::Struct(vec![Field::new("json", DataType::Utf8, true)].into());
        let binding = execution_value_type_from_declaration(
            &declaration,
            &carrier,
            false,
            ValueTypeStorageLifetime::held(Arc::new(Hold(exits.clone()))),
        )
        .unwrap();
        let ValueKind::Struct(fields) = binding.value_type().kind() else {
            panic!("Struct expected");
        };
        let field = fields[0].clone();
        let child = field.value_type().clone();
        drop(binding);
        drop(declaration);
        drop(carrier);
        assert_eq!(field.name(), "json");
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        drop(field);
        assert_eq!(exits.load(Ordering::SeqCst), 0);
        assert_eq!(child.kind(), &ValueKind::Nominal(ValueLogicalType::Json));
        drop(child);
        assert_eq!(exits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn inverse_projection_preserves_nested_domains_and_exact_parameter_roundtrips() {
        let domains = [
            (SqlType::Json, DataType::Utf8),
            (SqlType::Uuid, DataType::FixedSizeBinary(16)),
            (SqlType::LargeInt, DataType::FixedSizeBinary(16)),
            (SqlType::Variant, DataType::LargeBinary),
            (SqlType::Bitmap, DataType::Binary),
            (SqlType::Hll, DataType::LargeBinary),
            (SqlType::Object, DataType::Binary),
            (SqlType::Percentile, DataType::LargeBinary),
        ];
        for (domain, carrier) in domains {
            let declaration = SqlType::Struct(vec![
                (
                    "values".into(),
                    SqlType::Map(
                        Box::new(SqlType::String),
                        Box::new(SqlType::Array(Box::new(domain))),
                    ),
                ),
                (
                    "amount".into(),
                    SqlType::Decimal {
                        precision: 50,
                        scale: -2,
                    },
                ),
                ("time".into(), SqlType::DateTimeNs),
            ]);
            let carrier = DataType::Struct(
                vec![
                    Field::new(
                        "values",
                        DataType::Map(
                            Arc::new(Field::new(
                                "pairs",
                                DataType::Struct(
                                    vec![
                                        Field::new("k", DataType::LargeUtf8, false),
                                        Field::new(
                                            "v",
                                            DataType::FixedSizeList(
                                                Arc::new(Field::new("element", carrier, true)),
                                                5,
                                            ),
                                            false,
                                        ),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            true,
                        ),
                        true,
                    ),
                    Field::new("amount", DataType::Decimal256(50, -2), false),
                    Field::new(
                        "time",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                        true,
                    ),
                ]
                .into(),
            );
            for nullable in [false, true] {
                let original = bind(&declaration, &carrier, nullable);
                let projected = sql_type_from_execution_value(&original).unwrap();
                assert_eq!(projected, declaration);
                let restored = bind(
                    &projected,
                    original.layout().data_type(),
                    original.value_type().nullable(),
                );
                assert_eq!(restored, original);
            }
        }
    }

    #[test]
    fn inverse_projection_never_adopts_an_annotation_or_an_equal_carrier_domain() {
        let carrier =
            DataType::Struct(
                vec![Field::new("v", DataType::Utf8, true).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        novarocks_type_contract::NR_LOGICAL_TYPE_KEY.into(),
                        "json".into(),
                    )]),
                )]
                .into(),
            );
        let plain = SqlType::Struct(vec![("v".into(), SqlType::String)]);
        let json = SqlType::Struct(vec![("v".into(), SqlType::Json)]);
        let plain_binding = bind(&plain, &carrier, false);
        let json_binding = bind(&json, &carrier, false);
        assert_eq!(plain_binding.layout(), json_binding.layout());
        assert_eq!(
            sql_type_from_execution_value(&plain_binding).unwrap(),
            plain
        );
        assert_eq!(sql_type_from_execution_value(&json_binding).unwrap(), json);
        assert!(!plain_binding.same_semantics(&json_binding));
    }

    #[test]
    fn pure_unsigned_and_union_values_fail_inverse_without_a_guessed_sql_type() {
        use arrow_schema::{UnionFields, UnionMode};
        use novarocks_type_contract::ValueUnionField;

        let pure = ValueTypeStorageLifetime::pure;
        let uint =
            ValueTypeRef::try_new(ValueKind::Scalar(DataType::UInt64), false, pure()).unwrap();
        let binding = TypedValueBinding::try_new(
            uint,
            ValueLayout::try_new(&DataType::UInt64, pure()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            sql_type_from_execution_value(&binding).unwrap_err(),
            ExecutionColumnTypeError::UnrepresentableCarrier(DataType::UInt64)
        );
        let child =
            ValueTypeRef::try_new(ValueKind::Scalar(DataType::Int64), false, pure()).unwrap();
        let union = ValueTypeRef::try_new(
            ValueKind::Union(
                vec![ValueUnionField {
                    type_id: 1,
                    field: ValueField::new("v", child, pure()),
                }]
                .into(),
            ),
            false,
            pure(),
        )
        .unwrap();
        let carrier = DataType::Union(
            UnionFields::try_new([1], [Arc::new(Field::new("v", DataType::Int64, false))]).unwrap(),
            UnionMode::Dense,
        );
        let binding =
            TypedValueBinding::try_new(union, ValueLayout::try_new(&carrier, pure()).unwrap())
                .unwrap();
        assert_eq!(
            sql_type_from_execution_value(&binding).unwrap_err(),
            ExecutionColumnTypeError::UnrepresentableValue(
                "SQL has no declaration for a Union value"
            )
        );
    }

    #[test]
    fn declaration_preflight_is_independent_of_mismatched_carrier_shape() {
        let mut deep = SqlType::Int;
        for _ in 0..MAX_VALUE_TYPE_DEPTH {
            deep = SqlType::Array(Box::new(deep));
        }
        assert_eq!(
            execution_value_type_from_declaration(
                &deep,
                &DataType::Int32,
                false,
                ValueTypeStorageLifetime::pure()
            )
            .unwrap_err(),
            ExecutionColumnTypeError::Type(ExecutionValueTypeError::Depth)
        );
        let wide = SqlType::Struct(
            (0..MAX_VALUE_TYPE_NODES)
                .map(|index| (format!("f{index}"), SqlType::Int))
                .collect(),
        );
        assert_eq!(
            execution_value_type_from_declaration(
                &wide,
                &DataType::Int32,
                false,
                ValueTypeStorageLifetime::pure()
            )
            .unwrap_err(),
            ExecutionColumnTypeError::Type(ExecutionValueTypeError::Nodes)
        );

        #[derive(Debug, Eq, PartialEq)]
        enum ObservedError {
            Declaration(ExecutionColumnTypeError),
            Stop,
        }
        impl From<ExecutionColumnTypeError> for ObservedError {
            fn from(error: ExecutionColumnTypeError) -> Self {
                Self::Declaration(error)
            }
        }
        let declaration = SqlType::Struct(vec![
            ("a".into(), SqlType::Int),
            ("b".into(), SqlType::Json),
            ("c".into(), SqlType::Array(Box::new(SqlType::Uuid))),
        ]);
        let mut visits = 0;
        validate_execution_declaration_observed::<ObservedError>(&declaration, || {
            visits += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(visits, 5);
        let mut visits = 0;
        assert_eq!(
            validate_execution_declaration_observed::<ObservedError>(&declaration, || {
                visits += 1;
                if visits == 3 {
                    Err(ObservedError::Stop)
                } else {
                    Ok(())
                }
            })
            .unwrap_err(),
            ObservedError::Stop
        );
        assert_eq!(visits, 3);
    }
}

#[cfg(test)]
mod projection_observer_tests {
    use super::*;
    use arrow_schema::Field;
    use std::sync::Arc;
    #[derive(Debug)]
    enum Refusal {
        Projection { _error: ExecutionColumnTypeError },
        Original(Arc<()>),
    }
    impl From<ExecutionColumnTypeError> for Refusal {
        fn from(error: ExecutionColumnTypeError) -> Self {
            Self::Projection { _error: error }
        }
    }
    fn source() -> (DataType, SqlType, TypedValueBinding) {
        let name = "semantic_name_".repeat(40);
        let layout = DataType::Struct(
            vec![
                Field::new(name.clone(), DataType::Utf8, false).with_metadata(
                    std::collections::HashMap::<String, String>::from([(
                        "provider.attribute".into(),
                        "original bytes".into(),
                    )]),
                ),
            ]
            .into(),
        );
        let declaration = SqlType::Struct(vec![(name, SqlType::Json)]);
        let binding = execution_value_type_from_declaration(
            &declaration,
            &layout,
            false,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        (layout, declaration, binding)
    }
    #[test]
    fn actual_projection_uses_complete_declaration_and_keeps_source_attributes_independent() {
        let (source, expected, binding) = source();
        assert_eq!(
            sql_type_from_execution_value_observed(
                &binding,
                |_| Ok::<(), ExecutionColumnTypeError>(())
            )
            .unwrap(),
            expected
        );
        let DataType::Struct(fields) = &source else {
            panic!("source");
        };
        assert_eq!(
            fields[0]
                .metadata()
                .get("provider.attribute")
                .map(String::as_str),
            Some("original bytes")
        );
        let DataType::Struct(fields) = binding.data_type() else {
            panic!("execution");
        };
        assert!(fields[0].metadata().is_empty());
        assert_eq!(
            binding.value_type().child_type_at(0).unwrap().domain(),
            Some(ValueLogicalType::Json)
        );
    }
    #[test]
    fn projection_observes_original_name_bytes_and_preserves_every_refusal_prefix() {
        let (_, _, binding) = source();
        let mut baseline = Vec::new();
        sql_type_from_execution_value_observed(&binding, |units| {
            baseline.push(units);
            Ok::<(), Refusal>(())
        })
        .unwrap();
        let ValueKind::Struct(fields) = binding.value_type().kind() else {
            panic!("struct");
        };
        assert_eq!(baseline, vec![1, 1, fields[0].name().len(), 1]);
        for index in 0..baseline.len() {
            let cause = Arc::new(());
            let mut prefix = Vec::new();
            let error = sql_type_from_execution_value_observed(&binding, |units| {
                let at = prefix.len();
                prefix.push(units);
                if at == index {
                    Err(Refusal::Original(cause.clone()))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
            let Refusal::Original(original) = error else {
                panic!("preserve original observer failure");
            };
            assert!(Arc::ptr_eq(&original, &cause));
            assert_eq!(prefix, baseline[..=index]);
        }
    }
}

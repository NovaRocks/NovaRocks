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

//! Preserve independently declared Variant identity across the catalog read seam.
//!
//! Definition facts remain a display vocabulary. This adapter borrows only an
//! exact provider declaration containing Variant; it does not reinterpret DDL
//! spellings as general query types or rebuild the provider's Arrow fields.

use arrow::datatypes::{DataType, Field, TimeUnit};
use novarocks_spi::connector::{
    ConnectorTableColumnSemanticKind, ConnectorTableDefinitionFacts,
    ConnectorTableDefinitionType as D,
};
use novarocks_types::schema::SqlType;

fn invalid() -> String {
    "connector Variant declaration does not match its exact SQL read carrier".to_string()
}

pub(super) fn project_column(
    definitions: &ConnectorTableDefinitionFacts,
    ordinal: usize,
    field: &Field,
    semantic: ConnectorTableColumnSemanticKind,
) -> Result<Option<SqlType>, String> {
    let Ok(ordinal) = u32::try_from(ordinal) else {
        return Err(invalid());
    };
    // Definition ordinals are validated, strictly increasing original schema
    // ordinals. Hidden fields must not shift the lookup into the visible list.
    let Ok(index) = definitions
        .columns()
        .binary_search_by_key(&ordinal, |column| column.field_ordinal())
    else {
        return Ok(None);
    };
    let column = &definitions.columns()[index];
    if !contains_variant(column.data_type()) {
        return Ok(None);
    }
    if column.nullable() != field.is_nullable()
        || semantic != ConnectorTableColumnSemanticKind::None
    {
        return Err(invalid());
    }
    project_field(column.data_type(), field).map(Some)
}

fn contains_variant(declared: &D) -> bool {
    match declared {
        D::Variant => true,
        D::Array(item) => contains_variant(item),
        D::Map(key, value) => contains_variant(key) || contains_variant(value),
        D::Struct(fields) => fields
            .iter()
            .any(|field| contains_variant(field.data_type())),
        _ => false,
    }
}

fn project_field(declared: &D, field: &Field) -> Result<SqlType, String> {
    // A definition has no independent opaque/JSON child fact. Never wash a
    // recognized or unknown logical marker into an ordinary companion type.
    if field
        .metadata()
        .contains_key(novarocks_types::logical::NR_LOGICAL_TYPE_KEY)
    {
        return Err(invalid());
    }
    project_type(declared, field.data_type())
}

fn project_type(declared: &D, actual: &DataType) -> Result<SqlType, String> {
    match (declared, actual) {
        (D::Variant, DataType::LargeBinary) => Ok(SqlType::Variant),
        (
            D::Array(item),
            DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _),
        ) => Ok(SqlType::Array(Box::new(project_field(item, field)?))),
        (D::Map(key, value), DataType::Map(entries, _)) => {
            if entries
                .metadata()
                .contains_key(novarocks_types::logical::NR_LOGICAL_TYPE_KEY)
            {
                return Err(invalid());
            }
            let DataType::Struct(fields) = entries.data_type() else {
                return Err(invalid());
            };
            if fields.len() != 2 || fields[0].name() != "key" || fields[1].name() != "value" {
                return Err(invalid());
            }
            // Nullability and sortedness remain facts of the original Arrow
            // carrier. In particular, no non-null map-key rule is introduced.
            Ok(SqlType::Map(
                Box::new(project_field(key, &fields[0])?),
                Box::new(project_field(value, &fields[1])?),
            ))
        }
        (D::Struct(declared), DataType::Struct(actual)) if declared.len() == actual.len() => {
            let mut fields = Vec::new();
            fields
                .try_reserve_exact(declared.len())
                .map_err(|_| invalid())?;
            for (declaration, field) in declared.iter().zip(actual) {
                if declaration.name() != field.name() {
                    return Err(invalid());
                }
                fields.push((
                    field.name().clone(),
                    project_field(declaration.data_type(), field)?,
                ));
            }
            Ok(SqlType::Struct(fields))
        }
        _ if ordinary_carrier_matches(declared, actual) => {
            novarocks_sql::literal::arrow_data_type_to_sql_type(actual).map_err(|_| invalid())
        }
        _ => Err(invalid()),
    }
}

fn ordinary_carrier_matches(declared: &D, actual: &DataType) -> bool {
    match (declared, actual) {
        (D::Boolean, DataType::Boolean)
        // Iceberg INT can carry an independently admitted narrow SQL integer.
        | (D::Int, DataType::Int8 | DataType::Int16 | DataType::Int32)
        | (D::BigInt, DataType::Int64)
        | (D::Float, DataType::Float32)
        | (D::Double, DataType::Float64)
        | (D::Date, DataType::Date32)
        | (D::Time, DataType::Time64(TimeUnit::Microsecond))
        | (D::DateTime, DataType::Timestamp(TimeUnit::Microsecond, _))
        | (D::DateTimeNs, DataType::Timestamp(TimeUnit::Nanosecond, _))
        | (D::String, DataType::Utf8 | DataType::LargeUtf8)
        // The independent ordinary Binary declaration permits both offset
        // widths without granting Variant identity from the carrier alone.
        | (D::Binary { fixed_length: None }, DataType::Binary | DataType::LargeBinary) => true,
        (D::Decimal { precision, scale }, DataType::Decimal128(p, s)) => {
            u32::from(*p) == *precision && u32::try_from(*s).ok() == Some(*scale)
        }
        // The ordinary Arrow inverse interprets FixedSizeBinary(16) as
        // LargeInt. A display-only fixed Binary declaration cannot prove that
        // identity, and SqlType has no fixed Binary query companion variant.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Schema;
    use novarocks_spi::connector::{
        ConnectorRequestContext, ConnectorStopOwner, ConnectorTableColumnPlanningFact,
        ConnectorTableColumnRole, ConnectorTableColumnVisibility, ConnectorTableDefinitionColumn,
        ConnectorTableDefinitionStructField, ConnectorTablePlanningFacts,
    };
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn context() -> ConnectorRequestContext {
        ConnectorRequestContext::try_new(
            Instant::now() + Duration::from_secs(60),
            ConnectorStopOwner::new().view(),
            1_024,
            64 * 1_024,
        )
        .unwrap()
    }

    fn definitions(field: &Field, declared: D) -> ConnectorTableDefinitionFacts {
        ConnectorTableDefinitionFacts::try_new(
            &Arc::new(Schema::new(vec![field.clone()])),
            &ConnectorTablePlanningFacts::empty(),
            vec![ConnectorTableDefinitionColumn::new(
                0,
                declared,
                field.is_nullable(),
                None,
            )],
            None,
            &context(),
        )
        .unwrap()
    }

    fn projected(field: &Field, declared: D) -> Result<Option<SqlType>, String> {
        project_column(
            &definitions(field, declared),
            0,
            field,
            ConnectorTableColumnSemanticKind::None,
        )
    }

    fn record_declaration() -> D {
        D::Struct(vec![
            ConnectorTableDefinitionStructField::new("id", D::Int),
            ConnectorTableDefinitionStructField::new("v", D::Variant),
        ])
    }

    fn record_type(id: DataType, variant: DataType) -> DataType {
        DataType::Struct(vec![Field::new("id", id, false), Field::new("v", variant, true)].into())
    }

    #[test]
    fn m07_provider_variant_identity_requires_an_independent_declaration() {
        let field = Field::new("v", DataType::LargeBinary, true);
        assert_eq!(
            projected(&field, D::Variant).unwrap(),
            Some(SqlType::Variant)
        );
        // Equal storage bytes alone do not establish Variant identity.
        assert_eq!(
            projected(&field, D::Binary { fixed_length: None }).unwrap(),
            None
        );
        assert_eq!(
            project_column(
                &ConnectorTableDefinitionFacts::empty(),
                0,
                &field,
                ConnectorTableColumnSemanticKind::None,
            )
            .unwrap(),
            None
        );
        assert!(projected(&Field::new("v", DataType::Binary, true), D::Variant).is_err());
    }

    #[test]
    fn m07_provider_nested_variant_preserves_actual_companion_width_and_shape() {
        let expected = SqlType::Struct(vec![
            ("id".into(), SqlType::TinyInt),
            ("v".into(), SqlType::Variant),
        ]);
        let record = record_type(DataType::Int8, DataType::LargeBinary);
        let child = Arc::new(Field::new("item", record.clone(), false));
        for list in [
            DataType::List(child.clone()),
            DataType::LargeList(child.clone()),
            DataType::FixedSizeList(child.clone(), 3),
        ] {
            let field = Field::new("values", list, true);
            assert_eq!(
                projected(&field, D::Array(Box::new(record_declaration()))).unwrap(),
                Some(SqlType::Array(Box::new(expected.clone())))
            );
            // Projection returns only a logical value; the original field is
            // never rebuilt, and its precise list shape/nullability survives.
            assert!(!child.is_nullable());
        }
        assert_eq!(
            projected(&Field::new("r", record, false), record_declaration()).unwrap(),
            Some(expected)
        );
    }

    #[test]
    fn m07_provider_map_variant_preserves_nullable_keys_and_sorted_carrier() {
        for sorted in [false, true] {
            let entries = Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, true),
                        Field::new("value", DataType::LargeBinary, false),
                    ]
                    .into(),
                ),
                false,
            );
            let field = Field::new("m", DataType::Map(Arc::new(entries), sorted), true);
            assert_eq!(
                projected(&field, D::Map(Box::new(D::String), Box::new(D::Variant))).unwrap(),
                Some(SqlType::Map(
                    Box::new(SqlType::String),
                    Box::new(SqlType::Variant)
                ))
            );
            let DataType::Map(entries, original_sorted) = field.data_type() else {
                unreachable!()
            };
            let DataType::Struct(fields) = entries.data_type() else {
                unreachable!()
            };
            assert_eq!(*original_sorted, sorted);
            assert!(fields[0].is_nullable());
            assert!(!fields[1].is_nullable());
        }
    }

    #[test]
    fn m07_provider_variant_refuses_names_order_arity_and_ordinary_carrier_conflicts() {
        let declared = record_declaration();
        for actual in [
            record_type(DataType::Int64, DataType::LargeBinary),
            record_type(DataType::Int8, DataType::Binary),
            DataType::Struct(
                vec![
                    Field::new("v", DataType::LargeBinary, true),
                    Field::new("id", DataType::Int32, false),
                ]
                .into(),
            ),
            DataType::Struct(vec![Field::new("v", DataType::LargeBinary, true)].into()),
            DataType::Struct(
                vec![
                    Field::new("renamed", DataType::Int32, false),
                    Field::new("v", DataType::LargeBinary, true),
                ]
                .into(),
            ),
        ] {
            assert!(projected(&Field::new("r", actual, true), declared.clone()).is_err());
        }
        // The same LargeBinary carrier can retain distinct identities when
        // the original provider declaration independently proves each leaf.
        let declared = D::Struct(vec![
            ConnectorTableDefinitionStructField::new("id", D::Binary { fixed_length: None }),
            ConnectorTableDefinitionStructField::new("v", D::Variant),
        ]);
        assert_eq!(
            projected(
                &Field::new(
                    "r",
                    record_type(DataType::LargeBinary, DataType::LargeBinary),
                    true
                ),
                declared,
            )
            .unwrap(),
            Some(SqlType::Struct(vec![
                ("id".into(), SqlType::Binary),
                ("v".into(), SqlType::Variant),
            ]))
        );
    }

    #[test]
    fn m07_provider_variant_refuses_invalid_map_entries_and_unrepresentable_siblings() {
        let declared = D::Map(Box::new(D::String), Box::new(D::Variant));
        for entries in [
            DataType::Utf8,
            DataType::Struct(vec![Field::new("key", DataType::Utf8, true)].into()),
            DataType::Struct(
                vec![
                    Field::new("value", DataType::LargeBinary, true),
                    Field::new("key", DataType::Utf8, true),
                ]
                .into(),
            ),
        ] {
            assert!(
                projected(
                    &Field::new(
                        "m",
                        DataType::Map(Arc::new(Field::new("entries", entries, false)), false),
                        true
                    ),
                    declared.clone()
                )
                .is_err()
            );
        }
        // Width 16 must fail too: accepting it would silently reclassify a
        // provider's fixed Binary declaration as SQL LargeInt.
        for width in [7, 16] {
            let unrepresentable = D::Struct(vec![
                ConnectorTableDefinitionStructField::new(
                    "id",
                    D::Binary {
                        fixed_length: Some(width as u64),
                    },
                ),
                ConnectorTableDefinitionStructField::new("v", D::Variant),
            ]);
            assert!(
                projected(
                    &Field::new(
                        "r",
                        record_type(DataType::FixedSizeBinary(width), DataType::LargeBinary),
                        true
                    ),
                    unrepresentable,
                )
                .is_err()
            );
        }
    }

    #[test]
    fn m07_provider_variant_refuses_nullable_and_semantic_fact_contradictions() {
        let original = Field::new("v", DataType::LargeBinary, true);
        let facts = definitions(&original, D::Variant);
        assert!(
            project_column(
                &facts,
                0,
                &original.clone().with_nullable(false),
                ConnectorTableColumnSemanticKind::None
            )
            .is_err()
        );
        for semantic in [
            ConnectorTableColumnSemanticKind::Bitmap,
            ConnectorTableColumnSemanticKind::Hll,
        ] {
            assert!(project_column(&facts, 0, &original, semantic).is_err());
        }
        for marker in ["unknown", "bitmap", "json"] {
            let field =
                original
                    .clone()
                    .with_metadata(std::collections::HashMap::<String, String>::from([(
                        novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_string(),
                        marker.to_string(),
                    )]));
            assert!(
                project_column(&facts, 0, &field, ConnectorTableColumnSemanticKind::None).is_err()
            );
            let child = Field::new("id", DataType::Int32, false).with_metadata(
                std::collections::HashMap::<String, String>::from([(
                    novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_string(),
                    marker.to_string(),
                )]),
            );
            let record = Field::new(
                "r",
                DataType::Struct(vec![child, original.clone()].into()),
                true,
            );
            assert!(projected(&record, record_declaration()).is_err());
        }
    }

    #[test]
    fn m07_provider_variant_lookup_uses_original_ordinals_after_hidden_fields() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("hidden", DataType::Int64, false),
            Field::new("plain", DataType::Binary, true),
            Field::new("v", DataType::LargeBinary, true),
        ]));
        let planning = ConnectorTablePlanningFacts::try_new(
            &schema,
            (0..3)
                .map(|ordinal| {
                    ConnectorTableColumnPlanningFact::new(
                        ordinal,
                        if ordinal == 0 {
                            ConnectorTableColumnVisibility::Hidden
                        } else {
                            ConnectorTableColumnVisibility::Sql
                        },
                        ConnectorTableColumnSemanticKind::None,
                        ConnectorTableColumnRole::Ordinary,
                    )
                })
                .collect(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            &context(),
        )
        .unwrap();
        let definitions = ConnectorTableDefinitionFacts::try_new(
            &schema,
            &planning,
            vec![
                ConnectorTableDefinitionColumn::new(
                    1,
                    D::Binary { fixed_length: None },
                    true,
                    None,
                ),
                ConnectorTableDefinitionColumn::new(2, D::Variant, true, None),
            ],
            None,
            &context(),
        )
        .unwrap();
        for ordinal in [0, 1] {
            assert_eq!(
                project_column(
                    &definitions,
                    ordinal,
                    schema.field(ordinal),
                    ConnectorTableColumnSemanticKind::None
                )
                .unwrap(),
                None
            );
        }
        assert_eq!(
            project_column(
                &definitions,
                2,
                schema.field(2),
                ConnectorTableColumnSemanticKind::None
            )
            .unwrap(),
            Some(SqlType::Variant)
        );
    }
}

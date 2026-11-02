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

//! CTAS target and writer projections borrow the complete SQL source declaration.
//! Frontend DML owns routing and the durable staged-publication saga; the shared
//! typed renderer owns the SQL declaration vocabulary.

use novarocks_sql::planning::dml::DmlSourceColumn;
use novarocks_sql::semantic::TableColumnDef;

/// Refuse private persistence before constructing the target's public types.
pub(crate) fn validate_source_domains(columns: &[DmlSourceColumn]) -> Result<(), String> {
    use novarocks_physical_plan::ResultValueDomain;
    if columns.iter().any(|column| {
        !column.domain.matches_storage(column.value_type.data_type())
            || matches!(
                column.domain,
                ResultValueDomain::Object | ResultValueDomain::Percentile
            )
            || column.has_private_persistence_domain()
    }) {
        return Err("CTAS source contains an unsupported internal opaque value domain".into());
    }
    Ok(())
}

pub(crate) fn source_columns_to_table_column_defs(
    columns: &[DmlSourceColumn],
) -> Result<Vec<TableColumnDef>, String> {
    validate_source_domains(columns)?;
    columns
        .iter()
        .map(|column| {
            let data_type = novarocks_types::sql_type_from_value_type(&column.value_type)
                .map_err(|error| format!("CTAS: source type not supported: {error}"))?;
            Ok(TableColumnDef {
                name: column.name.clone(),
                data_type,
                nullable: column.value_type.nullable(),
                aggregation: None,
                default: None,
            })
        })
        .collect()
}

pub(crate) fn source_columns_to_write_fields(
    columns: &[DmlSourceColumn],
) -> Result<
    Vec<novarocks_spi::connector::ConnectorWriteFieldRequest>,
    novarocks_spi::connector::ConnectorError,
> {
    columns
        .iter()
        .map(|column| {
            novarocks_spi::connector::ConnectorWriteFieldRequest::try_new_typed(
                arrow::datatypes::Field::new(
                    &column.name,
                    column.value_type.data_type().clone(),
                    column.value_type.nullable(),
                ),
                column.value_type.clone(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        DmlSourceColumn, source_columns_to_table_column_defs, source_columns_to_write_fields,
    };
    use arrow::datatypes::{DataType, Field, TimeUnit};
    use novarocks_physical_plan::ResultValueDomain as Domain;
    use novarocks_type_contract::{
        FunctionValueType, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
        ValueTypeStorageLifetime as Storage,
    };
    use novarocks_types::schema::SqlType;
    use std::sync::Arc;

    fn plain(name: &str, carrier: DataType, nullable: bool) -> DmlSourceColumn {
        DmlSourceColumn {
            domain: Domain::Plain,
            name: name.into(),
            value_type: FunctionValueType::new(carrier, nullable),
        }
    }

    fn typed(
        name: &str,
        kind: ValueKind,
        carrier: DataType,
        nullable: bool,
        domain: Domain,
    ) -> DmlSourceColumn {
        DmlSourceColumn {
            domain,
            name: name.into(),
            value_type: FunctionValueType::try_from_value(
                ValueTypeRef::try_new(kind, nullable, Storage::pure()).unwrap(),
                ValueLayout::try_new(&carrier, Storage::pure()).unwrap(),
            )
            .unwrap(),
        }
    }

    fn nominal(domain: ValueLogicalType, nullable: bool) -> ValueTypeRef {
        ValueTypeRef::try_new(ValueKind::Nominal(domain), nullable, Storage::pure()).unwrap()
    }

    #[test]
    fn m07_ctas_refuses_private_domains_before_the_arrow_inverse_can_erase_them() {
        for (domain, logical) in [
            (Domain::Object, ValueLogicalType::Object),
            (Domain::Percentile, ValueLogicalType::Percentile),
        ] {
            let child = nominal(logical, true);
            let required_child = nominal(logical, false);
            let key = FunctionValueType::new(DataType::Utf8, false)
                .value_type()
                .clone();
            let sources = [
                typed(
                    "v",
                    ValueKind::Nominal(logical),
                    DataType::Binary,
                    true,
                    domain,
                ),
                typed(
                    "v",
                    ValueKind::List(child.clone()),
                    DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
                    true,
                    Domain::Plain,
                ),
                typed(
                    "v",
                    ValueKind::Struct(
                        vec![ValueField::new("v", required_child, Storage::pure())]
                            .into_boxed_slice(),
                    ),
                    DataType::Struct(vec![Field::new("v", DataType::Binary, false)].into()),
                    true,
                    Domain::Plain,
                ),
                typed(
                    "v",
                    ValueKind::Map { key, value: child },
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Field::new("key", DataType::Utf8, false),
                                    Field::new("value", DataType::Binary, true),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    true,
                    Domain::Plain,
                ),
            ];
            for source in sources {
                assert_eq!(
                    source.value_type.data_type().clone(),
                    source_columns_to_write_fields(std::slice::from_ref(&source)).unwrap()[0]
                        .field()
                        .data_type()
                        .clone()
                );
                assert!(source.has_private_persistence_domain());
                assert_eq!(
                    super::validate_source_domains(std::slice::from_ref(&source)).unwrap_err(),
                    "CTAS source contains an unsupported internal opaque value domain"
                );
                assert!(source_columns_to_table_column_defs(&[source]).is_err());
            }
        }
        assert!(super::validate_source_domains(&[plain("plain", DataType::Binary, true)]).is_ok());
    }

    #[test]
    fn arrow_schema_to_table_column_defs_basic() {
        let source = [
            plain("id", DataType::Int32, true),
            plain("name", DataType::Utf8, true),
            plain("amount", DataType::Decimal128(10, 2), true),
        ];
        let cols = source_columns_to_table_column_defs(&source).unwrap();
        assert_eq!(cols.len(), 3);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, SqlType::Int);
        assert!(cols[0].nullable);
        assert!(cols[0].aggregation.is_none());
        assert!(cols[0].default.is_none());
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].data_type, SqlType::String);
        assert_eq!(
            cols[2].data_type,
            SqlType::Decimal {
                precision: 10,
                scale: 2
            }
        );
        let original_schema = arrow::datatypes::Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]);
        let projected_schema = arrow::datatypes::Schema::new(
            source_columns_to_write_fields(&source)
                .unwrap()
                .iter()
                .map(|field| field.field().clone())
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            format!("{:?}", Arc::new(original_schema)),
            format!("{:?}", Arc::new(projected_schema)),
            "plain CTAS schema digest material remains unchanged"
        );
    }

    #[test]
    fn arrow_schema_to_table_column_defs_nullability_propagated() {
        let source = [
            plain("required_col", DataType::Int64, false),
            plain("optional_col", DataType::Utf8, true),
        ];
        let cols = source_columns_to_table_column_defs(&source).unwrap();
        assert!(
            !cols[0].nullable,
            "Int64 NOT NULL field should not be nullable"
        );
        assert!(cols[1].nullable, "Utf8 NULL field should be nullable");
        let fields = source_columns_to_write_fields(&source).unwrap();
        for (column, field) in source.iter().zip(&fields) {
            assert_eq!(field.execution_type(), Some(&column.value_type));
            assert_eq!(field.field().is_nullable(), column.value_type.nullable());
            assert!(field.field().metadata().as_arc().is_none());
        }
    }

    #[test]
    fn arrow_schema_to_table_column_defs_all_primitive_types() {
        let declarations = [
            ("b", DataType::Boolean, SqlType::Boolean),
            ("i8", DataType::Int8, SqlType::TinyInt),
            ("i16", DataType::Int16, SqlType::SmallInt),
            ("i32", DataType::Int32, SqlType::Int),
            ("i64", DataType::Int64, SqlType::BigInt),
            ("f32", DataType::Float32, SqlType::Float),
            ("f64", DataType::Float64, SqlType::Double),
            ("s", DataType::Utf8, SqlType::String),
            ("ls", DataType::LargeUtf8, SqlType::String),
            ("bin", DataType::Binary, SqlType::Binary),
            ("lbin", DataType::LargeBinary, SqlType::Binary),
            ("d", DataType::Date32, SqlType::Date),
            (
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                SqlType::DateTime,
            ),
            ("t", DataType::Time64(TimeUnit::Microsecond), SqlType::Time),
            (
                "ns",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                SqlType::DateTimeNs,
            ),
        ];
        let source = declarations
            .iter()
            .map(|(name, carrier, _)| plain(name, carrier.clone(), true))
            .collect::<Vec<_>>();
        let cols = source_columns_to_table_column_defs(&source).unwrap();
        for (column, (_, _, expected)) in cols.iter().zip(&declarations) {
            assert_eq!(&column.data_type, expected);
        }
    }

    #[test]
    fn arrow_schema_to_table_column_defs_rejects_unsupported() {
        let err = source_columns_to_table_column_defs(&[plain("e", DataType::Float16, true)])
            .unwrap_err();
        assert!(
            err.to_lowercase().contains("not supported"),
            "expected 'not supported' in error, got: {err}"
        );
    }

    #[test]
    fn arrow_schema_to_table_column_defs_rejects_interval() {
        let err = source_columns_to_table_column_defs(&[plain(
            "iv",
            DataType::Interval(arrow::datatypes::IntervalUnit::DayTime),
            true,
        )])
        .unwrap_err();
        assert!(
            err.to_lowercase().contains("not supported"),
            "expected 'not supported' in error, got: {err}"
        );
    }

    #[test]
    fn arrow_schema_to_table_column_defs_recurses_list() {
        let source = [plain(
            "ids",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        )];
        let cols = source_columns_to_table_column_defs(&source).unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].data_type, SqlType::Array(Box::new(SqlType::BigInt)));
    }

    #[test]
    fn arrow_schema_to_table_column_defs_recurses_struct_and_list() {
        let source = [
            plain(
                "meta",
                DataType::Struct(
                    vec![
                        Field::new("a", DataType::Int32, true),
                        Field::new("b", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
            plain(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ];
        let cols = source_columns_to_table_column_defs(&source).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(
            cols[0].data_type,
            SqlType::Struct(vec![
                ("a".into(), SqlType::Int),
                ("b".into(), SqlType::String)
            ])
        );
        assert_eq!(cols[1].data_type, SqlType::Array(Box::new(SqlType::BigInt)));
    }

    #[test]
    fn ctas_complete_nominal_declarations_reach_ddl_and_writer_separately() {
        let declarations = [
            (ValueLogicalType::Json, DataType::Utf8, SqlType::Json),
            (
                ValueLogicalType::Variant,
                DataType::LargeBinary,
                SqlType::Variant,
            ),
            (
                ValueLogicalType::LargeInt,
                DataType::FixedSizeBinary(16),
                SqlType::LargeInt,
            ),
            (
                ValueLogicalType::Uuid,
                DataType::FixedSizeBinary(16),
                SqlType::Uuid,
            ),
        ];
        for (logical, carrier, expected) in declarations {
            // Domain summaries are not the semantic type author, including nested Plain summaries.
            let source = typed(
                "semantic",
                ValueKind::Nominal(logical),
                carrier.clone(),
                false,
                Domain::Plain,
            );
            let ddl = source_columns_to_table_column_defs(std::slice::from_ref(&source)).unwrap();
            assert_eq!(ddl[0].data_type, expected);
            assert!(!ddl[0].nullable);
            let requests = source_columns_to_write_fields(std::slice::from_ref(&source)).unwrap();
            assert_eq!(requests[0].execution_type(), Some(&source.value_type));
            assert_eq!(requests[0].field().data_type(), &carrier);
            assert!(requests[0].field().metadata().as_arc().is_none());
        }
        let nominal = typed(
            "v",
            ValueKind::List(nominal(ValueLogicalType::Json, false)),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            true,
            Domain::Plain,
        );
        assert_eq!(
            source_columns_to_table_column_defs(std::slice::from_ref(&nominal)).unwrap()[0]
                .data_type,
            SqlType::Array(Box::new(SqlType::Json))
        );
        let fields = source_columns_to_write_fields(std::slice::from_ref(&nominal)).unwrap();
        assert_eq!(fields[0].execution_type(), Some(&nominal.value_type));
        let ValueKind::List(child) = fields[0].execution_type().unwrap().value_type().kind() else {
            panic!("expected declared list");
        };
        assert_eq!(child.kind(), &ValueKind::Nominal(ValueLogicalType::Json));
        assert!(!child.nullable());
        assert!(fields[0].field().is_nullable());
        let DataType::List(field) = fields[0].field().data_type() else {
            panic!("expected list layout");
        };
        assert!(field.metadata().as_arc().is_none());
        assert!(!field.is_nullable());
        assert_eq!(
            source_columns_to_table_column_defs(&[plain(
                "bytes",
                DataType::FixedSizeBinary(16),
                false
            )])
            .unwrap()[0]
                .data_type,
            SqlType::Binary
        );
    }

    // ---------- IF NOT EXISTS parser test ----------

    #[test]
    fn parse_create_table_if_not_exists_sets_flag() {
        use novarocks_parser::ast::{DmlStatement, Statement, TableStatement};

        let parsed = novarocks_parser::parse("CREATE TABLE IF NOT EXISTS t AS SELECT 1 AS x")
            .expect("parse");
        let [Statement::Dml(DmlStatement::CreateTableAsSelect(stmt))] = parsed.as_slice() else {
            panic!("expected CTAS");
        };
        let TableStatement::Create(table) = &stmt.table;
        assert!(
            table.if_not_exists,
            "IF NOT EXISTS must set the if_not_exists field to true"
        );
        assert_eq!(
            novarocks_parser::printer::print_query(&stmt.query),
            "SELECT 1 AS x"
        );
    }

    #[test]
    fn parse_create_table_without_if_not_exists_flag_is_false() {
        use novarocks_parser::ast::{DmlStatement, Statement, TableStatement};

        let parsed = novarocks_parser::parse("CREATE TABLE t AS SELECT 1 AS x").expect("parse");
        let [Statement::Dml(DmlStatement::CreateTableAsSelect(stmt))] = parsed.as_slice() else {
            panic!("expected CTAS");
        };
        let TableStatement::Create(table) = &stmt.table;
        assert!(
            !table.if_not_exists,
            "without IF NOT EXISTS the flag should be false"
        );
    }
}

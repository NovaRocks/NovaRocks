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

//! CTAS schema derivation: turn a produced Arrow schema into declared table
//! columns. Frontend DML owns CTAS routing and the durable staged-publication
//! saga. The per-type Arrow -> SqlType mapping itself is owned by
//! `novarocks_sql::syntax`, next to its `sql_type_to_arrow_type` inverse.

use novarocks_sql::syntax::TableColumnDef;

pub(crate) fn arrow_schema_to_table_column_defs(
    schema: &arrow::datatypes::Schema,
) -> Result<Vec<TableColumnDef>, String> {
    schema
        .fields()
        .iter()
        .map(|field| {
            let data_type = novarocks_sql::syntax::arrow_data_type_to_sql_type(field.data_type())?;
            Ok(TableColumnDef {
                name: field.name().clone(),
                data_type,
                nullable: field.is_nullable(),
                aggregation: None,
                default: None,
            })
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::arrow_schema_to_table_column_defs;
    use novarocks_catalog::schema::SqlType;

    // ---------- basic scalar types ----------

    #[test]
    fn arrow_schema_to_table_column_defs_basic() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(10, 2), true),
        ]);
        let cols = arrow_schema_to_table_column_defs(&schema).unwrap();
        assert_eq!(cols.len(), 3);

        assert_eq!(cols[0].name, "id");
        assert!(
            matches!(cols[0].data_type, SqlType::Int),
            "expected Int, got {:?}",
            cols[0].data_type
        );
        assert!(cols[0].nullable);
        assert!(cols[0].aggregation.is_none());
        assert!(cols[0].default.is_none());

        assert_eq!(cols[1].name, "name");
        assert!(
            matches!(cols[1].data_type, SqlType::String),
            "expected String, got {:?}",
            cols[1].data_type
        );

        assert_eq!(cols[2].name, "amount");
        assert!(
            matches!(
                cols[2].data_type,
                SqlType::Decimal {
                    precision: 10,
                    scale: 2
                }
            ),
            "expected Decimal(10,2), got {:?}",
            cols[2].data_type
        );
    }

    #[test]
    fn arrow_schema_to_table_column_defs_nullability_propagated() {
        let schema = Schema::new(vec![
            Field::new("required_col", DataType::Int64, false),
            Field::new("optional_col", DataType::Utf8, true),
        ]);
        let cols = arrow_schema_to_table_column_defs(&schema).unwrap();
        assert!(
            !cols[0].nullable,
            "Int64 NOT NULL field should not be nullable"
        );
        assert!(cols[1].nullable, "Utf8 NULL field should be nullable");
    }

    #[test]
    fn arrow_schema_to_table_column_defs_all_primitive_types() {
        let schema = Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i32", DataType::Int32, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("ls", DataType::LargeUtf8, true),
            Field::new("bin", DataType::Binary, true),
            Field::new("lbin", DataType::LargeBinary, true),
            Field::new("d", DataType::Date32, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("t", DataType::Time64(TimeUnit::Microsecond), true),
        ]);
        let cols = arrow_schema_to_table_column_defs(&schema).unwrap();
        assert!(matches!(cols[0].data_type, SqlType::Boolean));
        assert!(matches!(cols[1].data_type, SqlType::TinyInt));
        assert!(matches!(cols[2].data_type, SqlType::SmallInt));
        assert!(matches!(cols[3].data_type, SqlType::Int));
        assert!(matches!(cols[4].data_type, SqlType::BigInt));
        assert!(matches!(cols[5].data_type, SqlType::Float));
        assert!(matches!(cols[6].data_type, SqlType::Double));
        assert!(matches!(cols[7].data_type, SqlType::String));
        assert!(matches!(cols[8].data_type, SqlType::String)); // LargeUtf8
        assert!(matches!(cols[9].data_type, SqlType::Binary));
        assert!(matches!(cols[10].data_type, SqlType::Binary)); // LargeBinary
        assert!(matches!(cols[11].data_type, SqlType::Date));
        assert!(matches!(cols[12].data_type, SqlType::DateTime));
        assert!(matches!(cols[13].data_type, SqlType::Time));
    }

    // ---------- unsupported types ----------

    #[test]
    fn arrow_schema_to_table_column_defs_rejects_unsupported() {
        let schema = Schema::new(vec![
            Field::new("e", DataType::Float16, true), // unsupported
        ]);
        let err = arrow_schema_to_table_column_defs(&schema).unwrap_err();
        assert!(
            err.to_lowercase().contains("not supported"),
            "expected 'not supported' in error, got: {err}"
        );
    }

    #[test]
    fn arrow_schema_to_table_column_defs_rejects_interval() {
        use arrow::datatypes::IntervalUnit;
        let schema = Schema::new(vec![Field::new(
            "iv",
            DataType::Interval(IntervalUnit::DayTime),
            true,
        )]);
        let err = arrow_schema_to_table_column_defs(&schema).unwrap_err();
        assert!(
            err.to_lowercase().contains("not supported"),
            "expected 'not supported' in error, got: {err}"
        );
    }

    // ---------- nested types ----------

    #[test]
    fn arrow_schema_to_table_column_defs_recurses_list() {
        let elem = Field::new("item", DataType::Int64, true);
        let schema = Schema::new(vec![Field::new(
            "ids",
            DataType::List(Arc::new(elem)),
            true,
        )]);
        let cols = arrow_schema_to_table_column_defs(&schema).unwrap();
        assert_eq!(cols.len(), 1);
        assert!(
            matches!(&cols[0].data_type, SqlType::Array(inner) if matches!(inner.as_ref(), SqlType::BigInt)),
            "expected Array(BigInt), got {:?}",
            cols[0].data_type
        );
    }

    #[test]
    fn arrow_schema_to_table_column_defs_recurses_struct_and_list() {
        // Struct{a: Int32, b: Utf8}
        let struct_field = Field::new(
            "meta",
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );
        // List<Int64>
        let list_field = Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            true,
        );
        let schema = Schema::new(vec![struct_field, list_field]);
        let cols = arrow_schema_to_table_column_defs(&schema).unwrap();
        assert_eq!(cols.len(), 2);

        // Verify struct
        let SqlType::Struct(fields) = &cols[0].data_type else {
            panic!("expected Struct, got {:?}", cols[0].data_type);
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].0, "a");
        assert!(matches!(fields[0].1, SqlType::Int));
        assert_eq!(fields[1].0, "b");
        assert!(matches!(fields[1].1, SqlType::String));

        // Verify list
        let SqlType::Array(inner) = &cols[1].data_type else {
            panic!("expected Array, got {:?}", cols[1].data_type);
        };
        assert!(matches!(inner.as_ref(), SqlType::BigInt));
    }

    // ---------- IF NOT EXISTS parser test ----------

    #[test]
    fn parse_create_table_if_not_exists_sets_flag() {
        use novarocks_sql::syntax::{StarRocksDialect, parse_create_table_statement};

        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql("CREATE TABLE IF NOT EXISTS t AS SELECT 1 AS x")
            .expect("parser init");
        let stmt = parse_create_table_statement(&mut parser).expect("parse");
        assert!(
            stmt.if_not_exists,
            "IF NOT EXISTS must set the if_not_exists field to true"
        );
        assert!(stmt.as_select.is_some());
    }

    #[test]
    fn parse_create_table_without_if_not_exists_flag_is_false() {
        use novarocks_sql::syntax::{StarRocksDialect, parse_create_table_statement};

        let mut parser = sqlparser::parser::Parser::new(&StarRocksDialect)
            .try_with_sql("CREATE TABLE t AS SELECT 1 AS x")
            .expect("parser init");
        let stmt = parse_create_table_statement(&mut parser).expect("parse");
        assert!(
            !stmt.if_not_exists,
            "without IF NOT EXISTS the flag should be false"
        );
    }
}

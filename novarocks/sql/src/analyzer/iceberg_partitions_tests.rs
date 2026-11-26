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

use super::iceberg_metadata::{IcebergMetadataDerivedTypes, metadata_table_schema_with};
use crate::analysis::{QueryBody, Relation};
use crate::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable, SqlViewColumn,
};
use crate::planner::table::{
    ScanSource, SqlMetadataTableKind, SqlScanKind, SqlScanSource, SqlTableIdentity,
    SqlTableVersionSelector, TableDef,
};
use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::FunctionValueType;
use std::sync::Arc;

struct Catalog {
    partitioned: bool,
    body_override: Option<&'static str>,
    wrong_token: bool,
    calls: std::cell::RefCell<Vec<(Option<String>, String, String)>>,
}
impl PlannerTableProvider for Catalog {
    fn resolve_table_for_analysis(
        &self,
        _: Option<&str>,
        _: &str,
        _: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        Err("ordinary table lookup must not capture the metadata relation".into())
    }
    fn resolve_quoted_iceberg_metadata(
        &self,
        name: &novarocks_parser::ast::ObjectName,
        catalog: Option<&str>,
        database: &str,
    ) -> Option<crate::catalog::QuotedMetadataTable> {
        crate::compiler::resolve_quoted_iceberg_metadata_for_test(name, catalog, database, self)
    }

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        Some(self)
    }
}
impl IcebergMetadataTableProvider for Catalog {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        db: &str,
        table: &str,
        kind: SqlMetadataTableKind,
    ) -> Result<ResolvedAnalyzerTable, String> {
        assert_eq!(kind, SqlMetadataTableKind::Files);
        self.calls
            .borrow_mut()
            .push((catalog.map(str::to_owned), db.into(), table.into()));
        let partition = self
            .partitioned
            .then(|| DataType::Struct(vec![Field::new("region", DataType::Int32, true)].into()));
        let derived = IcebergMetadataDerivedTypes::try_new(partition, None).unwrap();
        let columns = metadata_table_schema_with(kind, &derived)
            .into_iter()
            .map(|c| novarocks_types::schema::ColumnDef {
                name: c.name,
                data_type: c.data_type,
                nullable: c.nullable,
                write_default: None,
                logical_type: c.logical_type,
            })
            .collect();
        let catalog = catalog.unwrap_or("ice");
        let binding = crate::binding::SqlTableBindingId::new_for_test(61);
        let source = ResolvedAnalyzerTable::from_planner(
            Some(catalog),
            db,
            TableDef {
                name: table.into(),
                columns,
                iceberg_row_lineage_metadata_columns: Vec::new(),
                source: ScanSource::Sql(SqlScanSource::new(
                    binding,
                    SqlTableIdentity::try_new(catalog.into(), db.into(), table.into())?,
                    SqlScanKind::Metadata {
                        kind,
                        version: SqlTableVersionSelector::Current,
                    },
                )),
            },
        );
        let (partition_column, grouping) = if self.partitioned {
            ("partition, ", "GROUP BY partition")
        } else {
            ("", "HAVING count(*) > 0")
        };
        let body = self.body_override.map_or_else(
            || format!(
                r#"SELECT {partition_column}sum(record_count) AS record_count, count(*) AS file_count,
                   sum(file_size_in_bytes) AS total_size FROM "{catalog}"."{db}"."{table}$files"
                   WHERE content = 0 {grouping}"#,
            ),
            str::to_owned,
        );
        let declaration = metadata_table_schema_with(SqlMetadataTableKind::Partitions, &derived)
            .into_iter()
            .map(|column| SqlViewColumn {
                name: column.name.into(),
                value_type: FunctionValueType::new(column.data_type, column.nullable),
            })
            .collect::<Arc<[_]>>();
        let mut attached = crate::planning::catalog::bind_derived_metadata_view(
            source,
            binding,
            &format!("{table}$partitions"),
            body.into(),
            declaration,
        )?;
        if self.wrong_token {
            let derived = Arc::get_mut(attached.derived_view.as_mut().unwrap()).unwrap();
            derived.binding = crate::binding::SqlTableBindingId::new_for_test(62);
        }
        Ok(attached)
    }
}
#[test]
fn partitions_analysis_schema_matches_nullable_sums_and_required_count() {
    for partitioned in [false, true] {
        for sql in [
            "SELECT * FROM \"orders$partitions\"",
            "SELECT p.* FROM DB.\"ORDERS$partitions\" p",
            "SELECT \"orders$partitions\".* FROM ICE.DB.\"ORDERS$partitions\"",
            "WITH orders AS (SELECT 7 AS content) SELECT p.* FROM \"orders$partitions\" p",
            "WITH \"orders$files\" AS (SELECT 7 AS content) SELECT * FROM \"orders$partitions\"",
        ] {
            let catalog = Catalog {
                partitioned,
                body_override: None,
                wrong_token: false,
                calls: Default::default(),
            };
            let query = novarocks_parser::parser::parse_query(sql).unwrap();
            let (resolved, _, _) = super::analyze(&query, &catalog, "db").unwrap();
            let partition = partitioned.then(|| {
                DataType::Struct(vec![Field::new("region", DataType::Int32, true)].into())
            });
            let expected = metadata_table_schema_with(
                SqlMetadataTableKind::Partitions,
                &IcebergMetadataDerivedTypes::try_new(partition, None).unwrap(),
            );
            assert_eq!(resolved.output_columns.len(), expected.len(), "{sql}");
            for (column, expected) in resolved.output_columns.iter().zip(expected) {
                assert_eq!(column.name, expected.name, "{sql}");
                assert_eq!(column.value_type.data_type(), &expected.data_type, "{sql}");
                assert_eq!(column.value_type.nullable(), expected.nullable, "{sql}");
            }
            let QueryBody::Select(outer) = &resolved.body else {
                panic!("select")
            };
            let Relation::Subquery { query: view, .. } = outer.from.as_ref().unwrap() else {
                panic!("partitions view")
            };
            let QueryBody::Select(inner) = &view.body else {
                panic!("view select")
            };
            assert!(inner.filter.is_some());
            assert_eq!(inner.group_by.len(), usize::from(partitioned));
            assert_eq!(inner.having.is_some(), !partitioned);
            let Relation::IcebergMetadataScan(scan) = inner.from.as_ref().unwrap() else {
                panic!("Files scan")
            };
            assert_eq!(scan.metadata_table_type, SqlMetadataTableKind::Files);
            let ScanSource::Sql(source) = &scan.table.source;
            assert_eq!(
                source.binding,
                crate::binding::SqlTableBindingId::new_for_test(61)
            );
            let calls = catalog.calls.borrow();
            assert_eq!(calls.len(), 1, "prebound body must not reload Files: {sql}");
        }
    }
}

#[test]
fn partitions_generated_body_requires_exactly_one_prebound_source_and_output_declaration() {
    let query =
        novarocks_parser::parser::parse_query(r#"SELECT * FROM "orders$partitions""#).unwrap();
    for body in [
        "SELECT",
        "SELECT 1 AS record_count, 1 AS file_count, 1 AS total_size",
        r#"SELECT * FROM "ice"."db"."another$files""#,
        r#"SELECT * FROM "orders$files""#,
        r#"SELECT * FROM "ice"."db"."orders$files" a JOIN "ice"."db"."orders$files" b ON true"#,
        r#"SELECT CAST(1 AS BIGINT) AS record_count, count(*) AS file_count,
                 sum(file_size_in_bytes) AS total_size FROM "ice"."db"."orders$files""#,
    ] {
        let catalog = Catalog {
            partitioned: false,
            body_override: Some(body),
            wrong_token: false,
            calls: Default::default(),
        };
        let error = super::analyze(&query, &catalog, "db").unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::Internal,
            "{body}"
        );
        assert_eq!(catalog.calls.borrow().len(), 1);
    }
    let catalog = Catalog {
        partitioned: false,
        body_override: None,
        wrong_token: true,
        calls: Default::default(),
    };
    let error = super::analyze(&query, &catalog, "db").unwrap_err();
    assert_eq!(
        error.kind(),
        crate::analyze_error::AnalyzeErrorKind::Internal
    );
    assert!(error.message().contains("differs from its source binding"));
}

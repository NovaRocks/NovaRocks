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

//! SQL-owned partition aggregation over the same frozen Files binding.

use novarocks_parser::{Span, ast};
use novarocks_types::naming::TableIdentity;

fn ident(value: &str, span: Span) -> ast::Ident {
    ast::Ident {
        value: value.into(),
        quoted: false,
        span,
    }
}
fn column(value: &str, span: Span) -> ast::Expr {
    ast::Expr::Identifier(ident(value, span))
}
fn call(name: &str, argument: &str, span: Span) -> ast::Expr {
    ast::Expr::FunctionCall(ast::AstBox::new(ast::FunctionCall {
        name: ast::ObjectName {
            parts: vec![ident(name, span)],
            span,
        },
        arguments: vec![novarocks_parser::build::call_argument(
            column(argument, span),
            None,
        )]
        .into(),
        distinct: false,
        processing_mode: None,
        order_by: None,
        filter: None,
        null_treatment: None,
        window: None,
        span,
    }))
}
fn number(value: &str, span: Span) -> ast::Expr {
    ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::Long {
            spelling: value.into(),
            parsed_value: value
                .parse::<i64>()
                .expect("partition integer constructor receives a signed integer"),
        },
        span,
    })
}
fn comparison(
    left: ast::Expr,
    operator: ast::ComparisonOperator,
    right: ast::Expr,
    span: Span,
) -> ast::Expr {
    ast::Expr::Predicated(ast::AstBox::new(ast::PredicatedExpr {
        value: left,
        predicate: ast::Predicate::Comparison(ast::ComparisonPredicate {
            operator,
            right,
            span,
        }),
        span,
    }))
}

/// Preserve metadata syntax separately from the ordinary relation name, so
/// an equal table or CTE name cannot shadow the frozen metadata relation.
pub(super) fn partitions_query(
    name: &ast::ObjectName,
    partitioned: bool,
    qualified: Option<&TableIdentity>,
) -> ast::Query {
    let span = name.span;
    let source = if let Some(base) = qualified {
        // Keep the generated reference on the exact qualified Files fact.
        // Its full name also prevents an outer CTE from shadowing this read.
        ast::ObjectName {
            parts: vec![
                ident(&base.catalog, span),
                ident(&base.namespace, span),
                ast::Ident {
                    value: format!("{}$files", base.table),
                    quoted: true,
                    span,
                },
            ],
            span,
        }
    } else {
        let mut source = name.clone();
        for part in &mut source.parts {
            part.value = part.value.to_lowercase();
        }
        if let Some(last) = source.parts.last_mut() {
            let base = super::TableNameView::new(name)
                .parts()
                .last()
                .expect("the last identifier requires a nonempty table name")
                .to_lowercase();
            last.value = format!("{base}$files");
            last.quoted = false;
        }
        source
    };
    let mut projection = Vec::new();
    if partitioned {
        projection.push(novarocks_parser::build::item(
            column("partition", span),
            None,
        ));
    }
    for (name, argument, alias) in [
        ("sum", "record_count", "record_count"),
        ("count", "*", "file_count"),
        ("sum", "file_size_in_bytes", "total_size"),
    ] {
        projection.push(ast::SelectItem::SingleColumn(ast::AstBox::new(
            ast::SingleColumn {
                expression: call(name, argument, span),
                alias: Some(ident(alias, span)),
                span,
            },
        )));
    }
    ast::Query {
        session_properties: Vec::new().into(),
        functions: Vec::new().into(),
        with: None,
        body: ast::AstBox::new(ast::SetExpr::Select(ast::AstBox::new(ast::Select {
            distinct: false,
            projection: projection.into(),
            from: Some(novarocks_parser::build::table(source, None, None)),
            selection: Some(comparison(
                column("content", span),
                ast::ComparisonOperator::Equal,
                number("0", span),
                span,
            )),
            group_by: if partitioned {
                novarocks_parser::build::group_by(vec![column("partition", span)])
            } else {
                None
            },
            having: (!partitioned).then(|| {
                comparison(
                    call("count", "*", span),
                    ast::ComparisonOperator::GreaterThan,
                    number("0", span),
                    span,
                )
            }),
            windows: Vec::new().into(),
            modifiers: Default::default(),
            span,
        }))),
        modifiers: Default::default(),
        span,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn partitions_ast_keeps_metadata_identity_and_empty_group_policy() {
        for parts in [
            vec!["Orders"],
            vec!["DB", "Orders"],
            vec!["ICE", "DB", "Orders"],
        ] {
            let span = Span::new(0, 1);
            let name = ast::ObjectName {
                parts: parts.iter().map(|p| ident(p, span)).collect(),
                span,
            };
            for partitioned in [true, false] {
                let query = partitions_query(&name, partitioned, None);
                let ast::SetExpr::Select(select) = query.body.as_ref() else {
                    panic!("select")
                };
                let Some(ast::Relation::Table(table)) = &select.from else {
                    panic!("metadata table")
                };
                let view = crate::analyzer::TableNameView::new(&table.name);
                assert_eq!(
                    view.parts().collect::<Vec<_>>(),
                    parts.iter().map(|p| p.to_lowercase()).collect::<Vec<_>>()
                );
                assert_eq!(view.metadata.unwrap().value, "files");
                let ast::Expr::Predicated(filter) = select.selection.as_ref().unwrap() else {
                    panic!("content filter")
                };
                assert_eq!(filter.value, column("content", span));
                let ast::Predicate::Comparison(predicate) = &filter.predicate else {
                    panic!("comparison")
                };
                assert_eq!(predicate.operator, ast::ComparisonOperator::Equal);
                assert_eq!(predicate.right, number("0", span));
                assert_eq!(select.projection.len(), if partitioned { 4 } else { 3 });
                if partitioned {
                    assert!(
                        crate::analyzer::simple_group_by_expressions(&select.group_by).unwrap()
                            == vec![&column("partition", span)]
                    );
                    assert!(select.having.is_none());
                } else {
                    assert!(select.group_by.is_none());
                    assert_eq!(
                        select.having,
                        Some(comparison(
                            call("count", "*", span),
                            ast::ComparisonOperator::GreaterThan,
                            number("0", span),
                            span
                        ))
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod analysis_tests {
    use super::super::iceberg_metadata::{IcebergMetadataDerivedTypes, metadata_table_schema_with};
    use crate::analysis::{QueryBody, Relation};
    use crate::catalog::{
        IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable,
    };
    use crate::planner::table::{
        ScanSource, SqlMetadataTableKind, SqlScanKind, SqlScanSource, SqlTableIdentity,
        SqlTableVersionSelector, TableDef,
    };
    use arrow::datatypes::{DataType, Field};
    struct Catalog {
        partitioned: bool,
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
            let partition = self.partitioned.then(|| {
                DataType::Struct(vec![Field::new("region", DataType::Int32, true)].into())
            });
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
            Ok(ResolvedAnalyzerTable::from_planner(
                catalog,
                db,
                TableDef {
                    name: table.into(),
                    columns,
                    iceberg_row_lineage_metadata_columns: Vec::new(),
                    source: ScanSource::Sql(SqlScanSource::new(
                        crate::binding::SqlTableBindingId::new_for_test(61),
                        SqlTableIdentity::try_new(
                            catalog.unwrap_or("ice").into(),
                            db.into(),
                            table.into(),
                        )?,
                        SqlScanKind::Metadata {
                            kind,
                            version: SqlTableVersionSelector::Current,
                        },
                    )),
                },
            ))
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
            ] {
                let catalog = Catalog {
                    partitioned,
                    calls: Default::default(),
                };
                let query = novarocks_parser::parser::parse_query(sql).unwrap();
                let (resolved, _, _) = super::super::analyze(&query, &catalog, "db").unwrap();
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
                assert_eq!(calls.len(), 2);
                assert_eq!(
                    calls[0], calls[1],
                    "schema and scan must name the same Files binding: {sql}"
                );
            }
        }
    }
}

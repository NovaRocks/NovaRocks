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

fn ident(value: &str, span: Span) -> ast::Ident {
    ast::Ident {
        value: value.into(),
        quoted: false,
        quote_style: None,
        span,
    }
}
fn column(value: &str, span: Span) -> ast::Expr {
    ast::Expr::Identifier(ident(value, span))
}
fn call(name: &str, argument: &str, span: Span) -> ast::Expr {
    ast::Expr::FunctionCall(ast::FunctionCall {
        name: ast::ObjectName {
            parts: vec![ident(name, span)],
            span,
        },
        arguments: vec![column(argument, span)],
        quantifier: ast::FunctionQuantifier::None,
        order_by: Vec::new(),
        separator: None,
        filter: None,
        null_treatment: None,
        over: None,
        substring_from_syntax: false,
        span,
    })
}
fn number(value: &str, span: Span) -> ast::Expr {
    ast::Expr::Literal(ast::Literal {
        kind: ast::LiteralKind::Number(value.into()),
        span,
    })
}
fn comparison(
    left: ast::Expr,
    operator: ast::BinaryOperator,
    right: ast::Expr,
    span: Span,
) -> ast::Expr {
    ast::Expr::Binary(ast::BinaryExpr {
        left: Box::new(left),
        operator,
        right: Box::new(right),
        span,
    })
}

/// Preserve metadata syntax separately from the ordinary relation name, so
/// an equal table or CTE name cannot shadow the frozen metadata relation.
pub(super) fn partitions_query(name: &ast::ObjectName, partitioned: bool) -> ast::Query {
    let span = name.span;
    let mut source = name.clone();
    for part in &mut source.parts {
        part.value = part.value.to_lowercase();
    }
    let mut projection = Vec::new();
    if partitioned {
        projection.push(ast::SelectItem::UnnamedExpr(column("partition", span)));
    }
    for (name, argument, alias) in [
        ("sum", "record_count", "record_count"),
        ("count", "*", "file_count"),
        ("sum", "file_size_in_bytes", "total_size"),
    ] {
        projection.push(ast::SelectItem::ExprWithAlias {
            expr: call(name, argument, span),
            alias: ident(alias, span),
            explicit_as: true,
            span,
        });
    }
    ast::Query {
        with: None,
        body: Box::new(ast::SetExpr::Select(Box::new(ast::Select {
            hints: Vec::new(),
            quantifier: ast::SelectQuantifier::None,
            projection,
            from: vec![ast::TableWithJoins {
                relation: ast::TableFactor::Table {
                    name: source,
                    metadata: Some(ident("files", span)),
                    alias: None,
                    version: None,
                    span,
                },
                joins: Vec::new(),
                span,
            }],
            selection: Some(comparison(
                column("content", span),
                ast::BinaryOperator::Equal,
                number("0", span),
                span,
            )),
            group_by: if partitioned {
                ast::GroupBy::Expressions {
                    expressions: vec![column("partition", span)],
                    span,
                }
            } else {
                ast::GroupBy::None
            },
            having: (!partitioned).then(|| {
                comparison(
                    call("count", "*", span),
                    ast::BinaryOperator::GreaterThan,
                    number("0", span),
                    span,
                )
            }),
            qualify: None,
            windows: Vec::new(),
            span,
        }))),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        limit_comma_offset: false,
        fetch: None,
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
                let query = partitions_query(&name, partitioned);
                let ast::SetExpr::Select(select) = query.body.as_ref() else {
                    panic!("select")
                };
                let ast::TableFactor::Table {
                    name: inner,
                    metadata,
                    ..
                } = &select.from[0].relation
                else {
                    panic!("metadata table")
                };
                assert_eq!(
                    inner
                        .parts
                        .iter()
                        .map(|p| p.value.as_str())
                        .collect::<Vec<_>>(),
                    parts.iter().map(|p| p.to_lowercase()).collect::<Vec<_>>()
                );
                assert_eq!(metadata.as_ref().unwrap().value, "files");
                let ast::Expr::Binary(filter) = select.selection.as_ref().unwrap() else {
                    panic!("content filter")
                };
                assert_eq!(*filter.left, column("content", span));
                assert_eq!(filter.operator, ast::BinaryOperator::Equal);
                assert_eq!(*filter.right, number("0", span));
                assert_eq!(select.projection.len(), if partitioned { 4 } else { 3 });
                if partitioned {
                    assert!(
                        matches!(&select.group_by, ast::GroupBy::Expressions { expressions, .. } if expressions == &vec![column("partition", span)])
                    );
                    assert!(select.having.is_none());
                } else {
                    assert!(matches!(select.group_by, ast::GroupBy::None));
                    assert_eq!(
                        select.having,
                        Some(comparison(
                            call("count", "*", span),
                            ast::BinaryOperator::GreaterThan,
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
    use super::*;
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
                "SELECT * FROM orders$partitions",
                "SELECT p.* FROM DB.ORDERS$partitions p",
                "SELECT `orders$partitions`.* FROM ICE.DB.ORDERS$partitions",
                "WITH orders AS (SELECT 7 AS content) SELECT p.* FROM orders$partitions p",
            ] {
                let catalog = Catalog {
                    partitioned,
                    calls: Default::default(),
                };
                let mut parsed = novarocks_parser::parse(sql).unwrap();
                let ast::Statement::Query(query) = parsed.remove(0) else {
                    panic!("query")
                };
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

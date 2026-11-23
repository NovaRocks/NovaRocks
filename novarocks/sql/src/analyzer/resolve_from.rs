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

use crate::parser::expression_for_diagnostic;
use arrow::datatypes::DataType;
use novarocks_parser::ast;

use crate::analysis::AnalyzedOutputField as OutputColumn;
use crate::analysis::*;
use crate::analyze_error::AnalyzeError;
use crate::column_id::ColumnId;
use crate::planner::table::TableDef;
use crate::planning::catalog::MetadataTableKind;
use crate::system_table::{StatementSystemRelation, SystemTableDeclaration};
use std::sync::Arc;

use super::scope::AnalyzerScope;

#[derive(Clone, Copy)]
pub(super) struct RelationAlias<'r> {
    pub(super) name: &'r ast::Ident,
    pub(super) columns: &'r [ast::Ident],
    pub(super) span: novarocks_parser::Span,
}
impl<'r> RelationAlias<'r> {
    fn from_node(node: &'r ast::AliasedRelation) -> Result<Self, AnalyzeError> {
        if node.column_names.as_ref().is_some_and(Vec::is_empty) {
            return Err(AnalyzeError::unsupported_query_shape(
                "empty relation column aliases are not supported",
                node.span,
            ));
        }
        Ok(Self {
            name: &node.alias,
            columns: node.column_names.as_deref().unwrap_or(&[]),
            span: node.span,
        })
    }
}
#[derive(Clone, Copy)]
struct TableArgument<'r> {
    name: Option<&'r ast::Ident>,
    expression: &'r ast::Expr,
    span: novarocks_parser::Span,
}
impl TableArgument<'_> {
    fn span(&self) -> novarocks_parser::Span {
        self.span
    }
}

#[derive(Clone, Copy)]
pub(crate) struct MetadataSuffix<'r> {
    pub(crate) value: &'r str,
    pub(crate) span: novarocks_parser::Span,
}
/// Borrow the current table lookup's base-name parts from the actual AST.
/// This view does not choose a default relation reference qualifier. The P12
/// quoted-metadata cutover must audit lookup and reference scopes separately.
pub struct TableNameView<'r> {
    name: &'r ast::ObjectName,
    base: &'r str,
    pub(crate) metadata: Option<MetadataSuffix<'r>>,
    pub(crate) base_span: novarocks_parser::Span,
}
impl<'r> TableNameView<'r> {
    pub fn new(name: &'r ast::ObjectName) -> Self {
        let mut view = Self {
            name,
            base: name.parts.last().map_or("", |part| part.value.as_str()),
            metadata: None,
            base_span: name.span,
        };
        if let Some(last) = name.parts.last()
            && !last.quoted
            && let Some((base, suffix)) = last.value.split_once('$')
            && !base.is_empty()
            && !suffix.is_empty()
        {
            view.base = base;
            let precise = !last.span.is_empty()
                && last.span.end() - last.span.start() == last.value.len()
                && name.span.start() <= last.span.start()
                && last.span.end() <= name.span.end();
            let suffix_span = if precise {
                let split = last.span.start() + base.len();
                view.base_span = novarocks_parser::Span::new(name.span.start(), split);
                novarocks_parser::Span::new(split + 1, last.span.end())
            } else {
                last.span
            };
            view.metadata = Some(MetadataSuffix {
                value: suffix,
                span: suffix_span,
            });
        }
        view
    }
    pub fn parts(&self) -> impl Iterator<Item = &'r str> + 'r {
        let parts: &'r [ast::Ident] = &self.name.parts;
        let last = parts.len().saturating_sub(1);
        let base = self.base;
        parts.iter().enumerate().map(move |(index, part)| {
            if index == last {
                base
            } else {
                part.value.as_str()
            }
        })
    }
}

impl<'a> super::AnalyzerContext<'a> {
    pub(super) fn analyze_from(
        &self,
        relation: &ast::Relation,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        self.analyze_from_with_outer(relation, None)
    }

    #[recursive::recursive]
    pub(super) fn analyze_from_with_outer(
        &self,
        relation: &ast::Relation,
        outer_scope: Option<&AnalyzerScope>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        self.analyze_relation_with_alias(relation, outer_scope, None)
    }

    #[recursive::recursive]
    #[inline(never)]
    fn analyze_join_after_left(
        &self,
        join: &ast::Join,
        current_rel: Relation,
        mut current_scope: AnalyzerScope,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        self.check_control()?;
        let (right_rel, mut right_scope) =
            self.analyze_from_with_outer(&join.right, Some(&current_scope))?;
        let (join_kind, constraint) = parse_join_operator(join.kind, join.criteria.as_ref());

        let mut using_output = Vec::new();
        let condition = match constraint {
            Some(ast::JoinCriteria::On(on)) => {
                // Build a merged scope for analyzing the ON condition
                let mut merged = self.new_scope();
                merged.merge(&current_scope);
                merged.merge(&right_scope);
                Some(self.require_boolean_predicate(
                    self.analyze_expr(&on.expression, &merged)?,
                    "JOIN ON clause",
                    on.expression.span(),
                )?)
            }
            Some(ast::JoinCriteria::Using(ast::JoinUsing { columns, .. })) => {
                // Convert USING(col1, col2) to ON left.col1 = right.col1 AND ...
                //
                // Each USING column resolves to *different* ColumnIds on
                // each side of the join (the LEFT child's binding and the
                // RIGHT child's binding). We build the BinaryOp with
                // distinct left/right ColumnRefs so that downstream
                // optimizer passes (in particular `shuffle_join_column_ids`
                // and HashPartitioned distribution derivation) see both
                // ids as the join key on its respective side. Using the
                // same merged id on both sides causes the optimizer to
                // produce a distribution spec keyed on only one id, which
                // fragment-builder cannot resolve against the other
                // child's scope.
                let mut conds = Vec::new();
                let mut seen = std::collections::HashSet::new();
                for col_obj in columns {
                    self.check_control()?;
                    let col_name = col_obj.value.clone();
                    if !seen.insert(col_name.to_lowercase()) {
                        return Err(AnalyzeError::duplicate_column_name(
                            format!("Column '{col_name}' appears multiple times in USING clause"),
                            col_obj.span,
                        ));
                    }
                    let (left_id, left_type) =
                        current_scope.resolve_at(None, &col_name, col_obj.span)?;
                    let (right_id, right_type) =
                        right_scope.resolve_at(None, &col_name, col_obj.span)?;
                    // After a previous FULL OUTER USING the left side
                    // carries a synthetic COALESCE for this column —
                    // `coalesce(prev_left.id, prev_right.id)` — because
                    // either side may be NULL-padded. The bare
                    // `left.id` reference returned by
                    // `current_scope.resolve` only sees ONE specific
                    // side's binding, so a row whose id originated on
                    // the other side compares as `NULL = right.id`
                    // and never joins. Use the computed column when it
                    // exists so the chained join matches on the merged
                    // value (`coalesce(coalesce(t1.id, t2.id), t3.id)`,
                    // and so on).
                    let left_ref = if let Some(expr) = current_scope.computed_column_for(&col_name)
                    {
                        expr.clone()
                    } else {
                        TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: left_id,
                                qualifier: None,
                                column: col_name.clone(),
                            },
                            value_type: left_type,
                        }
                    };
                    let right_ref = right_scope
                        .computed_column_for(&col_name)
                        .cloned()
                        .unwrap_or(TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: right_id,
                                qualifier: None,
                                column: col_name.clone(),
                            },
                            value_type: right_type,
                        });
                    let common = self.require_common_trino_type(
                        &left_ref.value_type,
                        &right_ref.value_type,
                        col_obj.span,
                    )?;
                    let coerce = |value| self.coerce_common_value(value, &common, col_obj.span);
                    let (left_key, right_key) =
                        (coerce(left_ref.clone())?, coerce(right_ref.clone())?);
                    let output = match join_kind {
                        JoinKind::FullOuter => {
                            let padded = |mut expr: TypedExpr| {
                                expr.value_type =
                                    super::helpers::with_nullability(expr.value_type, true);
                                coerce(expr)
                            };
                            super::resolve_expr::resolved_scalar_call_at(
                                self.function_catalog,
                                "coalesce",
                                vec![padded(left_ref)?, padded(right_ref)?],
                                col_obj.span,
                                self.sql_semantics.sql_mode().decimal_overflow_policy(),
                                self.constant_policy,
                                self.control,
                            )?
                        }
                        JoinKind::RightOuter | JoinKind::RightSemi | JoinKind::RightAnti => {
                            right_key.clone()
                        }
                        _ => left_key.clone(),
                    };
                    using_output.push((col_name, left_id, right_id, output));
                    let legacy_key = |key: &TypedExpr| {
                        use crate::common::binary_operator::{
                            requires_legacy_wide, uses_structural_comparison,
                        };
                        requires_legacy_wide(key.value_type.value_type())
                            || uses_structural_comparison(key.value_type.value_type())
                    };
                    if legacy_key(&left_key) || legacy_key(&right_key) {
                        conds.push(TypedExpr {
                            value_type: novarocks_type_contract::FunctionValueType::new(
                                DataType::Boolean,
                                false,
                            ),

                            kind: ExprKind::BinaryOp {
                                left: novarocks_parser::ast::AstBox::new(left_key),
                                op: BinOp::Eq,
                                right: novarocks_parser::ast::AstBox::new(right_key),
                                decimal_overflow_policy:
                                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                            },
                        });
                    } else {
                        conds.push(self.analyze_binary_operator(
                            novarocks_type_contract::OperatorFunction::Equal,
                            left_key,
                            right_key,
                            col_obj.span,
                        )?);
                    }
                }
                if conds.is_empty() {
                    None
                } else {
                    let mut result = conds.pop().unwrap();
                    while let Some(prev) = conds.pop() {
                        self.check_control()?;
                        result = TypedExpr {
                            value_type: novarocks_type_contract::FunctionValueType::new(
                                DataType::Boolean,
                                false,
                            ),

                            kind: ExprKind::BinaryOp {
                                left: novarocks_parser::ast::AstBox::new(prev),
                                op: BinOp::And,
                                right: novarocks_parser::ast::AstBox::new(result),
                                decimal_overflow_policy:
                                    novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                            },
                        };
                    }
                    Some(result)
                }
            }
            Some(ast::JoinCriteria::Natural(ast::NaturalJoin { span })) => {
                return Err(AnalyzeError::unsupported_query_shape(
                    "NATURAL JOIN is not yet supported",
                    *span,
                ));
            }
            None => None,
        };

        let prefer_right = matches!(join_kind, JoinKind::RightOuter);
        match join_kind {
            JoinKind::LeftSemi | JoinKind::LeftAnti => {}
            JoinKind::RightSemi | JoinKind::RightAnti => current_scope = right_scope,
            _ => {
                match join_kind {
                    JoinKind::LeftOuter => right_scope.mark_all_nullable(),
                    JoinKind::RightOuter => current_scope.mark_all_nullable(),
                    JoinKind::FullOuter => {
                        current_scope.mark_all_nullable();
                        right_scope.mark_all_nullable();
                    }
                    _ => {}
                }
                current_scope.merge(&right_scope);
            }
        }
        if !using_output.is_empty() {
            current_scope.apply_using_layout(&using_output, prefer_right);
            current_scope.publish_using_columns(using_output);
        }
        let current_rel = Relation::Join(novarocks_parser::ast::AstBox::new(JoinRelation {
            naa_author: None,
            left: current_rel,
            right: right_rel,
            join_type: join_kind,
            condition,
        }));

        Ok((current_rel, current_scope))
    }

    #[recursive::recursive]
    fn analyze_relation_with_alias(
        &self,
        factor: &ast::Relation,
        outer_scope: Option<&AnalyzerScope>,
        alias: Option<RelationAlias<'_>>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        self.check_control()?;
        match factor {
            ast::Relation::Aliased(node) => {
                if alias.is_some() {
                    return Err(AnalyzeError::unsupported_query_shape(
                        "nested relation aliases are not supported",
                        node.span,
                    ));
                }
                let alias = RelationAlias::from_node(node)?;
                self.analyze_relation_with_alias(&node.relation, outer_scope, Some(alias))
            }
            ast::Relation::Join(join) => {
                if alias.is_some() {
                    return Err(AnalyzeError::unsupported_query_shape(
                        "alias on parenthesized JOIN is not yet supported",
                        join.span,
                    ));
                }
                let (left, scope) = self.analyze_from_with_outer(&join.left, outer_scope)?;
                self.analyze_join_after_left(join, left, scope)
            }
            ast::Relation::Table(node) => self.analyze_table_relation(node, alias),
            ast::Relation::TableSubquery(node) => {
                self.analyze_derived_relation(&node.query, alias, node.span)
            }
            ast::Relation::Lateral(node) => {
                self.analyze_derived_relation(&node.query, alias, node.span)
            }
            ast::Relation::Unnest(node) => self.analyze_unnest(
                &node.expressions,
                alias,
                node.with_ordinality,
                node.span,
                outer_scope,
            ),
            ast::Relation::TableFunction(node) => self.analyze_table_function(node, alias),
            ast::Relation::JsonTable(_) => Err(AnalyzeError::unsupported_expression(
                "JSON_TABLE is not supported",
                factor.span(),
            )),
            ast::Relation::PatternRecognition(_)
            | ast::Relation::Sampled(_)
            | ast::Relation::Pivot(_)
            | ast::Relation::Nearest(_) => Err(AnalyzeError::unsupported_expression(
                "relation profile is not supported",
                factor.span(),
            )),
        }
    }

    pub(super) fn analyze_table_query(
        &self,
        table: &ast::Table,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        self.check_control()?;
        self.analyze_table_relation(table, None)
    }

    fn analyze_iceberg_partitions(
        &self,
        name: &ast::ObjectName,
        files: &TableDef,
        alias: Option<RelationAlias<'_>>,
        qualifier: String,
        qualified: Option<&novarocks_types::naming::TableIdentity>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        let partitioned = files
            .columns
            .iter()
            .any(|column| column.name.eq_ignore_ascii_case("partition"));
        let source = super::iceberg_partitions::partitions_query(name, partitioned, qualified);
        let query = self.analyze_query(&source)?;
        let output_columns = derived_table_output_columns(&query.output_columns, alias)?;
        let alias_name = alias
            .map(|alias| alias.name.value.clone())
            .unwrap_or(qualifier);
        let mut scope = self.new_scope();
        for column in &output_columns {
            self.check_control()?;
            scope.add_field(Some(&alias_name), column);
        }
        Ok((
            Relation::Subquery {
                query: novarocks_parser::ast::AstBox::new(query),
                alias: alias_name,
                output_columns,
            },
            scope,
        ))
    }

    fn analyze_table_relation(
        &self,
        node: &ast::Table,
        alias: Option<RelationAlias<'_>>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        if let Some((site, source)) = self.statement_relation
            && std::ptr::eq(site, node)
        {
            return self.analyze_system_table(
                source.declaration.clone(),
                alias,
                node.name
                    .parts
                    .last()
                    .map(|part| part.value.as_str())
                    .unwrap_or(&source.declaration.table),
                Some(source.kind),
            );
        }
        let name = &node.name;
        let version = node.query_period.as_ref();
        if let Some(period) = version {
            super::iceberg_ref::query_period_end(period)?;
        }
        if node.branch.is_some() {
            return Err(AnalyzeError::unsupported_query_shape(
                "table branches in FROM are not supported",
                node.span,
            ));
        }
        let view = TableNameView::new(name);
        let parts: Vec<String> = view.parts().map(str::to_owned).collect();
        let metadata = view.metadata;
        let name_span = view.base_span;

        // `$<metatype>` is a parser-owned relation field. Capability
        // admission stays in analysis, after catalog resolution.
        if let Some(metadata) = metadata {
            if let Some(version) = version {
                return Err(AnalyzeError::unsupported_query_shape(
                    "time travel is not supported on metadata relations",
                    version.span,
                ));
            }
            let metadata_ty = crate::planner::table::SqlMetadataTableKind::parse(metadata.value)
                .map_err(|_| {
                    AnalyzeError::unsupported_query_shape(
                        format!(
                            "unsupported iceberg metadata table type: {}",
                            metadata.value
                        ),
                        metadata.span,
                    )
                })?;
            // Reject branch/tag combo: `t.branch_dev$snapshots` is meaningless.
            if let Some(last) = parts.last()
                && (last.starts_with("branch_") || last.starts_with("tag_"))
            {
                return Err(AnalyzeError::invalid_query_shape(
                    format!(
                        "iceberg metadata table cannot be combined with branch/tag suffix: {parts:?}"
                    ),
                    name_span,
                ));
            }

            let (catalog_override, db_lower, tbl_lower) = match parts.as_slice() {
                [tbl] => (
                    None,
                    self.current_database.to_lowercase(),
                    tbl.to_lowercase(),
                ),
                [db, tbl] => (None, db.to_lowercase(), tbl.to_lowercase()),
                [cat, db, tbl] => (
                    Some(cat.to_lowercase()),
                    db.to_lowercase(),
                    tbl.to_lowercase(),
                ),
                _ => {
                    return Err(AnalyzeError::invalid_query_shape(
                        format!(
                            "iceberg metadata table requires <tbl> | <db>.<tbl> | <cat>.<db>.<tbl>, got: {parts:?}"
                        ),
                        name_span,
                    ));
                }
            };

            let metadata_provider = self.catalog.iceberg_metadata_provider().ok_or_else(|| {
                AnalyzeError::unsupported_query_shape(
                    "iceberg metadata table lookup is not supported by this catalog",
                    name_span,
                )
            })?;
            let table_def = metadata_provider
                .get_iceberg_metadata_table(
                    catalog_override.as_deref(),
                    &db_lower,
                    &tbl_lower,
                    metadata_ty.provider_kind(),
                )
                .map_err(|error| AnalyzeError::unknown_table(error, name_span))?
                .planner;
            if metadata_ty == MetadataTableKind::Partitions {
                return self.analyze_iceberg_partitions(
                    name,
                    &table_def,
                    alias,
                    format!("{tbl_lower}$partitions"),
                    None,
                );
            }
            return self.analyze_iceberg_metadata_scan(
                db_lower,
                table_def,
                metadata_ty,
                alias.map(|a| a.name.value.clone()),
            );
        }

        let (catalog_override, db, tbl) = match parts.len() {
            1 => (None, self.current_database.to_string(), parts[0].clone()),
            2 => (None, parts[0].clone(), parts[1].clone()),
            3 => (
                Some(parts[0].to_lowercase()),
                parts[1].clone(),
                parts[2].clone(),
            ),
            _ => {
                return Err(AnalyzeError::invalid_query_shape(
                    format!("unsupported table name: {name:?}"),
                    name_span,
                ));
            }
        };
        let db_lower = db.to_lowercase();
        let tbl_lower = tbl.to_lowercase();

        if parts.len() == 1 {
            if self.pending_ctes.contains(&tbl_lower) {
                return Err(AnalyzeError::invalid_query_shape(
                    format!("forward CTE reference is not supported: {tbl_lower}"),
                    name_span,
                ));
            }

            if let Some(&cte_id) = self.ctes.get(&tbl_lower) {
                if let Some(version) = version {
                    return Err(AnalyzeError::unsupported_query_shape(
                        "time travel is not supported on a CTE relation",
                        version.span,
                    ));
                }
                let (producer_columns, entry_id) = {
                    let registry = self.cte_registry.borrow();
                    let entry = registry.get(cte_id).ok_or_else(|| {
                        AnalyzeError::internal(format!("unknown CTE id: {cte_id}"))
                    })?;
                    (entry.output_columns.clone(), entry.id)
                };
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| tbl.clone());
                // Each CTE consume must mint fresh ColumnIds. The
                // producer's ColumnIds are owned by the body of the
                // WITH definition; if multiple consumes shared them,
                // downstream operators could not tell aliases apart
                // (e.g. `cte a, cte b WHERE a.x=1 AND b.x=2`).
                let producer_column_ids: Vec<ColumnId> = producer_columns
                    .iter()
                    .map(|col| {
                        self.check_control()?;
                        Ok(col.column_id)
                    })
                    .collect::<Result<Vec<_>, AnalyzeError>>()?;
                let mut output_columns: Vec<OutputColumn> = producer_columns
                    .into_iter()
                    .map(|col| {
                        self.check_control()?;
                        let new_id = self.alloc_column_id(
                            Some(alias_name.clone()),
                            col.name.clone(),
                            col.value_type.clone(),
                        );
                        // A consume changes only the symbol identity. Carry
                        // the producer's complete same-value source facts.
                        self.factory
                            .borrow_mut()
                            .transfer_value_provenance(col.column_id, new_id)
                            .map_err(AnalyzeError::internal)?;
                        Ok(OutputColumn {
                            field_name: col.field_name.clone(),
                            column_id: new_id,
                            name: col.name,
                            value_type: col.value_type.clone(),

                            is_internal: false,
                        })
                    })
                    .collect::<Result<Vec<_>, AnalyzeError>>()?;
                if let Some(alias) = alias {
                    crate::analysis::schema::apply_column_aliases(
                        &mut output_columns,
                        alias.columns,
                        alias.span,
                    )?;
                }
                let mut scope = self.new_scope();
                for col in &output_columns {
                    self.check_control()?;
                    scope.add_field(Some(&alias_name), col);
                }
                return Ok((
                    Relation::CTEConsume {
                        cte_id: entry_id,
                        alias: alias_name,
                        output_columns,
                        producer_column_ids,
                    },
                    scope,
                ));
            }
        }

        if name.parts.last().is_some_and(|part| part.quoted)
            && let Some(metadata) = self.catalog.resolve_quoted_iceberg_metadata(
                name,
                catalog_override.as_deref(),
                &db_lower,
            )
        {
            if let Some(version) = version {
                return Err(AnalyzeError::unsupported_query_shape(
                    "time travel is not supported on metadata relations",
                    version.span,
                ));
            }
            let table = metadata
                .resolved
                .map_err(|error| AnalyzeError::unknown_table(error, name_span))?
                .planner;
            if metadata.kind == MetadataTableKind::Partitions {
                return self.analyze_iceberg_partitions(
                    name,
                    &table,
                    alias,
                    tbl,
                    Some(&metadata.relation),
                );
            }
            let qualifier = alias.map(|a| a.name.value.clone()).unwrap_or(tbl);
            return self.analyze_iceberg_metadata_scan(
                metadata.relation.namespace,
                table,
                metadata.kind,
                Some(qualifier),
            );
        }

        if let Some(declaration) = self.catalog.resolve_system_table_for_analysis(
            catalog_override.as_deref(),
            &db_lower,
            &tbl_lower,
            version,
        )? {
            if let Some(version) = version {
                return Err(AnalyzeError::unsupported_query_shape(
                    "time travel is not supported on a system table",
                    version.span,
                ));
            }
            return self.analyze_system_table(declaration.clone(), alias, &tbl, None);
        }

        let resolved_table = if let Some(version) = version {
            // Diagnose an invalid saved selection before asking the
            // closed catalog for a key its collector did not publish.
            crate::planning::catalog::parse_time_travel_selection(version, &parts.join("."))?;
            self.catalog.resolve_table_version_for_analysis(
                catalog_override.as_deref(),
                &db_lower,
                &tbl_lower,
                version,
            )?
        } else {
            if let Some(view) = self.catalog.resolve_view_for_analysis(
                catalog_override.as_deref(),
                &db_lower,
                &tbl_lower,
            )? {
                return self.analyze_view_relation(view, alias, &tbl, name_span);
            }
            self.catalog
                .resolve_table_for_analysis(catalog_override.as_deref(), &db_lower, &tbl_lower)
                .map_err(|error| AnalyzeError::unknown_table(error, name_span))?
        };
        let table_def = resolved_table.planner;
        let alias_name = alias.map(|a| a.name.value.clone());

        // Build scope
        let mut scope = self.new_scope();
        let qualifier = alias_name.as_deref().unwrap_or(&table_def.name);
        let mut column_ids = scope.add_table(
            Some(qualifier),
            &resolved_table.catalog.columns,
            self.control,
        )?;
        let crate::planner::table::ScanSource::Sql(source) = &table_def.source;
        for field in &source.retired_read_fields {
            self.check_control()?;
            if let Some((ordinal, _)) = resolved_table
                .catalog
                .columns
                .iter()
                .enumerate()
                .find(|(_, column)| column.name == field.field_path[0])
            {
                self.factory.borrow_mut().register_retired_read_field(
                    column_ids[ordinal],
                    resolved_table.catalog.identity.fqn(),
                    field.clone(),
                );
            }
        }
        // Register Iceberg V3 row-lineage pseudo-columns (_row_id,
        // _last_updated_sequence_number) when the table carries them.
        // These are hidden from SELECT * but resolvable by explicit name.
        if !resolved_table.catalog.hidden_columns.is_empty() {
            let meta_ids = scope.add_iceberg_metadata_columns(
                qualifier,
                &resolved_table.catalog.hidden_columns,
                self.control,
            )?;
            column_ids.extend(meta_ids);
        }

        let column_aliases = alias
            .as_ref()
            .map(|alias| alias.columns.to_vec())
            .unwrap_or_default();
        let alias_span = alias.map(|alias| alias.span).unwrap_or(name_span);
        scope.apply_column_aliases(&column_aliases, alias_span)?;
        let relation = Relation::Scan(ScanRelation {
            column_aliases,
            alias_span,
            database: db_lower,
            table: table_def,
            alias: alias_name,
            column_ids,
        });

        Ok((relation, scope))
    }

    fn analyze_iceberg_metadata_scan(
        &self,
        database: String,
        table: TableDef,
        metadata_table_type: MetadataTableKind,
        alias: Option<String>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        let mut scope = self.new_scope();
        let qualifier = alias.as_deref().unwrap_or(&table.name);
        // Keep the captured schema and share the same ColumnIds with the scan.
        let column_ids = table
            .columns
            .iter()
            .map(|column| scope.add_table_column(Some(qualifier), column, self.control))
            .collect::<Result<Vec<_>, AnalyzeError>>()?;
        let relation = Relation::IcebergMetadataScan(IcebergMetadataScanRelation {
            database,
            table,
            metadata_table_type,
            alias,
            column_ids,
        });
        Ok((relation, scope))
    }

    fn analyze_system_table(
        &self,
        declaration: Arc<SystemTableDeclaration>,
        alias: Option<RelationAlias<'_>>,
        qualifier: &str,
        statement_relation: Option<StatementSystemRelation>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        let alias_name =
            alias.map_or_else(|| qualifier.to_owned(), |alias| alias.name.value.clone());
        let mut output_columns = Vec::new();
        let mut required_columns = Vec::new();
        for (ordinal, column) in declaration.columns.iter().enumerate() {
            self.check_control()?;
            if column.hidden && alias.is_some_and(|alias| !alias.columns.is_empty()) {
                continue;
            }
            output_columns.push(OutputColumn {
                column_id: self.alloc_column_id(
                    Some(alias_name.clone()),
                    column.name.clone(),
                    column.value_type.clone(),
                ),
                name: column.name.clone(),
                field_name: Some(column.name.clone()),
                value_type: column.value_type.clone(),
                // The scope hides expansion. An explicit reference produces a
                // regular SQL value and can become a public result column.
                is_internal: false,
            });
            required_columns.push(ordinal);
        }
        if let Some(alias) = alias {
            crate::analysis::schema::apply_column_aliases(
                &mut output_columns,
                alias.columns,
                alias.span,
            )?;
        }
        let mut scope = self.new_scope();
        self.add_system_table_scope_fields(
            &mut scope,
            &alias_name,
            &declaration,
            &output_columns,
            &required_columns,
        )?;
        Ok((
            Relation::SystemTable {
                declaration,
                alias: alias_name,
                output_columns,
                required_columns,
                statement_relation,
            },
            scope,
        ))
    }

    pub(super) fn add_system_table_scope_fields(
        &self,
        scope: &mut AnalyzerScope,
        qualifier: &str,
        declaration: &SystemTableDeclaration,
        output_columns: &[OutputColumn],
        required_columns: &[usize],
    ) -> Result<(), AnalyzeError> {
        for (column, &ordinal) in output_columns.iter().zip(required_columns) {
            self.check_control()?;
            if declaration.columns[ordinal].hidden {
                scope.add_hidden_field(qualifier, column);
            } else {
                scope.add_field(Some(qualifier), column);
            }
        }
        Ok(())
    }

    fn analyze_derived_relation(
        &self,
        subquery: &ast::Query,
        alias: Option<RelationAlias<'_>>,
        span: novarocks_parser::Span,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        let alias_name = alias
            .as_ref()
            .map(|a| a.name.value.clone())
            .ok_or_else(|| {
                AnalyzeError::invalid_query_shape("subquery in FROM requires an alias", span)
            })?;

        let resolved_query = self.analyze_query(subquery)?;
        let output_columns = derived_table_output_columns(&resolved_query.output_columns, alias)?;

        // Build scope from subquery output columns.
        // Reuse the inner query's ColumnId so that distribution /
        // equivalence specs remain valid across the alias boundary.
        let mut scope = self.new_scope();
        for col in &output_columns {
            self.check_control()?;
            scope.add_field(Some(&alias_name), col);
        }

        let relation = Relation::Subquery {
            query: novarocks_parser::ast::AstBox::new(resolved_query),
            alias: alias_name,
            output_columns,
        };

        Ok((relation, scope))
    }

    fn analyze_unnest(
        &self,
        array_exprs: &[ast::Expr],
        alias: Option<RelationAlias<'_>>,
        with_offset: bool,
        span: novarocks_parser::Span,
        outer_scope: Option<&AnalyzerScope>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        if with_offset {
            return Err(AnalyzeError::unsupported_query_shape(
                "UNNEST WITH OFFSET/ORDINALITY is not yet supported",
                span,
            ));
        }
        if array_exprs.is_empty() {
            return Err(AnalyzeError::invalid_query_shape(
                "UNNEST requires at least one ARRAY expression",
                span,
            ));
        }
        let Some(outer_scope) = outer_scope else {
            return Err(AnalyzeError::unsupported_query_shape(
                "UNNEST is currently supported only in LATERAL JOIN",
                span,
            ));
        };

        let alias_columns = alias
            .map(|a| {
                a.columns
                    .iter()
                    .map(|c| c.value.clone())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        if !alias_columns.is_empty() && alias_columns.len() != array_exprs.len() {
            return Err(AnalyzeError::mismatched_column_aliases(
                format!(
                    "UNNEST alias has {} columns but produces {} columns",
                    alias_columns.len(),
                    array_exprs.len()
                ),
                alias.map(|alias| alias.span).unwrap_or(span),
            ));
        }

        let alias_name = alias.map(|a| a.name.value.clone());
        let qualifier = alias_name.as_deref().unwrap_or("unnest");
        let mut args = Vec::with_capacity(array_exprs.len());
        let mut names = Vec::with_capacity(array_exprs.len());
        let mut scope = self.new_scope();
        for (idx, expr) in array_exprs.iter().enumerate() {
            self.check_control()?;
            let typed = self.analyze_expr(expr, outer_scope)?;
            if !matches!(&typed.value_type.data_type(), DataType::List(_)) {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "UNNEST argument {} must be ARRAY, got {:?}",
                        idx + 1,
                        typed.value_type.data_type()
                    ),
                    expr.span(),
                ));
            }
            names.push(alias_columns.get(idx).cloned().unwrap_or_else(|| {
                if array_exprs.len() == 1 {
                    "unnest".to_string()
                } else {
                    format!("unnest_{}", idx + 1)
                }
            }));
            args.push(typed);
        }
        let function_arguments = args
            .iter()
            .map(|argument| {
                crate::analysis::function_argument(argument, self.constant_policy, self.control)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(AnalyzeError::function_binding)?;
        let binding = self
            .function_catalog
            .resolve_table_binding("unnest", &function_arguments, self.control)
            .map_err(|error| {
                if let novarocks_functions::FunctionBindingError::Control(error) = error {
                    return AnalyzeError::control(error);
                }
                if matches!(
                    error,
                    novarocks_functions::FunctionBindingError::UnavailableImplementation(_)
                ) {
                    return AnalyzeError::function_binding(error).at_invalid_argument(span);
                }
                AnalyzeError::invalid_argument(format!("failed to bind UNNEST: {error}"), span)
            })?;
        let novarocks_functions::FunctionResultType::Relation(result_columns) =
            &binding.selected.result_type
        else {
            return Err(AnalyzeError::invalid_argument(
                "UNNEST binding must produce a relation",
                span,
            ));
        };
        if result_columns.len() != names.len() {
            return Err(AnalyzeError::invalid_argument(
                "UNNEST binding result count differs from analyzed arguments",
                span,
            ));
        }
        let mut output_columns = Vec::with_capacity(names.len());
        for (idx, (name, value_type)) in names.into_iter().zip(result_columns.iter()).enumerate() {
            self.check_control()?;
            super::helpers::validate_value_type(value_type, self.control)?;
            let column_id =
                self.alloc_column_id(Some(qualifier.to_owned()), name.clone(), value_type.clone());
            scope.add_analyzed_field_with_id(
                Some(qualifier),
                &name,
                alias_columns.get(idx).map(String::as_str),
                column_id,
                value_type.clone(),
            );
            output_columns.push(OutputColumn {
                field_name: alias_columns.get(idx).cloned(),
                column_id,
                name,
                value_type: value_type.clone(),
                is_internal: false,
            });
        }
        Ok((
            Relation::Unnest(UnnestRelation {
                args,
                binding: crate::binding::SqlFunctionBinding::new(
                    binding,
                    self.sql_semantics.sql_mode().decimal_overflow_policy(),
                ),
                output_columns,
                alias: alias_name,
            }),
            scope,
        ))
    }

    fn analyze_table_function(
        &self,
        invocation: &ast::TableFunctionInvocation,
        alias: Option<RelationAlias<'_>>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        if !invocation.copartitioning.is_empty() {
            return Err(AnalyzeError::unsupported_expression(
                "table function copartitioning is not supported",
                invocation.span,
            ));
        }
        let arguments = invocation
            .arguments
            .iter()
            .map(|argument| {
                let ast::TableFunctionArgumentValue::Expression(expression) = &argument.value
                else {
                    return Err(AnalyzeError::unsupported_expression(
                        "table and descriptor function arguments are not supported",
                        argument.span,
                    ));
                };
                Ok(TableArgument {
                    name: argument.name.as_ref(),
                    expression,
                    span: argument.span,
                })
            })
            .collect::<Result<Vec<_>, AnalyzeError>>()?;
        self.analyze_table_function_arguments(&invocation.name, &arguments, invocation.span, alias)
    }

    fn analyze_table_function_arguments(
        &self,
        name: &ast::ObjectName,
        arguments: &[TableArgument<'_>],
        span: novarocks_parser::Span,
        alias: Option<RelationAlias<'_>>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        let func_name = name
            .parts
            .last()
            .map(|part| part.value.to_ascii_lowercase())
            .unwrap_or_default();
        if func_name == "__nr_ivm_delta" {
            if arguments.iter().any(|argument| argument.name.is_some()) {
                return Err(AnalyzeError::invalid_argument(
                    "__nr_ivm_delta requires positional arguments",
                    span,
                ));
            }
            let expressions = arguments
                .iter()
                .map(|argument| argument.expression)
                .collect::<Vec<_>>();
            return self.analyze_iceberg_delta_table_function(&expressions, span, alias);
        }
        if func_name != "generate_series" {
            return Err(AnalyzeError::unsupported_query_shape(
                format!("unsupported table function: {func_name}"),
                name.span,
            ));
        }
        let any_named = arguments.iter().any(|argument| argument.name.is_some());
        if any_named && let Some(arg) = arguments.iter().find(|arg| arg.name.is_none()) {
            if matches!(arg.expression, ast::Expr::Lambda(_)) {
                return Err(AnalyzeError::invalid_argument(
                    "No viable statement for input",
                    arg.span(),
                ));
            }
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "Unexpected input '{}'.",
                    expression_for_diagnostic(arg.expression)
                ),
                arg.span(),
            ));
        }

        let (start, end, step) = if any_named {
            let mut start_v: Option<Option<i64>> = None;
            let mut end_v: Option<Option<i64>> = None;
            let mut step_v: Option<Option<i64>> = None;
            for arg in arguments {
                self.check_control()?;
                let Some(name) = arg.name else {
                    return Err(AnalyzeError::invalid_argument(
                        "Unknown table function: generate_series",
                        arg.span(),
                    ));
                };
                let key = name.value.to_ascii_lowercase();
                let expr = arg.expression;
                let value = if is_null_literal(expr) {
                    None
                } else {
                    Some(eval_const_i64(expr)?)
                };
                let slot = match key.as_str() {
                    "start" => &mut start_v,
                    "end" => &mut end_v,
                    "step" => &mut step_v,
                    _ => {
                        return Err(AnalyzeError::invalid_argument(
                            format!("Unknown table function: generate_series ({key})"),
                            name.span,
                        ));
                    }
                };
                if slot.is_some() {
                    return Err(AnalyzeError::invalid_argument(
                        "Unknown table function: generate_series",
                        name.span,
                    ));
                }
                *slot = Some(value);
            }
            let start = start_v.ok_or_else(|| {
                AnalyzeError::invalid_argument("Unknown table function: generate_series", span)
            })?;
            let end = end_v.ok_or_else(|| {
                AnalyzeError::invalid_argument("Unknown table function: generate_series", span)
            })?;
            // Named args do not allow NULL values for any parameter.
            if start.is_none() || end.is_none() || matches!(step_v, Some(None)) {
                return Err(AnalyzeError::invalid_argument(
                    "table function not support null parameter",
                    span,
                ));
            }
            let step = step_v.flatten().unwrap_or(1);
            if step == 0 {
                return Err(AnalyzeError::invalid_argument(
                    "generate_series step must not be zero",
                    span,
                ));
            }
            (start.unwrap(), end.unwrap(), step)
        } else {
            let values: Vec<i64> = arguments
                .iter()
                .map(|argument| eval_const_i64(argument.expression))
                .collect::<Result<_, _>>()?;
            match values.as_slice() {
                [s, e] => (*s, *e, 1i64),
                [s, e, st] => {
                    if *st == 0 {
                        return Err(AnalyzeError::invalid_argument(
                            "generate_series step must not be zero",
                            span,
                        ));
                    }
                    (*s, *e, *st)
                }
                _ => {
                    return Err(AnalyzeError::invalid_argument(
                        "Unknown table function: generate_series",
                        span,
                    ));
                }
            }
        };

        // Determine output column name from alias or default
        let column_name = alias
            .and_then(|a| a.columns.first().map(|c| c.value.clone()))
            .unwrap_or_else(|| "generate_series".to_string());
        let alias_name = alias.map(|a| a.name.value.clone());
        let qualifier = alias_name.as_deref().unwrap_or("generate_series");

        let mut scope = self.new_scope();
        let output_column_id = scope.add_column(
            Some(qualifier),
            &column_name,
            novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
        );

        let relation = Relation::GenerateSeries(GenerateSeriesRelation {
            start,
            end,
            step,
            column_name,
            alias: alias_name,
            output_column_id,
        });
        Ok((relation, scope))
    }

    /// Analyze the IVM-A1 internal table function
    /// `__nr_ivm_delta('cat.ns.tbl', from_snapshot_id, to_snapshot_id)`.
    /// This function is produced by the IVM refresh driver (and exercised
    /// directly in tests) to scan the snapshot-range delta of an Iceberg
    /// base table.
    fn analyze_iceberg_delta_table_function(
        &self,
        arguments: &[&ast::Expr],
        span: novarocks_parser::Span,
        alias: Option<RelationAlias<'_>>,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        if arguments.len() != 3 {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "__nr_ivm_delta requires 3 positional arguments \
                     (catalog.namespace.table, from_snapshot_id, to_snapshot_id), got {}",
                    arguments.len()
                ),
                span,
            ));
        }

        // Argument 0: three-part identifier as a string literal.
        let three_part = arguments[0];
        let three_part = match three_part {
            ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::String(s),
                ..
            }) => s.clone(),
            _ => {
                return Err(AnalyzeError::invalid_literal(
                    format!(
                        "__nr_ivm_delta argument 0 must be a string literal \
                         (catalog.namespace.table), got {three_part:?}"
                    ),
                    three_part.span(),
                ));
            }
        };
        let parts: Vec<&str> = three_part.split('.').collect();
        if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "__nr_ivm_delta argument 0 must be a three-part identifier \
                     'catalog.namespace.table', got '{three_part}'"
                ),
                arguments[0].span(),
            ));
        }
        let catalog = parts[0].to_string();
        let namespace = parts[1].to_string();
        let table_name = parts[2].to_string();

        // Argument 1 / 2: from_snapshot_id, to_snapshot_id (non-negative i64).
        let from_expr = arguments[1];
        let to_expr = arguments[2];
        let from_snapshot_id = eval_const_i64(from_expr).map_err(|error| {
            AnalyzeError::invalid_literal(
                format!("__nr_ivm_delta from_snapshot_id: {error}"),
                from_expr.span(),
            )
        })?;
        let to_snapshot_id = eval_const_i64(to_expr).map_err(|error| {
            AnalyzeError::invalid_literal(
                format!("__nr_ivm_delta to_snapshot_id: {error}"),
                to_expr.span(),
            )
        })?;
        if from_snapshot_id < 0 || to_snapshot_id < 0 {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "__nr_ivm_delta requires non-negative snapshot ids; \
                     got from={from_snapshot_id}, to={to_snapshot_id}"
                ),
                span,
            ));
        }

        // Look up the base table. `__nr_ivm_delta` requires that the table
        // exposes Iceberg v3 row-lineage metadata columns — without them
        // we cannot recover row identity across snapshots.
        let resolved_table = self
            .catalog
            .resolve_table_for_analysis(Some(&catalog), &namespace, &table_name)
            .map_err(|error| AnalyzeError::unknown_table(error, arguments[0].span()))?;
        let table_def = resolved_table.planner;
        if resolved_table.catalog.hidden_columns.is_empty() {
            return Err(AnalyzeError::invalid_argument(
                format!(
                    "__nr_ivm_delta requires base table '{three_part}' to expose Iceberg v3 \
                     row-lineage metadata columns; the table is not Iceberg v3"
                ),
                arguments[0].span(),
            ));
        }

        // Output schema = base table columns + row-lineage metadata columns.
        // Both are exposed as resolvable columns under the alias / table name.
        let alias_name = alias.map(|a| a.name.value.clone());
        let mut scope = self.new_scope();
        let qualifier = alias_name.as_deref().unwrap_or(&table_def.name);
        // Collect analyzer-allocated ColumnIds so the planner can reuse them on
        // the scan's output_columns, keeping ColumnRef ids consistent throughout
        // the plan (same pattern as `Relation::Scan`).
        let mut column_ids = scope.add_table(
            Some(qualifier),
            &resolved_table.catalog.columns,
            self.control,
        )?;
        let meta_ids = scope.add_iceberg_metadata_columns(
            qualifier,
            &resolved_table.catalog.hidden_columns,
            self.control,
        )?;
        column_ids.extend(meta_ids);

        let relation = Relation::IcebergDeltaScan(IcebergDeltaScanRelation {
            catalog,
            namespace,
            table_name,
            table: table_def,
            from_snapshot_id,
            to_snapshot_id,
            alias: alias_name,
            column_ids,
        });
        Ok((relation, scope))
    }
}

fn parse_join_operator(
    kind: ast::JoinType,
    criteria: Option<&ast::JoinCriteria>,
) -> (JoinKind, Option<&ast::JoinCriteria>) {
    let kind = match kind {
        ast::JoinType::Cross | ast::JoinType::Implicit => return (JoinKind::Cross, None),
        ast::JoinType::Inner => JoinKind::Inner,
        ast::JoinType::Left => JoinKind::LeftOuter,
        ast::JoinType::Right => JoinKind::RightOuter,
        ast::JoinType::Full => JoinKind::FullOuter,
    };
    (kind, criteria)
}

#[recursive::recursive]
fn eval_const_i64(expr: &ast::Expr) -> Result<i64, AnalyzeError> {
    match expr {
        ast::Expr::Literal(ast::Literal {
            kind:
                ast::LiteralKind::Long {
                    parsed_value: number,
                    ..
                },
            ..
        }) => Ok(*number),
        ast::Expr::Unary(unary) if matches!(unary.operator, ast::UnaryOperator::Minus) => {
            Ok(-eval_const_i64(&unary.expression)?)
        }
        ast::Expr::Binary(binary) => {
            let left = eval_const_i64(&binary.left)?;
            let right = eval_const_i64(&binary.right)?;
            match binary.operator {
                ast::BinaryOperator::Add => Ok(left + right),
                ast::BinaryOperator::Subtract => Ok(left - right),
                ast::BinaryOperator::Multiply => Ok(left * right),
                ast::BinaryOperator::Divide if right != 0 => Ok(left / right),
                ast::BinaryOperator::Modulo if right != 0 => Ok(left % right),
                operator => Err(AnalyzeError::invalid_argument(
                    format!("unsupported operator in constant expression: {operator:?}"),
                    expr.span(),
                )),
            }
        }
        _ => Err(AnalyzeError::invalid_literal(
            format!("expected constant integer expression, got: {expr:?}"),
            expr.span(),
        )),
    }
}

fn is_null_literal(expr: &ast::Expr) -> bool {
    matches!(
        expr,
        ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        })
    )
}

fn derived_table_output_columns(
    columns: &[OutputColumn],
    alias: Option<RelationAlias<'_>>,
) -> Result<Vec<OutputColumn>, AnalyzeError> {
    let Some(alias) = alias else {
        return Ok(columns.to_vec());
    };
    if alias.columns.is_empty() {
        return Ok(columns.to_vec());
    }
    let mut fields = columns.to_vec();
    crate::analysis::schema::apply_column_aliases(&mut fields, alias.columns, alias.span)?;
    Ok(fields)
}

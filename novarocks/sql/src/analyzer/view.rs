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

//! Stored-view analysis in its own saved context and declaration boundary.

use super::{AnalyzerContext, scope::AnalyzerScope};
use crate::analysis::{
    AnalyzedOutputField, AnalyzedProjectItem, ExprKind, QueryBody, Relation, ResolvedQuery,
    ResolvedSelect, TypedExpr,
};
use crate::analyze_error::{AnalyzeError, AnalyzeErrorKind};
use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable, SqlViewDefinition};
use arrow::datatypes::DataType as D;
use novarocks_parser::{Span, ast};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, ValueKind as K, ValueTypeRef};

pub(super) type ViewAnalysisStack = std::rc::Rc<
    std::cell::RefCell<Vec<(novarocks_types::naming::TableIdentity, std::sync::Arc<str>)>>,
>;

impl AnalyzerContext<'_> {
    pub(super) fn analyze_view_relation(
        &self,
        view: &SqlViewDefinition,
        alias: Option<&ast::TableAlias>,
        relation_name: &str,
        span: Span,
    ) -> Result<(Relation, AnalyzerScope), AnalyzeError> {
        self.check_control()?;
        view.validate().map_err(AnalyzeError::internal)?;
        if self.catalog.view_definition_target() == Some(&view.identity)
            || self
                .view_stack
                .borrow()
                .iter()
                .any(|key| key.0 == view.identity && key.1 == view.version)
        {
            return Err(AnalyzeError::view_is_recursive(
                format!("View '{}' is recursive", view.identity.fqn()),
                span,
            ));
        }
        let statements = novarocks_parser::parse(&view.raw_sql).map_err(|error| {
            AnalyzeError::invalid_view(
                format!(
                    "Failed parsing stored view '{}': {error}",
                    view.identity.fqn()
                ),
                span,
            )
        })?;
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            return Err(AnalyzeError::invalid_view(
                "Stored view source must contain exactly one query",
                span,
            ));
        };
        crate::sql_mode::validate_persisted_query_semantics_typed(query, &self.sql_semantics)
            .map_err(|error| stored_body_error(view, error, span))?;
        let query = crate::sql_mode::normalize_concat_query(query.clone(), &self.sql_semantics)
            .and_then(super::query_prepass::preanalyze)
            .map_err(|error| stored_body_error(view, error, span))?;
        let catalog = SavedViewCatalog {
            inner: self.catalog,
            catalog: &view.default_catalog,
        };
        let child = AnalyzerContext {
            control: self.control,
            constant_policy: self.constant_policy,
            materialized_values: None,
            catalog: &catalog,
            current_database: &view.default_database,
            function_catalog: self.function_catalog,
            sql_semantics: self.sql_semantics.clone(),
            factory: self.factory.clone(),
            view_stack: self.view_stack.clone(),
            ctes: Default::default(),
            pending_ctes: Default::default(),
            next_subquery_id: std::cell::Cell::new(self.next_subquery_id.get()),
            next_lambda_slot_id: std::cell::Cell::new(self.next_lambda_slot_id.get()),
            collected_subqueries: Default::default(),
            cte_registry: std::cell::RefCell::new(self.cte_registry.borrow().clone()),
        };
        self.view_stack
            .borrow_mut()
            .push((view.identity.clone(), view.version.clone()));
        let result = child.analyze_query(&query);
        self.view_stack.borrow_mut().pop();
        // CTE identities and counters remain globally unique, while neither
        // caller CTE visibility nor correlation enters the stored body.
        self.absorb_semantic_scope(&child);
        let mut resolved = result.map_err(|error| stored_body_error(view, error, span))?;
        let source = resolved
            .output_columns
            .iter()
            .filter(|field| !field.is_internal)
            .collect::<Vec<_>>();
        if source.len() != view.columns.len() {
            return Err(AnalyzeError::view_is_stale(
                format!("View '{}' output width changed", view.identity.fqn()),
                span,
            ));
        }
        let mut work = CompileCheckpoints::try_new(self.control, CompilePhase::Validate)
            .map_err(AnalyzeError::control)?;
        let mut passthrough_columns = Vec::with_capacity(source.len());
        for (ordinal, (field, declared)) in source.iter().zip(view.columns.iter()).enumerate() {
            work.step().map_err(AnalyzeError::control)?;
            let passthrough = field
                .value_type
                .same_semantics_ignoring_nullability_observed::<novarocks_functions::FunctionBindingError>(
                    &declared.value_type,
                    || {
                        work.step()
                            .map_err(novarocks_functions::FunctionBindingError::from)
                    },
                )
                .map_err(AnalyzeError::function_binding)?;
            passthrough_columns.push(passthrough);
            let fits = passthrough
                || field
                    .value_type
                    .fits_value_type_observed::<novarocks_functions::FunctionBindingError>(
                        &declared.value_type,
                        || {
                            work.step()
                                .map_err(novarocks_functions::FunctionBindingError::from)
                        },
                    )
                    .map_err(AnalyzeError::function_binding)?;
            if !same_public_name(field.field_name.as_deref(), &declared.name, &mut work)?
                || (!fits
                    && !store_compatible(
                        field.value_type.value_type(),
                        declared.value_type.value_type(),
                        &mut work,
                    )?)
            {
                return Err(AnalyzeError::view_is_stale(
                    format!(
                        "View '{}' declaration changed at column {}",
                        view.identity.fqn(),
                        ordinal + 1
                    ),
                    span,
                ));
            }
        }
        work.finish().map_err(AnalyzeError::control)?;
        let passthrough = passthrough_columns.iter().all(|passthrough| *passthrough);
        if passthrough {
            for (field, declared) in resolved
                .output_columns
                .iter_mut()
                .filter(|field| !field.is_internal)
                .zip(view.columns.iter())
            {
                field.field_name = Some(declared.name.to_string());
            }
        } else {
            let inner_alias = "__stored_view_body".to_string();
            let input_columns = resolved.output_columns.clone();
            let mut projection = Vec::with_capacity(view.columns.len());
            let mut output_columns = Vec::with_capacity(view.columns.len());
            for ((field, declared), passthrough) in source
                .iter()
                .zip(view.columns.iter())
                .zip(passthrough_columns)
            {
                self.check_control()?;
                let expression = TypedExpr {
                    kind: ExprKind::ColumnRef {
                        column_id: field.column_id,
                        qualifier: Some(inner_alias.clone()),
                        column: field.name.clone(),
                    },
                    value_type: field.value_type.clone(),
                };
                // A view TypeId does not rewrite an existing runtime binding's
                // NULL facts or physical layout. Only a logical conversion
                // may introduce a cast and a new value occurrence.
                let target = if passthrough {
                    field.value_type.clone()
                } else {
                    declared.value_type.clone()
                };
                if !passthrough {
                    // Existing Struct CAST may select by name. An assignment
                    // needing a different ordinal mapping has no such authority.
                    let mut cast_work =
                        CompileCheckpoints::try_new(self.control, CompilePhase::Validate)
                            .map_err(AnalyzeError::control)?;
                    let reorders = name_reordering_cast(
                        field.value_type.value_type(),
                        target.value_type(),
                        &mut cast_work,
                    )?;
                    cast_work.finish().map_err(AnalyzeError::control)?;
                    if reorders {
                        return Err(unsupported_assignment(
                            "Stored-view ROW assignment requires an ordinal conversion not implemented by the current CAST author",
                            span,
                        ));
                    }
                    let mode = self.sql_semantics.sql_mode();
                    if novarocks_functions::carrier_cast_can_produce_null_with_policy(
                        field.value_type.data_type(),
                        target.data_type(),
                        mode.decimal_overflow_policy(),
                        mode.allow_throw_exception(),
                    ) {
                        return Err(unsupported_assignment(
                            "Stored-view assignment requires an error-preserving conversion; the current CAST can produce a successful NULL",
                            span,
                        ));
                    }
                    novarocks_functions::PreparedCastRecipe::try_new(
                        novarocks_functions::CastOperation::Carrier,
                        &field.value_type,
                        &target,
                        mode.decimal_overflow_policy(),
                        mode.allow_throw_exception(),
                        self.control,
                    )
                    .map_err(AnalyzeError::view_cast_preparation)?;
                }
                let expression = if passthrough {
                    expression
                } else {
                    self.cast_to_value_type(expression, target.clone(), span)?
                };
                let id = if passthrough {
                    field.column_id
                } else {
                    let id = self.alloc_column_id(None, field.name.clone(), target.clone());
                    let logical = self.logical_output_type(None, &expression, &self.new_scope())?;
                    self.factory.borrow_mut().set_logical_type(id, logical);
                    id
                };
                projection.push(AnalyzedProjectItem {
                    expr: expression,
                    output_name: field.name.clone(),
                    field_name: Some(declared.name.to_string()),
                    output_column_id: id,
                });
                output_columns.push(AnalyzedOutputField {
                    column_id: id,
                    name: field.name.clone(),
                    field_name: Some(declared.name.to_string()),
                    value_type: target,
                    is_internal: false,
                });
            }
            resolved = ResolvedQuery {
                body: QueryBody::Select(ResolvedSelect {
                    from: Some(Relation::Subquery {
                        query: Box::new(resolved),
                        alias: inner_alias,
                        output_columns: input_columns,
                    }),
                    filter: None,
                    group_by: Vec::new(),
                    having: None,
                    projection,
                    has_aggregation: false,
                    distinct: false,
                    repeat: None,
                    apply_specs: Vec::new(),
                    predicate_apply_specs: Vec::new(),
                }),
                order_by: Vec::new(),
                limit: None,
                offset: None,
                output_columns,
                local_cte_ids: Vec::new(),
            };
        }
        let qualifier = alias
            .map_or(relation_name, |alias| alias.name.value.as_str())
            .to_string();
        let mut fields = resolved.output_columns.clone();
        if let Some(alias) = alias {
            crate::analysis::schema::apply_column_aliases(&mut fields, &alias.columns, alias.span)?;
        }
        let mut scope = self.new_scope();
        for field in &fields {
            scope.add_field(Some(&qualifier), field);
        }
        Ok((
            Relation::Subquery {
                query: Box::new(resolved),
                alias: qualifier,
                output_columns: fields,
            },
            scope,
        ))
    }
}

fn same_public_name(
    left: Option<&str>,
    right: &str,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, AnalyzeError> {
    Ok(match left {
        Some(left) => {
            for _ in left.chars().chain(right.chars()) {
                work.step().map_err(AnalyzeError::control)?;
            }
            work.flush().map_err(AnalyzeError::control)?;
            let equal = left.to_lowercase() == right.to_lowercase();
            work.step().map_err(AnalyzeError::control)?;
            equal
        }
        None => false,
    })
}

fn stored_body_error(view: &SqlViewDefinition, error: AnalyzeError, span: Span) -> AnalyzeError {
    if error.control_error().is_some() || error.source_failure().is_some() {
        return error;
    }
    if matches!(
        error.kind(),
        AnalyzeErrorKind::Internal
            | AnalyzeErrorKind::UnavailableImplementation
            | AnalyzeErrorKind::UnsupportedExpression
            | AnalyzeErrorKind::UnsupportedQueryShape
    ) {
        return error.at_view_reference(view.raw_sql.clone(), span);
    }
    AnalyzeError::invalid_view_from_analysis(
        &view.identity.fqn(),
        error,
        view.raw_sql.clone(),
        span,
    )
}

fn unsupported_assignment(message: &str, span: Span) -> AnalyzeError {
    if span.is_empty() {
        AnalyzeError::unsupported_expression_without_span(message)
    } else {
        AnalyzeError::unsupported_expression(message, span)
    }
}

/// Saved-context lookups are exact even when the body uses a bare name.
struct SavedViewCatalog<'a> {
    inner: &'a dyn PlannerTableProvider,
    catalog: &'a str,
}
impl PlannerTableProvider for SavedViewCatalog<'_> {
    fn resolve_table_version_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        version: &ast::TableVersion,
    ) -> Result<ResolvedAnalyzerTable, AnalyzeError> {
        self.inner.resolve_definition_table_version_for_analysis(
            Some(catalog.unwrap_or(self.catalog)),
            database,
            table,
            version,
        )
    }
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.inner.resolve_definition_table_for_analysis(
            Some(catalog.unwrap_or(self.catalog)),
            database,
            table,
        )
    }
    fn resolve_view_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        view: &str,
    ) -> Result<Option<&SqlViewDefinition>, AnalyzeError> {
        self.inner.resolve_definition_view_for_analysis(
            Some(catalog.unwrap_or(self.catalog)),
            database,
            view,
        )
    }
    fn resolve_inline_relation_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<Option<&crate::catalog::SqlInlineRelation>, AnalyzeError> {
        self.inner.resolve_definition_inline_relation_for_analysis(
            Some(catalog.unwrap_or(self.catalog)),
            database,
            table,
        )
    }
    fn view_definition_target(&self) -> Option<&novarocks_types::naming::TableIdentity> {
        self.inner.view_definition_target()
    }
    fn iceberg_metadata_provider(
        &self,
    ) -> Option<&dyn crate::catalog::IcebergMetadataTableProvider> {
        Some(self)
    }
}
impl crate::catalog::IcebergMetadataTableProvider for SavedViewCatalog<'_> {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        kind: crate::planning::catalog::MetadataTableKind,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.inner
            .iceberg_metadata_provider()
            .ok_or_else(|| "stored view catalog has no metadata-table facts".to_string())?
            .get_iceberg_metadata_table(
                Some(catalog.unwrap_or(self.catalog)),
                database,
                table,
                kind,
            )
    }
}

/// Trino 483 TypeCoercion compatibility over the public view type. Its value
/// nodes are nullable; required MAP keys retain their structural role.
fn store_compatible(
    source: &ValueTypeRef,
    target: &ValueTypeRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, AnalyzeError> {
    work.step().map_err(AnalyzeError::control)?;
    if source.nullable() && !target.nullable() {
        return Ok(false);
    }
    match (source.kind(), target.kind()) {
        (K::Scalar(D::Null), _) | (_, K::Scalar(D::Null)) => Ok(true),
        (K::String, K::String) | (K::Binary, K::Binary) => Ok(true),
        (K::Nominal(left), K::Nominal(right)) => Ok(left == right),
        (K::List(left), K::List(right)) => store_compatible(left, right, work),
        (
            K::Map {
                key: left_key,
                value: left_value,
            },
            K::Map {
                key: right_key,
                value: right_value,
            },
        ) => Ok(store_compatible(left_key, right_key, work)?
            && store_compatible(left_value, right_value, work)?),
        (K::Struct(left), K::Struct(right)) => {
            if left.len() != right.len() {
                return Ok(false);
            }
            for (left, right) in left.iter().zip(right.iter()) {
                if !store_compatible(left.value_type(), right.value_type(), work)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        (K::Scalar(left), K::Scalar(right)) if left == right => Ok(true),
        (K::Scalar(left), K::Scalar(right)) if trino_numeric(left) && trino_numeric(right) => {
            Ok(true)
        }
        (K::Scalar(left), K::Scalar(right)) if date_timestamp(left) && date_timestamp(right) => {
            Ok(true)
        }
        (K::Scalar(D::Time32(_) | D::Time64(_)), K::Scalar(D::Time32(_) | D::Time64(_))) => {
            Ok(true)
        }

        (K::Scalar(left), K::Scalar(right))
            if exact_only_scalar(left) || exact_only_scalar(right) =>
        {
            Err(AnalyzeError::unsupported_expression_without_span(
                "Stored-view non-identity assignment has no exact Trino type correspondence",
            ))
        }
        (K::FixedSizeList { .. } | K::Union(_), _) | (_, K::FixedSizeList { .. } | K::Union(_)) => {
            Err(AnalyzeError::unsupported_expression_without_span(
                "Stored-view non-identity assignment is unsupported for this type",
            ))
        }
        _ => Ok(false),
    }
}
fn trino_numeric(value: &D) -> bool {
    match value {
        D::Int8 | D::Int16 | D::Int32 | D::Int64 | D::Float32 | D::Float64 => true,
        D::Decimal128(precision, scale) => {
            (1..=38).contains(precision) && *scale >= 0 && (*scale as u8) <= *precision
        }
        _ => false,
    }
}
fn date_timestamp(value: &D) -> bool {
    matches!(value, D::Date32 | D::Date64 | D::Timestamp(..))
}
fn exact_only_scalar(value: &D) -> bool {
    matches!(
        value,
        D::UInt8
            | D::UInt16
            | D::UInt32
            | D::UInt64
            | D::Float16
            | D::Decimal256(..)
            | D::FixedSizeBinary(_)
            | D::Duration(_)
            | D::Interval(_)
    ) || matches!(value, D::Decimal128(..)) && !trino_numeric(value)
}
fn name_reordering_cast(
    source: &ValueTypeRef,
    target: &ValueTypeRef,
    work: &mut CompileCheckpoints<'_>,
) -> Result<bool, AnalyzeError> {
    work.step().map_err(AnalyzeError::control)?;
    if let (K::Struct(source), K::Struct(target)) = (source.kind(), target.kind()) {
        let mut reordered = false;
        let mut all_found = true;
        for (ordinal, field) in target.iter().enumerate() {
            let mut found = None;
            for (index, source) in source.iter().enumerate() {
                work.step().map_err(AnalyzeError::control)?;
                if source.name() == field.name() {
                    found = Some(index);
                    break;
                }
            }
            match found {
                Some(index) => reordered |= ordinal != index,
                None => all_found = false,
            }
        }
        if all_found && reordered {
            return Ok(true);
        }
    }
    for index in 0..source.child_count().min(target.child_count()) {
        if name_reordering_cast(
            source.child_type_at(index).expect("source child in bounds"),
            target.child_type_at(index).expect("target child in bounds"),
            work,
        )? {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SqlViewColumn;
    use novarocks_type_contract::{FunctionValueType, PureCompileControl};
    use novarocks_types::{naming::TableIdentity, schema::ColumnDef};
    use std::{cell::RefCell, collections::HashMap, sync::Arc};

    #[derive(Default)]
    struct Catalog {
        views: HashMap<String, SqlViewDefinition>,
        tables: crate::planning::catalog::PlannerMemoryCatalog,
        target: Option<TableIdentity>,
        lookup_error: Option<AnalyzeError>,
        table_lookups: RefCell<Vec<(String, String, String)>>,
    }
    impl PlannerTableProvider for Catalog {
        fn resolve_view_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            view: &str,
        ) -> Result<Option<&SqlViewDefinition>, AnalyzeError> {
            if view == "stop"
                && let Some(error) = &self.lookup_error
            {
                return Err(error.clone());
            }
            Ok(self.views.get(&format!(
                "{}.{}.{}",
                catalog.unwrap_or("caller"),
                database,
                view
            )))
        }
        fn view_definition_target(&self) -> Option<&TableIdentity> {
            self.target.as_ref()
        }
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<ResolvedAnalyzerTable, String> {
            self.table_lookups.borrow_mut().push((
                catalog.unwrap_or("caller").into(),
                database.into(),
                table.into(),
            ));
            if catalog != Some("ice") || database != "saved" {
                return Err("relation absent in the exact saved context".into());
            }
            self.tables
                .resolve_table_for_analysis(catalog, database, table)
        }
    }
    fn declaration(sql: &str, published: &str, ty: FunctionValueType) -> SqlViewDefinition {
        SqlViewDefinition {
            identity: TableIdentity::new("caller", "db", "v"),
            version: "version-1".into(),
            raw_sql: sql.into(),
            default_catalog: "caller".into(),
            default_database: "db".into(),
            columns: vec![SqlViewColumn {
                name: published.into(),
                value_type: crate::analyzer::view_value_type(
                    &ty,
                    &crate::compiler::SqlCompileControl::unbounded(),
                )
                .unwrap(),
            }]
            .into(),
        }
    }
    fn catalog(view: SqlViewDefinition) -> Catalog {
        let mut catalog = Catalog::default();
        catalog.views.insert(view.identity.fqn(), view);
        catalog
    }
    fn analyze(sql: &str, catalog: &Catalog) -> Result<ResolvedQuery, AnalyzeError> {
        let statements = novarocks_parser::parse(sql).unwrap();
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query fixture");
        };
        let (resolved, ctes, mut factory) = crate::analyzer::analyze(query, catalog, "db")?;
        // Check the actual logical construction, including CTE and wrapper IDs.
        let logical = crate::planner::plan_query(
            resolved.clone(),
            ctes,
            &mut factory,
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .unwrap();
        let actual = crate::planner::plan_output_columns(&logical).unwrap();
        crate::compiler::root_output::RootOutputSemantics::bind_analyzed(
            &resolved.output_columns,
            &actual,
            &factory,
        )
        .unwrap();
        Ok(resolved)
    }

    #[test]
    fn stored_replay_keeps_the_existing_uncaptured_semantics_refusals() {
        for (hint, feature) in [
            ("sql_mode='GROUP_CONCAT_LEGACY'", "GROUP_CONCAT_LEGACY"),
            (
                "decimal_overflow_to_double=true",
                "decimal_overflow_to_double",
            ),
            ("sql_mode='ERROR_IF_OVERFLOW'", "ERROR_IF_OVERFLOW"),
        ] {
            let catalog = catalog(declaration(
                &format!("SELECT /*+ SET_VAR({hint}) */ 1 AS x"),
                "x",
                FunctionValueType::new(D::Int64, false),
            ));
            let error = analyze("SELECT x FROM v", &catalog).unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
            assert!(error.message().contains(feature));
            assert!(catalog.table_lookups.borrow().is_empty());
        }
    }

    #[test]
    fn public_names_are_validated_before_outer_relation_aliases() {
        let ty = FunctionValueType::new(D::Int64, false);
        let mut catalog = catalog(declaration("SELECT 1 AS published", "Published", ty));
        let resolved = analyze("SELECT Published FROM v", &catalog).unwrap();
        assert_eq!(
            resolved.output_columns[0].field_name.as_deref(),
            Some("Published")
        );
        let QueryBody::Select(select) = resolved.body else {
            panic!("select");
        };
        let Some(Relation::Subquery {
            query,
            output_columns,
            ..
        }) = select.from
        else {
            panic!("view relation");
        };
        assert_eq!(query.output_columns[0].name, "published");
        assert_eq!(output_columns[0].field_name.as_deref(), Some("Published"));
        analyze("SELECT caller_name FROM v AS t(caller_name)", &catalog).unwrap();
        for changed in [
            "SELECT 1 AS changed",
            "SELECT 1",
            "VALUES (1)",
            "SELECT 1 AS published, 2 AS extra",
        ] {
            catalog.views.get_mut("caller.db.v").unwrap().raw_sql = changed.into();
            let error =
                analyze("SELECT caller_name FROM v AS t(caller_name)", &catalog).unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::ViewIsStale, "{changed}");
        }
    }

    #[test]
    fn saved_body_has_exact_context_and_its_own_cte_visibility() {
        let mut view = declaration(
            "SELECT a AS x FROM t",
            "x",
            FunctionValueType::new(D::Int64, false),
        );
        view.default_catalog = "ice".into();
        view.default_database = "saved".into();
        let mut catalog = catalog(view);
        catalog.tables.create_database("saved").unwrap();
        crate::planning::catalog::register_test_connector_read_table(
            &mut catalog.tables,
            "saved",
            "t",
            vec![ColumnDef {
                name: "a".into(),
                data_type: D::Int64,
                nullable: false,
                write_default: None,
                logical_type: None,
            }],
        )
        .unwrap();
        analyze("WITH t AS (SELECT 'caller' AS a) SELECT x FROM v", &catalog).unwrap();
        assert_eq!(
            catalog.table_lookups.borrow().as_slice(),
            &[("ice".into(), "saved".into(), "t".into())]
        );
        let view = catalog.views.get_mut("caller.db.v").unwrap();
        view.raw_sql = "WITH t AS (SELECT 1 AS a) SELECT a AS x FROM t".into();
        analyze("WITH t AS (SELECT 'caller' AS a) SELECT x FROM v", &catalog).unwrap();
        catalog.views.get_mut("caller.db.v").unwrap().raw_sql = "SELECT a FROM caller_only".into();
        let error = analyze(
            "WITH caller_only AS (SELECT 1 AS a) SELECT x FROM v",
            &catalog,
        )
        .unwrap_err();
        assert_eq!(error.kind(), AnalyzeErrorKind::InvalidView);
        assert_eq!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<AnalyzeError>()
                .unwrap()
                .kind(),
            AnalyzeErrorKind::UnknownTable
        );
    }

    #[test]
    fn sibling_views_keep_cte_and_scalar_subquery_identities_distinct() {
        let ty = FunctionValueType::new(D::Int64, true);
        let mut catalog = catalog(declaration(
            "WITH c AS (SELECT 1 AS a) SELECT (SELECT a FROM c) AS x",
            "x",
            ty.clone(),
        ));
        let mut sibling = declaration(
            "WITH c AS (SELECT 2 AS a) SELECT (SELECT a FROM c) AS x",
            "x",
            ty,
        );
        sibling.identity.table = "b".into();
        catalog.views.insert(sibling.identity.fqn(), sibling);
        for sql in [
            "SELECT v.x, b.x FROM v CROSS JOIN b",
            "SELECT a.x, b.x FROM v a CROSS JOIN v b",
        ] {
            let resolved = analyze(sql, &catalog).unwrap();
            assert_eq!(resolved.output_columns.len(), 2);
            assert_ne!(
                resolved.output_columns[0].column_id,
                resolved.output_columns[1].column_id
            );
        }
    }

    #[test]
    fn stored_boundaries_wrap_stale_children_and_preserve_actual_control_or_source() {
        let ty = FunctionValueType::new(D::Int64, false);
        let mut catalog = catalog(declaration("SELECT x FROM inner_view", "x", ty.clone()));
        let mut inner = declaration("SELECT 1 AS changed", "x", ty.clone());
        inner.identity.table = "inner_view".into();
        catalog.views.insert(inner.identity.fqn(), inner);
        let error = analyze("SELECT x FROM v", &catalog).unwrap_err();
        assert_eq!(error.kind(), AnalyzeErrorKind::InvalidView);
        assert_eq!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<AnalyzeError>()
                .unwrap()
                .kind(),
            AnalyzeErrorKind::ViewIsStale
        );
        catalog.views.get_mut("caller.db.v").unwrap().raw_sql = "SELECT x FROM stop".into();
        for cause in [
            novarocks_type_contract::CompileControlError::Cancelled,
            novarocks_type_contract::CompileControlError::DeadlineExceeded,
            novarocks_type_contract::CompileControlError::ResourceExhausted,
        ] {
            catalog.lookup_error = Some(AnalyzeError::control(cause.clone()));
            let error = analyze("SELECT x FROM v", &catalog).unwrap_err();
            assert_eq!(error.control_error(), Some(cause));
            assert_eq!(error.span(), None);
        }
        let original =
            AnalyzeError::view_cast_preparation(novarocks_functions::CastPrepareError::Unsupported);
        catalog.lookup_error = Some(original.clone());
        let error = analyze("SELECT x FROM v", &catalog).unwrap_err();
        assert_eq!(error.control_error(), original.control_error());
        let compiled = crate::compiler::SqlCompileError::from(error);
        assert!(matches!(
            compiled,
            crate::compiler::SqlCompileError::CompilationSource(_)
        ));
        let source = compiled.source_failure().expect("actual cast source");
        use novarocks_failure_contract::FailureProjection;
        assert_eq!(
            source.failure_fact(),
            novarocks_functions::CastPrepareError::Unsupported.failure_fact()
        );
        assert_eq!(
            source.downcast_ref::<novarocks_functions::CastPrepareError>(),
            Some(&novarocks_functions::CastPrepareError::Unsupported)
        );

        catalog.views.get_mut("caller.db.v").unwrap().raw_sql = "SELECT t.a AS x".into();
        catalog.lookup_error = None;
        let error = analyze("SELECT x FROM (SELECT 1 AS a) t CROSS JOIN v", &catalog).unwrap_err();
        assert_eq!(error.kind(), AnalyzeErrorKind::InvalidView);
    }

    #[test]
    fn numeric_assignment_preserves_public_type_for_select_values_and_set_roots() {
        for (sql, target, runtime_nullable) in [
            ("SELECT CAST(1 AS INT) AS x", D::Int64, true),
            (
                "SELECT x FROM (VALUES (CAST(1 AS INT))) AS t(x)",
                D::Int64,
                true,
            ),
            ("SELECT 1 AS x UNION ALL SELECT 2", D::Int64, false),
        ] {
            let catalog = catalog(declaration(
                sql,
                "x",
                FunctionValueType::new(target.clone(), true),
            ));
            let resolved = analyze("SELECT x FROM v", &catalog).unwrap();
            assert_eq!(
                resolved.output_columns[0].value_type.data_type(),
                &target,
                "{sql}"
            );
            assert_eq!(
                resolved.output_columns[0].value_type.nullable(),
                runtime_nullable,
                "{sql}"
            );
        }
        let catalog = catalog(declaration(
            "SELECT '1' AS x",
            "x",
            FunctionValueType::new(D::Int64, true),
        ));
        assert_eq!(
            analyze("SELECT x FROM v", &catalog).unwrap_err().kind(),
            AnalyzeErrorKind::ViewIsStale
        );
    }

    #[test]
    fn mixed_view_projection_keeps_exact_sibling_origins_and_authors_converted_metadata() {
        let body = "SELECT parse_json('{}') AS j, array<json>[] AS a, CAST(1 AS INT) AS n";
        let statements = novarocks_parser::parse(body).unwrap();
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query fixture");
        };
        let empty = Catalog::default();
        let (original, _, _) = crate::analyzer::analyze(query, &empty, "db").unwrap();
        let mut view = declaration(body, "j", original.output_columns[0].value_type.clone());
        view.columns = original
            .output_columns
            .iter()
            .enumerate()
            .map(|(ordinal, field)| SqlViewColumn {
                name: field.field_name.as_deref().unwrap().into(),
                value_type: if ordinal == 2 {
                    FunctionValueType::new(D::Int64, true)
                } else {
                    crate::analyzer::view_value_type(
                        &field.value_type,
                        &crate::compiler::SqlCompileControl::unbounded(),
                    )
                    .unwrap()
                },
            })
            .collect::<Vec<_>>()
            .into();
        let catalog = catalog(view);
        let statements = novarocks_parser::parse("SELECT * FROM v").unwrap();
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("query fixture");
        };
        let (resolved, _, factory) = crate::analyzer::analyze(query, &catalog, "db").unwrap();
        let QueryBody::Select(outer) = &resolved.body else {
            panic!("outer select");
        };
        let Some(Relation::Subquery { query: wrapper, .. }) = &outer.from else {
            panic!("view relation");
        };
        let QueryBody::Select(wrapped) = &wrapper.body else {
            panic!("controlled project");
        };
        let Some(Relation::Subquery { query: inner, .. }) = &wrapped.from else {
            panic!("body relation");
        };
        for ordinal in [0, 1] {
            assert_eq!(
                wrapper.output_columns[ordinal].value_type,
                inner.output_columns[ordinal].value_type
            );
            assert_eq!(
                wrapper.output_columns[ordinal].column_id,
                inner.output_columns[ordinal].column_id
            );
            assert!(factory.same_value_origin(
                wrapper.output_columns[ordinal].column_id,
                inner.output_columns[ordinal].column_id
            ));
        }
        assert_eq!(
            factory.logical_type(wrapper.output_columns[2].column_id),
            Some(novarocks_types::schema::SqlType::BigInt)
        );
        assert!(!factory.same_value_origin(
            wrapper.output_columns[2].column_id,
            inner.output_columns[2].column_id
        ));
    }

    #[test]
    fn public_view_type_does_not_freeze_a_row_child_nonnull_fact() {
        let body = "SELECT ROW(1) AS x";
        let empty = Catalog::default();
        let original = analyze(body, &empty).unwrap();
        let mut catalog = catalog(declaration(
            body,
            "x",
            original.output_columns[0].value_type.clone(),
        ));
        let evolved_body = "SELECT ROW(CAST(NULL AS BIGINT)) AS x";
        let expected = analyze(evolved_body, &empty).unwrap();
        let evolved = catalog.views.get_mut("caller.db.v").unwrap();
        evolved.version = "version-2".into();
        evolved.raw_sql = evolved_body.into();
        let resolved = analyze("SELECT x FROM v", &catalog).unwrap();
        assert_eq!(
            resolved.output_columns[0].value_type,
            expected.output_columns[0].value_type
        );
        let K::Struct(fields) = resolved.output_columns[0].value_type.value_type().kind() else {
            panic!("ROW output")
        };
        assert!(fields[0].value_type().nullable());
    }

    #[test]
    fn nullable_only_map_and_nominal_columns_keep_actual_bindings_with_or_without_a_numeric_cast() {
        use novarocks_types::schema::SqlType;
        let body = "SELECT m, u, v, n FROM ice.saved.t";
        for convert_numeric in [false, true] {
            let mut catalog = Catalog::default();
            catalog.tables.create_database("saved").unwrap();
            let map = D::Map(
                Arc::new(arrow::datatypes::Field::new(
                    "entries",
                    D::Struct(
                        vec![
                            arrow::datatypes::Field::new("key", D::Utf8, false),
                            arrow::datatypes::Field::new("value", D::Int32, false),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            );
            let columns = [
                ("m", map, None),
                ("u", D::FixedSizeBinary(16), Some(SqlType::Uuid)),
                ("v", D::LargeBinary, Some(SqlType::Variant)),
                ("n", D::Int32, None),
            ]
            .into_iter()
            .map(|(name, data_type, logical_type)| ColumnDef {
                name: name.into(),
                data_type,
                nullable: false,
                write_default: None,
                logical_type,
            })
            .collect();
            crate::planning::catalog::register_test_connector_read_table(
                &mut catalog.tables,
                "saved",
                "t",
                columns,
            )
            .unwrap();
            let original = analyze(body, &catalog).unwrap();
            let control = crate::compiler::SqlCompileControl::unbounded();
            let columns = original
                .output_columns
                .iter()
                .enumerate()
                .map(|(ordinal, field)| SqlViewColumn {
                    name: field.field_name.as_deref().unwrap().into(),
                    value_type: if convert_numeric && ordinal == 3 {
                        FunctionValueType::new(D::Int64, true)
                    } else {
                        crate::analyzer::view_value_type(&field.value_type, &control).unwrap()
                    },
                })
                .collect::<Vec<_>>();
            let mut view = declaration(body, "m", original.output_columns[0].value_type.clone());
            view.columns = columns.into();
            catalog.views.insert(view.identity.fqn(), view);
            let statements = novarocks_parser::parse("SELECT * FROM v").unwrap();
            let [ast::Statement::Query(query)] = statements.as_slice() else {
                panic!("query")
            };
            let (resolved, ctes, mut factory) =
                crate::analyzer::analyze(query, &catalog, "db").unwrap();
            let QueryBody::Select(outer) = &resolved.body else {
                panic!("outer select")
            };
            let Some(Relation::Subquery {
                query: view_query, ..
            }) = &outer.from
            else {
                panic!("view")
            };
            for ordinal in 0..3 {
                assert_eq!(
                    view_query.output_columns[ordinal].value_type,
                    original.output_columns[ordinal].value_type
                );
            }
            if convert_numeric {
                let QueryBody::Select(wrapper) = &view_query.body else {
                    panic!("wrapper")
                };
                let Some(Relation::Subquery { query: inner, .. }) = &wrapper.from else {
                    panic!("body")
                };
                for ordinal in 0..3 {
                    assert_eq!(
                        view_query.output_columns[ordinal].column_id,
                        inner.output_columns[ordinal].column_id
                    );
                    assert!(matches!(
                        wrapper.projection[ordinal].expr.kind,
                        ExprKind::ColumnRef { .. }
                    ));
                    assert_eq!(
                        wrapper.projection[ordinal].expr.value_type,
                        inner.output_columns[ordinal].value_type
                    );
                }
                assert_eq!(
                    view_query.output_columns[3].value_type.data_type(),
                    &D::Int64
                );
                assert!(!factory.same_value_origin(
                    view_query.output_columns[3].column_id,
                    inner.output_columns[3].column_id
                ));
            } else {
                let QueryBody::Select(body) = &view_query.body else {
                    panic!("body")
                };
                assert!(matches!(body.from, Some(Relation::Scan(_))));
                assert_eq!(
                    view_query.output_columns[3].value_type,
                    original.output_columns[3].value_type
                );
            }
            let fields = resolved.output_columns.clone();
            let plan = crate::planner::plan_query(resolved, ctes, &mut factory, &control).unwrap();
            let columns = crate::planner::plan_output_columns(&plan).unwrap();
            crate::compiler::root_output::RootOutputSemantics::bind_analyzed(
                &fields, &columns, &factory,
            )
            .unwrap();
        }
    }

    #[test]
    fn signed_narrowing_never_turns_assignment_overflow_into_a_successful_null() {
        for value in ["1", "2147483648"] {
            let catalog = catalog(declaration(
                &format!("SELECT CAST({value} AS BIGINT) AS x"),
                "x",
                FunctionValueType::new(D::Int32, true),
            ));
            let error = analyze("SELECT x FROM v", &catalog).unwrap_err();
            assert_eq!(error.kind(), AnalyzeErrorKind::UnsupportedExpression);
            assert!(error.message().contains("error-preserving conversion"));
        }
    }

    #[test]
    fn header_case_folding_uses_the_same_unicode_names_as_scope() {
        let catalog = catalog(declaration(
            "SELECT 1 AS ä",
            "Ä",
            FunctionValueType::new(D::Int64, false),
        ));
        analyze("SELECT Ä FROM v", &catalog).unwrap();
    }

    #[test]
    fn recursive_definition_preserves_direct_replace_and_stored_boundary_layering() {
        let mut catalog = catalog(declaration(
            "SELECT 1 AS x",
            "x",
            FunctionValueType::new(D::Int64, false),
        ));
        catalog.target = Some(TableIdentity::new("caller", "db", "v"));
        assert_eq!(
            analyze("SELECT * FROM v", &catalog).unwrap_err().kind(),
            AnalyzeErrorKind::ViewIsRecursive
        );
        let mut synthetic = novarocks_parser::parse("SELECT * FROM v").unwrap();
        let [ast::Statement::Query(query)] = synthetic.as_mut_slice() else {
            panic!("query fixture");
        };
        let ast::SetExpr::Select(select) = query.body.as_mut() else {
            panic!("select fixture");
        };
        let ast::TableFactor::Table { name, .. } = &mut select.from[0].relation else {
            panic!("view fixture");
        };
        name.span = Span::new(0, 0);
        let error = crate::analyzer::analyze(query, &catalog, "db").unwrap_err();
        assert_eq!(error.kind(), AnalyzeErrorKind::ViewIsRecursive);
        assert_eq!(
            error.to_user_error(Some("SELECT * FROM v")).location(),
            None
        );
        let mut outer = declaration(
            "SELECT * FROM v",
            "x",
            FunctionValueType::new(D::Int64, false),
        );
        outer.identity.table = "b".into();
        catalog.views.insert(outer.identity.fqn(), outer);
        let error = analyze("SELECT * FROM b", &catalog).unwrap_err();
        assert_eq!(error.kind(), AnalyzeErrorKind::InvalidView);
        assert_eq!(
            std::error::Error::source(&error)
                .unwrap()
                .downcast_ref::<AnalyzeError>()
                .unwrap()
                .kind(),
            AnalyzeErrorKind::ViewIsRecursive
        );
        catalog.views.remove("caller.db.v");
        assert_eq!(
            analyze("SELECT * FROM v", &catalog).unwrap_err().kind(),
            AnalyzeErrorKind::UnknownTable
        );
        catalog.target = None;
        let mut recursive = declaration(
            "SELECT * FROM b",
            "x",
            FunctionValueType::new(D::Int64, false),
        );
        recursive.identity.table = "v".into();
        catalog.views.insert(recursive.identity.fqn(), recursive);
        assert_eq!(
            analyze("SELECT * FROM v", &catalog).unwrap_err().kind(),
            AnalyzeErrorKind::InvalidView
        );
    }

    #[test]
    fn public_view_nullability_and_row_ordinals_are_separate_from_cast_capability() {
        let control = crate::compiler::SqlCompileControl::unbounded();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Validate).unwrap();
        let source = FunctionValueType::new(
            D::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                D::Int32,
                true,
            ))),
            false,
        );
        let target = FunctionValueType::new(
            D::List(Arc::new(arrow::datatypes::Field::new(
                "item",
                D::Int64,
                false,
            ))),
            false,
        );
        let target = crate::analyzer::view_value_type(&target, &control).unwrap();
        assert!(store_compatible(source.value_type(), target.value_type(), &mut work).unwrap());
        let source = FunctionValueType::new(
            D::Struct(
                vec![
                    arrow::datatypes::Field::new("a", D::Int32, false),
                    arrow::datatypes::Field::new("b", D::Int32, false),
                ]
                .into(),
            ),
            false,
        );
        let target = FunctionValueType::new(
            D::Struct(
                vec![
                    arrow::datatypes::Field::new("b", D::Int32, false),
                    arrow::datatypes::Field::new("a", D::Int32, false),
                ]
                .into(),
            ),
            false,
        );
        assert!(store_compatible(source.value_type(), target.value_type(), &mut work).unwrap());
        assert!(name_reordering_cast(source.value_type(), target.value_type(), &mut work).unwrap());
        work.finish().unwrap();
    }

    #[test]
    fn nested_capability_context_uses_caller_spans_and_keeps_each_actual_definition_source() {
        let inner = declaration(
            "\n\nSELECT 1 AS x",
            "x",
            FunctionValueType::new(D::Int64, false),
        );
        let mut outer = declaration(
            "\nSELECT x FROM v",
            "x",
            FunctionValueType::new(D::Int64, false),
        );
        outer.identity.table = "w".into();
        let body_start = inner.raw_sql.find('1').unwrap();
        let original = AnalyzeError::unsupported_expression(
            "actual implementation refusal",
            Span::new(body_start, body_start + 1),
        );
        let inner_start = outer.raw_sql.rfind('v').unwrap();
        let annotated = stored_body_error(
            &inner,
            original.clone(),
            Span::new(inner_start, inner_start + 1),
        );
        let caller = "SELECT '😀', x FROM w";
        let caller_start = caller.rfind('w').unwrap();
        let final_error =
            stored_body_error(&outer, annotated, Span::new(caller_start, caller_start + 1));
        assert_eq!(final_error.kind(), original.kind());
        assert_eq!(final_error.message(), original.message());
        let public = final_error.to_user_error(Some(caller));
        assert_eq!(public.location().unwrap().line(), 1);
        assert_eq!(public.location().unwrap().column(), caller_start as u32 + 1);
        assert_eq!(final_error.to_user_error(None).location(), None);
        assert_eq!(
            final_error.stored_view_definition_source(),
            Some(outer.raw_sql.as_ref())
        );
        let middle = std::error::Error::source(&final_error)
            .unwrap()
            .downcast_ref::<AnalyzeError>()
            .unwrap();
        assert_eq!(
            middle.stored_view_definition_source(),
            Some(inner.raw_sql.as_ref())
        );
        let cause = std::error::Error::source(middle)
            .unwrap()
            .downcast_ref::<AnalyzeError>()
            .unwrap();
        assert_eq!(cause, &original);
        assert_eq!(
            cause
                .to_user_error(middle.stored_view_definition_source())
                .location()
                .unwrap()
                .line(),
            3
        );
        let synthetic = stored_body_error(&inner, original, Span::new(0, 0));
        assert_eq!(synthetic.to_user_error(Some(caller)).location(), None);
    }

    #[test]
    fn assignment_walk_preserves_the_original_control_failure() {
        use std::sync::atomic::{AtomicU32, Ordering};
        struct Stop {
            observed: AtomicU32,
        }
        impl PureCompileControl for Stop {
            fn checkpoint(
                &self,
                _: CompilePhase,
                units: u32,
            ) -> Result<(), novarocks_type_contract::CompileControlError> {
                self.observed.fetch_add(units, Ordering::SeqCst);
                if units == 0 {
                    Ok(())
                } else {
                    Err(novarocks_type_contract::CompileControlError::Cancelled)
                }
            }
        }
        let fields = |data_type: D| {
            (0..300)
                .map(|ordinal| {
                    arrow::datatypes::Field::new(format!("f{ordinal}"), data_type.clone(), false)
                })
                .collect::<Vec<_>>()
        };
        let source = FunctionValueType::new(D::Struct(fields(D::Int32).into()), false);
        let target = FunctionValueType::new(D::Struct(fields(D::Int64).into()), false);
        let stop = Stop {
            observed: AtomicU32::new(0),
        };
        let mut work = CompileCheckpoints::try_new(&stop, CompilePhase::Validate).unwrap();
        let error =
            store_compatible(source.value_type(), target.value_type(), &mut work).unwrap_err();
        assert_eq!(
            error.control_error(),
            Some(novarocks_type_contract::CompileControlError::Cancelled)
        );
        assert!(stop.observed.load(Ordering::SeqCst) > 0);
    }
}

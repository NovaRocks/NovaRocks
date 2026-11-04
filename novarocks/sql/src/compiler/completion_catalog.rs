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

//! Owned catalog completion state and its fact-only analyzer catalog.

use std::collections::{HashMap, HashSet};

use novarocks_parser::ast::{self, Visit};
use novarocks_types::naming::{TableIdentity, normalize_identifier, resolve_catalog_table_name};

use super::SqlCompileError;
use super::completion::{
    CatalogLookupTarget, CatalogRelationFact, CatalogRelationNeed, CatalogRelationOutcome,
    CompileNeedId,
};
use crate::catalog::{
    IcebergMetadataTableProvider, PlannerTableProvider, ResolvedAnalyzerTable, TableLookupMode,
};
use crate::planner::table::{ScanSource, SqlMetadataTableKind, SqlScanKind};

/// The move-only catalog phase of one SQL compilation.
///
/// The query has already passed SQL-owned pre-analysis, including recursive
/// CTE unrolling. The state owns every value needed to resume analysis and
/// contains no application catalog, callback, or provider object.
pub(super) struct CatalogCompletionState {
    query: Box<ast::Query>,
    current_catalog: Option<Box<str>>,
    current_database: Box<str>,
    lookup_mode: TableLookupMode,
    needs: Box<[CatalogRelationNeed]>,
    all_needs: Vec<CatalogRelationNeed>,
    keys: HashSet<CatalogFactKey>,
    facts: Vec<CatalogRelationFact>,
    discovered_views: HashMap<TableIdentity, std::sync::Arc<crate::catalog::SqlViewDefinition>>,
    root_local_view_shadow: bool,
    definition_target: Option<TableIdentity>,
    discovery_round: bool,
    exchange_budget: Option<super::completion::CatalogExchangeBudget>,
    next_need_ordinal: u32,
}

impl CatalogCompletionState {
    pub(super) fn try_new(
        query: ast::Query,
        current_catalog: Option<&str>,
        current_database: &str,
        lookup_mode: TableLookupMode,
        first_need_ordinal: u32,
    ) -> Result<Self, SqlCompileError> {
        Self::try_new_with_additional_queries(
            query,
            &[],
            &[],
            current_catalog,
            current_database,
            lookup_mode,
            first_need_ordinal,
        )
    }

    pub(super) fn try_new_with_additional_queries(
        query: ast::Query,
        additional_queries: &[ast::Query],
        additional_relations: &[TableIdentity],
        current_catalog: Option<&str>,
        current_database: &str,
        lookup_mode: TableLookupMode,
        first_need_ordinal: u32,
    ) -> Result<Self, SqlCompileError> {
        let query =
            crate::analyzer::query_prepass::preanalyze(query).map_err(SqlCompileError::Analyze)?;
        let current_catalog = current_catalog
            .map(normalize_identifier)
            .transpose()
            .map_err(SqlCompileError::Compilation)?
            .map(String::into_boxed_str);
        let current_database = normalize_identifier(current_database)
            .map_err(SqlCompileError::Compilation)?
            .into_boxed_str();
        let mut collector = CatalogNeedCollector::new(
            current_catalog.as_deref(),
            &current_database,
            lookup_mode,
            first_need_ordinal,
        );
        collector.collect_query(&query, &CteScope::default())?;
        for additional in additional_queries {
            let additional = crate::analyzer::query_prepass::preanalyze(additional.clone())
                .map_err(SqlCompileError::Analyze)?;
            collector.collect_query(&additional, &CteScope::default())?;
        }
        for relation in additional_relations {
            collector.add_need(
                relation.clone(),
                CatalogLookupTarget::Table { mode: lookup_mode },
            )?;
        }
        let (needs, next_need_ordinal) = collector.finish()?;
        Ok(Self {
            query: Box::new(query),
            current_catalog,
            current_database,
            lookup_mode,
            keys: needs.iter().map(CatalogFactKey::from_need).collect(),
            all_needs: needs.to_vec(),
            facts: Vec::new(),
            discovered_views: HashMap::new(),
            root_local_view_shadow: true,
            definition_target: None,
            discovery_round: false,
            exchange_budget: None,
            needs,
            next_need_ordinal,
        })
    }

    pub(super) fn query(&self) -> &ast::Query {
        &self.query
    }

    pub(super) fn needs(&self) -> &[CatalogRelationNeed] {
        &self.needs
    }

    pub(super) const fn next_need_ordinal(&self) -> u32 {
        self.next_need_ordinal
    }

    pub(super) fn with_exchange_budget(
        mut self,
        limits: super::CompletionLimits,
    ) -> Result<Self, SqlCompileError> {
        let mut budget = super::completion::CatalogExchangeBudget::new(limits);
        budget.reserve(&self.needs, false)?;
        self.exchange_budget = Some(budget);
        Ok(self)
    }

    pub(super) fn with_definition_context(
        mut self,
        target: Option<TableIdentity>,
        control: &super::SqlCompileControl,
    ) -> Result<Self, SqlCompileError> {
        if !self.facts.is_empty() || self.exchange_budget.is_some() {
            return Err(SqlCompileError::InvalidRequest(
                "definition context must be frozen before catalog needs are published".into(),
            ));
        }
        let first = self
            .all_needs
            .first()
            .map_or(self.next_need_ordinal, |need| need.id().get());
        let mut collector = CatalogNeedCollector::new(
            self.current_catalog.as_deref(),
            &self.current_database,
            self.lookup_mode,
            first,
        )
        .observed(control)?;
        collector.exact_catalog = true;
        collector.collect_query(&self.query, &CteScope::default())?;
        let (needs, next) = collector.finish()?;
        self.keys = needs.iter().map(CatalogFactKey::from_need).collect();
        self.all_needs = needs.to_vec();
        self.needs = needs;
        self.next_need_ordinal = next;
        self.root_local_view_shadow = false;
        self.definition_target = target;
        Ok(self)
    }

    pub(super) const fn is_discovery_round(&self) -> bool {
        self.discovery_round
    }

    pub(super) fn accept_facts(
        &mut self,
        facts: Box<[CatalogRelationFact]>,
        control: &super::SqlCompileControl,
    ) -> Result<(), SqlCompileError> {
        control.check()?;
        if self.needs.is_empty() {
            return Err(SqlCompileError::InvalidRequest(
                "catalog continuation has no outstanding needs".into(),
            ));
        }
        let facts = match &mut self.exchange_budget {
            Some(budget) => budget.receive(&self.needs, facts)?,
            None => facts,
        };
        // Validate only the outstanding batch before retaining any of it.
        FactBackedCatalog::try_new(
            self.current_catalog.as_deref(),
            &self.current_database,
            self.lookup_mode,
            &self.needs,
            &facts,
        )?;
        let mut collector = CatalogNeedCollector::new(
            self.current_catalog.as_deref(),
            &self.current_database,
            self.lookup_mode,
            self.next_need_ordinal,
        )
        .observed(control)?;
        collector.keys = std::mem::take(&mut self.keys);
        for fact in facts.iter() {
            let CatalogRelationOutcome::View(view) = fact.outcome() else {
                continue;
            };
            control.check()?;
            let key = view.identity.clone();
            if let Some(previous) = self.discovered_views.get(&key) {
                if previous.version != view.version {
                    return Err(SqlCompileError::Compilation(
                        "one statement observed different versions of the same stored view".into(),
                    ));
                }
                if !same_view_version(previous, view, control)? {
                    return Err(SqlCompileError::Compilation(
                        "one stored-view version returned conflicting definition facts".into(),
                    ));
                }
                continue;
            }
            self.discovered_views.insert(key, view.clone());
            if self.definition_target.as_ref() == Some(&view.identity) {
                continue;
            }
            // Invalid stored syntax belongs to the actual view-reference
            // analysis boundary, where its caller span is still available.
            let Ok(statements) = novarocks_parser::parse(&view.raw_sql) else {
                continue;
            };
            let [ast::Statement::Query(query)] = statements.as_slice() else {
                continue;
            };
            let Ok(query) = crate::analyzer::query_prepass::preanalyze(query.clone()) else {
                continue;
            };
            let mut child = CatalogNeedCollector::new(
                Some(&view.default_catalog),
                &view.default_database,
                self.lookup_mode,
                collector.next_need_ordinal,
            )
            .observed(control)?;
            child.exact_catalog = true;
            child.keys = std::mem::take(&mut collector.keys);
            // Invalid saved syntax is diagnosed by the view analyzer. The
            // same collector still captures every other legal dependency.
            child.defer_semantic_refusals = true;
            child.collect_query(&query, &CteScope::default())?;
            let keys = std::mem::take(&mut child.keys);
            let (needs, next) = child.finish()?;
            collector.keys = keys;
            collector.next_need_ordinal = next;
            collector.needs.extend(needs);
        }
        let keys = std::mem::take(&mut collector.keys);
        let (needs, next) = collector.finish()?;
        self.keys = keys;
        self.facts.extend(facts);
        self.discovery_round = !needs.is_empty();
        self.all_needs.extend(needs.iter().cloned());
        self.needs = needs;
        if let Some(budget) = &mut self.exchange_budget {
            budget.reserve(&self.needs, self.discovery_round)?;
        }
        self.next_need_ordinal = next;
        control.check()?;
        Ok(())
    }

    pub(super) fn completed_catalog(&self) -> Result<FactBackedCatalog, SqlCompileError> {
        if !self.needs.is_empty() {
            return Err(SqlCompileError::InvalidRequest(
                "catalog dependency closure still has unanswered exact needs".into(),
            ));
        }
        let mut catalog = FactBackedCatalog::try_new(
            self.current_catalog.as_deref(),
            &self.current_database,
            self.lookup_mode,
            &self.all_needs,
            &self.facts,
        )?;
        catalog.root_local_view_shadow = self.root_local_view_shadow;
        catalog.definition_target = self.definition_target.clone();
        Ok(catalog)
    }

    pub(super) fn fact_catalog(
        &self,
        facts: &[CatalogRelationFact],
    ) -> Result<FactBackedCatalog, SqlCompileError> {
        if !self.facts.is_empty() || self.needs.is_empty() {
            if !facts.is_empty() {
                return Err(SqlCompileError::InvalidRequest(
                    "catalog facts were supplied after the closure response was retained".into(),
                ));
            }
            return self.completed_catalog();
        }
        if let Some(budget) = &self.exchange_budget {
            budget.validate_borrowed_bytes(facts)?;
        }
        // Legacy one-round callers cannot silently analyze an incomplete view
        // closure. Their application owners are migrated to accept_facts.
        let mut catalog = FactBackedCatalog::try_new(
            self.current_catalog.as_deref(),
            &self.current_database,
            self.lookup_mode,
            &self.needs,
            facts,
        )?;
        catalog.root_local_view_shadow = self.root_local_view_shadow;
        catalog.definition_target = self.definition_target.clone();
        if facts
            .iter()
            .any(|fact| matches!(fact.outcome(), CatalogRelationOutcome::View(_)))
        {
            return Err(SqlCompileError::InvalidRequest(
                "stored-view facts require the catalog dependency closure protocol".into(),
            ));
        }
        Ok(catalog)
    }
}

fn same_view_version(
    left: &crate::catalog::SqlViewDefinition,
    right: &crate::catalog::SqlViewDefinition,
    control: &super::SqlCompileControl,
) -> Result<bool, SqlCompileError> {
    use novarocks_type_contract::{CompileCheckpoints, CompilePhase};
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Validate)?;
    work.flush()?;
    if left.raw_sql != right.raw_sql
        || left.default_catalog != right.default_catalog
        || left.default_database != right.default_database
        || left.columns.len() != right.columns.len()
    {
        return Ok(false);
    }
    for (left, right) in left.columns.iter().zip(right.columns.iter()) {
        work.step()?;
        if left.name != right.name || left.source_field_name != right.source_field_name {
            return Ok(false);
        }
        let equal = left
            .value_type
            .exactly_equals_observed::<novarocks_functions::FunctionBindingError>(
                &right.value_type,
                || {
                    work.step()
                        .map_err(novarocks_functions::FunctionBindingError::from)
                },
            )?;
        if !equal {
            return Ok(false);
        }
    }
    work.finish()?;
    Ok(true)
}

#[derive(Clone, Default)]
struct CteScope {
    visible: HashSet<String>,
    pending: HashSet<String>,
}

struct CatalogNeedCollector<'a> {
    current_catalog: Option<&'a str>,
    current_database: &'a str,
    lookup_mode: TableLookupMode,
    exact_catalog: bool,
    defer_semantic_refusals: bool,
    work: Option<novarocks_type_contract::CompileCheckpoints<'a>>,
    next_need_ordinal: u32,
    keys: HashSet<CatalogFactKey>,
    needs: Vec<CatalogRelationNeed>,
}

impl<'a> CatalogNeedCollector<'a> {
    fn new(
        current_catalog: Option<&'a str>,
        current_database: &'a str,
        lookup_mode: TableLookupMode,
        first_need_ordinal: u32,
    ) -> Self {
        Self {
            current_catalog,
            current_database,
            lookup_mode,
            exact_catalog: false,
            defer_semantic_refusals: false,
            work: None,
            next_need_ordinal: first_need_ordinal,
            keys: HashSet::new(),
            needs: Vec::new(),
        }
    }

    fn observed(mut self, control: &'a super::SqlCompileControl) -> Result<Self, SqlCompileError> {
        self.work = Some(novarocks_type_contract::CompileCheckpoints::try_new(
            control,
            novarocks_type_contract::CompilePhase::Validate,
        )?);
        Ok(self)
    }
    fn step(&mut self) -> Result<(), SqlCompileError> {
        if let Some(work) = &mut self.work {
            work.step()?;
        }
        Ok(())
    }
    fn finish(self) -> Result<(Box<[CatalogRelationNeed]>, u32), SqlCompileError> {
        if let Some(work) = self.work {
            work.finish()?;
        }
        Ok((self.needs.into_boxed_slice(), self.next_need_ordinal))
    }

    fn collect_query(
        &mut self,
        query: &ast::Query,
        outer_scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        self.step()?;
        let mut body_scope = outer_scope.clone();
        if let Some(with) = &query.with {
            let local_names = with
                .ctes
                .iter()
                .map(|cte| cte.name.value.to_ascii_lowercase())
                .collect::<Vec<_>>();
            for name in &local_names {
                body_scope.pending.insert(name.clone());
            }
            for cte in &with.ctes {
                let name = cte.name.value.to_ascii_lowercase();
                body_scope.pending.remove(&name);
                self.collect_query(&cte.query, &body_scope)?;
                body_scope.visible.insert(name);
            }
        }

        self.collect_set_expr(&query.body, &body_scope)?;
        for order in &query.order_by {
            self.collect_expr(&order.expr, &body_scope)?;
        }
        if let Some(limit) = &query.limit {
            self.collect_expr(limit, &body_scope)?;
        }
        if let Some(offset) = &query.offset {
            self.collect_expr(&offset.value, &body_scope)?;
        }
        if let Some(quantity) = query
            .fetch
            .as_ref()
            .and_then(|fetch| fetch.quantity.as_ref())
        {
            self.collect_expr(quantity, &body_scope)?;
        }
        Ok(())
    }

    fn collect_set_expr(
        &mut self,
        expression: &ast::SetExpr,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        self.step()?;
        match expression {
            ast::SetExpr::Select(select) => self.collect_select(select, scope),
            ast::SetExpr::Values(values) => {
                for row in &values.rows {
                    for expression in row {
                        self.collect_expr(expression, scope)?;
                    }
                }
                Ok(())
            }
            ast::SetExpr::Query(query) => self.collect_query(query, scope),
            ast::SetExpr::SetOperation(operation) => {
                self.collect_set_expr(&operation.left, scope)?;
                self.collect_set_expr(&operation.right, scope)
            }
        }
    }

    fn collect_select(
        &mut self,
        select: &ast::Select,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        self.step()?;
        for hint in &select.hints {
            match &hint.value {
                ast::SelectHintValue::Bare => {}
                ast::SelectHintValue::Call { arguments } => {
                    for argument in arguments {
                        self.collect_expr(argument, scope)?;
                    }
                }
                ast::SelectHintValue::Assignment { value } => self.collect_expr(value, scope)?,
            }
        }
        if let ast::SelectQuantifier::Distinct { on, .. } = &select.quantifier {
            for expression in on {
                self.collect_expr(expression, scope)?;
            }
        }
        for item in &select.projection {
            match item {
                ast::SelectItem::UnnamedExpr(expression)
                | ast::SelectItem::ExprWithAlias {
                    expr: expression, ..
                } => {
                    self.collect_expr(expression, scope)?;
                }
                ast::SelectItem::Wildcard { options, .. }
                | ast::SelectItem::QualifiedWildcard { options, .. } => {
                    for replacement in &options.replace {
                        self.collect_expr(&replacement.expr, scope)?;
                    }
                }
            }
        }
        for relation in &select.from {
            self.collect_table_with_joins(relation, scope)?;
        }
        if let Some(selection) = &select.selection {
            self.collect_expr(selection, scope)?;
        }
        self.collect_group_by(&select.group_by, scope)?;
        if let Some(having) = &select.having {
            self.collect_expr(having, scope)?;
        }
        if let Some(qualify) = &select.qualify {
            self.collect_expr(qualify, scope)?;
        }
        for window in &select.windows {
            self.collect_window_spec(&window.specification, scope)?;
        }
        Ok(())
    }

    fn collect_group_by(
        &mut self,
        group_by: &ast::GroupBy,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        let groups: &[Vec<ast::Expr>] = match group_by {
            ast::GroupBy::None => return Ok(()),
            ast::GroupBy::Expressions { expressions, .. }
            | ast::GroupBy::Rollup { expressions, .. }
            | ast::GroupBy::Cube { expressions, .. } => std::slice::from_ref(expressions),
            ast::GroupBy::GroupingSets { sets, .. } => sets,
        };
        for group in groups {
            for expression in group {
                self.collect_expr(expression, scope)?;
            }
        }
        Ok(())
    }

    fn collect_window_spec(
        &mut self,
        window: &ast::WindowSpec,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        for expression in &window.partition_by {
            self.collect_expr(expression, scope)?;
        }
        for order in &window.order_by {
            self.collect_expr(&order.expr, scope)?;
        }
        if let Some(frame) = &window.window_frame {
            self.collect_window_bound(&frame.start_bound, scope)?;
            if let Some(end) = &frame.end_bound {
                self.collect_window_bound(end, scope)?;
            }
        }
        Ok(())
    }

    fn collect_window_bound(
        &mut self,
        bound: &ast::WindowFrameBound,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        match bound {
            ast::WindowFrameBound::CurrentRow(_) => Ok(()),
            ast::WindowFrameBound::Preceding(expression, _)
            | ast::WindowFrameBound::Following(expression, _) => {
                if let Some(expression) = expression {
                    self.collect_expr(expression, scope)?;
                }
                Ok(())
            }
        }
    }

    fn collect_table_with_joins(
        &mut self,
        relation: &ast::TableWithJoins,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        self.collect_table_factor(&relation.relation, scope)?;
        for join in &relation.joins {
            self.collect_table_factor(&join.relation, scope)?;
            if let ast::JoinConstraint::On(expression) = &join.constraint {
                self.collect_expr(expression, scope)?;
            }
        }
        Ok(())
    }

    fn collect_table_factor(
        &mut self,
        factor: &ast::TableFactor,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        self.step()?;
        match factor {
            ast::TableFactor::Table {
                name,
                metadata,
                version,
                hints,
                ..
            } => {
                if metadata.is_none() && name.parts.len() == 1 {
                    let name = name.parts[0].value.to_ascii_lowercase();
                    if scope.pending.contains(&name) {
                        if self.defer_semantic_refusals {
                            return Ok(());
                        }
                        return Err(SqlCompileError::Compilation(format!(
                            "forward CTE reference is not supported: {name}"
                        )));
                    }
                    if scope.visible.contains(&name) {
                        self.collect_table_decorations(version.as_ref(), hints, scope)?;
                        return Ok(());
                    }
                }
                let relation = match self.resolve_name(name) {
                    Ok(relation) => relation,
                    Err(SqlCompileError::Compilation(_)) if self.defer_semantic_refusals => {
                        return self.collect_table_decorations(version.as_ref(), hints, scope);
                    }
                    Err(error) => return Err(error),
                };
                let target = match metadata {
                    Some(metadata) => {
                        let kind = match SqlMetadataTableKind::parse(&metadata.value) {
                            Ok(kind) => kind,
                            Err(_) if self.defer_semantic_refusals => {
                                return self.collect_table_decorations(
                                    version.as_ref(),
                                    hints,
                                    scope,
                                );
                            }
                            Err(error) => return Err(SqlCompileError::Compilation(error)),
                        };
                        // The analyzer expands the partitions view over this exact
                        // Files fact; it must not request a provider Partitions read.
                        CatalogLookupTarget::IcebergMetadata {
                            kind: match kind {
                                SqlMetadataTableKind::Partitions => SqlMetadataTableKind::Files,
                                kind => kind,
                            },
                        }
                    }
                    None => CatalogLookupTarget::Table {
                        mode: self.lookup_mode,
                    },
                };
                self.add_need_qualified(
                    relation,
                    target,
                    !self.exact_catalog && metadata.is_none() && name.parts.len() < 3,
                )?;
                self.collect_table_decorations(version.as_ref(), hints, scope)
            }
            ast::TableFactor::Derived {
                subquery, hints, ..
            } => {
                self.collect_query(subquery, scope)?;
                self.collect_hints(hints, scope)
            }
            ast::TableFactor::TableFunction { expr, hints, .. } => {
                self.collect_internal_table_function_need(expr)?;
                self.collect_expr(expr, scope)?;
                self.collect_hints(hints, scope)
            }
            ast::TableFactor::Unnest { array_exprs, .. } => {
                for expression in array_exprs {
                    self.collect_expr(expression, scope)?;
                }
                Ok(())
            }
            ast::TableFactor::NestedJoin {
                table_with_joins, ..
            } => self.collect_table_with_joins(table_with_joins, scope),
        }
    }

    fn collect_table_decorations(
        &mut self,
        version: Option<&ast::TableVersion>,
        hints: &[ast::TableHint],
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        if let Some(version) = version {
            self.collect_expr(&version.value, scope)?;
        }
        self.collect_hints(hints, scope)
    }

    fn collect_hints(
        &mut self,
        hints: &[ast::TableHint],
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        for hint in hints {
            for argument in &hint.arguments {
                self.collect_expr(argument, scope)?;
            }
            if let Some(target) = &hint.target {
                self.collect_expr(target, scope)?;
            }
        }
        Ok(())
    }

    fn collect_expr(
        &mut self,
        expression: &ast::Expr,
        scope: &CteScope,
    ) -> Result<(), SqlCompileError> {
        let mut visitor = ExpressionQueryCollector {
            collector: self,
            scope,
            error: None,
        };
        visitor.visit_expr(expression);
        match visitor.error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn collect_internal_table_function_need(
        &mut self,
        expression: &ast::Expr,
    ) -> Result<(), SqlCompileError> {
        let ast::Expr::FunctionCall(function) = expression else {
            return Ok(());
        };
        let Some(name) = function.name.parts.last() else {
            return Ok(());
        };
        if !name.value.eq_ignore_ascii_case("__nr_ivm_delta") {
            return Ok(());
        }
        let Some(ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::String(identity),
            ..
        })) = function.arguments.first()
        else {
            return Ok(());
        };
        let parts = identity
            .split('.')
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        let relation = match resolve_catalog_table_name(
            &parts,
            Some(novarocks_types::naming::DEFAULT_CATALOG),
            self.current_database,
        ) {
            Ok(relation) => relation,
            Err(_) if self.defer_semantic_refusals => return Ok(()),
            Err(error) => return Err(SqlCompileError::Compilation(error)),
        };
        self.add_need(
            relation,
            CatalogLookupTarget::Table {
                mode: self.lookup_mode,
            },
        )
    }

    fn resolve_name(&self, name: &ast::ObjectName) -> Result<TableIdentity, SqlCompileError> {
        let parts = name
            .parts
            .iter()
            .map(|part| part.value.clone())
            .collect::<Vec<_>>();
        // A name with no catalog of its own is in the session's current
        // catalog, or in the default one when the session has set none.
        // Whether the table is there is what the lookup answers; a name that
        // resolves to nothing is an unknown table, not a malformed name.
        resolve_catalog_table_name(
            &parts,
            Some(
                self.current_catalog
                    .unwrap_or(novarocks_types::naming::DEFAULT_CATALOG),
            ),
            self.current_database,
        )
        .map_err(SqlCompileError::Compilation)
    }

    fn add_need(
        &mut self,
        relation: TableIdentity,
        target: CatalogLookupTarget,
    ) -> Result<(), SqlCompileError> {
        self.add_need_qualified(relation, target, false)
    }

    fn add_need_qualified(
        &mut self,
        relation: TableIdentity,
        target: CatalogLookupTarget,
        local_view_shadow: bool,
    ) -> Result<(), SqlCompileError> {
        self.step()?;
        let mut key = CatalogFactKey::new(relation.clone(), target);
        key.local_view_shadow = local_view_shadow;
        if !self.keys.insert(key) {
            return Ok(());
        }
        let id = CompileNeedId::new(self.next_need_ordinal);
        self.next_need_ordinal = self.next_need_ordinal.checked_add(1).ok_or_else(|| {
            SqlCompileError::InvalidRequest("catalog completion need identity overflow".to_string())
        })?;
        let need = CatalogRelationNeed::try_new(id, relation, target)
            .map_err(|error| SqlCompileError::InvalidRequest(error.to_string()))?;
        self.needs.push(if local_view_shadow {
            need.with_local_view_shadow()
        } else {
            need
        });
        Ok(())
    }
}

struct ExpressionQueryCollector<'collector, 'scope, 'context> {
    collector: &'collector mut CatalogNeedCollector<'context>,
    scope: &'scope CteScope,
    error: Option<SqlCompileError>,
}

impl Visit for ExpressionQueryCollector<'_, '_, '_> {
    fn visit_expr(&mut self, expression: &ast::Expr) {
        if self.error.is_some() {
            return;
        }
        if let Err(error) = self.collector.step() {
            self.error = Some(error);
            return;
        }
        ast::walk_expr(self, expression);
    }

    fn visit_query(&mut self, query: &ast::Query) {
        if self.error.is_none()
            && let Err(error) = self.collector.collect_query(query, self.scope)
        {
            self.error = Some(error);
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct CatalogFactKey {
    relation: TableIdentity,
    target: CatalogTargetKey,
    local_view_shadow: bool,
}

impl CatalogFactKey {
    fn from_need(need: &CatalogRelationNeed) -> Self {
        let mut key = Self::new(need.relation().clone(), need.target());
        key.local_view_shadow = need.allows_local_view_shadow();
        key
    }
    fn from_fact(fact: &CatalogRelationFact) -> Self {
        let mut key = Self::new(fact.relation().clone(), fact.target());
        key.local_view_shadow = fact.allows_local_view_shadow();
        key
    }

    fn new(relation: TableIdentity, target: CatalogLookupTarget) -> Self {
        Self {
            relation,
            target: CatalogTargetKey::from(target),
            local_view_shadow: false,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum CatalogTargetKey {
    TableSchemaOnly,
    TableExplainStats,
    IcebergMetadata(SqlMetadataTableKind),
}

impl From<CatalogLookupTarget> for CatalogTargetKey {
    fn from(value: CatalogLookupTarget) -> Self {
        match value {
            CatalogLookupTarget::Table {
                mode: TableLookupMode::SchemaOnly,
            } => Self::TableSchemaOnly,
            CatalogLookupTarget::Table {
                mode: TableLookupMode::ExplainStats,
            } => Self::TableExplainStats,
            CatalogLookupTarget::IcebergMetadata { kind } => Self::IcebergMetadata(kind),
        }
    }
}

enum CatalogEntry {
    Resolved(Box<ResolvedAnalyzerTable>),
    View(std::sync::Arc<crate::catalog::SqlViewDefinition>),
    Missing(Box<str>),
}

/// Analyzer catalog backed solely by one exact completion answer batch.
pub(super) struct FactBackedCatalog {
    current_catalog: Option<Box<str>>,
    current_database: Box<str>,
    lookup_mode: TableLookupMode,
    root_local_view_shadow: bool,
    definition_target: Option<TableIdentity>,
    entries: HashMap<CatalogFactKey, CatalogEntry>,
}

impl FactBackedCatalog {
    fn try_new(
        current_catalog: Option<&str>,
        current_database: &str,
        lookup_mode: TableLookupMode,
        needs: &[CatalogRelationNeed],
        facts: &[CatalogRelationFact],
    ) -> Result<Self, SqlCompileError> {
        let needs_by_id = needs
            .iter()
            .map(|need| (need.id(), need))
            .collect::<HashMap<_, _>>();
        let mut entries = HashMap::with_capacity(facts.len());
        for fact in facts {
            let need = needs_by_id.get(&fact.id()).ok_or_else(|| {
                SqlCompileError::Compilation(format!(
                    "catalog completion returned unexpected need {}",
                    fact.id().get()
                ))
            })?;
            if fact.relation() != need.relation()
                || fact.target() != need.target()
                || fact.allows_local_view_shadow() != need.allows_local_view_shadow()
            {
                return Err(SqlCompileError::Compilation(format!(
                    "catalog completion fact {} does not match its exact request",
                    fact.id().get()
                )));
            }
            let key = CatalogFactKey::from_fact(fact);
            let entry = match fact.outcome() {
                CatalogRelationOutcome::Resolved(table) => {
                    validate_resolved_target(fact, table)?;
                    CatalogEntry::Resolved(table.clone())
                }
                CatalogRelationOutcome::View(view) => CatalogEntry::View(view.clone()),
                CatalogRelationOutcome::Missing { reason } => CatalogEntry::Missing(reason.clone()),
            };
            if entries.insert(key, entry).is_some() {
                return Err(SqlCompileError::Compilation(format!(
                    "catalog completion repeats lookup for `{}`",
                    fact.relation().fqn()
                )));
            }
        }
        if entries.len() != needs.len() {
            return Err(SqlCompileError::Compilation(
                "catalog completion did not answer every exact request".to_string(),
            ));
        }
        Ok(Self {
            current_catalog: current_catalog.map(Into::into),
            current_database: current_database.into(),
            lookup_mode,
            root_local_view_shadow: true,
            definition_target: None,
            entries,
        })
    }

    fn entry(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        target: CatalogLookupTarget,
    ) -> Result<&CatalogEntry, String> {
        let parts = match catalog {
            Some(catalog) => vec![catalog.to_string(), database.to_string(), table.to_string()],
            None => vec![database.to_string(), table.to_string()],
        };
        // Same reading as when the need was collected: with no current
        // catalog the name is in the default one, and the answer to whether
        // the relation is there is below.
        let identity = resolve_catalog_table_name(
            &parts,
            Some(
                self.current_catalog
                    .as_deref()
                    .unwrap_or(novarocks_types::naming::DEFAULT_CATALOG),
            ),
            &self.current_database,
        )?;
        let mut key = CatalogFactKey::new(identity.clone(), target);
        key.local_view_shadow = self.root_local_view_shadow
            && catalog.is_none()
            && matches!(target, CatalogLookupTarget::Table { .. });
        self.entries.get(&key).ok_or_else(|| {
            format!(
                "catalog relation '{}' was not requested for {target:?}",
                identity.fqn()
            )
        })
    }

    fn resolve(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        target: CatalogLookupTarget,
    ) -> Result<ResolvedAnalyzerTable, String> {
        match self.entry(catalog, database, table, target)? {
            CatalogEntry::Resolved(table) => Ok(table.as_ref().clone()),
            CatalogEntry::Missing(reason) => {
                Err(format!("catalog relation '{table}' is missing: {reason}"))
            }
            CatalogEntry::View(_) => Err(format!(
                "catalog relation '{table}' requires stored-view declaration analysis"
            )),
        }
    }
}

impl PlannerTableProvider for FactBackedCatalog {
    fn resolve_table_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve(
            catalog,
            database,
            table,
            CatalogLookupTarget::Table {
                mode: self.lookup_mode,
            },
        )
    }

    fn view_definition_target(&self) -> Option<&TableIdentity> {
        self.definition_target.as_ref()
    }

    fn resolve_view_for_analysis(
        &self,
        catalog: Option<&str>,
        database: &str,
        view: &str,
    ) -> Result<Option<&crate::catalog::SqlViewDefinition>, crate::analyze_error::AnalyzeError>
    {
        // These names came from SQL syntax, unlike an absent fact for a
        // valid need. Preserve the naming author's capability refusal rather
        // than turning an unsupported quoted identifier into a protocol bug.
        for name in catalog.into_iter().chain([database, view]) {
            normalize_identifier(name)
                .map_err(crate::analyze_error::AnalyzeError::unsupported_expression_without_span)?;
        }
        let entry = self
            .entry(
                catalog,
                database,
                view,
                CatalogLookupTarget::Table {
                    mode: self.lookup_mode,
                },
            )
            .map_err(crate::analyze_error::AnalyzeError::internal)?;
        Ok(match entry {
            CatalogEntry::View(view) => Some(view.as_ref()),
            CatalogEntry::Resolved(_) | CatalogEntry::Missing(_) => None,
        })
    }

    fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
        Some(self)
    }
}

impl IcebergMetadataTableProvider for FactBackedCatalog {
    fn get_iceberg_metadata_table(
        &self,
        catalog: Option<&str>,
        database: &str,
        table: &str,
        metadata_table_type: SqlMetadataTableKind,
    ) -> Result<ResolvedAnalyzerTable, String> {
        self.resolve(
            catalog,
            database,
            table,
            CatalogLookupTarget::IcebergMetadata {
                kind: metadata_table_type,
            },
        )
    }
}

fn validate_resolved_target(
    fact: &CatalogRelationFact,
    table: &ResolvedAnalyzerTable,
) -> Result<(), SqlCompileError> {
    if table.catalog.identity != *fact.relation() {
        return Err(SqlCompileError::Compilation(format!(
            "catalog completion fact {} resolved `{}` instead of `{}`",
            fact.id().get(),
            table.catalog.identity.fqn(),
            fact.relation().fqn()
        )));
    }
    let ScanSource::Sql(source) = &table.planner.source;
    if source.table.catalog != fact.relation().catalog
        || source.table.namespace != fact.relation().namespace
        || source.table.table != fact.relation().table
    {
        return Err(SqlCompileError::Compilation(format!(
            "catalog completion fact {} carries a mismatched planner source identity",
            fact.id().get()
        )));
    }
    let matches_target = match (fact.target(), &source.kind) {
        (CatalogLookupTarget::Table { .. }, SqlScanKind::Metadata { .. }) => false,
        (CatalogLookupTarget::Table { .. }, _) => true,
        (
            CatalogLookupTarget::IcebergMetadata { kind: expected },
            SqlScanKind::Metadata { kind: actual, .. },
        ) => expected == *actual,
        (CatalogLookupTarget::IcebergMetadata { .. }, _) => false,
    };
    if !matches_target {
        return Err(SqlCompileError::Compilation(format!(
            "catalog completion fact {} carries a planner source incompatible with {:?}",
            fact.id().get(),
            fact.target()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binding::SqlTableBindingId;
    use crate::planner::table::{
        SqlScanSource, SqlTableIdentity, SqlTableVersionSelector, TableDef,
    };

    fn parse_query(sql: &str) -> ast::Query {
        let statements = novarocks_parser::parse(sql).expect("query must parse");
        let [ast::Statement::Query(query)] = statements.as_slice() else {
            panic!("expected one query statement");
        };
        query.clone()
    }

    fn state(sql: &str) -> CatalogCompletionState {
        CatalogCompletionState::try_new(
            parse_query(sql),
            Some("session_catalog"),
            "session_db",
            TableLookupMode::SchemaOnly,
            10,
        )
        .expect("catalog needs must collect")
    }

    fn keys(state: &CatalogCompletionState) -> HashSet<CatalogFactKey> {
        state
            .needs()
            .iter()
            .map(CatalogFactKey::from_need)
            .collect()
    }

    fn key(
        catalog: &str,
        namespace: &str,
        table: &str,
        target: CatalogLookupTarget,
    ) -> CatalogFactKey {
        CatalogFactKey::new(TableIdentity::new(catalog, namespace, table), target)
    }

    fn implicit_key(
        catalog: &str,
        namespace: &str,
        table: &str,
        target: CatalogLookupTarget,
    ) -> CatalogFactKey {
        let mut key = key(catalog, namespace, table, target);
        key.local_view_shadow = true;
        key
    }

    fn resolved_table(identity: &TableIdentity, kind: SqlScanKind) -> ResolvedAnalyzerTable {
        let planner = TableDef {
            name: identity.table.clone(),
            columns: Vec::new(),
            iceberg_row_lineage_metadata_columns: Vec::new(),
            source: ScanSource::Sql(SqlScanSource::new(
                SqlTableBindingId::new_for_test(1),
                SqlTableIdentity::try_new(
                    identity.catalog.clone(),
                    identity.namespace.clone(),
                    identity.table.clone(),
                )
                .expect("identity must be valid"),
                kind,
            )),
        };
        ResolvedAnalyzerTable::from_planner(Some(&identity.catalog), &identity.namespace, planner)
    }

    #[test]
    fn collects_all_relations_in_one_batch_with_exact_name_resolution() {
        let state = state(
            "SELECT * FROM local_table l \
             JOIN other_db.orders o ON l.id = o.id \
             JOIN remote_catalog.sales.orders r ON o.id = r.id",
        );
        let expected = HashSet::from([
            implicit_key(
                "session_catalog",
                "session_db",
                "local_table",
                CatalogLookupTarget::Table {
                    mode: TableLookupMode::SchemaOnly,
                },
            ),
            implicit_key(
                "session_catalog",
                "other_db",
                "orders",
                CatalogLookupTarget::Table {
                    mode: TableLookupMode::SchemaOnly,
                },
            ),
            key(
                "remote_catalog",
                "sales",
                "orders",
                CatalogLookupTarget::Table {
                    mode: TableLookupMode::SchemaOnly,
                },
            ),
        ]);
        assert_eq!(keys(&state), expected);
        assert_eq!(state.next_need_ordinal(), 13);
    }

    #[test]
    fn cte_shadowing_and_nested_queries_do_not_issue_catalog_needs_for_ctes() {
        let state = state(
            "WITH orders AS (SELECT * FROM source_catalog.raw.orders), \
                  nested AS (WITH orders AS (SELECT * FROM source_catalog.raw.inner_orders) \
                             SELECT * FROM orders) \
             SELECT * FROM orders \
             JOIN nested ON 1 = 1 \
             JOIN source_catalog.audit.orders a ON 1 = 1 \
             WHERE EXISTS (SELECT 1 FROM source_catalog.audit.events)",
        );
        let table_target = CatalogLookupTarget::Table {
            mode: TableLookupMode::SchemaOnly,
        };
        assert_eq!(
            keys(&state),
            HashSet::from([
                key("source_catalog", "raw", "orders", table_target),
                key("source_catalog", "raw", "inner_orders", table_target),
                key("source_catalog", "audit", "orders", table_target),
                key("source_catalog", "audit", "events", table_target),
            ])
        );
    }

    #[test]
    fn additional_mv_queries_and_targets_share_one_exact_catalog_batch() {
        let main = parse_query("SELECT * FROM iceberg.prod.orders");
        let candidates = [
            parse_query("SELECT * FROM iceberg.prod.orders"),
            parse_query("SELECT * FROM iceberg.raw.lineitem"),
        ];
        let targets = [TableIdentity::new("iceberg", "mv", "orders_rollup")];
        let state = CatalogCompletionState::try_new_with_additional_queries(
            main,
            &candidates,
            &targets,
            Some("iceberg"),
            "prod",
            TableLookupMode::SchemaOnly,
            10,
        )
        .expect("combined catalog batch");
        let target = CatalogLookupTarget::Table {
            mode: TableLookupMode::SchemaOnly,
        };
        assert_eq!(
            keys(&state),
            HashSet::from([
                key("iceberg", "prod", "orders", target),
                key("iceberg", "raw", "lineitem", target),
                key("iceberg", "mv", "orders_rollup", target),
            ])
        );
        assert_eq!(state.next_need_ordinal(), 13);
    }

    #[test]
    fn recursive_cte_is_preanalyzed_before_catalog_collection() {
        let state = state(
            "WITH RECURSIVE seq(n) AS (\
                 SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 2\
             ) SELECT * FROM seq",
        );
        assert!(state.needs().is_empty());
    }

    #[test]
    fn metadata_and_ordinary_lookups_are_distinct_and_each_is_deduplicated() {
        let state = state(
            "SELECT * FROM iceberg.prod.orders$files f1 \
             JOIN iceberg.prod.orders$files f2 ON 1 = 1 \
             JOIN iceberg.prod.orders o ON 1 = 1",
        );
        let identity = TableIdentity::new("iceberg", "prod", "orders");
        assert_eq!(state.needs().len(), 2);
        assert!(keys(&state).contains(&CatalogFactKey::new(
            identity.clone(),
            CatalogLookupTarget::IcebergMetadata {
                kind: SqlMetadataTableKind::Files,
            },
        )));
        assert!(keys(&state).contains(&CatalogFactKey::new(
            identity,
            CatalogLookupTarget::Table {
                mode: TableLookupMode::SchemaOnly,
            },
        )));
    }

    #[test]
    fn partitions_view_and_files_share_one_exact_frozen_catalog_fact() {
        let state = state(
            "WITH orders AS (SELECT 1 AS content) \
             SELECT * FROM orders$partitions p \
             JOIN session_catalog.session_db.orders$files f ON 1 = 1",
        );
        assert_eq!(state.needs().len(), 1);
        let need = &state.needs()[0];
        assert_eq!(
            need.relation(),
            &TableIdentity::new("session_catalog", "session_db", "orders")
        );
        assert!(matches!(
            need.target(),
            CatalogLookupTarget::IcebergMetadata {
                kind: SqlMetadataTableKind::Files
            }
        ));
        let table = resolved_table(
            need.relation(),
            SqlScanKind::Metadata {
                kind: SqlMetadataTableKind::Files,
                version: SqlTableVersionSelector::Current,
            },
        );
        let ScanSource::Sql(original_binding) = table.planner.source.clone();
        let fact = CatalogRelationFact::resolved(need, table).expect("exact Files fact");
        let catalog = state.fact_catalog(&[fact]).expect("frozen catalog");
        let resolved = catalog
            .get_iceberg_metadata_table(None, "session_db", "orders", SqlMetadataTableKind::Files)
            .expect("the view consumes its frozen Files fact");
        let ScanSource::Sql(resolved_source) = resolved.planner.source;
        assert_eq!(resolved_source, original_binding);
        assert!(
            catalog
                .get_iceberg_metadata_table(
                    None,
                    "session_db",
                    "orders",
                    SqlMetadataTableKind::Partitions,
                )
                .is_err()
        );
    }

    #[test]
    fn same_table_name_in_different_namespaces_remains_distinct() {
        let state = state(
            "SELECT * FROM iceberg.sales.orders s \
             JOIN iceberg.audit.orders a ON s.id = a.id",
        );
        let target = CatalogLookupTarget::Table {
            mode: TableLookupMode::SchemaOnly,
        };
        assert_eq!(state.needs().len(), 2);
        assert!(keys(&state).contains(&key("iceberg", "sales", "orders", target)));
        assert!(keys(&state).contains(&key("iceberg", "audit", "orders", target)));
    }

    #[test]
    fn internal_delta_table_function_collects_its_explicit_base_identity() {
        let state =
            state("SELECT * FROM TABLE(__nr_ivm_delta('iceberg.raw.events', 10, 20)) AS delta");
        assert_eq!(
            keys(&state),
            HashSet::from([key(
                "iceberg",
                "raw",
                "events",
                CatalogLookupTarget::Table {
                    mode: TableLookupMode::SchemaOnly,
                },
            )])
        );
    }

    #[test]
    fn absent_current_catalog_reads_an_unqualified_relation_in_the_default_one() {
        let state = CatalogCompletionState::try_new(
            parse_query("SELECT * FROM orders"),
            None,
            "db",
            TableLookupMode::SchemaOnly,
            1,
        )
        .expect("an unqualified relation is in the default catalog");
        let relations = state
            .needs()
            .iter()
            .map(|need| need.relation().fqn())
            .collect::<Vec<_>>();
        assert_eq!(relations, vec!["default_catalog.db.orders".to_string()]);
    }

    #[test]
    fn fact_catalog_returns_exact_missing_fact_and_never_uses_another_namespace() {
        let state = state(
            "SELECT * FROM iceberg.sales.orders s \
             JOIN iceberg.audit.orders a ON 1 = 1",
        );
        let facts = state
            .needs()
            .iter()
            .map(|need| {
                CatalogRelationFact::missing(need, format!("missing {}", need.relation().fqn()))
                    .expect("missing fact must be valid")
            })
            .collect::<Vec<_>>();
        let catalog = state.fact_catalog(&facts).expect("facts must bind exactly");
        let error = catalog
            .resolve_table_for_analysis(Some("iceberg"), "sales", "orders")
            .expect_err("missing fact must fail closed");
        assert!(error.contains("iceberg.sales.orders"));
        assert!(!error.contains("iceberg.audit.orders"));
        assert!(
            catalog
                .resolve_table_for_analysis(Some("iceberg"), "unknown", "orders")
                .expect_err("an unrequested namespace must fail closed")
                .contains("was not requested")
        );
    }

    #[test]
    fn resolved_fact_must_match_ordinary_vs_metadata_scan_kind() {
        let metadata_state = state("SELECT * FROM iceberg.prod.orders$entries");
        let metadata_need = &metadata_state.needs()[0];
        let ordinary = resolved_table(
            metadata_need.relation(),
            SqlScanKind::Data {
                version: SqlTableVersionSelector::Current,
            },
        );
        assert!(CatalogRelationFact::resolved(metadata_need, ordinary).is_err());

        let ordinary_state = state("SELECT * FROM iceberg.prod.orders");
        let ordinary_need = &ordinary_state.needs()[0];
        let metadata = resolved_table(
            ordinary_need.relation(),
            SqlScanKind::Metadata {
                kind: SqlMetadataTableKind::Files,
                version: SqlTableVersionSelector::Current,
            },
        );
        assert!(CatalogRelationFact::resolved(ordinary_need, metadata).is_err());
    }

    #[test]
    fn metadata_fact_must_match_the_exact_requested_kind() {
        let state = state("SELECT * FROM iceberg.prod.orders$entries");
        let need = &state.needs()[0];
        let wrong_kind = resolved_table(
            need.relation(),
            SqlScanKind::Metadata {
                kind: SqlMetadataTableKind::Files,
                version: SqlTableVersionSelector::Current,
            },
        );
        assert!(CatalogRelationFact::resolved(need, wrong_kind).is_err());
    }

    #[test]
    fn fact_catalog_rejects_mismatched_inner_planner_source_identity() {
        let state = state("SELECT * FROM iceberg.prod.orders");
        let need = &state.needs()[0];
        let wrong_identity = TableIdentity::new("iceberg", "other", "orders");
        let planner = resolved_table(
            &wrong_identity,
            SqlScanKind::Data {
                version: SqlTableVersionSelector::Current,
            },
        )
        .planner;
        let table = ResolvedAnalyzerTable {
            catalog: novarocks_types::schema::CatalogTable {
                identity: need.relation().clone(),
                columns: Vec::new(),
                hidden_columns: Vec::new(),
            },
            planner,
        };
        assert!(matches!(
            CatalogRelationFact::resolved(need, table),
            Err(
                super::super::completion::CompletionProtocolError::CatalogLookupTargetMismatch { .. }
            )
        ));
    }
}

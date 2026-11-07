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

//! Semantic analyzer: converts typed parser `Query` values into `ResolvedQuery`.
//!
//! This module performs name resolution, type inference, and scope management
//! without producing any physical plan concepts (tuple_id, slot_id, etc.).

/// Share the catalog declaration author with checked inline relation creation.
pub(crate) fn inline_column_value_type(
    column: &novarocks_types::schema::ColumnDef,
    control: &crate::compiler::SqlCompileControl,
) -> Result<novarocks_type_contract::FunctionValueType, crate::compiler::SqlCompileError> {
    helpers::column_value_type(column, control).map_err(Into::into)
}

/// A view TypeId carries no value NULL promises. Reuse the original recursive
/// declaration author without changing the analyzed source or its domains.
pub(crate) fn view_value_type(
    value: &novarocks_type_contract::FunctionValueType,
    control: &crate::compiler::SqlCompileControl,
) -> Result<novarocks_type_contract::FunctionValueType, crate::compiler::SqlCompileError> {
    root_output_declaration::widen_declaration(value, control)
}

#[cfg(test)]
mod canonical_call_tests;
#[cfg(test)]
mod column_type_control_tests;
#[cfg(test)]
mod element_at_control_tests;
#[cfg(test)]
mod extract_binding_tests;
#[cfg(test)]
mod full_value_type_tests;
pub(crate) mod functions;
mod helpers;
#[cfg(test)]
pub(crate) use helpers::sql_logical_projection;
mod literal_coercion;
#[cfg(test)]
mod load_op_column;
mod logical_output;
#[allow(
    dead_code,
    reason = "The T4 typed-query contract flip wires this staged pre-analysis module."
)]
pub(crate) mod query_prepass;
mod resolve_expr;
pub(crate) mod root_output_declaration;
#[cfg(test)]
mod scalar_rebind_contract_tests;
mod value_conversion;
mod view;
#[cfg(test)]
mod window_canonical_tests;
/// The analyzer owns this bound and enforces it; it is named outside the
/// analyzer only where a test has to build a chain that crosses it.
#[cfg(test)]
pub(crate) use resolve_expr::MAX_BOOLEAN_CHAIN_OPERANDS;
mod resolve_from;
mod scope;
mod subquery_rewrite;

pub mod alter_iceberg_ref;
pub mod iceberg_metadata;
mod iceberg_partitions;
pub mod iceberg_ref;
pub(crate) mod mv_lineage;

use arrow::datatypes::DataType;
use novarocks_parser::ast;

use crate::analyze_error::AnalyzeError;
use crate::catalog::PlannerTableProvider;
use crate::column_id::ColumnId;

use crate::analysis::{
    AnalyzedOutputField as OutputColumn, AnalyzedProjectItem as ProjectItem, ExprKind, JoinKind,
    JoinRelation, QueryBody, Relation, ResolvedQuery, ResolvedSelect, ResolvedSetOp,
    ResolvedValues, SetOpKind, SortItem, SubqueryInfo, TypedExpr,
};

use helpers::{expr_display_name, extract_limit, extract_offset};
use scope::AnalyzerScope;

#[cfg(test)]
pub(crate) fn display_expr_for_test(expr: &ast::Expr) -> String {
    helpers::expr_display_name(expr)
}

#[derive(Clone, Debug)]
struct RepeatGroupBySpec {
    grouping_sets: Vec<Vec<ast::Expr>>,
    all_group_by_exprs: Vec<ast::Expr>,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Analyze a parsed SQL query and produce a fully resolved query IR,
/// along with a registry of all non-recursive CTE definitions.
#[cfg(test)]
pub(crate) fn analyze(
    query: &ast::Query,
    catalog: &dyn PlannerTableProvider,
    current_database: &str,
) -> Result<
    (
        ResolvedQuery,
        crate::analysis::cte::CTERegistry,
        crate::column_id::ColumnRefFactory,
    ),
    AnalyzeError,
> {
    analyze_with_function_catalog(
        query,
        catalog,
        current_database,
        crate::functions::builtin_sql_function_catalog(),
        crate::constant::test_constant_policy(),
        &crate::compiler::SqlCompileControl::unbounded(),
    )
}

/// Analyze using the immutable function semantics captured by the compiler
/// request.
pub(crate) fn analyze_with_function_catalog(
    query: &ast::Query,
    catalog: &dyn PlannerTableProvider,
    current_database: &str,
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<
    (
        ResolvedQuery,
        crate::analysis::cte::CTERegistry,
        crate::column_id::ColumnRefFactory,
    ),
    AnalyzeError,
> {
    analyze_with_factory_and_function_catalog(
        query,
        catalog,
        current_database,
        crate::column_id::ColumnRefFactory::new(),
        function_catalog,
        constant_policy,
        control,
    )
}

/// Analyze using the semantic setting frozen by the SQL compiler.
#[cfg(test)]
pub(crate) fn analyze_with_function_catalog_and_sql_semantics(
    query: &ast::Query,
    catalog: &dyn PlannerTableProvider,
    current_database: &str,
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    sql_semantics: &crate::sql_mode::SqlSemanticSettings,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<
    (
        ResolvedQuery,
        crate::analysis::cte::CTERegistry,
        crate::column_id::ColumnRefFactory,
    ),
    AnalyzeError,
> {
    analyze_with_materialized_values_and_sql_semantics(
        query,
        catalog,
        current_database,
        function_catalog,
        sql_semantics,
        constant_policy,
        control,
        None,
    )
}

pub(crate) fn analyze_with_materialized_values_and_sql_semantics(
    query: &ast::Query,
    catalog: &dyn PlannerTableProvider,
    current_database: &str,
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    sql_semantics: &crate::sql_mode::SqlSemanticSettings,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
    materialized: Option<&crate::compiler::materialized_values::MaterializedValues>,
) -> Result<
    (
        ResolvedQuery,
        crate::analysis::cte::CTERegistry,
        crate::column_id::ColumnRefFactory,
    ),
    AnalyzeError,
> {
    analyze_with_factory_and_function_catalog_inner(
        query,
        catalog,
        current_database,
        crate::column_id::ColumnRefFactory::new(),
        function_catalog,
        sql_semantics,
        constant_policy,
        control,
        materialized,
    )
}

/// Test-only analysis helper that threads an existing [`ColumnRefFactory`] so that
/// ColumnIds allocated by this analysis never collide with ids the caller
/// already minted (used by MV rewrite candidate preparation, which analyzes
/// the MV defining SQL inside an already-planned user query).
#[cfg(test)]
#[allow(
    dead_code,
    reason = "Retained for staged SQL planner migration consumers and test helpers."
)]
pub(crate) fn analyze_with_factory(
    query: &ast::Query,
    catalog: &dyn PlannerTableProvider,
    current_database: &str,
    factory: crate::column_id::ColumnRefFactory,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<
    (
        ResolvedQuery,
        crate::analysis::cte::CTERegistry,
        crate::column_id::ColumnRefFactory,
    ),
    AnalyzeError,
> {
    analyze_with_factory_and_function_catalog(
        query,
        catalog,
        current_database,
        factory,
        crate::functions::builtin_sql_function_catalog(),
        constant_policy,
        control,
    )
}

/// Analyze with an existing factory and the immutable function catalog
/// supplied by one compiler request.
pub(crate) fn analyze_with_factory_and_function_catalog(
    query: &ast::Query,
    catalog: &dyn PlannerTableProvider,
    current_database: &str,
    factory: crate::column_id::ColumnRefFactory,
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
) -> Result<
    (
        ResolvedQuery,
        crate::analysis::cte::CTERegistry,
        crate::column_id::ColumnRefFactory,
    ),
    AnalyzeError,
> {
    analyze_with_factory_and_function_catalog_inner(
        query,
        catalog,
        current_database,
        factory,
        function_catalog,
        &crate::sql_mode::SqlSemanticSettings::default(),
        constant_policy,
        control,
        None,
    )
}

fn analyze_with_factory_and_function_catalog_inner(
    query: &ast::Query,
    catalog: &dyn PlannerTableProvider,
    current_database: &str,
    factory: crate::column_id::ColumnRefFactory,
    function_catalog: &dyn crate::compiler::SqlFunctionCatalog,
    sql_semantics: &crate::sql_mode::SqlSemanticSettings,
    constant_policy: novarocks_functions::ConstantPolicy,
    control: &crate::compiler::SqlCompileControl,
    materialized: Option<&crate::compiler::materialized_values::MaterializedValues>,
) -> Result<
    (
        ResolvedQuery,
        crate::analysis::cte::CTERegistry,
        crate::column_id::ColumnRefFactory,
    ),
    AnalyzeError,
> {
    novarocks_type_contract::PureCompileControl::checkpoint(
        control,
        novarocks_type_contract::CompilePhase::Validate,
        0,
    )
    .map_err(AnalyzeError::control)?;
    let query = crate::sql_mode::normalize_concat_query(query.clone(), sql_semantics)?;
    let query = query_prepass::preanalyze(query)?;
    let materialized_source = materialized
        .map(|source| source.loan(&query, control))
        .transpose()
        .map_err(|error| match error {
            crate::compiler::SqlCompileError::Cancelled => {
                AnalyzeError::control(novarocks_type_contract::CompileControlError::Cancelled)
            }
            crate::compiler::SqlCompileError::DeadlineExceeded => AnalyzeError::control(
                novarocks_type_contract::CompileControlError::DeadlineExceeded,
            ),
            crate::compiler::SqlCompileError::ResourceExhausted => AnalyzeError::control(
                novarocks_type_contract::CompileControlError::ResourceExhausted,
            ),
            other => AnalyzeError::internal(other.to_string()),
        })?;
    let materialized_values = materialized_source.as_ref().map(|source| source.view());
    let factory = std::rc::Rc::new(std::cell::RefCell::new(factory));
    let ctx = AnalyzerContext {
        grouping_scope: std::cell::RefCell::new(None),
        catalog,
        current_database,
        function_catalog,
        constant_policy,
        materialized_values,
        control,
        sql_semantics: sql_semantics.clone(),
        factory: factory.clone(),
        view_stack: Default::default(),
        ctes: std::collections::HashMap::new(),
        pending_ctes: std::collections::HashSet::new(),
        next_subquery_id: std::cell::Cell::new(0),
        next_lambda_slot_id: std::cell::Cell::new(0),
        collected_subqueries: std::cell::RefCell::new(Vec::new()),
        cte_registry: std::cell::RefCell::new(crate::analysis::cte::CTERegistry::new()),
    };
    let resolved = ctx.analyze_query(&query)?;
    ctx.check_control()?;
    let registry = ctx.cte_registry.into_inner();
    let col_factory = std::rc::Rc::try_unwrap(factory)
        .map(|cell| cell.into_inner())
        .unwrap_or_else(|rc| rc.borrow().clone());
    Ok((resolved, registry, col_factory))
}

// ---------------------------------------------------------------------------
// Analyzer context
// ---------------------------------------------------------------------------

/// Analysis state belongs to one SELECT, never to an enclosing query's children.
struct GroupingAnalysis {
    select_span: novarocks_parser::Span,
    keys: Vec<ast::Expr>,
    repeat: bool,
    enabled: std::cell::Cell<bool>,
    next_mask_index: std::cell::Cell<usize>,
    from_scope: std::cell::RefCell<Option<AnalyzerScope>>,
    resolved_keys: std::cell::RefCell<Option<Vec<TypedExpr>>>,
    calls: std::cell::RefCell<Vec<GroupingCall>>,
}

#[derive(Clone)]
struct GroupingCall {
    syntax: ast::FunctionCall,
    name: String,
    columns: Vec<String>,
    typed_arguments: Vec<TypedExpr>,
    column_id: Option<ColumnId>,
}

pub(super) struct AnalyzerContext<'a> {
    grouping_scope: std::cell::RefCell<Option<std::rc::Rc<GroupingAnalysis>>>,
    view_stack: view::ViewAnalysisStack,
    pub(super) control: &'a crate::compiler::SqlCompileControl,
    pub(super) constant_policy: novarocks_functions::ConstantPolicy,
    materialized_values: Option<crate::compiler::materialized_values::MaterializedValuesLoan<'a>>,
    pub(super) catalog: &'a dyn PlannerTableProvider,
    pub(super) current_database: &'a str,
    pub(super) function_catalog: &'a dyn crate::compiler::SqlFunctionCatalog,
    pub(super) sql_semantics: crate::sql_mode::SqlSemanticSettings,
    /// Shared factory for allocating globally unique ColumnIds.
    pub(super) factory: std::rc::Rc<std::cell::RefCell<crate::column_id::ColumnRefFactory>>,
    /// Currently visible CTE definitions from outer scopes or earlier entries
    /// in the same WITH clause, keyed by lowercase name.
    pub(super) ctes: std::collections::HashMap<String, crate::analysis::cte::CteId>,
    /// Names declared by the current WITH clause but not yet visible because
    /// their definitions have not been analyzed.
    pub(super) pending_ctes: std::collections::HashSet<String>,
    /// Counter for generating unique subquery placeholder IDs.
    pub(super) next_subquery_id: std::cell::Cell<usize>,
    /// Counter for generating synthetic slot ids used only inside lambda expressions.
    pub(super) next_lambda_slot_id: std::cell::Cell<i32>,
    /// Subqueries collected during expression analysis.
    /// Populated by `resolve_expr.rs`, consumed by `subquery_rewrite.rs`.
    pub(super) collected_subqueries: std::cell::RefCell<Vec<SubqueryInfo>>,
    /// Accumulated CTE registry for the current query analysis.
    pub(super) cte_registry: std::cell::RefCell<crate::analysis::cte::CTERegistry>,
}

impl<'a> AnalyzerContext<'a> {
    pub(super) fn check_control(&self) -> Result<(), AnalyzeError> {
        novarocks_type_contract::PureCompileControl::checkpoint(
            self.control,
            novarocks_type_contract::CompilePhase::Validate,
            0,
        )
        .map_err(AnalyzeError::control)
    }

    /// The closure ensures restoration on every ordinary success/error exit.
    /// Owned AST/state avoids a borrow into a nested query or a RefCell loan
    /// across recursive expression analysis.
    fn with_grouping_scope<T>(
        &self,
        select: Option<&ast::Select>,
        analyze: impl FnOnce(&Self) -> Result<T, AnalyzeError>,
    ) -> Result<T, AnalyzeError> {
        let state = select.map(|select| {
            let repeat = self.extract_repeat_from_group_by(select);
            let keys = if let Some(spec) = &repeat {
                spec.all_group_by_exprs.clone()
            } else if let ast::GroupBy::Expressions { expressions, .. } = &select.group_by {
                expressions.clone()
            } else {
                Vec::new()
            };
            std::rc::Rc::new(GroupingAnalysis {
                select_span: select.span,
                keys,
                repeat: repeat.is_some(),
                enabled: std::cell::Cell::new(false),
                next_mask_index: std::cell::Cell::new(0),
                from_scope: std::cell::RefCell::new(None),
                resolved_keys: std::cell::RefCell::new(None),
                calls: std::cell::RefCell::new(Vec::new()),
            })
        });
        let previous = self.grouping_scope.replace(state);
        let result = analyze(self);
        self.grouping_scope.replace(previous);
        result
    }

    fn analyze_grouping_function(
        &self,
        function: &ast::FunctionCall,
        scope: &AnalyzerScope,
    ) -> Result<TypedExpr, AnalyzeError> {
        use crate::analysis::LiteralValue;
        use novarocks_type_contract::FunctionValueType;
        let grouping = function.name.parts[0]
            .value
            .eq_ignore_ascii_case("grouping");
        let count = function.arguments.len();
        // Count occurrences, including duplicate arguments, before analyzing
        // an argument. This is Trino's GROUPING width/arity decision point.
        if grouping && count > 63 {
            return Err(AnalyzeError::too_many_arguments(
                "GROUPING supports at most 63 arguments",
                function.span,
            ));
        }
        if count == 0 {
            return Err(AnalyzeError::invalid_argument(
                "GROUPING requires at least one argument",
                function.span,
            ));
        }
        let state = self.grouping_scope.borrow().clone().ok_or_else(|| {
            AnalyzeError::invalid_argument("GROUPING requires a GROUP BY clause", function.span)
        })?;
        if !state.enabled.get() || state.keys.is_empty() {
            return Err(AnalyzeError::invalid_argument(
                "GROUPING arguments must be grouping columns",
                function.span,
            ));
        }
        let from_scope = state.from_scope.borrow().clone();
        let argument_scope = from_scope.as_ref().unwrap_or(scope);
        let mut typed_arguments = Vec::with_capacity(count);
        for argument in &function.arguments {
            self.check_control()?;
            typed_arguments.push(self.analyze_expr(argument, argument_scope)?);
        }
        let public_type = if grouping && count <= 31 {
            DataType::Int32
        } else {
            DataType::Int64
        };
        // Retries for alias/HAVING/ORDER BY resolution reuse only the same
        // original call occurrence, including its arguments and source span.
        let existing = state
            .calls
            .borrow()
            .iter()
            .find(|call| call.syntax == *function)
            .cloned();
        let call = if let Some(call) = existing {
            call
        } else {
            let index = state.next_mask_index.get();
            state.next_mask_index.set(index + 1);
            let name = format!("__grouping_fn_{index}");
            let column_id = state.repeat.then(|| {
                self.alloc_column_id(
                    None,
                    name.clone(),
                    FunctionValueType::new(DataType::Int64, false),
                )
            });
            let call = GroupingCall {
                syntax: function.clone(),
                name,
                columns: Vec::new(),
                typed_arguments,
                column_id,
            };
            state.calls.borrow_mut().push(call.clone());
            call
        };
        if !state.repeat {
            return Ok(TypedExpr {
                kind: ExprKind::Literal(LiteralValue::Int(0)),
                value_type: FunctionValueType::new(public_type, false),
            });
        }
        let mask = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: call.column_id.expect("Repeat call has an internal mask"),
                qualifier: None,
                column: call.name,
            },
            value_type: FunctionValueType::new(DataType::Int64, false),
        };
        if public_type == DataType::Int64 {
            return Ok(mask);
        }
        // The public INTEGER is a real typed conversion. Preserve its author's
        // NULL law and the original internal non-null BIGINT Repeat slot.
        let target = helpers::declared_cast_value_type(
            &mask,
            FunctionValueType::new(public_type, true),
            self.control,
        )?;
        self.cast_to_value_type(mask, target, function.span)
    }

    /// Reuse the completed GROUP BY author's actual expressions, including
    /// ordinal/alias expansion. Public types are already fixed before parent
    /// binding; this step grants membership and canonical Repeat key identity.
    fn validate_grouping_calls(&self, group_by: &[TypedExpr]) -> Result<(), AnalyzeError> {
        let Some(state) = self.grouping_scope.borrow().clone() else {
            return Ok(());
        };
        let existing = state.resolved_keys.borrow().clone();
        let keys = if let Some(keys) = existing {
            keys
        } else {
            let keys = group_by.to_vec();
            *state.resolved_keys.borrow_mut() = Some(keys.clone());
            keys
        };
        let mut calls = state.calls.borrow().clone();
        for call in &mut calls {
            call.columns.clear();
            for (argument, syntax) in call.typed_arguments.iter().zip(&call.syntax.arguments) {
                let mut matched = None;
                for (index, key) in keys.iter().enumerate() {
                    self.check_control()?;
                    let equal = match (&argument.kind, &key.kind) {
                        (
                            ExprKind::ColumnRef {
                                column_id: left, ..
                            },
                            ExprKind::ColumnRef {
                                column_id: right, ..
                            },
                        ) => left == right,
                        _ => crate::analysis::expr_identity::typed_expr_semantically_eq(
                            argument,
                            key,
                            self.control,
                        )
                        .map_err(|error| {
                            use crate::compiler::SqlCompileError;
                            use novarocks_type_contract::CompileControlError;
                            match error {
                                SqlCompileError::Cancelled => {
                                    AnalyzeError::control(CompileControlError::Cancelled)
                                }
                                SqlCompileError::DeadlineExceeded => {
                                    AnalyzeError::control(CompileControlError::DeadlineExceeded)
                                }
                                SqlCompileError::ResourceExhausted => {
                                    AnalyzeError::control(CompileControlError::ResourceExhausted)
                                }
                                SqlCompileError::Analyze(error) => error,
                                SqlCompileError::CompilationSource(source) => {
                                    AnalyzeError::shared_domain_source(source)
                                }
                                other => AnalyzeError::internal(other.to_string()),
                            }
                        })?,
                    };
                    if equal {
                        matched = Some(index);
                        break;
                    }
                }
                let Some(index) = matched else {
                    return Err(AnalyzeError::invalid_argument(
                        "GROUPING arguments must be grouping columns",
                        syntax.span(),
                    ));
                };
                let canonical = state.keys.get(index).map(expr_syntax_key).ok_or_else(|| {
                    AnalyzeError::internal("GROUP BY key ordinal is missing its original syntax")
                })?;
                call.columns.push(canonical);
            }
        }
        *state.calls.borrow_mut() = calls;
        Ok(())
    }

    fn append_grouping_calls(&self, select: &mut ResolvedSelect) {
        let state = self.grouping_scope.borrow().clone();
        let Some(state) = state else {
            return;
        };
        let Some(repeat) = &mut select.repeat else {
            return;
        };
        for call in state.calls.borrow().iter() {
            if repeat
                .grouping_fn_ids
                .iter()
                .any(|(_, id)| Some(*id) == call.column_id)
            {
                continue;
            }
            let Some(column_id) = call.column_id else {
                continue;
            };
            repeat
                .grouping_fn_args
                .push((call.name.clone(), call.columns.clone()));
            repeat.grouping_fn_ids.push((call.name.clone(), column_id));
            select.group_by.push(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id,
                    qualifier: None,
                    column: call.name.clone(),
                },
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            });
        }
    }

    fn grouping_display_name(&self, expression: &ast::Expr) -> String {
        let state = self.grouping_scope.borrow().clone();
        let Some(state) = state else {
            return expr_display_name(expression);
        };
        if !state.repeat {
            return expr_display_name(expression);
        }
        let calls = state.calls.borrow().clone();
        let mut display = GroupingDisplayFolder { calls: &calls };
        expr_display_name(&ast::Fold::fold_expr(&mut display, expression.clone()))
    }

    fn with_sql_semantics_scope<T>(
        &self,
        settings: crate::sql_mode::SqlSemanticSettings,
        analyze: impl FnOnce(&Self) -> Result<T, AnalyzeError>,
    ) -> Result<T, AnalyzeError> {
        if settings == self.sql_semantics {
            return analyze(self);
        }
        let child = AnalyzerContext {
            grouping_scope: std::cell::RefCell::new(self.grouping_scope.borrow().clone()),
            control: self.control,
            constant_policy: self.constant_policy,
            materialized_values: self.materialized_values,
            catalog: self.catalog,
            current_database: self.current_database,
            function_catalog: self.function_catalog,
            sql_semantics: settings,
            factory: self.factory.clone(),
            view_stack: self.view_stack.clone(),
            ctes: self.ctes.clone(),
            pending_ctes: self.pending_ctes.clone(),
            next_subquery_id: std::cell::Cell::new(self.next_subquery_id.get()),
            next_lambda_slot_id: std::cell::Cell::new(self.next_lambda_slot_id.get()),
            collected_subqueries: std::cell::RefCell::new(Vec::new()),
            cte_registry: std::cell::RefCell::new(self.cte_registry.borrow().clone()),
        };
        let result = analyze(&child);
        self.absorb_semantic_scope(&child);
        result
    }

    fn absorb_semantic_scope(&self, child: &Self) {
        self.next_subquery_id.set(child.next_subquery_id.get());
        self.next_lambda_slot_id
            .set(child.next_lambda_slot_id.get());
        self.collected_subqueries
            .borrow_mut()
            .extend(child.collected_subqueries.borrow_mut().drain(..));
        *self.cte_registry.borrow_mut() = child.cte_registry.borrow().clone();
    }

    /// Create a new empty AnalyzerScope sharing this context's factory.
    #[expect(
        private_interfaces,
        reason = "The stable SQL shape intentionally carries a crate-private implementation detail."
    )]
    pub(super) fn new_scope(&self) -> scope::AnalyzerScope {
        scope::AnalyzerScope::new(self.factory.clone())
    }

    /// Allocate a fresh ColumnId from the shared factory.
    pub(super) fn alloc_column_id(
        &self,
        qualifier: Option<String>,
        name: String,
        value_type: novarocks_type_contract::FunctionValueType,
    ) -> crate::column_id::ColumnId {
        self.factory
            .borrow_mut()
            .create(qualifier, name, value_type)
    }

    /// Allocate a unique subquery placeholder ID.
    pub(super) fn alloc_subquery_id(&self) -> usize {
        let id = self.next_subquery_id.get();
        self.next_subquery_id.set(id + 1);
        id
    }

    pub(super) fn alloc_lambda_slot_id(&self) -> i32 {
        const LAMBDA_SLOT_ID_BASE: i32 = 1_900_000_000;
        let offset = self.next_lambda_slot_id.get();
        self.next_lambda_slot_id.set(offset + 1);
        LAMBDA_SLOT_ID_BASE - offset
    }

    fn build_with_clause_context(
        &self,
        with_clause: &ast::With,
    ) -> Result<(AnalyzerContext<'a>, Vec<crate::analysis::cte::CteId>), AnalyzeError> {
        let mut pending_ctes = self.pending_ctes.clone();
        for cte in &with_clause.ctes {
            self.check_control()?;
            pending_ctes.insert(cte.name.value.to_lowercase());
        }

        let mut child_ctx = AnalyzerContext {
            grouping_scope: std::cell::RefCell::new(None),
            control: self.control,
            constant_policy: self.constant_policy,
            materialized_values: self.materialized_values,
            catalog: self.catalog,
            current_database: self.current_database,
            function_catalog: self.function_catalog,
            sql_semantics: self.sql_semantics.clone(),
            factory: self.factory.clone(),
            view_stack: self.view_stack.clone(),
            ctes: self.ctes.clone(),
            pending_ctes: pending_ctes.clone(),
            next_subquery_id: std::cell::Cell::new(self.next_subquery_id.get()),
            next_lambda_slot_id: std::cell::Cell::new(self.next_lambda_slot_id.get()),
            collected_subqueries: std::cell::RefCell::new(Vec::new()),
            cte_registry: std::cell::RefCell::new(self.cte_registry.borrow().clone()),
        };
        let mut local_cte_ids = Vec::with_capacity(with_clause.ctes.len());

        for cte in &with_clause.ctes {
            self.check_control()?;
            let name = cte.name.value.to_lowercase();
            pending_ctes.remove(&name);
            child_ctx.pending_ctes = pending_ctes.clone();

            let mut resolved_cte = child_ctx.analyze_query(&cte.query)?;
            crate::analysis::schema::apply_column_aliases(
                &mut resolved_cte.output_columns,
                &cte.columns,
                cte.span,
            )?;

            let output_columns = resolved_cte.output_columns.clone();
            let cte_id = child_ctx.cte_registry.borrow_mut().register(
                name.clone(),
                resolved_cte,
                output_columns,
            );

            child_ctx.ctes.insert(name, cte_id);
            local_cte_ids.push(cte_id);
        }

        Ok((child_ctx, local_cte_ids))
    }

    /// Top-level query analysis.
    fn analyze_query(&self, query: &ast::Query) -> Result<ResolvedQuery, AnalyzeError> {
        self.check_control()?;
        let settings = crate::sql_mode::query_sql_semantics(&self.sql_semantics, query)?;
        self.with_sql_semantics_scope(settings, |ctx| ctx.analyze_query_in_semantic_scope(query))
    }

    fn analyze_query_in_semantic_scope(
        &self,
        query: &ast::Query,
    ) -> Result<ResolvedQuery, AnalyzeError> {
        let (maybe_child_ctx, local_cte_ids) = if let Some(ref with_clause) = query.with {
            let (child_ctx, local_cte_ids) = self.build_with_clause_context(with_clause)?;
            (Some(child_ctx), local_cte_ids)
        } else {
            (None, Vec::new())
        };
        let ctx = maybe_child_ctx.as_ref().unwrap_or(self);
        let select = match query.body.as_ref() {
            ast::SetExpr::Select(select) => Some(select.as_ref()),
            _ => None,
        };
        let result = ctx.with_grouping_scope(select, |ctx| {
            // Analyze body (SELECT / SetOperation / VALUES)
            let (mut body, body_output) = ctx.analyze_set_expr(query.body.as_ref())?;

            // Analyze ORDER BY
            let order_by = ctx.analyze_order_by(query, &body_output, &body)?;
            if let QueryBody::Select(select) = &mut body {
                ctx.validate_grouping_calls(&select.group_by)?;
                ctx.append_grouping_calls(select);
            }

            // Extract LIMIT / OFFSET
            let limit = extract_limit(query)?;
            let offset = extract_offset(query)?;

            // Build output columns from the body
            let output_columns = body_output;

            Ok(ResolvedQuery {
                body,
                order_by,
                limit,
                offset,
                output_columns,
                local_cte_ids,
            })
        });
        if let Some(child_ctx) = maybe_child_ctx {
            self.absorb_semantic_scope(&child_ctx);
        }
        result
    }

    /// Analyze a SetExpr and return (QueryBody, output_columns).
    fn analyze_set_expr(
        &self,
        set_expr: &ast::SetExpr,
    ) -> Result<(QueryBody, Vec<OutputColumn>), AnalyzeError> {
        match set_expr {
            ast::SetExpr::Select(s) => {
                let same_select = self
                    .grouping_scope
                    .borrow()
                    .as_ref()
                    .is_some_and(|state| state.select_span == s.span);
                if !same_select {
                    return self.with_grouping_scope(Some(s), |ctx| ctx.analyze_set_expr(set_expr));
                }
                // Check if GROUP BY contains ROLLUP/CUBE/GROUPING SETS.
                if let Some(repeat_spec) = self.extract_repeat_from_group_by(s) {
                    return self.resolve_repeat_group_by(s, &repeat_spec);
                }
                let (sel, cols) = self.analyze_select(s)?;
                Ok((QueryBody::Select(sel), cols))
            }
            ast::SetExpr::SetOperation(_) => self.analyze_set_operation_chain(set_expr),
            ast::SetExpr::Values(values) => {
                let (resolved_values, cols) = self.analyze_values(values)?;
                Ok((QueryBody::Values(resolved_values), cols))
            }
            ast::SetExpr::Query(q) => {
                let resolved = self.analyze_query(q)?;
                let cols = resolved.output_columns.clone();
                Ok((resolved.body, cols))
            }
        }
    }

    /// Resolve a set operation chain without using one Rust stack frame per
    /// branch. A balanced UNION ALL syntax tree is folded in source order so
    /// pairwise type widening and output names retain left association.
    fn analyze_set_operation_chain(
        &self,
        set_expr: &ast::SetExpr,
    ) -> Result<(QueryBody, Vec<OutputColumn>), AnalyzeError> {
        let (leftmost, operations): (_, Vec<_>) = if matches!(set_expr, ast::SetExpr::SetOperation(operation)
                if operation.operator == ast::SetOperator::Union
                    && operation.quantifier == ast::SetQuantifier::All)
        {
            let mut pending = vec![set_expr];
            let mut operands = Vec::new();
            while let Some(current) = pending.pop() {
                self.check_control()?;
                match current {
                    ast::SetExpr::SetOperation(operation)
                        if operation.operator == ast::SetOperator::Union
                            && operation.quantifier == ast::SetQuantifier::All =>
                    {
                        pending.push(&operation.right);
                        pending.push(&operation.left);
                    }
                    other => operands.push(other),
                }
            }
            let first = operands.remove(0);
            let operations = operands
                .into_iter()
                .map(|right| {
                    self.check_control()?;
                    Ok((
                        ast::SetOperator::Union,
                        ast::SetQuantifier::All,
                        right,
                        set_expr.span(),
                    ))
                })
                .collect::<Result<Vec<_>, AnalyzeError>>()?;
            (first, operations)
        } else {
            let mut operations = Vec::new();
            let mut leftmost = set_expr;
            while let ast::SetExpr::SetOperation(operation) = leftmost {
                self.check_control()?;
                operations.push((
                    operation.operator,
                    operation.quantifier,
                    operation.right.as_ref(),
                    operation.span,
                ));
                leftmost = &operation.left;
            }
            operations.reverse();
            (leftmost, operations)
        };
        let mut left_query = self.analyze_set_operand(leftmost)?;
        for (operator, quantifier, right, span) in operations {
            self.check_control()?;
            let right_query = self.analyze_set_operand(right)?;
            let left_cols = &left_query.output_columns;
            let right_cols = &right_query.output_columns;
            if left_cols.len() != right_cols.len() {
                return Err(AnalyzeError::invalid_query_shape(
                    format!(
                        "set operation column count mismatch: left has {}, right has {}",
                        left_cols.len(),
                        right_cols.len()
                    ),
                    span,
                ));
            }
            let mut output_cols = Vec::with_capacity(left_cols.len());
            for (lc, rc) in left_cols.iter().zip(right_cols) {
                self.check_control()?;
                let value_type = helpers::assignment_common_value_type(
                    &lc.value_type,
                    &rc.value_type,
                    self.control,
                )?;
                let column_id = self.alloc_column_id(None, lc.name.clone(), value_type.clone());
                output_cols.push(OutputColumn {
                    field_name: lc.field_name.clone(),
                    column_id,
                    name: lc.name.clone(),
                    value_type,
                    is_internal: false,
                });
            }
            let kind = match operator {
                ast::SetOperator::Union => SetOpKind::Union,
                ast::SetOperator::Intersect => SetOpKind::Intersect,
                ast::SetOperator::Except => SetOpKind::Except,
            };
            let all = matches!(quantifier, ast::SetQuantifier::All);
            left_query = ResolvedQuery {
                body: QueryBody::SetOperation(ResolvedSetOp {
                    kind,
                    all,
                    left: Box::new(left_query),
                    right: Box::new(right_query),
                }),
                order_by: vec![],
                limit: None,
                offset: None,
                output_columns: output_cols,
                local_cte_ids: vec![],
            };
        }
        Ok((left_query.body, left_query.output_columns))
    }

    fn analyze_set_operand(&self, set_expr: &ast::SetExpr) -> Result<ResolvedQuery, AnalyzeError> {
        match set_expr {
            ast::SetExpr::Query(q) => self.analyze_query(q),
            _ => {
                let (body, output_columns) = self.analyze_set_expr(set_expr)?;
                Ok(ResolvedQuery {
                    body,
                    order_by: vec![],
                    limit: None,
                    offset: None,
                    output_columns,
                    local_cte_ids: vec![],
                })
            }
        }
    }

    /// Analyze a VALUES clause.
    fn analyze_values(
        &self,
        values: &ast::Values,
    ) -> Result<(ResolvedValues, Vec<OutputColumn>), AnalyzeError> {
        self.analyze_values_inner(Some(values), None)
    }

    fn analyze_inline_values(
        &self,
        values: &crate::catalog::SqlInlineRelation,
    ) -> Result<(ResolvedValues, Vec<OutputColumn>), AnalyzeError> {
        self.analyze_values_inner(None, Some(values))
    }

    /// Syntax VALUES infer their fields. An already materialized relation
    /// retains the original pools and starts from its complete declaration.
    fn analyze_values_inner(
        &self,
        values: Option<&ast::Values>,
        inline: Option<&crate::catalog::SqlInlineRelation>,
    ) -> Result<(ResolvedValues, Vec<OutputColumn>), AnalyzeError> {
        let scope = self.new_scope();
        let materialized = values.and_then(|values| {
            self.materialized_values
                .and_then(|source| source.rows_at(values))
        });
        let row_count = match inline {
            Some(inline) => inline.rows,
            None => materialized.map_or_else(
                || values.expect("syntax VALUES input").rows.len(),
                |rows| rows.len(),
            ),
        };
        let mut resolved_rows = if inline.is_some() {
            Vec::new()
        } else {
            Vec::with_capacity(row_count)
        };
        let mut column_types = match inline {
            Some(inline) => inline
                .columns
                .iter()
                .map(|column| column.value_type.clone())
                .collect::<Vec<_>>(),
            None => Vec::new(),
        };
        let mut batch = 0usize;
        let mut batch_start = 0usize;
        for row_idx in 0..row_count {
            self.check_control()?;
            let width = match inline {
                Some(inline) => {
                    while row_idx - batch_start
                        >= inline.batches[batch][0].resource_facts().rows as usize
                    {
                        batch_start += inline.batches[batch][0].resource_facts().rows as usize;
                        batch += 1;
                        self.check_control()?;
                    }
                    inline.columns.len()
                }
                None => materialized.map_or_else(
                    || values.expect("syntax VALUES input").rows[row_idx].len(),
                    |rows| rows[row_idx].len(),
                ),
            };
            let mut resolved_row = Vec::with_capacity(width);
            for col_idx in 0..width {
                self.check_control()?;
                let typed = match (inline, materialized) {
                    (Some(inline), _) => {
                        let ordinal = u32::try_from(row_idx - batch_start).map_err(|_| {
                            AnalyzeError::internal(
                                "inline relation ordinal exceeds its checked pool",
                            )
                        })?;
                        let value = inline.batches[batch][col_idx]
                            .value(ordinal)
                            .map_err(|error| AnalyzeError::internal(error.to_string()))?;
                        TypedExpr {
                            value_type: value.value_type().clone(),
                            kind: ExprKind::Constant(value),
                        }
                    }
                    (None, Some(rows)) => {
                        let value = &rows[row_idx][col_idx];
                        TypedExpr {
                            kind: ExprKind::Constant(value.clone()),
                            value_type: value.value_type().clone(),
                        }
                    }
                    (None, None) => self.analyze_expr(
                        &values.expect("syntax VALUES input").rows[row_idx][col_idx],
                        &scope,
                    )?,
                };
                if inline.is_none() {
                    if col_idx < column_types.len() {
                        column_types[col_idx] = helpers::assignment_common_value_type(
                            &column_types[col_idx],
                            &typed.value_type,
                            self.control,
                        )?;
                    } else {
                        column_types.push(typed.value_type.clone());
                    }
                }
                resolved_row.push(typed);
            }
            resolved_rows.push(resolved_row);
        }
        let output_cols = column_types
            .iter()
            .enumerate()
            .map(|(i, ty)| {
                self.check_control()?;
                let (name, field_name, value_type) = match inline {
                    Some(inline) => {
                        let name = inline.columns[i].name.to_string();
                        (name.clone(), Some(name), ty.clone())
                    }
                    None => (
                        format!("column_{i}"),
                        None,
                        helpers::with_nullability(ty.clone(), true),
                    ),
                };
                let column_id = self.alloc_column_id(None, name.clone(), value_type.clone());
                Ok(OutputColumn {
                    field_name,
                    column_id,
                    name,
                    value_type,
                    is_internal: false,
                })
            })
            .collect::<Result<Vec<_>, AnalyzeError>>()?;
        Ok((
            ResolvedValues {
                rows: resolved_rows,
                output_columns: output_cols.clone(),
            },
            output_cols,
        ))
    }

    /// Analyze a SELECT statement.
    fn analyze_select(
        &self,
        select: &ast::Select,
    ) -> Result<(ResolvedSelect, Vec<OutputColumn>), AnalyzeError> {
        let settings = crate::sql_mode::select_sql_semantics(&self.sql_semantics, select)?;
        self.with_sql_semantics_scope(settings, |ctx| ctx.analyze_select_in_semantic_scope(select))
    }

    fn analyze_select_in_semantic_scope(
        &self,
        select: &ast::Select,
    ) -> Result<(ResolvedSelect, Vec<OutputColumn>), AnalyzeError> {
        // --- FROM clause ---
        let (from, scope) = if select.from.is_empty() {
            // SELECT without FROM (dual)
            (None, self.new_scope())
        } else if select.from.len() == 1 {
            let (rel, scope) = self.analyze_from(&select.from[0])?;
            (Some(rel), scope)
        } else {
            // Multiple comma-separated FROM items → implicit CROSS JOIN.
            // Subsequent entries see the accumulated left-hand scope so
            // table-valued functions (e.g. `unnest(t.arr)`) can reference
            // earlier sibling columns (StarRocks implicit-lateral semantics).
            let mut iter = select.from.iter();
            let first = iter.next().unwrap();
            let (mut current_rel, mut current_scope) = self.analyze_from(first)?;
            for twj in iter {
                let (right_rel, right_scope) =
                    self.analyze_from_with_outer(twj, Some(&current_scope))?;
                current_scope.merge(&right_scope);
                current_rel = Relation::Join(Box::new(JoinRelation {
                    naa_author: None,
                    left: current_rel,
                    right: right_rel,
                    join_type: JoinKind::Cross,
                    condition: None,
                }));
            }
            (Some(current_rel), current_scope)
        };

        // Finish FROM recursion before allocating the SELECT analysis frame.
        // Nested derived tables must not retain every outer projection,
        // grouping and subquery-rewrite temporary on the planning stack.
        self.analyze_select_after_from(select, from, scope)
    }

    #[inline(never)]
    fn analyze_select_after_from(
        &self,
        select: &ast::Select,
        from: Option<Relation>,
        scope: AnalyzerScope,
    ) -> Result<(ResolvedSelect, Vec<OutputColumn>), AnalyzeError> {
        // --- WHERE clause ---
        let filter = match &select.selection {
            Some(expr) => Some(super::analyzer::subquery_rewrite::coerce_where_to_bool(
                self.analyze_expr(expr, &scope)?,
            )),
            None => None,
        };

        if let Some(state) = self.grouping_scope.borrow().as_ref() {
            *state.from_scope.borrow_mut() = Some(scope.clone());
            state.enabled.set(true);
        }
        // --- SELECT list (before GROUP BY so aliases are available) ---
        let (projection, mut output_columns) =
            self.analyze_projection(&select.projection, &scope)?;

        if let Some(state) = self.grouping_scope.borrow().as_ref() {
            state.enabled.set(false);
        }
        // --- GROUP BY (with SELECT alias fallback) ---
        let group_by_exprs = match &select.group_by {
            ast::GroupBy::None => Vec::new(),
            ast::GroupBy::Expressions { expressions, .. } => expressions.clone(),
            ast::GroupBy::Rollup { .. }
            | ast::GroupBy::Cube { .. }
            | ast::GroupBy::GroupingSets { .. } => {
                return Err(AnalyzeError::internal(
                    "internal error: repeat GROUP BY must be expanded before select analysis",
                ));
            }
        };
        let mut group_by = Vec::with_capacity(group_by_exprs.len());
        for gb_expr in &group_by_exprs {
            if let ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::Number(n),
                ..
            }) = gb_expr
            {
                let pos = n.parse::<usize>().map_err(|e| {
                    AnalyzeError::invalid_literal(
                        format!("invalid GROUP BY position: {e}"),
                        gb_expr.span(),
                    )
                })?;
                if pos == 0 || pos > projection.len() {
                    return Err(AnalyzeError::invalid_argument(
                        format!(
                            "GROUP BY position {pos} is out of range (1..{})",
                            projection.len()
                        ),
                        gb_expr.span(),
                    ));
                }
                let select_item = select.projection.get(pos - 1).ok_or_else(|| {
                    AnalyzeError::invalid_argument(
                        format!("GROUP BY position {pos} is out of range"),
                        gb_expr.span(),
                    )
                })?;
                let select_expr = match select_item {
                    ast::SelectItem::UnnamedExpr(expr)
                    | ast::SelectItem::ExprWithAlias { expr, .. } => expr,
                    _ => {
                        return Err(AnalyzeError::invalid_argument(
                            format!("GROUP BY position {pos} must reference a select expression"),
                            gb_expr.span(),
                        ));
                    }
                };
                if self.expr_contains_aggregate(select_expr) {
                    return Err(AnalyzeError::invalid_argument(
                        format!("GROUP BY position {pos} cannot reference an aggregate expression"),
                        gb_expr.span(),
                    ));
                }
                if contains_subquery_placeholder(&projection[pos - 1].expr) {
                    return Err(AnalyzeError::unsupported_query_shape(
                        "subquery is not supported in GROUP BY",
                        gb_expr.span(),
                    ));
                }
                group_by.push(projection[pos - 1].expr.clone());
                continue;
            }
            match self.analyze_expr(gb_expr, &scope) {
                Ok(typed) => {
                    if contains_subquery_placeholder(&typed) {
                        return Err(AnalyzeError::unsupported_query_shape(
                            "subquery is not supported in GROUP BY",
                            gb_expr.span(),
                        ));
                    }
                    group_by.push(typed);
                }
                Err(error) if error.control_error().is_some() => return Err(error),
                Err(_) => {
                    // Try SELECT aliases: GROUP BY alias_name
                    let mut alias_scope = scope.clone();
                    for item in &projection {
                        if let Some(name) = &item.field_name {
                            alias_scope.add_column(None, name, item.expr.value_type.clone());
                        }
                    }
                    let typed = self.analyze_expr(gb_expr, &alias_scope)?;
                    // Substitute alias ref with original expression
                    let typed = self.substitute_select_aliases(typed, &projection);
                    if contains_subquery_placeholder(&typed) {
                        return Err(AnalyzeError::unsupported_query_shape(
                            "subquery is not supported in GROUP BY",
                            gb_expr.span(),
                        ));
                    }
                    group_by.push(typed);
                }
            }
        }

        // BITMAP / HLL columns cannot participate in GROUP BY because they
        // have no comparable scalar identity. Reject upfront so the user
        // sees a clear error before lowering / codegen.
        for gb in &group_by {
            if let Some(logical) = scope.logical_type_of_expr(gb).filter(is_bitmap_or_hll_type) {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "BITMAP/HLL columns cannot appear in GROUP BY (column has type {logical:?})"
                    ),
                    select.span,
                ));
            }
        }

        // --- Detect aggregation ---
        let has_agg_in_select = self.select_has_aggregate_functions(&select.projection);
        let has_aggregation = !group_by.is_empty() || has_agg_in_select;

        if let Some(state) = self.grouping_scope.borrow().as_ref() {
            state.enabled.set(true);
        }
        // --- HAVING ---
        // Resolve against the FROM scope. If a HAVING reference matches a SELECT
        // alias, substitute with the aliased expression so the emitter sees the
        // real aggregate call (e.g. `total` → `sum(v)`).
        let having = match &select.having {
            Some(expr) => {
                let analyzed = self.analyze_expr(expr, &scope);
                match analyzed {
                    Ok(h) => Some(h),
                    Err(error) if error.control_error().is_some() => return Err(error),
                    Err(_) => {
                        // Maybe references a SELECT alias — build alias scope
                        let mut alias_scope = scope.clone();
                        for item in &projection {
                            self.check_control()?;
                            if let Some(name) = &item.field_name {
                                alias_scope.add_column(None, name, item.expr.value_type.clone());
                            }
                        }
                        let h = self.analyze_expr(expr, &alias_scope)?;
                        // Substitute alias refs with real expressions
                        Some(self.substitute_select_aliases(h, &projection))
                    }
                }
            }
            None => None,
        };

        // --- DISTINCT ---
        self.validate_grouping_calls(&group_by)?;

        let distinct = matches!(select.quantifier, ast::SelectQuantifier::Distinct { .. });

        let mut resolved_select = ResolvedSelect {
            from,
            filter,
            group_by,
            having,
            projection,
            has_aggregation,
            distinct,
            repeat: None,
            apply_specs: Vec::new(),
            predicate_apply_specs: Vec::new(),
        };

        // --- Subquery rewriting ---
        // If the WHERE or HAVING clause contained subqueries (recorded as
        // SubqueryPlaceholder nodes), rewrite them into JOINs now.
        let has_subqueries = !self.collected_subqueries.borrow().is_empty();
        if has_subqueries {
            let mut mutable_scope = scope;
            self.rewrite_subqueries(&mut resolved_select, &mut mutable_scope)?;
            sync_output_columns_from_projection(&mut output_columns, &resolved_select.projection);
        }

        Ok((resolved_select, output_columns))
    }

    /// Replace ColumnRef nodes that match SELECT aliases with the aliased expression.
    fn substitute_select_aliases(&self, expr: TypedExpr, projection: &[ProjectItem]) -> TypedExpr {
        self.substitute_select_aliases_inner(expr, projection, false)
    }

    /// Like [`substitute_select_aliases`] but only rewrites a `ColumnRef`
    /// when the name does NOT also resolve as a column from the FROM scope.
    /// SELECT items reference table columns far more often than they
    /// reference earlier aliases, so suppress the rewrite whenever the
    /// FROM column is in scope to avoid breaking semantics for queries
    /// like `SELECT n + 1 AS n, (n + 1) * (n + 1) AS sq FROM t`.
    fn substitute_select_aliases_for_select(
        &self,
        expr: TypedExpr,
        projection: &[ProjectItem],
        from_scope: &AnalyzerScope,
    ) -> TypedExpr {
        self.substitute_select_aliases_for_select_inner(expr, projection, from_scope, false)
    }

    fn substitute_select_aliases_for_select_inner(
        &self,
        expr: TypedExpr,
        projection: &[ProjectItem],
        from_scope: &AnalyzerScope,
        inside_agg: bool,
    ) -> TypedExpr {
        match expr.kind {
            ExprKind::ColumnRef {
                ref qualifier,
                ref column,
                ..
            } if qualifier.is_none() && !inside_agg => {
                if from_scope.resolve(None, column).is_ok() {
                    // The FROM clause already binds this column; do not
                    // shadow it with an earlier SELECT alias.
                    return expr;
                }
                let col_lower = column.to_lowercase();
                for item in projection {
                    if item
                        .field_name
                        .as_ref()
                        .is_some_and(|name| name.to_lowercase() == col_lower)
                    {
                        return item.expr.clone();
                    }
                }
                expr
            }
            ExprKind::BinaryOp {
                left,
                op,
                right,
                decimal_overflow_policy,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::BinaryOp {
                    left: Box::new(self.substitute_select_aliases_for_select_inner(
                        *left, projection, from_scope, inside_agg,
                    )),
                    op,
                    right: Box::new(self.substitute_select_aliases_for_select_inner(
                        *right, projection, from_scope, inside_agg,
                    )),
                    decimal_overflow_policy,
                },
            },
            ExprKind::UnaryOp { op, expr: inner } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::UnaryOp {
                    op,
                    expr: Box::new(self.substitute_select_aliases_for_select_inner(
                        *inner, projection, from_scope, inside_agg,
                    )),
                },
            },
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
                binding,
                volatility,
            } => {
                let is_agg =
                    crate::analyzer::functions::is_aggregate_function(self.function_catalog, &name);
                TypedExpr {
                    value_type: expr.value_type.clone(),

                    kind: ExprKind::FunctionCall {
                        name,
                        args: args
                            .into_iter()
                            .map(|arg| {
                                self.substitute_select_aliases_for_select_inner(
                                    arg,
                                    projection,
                                    from_scope,
                                    inside_agg || is_agg,
                                )
                            })
                            .collect(),
                        distinct,
                        binding,
                        volatility,
                    },
                }
            }
            ExprKind::LambdaFunction { params, body } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::LambdaFunction {
                    params,
                    body: Box::new(self.substitute_select_aliases_for_select_inner(
                        *body, projection, from_scope, inside_agg,
                    )),
                },
            },
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
                resolved,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::AggregateCall {
                    name,
                    args: args
                        .into_iter()
                        .map(|arg| {
                            self.substitute_select_aliases_for_select_inner(
                                arg, projection, from_scope, true,
                            )
                        })
                        .collect(),
                    distinct,
                    order_by: order_by
                        .into_iter()
                        .map(|item| SortItem {
                            expr: self.substitute_select_aliases_for_select_inner(
                                item.expr, projection, from_scope, true,
                            ),
                            ..item
                        })
                        .collect(),
                    resolved,
                },
            },
            ExprKind::Cast {
                expr: inner,
                target,
                decimal_overflow_policy,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::Cast {
                    expr: Box::new(self.substitute_select_aliases_for_select_inner(
                        *inner, projection, from_scope, inside_agg,
                    )),
                    target,
                    decimal_overflow_policy,
                },
            },
            ExprKind::Nested(inner) => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::Nested(Box::new(self.substitute_select_aliases_for_select_inner(
                    *inner, projection, from_scope, inside_agg,
                ))),
            },
            ExprKind::IsNull {
                expr: inner,
                negated,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::IsNull {
                    expr: Box::new(self.substitute_select_aliases_for_select_inner(
                        *inner, projection, from_scope, inside_agg,
                    )),
                    negated,
                },
            },
            ExprKind::IsTruthValue {
                expr: inner,
                value,
                negated,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::IsTruthValue {
                    expr: Box::new(self.substitute_select_aliases_for_select_inner(
                        *inner, projection, from_scope, inside_agg,
                    )),
                    value,
                    negated,
                },
            },
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::Case {
                    operand: operand.map(|expr| {
                        Box::new(self.substitute_select_aliases_for_select_inner(
                            *expr, projection, from_scope, inside_agg,
                        ))
                    }),
                    when_then: when_then
                        .into_iter()
                        .map(|(when, then)| {
                            (
                                self.substitute_select_aliases_for_select_inner(
                                    when, projection, from_scope, inside_agg,
                                ),
                                self.substitute_select_aliases_for_select_inner(
                                    then, projection, from_scope, inside_agg,
                                ),
                            )
                        })
                        .collect(),
                    else_expr: else_expr.map(|expr| {
                        Box::new(self.substitute_select_aliases_for_select_inner(
                            *expr, projection, from_scope, inside_agg,
                        ))
                    }),
                },
            },
            _ => expr,
        }
    }

    fn substitute_select_aliases_inner(
        &self,
        expr: TypedExpr,
        projection: &[ProjectItem],
        inside_agg: bool,
    ) -> TypedExpr {
        match expr.kind {
            ExprKind::ColumnRef {
                ref qualifier,
                ref column,
                ..
            } if qualifier.is_none() && !inside_agg => {
                // Check if this column name matches a SELECT alias
                let col_lower = column.to_lowercase();
                for item in projection {
                    if item
                        .field_name
                        .as_ref()
                        .is_some_and(|name| name.to_lowercase() == col_lower)
                    {
                        return item.expr.clone();
                    }
                }
                expr
            }
            ExprKind::BinaryOp {
                left,
                op,
                right,
                decimal_overflow_policy,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::BinaryOp {
                    left: Box::new(
                        self.substitute_select_aliases_inner(*left, projection, inside_agg),
                    ),
                    op,
                    right: Box::new(
                        self.substitute_select_aliases_inner(*right, projection, inside_agg),
                    ),
                    decimal_overflow_policy,
                },
            },
            ExprKind::UnaryOp { op, expr: inner } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::UnaryOp {
                    op,
                    expr: Box::new(
                        self.substitute_select_aliases_inner(*inner, projection, inside_agg),
                    ),
                },
            },
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
                binding,
                volatility,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::FunctionCall {
                    name,
                    args: args
                        .into_iter()
                        .map(|arg| {
                            self.substitute_select_aliases_inner(arg, projection, inside_agg)
                        })
                        .collect(),
                    distinct,
                    binding,
                    volatility,
                },
            },
            ExprKind::LambdaFunction { params, body } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::LambdaFunction {
                    params,
                    body: Box::new(
                        self.substitute_select_aliases_inner(*body, projection, inside_agg),
                    ),
                },
            },
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
                resolved,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::AggregateCall {
                    name,
                    args: args
                        .into_iter()
                        .map(|arg| self.substitute_select_aliases_inner(arg, projection, true))
                        .collect(),
                    distinct,
                    order_by: order_by
                        .into_iter()
                        .map(|item| SortItem {
                            expr: self.substitute_select_aliases_inner(item.expr, projection, true),
                            ..item
                        })
                        .collect(),
                    resolved,
                },
            },
            ExprKind::Cast {
                expr: inner,
                target,
                decimal_overflow_policy,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::Cast {
                    expr: Box::new(
                        self.substitute_select_aliases_inner(*inner, projection, inside_agg),
                    ),
                    target,
                    decimal_overflow_policy,
                },
            },
            ExprKind::Nested(inner) => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::Nested(Box::new(
                    self.substitute_select_aliases_inner(*inner, projection, inside_agg),
                )),
            },
            ExprKind::IsNull {
                expr: inner,
                negated,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::IsNull {
                    expr: Box::new(
                        self.substitute_select_aliases_inner(*inner, projection, inside_agg),
                    ),
                    negated,
                },
            },
            ExprKind::IsTruthValue {
                expr: inner,
                value,
                negated,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::IsTruthValue {
                    expr: Box::new(
                        self.substitute_select_aliases_inner(*inner, projection, inside_agg),
                    ),
                    value,
                    negated,
                },
            },
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => TypedExpr {
                value_type: expr.value_type.clone(),

                kind: ExprKind::Case {
                    operand: operand.map(|expr| {
                        Box::new(
                            self.substitute_select_aliases_inner(*expr, projection, inside_agg),
                        )
                    }),
                    when_then: when_then
                        .into_iter()
                        .map(|(when, then)| {
                            (
                                self.substitute_select_aliases_inner(when, projection, inside_agg),
                                self.substitute_select_aliases_inner(then, projection, inside_agg),
                            )
                        })
                        .collect(),
                    else_expr: else_expr.map(|expr| {
                        Box::new(
                            self.substitute_select_aliases_inner(*expr, projection, inside_agg),
                        )
                    }),
                },
            },
            // For other node types, return as-is
            _ => expr,
        }
    }

    /// Check if a SELECT's GROUP BY clause contains ROLLUP/CUBE/GROUPING SETS.
    /// Returns the explicit grouping-set levels plus the full GROUP BY key list.
    fn extract_repeat_from_group_by(&self, select: &ast::Select) -> Option<RepeatGroupBySpec> {
        match &select.group_by {
            ast::GroupBy::Rollup { expressions, .. } => {
                let groups = expressions
                    .iter()
                    .cloned()
                    .map(|expr| vec![expr])
                    .collect::<Vec<_>>();
                Some(RepeatGroupBySpec {
                    grouping_sets: rollup_grouping_sets(&groups),
                    all_group_by_exprs: flatten_grouping_groups(&groups),
                })
            }
            ast::GroupBy::Cube { expressions, .. } => {
                let groups = expressions
                    .iter()
                    .cloned()
                    .map(|expr| vec![expr])
                    .collect::<Vec<_>>();
                Some(RepeatGroupBySpec {
                    grouping_sets: cube_grouping_sets(&groups),
                    all_group_by_exprs: flatten_grouping_groups(&groups),
                })
            }
            ast::GroupBy::GroupingSets { sets, .. } => Some(RepeatGroupBySpec {
                grouping_sets: sets.clone(),
                all_group_by_exprs: unique_exprs_in_order(
                    sets.iter().flat_map(|set| set.iter().cloned()),
                ),
            }),
            ast::GroupBy::None | ast::GroupBy::Expressions { .. } => None,
        }
    }

    /// Resolve `GROUP BY ROLLUP/CUBE/GROUPING SETS` into a single SELECT with
    /// RepeatInfo instead of rewriting to UNION ALL branches.
    ///
    /// The SELECT is analyzed once with the union of all grouping keys in the
    /// GROUP BY. RepeatInfo records the per-level null patterns and grouping_id
    /// bitmaps so the Repeat operator can replay the grouping-set semantics.
    fn resolve_repeat_group_by(
        &self,
        select: &ast::Select,
        repeat_spec: &RepeatGroupBySpec,
    ) -> Result<(QueryBody, Vec<OutputColumn>), AnalyzeError> {
        use crate::analysis::RepeatInfo;

        let all_rollup_columns: Vec<String> = repeat_spec
            .all_group_by_exprs
            .iter()
            .map(expr_syntax_key)
            .collect();

        // StarRocks uses the last grouping argument as the least-significant
        // bit, so for GROUPING_ID(a, b) nulling b yields 0b01, not 0b10.
        let total_grouping_columns = all_rollup_columns.len();
        let mut repeat_column_ref_list: Vec<Vec<String>> =
            Vec::with_capacity(repeat_spec.grouping_sets.len());
        let mut grouping_ids: Vec<u64> = Vec::with_capacity(repeat_spec.grouping_sets.len());

        for grouping_set in &repeat_spec.grouping_sets {
            let non_null_cols: Vec<String> = grouping_set.iter().map(expr_syntax_key).collect();
            let active_cols: std::collections::HashSet<String> =
                non_null_cols.iter().cloned().collect();
            repeat_column_ref_list.push(non_null_cols);

            let mut bitmap: u64 = 0;
            for (idx, col_name) in all_rollup_columns.iter().enumerate() {
                if !active_cols.contains(col_name) {
                    let bit_pos = total_grouping_columns - 1 - idx;
                    bitmap |= 1u64 << bit_pos;
                }
            }
            grouping_ids.push(bitmap);
        }

        // Analyze the original expressions with real, query-local mask
        // references. Only the GROUP BY key union is expanded for Repeat.
        let mut modified_select = select.clone();
        modified_select.group_by = ast::GroupBy::Expressions {
            expressions: repeat_spec.all_group_by_exprs.clone(),
            span: select.span,
        };
        let (mut sel, mut cols) = self.analyze_select(&modified_select)?;
        let state = self
            .grouping_scope
            .borrow()
            .clone()
            .expect("SELECT grouping scope installed");
        let calls = state.calls.borrow().clone();
        let mut grouping_fn_args: Vec<_> = calls
            .iter()
            .map(|call| (call.name.clone(), call.columns.clone()))
            .collect();
        let mut grouping_fn_ids: Vec<_> = calls
            .iter()
            .map(|call| (call.name.clone(), call.column_id.expect("Repeat call mask")))
            .collect();
        if calls.is_empty() && !all_rollup_columns.is_empty() {
            let name = "__grouping_fn_0".to_string();
            state.next_mask_index.set(1);
            let id = self.alloc_column_id(
                None,
                name.clone(),
                novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            );
            grouping_fn_args.push((name.clone(), all_rollup_columns.clone()));
            grouping_fn_ids.push((name, id));
        }
        for (name, column_id) in &grouping_fn_ids {
            sel.group_by.push(TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: *column_id,
                    qualifier: None,
                    column: name.clone(),
                },
                value_type: novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
            });
        }

        // A grouping key that some level's grouping set leaves out holds NULL
        // in that level's rows, so every place that names it -- the key
        // itself, the projection item that reads it, and this query's output
        // column -- has to say so. The logical build widens the materialized
        // Repeat slot for the same reason; if only that layer widened, a
        // query wrapped in a CTE would hand a nullable child to a
        // non-nullable declared output and be refused while adapting it.
        let nulled_keys: std::collections::HashSet<crate::column_id::ColumnId> = grouping_ids
            .iter()
            .flat_map(|bitmap| {
                (0..total_grouping_columns).filter(move |idx| {
                    let bit = total_grouping_columns - 1 - *idx;
                    bitmap & (1u64 << bit) != 0
                })
            })
            .filter(|idx| {
                matches!(
                    sel.group_by.get(*idx).map(|key| &key.kind),
                    Some(ExprKind::ColumnRef { .. })
                )
            })
            .map(|idx| match &sel.group_by[idx].kind {
                ExprKind::ColumnRef { column_id, .. } => *column_id,
                _ => unreachable!("filtered grouping key is a column reference"),
            })
            .collect();
        if !nulled_keys.is_empty() {
            for key in &mut sel.group_by {
                if let ExprKind::ColumnRef { column_id, .. } = &key.kind
                    && nulled_keys.contains(column_id)
                {
                    key.value_type = key
                        .value_type
                        .with_nullable(true)
                        .expect("validated root NULL contract");
                }
            }
            let mut nulled_outputs = std::collections::HashSet::new();
            for item in &mut sel.projection {
                if let ExprKind::ColumnRef { column_id, .. } = &item.expr.kind
                    && nulled_keys.contains(column_id)
                {
                    item.expr.value_type = item
                        .expr
                        .value_type
                        .with_nullable(true)
                        .expect("validated root NULL contract");
                    nulled_outputs.insert(item.output_column_id);
                }
            }
            for column in &mut cols {
                if nulled_outputs.contains(&column.column_id) {
                    column.value_type = column
                        .value_type
                        .with_nullable(true)
                        .expect("validated root NULL contract");
                }
            }
        }

        // Attach RepeatInfo to the resolved SELECT.
        sel.repeat = Some(RepeatInfo {
            repeat_column_ref_list,
            repeat_column_ref_ids: Vec::new(),
            grouping_ids,
            all_rollup_columns,
            all_rollup_column_ids: Vec::new(),
            grouping_fn_args,
            grouping_fn_arg_ids: Vec::new(),
            grouping_fn_ids,
        });

        Ok((QueryBody::Select(sel), cols))
    }

    /// Analyze the SELECT projection list.
    fn analyze_projection(
        &self,
        items: &[ast::SelectItem],
        scope: &AnalyzerScope,
    ) -> Result<(Vec<ProjectItem>, Vec<OutputColumn>), AnalyzeError> {
        let mut projection: Vec<ProjectItem> = Vec::new();
        let mut output_columns = Vec::new();
        // StarRocks allows later SELECT items to reference earlier item
        // aliases by name. Track those aliases in a parallel scope so name
        // resolution succeeds; afterwards we rewrite every `ColumnRef` that
        // matches an earlier alias to the alias's already-analyzed
        // expression, so the codegen sees inlined expressions instead of
        // unbound projection slot references.
        let mut effective_scope = scope.clone();

        for item in items {
            self.check_control()?;
            match item {
                ast::SelectItem::UnnamedExpr(expr) => {
                    let typed = self.analyze_expr(expr, &effective_scope)?;
                    let typed =
                        self.substitute_select_aliases_for_select(typed, &projection, scope);
                    let (name, column_id) = match &typed.kind {
                        ExprKind::ColumnRef {
                            column_id, column, ..
                        } if !matches!(expr, ast::Expr::Cast(_)) && !is_grouping_call(expr) => {
                            (column.clone(), *column_id)
                        }
                        _ => {
                            let n = self.grouping_display_name(expr);
                            let id =
                                self.alloc_column_id(None, n.clone(), typed.value_type.clone());
                            (n, id)
                        }
                    };
                    // A direct reference keeps its original source record. New
                    // occurrences retain the exact producer's complete witness
                    // before planner symbol rewrites hand it to the root owner.
                    // A same-carrier explicit CAST may simplify to a reference,
                    // but still owns a separate output domain occurrence.
                    if !matches!(&typed.kind, ExprKind::ColumnRef { .. })
                        || matches!(expr, ast::Expr::Cast(_))
                        || is_grouping_call(expr)
                    {
                        let logical =
                            self.logical_output_type(Some(expr), &typed, &effective_scope)?;
                        self.factory
                            .borrow_mut()
                            .set_logical_type(column_id, logical);
                    }
                    output_columns.push(OutputColumn {
                        field_name: natural_field_name(expr),
                        column_id,
                        name: name.clone(),
                        value_type: typed.value_type.clone(),

                        is_internal: false,
                    });
                    projection.push(ProjectItem {
                        field_name: natural_field_name(expr),
                        expr: typed,
                        output_name: name,
                        output_column_id: column_id,
                    });
                }
                ast::SelectItem::ExprWithAlias { expr, alias, .. } => {
                    let typed = self.analyze_expr(expr, &effective_scope)?;
                    let typed =
                        self.substitute_select_aliases_for_select(typed, &projection, scope);
                    let name = alias.value.clone();
                    let column_id = match &typed.kind {
                        ExprKind::ColumnRef { column_id, .. }
                            if !matches!(expr, ast::Expr::Cast(_)) && !is_grouping_call(expr) =>
                        {
                            *column_id
                        }
                        _ => self.alloc_column_id(None, name.clone(), typed.value_type.clone()),
                    };
                    // A direct reference keeps its original source record. New
                    // occurrences retain the exact producer's complete witness
                    // before planner symbol rewrites hand it to the root owner.
                    // A same-carrier explicit CAST may simplify to a reference,
                    // but still owns a separate output domain occurrence.
                    if !matches!(&typed.kind, ExprKind::ColumnRef { .. })
                        || matches!(expr, ast::Expr::Cast(_))
                        || is_grouping_call(expr)
                    {
                        let logical =
                            self.logical_output_type(Some(expr), &typed, &effective_scope)?;
                        self.factory
                            .borrow_mut()
                            .set_logical_type(column_id, logical);
                    }
                    output_columns.push(OutputColumn {
                        field_name: Some(name.clone()),
                        column_id,
                        name: name.clone(),
                        value_type: typed.value_type.clone(),

                        is_internal: false,
                    });
                    // Make the alias visible to later items in the same
                    // projection list, but only if it does not already
                    // collide with a column from the FROM scope; otherwise
                    // the alias would shadow the underlying column and
                    // future items would resolve to the alias expression
                    // instead of the FROM column.
                    if scope.resolve(None, &name).is_err() {
                        effective_scope.add_column_with_id(
                            None,
                            &name,
                            column_id,
                            typed.value_type.clone(),
                        );
                    }
                    projection.push(ProjectItem {
                        field_name: Some(name.clone()),
                        expr: typed,
                        output_name: name,
                        output_column_id: column_id,
                    });
                }
                ast::SelectItem::Wildcard { .. } => {
                    for (qualifier, col_name, col_id, value_type, field_name) in
                        scope.iter_columns()
                    {
                        self.check_control()?;
                        // FULL OUTER USING columns are exposed as a synthetic
                        // `COALESCE(left.col, right.col)` expression. SELECT *
                        // expansion must use that expression instead of the
                        // raw left-side ColumnRef so right-only rows show
                        // the right-side value.
                        let typed = if let Some(expr) = field_name
                            .as_deref()
                            .and_then(|name| scope.computed_column_for(name))
                        {
                            expr.clone()
                        } else {
                            TypedExpr {
                                kind: ExprKind::ColumnRef {
                                    column_id: *col_id,
                                    qualifier: qualifier.clone(),
                                    column: col_name.clone(),
                                },
                                value_type: value_type.clone(),
                            }
                        };
                        output_columns.push(OutputColumn {
                            field_name: field_name.clone(),
                            column_id: *col_id,
                            name: col_name.clone(),
                            value_type: value_type.clone(),

                            is_internal: false,
                        });
                        projection.push(ProjectItem {
                            field_name: field_name.clone(),
                            expr: typed,
                            output_name: col_name.clone(),
                            output_column_id: *col_id,
                        });
                    }
                }
                ast::SelectItem::QualifiedWildcard { prefix, span, .. } => {
                    let qualifier_str = prefix
                        .iter()
                        .map(|ident| ident.value.as_str())
                        .collect::<Vec<_>>()
                        .join(".");
                    // Tables are registered under their alias (or bare table
                    // name) in scope, but users can address them with the
                    // multi-part name they typed (e.g. `db.t0.*`). Fall back
                    // to the last identifier segment when the full string
                    // does not match anything, so `<db>.<tbl>.*` finds the
                    // same columns as `<tbl>.*`.
                    let fallback_qualifier = qualifier_str
                        .rsplit('.')
                        .next()
                        .unwrap_or(&qualifier_str)
                        .to_string();
                    let mut found = false;
                    for (qualifier, col_name, col_id, value_type, field_name) in
                        scope.iter_qualified_columns(&qualifier_str).chain(
                            if fallback_qualifier != qualifier_str {
                                Some(scope.iter_qualified_columns(&fallback_qualifier))
                            } else {
                                None
                            }
                            .into_iter()
                            .flatten(),
                        )
                    {
                        self.check_control()?;
                        found = true;
                        let typed = TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: *col_id,
                                qualifier: qualifier.clone(),
                                column: col_name.clone(),
                            },
                            value_type: value_type.clone(),
                        };
                        output_columns.push(OutputColumn {
                            field_name: field_name.clone(),
                            column_id: *col_id,
                            name: col_name.clone(),
                            value_type: value_type.clone(),

                            is_internal: false,
                        });
                        projection.push(ProjectItem {
                            field_name: field_name.clone(),
                            expr: typed,
                            output_name: col_name.clone(),
                            output_column_id: *col_id,
                        });
                    }
                    if !found {
                        return Err(AnalyzeError::unknown_table(
                            format!("no columns found for qualifier `{qualifier_str}`"),
                            *span,
                        ));
                    }
                }
            }
        }

        Ok((projection, output_columns))
    }

    /// Like `analyze_projection` but uses `wildcard_scope` for `SELECT *`
    /// expansion and `expr_scope` for all other expression resolution.
    /// This prevents outer scope columns from leaking into wildcard expansion
    /// inside correlated subqueries.
    fn analyze_projection_with_wildcard_scope(
        &self,
        items: &[ast::SelectItem],
        expr_scope: &AnalyzerScope,
        wildcard_scope: &AnalyzerScope,
    ) -> Result<(Vec<ProjectItem>, Vec<OutputColumn>), AnalyzeError> {
        let mut projection = Vec::new();
        let mut output_columns = Vec::new();

        for item in items {
            self.check_control()?;
            match item {
                ast::SelectItem::Wildcard { .. } => {
                    for (qualifier, col_name, col_id, value_type, field_name) in
                        wildcard_scope.iter_columns()
                    {
                        self.check_control()?;
                        let typed = TypedExpr {
                            kind: ExprKind::ColumnRef {
                                column_id: *col_id,
                                qualifier: qualifier.clone(),
                                column: col_name.clone(),
                            },
                            value_type: value_type.clone(),
                        };
                        output_columns.push(OutputColumn {
                            field_name: field_name.clone(),
                            column_id: *col_id,
                            name: col_name.clone(),
                            value_type: value_type.clone(),

                            is_internal: false,
                        });
                        projection.push(ProjectItem {
                            field_name: field_name.clone(),
                            expr: typed,
                            output_name: col_name.clone(),
                            output_column_id: *col_id,
                        });
                    }
                }
                // All other items use the full scope (including outer for correlation)
                _ => {
                    let (mut p, mut o) =
                        self.analyze_projection(std::slice::from_ref(item), expr_scope)?;
                    projection.append(&mut p);
                    output_columns.append(&mut o);
                }
            }
        }

        Ok((projection, output_columns))
    }

    /// Rebuild the FROM scope from an already-resolved Relation tree.
    /// Used by ORDER BY fallback when the expression doesn't match projection columns.
    fn rebuild_from_scope(&self, relation: &Relation) -> Result<((), AnalyzerScope), AnalyzeError> {
        let mut scope = self.new_scope();
        self.collect_relation_scope(relation, &mut scope)?;
        Ok(((), scope))
    }

    fn collect_relation_scope(
        &self,
        relation: &Relation,
        scope: &mut AnalyzerScope,
    ) -> Result<(), AnalyzeError> {
        match relation {
            Relation::Scan(scan) => {
                let mut local_scope = self.new_scope();
                let qualifier = scan.alias.as_deref().unwrap_or(&scan.table.name);
                let base_len = scan.table.columns.len();
                let meta_len = scan.table.iceberg_row_lineage_metadata_columns.len();
                if scan.column_ids.len() >= base_len {
                    local_scope.add_table_with_ids(
                        Some(qualifier),
                        &scan.table.columns,
                        &scan.column_ids[..base_len],
                        self.control,
                    )?;
                } else {
                    local_scope.add_table(Some(qualifier), &scan.table.columns, self.control)?;
                }
                if !scan.table.iceberg_row_lineage_metadata_columns.is_empty() {
                    if scan.column_ids.len() == base_len + meta_len {
                        local_scope.add_iceberg_metadata_columns_with_ids(
                            qualifier,
                            &scan.table.iceberg_row_lineage_metadata_columns,
                            &scan.column_ids[base_len..],
                            self.control,
                        )?;
                    } else {
                        local_scope.add_iceberg_metadata_columns(
                            qualifier,
                            &scan.table.iceberg_row_lineage_metadata_columns,
                            self.control,
                        )?;
                    }
                }
                local_scope.apply_column_aliases(&scan.column_aliases, scan.alias_span)?;
                scope.merge(&local_scope);
                Ok(())
            }
            Relation::Subquery {
                alias,
                output_columns,
                ..
            } => {
                for col in output_columns {
                    scope.add_field(Some(alias.as_str()), col);
                }
                Ok(())
            }
            Relation::Join(join_rel) => {
                self.collect_relation_scope(&join_rel.left, scope)?;
                self.collect_relation_scope(&join_rel.right, scope)?;
                Ok(())
            }
            Relation::GenerateSeries(gs) => {
                let qualifier = gs.alias.as_deref().unwrap_or("generate_series");
                scope.add_column_with_id(
                    Some(qualifier),
                    &gs.column_name,
                    gs.output_column_id,
                    novarocks_type_contract::FunctionValueType::new(DataType::Int64, false),
                );
                Ok(())
            }
            Relation::Unnest(unnest) => {
                let qualifier = unnest.alias.as_deref().unwrap_or("unnest");
                for col in &unnest.output_columns {
                    scope.add_field(Some(qualifier), col);
                }
                Ok(())
            }
            Relation::CTEConsume {
                alias,
                output_columns,
                ..
            } => {
                for col in output_columns {
                    scope.add_field(Some(alias.as_str()), col);
                }
                Ok(())
            }
            Relation::IcebergMetadataScan(rel) => {
                // The relation already carries the resolved schema, including
                // the schema-derived partition and bound columns a frozen
                // shape cannot know about. Re-deriving the frozen shape here
                // would leave a partitioned table's `partition` unresolvable on
                // this path only.
                let qualifier = rel.alias.as_deref().unwrap_or(&rel.table.name);
                // ORDER BY-only columns must reference the original scan
                // bindings, just as ordinary Scan columns do above.
                scope.add_table_with_ids(
                    Some(qualifier),
                    &rel.table.columns,
                    &rel.column_ids,
                    self.control,
                )?;
                Ok(())
            }
            Relation::IcebergDeltaScan(rel) => {
                // Mirror Scan: expose base columns + row-lineage metadata
                // columns under the alias (or table name).
                let qualifier = rel.alias.as_deref().unwrap_or(&rel.table.name);
                let base_len = rel.table.columns.len();
                let meta_len = rel.table.iceberg_row_lineage_metadata_columns.len();
                if rel.column_ids.len() >= base_len {
                    scope.add_table_with_ids(
                        Some(qualifier),
                        &rel.table.columns,
                        &rel.column_ids[..base_len],
                        self.control,
                    )?;
                } else {
                    scope.add_table(Some(qualifier), &rel.table.columns, self.control)?;
                }
                if rel.column_ids.len() == base_len + meta_len {
                    scope.add_iceberg_metadata_columns_with_ids(
                        qualifier,
                        &rel.table.iceberg_row_lineage_metadata_columns,
                        &rel.column_ids[base_len..],
                        self.control,
                    )?;
                } else {
                    scope.add_iceberg_metadata_columns(
                        qualifier,
                        &rel.table.iceberg_row_lineage_metadata_columns,
                        self.control,
                    )?;
                }
                Ok(())
            }
        }
    }

    /// Analyze ORDER BY clause.
    fn analyze_order_by(
        &self,
        query: &ast::Query,
        body_output: &[OutputColumn],
        body: &QueryBody,
    ) -> Result<Vec<SortItem>, AnalyzeError> {
        let order_by_exprs = &query.order_by;
        if order_by_exprs.is_empty() {
            return Ok(vec![]);
        }

        // Build a projection scope from body output columns for ORDER BY resolution.
        let mut projection_scope = self.new_scope();
        for col in body_output {
            projection_scope.add_field(None, col);
        }
        // Also register qualified column refs from projection items
        // so ORDER BY a.id works when SELECT has a.id
        if let QueryBody::Select(sel) = body {
            for item in &sel.projection {
                if let ExprKind::ColumnRef {
                    column_id,
                    qualifier: Some(ref q),
                    ref column,
                } = item.expr.kind
                {
                    projection_scope.add_column_with_id(
                        Some(q),
                        column,
                        column_id,
                        item.expr.value_type.clone(),
                    );
                }
            }
        }

        // Pre-aggregation (FROM) scope used to rebind aggregate arguments
        // below. An ORDER BY aggregate's argument must reference a base column,
        // not a projection alias of the same name. Built once; rebuilding it
        // from base-table scans reuses their ColumnIds (no minting), so it has
        // no effect on non-aggregate ORDER BY items.
        let order_by_from_scope: Option<AnalyzerScope> = match body {
            QueryBody::Select(sel) => match sel
                .from
                .as_ref()
                .map(|rel| self.rebuild_from_scope(rel))
                .transpose()
            {
                Ok(scope) => scope.map(|(_, scope)| scope),
                Err(error) if error.control_error().is_some() => return Err(error),
                Err(_) => None,
            },
            _ => None,
        };

        let mut sort_items = Vec::with_capacity(order_by_exprs.len());
        for ob in order_by_exprs {
            // Try resolving against the projection scope first, then fall back
            // to a numeric literal reference (ORDER BY 1, 2, ...)
            let typed = match &ob.expr {
                ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::Number(n),
                    ..
                }) => {
                    // Positional reference: ORDER BY 1
                    let pos: usize = n.parse::<usize>().map_err(|e| {
                        AnalyzeError::invalid_literal(
                            format!("invalid ORDER BY position: {e}"),
                            ob.expr.span(),
                        )
                    })?;
                    if pos == 0 || pos > body_output.len() {
                        return Err(AnalyzeError::invalid_argument(
                            format!(
                                "ORDER BY position {pos} is out of range (1..{})",
                                body_output.len()
                            ),
                            ob.expr.span(),
                        ));
                    }
                    let col = &body_output[pos - 1];
                    TypedExpr {
                        kind: ExprKind::ColumnRef {
                            column_id: col.column_id,
                            qualifier: None,
                            column: col.name.clone(),
                        },
                        value_type: col.value_type.clone(),
                    }
                }
                _ => {
                    // First: check if the ORDER BY expression textually matches
                    // a SELECT list expression. If so, resolve as a reference to
                    // the output alias. This handles ORDER BY count(x) matching
                    // SELECT count(x) as alias.
                    let ob_text = expr_syntax_key(&ob.expr);
                    let mut matched_alias = None;
                    // Match ORDER BY expression text against SELECT item
                    // expressions (not aliases). This handles ORDER BY
                    // count(distinct x) matching SELECT count(distinct x) as y.
                    if let QueryBody::Select(sel) = body
                        && let ast::SetExpr::Select(ast_sel) = query.body.as_ref()
                    {
                        // Bare-identifier ORDER BY (`ORDER BY k1`) must prefer
                        // the projection's output alias over the SELECT item's
                        // underlying analyzed expression: the SELECT item may
                        // analyse to a synthetic expression that's no longer
                        // valid in the post-Project scope (FULL OUTER USING
                        // expands `k1` into `coalesce(t1.k1, t2.k1)`, which
                        // references columns the SORT operator can no longer
                        // see). Promote the alias-match path to fire first
                        // for plain identifiers; AST-text matching is still
                        // useful for `ORDER BY a.c` echoing `SELECT a.c`,
                        // where preserving qualifiers matters.
                        let ob_is_bare_ident = matches!(ob.expr, ast::Expr::Identifier(_));
                        for (ast_item, ir_item) in
                            ast_sel.projection.iter().zip(sel.projection.iter())
                        {
                            if ob_is_bare_ident
                                && ir_item
                                    .field_name
                                    .as_ref()
                                    .is_some_and(|name| name.to_lowercase() == ob_text)
                            {
                                let col_id = select_item_output_column_id(
                                    ir_item,
                                    self,
                                    &ir_item.output_name,
                                );
                                matched_alias = Some(TypedExpr {
                                    kind: ExprKind::ColumnRef {
                                        column_id: col_id,
                                        qualifier: None,
                                        column: ir_item.output_name.clone(),
                                    },
                                    value_type: ir_item.expr.value_type.clone(),
                                });
                                break;
                            }
                            let ast_expr_text = match ast_item {
                                ast::SelectItem::ExprWithAlias { expr, .. }
                                | ast::SelectItem::UnnamedExpr(expr) => expr_syntax_key(expr),
                                _ => continue,
                            };
                            // Exact AST match means the user wrote the
                            // same expression as the SELECT item: reuse the
                            // analyzed expression directly so qualifiers and
                            // sub-expressions are preserved. Without this,
                            // `SELECT a.c, b.c ... ORDER BY a.c, b.c` would
                            // produce two unqualified `c` refs and collapse
                            // both keys onto the first projection slot.
                            if ast_expr_text == ob_text {
                                matched_alias = Some(ir_item.expr.clone());
                                break;
                            }
                            // Output-name (alias) match: the user is
                            // referring to the projection's *output* column
                            // by name. Use a synthetic unqualified ColumnRef
                            // so post-aggregation contexts (GROUP BY,
                            // HAVING) can resolve the alias against the
                            // projection-output scope rather than against
                            // the pre-aggregation FROM-scope.
                            if ir_item
                                .field_name
                                .as_ref()
                                .is_some_and(|name| name.to_lowercase() == ob_text)
                            {
                                let col_id = select_item_output_column_id(
                                    ir_item,
                                    self,
                                    &ir_item.output_name,
                                );
                                matched_alias = Some(TypedExpr {
                                    kind: ExprKind::ColumnRef {
                                        column_id: col_id,
                                        qualifier: None,
                                        column: ir_item.output_name.clone(),
                                    },
                                    value_type: ir_item.expr.value_type.clone(),
                                });
                                break;
                            }
                        }
                    }
                    if let Some(alias_ref) = matched_alias {
                        alias_ref
                    } else {
                        // Try projection scope first, then fall back to FROM scope
                        match self.analyze_expr(&ob.expr, &projection_scope) {
                            Ok(typed) => {
                                if let QueryBody::Select(sel) = body {
                                    self.substitute_select_aliases(typed, &sel.projection)
                                } else {
                                    typed
                                }
                            }
                            Err(error) if error.control_error().is_some() => return Err(error),
                            Err(proj_err) => {
                                if let QueryBody::Select(sel) = body {
                                    if let Some(ref from_rel) = sel.from {
                                        let (_, from_scope) = self.rebuild_from_scope(from_rel)?;
                                        match self.analyze_expr(&ob.expr, &from_scope) {
                                            Ok(typed) => self
                                                .substitute_select_aliases(typed, &sel.projection),
                                            Err(error) if error.control_error().is_some() => {
                                                return Err(error);
                                            }
                                            Err(_) => {
                                                let mut alias_scope = from_scope.clone();
                                                for item in &sel.projection {
                                                    let col_id = select_item_output_column_id(
                                                        item,
                                                        self,
                                                        &item.output_name,
                                                    );
                                                    if let Some(name) = &item.field_name {
                                                        alias_scope.add_column_with_id(
                                                            None,
                                                            name,
                                                            col_id,
                                                            item.expr.value_type.clone(),
                                                        );
                                                    }
                                                }
                                                match self.analyze_expr(&ob.expr, &alias_scope) {
                                                    Ok(typed) => self.substitute_select_aliases(
                                                        typed,
                                                        &sel.projection,
                                                    ),
                                                    Err(error)
                                                        if error.control_error().is_some() =>
                                                    {
                                                        return Err(error);
                                                    }
                                                    Err(_) => return Err(proj_err),
                                                }
                                            }
                                        }
                                    } else {
                                        return Err(proj_err);
                                    }
                                } else {
                                    return Err(proj_err);
                                }
                            }
                        }
                    }
                }
            };

            // Rebind columns inside aggregate arguments to the base FROM scope.
            // The projection-scope resolution above can mis-bind a column
            // *inside* an aggregate (e.g. `v1` in `ORDER BY abs(min(v1)) +
            // abs(v1)` where `min(v1) AS v1` shadows the base column), minting a
            // phantom ColumnId the aggregate's child scope never produces. Only
            // aggregate args are touched; top-level references are unchanged.
            let typed = match &order_by_from_scope {
                Some(from_scope) => self.rebind_order_by_agg_args(typed, from_scope, false),
                None => typed,
            };
            if contains_subquery_placeholder(&typed) {
                return Err(AnalyzeError::unsupported_query_shape(
                    "subquery is not supported in ORDER BY",
                    ob.span,
                ));
            }

            let asc = ob.asc.unwrap_or(true);
            let nulls_first = ob.nulls_first.unwrap_or(asc);

            sort_items.push(SortItem {
                expr: typed,
                asc,
                nulls_first,
            });
        }

        // BITMAP / HLL columns cannot participate in ORDER BY because they
        // are opaque blobs with no ordering. Check the projection scope for
        // alias references, then the FROM scope for direct column refs.
        let from_scope_for_check: Option<AnalyzerScope> = if let QueryBody::Select(sel) = body {
            match sel
                .from
                .as_ref()
                .map(|rel| self.rebuild_from_scope(rel))
                .transpose()
            {
                Ok(scope) => scope.map(|(_, scope)| scope),
                Err(error) if error.control_error().is_some() => return Err(error),
                Err(_) => None,
            }
        } else {
            None
        };
        for item in &sort_items {
            let logical = projection_scope
                .logical_type_of_expr(&item.expr)
                .filter(is_bitmap_or_hll_type)
                .or_else(|| {
                    from_scope_for_check
                        .as_ref()
                        .and_then(|s| s.logical_type_of_expr(&item.expr))
                        .filter(is_bitmap_or_hll_type)
                });
            if let Some(logical) = logical {
                return Err(AnalyzeError::invalid_argument(
                    format!(
                        "BITMAP/HLL columns cannot appear in ORDER BY (column has type {logical:?})"
                    ),
                    query.span,
                ));
            }
        }

        Ok(sort_items)
    }

    /// Rebind `ColumnRef`s that occur inside an aggregate argument to the
    /// pre-aggregation `from_scope`, so an ORDER BY aggregate binds its
    /// argument to the base column rather than a SELECT alias of the same
    /// name. Aggregates cannot nest, so once `inside_agg` is set every
    /// `ColumnRef` reached is an aggregate argument and is re-resolved by name
    /// against the FROM scope (kept as-is when the name is not a base column).
    /// Top-level references (`inside_agg == false`) are returned unchanged —
    /// they may legitimately point at projection outputs (for example a FULL
    /// OUTER USING coalesce column that is only valid above the Project).
    fn rebind_order_by_agg_args(
        &self,
        expr: TypedExpr,
        from_scope: &AnalyzerScope,
        inside_agg: bool,
    ) -> TypedExpr {
        let TypedExpr {
            kind,
            mut value_type,
        } = expr;
        let kind = match kind {
            ExprKind::ColumnRef {
                column_id,
                qualifier,
                column,
            } => {
                let column_id = if inside_agg {
                    match from_scope.resolve_value_type(qualifier.as_deref(), &column) {
                        Ok((base_id, base_type)) => {
                            value_type = base_type;
                            base_id
                        }
                        Err(_) => column_id,
                    }
                } else {
                    column_id
                };
                ExprKind::ColumnRef {
                    column_id,
                    qualifier,
                    column,
                }
            }
            ExprKind::AggregateCall {
                name,
                args,
                distinct,
                order_by,
                resolved,
            } => ExprKind::AggregateCall {
                name,
                args: args
                    .into_iter()
                    .map(|arg| self.rebind_order_by_agg_args(arg, from_scope, true))
                    .collect(),
                distinct,
                order_by: order_by
                    .into_iter()
                    .map(|item| SortItem {
                        expr: self.rebind_order_by_agg_args(item.expr, from_scope, true),
                        asc: item.asc,
                        nulls_first: item.nulls_first,
                    })
                    .collect(),
                resolved,
            },
            ExprKind::BinaryOp {
                left,
                op,
                right,
                decimal_overflow_policy,
            } => ExprKind::BinaryOp {
                left: Box::new(self.rebind_order_by_agg_args(*left, from_scope, inside_agg)),
                op,
                right: Box::new(self.rebind_order_by_agg_args(*right, from_scope, inside_agg)),
                decimal_overflow_policy,
            },
            ExprKind::UnaryOp { op, expr: inner } => ExprKind::UnaryOp {
                op,
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
            },
            ExprKind::FunctionCall {
                name,
                args,
                distinct,
                binding,
                volatility,
            } => ExprKind::FunctionCall {
                name,
                args: args
                    .into_iter()
                    .map(|arg| self.rebind_order_by_agg_args(arg, from_scope, inside_agg))
                    .collect(),
                distinct,
                binding,
                volatility,
            },
            ExprKind::Cast {
                expr: inner,
                target,
                decimal_overflow_policy,
            } => ExprKind::Cast {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                target,
                decimal_overflow_policy,
            },
            ExprKind::IsNull {
                expr: inner,
                negated,
            } => ExprKind::IsNull {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                negated,
            },
            ExprKind::IsTruthValue {
                expr: inner,
                value,
                negated,
            } => ExprKind::IsTruthValue {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                value,
                negated,
            },
            ExprKind::Nested(inner) => ExprKind::Nested(Box::new(
                self.rebind_order_by_agg_args(*inner, from_scope, inside_agg),
            )),
            ExprKind::InList {
                expr: inner,
                list,
                negated,
            } => ExprKind::InList {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                list: list
                    .into_iter()
                    .map(|item| self.rebind_order_by_agg_args(item, from_scope, inside_agg))
                    .collect(),
                negated,
            },
            ExprKind::Between {
                expr: inner,
                low,
                high,
                negated,
            } => ExprKind::Between {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                low: Box::new(self.rebind_order_by_agg_args(*low, from_scope, inside_agg)),
                high: Box::new(self.rebind_order_by_agg_args(*high, from_scope, inside_agg)),
                negated,
            },
            ExprKind::Like {
                expr: inner,
                pattern,
                negated,
            } => ExprKind::Like {
                expr: Box::new(self.rebind_order_by_agg_args(*inner, from_scope, inside_agg)),
                pattern: Box::new(self.rebind_order_by_agg_args(*pattern, from_scope, inside_agg)),
                negated,
            },
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => ExprKind::Case {
                operand: operand
                    .map(|e| Box::new(self.rebind_order_by_agg_args(*e, from_scope, inside_agg))),
                when_then: when_then
                    .into_iter()
                    .map(|(when, then)| {
                        (
                            self.rebind_order_by_agg_args(when, from_scope, inside_agg),
                            self.rebind_order_by_agg_args(then, from_scope, inside_agg),
                        )
                    })
                    .collect(),
                else_expr: else_expr
                    .map(|e| Box::new(self.rebind_order_by_agg_args(*e, from_scope, inside_agg))),
            },
            // Leaves and node kinds that cannot carry an aggregate argument
            // needing a base-column rebind (Literal, LambdaParamRef,
            // SubqueryPlaceholder, WindowCall, LambdaFunction, Lambda): keep
            // as-is. This mirrors substitute_select_aliases_inner's coverage.
            other => other,
        };
        TypedExpr { kind, value_type }
    }
}

fn contains_subquery_placeholder(expr: &TypedExpr) -> bool {
    match &expr.kind {
        ExprKind::SubqueryPlaceholder { .. } => true,
        ExprKind::BinaryOp { left, right, .. } => {
            contains_subquery_placeholder(left) || contains_subquery_placeholder(right)
        }
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. } => contains_subquery_placeholder(expr),
        ExprKind::FunctionCall { args, .. } => args.iter().any(contains_subquery_placeholder),
        ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
            contains_subquery_placeholder(body)
        }
        ExprKind::AggregateCall { args, order_by, .. } => {
            args.iter().any(contains_subquery_placeholder)
                || order_by
                    .iter()
                    .any(|item| contains_subquery_placeholder(&item.expr))
        }
        ExprKind::InList { expr, list, .. } => {
            contains_subquery_placeholder(expr) || list.iter().any(contains_subquery_placeholder)
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            contains_subquery_placeholder(expr)
                || contains_subquery_placeholder(low)
                || contains_subquery_placeholder(high)
        }
        ExprKind::Like { expr, pattern, .. } => {
            contains_subquery_placeholder(expr) || contains_subquery_placeholder(pattern)
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|expr| contains_subquery_placeholder(expr))
                || when_then.iter().any(|(when, then)| {
                    contains_subquery_placeholder(when) || contains_subquery_placeholder(then)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|expr| contains_subquery_placeholder(expr))
        }
        ExprKind::Nested(inner) => contains_subquery_placeholder(inner),
        ExprKind::WindowCall {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().any(contains_subquery_placeholder)
                || partition_by.iter().any(contains_subquery_placeholder)
                || order_by
                    .iter()
                    .any(|item| contains_subquery_placeholder(&item.expr))
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::Constant(_) => false,
    }
}

fn select_item_output_column_id(
    item: &ProjectItem,
    ctx: &AnalyzerContext<'_>,
    fallback_name: &str,
) -> crate::column_id::ColumnId {
    if item.output_column_id != crate::column_id::ColumnId::UNSET {
        return item.output_column_id;
    }
    match &item.expr.kind {
        ExprKind::ColumnRef { column_id, .. } => *column_id,
        _ => ctx.alloc_column_id(
            None,
            fallback_name.to_string(),
            item.expr.value_type.clone(),
        ),
    }
}

fn is_grouping_call(expression: &ast::Expr) -> bool {
    matches!(expression, ast::Expr::FunctionCall(function)
        if function.name.parts.len() == 1
        && matches!(function.name.parts[0].value.to_ascii_lowercase().as_str(), "grouping" | "grouping_id"))
}

fn expr_syntax_key(expr: &ast::Expr) -> String {
    novarocks_parser::printer::print_expr(expr).to_lowercase()
}

/// Keep historical planner labels while all value/type binding uses the
/// original GROUPING call. Numeric user syntax never enters this substitution.
struct GroupingDisplayFolder<'a> {
    calls: &'a [GroupingCall],
}
impl ast::Fold for GroupingDisplayFolder<'_> {
    fn fold_query(&mut self, query: ast::Query) -> ast::Query {
        query
    }
    fn fold_expr(&mut self, expression: ast::Expr) -> ast::Expr {
        if let ast::Expr::FunctionCall(function) = &expression
            && let Some(index) = self.calls.iter().position(|call| call.syntax == *function)
        {
            return ast::Expr::Literal(ast::Literal {
                kind: ast::LiteralKind::Number((-9000 - index as i64).to_string()),
                span: function.span,
            });
        }
        ast::fold_expr(self, expression)
    }
}

fn flatten_grouping_groups(groups: &[Vec<ast::Expr>]) -> Vec<ast::Expr> {
    groups
        .iter()
        .flat_map(|group| group.iter().cloned())
        .collect()
}

fn unique_exprs_in_order<I>(exprs: I) -> Vec<ast::Expr>
where
    I: IntoIterator<Item = ast::Expr>,
{
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();
    for expr in exprs {
        let key = expr_syntax_key(&expr);
        if seen.insert(key) {
            result.push(expr);
        }
    }
    result
}

fn rollup_grouping_sets(groups: &[Vec<ast::Expr>]) -> Vec<Vec<ast::Expr>> {
    let mut grouping_sets = Vec::with_capacity(groups.len() + 1);
    for active_count in (0..=groups.len()).rev() {
        let grouping_set = groups
            .iter()
            .take(active_count)
            .flat_map(|group| group.iter().cloned())
            .collect();
        grouping_sets.push(grouping_set);
    }
    grouping_sets
}

fn cube_grouping_sets(groups: &[Vec<ast::Expr>]) -> Vec<Vec<ast::Expr>> {
    let group_count = groups.len();
    let total = 1usize.checked_shl(group_count as u32).unwrap_or(0);
    let mut grouping_sets = Vec::with_capacity(total);
    for mask in (0..total).rev() {
        let grouping_set = groups
            .iter()
            .enumerate()
            .filter(|(idx, _)| mask & (1usize << idx) != 0)
            .flat_map(|(_, group)| group.iter().cloned())
            .collect();
        grouping_sets.push(grouping_set);
    }
    grouping_sets
}

fn is_bitmap_or_hll_type(sql_type: &novarocks_types::schema::SqlType) -> bool {
    matches!(
        sql_type,
        novarocks_types::schema::SqlType::Bitmap | novarocks_types::schema::SqlType::Hll
    )
}

fn sync_output_columns_from_projection(
    output_columns: &mut [OutputColumn],
    projection: &[ProjectItem],
) {
    for (output, item) in output_columns.iter_mut().zip(projection.iter()) {
        output.value_type = item.expr.value_type.clone();
    }
}

/// A computed-base dereference never acquires a SQL field name from its suffix.
fn natural_field_name(expr: &ast::Expr) -> Option<String> {
    match expr {
        ast::Expr::Identifier(ident) => Some(ident.value.clone()),
        ast::Expr::CompoundIdentifier(parts) => parts.parts.last().map(|ident| ident.value.clone()),
        ast::Expr::Nested(nested) => natural_field_name(&nested.expression),
        ast::Expr::Access(access) if natural_field_name(&access.expr).is_some() => {
            match &access.kind {
                ast::AccessKind::Field(field) => Some(field.value.clone()),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analysis::{
        ApplyClause, ApplyPredicateSpec, ApplyScalarSpec, BinOp, ExprKind, LiteralValue, Relation,
        SubqueryKind,
    };
    use crate::binding::{SqlTableBindingId, SqlTableBindingScopeId};
    use crate::catalog::IcebergMetadataTableProvider;
    use crate::planner::table::{
        ScanSource, SqlMetadataTableKind, SqlScanKind, SqlScanSource, SqlTableIdentity,
        SqlTableVersionSelector, TableDef,
    };
    use novarocks_types::schema::ColumnDef;
    use std::num::{NonZeroU32, NonZeroU64};

    struct TestCatalog;

    #[test]
    fn table_column_lists_remain_relation_local_in_order_by_rebuild() {
        let query = parse_native_query(
            "SELECT l.x FROM t1 AS l(x,y,z) JOIN t2 AS r(a,b,c) ON l.x=r.a ORDER BY r.a",
        )
        .unwrap();
        let (resolved, _, _) = analyze(&query, &TestCatalog, "default").unwrap();
        let QueryBody::Select(select) = &resolved.body else {
            panic!("SELECT")
        };
        let Some(Relation::Join(join)) = &select.from else {
            panic!("JOIN")
        };
        let Relation::Scan(right) = &join.right else {
            panic!("right scan")
        };
        assert!(
            matches!(resolved.order_by[0].expr.kind, ExprKind::ColumnRef {column_id, ..} if column_id == right.column_ids[0])
        );
    }

    #[test]
    fn using_after_table_column_lists_uses_public_fields_and_correct_side() {
        let query = parse_native_query(
            "SELECT k FROM t1 AS l(k,l2,l3) RIGHT JOIN t2 AS r(k,r2,r3) USING(k)",
        )
        .unwrap();
        let (resolved, _, _) = analyze(&query, &TestCatalog, "default").unwrap();
        let QueryBody::Select(select) = &resolved.body else {
            panic!("SELECT")
        };
        let Some(Relation::Join(join)) = &select.from else {
            panic!("JOIN")
        };
        let Relation::Scan(right) = &join.right else {
            panic!("right scan")
        };
        assert!(
            matches!(select.projection[0].expr.kind, ExprKind::ColumnRef {column_id, ..} if column_id == right.column_ids[0])
        );
        let query = parse_native_query(
            "SELECT * FROM t1 AS l(k,l2,l3) FULL JOIN t2 AS r(k,r2,r3) USING(K)",
        )
        .unwrap();
        let (resolved, _, _) = analyze(&query, &TestCatalog, "default").unwrap();
        let QueryBody::Select(select) = &resolved.body else {
            panic!("SELECT")
        };
        assert_eq!(resolved.output_columns[0].field_name.as_deref(), Some("K"));
        assert!(
            matches!(&select.projection[0].expr.kind, ExprKind::FunctionCall {name, ..} if name == "coalesce")
        );
    }

    // SQL analysis owns names; labels remain covered by the existing planner tests.
    #[test]
    fn trino_relation_fields_keep_optional_names_by_occurrence() {
        for (sql, expected) in [
            (
                "SELECT CAST(1 AS INT), CAST(2 AS INT) AS Mixed",
                vec![None, Some("Mixed")],
            ),
            (
                "SELECT X, (x), x + 1 FROM (SELECT 1 AS x) AS t",
                vec![Some("X"), Some("x"), None],
            ),
            (
                "SELECT x, * FROM (SELECT 1 AS x, 2) AS t",
                vec![Some("x"), Some("x"), None],
            ),
            ("WITH c AS (SELECT 1) SELECT * FROM c", vec![None]),
            ("WITH c(a) AS (SELECT 1) SELECT * FROM c", vec![Some("a")]),
            ("SELECT * FROM (VALUES (1, 2)) AS t", vec![None, None]),
            (
                "SELECT * FROM (VALUES (1, 2)) AS t(a, B)",
                vec![Some("a"), Some("B")],
            ),
            ("SELECT 1 UNION ALL SELECT 2 AS n", vec![None]),
        ] {
            let query = parse_native_query(sql).unwrap();
            let (resolved, _, _) = analyze(&query, &TestCatalog, "default").unwrap();
            assert_eq!(
                resolved
                    .output_columns
                    .iter()
                    .map(|field| field.field_name.as_deref())
                    .collect::<Vec<_>>(),
                expected,
                "{sql}"
            );
        }
    }

    #[test]
    fn trino_struct_dereferences_keep_natural_and_computed_names() {
        for (sql, expected_name, expected_type) in [
            (
                "SELECT s.a FROM (SELECT CAST(ROW(1) AS STRUCT<a integer>) AS s) AS t;",
                Some("a"),
                DataType::Int32,
            ),
            (
                "SELECT a2[1].a FROM (VALUES ([CAST(ROW(1) AS STRUCT<a integer>)])) AS t (a2);",
                None,
                DataType::Int32,
            ),
            (
                "SELECT (s).a FROM (SELECT CAST(ROW(1) AS STRUCT<a integer>) AS s) AS t;",
                Some("a"),
                DataType::Int32,
            ),
            (
                "SELECT (CAST(s AS STRUCT<a bigint>)).a FROM (SELECT CAST(ROW(1) AS STRUCT<a integer>) AS s) AS t;",
                None,
                DataType::Int64,
            ),
            (
                "SELECT s.A FROM (SELECT CAST(ROW(1) AS STRUCT<a integer>) AS s) AS t;",
                Some("A"),
                DataType::Int32,
            ),
        ] {
            let query = parse_native_query(sql).unwrap();
            let (resolved, _, _) = analyze(&query, &TestCatalog, "default").unwrap();
            assert_eq!(resolved.output_columns.len(), 1, "{sql}");
            let output = &resolved.output_columns[0];
            assert_eq!(output.field_name.as_deref(), expected_name, "{sql}");
            assert_eq!(output.value_type.data_type(), &expected_type, "{sql}");
        }
    }

    #[test]
    fn anonymous_relation_labels_never_resolve_as_sql_names() {
        for sql in [
            "SELECT _col0 FROM (SELECT 1) AS t",
            "SELECT column_0 FROM (VALUES (1)) AS t",
            "SELECT t.`1` FROM (SELECT 1) AS t",
        ] {
            let query = parse_native_query(sql).unwrap();
            assert_eq!(
                analyze(&query, &TestCatalog, "default").unwrap_err().kind(),
                crate::analyze_error::AnalyzeErrorKind::UnknownColumn,
                "{sql}"
            );
        }
    }

    #[test]
    fn relation_alias_lists_use_visible_width_and_a_single_error_kind() {
        for sql in [
            "WITH c(a, b) AS (SELECT 1) SELECT * FROM c",
            "SELECT * FROM (SELECT 1) AS t(a, b)",
        ] {
            let query = parse_native_query(sql).unwrap();
            assert_eq!(
                analyze(&query, &TestCatalog, "default").unwrap_err().kind(),
                crate::analyze_error::AnalyzeErrorKind::MismatchedColumnAliases,
                "{sql}"
            );
        }
    }

    struct AnalysisObservation {
        checks: std::sync::atomic::AtomicUsize,
        cancel_at: usize,
    }
    impl crate::compiler::SqlCancellationObservation for AnalysisObservation {
        fn is_cancelled(&self) -> bool {
            self.checks
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                >= self.cancel_at
        }
    }

    #[test]
    fn request_control_is_observed_inside_cte_subquery_and_lambda_analysis() {
        for sql in [
            "WITH c AS (SELECT 1 AS k) SELECT k + 2 FROM c",
            "SELECT (SELECT 1) + 2",
            "SELECT array_map(x -> x + 1, [1, 2])",
        ] {
            let query = parse_native_query(sql).unwrap();
            let active = std::sync::Arc::new(AnalysisObservation {
                checks: std::sync::atomic::AtomicUsize::new(0),
                cancel_at: usize::MAX,
            });
            let control = crate::compiler::SqlCompileControl::new(None, active.clone());
            analyze_with_function_catalog(
                &query,
                &TestCatalog,
                "default",
                crate::functions::builtin_sql_function_catalog(),
                crate::constant::test_constant_policy(),
                &control,
            )
            .unwrap();
            let checks = active.checks.load(std::sync::atomic::Ordering::SeqCst);
            assert!(
                checks > 4,
                "actual nested expression work must observe request control: {sql}"
            );
            let cancel = std::sync::Arc::new(AnalysisObservation {
                checks: std::sync::atomic::AtomicUsize::new(0),
                cancel_at: checks / 2,
            });
            let control = crate::compiler::SqlCompileControl::new(None, cancel.clone());
            let error = analyze_with_function_catalog(
                &query,
                &TestCatalog,
                "default",
                crate::functions::builtin_sql_function_catalog(),
                crate::constant::test_constant_policy(),
                &control,
            )
            .unwrap_err();
            assert_eq!(
                error.control_error(),
                Some(novarocks_type_contract::CompileControlError::Cancelled)
            );
            assert_eq!(
                crate::compiler::SqlCompileError::from(error),
                crate::compiler::SqlCompileError::Cancelled
            );
            assert_eq!(
                cancel.checks.load(std::sync::atomic::Ordering::SeqCst),
                checks / 2 + 1,
                "control failure must not retry lexical scopes"
            );
        }
    }

    fn successful_analysis_checks(sql: &str) -> usize {
        let query = parse_native_query(sql).unwrap();
        let observation = std::sync::Arc::new(AnalysisObservation {
            checks: std::sync::atomic::AtomicUsize::new(0),
            cancel_at: usize::MAX,
        });
        let control = crate::compiler::SqlCompileControl::new(None, observation.clone());
        analyze_with_function_catalog(
            &query,
            &TestCatalog,
            "default",
            crate::functions::builtin_sql_function_catalog(),
            crate::constant::test_constant_policy(),
            &control,
        )
        .unwrap();
        observation.checks.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn assert_analysis_stops_at_cancel(sql: &str, cancel_at: usize) {
        let query = parse_native_query(sql).unwrap();
        let observation = std::sync::Arc::new(AnalysisObservation {
            checks: std::sync::atomic::AtomicUsize::new(0),
            cancel_at,
        });
        let control = crate::compiler::SqlCompileControl::new(None, observation.clone());
        let error = analyze_with_function_catalog(
            &query,
            &TestCatalog,
            "default",
            crate::functions::builtin_sql_function_catalog(),
            crate::constant::test_constant_policy(),
            &control,
        )
        .unwrap_err();
        assert_eq!(
            error.control_error(),
            Some(novarocks_type_contract::CompileControlError::Cancelled)
        );
        assert_eq!(
            observation.checks.load(std::sync::atomic::Ordering::SeqCst),
            cancel_at + 1,
            "typed cancellation must leave analysis without another observation"
        );
    }

    #[test]
    fn having_control_failure_does_not_retry_the_select_alias_scope() {
        let prefix_checks = successful_analysis_checks("SELECT 1 AS total");
        let sql = "SELECT 1 AS total HAVING total > 0";
        assert!(successful_analysis_checks(sql) > prefix_checks);
        // The prefix's final checkpoint is replaced by the first HAVING
        // expression checkpoint. This reaches HAVING itself, before its
        // legitimate name-resolution failure can trigger alias substitution.
        assert_analysis_stops_at_cancel(sql, prefix_checks - 1);
    }

    #[test]
    fn wide_qualified_wildcard_observes_control_during_column_expansion() {
        let columns = (0..320)
            .map(|index| format!("1 AS c{index}"))
            .collect::<Vec<_>>()
            .join(", ");
        let from = format!("FROM (SELECT {columns}) AS w");
        let narrow_checks = successful_analysis_checks(&format!("SELECT 1 {from}"));
        let sql = format!("SELECT w.* {from}");
        let checks = successful_analysis_checks(&sql);
        assert!(checks >= narrow_checks + 256);
        // All derived-column expressions have completed; the remaining tail
        // is the actual qualified wildcard expansion and final checkpoints.
        assert_analysis_stops_at_cancel(&sql, checks - 128);
    }

    #[test]
    fn values_rectangular_provenance_pass_observes_control_between_cells() {
        let row = format!("({})", vec!["1"; 16].join(", "));
        let sql = format!("VALUES {}", vec![row; 40].join(", "));
        let checks = successful_analysis_checks(&sql);
        assert!(checks > 16 * 40);
        // After expression analysis, the provenance pass examines the full
        // rectangle again. Cancellation here exercises that pass's own
        // observation rather than a literal-expression checkpoint.
        assert_analysis_stops_at_cancel(&sql, checks - 128);
    }

    #[test]
    fn analyzed_products_do_not_retain_the_borrowed_request_control() {
        let observation = std::sync::Arc::new(AnalysisObservation {
            checks: std::sync::atomic::AtomicUsize::new(0),
            cancel_at: usize::MAX,
        });
        let weak = std::sync::Arc::downgrade(&observation);
        let control = crate::compiler::SqlCompileControl::new(None, observation.clone());
        let query = parse_native_query("SELECT 1").unwrap();
        let products = analyze_with_function_catalog(
            &query,
            &TestCatalog,
            "default",
            crate::functions::builtin_sql_function_catalog(),
            crate::constant::test_constant_policy(),
            &control,
        )
        .unwrap();
        drop(control);
        drop(observation);
        assert!(weak.upgrade().is_none());
        assert_eq!(products.0.output_columns.len(), 1);
    }

    fn sql_test_scan_source(
        catalog: &str,
        namespace: &str,
        table: &str,
        kind: SqlScanKind,
    ) -> ScanSource {
        let mut ordinal = 2_166_136_261u32;
        for byte in catalog
            .bytes()
            .chain(namespace.bytes())
            .chain(table.bytes())
            .chain([match &kind {
                SqlScanKind::ConnectorRead => 1,
                SqlScanKind::PinnedFileSet => 8,
                SqlScanKind::TableExecute => 9,
                SqlScanKind::Data { .. } => 2,
                SqlScanKind::FrozenInputSet { .. } => 3,
                SqlScanKind::Metadata { .. } => 4,
                SqlScanKind::Delta { .. } => 5,
                SqlScanKind::MvTargetState { .. } => 6,
                SqlScanKind::MvTargetLocator { .. } => 7,
            }])
        {
            ordinal = ordinal.wrapping_mul(16_777_619) ^ u32::from(byte);
        }
        ScanSource::Sql(SqlScanSource::new(
            SqlTableBindingId::new(
                SqlTableBindingScopeId::new(NonZeroU64::new(41).expect("non-zero scope")),
                NonZeroU32::new(ordinal.max(1)).expect("non-zero binding ordinal"),
            ),
            SqlTableIdentity {
                catalog: catalog.to_string(),
                namespace: namespace.to_string(),
                table: table.to_string(),
            },
            kind,
        ))
    }

    fn metadata_table_columns(metadata_table_type: &SqlMetadataTableKind) -> Vec<ColumnDef> {
        crate::analyzer::iceberg_metadata::metadata_table_schema(*metadata_table_type)
            .into_iter()
            .map(|column| ColumnDef {
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
                write_default: None,
                logical_type: None,
            })
            .collect()
    }

    impl TestCatalog {
        fn get_table(&self, database: &str, table: &str) -> Result<TableDef, String> {
            match table {
                "t1" | "t2" | "t3" => {
                    let value_col = match table {
                        "t1" => "v1",
                        "t2" => "v2",
                        _ => "v3",
                    };
                    Ok(TableDef {
                        name: table.to_string(),
                        columns: vec![
                            ColumnDef {
                                name: "k1".to_string(),
                                data_type: arrow::datatypes::DataType::Int64,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                            ColumnDef {
                                name: "k2".to_string(),
                                data_type: arrow::datatypes::DataType::Int64,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                            ColumnDef {
                                name: value_col.to_string(),
                                data_type: arrow::datatypes::DataType::Utf8,
                                nullable: true,
                                write_default: None,
                                logical_type: None,
                            },
                        ],
                        iceberg_row_lineage_metadata_columns: vec![],
                        source: sql_test_scan_source(
                            "test_catalog",
                            database,
                            table,
                            SqlScanKind::Data {
                                version: SqlTableVersionSelector::Current,
                            },
                        ),
                    })
                }
                "array_test" => Ok(TableDef {
                    name: "array_test".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "s_1".to_string(),
                            data_type: arrow::datatypes::DataType::List(
                                arrow::datatypes::Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Utf8,
                                    true,
                                )
                                .into(),
                            ),
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "i_1".to_string(),
                            data_type: arrow::datatypes::DataType::List(
                                arrow::datatypes::Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Int64,
                                    true,
                                )
                                .into(),
                            ),
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "d_1".to_string(),
                            data_type: arrow::datatypes::DataType::List(
                                arrow::datatypes::Field::new(
                                    "item",
                                    arrow::datatypes::DataType::Decimal128(26, 2),
                                    true,
                                )
                                .into(),
                            ),
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: sql_test_scan_source(
                        "test_catalog",
                        database,
                        table,
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    ),
                }),
                "orders" => Ok(TableDef {
                    name: "orders".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "o_orderkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_orderstatus".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_totalprice".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_orderdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_orderpriority".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: sql_test_scan_source(
                        "test_catalog",
                        database,
                        table,
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    ),
                }),
                "lineitem" => Ok(TableDef {
                    name: "lineitem".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "l_orderkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_partkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_suppkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_quantity".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_extendedprice".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_discount".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_commitdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_receiptdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "l_shipdate".to_string(),
                            data_type: arrow::datatypes::DataType::Date32,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: sql_test_scan_source(
                        "test_catalog",
                        database,
                        table,
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    ),
                }),
                "supplier" => Ok(TableDef {
                    name: "supplier".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "s_suppkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "s_name".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "s_comment".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: sql_test_scan_source(
                        "test_catalog",
                        database,
                        table,
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    ),
                }),
                "part" => Ok(TableDef {
                    name: "part".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "p_partkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "p_name".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "p_brand".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: sql_test_scan_source(
                        "test_catalog",
                        database,
                        table,
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    ),
                }),
                "partsupp" => Ok(TableDef {
                    name: "partsupp".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "ps_partkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "ps_suppkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "ps_supplycost".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "ps_availqty".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: sql_test_scan_source(
                        "test_catalog",
                        database,
                        table,
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    ),
                }),
                "customer" => Ok(TableDef {
                    name: "customer".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "c_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "c_acctbal".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "c_phone".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: sql_test_scan_source(
                        "test_catalog",
                        database,
                        table,
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    ),
                }),
                "nation" => Ok(TableDef {
                    name: "nation".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "n_nationkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "n_name".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    source: sql_test_scan_source(
                        "test_catalog",
                        database,
                        table,
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    ),
                }),
                // IVM-A1 v3-row-lineage fixture: an iceberg-backed base
                // table exposing the row-lineage metadata pseudo-columns
                // (_row_id, _last_updated_sequence_number) that
                // `__nr_ivm_delta` requires.
                "iv_orders" => Ok(TableDef {
                    name: "iv_orders".to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "o_orderkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "o_custkey".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![
                        ColumnDef {
                            name: "_row_id".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                        ColumnDef {
                            name: "_last_updated_sequence_number".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                            logical_type: None,
                        },
                    ],
                    source: sql_test_scan_source(
                        "test_catalog",
                        database,
                        table,
                        SqlScanKind::Data {
                            version: SqlTableVersionSelector::Current,
                        },
                    ),
                }),
                _ => Err(format!("table not found: {table}")),
            }
        }
    }

    impl crate::catalog::PlannerTableProvider for TestCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            let planner = self.get_table(database, table)?;
            Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }

        fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
            Some(self)
        }
    }

    impl IcebergMetadataTableProvider for TestCatalog {
        fn get_iceberg_metadata_table(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
            metadata_table_type: SqlMetadataTableKind,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            let mut table_def = self.get_table(database, table)?;
            table_def.columns = metadata_table_columns(&metadata_table_type);
            table_def.source = sql_test_scan_source(
                catalog.unwrap_or("default_catalog"),
                database,
                table,
                SqlScanKind::Metadata {
                    kind: metadata_table_type,
                    version: SqlTableVersionSelector::Current,
                },
            );
            Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                catalog, database, table_def,
            ))
        }
    }

    fn parse_native_query(sql: &str) -> Result<ast::Query, String> {
        let mut statements = novarocks_parser::parse(sql).map_err(|error| error.to_string())?;
        let [novarocks_parser::ast::Statement::Query(query)] = statements.as_mut_slice() else {
            return Err("expected exactly one query".to_string());
        };
        Ok(query.clone())
    }

    fn parse_and_analyze(sql: &str) -> Result<ResolvedQuery, String> {
        let query = parse_native_query(sql)?;
        let (resolved, _registry, _factory) =
            analyze(&query, &TestCatalog, "default").map_err(|error| error.to_string())?;
        Ok(resolved)
    }

    fn parse_and_analyze_with_registry(
        sql: &str,
    ) -> Result<(ResolvedQuery, crate::analysis::cte::CTERegistry), String> {
        let query = parse_native_query(sql)?;
        let (resolved, registry, _factory) =
            analyze(&query, &TestCatalog, "default").map_err(|error| error.to_string())?;
        Ok((resolved, registry))
    }

    fn parse_raw_and_analyze(sql: &str) -> Result<ResolvedQuery, String> {
        let query = parse_native_query(sql)?;
        let (resolved, _registry, _factory) =
            analyze(&query, &TestCatalog, "default").map_err(|error| error.to_string())?;
        Ok(resolved)
    }

    #[test]
    fn decimal_literal_over_decimal128_precision_infers_decimal256() {
        let resolved =
            parse_raw_and_analyze("select 123456789012345678901234567890.123456789 as d")
                .expect("wide decimal literal should analyze");

        assert_eq!(
            resolved.output_columns[0].value_type.data_type().clone(),
            DataType::Decimal256(39, 9)
        );
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.projection[0].expr.value_type.data_type().clone(),
            DataType::Decimal256(39, 9)
        );
    }

    #[test]
    fn decimal_literal_above_decimal256_precision_is_rejected() {
        let err = parse_raw_and_analyze(
            "select 12345678901234567890123456789012345678901234567890123456789012345678901234567.1",
        )
        .expect_err("decimal literal above Decimal256 precision should fail during analysis");

        assert!(
            err.contains("decimal literal precision 78 exceeds maximum precision 76"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn array_agg_window_decimal_literal_preserves_decimal256_item() {
        let (resolved, _) = parse_and_analyze_with_registry(
            "select array_agg(123456789012345678901234567890.123456789) \
         over(partition by o_orderstatus order by o_orderkey) as arr from orders",
        )
        .expect("array_agg window over wide decimal literal should analyze");

        let expected = DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new(
            "item",
            DataType::Decimal256(39, 9),
            true,
        )));
        assert_eq!(
            resolved.output_columns[0].value_type.data_type().clone(),
            expected
        );
    }

    #[test]
    fn hex_string_literal_analyzes_as_binary() {
        let resolved = parse_raw_and_analyze("SELECT X'AB01'").expect("analysis should succeed");
        assert_eq!(
            resolved.output_columns[0].value_type.data_type().clone(),
            DataType::Binary
        );
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        let ExprKind::Literal(LiteralValue::Binary(bytes)) = &select.projection[0].expr.kind else {
            panic!("expected binary literal");
        };
        assert_eq!(bytes, &[0xab, 0x01]);
    }

    #[test]
    fn derived_values_column_alias_is_visible() {
        let sql = "SELECT col1 FROM (VALUES (1)) AS tmp(col1)";
        let (resolved, _) = parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        assert_eq!(resolved.output_columns[0].name, "col1");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        let Relation::Subquery {
            query,
            output_columns,
            ..
        } = select.from.as_ref().expect("expected FROM relation")
        else {
            panic!("expected derived subquery");
        };
        assert_eq!(query.output_columns[0].name, "column_0");
        assert_eq!(output_columns[0].name, "col1");
        let ExprKind::ColumnRef {
            qualifier, column, ..
        } = &select.projection[0].expr.kind
        else {
            panic!("expected column ref projection");
        };
        assert_eq!(qualifier.as_deref(), None);
        assert_eq!(column, "col1");
    }

    #[test]
    fn derived_table_column_alias_count_must_match() {
        let sql = "SELECT a FROM (VALUES (1, 2)) AS tmp(a)";
        let query = parse_native_query(sql).unwrap();
        let error = analyze(&query, &TestCatalog, "default").expect_err("analysis must fail");
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::MismatchedColumnAliases
        );
    }

    fn expr_has_qualified_column(expr: &TypedExpr, qualifier: &str, column: &str) -> bool {
        match &expr.kind {
            ExprKind::ColumnRef {
                qualifier: Some(q),
                column: c,
                ..
            } => q.eq_ignore_ascii_case(qualifier) && c.eq_ignore_ascii_case(column),
            ExprKind::BinaryOp { left, right, .. } => {
                expr_has_qualified_column(left, qualifier, column)
                    || expr_has_qualified_column(right, qualifier, column)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::Nested(expr) => expr_has_qualified_column(expr, qualifier, column),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => args
                .iter()
                .any(|arg| expr_has_qualified_column(arg, qualifier, column)),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| expr_has_qualified_column(expr, qualifier, column))
                    || when_then.iter().any(|(when, then)| {
                        expr_has_qualified_column(when, qualifier, column)
                            || expr_has_qualified_column(then, qualifier, column)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|expr| expr_has_qualified_column(expr, qualifier, column))
            }
            ExprKind::InList { expr, list, .. } => {
                expr_has_qualified_column(expr, qualifier, column)
                    || list
                        .iter()
                        .any(|item| expr_has_qualified_column(item, qualifier, column))
            }
            _ => false,
        }
    }

    fn expr_has_unqualified_column(expr: &TypedExpr, column: &str) -> bool {
        match &expr.kind {
            ExprKind::ColumnRef {
                qualifier: None,
                column: c,
                ..
            } => c.eq_ignore_ascii_case(column),
            ExprKind::BinaryOp { left, right, .. } => {
                expr_has_unqualified_column(left, column)
                    || expr_has_unqualified_column(right, column)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::Nested(expr) => expr_has_unqualified_column(expr, column),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => args
                .iter()
                .any(|arg| expr_has_unqualified_column(arg, column)),
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| expr_has_unqualified_column(expr, column))
                    || when_then.iter().any(|(when, then)| {
                        expr_has_unqualified_column(when, column)
                            || expr_has_unqualified_column(then, column)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|expr| expr_has_unqualified_column(expr, column))
            }
            ExprKind::InList { expr, list, .. } => {
                expr_has_unqualified_column(expr, column)
                    || list
                        .iter()
                        .any(|item| expr_has_unqualified_column(item, column))
            }
            _ => false,
        }
    }

    fn select_body(resolved: &ResolvedQuery) -> &ResolvedSelect {
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        sel
    }

    fn only_predicate_spec(sel: &ResolvedSelect) -> &ApplyPredicateSpec {
        assert_eq!(
            sel.predicate_apply_specs.len(),
            1,
            "expected one predicate Apply spec: {:?}",
            sel.predicate_apply_specs
        );
        &sel.predicate_apply_specs[0]
    }

    fn only_scalar_spec(sel: &ResolvedSelect) -> &ApplyScalarSpec {
        assert_eq!(
            sel.apply_specs.len(),
            1,
            "expected one scalar Apply spec: {:?}",
            sel.apply_specs
        );
        &sel.apply_specs[0]
    }

    fn apply_inner_filter(query: &ResolvedQuery) -> &TypedExpr {
        let QueryBody::Select(sel) = &query.body else {
            panic!("expected Select body in Apply inner query");
        };
        sel.filter
            .as_ref()
            .expect("expected Apply inner query filter")
    }

    #[test]
    fn json_in_value_form_admission_preserves_clause_and_json_evidence() {
        for (sql, clause) in [
            (
                "select parse_json('{}') in (select parse_json('{}'))",
                ApplyClause::Projection,
            ),
            (
                "select 1 where false or parse_json('{}') not in (select parse_json('null'))",
                ApplyClause::Where,
            ),
            (
                "select count(*) from orders having false or parse_json(cast(max(o_orderkey) as varchar)) in (select parse_json('1'))",
                ApplyClause::Having,
            ),
            (
                "select parse_json(cast(max(o_orderkey) as varchar)) as j from orders having false or j in (select parse_json('1'))",
                ApplyClause::Having,
            ),
            (
                "select false or parse_json('{}') in (select parse_json('{}'))",
                ApplyClause::Projection,
            ),
        ] {
            let resolved = parse_and_analyze(sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            let spec = only_scalar_spec(select_body(&resolved));
            assert_eq!(spec.clause, clause);
            let probe = spec.mark_probe.as_ref().expect("JSON value-form probe");
            assert_eq!(
                probe.value_type.logical_type(),
                novarocks_type_contract::ValueLogicalType::Json
            );
            assert_eq!(spec.inner.output_columns.len(), 1);
            assert!(!spec.need_check_max_rows);
            assert!(spec.output_column.value_type.nullable());
            let bindings = spec
                .mark_key_bindings
                .as_ref()
                .expect("two original key bindings");
            for binding in bindings {
                assert_eq!(
                    binding.function_id.as_str(),
                    novarocks_functions::json_canonical::FUNCTION_ID
                );
                let novarocks_functions::FunctionResultType::Scalar(result) =
                    &binding.selected.result_type
                else {
                    panic!("scalar key")
                };
                assert_eq!(result.data_type(), &DataType::Utf8);
                assert_eq!(
                    result.logical_type(),
                    novarocks_type_contract::ValueLogicalType::Physical
                );
            }
        }
    }

    #[test]
    fn json_in_subquery_rejects_unsupported_positions_and_erased_identity() {
        for sql in [
            "select 1 where parse_json('{}') in (select parse_json('{}'))",
            "select 1 where not (parse_json('{}') in (select parse_json('{}')))",
            "select 1 where case when parse_json('{}') in (select parse_json('{}')) then true else false end",
            "select parse_json('{}') in (select '{}')",
            "select parse_json('1') in (select cast(1 as variant))",
            "select cast(1 as variant) in (select parse_json('1'))",
            "select '{}' in (select parse_json('{}'))",
            "select parse_json('{}') in (select parse_json('{}'), parse_json('{}'))",
            "select parse_json('{}') in (select parse_json(cast(o_orderkey as varchar))) from orders",
            "select count(parse_json('{}') in (select parse_json('{}'))) from orders",
            "select * from orders join lineitem on parse_json('{}') in (select parse_json('{}'))",
        ] {
            assert!(parse_and_analyze(sql).is_err(), "must reject: {sql}");
        }
    }

    #[test]
    fn json_typed_literals_preserve_the_declared_domain_and_original_bytes() {
        for text in [
            "1.00",
            "123456789012345678901234567890",
            "1E400",
            "not JSON",
        ] {
            let sql = format!("select JSON '{text}'");
            let resolved = parse_and_analyze(&sql).unwrap_or_else(|error| panic!("{sql}: {error}"));
            let item = &select_body(&resolved).projection[0].expr;
            assert_eq!(
                item.value_type.logical_type(),
                novarocks_type_contract::ValueLogicalType::Json
            );
            assert!(!item.value_type.nullable());
            assert!(
                matches!(&item.kind, ExprKind::Literal(LiteralValue::String(actual)) if actual == text)
            );
        }
        let resolved = parse_and_analyze("select JSON '1.00' in (select JSON '1.0')")
            .expect("typed JSON mark admission");
        let spec = only_scalar_spec(select_body(&resolved));
        assert!(spec.mark_key_bindings.is_some());
        assert!(
            matches!(&spec.mark_probe.as_ref().unwrap().kind, ExprKind::Literal(LiteralValue::String(value)) if value == "1.00")
        );
    }

    #[test]
    fn json_comparison_rejects_foreign_domains_before_carrier_coercion() {
        for sql in [
            "select parse_json('1') in (cast(1 as variant))",
            "select cast(1 as variant) in (parse_json('1'))",
            "select parse_json('1') = cast(1 as variant)",
            "select parse_json('1') in ('1')",
            "select parse_json('1') in (1)",
            "select __json_canonical_key(parse_json('1'))",
        ] {
            assert!(parse_and_analyze(sql).is_err(), "must reject {sql}");
        }
        assert!(parse_and_analyze("select parse_json('1') in (parse_json('1'), null)").is_ok());
        assert!(parse_and_analyze("select '1.0' in ('1.00')").is_ok());
    }

    #[test]
    fn exists_subquery_rewrites_to_left_semi_join() {
        let sql = "SELECT o_orderpriority, count(*) FROM orders \
                    WHERE exists (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey) \
                    GROUP BY o_orderpriority";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(spec.kind, SubqueryKind::Exists { negated: false }));
        assert_eq!(spec.clause, ApplyClause::Where);
        assert!(spec.use_semi_anti);
        assert!(
            sel.filter.is_none() || !filter_has_placeholder(&sel.filter),
            "filter should not contain SubqueryPlaceholder"
        );
    }

    #[test]
    fn correlated_exists_self_join_apply_inner_keeps_outer_qualifier_for_ambiguous_column() {
        let sql = "SELECT l1.l_orderkey FROM lineitem l1 \
                   WHERE EXISTS (SELECT 1 FROM lineitem l2 \
                                 WHERE l2.l_partkey = l1.l_suppkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        let cond = apply_inner_filter(&spec.inner);

        assert!(
            expr_has_qualified_column(cond, "l1", "l_suppkey"),
            "outer self-join column must keep its qualifier in correlated EXISTS filter: {cond:?}"
        );
        let ExprKind::BinaryOp {
            left, op, right, ..
        } = &cond.kind
        else {
            panic!("expected binary correlation condition, got: {cond:?}");
        };
        assert_eq!(*op, BinOp::Eq);
        assert!(
            expr_has_qualified_column(left, "l2", "l_partkey"),
            "inner column should stay on the original SQL left side in Apply inner filter: {cond:?}"
        );
        assert!(
            expr_has_qualified_column(right, "l1", "l_suppkey"),
            "outer column should stay on the original SQL right side in Apply inner filter: {cond:?}"
        );
    }

    #[test]
    fn exists_multi_table_subquery_rewrites_to_semi_join() {
        // When the EXISTS subquery has multiple tables and a mix of correlation
        // and inner-join predicates, the subquery rewriter produces a LEFT SEMI
        // JOIN. The right side is the flattened inner FROM clause.
        //
        // Known limitation: ideally `l_suppkey = s_suppkey` and `s_name = 'test'`
        // should stay inside the subquery as inner predicates (preventing the
        // inner side from degenerating into a CROSS JOIN). The current rewriter
        // hoists all predicates to the SEMI condition and flattens the FROM
        // clause as a bare multi-table join. Predicate pushdown in the rewrite/CBO
        // phases later pushes them back down, so the runtime plan is
        // functionally correct (TPC-DS 98/99 pass), but the analyzer output is
        // suboptimal for this specific pattern. This test documents the current
        // behavior; a proper fix would be to partition predicates into
        // correlation vs inner during subquery rewriting.
        let sql = "SELECT o_orderkey FROM orders \
                    WHERE EXISTS (SELECT * FROM lineitem, supplier \
                                  WHERE l_orderkey = o_orderkey \
                                  AND l_suppkey = s_suppkey \
                                  AND s_name = 'test')";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(spec.kind, SubqueryKind::Exists { negated: false }));
        assert!(
            *apply_inner_filter(&spec.inner).value_type.data_type()
                == arrow::datatypes::DataType::Boolean,
            "inner EXISTS filter should retain correlation and residual predicates"
        );
    }

    #[test]
    fn not_exists_subquery_rewrites_to_left_anti_join() {
        let sql = "SELECT o_orderpriority FROM orders \
                    WHERE not exists (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(spec.kind, SubqueryKind::Exists { negated: true }));
        assert_eq!(spec.clause, ApplyClause::Where);
    }

    #[test]
    fn in_subquery_rewrites_to_left_semi_join() {
        let sql = "SELECT o_orderkey FROM orders \
                    WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(
            spec.kind,
            SubqueryKind::InSubquery { negated: false }
        ));
        assert!(spec.in_lhs.is_some());
    }

    #[test]
    fn not_in_subquery_rewrites_to_left_anti_join() {
        let sql = "SELECT s_suppkey FROM supplier \
                    WHERE s_suppkey NOT IN (SELECT ps_suppkey FROM partsupp)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(
            spec.kind,
            SubqueryKind::InSubquery { negated: true }
        ));
        assert!(spec.in_lhs.is_some());
    }

    #[test]
    fn tuple_not_in_keeps_every_value_pair_and_the_original_nullable_author() {
        use crate::analysis::{ExprKind, Relation, TypedExpr};
        use crate::common::{BinOp, JoinKind};

        fn pairs(expr: &TypedExpr, result: &mut Vec<(String, String)>) {
            match &expr.kind {
                ExprKind::BinaryOp {
                    left,
                    op: BinOp::And,
                    right,
                    ..
                } => {
                    pairs(left, result);
                    pairs(right, result);
                }
                ExprKind::BinaryOp {
                    left,
                    op: BinOp::Eq,
                    right,
                    ..
                } => {
                    let (
                        ExprKind::ColumnRef { column: left, .. },
                        ExprKind::ColumnRef { column: right, .. },
                    ) = (&left.kind, &right.kind)
                    else {
                        panic!("the SQL compares the two original column pairs")
                    };
                    result.push((left.clone(), right.clone()));
                }
                other => panic!("unexpected original comparison {other:?}"),
            }
        }
        for (sql, correlated) in [
            (
                "SELECT l1.l_orderkey FROM lineitem l1 WHERE (l1.l_orderkey,l1.l_shipdate) NOT IN (SELECT l2.l_orderkey,l2.l_shipdate FROM lineitem l2)",
                false,
            ),
            (
                "SELECT l1.l_orderkey FROM lineitem l1 WHERE (l1.l_orderkey,l1.l_shipdate) NOT IN (SELECT l2.l_orderkey,l2.l_shipdate FROM lineitem l2 WHERE l2.l_suppkey=l1.l_suppkey)",
                true,
            ),
        ] {
            let resolved = parse_and_analyze(sql).expect("analyze tuple NOT IN");
            let select = select_body(&resolved);
            let Some(Relation::Join(join)) = &select.from else {
                panic!("tuple NOT IN has an explicit comparison author")
            };
            assert_eq!(join.join_type, JoinKind::NullAwareLeftAnti);
            let author = join.naa_author.as_ref().expect("original NOT IN roles");
            let mut actual = Vec::new();
            pairs(&author.comparison, &mut actual);
            assert_eq!(
                actual,
                [
                    ("l_orderkey".into(), "l_orderkey".into()),
                    ("l_shipdate".into(), "l_shipdate".into())
                ]
            );
            // Only the second SQL pair carries NULL. It must still select
            // NAA and preserve UNKNOWN in the complete original comparison.
            assert!(author.comparison.value_type.nullable());
            assert!(join.condition.as_ref().unwrap().value_type.nullable());
            assert_eq!(author.eligibility.is_some(), correlated);
        }
    }

    #[test]
    fn correlated_not_in_nullable_key_rewrites_to_null_aware_left_anti() {
        let sql = "SELECT l1.l_orderkey FROM lineitem l1 \
                   WHERE l1.l_shipdate NOT IN ( \
                       SELECT l2.l_shipdate FROM lineitem l2 \
                       WHERE l2.l_suppkey = l1.l_suppkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);

        assert!(
            matches!(spec.kind, SubqueryKind::InSubquery { negated: true }),
            "correlated nullable NOT IN should be routed as a negated IN Apply spec"
        );
        assert!(
            spec.in_lhs
                .as_ref()
                .is_some_and(|lhs| lhs.value_type.nullable())
        );
    }

    #[test]
    fn correlated_not_in_nullable_filter_stays_residual_for_null_aware_anti() {
        let sql = "SELECT l1.l_orderkey FROM lineitem l1 \
                   WHERE l1.l_shipdate NOT IN ( \
                       SELECT l2.l_shipdate FROM lineitem l2 \
                       WHERE l2.l_receiptdate = l1.l_receiptdate)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        let filter = apply_inner_filter(&spec.inner);

        assert!(
            expr_has_qualified_column(filter, "l1", "l_receiptdate")
                && expr_has_qualified_column(filter, "l2", "l_receiptdate"),
            "correlated NOT IN filter should stay in Apply inner query, got: {filter:?}"
        );
    }

    #[test]
    fn subquery_unqualified_name_prefers_inner_over_outer_using_canonical() {
        let sql = "SELECT k1, k2, v1, v2 \
                   FROM t1 FULL OUTER JOIN t2 USING(k1, k2) \
                   WHERE EXISTS (SELECT 1 FROM t3 WHERE t3.k1 = k1 AND t3.k2 = k2)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        let cond = apply_inner_filter(&spec.inner);

        assert!(
            !expr_has_qualified_column(cond, "t1", "k1")
                && !expr_has_qualified_column(cond, "t2", "k1"),
            "inner subquery k1 must not inherit outer USING canonical qualifier: {cond:?}"
        );
        assert!(
            expr_has_qualified_column(cond, "t3", "k1")
                && expr_has_qualified_column(cond, "t3", "k2"),
            "subquery-local names should resolve to t3 columns: {cond:?}"
        );
        assert!(
            !expr_has_unqualified_column(cond, "k1") && !expr_has_unqualified_column(cond, "k2"),
            "subquery-local shadowing columns must stay qualified for codegen: {cond:?}"
        );
    }

    #[test]
    fn correlated_exists_apply_inner_preserves_original_compare_sides() {
        let sql = "SELECT s.s_1 FROM array_test s \
                   WHERE EXISTS (SELECT 1 FROM array_test t WHERE t.s_1 = s.d_1)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        let cond = apply_inner_filter(&spec.inner);

        let ExprKind::BinaryOp {
            left, op, right, ..
        } = &cond.kind
        else {
            panic!("expected binary correlation condition, got: {cond:?}");
        };
        assert_eq!(*op, BinOp::Eq);
        assert!(expr_has_qualified_column(left, "t", "s_1"));
        assert!(expr_has_qualified_column(right, "s", "d_1"));
    }

    #[test]
    fn uncorrelated_scalar_subquery_rewrites_to_cross_join() {
        let sql = "SELECT c_custkey FROM customer \
                    WHERE c_acctbal > (SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_scalar_spec(sel);
        assert_eq!(spec.clause, ApplyClause::Where);
        assert!(spec.correlation_column_ids.is_empty());
        assert!(
            sel.filter.is_some(),
            "filter should still contain the comparison"
        );
        assert!(
            !filter_has_placeholder(&sel.filter),
            "filter should not contain SubqueryPlaceholder"
        );
    }

    #[test]
    fn correlated_scalar_subquery_rewrites_to_left_join() {
        let sql = "SELECT l_orderkey FROM lineitem, part \
                    WHERE p_partkey = l_partkey \
                    AND l_quantity < (SELECT 0.2 * avg(l_quantity) FROM lineitem WHERE l_partkey = p_partkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_scalar_spec(sel);
        assert_eq!(spec.clause, ApplyClause::Where);
        assert!(
            !spec.correlation_column_ids.is_empty(),
            "correlated scalar subquery should record outer correlation columns"
        );
    }

    #[test]
    fn scalar_subquery_in_having_rewrites_to_cross_join() {
        let sql = "SELECT ps_partkey, sum(ps_supplycost) as value FROM partsupp \
                    GROUP BY ps_partkey \
                    HAVING sum(ps_supplycost) > (SELECT sum(ps_supplycost) * 0.0001 FROM partsupp)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_scalar_spec(sel);
        assert_eq!(spec.clause, ApplyClause::Having);
        assert!(
            sel.having.is_some() && !filter_has_placeholder(&sel.having),
            "HAVING should retain comparison with scalar Apply output and no placeholder"
        );
    }

    #[test]
    fn scalar_subquery_in_projection_without_outer_from_synthesizes_dummy() {
        // Mirrors the iceberg_in_list_predicate failure pattern: outer SELECT
        // has no FROM clause; the projection contains scalar subqueries.
        // Without a synthetic single-row FROM, the subquery rewriter would
        // bail out with `scalar subquery rewrite requires a FROM clause`.
        let sql = "SELECT COALESCE((SELECT count(*) FROM customer WHERE c_acctbal > 0), 0) AS n_pos, \
             COALESCE((SELECT count(*) FROM customer WHERE c_acctbal < 0), 0) AS n_neg";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        assert!(
            sel.from.is_none(),
            "analyzer should not synthesize FROM for Apply specs"
        );
        assert_eq!(sel.apply_specs.len(), 2);
        assert!(
            sel.apply_specs
                .iter()
                .all(|spec| spec.clause == ApplyClause::Projection),
            "projection scalar subqueries should be routed to projection Apply specs"
        );
        assert!(
            sel.projection
                .iter()
                .all(|item| !expr_has_placeholder(&item.expr)),
            "projection should not contain SubqueryPlaceholder"
        );
    }

    #[test]
    fn scalar_subquery_in_aggregate_argument_routes_before_aggregate() {
        let sql = "SELECT sum((SELECT max(c_custkey) FROM customer)) FROM customer";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_scalar_spec(sel);
        assert_eq!(spec.clause, ApplyClause::AggregateInput);
        assert!(
            sel.projection
                .iter()
                .all(|item| !expr_has_placeholder(&item.expr)),
            "aggregate argument should reference the Apply output column"
        );
    }

    #[test]
    fn multiple_subqueries_exists_and_not_exists() {
        // q21 pattern: EXISTS + NOT EXISTS in the same WHERE
        let sql = "SELECT s_name FROM supplier, lineitem l1, orders, nation \
                    WHERE s_suppkey = l1.l_suppkey \
                    AND o_orderkey = l1.l_orderkey \
                    AND exists (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey <> l1.l_suppkey) \
                    AND not exists (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey <> l1.l_suppkey)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        assert_eq!(sel.predicate_apply_specs.len(), 2);
        assert!(
            sel.predicate_apply_specs
                .iter()
                .any(|spec| matches!(spec.kind, SubqueryKind::Exists { negated: false })),
            "EXISTS should produce a predicate Apply spec"
        );
        assert!(
            sel.predicate_apply_specs
                .iter()
                .any(|spec| matches!(spec.kind, SubqueryKind::Exists { negated: true })),
            "NOT EXISTS should produce a predicate Apply spec"
        );
        assert!(
            !filter_has_placeholder(&sel.filter),
            "filter should not contain SubqueryPlaceholder"
        );
    }

    #[test]
    fn subquery_in_from_derived_table() {
        // q22 pattern: subquery inside a derived table in FROM
        let sql = "SELECT cntrycode FROM \
                    (SELECT substring(c_phone, 1, 2) as cntrycode, c_acctbal FROM customer \
                     WHERE c_acctbal > (SELECT avg(c_acctbal) FROM customer WHERE c_acctbal > 0.00)) as custsale";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        // The subquery in the derived table should be rewritten to a CROSS JOIN
        // within the derived table's ResolvedQuery
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn in_subquery_with_group_by_having() {
        // q18 pattern: IN subquery with GROUP BY and HAVING
        let sql = "SELECT o_orderkey FROM orders \
                    WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING sum(l_quantity) > 315)";
        let resolved = parse_and_analyze(sql).expect("analysis should succeed");
        let sel = select_body(&resolved);
        let spec = only_predicate_spec(sel);
        assert!(matches!(
            spec.kind,
            SubqueryKind::InSubquery { negated: false }
        ));
        assert!(spec.in_lhs.is_some());
    }

    fn filter_has_placeholder(filter: &Option<TypedExpr>) -> bool {
        match filter {
            Some(expr) => expr_has_placeholder(expr),
            None => false,
        }
    }

    fn expr_has_placeholder(expr: &TypedExpr) -> bool {
        match &expr.kind {
            ExprKind::SubqueryPlaceholder { .. } => true,
            ExprKind::BinaryOp { left, right, .. } => {
                expr_has_placeholder(left) || expr_has_placeholder(right)
            }
            ExprKind::UnaryOp { expr, .. } => expr_has_placeholder(expr),
            ExprKind::Nested(inner) => expr_has_placeholder(inner),
            _ => false,
        }
    }

    /// Deep check for SubqueryPlaceholder in any expression node.
    fn expr_has_placeholder_deep(expr: &TypedExpr) -> bool {
        match &expr.kind {
            ExprKind::SubqueryPlaceholder { .. } => true,
            ExprKind::BinaryOp { left, right, .. } => {
                expr_has_placeholder_deep(left) || expr_has_placeholder_deep(right)
            }
            ExprKind::UnaryOp { expr, .. } => expr_has_placeholder_deep(expr),
            ExprKind::Nested(inner) => expr_has_placeholder_deep(inner),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                args.iter().any(expr_has_placeholder_deep)
            }
            ExprKind::Cast { expr, .. } | ExprKind::IsNull { expr, .. } => {
                expr_has_placeholder_deep(expr)
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|o| expr_has_placeholder_deep(o))
                    || when_then
                        .iter()
                        .any(|(w, t)| expr_has_placeholder_deep(w) || expr_has_placeholder_deep(t))
                    || else_expr
                        .as_ref()
                        .is_some_and(|e| expr_has_placeholder_deep(e))
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                expr_has_placeholder_deep(expr)
                    || expr_has_placeholder_deep(low)
                    || expr_has_placeholder_deep(high)
            }
            ExprKind::Like { expr, pattern, .. } => {
                expr_has_placeholder_deep(expr) || expr_has_placeholder_deep(pattern)
            }
            ExprKind::InList { expr, list, .. } => {
                expr_has_placeholder_deep(expr) || list.iter().any(expr_has_placeholder_deep)
            }
            ExprKind::IsTruthValue { expr, .. } => expr_has_placeholder_deep(expr),
            ExprKind::WindowCall { args, .. } => args.iter().any(expr_has_placeholder_deep),
            _ => false,
        }
    }

    #[test]
    fn test_table_alias_qualified_reference() {
        // Simplified q3 pattern: table alias with qualified reference
        let sql = "SELECT o.o_orderkey FROM orders o WHERE o.o_custkey > 100";
        let resolved = parse_and_analyze(sql).expect("table alias qualified ref should work");
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn test_cte_with_alias() {
        // Simplified q1 pattern: CTE with alias
        let sql = "WITH order_totals AS (SELECT o_orderkey as ok, o_totalprice as total FROM orders) \
                   SELECT t1.ok FROM order_totals t1 WHERE t1.total > 100";
        let resolved = parse_and_analyze(sql).expect("CTE with alias should work");
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn test_cte_qualified_projection_uses_base_column_names() {
        let sql = "WITH joined AS ( \
                     SELECT o1.o_orderkey, o2.o_custkey \
                     FROM orders o1 JOIN orders o2 ON o1.o_orderkey = o2.o_orderkey \
                   ) \
                   SELECT o_orderkey, o_custkey FROM joined";
        let resolved =
            parse_and_analyze(sql).expect("qualified CTE projection should expose base names");
        assert_eq!(resolved.output_columns.len(), 2);
        assert_eq!(resolved.output_columns[0].name, "o_orderkey");
        assert_eq!(resolved.output_columns[1].name, "o_custkey");
    }

    #[test]
    fn test_single_use_cte_is_still_registered() {
        let sql = "WITH order_totals AS (SELECT o_orderkey AS ok FROM orders) \
                   SELECT ok FROM order_totals";
        let (resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        assert_eq!(registry.entries.len(), 1);
        assert_eq!(registry.entries[0].name, "order_totals");
        match &resolved.body {
            QueryBody::Select(sel) => match sel.from.as_ref().expect("should have FROM") {
                Relation::CTEConsume { cte_id, .. } => {
                    let entry = registry
                        .entries
                        .iter()
                        .find(|entry| entry.id == *cte_id)
                        .expect("cte id should exist in registry");
                    assert_eq!(entry.name, "order_totals");
                }
                _ => panic!("expected direct CTEConsume for single-use top-level CTE"),
            },
            _ => panic!("expected select body"),
        }
    }

    #[test]
    fn cte_consume_records_producer_column_ids_parallel_to_consumer_outputs() {
        let sql = "WITH c AS (SELECT o_orderkey AS k, o_custkey AS c FROM orders) \
                   SELECT a.k, a.c FROM c a";
        let (resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        let entry = registry
            .entries
            .iter()
            .find(|entry| entry.name == "c")
            .expect("cte entry");
        let producer_ids: Vec<_> = entry.output_columns.iter().map(|c| c.column_id).collect();

        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        let Relation::CTEConsume {
            output_columns,
            producer_column_ids,
            ..
        } = select.from.as_ref().expect("from relation")
        else {
            panic!("expected direct CTEConsume");
        };

        let consumer_ids: Vec<_> = output_columns.iter().map(|c| c.column_id).collect();
        assert_eq!(producer_column_ids, &producer_ids);
        assert_eq!(producer_column_ids.len(), output_columns.len());
        assert_ne!(
            consumer_ids, producer_ids,
            "consumer aliases must use fresh ColumnIds"
        );
    }

    #[test]
    fn test_forward_cte_reference_is_rejected() {
        let sql = "WITH a AS (SELECT 1 AS x) \
                   SELECT * FROM (\
                     WITH b AS (SELECT * FROM a), a AS (SELECT o_orderkey FROM orders) \
                     SELECT * FROM b\
                   ) s";
        let err = parse_and_analyze_with_registry(sql).expect_err("forward reference must fail");
        assert!(
            err.contains("forward CTE reference is not supported"),
            "err={err}"
        );
    }

    #[test]
    fn test_nested_with_inherits_enclosing_pending_ctes() {
        let sql = "WITH early AS (\
                     WITH nested AS (SELECT * FROM orders) \
                     SELECT * FROM nested\
                   ), orders AS (SELECT 1 AS x) \
                   SELECT * FROM early";
        let err = parse_and_analyze_with_registry(sql).expect_err("forward reference must fail");
        assert!(
            err.contains("forward CTE reference is not supported: orders"),
            "err={err}"
        );
    }

    #[test]
    fn test_inner_cte_shadows_outer_cte() {
        let sql = "WITH t AS (SELECT 1 AS x) \
                   SELECT * FROM (WITH t AS (SELECT 2 AS x) SELECT x FROM t) s";
        let (resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        let cte_id = match &resolved.body {
            QueryBody::Select(sel) => match sel.from.as_ref().expect("should have FROM") {
                Relation::Subquery { query, .. } => match &query.body {
                    QueryBody::Select(inner_sel) => match inner_sel.from.as_ref() {
                        Some(Relation::CTEConsume { cte_id, .. }) => *cte_id,
                        _ => panic!("expected CTEConsume inside shadowing subquery"),
                    },
                    _ => panic!("expected select inside subquery"),
                },
                _ => panic!("expected derived subquery"),
            },
            _ => panic!("expected select body"),
        };
        let entry = registry
            .entries
            .iter()
            .find(|entry| entry.id == cte_id)
            .expect("cte id should exist in registry");
        let inner_value = match &entry.resolved_query.body {
            QueryBody::Select(inner_sel) => match &inner_sel.projection[0].expr.kind {
                ExprKind::Literal(crate::analysis::LiteralValue::Int(v)) => Some(*v),
                _ => None,
            },
            _ => None,
        };
        assert_eq!(inner_value, Some(2));
    }

    #[test]
    fn test_inner_with_does_not_leak_to_sibling_scope() {
        let sql = "WITH t AS (SELECT 1 AS x) \
                   SELECT * FROM (WITH t AS (SELECT 2 AS x) SELECT 1) d, t";
        let (resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        let sibling_cte_id = match &resolved.body {
            QueryBody::Select(sel) => match sel.from.as_ref().expect("should have FROM") {
                Relation::Join(join) => match &join.right {
                    Relation::CTEConsume { cte_id, .. } => *cte_id,
                    _ => panic!("expected sibling CTEConsume"),
                },
                _ => panic!("expected join from comma-separated FROM items"),
            },
            _ => panic!("expected select body"),
        };
        let entry = registry
            .entries
            .iter()
            .find(|entry| entry.id == sibling_cte_id)
            .expect("cte id should exist in registry");
        let outer_value = match &entry.resolved_query.body {
            QueryBody::Select(inner_sel) => match &inner_sel.projection[0].expr.kind {
                ExprKind::Literal(crate::analysis::LiteralValue::Int(v)) => Some(*v),
                _ => None,
            },
            _ => None,
        };
        assert_eq!(outer_value, Some(1));
    }

    #[test]
    fn test_cte_with_comma_join_and_correlated_subquery() {
        // Closer to q1 pattern: CTE with comma-join and correlated subquery
        let sql = "WITH order_totals AS (\
                     SELECT o_orderkey as ok, o_custkey as ck, o_totalprice as total FROM orders\
                   ) \
                   SELECT t1.ok FROM order_totals t1, customer \
                   WHERE t1.total > (\
                     SELECT avg(t2.total) FROM order_totals t2 WHERE t1.ck = t2.ck\
                   ) AND t1.ck = c_custkey";
        let resolved = parse_and_analyze(sql)
            .expect("CTE with comma-join and correlated subquery should work");
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn test_correlated_subquery_with_inner_with_registers_cte() {
        let sql = "SELECT o_orderkey FROM orders o \
                   WHERE EXISTS (\
                     WITH filtered AS (SELECT l_orderkey FROM lineitem) \
                     SELECT 1 FROM filtered WHERE filtered.l_orderkey = o.o_orderkey\
                   )";
        let (_resolved, registry) =
            parse_and_analyze_with_registry(sql).expect("analysis should succeed");
        assert_eq!(registry.entries.len(), 1);
        assert_eq!(registry.entries[0].name, "filtered");
    }

    #[test]
    fn test_comma_join_multiple_aliases() {
        // Simplified q3 pattern: comma-join with multiple table aliases
        let sql = "SELECT o.o_orderkey, l.l_partkey \
                   FROM orders o, lineitem l \
                   WHERE o.o_orderkey = l.l_orderkey \
                   GROUP BY o.o_orderkey, l.l_partkey";
        let resolved =
            parse_and_analyze(sql).expect("comma-join with multiple aliases should work");
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn test_scalar_subquery_in_projection() {
        // q9 pattern: scalar subqueries in projection (SELECT list), not in WHERE
        let sql = "SELECT (SELECT count(*) FROM orders) as total_orders FROM lineitem \
                   WHERE l_orderkey = 1";
        let resolved = parse_and_analyze(sql).expect("scalar subquery in projection should work");
        assert!(!resolved.output_columns.is_empty());
    }

    #[test]
    fn test_case_with_scalar_subqueries_in_projection() {
        // q9 pattern: CASE WHEN with scalar subqueries in projection
        let sql = "SELECT CASE WHEN (SELECT count(*) FROM orders) > 100 \
                          THEN (SELECT avg(o_totalprice) FROM orders) \
                          ELSE (SELECT avg(o_totalprice) FROM orders WHERE o_totalprice > 0) \
                   END as bucket1 \
                   FROM lineitem WHERE l_orderkey = 1";
        let resolved =
            parse_and_analyze(sql).expect("CASE with scalar subqueries in projection should work");
        assert!(!resolved.output_columns.is_empty());
        assert_eq!(
            resolved.output_columns[0].value_type.data_type().clone(),
            arrow::datatypes::DataType::Float64,
            "rewritten CASE scalar-subquery projection should expose the final branch type"
        );
        // Verify that no SubqueryPlaceholder remains in the projection
        if let QueryBody::Select(sel) = &resolved.body {
            for item in &sel.projection {
                assert_eq!(
                    item.expr.value_type.data_type().clone(),
                    arrow::datatypes::DataType::Float64,
                    "projection expression type should be recomputed after placeholder replacement"
                );
                assert!(
                    !expr_has_placeholder_deep(&item.expr),
                    "projection should not contain SubqueryPlaceholder after rewriting: {:?}",
                    item.expr
                );
            }
        }
    }

    #[test]
    fn test_percentile_approx_with_array_literal_returns_array_type() {
        let resolved = parse_raw_and_analyze(
            "SELECT percentile_approx(o_totalprice, array<double>[0.25, 0.5]) FROM orders",
        )
        .expect("analysis should succeed");
        assert_eq!(resolved.output_columns.len(), 1);
        assert!(matches!(
            resolved.output_columns[0].value_type.data_type(),
            arrow::datatypes::DataType::List(_)
        ));
    }

    #[test]
    fn test_percentile_approx_weighted_rejects_null_weight_in_analyzer() {
        let err = parse_raw_and_analyze("SELECT percentile_approx_weighted(1, NULL, 0.9)")
            .expect_err("analysis should reject NULL weight");
        assert!(err.contains(
            "percentile_approx_weighted requires the second parameter (weight) to be numeric type, but got: NULL_TYPE."
        ));
    }

    #[test]
    fn test_percentile_approx_weighted_rejects_negative_scalar_percentile_in_analyzer() {
        let err = parse_raw_and_analyze("SELECT percentile_approx_weighted(1, 1, -0.1)")
            .expect_err("analysis should reject negative percentile");
        assert!(err.contains(
            "Type check failed. percentile parameter must be between 0 and 1 in percentile_approx_weighted, but got: -0.1"
        ));
    }

    #[test]
    fn test_percentile_approx_weighted_rejects_negative_array_percentile_in_analyzer() {
        let err = parse_raw_and_analyze("SELECT percentile_approx_weighted(1, 1, [-0.1, 0.5])")
            .expect_err("analysis should reject negative percentile array item");
        assert!(err.contains(
            "Type check failed. percentile array element[0] must be between 0 and 1 in percentile_approx_weighted, but got: -0.1"
        ));
    }

    #[test]
    fn test_group_concat_rejects_negative_order_by_position() {
        let err = parse_raw_and_analyze(
            "SELECT group_concat(distinct 3.1323, o_orderstatus order by 1, 2, -20) FROM orders GROUP BY o_orderkey",
        )
        .expect_err("negative group_concat ORDER BY position should be rejected");
        assert!(
            err.contains("ORDER BY position -20 is not in group_concat output list."),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_group_concat_rejects_empty_input() {
        let err = parse_raw_and_analyze("SELECT group_concat()")
            .expect_err("group_concat without input should be rejected");
        assert!(
            err.contains("group_concat should have at least one input."),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_group_concat_rejects_non_string_separator() {
        let err = parse_raw_and_analyze(
            "SELECT group_concat(\"中国\" ORDER BY \"第一\" SEPARATOR 1) FROM orders",
        )
        .expect_err("group_concat should reject non-string separator");
        assert!(
            err.contains(
                "group_concat requires separator to be of getType() STRING: group_concat('中国', 1)."
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_group_concat_rejects_array_input() {
        let err = parse_raw_and_analyze("SELECT group_concat([1,2]) FROM orders")
            .expect_err("group_concat should reject array input");
        assert!(
            err.contains(
                "No matching function with signature: group_concat(array<int(11)>, varchar)."
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_group_concat_rejects_map_input() {
        let err = parse_raw_and_analyze("SELECT group_concat(map(2,3)) FROM orders")
            .expect_err("group_concat should reject map input");
        assert!(
            err.contains(
                "No matching function with signature: group_concat(map<int(11),int(11)>, varchar)."
            ),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_array_map_lambda_cast_param_resolves_lambda_scope() {
        let resolved = parse_raw_and_analyze(
            "SELECT array_map(x -> CAST(x AS STRING), array_generate(1, 3, 1))",
        )
        .expect("array_map lambda parameter should resolve in lambda scope");
        assert_eq!(resolved.output_columns.len(), 1);
        match &resolved.output_columns[0].value_type.data_type() {
            arrow::datatypes::DataType::List(item) => {
                assert!(matches!(item.data_type(), arrow::datatypes::DataType::Utf8));
            }
            other => panic!("expected ARRAY<VARCHAR>, got {other:?}"),
        }
    }

    #[test]
    fn test_array_map_multi_param_lambda_analyzes() {
        let resolved = parse_raw_and_analyze("SELECT array_map((x, y) -> x + y, [1,2], [3,4])")
            .expect("array_map should analyze multi-parameter lambda");
        assert_eq!(resolved.output_columns.len(), 1);
        match &resolved.output_columns[0].value_type.data_type() {
            arrow::datatypes::DataType::List(item) => {
                // Ordinary integer array elements use the same INTEGER
                // declaration as scalar syntax. Lambda binding consumes it.
                assert!(
                    matches!(
                        item.data_type(),
                        arrow::datatypes::DataType::Int8
                            | arrow::datatypes::DataType::Int16
                            | arrow::datatypes::DataType::Int32
                            | arrow::datatypes::DataType::Int64
                    ),
                    "expected integer element type, got {:?}",
                    item.data_type()
                );
            }
            other => panic!("expected ARRAY<integer>, got {other:?}"),
        }
    }

    #[test]
    fn test_array_ordering_comparison_is_rejected() {
        let err = parse_raw_and_analyze("SELECT i_1 > [1, 2] FROM array_test")
            .expect_err("array ordering comparison should be rejected");
        assert!(
            err.contains("does not support binary predicate operation on ARRAY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn in_predicate_apply_rejects_scalar_vs_array_key() {
        let err = parse_raw_and_analyze(
            "SELECT 1 FROM array_test s WHERE 1 NOT IN (SELECT i_1 FROM array_test t)",
        )
        .expect_err("scalar-vs-array IN subquery should be rejected before planning");
        assert!(
            err.contains("does not support binary predicate operation between"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_time_slice_interval_literal_analyzes() {
        let resolved = parse_raw_and_analyze(
            "SELECT time_slice('9999-12-31 23:59:59', interval 5 year, ceil)",
        )
        .expect("time_slice interval literal should analyze");
        assert_eq!(resolved.output_columns.len(), 1);
    }

    #[test]
    fn test_any_match_lambda_analyzes_as_boolean() {
        let resolved = parse_raw_and_analyze("SELECT any_match(x -> x < 10, [1,2])")
            .expect("any_match should analyze lambda predicate");
        assert_eq!(resolved.output_columns.len(), 1);
        assert!(matches!(
            resolved.output_columns[0].value_type.data_type(),
            arrow::datatypes::DataType::Boolean
        ));
    }

    #[test]
    fn select_alias_is_visible_to_later_projection_expression() {
        let resolved = parse_raw_and_analyze("SELECT 1 AS l, l + 1")
            .expect("later projection item should resolve earlier alias");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        assert_eq!(sel.projection.len(), 2);
        let ExprKind::BinaryOp { left, .. } = &sel.projection[1].expr.kind else {
            panic!("expected binary op in second projection");
        };
        let ExprKind::Literal(crate::analysis::LiteralValue::Int(value)) = &left.kind else {
            panic!("expected alias to be substituted with original literal");
        };
        assert_eq!(*value, 1);
    }

    #[test]
    fn select_alias_is_visible_inside_lambda_body() {
        parse_raw_and_analyze("SELECT 'x' AS l, array_map(arg -> concat(arg, l), ['a'])")
            .expect("lambda body should resolve earlier select alias");
    }

    #[test]
    fn select_alias_without_as_is_visible_inside_lambda_body() {
        parse_raw_and_analyze(
            "SELECT cast(if (1 > rand(), '[]', '') as array<string>) l, \
             array_map(x -> concat(x, l), ['a'])",
        )
        .expect("lambda body should resolve earlier select alias without AS");
    }

    #[test]
    fn select_alias_inside_lambda_body_is_fully_substituted() {
        fn contains_unresolved_l(expr: &TypedExpr) -> bool {
            match &expr.kind {
                ExprKind::ColumnRef {
                    qualifier, column, ..
                } => qualifier.is_none() && column.eq_ignore_ascii_case("l"),
                ExprKind::BinaryOp { left, right, .. } => {
                    contains_unresolved_l(left) || contains_unresolved_l(right)
                }
                ExprKind::UnaryOp { expr, .. }
                | ExprKind::Cast { expr, .. }
                | ExprKind::Nested(expr)
                | ExprKind::IsNull { expr, .. }
                | ExprKind::IsTruthValue { expr, .. } => contains_unresolved_l(expr),
                ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                    args.iter().any(contains_unresolved_l)
                }
                ExprKind::LambdaFunction { body, .. } | ExprKind::Lambda { body, .. } => {
                    contains_unresolved_l(body)
                }
                ExprKind::Case {
                    operand,
                    when_then,
                    else_expr,
                } => {
                    operand
                        .as_ref()
                        .is_some_and(|expr| contains_unresolved_l(expr))
                        || when_then.iter().any(|(when, then)| {
                            contains_unresolved_l(when) || contains_unresolved_l(then)
                        })
                        || else_expr
                            .as_ref()
                            .is_some_and(|expr| contains_unresolved_l(expr))
                }
                _ => false,
            }
        }

        let resolved = parse_raw_and_analyze(
            "SELECT cast(if (1 > rand(), '[]', '') as array<string>) l, \
             array_map(x -> concat(x, l), ['a'])",
        )
        .expect("analysis should succeed");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        assert_eq!(sel.projection.len(), 2);
        assert!(
            !contains_unresolved_l(&sel.projection[1].expr),
            "projection still contains unresolved alias reference: {:?}",
            sel.projection[1].expr
        );
    }

    #[test]
    fn test_group_by_rollup() {
        // q5 pattern: GROUP BY ROLLUP(a, b)
        let sql = "SELECT o_orderstatus, o_orderpriority, count(*) as cnt \
                   FROM orders \
                   GROUP BY ROLLUP(o_orderstatus, o_orderpriority)";
        let resolved = parse_and_analyze(sql).expect("GROUP BY ROLLUP should work");
        assert!(!resolved.output_columns.is_empty());
        // ROLLUP(a, b) should produce a single Select with RepeatInfo containing
        // 3 levels: (a,b), (a), ()
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("ROLLUP should produce RepeatInfo");
            // 3 levels for 2 rollup dimensions
            assert_eq!(
                repeat.repeat_column_ref_list.len(),
                3,
                "ROLLUP(a,b) should have 3 levels"
            );
            // Level 0: both active
            assert_eq!(repeat.repeat_column_ref_list[0].len(), 2);
            // Level 1: only first active
            assert_eq!(repeat.repeat_column_ref_list[1].len(), 1);
            // Level 2: none active
            assert_eq!(repeat.repeat_column_ref_list[2].len(), 0);

            // Grouping IDs
            assert_eq!(repeat.grouping_ids[0], 0b00); // both active
            assert_eq!(repeat.grouping_ids[1], 0b01); // b NULLed (least-significant bit)
            assert_eq!(repeat.grouping_ids[2], 0b11); // both NULLed

            // All rollup columns
            assert_eq!(repeat.all_rollup_columns.len(), 2);
        } else {
            panic!(
                "ROLLUP should produce a Select with RepeatInfo, got: {:?}",
                std::mem::discriminant(&resolved.body)
            );
        }
    }

    #[test]
    fn test_group_by_rollup_output_columns() {
        // Verify ROLLUP preserves output column structure
        let sql = "SELECT o_orderstatus as status, count(*) as cnt \
                   FROM orders \
                   GROUP BY ROLLUP(o_orderstatus)";
        let resolved = parse_and_analyze(sql).expect("GROUP BY ROLLUP should work");
        assert_eq!(resolved.output_columns.len(), 2);
        assert_eq!(resolved.output_columns[0].name, "status");
        assert_eq!(resolved.output_columns[1].name, "cnt");
        // Also verify RepeatInfo is present
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("ROLLUP should produce RepeatInfo");
            assert_eq!(
                repeat.repeat_column_ref_list.len(),
                2,
                "ROLLUP(a) should have 2 levels"
            );
            assert_eq!(repeat.grouping_ids[0], 0b0); // active
            assert_eq!(repeat.grouping_ids[1], 0b1); // NULLed
        } else {
            panic!("expected Select body with RepeatInfo");
        }
    }

    #[test]
    fn test_group_by_rollup_with_grouping() {
        // Verify GROUPING() function calls are captured in RepeatInfo
        let sql = "SELECT o_orderstatus, grouping(o_orderstatus) as g_status, count(*) as cnt \
                   FROM orders \
                   GROUP BY ROLLUP(o_orderstatus)";
        let resolved = parse_and_analyze(sql).expect("GROUP BY ROLLUP with GROUPING should work");
        assert_eq!(resolved.output_columns.len(), 3);
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("ROLLUP should produce RepeatInfo");
            // GROUPING(o_orderstatus) should be recorded. The name is an internal
            // placeholder (__grouping_fn_N) rather than the user alias — the alias
            // mapping is handled later by the planner when constructing PlanRepeatNode.
            assert_eq!(repeat.grouping_fn_args.len(), 1);
            assert_eq!(repeat.grouping_fn_args[0].0, "__grouping_fn_0");
            assert_eq!(repeat.grouping_fn_args[0].1, vec!["o_orderstatus"]);
        } else {
            panic!("expected Select body with RepeatInfo");
        }
    }

    fn grouping_fn_ids(
        resolved: &ResolvedQuery,
    ) -> (crate::column_id::ColumnId, crate::column_id::ColumnId) {
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };

        let proj_id = sel
            .projection
            .iter()
            .find_map(|item| find_grouping_ref_id(&item.expr, "__grouping_fn_0"))
            .expect("expected projection grouping ColumnRef under its public cast");
        let gb_id = sel
            .group_by
            .iter()
            .find_map(|expr| match &expr.kind {
                ExprKind::ColumnRef {
                    column_id, column, ..
                } if column == "__grouping_fn_0" => Some(*column_id),
                _ => None,
            })
            .expect("expected group-by grouping ColumnRef");

        (proj_id, gb_id)
    }

    fn grouping_fn_group_by_id(sel: &ResolvedSelect, name: &str) -> crate::column_id::ColumnId {
        sel.group_by
            .iter()
            .find_map(|expr| match &expr.kind {
                ExprKind::ColumnRef {
                    column_id, column, ..
                } if column == name => Some(*column_id),
                _ => None,
            })
            .expect("expected group-by grouping ColumnRef")
    }

    fn find_grouping_ref_id(expr: &TypedExpr, name: &str) -> Option<crate::column_id::ColumnId> {
        match &expr.kind {
            ExprKind::ColumnRef {
                column_id, column, ..
            } if column == name => Some(*column_id),
            ExprKind::BinaryOp { left, right, .. } => {
                find_grouping_ref_id(left, name).or_else(|| find_grouping_ref_id(right, name))
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr)
            | ExprKind::LambdaFunction { body: expr, .. }
            | ExprKind::Lambda { body: expr, .. } => find_grouping_ref_id(expr, name),
            ExprKind::FunctionCall { args, .. } => {
                args.iter().find_map(|arg| find_grouping_ref_id(arg, name))
            }
            ExprKind::AggregateCall { args, order_by, .. } => args
                .iter()
                .find_map(|arg| find_grouping_ref_id(arg, name))
                .or_else(|| {
                    order_by
                        .iter()
                        .find_map(|item| find_grouping_ref_id(&item.expr, name))
                }),
            ExprKind::InList { expr, list, .. } => find_grouping_ref_id(expr, name).or_else(|| {
                list.iter()
                    .find_map(|item| find_grouping_ref_id(item, name))
            }),
            ExprKind::Between {
                expr, low, high, ..
            } => find_grouping_ref_id(expr, name)
                .or_else(|| find_grouping_ref_id(low, name))
                .or_else(|| find_grouping_ref_id(high, name)),
            ExprKind::Like { expr, pattern, .. } => {
                find_grouping_ref_id(expr, name).or_else(|| find_grouping_ref_id(pattern, name))
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => operand
                .as_ref()
                .and_then(|expr| find_grouping_ref_id(expr, name))
                .or_else(|| {
                    when_then.iter().find_map(|(when, then)| {
                        find_grouping_ref_id(when, name)
                            .or_else(|| find_grouping_ref_id(then, name))
                    })
                })
                .or_else(|| {
                    else_expr
                        .as_ref()
                        .and_then(|expr| find_grouping_ref_id(expr, name))
                }),
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => args
                .iter()
                .find_map(|arg| find_grouping_ref_id(arg, name))
                .or_else(|| {
                    partition_by
                        .iter()
                        .find_map(|expr| find_grouping_ref_id(expr, name))
                })
                .or_else(|| {
                    order_by
                        .iter()
                        .find_map(|item| find_grouping_ref_id(&item.expr, name))
                }),
            ExprKind::ColumnRef { .. }
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::Constant(_)
            | ExprKind::SubqueryPlaceholder { .. } => None,
        }
    }

    fn expr_contains_column_id(expr: &TypedExpr, target: crate::column_id::ColumnId) -> bool {
        match &expr.kind {
            ExprKind::ColumnRef { column_id, .. } => *column_id == target,
            ExprKind::BinaryOp { left, right, .. } => {
                expr_contains_column_id(left, target) || expr_contains_column_id(right, target)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr)
            | ExprKind::LambdaFunction { body: expr, .. }
            | ExprKind::Lambda { body: expr, .. } => expr_contains_column_id(expr, target),
            ExprKind::FunctionCall { args, .. } => {
                args.iter().any(|arg| expr_contains_column_id(arg, target))
            }
            ExprKind::AggregateCall { args, order_by, .. } => {
                args.iter().any(|arg| expr_contains_column_id(arg, target))
                    || order_by
                        .iter()
                        .any(|item| expr_contains_column_id(&item.expr, target))
            }
            ExprKind::InList { expr, list, .. } => {
                expr_contains_column_id(expr, target)
                    || list
                        .iter()
                        .any(|item| expr_contains_column_id(item, target))
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                expr_contains_column_id(expr, target)
                    || expr_contains_column_id(low, target)
                    || expr_contains_column_id(high, target)
            }
            ExprKind::Like { expr, pattern, .. } => {
                expr_contains_column_id(expr, target) || expr_contains_column_id(pattern, target)
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| expr_contains_column_id(expr, target))
                    || when_then.iter().any(|(when, then)| {
                        expr_contains_column_id(when, target)
                            || expr_contains_column_id(then, target)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|expr| expr_contains_column_id(expr, target))
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                args.iter().any(|arg| expr_contains_column_id(arg, target))
                    || partition_by
                        .iter()
                        .any(|expr| expr_contains_column_id(expr, target))
                    || order_by
                        .iter()
                        .any(|item| expr_contains_column_id(&item.expr, target))
            }
            ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::Constant(_)
            | ExprKind::SubqueryPlaceholder { .. } => false,
        }
    }

    fn contains_grouping_marker_literal(expr: &TypedExpr) -> bool {
        match &expr.kind {
            ExprKind::Literal(crate::analysis::LiteralValue::Int(v)) if *v <= -9000 => true,
            ExprKind::BinaryOp { left, right, .. } => {
                contains_grouping_marker_literal(left) || contains_grouping_marker_literal(right)
            }
            ExprKind::UnaryOp { expr, .. }
            | ExprKind::Cast { expr, .. }
            | ExprKind::IsNull { expr, .. }
            | ExprKind::IsTruthValue { expr, .. }
            | ExprKind::Nested(expr)
            | ExprKind::LambdaFunction { body: expr, .. }
            | ExprKind::Lambda { body: expr, .. } => contains_grouping_marker_literal(expr),
            ExprKind::FunctionCall { args, .. } => {
                args.iter().any(contains_grouping_marker_literal)
            }
            ExprKind::AggregateCall { args, order_by, .. } => {
                args.iter().any(contains_grouping_marker_literal)
                    || order_by
                        .iter()
                        .any(|item| contains_grouping_marker_literal(&item.expr))
            }
            ExprKind::InList { expr, list, .. } => {
                contains_grouping_marker_literal(expr)
                    || list.iter().any(contains_grouping_marker_literal)
            }
            ExprKind::Between {
                expr, low, high, ..
            } => {
                contains_grouping_marker_literal(expr)
                    || contains_grouping_marker_literal(low)
                    || contains_grouping_marker_literal(high)
            }
            ExprKind::Like { expr, pattern, .. } => {
                contains_grouping_marker_literal(expr) || contains_grouping_marker_literal(pattern)
            }
            ExprKind::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|expr| contains_grouping_marker_literal(expr))
                    || when_then.iter().any(|(when, then)| {
                        contains_grouping_marker_literal(when)
                            || contains_grouping_marker_literal(then)
                    })
                    || else_expr
                        .as_ref()
                        .is_some_and(|expr| contains_grouping_marker_literal(expr))
            }
            ExprKind::WindowCall {
                args,
                partition_by,
                order_by,
                ..
            } => {
                args.iter().any(contains_grouping_marker_literal)
                    || partition_by.iter().any(contains_grouping_marker_literal)
                    || order_by
                        .iter()
                        .any(|item| contains_grouping_marker_literal(&item.expr))
            }
            ExprKind::ColumnRef { .. }
            | ExprKind::LambdaParamRef { .. }
            | ExprKind::Literal(_)
            | ExprKind::Constant(_)
            | ExprKind::SubqueryPlaceholder { .. } => false,
        }
    }

    fn relation_contains_grouping_marker_literal(rel: &Relation) -> bool {
        match rel {
            Relation::Join(join) => {
                join.condition
                    .as_ref()
                    .is_some_and(contains_grouping_marker_literal)
                    || relation_contains_grouping_marker_literal(&join.left)
                    || relation_contains_grouping_marker_literal(&join.right)
            }
            Relation::Unnest(unnest) => unnest.args.iter().any(contains_grouping_marker_literal),
            Relation::Scan(_)
            | Relation::IcebergMetadataScan(_)
            | Relation::IcebergDeltaScan(_)
            | Relation::Subquery { .. }
            | Relation::GenerateSeries(_)
            | Relation::CTEConsume { .. } => false,
        }
    }

    fn outer_select_contains_grouping_marker_literal(sel: &ResolvedSelect) -> bool {
        sel.projection
            .iter()
            .any(|item| contains_grouping_marker_literal(&item.expr))
            || sel
                .filter
                .as_ref()
                .is_some_and(contains_grouping_marker_literal)
            || sel
                .having
                .as_ref()
                .is_some_and(contains_grouping_marker_literal)
            || sel
                .from
                .as_ref()
                .is_some_and(relation_contains_grouping_marker_literal)
    }

    #[test]
    fn grouping_public_width_is_bound_before_parent_expressions() {
        for group_by in ["GROUP BY a", "GROUP BY ROLLUP(a)"] {
            for (count, carrier) in [
                (1, DataType::Int32),
                (31, DataType::Int32),
                (32, DataType::Int64),
                (63, DataType::Int64),
            ] {
                let args = vec!["a"; count].join(",");
                let sql = format!(
                    "SELECT grouping({args}), grouping_id({args}), abs(grouping({args})), grouping({args}) + CAST(1 AS INTEGER) FROM (VALUES (CAST(1 AS INTEGER))) t(a) {group_by}"
                );
                let resolved = parse_raw_and_analyze(&sql).unwrap();
                assert_eq!(resolved.output_columns[0].field_name, None);
                assert_eq!(
                    resolved.output_columns[0].value_type.data_type(),
                    &carrier,
                    "{sql}"
                );
                assert_eq!(
                    resolved.output_columns[1].value_type.data_type(),
                    &DataType::Int64
                );
                let QueryBody::Select(select) = &resolved.body else {
                    panic!("select");
                };
                let ExprKind::FunctionCall { args, binding, .. } = &select.projection[2].expr.kind
                else {
                    panic!("bound ABS");
                };
                assert_eq!(
                    args[0].value_type.data_type(),
                    &carrier,
                    "ABS receives the actual public GROUPING type: {sql}"
                );
                let novarocks_functions::FunctionArgumentType::Value(argument) =
                    &binding.selected.argument_types[0]
                else {
                    panic!("value argument");
                };
                assert_eq!(argument.data_type(), &carrier);
                assert_eq!(
                    resolved.output_columns[2].value_type.data_type(),
                    if carrier == DataType::Int32 {
                        &DataType::Int64
                    } else {
                        &DataType::FixedSizeBinary(16)
                    },
                    "existing ABS promotion remains unchanged"
                );
                assert_eq!(
                    resolved.output_columns[3].value_type.data_type(),
                    &carrier,
                    "parent add: {sql}"
                );
                let QueryBody::Select(select) = resolved.body else {
                    panic!("select");
                };
                if group_by.contains("ROLLUP") {
                    for (name, id) in &select.repeat.as_ref().unwrap().grouping_fn_ids {
                        let mask = select.group_by.iter().find(|key| matches!(&key.kind, ExprKind::ColumnRef { column_id, .. } if column_id == id)).unwrap();
                        assert_eq!(mask.value_type.data_type(), &DataType::Int64, "{name}");
                        assert!(!mask.value_type.nullable());
                    }
                    if count <= 31 {
                        assert!(
                            matches!(&select.projection[0].expr.kind, ExprKind::Cast { expr, .. } if matches!(expr.kind, ExprKind::ColumnRef { .. }))
                        );
                        assert!(
                            select.projection[0].expr.value_type.nullable(),
                            "original narrowing CAST NULL law"
                        );
                    }
                } else {
                    assert!(matches!(
                        select.projection[0].expr.kind,
                        ExprKind::Literal(crate::analysis::LiteralValue::Int(0))
                    ));
                    assert!(!select.projection[0].expr.value_type.nullable());
                }
            }
        }
    }

    #[test]
    fn grouping_arity_error_and_query_boundaries_are_original_and_local() {
        let catalog = crate::catalog::memory::PlannerMemoryCatalog::default();
        let args = vec!["missing"; 64].join(",");
        let sql = format!("SELECT grouping({args}) FROM (VALUES (1)) t(a) GROUP BY a");
        let statements = novarocks_parser::parse(&sql).unwrap();
        let ast::Statement::Query(query) = &statements[0] else {
            panic!("query");
        };
        let error = analyze(query, &catalog, "default").unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::TooManyArguments
        );
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            panic!("select");
        };
        assert_eq!(error.span(), Some(select.projection[0].span()));
        assert_eq!(error.code().as_str(), "sql.analyze.too_many_arguments");
        for sql in [
            "SELECT grouping(a), (SELECT grouping(a)) FROM (VALUES (1)) t(a) GROUP BY ROLLUP(a)",
            "SELECT grouping(a) FROM (VALUES (1)) t(a) WHERE grouping(a)=0 GROUP BY ROLLUP(a)",
            "SELECT grouping(a) FROM (VALUES (1)) t(a) GROUP BY grouping(a)",
            "SELECT a FROM (VALUES (1)) t(a) GROUP BY grouping(a)",
            "SELECT (SELECT grouping(b) FROM (VALUES (1)) s(b)), grouping(a) FROM (VALUES (1)) t(a) GROUP BY ROLLUP(a)",
        ] {
            assert!(
                parse_raw_and_analyze(sql).is_err(),
                "must not borrow GROUPING qualification: {sql}"
            );
        }
        let resolved = parse_raw_and_analyze(
            "WITH c(g) AS (SELECT grouping(a) FROM (VALUES (1)) t(a) GROUP BY a) SELECT g FROM c",
        )
        .unwrap();
        assert_eq!(
            resolved.output_columns[0].value_type.data_type(),
            &DataType::Int32
        );
        for sql in [
            "SELECT a, grouping(a) FROM (VALUES (1)) t(a) GROUP BY 1",
            "SELECT a AS x, grouping(a) FROM (VALUES (1)) t(a) GROUP BY x",
        ] {
            assert!(
                parse_raw_and_analyze(sql).is_ok(),
                "original GROUP BY author: {sql}"
            );
        }
        // GROUPING_ID keeps its existing Nova BIGINT extension, including 64 occurrences.
        let args = vec!["a"; 64].join(",");
        let resolved = parse_raw_and_analyze(&format!(
            "SELECT grouping_id({args}) FROM (VALUES (1)) t(a) GROUP BY ROLLUP(a)"
        ))
        .unwrap();
        assert_eq!(
            resolved.output_columns[0].value_type.data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn grouping_order_by_late_call_keeps_distinct_default_mask_and_user_literal() {
        let resolved = parse_raw_and_analyze(
            "SELECT -9000, a FROM (VALUES (1)) t(a) GROUP BY ROLLUP(a) ORDER BY grouping(a)",
        )
        .unwrap();
        let QueryBody::Select(select) = &resolved.body else {
            panic!("select");
        };
        let repeat = select.repeat.as_ref().unwrap();
        assert_eq!(repeat.grouping_fn_ids.len(), 2);
        assert_ne!(repeat.grouping_fn_ids[0].0, repeat.grouping_fn_ids[1].0);
        assert_ne!(repeat.grouping_fn_ids[0].1, repeat.grouping_fn_ids[1].1);
        assert!(find_grouping_ref_id(&resolved.order_by[0].expr, "__grouping_fn_1").is_some());
        assert!(matches!(
            select.projection[0].expr.kind,
            ExprKind::Literal(crate::analysis::LiteralValue::Int(-9000))
        ));
        let resolved = parse_raw_and_analyze(
            "SELECT -9000, grouping(a) FROM (VALUES (1)) t(a) GROUP BY ROLLUP(a)",
        )
        .unwrap();
        let QueryBody::Select(select) = resolved.body else {
            panic!("select");
        };
        assert!(matches!(
            select.projection[0].expr.kind,
            ExprKind::Literal(crate::analysis::LiteralValue::Int(-9000))
        ));
        assert!(find_grouping_ref_id(&select.projection[1].expr, "__grouping_fn_0").is_some());
    }

    #[test]
    fn p1_grouping_marker_carries_group_by_id() {
        let resolved =
            parse_and_analyze("SELECT k1, grouping(k1) AS g FROM t1 GROUP BY ROLLUP(k1)")
                .expect("GROUP BY ROLLUP with GROUPING should work");
        let (proj_id, gb_id) = grouping_fn_ids(&resolved);
        assert_ne!(proj_id, crate::column_id::ColumnId::UNSET);
        assert_eq!(
            proj_id, gb_id,
            "projection grouping ref must reuse the group-by key id"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_in_having() {
        let resolved = parse_and_analyze(
            "SELECT k1, count(*) AS c FROM t1 GROUP BY ROLLUP(k1) HAVING grouping(k1) = 0",
        )
        .expect("GROUP BY ROLLUP with GROUPING in HAVING should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let having = sel.having.as_ref().expect("expected HAVING expression");
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let having_id = find_grouping_ref_id(having, "__grouping_fn_0")
            .expect("expected HAVING grouping ColumnRef");
        assert_eq!(
            having_id, gb_id,
            "HAVING grouping ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(having),
            "HAVING must not retain grouping marker literal: {having:?}"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_inside_nested_projection() {
        let resolved =
            parse_and_analyze("SELECT abs(grouping(k1)) AS g FROM t1 GROUP BY ROLLUP(k1)")
                .expect("nested GROUPING projection should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let projection = &sel.projection[0].expr;
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let proj_id = find_grouping_ref_id(projection, "__grouping_fn_0")
            .expect("expected nested projection grouping ColumnRef");
        assert_eq!(
            proj_id, gb_id,
            "nested projection grouping ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(projection),
            "nested projection must not retain grouping marker literal: {projection:?}"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_inside_cast_projection() {
        let resolved = parse_and_analyze(
            "SELECT CAST(grouping(k1) AS BIGINT) AS g FROM t1 GROUP BY ROLLUP(k1)",
        )
        .expect("CAST around GROUPING projection should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let projection = &sel.projection[0].expr;
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let proj_id = find_grouping_ref_id(projection, "__grouping_fn_0")
            .expect("expected cast projection grouping ColumnRef");
        assert_eq!(
            proj_id, gb_id,
            "cast projection grouping ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(projection),
            "cast projection must not retain grouping marker literal: {projection:?}"
        );
    }

    #[test]
    fn p1_grouping_id_marker_replaced_inside_nested_projection() {
        let resolved = parse_and_analyze(
            "SELECT abs(grouping_id(k1, k2)) AS gid \
             FROM t1 \
             GROUP BY GROUPING SETS ((k1, k2), ())",
        )
        .expect("nested GROUPING_ID projection should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let projection = &sel.projection[0].expr;
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let proj_id = find_grouping_ref_id(projection, "__grouping_fn_0")
            .expect("expected nested projection grouping_id ColumnRef");
        assert_eq!(
            proj_id, gb_id,
            "nested projection grouping_id ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(projection),
            "nested grouping_id projection must not retain grouping marker literal: {projection:?}"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_in_having_in_list() {
        let resolved =
            parse_and_analyze("SELECT k1 FROM t1 GROUP BY ROLLUP(k1) HAVING grouping(k1) IN (0)")
                .expect("GROUPING in HAVING IN list should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let having = sel.having.as_ref().expect("expected HAVING expression");
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let having_id = find_grouping_ref_id(having, "__grouping_fn_0")
            .expect("expected HAVING IN grouping ColumnRef");
        assert_eq!(
            having_id, gb_id,
            "HAVING IN grouping ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(having),
            "HAVING IN must not retain grouping marker literal: {having:?}"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_in_aggregate_order_by() {
        let resolved = parse_and_analyze(
            "SELECT array_agg(v1 ORDER BY grouping(k1) + 0) AS g \
             FROM t1 GROUP BY ROLLUP(k1)",
        )
        .expect("GROUPING in aggregate ORDER BY should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let projection = &sel.projection[0].expr;
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let proj_id = find_grouping_ref_id(projection, "__grouping_fn_0")
            .expect("expected aggregate ORDER BY grouping ColumnRef");
        assert_eq!(
            proj_id, gb_id,
            "aggregate ORDER BY grouping ref must reuse the group-by key id"
        );
        assert!(
            !contains_grouping_marker_literal(projection),
            "aggregate ORDER BY must not retain grouping marker literal: {projection:?}"
        );
    }

    #[test]
    fn p1_grouping_marker_replaced_in_having_in_subquery_join_condition() {
        let resolved = parse_and_analyze(
            "SELECT k1 FROM t1 \
             GROUP BY ROLLUP(k1) \
             HAVING grouping(k1) IN (SELECT k2 FROM t2)",
        )
        .expect("GROUPING in HAVING IN subquery should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let spec = only_predicate_spec(sel);
        let gb_id = grouping_fn_group_by_id(sel, "__grouping_fn_0");
        let lhs = spec
            .in_lhs
            .as_ref()
            .expect("HAVING IN Apply spec should retain analyzed LHS");
        assert!(
            expr_contains_column_id(lhs, gb_id),
            "Apply IN LHS grouping ref must reuse the group-by key id"
        );
        assert!(
            !outer_select_contains_grouping_marker_literal(sel),
            "outer select must not retain grouping marker literal: {sel:?}"
        );
    }

    #[test]
    fn p1_synthetic_grouping_does_not_replace_real_marker_literal() {
        let resolved = parse_and_analyze("SELECT -9000 AS marker, k1 FROM t1 GROUP BY ROLLUP(k1)")
            .expect("ROLLUP with real -9000 literal should work");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        assert!(
            sel.repeat
                .as_ref()
                .expect("ROLLUP should produce RepeatInfo")
                .grouping_fn_args
                .iter()
                .any(|(name, _)| name == "__grouping_fn_0"),
            "ROLLUP without explicit GROUPING should still synthesize grouping_fn"
        );
        assert!(
            find_grouping_ref_id(&sel.projection[0].expr, "__grouping_fn_0").is_none(),
            "real -9000 literal must not be rewritten to synthetic grouping ColumnRef"
        );
        match &sel.projection[0].expr.kind {
            ExprKind::Literal(crate::analysis::LiteralValue::Int(v)) => assert_eq!(*v, -9000),
            ExprKind::UnaryOp { expr, .. } => match &expr.kind {
                ExprKind::Literal(crate::analysis::LiteralValue::Int(v)) => {
                    assert_eq!(*v, 9000)
                }
                other => panic!("expected numeric literal under unary op, got {other:?}"),
            },
            other => panic!("expected real -9000 literal to remain literal, got {other:?}"),
        }
    }

    #[test]
    fn test_group_by_grouping_sets_single_column_resolves() {
        // Reduced from tests/sql/correctness/aggregate/agg_grouping_sets_v1 query 5:
        //     GROUP BY GROUPING SETS((), (k1))
        // Failed in standalone server with "Column 'k1' cannot be resolved.".
        let sql = "SELECT o_orderstatus, count(*) AS cnt \
                   FROM orders \
                   GROUP BY GROUPING SETS((), (o_orderstatus))";
        let resolved =
            parse_and_analyze(sql).expect("GROUPING SETS with single-column set should analyze");
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("GROUPING SETS should produce RepeatInfo");
            assert_eq!(repeat.repeat_column_ref_list.len(), 2);
        } else {
            panic!("expected Select body with RepeatInfo");
        }
    }

    #[test]
    fn test_group_by_grouping_sets_single_column_via_starrocks_dialect() {
        // Same as the test above but uses the StarRocks dialect via
        // parse_and_analyze_with_registry — the production parse path.
        let sql = "SELECT o_orderstatus, count(*) AS cnt \
                   FROM orders \
                   GROUP BY GROUPING SETS((), (o_orderstatus))";
        let (resolved, _) = parse_and_analyze_with_registry(sql)
            .expect("GROUPING SETS via StarRocks dialect should analyze");
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("GROUPING SETS should produce RepeatInfo");
            assert_eq!(repeat.repeat_column_ref_list.len(), 2);
        } else {
            panic!("expected Select body with RepeatInfo");
        }
    }

    #[test]
    fn test_group_by_grouping_sets_with_grouping_id() {
        let sql = "SELECT o_orderstatus, o_orderpriority, \
                          grouping_id(o_orderstatus, o_orderpriority) as gid, \
                          grouping(o_orderstatus, o_orderpriority) as grp, \
                          count(*) as cnt \
                   FROM orders \
                   GROUP BY GROUPING SETS ((), (o_orderstatus, o_orderpriority))";
        let resolved = parse_and_analyze(sql).expect("GROUPING SETS should work");
        if let QueryBody::Select(ref sel) = resolved.body {
            let repeat = sel
                .repeat
                .as_ref()
                .expect("GROUPING SETS should produce RepeatInfo");
            assert_eq!(repeat.repeat_column_ref_list.len(), 2);
            assert_eq!(repeat.grouping_ids, vec![0b11, 0b00]);
            assert_eq!(
                repeat.all_rollup_columns,
                vec!["o_orderstatus".to_string(), "o_orderpriority".to_string()]
            );
            assert_eq!(repeat.grouping_fn_args.len(), 2);
            assert_eq!(
                repeat.grouping_fn_args[0].1,
                vec!["o_orderstatus".to_string(), "o_orderpriority".to_string()]
            );
            assert_eq!(
                repeat.grouping_fn_args[1].1,
                vec!["o_orderstatus".to_string(), "o_orderpriority".to_string()]
            );
        } else {
            panic!("expected Select body with RepeatInfo");
        }
    }

    #[test]
    fn array_agg_without_arguments_reports_starrocks_error() {
        let err = parse_raw_and_analyze("select array_agg()")
            .expect_err("array_agg() should fail during analysis");
        assert_eq!(err, "array_agg should have at least one input.");
    }

    #[test]
    fn array_agg_with_multiple_arguments_keeps_order_syntax_error() {
        let err = parse_raw_and_analyze("select array_agg(1, 2 order by 1)")
            .expect_err("array_agg with multiple arguments should fail");
        assert_eq!(
            err,
            "Unexpected input 'order', the most similar input is {',', ')'}."
        );
    }

    #[test]
    fn bool_or_window_coerces_non_boolean_arguments() {
        let resolved = parse_and_analyze(
            "select bool_or(o_orderkey) over (partition by o_custkey order by o_orderkey) as v \
             from orders",
        )
        .expect("bool_or window should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let expr = &sel.projection[0].expr;
        assert_eq!(
            expr.value_type.data_type().clone(),
            arrow::datatypes::DataType::Boolean
        );
        let ExprKind::WindowCall { name, args, .. } = &expr.kind else {
            panic!("expected WindowCall, got {:?}", expr.kind);
        };
        assert_eq!(name, "bool_or");
        assert_eq!(args.len(), 1);
        let ExprKind::Cast { target, .. } = &args[0].kind else {
            panic!(
                "expected bool_or window argument cast, got {:?}",
                args[0].kind
            );
        };
        assert_eq!(target, &arrow::datatypes::DataType::Boolean);
        assert_eq!(
            args[0].value_type.data_type().clone(),
            arrow::datatypes::DataType::Boolean
        );
    }

    #[test]
    fn sum_avg_cast_varchar_arguments_to_double_in_analyzer_ir() {
        let resolved =
            parse_and_analyze("select sum(o_orderstatus), avg(o_orderstatus) from orders")
                .expect("string aggregate arguments should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };

        for expr in [&sel.projection[0].expr, &sel.projection[1].expr] {
            assert_eq!(
                expr.value_type.data_type().clone(),
                arrow::datatypes::DataType::Float64
            );
            let ExprKind::AggregateCall { name, args, .. } = &expr.kind else {
                panic!("expected AggregateCall, got {:?}", expr.kind);
            };
            assert!(matches!(name.as_str(), "sum" | "avg"));
            assert_eq!(args.len(), 1);
            let ExprKind::Cast {
                target,
                expr: inner,
                ..
            } = &args[0].kind
            else {
                panic!("expected aggregate argument cast, got {:?}", args[0].kind);
            };
            assert_eq!(target, &arrow::datatypes::DataType::Float64);
            assert_eq!(
                args[0].value_type.data_type().clone(),
                arrow::datatypes::DataType::Float64
            );
            assert_eq!(
                inner.value_type.data_type().clone(),
                arrow::datatypes::DataType::Utf8
            );
        }
    }

    #[test]
    fn length_casts_numeric_argument_to_varchar() {
        let resolved = parse_and_analyze("select length(o_orderkey) from orders")
            .expect("length should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let ExprKind::FunctionCall { name, args, .. } = &sel.projection[0].expr.kind else {
            panic!(
                "expected FunctionCall, got {:?}",
                sel.projection[0].expr.kind
            );
        };
        assert_eq!(name, "length");
        let ExprKind::Cast { target, expr, .. } = &args[0].kind else {
            panic!("expected length argument cast, got {:?}", args[0].kind);
        };
        assert_eq!(target, &arrow::datatypes::DataType::Utf8);
        assert_eq!(
            args[0].value_type.data_type().clone(),
            arrow::datatypes::DataType::Utf8
        );
        assert_eq!(
            expr.value_type.data_type().clone(),
            arrow::datatypes::DataType::Int64
        );
    }

    #[test]
    fn left_casts_value_argument_but_preserves_length_type() {
        let resolved = parse_and_analyze("select left(o_totalprice, 6) from orders")
            .expect("left should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let ExprKind::FunctionCall { name, args, .. } = &sel.projection[0].expr.kind else {
            panic!(
                "expected FunctionCall, got {:?}",
                sel.projection[0].expr.kind
            );
        };
        assert_eq!(name, "left");
        let ExprKind::Cast { target, expr, .. } = &args[0].kind else {
            panic!("expected left value argument cast, got {:?}", args[0].kind);
        };
        assert_eq!(target, &arrow::datatypes::DataType::Utf8);
        assert_eq!(
            expr.value_type.data_type().clone(),
            arrow::datatypes::DataType::Float64
        );
        assert_eq!(
            args[1].value_type.data_type().clone(),
            arrow::datatypes::DataType::Int64
        );
    }

    #[test]
    fn date_trunc_return_type_comes_from_value_argument() {
        let resolved = parse_and_analyze(
            "select date_trunc('week', o_orderstatus), date_trunc('week', o_orderdate) from orders",
        )
        .expect("date_trunc should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        assert_eq!(
            sel.projection[0].expr.value_type.data_type().clone(),
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            sel.projection[1].expr.value_type.data_type().clone(),
            arrow::datatypes::DataType::Date32
        );
    }

    #[test]
    fn date_trunc_casts_numeric_value_argument_to_datetime() {
        let resolved = parse_and_analyze("select date_trunc('week', o_orderkey) from orders")
            .expect("date_trunc should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let ExprKind::FunctionCall { args, .. } = &sel.projection[0].expr.kind else {
            panic!(
                "expected FunctionCall, got {:?}",
                sel.projection[0].expr.kind
            );
        };
        let ExprKind::Cast { target, expr, .. } = &args[1].kind else {
            panic!(
                "expected date_trunc value argument cast, got {:?}",
                args[1].kind
            );
        };
        assert_eq!(
            target,
            &arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
        );
        assert_eq!(
            expr.value_type.data_type().clone(),
            arrow::datatypes::DataType::Int64
        );
    }

    #[test]
    fn split_returns_varchar_array_and_casts_arguments() {
        let resolved = parse_and_analyze("select split(o_orderkey, 1) from orders")
            .expect("split should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let arrow::datatypes::DataType::List(item) = &sel.projection[0].expr.value_type.data_type()
        else {
            panic!("expected ARRAY return type");
        };
        assert_eq!(item.data_type(), &arrow::datatypes::DataType::Utf8);
        let ExprKind::FunctionCall { args, .. } = &sel.projection[0].expr.kind else {
            panic!(
                "expected FunctionCall, got {:?}",
                sel.projection[0].expr.kind
            );
        };
        assert!(matches!(args[0].kind, ExprKind::Cast { .. }));
        assert!(matches!(args[1].kind, ExprKind::Cast { .. }));
        assert_eq!(
            args[0].value_type.data_type().clone(),
            arrow::datatypes::DataType::Utf8
        );
        assert_eq!(
            args[1].value_type.data_type().clone(),
            arrow::datatypes::DataType::Utf8
        );
    }

    #[test]
    fn ceil_expression_analyzes_as_scalar_function() {
        let resolved = parse_and_analyze("select ceil(sum(o_totalprice)) from orders")
            .expect("ceil should analyze");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let ExprKind::FunctionCall { name, args, .. } = &sel.projection[0].expr.kind else {
            panic!(
                "expected FunctionCall, got {:?}",
                sel.projection[0].expr.kind
            );
        };
        assert_eq!(name, "ceil");
        assert_eq!(args.len(), 1);
        assert!(matches!(args[0].kind, ExprKind::AggregateCall { .. }));
    }

    struct CatalogAwareTestCatalog;

    impl CatalogAwareTestCatalog {
        fn table_def(catalog: Option<&str>, database: &str, table: &str) -> TableDef {
            let catalog_name = catalog.unwrap_or("default_catalog");
            TableDef {
                name: format!("{catalog_name}_{database}_{table}"),
                columns: vec![ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    write_default: None,
                    logical_type: None,
                }],
                iceberg_row_lineage_metadata_columns: vec![],
                source: sql_test_scan_source(
                    catalog_name,
                    database,
                    table,
                    SqlScanKind::Data {
                        version: SqlTableVersionSelector::Current,
                    },
                ),
            }
        }

        fn metadata_table_def(
            catalog: Option<&str>,
            database: &str,
            table: &str,
            metadata_table_type: SqlMetadataTableKind,
        ) -> TableDef {
            let mut table_def = Self::table_def(catalog, database, table);
            table_def.columns = metadata_table_columns(&metadata_table_type);
            table_def.source = sql_test_scan_source(
                catalog.unwrap_or("default_catalog"),
                database,
                table,
                SqlScanKind::Metadata {
                    kind: metadata_table_type,
                    version: SqlTableVersionSelector::Current,
                },
            );
            table_def
        }
    }

    impl crate::catalog::PlannerTableProvider for CatalogAwareTestCatalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                catalog,
                database,
                Self::table_def(catalog, database, table),
            ))
        }

        fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
            Some(self)
        }
    }

    impl IcebergMetadataTableProvider for CatalogAwareTestCatalog {
        fn get_iceberg_metadata_table(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
            metadata_table_type: SqlMetadataTableKind,
        ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
            Ok(crate::catalog::ResolvedAnalyzerTable::from_planner(
                catalog,
                database,
                Self::metadata_table_def(catalog, database, table, metadata_table_type),
            ))
        }
    }

    #[test]
    fn analyzer_passes_three_part_catalog_to_catalog_provider() {
        let query = parse_native_query("SELECT id FROM ice.db.orders").expect("parse");

        let (resolved, _, _) =
            analyze(&query, &CatalogAwareTestCatalog, "default").expect("analyze");

        let QueryBody::Select(select) = resolved.body else {
            panic!("expected select");
        };
        let Some(Relation::Scan(scan)) = select.from else {
            panic!("expected scan");
        };
        assert_eq!(scan.database, "db");
        assert_eq!(scan.table.name, "ice_db_orders");
    }

    #[test]
    fn analyzer_passes_uppercase_three_part_catalog_to_catalog_provider() {
        let query = parse_native_query("SELECT id FROM ICE.DB.ORDERS").expect("parse");

        let (resolved, _, _) =
            analyze(&query, &CatalogAwareTestCatalog, "default").expect("analyze");

        let QueryBody::Select(select) = resolved.body else {
            panic!("expected select");
        };
        let Some(Relation::Scan(scan)) = select.from else {
            panic!("expected scan");
        };
        assert_eq!(scan.database, "db");
        assert_eq!(scan.table.name, "ice_db_orders");
    }

    #[test]
    fn analyzer_uses_files_metadata_provider_for_partitions_view() {
        struct MetadataModeCatalog(std::cell::Cell<bool>);
        impl crate::catalog::PlannerTableProvider for MetadataModeCatalog {
            fn resolve_table_for_analysis(
                &self,
                catalog: Option<&str>,
                database: &str,
                table: &str,
            ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
                CatalogAwareTestCatalog.resolve_table_for_analysis(catalog, database, table)
            }

            fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
                Some(self)
            }
        }

        impl IcebergMetadataTableProvider for MetadataModeCatalog {
            fn get_iceberg_metadata_table(
                &self,
                catalog: Option<&str>,
                database: &str,
                table: &str,
                metadata_table_type: SqlMetadataTableKind,
            ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
                self.0
                    .set(metadata_table_type == SqlMetadataTableKind::Files);
                CatalogAwareTestCatalog.get_iceberg_metadata_table(
                    catalog,
                    database,
                    table,
                    metadata_table_type,
                )
            }
        }

        let catalog = MetadataModeCatalog(std::cell::Cell::new(false));
        let query =
            parse_native_query("SELECT record_count FROM ice.db.orders$partitions").expect("parse");

        let _ = analyze(&query, &catalog, "default").expect("analyze");
        assert!(
            catalog.0.get(),
            "Files metadata provider was not requested by the partitions view"
        );
    }

    #[test]
    fn analyzer_uses_lowercase_catalog_for_metadata_provider() {
        struct LowercaseMetadataModeCatalog(std::cell::Cell<bool>);
        impl crate::catalog::PlannerTableProvider for LowercaseMetadataModeCatalog {
            fn resolve_table_for_analysis(
                &self,
                catalog: Option<&str>,
                database: &str,
                table: &str,
            ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
                CatalogAwareTestCatalog.resolve_table_for_analysis(catalog, database, table)
            }

            fn iceberg_metadata_provider(&self) -> Option<&dyn IcebergMetadataTableProvider> {
                Some(self)
            }
        }

        impl IcebergMetadataTableProvider for LowercaseMetadataModeCatalog {
            fn get_iceberg_metadata_table(
                &self,
                catalog: Option<&str>,
                database: &str,
                table: &str,
                metadata_table_type: SqlMetadataTableKind,
            ) -> Result<crate::catalog::ResolvedAnalyzerTable, String> {
                assert_eq!(catalog, Some("ice"));
                assert_eq!(database, "db");
                assert_eq!(table, "orders");
                self.0
                    .set(metadata_table_type == SqlMetadataTableKind::Files);
                CatalogAwareTestCatalog.get_iceberg_metadata_table(
                    catalog,
                    database,
                    table,
                    metadata_table_type,
                )
            }
        }

        let catalog = LowercaseMetadataModeCatalog(std::cell::Cell::new(false));
        let query =
            parse_native_query("SELECT record_count FROM ICE.DB.ORDERS$partitions").expect("parse");

        let _ = analyze(&query, &catalog, "default").expect("analyze");
        assert!(
            catalog.0.get(),
            "Files metadata provider was not requested by the partitions view"
        );
    }

    #[test]
    fn analyzer_resolves_t_dollar_snapshots_to_metadata_scan() {
        use crate::planner::table::SqlMetadataTableKind;

        // Metadata table syntax is carried by the typed table-factor metadata field.
        let resolved = parse_raw_and_analyze("SELECT snapshot_id FROM orders$snapshots")
            .expect("analyze should succeed");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let from = sel.from.as_ref().expect("FROM clause should be present");
        match from {
            Relation::IcebergMetadataScan(rel) => {
                assert_eq!(rel.metadata_table_type, SqlMetadataTableKind::Snapshots);
                assert_eq!(rel.table.name, "orders");
                assert_eq!(rel.database, "default");
                assert!(rel.alias.is_none());
            }
            other => panic!("expected IcebergMetadataScan, got {other:?}"),
        }

        // The output column `snapshot_id` should resolve through the synthetic
        // metadata schema (Int64, NOT NULL).
        assert_eq!(resolved.output_columns.len(), 1);
        let col = &resolved.output_columns[0];
        assert_eq!(col.name, "snapshot_id");
        assert_eq!(
            col.value_type.data_type().clone(),
            arrow::datatypes::DataType::Int64
        );
        assert!(!col.value_type.nullable());
    }

    #[test]
    fn metadata_order_by_hidden_column_reuses_scan_column_id() {
        for sql in [
            "SELECT snapshot_id FROM orders$snapshots ORDER BY committed_at LIMIT 1",
            "SELECT snapshot_id FROM orders$snapshots AS s ORDER BY s.committed_at LIMIT 1",
        ] {
            let resolved = parse_raw_and_analyze(sql).expect("analyze metadata ORDER BY");
            let QueryBody::Select(select) = &resolved.body else {
                panic!("expected Select body");
            };
            let Some(Relation::IcebergMetadataScan(scan)) = &select.from else {
                panic!("expected metadata scan");
            };
            let index = scan
                .table
                .columns
                .iter()
                .position(|column| column.name == "committed_at")
                .expect("committed_at metadata column");
            let ExprKind::ColumnRef { column_id, .. } = &resolved.order_by[0].expr.kind else {
                panic!("expected ORDER BY column reference");
            };
            assert_eq!(
                *column_id, scan.column_ids[index],
                "ORDER BY must retain the original scan ColumnId: {sql}"
            );
            assert_eq!(resolved.output_columns.len(), 1);
            assert_eq!(resolved.output_columns[0].name, "snapshot_id");
        }
    }

    #[test]
    fn metadata_order_by_hidden_column_survives_optimizer_id_binding() {
        for sql in [
            "SELECT snapshot_id FROM orders$snapshots ORDER BY committed_at LIMIT 1",
            "SELECT snapshot_id FROM orders$snapshots AS s ORDER BY s.committed_at LIMIT 1",
        ] {
            let query = parse_native_query(sql).expect("parse metadata ORDER BY");
            let (resolved, registry, mut factory) =
                analyze(&query, &TestCatalog, "default").expect("analyze metadata ORDER BY");
            let logical_plan = crate::planner::logical::build::plan_query(
                resolved,
                registry,
                &mut factory,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .expect("plan metadata ORDER BY");
            let mut scalar_arena = crate::optimizer::scalar::ScalarArena::new();
            let optimizer_expr = crate::planner::optimizer_bridge::logical::try_to_optimizer_expr(
                &logical_plan,
                &mut scalar_arena,
                &crate::compiler::SqlCompileControl::unbounded(),
            )
            .expect("logical to optimizer expression");
            let optimized_tree = crate::optimizer::optimize_with_test_table_statistics(
                optimizer_expr,
                scalar_arena,
                &std::collections::HashMap::new(),
                factory,
                Vec::new(),
                &crate::optimizer::options::SessionOptimizerSettings::default(),
            )
            .expect("optimize metadata ORDER BY");
            crate::planner::optimizer_bridge::id_binding::verify_optimized_tree_id_binding(
                &optimized_tree,
            )
            .unwrap_or_else(|error| panic!("metadata ORDER BY binding failed for {sql}: {error}"));
        }
    }

    #[test]
    fn analyzer_rejects_branch_combined_with_metadata_suffix() {
        // The typed metadata suffix leaves `branch_dev` as the base name,
        // which is
        // illegal in combination with a metadata-table suffix.
        let err = parse_raw_and_analyze("SELECT * FROM orders.branch_dev$snapshots")
            .expect_err("must fail");
        assert!(
            err.contains("cannot be combined with branch/tag suffix"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn analyzer_recognizes_nr_ivm_delta_table_function() {
        // The TestCatalog's `iv_orders` table carries the v3 row-lineage
        // metadata columns required by __nr_ivm_delta. Note: the TestCatalog
        // returns the same table for any database name, so we can pass any
        // catalog/namespace strings in the three-part identifier.
        let resolved = parse_raw_and_analyze(
            "SELECT _row_id FROM __nr_ivm_delta('cat.ns.iv_orders', 100, 200) AS t",
        )
        .expect("analysis should succeed");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let from = sel.from.as_ref().expect("FROM should be present");
        match from {
            Relation::IcebergDeltaScan(rel) => {
                assert_eq!(rel.catalog, "cat");
                assert_eq!(rel.namespace, "ns");
                assert_eq!(rel.table_name, "iv_orders");
                assert_eq!(rel.from_snapshot_id, 100);
                assert_eq!(rel.to_snapshot_id, 200);
                assert_eq!(rel.alias.as_deref(), Some("t"));
                assert!(
                    rel.table
                        .iceberg_row_lineage_metadata_columns
                        .iter()
                        .any(|c| c.name == "_row_id"),
                    "expected _row_id in row-lineage metadata columns"
                );
            }
            other => panic!("expected IcebergDeltaScan, got {other:?}"),
        }
        // Output column `_row_id` should resolve through the row-lineage
        // metadata columns the analyzer registered into the scope.
        assert_eq!(resolved.output_columns.len(), 1);
        assert_eq!(resolved.output_columns[0].name, "_row_id");
        assert_eq!(
            resolved.output_columns[0].value_type.data_type().clone(),
            arrow::datatypes::DataType::Int64
        );
    }

    #[test]
    fn analyzer_preserves_row_lineage_metadata_column_ids_for_base_scan() {
        let resolved = parse_raw_and_analyze("SELECT _row_id FROM iv_orders AS t")
            .expect("analysis should succeed");
        let QueryBody::Select(sel) = &resolved.body else {
            panic!("expected Select body");
        };
        let from = sel.from.as_ref().expect("FROM should be present");
        let Relation::Scan(scan) = from else {
            panic!("expected base Scan, got {from:?}");
        };

        let base_len = scan.table.columns.len();
        let meta_len = scan.table.iceberg_row_lineage_metadata_columns.len();
        assert_eq!(
            scan.column_ids.len(),
            base_len + meta_len,
            "base scan must carry base column ids followed by row-lineage metadata ids"
        );
        let row_id = scan.column_ids[base_len];
        assert_ne!(row_id, ColumnId::UNSET);

        let ExprKind::ColumnRef {
            column_id, column, ..
        } = &sel.projection[0].expr.kind
        else {
            panic!("expected projection to resolve _row_id as ColumnRef");
        };
        assert_eq!(column, "_row_id");
        assert_eq!(
            *column_id, row_id,
            "projection must reference the metadata ColumnId from the base Scan relation"
        );
        assert_eq!(
            resolved.output_columns[0].column_id, row_id,
            "visible output must preserve the same metadata ColumnId"
        );
    }

    #[test]
    fn analyzer_rejects_nr_ivm_delta_with_negative_snapshot() {
        let err =
            parse_raw_and_analyze("SELECT * FROM __nr_ivm_delta('cat.ns.iv_orders', -1, 200) AS t")
                .expect_err("must fail");
        assert!(err.contains("non-negative"), "unexpected error: {err}");
    }

    #[test]
    fn analyzer_rejects_nr_ivm_delta_on_non_v3_table() {
        // `orders` is registered without row-lineage metadata columns.
        let err =
            parse_raw_and_analyze("SELECT * FROM __nr_ivm_delta('cat.ns.orders', 100, 200) AS t")
                .expect_err("must fail");
        assert!(
            err.contains("write.row-lineage") || err.contains("row-lineage metadata"),
            "expected row-lineage rebuild diagnostic, got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // output_column_id on ProjectItem
    // -----------------------------------------------------------------------

    /// A computed SELECT item (`a + b AS c`) must receive a non-UNSET
    /// `output_column_id` that matches the corresponding entry in
    /// `output_columns`.  A passthrough ColumnRef item reuses the
    /// source column's id.
    #[test]
    fn computed_project_item_gets_non_unset_output_column_id() {
        // `orders` has o_orderkey (Int64) and o_custkey (Int64).
        let sql = "SELECT o_orderkey + o_custkey AS sum_keys, o_orderkey FROM orders";
        let resolved = parse_raw_and_analyze(sql).expect("analysis should succeed");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected Select body");
        };

        assert_eq!(select.projection.len(), 2, "expected two projection items");
        assert_eq!(
            resolved.output_columns.len(),
            2,
            "expected two output columns"
        );

        // First item: computed expression (o_orderkey + o_custkey).
        let computed_item = &select.projection[0];
        let computed_out_col = &resolved.output_columns[0];
        assert_ne!(
            computed_item.output_column_id,
            crate::column_id::ColumnId::UNSET,
            "computed item must not have UNSET output_column_id"
        );
        assert_eq!(
            computed_item.output_column_id, computed_out_col.column_id,
            "computed item output_column_id must match the output_columns entry"
        );

        // Second item: passthrough ColumnRef (o_orderkey).
        let passthrough_item = &select.projection[1];
        let passthrough_out_col = &resolved.output_columns[1];
        assert_ne!(
            passthrough_item.output_column_id,
            crate::column_id::ColumnId::UNSET,
            "passthrough item must not have UNSET output_column_id"
        );
        assert_eq!(
            passthrough_item.output_column_id, passthrough_out_col.column_id,
            "passthrough item output_column_id must match the output_columns entry"
        );

        // The two items must have distinct ids.
        assert_ne!(
            computed_item.output_column_id, passthrough_item.output_column_id,
            "each projection item must receive a distinct output_column_id"
        );
    }

    #[test]
    fn p2_using_reference_keeps_analyzer_selected_id() {
        let resolved =
            parse_and_analyze("SELECT k1 FROM t1 JOIN t2 USING(k1)").expect("analysis succeeds");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected Select body");
        };
        let item = &select.projection[0];
        let ExprKind::ColumnRef {
            column_id,
            qualifier,
            column,
        } = &item.expr.kind
        else {
            panic!(
                "expected USING projection to resolve as ColumnRef, got {:?}",
                item.expr.kind
            );
        };
        assert_eq!(column, "k1");
        assert!(
            qualifier.is_none(),
            "USING projection must not rely on canonical qualifier steering: {:?}",
            item.expr.kind
        );
        assert_ne!(
            *column_id,
            crate::column_id::ColumnId::UNSET,
            "USING projection must carry a real ColumnId"
        );
        assert_eq!(
            *column_id, item.output_column_id,
            "USING projection output must reuse the analyzer-selected source ColumnId"
        );
        assert_eq!(
            *column_id, resolved.output_columns[0].column_id,
            "query output must expose the same USING source ColumnId"
        );
    }

    #[test]
    fn p2_full_outer_using_coalesce_has_project_output_id() {
        let resolved = parse_and_analyze("SELECT k1 FROM t1 FULL OUTER JOIN t2 USING(k1)")
            .expect("analysis succeeds");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected Select body");
        };
        let merged_item = &select.projection[0];
        assert_ne!(
            merged_item.output_column_id,
            crate::column_id::ColumnId::UNSET,
            "FULL OUTER USING merged projection must have a real output ColumnId"
        );
        assert_eq!(
            merged_item.output_column_id, resolved.output_columns[0].column_id,
            "FULL OUTER USING merged query output must expose the project output id"
        );
        let ExprKind::FunctionCall { name, args, .. } = &merged_item.expr.kind else {
            panic!(
                "expected FULL OUTER USING merged column to be COALESCE, got {:?}",
                merged_item.expr.kind
            );
        };
        assert_eq!(name, "coalesce");
        assert_eq!(args.len(), 2);
        assert!(
            args.iter()
                .all(|arg| matches!(arg.kind, ExprKind::ColumnRef { .. })),
            "FULL OUTER USING merged expression should coalesce the two side ColumnRefs"
        );
    }

    #[test]
    fn analyze_with_factory_threads_column_ids() {
        // Pre-seed the factory with 3 ids so threaded analysis must start at 4.
        let mut factory = crate::column_id::ColumnRefFactory::new();
        for i in 0..3_u32 {
            factory.create(
                None,
                format!("seed{i}"),
                novarocks_type_contract::FunctionValueType::new(
                    arrow::datatypes::DataType::Int64,
                    false,
                ),
            );
        }
        assert_eq!(factory.peek_next_id(), 4);

        let query = parse_native_query("SELECT 1 + 1 AS x").expect("parse");
        let (_resolved, _ctes, out_factory) = analyze_with_factory(
            &query,
            &TestCatalog,
            "db",
            factory,
            crate::constant::test_constant_policy(),
            &crate::compiler::SqlCompileControl::unbounded(),
        )
        .expect("analyze");
        // The analysis must have allocated its ids on top of the seeded ones.
        assert!(out_factory.peek_next_id() > 4);
        assert_eq!(out_factory.get(crate::column_id::ColumnId(1)).name, "seed0");
    }

    // ---------------------------------------------------------------------------
    // Task 3 — Apply framework scalar subquery routing tests
    // ---------------------------------------------------------------------------

    const CORRELATED_SCALAR_SQL: &str =
        "SELECT k1 FROM t1 WHERE k1 = (SELECT max(k2) FROM t2 WHERE t2.k1 = t1.k1)";

    fn parse_and_analyze_for_apply_specs(sql: &str) -> Result<ResolvedQuery, String> {
        let query = parse_native_query(sql)?;
        let (resolved, _cte, _factory) =
            analyze(&query, &TestCatalog, "default").map_err(|error| error.to_string())?;
        Ok(resolved)
    }

    /// In the Apply framework a correlated scalar WHERE subquery must be recorded as an
    /// ApplyScalarSpec instead of being rewritten into a join.
    #[test]
    fn current_route_records_where_scalar_subquery_apply_spec() {
        use crate::analysis::{ApplyClause, QueryBody};

        let query = parse_native_query(CORRELATED_SCALAR_SQL).expect("parse correlated query");
        let (resolved, _cte, _factory) =
            analyze(&query, &TestCatalog, "default").expect("analyze with apply framework");

        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.apply_specs.len(),
            1,
            "one scalar apply spec expected"
        );
        let spec = &select.apply_specs[0];
        assert_eq!(spec.clause, ApplyClause::Where);
        assert!(spec.need_check_max_rows);
        assert!(
            !spec.correlation_column_ids.is_empty(),
            "correlated subquery must record outer column ids"
        );
        // The placeholder must be gone from the WHERE predicate (replaced by a
        // ColumnRef to the spec's output column). Verify no SubqueryPlaceholder
        // remains in the WHERE filter.
        if let Some(ref filter) = select.filter {
            assert!(
                !expr_has_subquery_placeholder(filter),
                "WHERE filter must not contain a SubqueryPlaceholder after apply routing"
            );
        }
        // In the Apply framework the FROM should NOT have grown a join.
        let from_is_join = matches!(select.from, Some(crate::analysis::Relation::Join(_)));
        assert!(
            !from_is_join,
            "Apply framework must NOT rewrite the scalar subquery into a join"
        );
    }

    #[test]
    fn scalar_apply_factors_common_correlation_from_or_filter() {
        use crate::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 \
             WHERE (SELECT count(*) FROM t2 \
                    WHERE (t2.k1 = t1.k1 AND t2.k2 = 1) \
                       OR (t2.k1 = t1.k1 AND t2.k2 = 2)) > 0",
        )
        .expect("analyze scalar Apply with OR correlation");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.apply_specs.len(), 1);

        let QueryBody::Select(inner_select) = &select.apply_specs[0].inner.body else {
            panic!("expected scalar Apply inner select");
        };
        let filter = inner_select
            .filter
            .as_ref()
            .expect("expected scalar Apply inner filter");
        let ExprKind::BinaryOp {
            left,
            op: BinOp::And,
            right,
            ..
        } = &filter.kind
        else {
            panic!("expected common correlation to be factored into top-level AND: {filter:?}");
        };
        assert!(
            matches!(&left.kind, ExprKind::BinaryOp { op: BinOp::Eq, .. }),
            "left conjunct should be the common equality correlation: {left:?}"
        );
        assert!(
            matches!(&right.kind, ExprKind::BinaryOp { op: BinOp::Or, .. }),
            "right conjunct should retain the residual OR predicate: {right:?}"
        );
    }

    #[test]
    fn correlated_avg_scalar_subquery_comparison_preserves_float_type() {
        use crate::analysis::QueryBody;
        use arrow::datatypes::DataType;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 \
             WHERE k1 < (SELECT avg(k2) FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect("analyze correlated AVG scalar subquery");

        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.apply_specs.len(), 1);
        let spec = &select.apply_specs[0];
        assert_eq!(
            spec.output_column.value_type.data_type().clone(),
            DataType::Float64,
            "AVG scalar subquery output must remain Float64"
        );

        let filter = select.filter.as_ref().expect("expected WHERE filter");
        let ExprKind::BinaryOp {
            left,
            op: BinOp::Lt,
            right,
            ..
        } = &filter.kind
        else {
            panic!("expected k1 < scalar subquery comparison, got {filter:?}");
        };

        assert_eq!(
            left.value_type.data_type().clone(),
            DataType::Float64,
            "left INT operand should be widened to Float64 for AVG comparison"
        );
        assert_eq!(
            right.value_type.data_type().clone(),
            DataType::Float64,
            "scalar subquery RHS should stay Float64, not cast back to INT"
        );
        assert!(
            !expr_casts_column_to_type(right, spec.output_column.column_id, &DataType::Int64)
                && !expr_casts_column_to_type(
                    right,
                    spec.output_column.column_id,
                    &DataType::Int32
                ),
            "RHS must not contain CAST(scalar_avg AS integer): {right:?}"
        );
    }

    fn expr_casts_column_to_type(
        expr: &TypedExpr,
        column_id: crate::column_id::ColumnId,
        target: &arrow::datatypes::DataType,
    ) -> bool {
        match &expr.kind {
            ExprKind::Cast {
                expr: inner,
                target: cast_target,
                ..
            } => {
                (cast_target == target && expr_refs_column(inner, column_id))
                    || expr_casts_column_to_type(inner, column_id, target)
            }
            ExprKind::BinaryOp { left, right, .. } => {
                expr_casts_column_to_type(left, column_id, target)
                    || expr_casts_column_to_type(right, column_id, target)
            }
            ExprKind::Nested(inner)
            | ExprKind::UnaryOp { expr: inner, .. }
            | ExprKind::IsNull { expr: inner, .. } => {
                expr_casts_column_to_type(inner, column_id, target)
            }
            _ => false,
        }
    }

    fn expr_refs_column(expr: &TypedExpr, column_id: crate::column_id::ColumnId) -> bool {
        match &expr.kind {
            ExprKind::ColumnRef { column_id: id, .. } => *id == column_id,
            ExprKind::Cast { expr: inner, .. }
            | ExprKind::Nested(inner)
            | ExprKind::UnaryOp { expr: inner, .. }
            | ExprKind::IsNull { expr: inner, .. } => expr_refs_column(inner, column_id),
            ExprKind::BinaryOp { left, right, .. } => {
                expr_refs_column(left, column_id) || expr_refs_column(right, column_id)
            }
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                args.iter().any(|arg| expr_refs_column(arg, column_id))
            }
            _ => false,
        }
    }

    #[test]
    fn in_subquery_derived_table_internal_refs_are_not_outer_refs() {
        use crate::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 \
             WHERE k1 IN ( \
                 SELECT k1 \
                 FROM (SELECT k1 AS k1, k2 AS ranking FROM t2) tmp1 \
                 WHERE ranking <= 5 \
             )",
        )
        .expect("derived-table internals should not be treated as correlated outer refs");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        assert!(
            select.predicate_apply_specs[0]
                .correlation_column_ids
                .is_empty(),
            "uncorrelated IN subquery must not record correlation ids"
        );
    }

    /// Helper: returns true if `expr` or any descendant is a SubqueryPlaceholder.
    fn expr_has_subquery_placeholder(expr: &TypedExpr) -> bool {
        match &expr.kind {
            ExprKind::SubqueryPlaceholder { .. } => true,
            ExprKind::BinaryOp { left, right, .. } => {
                expr_has_subquery_placeholder(left) || expr_has_subquery_placeholder(right)
            }
            ExprKind::UnaryOp { expr: inner, .. } => expr_has_subquery_placeholder(inner),
            ExprKind::IsNull { expr: inner, .. } => expr_has_subquery_placeholder(inner),
            ExprKind::Cast { expr: inner, .. } => expr_has_subquery_placeholder(inner),
            ExprKind::Nested(inner) => expr_has_subquery_placeholder(inner),
            ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
                args.iter().any(expr_has_subquery_placeholder)
            }
            _ => false,
        }
    }

    #[test]
    fn exists_correlated_where_records_predicate_spec() {
        use crate::analysis::{QueryBody, SubqueryKind};

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        let spec = &select.predicate_apply_specs[0];
        assert!(matches!(spec.kind, SubqueryKind::Exists { negated: false }));
        assert!(spec.use_semi_anti);
        assert_eq!(spec.correlation_column_ids.len(), 1);
        assert!(spec.in_lhs.is_none());
        if let Some(filter) = &select.filter {
            assert!(
                !expr_has_subquery_placeholder(filter),
                "WHERE filter must not contain a SubqueryPlaceholder after apply routing"
            );
        }
    }

    #[test]
    fn not_exists_sets_negated() {
        use crate::analysis::{QueryBody, SubqueryKind};

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        assert!(matches!(
            select.predicate_apply_specs[0].kind,
            SubqueryKind::Exists { negated: true }
        ));
    }

    #[test]
    fn in_uncorrelated_records_spec_with_lhs() {
        use crate::analysis::{QueryBody, SubqueryKind};

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE t1.k1 IN (SELECT t2.k2 FROM t2)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        let spec = &select.predicate_apply_specs[0];
        assert!(matches!(
            spec.kind,
            SubqueryKind::InSubquery { negated: false }
        ));
        assert!(spec.in_lhs.is_some());
        assert!(
            spec.correlation_column_ids.is_empty(),
            "uncorrelated IN must not record correlation column ids"
        );
    }

    #[test]
    fn not_in_sets_negated() {
        use crate::analysis::{QueryBody, SubqueryKind};

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE t1.k1 NOT IN (SELECT t2.k2 FROM t2)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        assert!(matches!(
            select.predicate_apply_specs[0].kind,
            SubqueryKind::InSubquery { negated: true }
        ));
    }

    #[test]
    fn exists_inside_or_uses_explicit_value_form_rewrite() {
        use crate::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE k1 = 1 OR EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = 1)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert!(select.predicate_apply_specs.is_empty());
        assert!(
            matches!(select.from, Some(crate::analysis::Relation::Join(_))),
            "inside-OR EXISTS should use the explicit value-form rewrite"
        );
    }

    #[test]
    fn not_in_value_form_marker_relations_are_nullable() {
        use crate::analysis::{QueryBody, Relation};

        fn collect_marker_outputs(rel: &Relation, out: &mut Vec<(String, bool)>) {
            match rel {
                Relation::Subquery {
                    alias,
                    output_columns,
                    ..
                } => {
                    if alias.starts_with("__sq_null_") || alias.starts_with("__sq_any_") {
                        for column in output_columns {
                            if column.name.starts_with("__has_") {
                                out.push((column.name.clone(), column.value_type.nullable()));
                            }
                        }
                    }
                }
                Relation::Join(join) => {
                    collect_marker_outputs(&join.left, out);
                    collect_marker_outputs(&join.right, out);
                }
                _ => {}
            }
        }

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE k2 NOT IN (SELECT t2.k2 FROM t2) OR false",
        )
        .expect("analyze value-form NOT IN");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        let mut markers = Vec::new();
        if let Some(from) = &select.from {
            collect_marker_outputs(from, &mut markers);
        }
        assert_eq!(
            markers.len(),
            2,
            "nullable NOT IN should create null and nonempty marker relations"
        );
        for (name, nullable) in markers {
            assert!(nullable, "{name} marker output must be nullable");
        }
    }

    #[test]
    fn correlated_exists_inside_or_is_rejected_without_fallback() {
        let err = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE k1 = 1 OR EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect_err("correlated EXISTS value-form should not fall back");
        assert!(
            err.contains("correlated EXISTS subquery in value-form expression is not supported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn correlated_in_inside_or_is_rejected_without_fallback() {
        let err = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE k1 = 1 OR k2 IN (SELECT t2.k2 FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect_err("correlated IN value-form should not fall back");
        assert!(
            err.contains("correlated IN subquery in value-form expression is not supported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn exists_in_having_records_predicate_spec() {
        use crate::analysis::{ApplyClause, QueryBody};

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1, count(*) FROM t1 GROUP BY k1 HAVING EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(select.predicate_apply_specs.len(), 1);
        assert_eq!(select.predicate_apply_specs[0].clause, ApplyClause::Having);
    }

    #[test]
    fn multi_column_in_rewrites_without_apply_spec() {
        use crate::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE (k1, k2) IN (SELECT t2.k1, t2.k2 FROM t2)",
        )
        .expect("multi-column IN should use the local join rewrite");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert!(
            select.predicate_apply_specs.is_empty(),
            "multi-column IN is represented by the local join rewrite, not Apply"
        );
    }

    #[test]
    fn mixed_eq_and_non_eq_outer_ref_records_exists_spec() {
        use crate::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.v2 = t1.v1 AND t2.k2 > t1.k2)",
        )
        .expect("analyze in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "mixed equality and non-equality correlation should record an EXISTS predicate spec"
        );
    }

    #[test]
    fn pure_between_outer_ref_records_exists_spec() {
        use crate::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t1.k1 BETWEEN t2.k1 AND t2.k2)",
        )
        .expect("BETWEEN correlation should route through predicate Apply");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "BETWEEN correlation should record an EXISTS predicate spec"
        );
    }

    #[test]
    fn pure_non_eq_outer_ref_records_exists_spec() {
        use crate::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.k2 > t1.k2)",
        )
        .expect("analyze pure non-EQ correlation in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "pure non-equality correlation should record an EXISTS predicate spec"
        );
    }

    #[test]
    fn in_pure_non_eq_outer_ref_records_predicate_spec() {
        use crate::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE t1.k1 IN (SELECT t2.k1 FROM t2 WHERE t2.k2 > t1.k2)",
        )
        .expect("analyze pure non-EQ IN correlation in Apply framework");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "pure non-equality correlation should record an IN predicate spec"
        );
    }

    #[test]
    fn is_true_outer_ref_records_exists_spec() {
        use crate::analysis::QueryBody;

        let resolved = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE (t1.k1 = 1) IS TRUE)",
        )
        .expect("IS TRUE outer ref should route through predicate Apply");
        let QueryBody::Select(select) = &resolved.body else {
            panic!("expected select body");
        };
        assert_eq!(
            select.predicate_apply_specs.len(),
            1,
            "IS TRUE correlation should record an EXISTS predicate spec"
        );
    }

    #[test]
    fn in_rhs_projection_outer_ref_is_rejected_without_fallback() {
        let err = parse_and_analyze_for_apply_specs(
            "SELECT k1 FROM t1 WHERE t1.k1 IN (SELECT t1.k2 FROM t2)",
        )
        .expect_err("outer refs outside the inner filter should not fall back");
        assert!(
            err.contains("correlated EXISTS/IN subquery must use comparison predicates"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn correlated_exists_in_projection_is_rejected_clearly() {
        let err = parse_and_analyze("SELECT EXISTS (SELECT 1 FROM t2 WHERE t2.k1 = t1.k1) FROM t1")
            .expect_err("correlated EXISTS in SELECT list should be rejected");
        assert!(
            err.contains("correlated EXISTS subquery in SELECT list is not supported"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn group_by_subquery_is_rejected_clearly() {
        let err = parse_and_analyze("SELECT k1 FROM t1 GROUP BY (SELECT max(k2) FROM t2)")
            .expect_err("subquery in GROUP BY should be rejected");
        assert!(
            err.contains("subquery is not supported in GROUP BY"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn order_by_subquery_is_rejected_clearly() {
        let err = parse_and_analyze("SELECT k1 FROM t1 ORDER BY (SELECT max(k2) FROM t2)")
            .expect_err("subquery in ORDER BY should be rejected");
        assert!(
            err.contains("subquery is not supported in ORDER BY"),
            "unexpected error: {err}"
        );
    }
}

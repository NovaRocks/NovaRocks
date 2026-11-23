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

//! SQL-owned typed query preparation performed before catalog lookup.
//!
//! This module deliberately consumes and returns `novarocks_parser` nodes.
//! It must never rebuild SQL text or invoke a parser.  T4 wires
//! [`preanalyze`] into the production typed-query admission path.

use novarocks_parser::ast;
use novarocks_parser::{
    Span,
    ast::{Expr, Fold, Ident, LiteralKind, Query, Select, SelectItem, SetExpr, SetOperator, Visit},
};

use crate::analyze_error::AnalyzeError;

/// Applies syntax-complete, catalog-independent query semantics.
///
/// This is intentionally not connected to an analyzer entry point yet.  The
/// contract is owned here first so the SQLP-6 cutover can flip every producer
/// and consumer together without a raw-text bridge.
///
/// The pass is idempotent, and has to be: more than one owner preanalyzes the
/// same statement.  Collecting a completed plan's catalog needs requires the
/// rewritten query -- CTE unrolling decides which
/// relations the statement names -- and the analyzer preanalyzes whatever it
/// is handed.  So everything this pass applies it also consumes, and running
/// it on its own output changes nothing.
pub(crate) fn preanalyze(query: Query) -> Result<Query, AnalyzeError> {
    preanalyze_with_semantics(query, &crate::sql_mode::SqlSemanticSettings::default())
}

/// Unrolls with the statement session property, then the frozen session override, then the
/// original default. The selected depth still uses the existing finite unroller.
pub(crate) fn preanalyze_with_semantics(
    mut query: Query,
    semantics: &crate::sql_mode::SqlSemanticSettings,
) -> Result<Query, AnalyzeError> {
    reject_unimplemented_query_syntax(&query)?;
    let max_depth = recursive_cte_max_depth(&query)
        .or_else(|| {
            semantics
                .recursive_cte_max_depth()
                .and_then(|depth| usize::try_from(depth).ok())
        })
        .unwrap_or(crate::sql_mode::DEFAULT_RECURSIVE_CTE_MAX_DEPTH as usize);
    rewrite_nested_queries(&mut query, max_depth.max(1))?;

    remove_bare_top_level_dual(&mut query);
    Ok(query)
}

/// Capability rejection precedes catalog discovery and global function lookup.
/// Inline definitions cannot be ignored and rebound as unrelated global calls.
fn reject_unimplemented_query_syntax(query: &Query) -> Result<(), AnalyzeError> {
    #[derive(Default)]
    struct UnsupportedSyntax(Option<AnalyzeError>);
    impl Visit for UnsupportedSyntax {
        fn visit_query(&mut self, query: &Query) {
            if self.0.is_some() {
                return;
            }
            if let Some(function) = query.functions.first() {
                self.0 = Some(AnalyzeError::unsupported_expression(
                    "Inline functions are not supported",
                    function.span,
                ));
            } else {
                novarocks_parser::ast::walk_query(self, query);
            }
        }
        fn visit_window_frame(&mut self, frame: &novarocks_parser::ast::WindowFrame) {
            if self.0.is_some() {
                return;
            }
            if let Some(reason) = super::resolve_expr::unsupported_window_frame_reason(frame) {
                self.0 = Some(AnalyzeError::unsupported_expression(reason, frame.span));
                return;
            }
            novarocks_parser::ast::walk_window_frame(self, frame);
        }
        fn visit_expr(&mut self, expression: &Expr) {
            if self.0.is_some() {
                return;
            }
            if let Some(reason) = super::unsupported_control_expression_reason(expression) {
                self.0 = Some(AnalyzeError::unsupported_expression(
                    reason,
                    expression.span(),
                ));
                return;
            }
            match expression {
                Expr::Predicated(node) => {
                    if let Some(reason) = super::unsupported_predicate_reason(node) {
                        self.0 = Some(AnalyzeError::unsupported_expression(reason, node.span));
                        return;
                    }
                    self.visit_expr(&node.value);
                    if let novarocks_parser::ast::Predicate::In(predicate) = &node.predicate
                        && let Expr::InList(list) = &predicate.value_list
                    {
                        for value in &list.values {
                            self.visit_expr(value);
                        }
                    } else {
                        novarocks_parser::ast::walk_predicate(self, &node.predicate);
                    }
                }
                Expr::InList(list) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "IN lists are only supported as an IN predicate value list",
                        list.span,
                    ));
                }
                Expr::JsonConstructor(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "SQL/JSON constructor is not supported",
                        expression.span(),
                    ));
                }
                Expr::JsonExists(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "JSON_EXISTS is not supported",
                        expression.span(),
                    ))
                }
                Expr::JsonValue(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "JSON_VALUE is not supported",
                        expression.span(),
                    ))
                }
                Expr::JsonSerialize(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "JSON_SERIALIZE is not supported",
                        expression.span(),
                    ))
                }
                Expr::IntervalValueExpression(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "INTERVAL value expressions are not supported",
                        expression.span(),
                    ));
                }
                Expr::JsonObject(node) if !super::is_supported_json_object_profile(node) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "JSON_OBJECT clauses are not supported by the existing function",
                        node.span,
                    ));
                }
                Expr::JsonArray(node) if !super::is_supported_json_array_profile(node) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "JSON_ARRAY clauses are not supported by the existing function",
                        node.span,
                    ));
                }
                Expr::JsonQuery(node) if !super::is_plain_json_query(node) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "JSON_QUERY only supports its plain two-argument profile",
                        node.span,
                    ));
                }
                _ => novarocks_parser::ast::walk_expr(self, expression),
            }
        }
        fn visit_type_name(&mut self, data_type: &novarocks_parser::ast::TypeName) {
            if self.0.is_some() {
                return;
            }
            use novarocks_parser::ast::{DataTypeParameter, TypeKind};
            match &data_type.kind {
                TypeKind::ExplicitArray(_) => self.0 = Some(AnalyzeError::unsupported_expression("explicit ARRAY lengths are not supported", data_type.span)),
                TypeKind::Interval(_) => self.0 = Some(AnalyzeError::unsupported_expression("INTERVAL data types are not supported", data_type.span)),
                TypeKind::DateTime(value) if value.with_time_zone || value.precision.as_deref().is_some_and(|parameter| !matches!(parameter, DataTypeParameter::Numeric(number) if number.parsed_value == 6)) => self.0 = Some(AnalyzeError::unsupported_expression("TIME/TIMESTAMP only support microsecond precision without time zone", data_type.span)),
                _ => novarocks_parser::ast::walk_type_name(self, data_type),
            }
        }
        fn visit_literal(&mut self, literal: &novarocks_parser::ast::Literal) {
            if self.0.is_some() {
                return;
            }
            match &literal.kind {
                LiteralKind::Interval(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "INTERVAL literals are not supported",
                        literal.span,
                    ))
                }
                LiteralKind::Generic(value) => {
                    let rejected =
                        novarocks_expression::typing::declaration::validate_typed_string_type_name(
                            &value.type_name,
                            literal.span,
                        );
                    if let Err(error) = rejected {
                        self.0 = Some(super::helpers::type_declaration_error(error));
                    }
                }
                _ => novarocks_parser::ast::walk_literal(self, literal),
            }
        }
        fn visit_query_period(&mut self, period: &ast::QueryPeriod) {
            if self.0.is_some() {
                return;
            }
            if period.start.is_some() || period.end.is_none() {
                self.0 = Some(AnalyzeError::unsupported_expression(
                    "query period requires only an end expression",
                    period.span,
                ));
            } else {
                ast::walk_query_period(self, period);
            }
        }
        fn visit_relation(&mut self, relation: &ast::Relation) {
            if self.0.is_some() {
                return;
            }
            match relation {
                ast::Relation::Aliased(node)
                    if node.column_names.as_ref().is_some_and(Vec::is_empty) =>
                {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "empty relation column aliases are not supported",
                        node.span,
                    ))
                }
                ast::Relation::Table(node) if node.branch.is_some() => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "table branches in FROM are not supported",
                        node.span,
                    ))
                }
                ast::Relation::JsonTable(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "JSON_TABLE is not supported",
                        relation.span(),
                    ))
                }
                ast::Relation::Sampled(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "TABLESAMPLE is not supported",
                        relation.span(),
                    ))
                }
                ast::Relation::Pivot(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "PIVOT is not supported",
                        relation.span(),
                    ))
                }
                ast::Relation::Nearest(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "NEAREST is not supported",
                        relation.span(),
                    ))
                }
                ast::Relation::PatternRecognition(_) => {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "MATCH_RECOGNIZE is not supported",
                        relation.span(),
                    ))
                }
                ast::Relation::TableFunction(node)
                    if !node.copartitioning.is_empty()
                        || node.arguments.iter().any(|argument| {
                            !matches!(
                                argument.value,
                                ast::TableFunctionArgumentValue::Expression(_)
                            )
                        }) =>
                {
                    self.0 = Some(AnalyzeError::unsupported_expression(
                        "table function argument profile is not supported",
                        node.span,
                    ))
                }
                _ => ast::walk_relation(self, relation),
            }
        }
    }
    let mut visitor = UnsupportedSyntax::default();
    visitor.visit_query(query);
    match visitor.0 {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

fn recursive_cte_max_depth(query: &Query) -> Option<usize> {
    query.session_properties.iter().find_map(|property| {
        let [name] = property.name.parts.as_slice() else {
            return None;
        };
        if !name.value.eq_ignore_ascii_case("recursive_cte_max_depth") {
            return None;
        }
        let Expr::Literal(value) = &property.value else {
            return None;
        };
        let LiteralKind::Long {
            parsed_value: value,
            ..
        } = &value.kind
        else {
            return None;
        };
        usize::try_from(*value).ok().map(|depth| depth.max(1))
    })
}

fn root_select(body: &SetExpr) -> Option<&Select> {
    match body {
        SetExpr::Select(select) => Some(select),
        SetExpr::Query(query) => root_select(query.query.body.as_ref()),
        SetExpr::Values(_) | SetExpr::Table(_) | SetExpr::SetOperation(_) => None,
    }
}

fn rewrite_nested_queries(query: &mut Query, max_depth: usize) -> Result<(), AnalyzeError> {
    struct RecursiveUnroller {
        max_depth: usize,
        error: Option<AnalyzeError>,
    }

    impl Fold for RecursiveUnroller {
        fn fold_query(&mut self, mut query: Query) -> Query {
            // The default fold reaches every nested `Query`, including those in
            // expressions and table-function arguments, before this query is
            // unrolled. This preserves the legacy deepest-first traversal.
            query = novarocks_parser::ast::fold_query(self, query);
            if self.error.is_none()
                && let Some(with) = query.with.as_mut()
                && with.recursive
                && let Err(error) = unroll_with_clause(with, self.max_depth)
            {
                self.error = Some(error);
            }
            query
        }
    }

    let mut unroller = RecursiveUnroller {
        max_depth,
        error: None,
    };
    let rewritten = unroller.fold_query(query.clone());
    if let Some(error) = unroller.error {
        return Err(error);
    }
    *query = rewritten;
    Ok(())
}

fn unroll_with_clause(
    with: &mut novarocks_parser::ast::With,
    max_depth: usize,
) -> Result<(), AnalyzeError> {
    let originals = std::mem::take(&mut with.ctes);
    let mut rewritten = Vec::with_capacity(originals.len());
    for cte in originals {
        if let Some(cte) = try_unroll_cte(&cte, max_depth)? {
            rewritten.push(cte);
        } else {
            rewritten.push(cte);
        }
    }
    with.ctes = rewritten.into();
    Ok(())
}

fn try_unroll_cte(
    cte: &novarocks_parser::ast::Cte,
    max_depth: usize,
) -> Result<Option<novarocks_parser::ast::Cte>, AnalyzeError> {
    let name = cte.name.value.to_ascii_lowercase();
    let union_chain = extract_union_chain(cte.query.body.as_ref());
    if matches!(union_chain, Some(Err(())))
        && set_expr_references_table(cte.query.body.as_ref(), &name)
    {
        // ADR-0088: a mixed chain that is actually recursive is known before
        // catalog materialization and therefore cannot fall through to it.
        return Err(AnalyzeError::unsupported_query_shape(
            "unsupported recursive CTE shape: mixed UNION quantifiers",
            cte.span,
        ));
    }
    let Some(Ok((distinct, operands))) = union_chain else {
        return Ok(None);
    };
    let anchor = &operands[0];
    let recursive_parts = &operands[1..];
    if set_expr_references_table(anchor, &name) {
        // ADR-0088: an anchor cannot self-reference. Reject the shape before
        // catalog resolution even when no later UNION operand references it.
        return Err(AnalyzeError::unknown_table(
            format!("unknown table: {}", cte.name.value),
            cte.name.span,
        ));
    }
    let self_references = recursive_parts
        .iter()
        .any(|part| set_expr_references_table(part, &name));
    if !self_references {
        return Ok(None);
    }

    let aliases = derive_column_aliases(cte, anchor)?;
    let mut iterations = Vec::with_capacity(max_depth);
    let mut anchor = anchor.clone();
    pin_projection_aliases(&mut anchor, &aliases);
    iterations.push(query_with_body(cte.query.as_ref(), anchor));

    for index in 1..max_depth {
        let previous = iterations[index - 1].clone();
        let mut parts = recursive_parts.to_vec();
        for part in &mut parts {
            substitute_table_with_derived(part, &name, &previous, &aliases);
            pin_projection_aliases(part, &aliases);
        }
        iterations.push(query_with_body(
            cte.query.as_ref(),
            build_set_operation_chain(distinct, &parts),
        ));
    }

    let branches = iterations
        .into_iter()
        .enumerate()
        .map(|(index, query)| inline_iteration(query, &name, index, &aliases, cte.span))
        .collect::<Vec<_>>();
    let mut rewritten = cte.clone();
    rewritten.query = ast::AstBox::new(query_with_body(
        cte.query.as_ref(),
        build_set_operation_chain(false, &branches),
    ));
    Ok(Some(rewritten))
}

fn extract_union_chain(expr: &SetExpr) -> Option<Result<(bool, Vec<SetExpr>), ()>> {
    fn visit(
        expr: &SetExpr,
        distinct: &mut Option<bool>,
        parts: &mut Vec<SetExpr>,
        mixed: &mut bool,
    ) {
        if let SetExpr::SetOperation(operation) = expr {
            if operation.operator != SetOperator::Union || operation.corresponding.is_some() {
                parts.push(expr.clone());
                return;
            }
            match distinct {
                Some(existing) if *existing != operation.distinct => *mixed = true,
                Some(_) => {}
                None => *distinct = Some(operation.distinct),
            }
            for input in &operation.inputs {
                visit(input, distinct, parts, mixed);
            }
        } else {
            parts.push(expr.clone());
        }
    }

    let mut distinct = None;
    let mut parts = Vec::new();
    let mut mixed = false;
    visit(expr, &mut distinct, &mut parts, &mut mixed);
    if parts.len() < 2 {
        None
    } else if mixed {
        Some(Err(()))
    } else {
        distinct.map(|distinct| Ok((distinct, parts)))
    }
}

fn derive_column_aliases(
    cte: &novarocks_parser::ast::Cte,
    anchor: &SetExpr,
) -> Result<Vec<Ident>, AnalyzeError> {
    if let Some(columns) = &cte.columns {
        if columns.is_empty() {
            return Err(AnalyzeError::unsupported_query_shape(
                "empty CTE column aliases are not supported",
                cte.span,
            ));
        }
        return Ok(columns.clone());
    }
    let Some(select) = root_select(anchor) else {
        return Err(AnalyzeError::invalid_query_shape(
            "recursive CTE anchor must be a SELECT statement",
            anchor.span(),
        ));
    };
    Ok(select
        .projection
        .iter()
        .enumerate()
        .map(|(index, item)| {
            projection_alias(item)
                .unwrap_or_else(|| synthetic_ident(format!("__nr_rec_col_{index}"), cte.span))
        })
        .collect())
}

fn projection_alias(item: &SelectItem) -> Option<Ident> {
    let SelectItem::SingleColumn(column) = item else {
        return None;
    };
    column.alias.clone().or_else(|| {
        ast::identifier_chain_observed(&column.expression, |_| {
            Ok::<(), std::convert::Infallible>(())
        })
        .unwrap_or_else(|never| match never {})
        .map(|chain| chain.last().clone())
    })
}

fn pin_projection_aliases(expr: &mut SetExpr, aliases: &[Ident]) {
    match expr {
        SetExpr::Select(select) => {
            for (item, alias) in select.projection.iter_mut().zip(aliases) {
                if let SelectItem::SingleColumn(column) = item {
                    column.alias = Some(alias.clone());
                    column.span = alias.span;
                }
            }
        }
        SetExpr::Query(query) => pin_projection_aliases(query.query.body.as_mut(), aliases),
        SetExpr::SetOperation(operation) => {
            for input in &mut operation.inputs {
                pin_projection_aliases(input, aliases);
            }
        }
        SetExpr::Values(_) | SetExpr::Table(_) => {}
    }
}

fn query_with_body(template: &Query, body: SetExpr) -> Query {
    Query {
        session_properties: Vec::new().into(),
        functions: Vec::new().into(),
        with: None,
        body: ast::AstBox::new(body),
        modifiers: Default::default(),
        span: template.span,
    }
}

fn build_set_operation_chain(distinct: bool, parts: &[SetExpr]) -> SetExpr {
    let mut parts = parts.iter();
    let first = parts.next().expect("recursive CTE has an anchor").clone();
    parts.fold(first, |left, right| {
        novarocks_parser::build::set_operation(
            left,
            SetOperator::Union,
            distinct,
            right.clone(),
            None,
        )
    })
}

fn inline_iteration(
    query: Query,
    cte_name: &str,
    index: usize,
    aliases: &[Ident],
    span: Span,
) -> SetExpr {
    let mut relation = novarocks_parser::build::alias(
        novarocks_parser::build::derived(query),
        synthetic_ident(format!("__nr_rec_{cte_name}_{index}"), span),
        Some(aliases.to_vec()),
    );
    if let ast::Relation::Aliased(node) = &mut relation {
        node.span = span;
    }
    SetExpr::Select(ast::AstBox::new(Select {
        distinct: false,
        projection: vec![SelectItem::AllColumns(ast::AstBox::new(ast::AllColumns {
            target: None,
            aliases: Vec::new(),
            span,
        }))]
        .into(),
        from: Some(relation),
        selection: None,
        group_by: None,
        having: None,
        windows: ast::AstVec::new(),
        modifiers: Default::default(),
        span,
    }))
}

fn synthetic_ident(value: String, span: Span) -> Ident {
    Ident {
        value,
        quoted: false,
        span,
    }
}

struct TableReferenceVisitor<'a> {
    target: &'a str,
    found: bool,
}

impl Visit for TableReferenceVisitor<'_> {
    fn visit_query(&mut self, query: &Query) {
        scan_query_for_table_reference(self, query);
    }

    fn visit_table(&mut self, table: &ast::Table) {
        let name = &table.name;
        if name.parts.len() == 1 && name.parts[0].value.eq_ignore_ascii_case(self.target) {
            self.found = true;
        }
        novarocks_parser::ast::walk_table(self, table);
    }
}

fn set_expr_references_table(expr: &SetExpr, target: &str) -> bool {
    let mut visitor = TableReferenceVisitor {
        target,
        found: false,
    };
    scan_set_expr_for_table_reference(&mut visitor, expr);
    visitor.found
}

fn scan_query_for_table_reference(visitor: &mut TableReferenceVisitor<'_>, query: &Query) {
    for property in &query.session_properties {
        visitor.visit_expr(&property.value);
    }
    let mut outer_target_visible = true;
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            if outer_target_visible {
                if cte.name.value.eq_ignore_ascii_case(visitor.target) {
                    // A same-name nested CTE owns its definition and every
                    // following reference in this WITH scope. It must not be
                    // attributed to the enclosing recursive CTE anchor.
                    outer_target_visible = false;
                } else {
                    scan_query_for_table_reference(visitor, cte.query.as_ref());
                }
            }
        }
    }
    if !outer_target_visible {
        return;
    }
    scan_set_expr_for_table_reference(visitor, query.body.as_ref());
    novarocks_parser::ast::walk_query_modifiers(visitor, &query.modifiers);
}

fn scan_set_expr_for_table_reference(visitor: &mut TableReferenceVisitor<'_>, expr: &SetExpr) {
    match expr {
        SetExpr::Select(select) => {
            for item in &select.projection {
                match item {
                    SelectItem::SingleColumn(column) => visitor.visit_expr(&column.expression),
                    SelectItem::AllColumns(columns) => {
                        if let Some(target) = &columns.target {
                            visitor.visit_expr(target);
                        }
                    }
                }
            }
            if let Some(relation) = &select.from {
                visitor.visit_relation(relation);
            }
            if let Some(selection) = &select.selection {
                visitor.visit_expr(selection);
            }
            if let Some(group_by) = &select.group_by {
                visitor.visit_group_by(group_by);
            }
            if let Some(having) = &select.having {
                visitor.visit_expr(having);
            }
            for window in &select.windows {
                for expression in &window.specification.partition_by {
                    visitor.visit_expr(expression);
                }
                for order in window
                    .specification
                    .order_by
                    .iter()
                    .flat_map(|order| &order.items)
                {
                    visitor.visit_expr(&order.sort_key);
                }
                if let Some(frame) = &window.specification.window_frame {
                    scan_window_frame_bound_for_table_reference(visitor, &frame.start_bound);
                    if let Some(end_bound) = &frame.end_bound {
                        scan_window_frame_bound_for_table_reference(visitor, end_bound);
                    }
                }
            }
            novarocks_parser::ast::walk_query_modifiers(visitor, &select.modifiers);
        }
        SetExpr::Values(values) => {
            for row in &values.rows {
                visitor.visit_expr(row);
            }
        }
        SetExpr::Query(query) => scan_query_for_table_reference(visitor, &query.query),
        SetExpr::Table(table) => visitor.visit_table(table),
        SetExpr::SetOperation(operation) => {
            for input in &operation.inputs {
                scan_set_expr_for_table_reference(visitor, input);
            }
        }
    }
}

fn scan_window_frame_bound_for_table_reference(
    visitor: &mut TableReferenceVisitor<'_>,
    bound: &novarocks_parser::ast::WindowFrameBound,
) {
    match bound {
        novarocks_parser::ast::WindowFrameBound::Preceding(Some(expression), _)
        | novarocks_parser::ast::WindowFrameBound::Following(Some(expression), _) => {
            visitor.visit_expr(expression)
        }
        _ => {}
    }
}

fn substitute_table_with_derived(
    expr: &mut SetExpr,
    target: &str,
    replacement: &Query,
    aliases: &[Ident],
) {
    struct RecursiveTableSubstituter<'a> {
        target: &'a str,
        replacement: &'a Query,
        aliases: &'a [Ident],
    }

    impl Fold for RecursiveTableSubstituter<'_> {
        fn fold_query(&mut self, mut query: Query) -> Query {
            if let Some(with) = query.with.as_mut() {
                for cte in &mut with.ctes {
                    if cte.name.value.eq_ignore_ascii_case(self.target) {
                        // CTE scope begins at its own declaration. Earlier
                        // sibling CTEs can still reference the outer recursive
                        // source, while this declaration, later siblings, and
                        // the query body resolve the same name locally.
                        return query;
                    }
                    cte.query = ast::AstBox::new(self.fold_query(cte.query.clone().into_inner()));
                }
            }
            novarocks_parser::ast::fold_query(self, query)
        }

        fn fold_relation(&mut self, relation: ast::Relation) -> ast::Relation {
            let target = |table: &ast::Table| {
                table.name.parts.len() == 1
                    && table.name.parts[0].value.eq_ignore_ascii_case(self.target)
            };
            let (alias, span) = match relation {
                ast::Relation::Aliased(node) if matches!(node.relation.as_ref(), ast::Relation::Table(table) if target(table)) =>
                {
                    let node = node.into_inner();
                    (node.alias, node.span)
                }
                ast::Relation::Table(table) if target(&table) => (
                    synthetic_ident(self.target.to_owned(), table.span),
                    table.span,
                ),
                other => return ast::fold_relation(self, other),
            };
            let mut result = novarocks_parser::build::alias(
                novarocks_parser::build::derived(self.replacement.clone()),
                alias,
                Some(self.aliases.to_vec()),
            );
            if let ast::Relation::Aliased(node) = &mut result {
                node.span = span;
            }
            result
        }
    }

    let template = Query {
        session_properties: Vec::new().into(),
        functions: Vec::new().into(),
        with: None,
        body: ast::AstBox::new(expr.clone()),
        modifiers: Default::default(),
        span: expr.span(),
    };
    let mut substituter = RecursiveTableSubstituter {
        target,
        replacement,
        aliases,
    };
    *expr = substituter.fold_query(template).body.into_inner();
}

fn remove_bare_top_level_dual(query: &mut Query) {
    let SetExpr::Select(select) = query.body.as_mut() else {
        return;
    };
    if select.selection.is_some()
        || select.group_by.is_some()
        || select.having.is_some()
        || !select.windows.is_empty()
        || !select.modifiers.is_empty()
        || query.modifiers.order_by.is_some()
        || query.modifiers.limit.is_some()
        || query.modifiers.offset.is_some()
    {
        return;
    }
    let Some(ast::Relation::Table(table)) = &select.from else {
        return;
    };
    let name = &table.name;
    if name.parts.len() == 1
        && name.parts[0].value.eq_ignore_ascii_case("dual")
        && !name.parts[0].quoted
        && table.query_period.is_none()
        && table.branch.is_none()
    {
        select.from = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_parser::ast::SyntaxEq;

    fn parse_query(sql: &str) -> Query {
        novarocks_parser::parser::parse_query(sql).expect("typed query should parse")
    }

    fn inline_function() -> novarocks_parser::ast::FunctionSpecification {
        use novarocks_parser::{SYNTHETIC, ast, build};
        ast::FunctionSpecification {
            name: build::object_name(vec![build::ident("f", false)]).unwrap(),
            parameters: vec![],
            returns: build::simple_type("bigint"),
            characteristics: vec![],
            body: ast::RoutineBody::Sql(ast::ControlStatement::Return(ast::ReturnStatement {
                value: build::integer(1),
                span: SYNTHETIC,
            })),
            span: Span::new(4, 18),
        }
    }

    #[test]
    fn inline_functions_are_rejected_in_every_query_scope_before_catalog_discovery() {
        struct AddFunction;
        impl Fold for AddFunction {
            fn fold_query(&mut self, mut query: Query) -> Query {
                query.functions.push(inline_function());
                query
            }
        }

        // The outer Fold only enters child queries; the root is tested
        // separately. Unknown calls and relations must never win this error.
        for sql in [
            "WITH x AS (SELECT missing_function() FROM missing_table) SELECT * FROM x",
            "SELECT * FROM (SELECT missing_function() FROM missing_table) AS x",
            "SELECT (SELECT missing_function() FROM missing_table)",
            "(SELECT missing_function() FROM missing_table) UNION ALL SELECT 2",
        ] {
            let query = novarocks_parser::ast::fold_query(&mut AddFunction, parse_query(sql));
            assert!(query.functions.is_empty());
            let error = preanalyze(query).unwrap_err();
            assert_eq!(
                error.kind(),
                crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression
            );
            assert!(
                error.to_string().contains("Inline functions"),
                "{sql}: {error}"
            );
        }
        let mut query = parse_query("SELECT missing_function() FROM missing_table");
        query.functions.push(inline_function());
        let error = preanalyze(query).unwrap_err();
        assert_eq!(
            error.kind(),
            crate::analyze_error::AnalyzeErrorKind::UnsupportedExpression
        );
        assert!(error.to_string().contains("Inline functions"));
    }

    #[test]
    fn unrolls_recursive_cte_before_catalog_lookup() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n) SELECT x FROM n",
        );
        let query = preanalyze(query).expect("recursive CTE should unroll");
        let with = query.with.expect("WITH must remain");
        assert_eq!(with.ctes.len(), 1);
        assert!(!set_expr_references_table(
            with.ctes[0].query.body.as_ref(),
            "n"
        ));
    }

    #[test]
    fn recursive_depth_uses_root_property_then_session_then_default() {
        let sql =
            "WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n) SELECT x FROM n";
        let overridden_sql = "WITH SESSION recursive_cte_max_depth = 4 WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n) SELECT x FROM n";
        let session =
            crate::sql_mode::SqlSemanticSettings::default().with_recursive_cte_max_depth(2);
        for (source, semantics, expected_branches) in [
            (sql, session.clone(), 2),
            (overridden_sql, session, 4),
            (sql, crate::sql_mode::SqlSemanticSettings::default(), 5),
        ] {
            let rewritten = preanalyze_with_semantics(parse_query(source), &semantics)
                .expect("recursive CTE should retain finite depth semantics");
            let with = rewritten.with.as_ref().expect("WITH must remain");
            let (_, branches) = extract_union_chain(with.ctes[0].query.body.as_ref())
                .expect("unrolled query has multiple branches")
                .expect("unrolled branches retain UNION ALL");
            assert_eq!(branches.len(), expected_branches);
            assert!(!set_expr_references_table(
                with.ctes[0].query.body.as_ref(),
                "n"
            ));
            let replayed = preanalyze_with_semantics(rewritten.clone(), &semantics)
                .expect("preanalysis remains idempotent");
            assert!(rewritten.syntax_eq(&replayed));
        }
    }

    #[test]
    fn session_depth_does_not_change_recursive_anchor_refusal() {
        let semantics =
            crate::sql_mode::SqlSemanticSettings::default().with_recursive_cte_max_depth(2);
        let query =
            parse_query("WITH RECURSIVE n AS (SELECT * FROM n UNION ALL SELECT 1) SELECT * FROM n");
        let error = preanalyze_with_semantics(query, &semantics)
            .expect_err("session depth does not admit a recursive anchor");
        assert_eq!(error.message(), "unknown table: n");
    }

    #[test]
    fn recursive_anchor_self_reference_fails_before_catalog_lookup() {
        let query =
            parse_query("WITH RECURSIVE n AS (SELECT * FROM n UNION ALL SELECT 1) SELECT * FROM n");
        let error = preanalyze(query).expect_err("recursive anchor must be rejected");
        assert_eq!(error.message(), "unknown table: n");
    }

    #[test]
    fn recursive_anchor_nested_predicate_self_reference_fails_before_catalog_lookup() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 WHERE 1 IN (SELECT 1 FROM n) UNION ALL SELECT 2) SELECT * FROM n",
        );
        let error = preanalyze(query).expect_err("recursive nested predicate must be rejected");
        assert_eq!(error.message(), "unknown table: n");
    }

    #[test]
    fn recursive_anchor_nested_with_cte_body_self_reference_fails_before_catalog_lookup() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 WHERE EXISTS (WITH x AS (SELECT * FROM n) SELECT 1 FROM x) UNION ALL SELECT 2) SELECT * FROM n",
        );
        let error = preanalyze(query).expect_err("recursive nested CTE body must be rejected");
        assert_eq!(error.message(), "unknown table: n");
    }

    #[test]
    fn recursive_anchor_nested_same_name_with_shadows_outer_cte() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 WHERE EXISTS (WITH n AS (SELECT 1) SELECT 1 FROM n) UNION ALL SELECT 2) SELECT * FROM n",
        );
        assert!(preanalyze(query).is_ok());
    }

    #[test]
    fn unrolls_outer_recursive_reference_before_nested_same_name_cte() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n WHERE EXISTS (WITH x AS (SELECT * FROM n), n AS (SELECT 1) SELECT 1 FROM x)) SELECT x FROM n",
        );
        let query = preanalyze(query).expect("recursive CTE should unroll");
        let with = query.with.expect("WITH must remain");
        assert!(!set_expr_references_table(
            with.ctes[0].query.body.as_ref(),
            "n"
        ));
    }

    #[test]
    fn unrolls_recursive_cte_reference_inside_predicate_subquery() {
        let query = parse_query(
            "WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n WHERE EXISTS (SELECT 1 FROM n)) SELECT x FROM n",
        );
        let query = preanalyze(query).expect("recursive CTE should unroll");
        let with = query.with.expect("WITH must remain");
        assert!(!set_expr_references_table(
            with.ctes[0].query.body.as_ref(),
            "n"
        ));
    }

    #[test]
    fn unrolls_recursive_cte_inside_expression_subquery() {
        let query = parse_query(
            "SELECT EXISTS (WITH RECURSIVE n AS (SELECT 1 AS x UNION ALL SELECT x + 1 FROM n) SELECT x FROM n)",
        );
        let query = preanalyze(query).expect("recursive expression subquery should unroll");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        let SelectItem::SingleColumn(projection_column) = &select.projection[0] else {
            panic!("expected EXISTS predicate");
        };
        assert!(projection_column.alias.is_none(), "anonymous projection");
        let Expr::Exists(exists) = &projection_column.expression else {
            panic!("expected EXISTS predicate");
        };
        let Expr::Subquery(subquery) = &exists.subquery else {
            panic!("EXISTS query wrapper")
        };
        let with = subquery
            .query
            .with
            .as_ref()
            .expect("nested WITH must remain");
        assert!(!set_expr_references_table(
            with.ctes[0].query.body.as_ref(),
            "n"
        ));
    }

    #[test]
    fn removes_only_bare_top_level_dual() {
        let query = preanalyze(parse_query("SELECT 1 FROM dual")).expect("prepass");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        assert!(select.from.is_none());

        let query = preanalyze(parse_query("SELECT 1 FROM dual WHERE 1 = 1")).expect("prepass");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        assert!(select.from.is_some());
    }

    #[test]
    fn inner_join_keeps_constraint_during_prepass() {
        let query = novarocks_parser::parser::parse_query("SELECT * FROM a JOIN b ON a.id = b.id")
            .expect("parse an inner join");
        let query = preanalyze(query).expect("prepass");
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select");
        };
        let Some(ast::Relation::Join(join)) = &select.from else {
            panic!("expected join");
        };
        assert_eq!(join.kind, ast::JoinType::Inner);
        assert!(matches!(join.criteria, Some(ast::JoinCriteria::On(_))));
    }
}

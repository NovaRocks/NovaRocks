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

//! Canonical SQL rendering for the parser-owned syntax tree.

use crate::ast::*;

/// Renders parser AST nodes into one stable SQL spelling.
///
/// The printer intentionally keeps identifier spelling and numeric literal
/// spelling supplied by the AST. Those are syntax facts, while identifier
/// comparison and numeric normalization belong to later semantic owners.
#[derive(Default)]
pub struct Printer {
    output: String,
}

impl Printer {
    /// Creates an empty canonical SQL printer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Renders one top-level statement.
    pub fn statement(mut self, statement: &Statement) -> String {
        self.write_statement(statement);
        self.output
    }

    /// Renders a semicolon-separated statement sequence.
    pub fn statements(mut self, statements: &[Statement]) -> String {
        for (index, statement) in statements.iter().enumerate() {
            if index != 0 {
                self.output.push_str("; ");
            }
            self.write_statement(statement);
        }
        self.output
    }

    /// Renders one expression.
    pub fn expression(mut self, expression: &Expr) -> String {
        self.write_expr(expression);
        self.output
    }

    /// Renders a qualified object name.
    pub fn object_name(mut self, name: &ObjectName) -> String {
        self.write_object_name(name);
        self.output
    }

    /// Renders a syntax-level type name.
    pub fn type_name(mut self, type_name: &TypeName) -> String {
        self.write_type_name(type_name);
        self.output
    }

    fn write_statement(&mut self, statement: &Statement) {
        match statement {
            Statement::ShowProcessList(statement) => {
                crate::ast::legacy::session::write_show_process_list(statement, &mut self.output)
            }
            Statement::Statistics(statement) => {
                crate::ast::legacy::statistics::write_sql(statement, &mut self.output)
            }
            Statement::Catalog(statement) => {
                crate::ast::legacy::catalog::write_sql(statement, &mut self.output)
            }
            Statement::Iceberg(statement) => {
                crate::ast::legacy::iceberg::write_sql(statement, &mut self.output)
            }
            Statement::Maintenance(statement) => {
                crate::ast::legacy::maintenance::write_sql(statement, &mut self.output)
            }
            Statement::MaterializedView(statement) => {
                crate::ast::legacy::materialized_view::write_sql(statement, &mut self.output)
            }
            Statement::View(statement) => {
                crate::ast::legacy::view::write_sql(statement, &mut self.output)
            }
            Statement::Table(statement) => {
                crate::ast::legacy::table::write_sql(statement, &mut self.output)
            }
            Statement::Dml(statement) => {
                crate::ast::legacy::dml::write_sql(statement, &mut self.output)
            }
            Statement::Session(statement) => self.write_session_statement(statement),
            Statement::Query(query) => self.write_query(query),
            Statement::ExplainQuery(explain) => self.write_explain_query(explain),
        }
    }

    fn write_session_statement(&mut self, statement: &SessionStatement) {
        match statement {
            SessionStatement::Set(statement) => {
                self.output.push_str("SET ");
                for (index, assignment) in statement.assignments.iter().enumerate() {
                    if index != 0 {
                        self.output.push_str(", ");
                    }
                    match assignment.scope {
                        SetScope::Default => {}
                        SetScope::Session => self.output.push_str("SESSION "),
                        SetScope::Local => self.output.push_str("LOCAL "),
                        SetScope::Global => self.output.push_str("GLOBAL "),
                    }
                    self.write_ident(&assignment.name);
                    self.output.push_str(" = ");
                    self.write_expr(&assignment.value);
                }
            }
            SessionStatement::Use(statement) => {
                self.output.push_str("USE ");
                if let Some(catalog) = &statement.catalog {
                    self.write_ident(catalog);
                    self.output.push('.');
                }
                self.write_ident(&statement.database);
            }
            SessionStatement::Kill(statement) => {
                self.output.push_str("KILL");
                match statement.kind {
                    KillKind::Default => {}
                    KillKind::Query => self.output.push_str(" QUERY"),
                    KillKind::Connection => self.output.push_str(" CONNECTION"),
                }
                self.output.push(' ');
                self.write_literal(&statement.connection_id);
            }
            SessionStatement::TransactionControl(statement) => {
                self.output.push_str(statement.kind.sql());
            }
        }
    }

    fn write_explain_query(&mut self, explain: &ExplainQuery) {
        self.output.push_str("EXPLAIN");
        if explain.logical {
            self.output.push_str(" LOGICAL");
        }
        match explain.format {
            ExplainFormat::Default => {}
            ExplainFormat::Analyze => self.output.push_str(" ANALYZE"),
            ExplainFormat::Verbose => self.output.push_str(" VERBOSE"),
            ExplainFormat::Costs => self.output.push_str(" COSTS"),
            ExplainFormat::Logical => self.output.push_str(" LOGICAL"),
            ExplainFormat::Contract => self.output.push_str(" CONTRACT"),
        }
        self.output.push(' ');
        self.write_query(&explain.query);
    }

    fn write_query(&mut self, query: &Query) {
        self.write_query_with_properties(query, &query.session_properties);
    }

    #[recursive::recursive]

    fn write_query_with_properties(&mut self, query: &Query, properties: &[SessionProperty]) {
        let mut body = query.body.as_ref();
        while let SetExpr::Query(inner) = body {
            body = &inner.query.body;
        }
        let legacy_select = query.functions.is_empty()
            && matches!(body, SetExpr::Select(_))
            && properties
                .iter()
                .all(|property| property.name.parts.len() == 1);
        // Query facts with no legacy SELECT spelling still retain their full
        // query prefix. The new grammar owns parsing those shapes at cutover.
        let properties = if !properties.is_empty() && !legacy_select {
            self.output.push_str("WITH SESSION ");
            self.write_session_properties(properties);
            self.output.push(' ');
            &[][..]
        } else {
            properties
        };
        if !query.functions.is_empty() {
            self.write_inline_functions(&query.functions);
            self.output.push(' ');
        }
        if let Some(with) = &query.with {
            self.write_with(with);
            self.output.push(' ');
        }
        self.write_set_expr_with_properties(&query.body, properties);
        self.write_query_modifiers(&query.modifiers);
    }

    fn write_query_modifiers(&mut self, modifiers: &QueryModifiers) {
        if let Some(order_by) = &modifiers.order_by {
            self.output.push_str(" ORDER BY ");
            self.write_order_by_list(&order_by.items);
        }
        if let Some(offset) = &modifiers.offset {
            self.output.push_str(" OFFSET ");
            self.write_expr(&offset.row_count);
            self.output.push_str(" ROWS");
        }
        match &modifiers.limit {
            Some(QueryLimit::Limit(node)) => {
                self.output.push_str(" LIMIT ");
                self.write_expr(&node.row_count);
            }
            Some(QueryLimit::FetchFirst(node)) => self.write_fetch_first(node),
            None => {}
        }
    }

    fn write_with(&mut self, with: &With) {
        self.output.push_str("WITH");
        if with.recursive {
            self.output.push_str(" RECURSIVE");
        }
        self.output.push(' ');
        for (index, cte) in with.ctes.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_ident(&cte.name);
            if let Some(columns) = &cte.columns {
                self.output.push('(');
                self.write_ident_list(columns);
                self.output.push(')');
            }
            self.output.push_str(" AS (");
            self.write_query(&cte.query);
            self.output.push(')');
        }
    }

    fn write_set_expr_with_properties(
        &mut self,
        expression: &SetExpr,
        properties: &[SessionProperty],
    ) {
        match expression {
            SetExpr::Select(select) => self.write_select_with_properties(select, properties),
            SetExpr::Query(query) => {
                self.output.push('(');
                self.write_query_with_properties(&query.query, properties);
                self.output.push(')');
            }
            _ => self.write_set_expr(expression),
        }
    }

    #[recursive::recursive]
    fn write_set_expr(&mut self, expression: &SetExpr) {
        match expression {
            SetExpr::Select(select) => self.write_select(select),
            SetExpr::Values(values) => self.write_values(values),
            SetExpr::Table(table) => {
                self.output.push_str("TABLE ");
                self.write_table(table);
            }
            SetExpr::Query(query) => {
                self.output.push('(');
                self.write_query(&query.query);
                self.output.push(')');
            }
            SetExpr::SetOperation(operation) => {
                if let Some(first) = operation.inputs.first() {
                    self.write_set_expr(first);
                }
                for input in operation.inputs.iter().skip(1) {
                    self.output.push(' ');
                    self.output.push_str(match operation.operator {
                        SetOperator::Union => "UNION",
                        SetOperator::Intersect => "INTERSECT",
                        SetOperator::Except => "EXCEPT",
                    });
                    if !operation.distinct {
                        self.output.push_str(" ALL");
                    }
                    if let Some(corresponding) = &operation.corresponding {
                        self.output.push_str(" CORRESPONDING");
                        if !corresponding.columns.is_empty() {
                            self.output.push_str(" BY (");
                            self.write_ident_list(&corresponding.columns);
                            self.output.push(')');
                        }
                    }
                    self.output.push(' ');
                    self.write_set_right_operand(input);
                }
            }
        }
    }

    fn write_set_right_operand(&mut self, expression: &SetExpr) {
        if matches!(expression, SetExpr::SetOperation(_)) {
            self.output.push('(');
            self.write_set_expr(expression);
            self.output.push(')');
        } else {
            self.write_set_expr(expression);
        }
    }

    #[recursive::recursive]
    fn write_values(&mut self, values: &Values) {
        self.output.push_str("VALUES ");
        for (index, row) in values.rows.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            match row {
                Expr::Row(row) if row.fields.iter().all(|field| field.name.is_none()) => {
                    self.output.push('(');
                    for (index, field) in row.fields.iter().enumerate() {
                        if index != 0 {
                            self.output.push_str(", ");
                        }
                        self.write_expr(&field.expression);
                    }
                    self.output.push(')');
                }
                other => self.write_expr(other),
            }
        }
    }

    fn write_select(&mut self, select: &Select) {
        self.write_select_with_properties(select, &[]);
    }

    fn write_select_with_properties(&mut self, select: &Select, properties: &[SessionProperty]) {
        self.output.push_str("SELECT");
        if !properties.is_empty() {
            self.output.push_str(" /*+ SET_VAR(");
            self.write_session_properties(properties);
            self.output.push_str(") */");
        }
        if select.distinct {
            self.output.push_str(" DISTINCT");
        }
        if !select.projection.is_empty() {
            self.output.push(' ');
            for (index, item) in select.projection.iter().enumerate() {
                if index != 0 {
                    self.output.push_str(", ");
                }
                self.write_select_item(item);
            }
        }
        if let Some(relation) = &select.from {
            self.output.push_str(" FROM ");
            self.write_relation(relation);
        }
        if let Some(selection) = &select.selection {
            self.output.push_str(" WHERE ");
            self.write_expr(selection);
        }
        if let Some(group_by) = &select.group_by {
            self.write_group_by(group_by);
        }
        if let Some(having) = &select.having {
            self.output.push_str(" HAVING ");
            self.write_expr(having);
        }
        if let Some(qualify) = &select.legacy.qualify {
            self.output.push_str(" QUALIFY ");
            self.write_expr(qualify);
        }
        if !select.windows.is_empty() {
            self.output.push_str(" WINDOW ");
            for (index, named) in select.windows.iter().enumerate() {
                if index != 0 {
                    self.output.push_str(", ");
                }
                self.write_ident(&named.name);
                self.output.push_str(" AS ");
                self.write_window_specification(&named.specification);
            }
        }
        self.write_query_modifiers(&select.modifiers);
    }

    fn write_session_properties(&mut self, properties: &[SessionProperty]) {
        for (index, property) in properties.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_object_name(&property.name);
            self.output.push_str(" = ");
            self.write_expr(&property.value);
        }
    }

    #[recursive::recursive]
    fn write_select_item(&mut self, item: &SelectItem) {
        match item {
            SelectItem::SingleColumn(node) => self.write_single_column(node),
            SelectItem::AllColumns(node) => self.write_all_columns(node),
        }
    }
    #[recursive::recursive]
    fn write_single_column(&mut self, node: &SingleColumn) {
        self.write_expr(&node.expression);
        if let Some(alias) = &node.alias {
            self.output.push_str(" AS ");
            self.write_ident(alias);
        }
    }
    #[recursive::recursive]
    fn write_all_columns(&mut self, node: &AllColumns) {
        if let Some(target) = &node.target {
            let parenthesize = !matches!(
                target,
                Expr::Identifier(_)
                    | Expr::Dereference(_)
                    | Expr::LegacyParenthesized(_)
                    | Expr::FunctionCall(_)
                    | Expr::Subscript(_)
            );
            if parenthesize {
                self.output.push('(');
            }
            self.write_expr(target);
            if parenthesize {
                self.output.push(')');
            }
            self.output.push('.');
        }
        self.output.push('*');
        if !node.aliases.is_empty() {
            self.output.push_str(" AS (");
            self.write_ident_list(&node.aliases);
            self.output.push(')');
        }
    }

    #[recursive::recursive]
    fn write_group_by(&mut self, group_by: &GroupBy) {
        self.output.push_str(" GROUP BY ");
        if group_by.distinct {
            self.output.push_str("DISTINCT ");
        }
        for (index, element) in group_by.elements.iter().enumerate() {
            if index > 0 {
                self.output.push_str(", ");
            }
            match element {
                GroupingElement::Simple(node) => {
                    let parenthesized = node.columns.len() != 1;
                    if parenthesized {
                        self.output.push('(');
                    }
                    self.write_expr_list(&node.columns);
                    if parenthesized {
                        self.output.push(')');
                    }
                }
                GroupingElement::Sets(node) => {
                    self.output.push_str(match node.kind {
                        GroupingSetsKind::Explicit => "GROUPING SETS (",
                        GroupingSetsKind::Rollup => "ROLLUP (",
                        GroupingSetsKind::Cube => "CUBE (",
                    });
                    for (index, set) in node.sets.iter().enumerate() {
                        if index > 0 {
                            self.output.push_str(", ");
                        }
                        let parenthesized =
                            node.kind == GroupingSetsKind::Explicit || set.len() != 1;
                        if parenthesized {
                            self.output.push('(');
                        }
                        self.write_expr_list(set);
                        if parenthesized {
                            self.output.push(')');
                        }
                    }
                    self.output.push(')');
                }
                GroupingElement::Auto(_) => self.output.push_str("AUTO"),
            }
        }
    }

    fn write_order_by_list(&mut self, order_by: &[SortItem]) {
        for (index, order) in order_by.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_sort_item(order);
        }
    }

    #[recursive::recursive]
    fn write_sort_item(&mut self, item: &SortItem) {
        self.write_expr(&item.sort_key);
        self.output.push_str(match item.ordering {
            SortOrdering::Ascending => " ASC",
            SortOrdering::Descending => " DESC",
        });
        match item.null_ordering {
            NullOrdering::First => self.output.push_str(" NULLS FIRST"),
            NullOrdering::Last => self.output.push_str(" NULLS LAST"),
            NullOrdering::Undefined => {}
        }
    }

    fn write_fetch_first(&mut self, fetch: &FetchFirst) {
        self.output.push_str(" FETCH FIRST");
        if let Some(count) = &fetch.row_count {
            self.output.push(' ');
            self.write_expr(count);
        }
        self.output.push_str(" ROWS");
        self.output.push_str(if fetch.with_ties {
            " WITH TIES"
        } else {
            " ONLY"
        });
    }

    #[recursive::recursive]
    fn write_relation(&mut self, relation: &Relation) {
        match relation {
            Relation::Table(node) => self.write_table(node),
            Relation::TableSubquery(node) => {
                self.output.push('(');
                self.write_query(&node.query);
                self.output.push(')');
            }
            Relation::Lateral(node) => {
                self.output.push_str("LATERAL (");
                self.write_query(&node.query);
                self.output.push(')');
            }
            Relation::Aliased(node) => {
                let parentheses = matches!(node.relation.as_ref(), Relation::Join(_));
                if parentheses {
                    self.output.push('(');
                }
                self.write_relation(&node.relation);
                if parentheses {
                    self.output.push(')');
                }
                self.output.push_str(" AS ");
                self.write_ident(&node.alias);
                if let Some(columns) = &node.column_names {
                    self.output.push('(');
                    self.write_ident_list(columns);
                    self.output.push(')');
                }
            }
            Relation::Unnest(node) => {
                self.output.push_str("UNNEST(");
                self.write_expr_list(&node.expressions);
                self.output.push(')');
                if node.with_ordinality {
                    self.output.push_str(" WITH ORDINALITY");
                }
            }
            Relation::Join(node) => self.write_relation_join(node),
            Relation::Sampled(node) => {
                self.write_relation_suffix(&node.relation);
                self.output.push_str(match node.kind {
                    SamplingType::Bernoulli => " TABLESAMPLE BERNOULLI (",
                    SamplingType::System => " TABLESAMPLE SYSTEM (",
                });
                self.write_expr(&node.sample_percentage);
                self.output.push(')');
            }
            Relation::Nearest(node) => {
                self.output.push_str("NEAREST (FROM ");
                self.write_relation(&node.relation);
                if let Some(expr) = &node.where_clause {
                    self.output.push_str(" WHERE ");
                    self.write_expr(expr);
                }
                self.output.push_str(" MATCH ");
                self.write_expr(&node.match_expression);
                self.output.push(')');
            }
            Relation::Pivot(node) => self.write_pivot(node),
            Relation::TableFunction(node) => self.write_table_function(node),
            Relation::JsonTable(node) => self.write_json_table(node),
            Relation::PatternRecognition(node) => self.write_pattern_recognition(node),
            Relation::LegacyTableFunction(node) => {
                if node.lateral {
                    self.output.push_str("LATERAL ");
                }
                match node.syntax {
                    crate::ast::legacy::TableFunctionSyntax::TableWrapper => {
                        self.output.push_str("TABLE(");
                        self.write_expr(&node.expr);
                        self.output.push(')');
                    }
                    crate::ast::legacy::TableFunctionSyntax::BareCall => {
                        self.write_expr(&node.expr)
                    }
                }
            }
        }
    }
    #[recursive::recursive]
    fn write_table(&mut self, node: &Table) {
        self.write_object_name(&node.name);
        if let Some(period) = &node.query_period {
            self.output.push_str(match period.range_type {
                QueryPeriodRangeType::Timestamp => " FOR SYSTEM_TIME AS OF ",
                QueryPeriodRangeType::Version => " FOR VERSION AS OF ",
            });
            assert!(
                period.start.is_none(),
                "query period start has no public Trino constructor"
            );
            self.write_expr(
                period
                    .end
                    .as_ref()
                    .expect("query period requires its actual end expression"),
            );
        }
        if let Some(branch) = &node.branch {
            self.output.push_str(" @ ");
            self.write_ident(branch);
        }
    }
    #[recursive::recursive]
    fn write_relation_suffix(&mut self, relation: &Relation) {
        let parens = matches!(
            relation,
            Relation::Aliased(_)
                | Relation::Sampled(_)
                | Relation::PatternRecognition(_)
                | Relation::Pivot(_)
                | Relation::Join(_)
        );
        if parens {
            self.output.push('(');
        }
        self.write_relation(relation);
        if parens {
            self.output.push(')');
        }
    }
    #[recursive::recursive]
    fn write_join_operand(&mut self, relation: &Relation, parent: JoinType, right: bool) {
        let parentheses = matches!(relation,Relation::Join(child) if if parent==JoinType::Implicit {right && child.kind==JoinType::Implicit} else {right || child.kind==JoinType::Implicit});
        if parentheses {
            self.output.push('(');
        }
        self.write_relation(relation);
        if parentheses {
            self.output.push(')');
        }
    }
    #[recursive::recursive]
    fn write_relation_join(&mut self, node: &Join) {
        self.write_join_operand(&node.left, node.kind, false);
        if node.kind == JoinType::Implicit {
            self.output.push_str(", ");
            self.write_join_operand(&node.right, node.kind, true);
            return;
        }
        self.output.push(' ');
        if matches!(node.criteria, Some(JoinCriteria::Natural(_))) {
            self.output.push_str("NATURAL ");
        }
        self.output.push_str(match node.kind {
            JoinType::Cross => "CROSS JOIN ",
            JoinType::Inner => "JOIN ",
            JoinType::Left => "LEFT JOIN ",
            JoinType::Right => "RIGHT JOIN ",
            JoinType::Full => "FULL JOIN ",
            JoinType::Implicit => unreachable!(),
            JoinType::Legacy(operator) => match operator {
                crate::ast::legacy::JoinOperator::Inner => "JOIN ",
                crate::ast::legacy::JoinOperator::InnerExplicit => "INNER JOIN ",
                crate::ast::legacy::JoinOperator::LeftOuter => "LEFT JOIN ",
                crate::ast::legacy::JoinOperator::LeftOuterExplicit => "LEFT OUTER JOIN ",
                crate::ast::legacy::JoinOperator::RightOuter => "RIGHT JOIN ",
                crate::ast::legacy::JoinOperator::RightOuterExplicit => "RIGHT OUTER JOIN ",
                crate::ast::legacy::JoinOperator::FullOuter => "FULL JOIN ",
                crate::ast::legacy::JoinOperator::FullOuterExplicit => "FULL OUTER JOIN ",
                crate::ast::legacy::JoinOperator::Cross => "CROSS JOIN ",
                crate::ast::legacy::JoinOperator::LeftSemi => "LEFT SEMI JOIN ",
                crate::ast::legacy::JoinOperator::RightSemi => "RIGHT SEMI JOIN ",
                crate::ast::legacy::JoinOperator::LeftAnti => "LEFT ANTI JOIN ",
                crate::ast::legacy::JoinOperator::RightAnti => "RIGHT ANTI JOIN ",
            },
        });
        self.write_join_operand(&node.right, node.kind, true);
        match &node.criteria {
            Some(JoinCriteria::On(n)) => {
                self.output.push_str(" ON ");
                self.write_expr(&n.expression);
            }
            Some(JoinCriteria::Using(n)) => {
                self.output.push_str(" USING (");
                self.write_ident_list(&n.columns);
                self.output.push(')');
            }
            _ => {}
        }
    }
    #[recursive::recursive]
    fn write_table_function(&mut self, node: &TableFunctionInvocation) {
        self.output.push_str("TABLE(");
        self.write_object_name(&node.name);
        self.output.push('(');
        for (index, argument) in node.arguments.iter().enumerate() {
            if index > 0 {
                self.output.push_str(", ");
            }
            if let Some(name) = &argument.name {
                self.write_ident(name);
                self.output.push_str(" => ");
            }
            match &argument.value {
                TableFunctionArgumentValue::Expression(expr) => self.write_expr(expr),
                TableFunctionArgumentValue::Descriptor(node) => {
                    if let Some(descriptor) = &node.descriptor {
                        self.output.push_str("DESCRIPTOR(");
                        for (index, field) in descriptor.fields.iter().enumerate() {
                            if index > 0 {
                                self.output.push_str(", ");
                            }
                            self.write_ident(&field.name);
                            if let Some(ty) = &field.data_type {
                                self.output.push(' ');
                                self.write_type_name(ty);
                            }
                        }
                        self.output.push(')');
                    } else {
                        self.output.push_str("CAST(NULL AS DESCRIPTOR)");
                    }
                }
                TableFunctionArgumentValue::Table(node) => self.write_table_argument(node),
            }
        }
        if !node.copartitioning.is_empty() {
            self.output.push_str(" COPARTITION ");
            for (index, group) in node.copartitioning.iter().enumerate() {
                if index > 0 {
                    self.output.push_str(", ");
                }
                self.output.push('(');
                for (index, name) in group.iter().enumerate() {
                    if index > 0 {
                        self.output.push_str(", ");
                    }
                    self.write_object_name(name);
                }
                self.output.push(')');
            }
        }
        self.output.push_str("))");
    }
    #[recursive::recursive]
    fn write_table_argument(&mut self, node: &TableFunctionTableArgument) {
        let (relation, alias) = match node.table.as_ref() {
            Relation::Aliased(alias) => (alias.relation.as_ref(), Some(alias.as_ref())),
            other => (other, None),
        };
        self.output.push_str("TABLE(");
        match relation {
            Relation::TableSubquery(query) => self.write_query(&query.query),
            other => self.write_relation(other),
        }
        self.output.push(')');
        if let Some(alias) = alias {
            self.output.push_str(" AS ");
            self.write_ident(&alias.alias);
            if let Some(columns) = &alias.column_names {
                self.output.push('(');
                self.write_ident_list(columns);
                self.output.push(')');
            }
        }
        if let Some(partition) = &node.partition_by {
            self.output.push_str(" PARTITION BY ");
            if partition.is_empty() {
                self.output.push_str("()");
            } else {
                self.write_expr_list(partition);
            }
        }
        if let Some(empty) = &node.empty_table_treatment {
            self.output.push_str(match empty.treatment {
                EmptyTableTreatmentKind::Keep => " KEEP WHEN EMPTY",
                EmptyTableTreatmentKind::Prune => " PRUNE WHEN EMPTY",
            });
        }
        if let Some(order) = &node.order_by {
            self.output.push_str(" ORDER BY ");
            self.write_order_by_list(&order.items);
        }
    }
    #[recursive::recursive]
    fn write_pivot(&mut self, node: &Pivot) {
        self.write_relation_suffix(&node.input);
        self.output.push_str(" PIVOT (");
        for (index, aggregation) in node.aggregations.iter().enumerate() {
            if index > 0 {
                self.output.push_str(", ");
            }
            self.write_expr(&aggregation.expression);
            if let Some(alias) = &aggregation.alias {
                self.output.push_str(" AS ");
                self.write_ident(alias);
            }
        }
        self.output.push_str(" FOR ");
        let multi = node.pivot_columns.len() != 1;
        if multi {
            self.output.push('(');
        }
        self.write_expr_list(&node.pivot_columns);
        if multi {
            self.output.push(')');
        }
        self.output.push_str(" IN (");
        for (index, group) in node.value_groups.iter().enumerate() {
            if index > 0 {
                self.output.push_str(", ");
            }
            let multi = group.values.len() != 1;
            if multi {
                self.output.push('(');
            }
            self.write_expr_list(&group.values);
            if multi {
                self.output.push(')');
            }
            if let Some(alias) = &group.alias {
                self.output.push_str(" AS ");
                self.write_ident(alias);
            }
        }
        self.output.push(')');
        if let Some(group) = &node.group_by {
            self.write_group_by(group);
        }
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_pattern_recognition(&mut self, node: &PatternRecognitionRelation) {
        self.write_relation_suffix(&node.input);
        self.output.push_str(" MATCH_RECOGNIZE (");
        if !node.partition_by.is_empty() {
            self.output.push_str("PARTITION BY ");
            self.write_expr_list(&node.partition_by);
            self.output.push(' ');
        }
        if let Some(order) = &node.order_by {
            self.output.push_str("ORDER BY ");
            self.write_order_by_list(&order.items);
            self.output.push(' ');
        }
        if !node.measures.is_empty() {
            self.output.push_str("MEASURES ");
            for (index, measure) in node.measures.iter().enumerate() {
                if index > 0 {
                    self.output.push_str(", ");
                }
                self.write_expr(&measure.expression);
                self.output.push_str(" AS ");
                self.write_ident(&measure.name);
            }
            self.output.push(' ');
        }
        if let Some(rows) = node.rows_per_match {
            self.output.push_str(match rows {
                RowsPerMatch::One => "ONE ROW PER MATCH ",
                RowsPerMatch::AllShowEmpty => "ALL ROWS PER MATCH SHOW EMPTY MATCHES ",
                RowsPerMatch::AllOmitEmpty => "ALL ROWS PER MATCH OMIT EMPTY MATCHES ",
                RowsPerMatch::AllWithUnmatched => "ALL ROWS PER MATCH WITH UNMATCHED ROWS ",
                RowsPerMatch::Window => {
                    unreachable!("WINDOW rows-per-match cannot occur in MATCH_RECOGNIZE")
                }
            });
        }
        if let Some(skip) = &node.after_match_skip_to {
            self.write_skip_to(skip);
            self.output.push(' ');
        }
        if let Some(mode) = &node.pattern_search_mode {
            self.output.push_str(match mode.mode {
                PatternSearchModeKind::Initial => "INITIAL ",
                PatternSearchModeKind::Seek => "SEEK ",
            });
        }
        self.output.push_str("PATTERN (");
        self.write_row_pattern(&node.pattern);
        self.output.push(')');
        if !node.subsets.is_empty() {
            self.output.push_str(" SUBSET ");
            for (index, subset) in node.subsets.iter().enumerate() {
                if index > 0 {
                    self.output.push_str(", ");
                }
                self.write_ident(&subset.name);
                self.output.push_str(" = (");
                self.write_ident_list(&subset.identifiers);
                self.output.push(')');
            }
        }
        self.output.push_str(" DEFINE ");
        for (index, definition) in node.variable_definitions.iter().enumerate() {
            if index > 0 {
                self.output.push_str(", ");
            }
            self.write_ident(&definition.name);
            self.output.push_str(" AS ");
            self.write_expr(&definition.expression);
        }
        self.output.push(')');
    }

    #[recursive::recursive]
    fn write_window_specification(&mut self, specification: &WindowSpecification) {
        self.output.push('(');
        let mut needs_separator = false;
        if let Some(name) = &specification.existing_window_name {
            self.write_ident(name);
            needs_separator = true;
        }
        if !specification.partition_by.is_empty() {
            if needs_separator {
                self.output.push(' ');
            }
            self.output.push_str("PARTITION BY ");
            self.write_expr_list(&specification.partition_by);
            needs_separator = true;
        }
        if let Some(order_by) = &specification.order_by {
            if needs_separator {
                self.output.push(' ');
            }
            self.output.push_str("ORDER BY ");
            self.write_order_by_list(&order_by.items);
            needs_separator = true;
        }
        if let Some(frame) = &specification.window_frame {
            if needs_separator {
                self.output.push(' ');
            }
            self.write_window_frame(frame);
        }
        self.output.push(')');
    }

    #[recursive::recursive]
    fn write_window(&mut self, window: &Window) {
        match window {
            Window::Reference(node) => self.write_ident(&node.name),
            Window::Specification(node) => self.write_window_specification(node),
        }
    }
    fn write_skip_to(&mut self, skip: &SkipTo) {
        match skip {
            SkipTo::PastLast { .. } => self.output.push_str("AFTER MATCH SKIP PAST LAST ROW"),
            SkipTo::Next { .. } => self.output.push_str("AFTER MATCH SKIP TO NEXT ROW"),
            SkipTo::First { identifier, .. } => {
                self.output.push_str("AFTER MATCH SKIP TO FIRST ");
                self.write_ident(identifier);
            }
            SkipTo::Last { identifier, .. } => {
                self.output.push_str("AFTER MATCH SKIP TO LAST ");
                self.write_ident(identifier);
            }
        }
    }
    #[recursive::recursive]
    fn write_row_pattern(&mut self, pattern: &RowPattern) {
        match pattern {
            RowPattern::Variable { name, .. } => self.write_ident(name),
            RowPattern::Empty { .. } => self.output.push_str("()"),
            RowPattern::Anchor { kind, .. } => self.output.push_str(match kind {
                PatternAnchorKind::PartitionStart => "^",
                PatternAnchorKind::PartitionEnd => "$",
            }),
            RowPattern::Alternation { patterns, .. }
            | RowPattern::Concatenation { patterns, .. }
            | RowPattern::Permutation { patterns, .. } => {
                let (prefix, separator) = match pattern {
                    RowPattern::Alternation { .. } => ("(", " | "),
                    RowPattern::Concatenation { .. } => ("(", " "),
                    _ => ("PERMUTE(", ", "),
                };
                self.output.push_str(prefix);
                for (index, child) in patterns.iter().enumerate() {
                    if index != 0 {
                        self.output.push_str(separator);
                    }
                    self.write_row_pattern(child);
                }
                self.output.push(')');
            }
            RowPattern::Excluded { pattern, .. } => {
                self.output.push_str("{-");
                self.write_row_pattern(pattern);
                self.output.push_str("-}");
            }
            RowPattern::Quantified {
                pattern,
                quantifier,
                ..
            } => {
                self.output.push('(');
                self.write_row_pattern(pattern);
                self.write_pattern_quantifier(quantifier);
                self.output.push(')');
            }
        }
    }
    fn write_pattern_quantifier(&mut self, quantifier: &PatternQuantifier) {
        let greedy = match quantifier {
            PatternQuantifier::ZeroOrMore { greedy, .. } => {
                self.output.push('*');
                *greedy
            }
            PatternQuantifier::OneOrMore { greedy, .. } => {
                self.output.push('+');
                *greedy
            }
            PatternQuantifier::ZeroOrOne { greedy, .. } => {
                self.output.push('?');
                *greedy
            }
            PatternQuantifier::Range {
                greedy,
                at_least,
                at_most,
                ..
            } => {
                self.output.push('{');
                if let Some(value) = at_least {
                    self.output.push_str(&value.lexeme);
                }
                self.output.push(',');
                if let Some(value) = at_most {
                    self.output.push_str(&value.lexeme);
                }
                self.output.push('}');
                *greedy
            }
        };
        if !greedy {
            self.output.push('?');
        }
    }

    #[recursive::recursive]
    fn write_window_frame(&mut self, frame: &WindowFrame) {
        if !frame.measures.is_empty() {
            self.output.push_str("MEASURES ");
            for (index, measure) in frame.measures.iter().enumerate() {
                if index != 0 {
                    self.output.push_str(", ");
                }
                self.write_expr(&measure.expression);
                self.output.push_str(" AS ");
                self.write_ident(&measure.name);
            }
            self.output.push(' ');
        }
        self.output.push_str(match frame.units {
            WindowFrameUnits::Rows => "ROWS ",
            WindowFrameUnits::Range => "RANGE ",
            WindowFrameUnits::Groups => "GROUPS ",
        });
        if frame.end_bound.is_some() {
            self.output.push_str("BETWEEN ");
        }
        self.write_window_frame_bound(&frame.start_bound);
        if let Some(end_bound) = &frame.end_bound {
            self.output.push_str(" AND ");
            self.write_window_frame_bound(end_bound);
        }
        match frame.exclusion {
            WindowFrameExclusion::NoOthers => {}
            WindowFrameExclusion::CurrentRow => self.output.push_str(" EXCLUDE CURRENT ROW"),
            WindowFrameExclusion::Group => self.output.push_str(" EXCLUDE GROUP"),
            WindowFrameExclusion::Ties => self.output.push_str(" EXCLUDE TIES"),
        }

        if let Some(skip) = &frame.after_match_skip_to {
            self.output.push(' ');
            self.write_skip_to(skip);
        }
        if let Some(mode) = &frame.pattern_search_mode {
            self.output.push_str(match mode.mode {
                PatternSearchModeKind::Initial => " INITIAL",
                PatternSearchModeKind::Seek => " SEEK",
            });
        }
        if let Some(pattern) = &frame.pattern {
            self.output.push_str(" PATTERN(");
            self.write_row_pattern(pattern);
            self.output.push(')');
        }
        if !frame.subsets.is_empty() {
            self.output.push_str(" SUBSET ");
            for (index, subset) in frame.subsets.iter().enumerate() {
                if index != 0 {
                    self.output.push_str(", ");
                }
                self.write_ident(&subset.name);
                self.output.push_str(" = (");
                self.write_ident_list(&subset.identifiers);
                self.output.push(')');
            }
        }
        if !frame.variable_definitions.is_empty() {
            self.output.push_str(" DEFINE ");
            for (index, variable) in frame.variable_definitions.iter().enumerate() {
                if index != 0 {
                    self.output.push_str(", ");
                }
                self.write_ident(&variable.name);
                self.output.push_str(" AS ");
                self.write_expr(&variable.expression);
            }
        }
    }

    #[recursive::recursive]
    fn write_window_frame_bound(&mut self, bound: &WindowFrameBound) {
        match bound {
            WindowFrameBound::CurrentRow(_) => self.output.push_str("CURRENT ROW"),
            WindowFrameBound::Preceding(value, _) => {
                self.write_window_frame_value(value);
                self.output.push_str("PRECEDING");
            }
            WindowFrameBound::Following(value, _) => {
                self.write_window_frame_value(value);
                self.output.push_str("FOLLOWING");
            }
        }
    }

    fn write_window_frame_value(&mut self, value: &Option<Expr>) {
        if let Some(value) = value {
            self.write_expr(value);
            self.output.push(' ');
        } else {
            self.output.push_str("UNBOUNDED ");
        }
    }

    #[recursive::recursive]
    fn write_expr(&mut self, expression: &Expr) {
        match expression {
            Expr::AtLocal(node) => self.write_at_local(node),
            Expr::AtTimeZone(node) => self.write_at_time_zone(node),
            Expr::GroupingOperation(node) => self.write_grouping_operation(node),
            Expr::Trim(node) => self.write_trim(node),
            Expr::Overlay(node) => self.write_overlay(node),
            Expr::IntervalValueExpression(node) => self.write_interval_value_expression(node),
            Expr::MethodCall(node) => self.write_method_call(node),
            Expr::StaticMethodCall(node) => self.write_static_method_call(node),
            Expr::CurrentCatalog(node) => self.write_current_catalog(node),
            Expr::CurrentDate(node) => self.write_current_date(node),
            Expr::CurrentPath(node) => self.write_current_path(node),
            Expr::CurrentSchema(node) => self.write_current_schema(node),
            Expr::CurrentUser(node) => self.write_current_user(node),
            Expr::CurrentTime(node) => self.write_current_time(node),
            Expr::CurrentTimestamp(node) => self.write_current_timestamp(node),
            Expr::LocalTime(node) => self.write_local_time(node),
            Expr::LocalTimestamp(node) => self.write_local_timestamp(node),

            Expr::JsonConstructor(node) => self.write_json_constructor(node),
            Expr::JsonExists(node) => self.write_json_exists(node),
            Expr::JsonValue(node) => self.write_json_value(node),
            Expr::JsonQuery(node) => self.write_json_query(node),
            Expr::JsonSerialize(node) => self.write_json_serialize(node),
            Expr::JsonObject(node) => self.write_json_object(node),
            Expr::JsonArray(node) => self.write_json_array(node),

            Expr::Identifier(ident) => self.write_ident(ident),
            Expr::AllRows(_) => self.output.push_str("ALL"),
            Expr::Parameter(_) => self.output.push('?'),
            Expr::Literal(literal) => self.write_literal(literal),
            Expr::FunctionCall(call) => self.write_function_call(call),
            Expr::Extract(node) => self.write_extract_expr(node),
            Expr::If(node) => self.write_if_expr(node),
            Expr::NullIf(node) => self.write_null_if_expr(node),
            Expr::Coalesce(node) => self.write_coalesce_expr(node),
            Expr::Try(node) => self.write_try_expr(node),
            Expr::Format(node) => self.write_format_expr(node),

            Expr::WindowOperation(node) => {
                self.write_ident(&node.name);
                self.output.push_str(" OVER ");
                self.write_window(&node.window);
            }
            Expr::Unary(expression) => self.write_unary_expr(expression),
            Expr::Binary(expression) => self.write_binary_expr(expression),
            Expr::Logical(expression) => self.write_logical_expr(expression),
            Expr::LegacyParenthesized(expression) => {
                self.write_legacy_parenthesized_expr(expression)
            }
            Expr::InList(expression) => self.write_in_list_expr(expression),
            Expr::Predicated(expression) => self.write_predicated_expr(expression),
            Expr::Exists(expression) => self.write_exists_expr(expression),
            Expr::SearchedCase(node) => {
                self.write_case_expr(None, &node.when_clauses, node.default_value.as_ref())
            }
            Expr::SimpleCase(node) => self.write_case_expr(
                Some(&node.operand),
                &node.when_clauses,
                node.default_value.as_ref(),
            ),
            Expr::Unique(node) => {
                self.output.push_str("UNIQUE ");
                self.write_expr(&node.subquery);
            }
            Expr::Cast(expression) => self.write_cast_expr(expression),
            Expr::LegacyInterval(expression) => self.write_legacy_interval_expr(expression),
            Expr::Subquery(expression) => self.write_subquery_expr(expression),
            Expr::Row(expression) => self.write_row_expr(expression),
            Expr::Array(expression) => self.write_array_expr(expression),
            Expr::Lambda(expression) => self.write_lambda_expr(expression),
            Expr::Subscript(node) => self.write_subscript_expr(node),
            Expr::ArrayWildcardSubscript(node) => self.write_array_wildcard_subscript_expr(node),
            Expr::Dereference(node) => self.write_dereference_expr(node),
        }
    }

    fn write_ident(&mut self, ident: &Ident) {
        if ident.quoted {
            self.output.push('`');
            self.output.push_str(&ident.value.replace('`', "``"));
            self.output.push('`');
        } else {
            self.output.push_str(&ident.value);
        }
    }

    fn write_object_name(&mut self, name: &ObjectName) {
        self.write_ident_list_with_separator(&name.parts, ".");
    }

    #[recursive::recursive]
    fn write_type_name(&mut self, node: &TypeName) {
        match &node.kind {
            TypeKind::Generic(value) => {
                self.write_ident(&value.name);
                if !value.arguments.is_empty() {
                    let angle = matches!(
                        value.name.value.to_ascii_uppercase().as_str(),
                        "ARRAY" | "MAP"
                    );
                    self.output.push(if angle { '<' } else { '(' });
                    for (i, arg) in value.arguments.iter().enumerate() {
                        if i > 0 {
                            self.output.push_str(", ");
                        }
                        self.write_data_type_parameter(arg);
                    }
                    self.output.push(if angle { '>' } else { ')' });
                }
            }
            TypeKind::Row(value) => {
                self.output.push_str("STRUCT<");
                for (i, field) in value.fields.iter().enumerate() {
                    if i > 0 {
                        self.output.push_str(", ");
                    }
                    if let Some(name) = &field.name {
                        self.write_ident(name);
                        self.output.push(' ');
                    }
                    self.write_type_name(&field.data_type);
                }
                self.output.push('>');
            }
            TypeKind::DateTime(value) => {
                self.output.push_str(match value.kind {
                    DateTimeType::Time => "TIME",
                    DateTimeType::Timestamp => "TIMESTAMP",
                });
                if let Some(precision) = &value.precision {
                    self.output.push('(');
                    self.write_data_type_parameter(precision);
                    self.output.push(')');
                }
                if value.with_time_zone {
                    self.output.push_str(" WITH TIME ZONE");
                }
            }
            TypeKind::Interval(value) => {
                self.output.push_str("INTERVAL ");
                self.write_interval_qualifier(&value.qualifier);
            }
            TypeKind::ExplicitArray(value) => {
                self.write_type_name(&value.element);
                self.output.push_str(" ARRAY[");
                self.output.push_str(&value.length.spelling);
                self.output.push(']');
            }
        }
    }

    fn write_data_type_parameter(&mut self, value: &DataTypeParameter) {
        match value {
            DataTypeParameter::Numeric(value) => self.output.push_str(&value.spelling),
            DataTypeParameter::Type(value) => self.write_type_name(&value.data_type),
        }
    }

    fn write_interval_qualifier_field(&mut self, field: &IntervalQualifierField) {
        self.output.push_str(match field {
            IntervalQualifierField::Year => "YEAR",
            IntervalQualifierField::Month => "MONTH",
            IntervalQualifierField::Day => "DAY",
            IntervalQualifierField::Hour => "HOUR",
            IntervalQualifierField::Minute => "MINUTE",
            IntervalQualifierField::Second { .. } => "SECOND",
        });
        if let IntervalQualifierField::Second {
            fractional_precision: Some(p),
        } = field
        {
            self.output.push('(');
            self.write_data_type_parameter(p);
            self.output.push(')');
        }
    }

    fn write_interval_qualifier(&mut self, value: &IntervalQualifier) {
        let (first, precision, last) = match value {
            IntervalQualifier::Simple(v) => (&v.field, &v.precision, None),
            IntervalQualifier::Composite(v) => (&v.from, &v.precision, Some(&v.to)),
        };
        if let IntervalQualifierField::Second {
            fractional_precision,
        } = first
        {
            self.output.push_str("SECOND");
            if let Some(leading) = precision {
                self.output.push('(');
                self.write_data_type_parameter(leading);
                if let Some(fractional) = fractional_precision {
                    self.output.push_str(", ");
                    self.write_data_type_parameter(fractional);
                }
                self.output.push(')');
            }
        } else {
            self.write_interval_qualifier_field(first);
            if let Some(p) = precision {
                self.output.push('(');
                self.write_data_type_parameter(p);
                self.output.push(')');
            }
        }
        if let Some(last) = last {
            self.output.push_str(" TO ");
            self.write_interval_qualifier_field(last);
        }
    }

    fn write_literal(&mut self, literal: &Literal) {
        match &literal.kind {
            LiteralKind::Null => self.output.push_str("NULL"),
            LiteralKind::Boolean(value) => {
                self.output.push_str(if *value { "TRUE" } else { "FALSE" })
            }
            LiteralKind::Long { spelling, .. } => self.output.push_str(spelling),
            LiteralKind::Decimal(value) => {
                // Integer text in a DECIMAL string constructor must remain a
                // decimal node after printing, independent of integer width.
                if value.bytes().filter(|byte| *byte == b'.').count() == 1
                    && value.bytes().any(|byte| byte.is_ascii_digit())
                    && value
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || byte == b'.')
                {
                    self.output.push_str(value);
                } else {
                    self.output.push_str("DECIMAL ");
                    self.write_quoted_string(value);
                }
            }
            LiteralKind::Double(value) => {
                if value.get().is_finite() {
                    self.output.push_str(&format!("{:e}", value.get()));
                } else if value.get().is_infinite() {
                    self.output.push_str(if value.get().is_sign_negative() {
                        "-1e999"
                    } else {
                        "1e999"
                    });
                } else {
                    self.output.push_str("DOUBLE ");
                    self.write_quoted_string("NaN");
                }
            }
            LiteralKind::Binary(value) => {
                use std::fmt::Write;
                self.output.push_str("X'");
                for byte in value {
                    write!(self.output, "{byte:02X}").expect("writing to a String");
                }
                self.output.push('\'');
            }
            LiteralKind::String(value) => self.write_quoted_string(value),
            LiteralKind::Generic(value) => {
                self.output.push_str(&value.type_name);
                self.output.push(' ');
                self.write_quoted_string(&value.value);
            }
            LiteralKind::Interval(value) => {
                self.output.push_str("INTERVAL ");
                if value.sign == IntervalSign::Negative {
                    self.output.push('-');
                }
                self.write_quoted_string(&value.value);
                self.output.push(' ');
                self.write_interval_qualifier(&value.qualifier);
            }
        }
    }

    fn write_quoted_string(&mut self, value: &str) {
        self.output.push('\'');
        for character in value.chars() {
            match character {
                '\0' => self.output.push_str("\\0"),
                '\u{0008}' => self.output.push_str("\\b"),
                '\n' => self.output.push_str("\\n"),
                '\r' => self.output.push_str("\\r"),
                '\t' => self.output.push_str("\\t"),
                '\u{000b}' => self.output.push_str("\\v"),
                '\u{000c}' => self.output.push_str("\\f"),
                '\u{001a}' => self.output.push_str("\\Z"),
                '\\' => self.output.push_str("\\\\"),
                '\'' => self.output.push_str("''"),
                _ => self.output.push(character),
            }
        }
        self.output.push('\'');
    }

    #[recursive::recursive]
    fn write_if_expr(&mut self, node: &IfExpr) {
        self.output.push_str("IF(");
        self.write_expr(&node.condition);
        self.output.push_str(", ");
        self.write_expr(&node.true_value);
        if let Some(value) = &node.false_value {
            self.output.push_str(", ");
            self.write_expr(value);
        }
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_null_if_expr(&mut self, node: &NullIfExpr) {
        self.output.push_str("NULLIF(");
        self.write_expr(&node.first);
        self.output.push_str(", ");
        self.write_expr(&node.second);
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_coalesce_expr(&mut self, node: &CoalesceExpr) {
        self.output.push_str("COALESCE(");
        self.write_expr_list(&node.operands);
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_try_expr(&mut self, node: &TryExpr) {
        self.output.push_str("TRY(");
        self.write_expr(&node.inner);
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_format_expr(&mut self, node: &FormatExpr) {
        self.output.push_str("FORMAT(");
        self.write_expr_list(&node.arguments);
        self.output.push(')');
    }

    #[recursive::recursive]
    fn write_extract_expr(&mut self, node: &ExtractExpr) {
        self.output.push_str("EXTRACT(");
        self.output.push_str(node.field.as_str());
        self.output.push_str(" FROM ");
        self.write_expr(&node.expression);
        self.output.push(')');
    }

    #[recursive::recursive]
    fn write_function_call(&mut self, call: &FunctionCall) {
        if let Some(mode) = &call.processing_mode {
            self.write_processing_mode(mode);
        }

        self.write_object_name(&call.name);
        self.output.push('(');
        if call.distinct {
            self.output.push_str("DISTINCT ");
        }
        for (index, argument) in call.arguments.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_call_argument(argument);
        }
        if let Some(null_treatment) = call.null_treatment {
            if !call.arguments.is_empty() {
                self.output.push(' ');
            }
            self.output.push_str(match null_treatment {
                NullTreatment::IgnoreNulls => "IGNORE NULLS",
                NullTreatment::RespectNulls => "RESPECT NULLS",
            });
        }
        if let Some(order_by) = &call.order_by {
            if !call.arguments.is_empty() {
                self.output.push(' ');
            }
            self.output.push_str("ORDER BY ");
            self.write_order_by_list(&order_by.items);
        }
        self.output.push(')');
        if let Some(filter) = &call.filter {
            self.output.push_str(" FILTER (WHERE ");
            self.write_expr(filter);
            self.output.push(')');
        }
        if let Some(over) = &call.window {
            self.output.push_str(" OVER ");
            self.write_window(over);
        }
    }

    #[recursive::recursive]
    fn write_call_argument(&mut self, argument: &CallArgument) {
        if let Some(name) = &argument.name {
            self.write_ident(name);
            self.output.push_str(" => ");
        }
        self.write_expr(&argument.value);
    }
    fn write_processing_mode(&mut self, mode: &ProcessingMode) {
        self.output.push_str(match mode.mode {
            ProcessingModeKind::Running => "RUNNING ",
            ProcessingModeKind::Final => "FINAL ",
        });
    }

    fn write_unary_expr(&mut self, expression: &UnaryExpr) {
        match expression.operator {
            UnaryOperator::Not => self.output.push_str("NOT "),
            UnaryOperator::Plus => self.output.push('+'),
            UnaryOperator::Minus => self.output.push('-'),
            UnaryOperator::Legacy(crate::ast::legacy::UnaryOperator::BitwiseNot) => {
                self.output.push('~')
            }
        }
        let requires_separator = (matches!(expression.expression.as_ref(), Expr::Unary(_))
            && !matches!(expression.operator, UnaryOperator::Not))
            || (expression.operator == UnaryOperator::Minus
                && matches!(expression.expression.as_ref(), Expr::Literal(Literal {
                    kind: LiteralKind::Long { spelling: value, .. }, ..
                }) if value.starts_with('-')));
        if requires_separator {
            self.output.push(' ');
        }
        self.write_unary_operand(&expression.expression);
    }

    fn write_unary_operand(&mut self, expression: &Expr) {
        if self.requires_parentheses_for_prefix(expression) {
            self.output.push('(');
            self.write_expr(expression);
            self.output.push(')');
        } else {
            self.write_expr(expression);
        }
    }

    fn requires_parentheses_for_prefix(&self, expression: &Expr) -> bool {
        matches!(
            expression,
            Expr::Binary(_) | Expr::Logical(_) | Expr::Predicated(_)
        )
    }

    #[recursive::recursive]
    fn write_logical_expr(&mut self, expression: &LogicalExpr) {
        for (index, term) in expression.terms.iter().enumerate() {
            if index > 0 {
                self.output.push(' ');
                self.output.push_str(expression.operator.sql());
                self.output.push(' ');
            }
            let parentheses = match term {
                Expr::Logical(child) => {
                    child.operator.precedence() <= expression.operator.precedence()
                }
                Expr::Binary(child) => {
                    child.operator.precedence() < expression.operator.precedence()
                }
                _ => false,
            };
            if parentheses {
                self.output.push('(');
            }
            self.write_expr(term);
            if parentheses {
                self.output.push(')');
            }
        }
    }

    fn write_binary_expr(&mut self, expression: &BinaryExpr) {
        self.write_binary_operand(&expression.left, expression.operator, BinarySide::Left);
        self.output.push(' ');
        self.output.push_str(expression.operator.sql());
        self.output.push(' ');
        self.write_binary_operand(&expression.right, expression.operator, BinarySide::Right);
    }

    fn write_binary_operand(&mut self, operand: &Expr, parent: BinaryOperator, side: BinarySide) {
        let requires_parentheses = match operand {
            Expr::Logical(child) => child.operator.precedence() < parent.precedence(),
            Expr::Binary(child) => {
                child.operator.precedence() < parent.precedence()
                    || (child.operator.precedence() == parent.precedence()
                        && matches!(side, BinarySide::Right))
            }
            Expr::Predicated(_) => parent.precedence() > LogicalOperator::And.precedence(),
            _ => false,
        };
        if requires_parentheses {
            self.output.push('(');
        }
        self.write_expr(operand);
        if requires_parentheses {
            self.output.push(')');
        }
    }

    #[recursive::recursive]
    fn write_predicated_expr(&mut self, expression: &PredicatedExpr) {
        self.write_predicate_operand(&expression.value, false);
        self.write_predicate(&expression.predicate);
    }

    #[recursive::recursive]
    fn write_predicate(&mut self, predicate: &Predicate) {
        match predicate {
            Predicate::Comparison(node) => {
                self.output.push(' ');
                self.output.push_str(node.operator.sql());
                self.output.push(' ');
                self.write_predicate_operand(&node.right, true);
            }
            Predicate::DistinctFrom(node) => {
                self.output.push_str(if node.negated {
                    " IS NOT DISTINCT FROM "
                } else {
                    " IS DISTINCT FROM "
                });
                self.write_predicate_operand(&node.right, true);
            }
            Predicate::Between(node) => {
                self.output.push_str(if node.negated {
                    " NOT BETWEEN "
                } else {
                    " BETWEEN "
                });
                if let Some(symmetry) = node.symmetry {
                    self.output.push_str(match symmetry {
                        BetweenSymmetry::Symmetric => "SYMMETRIC ",
                        BetweenSymmetry::Asymmetric => "ASYMMETRIC ",
                    });
                }
                self.write_predicate_operand(&node.min, true);
                self.output.push_str(" AND ");
                self.write_predicate_operand(&node.max, true);
            }
            Predicate::BooleanTest(node) => {
                self.output
                    .push_str(if node.negated { " IS NOT " } else { " IS " });
                self.output.push_str(match node.truth_value {
                    TruthValue::True => "TRUE",
                    TruthValue::False => "FALSE",
                    TruthValue::Unknown => "UNKNOWN",
                });
            }
            Predicate::IsNull(node) => self.output.push_str(if node.negated {
                " IS NOT NULL"
            } else {
                " IS NULL"
            }),
            Predicate::Like(node) => {
                self.output
                    .push_str(if node.negated { " NOT LIKE " } else { " LIKE " });
                self.write_predicate_operand(&node.pattern, true);
                if let Some(escape) = &node.escape {
                    self.output.push_str(" ESCAPE ");
                    self.write_predicate_operand(escape, true);
                }
            }
            Predicate::In(node) => {
                self.output
                    .push_str(if node.negated { " NOT IN " } else { " IN " });
                self.write_expr(&node.value_list);
            }
            Predicate::Overlaps(node) => {
                self.output.push_str(" OVERLAPS ");
                self.write_predicate_operand(&node.right, true);
            }
            Predicate::QuantifiedComparison(node) => {
                self.output.push(' ');
                self.output.push_str(node.operator.sql());
                self.output.push(' ');
                self.output.push_str(match node.quantifier {
                    ComparisonQuantifier::All => "ALL ",
                    ComparisonQuantifier::Any => "ANY ",
                    ComparisonQuantifier::Some => "SOME ",
                });
                self.write_expr(&node.subquery);
            }
            Predicate::Match(node) => {
                self.output.push_str(" MATCH ");
                if node.unique {
                    self.output.push_str("UNIQUE ");
                }
                self.output.push_str(match node.match_type {
                    MatchType::Simple => "SIMPLE ",
                    MatchType::Partial => "PARTIAL ",
                    MatchType::Full => "FULL ",
                });
                self.write_expr(&node.subquery);
            }
        }
    }

    fn write_predicate_operand(&mut self, expression: &Expr, right: bool) {
        let parentheses = matches!(expression, Expr::Logical(_))
            || (right && matches!(expression, Expr::Predicated(_)))
            || matches!(expression, Expr::Binary(node) if node.operator.precedence() < 30);
        if parentheses {
            self.output.push('(');
        }
        self.write_expr(expression);
        if parentheses {
            self.output.push(')');
        }
    }

    fn write_in_list_expr(&mut self, expression: &InListExpr) {
        self.output.push('(');
        self.write_expr_list(&expression.values);
        self.output.push(')');
    }

    fn write_legacy_parenthesized_expr(&mut self, expression: &LegacyParenthesizedExpr) {
        self.output.push('(');
        self.write_expr(&expression.expression);
        self.output.push(')');
    }

    fn write_exists_expr(&mut self, expression: &ExistsExpr) {
        self.output.push_str("EXISTS ");
        self.write_expr(&expression.subquery);
    }

    fn write_case_expr(
        &mut self,
        operand: Option<&Expr>,
        clauses: &[WhenClause],
        default: Option<&Expr>,
    ) {
        self.output.push_str("CASE");
        if let Some(operand) = operand {
            self.output.push(' ');
            self.write_expr(operand);
        }
        for clause in clauses {
            self.output.push_str(" WHEN");
            match &clause.match_value {
                WhenMatch::Operand(expression) => {
                    self.output.push(' ');
                    self.write_expr(expression);
                }
                WhenMatch::Partial(predicate) => self.write_predicate(predicate),
            }
            self.output.push_str(" THEN ");
            self.write_expr(&clause.result);
        }
        if let Some(default) = default {
            self.output.push_str(" ELSE ");
            self.write_expr(default);
        }
        self.output.push_str(" END");
    }

    fn write_cast_expr(&mut self, expression: &CastExpr) {
        self.output.push_str(if expression.safe {
            "TRY_CAST("
        } else {
            "CAST("
        });
        self.write_expr(&expression.expression);
        self.output.push_str(" AS ");
        self.write_type_name(&expression.data_type);
        self.output.push(')');
    }

    fn write_legacy_interval_expr(&mut self, expression: &LegacyIntervalExpr) {
        self.output.push_str("INTERVAL ");
        self.write_expr(&expression.value);
        self.output.push(' ');
        self.write_interval_field(expression.leading_field);
        if let Some(precision) = &expression.leading_precision {
            self.output.push('(');
            self.write_expr(precision);
            self.output.push(')');
        }
        if let Some(last_field) = expression.last_field {
            self.output.push_str(" TO ");
            self.write_interval_field(last_field);
            if let Some(precision) = &expression.fractional_seconds_precision {
                self.output.push('(');
                self.write_expr(precision);
                self.output.push(')');
            }
        }
    }

    fn write_interval_field(&mut self, field: IntervalField) {
        self.output.push_str(match field {
            IntervalField::Year => "YEAR",
            IntervalField::Legacy(crate::ast::legacy::IntervalField::Quarter) => "QUARTER",
            IntervalField::Month => "MONTH",
            IntervalField::Legacy(crate::ast::legacy::IntervalField::Week) => "WEEK",
            IntervalField::Day => "DAY",
            IntervalField::Hour => "HOUR",
            IntervalField::Minute => "MINUTE",
            IntervalField::Second => "SECOND",
            IntervalField::Legacy(crate::ast::legacy::IntervalField::Millisecond) => "MILLISECOND",
            IntervalField::Legacy(crate::ast::legacy::IntervalField::Microsecond) => "MICROSECOND",
        });
    }
    fn write_subquery_expr(&mut self, expression: &SubqueryExpr) {
        self.output.push('(');
        self.write_query(&expression.query);
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_row_expr(&mut self, expression: &RowExpr) {
        let named = expression.fields.iter().any(|field| field.name.is_some());
        self.output.push_str(if named { "STRUCT(" } else { "ROW(" });
        for (index, field) in expression.fields.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            if let Some(name) = &field.name {
                self.write_ident(name);
                self.output.push_str(" := ");
            }
            self.write_expr(&field.expression);
        }
        self.output.push(')');
    }
    fn write_array_expr(&mut self, expression: &ArrayExpr) {
        self.output.push('[');
        self.write_expr_list(&expression.values);
        self.output.push(']');
    }

    fn write_lambda_expr(&mut self, expression: &LambdaExpr) {
        let parenthesized = expression.arguments.len() != 1;
        if parenthesized {
            self.output.push('(');
        }
        for (index, argument) in expression.arguments.iter().enumerate() {
            if index > 0 {
                self.output.push_str(", ");
            }
            self.write_ident(&argument.name);
        }
        if parenthesized {
            self.output.push(')');
        }
        self.output.push_str(" -> ");
        self.write_expr(&expression.body);
    }
    #[recursive::recursive]
    fn write_subscript_expr(&mut self, node: &SubscriptExpr) {
        self.write_expr(&node.base);
        self.output.push('[');
        self.write_expr(&node.index);
        self.output.push(']');
    }
    #[recursive::recursive]
    fn write_array_wildcard_subscript_expr(&mut self, node: &ArrayWildcardSubscriptExpr) {
        self.write_expr(&node.base);
        self.output.push_str("[*]");
    }
    #[recursive::recursive]
    fn write_dereference_expr(&mut self, node: &DereferenceExpr) {
        self.write_expr(&node.base);
        self.output.push('.');
        if let Some(field) = &node.field {
            self.write_ident(field);
        } else {
            self.output.push('*');
        }
    }
    fn write_expr_list(&mut self, expressions: &[Expr]) {
        for (index, expression) in expressions.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_expr(expression);
        }
    }
    fn write_ident_list(&mut self, idents: &[Ident]) {
        self.write_ident_list_with_separator(idents, ", ");
    }
    fn write_ident_list_with_separator(&mut self, idents: &[Ident], separator: &str) {
        for (index, ident) in idents.iter().enumerate() {
            if index != 0 {
                self.output.push_str(separator);
            }
            self.write_ident(ident);
        }
    }
}

/// Renders one statement into canonical SQL.
pub fn print_statement(statement: &Statement) -> String {
    Printer::new().statement(statement)
}
/// Renders statements separated by one semicolon and one space.
pub fn print_statements(statements: &[Statement]) -> String {
    Printer::new().statements(statements)
}
/// Renders one expression into canonical SQL.
pub fn print_expr(expression: &Expr) -> String {
    Printer::new().expression(expression)
}

/// Renders one typed query without wrapping it in a top-level statement.
pub fn print_query(query: &Query) -> String {
    let mut printer = Printer::new();
    printer.write_query(query);
    printer.output
}
/// Renders an object name into canonical SQL.
pub fn print_object_name(name: &ObjectName) -> String {
    Printer::new().object_name(name)
}
/// Renders a syntax-level type name into canonical SQL.
pub fn print_type_name(type_name: &TypeName) -> String {
    Printer::new().type_name(type_name)
}
/// Renders one syntax literal into canonical SQL.
pub fn print_literal(literal: &Literal) -> String {
    let mut printer = Printer::new();
    printer.write_literal(literal);
    printer.output
}

#[derive(Clone, Copy)]
enum BinarySide {
    Left,
    Right,
}

impl LogicalOperator {
    const fn precedence(self) -> u8 {
        match self {
            Self::And => 20,
            Self::Or => 10,
        }
    }
    const fn sql(self) -> &'static str {
        match self {
            Self::And => "AND",
            Self::Or => "OR",
        }
    }
}

impl ComparisonOperator {
    const fn sql(self) -> &'static str {
        match self {
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
        }
    }
}

impl BinaryOperator {
    const fn precedence(self) -> u8 {
        match self {
            Self::Add | Self::Subtract => 50,
            Self::Multiply | Self::Divide | Self::Modulo => 60,
            Self::Legacy(operator) => match operator {
                LegacyBinaryOperator::NamedArgument => 1,
                LegacyBinaryOperator::BitwiseOr => 35,
                LegacyBinaryOperator::BitwiseXor => 36,
                LegacyBinaryOperator::BitwiseAnd => 37,
                LegacyBinaryOperator::ShiftLeft | LegacyBinaryOperator::ShiftRight => 40,
                LegacyBinaryOperator::StringConcat => 50,
            },
        }
    }
    const fn sql(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::Legacy(operator) => match operator {
                LegacyBinaryOperator::NamedArgument => "=>",
                LegacyBinaryOperator::BitwiseAnd => "&",
                LegacyBinaryOperator::BitwiseOr => "|",
                LegacyBinaryOperator::BitwiseXor => "^",
                LegacyBinaryOperator::ShiftLeft => "<<",
                LegacyBinaryOperator::ShiftRight => ">>",
                LegacyBinaryOperator::StringConcat => "||",
            },
        }
    }
}

impl Printer {
    /// Writes the root-query function prefix, without a trailing separator.
    fn write_inline_functions(&mut self, functions: &[FunctionSpecification]) {
        if functions.is_empty() {
            return;
        }
        self.output.push_str("WITH ");
        for (index, function) in functions.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_function_specification(function, 0);
        }
    }

    fn write_function_specification(&mut self, function: &FunctionSpecification, indent: usize) {
        self.write_routine_indent(indent);
        self.output.push_str("FUNCTION ");
        self.write_object_name(&function.name);
        self.output.push('(');
        for (index, parameter) in function.parameters.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_function_parameter(parameter);
        }
        self.output.push_str(")\n");
        self.write_routine_indent(indent);
        self.output.push_str("RETURNS ");
        self.write_type_name(&function.returns);
        self.output.push('\n');
        for characteristic in &function.characteristics {
            self.write_routine_characteristic(characteristic, indent);
            self.output.push('\n');
        }
        match &function.body {
            RoutineBody::Sql(statement) => {
                // AstBuilder.java:4283-4288 enforces this root-body invariant.
                assert!(
                    matches!(
                        statement,
                        ControlStatement::Return(_) | ControlStatement::Compound(_)
                    ),
                    "SQL function body must start with RETURN or BEGIN"
                );
                self.write_control_statement(statement, indent);
            }
            RoutineBody::External { definition, .. } => {
                // A parsed DOLLAR_STRING cannot contain its own closing token.
                // There is no escape spelling for it and no raw SQL fallback.
                assert!(
                    !definition.contains("$$"),
                    "external function definition cannot contain its dollar-string delimiter"
                );
                self.write_routine_indent(indent);
                self.output.push_str("AS $$\n");
                // AstBuilder removes exactly one initial LF, CR or CRLF. The
                // inserted LF supplies that framing; decoded content is exact.
                self.output.push_str(definition);
                self.output.push_str("$$");
            }
        }
    }

    fn write_function_parameter(&mut self, parameter: &FunctionParameter) {
        if let Some(name) = &parameter.name {
            self.write_ident(name);
            self.output.push(' ');
        }
        self.write_type_name(&parameter.data_type);
    }

    fn write_routine_characteristic(
        &mut self,
        characteristic: &RoutineCharacteristic,
        indent: usize,
    ) {
        self.write_routine_indent(indent);
        match characteristic {
            RoutineCharacteristic::Language { language, .. } => {
                self.output.push_str("LANGUAGE ");
                self.write_ident(language);
            }
            RoutineCharacteristic::Deterministic { deterministic, .. } => {
                self.output.push_str(if *deterministic {
                    "DETERMINISTIC"
                } else {
                    "NOT DETERMINISTIC"
                });
            }
            RoutineCharacteristic::NullInput { called_on_null, .. } => {
                self.output.push_str(if *called_on_null {
                    "CALLED ON NULL INPUT"
                } else {
                    "RETURNS NULL ON NULL INPUT"
                });
            }
            RoutineCharacteristic::Security { security, .. } => {
                self.output.push_str("SECURITY ");
                self.output.push_str(match security {
                    RoutineSecurity::Invoker => "INVOKER",
                    RoutineSecurity::Definer => "DEFINER",
                });
            }
            RoutineCharacteristic::Comment { comment, .. } => {
                self.output.push_str("COMMENT ");
                self.write_quoted_string(comment);
            }
            RoutineCharacteristic::Properties { properties, .. } => {
                self.output.push_str("WITH (\n");
                for (index, property) in properties.iter().enumerate() {
                    self.write_routine_indent(indent + 1);
                    self.write_routine_property(property);
                    if index + 1 < properties.len() {
                        self.output.push(',');
                    }
                    self.output.push('\n');
                }
                self.write_routine_indent(indent);
                self.output.push(')');
            }
        }
    }

    fn write_routine_property(&mut self, property: &RoutineProperty) {
        self.write_ident(&property.name);
        self.output.push_str(" = ");
        match &property.value {
            Some(value) => self.write_expr(value),
            None => self.output.push_str("DEFAULT"),
        }
    }

    #[recursive::recursive]
    fn write_control_statement(&mut self, statement: &ControlStatement, indent: usize) {
        match statement {
            ControlStatement::Return(statement) => {
                self.write_routine_indent(indent);
                self.output.push_str("RETURN ");
                self.write_expr(&statement.value);
            }
            ControlStatement::Assignment(statement) => {
                self.write_routine_indent(indent);
                self.output.push_str("SET ");
                self.write_ident(&statement.target);
                self.output.push_str(" = ");
                self.write_expr(&statement.value);
            }
            ControlStatement::Case(statement) => self.write_routine_case(statement, indent),
            ControlStatement::If(statement) => self.write_routine_if(statement, indent),
            ControlStatement::Iterate(statement) => {
                self.write_routine_indent(indent);
                self.output.push_str("ITERATE ");
                self.write_ident(&statement.label);
            }
            ControlStatement::Leave(statement) => {
                self.write_routine_indent(indent);
                self.output.push_str("LEAVE ");
                self.write_ident(&statement.label);
            }
            ControlStatement::VariableDeclaration(statement) => {
                self.write_variable_declaration(statement, indent);
            }
            ControlStatement::Compound(statement) => {
                self.write_routine_indent(indent);
                self.output.push_str("BEGIN\n");
                for declaration in &statement.variable_declarations {
                    self.write_variable_declaration(declaration, indent + 1);
                    self.output.push_str(";\n");
                }
                self.write_routine_statement_list(&statement.statements, indent + 1);
                self.write_routine_indent(indent);
                self.output.push_str("END");
            }
            ControlStatement::Loop(statement) => {
                self.write_routine_begin_label(&statement.label, indent);
                self.output.push_str("LOOP\n");
                self.write_routine_statement_list(&statement.statements, indent + 1);
                self.write_routine_indent(indent);
                self.output.push_str("END LOOP");
            }
            ControlStatement::While(statement) => {
                self.write_routine_begin_label(&statement.label, indent);
                self.output.push_str("WHILE ");
                self.write_expr(&statement.expression);
                self.output.push_str(" DO\n");
                self.write_routine_statement_list(&statement.statements, indent + 1);
                self.write_routine_indent(indent);
                self.output.push_str("END WHILE");
            }
            ControlStatement::Repeat(statement) => {
                self.write_routine_begin_label(&statement.label, indent);
                self.output.push_str("REPEAT\n");
                self.write_routine_statement_list(&statement.statements, indent + 1);
                self.write_routine_indent(indent);
                self.output.push_str("UNTIL ");
                self.write_expr(&statement.condition);
                self.output.push('\n');
                self.write_routine_indent(indent);
                self.output.push_str("END REPEAT");
            }
        }
    }

    fn write_variable_declaration(&mut self, declaration: &VariableDeclaration, indent: usize) {
        self.write_routine_indent(indent);
        self.output.push_str("DECLARE ");
        self.write_ident_list(&declaration.names);
        self.output.push(' ');
        self.write_type_name(&declaration.data_type);
        if let Some(default) = &declaration.default_value {
            self.output.push_str(" DEFAULT ");
            self.write_expr(default);
        }
    }

    fn write_routine_case(&mut self, statement: &CaseStatement, indent: usize) {
        self.write_routine_indent(indent);
        self.output.push_str("CASE");
        if let Some(expression) = &statement.expression {
            self.output.push(' ');
            self.write_expr(expression);
        }
        self.output.push('\n');
        for clause in &statement.when_clauses {
            self.write_routine_indent(indent + 1);
            self.output.push_str("WHEN ");
            self.write_expr(&clause.expression);
            self.output.push_str(" THEN\n");
            self.write_routine_statement_list(&clause.statements, indent + 2);
        }
        if let Some(clause) = &statement.else_clause {
            self.write_routine_else(clause, indent + 1);
        }
        self.write_routine_indent(indent);
        self.output.push_str("END CASE");
    }

    fn write_routine_if(&mut self, statement: &IfStatement, indent: usize) {
        self.write_routine_indent(indent);
        self.output.push_str("IF ");
        self.write_expr(&statement.expression);
        self.output.push_str(" THEN\n");
        self.write_routine_statement_list(&statement.statements, indent + 1);
        for clause in &statement.else_if_clauses {
            self.write_routine_indent(indent);
            self.output.push_str("ELSEIF ");
            self.write_expr(&clause.expression);
            self.output.push_str(" THEN\n");
            self.write_routine_statement_list(&clause.statements, indent + 1);
        }
        if let Some(clause) = &statement.else_clause {
            self.write_routine_else(clause, indent);
        }
        self.write_routine_indent(indent);
        self.output.push_str("END IF");
    }

    fn write_routine_else(&mut self, clause: &ElseClause, indent: usize) {
        self.write_routine_indent(indent);
        self.output.push_str("ELSE\n");
        self.write_routine_statement_list(&clause.statements, indent + 1);
    }

    fn write_routine_statement_list(&mut self, statements: &[ControlStatement], indent: usize) {
        for statement in statements {
            self.write_control_statement(statement, indent);
            self.output.push_str(";\n");
        }
    }

    fn write_routine_begin_label(&mut self, label: &Option<Ident>, indent: usize) {
        self.write_routine_indent(indent);
        if let Some(label) = label {
            self.write_ident(label);
            self.output.push_str(": ");
        }
    }

    fn write_routine_indent(&mut self, indent: usize) {
        for _ in 0..indent {
            self.output.push_str("  ");
        }
    }
}

impl Printer {
    fn write_json_constructor(&mut self, node: &JsonConstructor) {
        self.output.push_str("JSON(");
        self.write_json_expression(&node.expression, Some(node.format));
        self.output.push(')');
    }

    fn write_json_exists(&mut self, node: &JsonExists) {
        self.output.push_str("JSON_EXISTS(");
        self.write_json_path_invocation(&node.path_invocation);
        self.output.push_str(match node.error_behavior {
            JsonExistsErrorBehavior::False => " FALSE ON ERROR",
            JsonExistsErrorBehavior::True => " TRUE ON ERROR",
            JsonExistsErrorBehavior::Unknown => " UNKNOWN ON ERROR",
            JsonExistsErrorBehavior::Error => " ERROR ON ERROR",
        });
        self.output.push(')');
    }

    fn write_json_value(&mut self, node: &JsonValue) {
        node.validate_constructor_invariants()
            .expect("invalid JSON_VALUE constructor facts");
        self.output.push_str("JSON_VALUE(");
        self.write_json_path_invocation(&node.path_invocation);
        self.write_json_returning(node.returned_type.as_ref(), None);
        self.write_json_value_behavior(node.empty_behavior, node.empty_default.as_ref());
        self.output.push_str(" ON EMPTY");
        self.write_json_value_behavior(node.error_behavior, node.error_default.as_ref());
        self.output.push_str(" ON ERROR)");
    }

    #[recursive::recursive]
    fn write_json_query(&mut self, node: &JsonQuery) {
        self.output.push_str("JSON_QUERY(");
        self.write_json_path_invocation(&node.path_invocation);
        self.write_json_returning(node.returned_type.as_ref(), node.output_format);
        self.write_json_query_wrapper(node.wrapper_behavior);
        self.write_json_query_quotes(node.quotes_behavior);
        self.write_json_query_behavior(node.empty_behavior);
        self.output.push_str(" ON EMPTY");
        self.write_json_query_behavior(node.error_behavior);
        self.output.push_str(" ON ERROR)");
    }

    fn write_json_serialize(&mut self, node: &JsonSerialize) {
        self.output.push_str("JSON_SERIALIZE(");
        self.write_json_expression(&node.expression, Some(node.input_format));
        self.write_json_returning(node.returned_type.as_ref(), node.output_format);
        // ERROR is the enum's effective default, with no optional-presence bit.
        match node.error_behavior {
            JsonSerializeErrorBehavior::Error => {}
            JsonSerializeErrorBehavior::Null => self.output.push_str(" NULL ON ERROR"),
        }
        self.output.push(')');
    }

    fn write_json_object(&mut self, node: &JsonObject) {
        // The grammar allows NULL/UNIQUE clauses only after a nonempty member
        // list. An empty list can only carry AstBuilder's true/false defaults.
        assert!(
            !node.members.is_empty() || (node.null_on_null && !node.unique_keys),
            "empty JSON_OBJECT has unprintable NULL/UNIQUE facts"
        );
        self.output.push_str("JSON_OBJECT(");
        for (index, member) in node.members.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_json_object_member(member);
        }
        if !node.members.is_empty() {
            self.output.push_str(if node.null_on_null {
                " NULL ON NULL"
            } else {
                " ABSENT ON NULL"
            });
            self.output.push_str(if node.unique_keys {
                " WITH UNIQUE KEYS"
            } else {
                " WITHOUT UNIQUE KEYS"
            });
        }
        self.write_json_returning(node.returned_type.as_ref(), node.output_format);
        self.output.push(')');
    }

    fn write_json_object_member(&mut self, member: &JsonObjectMember) {
        self.output.push_str("KEY ");
        self.write_expr(&member.key);
        self.output.push_str(" VALUE ");
        self.write_json_expression(&member.value, member.format);
    }

    fn write_json_array(&mut self, node: &JsonArray) {
        assert!(
            !node.elements.is_empty() || !node.null_on_null,
            "empty JSON_ARRAY has unprintable NULL facts"
        );
        self.output.push_str("JSON_ARRAY(");
        for (index, element) in node.elements.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_json_array_element(element);
        }
        if !node.elements.is_empty() {
            self.output.push_str(if node.null_on_null {
                " NULL ON NULL"
            } else {
                " ABSENT ON NULL"
            });
        }
        self.write_json_returning(node.returned_type.as_ref(), node.output_format);
        self.output.push(')');
    }

    fn write_json_array_element(&mut self, element: &JsonArrayElement) {
        self.write_json_expression(&element.value, element.format);
    }

    fn write_json_path_invocation(&mut self, node: &JsonPathInvocation) {
        self.write_json_expression(&node.input_expression, Some(node.input_format));
        self.output.push_str(", ");
        self.write_json_path_string(&node.json_path);
        if let Some(name) = &node.path_name {
            self.output.push_str(" AS ");
            self.write_ident(name);
        }
        if !node.path_parameters.is_empty() {
            self.output.push_str(" PASSING ");
            for (index, parameter) in node.path_parameters.iter().enumerate() {
                if index != 0 {
                    self.output.push_str(", ");
                }
                self.write_json_path_parameter(parameter);
            }
        }
    }

    fn write_json_path_parameter(&mut self, parameter: &JsonPathParameter) {
        self.write_json_expression(&parameter.parameter, parameter.format);
        self.output.push_str(" AS ");
        self.write_ident(&parameter.name);
    }

    fn write_json_path_string(&mut self, path: &JsonPathString) {
        // Payload is already decoded by the original StringLiteral author.
        self.write_quoted_string(&path.value);
    }

    fn write_json_expression(&mut self, expression: &Expr, format: Option<JsonFormat>) {
        self.write_expr(expression);
        if let Some(format) = format {
            self.output.push_str(" FORMAT ");
            self.write_json_format(format);
        }
    }

    fn write_json_format(&mut self, format: JsonFormat) {
        self.output.push_str(match format {
            JsonFormat::Json => "JSON",
            JsonFormat::Utf8 => "JSON ENCODING UTF8",
            JsonFormat::Utf16 => "JSON ENCODING UTF16",
            JsonFormat::Utf32 => "JSON ENCODING UTF32",
        });
    }

    fn write_json_returning(
        &mut self,
        returned_type: Option<&TypeName>,
        output_format: Option<JsonFormat>,
    ) {
        // FORMAT output syntax belongs inside RETURNING in the pinned grammar.
        assert!(
            returned_type.is_some() || output_format.is_none(),
            "JSON output format has no RETURNING type"
        );
        if let Some(data_type) = returned_type {
            self.output.push_str(" RETURNING ");
            self.write_type_name(data_type);
            if let Some(format) = output_format {
                self.output.push_str(" FORMAT ");
                self.write_json_format(format);
            }
        }
    }

    fn write_json_value_behavior(&mut self, behavior: JsonValueBehavior, default: Option<&Expr>) {
        self.output.push(' ');
        match behavior {
            JsonValueBehavior::Null => {
                assert!(default.is_none(), "NULL behavior has a default expression");
                self.output.push_str("NULL");
            }
            JsonValueBehavior::Error => {
                assert!(default.is_none(), "ERROR behavior has a default expression");
                self.output.push_str("ERROR");
            }
            JsonValueBehavior::Default => {
                self.output.push_str("DEFAULT ");
                self.write_expr(default.expect("DEFAULT behavior has no expression"));
            }
        }
    }

    fn write_json_query_wrapper(&mut self, behavior: JsonQueryWrapperBehavior) {
        self.output.push_str(match behavior {
            JsonQueryWrapperBehavior::Without => " WITHOUT ARRAY WRAPPER",
            JsonQueryWrapperBehavior::Conditional => " WITH CONDITIONAL ARRAY WRAPPER",
            JsonQueryWrapperBehavior::Unconditional => " WITH UNCONDITIONAL ARRAY WRAPPER",
        });
    }

    fn write_json_query_quotes(&mut self, behavior: Option<JsonQueryQuotesBehavior>) {
        if let Some(behavior) = behavior {
            self.output.push_str(match behavior {
                JsonQueryQuotesBehavior::Keep => " KEEP QUOTES ON SCALAR STRING",
                JsonQueryQuotesBehavior::Omit => " OMIT QUOTES ON SCALAR STRING",
            });
        }
    }

    fn write_json_query_behavior(&mut self, behavior: JsonQueryBehavior) {
        self.output.push_str(match behavior {
            JsonQueryBehavior::Null => " NULL",
            JsonQueryBehavior::Error => " ERROR",
            JsonQueryBehavior::EmptyArray => " EMPTY ARRAY",
            JsonQueryBehavior::EmptyObject => " EMPTY OBJECT",
        });
    }

    fn write_json_table(&mut self, node: &JsonTable) {
        node.validate_constructor_invariants()
            .expect("invalid JSON_TABLE constructor facts");
        self.output.push_str("JSON_TABLE(");
        self.write_json_path_invocation(&node.path_invocation);
        self.write_json_table_columns(&node.columns);
        if let Some(plan) = &node.plan {
            match plan {
                JsonTablePlan::Default(plan) => {
                    self.output.push_str(" PLAN DEFAULT (");
                    self.write_json_table_default_plan(plan);
                }
                JsonTablePlan::Specific(plan) => {
                    self.output.push_str(" PLAN (");
                    self.write_json_table_specific_plan(plan);
                }
            }
            self.output.push(')');
        }
        if let Some(behavior) = node.error_behavior {
            self.output.push_str(match behavior {
                JsonTableErrorBehavior::Error => " ERROR ON ERROR",
                JsonTableErrorBehavior::Empty => " EMPTY ON ERROR",
            });
        }
        self.output.push(')');
        // The outer TableFactor alias and its column list are root-owned.
    }

    fn write_json_table_columns(&mut self, columns: &[JsonTableColumn]) {
        assert!(!columns.is_empty(), "JSON_TABLE column list is empty");
        self.output.push_str(" COLUMNS (");
        for (index, column) in columns.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_json_table_column(column);
        }
        self.output.push(')');
    }

    #[recursive::recursive]
    fn write_json_table_column(&mut self, column: &JsonTableColumn) {
        match column {
            JsonTableColumn::Ordinality(node) => self.write_json_table_ordinality_column(node),
            JsonTableColumn::Value(node) => self.write_json_table_value_column(node),
            JsonTableColumn::Query(node) => self.write_json_table_query_column(node),
            JsonTableColumn::Nested(node) => self.write_json_table_nested_columns(node),
        }
    }

    fn write_json_table_ordinality_column(&mut self, node: &JsonTableOrdinalityColumn) {
        self.write_ident(&node.name);
        self.output.push_str(" FOR ORDINALITY");
    }

    fn write_json_table_value_column(&mut self, node: &JsonTableValueColumn) {
        node.validate_constructor_invariants()
            .expect("invalid JSON_TABLE value column constructor facts");
        self.write_ident(&node.name);
        self.output.push(' ');
        self.write_type_name(&node.data_type);
        if let Some(path) = &node.json_path {
            self.output.push_str(" PATH ");
            self.write_json_path_string(path);
        }
        self.write_json_value_behavior(node.empty_behavior, node.empty_default.as_ref());
        self.output.push_str(" ON EMPTY");
        if let Some(behavior) = node.error_behavior {
            self.write_json_value_behavior(behavior, node.error_default.as_ref());
            self.output.push_str(" ON ERROR");
        }
    }

    fn write_json_table_query_column(&mut self, node: &JsonTableQueryColumn) {
        self.write_ident(&node.name);
        self.output.push(' ');
        self.write_type_name(&node.data_type);
        self.output.push_str(" FORMAT ");
        self.write_json_format(node.format);
        if let Some(path) = &node.json_path {
            self.output.push_str(" PATH ");
            self.write_json_path_string(path);
        }
        self.write_json_query_wrapper(node.wrapper_behavior);
        self.write_json_query_quotes(node.quotes_behavior);
        self.write_json_query_behavior(node.empty_behavior);
        self.output.push_str(" ON EMPTY");
        if let Some(behavior) = node.error_behavior {
            self.write_json_query_behavior(behavior);
            self.output.push_str(" ON ERROR");
        }
    }

    fn write_json_table_nested_columns(&mut self, node: &JsonTableNestedColumns) {
        node.validate_constructor_invariants()
            .expect("invalid JSON_TABLE nested column constructor facts");
        self.output.push_str("NESTED PATH ");
        self.write_json_path_string(&node.json_path);
        if let Some(name) = &node.path_name {
            self.output.push_str(" AS ");
            self.write_ident(name);
        }
        self.write_json_table_columns(&node.columns);
    }

    fn write_json_table_default_plan(&mut self, plan: &JsonTableDefaultPlan) {
        self.output.push_str(match plan.parent_child {
            JsonTableParentChildPlanType::Outer => "OUTER",
            JsonTableParentChildPlanType::Inner => "INNER",
        });
        self.output.push_str(", ");
        self.output.push_str(match plan.siblings {
            JsonTableSiblingsPlanType::Union => "UNION",
            JsonTableSiblingsPlanType::Cross => "CROSS",
        });
    }

    #[recursive::recursive]
    fn write_json_table_specific_plan(&mut self, plan: &JsonTableSpecificPlan) {
        match plan {
            JsonTableSpecificPlan::Leaf(node) => self.write_json_table_plan_leaf(node),
            JsonTableSpecificPlan::ParentChild(node) => {
                self.write_json_table_plan_parent_child(node)
            }
            JsonTableSpecificPlan::Siblings(node) => self.write_json_table_plan_siblings(node),
        }
    }

    fn write_json_table_plan_leaf(&mut self, node: &JsonTablePlanLeaf) {
        self.write_ident(&node.name);
    }

    fn write_json_table_plan_parent_child(&mut self, node: &JsonTablePlanParentChild) {
        self.write_json_table_plan_leaf(&node.parent);
        self.output.push_str(match node.plan_type {
            JsonTableParentChildPlanType::Outer => " OUTER (",
            JsonTableParentChildPlanType::Inner => " INNER (",
        });
        self.write_json_table_specific_plan(&node.child);
        self.output.push(')');
    }

    fn write_json_table_plan_siblings(&mut self, node: &JsonTablePlanSiblings) {
        node.validate_constructor_invariants()
            .expect("invalid JSON_TABLE sibling plan constructor facts");
        for (index, sibling) in node.siblings.iter().enumerate() {
            if index != 0 {
                self.output.push_str(match node.plan_type {
                    JsonTableSiblingsPlanType::Union => " UNION ",
                    JsonTableSiblingsPlanType::Cross => " CROSS ",
                });
            }
            // Preserve nested specific-plan hierarchy, even for the same type.
            self.output.push('(');
            self.write_json_table_specific_plan(sibling);
            self.output.push(')');
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Span;

    fn span() -> Span {
        Span::new(0, 0)
    }

    fn ident(value: &str) -> Ident {
        Ident {
            value: value.to_owned(),
            quoted: false,
            span: span(),
        }
    }

    fn object_name(value: &str) -> ObjectName {
        ObjectName {
            parts: vec![ident(value)],
            span: span(),
        }
    }

    fn identifier(value: &str) -> Expr {
        Expr::Identifier(ident(value))
    }

    fn number(value: &str) -> Expr {
        Expr::Literal(Literal {
            kind: LiteralKind::Long {
                spelling: value.to_owned(),
                parsed_value: (value.to_owned()).parse::<i64>().unwrap(),
            },
            span: span(),
        })
    }

    fn binary(left: Expr, operator: BinaryOperator, right: Expr) -> Expr {
        Expr::Binary(BinaryExpr {
            left: crate::ast::AstBox::new(left),
            operator: operator,
            right: crate::ast::AstBox::new(right),
            span: span(),
        })
    }

    #[test]
    fn renders_extended_expression_forms() {
        let function = Expr::FunctionCall(AstBox::new(FunctionCall {
            name: object_name("Coalesce"),
            arguments: vec![identifier("a"), number("1")]
                .into_iter()
                .map(|value| crate::build::call_argument(value, None))
                .collect(),
            processing_mode: None,
            distinct: true,
            order_by: None,
            filter: Some(crate::build::is_null(identifier("b"), true)),
            null_treatment: Some(NullTreatment::IgnoreNulls),
            window: Some(Window::Specification(AstBox::new(WindowSpecification {
                existing_window_name: None,
                partition_by: vec![identifier("partition_key")].into(),
                order_by: None,
                window_frame: None,
                span: span(),
            }))),
            span: span(),
        }));
        assert_eq!(
            print_expr(&function),
            "Coalesce(DISTINCT a, 1 IGNORE NULLS) FILTER (WHERE b IS NOT NULL) OVER (PARTITION BY partition_key)"
        );

        let between = Expr::Predicated(AstBox::new(PredicatedExpr {
            value: binary(identifier("a"), BinaryOperator::Add, identifier("b")),
            predicate: Predicate::Between(BetweenPredicate {
                negated: true,
                symmetry: None,
                min: number("1"),
                max: number("2"),
                span: span(),
            }),
            span: span(),
        }));
        assert_eq!(print_expr(&between), "a + b NOT BETWEEN 1 AND 2");
        assert_eq!(
            print_expr(&crate::build::function(
                ObjectName {
                    parts: vec![crate::build::ident("json_query", false)],
                    span: crate::SYNTHETIC
                },
                vec![
                    identifier("payload"),
                    Expr::Literal(Literal {
                        kind: LiteralKind::String("name".to_owned()),
                        span: span()
                    })
                ],
            )),
            "json_query(payload, 'name')"
        );
        assert_eq!(
            print_expr(&Expr::Lambda(AstBox::new(LambdaExpr {
                arguments: vec![LambdaArgumentDeclaration {
                    name: ident("value"),
                    span: span()
                }]
                .into(),
                body: identifier("value"),
                span: span(),
            }))),
            "value -> value"
        );
    }

    #[test]
    fn renders_query_relations_and_modifiers() {
        let query = Query {
            session_properties: crate::ast::AstVec::new(),
            functions: crate::ast::AstVec::new(),
            with: Some(With {
                recursive: false,
                ctes: (vec![Cte {
                    name: ident("source"),
                    columns: None,
                    query: crate::ast::AstBox::new(Query {
                        session_properties: crate::ast::AstVec::new(),
                        functions: crate::ast::AstVec::new(),
                        with: None,
                        body: crate::ast::AstBox::new(SetExpr::Values(Values {
                            rows: (vec![vec![number("1")]])
                                .into_iter()
                                .map(|cells| {
                                    crate::ast::Expr::Row(crate::ast::RowExpr {
                                        fields: cells
                                            .into_iter()
                                            .map(|expression| crate::ast::RowField {
                                                name: None,
                                                span: expression.span(),
                                                expression,
                                            })
                                            .collect(),
                                        span: span(),
                                    })
                                })
                                .collect(),
                            span: span(),
                        })),
                        modifiers: Default::default(),
                        span: span(),
                    }),
                    span: span(),
                }])
                .into(),
                span: span(),
            }),
            body: crate::ast::AstBox::new(SetExpr::Select(crate::ast::AstBox::new(Select {
                projection: (vec![crate::ast::SelectItem::SingleColumn(
                    crate::ast::AstBox::new(crate::ast::SingleColumn {
                        expression: identifier("s"),
                        alias: Some(ident("value")),
                        span: span(),
                    }),
                )])
                .into(),
                from: Some(crate::build::alias(
                    crate::build::table(object_name("source"), None, None),
                    ident("s"),
                    None,
                )),
                selection: None,
                group_by: None,
                having: None,
                legacy: Default::default(),
                windows: AstVec::new(),
                modifiers: QueryModifiers {
                    order_by: Some(OrderBy {
                        items: vec![SortItem {
                            sort_key: identifier("value"),
                            ordering: SortOrdering::Descending,
                            null_ordering: NullOrdering::Last,
                            span: span(),
                        }]
                        .into(),
                        span: span(),
                    }),
                    limit: Some(QueryLimit::Limit(Limit {
                        row_count: number("10"),
                        span: span(),
                    })),
                    offset: Some(Offset {
                        row_count: number("2"),
                        span: span(),
                    }),
                },
                span: span(),
                distinct: true,
            }))),
            modifiers: Default::default(),
            span: span(),
        };

        assert_eq!(
            print_statement(&Statement::ExplainQuery(ExplainQuery {
                format: ExplainFormat::Verbose,
                logical: false,
                query: Box::new(query),
                span: span(),
            })),
            "EXPLAIN VERBOSE WITH source AS (VALUES (1)) SELECT DISTINCT s AS value FROM source AS s ORDER BY value DESC NULLS LAST OFFSET 2 ROWS LIMIT 10"
        );
    }

    #[test]
    fn parenthesizes_binary_expressions_and_prints_processlist() {
        let expression = binary(
            identifier("a"),
            BinaryOperator::Multiply,
            binary(identifier("b"), BinaryOperator::Add, identifier("c")),
        );
        assert_eq!(print_expr(&expression), "a * (b + c)");

        let show = Statement::ShowProcessList(ShowProcessList {
            full: false,
            span: span(),
        });
        assert_eq!(print_statement(&show), "SHOW PROCESSLIST");
        assert_eq!(
            print_statements(&[show.clone(), show]),
            "SHOW PROCESSLIST; SHOW PROCESSLIST"
        );
    }

    #[test]
    fn unary_minus_of_signed_literal_round_trips_without_starting_a_comment() {
        for source in ["- -1", "- -9223372036854775808"] {
            let tokens = crate::lex(source).unwrap();
            let expression = crate::parser::parse_expression(source, &tokens).unwrap();
            let canonical = print_expr(&expression);
            assert_eq!(canonical, source);
            let tokens = crate::lex(&canonical).unwrap();
            let reparsed = crate::parser::parse_expression(&canonical, &tokens).unwrap();
            assert_eq!(reparsed, expression);
        }
    }

    #[test]
    fn inline_function_prefixes_and_external_content_preserve_query_facts() {
        let Statement::Query(mut query) = crate::parse("WITH c AS (SELECT 2) SELECT 1")
            .unwrap()
            .remove(0)
        else {
            panic!("query");
        };
        query.session_properties.push(SessionProperty {
            name: crate::build::object_name(vec![crate::build::ident("setting", false)]).unwrap(),
            value: crate::build::integer(10),
            span: span(),
        });
        query.functions.push(FunctionSpecification {
            name: crate::build::object_name(vec![crate::build::ident("f", false)]).unwrap(),
            parameters: vec![],
            returns: crate::build::simple_type("BIGINT"),
            characteristics: vec![],
            body: RoutineBody::Sql(ControlStatement::Return(ReturnStatement {
                value: crate::build::integer(3),
                span: span(),
            })),
            span: span(),
        });
        assert_eq!(
            print_query(&query),
            "WITH SESSION setting = 10 WITH FUNCTION f()\nRETURNS BIGINT\nRETURN 3 WITH c AS (SELECT 2) SELECT 1"
        );
        for definition in ["", "\n", "\r\n'\\雪\n", "return 7"] {
            query.functions[0].body = RoutineBody::External {
                definition: definition.to_owned(),
                span: span(),
            };
            let printed = print_query(&query);
            assert!(printed.contains(&format!("AS $$\n{definition}$$ WITH c AS")));
        }
    }
}

impl Printer {
    fn write_at_operand(&mut self, value: &Expr) {
        let parentheses = matches!(
            value,
            Expr::Binary(_) | Expr::Logical(_) | Expr::Predicated(_) | Expr::Unary(_)
        );
        if parentheses {
            self.output.push('(');
        }
        self.write_expr(value);
        if parentheses {
            self.output.push(')');
        }
    }
    #[recursive::recursive]
    fn write_at_local(&mut self, node: &AtLocal) {
        self.write_at_operand(&node.value);
        self.output.push_str(" AT LOCAL");
    }
    #[recursive::recursive]
    fn write_at_time_zone(&mut self, node: &AtTimeZone) {
        node.validate_constructor_invariants()
            .expect("invalid AT TIME ZONE constructor facts");
        self.write_at_operand(&node.value);
        self.output.push_str(" AT TIME ZONE ");
        self.write_expr(&node.time_zone);
    }
    #[recursive::recursive]
    fn write_grouping_operation(&mut self, node: &GroupingOperation) {
        node.validate_constructor_invariants()
            .expect("invalid GROUPING constructor facts");
        self.output.push_str("GROUPING (");
        self.write_expr_list(&node.grouping_columns);
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_trim(&mut self, node: &Trim) {
        self.output.push_str("trim(");
        self.output.push_str(match node.specification {
            TrimSpecification::Both => "BOTH",
            TrimSpecification::Leading => "LEADING",
            TrimSpecification::Trailing => "TRAILING",
        });
        if let Some(character) = &node.trim_character {
            self.output.push(' ');
            self.write_expr(character);
        }
        self.output.push_str(" FROM ");
        self.write_expr(&node.trim_source);
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_overlay(&mut self, node: &Overlay) {
        self.output.push_str("OVERLAY(");
        self.write_expr(&node.value);
        self.output.push_str(" PLACING ");
        self.write_expr(&node.replacement);
        self.output.push_str(" FROM ");
        self.write_expr(&node.start);
        if let Some(length) = &node.length {
            self.output.push_str(" FOR ");
            self.write_expr(length);
        }
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_interval_value_expression(&mut self, node: &IntervalValueExpression) {
        self.output.push_str("((");
        self.write_binary_operand(&node.left, BinaryOperator::Subtract, BinarySide::Left);
        self.output.push_str(" - ");
        self.write_binary_operand(&node.right, BinaryOperator::Subtract, BinarySide::Right);
        self.output.push_str(") ");
        self.write_interval_qualifier(&node.data_type.qualifier);
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_method_call(&mut self, node: &MethodCall) {
        self.output.push('(');
        self.write_expr(&node.receiver);
        self.output.push_str(").");
        self.write_ident(&node.method);
        self.output.push('(');
        for (index, argument) in node.arguments.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_call_argument(argument);
        }
        self.output.push(')');
    }
    #[recursive::recursive]
    fn write_static_method_call(&mut self, node: &StaticMethodCall) {
        self.write_object_name(&node.type_name);
        self.output.push_str("::");
        self.write_ident(&node.method);
        self.output.push('(');
        for (index, argument) in node.arguments.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_call_argument(argument);
        }
        self.output.push(')');
    }
    fn write_current_catalog(&mut self, _node: &CurrentCatalog) {
        self.output.push_str("CURRENT_CATALOG");
    }
    fn write_current_date(&mut self, _node: &CurrentDate) {
        self.output.push_str("current_date");
    }
    fn write_current_path(&mut self, _node: &CurrentPath) {
        self.output.push_str("CURRENT_PATH");
    }
    fn write_current_schema(&mut self, _node: &CurrentSchema) {
        self.output.push_str("CURRENT_SCHEMA");
    }
    fn write_current_user(&mut self, _node: &CurrentUser) {
        self.output.push_str("CURRENT_USER");
    }
    fn write_current_time(&mut self, node: &CurrentTime) {
        self.output.push_str("current_time");
        if let Some(precision) = node.precision {
            self.output.push('(');
            self.output.push_str(&precision.to_string());
            self.output.push(')');
        }
    }
    fn write_current_timestamp(&mut self, node: &CurrentTimestamp) {
        self.output.push_str("current_timestamp");
        if let Some(precision) = node.precision {
            self.output.push('(');
            self.output.push_str(&precision.to_string());
            self.output.push(')');
        }
    }
    fn write_local_time(&mut self, node: &LocalTime) {
        self.output.push_str("localtime");
        if let Some(precision) = node.precision {
            self.output.push('(');
            self.output.push_str(&precision.to_string());
            self.output.push(')');
        }
    }
    fn write_local_timestamp(&mut self, node: &LocalTimestamp) {
        self.output.push_str("localtimestamp");
        if let Some(precision) = node.precision {
            self.output.push('(');
            self.output.push_str(&precision.to_string());
            self.output.push(')');
        }
    }
}

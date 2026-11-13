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
                crate::ast::session::write_show_process_list(statement, &mut self.output)
            }
            Statement::Statistics(statement) => {
                crate::ast::statistics::write_sql(statement, &mut self.output)
            }
            Statement::Catalog(statement) => {
                crate::ast::catalog::write_sql(statement, &mut self.output)
            }
            Statement::Iceberg(statement) => {
                crate::ast::iceberg::write_sql(statement, &mut self.output)
            }
            Statement::Maintenance(statement) => {
                crate::ast::maintenance::write_sql(statement, &mut self.output)
            }
            Statement::MaterializedView(statement) => {
                crate::ast::materialized_view::write_sql(statement, &mut self.output)
            }
            Statement::View(statement) => crate::ast::view::write_sql(statement, &mut self.output),
            Statement::Table(statement) => {
                crate::ast::table::write_sql(statement, &mut self.output)
            }
            Statement::Dml(statement) => crate::ast::dml::write_sql(statement, &mut self.output),
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
        if let Some(with) = &query.with {
            self.write_with(with);
            self.output.push(' ');
        }
        self.write_set_expr(&query.body);
        if !query.order_by.is_empty() {
            self.output.push_str(" ORDER BY ");
            self.write_order_by_list(&query.order_by);
        }
        if query.limit_comma_offset {
            let offset = query
                .offset
                .as_ref()
                .expect("comma LIMIT syntax requires an offset");
            let limit = query
                .limit
                .as_ref()
                .expect("comma LIMIT syntax requires a limit");
            self.output.push_str(" LIMIT ");
            self.write_expr(&offset.value);
            self.output.push_str(", ");
            self.write_expr(limit);
        } else if let Some(limit) = &query.limit {
            self.output.push_str(" LIMIT ");
            self.write_expr(limit);
        }
        if !query.limit_comma_offset
            && let Some(offset) = &query.offset
        {
            self.output.push_str(" OFFSET ");
            self.write_expr(&offset.value);
            match offset.rows {
                OffsetRows::None => {}
                OffsetRows::Row => self.output.push_str(" ROW"),
                OffsetRows::Rows => self.output.push_str(" ROWS"),
            }
        }
        if let Some(fetch) = &query.fetch {
            self.write_fetch(fetch);
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
            if !cte.columns.is_empty() {
                self.output.push('(');
                self.write_ident_list(&cte.columns);
                self.output.push(')');
            }
            self.output.push_str(" AS (");
            self.write_query(&cte.query);
            self.output.push(')');
        }
    }

    fn write_set_expr(&mut self, expression: &SetExpr) {
        match expression {
            SetExpr::Select(select) => self.write_select(select),
            SetExpr::Values(values) => self.write_values(values),
            SetExpr::Query(query) => {
                self.output.push('(');
                self.write_query(query);
                self.output.push(')');
            }
            SetExpr::SetOperation(operation) => {
                // The parser folds unparenthesized set operations from the
                // left, so retaining a left wrapper here would manufacture a
                // `SetExpr::Query` node on reparse. Only the right side needs
                // parentheses to preserve a non-default nesting shape.
                self.write_set_expr(&operation.left);
                self.output.push(' ');
                self.output.push_str(match operation.operator {
                    SetOperator::Union => "UNION",
                    SetOperator::Intersect => "INTERSECT",
                    SetOperator::Except => "EXCEPT",
                });
                match operation.quantifier {
                    SetQuantifier::Distinct => self.output.push_str(" DISTINCT"),
                    SetQuantifier::All => self.output.push_str(" ALL"),
                    SetQuantifier::None => {}
                }
                self.output.push(' ');
                self.write_set_right_operand(&operation.right);
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

    fn write_values(&mut self, values: &Values) {
        self.output.push_str("VALUES ");
        for (row_index, row) in values.rows.iter().enumerate() {
            if row_index != 0 {
                self.output.push_str(", ");
            }
            if values.explicit_row {
                self.output.push_str("ROW");
            }
            self.output.push('(');
            self.write_expr_list(row);
            self.output.push(')');
        }
    }

    fn write_select(&mut self, select: &Select) {
        self.output.push_str("SELECT");
        for hint in &select.hints {
            self.output.push_str(" /*+ ");
            self.write_ident(&hint.name);
            match &hint.value {
                SelectHintValue::Bare => {}
                SelectHintValue::Call { arguments } => {
                    self.output.push('(');
                    self.write_expr_list(arguments);
                    self.output.push(')');
                }
                SelectHintValue::Assignment { value } => {
                    self.output.push_str(" = ");
                    self.write_expr(value);
                }
            }
            self.output.push_str(" */");
        }
        match &select.quantifier {
            SelectQuantifier::None => {}
            SelectQuantifier::All(_) => self.output.push_str(" ALL"),
            SelectQuantifier::Distinct { on, .. } => {
                self.output.push_str(" DISTINCT");
                if !on.is_empty() {
                    self.output.push_str(" ON (");
                    self.write_expr_list(on);
                    self.output.push(')');
                }
            }
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
        if !select.from.is_empty() {
            self.output.push_str(" FROM ");
            for (index, relation) in select.from.iter().enumerate() {
                if index != 0 {
                    self.output.push_str(", ");
                }
                self.write_table_with_joins(relation);
            }
        }
        if let Some(selection) = &select.selection {
            self.output.push_str(" WHERE ");
            self.write_expr(selection);
        }
        self.write_group_by(&select.group_by);
        if let Some(having) = &select.having {
            self.output.push_str(" HAVING ");
            self.write_expr(having);
        }
        if let Some(qualify) = &select.qualify {
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
    }

    fn write_select_item(&mut self, item: &SelectItem) {
        match item {
            SelectItem::UnnamedExpr(expr) => self.write_expr(expr),
            SelectItem::ExprWithAlias {
                expr,
                alias,
                explicit_as,
                ..
            } => {
                self.write_expr(expr);
                if *explicit_as {
                    self.output.push_str(" AS ");
                } else {
                    self.output.push(' ');
                }
                self.write_ident(alias);
            }
            SelectItem::Wildcard { options, .. } => {
                self.output.push('*');
                self.write_wildcard_options(options);
            }
            SelectItem::QualifiedWildcard {
                prefix, options, ..
            } => {
                self.write_ident_list_with_separator(prefix, ".");
                self.output.push_str(".*");
                self.write_wildcard_options(options);
            }
        }
    }

    fn write_wildcard_options(&mut self, options: &WildcardOptions) {
        if !options.exclude.is_empty() {
            self.output.push_str(" EXCLUDE (");
            self.write_ident_list(&options.exclude);
            self.output.push(')');
        }
        if !options.replace.is_empty() {
            self.output.push_str(" REPLACE (");
            for (index, item) in options.replace.iter().enumerate() {
                if index != 0 {
                    self.output.push_str(", ");
                }
                self.write_expr(&item.expr);
                self.output.push_str(" AS ");
                self.write_ident(&item.alias);
            }
            self.output.push(')');
        }
    }

    fn write_group_by(&mut self, group_by: &GroupBy) {
        let (keyword, expressions): (&str, &[Expr]) = match group_by {
            GroupBy::None => return,
            GroupBy::Expressions { expressions, .. } => ("GROUP BY ", expressions),
            GroupBy::Rollup { expressions, .. } => ("GROUP BY ROLLUP (", expressions),
            GroupBy::Cube { expressions, .. } => ("GROUP BY CUBE (", expressions),
            GroupBy::GroupingSets { sets, .. } => {
                self.output.push_str(" GROUP BY GROUPING SETS (");
                for (set_index, set) in sets.iter().enumerate() {
                    if set_index != 0 {
                        self.output.push_str(", ");
                    }
                    self.output.push('(');
                    self.write_expr_list(set);
                    self.output.push(')');
                }
                self.output.push(')');
                return;
            }
        };
        self.output.push(' ');
        self.output.push_str(keyword);
        self.write_expr_list(expressions);
        if matches!(group_by, GroupBy::Rollup { .. } | GroupBy::Cube { .. }) {
            self.output.push(')');
        }
    }

    fn write_order_by_list(&mut self, order_by: &[OrderByExpr]) {
        for (index, order) in order_by.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_expr(&order.expr);
            match order.asc {
                Some(true) => self.output.push_str(" ASC"),
                Some(false) => self.output.push_str(" DESC"),
                None => {}
            }
            match order.nulls_first {
                Some(true) => self.output.push_str(" NULLS FIRST"),
                Some(false) => self.output.push_str(" NULLS LAST"),
                None => {}
            }
        }
    }

    fn write_fetch(&mut self, fetch: &Fetch) {
        self.output.push_str(" FETCH FIRST");
        if let Some(quantity) = &fetch.quantity {
            self.output.push(' ');
            self.write_expr(quantity);
        }
        if fetch.percent {
            self.output.push_str(" PERCENT");
        }
        self.output.push_str(" ROWS");
        self.output.push_str(if fetch.with_ties {
            " WITH TIES"
        } else {
            " ONLY"
        });
    }

    fn write_table_with_joins(&mut self, relation: &TableWithJoins) {
        self.write_table_factor(&relation.relation);
        for join in &relation.joins {
            self.output.push(' ');
            self.write_join(join);
        }
    }

    fn write_table_factor(&mut self, factor: &TableFactor) {
        match factor {
            TableFactor::Table {
                name,
                metadata,
                alias,
                version,
                ..
            } => {
                self.write_table_name(name, metadata.as_ref());
                if let Some(version) = version {
                    self.write_table_version(version);
                }
                if let Some(alias) = alias {
                    self.output.push(' ');
                    self.write_table_alias(alias);
                }
            }
            TableFactor::Derived {
                lateral,
                subquery,
                alias,
                ..
            } => {
                if *lateral {
                    self.output.push_str("LATERAL ");
                }
                self.output.push('(');
                self.write_query(subquery);
                self.output.push(')');
                if let Some(alias) = alias {
                    self.output.push(' ');
                    self.write_table_alias(alias);
                }
            }
            TableFactor::TableFunction {
                lateral,
                syntax,
                expr,
                alias,
                ..
            } => {
                if *lateral {
                    self.output.push_str("LATERAL ");
                }
                match syntax {
                    crate::ast::TableFunctionSyntax::TableWrapper => {
                        self.output.push_str("TABLE(");
                        self.write_expr(expr);
                        self.output.push(')');
                    }
                    crate::ast::TableFunctionSyntax::BareCall => self.write_expr(expr),
                }
                if let Some(alias) = alias {
                    self.output.push(' ');
                    self.write_table_alias(alias);
                }
            }
            TableFactor::Unnest {
                keyword,
                lateral,
                array_exprs,
                with_offset,
                alias,
                ..
            } => {
                if *lateral {
                    self.output.push_str("LATERAL ");
                }
                self.write_ident(keyword);
                self.output.push('(');
                self.write_expr_list(array_exprs);
                self.output.push(')');
                if *with_offset {
                    self.output.push_str(" WITH OFFSET");
                }
                if let Some(alias) = alias {
                    self.output.push(' ');
                    self.write_table_alias(alias);
                }
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias,
                ..
            } => {
                self.output.push('(');
                self.write_table_with_joins(table_with_joins);
                self.output.push(')');
                if let Some(alias) = alias {
                    self.output.push(' ');
                    self.write_table_alias(alias);
                }
            }
        }
    }

    fn write_table_alias(&mut self, alias: &TableAlias) {
        if alias.explicit_as {
            self.output.push_str("AS ");
        }
        self.write_ident(&alias.name);
        if !alias.columns.is_empty() {
            self.output.push('(');
            self.write_ident_list(&alias.columns);
            self.output.push(')');
        }
    }

    fn write_table_version(&mut self, version: &TableVersion) {
        self.output.push_str(match version.kind {
            TableVersionKind::ForSystemTimeAsOf => " FOR SYSTEM_TIME AS OF ",
            TableVersionKind::ForVersionAsOf => " FOR VERSION AS OF ",
        });
        self.write_expr(&version.value);
    }

    fn write_join(&mut self, join: &Join) {
        if matches!(join.constraint, JoinConstraint::Natural(_)) {
            self.output.push_str("NATURAL ");
        }
        self.output.push_str(match join.operator {
            JoinOperator::Inner => "JOIN ",
            JoinOperator::InnerExplicit => "INNER JOIN ",
            JoinOperator::LeftOuter => "LEFT JOIN ",
            JoinOperator::LeftOuterExplicit => "LEFT OUTER JOIN ",
            JoinOperator::RightOuter => "RIGHT JOIN ",
            JoinOperator::RightOuterExplicit => "RIGHT OUTER JOIN ",
            JoinOperator::FullOuter => "FULL JOIN ",
            JoinOperator::FullOuterExplicit => "FULL OUTER JOIN ",
            JoinOperator::Cross => "CROSS JOIN ",
            JoinOperator::LeftSemi => "LEFT SEMI JOIN ",
            JoinOperator::RightSemi => "RIGHT SEMI JOIN ",
            JoinOperator::LeftAnti => "LEFT ANTI JOIN ",
            JoinOperator::RightAnti => "RIGHT ANTI JOIN ",
        });
        self.write_table_factor(&join.relation);
        match &join.constraint {
            JoinConstraint::None | JoinConstraint::Natural(_) => {}
            JoinConstraint::On(expr) => {
                self.output.push_str(" ON ");
                self.write_expr(expr);
            }
            JoinConstraint::Using { columns, .. } => {
                self.output.push_str(" USING (");
                self.write_ident_list(columns);
                self.output.push(')');
            }
        }
    }

    fn write_window_specification(&mut self, specification: &WindowSpec) {
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
        if !specification.order_by.is_empty() {
            if needs_separator {
                self.output.push(' ');
            }
            self.output.push_str("ORDER BY ");
            self.write_order_by_list(&specification.order_by);
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

    fn write_window_frame(&mut self, frame: &WindowFrame) {
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
    }

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

    fn write_expr(&mut self, expression: &Expr) {
        match expression {
            Expr::Identifier(ident) => self.write_ident(ident),
            Expr::CompoundIdentifier(ident) => {
                self.write_ident_list_with_separator(&ident.parts, ".")
            }
            Expr::Literal(literal) => self.write_literal(literal),
            Expr::FunctionCall(call) => self.write_function_call(call),
            Expr::Unary(expression) => self.write_unary_expr(expression),
            Expr::Binary(expression) => self.write_binary_expr(expression),
            Expr::Nested(expression) => self.write_nested_expr(expression),
            Expr::Between(expression) => self.write_between_expr(expression),
            Expr::InList(expression) => self.write_in_list_expr(expression),
            Expr::InSubquery(expression) => self.write_in_subquery_expr(expression),
            Expr::Exists(expression) => self.write_exists_expr(expression),
            Expr::Like(expression) => self.write_like_expr(expression),
            Expr::IsPredicate(expression) => self.write_is_predicate_expr(expression),
            Expr::Case(expression) => self.write_case_expr(expression),
            Expr::Cast(expression) => self.write_cast_expr(expression),
            Expr::Interval(expression) => self.write_interval_expr(expression),
            Expr::Subquery(expression) => self.write_subquery_expr(expression),
            Expr::Tuple(expression) => self.write_tuple_expr(expression),
            Expr::Array(expression) => self.write_array_expr(expression),
            Expr::Map(expression) => self.write_map_expr(expression),
            Expr::Struct(expression) => self.write_struct_expr(expression),
            Expr::Lambda(expression) => self.write_lambda_expr(expression),
            Expr::Access(expression) => self.write_access_expr(expression),
            Expr::TypedString(expression) => self.write_typed_string_expr(expression),
        }
    }

    fn write_ident(&mut self, ident: &Ident) {
        if let Some(quote) = ident.quote_style {
            self.output.push(quote);
            self.output
                .push_str(&ident.value.replace(quote, &format!("{quote}{quote}")));
            self.output.push(quote);
        } else {
            self.output.push_str(&ident.value);
        }
    }

    fn write_object_name(&mut self, name: &ObjectName) {
        self.write_ident_list_with_separator(&name.parts, ".");
    }

    fn write_table_name(&mut self, name: &ObjectName, metadata: Option<&Ident>) {
        self.write_object_name(name);
        if let Some(metadata) = metadata {
            self.output.push('$');
            self.write_ident(metadata);
        }
    }

    fn write_type_name(&mut self, type_name: &TypeName) {
        self.write_object_name(&type_name.name);
        if type_name.arguments.is_empty() {
            return;
        }
        let generic = matches!(
            type_name
                .name
                .parts
                .last()
                .map(|part| part.value.to_ascii_uppercase())
                .as_deref(),
            Some("ARRAY" | "MAP" | "STRUCT")
        );
        self.output.push(if generic { '<' } else { '(' });
        for (index, argument) in type_name.arguments.iter().enumerate() {
            if index != 0 {
                self.output
                    .push_str(if type_name.argument_separator_spaces[index - 1] {
                        ", "
                    } else {
                        ","
                    });
            }
            match argument {
                TypeNameArgument::Type(data_type) => self.write_type_name(data_type),
                TypeNameArgument::Literal(literal) => self.write_literal(literal),
                TypeNameArgument::Field(field) => {
                    self.write_ident(&field.name);
                    self.output.push(' ');
                    self.write_type_name(&field.data_type);
                }
            }
        }
        self.output.push(if generic { '>' } else { ')' });
    }

    fn write_literal(&mut self, literal: &Literal) {
        match &literal.kind {
            LiteralKind::Null => self.output.push_str("NULL"),
            LiteralKind::Boolean(value) => {
                self.output.push_str(if *value { "TRUE" } else { "FALSE" })
            }
            LiteralKind::Number(value) => self.output.push_str(value),
            LiteralKind::HexString(value) => {
                self.output.push_str("X'");
                self.output.push_str(value);
                self.output.push('\'');
            }
            LiteralKind::String(value) => self.write_quoted_string(value),
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

    fn write_function_call(&mut self, call: &FunctionCall) {
        if call.name.parts.len() == 1
            && call.name.parts[0].value.eq_ignore_ascii_case("EXTRACT")
            && call.arguments.len() == 2
            && matches!(call.quantifier, FunctionQuantifier::None)
            && call.order_by.is_empty()
            && call.separator.is_none()
            && call.filter.is_none()
            && call.null_treatment.is_none()
            && call.over.is_none()
        {
            self.output.push_str("EXTRACT(");
            self.write_expr(&call.arguments[0]);
            self.output.push_str(" FROM ");
            self.write_expr(&call.arguments[1]);
            self.output.push(')');
            return;
        }
        if call.substring_from_syntax
            && call.name.parts.len() == 1
            && call.name.parts[0].value.eq_ignore_ascii_case("SUBSTRING")
            && matches!(call.arguments.len(), 2 | 3)
            && matches!(call.quantifier, FunctionQuantifier::None)
            && call.order_by.is_empty()
            && call.separator.is_none()
            && call.filter.is_none()
            && call.null_treatment.is_none()
            && call.over.is_none()
        {
            self.output.push_str("SUBSTRING(");
            self.write_expr(&call.arguments[0]);
            self.output.push_str(" FROM ");
            self.write_expr(&call.arguments[1]);
            if let Some(length) = call.arguments.get(2) {
                self.output.push_str(" FOR ");
                self.write_expr(length);
            }
            self.output.push(')');
            return;
        }
        self.write_object_name(&call.name);
        self.output.push('(');
        match call.quantifier {
            FunctionQuantifier::None => {}
            FunctionQuantifier::Distinct => self.output.push_str("DISTINCT "),
            FunctionQuantifier::All => self.output.push_str("ALL "),
        }
        self.write_expr_list(&call.arguments);
        if let Some(null_treatment) = call.null_treatment {
            if !call.arguments.is_empty() {
                self.output.push(' ');
            }
            self.output.push_str(match null_treatment {
                NullTreatment::IgnoreNulls => "IGNORE NULLS",
                NullTreatment::RespectNulls => "RESPECT NULLS",
            });
        }
        if !call.order_by.is_empty() {
            if !call.arguments.is_empty() {
                self.output.push(' ');
            }
            self.output.push_str("ORDER BY ");
            for (index, order) in call.order_by.iter().enumerate() {
                if index != 0 {
                    self.output.push_str(", ");
                }
                self.write_expr(&order.expr);
                match order.asc {
                    Some(true) => self.output.push_str(" ASC"),
                    Some(false) => self.output.push_str(" DESC"),
                    None => {}
                }
                match order.nulls_first {
                    Some(true) => self.output.push_str(" NULLS FIRST"),
                    Some(false) => self.output.push_str(" NULLS LAST"),
                    None => {}
                }
            }
        }
        if let Some(separator) = &call.separator {
            self.output.push_str(" SEPARATOR ");
            self.write_expr(separator);
        }
        self.output.push(')');
        if let Some(filter) = &call.filter {
            self.output.push_str(" FILTER (WHERE ");
            self.write_expr(filter);
            self.output.push(')');
        }
        if let Some(over) = &call.over {
            self.output.push_str(" OVER ");
            self.write_window_specification(over);
        }
    }

    fn write_unary_expr(&mut self, expression: &UnaryExpr) {
        match expression.operator {
            UnaryOperator::Not => self.output.push_str("NOT "),
            UnaryOperator::Plus => self.output.push('+'),
            UnaryOperator::Minus => self.output.push('-'),
            UnaryOperator::BitwiseNot => self.output.push('~'),
        }
        let requires_separator = (matches!(expression.expression.as_ref(), Expr::Unary(_))
            && !matches!(expression.operator, UnaryOperator::Not))
            || (expression.operator == UnaryOperator::Minus
                && matches!(expression.expression.as_ref(), Expr::Literal(Literal {
                    kind: LiteralKind::Number(value), ..
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
            Expr::Binary(_)
                | Expr::Between(_)
                | Expr::InList(_)
                | Expr::InSubquery(_)
                | Expr::Like(_)
                | Expr::IsPredicate(_)
        )
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
            Expr::Binary(child) => {
                child.operator.precedence() < parent.precedence()
                    || (child.operator.precedence() == parent.precedence()
                        && matches!(side, BinarySide::Right))
            }
            Expr::Between(_)
            | Expr::InList(_)
            | Expr::InSubquery(_)
            | Expr::Like(_)
            | Expr::IsPredicate(_) => parent.precedence() > BinaryOperator::And.precedence(),
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

    fn write_nested_expr(&mut self, expression: &NestedExpr) {
        self.output.push('(');
        self.write_expr(&expression.expression);
        self.output.push(')');
    }
    fn write_between_expr(&mut self, expression: &BetweenExpr) {
        self.write_expr(&expression.expr);
        self.output.push_str(if expression.negated {
            " NOT BETWEEN "
        } else {
            " BETWEEN "
        });
        self.write_expr(&expression.low);
        self.output.push_str(" AND ");
        self.write_expr(&expression.high);
    }
    fn write_in_list_expr(&mut self, expression: &InListExpr) {
        self.write_expr(&expression.expr);
        self.output.push_str(if expression.negated {
            " NOT IN ("
        } else {
            " IN ("
        });
        self.write_expr_list(&expression.list);
        self.output.push(')');
    }
    fn write_in_subquery_expr(&mut self, expression: &InSubqueryExpr) {
        self.write_expr(&expression.expr);
        self.output.push_str(if expression.negated {
            " NOT IN ("
        } else {
            " IN ("
        });
        self.write_query(&expression.query);
        self.output.push(')');
    }
    fn write_exists_expr(&mut self, expression: &ExistsExpr) {
        if expression.negated {
            self.output.push_str("NOT ");
        }
        self.output.push_str("EXISTS (");
        self.write_query(&expression.query);
        self.output.push(')');
    }

    fn write_like_expr(&mut self, expression: &LikeExpr) {
        self.write_expr(&expression.expr);
        if expression.negated {
            self.output.push_str(" NOT");
        }
        self.output.push(' ');
        self.output.push_str(match expression.operator {
            LikeOperator::Like => "LIKE",
            LikeOperator::ILike => "ILIKE",
            LikeOperator::RLike => "RLIKE",
            LikeOperator::SimilarTo => "SIMILAR TO",
        });
        self.output.push(' ');
        self.write_expr(&expression.pattern);
        if let Some(escape) = &expression.escape {
            self.output.push_str(" ESCAPE ");
            self.write_expr(escape);
        }
    }

    fn write_is_predicate_expr(&mut self, expression: &IsPredicateExpr) {
        self.write_expr(&expression.expr);
        self.output.push_str(match expression.predicate {
            IsPredicate::Null => " IS NULL",
            IsPredicate::NotNull => " IS NOT NULL",
            IsPredicate::True => " IS TRUE",
            IsPredicate::NotTrue => " IS NOT TRUE",
            IsPredicate::False => " IS FALSE",
            IsPredicate::NotFalse => " IS NOT FALSE",
            IsPredicate::Unknown => " IS UNKNOWN",
            IsPredicate::NotUnknown => " IS NOT UNKNOWN",
        });
    }

    fn write_case_expr(&mut self, expression: &CaseExpr) {
        self.output.push_str("CASE");
        if let Some(operand) = &expression.operand {
            self.output.push(' ');
            self.write_expr(operand);
        }
        let common = expression.conditions.len().min(expression.results.len());
        for index in 0..common {
            self.output.push_str(" WHEN ");
            self.write_expr(&expression.conditions[index]);
            self.output.push_str(" THEN ");
            self.write_expr(&expression.results[index]);
        }
        for condition in &expression.conditions[common..] {
            self.output.push_str(" WHEN ");
            self.write_expr(condition);
            self.output.push_str(" THEN NULL");
        }
        for result in &expression.results[common..] {
            self.output.push_str(" WHEN TRUE THEN ");
            self.write_expr(result);
        }
        if let Some(else_result) = &expression.else_result {
            self.output.push_str(" ELSE ");
            self.write_expr(else_result);
        }
        self.output.push_str(" END");
    }

    fn write_cast_expr(&mut self, expression: &CastExpr) {
        match expression.kind {
            CastKind::Cast => self.output.push_str("CAST("),
            CastKind::TryCast => self.output.push_str("TRY_CAST("),
            CastKind::Convert => self.output.push_str("CONVERT("),
        }
        self.write_expr(&expression.expr);
        self.output.push_str(match expression.kind {
            CastKind::Cast | CastKind::TryCast => " AS ",
            CastKind::Convert => ", ",
        });
        self.write_type_name(&expression.data_type);
        if let Some(format) = &expression.format {
            self.output.push_str(" FORMAT ");
            self.write_expr(format);
        }
        self.output.push(')');
    }

    fn write_interval_expr(&mut self, expression: &IntervalExpr) {
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
            IntervalField::Quarter => "QUARTER",
            IntervalField::Month => "MONTH",
            IntervalField::Week => "WEEK",
            IntervalField::Day => "DAY",
            IntervalField::Hour => "HOUR",
            IntervalField::Minute => "MINUTE",
            IntervalField::Second => "SECOND",
            IntervalField::Millisecond => "MILLISECOND",
            IntervalField::Microsecond => "MICROSECOND",
        });
    }
    fn write_subquery_expr(&mut self, expression: &SubqueryExpr) {
        self.output.push('(');
        self.write_query(&expression.query);
        self.output.push(')');
    }
    fn write_tuple_expr(&mut self, expression: &TupleExpr) {
        self.output.push('(');
        self.write_expr_list(&expression.expressions);
        self.output.push(')');
    }
    fn write_array_expr(&mut self, expression: &ArrayExpr) {
        if let Some(element_type) = &expression.element_type {
            self.output.push_str("ARRAY<");
            self.write_type_name(element_type);
            self.output.push('>');
        }
        self.output.push('[');
        self.write_expr_list(&expression.elements);
        self.output.push(']');
    }

    fn write_map_expr(&mut self, expression: &MapExpr) {
        self.output.push_str("MAP{");
        for (index, entry) in expression.entries.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            self.write_expr(&entry.key);
            self.output.push_str(": ");
            self.write_expr(&entry.value);
        }
        self.output.push('}');
    }

    fn write_struct_expr(&mut self, expression: &StructExpr) {
        self.output.push_str("STRUCT(");
        for (index, field) in expression.fields.iter().enumerate() {
            if index != 0 {
                self.output.push_str(", ");
            }
            if let Some(name) = &field.name {
                self.write_ident(name);
                self.output.push_str(" := ");
            }
            self.write_expr(&field.value);
        }
        self.output.push(')');
    }

    fn write_lambda_expr(&mut self, expression: &LambdaExpr) {
        if expression.parameters.len() == 1 && !expression.parenthesized_single_parameter {
            self.write_ident(&expression.parameters[0]);
        } else {
            self.output.push('(');
            self.write_ident_list(&expression.parameters);
            self.output.push(')');
        }
        self.output.push_str(" -> ");
        self.write_expr(&expression.body);
    }
    fn write_access_expr(&mut self, expression: &AccessExpr) {
        self.write_expr(&expression.expr);
        match &expression.kind {
            AccessKind::Field(name) => {
                self.output.push('.');
                self.write_ident(name);
            }
            AccessKind::Subscript(index) => {
                self.output.push('[');
                self.write_expr(index);
                self.output.push(']');
            }
            AccessKind::Json { operator, path } => {
                self.output.push_str(match operator {
                    JsonOperator::Arrow => " -> ",
                    JsonOperator::ArrowText => " ->> ",
                });
                self.write_expr(path);
            }
        }
    }
    fn write_typed_string_expr(&mut self, expression: &TypedStringExpr) {
        self.write_type_name(&expression.data_type);
        self.output.push(' ');
        self.write_literal(&expression.value);
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

impl BinaryOperator {
    const fn precedence(self) -> u8 {
        match self {
            Self::NamedArgument => 1,
            Self::Or => 10,
            Self::And => 20,
            Self::Equal
            | Self::NotEqual
            | Self::NullSafeEqual
            | Self::LessThan
            | Self::LessThanOrEqual
            | Self::GreaterThan
            | Self::GreaterThanOrEqual
            | Self::IsDistinctFrom
            | Self::IsNotDistinctFrom => 30,
            Self::BitwiseOr => 35,
            Self::BitwiseXor => 36,
            Self::BitwiseAnd => 37,
            Self::ShiftLeft | Self::ShiftRight => 40,
            Self::StringConcat | Self::Add | Self::Subtract => 50,
            Self::Multiply | Self::Divide | Self::Modulo => 60,
        }
    }
    const fn sql(self) -> &'static str {
        match self {
            Self::NamedArgument => "=>",
            Self::Or => "OR",
            Self::And => "AND",
            Self::Equal => "=",
            Self::NotEqual => "!=",
            Self::NullSafeEqual => "<=>",
            Self::LessThan => "<",
            Self::LessThanOrEqual => "<=",
            Self::GreaterThan => ">",
            Self::GreaterThanOrEqual => ">=",
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Modulo => "%",
            Self::BitwiseAnd => "&",
            Self::BitwiseOr => "|",
            Self::BitwiseXor => "^",
            Self::ShiftLeft => "<<",
            Self::ShiftRight => ">>",
            Self::StringConcat => "||",
            Self::IsDistinctFrom => "IS DISTINCT FROM",
            Self::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
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
            quote_style: None,
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
            kind: LiteralKind::Number(value.to_owned()),
            span: span(),
        })
    }

    fn binary(left: Expr, operator: BinaryOperator, right: Expr) -> Expr {
        Expr::Binary(BinaryExpr {
            left: Box::new(left),
            operator,
            right: Box::new(right),
            span: span(),
        })
    }

    #[test]
    fn renders_extended_expression_forms() {
        let function = Expr::FunctionCall(FunctionCall {
            name: object_name("Coalesce"),
            arguments: vec![identifier("a"), number("1")],
            quantifier: FunctionQuantifier::Distinct,
            order_by: Vec::new(),
            separator: None,
            filter: Some(Box::new(Expr::IsPredicate(IsPredicateExpr {
                expr: Box::new(identifier("b")),
                predicate: IsPredicate::NotNull,
                span: span(),
            }))),
            null_treatment: Some(NullTreatment::IgnoreNulls),
            over: Some(Box::new(WindowSpec {
                existing_window_name: None,
                partition_by: vec![identifier("partition_key")],
                order_by: Vec::new(),
                window_frame: None,
                span: span(),
            })),
            substring_from_syntax: false,
            span: span(),
        });
        assert_eq!(
            print_expr(&function),
            "Coalesce(DISTINCT a, 1 IGNORE NULLS) FILTER (WHERE b IS NOT NULL) OVER (PARTITION BY partition_key)"
        );

        let between = Expr::Between(BetweenExpr {
            expr: Box::new(binary(
                identifier("a"),
                BinaryOperator::Add,
                identifier("b"),
            )),
            negated: true,
            low: Box::new(number("1")),
            high: Box::new(number("2")),
            span: span(),
        });
        assert_eq!(print_expr(&between), "a + b NOT BETWEEN 1 AND 2");
        assert_eq!(
            print_expr(&Expr::Access(AccessExpr {
                expr: Box::new(identifier("payload")),
                kind: AccessKind::Json {
                    operator: JsonOperator::ArrowText,
                    path: Box::new(Expr::Literal(Literal {
                        kind: LiteralKind::String("name".to_owned()),
                        span: span(),
                    })),
                },
                span: span(),
            })),
            "payload ->> 'name'"
        );
        assert_eq!(
            print_expr(&Expr::Lambda(LambdaExpr {
                parameters: vec![ident("value")],
                parenthesized_single_parameter: false,
                body: Box::new(identifier("value")),
                span: span(),
            })),
            "value -> value"
        );
    }

    #[test]
    fn renders_query_relations_and_modifiers() {
        let query = Query {
            with: Some(With {
                recursive: false,
                ctes: vec![Cte {
                    name: ident("source"),
                    columns: Vec::new(),
                    query: Box::new(Query {
                        with: None,
                        body: Box::new(SetExpr::Values(Values {
                            rows: vec![vec![number("1")]],
                            explicit_row: false,
                            span: span(),
                        })),
                        order_by: Vec::new(),
                        limit: None,
                        offset: None,
                        limit_comma_offset: false,
                        fetch: None,
                        span: span(),
                    }),
                    span: span(),
                }],
                span: span(),
            }),
            body: Box::new(SetExpr::Select(Box::new(Select {
                hints: Vec::new(),
                quantifier: SelectQuantifier::Distinct {
                    on: vec![identifier("s")],
                    span: span(),
                },
                projection: vec![SelectItem::ExprWithAlias {
                    expr: identifier("s"),
                    alias: ident("value"),
                    explicit_as: true,
                    span: span(),
                }],
                from: vec![TableWithJoins {
                    relation: TableFactor::Table {
                        name: object_name("source"),
                        metadata: None,
                        alias: Some(TableAlias {
                            name: ident("s"),
                            columns: Vec::new(),
                            explicit_as: false,
                            span: span(),
                        }),
                        version: None,
                        span: span(),
                    },
                    joins: Vec::new(),
                    span: span(),
                }],
                selection: None,
                group_by: GroupBy::None,
                having: None,
                qualify: None,
                windows: Vec::new(),
                span: span(),
            }))),
            order_by: vec![OrderByExpr {
                expr: identifier("value"),
                asc: Some(false),
                nulls_first: Some(false),
                span: span(),
            }],
            limit: Some(number("10")),
            offset: Some(Offset {
                value: number("2"),
                rows: OffsetRows::Rows,
                span: span(),
            }),
            limit_comma_offset: false,
            fetch: Some(Fetch {
                quantity: Some(number("3")),
                percent: false,
                with_ties: false,
                span: span(),
            }),
            span: span(),
        };

        assert_eq!(
            print_statement(&Statement::ExplainQuery(ExplainQuery {
                format: ExplainFormat::Verbose,
                logical: false,
                query: Box::new(query),
                span: span(),
            })),
            "EXPLAIN VERBOSE WITH source AS (VALUES (1)) SELECT DISTINCT ON (s) s AS value FROM source s ORDER BY value DESC NULLS LAST LIMIT 10 OFFSET 2 ROWS FETCH FIRST 3 ROWS ONLY"
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
}

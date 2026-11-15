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

//! Recursive traversal and rebuilding helpers for every AST node.

use super::*;

/// Visits AST nodes by shared reference.
pub trait Visit {
    fn visit_statement(&mut self, statement: &Statement) {
        walk_statement(self, statement);
    }

    fn visit_show_process_list(&mut self, statement: &ShowProcessList) {
        walk_show_process_list(self, statement);
    }

    fn visit_statistics_statement(&mut self, statement: &StatisticsStatement) {
        walk_statistics_statement(self, statement);
    }

    fn visit_catalog_statement(&mut self, statement: &CatalogStatement) {
        walk_catalog_statement(self, statement);
    }

    fn visit_iceberg_statement(&mut self, statement: &IcebergStatement) {
        walk_iceberg_statement(self, statement);
    }

    fn visit_maintenance_statement(&mut self, statement: &MaintenanceStatement) {
        walk_maintenance_statement(self, statement);
    }

    fn visit_materialized_view_statement(&mut self, statement: &MaterializedViewStatement) {
        walk_materialized_view_statement(self, statement);
    }

    fn visit_view_statement(&mut self, statement: &ViewStatement) {
        walk_view_statement(self, statement);
    }

    fn visit_table_statement(&mut self, statement: &TableStatement) {
        walk_table_statement(self, statement);
    }

    fn visit_dml_statement(&mut self, statement: &DmlStatement) {
        walk_dml_statement(self, statement);
    }

    fn visit_session_statement(&mut self, statement: &SessionStatement) {
        walk_session_statement(self, statement);
    }

    fn visit_query(&mut self, query: &Query) {
        walk_query(self, query);
    }

    fn visit_explain_query(&mut self, query: &ExplainQuery) {
        walk_explain_query(self, query);
    }

    fn visit_ident(&mut self, ident: &Ident) {
        walk_ident(self, ident);
    }

    fn visit_object_name(&mut self, name: &ObjectName) {
        walk_object_name(self, name);
    }

    fn visit_type_name(&mut self, type_name: &TypeName) {
        walk_type_name(self, type_name);
    }

    fn visit_literal(&mut self, literal: &Literal) {
        walk_literal(self, literal);
    }

    fn visit_expr(&mut self, expression: &Expr) {
        walk_expr(self, expression);
    }

    fn visit_function_call(&mut self, call: &FunctionCall) {
        walk_function_call(self, call);
    }

    fn visit_unary_expr(&mut self, expression: &UnaryExpr) {
        walk_unary_expr(self, expression);
    }

    fn visit_binary_expr(&mut self, expression: &BinaryExpr) {
        walk_binary_expr(self, expression);
    }

    fn visit_nested_expr(&mut self, expression: &NestedExpr) {
        walk_nested_expr(self, expression);
    }

    fn visit_table_factor(&mut self, factor: &TableFactor) {
        walk_table_factor(self, factor);
    }
}

pub fn walk_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &Statement) {
    match statement {
        Statement::ShowProcessList(statement) => visitor.visit_show_process_list(statement),
        Statement::Statistics(statement) => visitor.visit_statistics_statement(statement),
        Statement::Catalog(statement) => visitor.visit_catalog_statement(statement),
        Statement::Iceberg(statement) => visitor.visit_iceberg_statement(statement),
        Statement::Maintenance(statement) => visitor.visit_maintenance_statement(statement),
        Statement::MaterializedView(statement) => {
            visitor.visit_materialized_view_statement(statement)
        }
        Statement::View(statement) => visitor.visit_view_statement(statement),
        Statement::Table(statement) => visitor.visit_table_statement(statement),
        Statement::Dml(statement) => visitor.visit_dml_statement(statement),
        Statement::Session(statement) => visitor.visit_session_statement(statement),
        Statement::Query(query) => visitor.visit_query(query),
        Statement::ExplainQuery(query) => visitor.visit_explain_query(query),
    }
}

pub fn walk_show_process_list<V: Visit + ?Sized>(_: &mut V, _: &ShowProcessList) {}

pub fn walk_statistics_statement<V: Visit + ?Sized>(
    visitor: &mut V,
    statement: &StatisticsStatement,
) {
    super::statistics::walk(visitor, statement);
}

pub fn walk_catalog_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &CatalogStatement) {
    super::catalog::walk(visitor, statement);
}

pub fn walk_iceberg_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &IcebergStatement) {
    super::iceberg::walk(visitor, statement);
}

pub fn walk_maintenance_statement<V: Visit + ?Sized>(
    visitor: &mut V,
    statement: &MaintenanceStatement,
) {
    super::maintenance::walk(visitor, statement);
}

pub fn walk_materialized_view_statement<V: Visit + ?Sized>(
    visitor: &mut V,
    statement: &MaterializedViewStatement,
) {
    super::materialized_view::walk(visitor, statement);
}

pub fn walk_view_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &ViewStatement) {
    super::view::walk(visitor, statement);
}

pub fn walk_table_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &TableStatement) {
    super::table::walk(visitor, statement);
}

pub fn walk_dml_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &DmlStatement) {
    super::dml::walk(visitor, statement);
}

pub fn walk_ident<V: Visit + ?Sized>(_: &mut V, _: &Ident) {}

pub fn walk_object_name<V: Visit + ?Sized>(visitor: &mut V, name: &ObjectName) {
    for part in &name.parts {
        visitor.visit_ident(part);
    }
}

pub fn walk_type_name<V: Visit + ?Sized>(visitor: &mut V, type_name: &TypeName) {
    visitor.visit_object_name(&type_name.name);
    for argument in &type_name.arguments {
        match argument {
            TypeNameArgument::Type(data_type) => visitor.visit_type_name(data_type),
            TypeNameArgument::Literal(literal) => visitor.visit_literal(literal),
            TypeNameArgument::Field(field) => {
                visitor.visit_ident(&field.name);
                visitor.visit_type_name(&field.data_type);
            }
        }
    }
}

pub fn walk_literal<V: Visit + ?Sized>(_: &mut V, _: &Literal) {}

pub fn walk_explain_query<V: Visit + ?Sized>(visitor: &mut V, query: &ExplainQuery) {
    visitor.visit_query(&query.query);
}

pub fn walk_query<V: Visit + ?Sized>(visitor: &mut V, query: &Query) {
    if let Some(with) = &query.with {
        for cte in &with.ctes {
            visitor.visit_ident(&cte.name);
            for column in &cte.columns {
                visitor.visit_ident(column);
            }
            visitor.visit_query(&cte.query);
        }
    }
    walk_set_expr(visitor, &query.body);
    for order in &query.order_by {
        visitor.visit_expr(&order.expr);
    }
    if let Some(limit) = &query.limit {
        visitor.visit_expr(limit);
    }
    if let Some(offset) = &query.offset {
        visitor.visit_expr(&offset.value);
    }
    if let Some(fetch) = &query.fetch
        && let Some(quantity) = &fetch.quantity
    {
        visitor.visit_expr(quantity);
    }
}

pub fn walk_set_expr<V: Visit + ?Sized>(visitor: &mut V, set_expr: &SetExpr) {
    match set_expr {
        SetExpr::Select(select) => walk_select(visitor, select),
        SetExpr::Values(values) => {
            for row in &values.rows {
                for expr in row {
                    visitor.visit_expr(expr);
                }
            }
        }
        SetExpr::Query(query) => visitor.visit_query(query),
        SetExpr::SetOperation(operation) => {
            walk_set_expr(visitor, &operation.left);
            walk_set_expr(visitor, &operation.right);
        }
    }
}

pub fn walk_select<V: Visit + ?Sized>(visitor: &mut V, select: &Select) {
    for hint in &select.hints {
        visitor.visit_ident(&hint.name);
        match &hint.value {
            SelectHintValue::Bare => {}
            SelectHintValue::Call { arguments } => {
                for argument in arguments {
                    visitor.visit_expr(argument);
                }
            }
            SelectHintValue::Assignment { value } => visitor.visit_expr(value),
        }
    }
    if let SelectQuantifier::Distinct { on, .. } = &select.quantifier {
        for expr in on {
            visitor.visit_expr(expr);
        }
    }
    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => visitor.visit_expr(expr),
            SelectItem::ExprWithAlias { expr, alias, .. } => {
                visitor.visit_expr(expr);
                visitor.visit_ident(alias);
            }
            SelectItem::Wildcard { options, .. }
            | SelectItem::QualifiedWildcard { options, .. } => {
                for ident in &options.exclude {
                    visitor.visit_ident(ident);
                }
                for replacement in &options.replace {
                    visitor.visit_expr(&replacement.expr);
                    visitor.visit_ident(&replacement.alias);
                }
            }
        }
        if let SelectItem::QualifiedWildcard { prefix, .. } = item {
            for ident in prefix {
                visitor.visit_ident(ident);
            }
        }
    }
    for relation in &select.from {
        walk_table_with_joins(visitor, relation);
    }
    if let Some(selection) = &select.selection {
        visitor.visit_expr(selection);
    }
    walk_group_by(visitor, &select.group_by);
    if let Some(having) = &select.having {
        visitor.visit_expr(having);
    }
    if let Some(qualify) = &select.qualify {
        visitor.visit_expr(qualify);
    }
    for window in &select.windows {
        walk_named_window(visitor, window);
    }
}

pub fn walk_group_by<V: Visit + ?Sized>(visitor: &mut V, group_by: &GroupBy) {
    let groups: &[Vec<Expr>] = match group_by {
        GroupBy::None => return,
        GroupBy::Expressions { expressions, .. }
        | GroupBy::Rollup { expressions, .. }
        | GroupBy::Cube { expressions, .. } => std::slice::from_ref(expressions),
        GroupBy::GroupingSets { sets, .. } => sets,
    };
    for group in groups {
        for expr in group {
            visitor.visit_expr(expr);
        }
    }
}

pub fn walk_table_with_joins<V: Visit + ?Sized>(visitor: &mut V, table: &TableWithJoins) {
    visitor.visit_table_factor(&table.relation);
    for join in &table.joins {
        visitor.visit_table_factor(&join.relation);
        match &join.constraint {
            JoinConstraint::On(expr) => visitor.visit_expr(expr),
            JoinConstraint::Using { columns, .. } => {
                for column in columns {
                    visitor.visit_ident(column);
                }
            }
            JoinConstraint::None | JoinConstraint::Natural(_) => {}
        }
    }
}

pub fn walk_table_factor<V: Visit + ?Sized>(visitor: &mut V, factor: &TableFactor) {
    match factor {
        TableFactor::Table {
            name,
            metadata,
            alias,
            version,
            ..
        } => {
            visitor.visit_object_name(name);
            if let Some(metadata) = metadata {
                visitor.visit_ident(metadata);
            }
            if let Some(alias) = alias {
                walk_table_alias(visitor, alias);
            }
            if let Some(version) = version {
                visitor.visit_expr(&version.value);
            }
        }
        TableFactor::Derived {
            subquery, alias, ..
        } => {
            visitor.visit_query(subquery);
            if let Some(alias) = alias {
                walk_table_alias(visitor, alias);
            }
        }
        TableFactor::TableFunction { expr, alias, .. } => {
            visitor.visit_expr(expr);
            if let Some(alias) = alias {
                walk_table_alias(visitor, alias);
            }
        }
        TableFactor::Unnest {
            keyword,
            array_exprs,
            alias,
            ..
        } => {
            visitor.visit_ident(keyword);
            for expr in array_exprs {
                visitor.visit_expr(expr);
            }
            if let Some(alias) = alias {
                walk_table_alias(visitor, alias);
            }
        }
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
            ..
        } => {
            walk_table_with_joins(visitor, table_with_joins);
            if let Some(alias) = alias {
                walk_table_alias(visitor, alias);
            }
        }
    }
}

pub fn walk_table_alias<V: Visit + ?Sized>(visitor: &mut V, alias: &TableAlias) {
    visitor.visit_ident(&alias.name);
    for column in &alias.columns {
        visitor.visit_ident(column);
    }
}

pub fn walk_named_window<V: Visit + ?Sized>(visitor: &mut V, window: &NamedWindow) {
    visitor.visit_ident(&window.name);
    walk_window_spec(visitor, &window.specification);
}

pub fn walk_window_spec<V: Visit + ?Sized>(visitor: &mut V, window: &WindowSpec) {
    if let Some(existing) = &window.existing_window_name {
        visitor.visit_ident(existing);
    }
    for expr in &window.partition_by {
        visitor.visit_expr(expr);
    }
    for order in &window.order_by {
        visitor.visit_expr(&order.expr);
    }
    if let Some(frame) = &window.window_frame {
        walk_window_frame_bound(visitor, &frame.start_bound);
        if let Some(end) = &frame.end_bound {
            walk_window_frame_bound(visitor, end);
        }
    }
}

pub fn walk_window_frame_bound<V: Visit + ?Sized>(visitor: &mut V, bound: &WindowFrameBound) {
    match bound {
        WindowFrameBound::Preceding(Some(expr), _) | WindowFrameBound::Following(Some(expr), _) => {
            visitor.visit_expr(expr)
        }
        _ => {}
    }
}

pub fn walk_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &Expr) {
    match expression {
        Expr::Identifier(ident) => visitor.visit_ident(ident),
        Expr::CompoundIdentifier(ident) => {
            for part in &ident.parts {
                visitor.visit_ident(part);
            }
        }
        Expr::Literal(literal) => visitor.visit_literal(literal),
        Expr::FunctionCall(call) => visitor.visit_function_call(call),
        Expr::Unary(expression) => visitor.visit_unary_expr(expression),
        Expr::Binary(expression) => visitor.visit_binary_expr(expression),
        Expr::Nested(expression) => visitor.visit_nested_expr(expression),
        Expr::Between(expression) => {
            visitor.visit_expr(&expression.expr);
            visitor.visit_expr(&expression.low);
            visitor.visit_expr(&expression.high);
        }
        Expr::InList(expression) => {
            visitor.visit_expr(&expression.expr);
            for item in &expression.list {
                visitor.visit_expr(item);
            }
        }
        Expr::InSubquery(expression) => {
            visitor.visit_expr(&expression.expr);
            visitor.visit_query(&expression.query);
        }
        Expr::Exists(expression) => visitor.visit_query(&expression.query),
        Expr::Like(expression) => {
            visitor.visit_expr(&expression.expr);
            visitor.visit_expr(&expression.pattern);
            if let Some(escape) = &expression.escape {
                visitor.visit_expr(escape);
            }
        }
        Expr::IsPredicate(expression) => visitor.visit_expr(&expression.expr),
        Expr::Case(expression) => {
            if let Some(operand) = &expression.operand {
                visitor.visit_expr(operand);
            }
            for condition in &expression.conditions {
                visitor.visit_expr(condition);
            }
            for result in &expression.results {
                visitor.visit_expr(result);
            }
            if let Some(result) = &expression.else_result {
                visitor.visit_expr(result);
            }
        }
        Expr::Cast(expression) => {
            visitor.visit_expr(&expression.expr);
            visitor.visit_type_name(&expression.data_type);
            if let Some(format) = &expression.format {
                visitor.visit_expr(format);
            }
        }
        Expr::Interval(expression) => {
            visitor.visit_expr(&expression.value);
            if let Some(precision) = &expression.leading_precision {
                visitor.visit_expr(precision);
            }
            if let Some(precision) = &expression.fractional_seconds_precision {
                visitor.visit_expr(precision);
            }
        }
        Expr::Subquery(expression) => visitor.visit_query(&expression.query),
        Expr::Tuple(expression) => {
            for expr in &expression.expressions {
                visitor.visit_expr(expr);
            }
        }
        Expr::Array(expression) => {
            if let Some(element_type) = &expression.element_type {
                visitor.visit_type_name(element_type);
            }
            for expr in &expression.elements {
                visitor.visit_expr(expr);
            }
        }
        Expr::Map(expression) => {
            for entry in &expression.entries {
                visitor.visit_expr(&entry.key);
                visitor.visit_expr(&entry.value);
            }
        }
        Expr::Struct(expression) => {
            for field in &expression.fields {
                if let Some(name) = &field.name {
                    visitor.visit_ident(name);
                }
                visitor.visit_expr(&field.value);
            }
        }
        Expr::Lambda(expression) => {
            for parameter in &expression.parameters {
                visitor.visit_ident(parameter);
            }
            visitor.visit_expr(&expression.body);
        }
        Expr::Access(expression) => {
            visitor.visit_expr(&expression.expr);
            match &expression.kind {
                AccessKind::Field(name) => visitor.visit_ident(name),
                AccessKind::Subscript(index) => visitor.visit_expr(index),
                AccessKind::Json { path, .. } => visitor.visit_expr(path),
            }
        }
        Expr::TypedString(expression) => {
            visitor.visit_type_name(&expression.data_type);
            visitor.visit_literal(&expression.value);
        }
    }
}

pub fn walk_function_call<V: Visit + ?Sized>(visitor: &mut V, call: &FunctionCall) {
    visitor.visit_object_name(&call.name);
    for argument in &call.arguments {
        visitor.visit_expr(argument);
    }
    for order in &call.order_by {
        visitor.visit_expr(&order.expr);
    }
    if let Some(separator) = &call.separator {
        visitor.visit_expr(separator);
    }
    if let Some(filter) = &call.filter {
        visitor.visit_expr(filter);
    }
    if let Some(over) = &call.over {
        walk_window_spec(visitor, over);
    }
}

pub fn walk_unary_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &UnaryExpr) {
    visitor.visit_expr(&expression.expression);
}

pub fn walk_binary_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &BinaryExpr) {
    visitor.visit_expr(&expression.left);
    visitor.visit_expr(&expression.right);
}

pub fn walk_nested_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &NestedExpr) {
    visitor.visit_expr(&expression.expression);
}

/// Rebuilds AST nodes by value, recursively folding all children by default.
pub trait Fold {
    fn fold_statement(&mut self, statement: Statement) -> Statement {
        fold_statement(self, statement)
    }

    fn fold_show_process_list(&mut self, statement: ShowProcessList) -> ShowProcessList {
        fold_show_process_list(self, statement)
    }

    fn fold_statistics_statement(&mut self, statement: StatisticsStatement) -> StatisticsStatement {
        fold_statistics_statement(self, statement)
    }

    fn fold_catalog_statement(&mut self, statement: CatalogStatement) -> CatalogStatement {
        fold_catalog_statement(self, statement)
    }

    fn fold_iceberg_statement(&mut self, statement: IcebergStatement) -> IcebergStatement {
        fold_iceberg_statement(self, statement)
    }

    fn fold_maintenance_statement(
        &mut self,
        statement: MaintenanceStatement,
    ) -> MaintenanceStatement {
        fold_maintenance_statement(self, statement)
    }

    fn fold_materialized_view_statement(
        &mut self,
        statement: MaterializedViewStatement,
    ) -> MaterializedViewStatement {
        fold_materialized_view_statement(self, statement)
    }

    fn fold_view_statement(&mut self, statement: ViewStatement) -> ViewStatement {
        fold_view_statement(self, statement)
    }

    fn fold_table_statement(&mut self, statement: TableStatement) -> TableStatement {
        fold_table_statement(self, statement)
    }

    fn fold_dml_statement(&mut self, statement: DmlStatement) -> DmlStatement {
        fold_dml_statement(self, statement)
    }

    fn fold_session_statement(&mut self, statement: SessionStatement) -> SessionStatement {
        fold_session_statement(self, statement)
    }

    fn fold_query(&mut self, query: Query) -> Query {
        fold_query(self, query)
    }

    fn fold_select(&mut self, select: Select) -> Select {
        fold_select(self, select)
    }

    fn fold_explain_query(&mut self, query: ExplainQuery) -> ExplainQuery {
        fold_explain_query(self, query)
    }

    fn fold_ident(&mut self, ident: Ident) -> Ident {
        fold_ident(self, ident)
    }

    fn fold_object_name(&mut self, name: ObjectName) -> ObjectName {
        fold_object_name(self, name)
    }

    fn fold_type_name(&mut self, type_name: TypeName) -> TypeName {
        fold_type_name(self, type_name)
    }

    fn fold_literal(&mut self, literal: Literal) -> Literal {
        fold_literal(self, literal)
    }

    fn fold_expr(&mut self, expression: Expr) -> Expr {
        fold_expr(self, expression)
    }

    fn fold_function_call(&mut self, call: FunctionCall) -> FunctionCall {
        fold_function_call(self, call)
    }

    fn fold_unary_expr(&mut self, expression: UnaryExpr) -> UnaryExpr {
        fold_unary_expr(self, expression)
    }

    fn fold_binary_expr(&mut self, expression: BinaryExpr) -> BinaryExpr {
        fold_binary_expr(self, expression)
    }

    fn fold_nested_expr(&mut self, expression: NestedExpr) -> NestedExpr {
        fold_nested_expr(self, expression)
    }

    fn fold_table_factor(&mut self, factor: TableFactor) -> TableFactor {
        fold_table_factor(self, factor)
    }
}

pub fn fold_statement<F: Fold + ?Sized>(folder: &mut F, statement: Statement) -> Statement {
    match statement {
        Statement::ShowProcessList(statement) => {
            Statement::ShowProcessList(folder.fold_show_process_list(statement))
        }
        Statement::Statistics(statement) => {
            Statement::Statistics(folder.fold_statistics_statement(statement))
        }
        Statement::Catalog(statement) => {
            Statement::Catalog(folder.fold_catalog_statement(statement))
        }
        Statement::Iceberg(statement) => {
            Statement::Iceberg(folder.fold_iceberg_statement(statement))
        }
        Statement::Maintenance(statement) => {
            Statement::Maintenance(folder.fold_maintenance_statement(statement))
        }
        Statement::MaterializedView(statement) => {
            Statement::MaterializedView(folder.fold_materialized_view_statement(statement))
        }
        Statement::View(statement) => Statement::View(folder.fold_view_statement(statement)),
        Statement::Table(statement) => Statement::Table(folder.fold_table_statement(statement)),
        Statement::Dml(statement) => Statement::Dml(folder.fold_dml_statement(statement)),
        Statement::Session(statement) => {
            Statement::Session(folder.fold_session_statement(statement))
        }
        Statement::Query(query) => Statement::Query(folder.fold_query(query)),
        Statement::ExplainQuery(query) => Statement::ExplainQuery(folder.fold_explain_query(query)),
    }
}

pub fn fold_show_process_list<F: Fold + ?Sized>(
    _: &mut F,
    statement: ShowProcessList,
) -> ShowProcessList {
    statement
}

pub fn fold_statistics_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: StatisticsStatement,
) -> StatisticsStatement {
    super::statistics::fold(folder, statement)
}

pub fn fold_catalog_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: CatalogStatement,
) -> CatalogStatement {
    super::catalog::fold(folder, statement)
}

pub fn fold_iceberg_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: IcebergStatement,
) -> IcebergStatement {
    super::iceberg::fold(folder, statement)
}

pub fn fold_maintenance_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: MaintenanceStatement,
) -> MaintenanceStatement {
    super::maintenance::fold(folder, statement)
}

pub fn fold_materialized_view_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: MaterializedViewStatement,
) -> MaterializedViewStatement {
    super::materialized_view::fold(folder, statement)
}

pub fn fold_view_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: ViewStatement,
) -> ViewStatement {
    super::view::fold(folder, statement)
}

pub fn fold_table_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: TableStatement,
) -> TableStatement {
    super::table::fold(folder, statement)
}

pub fn fold_dml_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: DmlStatement,
) -> DmlStatement {
    super::dml::fold(folder, statement)
}

pub fn walk_session_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &SessionStatement) {
    match statement {
        SessionStatement::Set(statement) => {
            for assignment in &statement.assignments {
                visitor.visit_ident(&assignment.name);
                visitor.visit_expr(&assignment.value);
            }
        }
        SessionStatement::Use(statement) => visitor.visit_ident(&statement.database),
        SessionStatement::Kill(statement) => visitor.visit_literal(&statement.connection_id),
        SessionStatement::TransactionControl(_) => {}
    }
}

pub fn fold_session_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: SessionStatement,
) -> SessionStatement {
    match statement {
        SessionStatement::Set(mut statement) => {
            for assignment in &mut statement.assignments {
                assignment.name = folder.fold_ident(assignment.name.clone());
                assignment.value = folder.fold_expr(assignment.value.clone());
            }
            SessionStatement::Set(statement)
        }
        SessionStatement::Use(mut statement) => {
            statement.database = folder.fold_ident(statement.database);
            SessionStatement::Use(statement)
        }
        SessionStatement::Kill(mut statement) => {
            statement.connection_id = folder.fold_literal(statement.connection_id);
            SessionStatement::Kill(statement)
        }
        SessionStatement::TransactionControl(statement) => {
            SessionStatement::TransactionControl(statement)
        }
    }
}

pub fn fold_ident<F: Fold + ?Sized>(_: &mut F, ident: Ident) -> Ident {
    ident
}

pub fn fold_object_name<F: Fold + ?Sized>(folder: &mut F, mut name: ObjectName) -> ObjectName {
    name.parts = name
        .parts
        .into_iter()
        .map(|part| folder.fold_ident(part))
        .collect();
    name
}

pub fn fold_type_name<F: Fold + ?Sized>(folder: &mut F, mut type_name: TypeName) -> TypeName {
    type_name.name = folder.fold_object_name(type_name.name);
    type_name.arguments = type_name
        .arguments
        .into_iter()
        .map(|argument| match argument {
            TypeNameArgument::Type(data_type) => {
                TypeNameArgument::Type(folder.fold_type_name(data_type))
            }
            TypeNameArgument::Literal(literal) => {
                TypeNameArgument::Literal(folder.fold_literal(literal))
            }
            TypeNameArgument::Field(field) => TypeNameArgument::Field(StructField {
                name: folder.fold_ident(field.name),
                data_type: folder.fold_type_name(field.data_type),
                span: field.span,
            }),
        })
        .collect();
    type_name
}

pub fn fold_literal<F: Fold + ?Sized>(_: &mut F, literal: Literal) -> Literal {
    literal
}

pub fn fold_explain_query<F: Fold + ?Sized>(
    folder: &mut F,
    mut query: ExplainQuery,
) -> ExplainQuery {
    query.query = Box::new(folder.fold_query(*query.query));
    query
}

pub fn fold_query<F: Fold + ?Sized>(folder: &mut F, mut query: Query) -> Query {
    query.with = query.with.map(|mut with| {
        with.ctes = with
            .ctes
            .into_iter()
            .map(|mut cte| {
                cte.name = folder.fold_ident(cte.name);
                cte.columns = cte
                    .columns
                    .into_iter()
                    .map(|column| folder.fold_ident(column))
                    .collect();
                cte.query = Box::new(folder.fold_query(*cte.query));
                cte
            })
            .collect();
        with
    });
    query.body = Box::new(fold_set_expr(folder, *query.body));
    query.order_by = query
        .order_by
        .into_iter()
        .map(|mut order| {
            order.expr = folder.fold_expr(order.expr);
            order
        })
        .collect();
    query.limit = query.limit.map(|limit| folder.fold_expr(limit));
    query.offset = query.offset.map(|mut offset| {
        offset.value = folder.fold_expr(offset.value);
        offset
    });
    query.fetch = query.fetch.map(|mut fetch| {
        fetch.quantity = fetch.quantity.map(|quantity| folder.fold_expr(quantity));
        fetch
    });
    query
}

pub fn fold_set_expr<F: Fold + ?Sized>(folder: &mut F, set_expr: SetExpr) -> SetExpr {
    match set_expr {
        SetExpr::Select(select) => SetExpr::Select(Box::new(folder.fold_select(*select))),
        SetExpr::Values(mut values) => {
            values.rows = values
                .rows
                .into_iter()
                .map(|row| row.into_iter().map(|expr| folder.fold_expr(expr)).collect())
                .collect();
            SetExpr::Values(values)
        }
        SetExpr::Query(query) => SetExpr::Query(Box::new(folder.fold_query(*query))),
        SetExpr::SetOperation(mut operation) => {
            operation.left = Box::new(fold_set_expr(folder, *operation.left));
            operation.right = Box::new(fold_set_expr(folder, *operation.right));
            SetExpr::SetOperation(operation)
        }
    }
}

pub fn fold_select<F: Fold + ?Sized>(folder: &mut F, mut select: Select) -> Select {
    select.hints = select
        .hints
        .into_iter()
        .map(|mut hint| {
            hint.name = folder.fold_ident(hint.name);
            hint.value = match hint.value {
                SelectHintValue::Bare => SelectHintValue::Bare,
                SelectHintValue::Call { arguments } => SelectHintValue::Call {
                    arguments: arguments
                        .into_iter()
                        .map(|argument| folder.fold_expr(argument))
                        .collect(),
                },
                SelectHintValue::Assignment { value } => SelectHintValue::Assignment {
                    value: folder.fold_expr(value),
                },
            };
            hint
        })
        .collect();
    select.quantifier = match select.quantifier {
        SelectQuantifier::Distinct { on, span } => SelectQuantifier::Distinct {
            on: on.into_iter().map(|expr| folder.fold_expr(expr)).collect(),
            span,
        },
        quantifier => quantifier,
    };
    select.projection = select
        .projection
        .into_iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => SelectItem::UnnamedExpr(folder.fold_expr(expr)),
            SelectItem::ExprWithAlias {
                expr,
                alias,
                explicit_as,
                span,
            } => SelectItem::ExprWithAlias {
                expr: folder.fold_expr(expr),
                alias: folder.fold_ident(alias),
                explicit_as,
                span,
            },
            SelectItem::Wildcard { mut options, span } => {
                options = fold_wildcard_options(folder, options);
                SelectItem::Wildcard { options, span }
            }
            SelectItem::QualifiedWildcard {
                prefix,
                mut options,
                span,
            } => {
                options = fold_wildcard_options(folder, options);
                SelectItem::QualifiedWildcard {
                    prefix: prefix
                        .into_iter()
                        .map(|part| folder.fold_ident(part))
                        .collect(),
                    options,
                    span,
                }
            }
        })
        .collect();
    select.from = select
        .from
        .into_iter()
        .map(|table| fold_table_with_joins(folder, table))
        .collect();
    select.selection = select.selection.map(|expr| folder.fold_expr(expr));
    select.group_by = fold_group_by(folder, select.group_by);
    select.having = select.having.map(|expr| folder.fold_expr(expr));
    select.qualify = select.qualify.map(|expr| folder.fold_expr(expr));
    select.windows = select
        .windows
        .into_iter()
        .map(|window| fold_named_window(folder, window))
        .collect();
    select
}

fn fold_wildcard_options<F: Fold + ?Sized>(
    folder: &mut F,
    mut options: WildcardOptions,
) -> WildcardOptions {
    options.exclude = options
        .exclude
        .into_iter()
        .map(|ident| folder.fold_ident(ident))
        .collect();
    options.replace = options
        .replace
        .into_iter()
        .map(|mut item| {
            item.expr = folder.fold_expr(item.expr);
            item.alias = folder.fold_ident(item.alias);
            item
        })
        .collect();
    options
}

pub fn fold_group_by<F: Fold + ?Sized>(folder: &mut F, group_by: GroupBy) -> GroupBy {
    let fold_exprs = |expressions: Vec<Expr>, folder: &mut F| {
        expressions
            .into_iter()
            .map(|expr| folder.fold_expr(expr))
            .collect()
    };
    match group_by {
        GroupBy::None => GroupBy::None,
        GroupBy::Expressions { expressions, span } => GroupBy::Expressions {
            expressions: fold_exprs(expressions, folder),
            span,
        },
        GroupBy::Rollup { expressions, span } => GroupBy::Rollup {
            expressions: fold_exprs(expressions, folder),
            span,
        },
        GroupBy::Cube { expressions, span } => GroupBy::Cube {
            expressions: fold_exprs(expressions, folder),
            span,
        },
        GroupBy::GroupingSets { sets, span } => GroupBy::GroupingSets {
            sets: sets
                .into_iter()
                .map(|set| fold_exprs(set, folder))
                .collect(),
            span,
        },
    }
}

pub fn fold_table_with_joins<F: Fold + ?Sized>(
    folder: &mut F,
    mut table: TableWithJoins,
) -> TableWithJoins {
    table.relation = folder.fold_table_factor(table.relation);
    table.joins = table
        .joins
        .into_iter()
        .map(|mut join| {
            join.relation = folder.fold_table_factor(join.relation);
            join.constraint = match join.constraint {
                JoinConstraint::On(expr) => JoinConstraint::On(folder.fold_expr(expr)),
                JoinConstraint::Using { columns, span } => JoinConstraint::Using {
                    columns: columns
                        .into_iter()
                        .map(|column| folder.fold_ident(column))
                        .collect(),
                    span,
                },
                constraint => constraint,
            };
            join
        })
        .collect();
    table
}

pub fn fold_table_factor<F: Fold + ?Sized>(folder: &mut F, factor: TableFactor) -> TableFactor {
    match factor {
        TableFactor::Table {
            name,
            metadata,
            alias,
            version,
            span,
        } => TableFactor::Table {
            name: folder.fold_object_name(name),
            metadata: metadata.map(|metadata| folder.fold_ident(metadata)),
            alias: alias.map(|alias| fold_table_alias(folder, alias)),
            version: version.map(|mut version| {
                version.value = folder.fold_expr(version.value);
                version
            }),
            span,
        },
        TableFactor::Derived {
            lateral,
            subquery,
            alias,
            span,
        } => TableFactor::Derived {
            lateral,
            subquery: Box::new(folder.fold_query(*subquery)),
            alias: alias.map(|alias| fold_table_alias(folder, alias)),
            span,
        },
        TableFactor::TableFunction {
            lateral,
            syntax,
            expr,
            alias,
            span,
        } => TableFactor::TableFunction {
            lateral,
            syntax,
            expr: folder.fold_expr(expr),
            alias: alias.map(|alias| fold_table_alias(folder, alias)),
            span,
        },
        TableFactor::Unnest {
            keyword,
            lateral,
            array_exprs,
            with_offset,
            alias,
            span,
        } => TableFactor::Unnest {
            keyword: folder.fold_ident(keyword),
            lateral,
            array_exprs: array_exprs
                .into_iter()
                .map(|expr| folder.fold_expr(expr))
                .collect(),
            with_offset,
            alias: alias.map(|alias| fold_table_alias(folder, alias)),
            span,
        },
        TableFactor::NestedJoin {
            table_with_joins,
            alias,
            span,
        } => TableFactor::NestedJoin {
            table_with_joins: Box::new(fold_table_with_joins(folder, *table_with_joins)),
            alias: alias.map(|alias| fold_table_alias(folder, alias)),
            span,
        },
    }
}

pub fn fold_table_alias<F: Fold + ?Sized>(folder: &mut F, mut alias: TableAlias) -> TableAlias {
    alias.name = folder.fold_ident(alias.name);
    alias.columns = alias
        .columns
        .into_iter()
        .map(|column| folder.fold_ident(column))
        .collect();
    alias
}

pub fn fold_named_window<F: Fold + ?Sized>(folder: &mut F, mut window: NamedWindow) -> NamedWindow {
    window.name = folder.fold_ident(window.name);
    window.specification = fold_window_spec(folder, window.specification);
    window
}

pub fn fold_window_spec<F: Fold + ?Sized>(folder: &mut F, mut window: WindowSpec) -> WindowSpec {
    window.existing_window_name = window
        .existing_window_name
        .map(|name| folder.fold_ident(name));
    window.partition_by = window
        .partition_by
        .into_iter()
        .map(|expr| folder.fold_expr(expr))
        .collect();
    window.order_by = window
        .order_by
        .into_iter()
        .map(|mut order| {
            order.expr = folder.fold_expr(order.expr);
            order
        })
        .collect();
    window.window_frame = window.window_frame.map(|mut frame| {
        frame.start_bound = fold_window_frame_bound(folder, frame.start_bound);
        frame.end_bound = frame
            .end_bound
            .map(|bound| fold_window_frame_bound(folder, bound));
        frame
    });
    window
}

pub fn fold_window_frame_bound<F: Fold + ?Sized>(
    folder: &mut F,
    bound: WindowFrameBound,
) -> WindowFrameBound {
    match bound {
        WindowFrameBound::Preceding(value, span) => {
            WindowFrameBound::Preceding(value.map(|expr| folder.fold_expr(expr)), span)
        }
        WindowFrameBound::Following(value, span) => {
            WindowFrameBound::Following(value.map(|expr| folder.fold_expr(expr)), span)
        }
        bound => bound,
    }
}

pub fn fold_expr<F: Fold + ?Sized>(folder: &mut F, expression: Expr) -> Expr {
    match expression {
        Expr::Identifier(ident) => Expr::Identifier(folder.fold_ident(ident)),
        Expr::CompoundIdentifier(mut ident) => {
            ident.parts = ident
                .parts
                .into_iter()
                .map(|part| folder.fold_ident(part))
                .collect();
            Expr::CompoundIdentifier(ident)
        }
        Expr::Literal(literal) => Expr::Literal(folder.fold_literal(literal)),
        Expr::FunctionCall(call) => Expr::FunctionCall(folder.fold_function_call(call)),
        Expr::Unary(expression) => Expr::Unary(folder.fold_unary_expr(expression)),
        Expr::Binary(expression) => Expr::Binary(folder.fold_binary_expr(expression)),
        Expr::Nested(expression) => Expr::Nested(folder.fold_nested_expr(expression)),
        Expr::Between(mut expression) => {
            expression.expr = Box::new(folder.fold_expr(*expression.expr));
            expression.low = Box::new(folder.fold_expr(*expression.low));
            expression.high = Box::new(folder.fold_expr(*expression.high));
            Expr::Between(expression)
        }
        Expr::InList(mut expression) => {
            expression.expr = Box::new(folder.fold_expr(*expression.expr));
            expression.list = expression
                .list
                .into_iter()
                .map(|item| folder.fold_expr(item))
                .collect();
            Expr::InList(expression)
        }
        Expr::InSubquery(mut expression) => {
            expression.expr = Box::new(folder.fold_expr(*expression.expr));
            expression.query = Box::new(folder.fold_query(*expression.query));
            Expr::InSubquery(expression)
        }
        Expr::Exists(mut expression) => {
            expression.query = Box::new(folder.fold_query(*expression.query));
            Expr::Exists(expression)
        }
        Expr::Like(mut expression) => {
            expression.expr = Box::new(folder.fold_expr(*expression.expr));
            expression.pattern = Box::new(folder.fold_expr(*expression.pattern));
            expression.escape = expression
                .escape
                .map(|escape| Box::new(folder.fold_expr(*escape)));
            Expr::Like(expression)
        }
        Expr::IsPredicate(mut expression) => {
            expression.expr = Box::new(folder.fold_expr(*expression.expr));
            Expr::IsPredicate(expression)
        }
        Expr::Case(mut expression) => {
            expression.operand = expression
                .operand
                .map(|expr| Box::new(folder.fold_expr(*expr)));
            expression.conditions = expression
                .conditions
                .into_iter()
                .map(|expr| folder.fold_expr(expr))
                .collect();
            expression.results = expression
                .results
                .into_iter()
                .map(|expr| folder.fold_expr(expr))
                .collect();
            expression.else_result = expression
                .else_result
                .map(|expr| Box::new(folder.fold_expr(*expr)));
            Expr::Case(expression)
        }
        Expr::Cast(mut expression) => {
            expression.expr = Box::new(folder.fold_expr(*expression.expr));
            expression.data_type = folder.fold_type_name(expression.data_type);
            expression.format = expression
                .format
                .map(|format| Box::new(folder.fold_expr(*format)));
            Expr::Cast(expression)
        }
        Expr::Interval(mut expression) => {
            expression.value = Box::new(folder.fold_expr(*expression.value));
            expression.leading_precision = expression
                .leading_precision
                .map(|precision| Box::new(folder.fold_expr(*precision)));
            expression.fractional_seconds_precision = expression
                .fractional_seconds_precision
                .map(|precision| Box::new(folder.fold_expr(*precision)));
            Expr::Interval(expression)
        }
        Expr::Subquery(mut expression) => {
            expression.query = Box::new(folder.fold_query(*expression.query));
            Expr::Subquery(expression)
        }
        Expr::Tuple(mut expression) => {
            expression.expressions = expression
                .expressions
                .into_iter()
                .map(|expr| folder.fold_expr(expr))
                .collect();
            Expr::Tuple(expression)
        }
        Expr::Array(mut expression) => {
            expression.element_type = expression
                .element_type
                .map(|element_type| folder.fold_type_name(element_type));
            expression.elements = expression
                .elements
                .into_iter()
                .map(|expr| folder.fold_expr(expr))
                .collect();
            Expr::Array(expression)
        }
        Expr::Map(mut expression) => {
            expression.entries = expression
                .entries
                .into_iter()
                .map(|mut entry| {
                    entry.key = folder.fold_expr(entry.key);
                    entry.value = folder.fold_expr(entry.value);
                    entry
                })
                .collect();
            Expr::Map(expression)
        }
        Expr::Struct(mut expression) => {
            expression.fields = expression
                .fields
                .into_iter()
                .map(|mut field| {
                    field.name = field.name.map(|name| folder.fold_ident(name));
                    field.value = folder.fold_expr(field.value);
                    field
                })
                .collect();
            Expr::Struct(expression)
        }
        Expr::Lambda(mut expression) => {
            expression.parameters = expression
                .parameters
                .into_iter()
                .map(|parameter| folder.fold_ident(parameter))
                .collect();
            expression.body = Box::new(folder.fold_expr(*expression.body));
            Expr::Lambda(expression)
        }
        Expr::Access(mut expression) => {
            expression.expr = Box::new(folder.fold_expr(*expression.expr));
            expression.kind = match expression.kind {
                AccessKind::Field(name) => AccessKind::Field(folder.fold_ident(name)),
                AccessKind::Subscript(index) => {
                    AccessKind::Subscript(Box::new(folder.fold_expr(*index)))
                }
                AccessKind::Json { operator, path } => AccessKind::Json {
                    operator,
                    path: Box::new(folder.fold_expr(*path)),
                },
            };
            Expr::Access(expression)
        }
        Expr::TypedString(mut expression) => {
            expression.data_type = folder.fold_type_name(expression.data_type);
            expression.value = folder.fold_literal(expression.value);
            Expr::TypedString(expression)
        }
    }
}

pub fn fold_function_call<F: Fold + ?Sized>(
    folder: &mut F,
    mut call: FunctionCall,
) -> FunctionCall {
    call.name = folder.fold_object_name(call.name);
    call.arguments = call
        .arguments
        .into_iter()
        .map(|argument| folder.fold_expr(argument))
        .collect();
    call.order_by = call
        .order_by
        .into_iter()
        .map(|mut order| {
            order.expr = folder.fold_expr(order.expr);
            order
        })
        .collect();
    call.separator = call
        .separator
        .map(|separator| Box::new(folder.fold_expr(*separator)));
    call.filter = call
        .filter
        .map(|filter| Box::new(folder.fold_expr(*filter)));
    call.over = call
        .over
        .map(|over| Box::new(fold_window_spec(folder, *over)));
    call
}

pub fn fold_unary_expr<F: Fold + ?Sized>(folder: &mut F, mut expression: UnaryExpr) -> UnaryExpr {
    expression.expression = Box::new(folder.fold_expr(*expression.expression));
    expression
}

pub fn fold_binary_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut expression: BinaryExpr,
) -> BinaryExpr {
    expression.left = Box::new(folder.fold_expr(*expression.left));
    expression.right = Box::new(folder.fold_expr(*expression.right));
    expression
}

pub fn fold_nested_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut expression: NestedExpr,
) -> NestedExpr {
    expression.expression = Box::new(folder.fold_expr(*expression.expression));
    expression
}

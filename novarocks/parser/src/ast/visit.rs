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
    fn visit_at_local(&mut self, node: &AtLocal) {
        super::walk_at_local(self, node);
    }
    fn visit_at_time_zone(&mut self, node: &AtTimeZone) {
        super::walk_at_time_zone(self, node);
    }
    fn visit_grouping_operation(&mut self, node: &GroupingOperation) {
        super::walk_grouping_operation(self, node);
    }
    fn visit_trim(&mut self, node: &Trim) {
        super::walk_trim(self, node);
    }
    fn visit_overlay(&mut self, node: &Overlay) {
        super::walk_overlay(self, node);
    }
    fn visit_interval_value_expression(&mut self, node: &IntervalValueExpression) {
        super::walk_interval_value_expression(self, node);
    }
    fn visit_method_call(&mut self, node: &MethodCall) {
        super::walk_method_call(self, node);
    }
    fn visit_static_method_call(&mut self, node: &StaticMethodCall) {
        super::walk_static_method_call(self, node);
    }
    fn visit_current_catalog(&mut self, node: &CurrentCatalog) {
        super::walk_current_catalog(self, node);
    }
    fn visit_current_date(&mut self, node: &CurrentDate) {
        super::walk_current_date(self, node);
    }
    fn visit_current_path(&mut self, node: &CurrentPath) {
        super::walk_current_path(self, node);
    }
    fn visit_current_schema(&mut self, node: &CurrentSchema) {
        super::walk_current_schema(self, node);
    }
    fn visit_current_user(&mut self, node: &CurrentUser) {
        super::walk_current_user(self, node);
    }
    fn visit_current_time(&mut self, node: &CurrentTime) {
        super::walk_current_time(self, node);
    }
    fn visit_current_timestamp(&mut self, node: &CurrentTimestamp) {
        super::walk_current_timestamp(self, node);
    }
    fn visit_local_time(&mut self, node: &LocalTime) {
        super::walk_local_time(self, node);
    }
    fn visit_local_timestamp(&mut self, node: &LocalTimestamp) {
        super::walk_local_timestamp(self, node);
    }

    fn visit_values(&mut self, node: &Values) {
        walk_values(self, node);
    }
    fn visit_select_item(&mut self, node: &SelectItem) {
        walk_select_item(self, node);
    }
    fn visit_single_column(&mut self, node: &SingleColumn) {
        walk_single_column(self, node);
    }
    fn visit_all_columns(&mut self, node: &AllColumns) {
        walk_all_columns(self, node);
    }

    fn visit_with(&mut self, node: &With) {
        walk_with(self, node);
    }
    fn visit_cte(&mut self, node: &Cte) {
        walk_cte(self, node);
    }
    fn visit_relation(&mut self, node: &Relation) {
        super::relation::walk_relation(self, node);
    }
    fn visit_join_criteria(&mut self, node: &JoinCriteria) {
        super::relation::walk_join_criteria(self, node);
    }
    fn visit_table_function_argument_value(&mut self, node: &TableFunctionArgumentValue) {
        super::relation::walk_table_function_argument_value(self, node);
    }
    fn visit_table(&mut self, node: &Table) {
        super::relation::walk_table(self, node);
    }
    fn visit_table_subquery(&mut self, node: &TableSubquery) {
        super::relation::walk_table_subquery(self, node);
    }
    fn visit_aliased_relation(&mut self, node: &AliasedRelation) {
        super::relation::walk_aliased_relation(self, node);
    }
    fn visit_lateral(&mut self, node: &Lateral) {
        super::relation::walk_lateral(self, node);
    }
    fn visit_unnest(&mut self, node: &Unnest) {
        super::relation::walk_unnest(self, node);
    }
    fn visit_join(&mut self, node: &Join) {
        super::relation::walk_join(self, node);
    }
    fn visit_join_on(&mut self, node: &JoinOn) {
        super::relation::walk_join_on(self, node);
    }
    fn visit_join_using(&mut self, node: &JoinUsing) {
        super::relation::walk_join_using(self, node);
    }
    fn visit_natural_join(&mut self, node: &NaturalJoin) {
        super::relation::walk_natural_join(self, node);
    }
    fn visit_sampled_relation(&mut self, node: &SampledRelation) {
        super::relation::walk_sampled_relation(self, node);
    }
    fn visit_query_period(&mut self, node: &QueryPeriod) {
        super::relation::walk_query_period(self, node);
    }
    fn visit_pivot(&mut self, node: &Pivot) {
        super::relation::walk_pivot(self, node);
    }
    fn visit_pivot_aggregation(&mut self, node: &PivotAggregation) {
        super::relation::walk_pivot_aggregation(self, node);
    }
    fn visit_pivot_value_group(&mut self, node: &PivotValueGroup) {
        super::relation::walk_pivot_value_group(self, node);
    }
    fn visit_nearest(&mut self, node: &Nearest) {
        super::relation::walk_nearest(self, node);
    }
    fn visit_table_function_invocation(&mut self, node: &TableFunctionInvocation) {
        super::relation::walk_table_function_invocation(self, node);
    }
    fn visit_table_function_argument(&mut self, node: &TableFunctionArgument) {
        super::relation::walk_table_function_argument(self, node);
    }
    fn visit_table_function_table_argument(&mut self, node: &TableFunctionTableArgument) {
        super::relation::walk_table_function_table_argument(self, node);
    }
    fn visit_table_function_descriptor_argument(&mut self, node: &TableFunctionDescriptorArgument) {
        super::relation::walk_table_function_descriptor_argument(self, node);
    }
    fn visit_descriptor(&mut self, node: &Descriptor) {
        super::relation::walk_descriptor(self, node);
    }
    fn visit_descriptor_field(&mut self, node: &DescriptorField) {
        super::relation::walk_descriptor_field(self, node);
    }
    fn visit_empty_table_treatment(&mut self, node: &EmptyTableTreatment) {
        super::relation::walk_empty_table_treatment(self, node);
    }
    fn visit_pattern_recognition_relation(&mut self, node: &PatternRecognitionRelation) {
        super::relation::walk_pattern_recognition_relation(self, node);
    }
    fn visit_legacy_table_function(&mut self, node: &super::legacy::TableFunction) {
        super::relation::walk_legacy_table_function(self, node);
    }
    fn visit_group_by(&mut self, node: &GroupBy) {
        walk_group_by(self, node);
    }
    fn visit_grouping_element(&mut self, node: &GroupingElement) {
        walk_grouping_element(self, node);
    }
    fn visit_simple_group_by(&mut self, node: &SimpleGroupBy) {
        walk_simple_group_by(self, node);
    }
    fn visit_grouping_sets(&mut self, node: &GroupingSets) {
        walk_grouping_sets(self, node);
    }
    fn visit_auto_group_by(&mut self, node: &AutoGroupBy) {
        walk_auto_group_by(self, node);
    }

    fn visit_all_rows(&mut self, _node: &AllRows) {}
    fn visit_parameter(&mut self, _node: &Parameter) {}
    fn visit_limit(&mut self, node: &Limit) {
        self.visit_expr(&node.row_count);
    }
    fn visit_offset(&mut self, node: &Offset) {
        self.visit_expr(&node.row_count);
    }
    fn visit_fetch_first(&mut self, node: &FetchFirst) {
        if let Some(count) = &node.row_count {
            self.visit_expr(count);
        }
    }
    fn visit_set_operation(&mut self, node: &SetOperation) {
        walk_set_operation(self, node);
    }

    fn visit_extract_expr(&mut self, node: &ExtractExpr) {
        super::control_expr::walk_extract_expr(self, node);
    }
    fn visit_if_expr(&mut self, node: &IfExpr) {
        super::control_expr::walk_if_expr(self, node);
    }
    fn visit_null_if_expr(&mut self, node: &NullIfExpr) {
        super::control_expr::walk_null_if_expr(self, node);
    }
    fn visit_coalesce_expr(&mut self, node: &CoalesceExpr) {
        super::control_expr::walk_coalesce_expr(self, node);
    }
    fn visit_try_expr(&mut self, node: &TryExpr) {
        super::control_expr::walk_try_expr(self, node);
    }
    fn visit_format_expr(&mut self, node: &FormatExpr) {
        super::control_expr::walk_format_expr(self, node);
    }

    fn visit_call_argument(&mut self, node: &CallArgument) {
        walk_call_argument(self, node);
    }
    fn visit_processing_mode(&mut self, node: &ProcessingMode) {
        walk_processing_mode(self, node);
    }
    fn visit_window(&mut self, node: &Window) {
        walk_window(self, node);
    }
    fn visit_window_reference(&mut self, node: &WindowReference) {
        walk_window_reference(self, node);
    }
    fn visit_window_specification(&mut self, node: &WindowSpecification) {
        walk_window_specification(self, node);
    }
    fn visit_window_definition(&mut self, node: &WindowDefinition) {
        walk_window_definition(self, node);
    }
    fn visit_window_frame(&mut self, node: &WindowFrame) {
        walk_window_frame(self, node);
    }
    fn visit_window_operation(&mut self, node: &WindowOperation) {
        walk_window_operation(self, node);
    }
    fn visit_measure_definition(&mut self, node: &MeasureDefinition) {
        super::row_pattern::walk_measure_definition(self, node);
    }
    fn visit_subset_definition(&mut self, node: &SubsetDefinition) {
        super::row_pattern::walk_subset_definition(self, node);
    }
    fn visit_variable_definition(&mut self, node: &VariableDefinition) {
        super::row_pattern::walk_variable_definition(self, node);
    }
    fn visit_pattern_search_mode(&mut self, node: &PatternSearchMode) {
        super::row_pattern::walk_pattern_search_mode(self, node);
    }
    fn visit_pattern_integer(&mut self, node: &PatternInteger) {
        super::row_pattern::walk_pattern_integer(self, node);
    }
    fn visit_skip_to(&mut self, node: &SkipTo) {
        super::row_pattern::walk_skip_to(self, node);
    }
    fn visit_row_pattern(&mut self, node: &RowPattern) {
        super::row_pattern::walk_row_pattern(self, node);
    }
    fn visit_pattern_quantifier(&mut self, node: &PatternQuantifier) {
        super::row_pattern::walk_pattern_quantifier(self, node);
    }

    fn visit_order_by(&mut self, node: &OrderBy) {
        walk_order_by(self, node);
    }
    fn visit_sort_item(&mut self, node: &SortItem) {
        walk_sort_item(self, node);
    }
    fn visit_subscript_expr(&mut self, node: &SubscriptExpr) {
        walk_subscript_expr(self, node);
    }
    fn visit_array_wildcard_subscript_expr(&mut self, node: &ArrayWildcardSubscriptExpr) {
        walk_array_wildcard_subscript_expr(self, node);
    }
    fn visit_dereference_expr(&mut self, node: &DereferenceExpr) {
        walk_dereference_expr(self, node);
    }
    fn visit_row_expr(&mut self, node: &RowExpr) {
        walk_row_expr(self, node);
    }
    fn visit_row_field(&mut self, node: &RowField) {
        walk_row_field(self, node);
    }
    fn visit_exists_expr(&mut self, node: &ExistsExpr) {
        walk_exists_expr(self, node);
    }
    fn visit_unique_expr(&mut self, node: &UniqueExpr) {
        walk_unique_expr(self, node);
    }
    fn visit_cast_expr(&mut self, node: &CastExpr) {
        walk_cast_expr(self, node);
    }
    fn visit_lambda_expr(&mut self, node: &LambdaExpr) {
        walk_lambda_expr(self, node);
    }
    fn visit_lambda_argument_declaration(&mut self, node: &LambdaArgumentDeclaration) {
        walk_lambda_argument_declaration(self, node);
    }
    fn visit_searched_case_expr(&mut self, node: &SearchedCaseExpr) {
        walk_searched_case_expr(self, node);
    }
    fn visit_simple_case_expr(&mut self, node: &SimpleCaseExpr) {
        walk_simple_case_expr(self, node);
    }
    fn visit_when_clause(&mut self, node: &WhenClause) {
        walk_when_clause(self, node);
    }
    fn visit_when_match(&mut self, node: &WhenMatch) {
        walk_when_match(self, node);
    }

    fn visit_predicated_expr(&mut self, node: &PredicatedExpr) {
        walk_predicated_expr(self, node);
    }
    fn visit_comparison_predicate(&mut self, node: &ComparisonPredicate) {
        walk_comparison_predicate(self, node);
    }
    fn visit_distinct_from_predicate(&mut self, node: &DistinctFromPredicate) {
        walk_distinct_from_predicate(self, node);
    }
    fn visit_between_predicate(&mut self, node: &BetweenPredicate) {
        walk_between_predicate(self, node);
    }
    fn visit_boolean_test_predicate(&mut self, node: &BooleanTestPredicate) {
        walk_boolean_test_predicate(self, node);
    }
    fn visit_is_null_predicate(&mut self, node: &IsNullPredicate) {
        walk_is_null_predicate(self, node);
    }
    fn visit_like_predicate(&mut self, node: &LikePredicate) {
        walk_like_predicate(self, node);
    }
    fn visit_in_predicate(&mut self, node: &InPredicate) {
        walk_in_predicate(self, node);
    }
    fn visit_overlaps_predicate(&mut self, node: &OverlapsPredicate) {
        walk_overlaps_predicate(self, node);
    }
    fn visit_quantified_comparison_predicate(&mut self, node: &QuantifiedComparisonPredicate) {
        walk_quantified_comparison_predicate(self, node);
    }
    fn visit_match_predicate(&mut self, node: &MatchPredicate) {
        walk_match_predicate(self, node);
    }
    fn visit_in_list_expr(&mut self, node: &InListExpr) {
        walk_in_list_expr(self, node);
    }
    fn visit_predicate(&mut self, node: &Predicate) {
        walk_predicate(self, node);
    }

    fn visit_type_name(&mut self, node: &TypeName) {
        walk_type_name(self, node);
    }
    fn visit_generic_data_type(&mut self, node: &GenericDataType) {
        walk_generic_data_type(self, node);
    }
    fn visit_row_data_type(&mut self, node: &RowDataType) {
        walk_row_data_type(self, node);
    }
    fn visit_row_data_type_field(&mut self, node: &RowDataTypeField) {
        walk_row_data_type_field(self, node);
    }
    fn visit_date_time_data_type(&mut self, node: &DateTimeDataType) {
        walk_date_time_data_type(self, node);
    }
    fn visit_interval_data_type(&mut self, node: &IntervalDataType) {
        walk_interval_data_type(self, node);
    }
    fn visit_explicit_array_data_type(&mut self, node: &ExplicitArrayDataType) {
        walk_explicit_array_data_type(self, node);
    }
    fn visit_array_length(&mut self, node: &ArrayLength) {
        walk_array_length(self, node);
    }
    fn visit_data_type_parameter(&mut self, node: &DataTypeParameter) {
        walk_data_type_parameter(self, node);
    }
    fn visit_numeric_parameter(&mut self, node: &NumericParameter) {
        walk_numeric_parameter(self, node);
    }
    fn visit_type_parameter(&mut self, node: &TypeParameter) {
        walk_type_parameter(self, node);
    }
    fn visit_interval_qualifier(&mut self, node: &IntervalQualifier) {
        walk_interval_qualifier(self, node);
    }
    fn visit_simple_interval_qualifier(&mut self, node: &SimpleIntervalQualifier) {
        walk_simple_interval_qualifier(self, node);
    }
    fn visit_composite_interval_qualifier(&mut self, node: &CompositeIntervalQualifier) {
        walk_composite_interval_qualifier(self, node);
    }
    fn visit_interval_qualifier_field(&mut self, node: &IntervalQualifierField) {
        walk_interval_qualifier_field(self, node);
    }
    fn visit_literal(&mut self, node: &Literal) {
        walk_literal(self, node);
    }
    fn visit_json_path_string(&mut self, node: &JsonPathString) {
        super::json::walk_json_path_string(self, node);
    }

    fn visit_json_path_invocation(&mut self, node: &JsonPathInvocation) {
        super::json::walk_json_path_invocation(self, node);
    }

    fn visit_json_path_parameter(&mut self, node: &JsonPathParameter) {
        super::json::walk_json_path_parameter(self, node);
    }

    fn visit_json_constructor(&mut self, node: &JsonConstructor) {
        super::json::walk_json_constructor(self, node);
    }

    fn visit_json_exists(&mut self, node: &JsonExists) {
        super::json::walk_json_exists(self, node);
    }

    fn visit_json_value(&mut self, node: &JsonValue) {
        super::json::walk_json_value(self, node);
    }

    fn visit_json_query(&mut self, node: &JsonQuery) {
        super::json::walk_json_query(self, node);
    }

    fn visit_json_serialize(&mut self, node: &JsonSerialize) {
        super::json::walk_json_serialize(self, node);
    }

    fn visit_json_object(&mut self, node: &JsonObject) {
        super::json::walk_json_object(self, node);
    }

    fn visit_json_object_member(&mut self, node: &JsonObjectMember) {
        super::json::walk_json_object_member(self, node);
    }

    fn visit_json_array(&mut self, node: &JsonArray) {
        super::json::walk_json_array(self, node);
    }

    fn visit_json_array_element(&mut self, node: &JsonArrayElement) {
        super::json::walk_json_array_element(self, node);
    }

    fn visit_json_table(&mut self, node: &JsonTable) {
        super::json::walk_json_table(self, node);
    }

    fn visit_json_table_ordinality_column(&mut self, node: &JsonTableOrdinalityColumn) {
        super::json::walk_json_table_ordinality_column(self, node);
    }

    fn visit_json_table_value_column(&mut self, node: &JsonTableValueColumn) {
        super::json::walk_json_table_value_column(self, node);
    }

    fn visit_json_table_query_column(&mut self, node: &JsonTableQueryColumn) {
        super::json::walk_json_table_query_column(self, node);
    }

    fn visit_json_table_nested_columns(&mut self, node: &JsonTableNestedColumns) {
        super::json::walk_json_table_nested_columns(self, node);
    }

    fn visit_json_table_default_plan(&mut self, node: &JsonTableDefaultPlan) {
        super::json::walk_json_table_default_plan(self, node);
    }

    fn visit_json_table_plan_leaf(&mut self, node: &JsonTablePlanLeaf) {
        super::json::walk_json_table_plan_leaf(self, node);
    }

    fn visit_json_table_plan_parent_child(&mut self, node: &JsonTablePlanParentChild) {
        super::json::walk_json_table_plan_parent_child(self, node);
    }

    fn visit_json_table_plan_siblings(&mut self, node: &JsonTablePlanSiblings) {
        super::json::walk_json_table_plan_siblings(self, node);
    }

    fn visit_json_table_column(&mut self, node: &JsonTableColumn) {
        super::json::walk_json_table_column(self, node);
    }

    fn visit_json_table_plan(&mut self, node: &JsonTablePlan) {
        super::json::walk_json_table_plan(self, node);
    }

    fn visit_json_table_specific_plan(&mut self, node: &JsonTableSpecificPlan) {
        super::json::walk_json_table_specific_plan(self, node);
    }

    fn visit_function_specification(&mut self, node: &FunctionSpecification) {
        super::routine::walk_function_specification(self, node);
    }

    fn visit_function_parameter(&mut self, node: &FunctionParameter) {
        super::routine::walk_function_parameter(self, node);
    }

    fn visit_routine_property(&mut self, node: &RoutineProperty) {
        super::routine::walk_routine_property(self, node);
    }

    fn visit_return_statement(&mut self, node: &ReturnStatement) {
        super::routine::walk_return_statement(self, node);
    }

    fn visit_assignment_statement(&mut self, node: &AssignmentStatement) {
        super::routine::walk_assignment_statement(self, node);
    }

    fn visit_case_statement(&mut self, node: &CaseStatement) {
        super::routine::walk_case_statement(self, node);
    }

    fn visit_case_statement_when_clause(&mut self, node: &CaseStatementWhenClause) {
        super::routine::walk_case_statement_when_clause(self, node);
    }

    fn visit_if_statement(&mut self, node: &IfStatement) {
        super::routine::walk_if_statement(self, node);
    }

    fn visit_else_if_clause(&mut self, node: &ElseIfClause) {
        super::routine::walk_else_if_clause(self, node);
    }

    fn visit_else_clause(&mut self, node: &ElseClause) {
        super::routine::walk_else_clause(self, node);
    }

    fn visit_iterate_statement(&mut self, node: &IterateStatement) {
        super::routine::walk_iterate_statement(self, node);
    }

    fn visit_leave_statement(&mut self, node: &LeaveStatement) {
        super::routine::walk_leave_statement(self, node);
    }

    fn visit_variable_declaration(&mut self, node: &VariableDeclaration) {
        super::routine::walk_variable_declaration(self, node);
    }

    fn visit_compound_statement(&mut self, node: &CompoundStatement) {
        super::routine::walk_compound_statement(self, node);
    }

    fn visit_loop_statement(&mut self, node: &LoopStatement) {
        super::routine::walk_loop_statement(self, node);
    }

    fn visit_while_statement(&mut self, node: &WhileStatement) {
        super::routine::walk_while_statement(self, node);
    }

    fn visit_repeat_statement(&mut self, node: &RepeatStatement) {
        super::routine::walk_repeat_statement(self, node);
    }

    fn visit_routine_body(&mut self, node: &RoutineBody) {
        super::routine::walk_routine_body(self, node);
    }

    fn visit_routine_characteristic(&mut self, node: &RoutineCharacteristic) {
        super::routine::walk_routine_characteristic(self, node);
    }

    fn visit_control_statement(&mut self, node: &ControlStatement) {
        super::routine::walk_control_statement(self, node);
    }

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

    fn visit_select(&mut self, select: &Select) {
        walk_select(self, select);
    }

    fn visit_session_property(&mut self, property: &SessionProperty) {
        self.visit_object_name(&property.name);
        self.visit_expr(&property.value);
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

    fn visit_expr(&mut self, expression: &Expr) {
        walk_expr(self, expression);
    }

    fn visit_function_call(&mut self, call: &FunctionCall) {
        walk_function_call(self, call);
    }

    fn visit_unary_expr(&mut self, expression: &UnaryExpr) {
        walk_unary_expr(self, expression);
    }

    fn visit_logical_expr(&mut self, expression: &LogicalExpr) {
        walk_logical_expr(self, expression);
    }

    fn visit_binary_expr(&mut self, expression: &BinaryExpr) {
        walk_binary_expr(self, expression);
    }

    fn visit_legacy_interval_expr(&mut self, expression: &LegacyIntervalExpr) {
        walk_legacy_interval_expr(self, expression);
    }

    fn visit_legacy_parenthesized_expr(&mut self, expression: &LegacyParenthesizedExpr) {
        walk_legacy_parenthesized_expr(self, expression);
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
    super::legacy::statistics::walk(visitor, statement);
}

pub fn walk_catalog_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &CatalogStatement) {
    super::legacy::catalog::walk(visitor, statement);
}

pub fn walk_iceberg_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &IcebergStatement) {
    super::legacy::iceberg::walk(visitor, statement);
}

pub fn walk_maintenance_statement<V: Visit + ?Sized>(
    visitor: &mut V,
    statement: &MaintenanceStatement,
) {
    super::legacy::maintenance::walk(visitor, statement);
}

pub fn walk_materialized_view_statement<V: Visit + ?Sized>(
    visitor: &mut V,
    statement: &MaterializedViewStatement,
) {
    super::legacy::materialized_view::walk(visitor, statement);
}

pub fn walk_view_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &ViewStatement) {
    super::legacy::view::walk(visitor, statement);
}

pub fn walk_table_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &TableStatement) {
    super::legacy::table::walk(visitor, statement);
}

pub fn walk_dml_statement<V: Visit + ?Sized>(visitor: &mut V, statement: &DmlStatement) {
    super::legacy::dml::walk(visitor, statement);
}

pub fn walk_ident<V: Visit + ?Sized>(_: &mut V, _: &Ident) {}

pub fn walk_object_name<V: Visit + ?Sized>(visitor: &mut V, name: &ObjectName) {
    for part in &name.parts {
        visitor.visit_ident(part);
    }
}

pub fn walk_explain_query<V: Visit + ?Sized>(visitor: &mut V, query: &ExplainQuery) {
    visitor.visit_query(&query.query);
}

#[recursive::recursive]

pub fn walk_query<V: Visit + ?Sized>(visitor: &mut V, query: &Query) {
    for function in &query.functions {
        visitor.visit_function_specification(function);
    }
    for property in &query.session_properties {
        visitor.visit_session_property(property);
    }
    if let Some(with) = &query.with {
        visitor.visit_with(with);
    }
    walk_set_expr(visitor, &query.body);
    walk_query_modifiers(visitor, &query.modifiers);
}

pub fn walk_query_modifiers<V: Visit + ?Sized>(visitor: &mut V, modifiers: &QueryModifiers) {
    if let Some(order) = &modifiers.order_by {
        visitor.visit_order_by(order);
    }
    // Preserve the original LIMIT/offset/FETCH traversal contract.
    if let Some(QueryLimit::Limit(node)) = &modifiers.limit {
        visitor.visit_limit(node);
    }
    if let Some(node) = &modifiers.offset {
        visitor.visit_offset(node);
    }
    if let Some(QueryLimit::FetchFirst(node)) = &modifiers.limit {
        visitor.visit_fetch_first(node);
    }
}

#[recursive::recursive]
pub fn walk_set_expr<V: Visit + ?Sized>(visitor: &mut V, set_expr: &SetExpr) {
    match set_expr {
        SetExpr::Select(select) => visitor.visit_select(select),
        SetExpr::Values(values) => visitor.visit_values(values),
        SetExpr::Table(table) => visitor.visit_table(table),
        SetExpr::Query(query) => visitor.visit_table_subquery(query),
        SetExpr::SetOperation(operation) => visitor.visit_set_operation(operation),
    }
}

pub fn walk_select<V: Visit + ?Sized>(visitor: &mut V, select: &Select) {
    for item in &select.projection {
        visitor.visit_select_item(item);
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
    if let Some(qualify) = &select.legacy.qualify {
        visitor.visit_expr(qualify);
    }
    for window in &select.windows {
        visitor.visit_window_definition(window);
    }
    walk_query_modifiers(visitor, &select.modifiers);
}

#[recursive::recursive]
pub fn walk_group_by<V: Visit + ?Sized>(visitor: &mut V, node: &GroupBy) {
    for element in &node.elements {
        visitor.visit_grouping_element(element);
    }
}
#[recursive::recursive]
pub fn walk_grouping_element<V: Visit + ?Sized>(visitor: &mut V, node: &GroupingElement) {
    match node {
        GroupingElement::Simple(node) => visitor.visit_simple_group_by(node),
        GroupingElement::Sets(node) => visitor.visit_grouping_sets(node),
        GroupingElement::Auto(node) => visitor.visit_auto_group_by(node),
    }
}
#[recursive::recursive]
pub fn walk_simple_group_by<V: Visit + ?Sized>(visitor: &mut V, node: &SimpleGroupBy) {
    for column in &node.columns {
        visitor.visit_expr(column);
    }
}
#[recursive::recursive]
pub fn walk_grouping_sets<V: Visit + ?Sized>(visitor: &mut V, node: &GroupingSets) {
    for set in &node.sets {
        for expression in set {
            visitor.visit_expr(expression);
        }
    }
}
pub fn walk_auto_group_by<V: Visit + ?Sized>(_visitor: &mut V, _node: &AutoGroupBy) {}

#[recursive::recursive]
pub fn walk_window_definition<V: Visit + ?Sized>(visitor: &mut V, window: &WindowDefinition) {
    visitor.visit_ident(&window.name);
    visitor.visit_window_specification(&window.specification);
}

#[recursive::recursive]
pub fn walk_window_specification<V: Visit + ?Sized>(visitor: &mut V, window: &WindowSpecification) {
    if let Some(existing) = &window.existing_window_name {
        visitor.visit_ident(existing);
    }
    for expr in &window.partition_by {
        visitor.visit_expr(expr);
    }
    if let Some(order) = &window.order_by {
        visitor.visit_order_by(order);
    }
    if let Some(frame) = &window.window_frame {
        visitor.visit_window_frame(frame);
    }
}

#[recursive::recursive]
pub fn walk_window_frame_bound<V: Visit + ?Sized>(visitor: &mut V, bound: &WindowFrameBound) {
    match bound {
        WindowFrameBound::Preceding(Some(expr), _) | WindowFrameBound::Following(Some(expr), _) => {
            visitor.visit_expr(expr)
        }
        _ => {}
    }
}

#[recursive::recursive]
fn walk_array_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &ArrayExpr) {
    for value in &expression.values {
        visitor.visit_expr(value);
    }
}

#[recursive::recursive]
pub fn walk_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &Expr) {
    match expression {
        Expr::AtLocal(node) => visitor.visit_at_local(node),
        Expr::AtTimeZone(node) => visitor.visit_at_time_zone(node),
        Expr::GroupingOperation(node) => visitor.visit_grouping_operation(node),
        Expr::Trim(node) => visitor.visit_trim(node),
        Expr::Overlay(node) => visitor.visit_overlay(node),
        Expr::IntervalValueExpression(node) => visitor.visit_interval_value_expression(node),
        Expr::MethodCall(node) => visitor.visit_method_call(node),
        Expr::StaticMethodCall(node) => visitor.visit_static_method_call(node),
        Expr::CurrentCatalog(node) => visitor.visit_current_catalog(node),
        Expr::CurrentDate(node) => visitor.visit_current_date(node),
        Expr::CurrentPath(node) => visitor.visit_current_path(node),
        Expr::CurrentSchema(node) => visitor.visit_current_schema(node),
        Expr::CurrentUser(node) => visitor.visit_current_user(node),
        Expr::CurrentTime(node) => visitor.visit_current_time(node),
        Expr::CurrentTimestamp(node) => visitor.visit_current_timestamp(node),
        Expr::LocalTime(node) => visitor.visit_local_time(node),
        Expr::LocalTimestamp(node) => visitor.visit_local_timestamp(node),

        Expr::JsonConstructor(node) => visitor.visit_json_constructor(node),
        Expr::JsonExists(node) => visitor.visit_json_exists(node),
        Expr::JsonValue(node) => visitor.visit_json_value(node),
        Expr::JsonQuery(node) => visitor.visit_json_query(node),
        Expr::JsonSerialize(node) => visitor.visit_json_serialize(node),
        Expr::JsonObject(node) => visitor.visit_json_object(node),
        Expr::JsonArray(node) => visitor.visit_json_array(node),

        Expr::Identifier(ident) => visitor.visit_ident(ident),
        Expr::AllRows(node) => visitor.visit_all_rows(node),
        Expr::Parameter(node) => visitor.visit_parameter(node),
        Expr::Literal(literal) => visitor.visit_literal(literal),
        Expr::FunctionCall(call) => visitor.visit_function_call(call),
        Expr::Extract(node) => visitor.visit_extract_expr(node),
        Expr::If(node) => visitor.visit_if_expr(node),
        Expr::NullIf(node) => visitor.visit_null_if_expr(node),
        Expr::Coalesce(node) => visitor.visit_coalesce_expr(node),
        Expr::Try(node) => visitor.visit_try_expr(node),
        Expr::Format(node) => visitor.visit_format_expr(node),

        Expr::WindowOperation(node) => visitor.visit_window_operation(node),
        Expr::Unary(expression) => visitor.visit_unary_expr(expression),
        Expr::Binary(expression) => visitor.visit_binary_expr(expression),
        Expr::Logical(expression) => visitor.visit_logical_expr(expression),
        Expr::Exists(node) => visitor.visit_exists_expr(node),
        Expr::Unique(node) => visitor.visit_unique_expr(node),
        Expr::Cast(node) => visitor.visit_cast_expr(node),
        Expr::Lambda(node) => visitor.visit_lambda_expr(node),
        Expr::SearchedCase(node) => visitor.visit_searched_case_expr(node),
        Expr::SimpleCase(node) => visitor.visit_simple_case_expr(node),
        Expr::Predicated(node) => visitor.visit_predicated_expr(node),
        Expr::InList(node) => visitor.visit_in_list_expr(node),
        Expr::LegacyParenthesized(expression) => {
            visitor.visit_legacy_parenthesized_expr(expression)
        }

        Expr::LegacyInterval(expression) => visitor.visit_legacy_interval_expr(expression),
        Expr::Subquery(expression) => visitor.visit_query(&expression.query),
        Expr::Row(expression) => visitor.visit_row_expr(expression),
        Expr::Array(expression) => walk_array_expr(visitor, expression),

        Expr::Subscript(node) => visitor.visit_subscript_expr(node),
        Expr::ArrayWildcardSubscript(node) => visitor.visit_array_wildcard_subscript_expr(node),
        Expr::Dereference(node) => visitor.visit_dereference_expr(node),
    }
}

#[recursive::recursive]
pub fn walk_function_call<V: Visit + ?Sized>(visitor: &mut V, call: &FunctionCall) {
    visitor.visit_object_name(&call.name);
    if let Some(mode) = &call.processing_mode {
        visitor.visit_processing_mode(mode);
    }
    for argument in &call.arguments {
        visitor.visit_call_argument(argument);
    }
    if let Some(order) = &call.order_by {
        visitor.visit_order_by(order);
    }
    if let Some(filter) = &call.filter {
        visitor.visit_expr(filter);
    }
    if let Some(over) = &call.window {
        visitor.visit_window(over);
    }
}

pub fn walk_unary_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &UnaryExpr) {
    visitor.visit_expr(&expression.expression);
}

pub fn walk_binary_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &BinaryExpr) {
    visitor.visit_expr(&expression.left);
    visitor.visit_expr(&expression.right);
}

#[recursive::recursive]
pub fn walk_legacy_interval_expr<V: Visit + ?Sized>(
    visitor: &mut V,
    expression: &LegacyIntervalExpr,
) {
    visitor.visit_expr(&expression.value);
    if let Some(precision) = &expression.leading_precision {
        visitor.visit_expr(precision);
    }
    if let Some(precision) = &expression.fractional_seconds_precision {
        visitor.visit_expr(precision);
    }
}

pub fn walk_legacy_parenthesized_expr<V: Visit + ?Sized>(
    visitor: &mut V,
    expression: &LegacyParenthesizedExpr,
) {
    visitor.visit_expr(&expression.expression);
}

/// Rebuilds AST nodes by value, recursively folding all children by default.
pub trait Fold {
    fn fold_at_local(&mut self, node: AtLocal) -> AtLocal {
        super::fold_at_local(self, node)
    }
    fn fold_at_time_zone(&mut self, node: AtTimeZone) -> AtTimeZone {
        super::fold_at_time_zone(self, node)
    }
    fn fold_grouping_operation(&mut self, node: GroupingOperation) -> GroupingOperation {
        super::fold_grouping_operation(self, node)
    }
    fn fold_trim(&mut self, node: Trim) -> Trim {
        super::fold_trim(self, node)
    }
    fn fold_overlay(&mut self, node: Overlay) -> Overlay {
        super::fold_overlay(self, node)
    }
    fn fold_interval_value_expression(
        &mut self,
        node: IntervalValueExpression,
    ) -> IntervalValueExpression {
        super::fold_interval_value_expression(self, node)
    }
    fn fold_method_call(&mut self, node: MethodCall) -> MethodCall {
        super::fold_method_call(self, node)
    }
    fn fold_static_method_call(&mut self, node: StaticMethodCall) -> StaticMethodCall {
        super::fold_static_method_call(self, node)
    }
    fn fold_current_catalog(&mut self, node: CurrentCatalog) -> CurrentCatalog {
        super::fold_current_catalog(self, node)
    }
    fn fold_current_date(&mut self, node: CurrentDate) -> CurrentDate {
        super::fold_current_date(self, node)
    }
    fn fold_current_path(&mut self, node: CurrentPath) -> CurrentPath {
        super::fold_current_path(self, node)
    }
    fn fold_current_schema(&mut self, node: CurrentSchema) -> CurrentSchema {
        super::fold_current_schema(self, node)
    }
    fn fold_current_user(&mut self, node: CurrentUser) -> CurrentUser {
        super::fold_current_user(self, node)
    }
    fn fold_current_time(&mut self, node: CurrentTime) -> CurrentTime {
        super::fold_current_time(self, node)
    }
    fn fold_current_timestamp(&mut self, node: CurrentTimestamp) -> CurrentTimestamp {
        super::fold_current_timestamp(self, node)
    }
    fn fold_local_time(&mut self, node: LocalTime) -> LocalTime {
        super::fold_local_time(self, node)
    }
    fn fold_local_timestamp(&mut self, node: LocalTimestamp) -> LocalTimestamp {
        super::fold_local_timestamp(self, node)
    }

    fn fold_values(&mut self, node: Values) -> Values {
        fold_values(self, node)
    }
    fn fold_select_item(&mut self, node: SelectItem) -> SelectItem {
        fold_select_item(self, node)
    }
    fn fold_single_column(&mut self, node: SingleColumn) -> SingleColumn {
        fold_single_column(self, node)
    }
    fn fold_all_columns(&mut self, node: AllColumns) -> AllColumns {
        fold_all_columns(self, node)
    }

    fn fold_with(&mut self, node: With) -> With {
        fold_with(self, node)
    }
    fn fold_cte(&mut self, node: Cte) -> Cte {
        fold_cte(self, node)
    }
    fn fold_relation(&mut self, node: Relation) -> Relation {
        super::relation::fold_relation(self, node)
    }
    fn fold_join_criteria(&mut self, node: JoinCriteria) -> JoinCriteria {
        super::relation::fold_join_criteria(self, node)
    }
    fn fold_table_function_argument_value(
        &mut self,
        node: TableFunctionArgumentValue,
    ) -> TableFunctionArgumentValue {
        super::relation::fold_table_function_argument_value(self, node)
    }
    fn fold_table(&mut self, node: Table) -> Table {
        super::relation::fold_table(self, node)
    }
    fn fold_table_subquery(&mut self, node: TableSubquery) -> TableSubquery {
        super::relation::fold_table_subquery(self, node)
    }
    fn fold_aliased_relation(&mut self, node: AliasedRelation) -> AliasedRelation {
        super::relation::fold_aliased_relation(self, node)
    }
    fn fold_lateral(&mut self, node: Lateral) -> Lateral {
        super::relation::fold_lateral(self, node)
    }
    fn fold_unnest(&mut self, node: Unnest) -> Unnest {
        super::relation::fold_unnest(self, node)
    }
    fn fold_join(&mut self, node: Join) -> Join {
        super::relation::fold_join(self, node)
    }
    fn fold_join_on(&mut self, node: JoinOn) -> JoinOn {
        super::relation::fold_join_on(self, node)
    }
    fn fold_join_using(&mut self, node: JoinUsing) -> JoinUsing {
        super::relation::fold_join_using(self, node)
    }
    fn fold_natural_join(&mut self, node: NaturalJoin) -> NaturalJoin {
        super::relation::fold_natural_join(self, node)
    }
    fn fold_sampled_relation(&mut self, node: SampledRelation) -> SampledRelation {
        super::relation::fold_sampled_relation(self, node)
    }
    fn fold_query_period(&mut self, node: QueryPeriod) -> QueryPeriod {
        super::relation::fold_query_period(self, node)
    }
    fn fold_pivot(&mut self, node: Pivot) -> Pivot {
        super::relation::fold_pivot(self, node)
    }
    fn fold_pivot_aggregation(&mut self, node: PivotAggregation) -> PivotAggregation {
        super::relation::fold_pivot_aggregation(self, node)
    }
    fn fold_pivot_value_group(&mut self, node: PivotValueGroup) -> PivotValueGroup {
        super::relation::fold_pivot_value_group(self, node)
    }
    fn fold_nearest(&mut self, node: Nearest) -> Nearest {
        super::relation::fold_nearest(self, node)
    }
    fn fold_table_function_invocation(
        &mut self,
        node: TableFunctionInvocation,
    ) -> TableFunctionInvocation {
        super::relation::fold_table_function_invocation(self, node)
    }
    fn fold_table_function_argument(
        &mut self,
        node: TableFunctionArgument,
    ) -> TableFunctionArgument {
        super::relation::fold_table_function_argument(self, node)
    }
    fn fold_table_function_table_argument(
        &mut self,
        node: TableFunctionTableArgument,
    ) -> TableFunctionTableArgument {
        super::relation::fold_table_function_table_argument(self, node)
    }
    fn fold_table_function_descriptor_argument(
        &mut self,
        node: TableFunctionDescriptorArgument,
    ) -> TableFunctionDescriptorArgument {
        super::relation::fold_table_function_descriptor_argument(self, node)
    }
    fn fold_descriptor(&mut self, node: Descriptor) -> Descriptor {
        super::relation::fold_descriptor(self, node)
    }
    fn fold_descriptor_field(&mut self, node: DescriptorField) -> DescriptorField {
        super::relation::fold_descriptor_field(self, node)
    }
    fn fold_empty_table_treatment(&mut self, node: EmptyTableTreatment) -> EmptyTableTreatment {
        super::relation::fold_empty_table_treatment(self, node)
    }
    fn fold_pattern_recognition_relation(
        &mut self,
        node: PatternRecognitionRelation,
    ) -> PatternRecognitionRelation {
        super::relation::fold_pattern_recognition_relation(self, node)
    }
    fn fold_legacy_table_function(
        &mut self,
        node: super::legacy::TableFunction,
    ) -> super::legacy::TableFunction {
        super::relation::fold_legacy_table_function(self, node)
    }
    fn fold_group_by(&mut self, node: GroupBy) -> GroupBy {
        fold_group_by(self, node)
    }
    fn fold_grouping_element(&mut self, node: GroupingElement) -> GroupingElement {
        fold_grouping_element(self, node)
    }
    fn fold_simple_group_by(&mut self, node: SimpleGroupBy) -> SimpleGroupBy {
        fold_simple_group_by(self, node)
    }
    fn fold_grouping_sets(&mut self, node: GroupingSets) -> GroupingSets {
        fold_grouping_sets(self, node)
    }
    fn fold_auto_group_by(&mut self, node: AutoGroupBy) -> AutoGroupBy {
        fold_auto_group_by(self, node)
    }

    fn fold_all_rows(&mut self, node: AllRows) -> AllRows {
        node
    }
    fn fold_parameter(&mut self, node: Parameter) -> Parameter {
        node
    }
    fn fold_limit(&mut self, mut node: Limit) -> Limit {
        node.row_count = self.fold_expr(node.row_count);
        node
    }
    fn fold_offset(&mut self, mut node: Offset) -> Offset {
        node.row_count = self.fold_expr(node.row_count);
        node
    }
    fn fold_fetch_first(&mut self, mut node: FetchFirst) -> FetchFirst {
        node.row_count = node.row_count.map(|count| self.fold_expr(count));
        node
    }
    fn fold_set_operation(&mut self, node: SetOperation) -> SetOperation {
        fold_set_operation(self, node)
    }

    fn fold_extract_expr(&mut self, node: ExtractExpr) -> ExtractExpr {
        super::control_expr::fold_extract_expr(self, node)
    }
    fn fold_if_expr(&mut self, node: IfExpr) -> IfExpr {
        super::control_expr::fold_if_expr(self, node)
    }
    fn fold_null_if_expr(&mut self, node: NullIfExpr) -> NullIfExpr {
        super::control_expr::fold_null_if_expr(self, node)
    }
    fn fold_coalesce_expr(&mut self, node: CoalesceExpr) -> CoalesceExpr {
        super::control_expr::fold_coalesce_expr(self, node)
    }
    fn fold_try_expr(&mut self, node: TryExpr) -> TryExpr {
        super::control_expr::fold_try_expr(self, node)
    }
    fn fold_format_expr(&mut self, node: FormatExpr) -> FormatExpr {
        super::control_expr::fold_format_expr(self, node)
    }

    fn fold_call_argument(&mut self, node: CallArgument) -> CallArgument {
        fold_call_argument(self, node)
    }
    fn fold_processing_mode(&mut self, node: ProcessingMode) -> ProcessingMode {
        fold_processing_mode(self, node)
    }
    fn fold_window(&mut self, node: Window) -> Window {
        fold_window(self, node)
    }
    fn fold_window_reference(&mut self, node: WindowReference) -> WindowReference {
        fold_window_reference(self, node)
    }
    fn fold_window_specification(&mut self, node: WindowSpecification) -> WindowSpecification {
        fold_window_specification(self, node)
    }
    fn fold_window_definition(&mut self, node: WindowDefinition) -> WindowDefinition {
        fold_window_definition(self, node)
    }
    fn fold_window_frame(&mut self, node: WindowFrame) -> WindowFrame {
        fold_window_frame(self, node)
    }
    fn fold_window_operation(&mut self, node: WindowOperation) -> WindowOperation {
        fold_window_operation(self, node)
    }
    fn fold_measure_definition(&mut self, node: MeasureDefinition) -> MeasureDefinition {
        super::row_pattern::fold_measure_definition(self, node)
    }
    fn fold_subset_definition(&mut self, node: SubsetDefinition) -> SubsetDefinition {
        super::row_pattern::fold_subset_definition(self, node)
    }
    fn fold_variable_definition(&mut self, node: VariableDefinition) -> VariableDefinition {
        super::row_pattern::fold_variable_definition(self, node)
    }
    fn fold_pattern_search_mode(&mut self, node: PatternSearchMode) -> PatternSearchMode {
        super::row_pattern::fold_pattern_search_mode(self, node)
    }
    fn fold_pattern_integer(&mut self, node: PatternInteger) -> PatternInteger {
        super::row_pattern::fold_pattern_integer(self, node)
    }
    fn fold_skip_to(&mut self, node: SkipTo) -> SkipTo {
        super::row_pattern::fold_skip_to(self, node)
    }
    fn fold_row_pattern(&mut self, node: RowPattern) -> RowPattern {
        super::row_pattern::fold_row_pattern(self, node)
    }
    fn fold_pattern_quantifier(&mut self, node: PatternQuantifier) -> PatternQuantifier {
        super::row_pattern::fold_pattern_quantifier(self, node)
    }

    fn fold_order_by(&mut self, node: OrderBy) -> OrderBy {
        fold_order_by(self, node)
    }
    fn fold_sort_item(&mut self, node: SortItem) -> SortItem {
        fold_sort_item(self, node)
    }
    fn fold_subscript_expr(&mut self, node: SubscriptExpr) -> SubscriptExpr {
        fold_subscript_expr(self, node)
    }
    fn fold_array_wildcard_subscript_expr(
        &mut self,
        node: ArrayWildcardSubscriptExpr,
    ) -> ArrayWildcardSubscriptExpr {
        fold_array_wildcard_subscript_expr(self, node)
    }
    fn fold_dereference_expr(&mut self, node: DereferenceExpr) -> DereferenceExpr {
        fold_dereference_expr(self, node)
    }
    fn fold_row_expr(&mut self, node: RowExpr) -> RowExpr {
        fold_row_expr(self, node)
    }
    fn fold_row_field(&mut self, node: RowField) -> RowField {
        fold_row_field(self, node)
    }
    fn fold_exists_expr(&mut self, node: ExistsExpr) -> ExistsExpr {
        fold_exists_expr(self, node)
    }
    fn fold_unique_expr(&mut self, node: UniqueExpr) -> UniqueExpr {
        fold_unique_expr(self, node)
    }
    fn fold_cast_expr(&mut self, node: CastExpr) -> CastExpr {
        fold_cast_expr(self, node)
    }
    fn fold_lambda_expr(&mut self, node: LambdaExpr) -> LambdaExpr {
        fold_lambda_expr(self, node)
    }
    fn fold_lambda_argument_declaration(
        &mut self,
        node: LambdaArgumentDeclaration,
    ) -> LambdaArgumentDeclaration {
        fold_lambda_argument_declaration(self, node)
    }
    fn fold_searched_case_expr(&mut self, node: SearchedCaseExpr) -> SearchedCaseExpr {
        fold_searched_case_expr(self, node)
    }
    fn fold_simple_case_expr(&mut self, node: SimpleCaseExpr) -> SimpleCaseExpr {
        fold_simple_case_expr(self, node)
    }
    fn fold_when_clause(&mut self, node: WhenClause) -> WhenClause {
        fold_when_clause(self, node)
    }
    fn fold_when_match(&mut self, node: WhenMatch) -> WhenMatch {
        fold_when_match(self, node)
    }

    fn fold_predicated_expr(&mut self, node: PredicatedExpr) -> PredicatedExpr {
        fold_predicated_expr(self, node)
    }
    fn fold_comparison_predicate(&mut self, node: ComparisonPredicate) -> ComparisonPredicate {
        fold_comparison_predicate(self, node)
    }
    fn fold_distinct_from_predicate(
        &mut self,
        node: DistinctFromPredicate,
    ) -> DistinctFromPredicate {
        fold_distinct_from_predicate(self, node)
    }
    fn fold_between_predicate(&mut self, node: BetweenPredicate) -> BetweenPredicate {
        fold_between_predicate(self, node)
    }
    fn fold_boolean_test_predicate(&mut self, node: BooleanTestPredicate) -> BooleanTestPredicate {
        fold_boolean_test_predicate(self, node)
    }
    fn fold_is_null_predicate(&mut self, node: IsNullPredicate) -> IsNullPredicate {
        fold_is_null_predicate(self, node)
    }
    fn fold_like_predicate(&mut self, node: LikePredicate) -> LikePredicate {
        fold_like_predicate(self, node)
    }
    fn fold_in_predicate(&mut self, node: InPredicate) -> InPredicate {
        fold_in_predicate(self, node)
    }
    fn fold_overlaps_predicate(&mut self, node: OverlapsPredicate) -> OverlapsPredicate {
        fold_overlaps_predicate(self, node)
    }
    fn fold_quantified_comparison_predicate(
        &mut self,
        node: QuantifiedComparisonPredicate,
    ) -> QuantifiedComparisonPredicate {
        fold_quantified_comparison_predicate(self, node)
    }
    fn fold_match_predicate(&mut self, node: MatchPredicate) -> MatchPredicate {
        fold_match_predicate(self, node)
    }
    fn fold_in_list_expr(&mut self, node: InListExpr) -> InListExpr {
        fold_in_list_expr(self, node)
    }
    fn fold_predicate(&mut self, node: Predicate) -> Predicate {
        fold_predicate(self, node)
    }

    fn fold_type_name(&mut self, node: TypeName) -> TypeName {
        fold_type_name(self, node)
    }
    fn fold_generic_data_type(&mut self, node: GenericDataType) -> GenericDataType {
        fold_generic_data_type(self, node)
    }
    fn fold_row_data_type(&mut self, node: RowDataType) -> RowDataType {
        fold_row_data_type(self, node)
    }
    fn fold_row_data_type_field(&mut self, node: RowDataTypeField) -> RowDataTypeField {
        fold_row_data_type_field(self, node)
    }
    fn fold_date_time_data_type(&mut self, node: DateTimeDataType) -> DateTimeDataType {
        fold_date_time_data_type(self, node)
    }
    fn fold_interval_data_type(&mut self, node: IntervalDataType) -> IntervalDataType {
        fold_interval_data_type(self, node)
    }
    fn fold_explicit_array_data_type(
        &mut self,
        node: ExplicitArrayDataType,
    ) -> ExplicitArrayDataType {
        fold_explicit_array_data_type(self, node)
    }
    fn fold_array_length(&mut self, node: ArrayLength) -> ArrayLength {
        fold_array_length(self, node)
    }
    fn fold_data_type_parameter(&mut self, node: DataTypeParameter) -> DataTypeParameter {
        fold_data_type_parameter(self, node)
    }
    fn fold_numeric_parameter(&mut self, node: NumericParameter) -> NumericParameter {
        fold_numeric_parameter(self, node)
    }
    fn fold_type_parameter(&mut self, node: TypeParameter) -> TypeParameter {
        fold_type_parameter(self, node)
    }
    fn fold_interval_qualifier(&mut self, node: IntervalQualifier) -> IntervalQualifier {
        fold_interval_qualifier(self, node)
    }
    fn fold_simple_interval_qualifier(
        &mut self,
        node: SimpleIntervalQualifier,
    ) -> SimpleIntervalQualifier {
        fold_simple_interval_qualifier(self, node)
    }
    fn fold_composite_interval_qualifier(
        &mut self,
        node: CompositeIntervalQualifier,
    ) -> CompositeIntervalQualifier {
        fold_composite_interval_qualifier(self, node)
    }
    fn fold_interval_qualifier_field(
        &mut self,
        node: IntervalQualifierField,
    ) -> IntervalQualifierField {
        fold_interval_qualifier_field(self, node)
    }
    fn fold_literal(&mut self, node: Literal) -> Literal {
        fold_literal(self, node)
    }
    fn fold_json_path_string(&mut self, node: JsonPathString) -> JsonPathString {
        super::json::fold_json_path_string(self, node)
    }

    fn fold_json_path_invocation(&mut self, node: JsonPathInvocation) -> JsonPathInvocation {
        super::json::fold_json_path_invocation(self, node)
    }

    fn fold_json_path_parameter(&mut self, node: JsonPathParameter) -> JsonPathParameter {
        super::json::fold_json_path_parameter(self, node)
    }

    fn fold_json_constructor(&mut self, node: JsonConstructor) -> JsonConstructor {
        super::json::fold_json_constructor(self, node)
    }

    fn fold_json_exists(&mut self, node: JsonExists) -> JsonExists {
        super::json::fold_json_exists(self, node)
    }

    fn fold_json_value(&mut self, node: JsonValue) -> JsonValue {
        super::json::fold_json_value(self, node)
    }

    fn fold_json_query(&mut self, node: JsonQuery) -> JsonQuery {
        super::json::fold_json_query(self, node)
    }

    fn fold_json_serialize(&mut self, node: JsonSerialize) -> JsonSerialize {
        super::json::fold_json_serialize(self, node)
    }

    fn fold_json_object(&mut self, node: JsonObject) -> JsonObject {
        super::json::fold_json_object(self, node)
    }

    fn fold_json_object_member(&mut self, node: JsonObjectMember) -> JsonObjectMember {
        super::json::fold_json_object_member(self, node)
    }

    fn fold_json_array(&mut self, node: JsonArray) -> JsonArray {
        super::json::fold_json_array(self, node)
    }

    fn fold_json_array_element(&mut self, node: JsonArrayElement) -> JsonArrayElement {
        super::json::fold_json_array_element(self, node)
    }

    fn fold_json_table(&mut self, node: JsonTable) -> JsonTable {
        super::json::fold_json_table(self, node)
    }

    fn fold_json_table_ordinality_column(
        &mut self,
        node: JsonTableOrdinalityColumn,
    ) -> JsonTableOrdinalityColumn {
        super::json::fold_json_table_ordinality_column(self, node)
    }

    fn fold_json_table_value_column(&mut self, node: JsonTableValueColumn) -> JsonTableValueColumn {
        super::json::fold_json_table_value_column(self, node)
    }

    fn fold_json_table_query_column(&mut self, node: JsonTableQueryColumn) -> JsonTableQueryColumn {
        super::json::fold_json_table_query_column(self, node)
    }

    fn fold_json_table_nested_columns(
        &mut self,
        node: JsonTableNestedColumns,
    ) -> JsonTableNestedColumns {
        super::json::fold_json_table_nested_columns(self, node)
    }

    fn fold_json_table_default_plan(&mut self, node: JsonTableDefaultPlan) -> JsonTableDefaultPlan {
        super::json::fold_json_table_default_plan(self, node)
    }

    fn fold_json_table_plan_leaf(&mut self, node: JsonTablePlanLeaf) -> JsonTablePlanLeaf {
        super::json::fold_json_table_plan_leaf(self, node)
    }

    fn fold_json_table_plan_parent_child(
        &mut self,
        node: JsonTablePlanParentChild,
    ) -> JsonTablePlanParentChild {
        super::json::fold_json_table_plan_parent_child(self, node)
    }

    fn fold_json_table_plan_siblings(
        &mut self,
        node: JsonTablePlanSiblings,
    ) -> JsonTablePlanSiblings {
        super::json::fold_json_table_plan_siblings(self, node)
    }

    fn fold_json_table_column(&mut self, node: JsonTableColumn) -> JsonTableColumn {
        super::json::fold_json_table_column(self, node)
    }

    fn fold_json_table_plan(&mut self, node: JsonTablePlan) -> JsonTablePlan {
        super::json::fold_json_table_plan(self, node)
    }

    fn fold_json_table_specific_plan(
        &mut self,
        node: JsonTableSpecificPlan,
    ) -> JsonTableSpecificPlan {
        super::json::fold_json_table_specific_plan(self, node)
    }

    fn fold_function_specification(
        &mut self,
        node: FunctionSpecification,
    ) -> FunctionSpecification {
        super::routine::fold_function_specification(self, node)
    }

    fn fold_function_parameter(&mut self, node: FunctionParameter) -> FunctionParameter {
        super::routine::fold_function_parameter(self, node)
    }

    fn fold_routine_property(&mut self, node: RoutineProperty) -> RoutineProperty {
        super::routine::fold_routine_property(self, node)
    }

    fn fold_return_statement(&mut self, node: ReturnStatement) -> ReturnStatement {
        super::routine::fold_return_statement(self, node)
    }

    fn fold_assignment_statement(&mut self, node: AssignmentStatement) -> AssignmentStatement {
        super::routine::fold_assignment_statement(self, node)
    }

    fn fold_case_statement(&mut self, node: CaseStatement) -> CaseStatement {
        super::routine::fold_case_statement(self, node)
    }

    fn fold_case_statement_when_clause(
        &mut self,
        node: CaseStatementWhenClause,
    ) -> CaseStatementWhenClause {
        super::routine::fold_case_statement_when_clause(self, node)
    }

    fn fold_if_statement(&mut self, node: IfStatement) -> IfStatement {
        super::routine::fold_if_statement(self, node)
    }

    fn fold_else_if_clause(&mut self, node: ElseIfClause) -> ElseIfClause {
        super::routine::fold_else_if_clause(self, node)
    }

    fn fold_else_clause(&mut self, node: ElseClause) -> ElseClause {
        super::routine::fold_else_clause(self, node)
    }

    fn fold_iterate_statement(&mut self, node: IterateStatement) -> IterateStatement {
        super::routine::fold_iterate_statement(self, node)
    }

    fn fold_leave_statement(&mut self, node: LeaveStatement) -> LeaveStatement {
        super::routine::fold_leave_statement(self, node)
    }

    fn fold_variable_declaration(&mut self, node: VariableDeclaration) -> VariableDeclaration {
        super::routine::fold_variable_declaration(self, node)
    }

    fn fold_compound_statement(&mut self, node: CompoundStatement) -> CompoundStatement {
        super::routine::fold_compound_statement(self, node)
    }

    fn fold_loop_statement(&mut self, node: LoopStatement) -> LoopStatement {
        super::routine::fold_loop_statement(self, node)
    }

    fn fold_while_statement(&mut self, node: WhileStatement) -> WhileStatement {
        super::routine::fold_while_statement(self, node)
    }

    fn fold_repeat_statement(&mut self, node: RepeatStatement) -> RepeatStatement {
        super::routine::fold_repeat_statement(self, node)
    }

    fn fold_routine_body(&mut self, node: RoutineBody) -> RoutineBody {
        super::routine::fold_routine_body(self, node)
    }

    fn fold_routine_characteristic(
        &mut self,
        node: RoutineCharacteristic,
    ) -> RoutineCharacteristic {
        super::routine::fold_routine_characteristic(self, node)
    }

    fn fold_control_statement(&mut self, node: ControlStatement) -> ControlStatement {
        super::routine::fold_control_statement(self, node)
    }

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

    #[recursive::recursive]

    fn fold_query(&mut self, query: Query) -> Query {
        fold_query(self, query)
    }

    fn fold_session_property(&mut self, mut property: SessionProperty) -> SessionProperty {
        property.name = self.fold_object_name(property.name);
        property.value = self.fold_expr(property.value);
        property
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

    fn fold_expr(&mut self, expression: Expr) -> Expr {
        fold_expr(self, expression)
    }

    fn fold_function_call(&mut self, call: FunctionCall) -> FunctionCall {
        fold_function_call(self, call)
    }

    fn fold_unary_expr(&mut self, expression: UnaryExpr) -> UnaryExpr {
        fold_unary_expr(self, expression)
    }

    fn fold_logical_expr(&mut self, expression: LogicalExpr) -> LogicalExpr {
        fold_logical_expr(self, expression)
    }

    fn fold_binary_expr(&mut self, expression: BinaryExpr) -> BinaryExpr {
        fold_binary_expr(self, expression)
    }

    fn fold_legacy_interval_expr(&mut self, expression: LegacyIntervalExpr) -> LegacyIntervalExpr {
        fold_legacy_interval_expr(self, expression)
    }

    fn fold_legacy_parenthesized_expr(
        &mut self,
        expression: LegacyParenthesizedExpr,
    ) -> LegacyParenthesizedExpr {
        fold_legacy_parenthesized_expr(self, expression)
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
    super::legacy::statistics::fold(folder, statement)
}

pub fn fold_catalog_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: CatalogStatement,
) -> CatalogStatement {
    super::legacy::catalog::fold(folder, statement)
}

pub fn fold_iceberg_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: IcebergStatement,
) -> IcebergStatement {
    super::legacy::iceberg::fold(folder, statement)
}

pub fn fold_maintenance_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: MaintenanceStatement,
) -> MaintenanceStatement {
    super::legacy::maintenance::fold(folder, statement)
}

pub fn fold_materialized_view_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: MaterializedViewStatement,
) -> MaterializedViewStatement {
    super::legacy::materialized_view::fold(folder, statement)
}

pub fn fold_view_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: ViewStatement,
) -> ViewStatement {
    super::legacy::view::fold(folder, statement)
}

pub fn fold_table_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: TableStatement,
) -> TableStatement {
    super::legacy::table::fold(folder, statement)
}

pub fn fold_dml_statement<F: Fold + ?Sized>(
    folder: &mut F,
    statement: DmlStatement,
) -> DmlStatement {
    super::legacy::dml::fold(folder, statement)
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

pub fn fold_explain_query<F: Fold + ?Sized>(
    folder: &mut F,
    mut query: ExplainQuery,
) -> ExplainQuery {
    query.query = Box::new(folder.fold_query(*query.query));
    query
}

#[recursive::recursive]

pub fn fold_query<F: Fold + ?Sized>(folder: &mut F, mut query: Query) -> Query {
    query.functions = query
        .functions
        .into_iter()
        .map(|function| folder.fold_function_specification(function))
        .collect();
    query.session_properties = query
        .session_properties
        .into_iter()
        .map(|property| folder.fold_session_property(property))
        .collect();
    query.with = query.with.map(|with| folder.fold_with(with));
    query.body = AstBox::new(fold_set_expr(folder, query.body.into_inner()));
    query.modifiers = fold_query_modifiers(folder, query.modifiers);
    query
}

pub fn fold_query_modifiers<F: Fold + ?Sized>(
    folder: &mut F,
    mut modifiers: QueryModifiers,
) -> QueryModifiers {
    modifiers.order_by = modifiers.order_by.map(|order| folder.fold_order_by(order));
    let limit = modifiers.limit.take();
    let fetch = match limit {
        Some(QueryLimit::Limit(node)) => {
            modifiers.limit = Some(QueryLimit::Limit(folder.fold_limit(node)));
            None
        }
        Some(QueryLimit::FetchFirst(node)) => Some(node),
        None => None,
    };
    modifiers.offset = modifiers.offset.map(|node| folder.fold_offset(node));
    if let Some(node) = fetch {
        modifiers.limit = Some(QueryLimit::FetchFirst(folder.fold_fetch_first(node)));
    }
    modifiers
}

#[recursive::recursive]
pub fn fold_set_expr<F: Fold + ?Sized>(folder: &mut F, set_expr: SetExpr) -> SetExpr {
    match set_expr {
        SetExpr::Select(select) => {
            SetExpr::Select(AstBox::new(folder.fold_select(select.into_inner())))
        }
        SetExpr::Values(values) => SetExpr::Values(folder.fold_values(values)),
        SetExpr::Table(table) => SetExpr::Table(AstBox::new(folder.fold_table(table.into_inner()))),
        SetExpr::Query(query) => {
            SetExpr::Query(AstBox::new(folder.fold_table_subquery(query.into_inner())))
        }
        SetExpr::SetOperation(operation) => {
            SetExpr::SetOperation(folder.fold_set_operation(operation))
        }
    }
}

pub fn fold_select<F: Fold + ?Sized>(folder: &mut F, mut select: Select) -> Select {
    select.projection = select
        .projection
        .into_iter()
        .map(|item| folder.fold_select_item(item))
        .collect();
    select.from = select.from.map(|relation| folder.fold_relation(relation));
    select.selection = select.selection.map(|expr| folder.fold_expr(expr));
    select.group_by = select.group_by.map(|node| folder.fold_group_by(node));
    select.having = select.having.map(|expr| folder.fold_expr(expr));
    select.legacy.qualify = select.legacy.qualify.map(|expr| folder.fold_expr(expr));
    select.windows = select
        .windows
        .into_iter()
        .map(|window| folder.fold_window_definition(window))
        .collect();
    select.modifiers = fold_query_modifiers(folder, select.modifiers);
    select
}

#[recursive::recursive]
pub fn fold_group_by<F: Fold + ?Sized>(folder: &mut F, mut node: GroupBy) -> GroupBy {
    node.elements = node
        .elements
        .into_iter()
        .map(|element| folder.fold_grouping_element(element))
        .collect();
    node
}
#[recursive::recursive]
pub fn fold_grouping_element<F: Fold + ?Sized>(
    folder: &mut F,
    node: GroupingElement,
) -> GroupingElement {
    match node {
        GroupingElement::Simple(node) => GroupingElement::Simple(folder.fold_simple_group_by(node)),
        GroupingElement::Sets(node) => GroupingElement::Sets(folder.fold_grouping_sets(node)),
        GroupingElement::Auto(node) => GroupingElement::Auto(folder.fold_auto_group_by(node)),
    }
}
#[recursive::recursive]
pub fn fold_simple_group_by<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: SimpleGroupBy,
) -> SimpleGroupBy {
    node.columns = node
        .columns
        .into_iter()
        .map(|expr| folder.fold_expr(expr))
        .collect();
    node
}
#[recursive::recursive]
pub fn fold_grouping_sets<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: GroupingSets,
) -> GroupingSets {
    node.sets = node
        .sets
        .into_iter()
        .map(|set| set.into_iter().map(|expr| folder.fold_expr(expr)).collect())
        .collect();
    node
}
pub fn fold_auto_group_by<F: Fold + ?Sized>(_folder: &mut F, node: AutoGroupBy) -> AutoGroupBy {
    node
}

#[recursive::recursive]
pub fn fold_window_definition<F: Fold + ?Sized>(
    folder: &mut F,
    mut window: WindowDefinition,
) -> WindowDefinition {
    window.name = folder.fold_ident(window.name);
    window.specification = folder.fold_window_specification(window.specification);
    window
}

#[recursive::recursive]
pub fn fold_window_specification<F: Fold + ?Sized>(
    folder: &mut F,
    mut window: WindowSpecification,
) -> WindowSpecification {
    window.existing_window_name = window
        .existing_window_name
        .map(|name| folder.fold_ident(name));
    window.partition_by = window
        .partition_by
        .into_iter()
        .map(|expr| folder.fold_expr(expr))
        .collect();
    window.order_by = window.order_by.map(|order| folder.fold_order_by(order));
    window.window_frame = window
        .window_frame
        .map(|frame| folder.fold_window_frame(frame));
    window
}

#[recursive::recursive]
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

#[recursive::recursive]
fn fold_array_expr<F: Fold + ?Sized>(folder: &mut F, mut expression: ArrayExpr) -> ArrayExpr {
    expression.values = expression
        .values
        .into_iter()
        .map(|value| folder.fold_expr(value))
        .collect();
    expression
}

#[recursive::recursive]
pub fn fold_expr<F: Fold + ?Sized>(folder: &mut F, expression: Expr) -> Expr {
    match expression {
        Expr::AtLocal(node) => Expr::AtLocal(AstBox::new(folder.fold_at_local(node.into_inner()))),
        Expr::AtTimeZone(node) => {
            Expr::AtTimeZone(AstBox::new(folder.fold_at_time_zone(node.into_inner())))
        }
        Expr::GroupingOperation(node) => Expr::GroupingOperation(AstBox::new(
            folder.fold_grouping_operation(node.into_inner()),
        )),
        Expr::Trim(node) => Expr::Trim(AstBox::new(folder.fold_trim(node.into_inner()))),
        Expr::Overlay(node) => Expr::Overlay(AstBox::new(folder.fold_overlay(node.into_inner()))),
        Expr::IntervalValueExpression(node) => Expr::IntervalValueExpression(AstBox::new(
            folder.fold_interval_value_expression(node.into_inner()),
        )),
        Expr::MethodCall(node) => {
            Expr::MethodCall(AstBox::new(folder.fold_method_call(node.into_inner())))
        }
        Expr::StaticMethodCall(node) => Expr::StaticMethodCall(AstBox::new(
            folder.fold_static_method_call(node.into_inner()),
        )),
        Expr::CurrentCatalog(node) => Expr::CurrentCatalog(folder.fold_current_catalog(node)),
        Expr::CurrentDate(node) => Expr::CurrentDate(folder.fold_current_date(node)),
        Expr::CurrentPath(node) => Expr::CurrentPath(folder.fold_current_path(node)),
        Expr::CurrentSchema(node) => Expr::CurrentSchema(folder.fold_current_schema(node)),
        Expr::CurrentUser(node) => Expr::CurrentUser(folder.fold_current_user(node)),
        Expr::CurrentTime(node) => Expr::CurrentTime(folder.fold_current_time(node)),
        Expr::CurrentTimestamp(node) => Expr::CurrentTimestamp(folder.fold_current_timestamp(node)),
        Expr::LocalTime(node) => Expr::LocalTime(folder.fold_local_time(node)),
        Expr::LocalTimestamp(node) => Expr::LocalTimestamp(folder.fold_local_timestamp(node)),

        Expr::JsonConstructor(node) => Expr::JsonConstructor(crate::ast::AstBox::new(
            folder.fold_json_constructor(node.into_inner()),
        )),
        Expr::JsonExists(node) => Expr::JsonExists(crate::ast::AstBox::new(
            folder.fold_json_exists(node.into_inner()),
        )),
        Expr::JsonValue(node) => Expr::JsonValue(crate::ast::AstBox::new(
            folder.fold_json_value(node.into_inner()),
        )),
        Expr::JsonQuery(node) => {
            Expr::JsonQuery(AstBox::new(folder.fold_json_query(node.into_inner())))
        }
        Expr::JsonSerialize(node) => Expr::JsonSerialize(crate::ast::AstBox::new(
            folder.fold_json_serialize(node.into_inner()),
        )),
        Expr::JsonObject(node) => Expr::JsonObject(crate::ast::AstBox::new(
            folder.fold_json_object(node.into_inner()),
        )),
        Expr::JsonArray(node) => Expr::JsonArray(crate::ast::AstBox::new(
            folder.fold_json_array(node.into_inner()),
        )),

        Expr::Identifier(ident) => Expr::Identifier(folder.fold_ident(ident)),
        Expr::AllRows(node) => Expr::AllRows(folder.fold_all_rows(node)),
        Expr::Parameter(node) => Expr::Parameter(folder.fold_parameter(node)),

        Expr::Literal(literal) => Expr::Literal(folder.fold_literal(literal)),
        Expr::FunctionCall(call) => {
            Expr::FunctionCall(AstBox::new(folder.fold_function_call(call.into_inner())))
        }
        Expr::Extract(node) => {
            Expr::Extract(AstBox::new(folder.fold_extract_expr(node.into_inner())))
        }
        Expr::If(node) => Expr::If(AstBox::new(folder.fold_if_expr(node.into_inner()))),
        Expr::NullIf(node) => {
            Expr::NullIf(AstBox::new(folder.fold_null_if_expr(node.into_inner())))
        }
        Expr::Coalesce(node) => {
            Expr::Coalesce(AstBox::new(folder.fold_coalesce_expr(node.into_inner())))
        }
        Expr::Try(node) => Expr::Try(AstBox::new(folder.fold_try_expr(node.into_inner()))),
        Expr::Format(node) => Expr::Format(AstBox::new(folder.fold_format_expr(node.into_inner()))),
        Expr::WindowOperation(node) => {
            Expr::WindowOperation(AstBox::new(folder.fold_window_operation(node.into_inner())))
        }
        Expr::Unary(expression) => Expr::Unary(folder.fold_unary_expr(expression)),
        Expr::Binary(expression) => Expr::Binary(folder.fold_binary_expr(expression)),
        Expr::Logical(expression) => Expr::Logical(folder.fold_logical_expr(expression)),
        Expr::Exists(node) => Expr::Exists(AstBox::new(folder.fold_exists_expr(node.into_inner()))),
        Expr::Unique(node) => Expr::Unique(AstBox::new(folder.fold_unique_expr(node.into_inner()))),
        Expr::Cast(node) => Expr::Cast(AstBox::new(folder.fold_cast_expr(node.into_inner()))),
        Expr::Lambda(node) => Expr::Lambda(AstBox::new(folder.fold_lambda_expr(node.into_inner()))),
        Expr::SearchedCase(node) => Expr::SearchedCase(AstBox::new(
            folder.fold_searched_case_expr(node.into_inner()),
        )),
        Expr::SimpleCase(node) => {
            Expr::SimpleCase(AstBox::new(folder.fold_simple_case_expr(node.into_inner())))
        }
        Expr::Predicated(node) => {
            Expr::Predicated(AstBox::new(folder.fold_predicated_expr(node.into_inner())))
        }
        Expr::InList(node) => {
            Expr::InList(AstBox::new(folder.fold_in_list_expr(node.into_inner())))
        }
        Expr::LegacyParenthesized(expression) => {
            Expr::LegacyParenthesized(folder.fold_legacy_parenthesized_expr(expression))
        }

        Expr::LegacyInterval(expression) => {
            Expr::LegacyInterval(folder.fold_legacy_interval_expr(expression))
        }
        Expr::Subquery(mut expression) => {
            expression.query = AstBox::new(folder.fold_query(expression.query.into_inner()));
            Expr::Subquery(expression)
        }
        Expr::Row(expression) => Expr::Row(folder.fold_row_expr(expression)),
        Expr::Array(expression) => Expr::Array(fold_array_expr(folder, expression)),

        Expr::Subscript(node) => {
            Expr::Subscript(AstBox::new(folder.fold_subscript_expr(node.into_inner())))
        }
        Expr::ArrayWildcardSubscript(node) => Expr::ArrayWildcardSubscript(AstBox::new(
            folder.fold_array_wildcard_subscript_expr(node.into_inner()),
        )),
        Expr::Dereference(node) => {
            Expr::Dereference(AstBox::new(folder.fold_dereference_expr(node.into_inner())))
        }
    }
}

#[recursive::recursive]
pub fn fold_function_call<F: Fold + ?Sized>(
    folder: &mut F,
    mut call: FunctionCall,
) -> FunctionCall {
    call.name = folder.fold_object_name(call.name);
    call.processing_mode = call
        .processing_mode
        .map(|mode| folder.fold_processing_mode(mode));
    call.arguments = call
        .arguments
        .into_iter()
        .map(|argument| folder.fold_call_argument(argument))
        .collect();
    call.order_by = call.order_by.map(|order| folder.fold_order_by(order));
    call.filter = call.filter.map(|filter| folder.fold_expr(filter));
    call.window = call.window.map(|window| folder.fold_window(window));
    call
}

pub fn fold_unary_expr<F: Fold + ?Sized>(folder: &mut F, mut expression: UnaryExpr) -> UnaryExpr {
    expression.expression = AstBox::new(folder.fold_expr(expression.expression.into_inner()));
    expression
}

pub fn fold_binary_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut expression: BinaryExpr,
) -> BinaryExpr {
    expression.left = AstBox::new(folder.fold_expr(expression.left.into_inner()));
    expression.right = AstBox::new(folder.fold_expr(expression.right.into_inner()));
    expression
}

#[recursive::recursive]
pub fn fold_legacy_interval_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut expression: LegacyIntervalExpr,
) -> LegacyIntervalExpr {
    expression.value = AstBox::new(folder.fold_expr(expression.value.into_inner()));
    expression.leading_precision = expression
        .leading_precision
        .map(|precision| AstBox::new(folder.fold_expr(precision.into_inner())));
    expression.fractional_seconds_precision = expression
        .fractional_seconds_precision
        .map(|precision| AstBox::new(folder.fold_expr(precision.into_inner())));
    expression
}

pub fn fold_legacy_parenthesized_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut expression: LegacyParenthesizedExpr,
) -> LegacyParenthesizedExpr {
    expression.expression = AstBox::new(folder.fold_expr(expression.expression.into_inner()));
    expression
}

#[recursive::recursive]
pub fn walk_logical_expr<V: Visit + ?Sized>(visitor: &mut V, expression: &LogicalExpr) {
    for term in &expression.terms {
        visitor.visit_expr(term);
    }
}

#[recursive::recursive]
pub fn fold_logical_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut expression: LogicalExpr,
) -> LogicalExpr {
    expression.terms = expression
        .terms
        .into_iter()
        .map(|term| folder.fold_expr(term))
        .collect();
    expression
}

#[recursive::recursive]
pub fn walk_row_expr<V: Visit + ?Sized>(visitor: &mut V, node: &RowExpr) {
    for field in &node.fields {
        visitor.visit_row_field(field);
    }
}

#[recursive::recursive]
pub fn walk_row_field<V: Visit + ?Sized>(visitor: &mut V, node: &RowField) {
    if let Some(name) = &node.name {
        visitor.visit_ident(name);
    }
    visitor.visit_expr(&node.expression);
}

#[recursive::recursive]
pub fn fold_row_expr<F: Fold + ?Sized>(folder: &mut F, mut node: RowExpr) -> RowExpr {
    node.fields = node
        .fields
        .into_iter()
        .map(|field| folder.fold_row_field(field))
        .collect();
    node
}

#[recursive::recursive]
pub fn fold_row_field<F: Fold + ?Sized>(folder: &mut F, mut node: RowField) -> RowField {
    node.name = node.name.map(|name| folder.fold_ident(name));
    node.expression = folder.fold_expr(node.expression);
    node
}

#[recursive::recursive]
pub fn walk_subscript_expr<V: Visit + ?Sized>(visitor: &mut V, node: &SubscriptExpr) {
    visitor.visit_expr(&node.base);
    visitor.visit_expr(&node.index);
}
#[recursive::recursive]
pub fn walk_array_wildcard_subscript_expr<V: Visit + ?Sized>(
    visitor: &mut V,
    node: &ArrayWildcardSubscriptExpr,
) {
    visitor.visit_expr(&node.base);
}
#[recursive::recursive]
pub fn walk_dereference_expr<V: Visit + ?Sized>(visitor: &mut V, node: &DereferenceExpr) {
    visitor.visit_expr(&node.base);
    if let Some(field) = &node.field {
        visitor.visit_ident(field);
    }
}
#[recursive::recursive]
pub fn fold_subscript_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: SubscriptExpr,
) -> SubscriptExpr {
    node.base = folder.fold_expr(node.base);
    node.index = folder.fold_expr(node.index);
    node
}
#[recursive::recursive]
pub fn fold_array_wildcard_subscript_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: ArrayWildcardSubscriptExpr,
) -> ArrayWildcardSubscriptExpr {
    node.base = folder.fold_expr(node.base);
    node
}
#[recursive::recursive]
pub fn fold_dereference_expr<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: DereferenceExpr,
) -> DereferenceExpr {
    node.base = folder.fold_expr(node.base);
    node.field = node.field.map(|field| folder.fold_ident(field));
    node
}

#[recursive::recursive]
pub fn walk_order_by<V: Visit + ?Sized>(visitor: &mut V, node: &OrderBy) {
    for item in &node.items {
        visitor.visit_sort_item(item);
    }
}

#[recursive::recursive]
pub fn walk_sort_item<V: Visit + ?Sized>(visitor: &mut V, node: &SortItem) {
    visitor.visit_expr(&node.sort_key);
}

#[recursive::recursive]
pub fn fold_order_by<F: Fold + ?Sized>(folder: &mut F, mut node: OrderBy) -> OrderBy {
    node.items = node
        .items
        .into_iter()
        .map(|item| folder.fold_sort_item(item))
        .collect();
    node
}

#[recursive::recursive]
pub fn fold_sort_item<F: Fold + ?Sized>(folder: &mut F, mut node: SortItem) -> SortItem {
    node.sort_key = folder.fold_expr(node.sort_key);
    node
}

#[recursive::recursive]
pub fn walk_window_frame<V: Visit + ?Sized>(visitor: &mut V, node: &WindowFrame) {
    super::visit::walk_window_frame_bound(visitor, &node.start_bound);
    if let Some(child) = &node.end_bound {
        super::visit::walk_window_frame_bound(visitor, child);
    }
    for child in &node.measures {
        visitor.visit_measure_definition(child);
    }
    if let Some(child) = &node.after_match_skip_to {
        visitor.visit_skip_to(child);
    }
    if let Some(child) = &node.pattern_search_mode {
        visitor.visit_pattern_search_mode(child);
    }
    if let Some(child) = &node.pattern {
        visitor.visit_row_pattern(child);
    }
    for child in &node.subsets {
        visitor.visit_subset_definition(child);
    }
    for child in &node.variable_definitions {
        visitor.visit_variable_definition(child);
    }
}

#[recursive::recursive]
pub fn fold_window_frame<F: Fold + ?Sized>(folder: &mut F, mut node: WindowFrame) -> WindowFrame {
    node.start_bound = super::visit::fold_window_frame_bound(folder, node.start_bound);
    node.end_bound = node
        .end_bound
        .map(|child| super::visit::fold_window_frame_bound(folder, child));
    node.measures = node
        .measures
        .into_iter()
        .map(|child| folder.fold_measure_definition(child))
        .collect();
    node.after_match_skip_to = node
        .after_match_skip_to
        .map(|child| folder.fold_skip_to(child));
    node.pattern_search_mode = node
        .pattern_search_mode
        .map(|child| folder.fold_pattern_search_mode(child));
    node.pattern = node.pattern.map(|child| folder.fold_row_pattern(child));
    node.subsets = node
        .subsets
        .into_iter()
        .map(|child| folder.fold_subset_definition(child))
        .collect();
    node.variable_definitions = node
        .variable_definitions
        .into_iter()
        .map(|child| folder.fold_variable_definition(child))
        .collect();
    node
}

#[recursive::recursive]
pub fn walk_window<V: Visit + ?Sized>(visitor: &mut V, node: &Window) {
    match node {
        Window::Reference(node) => visitor.visit_window_reference(node),
        Window::Specification(node) => visitor.visit_window_specification(node),
    }
}
#[recursive::recursive]
pub fn walk_window_reference<V: Visit + ?Sized>(visitor: &mut V, node: &WindowReference) {
    visitor.visit_ident(&node.name);
}
#[recursive::recursive]
pub fn walk_window_operation<V: Visit + ?Sized>(visitor: &mut V, node: &WindowOperation) {
    visitor.visit_ident(&node.name);
    visitor.visit_window(&node.window);
}
#[recursive::recursive]
pub fn fold_window<F: Fold + ?Sized>(folder: &mut F, node: Window) -> Window {
    match node {
        Window::Reference(node) => Window::Reference(folder.fold_window_reference(node)),
        Window::Specification(node) => Window::Specification(AstBox::new(
            folder.fold_window_specification(node.into_inner()),
        )),
    }
}
#[recursive::recursive]
pub fn fold_window_reference<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: WindowReference,
) -> WindowReference {
    node.name = folder.fold_ident(node.name);
    node
}
#[recursive::recursive]
pub fn fold_window_operation<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: WindowOperation,
) -> WindowOperation {
    node.name = folder.fold_ident(node.name);
    node.window = folder.fold_window(node.window);
    node
}

#[recursive::recursive]
pub fn walk_call_argument<V: Visit + ?Sized>(visitor: &mut V, node: &CallArgument) {
    if let Some(name) = &node.name {
        visitor.visit_ident(name);
    }
    visitor.visit_expr(&node.value);
}
pub fn walk_processing_mode<V: Visit + ?Sized>(visitor: &mut V, node: &ProcessingMode) {
    let _ = (visitor, node);
}
#[recursive::recursive]
pub fn fold_call_argument<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: CallArgument,
) -> CallArgument {
    node.name = node.name.map(|name| folder.fold_ident(name));
    node.value = folder.fold_expr(node.value);
    node
}
pub fn fold_processing_mode<F: Fold + ?Sized>(
    folder: &mut F,
    node: ProcessingMode,
) -> ProcessingMode {
    let _ = folder;
    node
}

#[recursive::recursive]
pub fn walk_set_operation<V: Visit + ?Sized>(visitor: &mut V, node: &SetOperation) {
    if let Some(corresponding) = &node.corresponding {
        for column in &corresponding.columns {
            visitor.visit_ident(column);
        }
    }
    for input in &node.inputs {
        walk_set_expr(visitor, input);
    }
}
#[recursive::recursive]
pub fn fold_set_operation<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: SetOperation,
) -> SetOperation {
    node.inputs = node
        .inputs
        .into_iter()
        .map(|input| fold_set_expr(folder, input))
        .collect();
    if let Some(corresponding) = &mut node.corresponding {
        corresponding.columns = std::mem::take(&mut corresponding.columns)
            .into_iter()
            .map(|column| folder.fold_ident(column))
            .collect();
    }
    node
}

#[recursive::recursive]
pub fn walk_with<V: Visit + ?Sized>(visitor: &mut V, node: &With) {
    for cte in &node.ctes {
        visitor.visit_cte(cte);
    }
}
#[recursive::recursive]
pub fn walk_cte<V: Visit + ?Sized>(visitor: &mut V, node: &Cte) {
    visitor.visit_ident(&node.name);
    for column in node.columns.iter().flatten() {
        visitor.visit_ident(column);
    }
    visitor.visit_query(&node.query);
}
#[recursive::recursive]
pub fn fold_with<F: Fold + ?Sized>(folder: &mut F, mut node: With) -> With {
    node.ctes = node
        .ctes
        .into_iter()
        .map(|cte| folder.fold_cte(cte))
        .collect();
    node
}
#[recursive::recursive]
pub fn fold_cte<F: Fold + ?Sized>(folder: &mut F, mut node: Cte) -> Cte {
    node.name = folder.fold_ident(node.name);
    node.columns = node.columns.map(|columns| {
        columns
            .into_iter()
            .map(|column| folder.fold_ident(column))
            .collect()
    });
    node.query = AstBox::new(folder.fold_query(node.query.into_inner()));
    node
}

#[recursive::recursive]
pub fn walk_values<V: Visit + ?Sized>(visitor: &mut V, node: &Values) {
    for row in &node.rows {
        visitor.visit_expr(row);
    }
}
#[recursive::recursive]
pub fn fold_values<F: Fold + ?Sized>(folder: &mut F, mut node: Values) -> Values {
    node.rows = node
        .rows
        .into_iter()
        .map(|row| folder.fold_expr(row))
        .collect();
    node
}
#[recursive::recursive]
pub fn walk_select_item<V: Visit + ?Sized>(visitor: &mut V, node: &SelectItem) {
    match node {
        SelectItem::SingleColumn(node) => visitor.visit_single_column(node),
        SelectItem::AllColumns(node) => visitor.visit_all_columns(node),
    }
}
#[recursive::recursive]
pub fn walk_single_column<V: Visit + ?Sized>(visitor: &mut V, node: &SingleColumn) {
    visitor.visit_expr(&node.expression);
    if let Some(alias) = &node.alias {
        visitor.visit_ident(alias);
    }
}
#[recursive::recursive]
pub fn walk_all_columns<V: Visit + ?Sized>(visitor: &mut V, node: &AllColumns) {
    if let Some(target) = &node.target {
        visitor.visit_expr(target);
    }
    for alias in &node.aliases {
        visitor.visit_ident(alias);
    }
}
#[recursive::recursive]
pub fn fold_select_item<F: Fold + ?Sized>(folder: &mut F, node: SelectItem) -> SelectItem {
    match node {
        SelectItem::SingleColumn(node) => {
            SelectItem::SingleColumn(AstBox::new(folder.fold_single_column(node.into_inner())))
        }
        SelectItem::AllColumns(node) => {
            SelectItem::AllColumns(AstBox::new(folder.fold_all_columns(node.into_inner())))
        }
    }
}
#[recursive::recursive]
pub fn fold_single_column<F: Fold + ?Sized>(
    folder: &mut F,
    mut node: SingleColumn,
) -> SingleColumn {
    node.expression = folder.fold_expr(node.expression);
    node.alias = node.alias.map(|alias| folder.fold_ident(alias));
    node
}
#[recursive::recursive]
pub fn fold_all_columns<F: Fold + ?Sized>(folder: &mut F, mut node: AllColumns) -> AllColumns {
    node.target = node.target.map(|target| folder.fold_expr(target));
    node.aliases = node
        .aliases
        .into_iter()
        .map(|alias| folder.fold_ident(alias))
        .collect();
    node
}

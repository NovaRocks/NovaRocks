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

//! Syntax-only SQL abstract syntax tree nodes.

pub(crate) mod catalog;
pub(crate) mod command;
mod control_expr;
pub(crate) mod dml;
mod equivalence;
mod expr;
pub(crate) mod iceberg;
pub(crate) mod maintenance;
pub(crate) mod materialized_view;
mod name_chain;
mod predicate;
pub use control_expr::*;
mod query;
pub use predicate::*;
mod relation;
mod routine;

pub use routine::{
    AssignmentStatement, CaseStatement, CaseStatementWhenClause, CompoundStatement,
    ControlStatement, ElseClause, ElseIfClause, FunctionParameter, FunctionSpecification,
    IfStatement, IterateStatement, LeaveStatement, LoopStatement, RepeatStatement, ReturnStatement,
    RoutineBody, RoutineCharacteristic, RoutineProperty, RoutineSecurity, VariableDeclaration,
    WhileStatement,
};
pub(crate) mod session;
pub(crate) mod statistics;
mod storage;
mod type_literal;
pub use type_literal::*;
mod row_pattern;
pub(crate) mod table;
mod validate;
pub(crate) mod view;
mod visit;
mod window;
pub use storage::{AstBox, AstVec};

pub use catalog::{
    CatalogProperty, CatalogStatement, CreateCatalog, CreateDatabase, DropCatalog, DropDatabase,
    DropTable, ShowCreateTable, TruncateTable,
};
pub use command::{Property, PropertyKeyValue};
pub use dml::{
    AddEqualityDelete, Assignment, CreateTableAsSelect, Delete, DmlStatement, Insert,
    InsertPartitionEntry, InsertPartitions, Merge, MergeClause, MergeMatchedAction,
    MergeNotMatchedAction, MutationSource, Update,
};
pub use equivalence::{SyntaxEq, syntax_eq_explain_query, syntax_eq_expr, syntax_eq_query};
pub use expr::{
    AccessExpr, AccessKind, ArrayExpr, ArrayWildcardSubscriptExpr, BinaryExpr, BinaryOperator,
    CallArgument, DereferenceExpr, Expr, FunctionCall, IntervalExpr, IntervalField, JsonOperator,
    LogicalExpr, LogicalOperator, NestedExpr, NullTreatment, ProcessingMode, ProcessingModeKind,
    RowExpr, RowField, SubqueryExpr, SubscriptExpr, UnaryExpr, UnaryOperator,
};
pub use iceberg::{
    AddFiles, AlterIcebergTable, ColumnPath, ColumnPosition, IcebergColumnAction,
    IcebergPartitionChange, IcebergPartitionField, IcebergPropertiesAction, IcebergReferenceAction,
    IcebergReferenceKind, IcebergSchemaChange, IcebergStatement, IcebergTableAction,
    RawReferenceOptions, ReferenceAnchor,
};
pub use maintenance::{
    CallStatement, ExpireSnapshots, ExpireSnapshotsOption, MaintenanceStatement, MaintenanceValue,
    OptimizeTable, ProcedureArgument, ProcedureArgumentMode, ProcedureMap, ProcedureMapEntry,
    RemoveOrphanFiles, RewriteManifests,
};
pub use materialized_view::{
    AlterMaterializedView, CreateMaterializedView, DropMaterializedView,
    ExplainRefreshMaterializedView, MaterializedViewAlterAction, MaterializedViewDistribution,
    MaterializedViewExplainLevel, MaterializedViewPartitionArgument,
    MaterializedViewPartitionField, MaterializedViewProperty, MaterializedViewRefreshMode,
    MaterializedViewRefreshPolicy, MaterializedViewStatement, RefreshMaterializedView,
};
pub use query::{
    Corresponding, Cte, ExplainFormat, ExplainQuery, Fetch, GroupBy, NullOrdering, Offset,
    OffsetRows, OrderBy, Query, QueryModifiers, ReplaceSelectItem, Select, SelectItem,
    SelectQuantifier, SessionProperty, SetExpr, SetOperation, SetOperator, SetQuantifier, SortItem,
    SortOrdering, Values, WildcardOptions, With,
};
pub use relation::{
    Join, JoinConstraint, JoinOperator, TableAlias, TableFactor, TableFunctionSyntax, TableVersion,
    TableVersionKind, TableWithJoins,
};
pub use row_pattern::{
    MeasureDefinition, PatternAnchorKind, PatternInteger, PatternQuantifier, PatternSearchMode,
    PatternSearchModeKind, RowPattern, SkipTo, SubsetDefinition, VariableDefinition,
};
pub use session::ShowProcessList;
pub use session::{
    KillKind, KillStatement, SessionStatement, SetAssignment, SetScope, SetStatement,
    TransactionControlKind, TransactionControlStatement, UseStatement,
};
pub use statistics::{AnalyzeTable, ShowStats, StatisticsStatement};
pub use table::{
    ColumnDefinition, CreateTable, LegacyRangePartition, LegacyRangePartitionDefinition,
    PartitionTransform, TableDistribution, TableKey, TableKeyKind, TablePartition,
    TablePartitionTransform, TableProperty, TableStatement,
};
pub use validate::{validate_statement, validate_statements};
pub use view::{CreateView, DropView, ShowCreateView, ViewStatement};
pub use visit::{
    Fold, Visit, fold_binary_expr, fold_expr, fold_function_call, fold_ident, fold_logical_expr,
    fold_nested_expr, fold_object_name, fold_query, fold_select, fold_statement, fold_table_factor,
    fold_unary_expr, fold_view_statement, walk_binary_expr, walk_expr, walk_function_call,
    walk_ident, walk_logical_expr, walk_nested_expr, walk_object_name, walk_query, walk_select,
    walk_statement, walk_table_factor, walk_unary_expr, walk_view_statement,
};
pub use window::{
    Window, WindowDefinition, WindowFrame, WindowFrameBound, WindowFrameExclusion,
    WindowFrameUnits, WindowOperation, WindowReference, WindowSpecification,
};

use crate::Span;

/// A top-level SQL statement.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Statement {
    ShowProcessList(ShowProcessList),
    Statistics(StatisticsStatement),
    Catalog(CatalogStatement),
    Iceberg(IcebergStatement),
    Maintenance(MaintenanceStatement),
    MaterializedView(MaterializedViewStatement),
    View(ViewStatement),
    Table(TableStatement),
    Dml(DmlStatement),
    Session(SessionStatement),
    Query(Query),
    ExplainQuery(ExplainQuery),
}

impl Statement {
    pub const fn span(&self) -> Span {
        match self {
            Self::ShowProcessList(statement) => statement.span,
            Self::Statistics(statement) => statement.span(),
            Self::Catalog(statement) => statement.span(),
            Self::Iceberg(statement) => statement.span(),
            Self::Maintenance(statement) => statement.span(),
            Self::MaterializedView(statement) => statement.span(),
            Self::View(statement) => statement.span(),
            Self::Table(statement) => statement.span(),
            Self::Dml(statement) => statement.span(),
            Self::Session(statement) => statement.span(),
            Self::Query(query) => query.span,
            Self::ExplainQuery(query) => query.span,
        }
    }
}

/// Decoded identifier text and whether its source name was delimited.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Ident {
    pub value: String,
    pub quoted: bool,
    pub span: Span,
}

/// A qualified SQL object name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectName {
    pub parts: Vec<Ident>,
    pub span: Span,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{Fold, Visit};

    fn span(start: usize, end: usize) -> Span {
        Span::new(start, end)
    }

    #[test]
    fn every_seed_node_retains_its_source_span() {
        let name = ObjectName {
            parts: vec![Ident {
                value: "catalog".to_owned(),
                quoted: false,
                span: span(0, 7),
            }],
            span: span(0, 7),
        };
        let type_name = TypeName {
            kind: TypeKind::Generic(GenericDataType {
                name: name.parts[0].clone(),
                arguments: AstVec::new(),
            }),
            span: span(0, 7),
        };
        let literal = Literal {
            kind: LiteralKind::Long {
                spelling: "42".to_owned(),
                parsed_value: ("42".to_owned()).parse::<i64>().unwrap(),
            },
            span: span(8, 10),
        };
        let expression = Expr::Literal(literal.clone());
        let statement = Statement::ShowProcessList(ShowProcessList {
            full: false,
            span: span(11, 24),
        });

        assert_eq!(name.span, span(0, 7));
        assert_eq!(type_name.span, span(0, 7));
        assert_eq!(literal.span, span(8, 10));
        assert_eq!(expression.span(), span(8, 10));
        assert_eq!(statement.span(), span(11, 24));
    }

    #[test]
    fn visitor_reaches_every_nested_node() {
        struct CountVisitor(usize);

        impl Visit for CountVisitor {
            fn visit_ident(&mut self, _: &Ident) {
                self.0 += 1;
            }

            fn visit_literal(&mut self, _: &Literal) {
                self.0 += 1;
            }
        }

        let expression = Expr::Binary(BinaryExpr {
            left: Box::new(Expr::Identifier(Ident {
                value: "a".to_owned(),
                quoted: false,
                span: span(0, 1),
            })),
            operator: BinaryOperator::Add,
            right: Box::new(Expr::FunctionCall(AstBox::new(FunctionCall {
                name: ObjectName {
                    parts: vec![Ident {
                        value: "abs".to_owned(),
                        quoted: false,
                        span: span(4, 7),
                    }],
                    span: span(4, 7),
                },
                arguments: vec![Expr::Literal(Literal {
                    kind: LiteralKind::Long {
                        spelling: "1".to_owned(),
                        parsed_value: ("1".to_owned()).parse::<i64>().unwrap(),
                    },
                    span: span(8, 9),
                })]
                .into_iter()
                .map(|value| crate::build::call_argument(value, None))
                .collect(),
                processing_mode: None,
                distinct: false,
                order_by: None,
                filter: None,
                null_treatment: None,
                window: None,
                span: span(4, 10),
            }))),
            span: span(0, 10),
        });

        let mut visitor = CountVisitor(0);
        visitor.visit_expr(&expression);
        assert_eq!(visitor.0, 3);
    }

    #[test]
    fn fold_rebuilds_nested_expressions() {
        struct Rename;

        impl Fold for Rename {
            fn fold_ident(&mut self, mut ident: Ident) -> Ident {
                if ident.value == "old" {
                    ident.value = "new".to_owned();
                }
                ident
            }
        }

        let expression = Expr::Nested(NestedExpr {
            expression: Box::new(Expr::Identifier(Ident {
                value: "old".to_owned(),
                quoted: false,
                span: span(1, 4),
            })),
            span: span(0, 5),
        });

        let expression = Rename.fold_expr(expression);
        assert_eq!(
            expression,
            Expr::Nested(NestedExpr {
                expression: Box::new(Expr::Identifier(Ident {
                    value: "new".to_owned(),
                    quoted: false,
                    span: span(1, 4),
                })),
                span: span(0, 5),
            })
        );
    }
}

pub use routine::{
    fold_assignment_statement, fold_case_statement, fold_case_statement_when_clause,
    fold_compound_statement, fold_control_statement, fold_else_clause, fold_else_if_clause,
    fold_function_parameter, fold_function_specification, fold_if_statement,
    fold_iterate_statement, fold_leave_statement, fold_loop_statement, fold_repeat_statement,
    fold_return_statement, fold_routine_body, fold_routine_characteristic, fold_routine_property,
    fold_variable_declaration, fold_while_statement, walk_assignment_statement,
    walk_case_statement, walk_case_statement_when_clause, walk_compound_statement,
    walk_control_statement, walk_else_clause, walk_else_if_clause, walk_function_parameter,
    walk_function_specification, walk_if_statement, walk_iterate_statement, walk_leave_statement,
    walk_loop_statement, walk_repeat_statement, walk_return_statement, walk_routine_body,
    walk_routine_characteristic, walk_routine_property, walk_variable_declaration,
    walk_while_statement,
};

pub use visit::{fold_query_modifiers, walk_query_modifiers};

mod json;

pub use json::{
    JsonArray, JsonArrayElement, JsonAstInvariantError, JsonConstructor, JsonExists,
    JsonExistsErrorBehavior, JsonFormat, JsonObject, JsonObjectMember, JsonPathInvocation,
    JsonPathParameter, JsonPathString, JsonQuery, JsonQueryBehavior, JsonQueryQuotesBehavior,
    JsonQueryWrapperBehavior, JsonSerialize, JsonSerializeErrorBehavior, JsonTable,
    JsonTableColumn, JsonTableDefaultPlan, JsonTableErrorBehavior, JsonTableNestedColumns,
    JsonTableOrdinalityColumn, JsonTableParentChildPlanType, JsonTablePlan, JsonTablePlanLeaf,
    JsonTablePlanParentChild, JsonTablePlanSiblings, JsonTableQueryColumn,
    JsonTableSiblingsPlanType, JsonTableSpecificPlan, JsonTableValueColumn, JsonValue,
    JsonValueBehavior,
};

// Re-export helpers if following the existing ast module API.
pub use json::{
    fold_json_array, fold_json_array_element, fold_json_constructor, fold_json_exists,
    fold_json_object, fold_json_object_member, fold_json_path_invocation, fold_json_path_parameter,
    fold_json_path_string, fold_json_query, fold_json_serialize, fold_json_table,
    fold_json_table_column, fold_json_table_default_plan, fold_json_table_nested_columns,
    fold_json_table_ordinality_column, fold_json_table_plan, fold_json_table_plan_leaf,
    fold_json_table_plan_parent_child, fold_json_table_plan_siblings, fold_json_table_query_column,
    fold_json_table_specific_plan, fold_json_table_value_column, fold_json_value, walk_json_array,
    walk_json_array_element, walk_json_constructor, walk_json_exists, walk_json_object,
    walk_json_object_member, walk_json_path_invocation, walk_json_path_parameter,
    walk_json_path_string, walk_json_query, walk_json_serialize, walk_json_table,
    walk_json_table_column, walk_json_table_default_plan, walk_json_table_nested_columns,
    walk_json_table_ordinality_column, walk_json_table_plan, walk_json_table_plan_leaf,
    walk_json_table_plan_parent_child, walk_json_table_plan_siblings, walk_json_table_query_column,
    walk_json_table_specific_plan, walk_json_table_value_column, walk_json_value,
};

#[cfg(test)]
mod control_expr_tests;

pub use visit::{fold_row_expr, fold_row_field, walk_row_expr, walk_row_field};

pub use name_chain::{IdentifierChain, identifier_chain_observed};
pub use visit::{
    fold_array_wildcard_subscript_expr, fold_dereference_expr, fold_subscript_expr,
    walk_array_wildcard_subscript_expr, walk_dereference_expr, walk_subscript_expr,
};

pub use visit::{fold_order_by, fold_sort_item, walk_order_by, walk_sort_item};

pub use row_pattern::{
    fold_measure_definition, fold_pattern_integer, fold_pattern_quantifier,
    fold_pattern_search_mode, fold_row_pattern, fold_skip_to, fold_subset_definition,
    fold_variable_definition, walk_measure_definition, walk_pattern_integer,
    walk_pattern_quantifier, walk_pattern_search_mode, walk_row_pattern, walk_skip_to,
    walk_subset_definition, walk_variable_definition,
};
pub use visit::{
    fold_window, fold_window_definition, fold_window_frame, fold_window_operation,
    fold_window_reference, fold_window_specification, walk_window, walk_window_definition,
    walk_window_frame, walk_window_operation, walk_window_reference, walk_window_specification,
};

pub use visit::{
    fold_call_argument, fold_processing_mode, walk_call_argument, walk_processing_mode,
};

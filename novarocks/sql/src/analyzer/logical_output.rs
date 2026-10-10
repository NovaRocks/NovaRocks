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

//! SQL semantic output provenance for physically ambiguous scalar values.

use arrow::datatypes::{DataType, Field};
use novarocks_parser::{Span, ast};
use novarocks_types::logical::{LogicalType, field_with_logical_type};
use novarocks_types::schema::SqlType;

use super::{AnalyzerContext, scope::AnalyzerScope};
use crate::analysis::{ExprKind, TypedExpr};
use crate::analyze_error::AnalyzeError;

#[path = "logical_container.rs"]
mod container;

impl AnalyzerContext<'_> {
    pub(super) fn logical_output_type(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        // The existing independent source witness also covers explicitly typed
        // empty/all-null JSON literals and catalog JSON-list columns. A physical
        // List<Utf8> or an output CAST alone does not establish this witness.
        if self.json_list_provenance(source, expression, scope)
            && matches!(&expression.value_type.data_type, DataType::List(item)
                if item.data_type() == &DataType::Utf8)
        {
            return Some(SqlType::Array(Box::new(SqlType::Json)));
        }
        // A public CAST target alone cannot prove Json contents or serialized
        // Variant identity. Json retains its already-proven source domain. The
        // existing exact LargeBinary -> LargeBinary Variant CAST returns the
        // original array, so a separately proven Variant source also retains
        // its identity. Other explicit CASTs keep their current owner semantics,
        // including an explicit VARBINARY/VARCHAR CAST clearing that identity.
        if let Some(ast::Expr::Cast(cast)) = source {
            let target_name = cast.data_type.name.parts.last();
            let json_target = target_name.is_some_and(|part| {
                part.value.eq_ignore_ascii_case("json") || part.value.eq_ignore_ascii_case("jsonb")
            });
            let variant_target =
                target_name.is_some_and(|part| part.value.eq_ignore_ascii_case("variant"));
            return match (&expression.kind, json_target, variant_target) {
                (ExprKind::Cast { expr: inner, .. }, true, _) => self
                    .logical_output_type(Some(&cast.expr), inner, scope)
                    .filter(|domain| *domain == SqlType::Json),
                (ExprKind::Cast { expr: inner, .. }, _, true)
                    if inner.value_type.data_type == DataType::LargeBinary
                        && expression.value_type.data_type == DataType::LargeBinary =>
                {
                    self.logical_output_type(Some(&cast.expr), inner, scope)
                        .filter(|domain| *domain == SqlType::Variant)
                }
                _ => None,
            };
        }
        // Materialized implicit coercions preserve a domain only while both
        // carriers remain compatible with that already-established domain.
        if let ExprKind::Cast { expr: inner, .. } = &expression.kind {
            let domain = self.logical_output_type(source, inner, scope)?;
            return (logical_carrier_matches(&domain, &inner.value_type.data_type)
                && logical_carrier_matches(&domain, &expression.value_type.data_type))
            .then_some(domain);
        }
        let source_binding = match source {
            Some(ast::Expr::Identifier(ident)) => scope.resolve(None, &ident.value).ok(),
            Some(ast::Expr::CompoundIdentifier(parts)) if parts.parts.len() >= 2 => {
                let n = parts.parts.len();
                scope
                    .resolve(Some(&parts.parts[n - 2].value), &parts.parts[n - 1].value)
                    .ok()
            }
            _ => None,
        };
        if let Some((column_id, _, _)) = source_binding {
            return self
                .factory
                .borrow()
                .logical_type(column_id)
                .filter(|domain| {
                    logical_carrier_matches(domain, &expression.value_type.data_type)
                });
        }
        let domain = match &expression.kind {
            ExprKind::ColumnRef { .. } => scope.logical_type_of_expr(expression),
            ExprKind::FunctionCall { binding, args, .. } => {
                if let Some(domain) = crate::functions::scalar_output_logical_type(binding) {
                    Some(domain)
                } else if binding.kind == novarocks_functions::FunctionKind::Scalar {
                    let source_args = match source {
                        Some(ast::Expr::FunctionCall(function)) => {
                            Some(function.arguments.as_slice())
                        }
                        _ => None,
                    };
                    let source_arg = |index: usize| source_args.and_then(|args| args.get(index));
                    match binding.function_id.as_str() {
                        "builtin.scalar/coalesce/v1" => self.merge_logical_values(
                            args.iter().enumerate().map(|(i, arg)| (source_arg(i), arg)),
                            scope,
                        ),
                        "builtin.scalar/ifnull/v1" | "builtin.scalar/nvl/v1" if args.len() == 2 => {
                            self.merge_logical_values(
                                args.iter().enumerate().map(|(i, arg)| (source_arg(i), arg)),
                                scope,
                            )
                        }
                        "builtin.scalar/if/v1" if args.len() == 3 => self.merge_logical_values(
                            args.iter()
                                .enumerate()
                                .skip(1)
                                .map(|(i, arg)| (source_arg(i), arg)),
                            scope,
                        ),
                        // The comparator can affect equality but never supplies
                        // the returned value. Its own logical domain is irrelevant.
                        "builtin.scalar/nullif/v1" if args.len() == 2 => {
                            self.logical_output_type(source_arg(0), &args[0], scope)
                        }
                        _ => self.container_output_type(source, expression, scope),
                    }
                } else {
                    None
                }
            }
            ExprKind::AggregateCall { resolved, args, .. } => {
                crate::functions::aggregate_output_logical_type(resolved).or_else(|| {
                    if resolved.kind == novarocks_functions::FunctionKind::Aggregate
                        && resolved.function_id.as_str() == "builtin.aggregate/any_value/v1"
                        && args.len() == 1
                    {
                        self.logical_output_type(
                            container::argument_source(source, 0),
                            &args[0],
                            scope,
                        )
                    } else {
                        self.container_output_type(source, expression, scope)
                    }
                })
            }
            ExprKind::WindowCall {
                aggregate_binding: Some(binding),
                args,
                ..
            } => crate::functions::aggregate_output_logical_type(binding).or_else(|| {
                if binding.kind == novarocks_functions::FunctionKind::Aggregate
                    && binding.function_id.as_str() == "builtin.aggregate/any_value/v1"
                    && args.len() == 1
                {
                    self.logical_output_type(container::argument_source(source, 0), &args[0], scope)
                } else {
                    self.container_output_type(source, expression, scope)
                }
            }),
            ExprKind::WindowCall {
                aggregate_binding: None,
                ..
            } => self.window_value_output_type(source, expression, scope),
            ExprKind::Case {
                when_then,
                else_expr,
                ..
            } => {
                let case = match source {
                    Some(ast::Expr::Case(case)) => Some(case),
                    _ => None,
                };
                self.merge_logical_values(
                    when_then
                        .iter()
                        .enumerate()
                        .map(|(i, (_, value))| (case.and_then(|case| case.results.get(i)), value))
                        .chain(else_expr.as_deref().map(|value| {
                            (case.and_then(|case| case.else_result.as_deref()), value)
                        })),
                    scope,
                )
            }
            ExprKind::Nested(inner) => self.logical_output_type(
                source.and_then(|source| match source {
                    ast::Expr::Nested(nested) => Some(nested.expression.as_ref()),
                    _ => None,
                }),
                inner,
                scope,
            ),
            _ => None,
        };
        domain.filter(|domain| logical_carrier_matches(domain, &expression.value_type.data_type))
    }

    fn merge_logical_values<'a>(
        &self,
        values: impl IntoIterator<Item = (Option<&'a ast::Expr>, &'a TypedExpr)>,
        scope: &AnalyzerScope,
    ) -> Option<SqlType> {
        let mut domain = None;
        for (source, value) in values {
            if is_null_value(source, value) {
                continue;
            }
            let current = self.logical_output_type(source, value, scope)?;
            match &domain {
                Some(previous) if *previous != current => return None,
                None => domain = Some(current),
                _ => {}
            }
        }
        domain
    }

    /// Value provenance is separate from a public CAST target's type spelling.
    /// Only catalog JSON, exact JSON producers, and their preserving operators
    /// establish this fact. Derived/CTE columns carry it by ColumnId.
    pub(super) fn json_list_provenance(
        &self,
        source: Option<&ast::Expr>,
        expression: &TypedExpr,
        scope: &AnalyzerScope,
    ) -> bool {
        if let Some(ast::Expr::Cast(cast)) = source {
            let Some(ast::TypeNameArgument::Type(item)) = cast.data_type.arguments.first() else {
                return false;
            };
            let array_json = cast
                .data_type
                .name
                .parts
                .last()
                .is_some_and(|part| part.value.eq_ignore_ascii_case("array"))
                && item.name.parts.last().is_some_and(|part| {
                    matches!(part.value.to_ascii_lowercase().as_str(), "json" | "jsonb")
                });
            return array_json
                && matches!(&expression.kind,
                ExprKind::Cast { expr: inner, .. } if self.json_list_provenance(Some(&cast.expr), inner, scope));
        }
        let source_id = match source {
            Some(ast::Expr::Identifier(ident)) => scope.resolve(None, &ident.value).ok(),
            Some(ast::Expr::CompoundIdentifier(parts)) if parts.parts.len() >= 2 => {
                let n = parts.parts.len();
                scope
                    .resolve(Some(&parts.parts[n - 2].value), &parts.parts[n - 1].value)
                    .ok()
            }
            _ => None,
        };
        if let Some((column_id, _, _)) = source_id {
            return self.factory.borrow().has_json_list_provenance(column_id);
        }
        match &expression.kind {
            // Internal output adapters and materialized binding coercions do
            // not establish proof. The original source must establish it.
            ExprKind::Cast { expr: inner, .. } => self.json_list_provenance(source, inner, scope),
            ExprKind::ColumnRef { column_id, .. } => {
                self.factory.borrow().has_json_list_provenance(*column_id)
            }
            ExprKind::Nested(inner) => self.json_list_provenance(
                source.and_then(|source| match source {
                    ast::Expr::Nested(nested) => Some(nested.expression.as_ref()),
                    _ => None,
                }),
                inner,
                scope,
            ),
            ExprKind::FunctionCall { binding, args, .. }
                if binding.function_id.as_str() == "builtin.scalar/__array_literal/v1" =>
            {
                let Some(ast::Expr::Array(array)) = source else {
                    return false;
                };
                self.json_array_elements_provenance(array, args, scope)
            }
            ExprKind::FunctionCall { binding, args, .. }
                if binding.function_id.as_str() == "builtin.scalar/array_sortby/v1" =>
            {
                let Some(ast::Expr::FunctionCall(function)) = source else {
                    return false;
                };
                function
                    .arguments
                    .first()
                    .zip(args.first())
                    .is_some_and(|(source, value)| {
                        self.json_list_provenance(Some(source), value, scope)
                    })
            }
            ExprKind::AggregateCall { resolved, args, .. }
                if resolved.function_id.as_str() == "builtin.aggregate/array_agg/v1" =>
            {
                let Some(ast::Expr::FunctionCall(function)) = source else {
                    return false;
                };
                function
                    .arguments
                    .first()
                    .zip(args.first())
                    .is_some_and(|(source, value)| {
                        self.logical_output_type(Some(source), value, scope) == Some(SqlType::Json)
                    })
            }
            ExprKind::WindowCall {
                aggregate_binding: Some(binding),
                args,
                ..
            } if binding.function_id.as_str() == "builtin.aggregate/array_agg/v1" => {
                let Some(ast::Expr::FunctionCall(function)) = source else {
                    return false;
                };
                function
                    .arguments
                    .first()
                    .zip(args.first())
                    .is_some_and(|(source, value)| {
                        self.logical_output_type(Some(source), value, scope) == Some(SqlType::Json)
                    })
            }
            ExprKind::Case {
                when_then,
                else_expr,
                ..
            } => {
                let Some(ast::Expr::Case(case)) = source else {
                    return false;
                };
                let mut saw_json = false;
                for (source, value) in case
                    .results
                    .iter()
                    .zip(when_then.iter().map(|(_, value)| value))
                    .chain(case.else_result.as_deref().zip(else_expr.as_deref()))
                {
                    if matches!(
                        source,
                        ast::Expr::Literal(ast::Literal {
                            kind: ast::LiteralKind::Null,
                            ..
                        })
                    ) {
                        continue;
                    }
                    if !self.json_list_provenance(Some(source), value, scope) {
                        return false;
                    }
                    saw_json = true;
                }
                saw_json
            }
            _ => false,
        }
    }

    pub(super) fn json_array_elements_provenance(
        &self,
        array: &ast::ArrayExpr,
        args: &[TypedExpr],
        scope: &AnalyzerScope,
    ) -> bool {
        if array.element_type.as_ref().is_some_and(|target| {
            !target.name.parts.last().is_some_and(|part| {
                matches!(part.value.to_ascii_lowercase().as_str(), "json" | "jsonb")
            })
        }) {
            return false;
        }
        if array.elements.len() != args.len() {
            return false;
        }
        let mut saw_json = false;
        let all_json = array.elements.iter().zip(args).all(|(source, value)| {
            if matches!(
                source,
                ast::Expr::Literal(ast::Literal {
                    kind: ast::LiteralKind::Null,
                    ..
                })
            ) {
                return true;
            }
            let json = self.logical_output_type(Some(source), value, scope) == Some(SqlType::Json);
            saw_json |= json;
            json
        });
        all_json && (saw_json || array.element_type.is_some())
    }

    /// Keep scalar/aggregate bindings exact. The ordinary output CAST adapts
    /// the List item's semantic field metadata. An explicitly typed empty
    /// JSON literal also materializes its Utf8 item carrier without any values.
    pub(super) fn adapt_json_list_output(
        &self,
        expression: TypedExpr,
        json_input: bool,
        span: Span,
    ) -> Result<TypedExpr, AnalyzeError> {
        if !json_input {
            return Ok(expression);
        }
        let binding = match &expression.kind {
            ExprKind::AggregateCall { resolved, .. } => Some(resolved),
            ExprKind::FunctionCall { binding, .. } => Some(binding),
            ExprKind::WindowCall {
                aggregate_binding, ..
            } => aggregate_binding.as_ref(),
            _ => None,
        };
        if !binding.is_some_and(|binding| {
            matches!(
                binding.function_id.as_str(),
                "builtin.aggregate/array_agg/v1"
                    | "builtin.scalar/__array_literal/v1"
                    | "builtin.scalar/array_sortby/v1"
            )
        }) {
            return Ok(expression);
        }
        let DataType::List(item) = &expression.value_type.data_type else {
            return Err(AnalyzeError::type_mismatch(
                "JSON list producer binding must declare a List result",
                span,
            ));
        };
        let empty_json_literal = matches!(&expression.kind,
            ExprKind::FunctionCall { binding, args, .. }
                if binding.function_id.as_str() == "builtin.scalar/__array_literal/v1" && args.is_empty());
        if item.data_type() != &DataType::Utf8
            && !(empty_json_literal && item.data_type() == &DataType::Null)
        {
            return Err(AnalyzeError::type_mismatch(
                "JSON list elements must use their declared Utf8 physical carrier",
                span,
            ));
        }
        let target = DataType::List(std::sync::Arc::new(field_with_logical_type(
            Field::new(item.name(), DataType::Utf8, item.is_nullable()),
            LogicalType::Json,
        )));
        let mut value_type = expression.value_type.clone();
        value_type.data_type = target.clone();
        Ok(TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(expression),
                target: target.clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type,
        })
    }
}

// Compatibility checks consume a supplied logical authority; they never infer
// one from the physical carrier. Offset-width adaptations retain the same
// String/Binary/Json domain, while conversions to another family clear it.
fn logical_carrier_matches(domain: &SqlType, carrier: &DataType) -> bool {
    use arrow::datatypes::TimeUnit;
    match (domain, carrier) {
        (SqlType::String | SqlType::Json, DataType::Utf8 | DataType::LargeUtf8)
        | (
            SqlType::Binary
            | SqlType::Hll
            | SqlType::Bitmap
            | SqlType::Object
            | SqlType::Percentile,
            DataType::Binary | DataType::LargeBinary,
        )
        | (SqlType::Variant, DataType::LargeBinary)
        | (SqlType::TinyInt, DataType::Int8)
        | (SqlType::SmallInt, DataType::Int16)
        | (SqlType::Int, DataType::Int32)
        | (SqlType::BigInt, DataType::Int64)
        | (SqlType::LargeInt, DataType::FixedSizeBinary(16))
        | (SqlType::Float, DataType::Float32)
        | (SqlType::Double, DataType::Float64)
        | (SqlType::Boolean, DataType::Boolean)
        | (SqlType::Date, DataType::Date32)
        | (SqlType::Time, DataType::Time64(TimeUnit::Microsecond))
        | (SqlType::DateTime, DataType::Timestamp(TimeUnit::Microsecond, _))
        | (SqlType::DateTimeNs, DataType::Timestamp(TimeUnit::Nanosecond, _)) => true,
        (
            SqlType::Decimal { precision, scale },
            DataType::Decimal128(p, s) | DataType::Decimal256(p, s),
        ) => precision == p && scale == s,
        (
            SqlType::Array(item),
            DataType::List(field) | DataType::LargeList(field) | DataType::FixedSizeList(field, _),
        ) => logical_carrier_matches(item, field.data_type()),
        (SqlType::Map(key, value), DataType::Map(entries, _)) => match entries.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                logical_carrier_matches(key, fields[0].data_type())
                    && logical_carrier_matches(value, fields[1].data_type())
            }
            _ => false,
        },
        (SqlType::Struct(expected), DataType::Struct(actual)) => {
            expected.len() == actual.len()
                && expected.iter().zip(actual).all(|((name, domain), field)| {
                    name == field.name() && logical_carrier_matches(domain, field.data_type())
                })
        }
        _ => false,
    }
}
fn is_null_value(source: Option<&ast::Expr>, value: &TypedExpr) -> bool {
    if value.value_type.data_type == DataType::Null {
        return true;
    }
    match &value.kind {
        ExprKind::Literal(crate::analysis::LiteralValue::Null) => return true,
        ExprKind::Cast { expr, .. } | ExprKind::Nested(expr) if is_null_value(None, expr) => {
            return true;
        }
        _ => {}
    }
    matches!(
        source,
        Some(ast::Expr::Literal(ast::Literal {
            kind: ast::LiteralKind::Null,
            ..
        }))
    )
}

#[cfg(test)]
mod scalar_domain_tests {
    use super::*;
    use crate::catalog::{PlannerTableProvider, ResolvedAnalyzerTable};
    use crate::column_id::ColumnRefFactory;
    use crate::planner::table::{SqlScanKind, TableDef};
    use novarocks_types::schema::ColumnDef;
    use std::cell::{Cell, RefCell};
    use std::rc::Rc;
    use std::sync::Arc;

    struct Catalog;
    fn declared(name: &str, data_type: DataType, logical_type: SqlType) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type,
            nullable: true,
            write_default: None,
            logical_type: Some(logical_type),
        }
    }
    impl PlannerTableProvider for Catalog {
        fn resolve_table_for_analysis(
            &self,
            catalog: Option<&str>,
            database: &str,
            table: &str,
        ) -> Result<ResolvedAnalyzerTable, String> {
            let json_item = field_with_logical_type(
                Field::new("item", DataType::Utf8, true),
                LogicalType::Json,
            );
            let planner = TableDef {
                name: table.into(),
                columns: vec![
                    declared("j", DataType::Utf8, SqlType::Json),
                    declared("h", DataType::Binary, SqlType::Hll),
                    declared("b", DataType::Binary, SqlType::Bitmap),
                    declared("s", DataType::Utf8, SqlType::String),
                    declared("v", DataType::LargeBinary, SqlType::Variant),
                    declared("o", DataType::Binary, SqlType::Object),
                    declared("p", DataType::Binary, SqlType::Percentile),
                    declared(
                        "a",
                        DataType::List(Arc::new(json_item)),
                        SqlType::Array(Box::new(SqlType::Json)),
                    ),
                    declared(
                        "m",
                        DataType::Map(
                            Arc::new(Field::new(
                                "entries",
                                DataType::Struct(
                                    vec![
                                        Field::new("key", DataType::Utf8, true),
                                        Field::new("value", DataType::Binary, true),
                                    ]
                                    .into(),
                                ),
                                false,
                            )),
                            false,
                        ),
                        SqlType::Map(Box::new(SqlType::String), Box::new(SqlType::Hll)),
                    ),
                    declared(
                        "r",
                        DataType::Struct(
                            vec![
                                Field::new("json", DataType::Utf8, true),
                                Field::new("bitmap", DataType::Binary, true),
                            ]
                            .into(),
                        ),
                        SqlType::Struct(vec![
                            ("json".into(), SqlType::Json),
                            ("bitmap".into(), SqlType::Bitmap),
                        ]),
                    ),
                ],
                iceberg_row_lineage_metadata_columns: vec![],
                source: crate::compiler::mv_rewrite::test_scan_source_for(
                    "ice",
                    database,
                    table,
                    SqlScanKind::ConnectorRead,
                ),
            };
            Ok(ResolvedAnalyzerTable::from_planner(
                catalog, database, planner,
            ))
        }
    }
    fn query(sql: &str) -> ast::Query {
        let mut statements = novarocks_parser::parse(sql).unwrap();
        let [ast::Statement::Query(query)] = statements.as_mut_slice() else {
            panic!("expected query")
        };
        query.clone()
    }
    fn output_domains(sql: &str) -> Vec<Option<SqlType>> {
        let (query, _, factory) = super::super::analyze(&query(sql), &Catalog, "db").unwrap();
        query
            .output_columns
            .iter()
            .map(|column| factory.logical_type(column.column_id))
            .collect()
    }
    fn fixture_control() -> &'static crate::compiler::SqlCompileControl {
        static CONTROL: std::sync::OnceLock<crate::compiler::SqlCompileControl> =
            std::sync::OnceLock::new();
        CONTROL.get_or_init(crate::compiler::SqlCompileControl::unbounded)
    }
    fn context(factory: Rc<RefCell<ColumnRefFactory>>) -> AnalyzerContext<'static> {
        AnalyzerContext {
            constant_policy: crate::constant::test_constant_policy(),
            control: fixture_control(),
            catalog: &Catalog,
            current_database: "db",
            function_catalog: crate::functions::builtin_sql_function_catalog(),
            sql_semantics: Default::default(),
            factory,
            ctes: Default::default(),
            pending_ctes: Default::default(),
            next_subquery_id: Cell::new(0),
            next_lambda_slot_id: Cell::new(0),
            collected_subqueries: RefCell::new(Vec::new()),
            cte_registry: RefCell::new(Default::default()),
        }
    }

    #[test]
    fn m07_json_list_explicit_literal_target_keeps_its_source_decision() {
        assert_eq!(
            output_domains("select array<json>[],array<json>[null]"),
            vec![Some(SqlType::Array(Box::new(SqlType::Json))); 2],
        );
        assert_eq!(
            output_domains(
                "with q as (select array<json>[] as j) select array_sortby(j,[1]) from q"
            ),
            vec![Some(SqlType::Array(Box::new(SqlType::Json)))],
        );
        assert_eq!(
            output_domains("select array<varchar>[json_object('k',1)],array<json>['plain']"),
            vec![Some(SqlType::Array(Box::new(SqlType::String))); 2],
        );
        assert_eq!(
            output_domains("select cast(['plain'] as array<json>)"),
            vec![None],
        );
    }

    #[test]
    fn m07_json_list_missing_marker_requires_its_independent_source_witness() {
        for (marker, witness, accepted) in [
            (None, false, false),
            (None, true, true),
            (Some("json"), false, true),
            (Some("json"), true, true),
            (Some("unknown"), true, false),
            (Some("hll"), true, false),
        ] {
            let item = Field::new("item", DataType::Utf8, true);
            let item = match marker {
                Some(value) => item.with_metadata(
                    [(
                        novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_owned(),
                        value.to_owned(),
                    )]
                    .into(),
                ),
                None => item,
            };
            let carrier = DataType::List(Arc::new(item));
            let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
            let id = factory.borrow_mut().create(
                None,
                "source".into(),
                novarocks_type_contract::FunctionValueType::new(carrier.clone(), true),
            );
            factory
                .borrow_mut()
                .set_logical_type(id, Some(SqlType::Array(Box::new(SqlType::Json))));
            factory.borrow_mut().set_json_list_provenance(id, witness);
            let scope = AnalyzerScope::new(factory.clone());
            let context = context(factory);
            let source = TypedExpr {
                kind: ExprKind::ColumnRef {
                    column_id: id,
                    qualifier: None,
                    column: "source".into(),
                },
                value_type: novarocks_type_contract::FunctionValueType::new(carrier, true),
            };
            if witness && !accepted {
                // A binding coercion must not hide the original conflicting
                // item marker behind an ordinary List<Utf8> argument.
                let target = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
                let coerced = TypedExpr {
                    kind: ExprKind::Cast {
                        expr: Box::new(source.clone()),
                        target: target.clone(),
                        decimal_overflow_policy:
                            novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
                    },
                    value_type: novarocks_type_contract::FunctionValueType::new(target, true),
                };
                let bound = super::super::resolve_expr::resolved_scalar_call_at(
                    context.function_catalog,
                    "row",
                    vec![coerced],
                    Span::new(0, 0),
                    context.sql_semantics.sql_mode().decimal_overflow_policy(),
                    context.constant_policy,
                    context.control,
                )
                .unwrap();
                assert!(
                    context
                        .adapt_bound_output_domains(bound, None, &scope, Span::new(0, 0))
                        .is_err()
                );
            }
            let bound = super::super::resolve_expr::resolved_scalar_call_at(
                context.function_catalog,
                "row",
                vec![source],
                Span::new(0, 0),
                context.sql_semantics.sql_mode().decimal_overflow_policy(),
                context.constant_policy,
                context.control,
            )
            .unwrap();
            let result = context.adapt_bound_output_domains(bound, None, &scope, Span::new(0, 0));
            assert_eq!(
                result.is_ok(),
                accepted,
                "marker={marker:?} witness={witness}: {result:?}"
            );
            if accepted {
                let result = result.unwrap();
                let DataType::Struct(fields) = result.value_type.data_type else {
                    panic!("row result");
                };
                let DataType::List(item) = fields[0].data_type() else {
                    panic!("list result");
                };
                assert_eq!(
                    novarocks_types::logical::logical_type_of_field(item),
                    Some(LogicalType::Json)
                );
            }
        }
    }

    #[test]
    fn m07_scalar_catalog_domains_survive_projection_cte_and_union() {
        assert_eq!(
            output_domains("select j,h,b,s,v,a,m,r from t"),
            vec![
                Some(SqlType::Json),
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::String),
                Some(SqlType::Variant),
                Some(SqlType::Array(Box::new(SqlType::Json))),
                Some(SqlType::Map(
                    Box::new(SqlType::String),
                    Box::new(SqlType::Hll)
                )),
                Some(SqlType::Struct(vec![
                    ("json".into(), SqlType::Json),
                    ("bitmap".into(), SqlType::Bitmap),
                ])),
            ]
        );
        assert_eq!(
            output_domains("with q as (select h as x from t) select x from q"),
            vec![Some(SqlType::Hll)]
        );
        assert_eq!(
            output_domains("select h from t union all select h from t"),
            vec![Some(SqlType::Hll)]
        );
        assert_eq!(
            output_domains("select null union all select b from t"),
            vec![Some(SqlType::Bitmap)]
        );
        assert_eq!(
            output_domains("select h from t union all select b from t"),
            vec![None]
        );
    }

    #[test]
    fn m07_scalar_bound_conditionals_keep_only_homogeneous_returned_domains() {
        assert_eq!(
            output_domains(
                "select coalesce(j,null),ifnull(null,h),ifnull(b,null),if(true,h,null),case when true then h else null end from t"
            ),
            vec![
                Some(SqlType::Json),
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::Hll),
                Some(SqlType::Hll)
            ]
        );
        assert_eq!(
            output_domains(
                "select coalesce(v,v),ifnull(a,null),case when true then b else b end,coalesce(m,m),if(true,r,r) from t"
            ),
            vec![
                Some(SqlType::Variant),
                Some(SqlType::Array(Box::new(SqlType::Json))),
                Some(SqlType::Bitmap),
                Some(SqlType::Map(
                    Box::new(SqlType::String),
                    Box::new(SqlType::Hll)
                )),
                Some(SqlType::Struct(vec![
                    ("json".into(), SqlType::Json),
                    ("bitmap".into(), SqlType::Bitmap),
                ])),
            ]
        );
        assert_eq!(
            output_domains(
                "select coalesce(null,parse_json('null')),if(false,null,json_object('k',1)),case when true then null else parse_json('null') end"
            ),
            vec![Some(SqlType::Json); 3]
        );
        assert_eq!(
            output_domains("select coalesce(null,null),case when true then null else null end"),
            vec![None, None]
        );
    }

    #[test]
    fn m07_scalar_mixed_values_and_explicit_string_cast_never_guess_json_or_opaque() {
        assert_eq!(
            output_domains(
                "select coalesce(j,s),ifnull(j,'plain'),if(true,h,b),case when true then h else b end,cast(j as varchar),cast(h as varbinary),cast(s as json) from t"
            ),
            vec![None; 7]
        );
        assert_eq!(
            output_domains("select cast(j as json),coalesce(cast(j as json),null) from t"),
            vec![Some(SqlType::Json); 2]
        );
        // Explicit target spelling alone cannot make an arbitrary Utf8 a Json.
        assert_eq!(output_domains("select cast('plain' as json)"), vec![None]);
    }

    #[test]
    fn m07_scalar_nullif_comparator_does_not_supply_the_returned_domain() {
        assert_eq!(
            output_domains("select nullif(h,b),nullif(j,s),nullif(s,j),nullif(null,j) from t"),
            vec![
                Some(SqlType::Hll),
                Some(SqlType::Json),
                Some(SqlType::String),
                None
            ]
        );
    }

    #[test]
    fn m07_scalar_shadowed_spelling_cannot_use_builtin_value_preservation() {
        let (resolved, _, factory) =
            super::super::analyze(&query("select coalesce(j,null) from t"), &Catalog, "db")
                .unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("expected select")
        };
        let mut expression = select.projection[0].expr.clone();
        let ExprKind::FunctionCall { binding, name, .. } = &mut expression.kind else {
            panic!("expected bound function")
        };
        assert_eq!(name, "coalesce");
        // Keep the actual selected argument/result signature and surface name,
        // but replace the producer identity as an external catalog would.
        let mut shadowed = binding.resolved().clone();
        shadowed.function_id =
            novarocks_functions::FunctionId::try_new("test.shadow/coalesce/v1").unwrap();
        *binding =
            crate::binding::SqlFunctionBinding::new(shadowed, binding.decimal_overflow_policy());
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        assert_eq!(
            context(factory).logical_output_type(None, &expression, &scope),
            None
        );
    }

    #[test]
    fn m07_scalar_implicit_offset_adaptation_preserves_authority_but_family_change_clears_it() {
        let factory = Rc::new(RefCell::new(ColumnRefFactory::new()));
        let id = factory.borrow_mut().create(
            None,
            "j".into(),
            novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
        );
        factory
            .borrow_mut()
            .set_logical_type(id, Some(SqlType::Json));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        let column = TypedExpr {
            kind: ExprKind::ColumnRef {
                column_id: id,
                qualifier: None,
                column: "j".into(),
            },
            value_type: novarocks_type_contract::FunctionValueType::new(DataType::Utf8, true),
        };
        let cast = |target: DataType| TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(column.clone()),
                target: target.clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(target, true),
        };
        assert_eq!(
            context.logical_output_type(None, &cast(DataType::LargeUtf8), &scope),
            Some(SqlType::Json)
        );
        assert_eq!(
            context.logical_output_type(None, &cast(DataType::Binary), &scope),
            None
        );
        assert_eq!(
            context.logical_output_type(None, &cast(DataType::Utf8), &scope),
            Some(SqlType::Json)
        );
    }

    fn output_expressions(sql: &str) -> Vec<TypedExpr> {
        let (query, _, _) = super::super::analyze(&query(sql), &Catalog, "db").unwrap();
        let crate::analysis::QueryBody::Select(select) = query.body else {
            panic!("expected select")
        };
        select
            .projection
            .into_iter()
            .map(|item| item.expr)
            .collect()
    }

    fn original_window(expression: &TypedExpr) -> &TypedExpr {
        match &expression.kind {
            ExprKind::Cast { expr, .. } => original_window(expr),
            ExprKind::WindowCall { .. } => expression,
            _ => panic!("expected selected window"),
        }
    }

    #[test]
    fn m07_window_value_domains_follow_all_actual_value_suppliers() {
        assert_eq!(
            output_domains(
                "select first_value(j) over (),last_value(h) over (),first_value(b) over (),last_value(v) over (),first_value(o) over (),last_value(p) over (),lead(j,1,j) over (),lag(j,1,null) over () from t"
            ),
            vec![
                Some(SqlType::Json),
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::Variant),
                Some(SqlType::Object),
                Some(SqlType::Percentile),
                Some(SqlType::Json),
                Some(SqlType::Json)
            ]
        );
        assert_eq!(
            output_domains(
                "select lead(j,1,s) over (),lag(s,1,j) over (),lead(h,1,b) over (),lag(b,1,h) over (),lead(j,1,cast(null as varchar)) over (),first_value(cast(j as varchar)) over (),lag(cast(j as varchar),1,j) over () from t"
            ),
            vec![
                Some(SqlType::String),
                Some(SqlType::String),
                Some(SqlType::Binary),
                Some(SqlType::Binary),
                Some(SqlType::Json),
                Some(SqlType::String),
                Some(SqlType::String)
            ]
        );
        // The offset is a control operand, not a supplier of the result value.
        assert_eq!(
            output_domains("select lead(j,2) over (),lag(h,2) over () from t"),
            vec![Some(SqlType::Json), Some(SqlType::Hll)]
        );
    }

    #[test]
    fn m07_window_nested_value_identity_preserves_shape_and_null_siblings() {
        let sql = "with q as (select [to_bitmap(1)] as b) select first_value(b) over (),last_value(b) over (),lead(b,1,b) over (),lag(b,1,null) over () from q";
        assert_eq!(
            output_domains(sql),
            vec![Some(SqlType::Array(Box::new(SqlType::Bitmap))); 4]
        );
        for expression in output_expressions(sql) {
            let selected = original_window(&expression);
            let ExprKind::WindowCall {
                binding,
                args,
                aggregate_binding,
                ..
            } = &selected.kind
            else {
                unreachable!()
            };
            assert!(aggregate_binding.is_none());
            assert_eq!(binding.kind, novarocks_functions::FunctionKind::Window);
            let novarocks_functions::FunctionResultType::Scalar(result) =
                &binding.selected.result_type
            else {
                panic!("scalar selection")
            };
            assert_eq!(result.data_type, selected.value_type.data_type);
            let DataType::List(output) = &expression.value_type.data_type else {
                panic!("list output")
            };
            let DataType::List(input) = &args[0].value_type.data_type else {
                panic!("list input")
            };
            assert_eq!(output.name(), input.name());
            assert_eq!(output.is_nullable(), input.is_nullable());
            assert_eq!(output.data_type(), input.data_type());
            assert_eq!(
                novarocks_types::logical::logical_type_of_field(output),
                Some(LogicalType::Bitmap)
            );
        }
        let expression =
            output_expressions("select first_value(row(null,to_bitmap(1))) over ()").remove(0);
        let DataType::Struct(fields) = &expression.value_type.data_type else {
            panic!("struct output")
        };
        assert_eq!(fields[0].data_type(), &DataType::Null);
        assert_eq!(fields[0].name(), "col1");
        assert_eq!(fields[1].name(), "col2");
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(&fields[1]),
            Some(LogicalType::Bitmap)
        );
        assert_eq!(
            output_domains("select first_value(row(null,to_bitmap(1))) over ()"),
            vec![None]
        );
        // Current default-argument admission rejects these different nested
        // carriers before binding. Domain forwarding must not widen that owner.
        let mixed = "with q as (select [parse_json('{}')] as j,['plain'] as s) select lead(j,1,s) over () from q";
        let error = super::super::analyze(&query(mixed), &Catalog, "db").unwrap_err();
        assert!(error.to_string().contains("third parameter"), "{error}");
    }

    #[test]
    fn m07_window_variant_typed_null_defaults_are_neutral_without_admitting_unproven_values() {
        assert_eq!(
            output_domains(
                "select lead(v,1,cast(null as variant)) over (),lag(v,1,cast(null as variant)) over () from t"
            ),
            vec![Some(SqlType::Variant); 2]
        );
        // Same-domain public CAST is an actual LargeBinary identity operation,
        // not an unproven payload. Both value and default contributions retain
        // the independently established catalog Variant fact.
        assert_eq!(
            output_domains(
                "select lead(v,1,cast(v as variant)) over (),lag(v,1,cast(v as variant)) over (),first_value(cast(v as variant)) over () from t"
            ),
            vec![Some(SqlType::Variant); 3]
        );
        for sql in [
            "select lead(v,1,cast(s as variant)) over () from t",
            "select lag(v,1,cast(s as variant)) over () from t",
            "select first_value(cast(X'AB01' as variant)) over ()",
            "select first_value(cast(cast(v as varbinary) as variant)) over () from t",
        ] {
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("LargeBinary requires its original logical identity"),
                "{error}"
            );
        }
        // A NULL default cannot erase a known sibling in a partial Struct or
        // a nested Map value. SqlType has no complete Null-bearing counterpart.
        let sql = "with q as (select row(null,map('k',to_bitmap(1))) as r) select lead(r,1,null) over (),lag(r,1,null) over () from q";
        for expression in output_expressions(sql) {
            let ExprKind::WindowCall { args, .. } = &original_window(&expression).kind else {
                unreachable!()
            };
            let DataType::Struct(fields) = &expression.value_type.data_type else {
                panic!("struct result")
            };
            assert_eq!(fields[0].data_type(), &DataType::Null);
            let DataType::Map(entries, sorted) = fields[1].data_type() else {
                panic!("map sibling")
            };
            let DataType::Struct(input) = &args[0].value_type.data_type else {
                panic!("struct input")
            };
            let DataType::Map(input_entries, input_sorted) = input[1].data_type() else {
                panic!("map input")
            };
            assert_eq!(sorted, input_sorted);
            assert_eq!(entries.name(), input_entries.name());
            assert_eq!(entries.is_nullable(), input_entries.is_nullable());
            let DataType::Struct(map_fields) = entries.data_type() else {
                panic!("map entries")
            };
            let DataType::Struct(input_fields) = input_entries.data_type() else {
                panic!("input entries")
            };
            assert_eq!(map_fields.len(), 2);
            for (actual, input) in map_fields.iter().zip(input_fields) {
                assert_eq!(actual.name(), input.name());
                assert_eq!(actual.is_nullable(), input.is_nullable());
                assert_eq!(actual.data_type(), input.data_type());
            }
            assert_eq!(
                novarocks_types::logical::logical_type_of_field(&map_fields[1]),
                Some(LogicalType::Bitmap)
            );
        }
        assert_eq!(output_domains(sql), vec![None; 2]);
    }

    #[test]
    fn m07_variant_public_identity_cast_requires_independent_exact_source_fact() {
        assert_eq!(
            output_domains(
                "select cast(v as variant),cast(cast(v as variant) as variant),cast((v) as variant) from t"
            ),
            vec![Some(SqlType::Variant); 3]
        );
        assert_eq!(
            output_domains(
                "select cast(s as variant),cast(j as variant),cast(X'AB01' as variant),cast(cast(v as varbinary) as variant),cast(v as varbinary) from t"
            ),
            vec![None; 5]
        );
        assert_eq!(
            output_domains(
                "select first_value(cast(variant_get(parse_json('{\"a\":1}'),'$.a') as variant)) over (),last_value(cast(variant_get(parse_json('{\"a\":1}'),'$.a') as variant)) over ()"
            ),
            vec![Some(SqlType::Variant); 2]
        );
        let sql = "select first_value(cast(variant_get(parse_json('{\"a\":1}'),'$.a') as variant)) over ()";
        let expression = output_expressions(sql).remove(0);
        let selected = original_window(&expression);
        let ExprKind::WindowCall { binding, args, .. } = &selected.kind else {
            panic!("window")
        };
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("scalar")
        };
        assert_eq!(result.data_type, DataType::LargeBinary);
        assert_eq!(selected.value_type.data_type, result.data_type);
        let ExprKind::Cast { expr, target, .. } = &args[0].kind else {
            panic!("public CAST")
        };
        assert_eq!(target, &DataType::LargeBinary);
        assert_eq!(expr.value_type.data_type, DataType::LargeBinary);
        let ExprKind::FunctionCall { binding, .. } = &expr.kind else {
            panic!("original producer")
        };
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/variant_get/v1"
        );
    }

    #[test]
    fn m07_window_value_domains_require_exact_selected_binding_and_carrier() {
        let (resolved, _, factory) = super::super::analyze(
            &query("select first_value(parse_json('{}')) over ()"),
            &Catalog,
            "db",
        )
        .unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("select")
        };
        let original = original_window(&select.projection[0].expr).clone();
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        assert_eq!(
            context.logical_output_type(None, &original, &scope),
            Some(SqlType::Json)
        );
        for change in 0..3 {
            let mut candidate = original.clone();
            let ExprKind::WindowCall { binding, .. } = &mut candidate.kind else {
                unreachable!()
            };
            let mut selected = binding.resolved().clone();
            match change {
                0 => {
                    selected.function_id =
                        novarocks_functions::FunctionId::try_new("test.shadow/first_value/v1")
                            .unwrap()
                }
                1 => selected.kind = novarocks_functions::FunctionKind::Scalar,
                _ => {
                    let novarocks_functions::FunctionResultType::Scalar(result) =
                        &mut selected.selected.result_type
                    else {
                        unreachable!()
                    };
                    result.data_type = DataType::Binary;
                }
            }
            *binding = crate::binding::SqlFunctionBinding::new(
                selected,
                binding.decimal_overflow_policy(),
            );
            assert_eq!(context.logical_output_type(None, &candidate, &scope), None);
            let before = format!("{:?}", candidate.kind);
            let adapted = context
                .adapt_bound_output_domains(candidate, None, &scope, Span::new(0, 0))
                .unwrap();
            assert_eq!(format!("{:?}", adapted.kind), before);
        }
        let ExprKind::WindowCall { binding, args, .. } = &original.kind else {
            unreachable!()
        };
        let selection = binding.resolved().clone();
        let arguments = format!("{args:?}");
        let adapted = context
            .adapt_bound_output_domains(original.clone(), None, &scope, Span::new(0, 0))
            .unwrap();
        let ExprKind::WindowCall { binding, args, .. } = &original_window(&adapted).kind else {
            unreachable!()
        };
        assert_eq!(binding.resolved(), &selection);
        assert_eq!(format!("{args:?}"), arguments);
    }

    #[test]
    fn m07_window_input_coercion_cannot_wash_original_nested_markers() {
        for sql in [
            "select first_value(m) over () from t",
            "select last_value(r) over () from t",
        ] {
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert!(
                error.to_string().contains("logical identity differs"),
                "{error}"
            );
        }
        let (resolved, _, factory) = super::super::analyze(
            &query("select first_value([to_bitmap(1)]) over ()"),
            &Catalog,
            "db",
        )
        .unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("select")
        };
        let mut original = original_window(&select.projection[0].expr).clone();
        let ExprKind::WindowCall { args, .. } = &mut original.kind else {
            unreachable!()
        };
        let clean = args[0].clone();
        let corrupted = DataType::List(Arc::new(
            Field::new("item", DataType::Binary, true).with_metadata(
                [(
                    novarocks_types::logical::NR_LOGICAL_TYPE_KEY.to_owned(),
                    "unknown".to_owned(),
                )]
                .into(),
            ),
        ));
        let bad_source = TypedExpr {
            kind: clean.kind.clone(),
            value_type: novarocks_type_contract::FunctionValueType::new(
                corrupted,
                clean.value_type.nullable,
            ),
        };
        args[0] = TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(bad_source),
                target: clean.value_type.data_type.clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                clean.value_type.data_type,
                clean.value_type.nullable,
            ),
        };
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        assert!(
            context
                .adapt_bound_output_domains(original, None, &scope, Span::new(0, 0))
                .unwrap_err()
                .to_string()
                .contains("unknown or incompatible logical marker")
        );
    }

    #[test]
    fn m07_window_partial_null_value_keeps_known_origin_through_internal_cast() {
        let (resolved, _, factory) = super::super::analyze(
            &query("select first_value(row(null,to_bitmap(1))) over ()"),
            &Catalog,
            "db",
        )
        .unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("select")
        };
        let mut selected_window = original_window(&select.projection[0].expr).clone();
        let ExprKind::WindowCall { args, binding, .. } = &mut selected_window.kind else {
            unreachable!()
        };
        let value = args[0].clone();
        let DataType::Struct(fields) = &value.value_type.data_type else {
            panic!("struct")
        };
        let clean = DataType::Struct(
            fields
                .iter()
                .map(|field| {
                    Arc::new(Field::new(
                        field.name(),
                        field.data_type().clone(),
                        field.is_nullable(),
                    ))
                })
                .collect::<Vec<_>>()
                .into(),
        );
        let coerced = TypedExpr {
            kind: ExprKind::Cast {
                expr: Box::new(value),
                target: clean.clone(),
                decimal_overflow_policy: novarocks_type_contract::DecimalOverflowPolicy::OutputNull,
            },
            value_type: novarocks_type_contract::FunctionValueType::new(
                clean.clone(),
                args[0].value_type.nullable,
            ),
        };
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        // Resolve the real installed Window overload for the executable
        // coerced argument. The provenance adapter must retain this selection.
        let original_binding = context
            .function_catalog
            .resolve_window_binding(
                "first_value",
                &[crate::analysis::function_argument(
                    &coerced,
                    context.constant_policy,
                    context.control,
                )
                .unwrap()],
                context.control,
            )
            .unwrap();
        *binding = crate::binding::SqlFunctionBinding::new(
            original_binding.clone(),
            binding.decimal_overflow_policy(),
        );
        *args = vec![coerced];
        selected_window.value_type.data_type = clean;
        let adapted = context
            .adapt_bound_output_domains(selected_window, None, &scope, Span::new(0, 0))
            .unwrap();
        let DataType::Struct(fields) = &adapted.value_type.data_type else {
            panic!("struct")
        };
        assert_eq!(fields[0].data_type(), &DataType::Null);
        assert_eq!(fields[1].name(), "col2");
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(&fields[1]),
            Some(LogicalType::Bitmap)
        );
        let ExprKind::WindowCall { binding, args, .. } = &original_window(&adapted).kind else {
            unreachable!()
        };
        assert_eq!(binding.resolved(), &original_binding);
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("scalar")
        };
        assert_eq!(result.data_type, args[0].value_type.data_type);
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(match &result.data_type {
                DataType::Struct(fields) => &fields[1],
                _ => panic!("selected struct"),
            }),
            None
        );
    }

    #[test]
    fn m07_window_final_compiler_scalar_proof_matches_the_frozen_output() {
        use crate::compiler::{
            DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl,
            SqlCompileIntent, SqlCompileProgress, SqlCompiler, SqlFinalPlanCompileRequest,
            SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
            builtin_sql_function_catalog, noop_constant_evaluator,
        };
        use novarocks_physical_plan::{
            MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId,
            ResultValueDomain as D, ScanReadBudget,
        };
        use novarocks_result_contract::{ScalarOpaqueType as O, ScalarValueType as V};
        for (sql, domain, scalar) in [
            (
                "select first_value(parse_json('{}')) over ()",
                D::Json,
                V::Json,
            ),
            (
                "select last_value(to_bitmap(1)) over ()",
                D::Bitmap,
                V::Opaque(O::Bitmap),
            ),
            (
                "select lead(parse_json('{}'),1,null) over ()",
                D::Json,
                V::Json,
            ),
            (
                "select lag(parse_json('{}'),1,'plain') over ()",
                D::Plain,
                V::String,
            ),
            (
                "select lead('plain',1,parse_json('{}')) over ()",
                D::Plain,
                V::String,
            ),
            (
                "select first_value(cast(variant_get(parse_json('{\"a\":1}'),'$.a') as variant)) over ()",
                D::Variant,
                V::Variant,
            ),
            (
                "with q as (select variant_get(parse_json('{\"a\":1}'),'$.a') as v) select lead(v,1,cast(v as variant)) over () from q",
                D::Variant,
                V::Variant,
            ),
            (
                "with q as (select variant_get(parse_json('{\"a\":1}'),'$.a') as v) select lag(v,1,cast(v as variant)) over () from q",
                D::Variant,
                V::Variant,
            ),
        ] {
            let request = SqlFinalPlanCompileRequest::new(
                PlanVersionId::try_new([17; 16]).unwrap(),
                SqlStatementInput::sql(sql),
                SqlCompileIntent::Query,
                SqlSessionContext {
                    sql_semantics: Default::default(),
                    current_catalog: Some("iceberg".to_owned()),
                    current_database: "db".to_owned(),
                    optimizer_settings: SessionOptimizerSettings::default(),
                },
                SqlPlanningEnvironment::Distributed,
                builtin_sql_function_catalog().snapshot(),
                noop_constant_evaluator(),
                crate::constant::test_constant_policy(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                SqlCompileControl::unbounded(),
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                ScanReadBudget {
                    max_batch_rows: MAX_SCAN_BATCH_ROWS,
                    max_batch_bytes: MAX_SCAN_BATCH_BYTES,
                },
                DEFAULT_COMPLETION_LIMITS,
            );
            let SqlCompileProgress::Complete(completed) =
                SqlCompiler::start(request.try_into_completion().unwrap(), fixture_control())
                    .unwrap()
            else {
                panic!("source-free window unexpectedly needs observations: {sql}");
            };
            let result = completed.plan().result_port().unwrap();
            assert_eq!(result.fields[0].domain, domain, "{sql}");
            let schema = completed.scalar_schema().unwrap();
            assert_eq!(schema.field().value_type, scalar, "{sql}");
            assert_eq!(
                schema.field().nullable,
                result.fields[0].ty.nullable,
                "{sql}"
            );
            assert_eq!(schema.source_slot(), None);
            novarocks_physical_plan::validate_plan(completed.plan()).unwrap();
        }
    }

    #[test]
    fn m07_projection_occurrences_retain_exact_binary_producer_witnesses() {
        for (sql, expected) in [
            (
                "select percentile_hash(cast(1 as double)),to_bitmap(1),hll_hash('x')",
                vec![
                    Some(SqlType::Percentile),
                    Some(SqlType::Bitmap),
                    Some(SqlType::Hll),
                ],
            ),
            (
                "select percentile_hash(cast(1 as double)) as p,to_bitmap(1) as b,hll_hash('x') as h",
                vec![
                    Some(SqlType::Percentile),
                    Some(SqlType::Bitmap),
                    Some(SqlType::Hll),
                ],
            ),
            (
                "with q as (select percentile_hash(cast(1 as double)) as p) select p,p as again from q",
                vec![Some(SqlType::Percentile); 2],
            ),
            (
                "select to_bitmap(1) as b,b as again,bitmap_to_binary(b) as raw",
                vec![Some(SqlType::Bitmap), Some(SqlType::Bitmap), None],
            ),
            (
                "with q as (select percentile_hash(cast(1 as double)) as p) select cast(p as varbinary) as raw,p from q",
                vec![None, Some(SqlType::Percentile)],
            ),
        ] {
            let (query, _, factory) = super::super::analyze(&query(sql), &Catalog, "db").unwrap();
            assert_eq!(
                query
                    .output_columns
                    .iter()
                    .map(|column| factory.borrowed_logical_type(column.column_id).cloned())
                    .collect::<Vec<_>>(),
                expected,
                "{sql}",
            );
            // Native V1's selected Binary signatures retain their exact type
            // contracts; the root occurrence carries the existing source proof.
            assert!(
                query
                    .output_columns
                    .iter()
                    .all(|column| column.value_type.logical_type
                        == novarocks_type_contract::ValueLogicalType::Physical),
                "{sql}"
            );
        }
    }

    #[test]
    fn m07_scalar_exact_aggregate_and_window_producers_forward_domains() {
        assert_eq!(
            output_domains(
                "select bitmap_union(to_bitmap(1)),hll_union(hll_hash('x')),percentile_union(percentile_hash(1.0)),any_value(to_bitmap(1))"
            ),
            vec![
                Some(SqlType::Bitmap),
                Some(SqlType::Hll),
                Some(SqlType::Percentile),
                Some(SqlType::Bitmap)
            ]
        );
        assert_eq!(
            output_domains("select bitmap_union(to_bitmap(1)) over ()"),
            vec![Some(SqlType::Bitmap)]
        );
        assert_eq!(
            output_domains(
                "select array_agg(to_bitmap(1)),map_agg(cast(1 as bigint),hll_hash('x'))"
            ),
            vec![
                Some(SqlType::Array(Box::new(SqlType::Bitmap))),
                Some(SqlType::Map(
                    Box::new(SqlType::BigInt),
                    Box::new(SqlType::Hll)
                ))
            ]
        );
    }

    #[test]
    fn m07_scalar_nested_wrappers_and_selectors_keep_exact_value_origins() {
        assert_eq!(
            output_domains(
                "select element_at([to_bitmap(1),null],1),element_at(map(1,hll_hash('x')),1),element_at(map_values(map(1,percentile_hash(1.0))),1),named_struct('value',to_bitmap(1)).value"
            ),
            vec![
                Some(SqlType::Bitmap),
                Some(SqlType::Hll),
                Some(SqlType::Percentile),
                Some(SqlType::Bitmap)
            ]
        );
        assert_eq!(
            output_domains(
                "select element_at(array_slice([hll_hash('x')],1),1),element_at(array_sortby([to_bitmap(1)],[1]),1),element_at(map_from_arrays([cast(1 as bigint)],[percentile_hash(1.0)]),1)"
            ),
            vec![
                Some(SqlType::Hll),
                Some(SqlType::Bitmap),
                Some(SqlType::Percentile)
            ]
        );
        assert_eq!(
            output_domains("select element_at([o],1),element_at([p],1),element_at([v],1) from t"),
            vec![
                Some(SqlType::Object),
                Some(SqlType::Percentile),
                Some(SqlType::Variant)
            ]
        );
    }

    #[test]
    fn m07_scalar_null_companion_retains_actual_null_and_independent_marker() {
        let expressions = output_expressions(
            "select row(null,to_bitmap(1)),named_struct('missing',null,'value',percentile_hash(1.0))",
        );
        for (expression, marker, names) in [
            (&expressions[0], LogicalType::Bitmap, ["col1", "col2"]),
            (
                &expressions[1],
                LogicalType::Percentile,
                ["missing", "value"],
            ),
        ] {
            let DataType::Struct(fields) = &expression.value_type.data_type else {
                panic!("expected struct")
            };
            assert_eq!(fields[0].data_type(), &DataType::Null);
            assert_eq!(fields[0].name(), names[0]);
            assert_eq!(fields[1].name(), names[1]);
            assert_eq!(
                novarocks_types::logical::logical_type_of_field(&fields[1]),
                Some(marker)
            );
        }
        // SqlType has no Null member. The exact actual fields carry this proof,
        // rather than a fabricated all-members SqlType or a plain opaque value.
        assert_eq!(output_domains("select row(null,to_bitmap(1))"), vec![None]);
        assert_eq!(
            output_domains(
                "select named_struct('missing',null,'value',percentile_hash(1.0)).value"
            ),
            vec![Some(SqlType::Percentile)]
        );
    }

    #[test]
    fn m07_scalar_wrapper_output_adapter_preserves_selected_overload_and_shape() {
        let expression = output_expressions("select [to_bitmap(1),null]").remove(0);
        let ExprKind::Cast { expr, target, .. } = &expression.kind else {
            panic!("expected explicit output adapter")
        };
        let ExprKind::FunctionCall { binding, .. } = &expr.kind else {
            panic!("expected original bound function")
        };
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/__array_literal/v1"
        );
        let novarocks_functions::FunctionResultType::Scalar(result) = &binding.selected.result_type
        else {
            panic!("expected scalar result")
        };
        assert_eq!(result.data_type, expr.value_type.data_type);
        let (DataType::List(actual), DataType::List(adapted)) =
            (&expr.value_type.data_type, target)
        else {
            panic!("expected lists")
        };
        assert_eq!(actual.name(), adapted.name());
        assert_eq!(actual.is_nullable(), adapted.is_nullable());
        assert_eq!(actual.data_type(), adapted.data_type());
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(actual),
            None
        );
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(adapted),
            Some(LogicalType::Bitmap)
        );
    }

    #[test]
    fn m07_scalar_wrapper_mixed_domains_clear_and_missing_source_markers_refuse() {
        assert_eq!(
            output_domains(
                "select element_at([to_bitmap(1),hll_hash('x')],1),element_at([parse_json('{}'),'plain'],1)"
            ),
            vec![Some(SqlType::Binary), Some(SqlType::String)]
        );
        // These trusted catalog facts deliberately lack their declared nested
        // marker in Catalog. A wrapper must not launder them by adding a marker.
        for sql in ["select map_values(m) from t", "select row(r) from t"] {
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert!(
                error.to_string().contains("logical identity differs"),
                "{error}"
            );
        }
        assert_eq!(
            output_domains("select cast(element_at([to_bitmap(1)],1) as varbinary)"),
            vec![None]
        );
    }

    #[test]
    fn m07_scalar_shadowed_wrapper_binding_cannot_project_child_domain() {
        let (resolved, _, factory) =
            super::super::analyze(&query("select [to_bitmap(1)]"), &Catalog, "db").unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("expected select")
        };
        let ExprKind::Cast { expr, .. } = select.projection[0].expr.clone().kind else {
            panic!("expected adapter")
        };
        let mut original = *expr;
        let ExprKind::FunctionCall { binding, .. } = &mut original.kind else {
            panic!("expected original binding")
        };
        let mut shadowed = binding.resolved().clone();
        shadowed.function_id =
            novarocks_functions::FunctionId::try_new("test.shadow/__array_literal/v1").unwrap();
        *binding =
            crate::binding::SqlFunctionBinding::new(shadowed, binding.decimal_overflow_policy());
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        let adapted = context
            .adapt_bound_output_domains(original, None, &scope, Span::new(0, 0))
            .unwrap();
        let DataType::List(field) = &adapted.value_type.data_type else {
            panic!("expected list")
        };
        assert_eq!(novarocks_types::logical::logical_type_of_field(field), None);
        assert_eq!(context.logical_output_type(None, &adapted, &scope), None);
    }

    fn original_transform(expression: &TypedExpr) -> &TypedExpr {
        let mut original = expression;
        while let ExprKind::Cast { expr, .. } = &original.kind {
            original = expr;
        }
        original
    }

    #[test]
    fn m07_container_transforms_forward_exact_leaf_domains() {
        assert_eq!(
            output_domains(
                "select element_at(array_flatten([[to_bitmap(1)]]),1),element_at(array_repeat(parse_json('{}'),2),1),element_at(__array_struct_subfield([named_struct('payload',to_bitmap(1))],'PAYLOAD'),1)"
            ),
            vec![
                Some(SqlType::Bitmap),
                Some(SqlType::Json),
                Some(SqlType::Bitmap)
            ]
        );
        let expressions = output_expressions(
            "select array_flatten([[parse_json('{}')]]),array_repeat(to_bitmap(1),2),__array_struct_subfield([named_struct('payload',parse_json('{}'))],'PAYLOAD')",
        );
        for (expression, id, marker) in [
            (
                &expressions[0],
                "builtin.scalar/array_flatten/v1",
                LogicalType::Json,
            ),
            (
                &expressions[1],
                "builtin.scalar/array_repeat/v1",
                LogicalType::Bitmap,
            ),
            (
                &expressions[2],
                "builtin.scalar/__array_struct_subfield/v1",
                LogicalType::Json,
            ),
        ] {
            let ExprKind::FunctionCall { binding, .. } = &original_transform(expression).kind
            else {
                panic!("selected call");
            };
            assert_eq!(binding.function_id.as_str(), id);
            let novarocks_functions::FunctionResultType::Scalar(selected) =
                &binding.selected.result_type
            else {
                panic!("scalar selection");
            };
            assert_eq!(
                &selected.data_type,
                &original_transform(expression).value_type.data_type
            );
            let (DataType::List(actual), DataType::List(selected)) =
                (&expression.value_type.data_type, &selected.data_type)
            else {
                panic!("List result");
            };
            assert_eq!(actual.name(), selected.name());
            assert_eq!(actual.is_nullable(), selected.is_nullable());
            assert_eq!(
                novarocks_types::logical::logical_type_of_field(actual),
                Some(marker)
            );
        }
    }

    #[test]
    fn m07_container_zip_preserves_selected_names_nullability_and_partial_null() {
        assert_eq!(
            output_domains("select arrays_zip([to_bitmap(1)],[parse_json('{}')])"),
            vec![Some(SqlType::Array(Box::new(SqlType::Struct(vec![
                ("col1".to_owned(), SqlType::Bitmap),
                ("col2".to_owned(), SqlType::Json),
            ]))))]
        );
        let sql = "select arrays_zip([row(null,to_bitmap(1))],null)";
        assert_eq!(output_domains(sql), vec![None]);
        let expression = output_expressions(sql).remove(0);
        let ExprKind::FunctionCall { binding, .. } = &original_transform(&expression).kind else {
            panic!("zip call");
        };
        let novarocks_functions::FunctionResultType::Scalar(selected) =
            &binding.selected.result_type
        else {
            panic!("scalar");
        };
        let (DataType::List(item), DataType::List(selected_item)) =
            (&expression.value_type.data_type, &selected.data_type)
        else {
            panic!("List");
        };
        assert_eq!(item.name(), selected_item.name());
        assert_eq!(item.is_nullable(), selected_item.is_nullable());
        let (DataType::Struct(fields), DataType::Struct(selected_fields)) =
            (item.data_type(), selected_item.data_type())
        else {
            panic!("Struct");
        };
        assert_eq!(fields.len(), 2);
        for (i, (field, selected)) in fields.iter().zip(selected_fields).enumerate() {
            assert_eq!(field.name(), &format!("col{}", i + 1));
            assert_eq!(field.name(), selected.name());
            assert_eq!(field.is_nullable(), selected.is_nullable());
            assert!(field.is_nullable());
        }
        assert_eq!(fields[1].data_type(), &DataType::Null);
        let DataType::Struct(children) = fields[0].data_type() else {
            panic!("partial source");
        };
        assert_eq!(children[0].data_type(), &DataType::Null);
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(&children[1]),
            Some(LogicalType::Bitmap)
        );
    }

    #[test]
    fn m07_container_transforms_preserve_null_siblings_and_ordinary_values() {
        for sql in [
            "select array_flatten([[row(null,to_bitmap(1))]])",
            "select array_repeat(row(null,to_bitmap(1)),2)",
            "select __array_struct_subfield([named_struct('v',row(null,to_bitmap(1)))],'v')",
        ] {
            assert_eq!(output_domains(sql), vec![None]);
            let expression = output_expressions(sql).remove(0);
            let DataType::List(item) = &expression.value_type.data_type else {
                panic!("List");
            };
            let DataType::Struct(fields) = item.data_type() else {
                panic!("Struct");
            };
            assert_eq!(fields[0].data_type(), &DataType::Null);
            assert_eq!(
                novarocks_types::logical::logical_type_of_field(&fields[1]),
                Some(LogicalType::Bitmap)
            );
        }
        for sql in [
            "select array_flatten([[1,2]])",
            "select array_repeat('plain',2)",
            "select arrays_zip([1],[2])",
        ] {
            let expression = output_expressions(sql).remove(0);
            let DataType::List(item) = &expression.value_type.data_type else {
                panic!("ordinary List");
            };
            assert_eq!(novarocks_types::logical::logical_type_of_field(item), None);
        }
        let expression = output_expressions("select array_repeat(null,2)").remove(0);
        let DataType::List(item) = &expression.value_type.data_type else {
            panic!("NULL List");
        };
        assert_eq!(item.data_type(), &DataType::Null);
    }

    #[test]
    fn m07_container_trusted_and_null_variant_preserve_identity_unknown_cast_is_rejected() {
        let expression =
            output_expressions("select array_repeat(cast(null as variant),2)").remove(0);
        let DataType::List(item) = &expression.value_type.data_type else {
            panic!("typed NULL List");
        };
        assert_eq!(item.data_type(), &DataType::LargeBinary);
        assert_eq!(novarocks_types::logical::logical_type_of_field(item), None);
        // Same-carrier CAST preserves an independently established Variant
        // identity. Target spelling alone does not establish serialized bytes.
        assert_eq!(
            output_domains(
                "select array_repeat(cast(v as variant),2),element_at(array_repeat(cast(v as variant),2),1) from t"
            ),
            vec![
                Some(SqlType::Array(Box::new(SqlType::Variant))),
                Some(SqlType::Variant)
            ],
        );
        for sql in ["select array_repeat(cast('x' as variant),2)"] {
            let error = super::super::analyze(&query(sql), &Catalog, "db").unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("LargeBinary requires its original logical identity"),
                "{error}"
            );
        }
    }

    #[test]
    fn m07_container_transform_requires_selected_identity_kind_and_result() {
        let (resolved, _, factory) = super::super::analyze(
            &query("select array_repeat(to_bitmap(1),2)"),
            &Catalog,
            "db",
        )
        .unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("select");
        };
        let original = original_transform(&select.projection[0].expr).clone();
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        let ExprKind::FunctionCall { binding, args, .. } = &original.kind else {
            panic!("call");
        };
        let selection = binding.resolved().clone();
        let before_args = format!("{args:?}");
        let adapted = context
            .adapt_bound_output_domains(original.clone(), None, &scope, Span::new(0, 0))
            .unwrap();
        let ExprKind::FunctionCall { binding, args, .. } = &original_transform(&adapted).kind
        else {
            panic!("call");
        };
        assert_eq!(binding.resolved(), &selection);
        assert_eq!(format!("{args:?}"), before_args);
        for change in 0..6 {
            let mut candidate = original.clone();
            let ExprKind::FunctionCall { binding, .. } = &mut candidate.kind else {
                unreachable!();
            };
            let mut selected = binding.resolved().clone();
            match change {
                0 => {
                    selected.function_id =
                        novarocks_functions::FunctionId::try_new("test.shadow/array_repeat/v1")
                            .unwrap()
                }
                1 => selected.kind = novarocks_functions::FunctionKind::Aggregate,
                2 => {
                    let novarocks_functions::FunctionResultType::Scalar(result) =
                        &mut selected.selected.result_type
                    else {
                        unreachable!();
                    };
                    result.data_type =
                        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
                }
                3 => selected.logical_argument_count = 1,
                4 => {
                    let novarocks_functions::FunctionArgumentType::Value(value) =
                        &mut selected.selected.argument_types[0]
                    else {
                        unreachable!();
                    };
                    value.data_type = DataType::Utf8;
                }
                _ => {
                    let novarocks_functions::FunctionResultType::Scalar(result) =
                        &mut selected.selected.result_type
                    else {
                        unreachable!();
                    };
                    result.nullable = !candidate.value_type.nullable;
                }
            }
            *binding = crate::binding::SqlFunctionBinding::new(
                selected,
                binding.decimal_overflow_policy(),
            );
            assert_eq!(context.logical_output_type(None, &candidate, &scope), None);
            let before = format!("{candidate:?}");
            let result = context
                .adapt_bound_output_domains(candidate, None, &scope, Span::new(0, 0))
                .unwrap();
            assert_eq!(format!("{result:?}"), before);
        }
    }

    #[test]
    fn m07_container_internal_sortby_subfield_keeps_nested_marker_and_binding() {
        let sql = "select array_sortby((x)->x.key,[named_struct('key',1,'payload',to_bitmap(1))])";
        let expression = output_expressions(sql).remove(0);
        let ExprKind::FunctionCall { args, .. } = &original_transform(&expression).kind else {
            panic!("sortby");
        };
        let ExprKind::FunctionCall { binding, .. } = &original_transform(&args[1]).kind else {
            panic!("subfield");
        };
        assert_eq!(
            binding.function_id.as_str(),
            "builtin.scalar/__array_struct_subfield/v1"
        );
        let DataType::List(item) = &expression.value_type.data_type else {
            panic!("List");
        };
        let DataType::Struct(fields) = item.data_type() else {
            panic!("Struct");
        };
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(&fields[1]),
            Some(LogicalType::Bitmap)
        );
        // SORTBY executes its exact ordinary List<Utf8> key selection. Its
        // coercion retains the independently adapted JSON subfield underneath.
        let (resolved, _, factory) = super::super::analyze(
            &query("select array_sortby((x)->x.key,[named_struct('key',parse_json('{}'),'payload',1)])"),
            &Catalog,
            "db",
        ).unwrap();
        let crate::analysis::QueryBody::Select(select) = resolved.body else {
            panic!("select");
        };
        let json = select.projection[0].expr.clone();
        let ExprKind::FunctionCall { binding, args, .. } = &original_transform(&json).kind else {
            panic!("sortby");
        };
        let outer_selection = binding.resolved().clone();
        let outer_arguments = format!("{args:?}");
        let novarocks_functions::FunctionArgumentType::Value(selected_key) =
            &binding.selected.argument_types[1]
        else {
            panic!("selected key value");
        };
        let key = &args[1];
        assert_eq!(key.value_type.data_type, selected_key.data_type);
        assert_eq!(key.value_type.nullable, selected_key.nullable);
        let DataType::List(outer_item) = &key.value_type.data_type else {
            panic!("key List");
        };
        assert_eq!(outer_item.data_type(), &DataType::Utf8);
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(outer_item),
            None
        );
        let ExprKind::Cast {
            expr: adapted_key,
            target,
            ..
        } = &key.kind
        else {
            panic!("selected key coercion");
        };
        assert_eq!(target, &selected_key.data_type);
        let DataType::List(inner_item) = &adapted_key.value_type.data_type else {
            panic!("adapted key List");
        };
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(inner_item),
            Some(LogicalType::Json)
        );
        let ExprKind::FunctionCall {
            binding: subfield_binding,
            args: subfield_args,
            ..
        } = &original_transform(adapted_key).kind
        else {
            panic!("subfield call");
        };
        assert_eq!(
            subfield_binding.function_id.as_str(),
            "builtin.scalar/__array_struct_subfield/v1"
        );
        assert_eq!(
            subfield_binding.kind,
            novarocks_functions::FunctionKind::Scalar
        );
        let novarocks_functions::FunctionResultType::Scalar(subfield_result) =
            &subfield_binding.selected.result_type
        else {
            panic!("scalar subfield selection");
        };
        assert_eq!(
            subfield_result.data_type,
            original_transform(adapted_key).value_type.data_type
        );
        assert_eq!(
            subfield_result.nullable,
            original_transform(adapted_key).value_type.nullable
        );
        let DataType::List(selected_item) = &subfield_result.data_type else {
            panic!("selected subfield List");
        };
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(selected_item),
            None
        );
        assert!(
            matches!(&subfield_args[1].kind, ExprKind::Literal(crate::analysis::LiteralValue::String(name)) if name == "key")
        );
        let DataType::List(source_item) = &subfield_args[0].value_type.data_type else {
            panic!("source List");
        };
        let DataType::Struct(source_fields) = source_item.data_type() else {
            panic!("source Struct");
        };
        assert_eq!(source_fields[0].name(), "key");
        assert_eq!(
            novarocks_types::logical::logical_type_of_field(&source_fields[0]),
            Some(LogicalType::Json)
        );
        let factory = Rc::new(RefCell::new(factory));
        let scope = AnalyzerScope::new(factory.clone());
        let context = context(factory);
        assert_eq!(
            context.logical_output_type(None, key, &scope),
            Some(SqlType::Array(Box::new(SqlType::Json)))
        );
        let readapted = context
            .adapt_bound_output_domains(json.clone(), None, &scope, Span::new(0, 0))
            .unwrap();
        let ExprKind::FunctionCall { binding, args, .. } = &original_transform(&readapted).kind
        else {
            panic!("sortby selection after readaptation");
        };
        assert_eq!(binding.resolved(), &outer_selection);
        assert_eq!(format!("{args:?}"), outer_arguments);
    }

    #[test]
    fn m07_container_final_compiler_scalar_proof_matches_exact_domains() {
        use crate::compiler::{
            DEFAULT_COMPLETION_LIMITS, SessionOptimizerSettings, SqlCompileControl,
            SqlCompileIntent, SqlCompileProgress, SqlCompiler, SqlFinalPlanCompileRequest,
            SqlPlanningEnvironment, SqlSessionContext, SqlStatementInput,
            builtin_sql_function_catalog, noop_constant_evaluator,
        };
        use novarocks_physical_plan::{
            MAX_SCAN_BATCH_BYTES, MAX_SCAN_BATCH_ROWS, PipelineDopDomain, PlanVersionId,
            ResultValueDomain as D, ScanReadBudget,
        };
        use novarocks_result_contract::{
            NamedScalarField as N, ScalarField as F, ScalarOpaqueType as O, ScalarValueType as V,
        };
        for (sql, domain, scalar) in [
            (
                "select element_at(array_flatten([[to_bitmap(1)]]),1)",
                D::Bitmap,
                V::Opaque(O::Bitmap),
            ),
            (
                "select element_at(array_repeat(parse_json('{}'),2),1)",
                D::Json,
                V::Json,
            ),
            (
                "select element_at(__array_struct_subfield([named_struct('payload',to_bitmap(1))],'payload'),1)",
                D::Bitmap,
                V::Opaque(O::Bitmap),
            ),
            (
                "select array_repeat(parse_json('{}'),2)",
                D::Plain,
                V::List(Box::new(F {
                    nullable: true,
                    value_type: V::Json,
                })),
            ),
            (
                "select array_repeat(cast(variant_get(parse_json('{}'),'$') as variant),2)",
                D::Plain,
                V::List(Box::new(F {
                    nullable: true,
                    value_type: V::Variant,
                })),
            ),
            (
                "select element_at(array_repeat(cast(variant_get(parse_json('{}'),'$') as variant),2),1)",
                D::Variant,
                V::Variant,
            ),
            (
                "select arrays_zip([to_bitmap(1)],[parse_json('{}')])",
                D::Plain,
                V::List(Box::new(F {
                    nullable: true,
                    value_type: V::Struct(vec![
                        N {
                            name: "col1".to_owned(),
                            field: F {
                                nullable: true,
                                value_type: V::Opaque(O::Bitmap),
                            },
                        },
                        N {
                            name: "col2".to_owned(),
                            field: F {
                                nullable: true,
                                value_type: V::Json,
                            },
                        },
                    ]),
                })),
            ),
        ] {
            let request = SqlFinalPlanCompileRequest::new(
                PlanVersionId::try_new([17; 16]).unwrap(),
                SqlStatementInput::sql(sql),
                SqlCompileIntent::Query,
                SqlSessionContext {
                    sql_semantics: Default::default(),
                    current_catalog: Some("iceberg".to_owned()),
                    current_database: "db".to_owned(),
                    optimizer_settings: SessionOptimizerSettings::default(),
                },
                SqlPlanningEnvironment::Distributed,
                builtin_sql_function_catalog().snapshot(),
                noop_constant_evaluator(),
                crate::constant::test_constant_policy(),
                crate::compiler::SqlPhysicalEmissionMode::OriginalNativeV1,
                SqlCompileControl::unbounded(),
                PipelineDopDomain {
                    min: 1,
                    max: 8,
                    requires_power_of_two: true,
                },
                ScanReadBudget {
                    max_batch_rows: MAX_SCAN_BATCH_ROWS,
                    max_batch_bytes: MAX_SCAN_BATCH_BYTES,
                },
                DEFAULT_COMPLETION_LIMITS,
            );
            let SqlCompileProgress::Complete(completed) =
                SqlCompiler::start(request.try_into_completion().unwrap(), fixture_control())
                    .unwrap()
            else {
                panic!("source-free transform unexpectedly needs observations: {sql}");
            };
            let result = completed.plan().result_port().unwrap();
            assert_eq!(result.fields[0].domain, domain, "{sql}");
            let schema = completed.scalar_schema().unwrap();
            assert_eq!(schema.field().value_type, scalar, "{sql}");
            assert_eq!(
                schema.field().nullable,
                result.fields[0].ty.nullable,
                "{sql}"
            );
            assert_eq!(schema.source_slot(), None);
            novarocks_physical_plan::validate_plan(completed.plan()).unwrap();
        }
    }
}

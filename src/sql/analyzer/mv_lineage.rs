//! IVM-A11 MV lineage builder.
//!
//! Given a ResolvedQuery for a single-base projection/filter MV plus the
//! base table's current Iceberg schema, produce the field-id-based
//! lineage that A11's contract persists.

use crate::meta::repository::mv_contract::{
    BaseFieldRecord, ExpressionKind, ExpressionLineage, FilterLineage, JoinContract,
    JoinContractKind, JoinPredicateLineage, OutputColumnLineage, QualifiedFieldLineage,
};
use crate::sql::analysis::{
    BinOp, ExprKind, JoinKind, JoinRelation, QueryBody, Relation, ResolvedQuery, ResolvedSelect,
    TypedExpr,
};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct LineageResult {
    pub base_fields: Vec<BaseFieldRecord>,
    pub output_columns: Vec<OutputColumnLineage>,
    pub filter: Option<FilterLineage>,
}

pub(crate) struct JoinLineageResult {
    pub base_fields_by_table: BTreeMap<String, Vec<BaseFieldRecord>>,
    pub output_columns: Vec<OutputColumnLineage>,
    pub filter: Option<FilterLineage>,
    pub join: JoinContract,
}

/// Build A11 lineage for a single-base projection/filter MV. Caller
/// must have already classified the shape as ProjectionFilter; this
/// function defensively asserts that the resolved query is a single
/// SELECT over a single base scan and returns an error otherwise.
pub(crate) fn build_projection_filter_lineage(
    resolved: &ResolvedQuery,
    base_iceberg_schema: &iceberg::spec::Schema,
) -> Result<LineageResult, String> {
    let select = match &resolved.body {
        QueryBody::Select(s) => s,
        _ => return Err("A11 lineage builder requires a SELECT query".to_string()),
    };
    single_scan_or_err(select)?;

    let mut output_columns = Vec::with_capacity(select.projection.len());
    let mut referenced: std::collections::BTreeMap<i32, BaseFieldRecord> =
        std::collections::BTreeMap::new();

    for item in &select.projection {
        let mut col_refs: Vec<(Option<String>, String)> = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        collect_column_refs(&item.expr, &mut col_refs, &mut kind_hint);

        let mut field_ids = Vec::with_capacity(col_refs.len());
        for (_qualifier, name) in &col_refs {
            let field = resolve_field(base_iceberg_schema, name)?;
            field_ids.push(field.id);
            referenced
                .entry(field.id)
                .or_insert_with(|| BaseFieldRecord {
                    field_id: field.id,
                    name_at_create: field.name.clone(),
                    type_signature: format!("{}", field.field_type),
                    required: field.required,
                });
        }
        field_ids.sort_unstable();
        field_ids.dedup();

        output_columns.push(OutputColumnLineage {
            expression: ExpressionLineage {
                kind: kind_hint.into_kind(),
                referenced_base_field_ids: field_ids,
                referenced_base_fields: vec![],
            },
        });
    }

    let filter = if let Some(filter_expr) = &select.filter {
        let mut col_refs: Vec<(Option<String>, String)> = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        collect_column_refs(filter_expr, &mut col_refs, &mut kind_hint);

        let mut field_ids = Vec::with_capacity(col_refs.len());
        for (_qualifier, name) in &col_refs {
            let field = resolve_field(base_iceberg_schema, name)?;
            field_ids.push(field.id);
            referenced
                .entry(field.id)
                .or_insert_with(|| BaseFieldRecord {
                    field_id: field.id,
                    name_at_create: field.name.clone(),
                    type_signature: format!("{}", field.field_type),
                    required: field.required,
                });
        }
        field_ids.sort_unstable();
        field_ids.dedup();
        Some(FilterLineage {
            referenced_base_field_ids: field_ids,
            referenced_base_fields: vec![],
        })
    } else {
        None
    };

    let base_fields = referenced.into_values().collect();
    Ok(LineageResult {
        base_fields,
        output_columns,
        filter,
    })
}

pub(crate) fn build_join_projection_filter_lineage(
    resolved: &ResolvedQuery,
    base_schemas: &[(&str, &str, &iceberg::spec::Schema)],
) -> Result<JoinLineageResult, String> {
    let select = match &resolved.body {
        QueryBody::Select(s) => s,
        _ => return Err("join lineage builder requires a SELECT query".to_string()),
    };
    let join = match select.from.as_ref() {
        Some(Relation::Join(join)) => join,
        Some(_) => return Err("join lineage builder requires a join relation".to_string()),
        None => return Err("join lineage builder requires a FROM clause".to_string()),
    };

    let mut collector = QualifiedLineageCollector::new(base_schemas);
    let output_columns = select
        .projection
        .iter()
        .map(|item| collector.output_lineage(&item.expr))
        .collect::<Result<Vec<_>, _>>()?;
    let filter = select
        .filter
        .as_ref()
        .map(|expr| collector.filter_lineage(expr))
        .transpose()?;
    let join_contract = collector.join_contract(join)?;

    Ok(JoinLineageResult {
        base_fields_by_table: collector.into_base_fields_by_table(),
        output_columns,
        filter,
        join: join_contract,
    })
}

struct QualifiedLineageCollector<'a> {
    schemas: BTreeMap<String, (&'a str, &'a iceberg::spec::Schema)>,
    base_fields_by_table: BTreeMap<String, BTreeMap<i32, BaseFieldRecord>>,
}

impl<'a> QualifiedLineageCollector<'a> {
    fn new(base_schemas: &[(&'a str, &'a str, &'a iceberg::spec::Schema)]) -> Self {
        let mut schemas = BTreeMap::new();
        for (table_fqn, alias, schema) in base_schemas {
            schemas.insert(alias.to_ascii_lowercase(), (*table_fqn, *schema));
        }
        Self {
            schemas,
            base_fields_by_table: BTreeMap::new(),
        }
    }

    fn output_lineage(&mut self, expr: &TypedExpr) -> Result<OutputColumnLineage, String> {
        let mut refs = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        self.collect_qualified_refs(expr, &mut refs, &mut kind_hint)?;
        Ok(OutputColumnLineage {
            expression: ExpressionLineage {
                kind: kind_hint.into_kind(),
                referenced_base_field_ids: Vec::new(),
                referenced_base_fields: refs,
            },
        })
    }

    fn filter_lineage(&mut self, expr: &TypedExpr) -> Result<FilterLineage, String> {
        let mut refs = Vec::new();
        let mut kind_hint = ExpressionKindHint::default();
        self.collect_qualified_refs(expr, &mut refs, &mut kind_hint)?;
        Ok(FilterLineage {
            referenced_base_field_ids: Vec::new(),
            referenced_base_fields: refs,
        })
    }

    fn collect_qualified_refs(
        &mut self,
        expr: &TypedExpr,
        out: &mut Vec<QualifiedFieldLineage>,
        kind: &mut ExpressionKindHint,
    ) -> Result<(), String> {
        match &expr.kind {
            ExprKind::ColumnRef { qualifier, column } => {
                kind.saw_column();
                let qualifier = qualifier
                    .as_ref()
                    .ok_or_else(|| format!("join MV column `{column}` must be qualified"))?;
                out.push(self.resolve_field(qualifier, column)?);
            }
            ExprKind::Literal(_) => kind.saw_literal(),
            ExprKind::Cast { .. } => {
                kind.saw_cast();
                for child in typed_expr_children(expr) {
                    self.collect_qualified_refs(child, out, kind)?;
                }
            }
            ExprKind::Nested(_) => {
                for child in typed_expr_children(expr) {
                    self.collect_qualified_refs(child, out, kind)?;
                }
            }
            _ => {
                kind.saw_func();
                for child in typed_expr_children(expr) {
                    self.collect_qualified_refs(child, out, kind)?;
                }
            }
        }
        out.sort_by(|a, b| {
            (a.table_fqn.as_str(), a.field_id).cmp(&(b.table_fqn.as_str(), b.field_id))
        });
        out.dedup_by(|a, b| a.table_fqn == b.table_fqn && a.field_id == b.field_id);
        Ok(())
    }

    fn resolve_field(
        &mut self,
        qualifier: &str,
        column: &str,
    ) -> Result<QualifiedFieldLineage, String> {
        let key = qualifier.to_ascii_lowercase();
        let (table_fqn, schema) = self.schemas.get(&key).ok_or_else(|| {
            format!("join MV qualifier `{qualifier}` does not match a base table alias")
        })?;
        let field = resolve_field(schema, column)?;
        self.base_fields_by_table
            .entry((*table_fqn).to_string())
            .or_default()
            .entry(field.id)
            .or_insert_with(|| BaseFieldRecord {
                field_id: field.id,
                name_at_create: field.name.clone(),
                type_signature: format!("{}", field.field_type),
                required: field.required,
            });
        Ok(QualifiedFieldLineage {
            table_fqn: (*table_fqn).to_string(),
            qualifier_at_create: qualifier.to_string(),
            field_id: field.id,
        })
    }

    fn join_contract(&mut self, join: &JoinRelation) -> Result<JoinContract, String> {
        if join.join_type != JoinKind::Inner {
            return Err("incremental join MV supports only inner equi-join lineage".to_string());
        }
        let condition = join
            .condition
            .as_ref()
            .ok_or_else(|| "join MV requires ON condition".to_string())?;
        let sides = join_side_qualifiers(join)?;
        let mut predicates = Vec::new();
        self.collect_join_predicates(condition, &sides, &mut predicates)?;
        if predicates.is_empty() {
            return Err("incremental join MV requires at least one join predicate".to_string());
        }
        Ok(JoinContract {
            kind: JoinContractKind::InnerEquiJoin,
            predicates,
        })
    }

    fn collect_join_predicates(
        &mut self,
        expr: &TypedExpr,
        sides: &JoinSideQualifiers,
        out: &mut Vec<JoinPredicateLineage>,
    ) -> Result<(), String> {
        match &unwrap_nested_expr(expr).kind {
            ExprKind::BinaryOp {
                left,
                op: BinOp::And,
                right,
            } => {
                self.collect_join_predicates(left, sides, out)?;
                self.collect_join_predicates(right, sides, out)
            }
            ExprKind::BinaryOp {
                left,
                op: BinOp::Eq,
                right,
            } => {
                let left_ref = self.single_qualified_column(left)?;
                let right_ref = self.single_qualified_column(right)?;
                out.push(normalize_join_predicate(left_ref, right_ref, sides)?);
                Ok(())
            }
            _ => Err(
                "incremental join MV supports only AND-combined equi-join predicates".to_string(),
            ),
        }
    }

    fn single_qualified_column(
        &mut self,
        expr: &TypedExpr,
    ) -> Result<QualifiedFieldLineage, String> {
        let ExprKind::ColumnRef { qualifier, column } = &unwrap_nested_expr(expr).kind else {
            return Err(
                "incremental join MV join key must be a qualified column reference".to_string(),
            );
        };
        let qualifier = qualifier
            .as_ref()
            .ok_or_else(|| "incremental join MV join key must be <alias>.<column>".to_string())?;
        self.resolve_field(qualifier, column)
    }

    fn into_base_fields_by_table(self) -> BTreeMap<String, Vec<BaseFieldRecord>> {
        self.base_fields_by_table
            .into_iter()
            .map(|(table, fields)| (table, fields.into_values().collect()))
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JoinSide {
    Left,
    Right,
}

struct JoinSideQualifiers {
    left: BTreeSet<String>,
    right: BTreeSet<String>,
}

fn join_side_qualifiers(join: &JoinRelation) -> Result<JoinSideQualifiers, String> {
    Ok(JoinSideQualifiers {
        left: relation_qualifiers(&join.left, "left")?,
        right: relation_qualifiers(&join.right, "right")?,
    })
}

fn relation_qualifiers(relation: &Relation, side_name: &str) -> Result<BTreeSet<String>, String> {
    match relation {
        Relation::Scan(scan) => {
            let qualifier = scan.alias.as_deref().unwrap_or(&scan.table.name);
            Ok(one_qualifier(qualifier))
        }
        Relation::Subquery { alias, .. } => Ok(one_qualifier(alias)),
        _ => Err(format!(
            "join MV lineage requires a single scan or subquery on the {side_name} side"
        )),
    }
}

fn one_qualifier(qualifier: &str) -> BTreeSet<String> {
    BTreeSet::from([qualifier.to_ascii_lowercase()])
}

fn normalize_join_predicate(
    left_ref: QualifiedFieldLineage,
    right_ref: QualifiedFieldLineage,
    sides: &JoinSideQualifiers,
) -> Result<JoinPredicateLineage, String> {
    let left_side = join_side_for_qualifier(&left_ref.qualifier_at_create, sides)?;
    let right_side = join_side_for_qualifier(&right_ref.qualifier_at_create, sides)?;
    match (left_side, right_side) {
        (JoinSide::Left, JoinSide::Right) => Ok(JoinPredicateLineage {
            left: left_ref,
            right: right_ref,
        }),
        (JoinSide::Right, JoinSide::Left) => Ok(JoinPredicateLineage {
            left: right_ref,
            right: left_ref,
        }),
        _ => Err(
            "incremental join MV join predicate must reference one column from each join side"
                .to_string(),
        ),
    }
}

fn join_side_for_qualifier(
    qualifier: &str,
    sides: &JoinSideQualifiers,
) -> Result<JoinSide, String> {
    let key = qualifier.to_ascii_lowercase();
    let on_left = sides.left.contains(&key);
    let on_right = sides.right.contains(&key);
    match (on_left, on_right) {
        (true, false) => Ok(JoinSide::Left),
        (false, true) => Ok(JoinSide::Right),
        (true, true) => Err(format!(
            "join MV qualifier `{qualifier}` is ambiguous across join sides"
        )),
        (false, false) => Err(format!(
            "join MV qualifier `{qualifier}` does not match either join side"
        )),
    }
}

fn unwrap_nested_expr(mut expr: &TypedExpr) -> &TypedExpr {
    while let ExprKind::Nested(inner) = &expr.kind {
        expr = inner.as_ref();
    }
    expr
}

fn typed_expr_children(expr: &TypedExpr) -> Vec<&TypedExpr> {
    match &expr.kind {
        ExprKind::BinaryOp { left, right, .. } => vec![left.as_ref(), right.as_ref()],
        ExprKind::UnaryOp { expr, .. }
        | ExprKind::Cast { expr, .. }
        | ExprKind::IsNull { expr, .. }
        | ExprKind::IsTruthValue { expr, .. }
        | ExprKind::Nested(expr)
        | ExprKind::Lambda { body: expr, .. } => vec![expr.as_ref()],
        ExprKind::FunctionCall { args, .. }
        | ExprKind::AggregateCall { args, .. }
        | ExprKind::WindowCall { args, .. } => args.iter().collect(),
        ExprKind::LambdaFunction { body, .. } => vec![body.as_ref()],
        ExprKind::InList { expr, list, .. } => {
            let mut out = Vec::with_capacity(1 + list.len());
            out.push(expr.as_ref());
            out.extend(list.iter());
            out
        }
        ExprKind::Between {
            expr, low, high, ..
        } => vec![expr.as_ref(), low.as_ref(), high.as_ref()],
        ExprKind::Like { expr, pattern, .. } => vec![expr.as_ref(), pattern.as_ref()],
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut out = Vec::new();
            if let Some(operand) = operand {
                out.push(operand.as_ref());
            }
            for (when, then) in when_then {
                out.push(when);
                out.push(then);
            }
            if let Some(else_expr) = else_expr {
                out.push(else_expr.as_ref());
            }
            out
        }
        ExprKind::ColumnRef { .. }
        | ExprKind::LambdaParamRef { .. }
        | ExprKind::Literal(_)
        | ExprKind::SubqueryPlaceholder { .. } => Vec::new(),
    }
}

fn single_scan_or_err(select: &ResolvedSelect) -> Result<(), String> {
    match select.from.as_ref() {
        Some(Relation::Scan(_)) => Ok(()),
        Some(_) => Err(
            "A11 lineage builder requires a single-base SCAN, not a join or subquery".to_string(),
        ),
        None => Err("A11 lineage builder requires a FROM clause".to_string()),
    }
}

fn resolve_field<'a>(
    schema: &'a iceberg::spec::Schema,
    column_name: &str,
) -> Result<&'a iceberg::spec::NestedField, String> {
    schema
        .as_struct()
        .fields()
        .iter()
        .find(|f| f.name.eq_ignore_ascii_case(column_name))
        .map(|f| f.as_ref())
        .ok_or_else(|| {
            format!(
                "base iceberg schema does not contain column {column_name}; cannot build A11 lineage"
            )
        })
}

/// Walks a TypedExpr, collecting every ColumnRef as (qualifier, name).
/// Also updates a coarse ExpressionKindHint.
fn collect_column_refs(
    expr: &TypedExpr,
    out: &mut Vec<(Option<String>, String)>,
    kind: &mut ExpressionKindHint,
) {
    match &expr.kind {
        ExprKind::ColumnRef { qualifier, column } => {
            out.push((qualifier.clone(), column.clone()));
            kind.saw_column();
        }
        ExprKind::Literal(_) => {
            kind.saw_literal();
        }
        ExprKind::Cast { expr, .. } => {
            // A CAST over a plain column or literal (e.g. CAST(amount AS DOUBLE))
            // is a common projection; mark as cast so the kind can be classified
            // as ExpressionKind::Cast when no other operations are present.
            kind.saw_cast();
            collect_column_refs(expr, out, kind);
        }
        ExprKind::BinaryOp { left, right, .. } => {
            kind.saw_func();
            collect_column_refs(left, out, kind);
            collect_column_refs(right, out, kind);
        }
        ExprKind::UnaryOp { expr, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
        }
        ExprKind::FunctionCall { args, .. } | ExprKind::AggregateCall { args, .. } => {
            kind.saw_func();
            for a in args {
                collect_column_refs(a, out, kind);
            }
        }
        ExprKind::IsNull { expr, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
        }
        ExprKind::InList { expr, list, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
            for e in list {
                collect_column_refs(e, out, kind);
            }
        }
        ExprKind::Between {
            expr, low, high, ..
        } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
            collect_column_refs(low, out, kind);
            collect_column_refs(high, out, kind);
        }
        ExprKind::Like { expr, pattern, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
            collect_column_refs(pattern, out, kind);
        }
        ExprKind::Case {
            operand,
            when_then,
            else_expr,
        } => {
            kind.saw_func();
            if let Some(op) = operand {
                collect_column_refs(op, out, kind);
            }
            for (w, t) in when_then {
                collect_column_refs(w, out, kind);
                collect_column_refs(t, out, kind);
            }
            if let Some(e) = else_expr {
                collect_column_refs(e, out, kind);
            }
        }
        ExprKind::IsTruthValue { expr, .. } => {
            kind.saw_func();
            collect_column_refs(expr, out, kind);
        }
        ExprKind::Nested(inner) => {
            collect_column_refs(inner, out, kind);
        }
        // Lambda, window, subquery placeholder, lambda param — not expected
        // in A11 phase 1 projection/filter MVs. A9 shape classification
        // rejects them before reaching here.
        _ => {
            kind.saw_func();
        }
    }
}

#[derive(Default)]
struct ExpressionKindHint {
    saw_column: bool,
    saw_literal: bool,
    saw_func: bool,
    saw_cast: bool,
}

impl ExpressionKindHint {
    fn saw_column(&mut self) {
        self.saw_column = true;
    }
    fn saw_literal(&mut self) {
        self.saw_literal = true;
    }
    fn saw_func(&mut self) {
        self.saw_func = true;
    }
    fn saw_cast(&mut self) {
        self.saw_cast = true;
    }
    fn into_kind(self) -> ExpressionKind {
        match (
            self.saw_column,
            self.saw_literal,
            self.saw_func,
            self.saw_cast,
        ) {
            (true, false, false, false) => ExpressionKind::Column,
            (false, true, false, false) => ExpressionKind::Literal,
            (false, false, true, false) => ExpressionKind::Func,
            // Cast over a column or literal with no other operations → Cast
            (_, _, false, true) => ExpressionKind::Cast,
            // All other combinations → Mixed
            _ => ExpressionKind::Mixed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::catalog::{CatalogProvider, ColumnDef, TableDef, TableStorage};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::sync::Arc;

    fn base_schema() -> Schema {
        Schema::builder()
            .with_schema_id(0)
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::required(
                    2,
                    "region",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "amount",
                    Type::Primitive(PrimitiveType::Double),
                )),
            ])
            .build()
            .expect("build schema")
    }

    struct JoinLineageFixture {
        left_schema: Schema,
        right_schema: Schema,
    }

    struct SingleLineageFixture {
        schema: Schema,
    }

    impl SingleLineageFixture {
        fn new() -> Self {
            Self {
                schema: base_schema(),
            }
        }

        fn analyze(&self, sql: &str) -> ResolvedQuery {
            let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse");
            let sqlparser::ast::Statement::Query(query) = stmt else {
                panic!("expected query");
            };
            let (resolved, _registry) =
                crate::sql::analyzer::analyze(&query, &SingleLineageCatalog, "default")
                    .expect("analyze");
            resolved
        }
    }

    struct SingleLineageCatalog;

    impl CatalogProvider for SingleLineageCatalog {
        fn get_table(&self, _database: &str, table: &str) -> Result<TableDef, String> {
            match table {
                "fact" => Ok(TableDef {
                    name: table.to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                        },
                        ColumnDef {
                            name: "region".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: false,
                            write_default: None,
                        },
                        ColumnDef {
                            name: "amount".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from("/tmp/fact.parquet"),
                    },
                }),
                _ => Err(format!("table not found: {table}")),
            }
        }
    }

    impl JoinLineageFixture {
        fn new() -> Self {
            Self {
                left_schema: Schema::builder()
                    .with_schema_id(0)
                    .with_fields(vec![
                        Arc::new(NestedField::required(
                            10,
                            "id",
                            Type::Primitive(PrimitiveType::Long),
                        )),
                        Arc::new(NestedField::optional(
                            11,
                            "payload",
                            Type::Primitive(PrimitiveType::String),
                        )),
                    ])
                    .build()
                    .expect("build left schema"),
                right_schema: Schema::builder()
                    .with_schema_id(0)
                    .with_fields(vec![
                        Arc::new(NestedField::required(
                            20,
                            "id",
                            Type::Primitive(PrimitiveType::Long),
                        )),
                        Arc::new(NestedField::optional(
                            21,
                            "payload",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::optional(
                            22,
                            "amount",
                            Type::Primitive(PrimitiveType::Double),
                        )),
                    ])
                    .build()
                    .expect("build right schema"),
            }
        }

        fn analyze(&self, sql: &str) -> ResolvedQuery {
            let stmt = crate::sql::parser::parse_sql_raw(sql).expect("parse");
            let sqlparser::ast::Statement::Query(query) = stmt else {
                panic!("expected query");
            };
            let (resolved, _registry) =
                crate::sql::analyzer::analyze(&query, &JoinLineageCatalog, "default")
                    .expect("analyze");
            resolved
        }
    }

    struct JoinLineageCatalog;

    impl CatalogProvider for JoinLineageCatalog {
        fn get_table(&self, _database: &str, table: &str) -> Result<TableDef, String> {
            match table {
                "left_tbl" | "right_tbl" => Ok(TableDef {
                    name: table.to_string(),
                    columns: vec![
                        ColumnDef {
                            name: "id".to_string(),
                            data_type: arrow::datatypes::DataType::Int64,
                            nullable: false,
                            write_default: None,
                        },
                        ColumnDef {
                            name: "payload".to_string(),
                            data_type: arrow::datatypes::DataType::Utf8,
                            nullable: true,
                            write_default: None,
                        },
                        ColumnDef {
                            name: "amount".to_string(),
                            data_type: arrow::datatypes::DataType::Float64,
                            nullable: true,
                            write_default: None,
                        },
                    ],
                    iceberg_row_lineage_metadata_columns: vec![],
                    iceberg_table: None,
                    storage: TableStorage::LocalParquetFile {
                        path: std::path::PathBuf::from(format!("/tmp/{table}.parquet")),
                    },
                }),
                _ => Err(format!("table not found: {table}")),
            }
        }
    }

    #[test]
    fn single_base_aggregate_lineage_records_aggregate_input_columns() {
        let fixture = SingleLineageFixture::new();
        let resolved = fixture.analyze(
            "select region, sum(amount) as total, count(amount) as non_null_amounts, count(*) as rows \
             from ns.fact group by region",
        );

        let result = build_projection_filter_lineage(&resolved, &fixture.schema).expect("lineage");

        assert_eq!(result.output_columns.len(), 4);
        assert_eq!(
            result.output_columns[0]
                .expression
                .referenced_base_field_ids,
            vec![2]
        );
        assert_eq!(
            result.output_columns[1]
                .expression
                .referenced_base_field_ids,
            vec![3]
        );
        assert_eq!(
            result.output_columns[2]
                .expression
                .referenced_base_field_ids,
            vec![3]
        );
        assert!(
            result.output_columns[3]
                .expression
                .referenced_base_field_ids
                .is_empty()
        );
        let base_field_ids = result
            .base_fields
            .iter()
            .map(|field| field.field_id)
            .collect::<Vec<_>>();
        assert_eq!(base_field_ids, vec![2, 3]);
    }

    mod join_lineage {
        use super::*;

        fn lineage_for_on(on_expr: &str) -> Result<JoinLineageResult, String> {
            let sql = format!(
                "select l.id as left_id, r.id as right_id \
                 from ns.left_tbl l join ns.right_tbl r on {on_expr} \
                 where l.id > 0"
            );
            let fixture = JoinLineageFixture::new();
            let resolved = fixture.analyze(&sql);
            build_join_projection_filter_lineage(
                &resolved,
                &[
                    ("ice.ns.left_tbl", "l", &fixture.left_schema),
                    ("ice.ns.right_tbl", "r", &fixture.right_schema),
                ],
            )
        }

        #[test]
        fn distinguishes_same_named_columns_by_alias() {
            let result = lineage_for_on("l.id = r.id").expect("join lineage");
            assert_eq!(result.output_columns.len(), 2);
            assert_eq!(
                result.output_columns[0].expression.referenced_base_fields[0].table_fqn,
                "ice.ns.left_tbl"
            );
            assert_eq!(
                result.output_columns[1].expression.referenced_base_fields[0].table_fqn,
                "ice.ns.right_tbl"
            );
            assert_eq!(result.join.predicates.len(), 1);
            assert_eq!(result.join.predicates[0].left.table_fqn, "ice.ns.left_tbl");
            assert_eq!(result.join.predicates[0].left.qualifier_at_create, "l");
            assert_eq!(result.join.predicates[0].left.field_id, 10);
            assert_eq!(
                result.join.predicates[0].right.table_fqn,
                "ice.ns.right_tbl"
            );
            assert_eq!(result.join.predicates[0].right.qualifier_at_create, "r");
            assert_eq!(result.join.predicates[0].right.field_id, 20);
            assert_eq!(
                result.filter.as_ref().unwrap().referenced_base_fields[0].table_fqn,
                "ice.ns.left_tbl"
            );
            assert_eq!(result.base_fields_by_table["ice.ns.left_tbl"].len(), 1);
            assert_eq!(result.base_fields_by_table["ice.ns.right_tbl"].len(), 1);
        }

        #[test]
        fn accepts_parenthesized_join_predicate() {
            let result = lineage_for_on("(l.id = r.id)").expect("join lineage");
            assert_eq!(result.join.predicates.len(), 1);
            assert_eq!(result.join.predicates[0].left.table_fqn, "ice.ns.left_tbl");
            assert_eq!(
                result.join.predicates[0].right.table_fqn,
                "ice.ns.right_tbl"
            );
        }

        #[test]
        fn accepts_parenthesized_join_key_operands() {
            let result = lineage_for_on("(l.id) = (r.id)").expect("join lineage");
            assert_eq!(result.join.predicates.len(), 1);
            assert_eq!(result.join.predicates[0].left.qualifier_at_create, "l");
            assert_eq!(result.join.predicates[0].right.qualifier_at_create, "r");
        }

        #[test]
        fn normalizes_reversed_join_predicate_to_left_right_order() {
            let result = lineage_for_on("r.id = l.id").expect("join lineage");
            assert_eq!(result.join.predicates.len(), 1);
            assert_eq!(result.join.predicates[0].left.table_fqn, "ice.ns.left_tbl");
            assert_eq!(result.join.predicates[0].left.qualifier_at_create, "l");
            assert_eq!(
                result.join.predicates[0].right.table_fqn,
                "ice.ns.right_tbl"
            );
            assert_eq!(result.join.predicates[0].right.qualifier_at_create, "r");
        }

        #[test]
        fn rejects_same_side_join_predicate() {
            let err = match lineage_for_on("l.id = l.id") {
                Ok(_) => panic!("same side predicate should be rejected"),
                Err(err) => err,
            };
            assert!(
                err.contains("one column from each join side"),
                "unexpected error: {err}"
            );
        }

        #[test]
        fn aggregate_output_records_qualified_input_columns() {
            let sql = "select l.id, sum(r.amount) as total \
                       from ns.left_tbl l join ns.right_tbl r on l.id = r.id \
                       group by l.id";
            let fixture = JoinLineageFixture::new();
            let resolved = fixture.analyze(sql);
            let result = build_join_projection_filter_lineage(
                &resolved,
                &[
                    ("ice.ns.left_tbl", "l", &fixture.left_schema),
                    ("ice.ns.right_tbl", "r", &fixture.right_schema),
                ],
            )
            .expect("join lineage");

            assert_eq!(
                result.output_columns[0].expression.referenced_base_fields,
                vec![QualifiedFieldLineage {
                    table_fqn: "ice.ns.left_tbl".to_string(),
                    qualifier_at_create: "l".to_string(),
                    field_id: 10,
                }]
            );
            assert_eq!(
                result.output_columns[1].expression.referenced_base_fields,
                vec![QualifiedFieldLineage {
                    table_fqn: "ice.ns.right_tbl".to_string(),
                    qualifier_at_create: "r".to_string(),
                    field_id: 22,
                }]
            );
        }
    }

    #[test]
    fn expression_kind_hint_cast_over_column_is_cast() {
        let mut h = ExpressionKindHint::default();
        h.saw_cast();
        h.saw_column();
        assert_eq!(h.into_kind(), ExpressionKind::Cast);
    }

    #[test]
    fn expression_kind_hint_cast_over_literal_is_cast() {
        let mut h = ExpressionKindHint::default();
        h.saw_cast();
        h.saw_literal();
        assert_eq!(h.into_kind(), ExpressionKind::Cast);
    }

    #[test]
    fn expression_kind_hint_cast_plus_func_is_mixed() {
        let mut h = ExpressionKindHint::default();
        h.saw_cast();
        h.saw_func();
        h.saw_column();
        assert_eq!(h.into_kind(), ExpressionKind::Mixed);
    }

    #[test]
    fn expression_kind_hint_pure_column_is_column() {
        let mut h = ExpressionKindHint::default();
        h.saw_column();
        assert_eq!(h.into_kind(), ExpressionKind::Column);
    }

    #[test]
    fn expression_kind_hint_pure_literal_is_literal() {
        let mut h = ExpressionKindHint::default();
        h.saw_literal();
        assert_eq!(h.into_kind(), ExpressionKind::Literal);
    }

    #[test]
    fn resolve_field_finds_column_case_insensitive() {
        let s = base_schema();
        let f = resolve_field(&s, "REGION").expect("find region");
        assert_eq!(f.id, 2);
        assert_eq!(format!("{}", f.field_type), "string");
    }

    #[test]
    fn resolve_field_errors_on_missing_column() {
        let s = base_schema();
        let err = resolve_field(&s, "nope").unwrap_err();
        assert!(err.contains("nope"), "{err}");
    }
}

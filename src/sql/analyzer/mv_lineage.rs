//! IVM-A11 MV lineage builder.
//!
//! Given a ResolvedQuery for a single-base projection/filter MV plus the
//! base table's current Iceberg schema, produce the field-id-based
//! lineage that A11's contract persists.

use crate::meta::repository::mv_contract::{
    BaseFieldRecord, ExpressionKind, ExpressionLineage, FilterLineage, OutputColumnLineage,
};
use crate::sql::analysis::{
    ExprKind, QueryBody, Relation, ResolvedQuery, ResolvedSelect, TypedExpr,
};

pub(crate) struct LineageResult {
    pub base_fields: Vec<BaseFieldRecord>,
    pub output_columns: Vec<OutputColumnLineage>,
    pub filter: Option<FilterLineage>,
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
        ExprKind::FunctionCall { args, .. } => {
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
        // Lambda, window, aggregate, subquery placeholder, lambda param —
        // not expected in A11 phase 1 projection/filter MVs. A9 shape
        // classification rejects them before reaching here.
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
